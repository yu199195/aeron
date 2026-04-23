# Archive.launch 源码解析

## 1. 概述

在 `ClusteredMediaDriver`、`ArchivingMediaDriver`、`ClusterBackupMediaDriver` 等聚合组件中， Archive 与 MediaDriver 常在同一进程中启动。典型调用方式如下：

```java
archive = Archive.launch(archiveCtx
    .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
    .aeronDirectoryName(driver.aeronDirectoryName())
    .errorHandler(errorHandler)
    .errorCounter(errorCounter));
```

本文分析该 `Archive.launch()` 调用及其中各参数的职责与实现原理。

---

## 2. 调用上下文

以 `ClusteredMediaDriver.launch()` 为例，完整调用链如下：

```
ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusModuleCtx)
    → MediaDriver.launch(driverCtx)
    → 构造 errorCounter / errorHandler
    → Archive.launch(archiveCtx.mediaDriverAgentInvoker(...).aeronDirectoryName(...)...)
    → ConsensusModule.launch(...)
```

即：先启动 MediaDriver，再基于已启动的 driver 配置 archiveCtx，最后 launch Archive。

---

## 3. Archive.launch 方法本身

### 3.1 入口代码

```java
// Archive.java:242
public static Archive launch(final Context ctx)
{
    final Archive archive = new Archive(ctx);
    if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
    {
        archive.conductorInvoker.start();
    }
    else
    {
        AgentRunner.startOnThread(archive.conductorRunner, ctx.threadFactory());
    }
    return archive;
}
```

- 使用 `Archive.Context ctx` 创建 `Archive` 实例
- 根据 `threadingMode` 决定：
  - `INVOKER`：使用 `conductorInvoker.start()`，由外部线程通过 `invoke()` 驱动
  - 其它：启动独立线程运行 `conductorRunner`（如 DEDICATED 模式）

### 3.2 构造过程

```java
// Archive.java:127-164
Archive(final Context ctx)
{
    ctx.conclude();
    this.ctx = ctx;

    final ArchiveConductor conductor = DEDICATED == ctx.threadingMode() ?
        (new DedicatedModeArchiveConductor(ctx)) : (new SharedModeArchiveConductor(ctx));

    if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
    {
        conductorInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), conductor);
        conductorRunner = null;
    }
    else
    {
        conductorInvoker = null;
        conductorRunner = new AgentRunner(...);
    }
}
```

- `ctx.conclude()`：完成 Context 的校验与默认值填充
- 根据 threading 模式创建 `DedicatedModeArchiveConductor` 或 `SharedModeArchiveConductor`
- 若为 INVOKER 模式：只创建 `AgentInvoker`，不启动线程，由调用方驱动

---

## 4. 各参数详解

### 4.1 mediaDriverAgentInvoker(driver.sharedAgentInvoker())

**作用：** 将 MediaDriver 的共享 AgentInvoker 传给 Archive，实现“同线程驱动”的嵌入模式。

**原理：**

1. **MediaDriver.sharedAgentInvoker()**

   - MediaDriver 在非线程模式（如 `CONDUCTOR` / `SHARED` 等）下会创建 `sharedInvoker`
   - 调用方线程周期性调用 `sharedInvoker.invoke()` 驱动 MediaDriver 的 Agent 逻辑
   - `sharedAgentInvoker()` 返回的就是这个 invoker

2. **Archive 如何使用**

   - 在 `ctx.conclude()` 中，若设置了 `mediaDriverAgentInvoker`，则必须使用 `threadingMode = INVOKER`
   - Archive 内部会创建自己的 conductor invoker，并通过 `invokeDriverConductor()` 定期调用 `driverAgentInvoker.invoke()`：

   ```java
   // ArchiveConductor.java:395-410
   final int invokeDriverConductor()
   {
       if (null != driverAgentInvoker)
       {
           workCount += driverAgentInvoker.invoke();
           if (driverAgentInvoker.isClosed())
               throw new AgentTerminationException("unexpected driver close");
       }
       return workCount;
   }
   ```

3. **调用链示意**

   ```
   调用方线程循环
       → conductorInvoker.invoke()   (Archive conductor)
           → invokeDriverConductor()
               → driverAgentInvoker.invoke()   (MediaDriver shared agents)
   ```

**效果：** Archive 与 MediaDriver 在同一线程/调用链中执行，避免额外线程和锁，降低延迟和上下文切换。

---

### 4.2 aeronDirectoryName(driver.aeronDirectoryName())

**作用：** 使 Archive 的 Aeron 客户端与 MediaDriver 使用同一 CnC 与共享内存目录，保证 IPC 通信。

**原理：**

1. **aeron 目录的含义**

   - 存放 CnC（Command and Control）文件、计数器、Log 等
   - MediaDriver 在此目录创建并管理这些资源
   - Aeron Client 必须使用相同目录才能与 MediaDriver 正确通信

2. **Archive 中的使用**

   - 若 Archive 未显式传入 `Aeron` 实例，会在 `ctx.conclude()` 中创建新的 Aeron client：
   - `Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectoryName)...)`
   - `aeronDirectoryName` 若未设置，会从已有 `Aeron` 取；若为独立进程启动 Archive，必须显式指定

3. **为何用 driver.aeronDirectoryName()**

   - `driver.aeronDirectoryName()` 返回 MediaDriver 当前使用的目录路径
   - 传入 archiveCtx 后，Archive 的 Aeron client 会连到同一个 MediaDriver，实现进程内通信

---

### 4.3 errorHandler(errorHandler)

**作用：** 统一错误处理逻辑，保证 Archive 和 MediaDriver 的异常能被同一 handler 处理。

**来源：**

```java
final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
    archiveCtx.errorHandler() : driverCtx.errorHandler();
```

- 优先使用 `archiveCtx.errorHandler()`，否则使用 `driverCtx.errorHandler()`
- 在聚合场景下，通常希望所有组件共用一个 error handler（日志、监控、关闭流程等）

---

### 4.4 errorCounter(errorCounter)

**作用：** 用于错误计数，可与 Aeron 的 CnC 共享，便于监控和诊断。

**来源：**

```java
final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
    archiveCtx.errorCounter() : new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);
```

- 若 archiveCtx 已提供 `errorCounter`，则直接使用
- 否则基于 MediaDriver 的 `countersValuesBuffer` 分配新的 `AtomicCounter`
- 这样错误计数与 CnC 中的计数器体系一致，便于统一查看（如 `aeron-stat`）

---

## 5. 与 threadingMode 的约束

设置 `mediaDriverAgentInvoker` 时，必须同时设置 `threadingMode(INVOKER)`：

```java
// Archive.java:1224-1229
if (null != mediaDriverAgentInvoker && ArchiveThreadingMode.INVOKER != threadingMode)
{
    throw new ConfigurationException(
        "Archive.Context.threadingMode(ArchiveThreadingMode.INVOKER) must be set if " +
        "Archive.Context.mediaDriverAgentInvoker is set");
}
```

原因：只有 INVOKER 模式下，Archive 的 conductor 才由外部调用 `invoke()` 驱动，从而在循环中顺带调用 `driverAgentInvoker.invoke()`，实现与 MediaDriver 共享同一线程/调用链。

---

## 6. 典型使用场景

| 场景                         | mediaDriverAgentInvoker | aeronDirectoryName | 说明                                   |
|-----------------------------|-------------------------|--------------------|----------------------------------------|
| ClusteredMediaDriver        | ✓ driver.sharedAgentInvoker() | driver.aeronDirectoryName() | 聚合部署，共享线程和 CnC 目录          |
| ArchivingMediaDriver        | ✓ driver.sharedAgentInvoker() | driverCtx.aeronDirectoryName() | MediaDriver + Archive 同进程           |
| ClusterBackupMediaDriver    | ✓ driver.sharedAgentInvoker() | -                  | 备份节点，同样嵌入                     |
|  standalone Archive 进程    | 不设置                  | 显式指定 MediaDriver 目录 | 与 MediaDriver 不同进程，需明确目录    |

---

## 7. 相关类与调用关系

```
ClusteredMediaDriver / ArchivingMediaDriver
    └── MediaDriver
    │       └── sharedAgentInvoker()
    └── Archive.launch(archiveCtx
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .aeronDirectoryName(driver.aeronDirectoryName())
            .errorHandler(...)
            .errorCounter(...))
                └── Archive
                    └── SharedModeArchiveConductor / DedicatedModeArchiveConductor
                            └── invokeDriverConductor() → driverAgentInvoker.invoke()
```

---

## 8. 小结

`Archive.launch(archiveCtx.mediaDriverAgentInvoker(...).aeronDirectoryName(...)...)` 的作用可以概括为：

1. **mediaDriverAgentInvoker**：使 Archive 与 MediaDriver 共享同一 Agent 调用链，实现轻量、同线程的嵌入模式。
2. **aeronDirectoryName**：确保 Archive 的 Aeron 客户端连到同一 MediaDriver 的 CnC 和共享内存。
3. **errorHandler / errorCounter**：统一错误处理和计数，便于运维和监控。

配合 `threadingMode(INVOKER)`，可构建高效的进程内 MediaDriver + Archive 聚合部署，典型场景如 Aeron Cluster 和 ArchivingMediaDriver。

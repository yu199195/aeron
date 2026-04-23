# ArchivingMediaDriver.launch 与 AeronArchive.connect 原理解析

## 1. 概述

在 Aeron Archive 的典型使用场景中（如 `ArchiveSender`），需要完成两件事：

1. **ArchivingMediaDriver.launch**：在当前进程内以嵌入式方式启动 MediaDriver + Archive 一体化服务。
2. **AeronArchive.connect**：创建与本地/远程 Archive 的控制会话（Control Session），以便执行 `startRecording`、`stopRecording`、`replay` 等操作。

本文档详解这两个方法的实现原理、调用链及其内部子函数。

---

## 2. ArchivingMediaDriver.launch

### 2.1 作用

`ArchivingMediaDriver` 是 MediaDriver 与 Archive 的聚合组件，在同一进程中运行。`launch(driverCtx, archiveCtx)` 完成：

- 启动 MediaDriver（负责 Aeron 的底层 IPC/UDP 通信）
- 启动 Archive（负责录制、回放、目录管理等）
- 使 Archive 复用 MediaDriver 的 CnC 目录、错误处理、可选的 AgentInvoker

### 2.2 入口与调用链

```java
ArchivingMediaDriver.launch(driverCtx, archiveCtx)
    ├── MediaDriver.launch(driverCtx)
    ├── 构造 errorCounter / errorHandler
    └── Archive.launch(archiveCtx
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .errorHandler(errorHandler)
            .errorCounter(errorCounter))
```

### 2.3 子函数说明

| 步骤 | 子函数/调用 | 说明 |
|-----|-------------|------|
| 1 | `MediaDriver.launch(driverCtx)` | 解析 aeron 目录、创建 CnC 文件、初始化 Counters/Proxies；创建 DriverConductor、Receiver、Sender 三大 Agent；按 `threadingMode` 启动工作线程或 Invoker |
| 2 | `errorCounter` | 优先使用 `archiveCtx.errorCounter()`；否则基于 `driverCtx.countersValuesBuffer()` 创建 `AtomicCounter`，与 CnC 共享 |
| 3 | `errorHandler` | 优先使用 `archiveCtx.errorHandler()`，否则使用 `driverCtx.errorHandler()` |
| 4 | `Archive.launch(archiveCtx...)` | 将 `mediaDriverAgentInvoker`、`aeronDirectoryName`、`errorHandler`、`errorCounter` 注入 archiveCtx；Archive 与 MediaDriver 共享同一 CnC 目录；若设置了 `mediaDriverAgentInvoker`，则 archive 必须设置 `threadingMode(ArchiveThreadingMode.INVOKER)`，否则 conclude 时会抛 ConfigurationException |

### 2.4 关键参数

- **mediaDriverAgentInvoker**：使 Archive 与 MediaDriver 共享同一 Agent 调用链，实现轻量嵌入模式；此时 `archiveCtx` 需 `threadingMode(INVOKER)`。
- **aeronDirectoryName**：Archive 的 Aeron 客户端必须使用与 MediaDriver 相同的 CnC 目录，才能通过 IPC 正常通信。
- **errorHandler / errorCounter**：统一错误处理与计数，便于监控和排查。

### 2.5 关闭顺序

`ArchivingMediaDriver.close()` 按 `archive → driver` 顺序调用 `CloseHelper.closeAll`，先关闭 Archive，再关闭 MediaDriver。

---

## 3. AeronArchive.connect

### 3.1 作用

`AeronArchive.connect(ctx)` 建立与 Archive 的控制会话（Control Session），返回 `AeronArchive` 客户端实例。之后可调用 `startRecording`、`stopRecording`、`replay` 等 API。

### 3.2 连接流程

```
connect(ctx)
    → asyncConnect(ctx)  // 创建 AsyncConnect，订阅 controlResponseChannel
    → 循环 poll() 直到返回 AeronArchive
        → checkDeadline()      // 超时检查
        → ctx.runInvokers()    // 驱动 Aeron Conductor 处理异步注册
        → 按 state 执行对应步骤
```

### 3.3 AsyncConnect 状态机

| 状态 | 说明 | 主要动作 |
|------|------|----------|
| AWAIT_SUBSCRIPTION | 等待 controlResponse Subscription 注册完成 | `getSubscription()` 获取；创建 `ControlResponsePoller` |
| ADD_PUBLICATION | 添加 controlRequest 的 ExclusivePublication | `asyncAddExclusivePublication()`；创建 `ArchiveProxy` |
| AWAIT_PUBLICATION_CONNECTED | 等待 Publication 连接 | `archiveProxy.publication().isConnected()` |
| SEND_CONNECT_REQUEST | 发送 connect 请求到 Archive | `tryResolveChannelEndpointPort()` 解析 responseChannel；`archiveProxy.tryConnect()` 发送 |
| AWAIT_SUBSCRIPTION_CONNECTED | 等待 response subscription 连接 | `controlResponsePoller.subscription().isConnected()` |
| AWAIT_CONNECT_RESPONSE | 等待 Archive 返回 connect 响应 | `controlResponsePoller.poll()`；解析 `controlSessionId` |
| SEND_ARCHIVE_ID_REQUEST | 可选：请求 archive-id | `archiveProxy.archiveId()` |
| AWAIT_ARCHIVE_ID_RESPONSE | 可选：等待 archive-id 响应 | 同 pollForResponse |
| SEND_CHALLENGE_RESPONSE | 若被挑战：发送认证响应 | `archiveProxy.tryChallengeResponse()` |
| AWAIT_CHALLENGE_RESPONSE | 等待 challenge 响应 | 同 pollForResponse |
| DONE | 连接完成 | `transitionToDone(archiveId)`：发送 keepAlive，构造 `AeronArchive` 并返回 |

### 3.4 主要子函数

| 函数 | 说明 |
|------|------|
| `asyncConnect(ctx)` | 调用 `ctx.conclude()`；`aeron.asyncAddSubscription(controlResponseChannel, controlResponseStreamId)`；初始状态 `AWAIT_SUBSCRIPTION` |
| `poll()` | 每轮执行 `checkDeadline()`、`ctx.runInvokers()`，再按当前 state 执行对应步骤；state 为 DONE 时返回 `aeronArchive` |
| `awaitSubscription()` | 获取已注册的 Subscription，创建 `ControlResponsePoller`，转入 `ADD_PUBLICATION` |
| `addPublication()` | `asyncAddExclusivePublication(controlRequestChannel)`；拿到 Publication 后创建 `ArchiveProxy`，转入 `AWAIT_PUBLICATION_CONNECTED` |
| `sendConnectRequest()` | 解析 responseChannel 端口，`archiveProxy.tryConnect(responseChannel, streamId, correlationId)`，转入 `AWAIT_SUBSCRIPTION_CONNECTED` |
| `pollForResponse()` | `controlResponsePoller.poll()`；收到 OK 后解析 `controlSessionId`；若需 archive-id 则 `SEND_ARCHIVE_ID_REQUEST`；若被 challenge 则 `SEND_CHALLENGE_RESPONSE`；否则 `transitionToDone()` |
| `transitionToDone(archiveId)` | `archiveProxy.keepAlive()` 发送保活；构造 `new AeronArchive(ctx, controlResponsePoller, archiveProxy, controlSessionId, archiveId)`；state 置为 DONE |
| `checkDeadline()` | 检查 `deadlineNs` 是否超时；超时抛出 `TimeoutException` |

### 3.5 通道与协议

- **controlRequestChannel**：客户端向 Archive 发送请求的通道（如 `aeron:udp?endpoint=localhost:8010`）。
- **controlResponseChannel**：Archive 向客户端发送响应的通道（通常为 MDC 控制模式，自动解析端口）。
- **connect 协议**：客户端发送 connect 请求，Archive 返回 `controlSessionId`；后续所有请求都带上该 sessionId。

### 3.6 与 ArchiveSender 的配合

在 ArchiveSender 中：

1. `aeronCtx.aeronDirectoryName(archivingDriver.mediaDriver().aeronDirectoryName())`：确保 Aeron 客户端连到 ArchivingMediaDriver 使用的 MediaDriver。
2. `AeronArchive.connect(new AeronArchive.Context().aeron(aeron))`：使用该 Aeron 实例，`controlRequestChannel`/`controlResponseChannel` 使用 Context 默认值（连接 localhost:8010）。

---

## 4. 整体数据流

```
ArchiveSender
    │
    ├── ArchivingMediaDriver.launch(driverCtx, archiveCtx)
    │       ├── MediaDriver：CnC、IPC、UDP 收发
    │       └── Archive：监听 controlChannel (localhost:8010)，管理录制/回放
    │
    ├── Aeron.connect(aeronCtx)  // aeronDirectoryName 与 driver 一致
    │
    └── AeronArchive.connect(ctx.aeron(aeron))
            │
            ├── 订阅 controlResponseChannel（收 Archive 响应）
            ├── 添加 controlRequest Publication（发请求到 localhost:8010）
            ├── 发送 connect → 收到 controlSessionId
            └── startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL)
                    → Archive 以 spy 方式订阅本地 Publication，录制到磁盘
```

---

## 5. 相关类与依赖

| 类 | 职责 |
|----|------|
| `ArchivingMediaDriver` | MediaDriver + Archive 聚合，提供 `launch` 与 `close` |
| `MediaDriver` | 底层 CnC、Conductor、Receiver、Sender |
| `Archive` | 录制、回放、目录、控制会话管理 |
| `AeronArchive` | Archive 客户端，封装 connect、startRecording、replay 等 |
| `ArchiveProxy` | 发送 control 协议消息（connect、startRecording 等） |
| `ControlResponsePoller` | 接收并解析 Archive 的 control 响应 |
| `AsyncConnect` | 异步建立连接的状态机实现 |

---

## 6. 小结

1. **ArchivingMediaDriver.launch**：先启动 MediaDriver，再启动 Archive，并让 Archive 共享 driver 的 AgentInvoker、aeron 目录、错误处理，实现同进程嵌入式部署。
2. **AeronArchive.connect**：通过 `AsyncConnect` 状态机，先订阅 controlResponse、再添加 controlRequest Publication、发送 connect 请求、等待 `controlSessionId`，最终返回 `AeronArchive` 实例，可执行录制与回放操作。
3. 使用 ArchivingMediaDriver 时，应用需保证 `Aeron.Context.aeronDirectoryName` 与 `archivingDriver.mediaDriver().aeronDirectoryName()` 一致；否则 Aeron 无法连到本进程的 MediaDriver。

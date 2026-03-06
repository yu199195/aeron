# MediaDriver.launchEmbedded(ctx) 流程图

本文档描述 `MediaDriver.launchEmbedded(Context ctx)` 的完整执行流程，从入口到目录准备、CnC 初始化、三大 Agent 创建与线程启动。  
**所有图示均为纯文本/表格，任意 Markdown 预览均可直接显示。**

---

## 1. 总体流程图（ASCII，可直接预览）

```
                    ┌─────────────────────────────┐
                    │   launchEmbedded(ctx)        │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │ ctx.aeronDirectoryName()     │
                    │ 是否为默认值 "aeron" ?       │
                    └──────────────┬───────────────┘
                          │                │
                          │ 是             │ 否
                          ▼                │
            ┌──────────────────────────┐   │
            │ ctx.aeronDirectoryName(  │   │
            │   generateRandomDirName() │   │
            │ )  → "aeron-" + UUID      │   │
            └──────────────┬────────────┘   │
                           │               │
                           └───────┬───────┘
                                   ▼
                    ┌─────────────────────────────┐
                    │      launch(ctx)            │
                    └──────────────┬──────────────┘
                                   │
                                   ▼
                    ┌─────────────────────────────┐
                    │  new MediaDriver(ctx)       │
                    └──────────────┬──────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         ▼                         ▼                         ▼
┌─────────────────┐    ┌─────────────────────┐    ┌─────────────────────┐
│ concludeAeron   │    │ ensureDirectoryIs    │    │ validateSocket       │
│ Directory()     │───▶│ Recreated(ctx)      │───▶│ BufferLengths(ctx)  │
└─────────────────┘    └─────────────────────┘    └──────────┬──────────┘
                                                               │
                                                               ▼
                                                    ┌─────────────────────┐
                                                    │   ctx.conclude()    │
                                                    │ (CnC/Counters/      │
                                                    │  Proxies/ready)     │
                                                    └──────────┬──────────┘
                                                               │
                                                               ▼
                                                    ┌─────────────────────┐
                                                    │ new DriverConductor │
                                                    │ new Receiver        │
                                                    │ new Sender          │
                                                    │ 注入 Proxy          │
                                                    └──────────┬──────────┘
                                                               │
                                                               ▼
                                                    ┌─────────────────────┐
                                                    │ switch(threadingMode)│
                                                    │ 创建 Runner/Invoker  │
                                                    └──────────┬──────────┘
                                                               │
    ┌──────────────────────────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ [可选] Windows 高精度定时器   │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐     ┌─────────────────────────────┐
│ startOnThread(conductorRunner)│ ... │ startOnThread(receiverRunner)│
│ startOnThread(senderRunner)  │     │ 或 sharedInvoker.start()     │
└──────────────┬───────────────┘     └──────────────┬───────────────┘
               │                                   │
               └─────────────────┬─────────────────┘
                                 ▼
                    ┌─────────────────────────────┐
                    │   return mediaDriver        │
                    └─────────────────────────────┘
```

### ensureDirectoryIsRecreated 分支（ASCII）

```
ensureDirectoryIsRecreated(ctx)
               │
               ▼
    ┌──────────────────────┐
    │ aeronDirectory 已存在? │
    └──────────┬────────────┘
         │            │
         │ 是         │ 否
         ▼            │
  warnIfDirectoryExists 打印 WARNING
         │            │
         ▼            │
  ┌──────────────┐    │
  │dirDeleteOnStart?│   │
  └──────┬───────┘    │
     │       │        │
     │ 否    │ 是     │
     ▼       │        │
 mapExistingCncFile   │
 isDriverActive? 抛异常
 reportExistingErrors │
     │       │        │
     └───┬───┘        │
         ▼            │
  ctx.deleteDirectory │
         │            │
         └─────┬──────┘
               ▼
    IoUtil.ensureDirectoryExists(aeronDirectory, "aeron")
```

---

## 2. 逐层调用关系（文字树）

```
launchEmbedded(ctx)
├── 若 ctx.aeronDirectoryName() == "aeron"
│   └── ctx.aeronDirectoryName(CommonContext.generateRandomDirName())
│       └── "aeron-" + UUID.randomUUID()
└── launch(ctx)
    ├── new MediaDriver(ctx)
    │   ├── ctx.concludeAeronDirectory()
    │   │   └── aeronDirectory = new File(aeronDirectoryName).getCanonicalFile()
    │   ├── ensureDirectoryIsRecreated(ctx)
    │   │   ├── 若 ctx.aeronDirectory().isDirectory()
    │   │   │   ├── warnIfDirectoryExists 则 System.err.println
    │   │   │   ├── 若 !ctx.dirDeleteOnStart()
    │   │   │   │   ├── ctx.mapExistingCncFile(logger)
    │   │   │   │   ├── CommonContext.isDriverActive(...) → 活跃则抛 ActiveDriverException
    │   │   │   │   └── reportExistingErrors(ctx, cncByteBuffer)
    │   │   │   └── ctx.deleteDirectory()
    │   │   └── IoUtil.ensureDirectoryExists(ctx.aeronDirectory(), "aeron")
    │   ├── validateSocketBufferLengths(ctx)
    │   │   └── 校验 SO_SNDBUF/SO_RCVBUF、MTU、initialWindowLength 等
    │   └── try
    │       ├── ctx.conclude()
    │       │   ├── super.conclude()、concludeNullProperties、resolveOsSocketBufLengths
    │       │   ├── 各类 validateValueRange / validateMtuLength / validatePageSize 等
    │       │   ├── 计算 cncFileLength，mapNewFile(cncFile(), cncFileLength)
    │       │   ├── CncFileDescriptor.createMetaDataBuffer / fillMetaData
    │       │   ├── concludeCounters / concludeDependantProperties / concludeIdleStrategies
    │       │   ├── toDriverCommands.nextCorrelationId / consumerHeartbeatTime
    │       │   └── CncFileDescriptor.signalCncReady、cncByteBuffer.force()
    │       ├── this.ctx = ctx
    │       ├── new DriverConductor(ctx)、new Receiver(ctx)、new Sender(ctx)
    │       ├── ctx.receiverProxy().receiver(receiver) 等注入
    │       └── switch (ctx.threadingMode()) 创建 conductorRunner/senderRunner/receiverRunner 或 sharedRunner/sharedInvoker
    ├── [可选] Windows 下 HighResolutionTimer.enable()
    ├── 若 conductorRunner != null → AgentRunner.startOnThread(conductorRunner, ...)
    ├── 若 senderRunner != null   → AgentRunner.startOnThread(senderRunner, ...)
    ├── 若 receiverRunner != null → AgentRunner.startOnThread(receiverRunner, ...)
    ├── 若 sharedNetworkRunner != null → AgentRunner.startOnThread(sharedNetworkRunner, ...)
    ├── 若 sharedRunner != null  → AgentRunner.startOnThread(sharedRunner, ...)
    └── 若 sharedInvoker != null → sharedInvoker.start()
    return mediaDriver
```

---

## 3. 时序步骤（纯文本，可直接预览）

| 步骤 | 谁执行 | 做什么 |
|------|--------|--------|
| 1 | 应用 | 调用 `launchEmbedded(ctx)` |
| 2 | launchEmbedded | 若目录名为默认，则 `ctx.aeronDirectoryName(generateRandomDirName())` |
| 3 | launchEmbedded | 调用 `launch(ctx)` |
| 4 | launch | `new MediaDriver(ctx)` |
| 5 | 构造函数 | `ctx.concludeAeronDirectory()`：解析目录为 File |
| 6 | 构造函数 | `ensureDirectoryIsRecreated(ctx)`：目录存在则检查/删除，再确保存在 |
| 7 | 构造函数 | `validateSocketBufferLengths(ctx)`：校验 Socket/MTU |
| 8 | 构造函数 | `ctx.conclude()`：CnC 文件、Counters、Proxies、signalCncReady |
| 9 | 构造函数 | `new DriverConductor, Receiver, Sender` 并注入 Proxy |
| 10 | 构造函数 | `switch(threadingMode)` 创建 Runner/Invoker |
| 11 | launch | 可选：Windows 下 `HighResolutionTimer.enable()` |
| 12 | launch | `startOnThread(conductorRunner)` 等或 `sharedInvoker.start()` |
| 13 | launch | `return mediaDriver` |

---

## 4. 关键方法说明

| 方法 | 作用 |
|------|------|
| `launchEmbedded(ctx)` | 入口；目录名为默认则改为随机名，再调用 `launch(ctx)`。 |
| `generateRandomDirName()` | 返回 `"aeron-" + UUID`，避免多实例冲突。 |
| `concludeAeronDirectory()` | 将 `aeronDirectoryName` 解析为规范路径 `File`，赋给 `aeronDirectory`。 |
| `ensureDirectoryIsRecreated(ctx)` | 目录已存在则按配置检查旧 Driver、报错并删除目录；最后保证目录存在。 |
| `validateSocketBufferLengths(ctx)` | 校验系统/配置的 Socket 缓冲区与 MTU、initialWindowLength 是否满足要求。 |
| `ctx.conclude()` | 校验参数、创建并映射 CnC 文件、初始化 Counters/Proxies、标记 CnC ready。 |
| `new DriverConductor/Receiver/Sender` | 创建三大 Agent；Conductor 处理命令与资源，Receiver 收包写 log，Sender 从 log 发包。 |
| `AgentRunner.startOnThread(runner, factory)` | 用 `factory` 创建新线程，在该线程中循环执行 `runner.run()`（内部调用 agent.doWork()）。 |

---

## 5. 线程模式与启动对象

| threadingMode | 启动的 Runner/Invoker |
|---------------|------------------------|
| **DEDICATED**（默认） | conductorRunner、senderRunner、receiverRunner 各占一线程。 |
| **SHARED** | 仅 sharedRunner（NamedCompositeAgent 包含 sender + receiver + conductor）。 |
| **SHARED_NETWORK** | sharedNetworkRunner（sender + receiver）+ conductorRunner。 |
| **INVOKER** | 仅 sharedInvoker.start()，无新线程；由外部调用 invoker.invoke() 驱动。 |

---

## 6. CnC 文件源码解析（cnc.dat）

> 源码位置：`aeron-client/src/main/java/io/aeron/CncFileDescriptor.java`

### 6.1 CnC 是什么

**CnC = Command and Control**（命令与控制），物理文件名为 aeron 目录下的 **`cnc.dat`**。

它是一个**内存映射文件（mmap）**，是 **Media Driver 和 Aeron Client 之间通信的唯一桥梁**。Driver 和 Client 各自 mmap 同一个文件，通过共享内存实现零拷贝的命令传递与事件通知——不走网络、不走 socket、不走 pipe。

- **Client 想告诉 Driver "帮我创建一个 Publication"** → 写入 `to-driver buffer`
- **Driver 想告诉 Client "Publication 创建好了"** → 写入 `to-clients buffer`
- **Driver 的各种运行时计数器（position、心跳等）** → `Counters` 区域
- **Driver 记录的错误日志** → `Error Log` 区域

### 6.2 CnC 文件物理布局（6 段连续拼接）

```
偏移量                    区段内容                     典型大小         用途
┌──────────────────────────────────────────────────────────────────────────────────────┐
│ 0                       Meta Data（元数据头）          128 字节        文件自身的描述信息           │
│                         (2 × CACHE_LINE = 128B)       (固定)                                   │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ 128                     to-driver Buffer              1 MB (默认)     Client → Driver 的命令      │
│                         (ManyToOneRingBuffer)                         环形缓冲区                  │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ 128 + 1MB               to-clients Buffer             1 MB (默认)     Driver → Client 的事件      │
│                         (BroadcastTransmitter)                        广播缓冲区                  │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ ...                     Counters Metadata Buffer      变长            每个计数器的标签/类型等       │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ ...                     Counters Values Buffer        1 MB (默认)     每个计数器的实际值            │
│                                                                       (position, 心跳等)          │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ ...                     Error Log                     1 MB (默认)     Driver 运行时错误记录        │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

对应源码 `CncFileDescriptor.java` 第 33-47 行的注释：

```java
/*
 *  +-----------------------------+
 *  |          Meta Data          |   ← 128 字节固定头
 *  +-----------------------------+
 *  |      to-driver Buffer       |   ← Client → Driver 命令环形缓冲
 *  +-----------------------------+
 *  |      to-clients Buffer      |   ← Driver → Client 广播缓冲
 *  +-----------------------------+
 *  |   Counters Metadata Buffer  |   ← 计数器标签/类型
 *  +-----------------------------+
 *  |    Counters Values Buffer   |   ← 计数器值（position/心跳等）
 *  +-----------------------------+
 *  |          Error Log          |   ← 错误日志
 *  +-----------------------------+
 */
```

### 6.3 Meta Data 头部结构（128 字节，前 52 字节有意义）

```
偏移      大小      字段                          说明
─────────────────────────────────────────────────────────────────────
 0        4B       CnC Version                   语义版本号(0.2.0)；
                                                  最后写入（volatile），写入后表示
                                                  "文件就绪，Client 可连接"
 4        4B       to-driver buffer length        Client→Driver 环形缓冲区长度
 8        4B       to-clients buffer length       Driver→Client 广播缓冲区长度
12        4B       Counters Metadata length       计数器元数据区长度
16        4B       Counters Values length         计数器值区长度
20        4B       Error Log length               错误日志区长度
24        8B       Client Liveness Timeout (ns)   客户端存活超时
32        8B       Driver Start Timestamp (ms)    Driver 启动时间戳（epoch）
40        8B       Driver PID                     Driver 进程 PID
48        4B       File page size                 文件页对齐大小
52-127    --       (padding 到 128B cache line 对齐)
```

对应源码中各 offset 常量的定义：

```java
CNC_VERSION_FIELD_OFFSET                      = 0;         // int
TO_DRIVER_BUFFER_LENGTH_FIELD_OFFSET          = 4;         // int
TO_CLIENTS_BUFFER_LENGTH_FIELD_OFFSET         = 8;         // int
COUNTERS_METADATA_BUFFER_LENGTH_FIELD_OFFSET  = 12;        // int
COUNTERS_VALUES_BUFFER_LENGTH_FIELD_OFFSET    = 16;        // int
ERROR_LOG_BUFFER_LENGTH_FIELD_OFFSET          = 20;        // int
CLIENT_LIVENESS_TIMEOUT_FIELD_OFFSET          = 24;        // long
START_TIMESTAMP_FIELD_OFFSET                  = 32;        // long
PID_FIELD_OFFSET                              = 40;        // long
FILE_PAGE_SIZE_FIELD_OFFSET                   = 48;        // int

META_DATA_LENGTH = CACHE_LINE_LENGTH * 2 = 128;            // 两条 cache line
```

**关键设计点：**
- `CnC Version` 字段通过 `putIntVolatile` **最后写入**（`signalCncReady()` 方法），利用 volatile 的 store-store barrier 保证 Client 读到 version 时，前面所有字段已经对其可见。
- Client 连接时会轮询 version 字段，直到看到合法版本号才继续映射其它区段（见 `Aeron.Context.connectToDriver()`）。

### 6.4 六个区段各自的作用

| 区段 | 底层数据结构 | 方向 | 作用 |
|------|-------------|------|------|
| **to-driver Buffer** | `ManyToOneRingBuffer` | Client → Driver | 多个 Client 并发写命令（ADD_PUBLICATION、ADD_SUBSCRIPTION、REMOVE 等），Driver 的 Conductor 消费 |
| **to-clients Buffer** | `BroadcastTransmitter` / `CopyBroadcastReceiver` | Driver → Client | Driver 广播响应/事件（PUBLICATION_READY、SUBSCRIPTION_READY、AVAILABLE_IMAGE、ERROR 等），所有 Client 都能收到 |
| **Counters Metadata** | `CountersManager` 元数据 | 双向读 | 每个计数器的标签（label）、类型、key 等描述信息 |
| **Counters Values** | `CountersManager` 值区 | Driver 写，Client 读 | 每个计数器的 `long` 值（如 publisher position、subscriber position、positionLimit、心跳时间戳等） |
| **Error Log** | `DistinctErrorLog` | Driver 写，工具/Client 读 | Driver 运行期间遇到的异常去重记录 |

### 6.5 CnC 文件创建时机

在 `ctx.conclude()` 中（`MediaDriver.Context.conclude()`），对应流程图第 8 步：

```
1. 计算 cncFileLength = 对齐(META_DATA + conductor + toClients + countersMeta + countersValues + errorLog)
2. cncByteBuffer = mapNewFile(cncFile(), cncFileLength)     ← 创建并 mmap 文件
3. cncMetaDataBuffer = createMetaDataBuffer(cncByteBuffer)  ← 取前 128 字节
4. fillMetaData(...)                                         ← 写入各段长度、timeout、PID 等
5. concludeCounters / concludeDependantProperties / concludeIdleStrategies
6. toDriverCommands.consumerHeartbeatTime(epochClock.time()) ← 初始化心跳
7. signalCncReady(cncMetaDataBuffer)                         ← 最后 volatile 写入 version
8. cncByteBuffer.force()                                     ← 刷盘确保持久化
```

### 6.6 Driver 与 Client 的交互示意

```
┌────────────────────────────────────────────────────────────────────┐
│                    cnc.dat  (内存映射文件)                          │
│                                                                    │
│   ┌──────────┐     ┌──────────────┐     ┌───────────────────────┐  │
│   │ MetaData │     │ to-driver    │     │    to-clients         │  │
│   │ (头部)   │     │ RingBuffer   │     │    BroadcastBuffer    │  │
│   └──────────┘     └──────┬───────┘     └───────────┬───────────┘  │
│                           │                         │              │
│                  Client 写命令              Driver 写响应/事件      │
│                           │                         │              │
│   ┌──────────────────┐    │    ┌──────────────────┐  │             │
│   │ Counters Meta    │    │    │ Counters Values  │  │             │
│   └──────────────────┘    │    └──────────────────┘  │             │
│   ┌──────────────────┐    │                          │             │
│   │    Error Log     │    │                          │             │
│   └──────────────────┘    │                          │             │
└───────────────────────────┼──────────────────────────┼─────────────┘
                            │                          │
              ┌─────────────┘                          └──────────────┐
              ▼                                                       ▼
    ┌───────────────────┐                                  ┌──────────────────┐
    │   Media Driver    │                                  │   Aeron Client   │
    │                   │                                  │                  │
    │  Conductor 消费    │◄──── Counters 写值 ────────────▶│  poll Counters   │
    │  to-driver 命令    │                                  │  读 to-clients   │
    │  写 to-clients 响应 │                                  │  写 to-driver 命令│
    └───────────────────┘                                  └──────────────────┘
```

### 6.7 为什么用内存映射文件而不是 Socket/Pipe

| 优势 | 说明 |
|------|------|
| **零拷贝** | Driver 和 Client 在同一台机器上，mmap 后两端直接读写同一块物理内存 |
| **无系统调用开销** | 写入/读取是普通内存操作，不需要 `send()`/`recv()` |
| **持久化可观测** | 文件在磁盘上，可用 `AeronStat` 等工具随时查看 Counters 状态 |
| **崩溃检测** | Client 重启后可 mmap 旧文件，通过心跳时间戳判断 Driver 是否还活着 |

### 6.8 一句话总结

> **`cnc.dat` 是 Aeron Driver 与 Client 之间的"共享内存通信协议文件"**：头部 128 字节描述各段长度与 Driver 元信息，后续紧跟 5 段连续 buffer，分别承载命令、响应、计数器、错误日志，全部通过 mmap 实现零拷贝的进程间通信。

---

## 7. 三大核心 Agent 源码解析

`MediaDriver` 的构造函数中创建了三大核心 Agent：

```java
final DriverConductor conductor = new DriverConductor(ctx);
final Receiver receiver = new Receiver(ctx);
final Sender sender = new Sender(ctx);
```

它们各司其职，共同构成 Aeron Media Driver 的数据平面和控制平面。

### 7.1 三大 Agent 总览

| Agent | 角色 | 核心职责 | 源码位置 |
|-------|------|---------|----------|
| **DriverConductor** | "大脑" / 控制平面 | 消费 Client 命令、管理所有资源生命周期、协调 Receiver 和 Sender | `aeron-driver/.../DriverConductor.java` |
| **Receiver** | "接收引擎" / 数据平面入口 | 从 UDP Socket 接收数据帧、重建 PublicationImage、发送流控反馈 | `aeron-driver/.../Receiver.java` |
| **Sender** | "发送引擎" / 数据平面出口 | 从 LogBuffer 读取数据帧、通过 UDP Socket 发送到网络、接收控制帧 | `aeron-driver/.../Sender.java` |

三者之间的通信关系：

```
                    ┌──────────────────────────────────────────────┐
                    │              DriverConductor                  │
                    │  (消费 Client 命令，管理资源生命周期)           │
                    └──────┬──────────────────────┬────────────────┘
                           │                      │
              ReceiverProxy│命令队列      SenderProxy│命令队列
              (addSubscription,           (newPublication,
               newImage 等)                removePublication 等)
                           │                      │
                           ▼                      ▼
              ┌────────────────────┐   ┌────────────────────┐
              │     Receiver       │   │      Sender        │
              │  (UDP 收包,         │   │  (LogBuffer → UDP   │
              │   写入 LogBuffer,   │   │   发送,             │
              │   发送 SM/NAK)      │   │   接收 SM/NAK)      │
              └────────────────────┘   └────────────────────┘
                        │                        ▲
                        │     网络（UDP）          │
                        └────────────────────────┘
                           SM / NAK / RTT
```

---

### 7.2 DriverConductor 源码解析

> 源码位置：`aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java`

#### 7.2.1 定位与职责

DriverConductor 实现了 `Agent` 接口，是 Media Driver 的"大脑"：

- **消费客户端命令**：从 CnC `to-driver` RingBuffer 中读取 ADD_PUBLICATION / ADD_SUBSCRIPTION / REMOVE / KEEPALIVE 等命令
- **管理资源生命周期**：创建/销毁 Publication、Subscription、Image、ChannelEndpoint
- **协调其他 Agent**：通过 `ReceiverProxy` 和 `SenderProxy`（线程安全命令队列）向 Receiver / Sender 下发指令
- **通知 Client**：通过 `ClientProxy` 将 PUBLICATION_READY / SUBSCRIPTION_READY / AVAILABLE_IMAGE 等事件写入 CnC `to-clients` BroadcastBuffer
- **定时任务**：心跳检查、客户端存活检测、Position 追踪、资源回收

#### 7.2.2 关键字段

```
字段                              类型                        作用
──────────────────────────────────────────────────────────────────────────────
toDriverCommands                  RingBuffer                  CnC to-driver 环形缓冲区
clientCommandAdapter              ClientCommandAdapter        命令适配器：读命令并分发
clientProxy                       ClientProxy                 向 Client 回写事件
receiverProxy                     ReceiverProxy               向 Receiver 发指令
senderProxy                       SenderProxy                 向 Sender 发指令
driverCmdQueue                    ManyToOneConcurrentLinkedQueue  Receiver/Sender → Conductor
networkPublications               ArrayList<NetworkPublication>    所有网络 Publication
ipcPublications                   ArrayList<IpcPublication>        所有 IPC Publication
publicationImages                 ArrayList<PublicationImage>      所有接收到的 Image
subscriptionLinks                 ArrayList<SubscriptionLink>      所有 Subscription 关联
clients                           ArrayList<AeronClient>           已注册的 Client 列表
countersManager                   CountersManager                  CnC Counters 管理器
nextSessionId                     int                              递增的 sessionId（随机起点）
```

#### 7.2.3 构造函数

```java
DriverConductor(final MediaDriver.Context ctx)
```

从 `ctx` 中提取所有依赖（各种 Proxy、RingBuffer、时钟、计数器等），创建 `ClientCommandAdapter`。此时 Agent 尚未启动，仅做字段初始化。

#### 7.2.4 doWork() —— 核心工作循环

```
每次调用执行 6 个步骤：

  ┌───────────────────────────────────────────────────┐
  │                    doWork()                        │
  ├───────────────────────────────────────────────────┤
  │ 1. processTimers(nowNs)                           │
  │    └─ 心跳检查、客户端存活检测、Publication/Image 清理 │
  │                                                     │
  │ 2. clientCommandAdapter.receive()                  │
  │    └─ 从 CnC to-driver RingBuffer 读取 Client 命令  │
  │       路由到 onAddNetworkPublication /              │
  │       onAddIpcSubscription / onClientKeepalive 等   │
  │    ※ 如有异步命令飞行中则跳过（防乱序）               │
  │                                                     │
  │ 3. drainCommandQueue()                             │
  │    └─ 处理 Receiver/Sender 发来的内部命令            │
  │                                                     │
  │ 4. trackStreamPositions(workCount, nowNs)          │
  │    └─ 追踪所有 Publication/Image 的 position         │
  │       更新流控计数器（publisherLimit 等）             │
  │                                                     │
  │ 5. nameResolver.doWork()                           │
  │    └─ 驱动名称解析器（DNS 刷新）                     │
  │                                                     │
  │ 6. freeEndOfLifeResources()                        │
  │    └─ 释放 linger 超时的已关闭资源                    │
  └───────────────────────────────────────────────────┘
```

#### 7.2.5 onAddNetworkPublication —— 创建网络 Publication

当 Client 调用 `aeron.addPublication("aeron:udp?endpoint=...", streamId)` 时触发。

```
Client 命令                    Conductor 处理
─────────                    ──────────────
ADD_PUBLICATION    ──→    1. 异步解析 channel URI（DNS）→ UdpChannel
                          2. 获取/创建 SendChannelEndpoint（复用 UDP Socket）
                          3. 非 exclusive: 复用已有 Publication
                          4. 新建: 分配 LogBuffer、sessionId、Counters
                          5. 通过 SenderProxy 通知 Sender
                          6. 回写 PUBLICATION_READY（含 LogBuffer 文件路径）
                          7. linkSpies：自动关联已有 Spy Subscription
```

#### 7.2.6 onAddIpcPublication —— 创建 IPC Publication

与网络 Publication 类似，但 **不需要 ChannelEndpoint 和 UDP Socket**。发送端和接收端通过 mmap 同一个 LogBuffer 文件实现零拷贝通信。

#### 7.2.7 onAddNetworkSubscription —— 创建网络 Subscription

当 Client 调用 `aeron.addSubscription("aeron:udp?endpoint=...", streamId)` 时触发。

```
Client 命令                      Conductor 处理
─────────                      ──────────────
ADD_SUBSCRIPTION    ──→    1. 异步解析 channel URI → UdpChannel
                            2. 获取/创建 ReceiveChannelEndpoint（复用 UDP Socket）
                            3. 通过 ReceiverProxy 通知 Receiver 注册 subscription
                            4. 回写 SUBSCRIPTION_READY
                            5. linkMatchingImages：关联已有的匹配 Image
```

#### 7.2.8 onAddIpcSubscription —— 创建 IPC Subscription

直接遍历 `ipcPublications`，找到匹配的 IPC Publication，立即通知 Client "Image 可用"。

#### 7.2.9 onClientKeepalive —— 心跳

Client 定期发送 KEEPALIVE 命令，Conductor 更新该 Client 的最后心跳时间。超过 `clientLivenessTimeoutNs` 未心跳则清理该 Client 的所有资源。

#### 7.2.10 onClose —— 关闭

```
1. 停止异步任务执行器
2. 关闭名称解析器
3. 关闭所有 ChannelEndpoint（释放 UDP Socket）
4. 释放所有 LogBuffer（mmap 内存）
5. 清除心跳，刷盘 CnC 文件
6. 关闭 Context
```

---

### 7.3 Receiver 源码解析

> 源码位置：`aeron-driver/src/main/java/io/aeron/driver/Receiver.java`

#### 7.3.1 定位与职责

Receiver 是 Media Driver 的"接收引擎"：

- **从网络收包**：通过 `DataTransportPoller`（封装 `Selector`）从注册的 UDP Socket 接收数据帧
- **帧分发**：收到的帧由 `DataPacketDispatcher` 根据 streamId/sessionId 路由到对应的 `PublicationImage`
- **流控反馈**：遍历 PublicationImage，发送 Status Message（SM）给 Sender，告知接收端 position
- **丢包处理**：检测到 gap 后发送 NAK（Negative Acknowledgment）请求 Sender 重传
- **RTT 测量**：发起 RTT 请求帧，用于 Congestion Control
- **清理断连 Image**：超时未收到数据的 Image 从数组中移除

#### 7.3.2 关键字段

```
字段                              类型                        作用
──────────────────────────────────────────────────────────────────────────────
dataTransportPoller               DataTransportPoller         数据传输轮询器（内含 Selector）
commandQueue                      OneToOneConcurrentArrayQueue  Conductor → Receiver 命令队列
publicationImages                 PublicationImage[]           当前活跃的 Image 数组
pendingSetupMessages              ArrayList                   待处理的 Setup 消息
conductorProxy                    DriverConductorProxy        Receiver → Conductor 通知代理
totalBytesReceived                AtomicCounter               累计接收字节数
```

#### 7.3.3 doWork() —— 核心工作循环

```
每次调用执行 5 个步骤：

  ┌───────────────────────────────────────────────────────┐
  │                     doWork()                           │
  ├───────────────────────────────────────────────────────┤
  │ 1. commandQueue.drain()                               │
  │    └─ 排空 Conductor 发来的命令                         │
  │       (addSubscription / removeSubscription /           │
  │        newPublicationImage / registerEndpoint 等)       │
  │                                                         │
  │ 2. dataTransportPoller.pollTransports()                │
  │    └─ 【核心】从所有注册的 UDP Socket 接收数据帧          │
  │       数据帧由 DataPacketDispatcher 路由到对应的 Image    │
  │       Image 将帧写入 LogBuffer 的 term 中                │
  │                                                         │
  │ 3. 遍历 publicationImages[]                            │
  │    ├─ 已连接的 Image:                                   │
  │    │  ├─ sendPendingStatusMessage → 发送 SM 给 Sender   │
  │    │  ├─ processPendingLoss → 发送 NAK 请求重传          │
  │    │  └─ initiateAnyRttMeasurements → RTT 测量          │
  │    └─ 已断连的 Image:                                   │
  │       └─ 从数组中移除，从 Dispatcher 注销，释放资源        │
  │                                                         │
  │ 4. checkPendingSetupMessages()                         │
  │    └─ Setup 消息超时处理（MDC 握手机制）                  │
  │                                                         │
  │ 5. DNS 重新解析检查（定期）                              │
  └───────────────────────────────────────────────────────┘
```

#### 7.3.4 关键处理方法

| 方法 | 触发方 | 作用 |
|------|--------|------|
| `onAddSubscription(endpoint, streamId)` | Conductor via ReceiverProxy | 在 Dispatcher 中注册 streamId 订阅 |
| `onAddSubscription(endpoint, streamId, sessionId)` | Conductor via ReceiverProxy | 注册带 session 过滤的订阅 |
| `onRemoveSubscription(...)` | Conductor via ReceiverProxy | 从 Dispatcher 中移除订阅 |
| `onNewPublicationImage(endpoint, image)` | Conductor via ReceiverProxy | 将新 Image 加入轮询数组 |
| `onRegisterReceiveChannelEndpoint(endpoint)` | Conductor via ReceiverProxy | 将 UDP Socket 注册到 Selector |
| `onCloseReceiveChannelEndpoint(endpoint)` | Conductor via ReceiverProxy | 从 Selector 注销，清理 pending setup |
| `onAddDestination(endpoint, transport)` | Conductor via ReceiverProxy | 多目的地订阅：注册额外的传输 |

#### 7.3.5 数据流路径

```
网络数据包 (UDP)
      │
      ▼
DataTransportPoller.pollTransports()
      │
      ▼
ReceiveChannelEndpoint.onDataPacket()
      │
      ▼
DataPacketDispatcher.onDataPacket(streamId, sessionId, ...)
      │
      ├─ 已知 Image → PublicationImage.insertPacket() → 写入 LogBuffer term
      │
      └─ 未知 session → 触发 onSetupMessage 创建新 Image
```

---

### 7.4 Sender 源码解析

> 源码位置：`aeron-driver/src/main/java/io/aeron/driver/Sender.java`

#### 7.4.1 定位与职责

Sender 是 Media Driver 的"发送引擎"：

- **从 LogBuffer 发送数据**：遍历所有 NetworkPublication，从 LogBuffer 的 term 中读取数据帧并通过 UDP Socket 发出
- **接收控制帧**：通过 `ControlTransportPoller` 接收来自 Receiver 的 SM（Status Message）和 NAK
  - SM 包含接收端 position → 更新 `senderLimit`（流控窗口）
  - NAK 触发数据重传
- **管理 SendChannelEndpoint**：注册/注销发送端的 UDP Socket
- **DNS 重新解析**：定期检查并更新目标地址

#### 7.4.2 关键设计：Cache Line Padding

```java
class SenderLhsPadding { byte p000...p063; }      // 64 字节左填充
class SenderHotFields extends SenderLhsPadding {    // 热点字段
    long controlPollDeadlineNs;
    long reResolutionDeadlineNs;
    int dutyCycleCounter;
    int roundRobinIndex;
}
class SenderRhsPadding extends SenderHotFields { byte p064...p127; }  // 64 字节右填充
public final class Sender extends SenderRhsPadding implements Agent { ... }
```

热点字段被前后各 64 字节的 padding 包裹，确保它们独占一个 cache line，避免 **false sharing**——这是高性能并发编程中避免 CPU 缓存行争用的经典手法。

#### 7.4.3 doWork() —— 核心工作循环

```
每次调用执行 4 个步骤：

  ┌───────────────────────────────────────────────────────┐
  │                     doWork()                           │
  ├───────────────────────────────────────────────────────┤
  │ 1. commandQueue.drain()                               │
  │    └─ 排空 Conductor 发来的命令                         │
  │       (newPublication / removePublication /             │
  │        registerEndpoint / closeEndpoint 等)             │
  │                                                         │
  │ 2. doSend(nowNs)                                       │
  │    └─ 【核心】Round-Robin 遍历 networkPublications       │
  │       每个 publication.send(nowNs) 内部:                 │
  │       ├─ 检查 senderPosition vs senderLimit (流控)      │
  │       ├─ 从 LogBuffer term 读取数据帧                    │
  │       ├─ 通过 DatagramChannel 发送到网络                  │
  │       └─ 更新 senderPosition                             │
  │                                                         │
  │ 3. 条件性轮询控制通道                                    │
  │    触发条件（任一满足即轮询）：                            │
  │    ├─ 本轮没有发送任何数据（空闲）                        │
  │    ├─ dutyCycleCounter 达到阈值                          │
  │    ├─ 距上次轮询超时                                     │
  │    └─ 检测到 short send                                  │
  │    controlTransportPoller.pollTransports()               │
  │    └─ 接收 SM (Status Message) → 更新 senderLimit        │
  │       接收 NAK → 标记帧需要重传                           │
  │       接收 RTT Reply → 计算 RTT                          │
  │                                                         │
  │ 4. DNS 重新解析检查（定期）                              │
  └───────────────────────────────────────────────────────┘
```

#### 7.4.4 doSend() —— Round-Robin 公平发送

```java
int startingIndex = roundRobinIndex++;
if (startingIndex >= length) { roundRobinIndex = startingIndex = 0; }

for (int i = startingIndex; i < length; i++)     // 从 startingIndex 到末尾
    bytesSent += publications[i].send(nowNs);
for (int i = 0; i < startingIndex; i++)           // 从 0 到 startingIndex（回绕）
    bytesSent += publications[i].send(nowNs);
```

每次 `doWork()` 调用时，`roundRobinIndex` 递增，保证遍历的起始位置不同，确保多个 Publication 间的公平性。

#### 7.4.5 关键处理方法

| 方法 | 触发方 | 作用 |
|------|--------|------|
| `onRegisterSendChannelEndpoint(endpoint)` | Conductor via SenderProxy | 将控制通道注册到 ControlTransportPoller |
| `onCloseSendChannelEndpoint(endpoint)` | Conductor via SenderProxy | 从 ControlTransportPoller 注销 |
| `onNewNetworkPublication(publication)` | Conductor via SenderProxy | 加入轮询数组，注册到 endpoint |
| `onRemoveNetworkPublication(publication)` | Conductor via SenderProxy | 从轮询数组移除，释放 Sender 端资源 |
| `onAddDestination(endpoint, uri, addr, id)` | Conductor via SenderProxy | MDC: 添加发送目的地 |
| `onRemoveDestination(...)` | Conductor via SenderProxy | MDC: 移除发送目的地 |

#### 7.4.6 数据流路径

```
Client 写消息
      │
      ▼
Publication.offer(buffer)
      │
      ▼
LogBuffer Term（mmap 共享内存）
      │
      ▼  Sender.doSend()
NetworkPublication.send(nowNs)
      │
      ├─ 检查 senderPosition < senderLimit (流控)
      ├─ 从 term 读取帧
      ▼
SendChannelEndpoint → DatagramChannel.send()
      │
      ▼
网络 (UDP)
```

---

### 7.5 三大 Agent 的线程模型

| 线程模式 | Conductor | Sender | Receiver | 说明 |
|----------|-----------|--------|----------|------|
| **DEDICATED** (默认) | 独占线程 | 独占线程 | 独占线程 | 性能最佳，适合高吞吐场景 |
| **SHARED_NETWORK** | 独占线程 | 共享线程 | 共享线程 | Sender+Receiver 共享一个线程 |
| **SHARED** | 共享线程 | 共享线程 | 共享线程 | 三者共享一个线程，节省资源 |
| **INVOKER** | 调用方驱动 | 调用方驱动 | 调用方驱动 | 无额外线程，由外部调用 invoke() |

所有 Agent 都实现了 Agrona 的 `Agent` 接口，核心方法是 `doWork()`，由 `AgentRunner` 循环调用。`AgentRunner` 内部使用 `IdleStrategy` 在无工作时降低 CPU 使用率。

---

### 7.6 三大 Agent 交互全景图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Aeron Client (用户进程)                          │
│                                                                         │
│  Publication.offer()  ──→  LogBuffer (mmap)                              │
│                                                                         │
│  Subscription.poll()  ←──  LogBuffer (mmap)                              │
│                                                                         │
│  CnC to-driver  ──→  命令 (ADD_PUB/ADD_SUB/KEEPALIVE)                   │
│  CnC to-clients ←──  事件 (PUB_READY/SUB_READY/AVAIL_IMAGE)             │
└───────────┬─────────────────────────────────────────────┬───────────────┘
            │  mmap                                       │  mmap
            ▼                                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Media Driver                                   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────┐       │
│  │                    DriverConductor                            │       │
│  │                                                              │       │
│  │  读 to-driver RingBuffer ───→ 处理命令                       │       │
│  │  写 to-clients Broadcast ───→ 通知 Client                   │       │
│  │  ReceiverProxy ──────────→ Receiver 命令队列                 │       │
│  │  SenderProxy ────────────→ Sender 命令队列                   │       │
│  │  管理: Publication / Subscription / Image / Endpoint / Client │       │
│  └──────────────────────────────────────────────────────────────┘       │
│         │ ReceiverProxy                    │ SenderProxy                │
│         ▼                                  ▼                            │
│  ┌────────────────────┐          ┌────────────────────┐                │
│  │     Receiver        │          │      Sender        │                │
│  │                    │          │                    │                │
│  │  UDP 收包           │  SM/NAK  │  LogBuffer → UDP   │                │
│  │  → 写入 LogBuffer   │ ←──────→ │  发送数据帧         │                │
│  │  发送 SM/NAK        │  (网络)  │  接收 SM/NAK       │                │
│  └────────────────────┘          └────────────────────┘                │
└─────────────────────────────────────────────────────────────────────────┘
```

---

以上流程图与调用关系对应源码中的 `launchEmbedded(ctx)` → `launch(ctx)` → `new MediaDriver(ctx)` 及后续线程启动过程；结合源码内中文注释可逐行对照阅读。  
**图示均为 ASCII 与表格，在任意 .md 预览中均可正常显示。**

# Aeron Agent 深度解析

> 基于源码 `aeron-agent` 模块整理，版本 1.44.x+

---

## 目录

1. [概述](#1-概述)
2. [核心原理：Java Agent + ByteBuddy](#2-核心原理java-agent--bytebuddy)
3. [整体架构](#3-整体架构)
4. [模块详解](#4-模块详解)
   - [4.1 入口层：EventLogAgent & DynamicLoggingAgent](#41-入口层eventlogagent--dynamicloggingagent)
   - [4.2 配置层：ConfigOption & EventConfiguration](#42-配置层configoption--eventconfiguration)
   - [4.3 组件层：ComponentLogger & EventCodeType](#43-组件层componentlogger--eventcodetype)
   - [4.4 插桩层：各 Interceptor 类](#44-插桩层各-interceptor-类)
   - [4.5 事件流：RingBuffer 管道](#45-事件流ringbuffer-管道)
   - [4.6 消费层：EventLogReaderAgent](#46-消费层eventlogreaderagent)
5. [事件类型全览](#5-事件类型全览)
   - [5.1 Driver 事件（60 个）](#51-driver-事件60-个)
   - [5.2 Archive 事件（45 个）](#52-archive-事件45-个)
   - [5.3 Cluster 事件（25 个）](#53-cluster-事件25-个)
6. [消息 ID 编码规则](#6-消息-id-编码规则)
7. [启动方式与配置参数](#7-启动方式与配置参数)
8. [ServiceLoader 扩展机制](#8-serviceloader-扩展机制)
9. [自定义扩展方式](#9-自定义扩展方式)
   - [9.1 自定义 EventLogReaderAgent（替换输出目标）](#91-自定义-eventlogreaderagent替换输出目标)
   - [9.2 自定义 ComponentLogger（新增监控组件）](#92-自定义-componentlogger新增监控组件)
   - [9.3 USER 事件类型（第三方事件）](#93-user-事件类型第三方事件)
10. [接入 Prometheus 完整方案](#10-接入-prometheus-完整方案)

---

## 1. 概述

`aeron-agent` 是一个 **Java Instrumentation Agent**，以零侵入方式接入运行中的 Aeron 进程，通过字节码注入拦截 Aeron 内部方法，将关键事件写入共享内存 Ring Buffer，再由独立的消费线程异步输出到文件或 stdout。

**核心特性：**

- **零侵入**：不修改任何 Aeron 源码，通过 JVM Instrumentation API 在运行时注入字节码
- **低开销**：Ring Buffer 无锁写入，消费线程异步处理，不阻塞业务线程
- **动态开关**：支持对运行中进程动态 attach/detach，无需重启
- **可扩展**：通过 `ServiceLoader` + `ComponentLogger` 接口支持第三方扩展
- **全面覆盖**：涵盖 Driver、Archive、Cluster 三大组件的 130+ 事件类型

---

## 2. 核心原理：Java Agent + ByteBuddy

### 2.1 Java Instrumentation API

JVM 规范允许通过 `-javaagent` 参数（静态）或 `Attach API`（动态）在类加载前后修改字节码：

```
JVM 启动
  │
  ├─ 静态接入：-javaagent:aeron-agent.jar
  │    └─ JVM 调用 EventLogAgent.premain(agentArgs, Instrumentation)
  │
  └─ 动态接入：ByteBuddyAgent.attach(agentJar, pid, agentArgs)
       └─ JVM 调用 EventLogAgent.agentmain(agentArgs, Instrumentation)
```

`Instrumentation` 接口是 JVM 提供的钩子，允许注册 `ClassFileTransformer`，在类加载时替换字节码。

### 2.2 ByteBuddy 字节码生成

[ByteBuddy](https://bytebuddy.net/) 是一个运行时字节码生成库，提供了类型安全的 API 来描述字节码修改逻辑。Aeron Agent 使用它的 `AgentBuilder` + `@Advice` 模式：

```java
// 来自 DriverComponentLogger.java
agentBuilder
    .type(nameEndsWith("ClientCommandAdapter"))        // 匹配目标类
    .transform((builder, ...) -> builder
        .visit(to(CmdInterceptor.class)                // 注入 Advice
            .on(named("onMessage"))))                  // 插桩到指定方法
```

`@Advice.OnMethodEnter` 在方法**入口**前注入代码，不影响原方法的返回值和异常，属于 **before-advice** 模式：

```java
// 来自 CmdInterceptor.java
@Advice.OnMethodEnter
static void logCmd(final int msgTypeId, final DirectBuffer buffer,
                   final int index, final int length)
{
    switch (msgTypeId)
    {
        case ADD_PUBLICATION:
            LOGGER.log(CMD_IN_ADD_PUBLICATION, buffer, index, length);
            break;
        // ...
    }
}
```

### 2.3 Retransformation 策略

Agent 使用 `RETRANSFORMATION` 策略，可以对**已加载的类**重新应用字节码变换，这使得动态 attach 成为可能：

```java
// 来自 EventLogAgent.java
AgentBuilder agentBuilder = new AgentBuilder.Default(...)
    .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
    .with(AgentBuilder.RedefinitionStrategy.DiscoveryStrategy.Reiterating.INSTANCE);
```

`Reiterating.INSTANCE` 表示重复扫描所有已加载类，确保不遗漏。

---

## 3. 整体架构

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           aeron-agent 架构                                │
│                                                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                        业务进程（Aeron JVM）                          │  │
│  │                                                                       │  │
│  │   MediaDriver          Archive             ConsensusModule           │  │
│  │   ├─ ClientCommandAdapter  ├─ ControlSessionAdapter  ├─ Election     │  │
│  │   ├─ UdpChannelTransport   ├─ RecordingSession       ├─ LogPublisher │  │
│  │   ├─ DriverConductor       └─ ReplaySession          └─ ...         │  │
│  │   └─ ...                                                             │  │
│  │         ↑ ByteBuddy 字节码注入（@Advice.OnMethodEnter）               │  │
│  │         │                                                             │  │
│  │   ┌─────┴──────────────────────────────────────────────────┐        │  │
│  │   │               Interceptor 层（业务线程执行）              │        │  │
│  │   │  CmdInterceptor / ChannelEndpointInterceptor /          │        │  │
│  │   │  ArchiveInterceptor / ClusterInterceptor / ...          │        │  │
│  │   │            │ LOGGER.log(eventCode, buffer, ...)         │        │  │
│  │   └────────────┼───────────────────────────────────────────┘        │  │
│  │                ▼ 无锁写入（tryClaim + commit）                        │  │
│  │   ┌────────────────────────────────────────────────────────┐        │  │
│  │   │   ManyToOneRingBuffer（默认 8MB，直接内存，缓存行对齐）   │        │  │
│  │   └────────────────────────────────────────────────────────┘        │  │
│  │                ▼ 独立 daemon 线程（event-log-reader）                 │  │
│  │   ┌────────────────────────────────────────────────────────┐        │  │
│  │   │              EventLogReaderAgent.doWork()               │        │  │
│  │   │   ringBuffer.read(handler, 20) → decode → output        │        │  │
│  │   └─────────────────────┬──────────────────────────────────┘        │  │
│  │                          │                                            │  │
│  │              ┌───────────┼───────────┐                               │  │
│  │              ▼           ▼           ▼                               │  │
│  │           stdout      文件         自定义（可替换）                    │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

### 数据流时序

```
业务线程                      Ring Buffer                  event-log-reader 线程
    │                              │                               │
    │ 1. 方法被调用                 │                               │
    │ 2. @Advice 注入的字节码执行   │                               │
    │──tryClaim(msgTypeId, len)───►│                               │
    │ 3. 写入 header + payload     │                               │
    │──commit(claimedIndex)───────►│                               │
    │ 4. 方法继续正常执行           │                               │
    │                              │◄──read(handler, 20)───────────│
    │                              │ 5. 批量读取最多 20 条          │
    │                              │──onMessage(id, buf, i, len)──►│
    │                              │ 6. decode → StringBuilder     │
    │                              │ 7. write to file/stdout       │
```

---

## 4. 模块详解

### 4.1 入口层：EventLogAgent & DynamicLoggingAgent

#### EventLogAgent.java

主入口，提供两个 JVM 标准入口方法：

| 方法 | 触发时机 | 用途 |
|------|----------|------|
| `premain(agentArgs, instrumentation)` | JVM 启动时，main 方法之前 | 静态接入 |
| `agentmain(agentArgs, instrumentation)` | 动态 attach 时 | 动态接入/停止 |

`startLogging()` 的核心流程：

```java
// 1. ServiceLoader 加载所有 ComponentLogger
for (ComponentLogger logger : ServiceLoader.load(ComponentLogger.class)) {
    loggers.add(logger);
}

// 2. 构造 ByteBuddy AgentBuilder（禁用类型校验，使用 RETRANSFORMATION）
AgentBuilder agentBuilder = new AgentBuilder.Default(new ByteBuddy()
    .with(TypeValidation.DISABLED))
    .disableClassFormatChanges()
    .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION);

// 3. 每个 ComponentLogger 注册自己的插桩规则
for (ComponentLogger logger : loggers) {
    agentBuilder = logger.addInstrumentation(agentBuilder, configOptions);
}

// 4. 安装到 JVM
logTransformer = agentBuilder.installOn(instrumentation);

// 5. 启动消费线程（daemon 线程，名称 "event-log-reader"）
readerAgentRunner = new AgentRunner(
    new SleepingMillisIdleStrategy(1ms), ...
    newReaderAgent(configOptions, loggers));
thread = new Thread(readerAgentRunner);
thread.setDaemon(true);
thread.start();
```

`stopLogging()` 的核心流程：

```java
// 还原所有被修改的字节码
logTransformer.reset(instrumentation, RETRANSFORMATION);
// 关闭 Ring Buffer（unblock 唤醒可能阻塞的消费者）
EVENT_RING_BUFFER.unblock();
// 关闭消费线程
CloseHelper.close(readerAgentRunner);
```

#### DynamicLoggingAgent.java

命令行工具，用于对**已运行进程**动态开关日志：

```bash
# 启动日志（向进程 PID 注入 agent）
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
    /path/to/aeron-agent.jar <PID> start [config.properties]

# 停止日志（发送 STOP 命令）
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
    /path/to/aeron-agent.jar <PID> stop
```

内部调用 `ByteBuddyAgent.attach(agentJar, pid, agentArgs)`，通过 JVM Attach API 连接目标进程。配置参数格式为 `key=value|key=value`（竖线分隔），由 `ConfigOption.buildAgentArgs()` 序列化，`parseAgentArgs()` 反序列化。

---

### 4.2 配置层：ConfigOption & EventConfiguration

#### ConfigOption.java — 所有系统属性名

| 属性名 | 含义 | 示例值 |
|--------|------|--------|
| `aeron.event.log` | 启用的 Driver 事件 | `all` / `admin` / `FRAME_IN,NAK_SENT` |
| `aeron.event.log.disable` | 禁用的 Driver 事件（从 enable 集合中移除） | `FRAME_IN,FRAME_OUT` |
| `aeron.event.archive.log` | 启用的 Archive 事件 | `all` / `CMD_IN_START_RECORDING` |
| `aeron.event.archive.log.disable` | 禁用的 Archive 事件 | |
| `aeron.event.cluster.log` | 启用的 Cluster 事件 | `all` / `ELECTION_STATE_CHANGE` |
| `aeron.event.cluster.log.disable` | 禁用的 Cluster 事件 | |
| `aeron.event.log.filename` | 输出文件路径（不设则输出到 stdout） | `/var/log/aeron-events.log` |
| `aeron.event.log.reader.classname` | 自定义 Reader Agent 类名 | `com.example.MyReaderAgent` |
| `aeron.event.buffer.length` | Ring Buffer 大小（字节） | `16777216`（16MB）|

Driver 事件特殊别名：
- `all`：启用所有 Driver 事件
- `admin`：启用所有 Driver 事件，**排除** `FRAME_IN` 和 `FRAME_OUT`（帧级别太频繁）

#### EventConfiguration.java — 共享 Ring Buffer

```java
// 静态初始化，整个 JVM 共享一个 Ring Buffer
public static final ManyToOneRingBuffer EVENT_RING_BUFFER;

static {
    EVENT_RING_BUFFER = new ManyToOneRingBuffer(
        new UnsafeBuffer(allocateDirectAligned(
            getSizeAsInt(BUFFER_LENGTH_PROP_NAME, BUFFER_LENGTH_DEFAULT) + TRAILER_LENGTH,
            CACHE_LINE_LENGTH)));   // 64 字节缓存行对齐，避免 false sharing
}
```

关键常量：
- `BUFFER_LENGTH_DEFAULT = 8 * 1024 * 1024`（8MB）
- `MAX_EVENT_LENGTH = 4096 - lineSeparator().length()`（单条事件最大长度）
- `EVENT_READER_FRAME_LIMIT = 20`（消费者每次最多读 20 条）

---

### 4.3 组件层：ComponentLogger & EventCodeType

#### EventCodeType 枚举 — 组件类型标识

```java
public enum EventCodeType {
    DRIVER   (0),      // 媒体驱动
    ARCHIVE  (1),      // 归档服务
    CLUSTER  (2),      // 集群共识
    STANDBY  (3),      // 集群备用节点
    SEQUENCER(4),      // 序列器（保留）
    USER     (0xFFFF); // 第三方自定义事件
}
```

#### ComponentLogger 接口 — 扩展点核心

```java
public interface ComponentLogger {

    // 组件类型码（对应 EventCodeType 中的 typeCode）
    int typeCode();

    // 解码 Ring Buffer 中的事件到可读字符串
    void decode(MutableDirectBuffer buffer, int offset,
                int eventCodeId, StringBuilder builder);

    // 向 ByteBuddy AgentBuilder 注册插桩规则，返回更新后的 builder
    AgentBuilder addInstrumentation(AgentBuilder agentBuilder,
                                    Map<String, String> configOptions);

    // 停止日志时重置状态（清空 ENABLED_EVENTS）
    void reset();

    // 版本信息，如 "version=1.44.1 commit=abc123"
    String version();
}
```

内置三个实现，通过 ServiceLoader 注册（`META-INF/services/io.aeron.agent.ComponentLogger`）：

```
io.aeron.agent.DriverComponentLogger
io.aeron.agent.ArchiveComponentLogger
io.aeron.agent.ClusterComponentLogger
```

---

### 4.4 插桩层：各 Interceptor 类

每个 Interceptor 类是纯静态的，使用 `@Advice.OnMethodEnter` 注解。ByteBuddy 会把这些方法的字节码**内联**（inline）到目标方法的入口处，而非通过反射调用，因此性能开销极低。

#### 插桩目标汇总

| Interceptor 类 | 插桩目标类 | 插桩方法 | 事件类型 |
|----------------|-----------|----------|----------|
| `CmdInterceptor` | `ClientCommandAdapter` | `onMessage` | 所有 CMD_IN_* 命令 |
| `CmdInterceptor` | `ClientProxy` | `transmit` | 所有 CMD_OUT_* 响应 |
| `ChannelEndpointInterceptor.UdpChannelTransport.SendHook` | `UdpChannelTransport` | `sendHook` | `FRAME_OUT` |
| `ChannelEndpointInterceptor.UdpChannelTransport.ReceiveHook` | `UdpChannelTransport` | `receiveHook` | `FRAME_IN` |
| `ChannelEndpointInterceptor.UdpChannelTransport.ResendHook` | `UdpChannelTransport` | `resendHook` | `RESEND` |
| `ChannelEndpointInterceptor.ReceiveChannelEndpointInterceptor.NakSent` | `ReceiveChannelEndpoint` | `sendNakMessage` | `NAK_SENT` |
| `ChannelEndpointInterceptor.SendChannelEndpointInterceptor.NakReceived` | `SendChannelEndpoint` | `onNakMessage` | `NAK_RECEIVED` |
| `ChannelEndpointInterceptor.SenderProxy.*` | `SenderProxy` | `registerSendChannelEndpoint` / `closeSendChannelEndpoint` | `SEND_CHANNEL_CREATION` / `SEND_CHANNEL_CLOSE` |
| `CleanupInterceptor.CleanupImage` | `DriverConductor` | `cleanupImage` | `REMOVE_IMAGE_CLEANUP` |
| `CleanupInterceptor.CleanupPublication` | `DriverConductor` | `cleanupPublication` | `REMOVE_PUBLICATION_CLEANUP` |
| `DriverInterceptor.UntetheredSubscriptionStateChange` | `UntetheredSubscription` | `logStateChange` | `UNTETHERED_SUBSCRIPTION_STATE_CHANGE` |
| `DriverInterceptor.NameResolution.Resolve` | `TimeTrackingNameResolver` | `logResolve` | `NAME_RESOLUTION_RESOLVE` |
| `DriverInterceptor.FlowControl.ReceiverAdded` | `AbstractMinMulticastFlowControl` | `receiverAdded` | `FLOW_CONTROL_RECEIVER_ADDED` |
| `DriverInterceptor.Revoke.PublicationRevoke` | `NetworkPublication` / `IpcPublication` | `logRevoke` | `PUBLICATION_REVOKE` |
| `ControlInterceptor.*` | `ControlSessionAdapter` | 各控制命令方法 | 所有 Archive CMD_IN_* |
| `ArchiveInterceptor.*` | `RecordingSession` / `ReplaySession` 等 | 状态变更方法 | Archive 状态变更事件 |
| `ClusterInterceptor.*` | `Election` / `ConsensusModule` 等 | 选举/状态变更方法 | 所有 Cluster 事件 |

---

### 4.5 事件流：RingBuffer 管道

写入端（业务线程，无锁）：

```
DriverEventLogger.log(eventCode, buffer, offset, length)
  │
  ├─ 1. 计算编码长度
  ├─ 2. ringBuffer.tryClaim(msgTypeId, encodedLength)
  │      返回 claimedIndex（失败返回负数，直接丢弃该事件）
  ├─ 3. DriverEventEncoder.encode(buffer, offset, length, ringBuffer, claimedIndex)
  │      将时间戳、线程ID、原始数据写入 Ring Buffer 的 claim 区域
  └─ 4. ringBuffer.commit(claimedIndex)
         发布消息，对消费者可见
```

消息 ID 的 32 位编码（写入时由 Logger 构造，读取时由 ReaderAgent 拆解）：

```
高 16 位：EventCodeType.typeCode（0=DRIVER, 1=ARCHIVE, 2=CLUSTER, 0xFFFF=USER）
低 16 位：具体 EventCode.id()
```

消费端（event-log-reader 线程）：

```
EventLogReaderAgent.doWork()
  │
  └─ ringBuffer.read(messageHandler, 20)  // 每次最多 20 条
       │
       └─ onMessage(msgTypeId, buffer, index, length)
            │
            ├─ eventCodeTypeId = msgTypeId >> 16
            ├─ eventCodeId = msgTypeId & 0xFFFF
            ├─ componentLogger = loggers.get(eventCodeTypeId)
            └─ componentLogger.decode(buffer, index, eventCodeId, builder)
                 │
                 └─ EventCode.decode() → DissectFunction.dissect()
                      └─ 格式化为可读字符串，含时间戳、线程名、事件详情
```

---

### 4.6 消费层：EventLogReaderAgent

默认消费者，实现 Agrona `Agent` 接口，运行在独立 daemon 线程中。

**输出目标选择逻辑：**

```java
if (filename != null) {
    // 输出到文件（FileChannel，append 模式）
    // 使用直接内存 ByteBuffer 批量写入，减少系统调用次数
    fileChannel = FileChannel.open(Paths.get(filename), CREATE, APPEND, WRITE);
    byteBuffer = allocateDirectAligned(MAX_EVENT_LENGTH * 2, CACHE_LINE_LENGTH);
} else {
    // 输出到 stdout（PrintStream）
    this.out = System.out;
}
```

**批量写入优化：**

写文件时，先把多条事件追加到 `ByteBuffer`，当 buffer 快满或 `doWork()` 结束时才调用 `FileChannel.write()`，减少系统调用次数。

**可替换性：**

通过 `aeron.event.log.reader.classname` 完全替换此类。`EventLogAgent` 支持三种构造器签名（按优先级尝试）：

```java
// 优先级 1：接收文件名 + ComponentLogger 列表
MyAgent(String filename, List<ComponentLogger> loggers)

// 优先级 2：只接收文件名
MyAgent(String filename)

// 优先级 3：无参构造
MyAgent()
```

---

## 5. 事件类型全览

### 5.1 Driver 事件（60 个）

类型码：`EventCodeType.DRIVER = 0`

#### 网络 I/O（帧级别）

| 枚举值 | ID | 含义 | 注意 |
|--------|----|------|------|
| `FRAME_IN` | 1 | 收到 UDP 帧（含完整帧内容） | 高频，`admin` 模式下禁用 |
| `FRAME_OUT` | 2 | 发出 UDP 帧 | 高频，`admin` 模式下禁用 |
| `NAK_SENT` | 54 | 接收端发送的 NAK（请求重传） | 丢包指标 |
| `NAK_RECEIVED` | 58 | 发送端收到的 NAK | |
| `RESEND` | 55 | 数据重发（响应 NAK） | 与 NAK 配合判断丢包 |

#### Publication 生命周期

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_ADD_PUBLICATION` | 3 | 客户端请求创建 Publication |
| `CMD_IN_ADD_EXCLUSIVE_PUBLICATION` | 32 | 客户端请求创建独占 Publication |
| `CMD_IN_REMOVE_PUBLICATION` | 4 | 客户端请求移除 Publication |
| `CMD_OUT_PUBLICATION_READY` | 7 | Driver 通知 Publication 就绪 |
| `CMD_OUT_EXCLUSIVE_PUBLICATION_READY` | 33 | Driver 通知独占 Publication 就绪 |
| `REMOVE_PUBLICATION_CLEANUP` | 14 | Driver 清理 Publication |
| `PUBLICATION_REVOKE` | 59 | Publication 被撤销 |
| `PUBLICATION_IMAGE_REVOKE` | 60 | Publication Image 被撤销 |

#### Subscription & Image 生命周期

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_ADD_SUBSCRIPTION` | 5 | 客户端请求创建 Subscription |
| `CMD_IN_REMOVE_SUBSCRIPTION` | 6 | 客户端请求移除 Subscription |
| `CMD_OUT_SUBSCRIPTION_READY` | 37 | Driver 通知 Subscription 就绪 |
| `CMD_OUT_AVAILABLE_IMAGE` | 8 | 新 Image 可用（有新的发布者） |
| `CMD_OUT_ON_UNAVAILABLE_IMAGE` | 17 | Image 不可用（发布者断开） |
| `REMOVE_IMAGE_CLEANUP` | 16 | Image 清理 |
| `REMOVE_SUBSCRIPTION_CLEANUP` | 15 | Subscription 清理 |
| `UNTETHERED_SUBSCRIPTION_STATE_CHANGE` | 45 | 非绑定订阅状态变更 |
| `CMD_IN_REJECT_IMAGE` | 57 | 拒绝某 Image |

#### 通道（Channel）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `SEND_CHANNEL_CREATION` | 23 | 发送通道创建 |
| `RECEIVE_CHANNEL_CREATION` | 24 | 接收通道创建 |
| `SEND_CHANNEL_CLOSE` | 25 | 发送通道关闭 |
| `RECEIVE_CHANNEL_CLOSE` | 26 | 接收通道关闭 |

#### Destination（多播/多路径）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_ADD_DESTINATION` | 30 | 添加目的地 |
| `CMD_IN_REMOVE_DESTINATION` | 31 | 移除目的地 |
| `CMD_IN_ADD_RCV_DESTINATION` | 41 | 添加接收目的地 |
| `CMD_IN_REMOVE_RCV_DESTINATION` | 42 | 移除接收目的地 |
| `CMD_IN_REMOVE_DESTINATION_BY_ID` | 56 | 按 ID 移除目的地 |

#### 计数器（Counter）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_ADD_COUNTER` | 35 | 创建计数器 |
| `CMD_IN_REMOVE_COUNTER` | 36 | 移除计数器 |
| `CMD_OUT_COUNTER_READY` | 38 | 计数器就绪 |
| `CMD_OUT_ON_UNAVAILABLE_COUNTER` | 39 | 计数器不可用 |

#### 客户端管理

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_KEEPALIVE_CLIENT` | 13 | 客户端心跳 |
| `CMD_IN_CLIENT_CLOSE` | 40 | 客户端关闭 |
| `CMD_OUT_ON_CLIENT_TIMEOUT` | 43 | 客户端超时 |
| `CMD_IN_TERMINATE_DRIVER` | 44 | 驱动器终止请求 |

#### 名称解析

| 枚举值 | ID | 含义 |
|--------|----|------|
| `NAME_RESOLUTION_NEIGHBOR_ADDED` | 46 | 新邻居节点发现 |
| `NAME_RESOLUTION_NEIGHBOR_REMOVED` | 47 | 邻居节点移除 |
| `NAME_RESOLUTION_RESOLVE` | 50 | 名称解析（DNS） |
| `NAME_RESOLUTION_LOOKUP` | 52 | 名称查找 |
| `NAME_RESOLUTION_HOST_NAME` | 53 | 主机名解析 |

#### 流控

| 枚举值 | ID | 含义 |
|--------|----|------|
| `FLOW_CONTROL_RECEIVER_ADDED` | 48 | 流控接收者加入 |
| `FLOW_CONTROL_RECEIVER_REMOVED` | 49 | 流控接收者移除 |

#### 其他

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_OUT_ON_OPERATION_SUCCESS` | 12 | 操作成功 |
| `CMD_OUT_ERROR` | 34 | 错误响应 |
| `TEXT_DATA` | 51 | 自由文本事件 |

---

### 5.2 Archive 事件（45 个）

类型码：`EventCodeType.ARCHIVE = 1`

#### 会话管理

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_CONNECT` | 1 | 客户端连接 Archive |
| `CMD_IN_AUTH_CONNECT` | 27 | 认证连接 |
| `CMD_IN_CLOSE_SESSION` | 2 | 关闭会话 |
| `CMD_IN_KEEP_ALIVE` | 28 | 会话心跳 |
| `CMD_OUT_RESPONSE` | 30 | 控制响应 |
| `CONTROL_SESSION_STATE_CHANGE` | 35 | 控制会话状态变更 |

#### 录制操作

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_START_RECORDING` | 3 | 开始录制（v1） |
| `CMD_IN_START_RECORDING2` | 31 | 开始录制（v2，支持 sourceIdentity） |
| `CMD_IN_STOP_RECORDING` | 4 | 停止录制（按订阅） |
| `CMD_IN_STOP_RECORDING_SUBSCRIPTION` | 13 | 停止录制（按 subscriptionId） |
| `CMD_IN_STOP_RECORDING_BY_IDENTITY` | 33 | 停止录制（按 recordingId） |
| `CMD_IN_EXTEND_RECORDING` | 10 | 扩展录制（v1） |
| `CMD_IN_EXTEND_RECORDING2` | 32 | 扩展录制（v2） |
| `CMD_IN_RECORDING_POSITION` | 11 | 查询录制当前位置 |
| `CMD_IN_START_POSITION` | 21 | 查询录制起始位置 |
| `CMD_IN_STOP_POSITION` | 14 | 查询录制停止位置 |
| `CMD_IN_MAX_RECORDED_POSITION` | 45 | 查询最大录制位置 |
| `RECORDING_SESSION_STATE_CHANGE` | 44 | 录制会话状态变更 |
| `RECORDING_SIGNAL` | 40 | 录制信号（START/STOP/SYNC 等） |

#### 目录查询

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_LIST_RECORDINGS` | 7 | 列出所有录制 |
| `CMD_IN_LIST_RECORDINGS_FOR_URI` | 8 | 按 URI 列出录制 |
| `CMD_IN_LIST_RECORDING` | 9 | 获取单条录制信息 |
| `CMD_IN_LIST_RECORDING_SUBSCRIPTIONS` | 16 | 列出录制订阅 |
| `CMD_IN_FIND_LAST_MATCHING_RECORD` | 15 | 查找最后匹配的录制 |

#### Segment 管理

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_TRUNCATE_RECORDING` | 12 | 截断录制 |
| `CMD_IN_PURGE_SEGMENTS` | 24 | 清除 segment 文件 |
| `CMD_IN_PURGE_RECORDING` | 38 | 清除整条录制 |
| `CMD_IN_DETACH_SEGMENTS` | 22 | 分离 segment |
| `CMD_IN_DELETE_DETACHED_SEGMENTS` | 23 | 删除已分离的 segment |
| `CMD_IN_ATTACH_SEGMENTS` | 25 | 附加 segment |
| `CMD_IN_MIGRATE_SEGMENTS` | 26 | 迁移 segment |
| `CATALOG_RESIZE` | 37 | Catalog 文件扩容 |

#### 回放操作

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_REPLAY` | 5 | 开始回放 |
| `CMD_IN_START_BOUNDED_REPLAY` | 17 | 开始有界回放 |
| `CMD_IN_STOP_REPLAY` | 6 | 停止回放 |
| `CMD_IN_STOP_ALL_REPLAYS` | 18 | 停止所有回放 |
| `CMD_IN_REQUEST_REPLAY_TOKEN` | 42 | 请求回放令牌 |
| `REPLAY_SESSION_STATE_CHANGE` | 43 | 回放会话状态变更 |
| `REPLAY_SESSION_ERROR` | 36 | 回放错误 |

#### 复制（Replication）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `CMD_IN_REPLICATE` | 19 | 开始复制（v1） |
| `CMD_IN_REPLICATE2` | 39 | 开始复制（v2） |
| `CMD_IN_TAGGED_REPLICATE` | 29 | 带标签的复制 |
| `CMD_IN_STOP_REPLICATION` | 20 | 停止复制 |
| `REPLICATION_SESSION_STATE_CHANGE` | 34 | 复制会话状态变更 |
| `REPLICATION_SESSION_DONE` | 41 | 复制完成 |

---

### 5.3 Cluster 事件（25 个）

类型码：`EventCodeType.CLUSTER = 2`

#### 选举（Election）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `NEW_ELECTION` | 22 | 新一轮选举开始（1.44.0+） |
| `ELECTION_STATE_CHANGE` | 1 | 选举状态机流转（INIT→CANVASS→CANDIDATE_BALLOT→...→LEADER） |
| `CANVASS_POSITION` | 5 | 节点广播自己的日志位置（拉票前摸底） |
| `REQUEST_VOTE` | 6 | 候选人发送投票请求 |
| `VOTE` | 25 | 节点投出选票（响应 REQUEST_VOTE，1.50.0+） |

#### 领导权（Leadership）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `NEW_LEADERSHIP_TERM` | 2 | 新领导任期开始（含 leadershipTermId、logPosition） |
| `REPLAY_NEW_LEADERSHIP_TERM` | 10 | 回放历史领导任期事件 |
| `ROLE_CHANGE` | 4 | 节点角色变更（FOLLOWER/CANDIDATE/LEADER） |

#### 状态机（ConsensusModule）

| 枚举值 | ID | 含义 |
|--------|----|------|
| `STATE_CHANGE` | 3 | ConsensusModule 状态变更（INIT/ACTIVE/SUSPENDED/CLOSED） |
| `CLUSTER_BACKUP_STATE_CHANGE` | 16 | 备份模式状态变更 |

#### 日志复制

| 枚举值 | ID | 含义 |
|--------|----|------|
| `APPEND_POSITION` | 11 | Follower 上报追加日志的位置 |
| `COMMIT_POSITION` | 12 | Leader 广播已提交的日志位置 |
| `CATCHUP_POSITION` | 7 | Follower 追赶（catch-up）中的当前位置 |
| `STOP_CATCHUP` | 8 | 停止 Follower 追赶 |
| `TRUNCATE_LOG_ENTRY` | 9 | 截断日志条目（选举后日志回滚） |

#### 会话管理

| 枚举值 | ID | 含义 |
|--------|----|------|
| `APPEND_SESSION_OPEN` | 23 | 客户端会话开启（1.49.0+） |
| `APPEND_SESSION_CLOSE` | 14 | 客户端会话关闭 |
| `CLUSTER_SESSION_STATE_CHANGE` | 24 | 会话状态变更（1.49.0+） |
| `SERVICE_ACK` | 19 | ClusteredService 对 ConsensusModule 的 ACK |

#### 终止与快照

| 枚举值 | ID | 含义 |
|--------|----|------|
| `TERMINATION_POSITION` | 17 | 通知节点终止（携带日志位置） |
| `TERMINATION_ACK` | 18 | 节点对终止指令的 ACK |
| `REPLICATION_ENDED` | 20 | Archive 复制结束 |
| `STANDBY_SNAPSHOT_NOTIFICATION` | 21 | 备用快照通知 |

---

## 6. 消息 ID 编码规则

Ring Buffer 中每条消息的 `msgTypeId` 是一个 32 位整数，编码规则：

```
 31          16 15           0
 ┌─────────────┬─────────────┐
 │ typeCode    │  eventCode  │
 │ (高 16 位)  │  (低 16 位) │
 └─────────────┴─────────────┘

例：ClusterEventCode.ELECTION_STATE_CHANGE (id=1)
  typeCode = 2 (CLUSTER)
  msgTypeId = (2 << 16) | 1 = 0x00020001 = 131073

解码（来自 EventLogReaderAgent.onMessage）：
  eventCodeTypeId = msgTypeId >> 16     // = 2 (CLUSTER)
  eventCodeId     = msgTypeId & 0xFFFF  // = 1 (ELECTION_STATE_CHANGE)
```

每个 EventCode 提供辅助方法：

```java
// ClusterEventCode 示例
public int toEventCodeId() {
    return EVENT_CODE_TYPE << 16 | (id & 0xFFFF);
}

public static ClusterEventCode fromEventCodeId(int eventCodeId) {
    return get(eventCodeId - (EVENT_CODE_TYPE << 16));
}
```

---

## 7. 启动方式与配置参数

### 7.1 静态接入（推荐用于开发/测试）

```bash
java \
  -javaagent:/path/to/aeron-agent.jar \
  -Daeron.event.log=admin \
  -Daeron.event.cluster.log=all \
  -Daeron.event.archive.log=RECORDING_SESSION_STATE_CHANGE,REPLAY_SESSION_ERROR \
  -Daeron.event.log.filename=/var/log/aeron.log \
  -jar your-app.jar
```

### 7.2 动态接入（推荐用于生产环境）

```bash
# 对 PID=12345 的进程动态开启日志（不重启）
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
    /path/to/aeron-agent.jar 12345 start /path/to/logging.properties

# 停止日志
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
    /path/to/aeron-agent.jar 12345 stop
```

`logging.properties` 文件示例：

```properties
aeron.event.log=admin
aeron.event.cluster.log=ELECTION_STATE_CHANGE,ROLE_CHANGE,APPEND_POSITION,COMMIT_POSITION
aeron.event.archive.log=RECORDING_SESSION_STATE_CHANGE,REPLAY_SESSION_ERROR
aeron.event.log.filename=/var/log/aeron-events.log
```

### 7.3 精确控制事件集合

```bash
# 只看丢包相关
-Daeron.event.log=NAK_SENT,NAK_RECEIVED,RESEND

# 看所有命令，但排除心跳（太频繁）
-Daeron.event.log=admin
-Daeron.event.log.disable=CMD_IN_KEEPALIVE_CLIENT

# 看 admin 事件并额外添加帧级别的 IN
-Daeron.event.log=admin,FRAME_IN
```

---

## 8. ServiceLoader 扩展机制

Agent 通过标准 Java `ServiceLoader` 发现 `ComponentLogger` 实现：

```
classpath:/META-INF/services/io.aeron.agent.ComponentLogger
  ├── io.aeron.agent.DriverComponentLogger    （内置）
  ├── io.aeron.agent.ArchiveComponentLogger   （内置）
  ├── io.aeron.agent.ClusterComponentLogger   （内置）
  └── com.example.MyComponentLogger           （第三方扩展）
```

`startLogging()` 中的加载逻辑：

```java
// 自动发现 classpath 上所有 ComponentLogger 实现
for (ComponentLogger logger : ServiceLoader.load(ComponentLogger.class)) {
    loggers.add(logger);
}
```

这意味着：**只要把包含 ComponentLogger 实现和 services 文件的 jar 放入 classpath，并与 aeron-agent.jar 一起加载，扩展就会自动生效。**

---

## 9. 自定义扩展方式

### 9.1 自定义 EventLogReaderAgent（替换输出目标）

这是最常见的扩展方式，适用于将事件输出到 Prometheus、Kafka、数据库等。

**步骤：**

1. 实现 Agrona `Agent` 接口
2. 在构造器中接收 `String filename`（或忽略）
3. 在 `doWork()` 中消费 `EventConfiguration.EVENT_RING_BUFFER`
4. 通过系统属性指定类名

```java
package com.example;

import io.aeron.agent.EventConfiguration;
import io.aeron.agent.EventCodeType;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;

public class MyEventReaderAgent implements Agent {

    private final MessageHandler handler = this::onEvent;

    // 支持三种构造器，EventLogAgent 按优先级尝试
    public MyEventReaderAgent(String filename, List<ComponentLogger> loggers) { }
    public MyEventReaderAgent(String filename) { }
    public MyEventReaderAgent() { }

    @Override
    public int doWork() {
        // 每次最多消费 20 条，返回实际消费数量
        return EventConfiguration.EVENT_RING_BUFFER.read(handler, 20);
    }

    private void onEvent(int msgTypeId, MutableDirectBuffer buffer,
                          int index, int length) {
        int typeCode    = msgTypeId >> 16;
        int eventCodeId = msgTypeId & 0xFFFF;

        switch (typeCode) {
            case 0: handleDriver(eventCodeId, buffer, index, length);  break;
            case 1: handleArchive(eventCodeId, buffer, index, length); break;
            case 2: handleCluster(eventCodeId, buffer, index, length); break;
        }
    }

    @Override public String roleName() { return "my-event-reader"; }
    @Override public void onStart() { /* 初始化 */ }
    @Override public void onClose() { /* 释放资源 */ }
}
```

**激活：**

```bash
-Daeron.event.log.reader.classname=com.example.MyEventReaderAgent
```

---

### 9.2 自定义 ComponentLogger（新增监控组件）

适用于监控 Aeron 之外的第三方组件，或为自定义类添加插桩。

**步骤：**

**1. 定义事件码枚举（实现 `EventCode` 接口）：**

```java
package com.example;

import io.aeron.agent.DissectFunction;
import io.aeron.agent.EventCode;
import org.agrona.MutableDirectBuffer;

public enum MyEventCode implements EventCode {
    MY_OPERATION_START(1, MyEventDissector::dissectStart),
    MY_OPERATION_END  (2, MyEventDissector::dissectEnd);

    private final int id;
    private final DissectFunction<MyEventCode> dissector;

    MyEventCode(int id, DissectFunction<MyEventCode> dissector) {
        this.id = id;
        this.dissector = dissector;
    }

    @Override public int id() { return id; }

    public void decode(MutableDirectBuffer buf, int offset, StringBuilder sb) {
        dissector.dissect(this, buf, offset, sb);
    }
}
```

**2. 实现 Dissector（解码为可读字符串）：**

```java
public class MyEventDissector {
    public static void dissectStart(MyEventCode code, MutableDirectBuffer buffer,
                                     int offset, StringBuilder builder) {
        builder.append("[").append(code.name()).append("] ")
               .append("operationId=").append(buffer.getLong(offset));
    }
}
```

**3. 实现 ComponentLogger：**

```java
package com.example;

import io.aeron.agent.ComponentLogger;
import net.bytebuddy.agent.builder.AgentBuilder;
import static net.bytebuddy.asm.Advice.to;
import static net.bytebuddy.matcher.ElementMatchers.*;

public class MyComponentLogger implements ComponentLogger {

    // 使用 USER 类型码（0xFFFF）避免与内置冲突，或选取 5+ 的自定义值
    private static final int TYPE_CODE = 10;

    @Override public int typeCode() { return TYPE_CODE; }

    @Override
    public AgentBuilder addInstrumentation(AgentBuilder agentBuilder,
                                            Map<String, String> configOptions) {
        // 仅在配置中包含特定 key 时启用
        if (!configOptions.containsKey("my.event.log")) {
            return agentBuilder; // 返回原 builder 表示不注册插桩
        }

        return agentBuilder
            .type(named("com.example.MyService"))        // 精确类名匹配
            .transform((builder, ...) -> builder
                .visit(to(MyInterceptor.class)
                    .on(named("doOperation"))));
    }

    @Override
    public void decode(MutableDirectBuffer buffer, int offset,
                        int eventCodeId, StringBuilder builder) {
        MyEventCode.values()[eventCodeId - 1].decode(buffer, offset, builder);
    }

    @Override public void reset() { }
    @Override public String version() { return "1.0.0"; }
}
```

**4. 注册 Interceptor：**

```java
public class MyInterceptor {
    @Advice.OnMethodEnter
    static void logStart(@Advice.Argument(0) long operationId) {
        // 写入 Ring Buffer
        final int claimIndex = EventConfiguration.EVENT_RING_BUFFER
            .tryClaim((10 << 16) | 1, Long.BYTES); // typeCode=10, eventId=1
        if (claimIndex > 0) {
            EventConfiguration.EVENT_RING_BUFFER.buffer().putLong(claimIndex, operationId);
            EventConfiguration.EVENT_RING_BUFFER.commit(claimIndex);
        }
    }
}
```

**5. 注册 ServiceLoader：**

```
# src/main/resources/META-INF/services/io.aeron.agent.ComponentLogger
com.example.MyComponentLogger
```

---

### 9.3 USER 事件类型（第三方事件）

`EventCodeType.USER = 0xFFFF` 专为第三方预留，避免与 Aeron 内置类型码冲突：

```java
// msgTypeId 构造
int msgTypeId = (0xFFFF << 16) | myEventId;

// 在自定义 ComponentLogger.typeCode() 中返回
@Override
public int typeCode() { return 0xFFFF; }
```

---

## 10. 接入 Prometheus 完整方案

### 方案架构

```
Aeron 进程（有 aeron-agent.jar 接入）
    │
    │ -Daeron.event.log.reader.classname=PrometheusEventReaderAgent
    ▼
PrometheusEventReaderAgent（自定义 Reader）
    │ 消费 RingBuffer，按 eventCode 分类
    ▼
Micrometer Counter / Gauge
    │
    ▼
/actuator/prometheus（Spring Boot Actuator）
    │
    ▼
Prometheus Scrape（每 15s）
    │
    ▼
Grafana Dashboard
```

### 关键事件 → Prometheus 指标映射

| Aeron 事件 | Prometheus 指标 | 类型 | 告警场景 |
|------------|----------------|------|----------|
| `FRAME_IN` | `aeron_driver_frame_in_total` | Counter | 流量监控 |
| `FRAME_OUT` | `aeron_driver_frame_out_total` | Counter | 流量监控 |
| `NAK_SENT` | `aeron_driver_nak_sent_total` | Counter | NAK 突增 → 网络抖动 |
| `NAK_RECEIVED` | `aeron_driver_nak_received_total` | Counter | 与 NAK_SENT 配合 |
| `RESEND` | `aeron_driver_resend_total` | Counter | 重传 → 丢包 |
| `CMD_OUT_ERROR` | `aeron_driver_error_total` | Counter | > 0 告警 |
| `ELECTION_STATE_CHANGE` | `aeron_cluster_election_state_change_total` | Counter | 选举频繁 → 不稳定 |
| `ROLE_CHANGE` | `aeron_cluster_role_change_total` | Counter | Leader 频繁切换 |
| `APPEND_POSITION` | `aeron_cluster_append_position_total` | Counter | 写入吞吐 |
| `COMMIT_POSITION` | `aeron_cluster_commit_position_total` | Counter | 复制延迟 = append - commit |
| `RECORDING_SESSION_STATE_CHANGE` | `aeron_archive_recording_state_change_total` | Counter | 录制异常 |
| `REPLAY_SESSION_ERROR` | `aeron_archive_replay_error_total` | Counter | > 0 告警 |
| `CATALOG_RESIZE` | `aeron_archive_catalog_resize_total` | Counter | 存储容量告警 |

### 启动命令

```bash
java \
  -javaagent:/path/to/aeron-agent.jar \
  -Daeron.event.log=NAK_SENT,NAK_RECEIVED,RESEND,CMD_OUT_ERROR \
  -Daeron.event.cluster.log=ELECTION_STATE_CHANGE,ROLE_CHANGE,APPEND_POSITION,COMMIT_POSITION \
  -Daeron.event.archive.log=RECORDING_SESSION_STATE_CHANGE,REPLAY_SESSION_ERROR,CATALOG_RESIZE \
  -Daeron.event.log.reader.classname=io.aeron.spring.cluster.metrics.PrometheusEventReaderAgent \
  -jar your-app.jar
```

---

*文档基于 Aeron 1.44.x 源码整理，事件枚举 ID 为源码中的真实值。*

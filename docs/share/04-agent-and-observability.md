# Aeron Agent 与可观测性 - 第五次分享

> 前置知识：已完成前四次分享
> 时间预估：45～60 分钟

---

## 目录

1. [可观测性概述](#1-可观测性概述)
2. [Aeron 的可观测性手段](#2-aeron-的可观测性手段)
3. [aeron-agent：零侵入诊断](#3-aeron-agent零侵入诊断)
4. [Java Agent + ByteBuddy 原理](#4-java-agent--bytebuddy-原理)
5. [整体架构](#5-整体架构)
6. [事件体系](#6-事件体系)
7. [启动方式与配置](#7-启动方式与配置)
8. [扩展机制](#8-扩展机制)
9. [Counters：实时监控指标](#9-counters实时监控指标)
10. [生产排查实战](#10-生产排查实战)
11. [关键设计总结](#11-关键设计总结)

---

## 1. 可观测性概述

### 1.1 为什么 Aeron 的可观测性是难点

Aeron 的极致性能来源于：
- 无 GC（不能用传统日志库，会导致对象分配）
- 无锁（不能加锁打日志）
- 忙轮询（业务线程不能被阻塞）
- mmap 共享内存（状态不在 JVM 堆上）

这意味着传统的 `log.info()` 方式完全不适用。

### 1.2 Aeron 的可观测性原则

```
原则 1：零 GC
  不能在热路径上分配对象
  日志写入 → Ring Buffer（固定大小内存）→ 异步消费输出

原则 2：零阻塞
  业务线程的日志写入必须是非阻塞的（tryClaim 返回 INSUFFICIENT_CAPACITY 则丢弃）
  消费线程异步处理格式化和 IO

原则 3：零侵入
  不修改业务代码
  通过 Java Agent + ByteBuddy 字节码注入

原则 4：按需开关
  生产环境默认关闭
  通过系统属性或动态 attach 开关特定事件类型
```

---

## 2. Aeron 的可观测性手段

| 手段 | 适用场景 | 延迟影响 | 信息量 |
|------|---------|---------|--------|
| **aeron-agent** | 深度诊断、事件追踪 | 极低（无锁 Ring Buffer） | 最全（130+ 事件） |
| **Counters（CnC）** | 实时监控、告警 | 零（mmap 共享内存读取） | 关键指标 |
| **Error Log（CnC）** | 错误排查 | 零影响 | 错误信息 |
| **AeronStat** | 运维监控仪表盘 | 零（只读 mmap） | Counters 可视化 |
| **Archive Catalog** | 录制数据管理 | 零影响 | 录制元数据 |

---

## 3. aeron-agent：零侵入诊断

### 3.1 核心能力

**aeron-agent** 是一个 **Java Instrumentation Agent**，提供：

- 零侵入：不修改任何 Aeron 源码，运行时字节码注入
- 低开销：Ring Buffer 无锁写入 + 异步消费，对业务线程影响 < 1μs
- 动态开关：对运行中进程 attach/detach，无需重启
- 全面覆盖：Driver（60+）、Archive（45+）、Cluster（25+）三类 130+ 事件
- 可扩展：通过 SPI 支持自定义事件类型和输出目标

### 3.2 典型使用场景

```
场景 1：排查 Publication 背压问题
  aeron.event.log=FLOW_CONTROL_CALCULATED,SEND_CHANNEL_CREATION
  → 看 flowControl 计算的 window 大小
  → 看 senderLimit 变化

场景 2：诊断连接建立失败
  aeron.event.log=CHANNEL_STATUS_CHANGE,IMAGE_CREATION
  → 看 SETUP 帧是否发出
  → 看 Image 是否成功创建

场景 3：追踪 Cluster 选举过程
  aeron.event.cluster.log=all
  → 看完整的 Election 状态机流转
  → 看 CANVASS/NOMINATE/VOTE 消息

场景 4：Archive 录制诊断
  aeron.event.archive.log=CATALOG_EXTEND,RECORDING_SIGNAL
  → 看录制是否正常开始
  → 看 RecordingSignal 是否到达客户端
```

---

## 4. Java Agent + ByteBuddy 原理

### 4.1 Java Instrumentation API

```
两种接入方式：

静态接入（JVM 启动时）：
  java -javaagent:aeron-agent.jar=aeron.event.log=all -jar myapp.jar
         │
         └─ JVM 调用 EventLogAgent.premain(agentArgs, Instrumentation)

动态接入（运行中进程）：
  DynamicLoggingAgent <pid> start aeron.event.log=CMD_IN_ADD_PUBLICATION
         │
         └─ ByteBuddyAgent.attach(agentJar, pid, agentArgs)
            → JVM 调用 EventLogAgent.agentmain(agentArgs, Instrumentation)

停止：
  DynamicLoggingAgent <pid> stop
         └─ agentmain 收到 "stop" → 还原字节码 + 停止 Reader 线程
```

### 4.2 ByteBuddy 字节码注入

```java
// DriverComponentLogger.addInstrumentation() 的核心逻辑
agentBuilder
    // 匹配目标类（按类名后缀）
    .type(nameEndsWith("ClientCommandAdapter"))
    // 对匹配到的类进行字节码变换
    .transform((builder, typeDescription, classLoader, module) -> builder
        // 在 onMessage 方法入口注入 CmdInterceptor 的 Advice
        .visit(to(CmdInterceptor.class).on(named("onMessage"))))
    // 更多插桩点...
    .type(nameEndsWith("UdpChannelTransport"))
    .transform((builder, ...) -> builder
        .visit(to(ChannelEndpointInterceptor.class).on(named("sendHook"))))
```

**注入的代码（Advice）：**

```java
// CmdInterceptor.java（简化）
@Advice.OnMethodEnter
static void logCmd(
    @Advice.Argument(0) final int msgTypeId,
    @Advice.Argument(1) final DirectBuffer buffer,
    @Advice.Argument(2) final int index,
    @Advice.Argument(3) final int length)
{
    // 这段代码被"粘贴"到 onMessage 方法入口处
    // 根据命令类型写入 Ring Buffer
    switch (msgTypeId) {
        case ADD_PUBLICATION:
            LOGGER.log(CMD_IN_ADD_PUBLICATION, buffer, index, length);
            break;
        case ADD_SUBSCRIPTION:
            LOGGER.log(CMD_IN_ADD_SUBSCRIPTION, buffer, index, length);
            break;
        // 60+ 种命令类型...
    }
}
```

### 4.3 Retransformation（动态 attach 的关键）

```
普通 ClassFileTransformer：
  只在类加载时生效，无法修改已加载的类

RETRANSFORMATION 策略：
  ① 注册 ClassFileTransformer
  ② instrumentation.retransformClasses(alreadyLoadedClass)
  ③ JVM 重新应用所有 transformer 到该类的字节码
  ④ 已运行的方法调用不受影响，下次调用生效

这使得 DynamicLoggingAgent 可以在进程运行时动态开启日志，
无需重启。
```

---

## 5. 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                       Aeron 业务进程（JVM）                            │
│                                                                      │
│  ┌─── 被插桩的 Aeron 内部方法 ─────────────────────────────────────┐  │
│  │  MediaDriver              Archive             Cluster           │  │
│  │  ├─ ClientCommandAdapter  ├─ ControlSession   ├─ Election       │  │
│  │  ├─ UdpChannelTransport   ├─ RecordingSession ├─ LogPublisher   │  │
│  │  └─ DriverConductor       └─ ReplaySession    └─ ConsensusAgent │  │
│  │         ↑                       ↑                   ↑           │  │
│  │      @Advice.OnMethodEnter（ByteBuddy 注入，业务线程执行）        │  │
│  └─────────────────────────┬───────────────────────────────────────┘  │
│                             │ LOGGER.log(eventCode, buffer, index, len) │
│                             ▼ ManyToOneRingBuffer.tryClaim + commit     │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │        ManyToOneRingBuffer（默认 8MB，直接内存，缓存行对齐）       │  │
│  │        多生产者（各业务线程）→ 单消费者（event-log-reader）        │  │
│  └────────────────────────────┬─────────────────────────────────────┘  │
│                               │ 异步消费（SleepingMillisIdleStrategy）  │
│  ┌────────────────────────────▼─────────────────────────────────────┐  │
│  │              EventLogReaderAgent（daemon 线程）                   │  │
│  │              ringBuffer.read(handler, 20)                        │  │
│  │              → 按 ComponentLogger.decode() 解码                   │  │
│  │              → Dissector 转换为可读文本                            │  │
│  └────────────────────────────┬─────────────────────────────────────┘  │
│                               │                                       │
│              ┌────────────────┼────────────────┐                      │
│              ▼                ▼                ▼                      │
│           stdout           文件            自定义输出                  │
└──────────────────────────────────────────────────────────────────────┘
```

### 5.1 数据流时序

```
业务线程                    Ring Buffer             event-log-reader
    │                           │                         │
    │ 1. Aeron 方法被调用        │                         │
    │ 2. @Advice 注入代码执行    │                         │
    │─── tryClaim(type, len) ──▶│                         │
    │─── 写入 header+payload ──▶│                         │
    │─── commit(index) ────────▶│  ← volatile write       │
    │ 3. 原方法继续执行           │                         │
    │                           │◀──── read(handler, 20) ──│ 每 1ms 消费一批
    │                           │─── onMessage(...) ──────▶│
    │                           │                          │ decode → 文本
    │                           │                          │ write → stdout/file
```

---

## 6. 事件体系

### 6.1 三类事件

```
Driver 事件（DriverEventCode，60+ 个）：
  命令类：CMD_IN_ADD_PUBLICATION / CMD_IN_ADD_SUBSCRIPTION / ...
  网络类：FRAME_IN / FRAME_OUT（UDP 帧收发）
  状态类：CHANNEL_STATUS_CHANGE / IMAGE_CREATION / IMAGE_CLOSE
  流控类：FLOW_CONTROL_CALCULATED / SEND_CHANNEL_CREATION
  NAK 类：NAK_MESSAGE_SENT / NAK_MESSAGE_RECEIVED
  管理类：REMOVE_IMAGE_CLEANUP / REMOVE_PUBLICATION_CLEANUP

Archive 事件（ArchiveEventCode，45+ 个）：
  录制类：CATALOG_EXTEND / RECORDING_SESSION_CREATED
  回放类：REPLAY_SESSION_CREATED / REPLAY_SESSION_DONE
  控制类：CONTROL_SESSION_CONNECT / RECORDING_SIGNAL
  文件类：SEGMENT_FILE_CREATED / SEGMENT_FILE_DELETE

Cluster 事件（ClusterEventCode，25+ 个）：
  选举类：NEW_LEADERSHIP_TERM / ELECTION_STATE_CHANGE / CANVASS_POSITION
         NOMINATE / REQUEST_VOTE / VOTE
  日志类：LOG_APPEND_POSITION / LOG_REPLICATION_SESSION_STATE_CHANGE
  快照类：SNAPSHOT_TAKEN / SNAPSHOT_LOADED
  角色类：ROLE_CHANGE / STATE_CHANGE
```

### 6.2 消息 ID 编码规则

```
消息写入 Ring Buffer 时的 typeId 编码：

typeId = (componentTypeCode << 16) | eventCode.id()

componentTypeCode：
  DRIVER  = 1
  ARCHIVE = 2
  CLUSTER = 3
  USER    = 4（第三方扩展）

示例：
  CMD_IN_ADD_PUBLICATION = (1 << 16) | 1 = 0x00010001
  CATALOG_EXTEND         = (2 << 16) | 5 = 0x00020005

Reader 侧：
  componentTypeCode = typeId >>> 16
  eventCode = typeId & 0xFFFF
  → 找到对应的 ComponentLogger.decode()
```

### 6.3 日志输出格式示例

```
# Driver CMD 事件
[00:01:23.456] DRIVER CMD_IN_ADD_PUBLICATION [clientId=1, correlationId=2, streamId=10, channel="aeron:udp?endpoint=localhost:40123"]

# UDP 帧事件
[00:01:23.457] DRIVER FRAME_OUT [127.0.0.1:40123 -> 127.0.0.1:40456 type=DATA sessionId=1234 streamId=10 termId=0 termOffset=0 frameLength=128]

# Archive 录制事件
[00:01:24.123] ARCHIVE RECORDING_SESSION_CREATED [recordingId=0, startPosition=0, sessionId=1234, streamId=10, channel="spy:aeron:udp?..."]

# Cluster 选举事件
[00:01:25.001] CLUSTER ELECTION_STATE_CHANGE [INIT -> CANVASS memberId=0 leadershipTermId=0 logPosition=0]
[00:01:25.002] CLUSTER CANVASS_POSITION [memberId=1 leadershipTermId=0 logPosition=0]
[00:01:25.003] CLUSTER NEW_LEADERSHIP_TERM [leadershipTermId=1 logPosition=0 leaderId=0]
```

---

## 7. 启动方式与配置

### 7.1 静态 JVM Agent

```bash
java -javaagent:/path/to/aeron-agent.jar \
     -Daeron.event.log=CMD_IN_ADD_PUBLICATION,CMD_IN_ADD_SUBSCRIPTION \
     -Daeron.event.archive.log=all \
     -Daeron.event.cluster.log=ELECTION_STATE_CHANGE,ROLE_CHANGE \
     -Daeron.event.log.filename=/var/log/aeron-events.log \
     -jar myapp.jar
```

### 7.2 动态 attach（无需重启）

```bash
# 查找 Aeron 进程 PID
jps -l | grep MyAeronApp

# 动态开启诊断日志
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
     <pid> start "aeron.event.log=FLOW_CONTROL_CALCULATED|aeron.event.log.filename=/tmp/aeron.log"

# 动态停止日志
java -cp aeron-agent.jar io.aeron.agent.DynamicLoggingAgent \
     <pid> stop
```

### 7.3 主要配置参数

| 系统属性 | 含义 | 示例值 |
|---------|------|--------|
| `aeron.event.log` | Driver 事件（逗号分隔） | `all` / `admin` / `CMD_IN_ADD_PUBLICATION` |
| `aeron.event.log.disable` | 从已启用中排除的事件 | `FRAME_IN,FRAME_OUT`（高频帧太多时排除） |
| `aeron.event.archive.log` | Archive 事件 | `all` / `RECORDING_SIGNAL` |
| `aeron.event.cluster.log` | Cluster 事件 | `all` / `ELECTION_STATE_CHANGE` |
| `aeron.event.log.filename` | 输出文件路径（不设则 stdout） | `/var/log/aeron.log` |
| `aeron.event.buffer.length` | Ring Buffer 大小 | `8388608`（默认 8MB） |
| `aeron.event.log.reader.classname` | 自定义 Reader 类 | `com.myco.AeronPrometheusReader` |

### 7.4 预设事件组

```
all:   所有事件（包括高频帧事件，谨慎使用）

admin: "管理面"事件子集，排除高频的 FRAME_IN/FRAME_OUT/NAK_MESSAGE 等
       适合生产环境长期开启：
       - CMD_IN_* / CMD_OUT_*（所有命令）
       - CHANNEL_STATUS_CHANGE（通道状态变化）
       - IMAGE_CREATION / IMAGE_CLOSE（Image 生命周期）
       - FLOW_CONTROL_CALCULATED（流控）
```

---

## 8. 扩展机制

### 8.1 SPI ComponentLogger

```
aeron-agent 通过 ServiceLoader 发现 ComponentLogger：
META-INF/services/io.aeron.agent.ComponentLogger

内置实现：
  io.aeron.agent.DriverComponentLogger
  io.aeron.agent.ArchiveComponentLogger
  io.aeron.agent.ClusterComponentLogger

每个 ComponentLogger 负责：
  ① typeCode()          → 消息类型码（DRIVER=1/ARCHIVE=2/CLUSTER=3）
  ② addInstrumentation()→ 向 AgentBuilder 注册插桩点
  ③ decode()            → Reader 侧反序列化消息
  ④ reset()             → 停止日志时清理状态
```

### 8.2 自定义 Reader（接入 Prometheus）

```java
// 替换默认的 stdout 输出，把事件写入 Prometheus 指标
public class AeronPrometheusReader implements Agent, MessageHandler {

    private final Counter publisherCount = Counter.build()
        .name("aeron_add_publication_total").register();

    @Override
    public int doWork() {
        return EVENT_RING_BUFFER.read(this, 20); // 批量读取最多 20 条
    }

    @Override
    public void onMessage(int typeId, MutableDirectBuffer buffer, int offset, int length) {
        int componentType = typeId >>> 16;
        int eventCode = typeId & 0xFFFF;

        if (componentType == DRIVER_TYPE_CODE && eventCode == CMD_IN_ADD_PUBLICATION.id()) {
            publisherCount.inc(); // 增加 Prometheus 计数器
        }
        // ... 其他事件处理
    }
}
```

配置：
```
-Daeron.event.log.reader.classname=com.myco.AeronPrometheusReader
```

### 8.3 自定义 ComponentLogger（监控业务代码）

```java
// 对自定义组件中的方法插桩
public class MyServiceComponentLogger implements ComponentLogger {
    @Override
    public AgentBuilder addInstrumentation(AgentBuilder agentBuilder, ...) {
        return agentBuilder
            .type(nameEndsWith("MyOrderProcessor"))
            .transform((builder, ...) -> builder
                .visit(to(MyOrderInterceptor.class).on(named("processOrder"))));
    }
}

@Advice.OnMethodEnter
static void logOrder(@Advice.Argument(0) final DirectBuffer buffer,
                     @Advice.Argument(1) final int offset,
                     @Advice.Argument(2) final int length) {
    MY_LOGGER.log(ORDER_PROCESSED, buffer, offset, length);
}
```

---

## 9. Counters：实时监控指标

### 9.1 Counter 机制

Aeron 的 Counter 存储在 **CnC 文件的 Counters Values Buffer** 中（mmap 共享内存），任何进程都可以零拷贝读取，不需要 Agent：

```
cnc.dat
┌──────────────────────────────────────────────────────┐
│  Counters Metadata Buffer                            │
│  [id=0, typeId=1, label="aeron.publication.limit 1"] │  Counter 标签
│  [id=1, typeId=2, label="aeron.sub-pos 2"]           │
└──────────────────────────────────────────────────────┘
┌──────────────────────────────────────────────────────┐
│  Counters Values Buffer                              │
│  [id=0, value=1048576]  ← publication position limit │  Counter 数值
│  [id=1, value=983040]   ← subscriber position        │
└──────────────────────────────────────────────────────┘
```

### 9.2 关键 Counter 类型

| Counter | 含义 | 监控用途 |
|---------|------|---------|
| `publication-limit` | Publication 的发送上限（流控窗口上限） | 持续 < position → 背压问题 |
| `publisher-pos` | Publisher 当前写入 position | 增速 = 写入速度 |
| `subscriber-pos` | Subscriber 当前消费 position | 落后 = 消费慢 |
| `sender-pos` | Sender 当前发送 position | 落后 publisher → sender 慢 |
| `receiver-hwm` | Receiver 收到的最高 position | 落后 sender-pos → 网络丢包 |
| `recording-pos` | Archive 录制进度 | 落后 publisher → 写盘慢 |
| `system-errors` | 系统错误计数 | 非零 → 需要查 Error Log |

### 9.3 AeronStat 工具

```bash
# 实时查看所有 Counters（类似 vmstat/iostat）
java -cp aeron-all.jar io.aeron.samples.AeronStat

输出示例：
  0: publisher-limit[pub-id=1] = 16777216
  1: publisher-pos[sessionId=1234, streamId=10] = 16384000
  2: subscriber-pos[sessionId=1234, subId=1] = 16383000
  3: recorder-pos[recordingId=0] = 16380000
  4: system-errors = 0
  5: commit-position[Cluster] = 1024000
```

### 9.4 程序化读取 Counters

```java
// 从 CnC 文件读取 Counters（只读，零影响）
try (Aeron aeron = Aeron.connect()) {
    CountersReader counters = aeron.countersReader();

    counters.forEach((counterId, typeId, keyBuffer, label) -> {
        long value = counters.getCounterValue(counterId);
        System.out.printf("Counter[%d] %s = %d%n", counterId, label, value);
    });

    // 持续监控特定 Counter
    int targetCounterId = 3; // 假设 recording-pos 的 ID
    while (true) {
        long pos = counters.getCounterValue(targetCounterId);
        System.out.println("Recording position: " + pos);
        Thread.sleep(1000);
    }
}
```

---

## 10. 生产排查实战

### 10.1 排查背压问题

```
症状：publication.offer() 持续返回 BACK_PRESSURED (-2)

步骤 1：读取 Counters
  publisher-pos    = 100000000
  publication-limit = 100000256  ← limit 非常接近 publisher-pos

步骤 2：开启 Agent 查看流控
  -Daeron.event.log=FLOW_CONTROL_CALCULATED,SEND_CHANNEL_CREATION

步骤 3：查看日志
  [FLOW_CONTROL_CALCULATED] receiverWindowSize=256 subscriberPosition=99800000
  → receiverWindowSize 太小
  → Subscriber 消费速度跟不上

解决：
  ① 检查 Subscriber 的 poll 是否有 CPU 抢占（提高优先级/绑核）
  ② 增大 rcvbuf 大小（SO_RCVBUF）
  ③ 增大 initialWindowLength（接收窗口）
```

### 10.2 排查连接建立失败

```
症状：Subscriber 拿不到 Image（等待超时）

步骤 1：开启 Agent
  -Daeron.event.log=CMD_IN_ADD_SUBSCRIPTION,FRAME_OUT,IMAGE_CREATION,CHANNEL_STATUS_CHANGE

步骤 2：查看日志
  ✓ CMD_IN_ADD_SUBSCRIPTION → addSubscription 命令已发送
  ✓ FRAME_OUT SETUP → SETUP 帧已发出
  ✗ IMAGE_CREATION → 没有日志！

步骤 3：查看 FRAME_IN
  -Daeron.event.log=FRAME_IN,FRAME_OUT
  → 没有收到任何帧

结论：网络层问题（防火墙/端口冲突）
```

### 10.3 排查 Cluster 选举卡住

```
症状：Cluster 长时间没有 Leader

步骤：
  -Daeron.event.cluster.log=ELECTION_STATE_CHANGE,CANVASS_POSITION,NOMINATE,VOTE

查看日志：
  [ELECTION_STATE_CHANGE] INIT -> CANVASS
  [CANVASS_POSITION] memberId=1 ...
  [CANVASS_POSITION] memberId=2 ...
  → 没有 NOMINATE
  → 没有 VOTE

分析：
  CANVASS 收到了其他节点响应，但没有进入 NOMINATE
  原因：自己的 logLeadershipTermId 或 logPosition 不是最新的
       所以等待其他更新的节点发起 NOMINATE

解决：
  检查 RecordingLog 是否损坏
  检查 Archive 录制是否正常
```

---

## 11. 关键设计总结

### 11.1 高性能日志的技术要点

```
传统日志：
  业务线程 → String.format() → Log4j → 写文件
  问题：对象分配（GC）、格式化耗时、I/O 阻塞

Aeron Agent 的方案：
  业务线程 → tryClaim（1次 CAS）→ copyBytes → commit（1次 volatile write）
             ~100ns，零对象分配，从不阻塞

  daemon 线程 → RingBuffer.read() → decode → write file
               异步批量处理，不影响业务线程

关键技术：
  ① ManyToOneRingBuffer：多生产者/单消费者，无锁，直接内存
  ② @Advice.OnMethodEnter：不改变原方法行为，只是"追加"记录代码
  ③ 二进制格式写入 + 文本化延迟到消费侧：写入开销最小
```

### 11.2 可观测性层次

```
Level 0：应用指标（Counter）
  延迟：0（mmap 直接读取）
  粒度：聚合数值（position、error count）
  适合：Prometheus/Grafana 监控面板

Level 1：管理事件（admin 事件组）
  延迟：~100ns（Ring Buffer 写入）
  粒度：命令/状态变化
  适合：生产环境长期开启

Level 2：所有事件（all）
  延迟：~100ns（但事件量大，消费端可能积压）
  粒度：每个 UDP 帧
  适合：短时间问题诊断

Level 3：自定义扩展
  业务事件 + Aeron 事件的联合追踪
  适合：全链路追踪
```

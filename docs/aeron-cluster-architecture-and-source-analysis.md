# Aeron Cluster 架构与源码解析

## 1. 概述

Aeron Cluster 是基于 **Raft 共识算法** 实现的容错分布式服务框架，提供强一致性的日志复制、自动 Leader 选举、状态快照与恢复等能力。本文档深入解析 aeron-cluster 模块的架构设计、核心组件、启动流程及关键源码实现。

### 1.1 核心价值

- **极致性能**：微秒级延迟（P50 < 100μs）
- **强一致性**：基于 Raft 的日志序列化与多数派提交
- **容错能力**：自动故障检测、Leader 选举、成员变更
- **确定性处理**：状态机模型、精确回放、快照恢复
- **零外部依赖**：无需 ZooKeeper 等外部协调服务

### 1.2 典型应用场景

- 金融交易系统（订单簿、撮合引擎）
- 电信计费系统
- 实时风控
- 分布式状态机
- 需要微秒级延迟与强一致性的任何系统

---

## 2. 整体架构

### 2.1 三层架构

```
┌─────────────────────────────────────────────────────────────────┐
│                  Application Layer (用户业务)                     │
│         ClusteredService 接口实现 (业务状态机)                    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                Cluster Layer (共识与协调)                         │
│  ┌────────────────────┐          ┌────────────────────┐        │
│  │ ConsensusModule    │←────────→│ClusteredService    │        │
│  │  - 选举与角色管理   │          │    Container       │        │
│  │  - 日志序列化      │          │  - 业务服务容器     │        │
│  │  - 客户端会话管理  │          │  - 日志消费与回放   │        │
│  └────────────────────┘          └────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│              Archive Layer (持久化)                              │
│            Archive - 日志录制、快照存储、回放                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│         Transport Layer (传输)                                   │
│         MediaDriver - UDP/IPC 可靠传输                           │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 核心模块职责

| 模块 | 位置 | 核心职责 |
|------|------|----------|
| **ClusteredMediaDriver** | `io.aeron.cluster.ClusteredMediaDriver` | 聚合 MediaDriver、Archive、ConsensusModule 的启动器 |
| **ConsensusModule** | `io.aeron.cluster.ConsensusModule` | 共识模块主类，协调选举、日志复制、会话管理 |
| **ConsensusModuleAgent** | `io.aeron.cluster.ConsensusModuleAgent` | 共识状态机引擎，处理角色转换与日志操作 |
| **Election** | `io.aeron.cluster.Election` | Raft 选举逻辑实现 |
| **RecordingLog** | `io.aeron.cluster.RecordingLog` | 持久化日志元数据管理（term、snapshot） |
| **ClusteredServiceContainer** | `io.aeron.cluster.service.ClusteredServiceContainer` | 用户服务容器，消费日志并调用业务逻辑 |
| **ClusteredService** | `io.aeron.cluster.service.ClusteredService` | 用户业务服务接口 |

---

## 3. 核心类源码解析

### 3.1 ClusteredMediaDriver

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java`

#### 3.1.1 类职责

聚合组件，在同一进程中启动：
1. **MediaDriver**：底层 UDP/IPC 传输引擎
2. **Archive**：日志与快照持久化服务
3. **ConsensusModule**：集群共识模块

#### 3.1.2 启动流程

```java
// ClusteredMediaDriver.java:84-124
public static ClusteredMediaDriver launch(
    final MediaDriver.Context driverCtx,
    final Archive.Context archiveCtx,
    final ConsensusModule.Context consensusModuleCtx)
{
    MediaDriver driver = null;
    Archive archive = null;
    ConsensusModule consensusModule = null;

    try
    {
        // 1. 启动 MediaDriver
        driver = MediaDriver.launch(driverCtx);

        // 2. 准备 Archive 错误处理器与计数器
        final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
        final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
            archiveCtx.errorCounter() :
            new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);
        final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
            archiveCtx.errorHandler() : driverCtx.errorHandler();

        // 3. 启动 Archive，与 MediaDriver 共享 AgentInvoker 和 CnC 目录
        archive = Archive.launch(archiveCtx
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .aeronDirectoryName(driver.aeronDirectoryName())
            .errorHandler(errorHandler)
            .errorCounter(errorCounter));

        // 4. 启动 ConsensusModule
        consensusModule = ConsensusModule.launch(consensusModuleCtx
            .aeronDirectoryName(driverCtx.aeronDirectoryName()));

        return new ClusteredMediaDriver(driver, archive, consensusModule);
    }
    catch (final Exception ex)
    {
        CloseHelper.quietCloseAll(consensusModule, archive, driver);
        throw ex;
    }
}
```

**关键设计点**：

- **mediaDriverAgentInvoker**：Archive 与 MediaDriver 共享同一 Agent 调用链，避免线程切换
- **aeronDirectoryName**：确保 Archive 的 Aeron client 连接到同进程 MediaDriver 的 CnC 文件
- **错误处理统一**：三个组件共享 errorHandler/errorCounter，便于监控

---

### 3.2 ConsensusModule

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java`

#### 3.2.1 类职责

集群共识的核心模块，负责：
- 维护集群成员关系
- 协调 Leader 选举
- 日志序列化与复制
- 客户端会话管理
- 与 Archive 协作持久化日志
- 与 ClusteredServiceContainer 通信

#### 3.2.2 配置上下文

```java
// ConsensusModule.Context 关键配置
public static class Context implements Cloneable
{
    // 集群成员配置：格式为 "memberId,ingressEndpoint,consensusEndpoint,logEndpoint,archiveEndpoint|..."
    private String clusterMembers;

    // 当前节点 ID
    private int clusterMemberId = NULL_VALUE;

    // 客户端消息接入通道
    private String ingressChannel;

    // 日志复制通道（Leader → Followers）
    private String logChannel;

    // 成员间共识通信通道
    private String replicationChannel;

    // 选举超时配置
    private long leaderHeartbeatTimeoutNs = TimeUnit.SECONDS.toNanos(10);
    private long electionTimeoutNs = TimeUnit.SECONDS.toNanos(2);

    // 快照配置
    private int snapshotCounter = 0;
    private long snapshotIntervalLength = 1024 * 1024;

    // 认证与授权
    private AuthenticatorSupplier authenticatorSupplier;
    private AuthorisationServiceSupplier authorisationServiceSupplier;

    // ...
}
```

#### 3.2.3 启动入口

```java
// ConsensusModule.java:1057-1093
public static ConsensusModule launch(final Context ctx)
{
    return new ConsensusModule(ctx).start();
}

private ConsensusModule(final Context ctx)
{
    ctx.conclude();  // 校验配置、填充默认值
    this.ctx = ctx;

    // 创建 ConsensusModuleAgent（核心状态机）
    agent = new ConsensusModuleAgent(ctx);

    // 根据 threading mode 决定启动方式
    if (INVOKER == ctx.threadingMode())
    {
        invoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), agent);
    }
    else
    {
        runner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(),
            ctx.errorCounter(), agent);
    }
}

private ConsensusModule start()
{
    if (null != invoker)
        invoker.start();
    else
        AgentRunner.startOnThread(runner, ctx.threadFactory());

    return this;
}
```

---

### 3.3 ConsensusModuleAgent

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java:121`

#### 3.3.1 核心状态机

ConsensusModuleAgent 是整个共识模块的状态机引擎，实现 `Agent` 接口，主循环处理：

```java
// ConsensusModuleAgent.java:主工作循环
public int doWork()
{
    int workCount = 0;

    final long nowNs = clusterClock.timeNanos();
    final long nowMs = clusterClock.timeMillis();

    try
    {
        // 处理角色逻辑
        workCount += role.doWork(nowNs);

        // 处理客户端会话
        workCount += sessionManager.doWork(nowMs);

        // 处理定时器
        workCount += timerService.poll(nowMs);

        // 检查心跳超时
        workCount += election.doWork(nowNs);

        // 处理 Archive 操作
        workCount += consensusModuleAdapter.poll();

        // 处理服务 ACK
        workCount += serviceAckPoller.poll();
    }
    catch (final AgentTerminationException ex)
    {
        runTerminationHook();
        throw ex;
    }

    return workCount;
}
```

#### 3.3.2 角色状态转换

Cluster 节点有三种角色：

```java
// Cluster.Role (aeron-cluster/src/main/java/io/aeron/cluster/service/Cluster.java:51)
enum Role
{
    FOLLOWER(0),    // 跟随者：接收 Leader 的日志复制
    CANDIDATE(1),   // 候选人：参与选举，争取成为 Leader
    LEADER(2);      // 领导者：处理客户端请求，序列化日志并复制
}
```

角色转换由 `Election` 类驱动，状态图如下：

```
       启动
        ↓
    ┌───────┐
    │ INIT  │
    └───┬───┘
        ↓
    ┌────────┐
    │CANVASS │ (拉票)
    └───┬────┘
        ↓
    ┌─────────┐
    │NOMINATE │ (提名)
    └───┬─────┘
        ↓
    ┌──────────────┐       ┌──────────────┐
    │CANDIDATE_    │──No──→│FOLLOWER_     │
    │  BALLOT      │       │  BALLOT      │
    └──────┬───────┘       └──────┬───────┘
           │ 获得多数派投票        │ 投票给他人
           ↓                      ↓
    ┌──────────────┐       ┌──────────────┐
    │LEADER_READY  │       │FOLLOWER_READY│
    │  (成为Leader)│       │ (跟随Leader) │
    └──────────────┘       └──────────────┘
```

---

### 3.4 Election (选举逻辑)

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/Election.java:70`

#### 3.4.1 选举触发条件

- 节点启动时无 Leader
- Leader 心跳超时 (`leaderHeartbeatTimeoutNs`)
- 成员配置变更

#### 3.4.2 选举状态机

```java
// Election.java:核心状态转换
int doWork(final long nowNs)
{
    int workCount = 0;

    switch (state)
    {
        case CANVASS:
            workCount += canvass(nowNs);
            break;

        case NOMINATE:
            workCount += nominate(nowNs);
            break;

        case CANDIDATE_BALLOT:
            workCount += candidateBallot(nowNs);
            break;

        case FOLLOWER_BALLOT:
            workCount += followerBallot(nowNs);
            break;

        case LEADER_INIT:
        case LEADER_REPLAY:
        case LEADER_LOG_REPLICATION:
        case LEADER_READY:
            workCount += leader(nowNs);
            break;

        case FOLLOWER_CATCHUP:
        case FOLLOWER_REPLAY:
        case FOLLOWER_READY:
            workCount += follower(nowNs);
            break;
    }

    return workCount;
}
```

#### 3.4.3 关键阶段说明

**1. CANVASS (拉票阶段)**

```java
private int canvass(final long nowNs)
{
    // 向所有成员发送 CanvassPosition 消息
    // 收集各节点的日志位置 (logPosition, leadershipTermId)

    if (收到所有成员响应 || 超时)
    {
        // 比较日志完整性，最新的节点有资格提名
        state(NOMINATE, nowNs);
    }
}
```

**2. NOMINATE (提名阶段)**

```java
private int nominate(final long nowNs)
{
    if (本节点日志最新)
    {
        candidateTermId = logLeadershipTermId + 1;
        // 自我提名为候选人
        state(CANDIDATE_BALLOT, nowNs);
    }
    else
    {
        // 等待他人提名
        state(FOLLOWER_BALLOT, nowNs);
    }
}
```

**3. CANDIDATE_BALLOT (候选投票)**

```java
private int candidateBallot(final long nowNs)
{
    // 发送 RequestVote(candidateTermId, logPosition)

    if (收到多数派 Vote)
    {
        // 成为 Leader
        publishNewLeadershipTerm();
        state(LEADER_INIT, nowNs);
    }
    else if (超时 || 收到更高 term 的消息)
    {
        // 转为 Follower
        state(FOLLOWER_BALLOT, nowNs);
    }
}
```

**4. FOLLOWER_BALLOT (跟随投票)**

```java
private int followerBallot(final long nowNs)
{
    // 收到 RequestVote 消息
    if (candidateTermId > localTermId && 候选人日志不落后)
    {
        // 投票给该候选人
        sendVote(candidateMemberId);
    }

    // 收到 NewLeadershipTerm 消息
    if (leaderMemberId 确认)
    {
        state(FOLLOWER_CATCHUP_INIT, nowNs);
    }
}
```

---

### 3.5 RecordingLog (日志元数据管理)

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java:46`

#### 3.5.1 职责

管理集群的持久化日志元数据，包括：
- **Leadership Term 记录**：每个 term 的 recordingId、起始位置、结束位置
- **Snapshot 记录**：快照的 recordingId、serviceId、日志位置、时间戳

RecordingLog 文件格式：

```
Record Layout (每条记录):
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        Recording ID                           |
 |                                                               |
 +---------------------------------------------------------------+
 |                     Leadership Term ID                        |
 |                                                               |
 +---------------------------------------------------------------+
 |              Log Position at beginning of term                |
 |                                                               |
 +---------------------------------------------------------------+
 |        Log Position when entry was created                    |
 |                                                               |
 +---------------------------------------------------------------+
 |               Timestamp (epoch millis)                        |
 |                                                               |
 +---------------------------------------------------------------+
 |                  Service ID (for Snapshot)                    |
 +---------------------------------------------------------------+
 |R|               Entry Type (0=Term, 1=Snapshot)               |
 +---------------------------------------------------------------+
```

#### 3.5.2 核心方法

```java
// RecordingLog.java:关键 API
public class RecordingLog
{
    // 追加新的 Leadership Term 记录
    public void appendTerm(
        long recordingId,
        long leadershipTermId,
        long termBaseLogPosition,
        long timestamp)

    // 追加快照记录
    public void appendSnapshot(
        long recordingId,
        long leadershipTermId,
        long termBaseLogPosition,
        long logPosition,
        long timestamp,
        int serviceId)

    // 提交日志位置（更新当前 term 的结束位置）
    public void commitLogPosition(long leadershipTermId, long logPosition)

    // 恢复最新状态：返回最新快照 + 后续 term 列表
    public RecoveryPlan createRecoveryPlan(Archive archive)

    // 无效化旧快照（保留指定位置后的快照）
    public void invalidateLatestSnapshot()
}
```

---

### 3.6 ClusteredService 接口

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredService.java:36`

#### 3.6.1 用户业务接口

用户实现该接口以定义业务逻辑：

```java
public interface ClusteredService
{
    // 服务启动，加载快照
    void onStart(Cluster cluster, Image snapshotImage);

    // 客户端会话打开
    void onSessionOpen(ClientSession session, long timestamp);

    // 客户端会话关闭
    void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason);

    // 处理客户端消息（核心业务逻辑）
    void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    // 定时器触发
    void onTimerEvent(long correlationId, long timestamp);

    // 保存快照
    void onTakeSnapshot(ExclusivePublication snapshotPublication);

    // 角色变更通知 (LEADER ↔ FOLLOWER)
    void onRoleChange(Cluster.Role newRole);

    // 服务终止
    void onTerminate(Cluster cluster);

    // 新的领导任期开始
    default void onNewLeadershipTermEvent(
        long leadershipTermId,
        long logPosition,
        long timestamp,
        long termBaseLogPosition,
        int leaderMemberId,
        int logSessionId,
        TimeUnit timeUnit,
        int appVersion) {}

    // 后台任务（非确定性，不能修改状态）
    default int doBackgroundWork(long nowNs) { return 0; }
}
```

#### 3.6.2 确定性保证

关键原则：
- **只能在 `onSessionMessage`、`onTimerEvent` 等事件回调中修改业务状态**
- **不能在 `onStart`、`onRoleChange`、`onTakeSnapshot` 中发送消息或调度定时器**
- **不能在 `doBackgroundWork` 中修改状态或调用 `cluster.offer()`**

---

## 4. 消息处理流程

### 4.1 客户端消息处理 (Leader 节点)

```
┌─────────────┐
│AeronCluster │ (Client)
│  .offer()   │
└──────┬──────┘
       │ UDP
       ↓
┌──────────────────────┐
│ ConsensusModule      │ (Leader)
│  IngressAdapter      │
└──────┬───────────────┘
       │
       ↓ 分配 logPosition
┌──────────────────────┐
│  LogPublisher        │
│   .appendMessage()   │
└──────┬───────────────┘
       │
       ↓ 发布到 log stream
┌──────────────────────┐
│  Archive             │
│  (开始录制)           │
└──────────────────────┘
       │
       ↓ UDP 复制
┌──────────────────────┐
│  Follower 1, 2, ...  │
│  (接收日志复制)       │
└──────────────────────┘
       │
       ↓ 多数派确认
┌──────────────────────┐
│ ConsensusModule      │
│  推进 commitPosition  │
└──────┬───────────────┘
       │
       ↓ 已提交日志
┌──────────────────────┐
│ClusteredService      │
│  Container           │
│  BoundedLogAdapter   │
└──────┬───────────────┘
       │
       ↓
┌──────────────────────┐
│ ClusteredService     │
│  .onSessionMessage() │
│  (用户业务逻辑)       │
└──────────────────────┘
```

### 4.2 日志复制协议

Leader 发布消息到 log stream：

```java
// LogPublisher.java:appendMessage
public long appendMessage(
    final long leadershipTermId,
    final long clusterSessionId,
    final long timestamp,
    final DirectBuffer buffer,
    final int offset,
    final int length)
{
    // 1. 编码 Session Message Header (SBE)
    sessionMessageHeaderEncoder
        .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
        .leadershipTermId(leadershipTermId)
        .clusterSessionId(clusterSessionId)
        .timestamp(timestamp);

    // 2. 发布到 log publication
    final long position = publication.offer(buffer, offset, length);

    // 3. Archive 自动录制此 stream
    return position;
}
```

Follower 接收日志：

```java
// LogAdapter.java:poll
public int poll(final long commitPosition)
{
    return subscription.poll(fragmentHandler, commitPosition);
}

// ConsensusModuleAgent.java:处理日志消息
private void onSessionMessage(
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final Header header)
{
    // 1. 解码消息头
    sessionMessageHeaderDecoder.wrap(buffer, offset, ...);

    // 2. 推进本地 logPosition
    logPosition = header.position();

    // 3. 等待多数派确认后，推进 commitPosition
    if (logPosition <= commitPosition)
    {
        // 4. 转发给 ClusteredServiceContainer
        serviceProxy.scheduleTimer(...);
    }
}
```

---

## 5. 快照与恢复

### 5.1 快照触发

触发条件：
- 达到配置的 `snapshotIntervalLength`（日志字节数阈值）
- 管理员手动发送 `AdminRequest(SNAPSHOT)`

流程：

```java
// ConsensusModuleAgent.java:takeSnapshot
private void takeSnapshot(final long timestamp)
{
    // 1. 发送 ClusterAction.SNAPSHOT 到 ServiceContainer
    serviceProxy.snapshot(timestamp);

    // 2. 等待所有 Service ACK
    awaitServicesReadyForSnapshot();

    // 3. ConsensusModule 保存自身状态
    snapshotTaker.markBegin(
        SNAPSHOT_TYPE_ID,
        logPosition,
        leadershipTermId,
        0);

    // 保存 sessions
    for (ClusterSession session : sessionByIdMap.values())
        snapshotTaker.snapshotSession(session);

    // 保存 timers
    timerService.snapshot(snapshotTaker);

    snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0);

    // 4. 记录快照到 RecordingLog
    recordingLog.appendSnapshot(
        snapshotRecordingId,
        leadershipTermId,
        termBaseLogPosition,
        logPosition,
        timestamp,
        SERVICE_ID);

    // 5. 通知 Service 保存快照
    // Service 实现 ClusteredService.onTakeSnapshot()
}
```

### 5.2 Service 快照实现示例

```java
public class MyService implements ClusteredService
{
    private final Map<Long, Order> orderBook = new HashMap<>();

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication)
    {
        // 使用 SBE 或自定义序列化
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (Order order : orderBook.values())
        {
            // 编码订单数据
            orderEncoder.wrap(buffer, 0)
                .orderId(order.id)
                .quantity(order.quantity)
                .price(order.price);

            // 发布到快照流
            while (snapshotPublication.offer(buffer, 0, orderEncoder.encodedLength()) < 0)
            {
                cluster.idleStrategy().idle();
            }
        }
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage)
    {
        if (null != snapshotImage)
        {
            // 加载快照
            loadSnapshot(snapshotImage);
        }
    }

    private void loadSnapshot(Image snapshotImage)
    {
        final FragmentHandler handler = (buffer, offset, length, header) ->
        {
            orderDecoder.wrap(buffer, offset, ...);
            Order order = new Order(
                orderDecoder.orderId(),
                orderDecoder.quantity(),
                orderDecoder.price()
            );
            orderBook.put(order.id, order);
        };

        while (snapshotImage.poll(handler, 10) > 0)
        {
            cluster.idleStrategy().idle();
        }
    }
}
```

### 5.3 恢复流程

节点启动时的恢复步骤：

```java
// ConsensusModuleAgent.java:recover
private void recover()
{
    // 1. 从 RecordingLog 构建恢复计划
    final RecoveryPlan plan = recordingLog.createRecoveryPlan(archive);

    // 2. 加载最新快照
    if (plan.snapshots.size() > 0)
    {
        final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

        // 恢复 ConsensusModule 状态
        loadSnapshot(snapshot.recordingId);

        // 通知 Service 恢复
        serviceProxy.joinLog(
            snapshot.logPosition,
            Long.MAX_VALUE,
            leaderMemberId,
            logSessionId,
            snapshot.logPosition,
            snapshot.leadershipTermId,
            ...);
    }

    // 3. 回放快照后的日志
    for (RecordingLog.Entry entry : plan.logs)
    {
        replayLog(entry.recordingId, entry.logPosition);
    }

    // 4. 恢复完成，转入正常工作状态
    state(ConsensusModule.State.ACTIVE, nowNs);
}
```

---

## 6. 协议与编码

### 6.1 SBE 协议定义

位置：`aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml`

关键消息类型：

| 消息 | templateId | 用途 |
|------|-----------|------|
| **SessionMessage** | 2 | 客户端业务消息 |
| **SessionOpen** | 3 | 会话开启 |
| **SessionClose** | 4 | 会话关闭 |
| **TimerEvent** | 5 | 定时器触发 |
| **RequestVote** | 50 | 选举投票请求 |
| **Vote** | 51 | 投票响应 |
| **NewLeadershipTerm** | 52 | 新任期通知 |
| **AppendPosition** | 53 | 日志位置同步 |
| **CommitPosition** | 54 | 提交位置更新 |
| **CatchupPosition** | 55 | 追赶同步 |
| **CanvassPosition** | 56 | 拉票消息 |
| **SnapshotMarker** | 100 | 快照标记 |

### 6.2 SessionMessage 编码示例

```xml
<sbe:message name="SessionMessage" id="2" description="Client session message">
    <field name="leadershipTermId" id="1" type="int64"/>
    <field name="clusterSessionId" id="2" type="int64"/>
    <field name="timestamp" id="3" type="time_t"/>
    <data name="payload" id="4" type="varDataEncoding"/>
</sbe:message>
```

编码代码：

```java
// Client 发送
sessionMessageEncoder
    .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
    .leadershipTermId(termId)
    .clusterSessionId(sessionId)
    .timestamp(timestamp)
    .putPayload(messageBuffer, messageOffset, messageLength);

publication.offer(buffer, offset, sessionMessageEncoder.encodedLength());
```

解码代码：

```java
// Server 接收
sessionMessageDecoder.wrap(buffer, offset, blockLength, version);
final long termId = sessionMessageDecoder.leadershipTermId();
final long sessionId = sessionMessageDecoder.clusterSessionId();
final long timestamp = sessionMessageDecoder.timestamp();

final int payloadLength = sessionMessageDecoder.payloadLength();
sessionMessageDecoder.getPayload(payloadBuffer, 0, payloadLength);
```

---

## 7. 性能优化技术

### 7.1 零拷贝日志复制

- LogPublisher 直接操作 ExclusivePublication 的共享内存 buffer
- Follower 通过 Subscription 的 Image 直接读取共享内存
- 消息从 Client → Leader → Follower → Service 全程避免序列化/反序列化

### 7.2 Busy-Spin 与 IdleStrategy

```java
// 低延迟配置
ConsensusModule.Context ctx = new ConsensusModule.Context()
    .idleStrategy(new BusySpinIdleStrategy());  // CPU 100% 但延迟最低

// 平衡配置
    .idleStrategy(new BackoffIdleStrategy(
        100, 1000, 1, TimeUnit.MICROSECONDS.toNanos(1)));
```

### 7.3 线程亲和性 (Thread Affinity)

```java
// 绑定线程到特定 CPU 核心
ConsensusModule.Context ctx = new ConsensusModule.Context()
    .threadFactory(new AffinityThreadFactory("consensus", cpuId));
```

### 7.4 预分配 Buffer

```java
// 避免运行时分配
private final ExpandableArrayBuffer offerBuffer = new ExpandableArrayBuffer(4096);
private final SessionMessageHeaderEncoder headerEncoder = new SessionMessageHeaderEncoder();
```

---

## 8. 配置最佳实践

### 8.1 三节点生产配置示例

```java
// Node 0 配置
ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
    .clusterMemberId(0)
    .clusterMembers(
        "0,node0:9000,node0:9001,node0:9002,node0:8010|" +
        "1,node1:9000,node1:9001,node1:9002,node1:8010|" +
        "2,node2:9000,node2:9001,node2:9002,node2:8010")
    .ingressChannel("aeron:udp?endpoint=node0:9000")
    .logChannel("aeron:udp?endpoint=node0:9001|control-mode=manual")
    .replicationChannel("aeron:udp?endpoint=node0:9002")
    .archiveContext(new AeronArchive.Context()
        .controlRequestChannel("aeron:udp?endpoint=node0:8010"))

    // 性能调优
    .idleStrategy(new BackoffIdleStrategy(1, 100, 1, 1000))
    .sessionTimeoutNs(TimeUnit.SECONDS.toNanos(10))
    .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(10))

    // 快照策略
    .snapshotIntervalLength(64 * 1024 * 1024)  // 64MB

    // 错误处理
    .errorHandler(new CountedErrorHandler(errorLog));

MediaDriver.Context driverCtx = new MediaDriver.Context()
    .threadingMode(ThreadingMode.SHARED)
    .dirDeleteOnStart(false)
    .sharedIdleStrategy(new BackoffIdleStrategy(...));

Archive.Context archiveCtx = new Archive.Context()
    .archiveDir(new File("/data/aeron/archive"))
    .controlChannel("aeron:udp?endpoint=node0:8010")
    .threadingMode(ArchiveThreadingMode.SHARED);

ClusteredMediaDriver clusteredMediaDriver =
    ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx);

ClusteredServiceContainer.Context serviceCtx =
    new ClusteredServiceContainer.Context()
        .clusteredService(new MyService())
        .aeronDirectoryName(driverCtx.aeronDirectoryName());

ClusteredServiceContainer container =
    ClusteredServiceContainer.launch(serviceCtx);
```

### 8.2 关键参数调优

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `leaderHeartbeatTimeoutNs` | 10s | Leader 心跳超时，过小易误判 |
| `electionTimeoutNs` | 2s | 选举超时，应 < heartbeat |
| `sessionTimeoutNs` | 10s | 客户端会话超时 |
| `snapshotIntervalLength` | 64MB-256MB | 快照间隔，平衡恢复速度与磁盘 |
| `fragmentLimit` | 10 | 每次 poll 的消息数量 |
| `threadingMode` | SHARED | 共享线程模式，适合嵌入式部署 |

---

## 9. 监控与调试

### 9.1 关键计数器

使用 `aeron-stat` 或 Aeron Counters API 监控：

| Counter | 说明 |
|---------|------|
| `cluster-role` | 当前角色 (0=Follower, 1=Candidate, 2=Leader) |
| `cluster-commit-pos` | 已提交日志位置 |
| `cluster-append-pos` | Leader 追加日志位置 |
| `cluster-election-count` | 选举次数（频繁变化表示不稳定） |
| `cluster-leadership-term-id` | 当前任期 ID |
| `errors` | 错误计数 |

### 9.2 日志分析

启用详细日志：

```java
System.setProperty("aeron.cluster.log.level", "DEBUG");
```

关键日志：
- `Election state: CANVASS → NOMINATE → LEADER_READY`
- `New leadership term: termId=X, leaderMemberId=Y`
- `Snapshot taken at position=Z`
- `Service ACK received from serviceId=...`

---

## 10. 常见问题

### 10.1 脑裂预防

Aeron Cluster 通过 **多数派机制** 预防脑裂：
- 3 节点需要 2 个确认（quorum=2）
- 5 节点需要 3 个确认（quorum=3）
- 网络分区后，只有包含多数派的分区能继续服务

### 10.2 数据一致性保证

- **顺序一致性**：所有节点按相同顺序执行日志消息
- **持久性**：日志只有在多数派持久化后才提交
- **确定性回放**：崩溃恢复通过回放日志达到一致状态

### 10.3 成员变更

Aeron Cluster 支持动态成员变更（需使用 Admin API）：

```java
// 添加新节点 (通过 ClusterTool 或 Admin API)
ClusterTool.addPassiveMember(
    clusterDir,
    memberId,
    clusterMember,
    memberEndpoints);
```

变更流程：
1. 新节点以 PASSIVE 模式启动
2. Leader 复制快照与日志到新节点
3. 新节点追上后，更新集群配置
4. 新节点转为 ACTIVE 并参与共识

---

## 11. 总结

Aeron Cluster 通过以下设计实现了微秒级延迟的强一致性集群：

**核心技术**：
1. **Raft 共识算法**：Leader 选举、日志复制、安全性保证
2. **零拷贝架构**：UDP/IPC 传输 + 共享内存日志
3. **确定性状态机**：精确回放、快照恢复
4. **SBE 高效编码**：零解析开销的二进制协议
5. **嵌入式部署**：MediaDriver + Archive + Consensus 同进程

**架构优势**：
- 无外部依赖（无需 ZooKeeper）
- 可预测的低延迟（P99 < 300μs）
- 强一致性保证（Raft 多数派）
- 灵活的快照策略（时间旅行能力）
- 完善的工具链（ClusterTool、aeron-stat）

**适用场景**：
- 金融交易系统
- 实时计费
- 游戏服务器状态同步
- 任何需要微秒级延迟 + 强一致性的分布式系统

---

## 12. 参考资料

- [Aeron Cluster Wiki](https://github.com/aeron-io/aeron/wiki/Cluster-Tutorial)
- [Raft 论文](https://raft.github.io/raft.pdf)
- [SBE 规范](https://github.com/aeron-io/simple-binary-encoding)
- 源码位置：`aeron-cluster/src/main/java/io/aeron/cluster/`
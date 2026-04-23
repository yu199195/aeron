# Aeron Cluster 启动与初始化流程源码解析

## 1. 概述

本文档深入分析 Aeron Cluster 从启动到进入工作状态的完整流程，涵盖 `ClusteredMediaDriver.launch()`、`ConsensusModule.launch()` 以及 `ClusteredServiceContainer.launch()` 的调用链与内部实现。

---

## 2. 总体启动架构

### 2.1 三组件启动顺序

```
┌──────────────────────────────────────────────────────────┐
│  1. ClusteredMediaDriver.launch()                        │
│     ├─ MediaDriver.launch()                              │
│     ├─ Archive.launch()                                  │
│     └─ ConsensusModule.launch()                          │
└──────────────────────────────────────────────────────────┘
                        ↓
┌──────────────────────────────────────────────────────────┐
│  2. ClusteredServiceContainer.launch()                   │
│     └─ 连接到 ConsensusModule，消费日志                  │
└──────────────────────────────────────────────────────────┘
```

### 2.2 进程模型

**典型部署（单进程模式）**：

```
┌────────────────────────────────────────────────────────┐
│              Java Process (Node 0)                      │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ MediaDriver  │  │   Archive    │  │  Consensus   │ │
│  │  (SHARED)    │◀─┤   (SHARED)   │◀─┤   Module     │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│         ↑                                      ↑        │
│         │                                      │        │
│  ┌──────┴──────────────────────────────────────┴─────┐ │
│  │        ClusteredServiceContainer                  │ │
│  │          (用户业务服务)                            │ │
│  └───────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

**关键点**：
- MediaDriver、Archive、ConsensusModule 共享同一线程调用链（通过 `mediaDriverAgentInvoker`）
- ClusteredServiceContainer 独立线程，通过 IPC 与 ConsensusModule 通信

---

## 3. ClusteredMediaDriver.launch() 详解

### 3.1 入口代码

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java:84`

```java
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
        // 步骤 1: 启动 MediaDriver
        driver = MediaDriver.launch(driverCtx);

        // 步骤 2: 配置 Archive 的错误处理
        final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
        final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
            archiveCtx.errorCounter() :
            new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);

        final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
            archiveCtx.errorHandler() : driverCtx.errorHandler();

        // 步骤 3: 启动 Archive（嵌入模式）
        archive = Archive.launch(archiveCtx
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .aeronDirectoryName(driver.aeronDirectoryName())
            .errorHandler(errorHandler)
            .errorCounter(errorCounter));

        // 步骤 4: 启动 ConsensusModule
        consensusModule = ConsensusModule.launch(consensusModuleCtx
            .aeronDirectoryName(driverCtx.aeronDirectoryName()));

        return new ClusteredMediaDriver(driver, archive, consensusModule);
    }
    catch (final Exception ex)
    {
        // 失败时清理所有已启动的组件
        CloseHelper.quietCloseAll(consensusModule, archive, driver);
        throw ex;
    }
}
```

### 3.2 步骤 1: MediaDriver.launch()

**职责**：启动底层传输引擎

内部流程（简化）：

```java
// MediaDriver.java:launch
public static MediaDriver launch(final Context ctx)
{
    ctx.conclude();  // 校验配置、创建目录、初始化 CnC 文件

    // 创建核心组件
    final MediaDriver driver = new MediaDriver(ctx);

    // 根据 threadingMode 启动
    if (ctx.threadingMode() == ThreadingMode.SHARED)
    {
        // SHARED 模式：创建 sharedAgentInvoker，由外部驱动
        driver.sharedInvoker = new AgentInvoker(...,
            new CompositeAgent(driver.sender, driver.receiver, driver.conductor));
        driver.sharedInvoker.start();
    }
    else
    {
        // 其他模式：启动独立线程
        AgentRunner.startOnThread(driver.senderRunner, ...);
        AgentRunner.startOnThread(driver.receiverRunner, ...);
        AgentRunner.startOnThread(driver.conductorRunner, ...);
    }

    return driver;
}
```

**关键产物**：
- `driver.sharedAgentInvoker()`：返回可由外部调用的 Invoker
- `driver.aeronDirectoryName()`：CnC 文件目录路径（如 `/dev/shm/aeron-<user>`）
- `countersValuesBuffer()`：共享内存计数器缓冲区

### 3.3 步骤 2: Archive.launch()

**职责**：启动日志与快照持久化服务

关键参数传递：

```java
archive = Archive.launch(archiveCtx
    // 与 MediaDriver 共享 Agent 调用链
    .mediaDriverAgentInvoker(driver.sharedAgentInvoker())

    // 使用同一 CnC 目录，确保 Archive 的 Aeron client 能连接到 MediaDriver
    .aeronDirectoryName(driver.aeronDirectoryName())

    // 统一错误处理
    .errorHandler(errorHandler)
    .errorCounter(errorCounter));
```

内部实现（简化）：

```java
// Archive.java:launch
public static Archive launch(final Context ctx)
{
    ctx.conclude();  // 校验：设置了 mediaDriverAgentInvoker 必须用 INVOKER 模式

    // 创建 ArchiveConductor
    final Archive archive = new Archive(ctx);

    if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
    {
        // Invoker 模式：由 ClusteredMediaDriver 主循环驱动
        archive.conductorInvoker.start();
    }
    else
    {
        // 独立线程模式
        AgentRunner.startOnThread(archive.conductorRunner, ...);
    }

    return archive;
}
```

**关键约束（Archive.Context.conclude()）**：

```java
// Archive.java:1224
if (null != mediaDriverAgentInvoker && ArchiveThreadingMode.INVOKER != threadingMode)
{
    throw new ConfigurationException(
        "Archive.Context.threadingMode(ArchiveThreadingMode.INVOKER) must be set");
}
```

**ArchiveConductor 如何驱动 MediaDriver**：

```java
// ArchiveConductor.java:doWork
public int doWork()
{
    int workCount = 0;

    // 1. 驱动 MediaDriver（如果设置了 mediaDriverAgentInvoker）
    if (null != driverAgentInvoker)
    {
        workCount += driverAgentInvoker.invoke();
    }

    // 2. 处理 Archive 自身业务
    workCount += aeronClientInvoker.invoke();
    workCount += recorder.doWork();
    workCount += replayer.doWork();
    workCount += controlSessionProxy.doWork();

    return workCount;
}
```

### 3.4 步骤 3: ConsensusModule.launch()

**职责**：启动集群共识模块

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java:1057`

```java
public static ConsensusModule launch(final Context ctx)
{
    return new ConsensusModule(ctx).start();
}

private ConsensusModule(final Context ctx)
{
    // 1. 校验配置（集群成员、通道、认证等）
    ctx.conclude();
    this.ctx = ctx;

    // 2. 创建 ConsensusModuleAgent（核心状态机）
    agent = new ConsensusModuleAgent(ctx);

    // 3. 根据 threading mode 创建 Invoker 或 Runner
    if (INVOKER == ctx.threadingMode())
    {
        invoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), agent);
    }
    else
    {
        runner = new AgentRunner(
            ctx.idleStrategy(),
            ctx.errorHandler(),
            ctx.errorCounter(),
            agent);
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

**Context.conclude() 核心逻辑**：

```java
// ConsensusModule.java:Context.conclude()
public void conclude()
{
    // 1. 解析集群成员配置
    if (null == clusterMembers)
        throw new ConfigurationException("clusterMembers is required");

    clusterMembers = ClusterMember.parse(clusterMembersString);

    // 2. 创建集群目录（存放 RecordingLog、Mark File 等）
    if (null == clusterDir)
    {
        clusterDir = new File(System.getProperty("aeron.cluster.dir",
            "aeron-cluster"));
    }
    clusterDir.mkdirs();

    // 3. 创建 Mark File（用于进程间协调与状态持久化）
    if (null == clusterMarkFile)
    {
        clusterMarkFile = new ClusterMarkFile(
            new File(clusterDir, ClusterMarkFile.FILENAME),
            ClusterComponentType.CONSENSUS_MODULE,
            errorHandler,
            epochClock,
            0);
    }

    // 4. 创建或打开 RecordingLog
    if (null == recordingLog)
    {
        recordingLog = new RecordingLog(clusterDir, true);
    }

    // 5. 创建 AeronArchive.Context（用于与 Archive 通信）
    if (null == archiveContext)
    {
        archiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveControlChannel())
            .controlResponseChannel(archiveControlResponseChannel())
            .aeronDirectoryName(aeronDirectoryName);
    }

    // 6. 创建 Authenticator 与 AuthorisationService
    if (null == authenticator)
    {
        authenticator = authenticatorSupplier.get();
    }

    // ... 其他配置
}
```

---

## 4. ConsensusModuleAgent 初始化

### 4.1 构造流程

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java`

```java
ConsensusModuleAgent(final ConsensusModule.Context ctx)
{
    this.ctx = ctx;
    this.aeron = ctx.aeron();
    this.epochClock = ctx.epochClock();
    this.clusterClock = ctx.clusterClock();
    this.cachedTimeNs = epochClock.nanoTime();
    this.clusterMembers = ClusterMember.parse(ctx.clusterMembers());
    this.sessionTimeoutNs = ctx.sessionTimeoutNs();
    this.leaderHeartbeatTimeoutNs = ctx.leaderHeartbeatTimeoutNs();

    // 加载 RecordingLog（持久化的 term 和 snapshot 记录）
    this.recordingLog = ctx.recordingLog();

    // 创建会话管理器
    this.sessionManager = new SessionManager(ctx);

    // 创建定时器服务
    this.timerService = ctx.timerServiceSupplier().newInstance(
        cachedTimeNs, clusterTimeUnit);

    // 创建日志发布器（Leader 用于发布消息到 log stream）
    this.logPublisher = new LogPublisher();

    // 创建共识发布器（用于成员间通信：投票、心跳等）
    this.consensusPublisher = new ConsensusPublisher();

    // ... 其他初始化
}
```

### 4.2 Agent 启动后的首次工作

```java
// ConsensusModuleAgent.java:onStart
public void onStart()
{
    // 1. 连接到 Archive
    archive = AeronArchive.connect(ctx.archiveContext().clone());

    // 2. 加载持久化状态（从 RecordingLog）
    recoverState();

    // 3. 如果是第一次启动或需要选举，进入选举流程
    if (需要选举)
    {
        election = new Election(...);
        election.doWork(cachedTimeNs);
    }

    // 4. 如果是恢复启动且有明确 Leader，直接进入 Follower 模式
    if (已知 Leader)
    {
        becomeFollower(leaderMemberId);
    }
}
```

### 4.3 recoverState() 详解

```java
// ConsensusModuleAgent.java:recoverState
private void recoverState()
{
    // 1. 从 RecordingLog 构建恢复计划
    final RecoveryPlan plan = recordingLog.createRecoveryPlan(archive);

    // 2. 加载最新快照（如果存在）
    if (!plan.snapshots.isEmpty())
    {
        final RecordingLog.Snapshot snapshot = plan.snapshots.get(0);

        // 2.1 恢复 ConsensusModule 状态
        loadSnapshot(snapshot.recordingId, snapshot.serviceId);

        // 2.2 更新状态变量
        leadershipTermId = snapshot.leadershipTermId;
        logPosition = snapshot.logPosition;
        termBaseLogPosition = snapshot.termBaseLogPosition;

        // 2.3 通知 Service 加载快照
        // （通过 serviceProxy 发送消息到 ClusteredServiceContainer）
    }

    // 3. 回放快照后的日志
    for (RecordingLog.Log log : plan.logs)
    {
        if (log.logPosition > logPosition)
        {
            replayLog(log.recordingId, log.logPosition);
        }
    }

    // 4. 恢复完成，初始化角色
    if (plan.hasReplay())
    {
        // 需要选举或等待 Leader
        election = new Election(...);
    }
}
```

### 4.4 loadSnapshot() 实现

```java
// ConsensusModuleAgent.java:loadSnapshot
private void loadSnapshot(final long recordingId, final int serviceId)
{
    // 1. 从 Archive 创建快照回放流
    final String channel = ctx.replayChannel();
    final int streamId = ctx.replayStreamId();

    final long length = archive.replayLength(recordingId);
    final long replaySessionId = archive.startReplay(
        recordingId,
        0,  // position
        length,
        channel,
        streamId);

    // 2. 订阅回放流
    final String replaySubscriptionChannel = ChannelUri.addSessionId(channel, replaySessionId);
    try (Subscription subscription = aeron.addSubscription(replaySubscriptionChannel, streamId))
    {
        // 3. Poll 快照数据并恢复状态
        final Image image = awaitImage(subscription, replaySessionId);

        final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(
            image, this);

        // 4. 消费快照消息
        while (true)
        {
            final int fragments = image.poll(adapter, FRAGMENT_LIMIT);
            if (fragments == 0)
            {
                if (image.isClosed() || image.isEndOfStream())
                    break;

                idleStrategy.idle();
            }
        }

        // 5. 恢复完成
        // 此时 sessionManager、timerService、clusterMembers 等已恢复
    }
}
```

**ConsensusModuleSnapshotAdapter 处理快照消息**：

```java
// ConsensusModuleSnapshotAdapter.java:onFragment
public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
{
    messageHeaderDecoder.wrap(buffer, offset);

    final int templateId = messageHeaderDecoder.templateId();

    switch (templateId)
    {
        case SnapshotMarkerDecoder.TEMPLATE_ID:
            // 快照开始/结束标记
            snapshotMarkerDecoder.wrap(buffer, offset + HEADER_LENGTH, ...);
            final long snapshotPosition = snapshotMarkerDecoder.logPosition();
            break;

        case ClusterSessionEncoder.TEMPLATE_ID:
            // 恢复客户端会话
            clusterSessionDecoder.wrap(buffer, offset + HEADER_LENGTH, ...);
            final long sessionId = clusterSessionDecoder.clusterSessionId();
            sessionManager.restoreSession(sessionId, ...);
            break;

        case TimerEncoder.TEMPLATE_ID:
            // 恢复定时器
            timerDecoder.wrap(buffer, offset + HEADER_LENGTH, ...);
            timerService.restoreTimer(correlationId, deadline);
            break;

        case ClusterMembersEncoder.TEMPLATE_ID:
            // 恢复集群成员配置
            // ...
            break;
    }
}
```

---

## 5. ClusteredServiceContainer.launch() 详解

### 5.1 入口代码

**位置**：`aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceContainer.java`

```java
public static ClusteredServiceContainer launch(final Context ctx)
{
    return new ClusteredServiceContainer(ctx).start();
}

private ClusteredServiceContainer(final Context ctx)
{
    ctx.conclude();
    this.ctx = ctx;

    // 创建 ClusteredServiceAgent
    agent = new ClusteredServiceAgent(ctx);

    // 根据 threading mode 启动
    if (INVOKER == ctx.threadingMode())
    {
        invoker = new AgentInvoker(..., agent);
    }
    else
    {
        runner = new AgentRunner(..., agent);
    }
}
```

### 5.2 Context.conclude()

```java
// ClusteredServiceContainer.Context.conclude()
public void conclude()
{
    // 1. 确保提供了 ClusteredService 实例
    if (null == clusteredService)
        throw new ConfigurationException("clusteredService must be supplied");

    // 2. 创建或连接到 Aeron
    if (null == aeron)
    {
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(aeronDirectoryName));
    }

    // 3. 创建 Mark File
    if (null == markFile)
    {
        markFile = new ClusterMarkFile(
            new File(clusterDir, ClusterMarkFile.LINK_FILENAME),
            ClusterComponentType.CONTAINER,
            ...);
    }

    // 4. 打开或创建 RecordingLog（与 ConsensusModule 共享）
    if (null == recordingLog)
    {
        recordingLog = new RecordingLog(clusterDir, false);
    }

    // ... 其他配置
}
```

### 5.3 ClusteredServiceAgent 初始化

```java
// ClusteredServiceAgent.java:构造
ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
{
    this.ctx = ctx;
    this.aeron = ctx.aeron();
    this.clusteredService = ctx.clusteredService();

    // 加载 RecordingLog
    this.recordingLog = ctx.recordingLog();

    // 创建与 ConsensusModule 的通信通道
    // ConsensusModule → Service: 通过 serviceControlChannel
    // Service → ConsensusModule: 通过 consensusModuleProxy

    // ... 初始化
}
```

### 5.4 Service 启动流程

```java
// ClusteredServiceAgent.java:onStart
public void onStart()
{
    // 1. 订阅来自 ConsensusModule 的控制消息
    serviceControlSubscription = aeron.addSubscription(
        ctx.serviceControlChannel(),
        ctx.serviceStreamId());

    // 2. 创建 ConsensusModuleProxy（用于向 ConsensusModule 发送 ACK）
    consensusModuleProxy = new ConsensusModuleProxy(
        aeron.addExclusivePublication(
            ctx.consensusModuleChannel(),
            ctx.consensusModuleStreamId()));

    // 3. 恢复状态（加载快照）
    recoverState();

    // 4. 调用用户 Service 的 onStart()
    clusteredService.onStart(cluster, snapshotImage);

    // 5. 发送 ACK 给 ConsensusModule，表示 Service 已就绪
    consensusModuleProxy.ack(
        logPosition,
        clusterClock.time(),
        0,  // ackId
        ctx.serviceId());
}
```

### 5.5 日志消费主循环

```java
// ClusteredServiceAgent.java:doWork
public int doWork()
{
    int workCount = 0;

    // 1. 处理来自 ConsensusModule 的控制消息
    workCount += serviceControlAdapter.poll();

    // 2. 消费已提交的日志消息
    if (role == Cluster.Role.LEADER || role == Cluster.Role.FOLLOWER)
    {
        final int fragments = logAdapter.poll(commitPosition);
        workCount += fragments;

        if (fragments > 0)
        {
            // 调用用户 Service 的回调（已在 logAdapter 中完成）
        }
    }

    // 3. 处理定时器到期
    workCount += timerService.poll(clusterClock.time());

    // 4. 用户后台任务
    workCount += clusteredService.doBackgroundWork(cachedTimeNs);

    return workCount;
}
```

**BoundedLogAdapter 消费日志**：

```java
// BoundedLogAdapter.java:poll
public int poll(final long commitPosition)
{
    return subscription.poll(this, commitPosition);
}

// BoundedLogAdapter.java:onFragment
public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
{
    messageHeaderDecoder.wrap(buffer, offset);

    final int templateId = messageHeaderDecoder.templateId();

    switch (templateId)
    {
        case SessionMessageHeaderDecoder.TEMPLATE_ID:
            // 客户端消息
            sessionMessageHeaderDecoder.wrap(buffer, offset + HEADER_LENGTH, ...);

            // 调用用户 Service
            clusteredService.onSessionMessage(
                session,
                timestamp,
                buffer,
                offset + SESSION_HEADER_LENGTH,
                length - SESSION_HEADER_LENGTH,
                header);
            break;

        case TimerEventDecoder.TEMPLATE_ID:
            // 定时器事件
            timerEventDecoder.wrap(buffer, offset + HEADER_LENGTH, ...);
            clusteredService.onTimerEvent(correlationId, timestamp);
            break;

        case SessionOpenEventDecoder.TEMPLATE_ID:
            // 会话打开
            clusteredService.onSessionOpen(session, timestamp);
            break;

        case SessionCloseEventDecoder.TEMPLATE_ID:
            // 会话关闭
            clusteredService.onSessionClose(session, timestamp, closeReason);
            break;

        case ClusterActionRequestDecoder.TEMPLATE_ID:
            // 集群操作（如快照）
            if (action == ClusterAction.SNAPSHOT)
            {
                takeSnapshot(timestamp);
            }
            break;
    }
}
```

---

## 6. 选举启动流程

### 6.1 Election 初始化

**触发条件**：
- 首次启动且无持久化 Leader 信息
- Leader 心跳超时

```java
// ConsensusModuleAgent.java:初始化选举
election = new Election(
    true,  // isNodeStartup
    leadershipTermId,
    commitPosition,
    appendPosition,
    clusterMembers,
    thisMember,
    consensusPublisher,
    ctx,
    this);
```

### 6.2 Election 状态机启动

```java
// Election.java:构造后立即进入 CANVASS 状态
Election(...)
{
    this.clusterMembers = clusterMembers;
    this.thisMember = thisMember;
    this.consensusPublisher = consensusPublisher;

    // 初始化为 CANVASS 状态
    state(CANVASS, ctx.epochClock().nanoTime());
}

private void state(final ElectionState newState, final long nowNs)
{
    this.state = newState;
    this.timeOfLastStateChangeNs = nowNs;

    // 记录状态变更日志
    ctx.electionStateCounter().setOrdered(newState.code());
}
```

### 6.3 CANVASS 阶段

```java
// Election.java:canvass
private int canvass(final long nowNs)
{
    int workCount = 0;

    // 向所有成员发送 CanvassPosition 消息
    if (isPassiveMember)
    {
        // Passive 成员不参与选举，只观察
        return workCount;
    }

    // 发送当前节点的日志位置给所有成员
    for (final ClusterMember member : clusterMembers)
    {
        if (member != thisMember)
        {
            consensusPublisher.canvassPosition(
                member.publication(),
                leadershipTermId,
                logPosition,
                leadershipTermId,
                thisMember.id());
        }
    }

    // 收集响应
    workCount += consensusModuleAdapter.poll();

    // 检查是否收到所有响应或超时
    if (haveAllMembersResponded() || nowNs >= nominationDeadlineNs)
    {
        // 进入 NOMINATE 阶段
        state(NOMINATE, nowNs);
    }

    return workCount;
}
```

**ConsensusModuleAdapter 处理 CanvassPosition 响应**：

```java
// ConsensusModuleAdapter.java:onCanvassPosition
void onCanvassPosition(
    final long logLeadershipTermId,
    final long logPosition,
    final long leadershipTermId,
    final int followerMemberId)
{
    // 记录该成员的日志位置
    election.onCanvassPosition(
        logLeadershipTermId,
        logPosition,
        leadershipTermId,
        followerMemberId);
}
```

### 6.4 NOMINATE 阶段

```java
// Election.java:nominate
private int nominate(final long nowNs)
{
    // 比较所有成员的日志完整性
    if (isLogUpToDate())
    {
        // 本节点日志最新，自我提名
        candidateTermId = logLeadershipTermId + 1;
        state(CANDIDATE_BALLOT, nowNs);
    }
    else
    {
        // 等待其他节点提名
        state(FOLLOWER_BALLOT, nowNs);
    }

    return 1;
}

private boolean isLogUpToDate()
{
    // 比较 (leadershipTermId, logPosition) 元组
    // Term ID 更大的更新；Term ID 相同则 position 更大的更新
    for (final ClusterMember member : clusterMembers)
    {
        if (compareLog(
            member.leadershipTermId(), member.logPosition(),
            this.leadershipTermId, this.logPosition) > 0)
        {
            return false;  // 存在更新的节点
        }
    }
    return true;
}
```

### 6.5 CANDIDATE_BALLOT 阶段

```java
// Election.java:candidateBallot
private int candidateBallot(final long nowNs)
{
    int workCount = 0;

    // 发送 RequestVote 消息
    for (final ClusterMember member : clusterMembers)
    {
        if (member != thisMember)
        {
            consensusPublisher.requestVote(
                member.publication(),
                logLeadershipTermId,
                logPosition,
                candidateTermId,
                thisMember.id());
        }
    }

    // 处理投票响应
    workCount += consensusModuleAdapter.poll();

    // 检查是否获得多数派投票
    if (haveReceivedMajorityVotes())
    {
        // 成为 Leader
        leaderMemberId = thisMember.id();
        leadershipTermId = candidateTermId;

        // 发布 NewLeadershipTerm 消息
        publishNewLeadershipTerm();

        // 进入 LEADER_INIT 状态
        state(LEADER_INIT, nowNs);
    }
    else if (nowNs >= nominationDeadlineNs || receivedHigherTermVote)
    {
        // 选举失败，重新开始
        state(CANVASS, nowNs);
    }

    return workCount;
}

private void publishNewLeadershipTerm()
{
    for (final ClusterMember member : clusterMembers)
    {
        consensusPublisher.newLeadershipTerm(
            member.publication(),
            logLeadershipTermId,
            logPosition,
            leadershipTermId,
            termBaseLogPosition,
            leadershipTermId,
            thisMember.id(),
            logSessionId,
            cluster.timeUnit(),
            appVersion);
    }
}
```

### 6.6 FOLLOWER_BALLOT 阶段

```java
// Election.java:followerBallot
private int followerBallot(final long nowNs)
{
    int workCount = 0;

    // 处理来自候选人的 RequestVote
    workCount += consensusModuleAdapter.poll();

    // 如果收到 NewLeadershipTerm，转为 FOLLOWER_CATCHUP
    if (leaderMemberId != NULL_VALUE)
    {
        state(FOLLOWER_CATCHUP_INIT, nowNs);
    }
    else if (nowNs >= nominationDeadlineNs)
    {
        // 超时，重新开始选举
        state(CANVASS, nowNs);
    }

    return workCount;
}
```

**ConsensusModuleAdapter 处理 RequestVote**：

```java
// ConsensusModuleAdapter.java:onRequestVote
void onRequestVote(
    final long logLeadershipTermId,
    final long logPosition,
    final long candidateTermId,
    final int candidateId)
{
    // 投票逻辑
    if (candidateTermId > thisMember.leadershipTermId &&
        isLogUpToDate(logLeadershipTermId, logPosition))
    {
        // 投票给该候选人
        consensusPublisher.vote(
            candidateMember.publication(),
            candidateTermId,
            logLeadershipTermId,
            logPosition,
            candidateId,
            thisMember.id(),
            true);  // granted

        // 更新本地 term
        thisMember.leadershipTermId(candidateTermId);
    }
    else
    {
        // 拒绝投票
        consensusPublisher.vote(..., false);
    }
}
```

---

## 7. Leader 初始化与日志复制启动

### 7.1 LEADER_INIT 状态

```java
// Election.java:leader (LEADER_INIT 阶段)
private int leader(final long nowNs)
{
    int workCount = 0;

    switch (state)
    {
        case LEADER_INIT:
            // 1. 创建新的 log publication
            createLogPublication();

            // 2. 开始录制日志
            startRecording();

            // 3. 向 Followers 发送 AppendPosition
            publishAppendPosition();

            // 4. 转为 LEADER_READY
            state(LEADER_READY, nowNs);
            break;

        case LEADER_READY:
            // 处理客户端请求、发送心跳
            workCount += processClientMessages();
            workCount += sendHeartbeats(nowNs);
            break;
    }

    return workCount;
}

private void createLogPublication()
{
    // 创建 log channel publication
    final String channel = ctx.logChannel();
    final int streamId = ctx.logStreamId();

    logPublication = aeron.addExclusivePublication(channel, streamId);

    // 记录 recordingId 到 RecordingLog
    recordingLog.appendTerm(
        recordingId,
        leadershipTermId,
        termBaseLogPosition,
        clusterClock.timeMillis());
}

private void startRecording()
{
    // 通过 Archive 开始录制 log stream
    archive.startRecording(
        logPublication.channel(),
        logPublication.streamId(),
        SourceLocation.LOCAL,
        true);  // autoStop

    recordingId = awaitRecordingId(logPublication.sessionId());
}
```

### 7.2 Followers 同步

**Follower 收到 NewLeadershipTerm 后**：

```java
// Election.java:follower (FOLLOWER_CATCHUP_INIT 阶段)
private int follower(final long nowNs)
{
    switch (state)
    {
        case FOLLOWER_CATCHUP_INIT:
            // 1. 订阅 Leader 的 log stream
            subscribeToLog();

            // 2. 开始录制
            startRecording();

            // 3. 转为 FOLLOWER_CATCHUP
            state(FOLLOWER_CATCHUP, nowNs);
            break;

        case FOLLOWER_CATCHUP:
            // 追赶 Leader 的日志
            if (logPosition >= leader.appendPosition)
            {
                state(FOLLOWER_READY, nowNs);
            }
            break;

        case FOLLOWER_READY:
            // 正常复制日志
            workCount += logAdapter.poll(leader.commitPosition);
            break;
    }

    return workCount;
}
```

---

## 8. 启动完成状态

### 8.1 各组件状态

**启动成功后**：

```
ConsensusModule (Leader):
  - Role: LEADER
  - Election State: LEADER_READY
  - Log Publication: active
  - Recording: started

ConsensusModule (Follower):
  - Role: FOLLOWER
  - Election State: FOLLOWER_READY
  - Log Subscription: subscribed to Leader
  - Recording: started

ClusteredServiceContainer (All nodes):
  - State: ACTIVE
  - Log Adapter: consuming committed log
  - User Service: onStart() completed
```

### 8.2 日志与监控

**关键日志输出**：

```
[Consensus] Election state: CANVASS → NOMINATE → CANDIDATE_BALLOT → LEADER_READY
[Consensus] New leadership term: termId=1, leaderMemberId=0, logPosition=0
[Consensus] Recording started: recordingId=123, streamId=100
[Service] ClusteredService.onStart() completed
[Service] ACK sent to ConsensusModule: logPosition=0, serviceId=0
[Consensus] All services ready, accepting client sessions
```

**关键计数器**：

```bash
# 使用 aeron-stat 查看
aeron-stat

# 输出示例
cluster-role: 2 (LEADER)
cluster-commit-pos: 0
cluster-append-pos: 0
cluster-leadership-term-id: 1
cluster-election-count: 1
```

---

## 9. 启动失败常见原因

### 9.1 配置错误

**症状**：启动时抛出 `ConfigurationException`

**原因**：
- `clusterMembers` 未配置或格式错误
- `mediaDriverAgentInvoker` 设置但 `threadingMode` 不是 `INVOKER`
- Archive 控制通道配置错误

**解决**：检查配置，确保格式正确

### 9.2 端口冲突

**症状**：`RegistrationException: channel already in use`

**原因**：ingress/log/replication 端口被占用

**解决**：
```bash
# 检查端口占用
netstat -an | grep 9000

# 或配置不同端口
```

### 9.3 RecordingLog 损坏

**症状**：启动时抛出 `ClusterException: invalid recording log`

**原因**：RecordingLog 文件损坏或不一致

**解决**：
```bash
# 使用 ClusterTool 验证
java -cp aeron-all.jar io.aeron.cluster.ClusterTool \
  describe \
  /path/to/cluster-dir

# 或删除 cluster-dir 重新启动（丢失数据）
```

### 9.4 选举超时

**症状**：日志显示 `Election timeout, restarting canvass`

**原因**：
- 网络延迟过高
- 节点时钟不同步
- 配置的 `electionTimeoutNs` 过短

**解决**：
```java
// 增加选举超时
ConsensusModule.Context ctx = new ConsensusModule.Context()
    .electionTimeoutNs(TimeUnit.SECONDS.toNanos(5));
```

---

## 10. 启动流程总结

### 10.1 时序图

```
Node 0 (Leader)                Node 1 (Follower)              Node 2 (Follower)
     │                              │                              │
     ├─ MediaDriver.launch()       ├─ MediaDriver.launch()       ├─ MediaDriver.launch()
     ├─ Archive.launch()            ├─ Archive.launch()            ├─ Archive.launch()
     ├─ ConsensusModule.launch()   ├─ ConsensusModule.launch()   ├─ ConsensusModule.launch()
     │   └─ recoverState()          │   └─ recoverState()          │   └─ recoverState()
     │                              │                              │
     ├─ Election: CANVASS ─────────┼─→ Election: CANVASS ─────────┼─→ Election: CANVASS
     │   (发送 CanvassPosition)     │   (响应日志位置)              │   (响应日志位置)
     │                              │                              │
     ├─ Election: NOMINATE          ├─ Election: NOMINATE          ├─ Election: NOMINATE
     │   (本节点日志最新，提名)      │   (等待提名)                  │   (等待提名)
     │                              │                              │
     ├─ CANDIDATE_BALLOT ───────────┼─→ FOLLOWER_BALLOT ───────────┼─→ FOLLOWER_BALLOT
     │   (RequestVote)              │   (Vote: granted)            │   (Vote: granted)
     │                              │                              │
     ├─ LEADER_READY ───────────────┼─→ NewLeadershipTerm ─────────┼─→ NewLeadershipTerm
     │   - 创建 log publication      │                              │
     │   - 开始录制                  │                              │
     │                              │                              │
     │                              ├─ FOLLOWER_READY             ├─ FOLLOWER_READY
     │                              │   - 订阅 log                 │   - 订阅 log
     │                              │   - 开始录制                 │   - 开始录制
     │                              │                              │
     ├─ Service.launch()            ├─ Service.launch()            ├─ Service.launch()
     │   └─ onStart()               │   └─ onStart()               │   └─ onStart()
     │                              │                              │
     ├─ 接受客户端连接               ├─ 复制日志                    ├─ 复制日志
     ▼                              ▼                              ▼
```

### 10.2 关键检查点

| 阶段 | 检查点 | 成功标志 |
|------|--------|---------|
| MediaDriver 启动 | CnC 文件创建 | `aeron-stat` 显示计数器 |
| Archive 启动 | Archive 目录创建 | `archive/recording-log.dat` 存在 |
| ConsensusModule 启动 | RecordingLog 加载 | 日志显示 `RecoveryPlan: snapshots=X, logs=Y` |
| 选举完成 | Leader 选出 | `cluster-role: 2`, `cluster-leadership-term-id: X` |
| Service 启动 | onStart() 完成 | 日志显示 `ACK sent to ConsensusModule` |
| 完全就绪 | 接受客户端连接 | 日志显示 `Cluster ready` |

---

## 11. 参考代码位置

- **ClusteredMediaDriver**: `aeron-cluster/src/main/java/io/aeron/cluster/ClusteredMediaDriver.java:32`
- **ConsensusModule**: `aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModule.java:1057`
- **ConsensusModuleAgent**: `aeron-cluster/src/main/java/io/aeron/cluster/ConsensusModuleAgent.java:121`
- **Election**: `aeron-cluster/src/main/java/io/aeron/cluster/Election.java:70`
- **RecordingLog**: `aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java:46`
- **ClusteredServiceContainer**: `aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceContainer.java`
- **ClusteredServiceAgent**: `aeron-cluster/src/main/java/io/aeron/cluster/service/ClusteredServiceAgent.java`

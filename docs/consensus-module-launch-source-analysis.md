# ConsensusModule.launch() 源码深度解析

本文档详细解析 `ConsensusModule.launch(Context ctx)` 的完整调用链，包含所有嵌套方法的中文注释和流程图。

---

## 1. 入口代码分析

### 1.1 调用入口

**位置**：`ClusteredMediaDriver.java:216`

```java
// 步骤 4: 启动 ConsensusModule
consensusModule = ConsensusModule.launch(consensusModuleCtx
    .aeronDirectoryName(driverCtx.aeronDirectoryName()));
```

**调用链路**：
```
ClusteredMediaDriver.launch()
    └─> ConsensusModule.launch(Context ctx)           [步骤 1]
            └─> new ConsensusModule(ctx)               [步骤 2]
                    ├─> ctx.conclude()                 [步骤 2.1 - 核心配置]
                    └─> new ConsensusModuleAgent(ctx)  [步骤 2.2 - 创建代理]
            └─> 启动 Agent (Runner 或 Invoker)         [步骤 3]
```

---

## 2. 完整源码解析（带中文注释）

### 2.1 ConsensusModule.launch() 方法

**位置**：`ConsensusModule.java:334-348`

```java
/**
 * 通过提供配置上下文启动 {@link ConsensusModule}。
 * <p>
 * 这是 Aeron Cluster 共识模块的主要入口点，负责：
 * <ul>
 *   <li>初始化配置（conclude()）</li>
 *   <li>创建 ConsensusModuleAgent（核心状态机）</li>
 *   <li>启动 Agent（Runner 或 Invoker 模式）</li>
 * </ul>
 *
 * @param ctx 配置参数上下文
 * @return 新创建的 {@link ConsensusModule} 实例
 */
public static ConsensusModule launch(final Context ctx)
{
    // ==================== 步骤 1: 创建 ConsensusModule 实例 ====================
    // 内部调用 new ConsensusModule(ctx)，完成配置初始化和 Agent 创建
    final ConsensusModule consensusModule = new ConsensusModule(ctx);

    // ==================== 步骤 2: 选择启动模式 ====================
    // 两种模式：
    // 1. Runner 模式：独立线程运行（生产环境）
    // 2. Invoker 模式：由外部调用者驱动（嵌入式或测试环境）

    if (null != consensusModule.conductorRunner)  // ← Runner 模式
    {
        // 在独立线程上启动 ConsensusModuleAgent
        // - threadFactory: 用于创建新线程（可自定义）
        // - conductorRunner: 包装了 Agent 的执行器
        AgentRunner.startOnThread(
            consensusModule.conductorRunner,
            ctx.threadFactory());
    }
    else  // ← Invoker 模式
    {
        // 启动 Invoker（不创建新线程）
        // 由外部通过 conductorInvoker.invoke() 驱动
        consensusModule.conductorInvoker.start();
    }

    // ==================== 步骤 3: 返回实例 ====================
    return consensusModule;
}
```

---

### 2.2 ConsensusModule 构造函数

**位置**：`ConsensusModule.java:280-316`

```java
/**
 * ConsensusModule 构造函数：初始化配置并创建核心 Agent。
 * <p>
 * 执行流程：
 * <ol>
 *   <li>调用 ctx.conclude() 完成配置初始化</li>
 *   <li>创建 ConsensusModuleAgent（核心状态机）</li>
 *   <li>根据配置选择 Runner 或 Invoker 模式</li>
 * </ol>
 *
 * @param ctx 配置上下文
 * @throws ConcurrentConcludeException 如果配置已被并发初始化
 * @throws Exception 如果初始化失败，标记 Mark File 为失败状态
 */
ConsensusModule(final Context ctx)
{
    try
    {
        // ==================== 步骤 1: 配置初始化（核心逻辑） ====================
        // conclude() 负责：
        // - 校验配置参数
        // - 创建工作目录
        // - 连接到 Aeron
        // - 初始化 RecordingLog
        // - 分配计数器
        ctx.conclude();  // ← 详见 2.3 节

        // 保存配置上下文
        this.ctx = ctx;

        // ==================== 步骤 2: 创建 ConsensusModuleAgent ====================
        // ConsensusModuleAgent 是核心状态机，负责：
        // - Raft 选举（Election）
        // - 日志复制（Log Replication）
        // - 会话管理（Session Management）
        // - 快照保存（Snapshot）
        conductor = new ConsensusModuleAgent(ctx);  // ← 详见第 3 节

        // ==================== 步骤 3: 选择运行模式 ====================
        // 根据 ctx.useAgentInvoker() 决定使用 Runner 还是 Invoker

        if (ctx.useAgentInvoker())  // ← Invoker 模式
        {
            // Invoker 模式：由外部驱动（如 ClusteredMediaDriver 共享线程）
            // - errorHandler: 错误处理器
            // - errorCounter: 错误计数器
            // - conductor: ConsensusModuleAgent 实例
            conductorInvoker = new AgentInvoker(
                ctx.errorHandler(),
                ctx.errorCounter(),
                conductor);
            conductorRunner = null;  // ← Runner 为 null
        }
        else  // ← Runner 模式（默认）
        {
            // Runner 模式：独立线程运行（生产环境推荐）
            // - idleStrategy: 空闲策略（如 BackoffIdleStrategy）
            // - errorHandler: 错误处理器
            // - errorCounter: 错误计数器
            // - conductor: ConsensusModuleAgent 实例
            conductorRunner = new AgentRunner(
                ctx.idleStrategy(),
                ctx.errorHandler(),
                ctx.errorCounter(),
                conductor);
            conductorInvoker = null;  // ← Invoker 为 null
        }
    }
    catch (final ConcurrentConcludeException ex)
    {
        // 配置已被其他线程并发初始化，直接抛出
        throw ex;
    }
    catch (final Exception ex)
    {
        // ==================== 异常处理 ====================
        // 初始化失败时，标记 Mark File 为失败状态
        final ClusterMarkFile markFile = ctx.markFile;
        if (null != markFile)
        {
            markFile.signalFailedStart();  // ← 写入失败标记，供监控工具检测
        }

        // 清理已分配的资源
        CloseHelper.quietClose(ctx::close);

        // 重新抛出异常
        throw ex;
    }
}
```

---

### 2.3 Context.conclude() 方法（核心配置初始化）

**位置**：`ConsensusModule.java:1667-2100`（简化版，完整版 400+ 行）

```java
/**
 * 完成配置初始化：在用户未提供时设置默认值，创建必要的资源。
 * <p>
 * 这是 ConsensusModule 启动前的关键步骤，负责：
 * <ul>
 *   <li>校验配置参数（如集群成员、通道配置）</li>
 *   <li>创建工作目录（cluster-dir、mark-file-dir）</li>
 *   <li>连接到 Aeron MediaDriver</li>
 *   <li>初始化 RecordingLog（持久化日志元数据）</li>
 *   <li>分配计数器（用于监控和状态追踪）</li>
 *   <li>创建 Archive 客户端（用于日志录制）</li>
 * </ul>
 * <p>
 * <b>执行顺序</b>：
 * <pre>
 * 1. 并发检查（确保只执行一次）
 * 2. 参数校验
 * 3. 目录创建
 * 4. 连接 Aeron
 * 5. 创建 RecordingLog
 * 6. 分配计数器
 * 7. 创建 Archive 客户端
 * 8. 初始化认证与授权
 * </pre>
 *
 * @throws ConcurrentConcludeException 如果 conclude() 被并发调用
 * @throws ClusterException 如果配置无效或资源创建失败
 */
@SuppressWarnings("MethodLength")
public void conclude()
{
    // ==================== 步骤 1: 并发检查 ====================
    // 使用 VarHandle 原子操作确保 conclude() 只执行一次
    // IS_CONCLUDED_VH: VarHandle 指向 isConcluded 字段
    if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
    {
        // 如果已经执行过，抛出并发异常
        throw new ConcurrentConcludeException();
    }

    // ==================== 步骤 2: 参数校验 ====================

    // 2.1 校验日志通道配置
    validateLogChannel();  // ← 确保 logChannel 格式正确

    // 2.2 校验 Service 数量
    if (serviceCount < 0 || serviceCount > MAX_SERVICE_COUNT)
    {
        throw new ClusterException(
            "service count of range [0, " + MAX_SERVICE_COUNT + "]: " + serviceCount);
    }

    // 2.3 校验集群成员配置（必需）
    if (null == clusterMembers)
    {
        // clusterMembers 格式：
        // "0,node0:20000,node0:20001,node0:20002,node0:20003,node0:8010|..."
        throw new ClusterException("ConsensusModule.Context.clusterMembers must be set");
    }

    // ==================== 步骤 3: 目录初始化 ====================

    // 3.1 创建集群工作目录
    if (null == clusterDir)
    {
        clusterDir = new File(clusterDirectoryName);
    }

    // 3.2 创建 Mark File 目录
    if (null == markFileDir)
    {
        final String dir = ClusteredServiceContainer.Configuration.markFileDir();
        markFileDir = Strings.isEmpty(dir) ? clusterDir : new File(dir);
    }

    // 3.3 获取目录的标准路径
    try
    {
        clusterDir = clusterDir.getCanonicalFile();
        clusterDirectoryName = clusterDir.getAbsolutePath();
        markFileDir = markFileDir.getCanonicalFile();

        if (Strings.isEmpty(clusterServicesDirectoryName))
        {
            clusterServicesDirectoryName = clusterDirectoryName;
        }
        else
        {
            clusterServicesDirectoryName =
                new File(clusterServicesDirectoryName).getCanonicalPath();
        }
    }
    catch (final IOException ex)
    {
        throw new UncheckedIOException(ex);
    }

    // 3.4 可选：删除旧目录（测试环境）
    if (deleteDirOnStart)
    {
        IoUtil.delete(clusterDir, false);  // ← 递归删除旧数据
    }

    // 3.5 确保目录存在
    IoUtil.ensureDirectoryExists(clusterDir, "cluster");
    IoUtil.ensureDirectoryExists(markFileDir, "mark file");

    // ==================== 步骤 4: 超时参数校验 ====================
    if (startupCanvassTimeoutNs / leaderHeartbeatTimeoutNs < 2)
    {
        throw new ClusterException(
            "startupCanvassTimeoutNs=" + startupCanvassTimeoutNs +
            " must be a multiple of leaderHeartbeatTimeoutNs=" + leaderHeartbeatTimeoutNs);
    }

    // ==================== 步骤 5: 初始化时钟 ====================

    // 5.1 集群时钟（用于确定性时间）
    if (null == clusterClock)
    {
        final String clockClassName = System.getProperty(
            CLUSTER_CLOCK_PROP_NAME, MillisecondClusterClock.class.getName());
        try
        {
            // 反射创建时钟实例
            clusterClock = (ClusterClock)Class.forName(clockClassName)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception e)
        {
            throw new ClusterException("failed to instantiate ClusterClock " + clockClassName, e);
        }
    }

    // 5.2 Epoch 时钟（用于超时计算）
    if (null == epochClock)
    {
        epochClock = SystemEpochClock.INSTANCE;
    }

    // ==================== 步骤 6: 创建 Mark File ====================
    // Mark File 用于：
    // - 进程间协调（防止多进程启动同一集群）
    // - 错误日志缓冲区
    // - 活跃性检测
    if (null == markFile)
    {
        // 获取文件页大小
        final int filePageSize = null != aeron ?
            aeron.context().filePageSize() :
            driverFilePageSize(
                new File(aeronDirectoryName),
                epochClock,
                new CommonContext().driverTimeoutMs());

        // 创建 Mark File
        markFile = new ClusterMarkFile(
            new File(markFileDir, ClusterMarkFile.FILENAME),  // ← 文件路径
            ClusterComponentType.CONSENSUS_MODULE,            // ← 组件类型
            errorBufferLength,                                // ← 错误缓冲区大小
            epochClock,                                        // ← 时钟
            ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS,  // ← 活跃超时
            filePageSize);                                    // ← 文件页大小
    }

    // 创建 Mark File 链接（供外部工具读取）
    MarkFile.ensureMarkFileLink(
        clusterDir,
        new File(markFile.parentDirectory(), ClusterMarkFile.FILENAME),
        ClusterMarkFile.LINK_FILENAME);

    // ==================== 步骤 7: 创建 NodeStateFile ====================
    // NodeStateFile 持久化节点状态（如 candidateTermId）
    if (null == nodeStateFile)
    {
        try
        {
            nodeStateFile = new NodeStateFile(
                clusterDir,
                true,  // ← 创建新文件
                fileSyncLevel());
        }
        catch (final IOException ex)
        {
            throw new ClusterException("unable to create node-state file", ex);
        }
    }

    // 迁移 candidateTermId（从 Mark File 到 NodeStateFile）
    if (Aeron.NULL_VALUE == nodeStateFile.candidateTerm().candidateTermId() &&
        Aeron.NULL_VALUE != markFile.candidateTermId())
    {
        nodeStateFile.updateCandidateTermId(
            markFile.candidateTermId(),
            Aeron.NULL_VALUE,
            epochClock.time());
    }

    // ==================== 步骤 8: 初始化错误处理 ====================

    // 8.1 创建错误日志（写入 Mark File 的错误缓冲区）
    if (null == errorLog)
    {
        errorLog = new DistinctErrorLog(
            markFile.errorBuffer(),
            epochClock,
            StandardCharsets.US_ASCII);
    }

    // 8.2 设置错误处理器
    errorHandler = CommonContext.setupErrorHandler(errorHandler, errorLog);

    // ==================== 步骤 9: 创建 RecordingLog ====================
    // RecordingLog 持久化日志元数据（term、snapshot）
    if (null == recordingLog)
    {
        recordingLog = new RecordingLog(clusterDir, true);  // ← 可写模式
    }

    // ==================== 步骤 10: 设置 Agent 角色名 ====================
    if (Strings.isEmpty(agentRoleName))
    {
        agentRoleName = "consensus-module-" + clusterId + "-" + clusterMemberId;
        // 示例："consensus-module-0-0"
    }

    // ==================== 步骤 11: 连接到 Aeron ====================
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    if (null == aeron)
    {
        ownsAeronClient = true;  // ← 标记为自己创建的 Aeron 实例

        // 创建 Aeron 客户端
        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName)      // ← CnC 目录
                .errorHandler(errorHandler)                  // ← 错误处理器
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)  // ← 订阅错误处理
                .epochClock(epochClock)                      // ← 时钟
                .useConductorAgentInvoker(true)              // ← 使用 Invoker 模式
                .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)  // ← 等待策略
                .clientLock(NoOpLock.INSTANCE)               // ← 无锁模式
                .clientName(agentRoleName));                 // ← 客户端名称

        // 分配错误计数器
        if (null == errorCounter)
        {
            errorCounter = ClusterCounters.allocateVersioned(
                aeron,
                buffer,
                "Cluster Errors",                           // ← 计数器名称
                CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID,       // ← 类型 ID
                clusterId,                                  // ← 集群 ID
                ConsensusModuleVersion.VERSION,             // ← 版本
                ConsensusModuleVersion.GIT_SHA);            // ← Git SHA
        }
    }

    // ==================== 步骤 12: 校验 Aeron 配置 ====================

    // 12.1 校验 ingress 通道
    if (null == ingressChannel)
    {
        throw new ClusterException("ingressChannel must be specified");
    }

    // 12.2 校验错误处理器类型
    if (!(aeron.context().subscriberErrorHandler() instanceof RethrowingErrorHandler))
    {
        throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
    }

    // 12.3 校验 Invoker 模式
    if (!aeron.context().useConductorAgentInvoker())
    {
        throw new ClusterException("Aeron client must use conductor agent invoker");
    }

    // 12.4 校验错误计数器
    if (null == errorCounter)
    {
        throw new ClusterException("error counter must be supplied if aeron client is");
    }

    // ==================== 步骤 13: 创建计数错误处理器 ====================
    if (null == countedErrorHandler)
    {
        countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
        if (ownsAeronClient)
        {
            aeron.context().errorHandler(countedErrorHandler);
        }
    }

    // ==================== 步骤 14: 分配所有计数器 ====================
    // 这些计数器用于监控集群状态（通过 aeron-stat 查看）

    // 14.1 ConsensusModule 状态计数器
    if (null == moduleStateCounter)
    {
        final CountersReader counters = aeron.countersReader();

        // 检查是否已有相同 clusterId 的 ConsensusModule 实例
        if (Aeron.NULL_VALUE != ClusterCounters.find(
            counters, CONSENSUS_MODULE_STATE_TYPE_ID, clusterId))
        {
            throw new ClusterException(
                "existing consensus module detected for clusterId=" + clusterId);
        }

        // 分配状态计数器
        moduleStateCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Consensus Module state",
            CONSENSUS_MODULE_STATE_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, moduleStateCounter, CONSENSUS_MODULE_STATE_TYPE_ID);

    // 14.2 选举状态计数器
    if (null == electionStateCounter)
    {
        electionStateCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster election state",
            ELECTION_STATE_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, electionStateCounter, ELECTION_STATE_TYPE_ID);

    // 14.3 选举次数计数器
    if (null == electionCounter)
    {
        electionCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster election count",
            CLUSTER_ELECTION_COUNT_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, electionCounter, CLUSTER_ELECTION_COUNT_TYPE_ID);

    // 14.4 Leadership Term ID 计数器
    if (null == leadershipTermId)
    {
        leadershipTermId = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster leadership term id",
            CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, leadershipTermId, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);

    // 14.5 集群节点角色计数器
    if (null == clusterNodeRoleCounter)
    {
        clusterNodeRoleCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster node role",
            CLUSTER_NODE_ROLE_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, clusterNodeRoleCounter, CLUSTER_NODE_ROLE_TYPE_ID);

    // 14.6 提交位置计数器
    if (null == commitPosition)
    {
        commitPosition = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster commit-pos:",
            COMMIT_POSITION_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, commitPosition, COMMIT_POSITION_TYPE_ID);

    // 14.7 集群控制开关计数器
    if (null == clusterControlToggle)
    {
        clusterControlToggle = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster control toggle",
            CONTROL_TOGGLE_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, clusterControlToggle, CONTROL_TOGGLE_TYPE_ID);

    // 14.8 节点控制开关计数器
    if (null == nodeControlToggle)
    {
        nodeControlToggle = ClusterCounters.allocate(
            aeron,
            buffer,
            "Node control toggle",
            NODE_CONTROL_TOGGLE_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, nodeControlToggle, NODE_CONTROL_TOGGLE_TYPE_ID);

    // 14.9 快照计数器
    if (null == snapshotCounter)
    {
        snapshotCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster snapshot count",
            SNAPSHOT_COUNTER_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, snapshotCounter, SNAPSHOT_COUNTER_TYPE_ID);

    // 14.10 超时客户端计数器
    if (null == timedOutClientCounter)
    {
        timedOutClientCounter = ClusterCounters.allocate(
            aeron,
            buffer,
            "Cluster timed out client count",
            CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID,
            clusterId);
    }
    validateCounterTypeId(aeron, timedOutClientCounter, CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID);

    // ==================== 步骤 15: 创建 AeronArchive 上下文 ====================
    // AeronArchive 用于日志录制与回放
    if (null == archiveContext)
    {
        archiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveControlChannel())          // ← Archive 控制通道
            .controlResponseChannel(archiveControlResponseChannel()) // ← Archive 响应通道
            .controlRequestStreamId(ctx.archiveControlStreamId())    // ← 控制流 ID
            .aeronDirectoryName(aeronDirectoryName);                 // ← CnC 目录
    }

    // ==================== 步骤 16: 初始化认证与授权 ====================

    // 16.1 创建 Authenticator
    if (null == authenticator)
    {
        authenticator = authenticatorSupplier.get();
    }

    // 16.2 创建 AuthorisationService
    if (null == authorisationServiceSupplier)
    {
        authorisationServiceSupplier = AuthorisationService.ALLOW_ALL_SUPPLIER;
    }

    // ==================== 步骤 17: 其他初始化（省略） ====================
    // - ingressEndpoints
    // - replicationChannel
    // - idleStrategy
    // - ...

    // conclude() 完成！所有配置已初始化，可以创建 ConsensusModuleAgent
}
```

---

## 3. ConsensusModuleAgent 创建流程

**位置**：`ConsensusModuleAgent.java:121` (构造函数)

```java
/**
 * ConsensusModuleAgent 构造函数：创建集群共识模块的核心状态机。
 * <p>
 * 主要职责：
 * <ul>
 *   <li>初始化 Aeron 通信组件（Publication、Subscription）</li>
 *   <li>连接到 Archive（用于日志录制）</li>
 *   <li>加载 RecordingLog（恢复持久化状态）</li>
 *   <li>创建会话管理器（SessionManager）</li>
 *   <li>创建定时器服务（TimerService）</li>
 *   <li>创建日志发布器（LogPublisher）</li>
 *   <li>根据持久化状态决定启动选举或恢复日志</li>
 * </ul>
 *
 * @param ctx ConsensusModule 配置上下文（已完成 conclude()）
 */
ConsensusModuleAgent(final ConsensusModule.Context ctx)
{
    // ==================== 步骤 1: 保存配置 ====================
    this.ctx = ctx;
    this.aeron = ctx.aeron();
    this.epochClock = ctx.epochClock();
    this.clusterClock = ctx.clusterClock();
    this.cachedTimeNs = epochClock.nanoTime();
    this.clusterMembers = ClusterMember.parse(ctx.clusterMembers());
    this.sessionTimeoutNs = ctx.sessionTimeoutNs();
    this.leaderHeartbeatTimeoutNs = ctx.leaderHeartbeatTimeoutNs();

    // ==================== 步骤 2: 加载 RecordingLog ====================
    // RecordingLog 持久化了日志元数据（term、snapshot）
    this.recordingLog = ctx.recordingLog();

    // ==================== 步骤 3: 创建会话管理器 ====================
    // SessionManager 负责：
    // - 管理客户端会话生命周期
    // - 分配 sessionId
    // - 处理会话超时
    this.sessionManager = new SessionManager(ctx);

    // ==================== 步骤 4: 创建定时器服务 ====================
    // TimerService 负责：
    // - 调度定时器（通过 cluster.scheduleTimer()）
    // - 触发定时器到期事件
    this.timerService = ctx.timerServiceSupplier().newInstance(
        cachedTimeNs,
        clusterTimeUnit);

    // ==================== 步骤 5: 创建日志发布器 ====================
    // LogPublisher 负责：
    // - 将客户端消息序列化到日志流
    // - 发布集群事件（如 NewLeadershipTerm）
    this.logPublisher = new LogPublisher();

    // ==================== 步骤 6: 创建共识发布器 ====================
    // ConsensusPublisher 负责：
    // - 发送选举消息（RequestVote、Vote）
    // - 发送心跳消息（NewLeadershipTerm、CommitPosition）
    this.consensusPublisher = new ConsensusPublisher();

    // ==================== 步骤 7: 连接到 Archive ====================
    // 后续在 onStart() 中执行，此处仅初始化字段

    // ... 其他初始化 ...
}
```

**Agent 启动后的 onStart() 流程**：

```java
/**
 * Agent 启动回调：在 Agent 线程启动后调用一次。
 * <p>
 * 执行流程：
 * <ol>
 *   <li>连接到 Archive</li>
 *   <li>加载 RecordingLog 恢复持久化状态</li>
 *   <li>根据状态决定启动选举或恢复日志</li>
 *   <li>创建 Ingress Publication（接收客户端请求）</li>
 * </ol>
 */
public void onStart()
{
    // ==================== 步骤 1: 连接到 Archive ====================
    archive = AeronArchive.connect(ctx.archiveContext().clone());

    // ==================== 步骤 2: 加载持久化状态 ====================
    recoverState();  // ← 从 RecordingLog 加载 term 和 snapshot

    // ==================== 步骤 3: 决定启动模式 ====================
    if (需要选举)
    {
        // 启动选举流程
        election = new Election(...);
        election.doWork(cachedTimeNs);
    }
    else if (已知 Leader)
    {
        // 直接进入 Follower 模式
        becomeFollower(leaderMemberId);
    }

    // ==================== 步骤 4: 创建 Ingress Publication ====================
    // 用于接收客户端请求
    ingressAdapter = new IngressAdapter(
        aeron.addSubscription(ctx.ingressChannel(), ctx.ingressStreamId()),
        sessionManager,
        this);

    // onStart() 完成！Agent 开始执行 doWork() 主循环
}
```

---

## 4. 完整流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                  ConsensusModule.launch(ctx) 完整流程                │
└─────────────────────────────────────────────────────────────────────┘

入口：ClusteredMediaDriver.launch()
    │
    ├─> consensusModule = ConsensusModule.launch(consensusModuleCtx
    │       .aeronDirectoryName(driverCtx.aeronDirectoryName()));
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 1: ConsensusModule.launch(Context ctx)                       │
│ 位置: ConsensusModule.java:334                                    │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> final ConsensusModule consensusModule = new ConsensusModule(ctx);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2: new ConsensusModule(Context ctx)                          │
│ 位置: ConsensusModule.java:280                                    │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> ctx.conclude();  ← 核心配置初始化
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.1: Context.conclude()                                      │
│ 位置: ConsensusModule.java:1667                                   │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 并发检查（IS_CONCLUDED_VH.getAndSet）                       │
│ ├─ 2. 参数校验（clusterMembers、serviceCount、logChannel）        │
│ ├─ 3. 创建目录（clusterDir、markFileDir）                         │
│ ├─ 4. 创建 ClusterMarkFile（进程协调 + 错误缓冲区）               │
│ ├─ 5. 创建 NodeStateFile（持久化 candidateTermId）                │
│ ├─ 6. 初始化错误处理（errorLog、errorHandler）                    │
│ ├─ 7. 创建 RecordingLog（持久化 term 和 snapshot 元数据）         │
│ ├─ 8. 连接到 Aeron MediaDriver                                    │
│ │      └─> aeron = Aeron.connect(                                 │
│ │               aeronDirectoryName,                               │
│ │               useConductorAgentInvoker(true))                   │
│ ├─ 9. 分配计数器（moduleState、electionState、commitPosition...） │
│ ├─10. 创建 AeronArchive.Context（Archive 控制通道）               │
│ ├─11. 初始化认证与授权（Authenticator、AuthorisationService）     │
│ └─12. 其他配置（idleStrategy、threadFactory...）                  │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> conductor = new ConsensusModuleAgent(ctx);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.2: new ConsensusModuleAgent(Context ctx)                   │
│ 位置: ConsensusModuleAgent.java:121                               │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 保存配置（aeron、epochClock、clusterMembers...）            │
│ ├─ 2. 加载 RecordingLog（recordingLog = ctx.recordingLog()）      │
│ ├─ 3. 创建 SessionManager（管理客户端会话）                       │
│ ├─ 4. 创建 TimerService（定时器调度）                             │
│ ├─ 5. 创建 LogPublisher（日志序列化）                             │
│ ├─ 6. 创建 ConsensusPublisher（选举消息）                         │
│ └─ 7. 初始化字段（archive 在 onStart() 中连接）                   │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> 根据配置选择运行模式：
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.3: 创建 Runner 或 Invoker                                  │
│ 位置: ConsensusModule.java:289-299                                │
│                                                                   │
│ if (ctx.useAgentInvoker())  ← Invoker 模式                        │
│ {                                                                 │
│     conductorInvoker = new AgentInvoker(                          │
│         ctx.errorHandler(),                                       │
│         ctx.errorCounter(),                                       │
│         conductor);                                               │
│ }                                                                 │
│ else  ← Runner 模式（默认）                                        │
│ {                                                                 │
│     conductorRunner = new AgentRunner(                            │
│         ctx.idleStrategy(),                                       │
│         ctx.errorHandler(),                                       │
│         ctx.errorCounter(),                                       │
│         conductor);                                               │
│ }                                                                 │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> return consensusModule;
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 3: 启动 Agent                                                 │
│ 位置: ConsensusModule.java:338-345                                │
│                                                                   │
│ if (null != consensusModule.conductorRunner)  ← Runner 模式        │
│ {                                                                 │
│     // 在新线程上启动 Agent                                        │
│     AgentRunner.startOnThread(                                    │
│         consensusModule.conductorRunner,                          │
│         ctx.threadFactory());                                     │
│ }                                                                 │
│ else  ← Invoker 模式                                               │
│ {                                                                 │
│     // 启动 Invoker（由外部驱动）                                  │
│     consensusModule.conductorInvoker.start();                     │
│ }                                                                 │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> Agent 线程启动后调用 conductor.onStart()
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 4: ConsensusModuleAgent.onStart()                            │
│ 位置: ConsensusModuleAgent.java:onStart                           │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 连接到 Archive                                              │
│ │      └─> archive = AeronArchive.connect(ctx.archiveContext())  │
│ ├─ 2. 恢复持久化状态                                               │
│ │      └─> recoverState()                                        │
│ │          ├─> 加载最新快照（如果存在）                            │
│ │          └─> 重放快照后的日志（如果存在）                        │
│ ├─ 3. 决定启动模式                                                 │
│ │      ├─> if (需要选举) → 启动 Election                          │
│ │      └─> else if (已知 Leader) → 进入 Follower 模式             │
│ └─ 4. 创建 Ingress Adapter（接收客户端请求）                       │
│        └─> ingressAdapter = new IngressAdapter(...)               │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> Agent 开始执行主循环：conductor.doWork()
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 5: Agent 主循环                                               │
│ 位置: ConsensusModuleAgent.java:doWork                            │
│                                                                   │
│ while (running)                                                   │
│ {                                                                 │
│     int workCount = 0;                                            │
│                                                                   │
│     // 1. 执行选举逻辑（如果正在选举）                              │
│     if (null != election)                                         │
│         workCount += election.doWork(nowNs);                      │
│                                                                   │
│     // 2. 处理客户端请求（Leader）                                 │
│     if (role == LEADER && state == ACTIVE)                        │
│         workCount += ingressAdapter.poll();                       │
│                                                                   │
│     // 3. 复制日志（Leader）                                       │
│     if (role == LEADER)                                           │
│         workCount += updateLeaderPosition(nowNs);                 │
│                                                                   │
│     // 4. 消费日志（Follower）                                     │
│     if (role == FOLLOWER)                                         │
│         workCount += logAdapter.poll(commitPosition);             │
│                                                                   │
│     // 5. 处理集群间通信（选举、心跳）                              │
│     workCount += consensusModuleAdapter.poll();                   │
│                                                                   │
│     // 6. Idle 策略                                                │
│     if (0 == workCount)                                           │
│         idleStrategy.idle();                                      │
│ }                                                                 │
└───────────────────────────────────────────────────────────────────┘
    │
    └─> ConsensusModule 运行中！
```

---

## 5. 关键组件说明

### 5.1 Context.conclude() 核心职责

| 步骤 | 职责 | 输出 |
|------|------|------|
| 1. 并发检查 | 确保 conclude() 只执行一次 | - |
| 2. 参数校验 | 校验 clusterMembers、serviceCount 等 | 抛出异常或继续 |
| 3. 目录创建 | 创建 clusterDir、markFileDir | 文件系统目录 |
| 4. Mark File | 创建 ClusterMarkFile | 进程协调文件 |
| 5. NodeStateFile | 创建节点状态文件 | 持久化 candidateTermId |
| 6. 错误处理 | 创建 errorLog、errorHandler | 错误日志系统 |
| 7. RecordingLog | 加载日志元数据 | RecordingLog 实例 |
| 8. 连接 Aeron | 连接到 MediaDriver | Aeron 客户端 |
| 9. 分配计数器 | 创建所有监控计数器 | 13+ 个计数器 |
| 10. Archive Context | 创建 AeronArchive.Context | Archive 控制通道配置 |
| 11. 认证授权 | 创建 Authenticator、AuthorisationService | 安全组件 |

### 5.2 ConsensusModuleAgent 核心职责

| 组件 | 职责 |
|------|------|
| **SessionManager** | 管理客户端会话生命周期 |
| **TimerService** | 定时器调度与触发 |
| **LogPublisher** | 将消息序列化到日志流 |
| **ConsensusPublisher** | 发送选举与心跳消息 |
| **Election** | Raft 选举流程 |
| **IngressAdapter** | 接收客户端请求 |
| **LogAdapter** | 消费日志（Follower） |

### 5.3 Runner vs Invoker 模式

| 模式 | 线程模型 | 适用场景 |
|------|---------|---------|
| **Runner** | 独立线程 | 生产环境（默认） |
| **Invoker** | 由外部驱动 | 嵌入式、共享线程（如 ClusteredMediaDriver） |

**Runner 模式**：
```java
AgentRunner.startOnThread(conductorRunner, threadFactory);
// 创建新线程，执行：
// while (running) {
//     conductor.doWork();
//     idleStrategy.idle();
// }
```

**Invoker 模式**：
```java
conductorInvoker.start();
// 由外部调用：
// conductorInvoker.invoke();  // ← 执行一次 doWork()
```

---

## 6. 总结

### 6.1 启动流程总结

```
1. ConsensusModule.launch(ctx)
   └─> 2. new ConsensusModule(ctx)
           ├─> 2.1 ctx.conclude()          ← 初始化所有配置
           ├─> 2.2 new ConsensusModuleAgent(ctx)  ← 创建核心状态机
           └─> 2.3 创建 Runner/Invoker     ← 选择运行模式
   └─> 3. 启动 Agent
           └─> 4. onStart()                ← 连接 Archive、恢复状态
                   └─> 5. doWork() 主循环   ← 处理选举、日志、会话
```

### 6.2 关键要点

1. **conclude() 是配置初始化的核心**
   - 创建所有必要的目录和文件
   - 连接到 Aeron 和 Archive
   - 分配所有监控计数器

2. **ConsensusModuleAgent 是状态机核心**
   - 管理选举、日志复制、会话
   - 通过 doWork() 主循环驱动所有逻辑

3. **Runner vs Invoker 决定线程模型**
   - Runner：独立线程（生产环境）
   - Invoker：共享线程（嵌入式）

4. **onStart() 恢复持久化状态**
   - 从 RecordingLog 加载 term 和 snapshot
   - 决定启动选举或进入 Follower 模式

---

## 7. 源码位置索引

| 方法/类 | 位置 | 行号 |
|---------|------|------|
| `ConsensusModule.launch()` | `ConsensusModule.java` | 334-348 |
| `ConsensusModule(Context)` | `ConsensusModule.java` | 280-316 |
| `Context.conclude()` | `ConsensusModule.java` | 1667-2100 |
| `ConsensusModuleAgent(Context)` | `ConsensusModuleAgent.java` | 121 |
| `ConsensusModuleAgent.onStart()` | `ConsensusModuleAgent.java` | onStart |
| `ConsensusModuleAgent.doWork()` | `ConsensusModuleAgent.java` | doWork |

---

**完整流程总耗时**：通常 < 100ms（无数据恢复）
**主要耗时点**：
- Archive 连接：10-50ms
- RecordingLog 加载：1-10ms（取决于 term 数量）
- 快照加载：10ms - 5s（取决于快照大小）

# ClusteredServiceContainer.launch() 源码深度解析

本文档详细解析 `ClusteredServiceContainer.launch(serviceContainerContext)` 的完整调用链，包含所有嵌套方法的中文注释和流程图。

---

## 1. 入口代码分析

### 1.1 调用入口

**位置**：`ClusterNode.java:190`

```java
// 步骤 5: 启动 ClusteredServiceContainer
container = ClusteredServiceContainer.launch(serviceContainerContext);
```

**调用链路**：
```
ClusterNode.main()
    └─> ClusteredServiceContainer.launch(Context ctx)           [步骤 1]
            └─> new ClusteredServiceContainer(ctx)               [步骤 2]
                    ├─> ctx.conclude()                           [步骤 2.1 - 核心配置]
                    └─> new ClusteredServiceAgent(ctx)           [步骤 2.2 - 创建代理]
            └─> AgentRunner.startOnThread()                      [步骤 3]
```

---

## 2. 完整源码解析（带中文注释）

### 2.1 ClusteredServiceContainer.launch() 方法

**位置**：`ClusteredServiceContainer.java:150-156`

```java
/**
 * 通过提供配置上下文启动 {@link ClusteredServiceContainer}。
 * <p>
 * 这是 Aeron Cluster 业务服务容器的主要入口点，负责：
 * <ul>
 *   <li>初始化配置（调用 {@link Context#conclude()}）</li>
 *   <li>创建 {@link ClusteredServiceAgent}（Service 业务逻辑执行者）</li>
 *   <li>启动 Agent（在独立线程上运行）</li>
 * </ul>
 * <p>
 * <b>启动流程</b>：
 * <pre>
 * 1. 调用构造函数 new ClusteredServiceContainer(ctx)
 *    ├─ ctx.conclude()：完成配置初始化
 *    │   ├─ 创建工作目录（cluster-dir、mark-file-dir）
 *    │   ├─ 连接到 Aeron MediaDriver
 *    │   ├─ 分配计数器（用于监控）
 *    │   └─ 创建 Archive 客户端
 *    └─ new ClusteredServiceAgent(ctx)：创建 Service 代理
 *
 * 2. 启动 Agent（Runner 模式）
 *    └─ AgentRunner.startOnThread() - 在独立线程上运行
 * </pre>
 * <p>
 * <b>与 ConsensusModule 的关系</b>：
 * <ul>
 *   <li><b>ConsensusModule</b>：负责 Raft 共识、日志复制、会话管理</li>
 *   <li><b>ClusteredServiceContainer</b>：负责运行业务逻辑（ClusteredService）</li>
 *   <li><b>通信方式</b>：ConsensusModule 通过 serviceControlPublication 向 Service 发送命令</li>
 * </ul>
 *
 * @param ctx 配置参数上下文（必须包含 clusteredService、clusterId、serviceId 等）
 * @return 新创建并已启动的 {@link ClusteredServiceContainer} 实例
 * @throws ClusterException 如果配置无效或启动失败
 */
public static ClusteredServiceContainer launch(final Context ctx)
{
    // ==================== 步骤 1: 创建 ClusteredServiceContainer 实例 ====================
    // 内部调用 new ClusteredServiceContainer(ctx)，完成以下工作：
    // 1. ctx.conclude()：初始化所有配置
    // 2. new ClusteredServiceAgent(ctx)：创建 Service 代理
    // 3. 创建 AgentRunner（准备在独立线程上运行）
    final ClusteredServiceContainer clusteredServiceContainer = new ClusteredServiceContainer(ctx);

    // ==================== 步骤 2: 启动 Agent ====================
    // 在新线程上启动 ClusteredServiceAgent
    // - threadFactory: 用于创建新线程（可自定义线程名、优先级等）
    // - serviceAgentRunner: 包装了 Agent 的执行器，持续调用 doWork()
    // 线程启动后会执行：
    // while (running) {
    //     agent.doWork();  // ← ClusteredServiceAgent.doWork()
    //     idleStrategy.idle(); // ← 无工作时的空闲策略
    // }
    AgentRunner.startOnThread(clusteredServiceContainer.serviceAgentRunner, ctx.threadFactory());

    // ==================== 步骤 3: 返回实例 ====================
    // 此时 ClusteredServiceContainer 已经启动：
    // - Agent 开始执行 onStart()（连接到 ConsensusModule、加载快照）
    // - 随后进入主循环 doWork()（接收命令、执行业务逻辑）
    return clusteredServiceContainer;
}
```

---

### 2.2 ClusteredServiceContainer 构造函数

**位置**：`ClusteredServiceContainer.java:110-132`

```java
/**
 * ClusteredServiceContainer 构造函数：初始化配置并创建 Service Agent。
 * <p>
 * 执行流程：
 * <ol>
 *   <li>调用 ctx.conclude() 完成配置初始化</li>
 *   <li>创建 ClusteredServiceAgent（Service 业务逻辑执行者）</li>
 *   <li>创建 AgentRunner（准备在独立线程上运行）</li>
 * </ol>
 *
 * @param ctx 配置上下文
 * @throws ConcurrentConcludeException 如果配置已被并发初始化
 * @throws Exception 如果初始化失败，标记 Mark File 为失败状态
 */
private ClusteredServiceContainer(final Context ctx)
{
    // 保存配置上下文
    this.ctx = ctx;

    try
    {
        // ==================== 步骤 1: 配置初始化（核心逻辑） ====================
        // conclude() 负责：
        // - 校验配置参数（serviceId、clusterId 等）
        // - 创建工作目录
        // - 连接到 Aeron
        // - 分配计数器
        ctx.conclude();  // ← 详见 2.3 节

        // ==================== 步骤 2: 创建 ClusteredServiceAgent ====================
        // ClusteredServiceAgent 是 Service 的核心代理，负责：
        // - 接收 ConsensusModule 的命令（通过 serviceControlSubscription）
        // - 消费日志（调用 ClusteredService.onSessionMessage()）
        // - 保存/恢复快照（调用 ClusteredService.onTakeSnapshot()/onStart()）
        // - 管理 Service 生命周期
        final ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);

        // ==================== 步骤 3: 创建 AgentRunner ====================
        // Runner 模式：独立线程运行（ClusteredServiceContainer 始终使用 Runner 模式）
        // - idleStrategy: 空闲策略（如 BackoffIdleStrategy，无工作时降低 CPU 占用）
        // - errorHandler: 错误处理器（记录到 Mark File 错误缓冲区）
        // - errorCounter: 错误计数器（用于监控）
        // - agent: ClusteredServiceAgent 实例
        serviceAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), agent);
    }
    catch (final Exception ex)
    {
        // ==================== 异常处理 ====================
        // 初始化失败时，标记 Mark File 为失败状态
        final ClusterMarkFile markFile = ctx.markFile;
        if (null != markFile)
        {
            // 写入失败标记，供监控工具检测（如 aeron-stat）
            // 这样管理员可以通过 Mark File 知道 Service 启动失败的原因
            markFile.signalFailedStart();
        }

        // 清理已分配的资源（关闭 Aeron 连接等）
        ctx.close();

        // 重新抛出异常，让调用者知道启动失败
        throw ex;
    }
}
```

---

### 2.3 Context.conclude() 方法（核心配置初始化）

**位置**：`ClusteredServiceContainer.java:823-1070`（简化版，完整版 250+ 行）

```java
/**
 * 完成配置初始化：在用户未提供时设置默认值，创建必要的资源。
 * <p>
 * 这是 ClusteredServiceContainer 启动前的关键步骤，负责：
 * <ul>
 *   <li>校验配置参数（如 serviceId、clusterId）</li>
 *   <li>创建工作目录（cluster-dir、mark-file-dir）</li>
 *   <li>连接到 Aeron MediaDriver</li>
 *   <li>分配计数器（用于监控和状态追踪）</li>
 *   <li>创建 Archive 客户端（用于日志回放和快照加载）</li>
 * </ul>
 * <p>
 * <b>执行顺序</b>：
 * <pre>
 * 1. 并发检查（确保只执行一次）
 * 2. 参数校验（serviceId、clusterId）
 * 3. 目录创建（clusterDir、markFileDir）
 * 4. 时钟初始化（epochClock、nanoClock）
 * 5. Mark File 创建（进程协调、错误缓冲区）
 * 6. 错误处理初始化（errorLog、errorHandler）
 * 7. 连接 Aeron（创建 Aeron 客户端）
 * 8. 分配所有计数器（errorCounter、dutyCycleTracker 等）
 * 9. 创建 AeronArchive.Context（Archive 控制通道）
 * </pre>
 * <p>
 * <b>重要约束</b>：
 * <ul>
 *   <li>此方法只能被调用一次（通过 VarHandle 原子操作保证）</li>
 *   <li>必须在 ClusteredServiceContainer 构造函数中调用</li>
 *   <li>如果配置无效或资源创建失败，会抛出异常</li>
 * </ul>
 *
 * @throws ConcurrentConcludeException 如果 conclude() 被并发调用
 * @throws ClusterException 如果配置无效或资源创建失败
 * @throws UncheckedIOException 如果目录操作失败
 */
public void conclude()
{
    // ==================== 步骤 1: 并发检查 ====================
    // 使用 VarHandle 原子操作确保 conclude() 只执行一次
    // IS_CONCLUDED_VH: VarHandle 指向 isConcluded 字段
    if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
    {
        // 如果已经执行过（旧值为 true），抛出并发异常
        throw new ConcurrentConcludeException();
    }

    // ==================== 步骤 2: 参数校验 ====================

    // 2.1 校验 Service ID（必须在 [0, MAX_SERVICE_COUNT-1] 范围内）
    final int maxId = MAX_SERVICE_COUNT - 1;
    if (serviceId < 0 || serviceId > maxId)
    {
        throw new ConfigurationException("service id outside allowed range [0," + maxId + "]: " + serviceId);
    }

    // ==================== 步骤 3: 初始化工厂和策略 ====================

    // 3.1 线程工厂（用于创建 Agent 线程）
    if (null == threadFactory)
    {
        threadFactory = Thread::new;
    }

    // 3.2 空闲策略（无工作时降低 CPU 占用）
    if (null == idleStrategySupplier)
    {
        idleStrategySupplier = Configuration.idleStrategySupplier(null);
    }

    // 3.3 应用版本校验器（用于检查集群节点版本兼容性）
    if (null == appVersionValidator)
    {
        appVersionValidator = AppVersionValidator.SEMANTIC_VERSIONING_VALIDATOR;
    }

    // 3.4 时钟初始化
    if (null == epochClock)
    {
        epochClock = SystemEpochClock.INSTANCE;  // ← 用于超时计算、Mark File 活跃性检测
    }

    if (null == nanoClock)
    {
        nanoClock = SystemNanoClock.INSTANCE;  // ← 用于高精度时间戳
    }

    // ==================== 步骤 4: 目录初始化 ====================

    // 4.1 创建集群工作目录
    if (null == clusterDir)
    {
        clusterDir = new File(clusterDirectoryName);
    }

    // 4.2 创建 Mark File 目录
    if (null == markFileDir)
    {
        final String dir = Configuration.markFileDir();
        markFileDir = Strings.isEmpty(dir) ? clusterDir : new File(dir);
    }

    // 4.3 获取目录的标准路径（绝对路径）
    try
    {
        clusterDir = clusterDir.getCanonicalFile();
        clusterDirectoryName = clusterDir.getAbsolutePath();
        markFileDir = markFileDir.getCanonicalFile();
    }
    catch (final IOException e)
    {
        throw new UncheckedIOException(e);
    }

    // 4.4 确保目录存在
    IoUtil.ensureDirectoryExists(clusterDir, "cluster");
    IoUtil.ensureDirectoryExists(markFileDir, "mark file");

    // ==================== 步骤 5: 创建 Mark File ====================
    // Mark File 用于：
    // - 进程间协调（防止多进程启动同一 Service）
    // - 错误日志缓冲区（保存最近的错误信息）
    // - 活跃性检测（由外部监控工具读取）
    if (null == markFile)
    {
        // 获取文件页大小（用于内存映射文件）
        final int filePageSize = null != aeron ? aeron.context().filePageSize() :
            driverFilePageSize(new File(aeronDirectoryName), epochClock, new CommonContext().driverTimeoutMs());

        // 创建 Mark File（每个 Service 有独立的 Mark File）
        // 文件名：cluster-mark-service-{serviceId}.dat
        markFile = new ClusterMarkFile(
            new File(markFileDir, ClusterMarkFile.markFilenameForService(serviceId)),  // ← 文件路径
            ClusterComponentType.CONTAINER,                                            // ← 组件类型
            errorBufferLength,                                                         // ← 错误缓冲区大小
            epochClock,                                                                 // ← 时钟
            LIVENESS_TIMEOUT_MS,                                                       // ← 活跃超时（10 秒）
            filePageSize);                                                              // ← 文件页大小
    }

    // 创建 Mark File 链接（供外部工具读取）
    MarkFile.ensureMarkFileLink(
        clusterDir,
        new File(markFile.parentDirectory(), ClusterMarkFile.markFilenameForService(serviceId)),
        ClusterMarkFile.linkFilenameForService(serviceId));

    // ==================== 步骤 6: 初始化错误处理 ====================

    // 6.1 创建错误日志（写入 Mark File 的错误缓冲区）
    if (null == errorLog)
    {
        errorLog = new DistinctErrorLog(
            markFile.errorBuffer(),  // ← 错误缓冲区（内存映射）
            epochClock,              // ← 时钟（用于错误时间戳）
            US_ASCII);               // ← 字符编码
    }

    // 6.2 设置错误处理器（如果用户未提供，使用默认的打印到 errorLog）
    errorHandler = CommonContext.setupErrorHandler(this.errorHandler, errorLog);

    // 6.3 创建委托错误处理器（支持链式错误处理）
    if (null == delegatingErrorHandler)
    {
        delegatingErrorHandler = Configuration.newDelegatingErrorHandler();
        if (null != delegatingErrorHandler)
        {
            delegatingErrorHandler.next(errorHandler);
            errorHandler = delegatingErrorHandler;
        }
    }
    else
    {
        delegatingErrorHandler.next(errorHandler);
        errorHandler = delegatingErrorHandler;
    }

    // ==================== 步骤 7: 设置 Service 名称 ====================
    if (Strings.isEmpty(serviceName))
    {
        // Service 名称格式：clustered-service-{clusterId}-{serviceId}
        // 示例：clustered-service-0-0
        serviceName = "clustered-service-" + clusterId + "-" + serviceId;
    }

    // ==================== 步骤 8: 连接到 Aeron ====================
    if (null == aeron)
    {
        ownsAeronClient = true;  // ← 标记为自己创建的 Aeron 实例

        // 创建 Aeron 客户端
        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName)              // ← CnC 目录
                .errorHandler(errorHandler)                          // ← 错误处理器
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE)  // ← 订阅错误处理
                .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE) // ← 等待策略
                .epochClock(epochClock)                              // ← 时钟
                .clientName(serviceName));                           // ← 客户端名称
    }

    // 校验 Aeron 配置
    if (!(aeron.context().subscriberErrorHandler() instanceof RethrowingErrorHandler))
    {
        throw new ClusterException("Aeron client must use a RethrowingErrorHandler");
    }

    // ==================== 步骤 9: 分配计数器 ====================
    final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();

    // 9.1 错误计数器
    if (null == errorCounter)
    {
        errorCounter = ClusterCounters.allocateServiceErrorCounter(aeron, tempBuffer, clusterId, serviceId);
    }

    // 9.2 计数错误处理器（将错误计数写入计数器）
    if (null == countedErrorHandler)
    {
        countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
        if (ownsAeronClient)
        {
            aeron.context().errorHandler(countedErrorHandler);
        }
    }

    // 9.3 Duty Cycle Tracker（用于监控 Agent 循环耗时）
    if (null == dutyCycleTracker)
    {
        dutyCycleTracker = new DutyCycleStallTracker(
            ClusterCounters.allocateServiceCounter(
                aeron,
                tempBuffer,
                "Cluster container max cycle time in ns",
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID,
                clusterId,
                serviceId),
            ClusterCounters.allocateServiceCounter(
                aeron,
                tempBuffer,
                "Cluster container work cycle time threshold in ns exceeded",
                AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                clusterId,
                serviceId),
            cycleThresholdNs);
    }

    // ==================== 步骤 10: 创建 AeronArchive.Context ====================
    // AeronArchive 用于日志回放和快照加载
    if (null == archiveContext)
    {
        archiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveControlChannel())          // ← Archive 控制通道
            .controlResponseChannel(archiveControlResponseChannel()) // ← Archive 响应通道
            .controlRequestStreamId(archiveControlStreamId())        // ← 控制流 ID
            .aeronDirectoryName(aeronDirectoryName);                 // ← CnC 目录
    }

    // ==================== 步骤 11: 校验 ClusteredService ====================
    // 必须提供 ClusteredService 实例（这是业务逻辑的核心）
    if (null == clusteredService)
    {
        throw new ClusterException("ClusteredService must be supplied");
    }

    // conclude() 完成！所有配置已初始化，可以创建 ClusteredServiceAgent
}
```

---

## 3. ClusteredServiceAgent 创建流程

**位置**：`ClusteredServiceAgent.java:构造函数`

```java
/**
 * ClusteredServiceAgent 构造函数：创建 Service 的核心代理。
 * <p>
 * 主要职责：
 * <ul>
 *   <li>初始化 Aeron 通信组件（Subscription、Publication）</li>
 *   <li>连接到 ConsensusModule（通过 serviceControlSubscription）</li>
 *   <li>准备接收命令和日志</li>
 *   <li>初始化快照加载器</li>
 * </ul>
 * <p>
 * <b>通信架构</b>：
 * <pre>
 * ConsensusModule                          ClusteredServiceAgent
 *        │                                          │
 *        ├─> serviceControlPublication             │
 *        │        └─────────────────────────────> serviceControlSubscription
 *        │                                          │
 *        │                                          ├─> 接收命令：
 *        │                                          │   - JOIN_LOG
 *        │                                          │   - READY
 *        │                                          │   - TERMINATE
 *        │                                          │   - SNAPSHOT
 *        │                                          │
 *        ├─> logPublication (multicast)            │
 *        │        └─────────────────────────────> logSubscription
 *        │                                          │
 *        │                                          ├─> 消费日志：
 *        │                                          │   - SessionMessage
 *        │                                          │   - TimerEvent
 *        │                                          │   - ClusterAction
 *        │                                          │
 *        │                                          │
 *        │ <──────────────────────────────────── consensusModulePublication
 *        │                                          │
 *        └─ 接收 ACK：                             └─> 发送 ACK：
 *           - SERVICE_ACK                              - onSessionMessage 完成
 *           - SNAPSHOT_COMPLETE                        - onTakeSnapshot 完成
 * </pre>
 *
 * @param ctx ClusteredServiceContainer 配置上下文（已完成 conclude()）
 */
ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
{
    // ==================== 步骤 1: 保存配置 ====================
    this.ctx = ctx;
    this.aeron = ctx.aeron();
    this.epochClock = ctx.epochClock();
    this.nanoClock = ctx.nanoClock();
    this.clusteredService = ctx.clusteredService();
    this.serviceId = ctx.serviceId();

    // ==================== 步骤 2: 订阅 Service Control Channel ====================
    // 接收 ConsensusModule 发送的命令（JOIN_LOG、READY、TERMINATE、SNAPSHOT）
    this.serviceControlSubscription = aeron.addSubscription(
        ctx.serviceControlChannel(),  // ← 控制通道（IPC）
        ctx.serviceStreamId());       // ← 流 ID

    // ==================== 步骤 3: 创建 Consensus Module Publication ====================
    // 用于向 ConsensusModule 发送 ACK（SERVICE_ACK、SNAPSHOT_COMPLETE）
    this.consensusModulePublication = aeron.addExclusivePublication(
        ctx.consensusModuleChannel(),  // ← 返回通道（IPC）
        ctx.consensusModuleStreamId());

    // ==================== 步骤 4: 初始化 Service Adapter ====================
    // ServiceAdapter 负责解析 ConsensusModule 发送的命令
    this.serviceAdapter = new ServiceAdapter(serviceControlSubscription, this);

    // ==================== 步骤 5: 初始化其他组件 ====================
    // - snapshotLoader: 用于加载快照
    // - logAdapter: 用于消费日志
    // - activeLogEvent: 用于跟踪当前正在处理的日志事件

    // onStart() 中会完成以下工作：
    // - 连接到 Archive
    // - 加载最新快照
    // - 订阅日志
}
```

---

## 4. 完整流程图

### 4.1 高层视图：启动流程

```
┌─────────────────────────────────────────────────────────────────┐
│            ClusteredServiceContainer.launch() 完整流程           │
└─────────────────────────────────────────────────────────────────┘

入口：ClusterNode.main()
    │
    ├─> container = ClusteredServiceContainer.launch(serviceContainerContext);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 1: ClusteredServiceContainer.launch(Context ctx)            │
│ 位置: ClusteredServiceContainer.java:150                         │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> final ClusteredServiceContainer container = new ClusteredServiceContainer(ctx);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2: new ClusteredServiceContainer(Context ctx)               │
│ 位置: ClusteredServiceContainer.java:110                         │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> ctx.conclude();  ← 核心配置初始化
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.1: Context.conclude()                                      │
│ 位置: ClusteredServiceContainer.java:823                         │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 并发检查（IS_CONCLUDED_VH.getAndSet）                       │
│ ├─ 2. 参数校验（serviceId、clusterId）                            │
│ ├─ 3. 创建目录（clusterDir、markFileDir）                         │
│ ├─ 4. 创建 ClusterMarkFile（进程协调 + 错误缓冲区）               │
│ ├─ 5. 初始化错误处理（errorLog、errorHandler）                    │
│ ├─ 6. 连接 Aeron MediaDriver                                     │
│ │      └─> aeron = Aeron.connect(aeronDirectoryName)             │
│ ├─ 7. 分配计数器（errorCounter、dutyCycleTracker）                │
│ ├─ 8. 创建 AeronArchive.Context（Archive 控制通道）               │
│ └─ 9. 校验 ClusteredService（必须提供）                           │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> final ClusteredServiceAgent agent = new ClusteredServiceAgent(ctx);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.2: new ClusteredServiceAgent(Context ctx)                 │
│ 位置: ClusteredServiceAgent.java:构造函数                         │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 保存配置（aeron、epochClock、clusteredService）            │
│ ├─ 2. 订阅 Service Control Channel                               │
│ │      └─> serviceControlSubscription = aeron.addSubscription()  │
│ ├─ 3. 创建 Consensus Module Publication                          │
│ │      └─> consensusModulePublication = aeron.addExclusivePublication() │
│ ├─ 4. 创建 ServiceAdapter（解析 ConsensusModule 命令）           │
│ └─ 5. 初始化其他组件（snapshotLoader、logAdapter）                │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> serviceAgentRunner = new AgentRunner(..., agent);
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 2.3: 创建 AgentRunner                                        │
│ 位置: ClusteredServiceContainer.java:131                         │
│                                                                   │
│ serviceAgentRunner = new AgentRunner(                             │
│     ctx.idleStrategy(),                                           │
│     ctx.errorHandler(),                                           │
│     ctx.errorCounter(),                                           │
│     agent);                                                       │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> return clusteredServiceContainer;
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 3: 启动 Agent                                                │
│ 位置: ClusteredServiceContainer.java:153                         │
│                                                                   │
│ AgentRunner.startOnThread(                                        │
│     clusteredServiceContainer.serviceAgentRunner,                 │
│     ctx.threadFactory());                                         │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> Agent 线程启动后调用 agent.onStart()
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 4: ClusteredServiceAgent.onStart()                          │
│ 位置: ClusteredServiceAgent.java:onStart                         │
│                                                                   │
│ 执行内容：                                                         │
│ ├─ 1. 连接到 Archive                                             │
│ │      └─> archive = AeronArchive.connect(ctx.archiveContext())  │
│ ├─ 2. 等待接收 JOIN_LOG 命令（来自 ConsensusModule）             │
│ ├─ 3. 加载最新快照（如果存在）                                    │
│ │      └─> clusteredService.onStart(cluster, snapshotImage)      │
│ ├─ 4. 订阅日志                                                    │
│ │      └─> logSubscription = aeron.addSubscription(logChannel)   │
│ └─ 5. 发送 READY ACK 给 ConsensusModule                          │
└───────────────────────────────────────────────────────────────────┘
    │
    ├─> Agent 开始执行主循环：agent.doWork()
    │
    ▼
┌───────────────────────────────────────────────────────────────────┐
│ 步骤 5: Agent 主循环                                              │
│ 位置: ClusteredServiceAgent.java:doWork                          │
│                                                                   │
│ while (running)                                                   │
│ {                                                                 │
│     int workCount = 0;                                            │
│                                                                   │
│     // 1. 接收 ConsensusModule 命令                              │
│     workCount += serviceAdapter.poll();                           │
│                                                                   │
│     // 2. 消费日志                                                │
│     workCount += logAdapter.poll(cluster.commitPosition());       │
│                                                                   │
│     // 3. 处理定时器事件                                          │
│     workCount += timerService.poll(nowNs);                        │
│                                                                   │
│     // 4. Idle 策略                                               │
│     if (0 == workCount)                                           │
│         idleStrategy.idle();                                      │
│ }                                                                 │
└───────────────────────────────────────────────────────────────────┘
    │
    └─> ClusteredServiceContainer 运行中！
```

### 4.2 详细视图：通信架构

```
┌────────────────────────────────────────────────────────────────────┐
│               ConsensusModule ↔ ClusteredServiceAgent              │
└────────────────────────────────────────────────────────────────────┘

ConsensusModule                          ClusteredServiceAgent
   (Leader)                                  (Service Container)
      │                                              │
      │                                              │
      │ ========== 通道 1: Service Control ========= │
      │                                              │
      ├─> serviceControlPublication                 │
      │   (IPC channel)                             │
      │   streamId = 103                            │
      │          └──────────────────────────────> serviceControlSubscription
      │                                              │
      │   发送命令：                                  │   接收命令：
      │   ├─ JOIN_LOG                               ├─ 解析命令
      │   ├─ READY                                  ├─ 订阅日志
      │   ├─ SNAPSHOT                               ├─ 执行快照
      │   └─ TERMINATE                              └─ 终止 Service
      │                                              │
      │                                              │
      │ ========== 通道 2: Log Channel ============= │
      │                                              │
      ├─> logPublication                            │
      │   (UDP multicast)                           │
      │   endpoint = 224.0.1.1:20002                │
      │   streamId = 103                            │
      │          └──────────────────────────────> logSubscription
      │                                              │
      │   发布日志：                                  │   消费日志：
      │   ├─ SessionMessage                         ├─ clusteredService.onSessionMessage()
      │   ├─ TimerEvent                             ├─ clusteredService.onTimerEvent()
      │   ├─ ClusterAction.SNAPSHOT                 ├─ clusteredService.onTakeSnapshot()
      │   └─ NewLeadershipTerm                      └─ 更新 cluster.leadershipTermId
      │                                              │
      │                                              │
      │ ========== 通道 3: Consensus Module ACK ==== │
      │                                              │
      │ <───────────────────────────────────────── consensusModulePublication
      │                                              │   (IPC channel)
      │                                              │   streamId = 104
      │                                              │
      │   接收 ACK：                                 │   发送 ACK：
      │   ├─ SERVICE_ACK                            ├─ 确认命令执行完成
      │   │   - ackId = JOIN_LOG                    │   - ackId
      │   │   - relevantId = logPosition            │   - relevantId
      │   ├─ SNAPSHOT_COMPLETE                      ├─ 确认快照完成
      │   │   - ackId = SNAPSHOT                    │   - recordingId
      │   └─ ERROR                                  └─ 报告错误
      │       - errorMessage                             - errorMessage
      │                                              │
      │                                              │
      │ ========== 通道 4: Snapshot Channel ======== │
      │                                              │
      │ (用于快照保存，由 ConsensusModule 创建)       │
      │                                              │
      ├─> snapshotPublication                       │
      │   (IPC channel)                             │
      │   streamId = 106                            │
      │          └──────────────────────────────> Archive 录制
      │                                              │
      │   Service 写入快照：                          │   Archive 保存：
      │   └─ clusteredService.onTakeSnapshot()      └─> recording-XXX.dat
      │       ├─ 保存业务状态
      │       └─ 发送 SNAPSHOT_COMPLETE ACK
      │                                              │
```

---

## 5. 关键组件说明

### 5.1 Context.conclude() 核心职责

| 步骤 | 职责 | 输出 |
|------|------|------|
| 1. 并发检查 | 确保 conclude() 只执行一次 | - |
| 2. 参数校验 | 校验 serviceId、clusterId | 抛出异常或继续 |
| 3. 目录创建 | 创建 clusterDir、markFileDir | 文件系统目录 |
| 4. Mark File | 创建 ClusterMarkFile | 进程协调文件 |
| 5. 错误处理 | 创建 errorLog、errorHandler | 错误日志系统 |
| 6. 连接 Aeron | 连接到 MediaDriver | Aeron 客户端 |
| 7. 分配计数器 | 创建所有监控计数器 | errorCounter、dutyCycleTracker |
| 8. Archive Context | 创建 AeronArchive.Context | Archive 控制通道配置 |
| 9. 校验 Service | 确保提供 ClusteredService 实例 | - |

### 5.2 ClusteredServiceAgent 核心职责

| 组件 | 职责 |
|------|------|
| **serviceAdapter** | 接收 ConsensusModule 命令（JOIN_LOG、READY、SNAPSHOT、TERMINATE）|
| **logAdapter** | 消费日志，调用 ClusteredService.onSessionMessage() |
| **snapshotLoader** | 加载快照，调用 ClusteredService.onStart() |
| **consensusModulePublication** | 向 ConsensusModule 发送 ACK（SERVICE_ACK、SNAPSHOT_COMPLETE）|
| **timerService** | 定时器触发，调用 ClusteredService.onTimerEvent() |

### 5.3 ClusteredServiceContainer vs ConsensusModule

| 维度 | **ClusteredServiceContainer** | **ConsensusModule** |
|------|------------------------------|---------------------|
| **职责** | 运行业务逻辑（ClusteredService） | Raft 共识、日志复制、会话管理 |
| **线程模型** | 始终使用 Runner 模式（独立线程） | Runner 或 Invoker 模式 |
| **通信方式** | 订阅 logChannel，接收 serviceControl | 发布到 logChannel，发送 serviceControl |
| **启动顺序** | 在 ConsensusModule 之后启动 | 首先启动 |
| **数量** | 每个节点可运行多个 Service（serviceId 0-9） | 每个节点只有一个 ConsensusModule |
| **状态管理** | 通过快照保存/恢复业务状态 | 通过 RecordingLog 管理 term 和 snapshot 元数据 |

---

## 6. 启动时序图

```
时间轴：ClusteredServiceContainer 启动过程（详细版）

t0 ─────────────────────────────────────────────────────────────────→

════════════════════════════════════════════════════════════════════
t0: ClusterNode.main() 启动
════════════════════════════════════════════════════════════════════

ClusterNode:
    ├─> 1. 启动 MediaDriver
    ├─> 2. 启动 Archive
    ├─> 3. 启动 ConsensusModule
    └─> 4. 启动 ClusteredServiceContainer ← 我们在这里

════════════════════════════════════════════════════════════════════
t1 (50ms): ClusteredServiceContainer.launch() 开始
════════════════════════════════════════════════════════════════════

ClusteredServiceContainer:
    ├─> new ClusteredServiceContainer(ctx)
    │   ├─> ctx.conclude()
    │   │   ├─ 并发检查
    │   │   ├─ 参数校验（serviceId=0, clusterId=0）
    │   │   ├─ 创建目录
    │   │   ├─ 创建 Mark File: cluster-mark-service-0.dat
    │   │   ├─ 连接 Aeron: serviceName="clustered-service-0-0"
    │   │   ├─ 分配计数器
    │   │   └─ 创建 Archive Context
    │   │
    │   ├─> new ClusteredServiceAgent(ctx)
    │   │   ├─ 订阅 serviceControlSubscription (IPC)
    │   │   ├─ 创建 consensusModulePublication (IPC)
    │   │   └─ 创建 ServiceAdapter
    │   │
    │   └─> new AgentRunner(..., agent)
    │
    └─> AgentRunner.startOnThread()
        └─> 新线程启动

════════════════════════════════════════════════════════════════════
t2 (100ms): ClusteredServiceAgent.onStart() 开始
════════════════════════════════════════════════════════════════════

ClusteredServiceAgent:
    ├─> 连接到 Archive
    │   └─> archive = AeronArchive.connect(ctx.archiveContext())
    │
    ├─> 等待 JOIN_LOG 命令（来自 ConsensusModule）
    │   └─> serviceAdapter.poll()
    │       └─> 收到: JOIN_LOG(logChannel="aeron:udp?endpoint=224.0.1.1:20002", ...)
    │
    ├─> 加载快照（如果存在）
    │   └─> clusteredService.onStart(cluster, snapshotImage)
    │       ├─ 恢复 globalMessageCount
    │       ├─ 恢复 sessionMessageCounts
    │       └─ 恢复其他业务状态
    │
    ├─> 订阅日志
    │   └─> logSubscription = aeron.addSubscription(logChannel, logStreamId)
    │
    └─> 发送 READY ACK
        └─> consensusModulePublication.offer(SERVICE_ACK, ackId=JOIN_LOG)

════════════════════════════════════════════════════════════════════
t3 (150ms): 进入主循环
════════════════════════════════════════════════════════════════════

ClusteredServiceAgent.doWork():
    │
    ├─> serviceAdapter.poll()        // 接收 ConsensusModule 命令
    ├─> logAdapter.poll()             // 消费日志
    │   └─> clusteredService.onSessionMessage()  // 处理业务逻辑
    ├─> timerService.poll()           // 处理定时器
    └─> idleStrategy.idle()           // 无工作时空闲

════════════════════════════════════════════════════════════════════
t4 (200ms): 开始接收客户端消息
════════════════════════════════════════════════════════════════════

ConsensusModule (Leader):
    └─> 客户端发送: "Hello from Client!"
        └─> 写入日志
            └─> logPublication.offer(SessionMessage)

ClusteredServiceAgent:
    └─> logAdapter.poll()
        └─> 收到: SessionMessage("Hello from Client!")
            └─> clusteredService.onSessionMessage(...)
                ├─ globalMessageCount++
                ├─ sessionMessageCounts.update(sessionId)
                └─> session.offer("Echo: Hello from Client!")
                    └─> (只有 Leader 真正发送)

════════════════════════════════════════════════════════════════════
t5 (5 分钟后): 触发快照
════════════════════════════════════════════════════════════════════

ConsensusModule (Leader):
    └─> 写入 ClusterAction.SNAPSHOT 到日志
        └─> 发送 SNAPSHOT 命令到 Service
            └─> serviceControlPublication.offer(SNAPSHOT)

ClusteredServiceAgent:
    └─> serviceAdapter.poll()
        └─> 收到: SNAPSHOT 命令
            └─> clusteredService.onTakeSnapshot(snapshotPublication)
                ├─ 保存 globalMessageCount
                ├─ 保存 sessionMessageCounts
                └─ 完成后发送 ACK
                    └─> consensusModulePublication.offer(SNAPSHOT_COMPLETE)

════════════════════════════════════════════════════════════════════
启动总耗时：~150ms（不包括快照加载时间）
════════════════════════════════════════════════════════════════════
```

---

## 7. 总结

### 7.1 启动流程总结

```
1. ClusteredServiceContainer.launch(ctx)
   └─> 2. new ClusteredServiceContainer(ctx)
           ├─> 2.1 ctx.conclude()          ← 初始化所有配置
           ├─> 2.2 new ClusteredServiceAgent(ctx)  ← 创建 Service 代理
           └─> 2.3 new AgentRunner()        ← 创建 Runner
   └─> 3. AgentRunner.startOnThread()      ← 启动 Agent
           └─> 4. agent.onStart()           ← 连接 Archive、加载快照、订阅日志
                   └─> 5. agent.doWork()    ← 主循环：接收命令、消费日志
```

### 7.2 关键要点

1. **conclude() 是配置初始化的核心**
   - 创建所有必要的目录和文件
   - 连接到 Aeron 和 Archive
   - 分配所有监控计数器

2. **ClusteredServiceAgent 是业务逻辑执行者**
   - 接收 ConsensusModule 命令
   - 消费日志，调用 ClusteredService 回调
   - 保存/恢复快照

3. **始终使用 Runner 模式**
   - ClusteredServiceContainer 不支持 Invoker 模式
   - Agent 在独立线程上运行

4. **与 ConsensusModule 的通信**
   - 通过 IPC 接收命令（serviceControlSubscription）
   - 通过 UDP multicast 消费日志（logSubscription）
   - 通过 IPC 发送 ACK（consensusModulePublication）

### 7.3 与 ConsensusModule 的对比

| 特性 | **ClusteredServiceContainer** | **ConsensusModule** |
|------|------------------------------|---------------------|
| **职责** | 业务逻辑执行 | Raft 共识、日志复制 |
| **启动顺序** | 后启动（依赖 ConsensusModule）| 先启动 |
| **线程模型** | 始终 Runner | Runner 或 Invoker |
| **快照内容** | 业务状态 | 会话、定时器、元数据 |
| **日志消费** | 被动消费（从 logSubscription）| 主动发布（到 logPublication）|

---

## 8. 源码位置索引

| 方法/类 | 位置 | 行号 |
|---------|------|------|
| `ClusteredServiceContainer.launch()` | `ClusteredServiceContainer.java` | 150-156 |
| `ClusteredServiceContainer(Context)` | `ClusteredServiceContainer.java` | 110-132 |
| `Context.conclude()` | `ClusteredServiceContainer.java` | 823-1070 |
| `ClusteredServiceAgent(Context)` | `ClusteredServiceAgent.java` | 构造函数 |
| `ClusteredServiceAgent.onStart()` | `ClusteredServiceAgent.java` | onStart |
| `ClusteredServiceAgent.doWork()` | `ClusteredServiceAgent.java` | doWork |

---

**完整流程总耗时**：通常 < 200ms（无快照加载）
**主要耗时点**：
- Archive 连接：10-50ms
- 快照加载：1ms - 5s（取决于快照大小）
- 日志订阅：10-50ms

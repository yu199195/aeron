/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.RethrowingErrorHandler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.AppVersionValidator;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.ClusterComponentType;
import io.aeron.cluster.codecs.mark.MarkFileHeaderEncoder;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.driver.status.DutyCycleStallTracker;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.version.Versioned;
import org.agrona.CloseHelper;
import org.agrona.DelegatingErrorHandler;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.Strings;
import org.agrona.SystemUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.ChannelUri.addAliasIfAbsent;
import static io.aeron.CommonContext.driverFilePageSize;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.LIVENESS_TIMEOUT_MS;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MAX_SERVICE_COUNT;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SERVICE_NAME_PROP_NAME;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * Container for a service in the cluster managed by the Consensus Module. This is where business logic resides and
 * loaded via {@link ClusteredServiceContainer.Configuration#SERVICE_CLASS_NAME_PROP_NAME} or
 * {@link ClusteredServiceContainer.Context#clusteredService(ClusteredService)}.
 */
@Versioned
public final class ClusteredServiceContainer implements AutoCloseable
{
    /**
     * Launch the clustered service container and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredServiceContainer ignore = launch(new Context().terminationHook(barrier::signalAll)))
        {
            barrier.await();

            System.out.println("Shutdown ClusteredServiceContainer...");
        }
    }

    private final Context ctx;
    private final AgentRunner serviceAgentRunner;

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
        // ==================== 保存配置上下文 ====================
        this.ctx = ctx;

        try
        {
            // ==================== 步骤 1: 配置初始化（核心逻辑） ====================
            // conclude() 负责：
            // - 校验配置参数（serviceId、clusterId 等）
            // - 创建工作目录
            // - 连接到 Aeron
            // - 分配计数器
            ctx.conclude();  // ← 详见 Context.conclude() 方法

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

    /**
     * Launch an ClusteredServiceContainer using a default configuration.
     *
     * @return a new instance of a ClusteredServiceContainer.
     */
    public static ClusteredServiceContainer launch()
    {
        return launch(new Context());
    }

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

    /**
     * Get the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     *
     * @return the {@link Context} that is used by this {@link ClusteredServiceContainer}.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(serviceAgentRunner);
    }

    /**
     * Configuration options for the consensus module and service container within a cluster.
     */
    @Config(existsInC = false)
    public static final class Configuration
    {
        /**
         * Type of snapshot for this service.
         */
        public static final long SNAPSHOT_TYPE_ID = 2;

        /**
         * Update interval for cluster mark file in nanoseconds.
         */
        public static final long MARK_FILE_UPDATE_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

        /**
         * Timeout in milliseconds to detect liveness.
         */
        public static final long LIVENESS_TIMEOUT_MS = 10 * TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

        /**
         * Property name for the identity of the cluster instance.
         */
        @Config
        public static final String CLUSTER_ID_PROP_NAME = "aeron.cluster.id";

        /**
         * Default identity for a clustered instance.
         */
        @Config
        public static final int CLUSTER_ID_DEFAULT = 0;

        /**
         * Identity for a clustered service. Services should be numbered from 0 and be contiguous.
         */
        @Config
        public static final String SERVICE_ID_PROP_NAME = "aeron.cluster.service.id";

        /**
         * Default identity for a clustered service.
         */
        @Config
        public static final int SERVICE_ID_DEFAULT = 0;

        /**
         * The max number of services supported by the cluster instance.
         */
        public static final int MAX_SERVICE_COUNT = 10;

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        @Config
        public static final String SERVICE_NAME_PROP_NAME = "aeron.cluster.service.name";

        /**
         * Name for a clustered service to be the role of the {@link Agent}.
         */
        @Config
        public static final String SERVICE_NAME_DEFAULT = "clustered-service";

        /**
         * Class name for dynamically loading a {@link ClusteredService}. This is used if
         * {@link Context#clusteredService()} is not set.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String SERVICE_CLASS_NAME_PROP_NAME = "aeron.cluster.service.class.name";

        /**
         * Channel to be used for log or snapshot replay on startup.
         */
        @Config
        public static final String REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.replay.channel";

        /**
         * Default channel to be used for log or snapshot replay on startup.
         */
        @Config
        public static final String REPLAY_CHANNEL_DEFAULT = CommonContext.IPC_CHANNEL;

        /**
         * Stream id within a channel for the clustered log or snapshot replay.
         */
        @Config
        public static final String REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.replay.stream.id";

        /**
         * Default stream id for the log or snapshot replay within a channel.
         */
        @Config
        public static final int REPLAY_STREAM_ID_DEFAULT = 103;

        /**
         * Channel for control communications between the local consensus module and services.
         */
        @Config
        public static final String CONTROL_CHANNEL_PROP_NAME = "aeron.cluster.control.channel";

        /**
         * Default channel for communications between the local consensus module and services. This should be IPC.
         */
        @Config
        public static final String CONTROL_CHANNEL_DEFAULT = "aeron:ipc?term-length=128k";

        /**
         * Stream id within the control channel for communications from the consensus module to the services.
         */
        @Config
        public static final String SERVICE_STREAM_ID_PROP_NAME = "aeron.cluster.service.stream.id";

        /**
         * Default stream id within the control channel for communications from the consensus module.
         */
        @Config
        public static final int SERVICE_STREAM_ID_DEFAULT = 104;

        /**
         * Stream id within the control channel for communications from the services to the consensus module.
         */
        @Config
        public static final String CONSENSUS_MODULE_STREAM_ID_PROP_NAME = "aeron.cluster.consensus.module.stream.id";

        /**
         * Default stream id within a channel for communications from the services to the consensus module.
         */
        @Config
        public static final int CONSENSUS_MODULE_STREAM_ID_DEFAULT = 105;

        /**
         * Channel to be used for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_CHANNEL_PROP_NAME = "aeron.cluster.snapshot.channel";

        /**
         * Default channel to be used for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_CHANNEL_DEFAULT = "aeron:ipc?alias=snapshot";

        /**
         * Stream id within a channel for archiving snapshots.
         */
        @Config
        public static final String SNAPSHOT_STREAM_ID_PROP_NAME = "aeron.cluster.snapshot.stream.id";

        /**
         * Default stream id for the archived snapshots within a channel.
         */
        @Config
        public static final int SNAPSHOT_STREAM_ID_DEFAULT = 106;

        /**
         * Directory to use for the aeron cluster.
         */
        @Config
        public static final String CLUSTER_DIR_PROP_NAME = "aeron.cluster.dir";

        /**
         * Default directory to use for the aeron cluster.
         */
        @Config
        public static final String CLUSTER_DIR_DEFAULT = "aeron-cluster";

        /**
         * Directory to use for the aeron cluster services, will default to
         * {@link io.aeron.cluster.ConsensusModule.Context#clusterDir()} if not specified.
         */
        @Config(defaultType = DefaultType.STRING)
        public static final String CLUSTER_SERVICES_DIR_PROP_NAME = "aeron.cluster.services.dir";

        /**
         * Directory to use for the Cluster component's mark file.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String MARK_FILE_DIR_PROP_NAME = "aeron.cluster.mark.file.dir";

        /**
         * Length in bytes of the error buffer for the cluster container.
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.cluster.service.error.buffer.length";

        /**
         * Default length in bytes of the error buffer for the cluster container.
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Is this a responding service to client requests property.
         */
        @Config
        public static final String RESPONDER_SERVICE_PROP_NAME = "aeron.cluster.service.responder";

        /**
         * Default to true that this a responding service to client requests.
         */
        @Config
        public static final boolean RESPONDER_SERVICE_DEFAULT = true;

        /**
         * Fragment limit to use when polling the log.
         */
        @Config
        public static final String LOG_FRAGMENT_LIMIT_PROP_NAME = "aeron.cluster.log.fragment.limit";

        /**
         * Default fragment limit for polling log.
         */
        @Config
        public static final int LOG_FRAGMENT_LIMIT_DEFAULT = 50;

        /**
         * Delegating {@link ErrorHandler} which will be first in the chain before delegating to the
         * {@link Context#errorHandler()}.
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "")
        public static final String DELEGATING_ERROR_HANDLER_PROP_NAME =
            "aeron.cluster.service.delegating.error.handler";

        /**
         * Property name for threshold value for the container work cycle threshold to track
         * for being exceeded.
         */
        @Config(id = "SERVICE_CYCLE_THRESHOLD")
        public static final String CYCLE_THRESHOLD_PROP_NAME = "aeron.cluster.service.cycle.threshold";

        /**
         * Default threshold value for the container work cycle threshold to track for being exceeded.
         */
        @Config(
            id = "SERVICE_CYCLE_THRESHOLD",
            defaultType = DefaultType.LONG,
            defaultLong = 100_000_000L)
        public static final long CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(100);

        /**
         * Property name for threshold value, which is used for tracking snapshot duration breaches.
         *
         * @since 1.44.0
         */
        @Config
        public static final String SNAPSHOT_DURATION_THRESHOLD_PROP_NAME = "aeron.cluster.service.snapshot.threshold";

        /**
         * Default threshold value, which is used for tracking snapshot duration breaches.
         *
         * @since 1.44.0
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 1000L * 1000 * 1000)
        public static final long SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

        /**
         * Counter type id for the cluster node role.
         */
        public static final int CLUSTER_NODE_ROLE_TYPE_ID = AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID;

        /**
         * Counter type id of the commit position.
         */
        public static final int COMMIT_POSITION_TYPE_ID = AeronCounters.CLUSTER_COMMIT_POSITION_TYPE_ID;

        /**
         * Counter type id for the clustered service error count.
         */
        public static final int CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID =
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID;

        /**
         * The value {@link #CLUSTER_ID_DEFAULT} or system property {@link #CLUSTER_ID_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_ID_DEFAULT} or system property {@link #CLUSTER_ID_PROP_NAME} if set.
         */
        public static int clusterId()
        {
            return Integer.getInteger(CLUSTER_ID_PROP_NAME, CLUSTER_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_ID_DEFAULT} or system property {@link #SERVICE_ID_PROP_NAME} if set.
         */
        public static int serviceId()
        {
            return Integer.getInteger(SERVICE_ID_PROP_NAME, SERVICE_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_NAME_DEFAULT} or system property {@link #SERVICE_NAME_PROP_NAME} if set.
         */
        public static String serviceName()
        {
            return System.getProperty(SERVICE_NAME_PROP_NAME, SERVICE_NAME_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #REPLAY_CHANNEL_DEFAULT} or system property {@link #REPLAY_CHANNEL_PROP_NAME} if set.
         */
        public static String replayChannel()
        {
            return System.getProperty(REPLAY_CHANNEL_PROP_NAME, REPLAY_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #REPLAY_STREAM_ID_DEFAULT} or system property {@link #REPLAY_STREAM_ID_PROP_NAME}
         * if set.
         */
        public static int replayStreamId()
        {
            return Integer.getInteger(REPLAY_STREAM_ID_PROP_NAME, REPLAY_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #CONTROL_CHANNEL_DEFAULT} or system property
         * {@link #CONTROL_CHANNEL_PROP_NAME} if set.
         */
        public static String controlChannel()
        {
            return System.getProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #CONSENSUS_MODULE_STREAM_ID_DEFAULT} or system property
         * {@link #CONSENSUS_MODULE_STREAM_ID_PROP_NAME} if set.
         */
        public static int consensusModuleStreamId()
        {
            return Integer.getInteger(CONSENSUS_MODULE_STREAM_ID_PROP_NAME, CONSENSUS_MODULE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SERVICE_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         *
         * @return {@link #SERVICE_STREAM_ID_DEFAULT} or system property
         * {@link #SERVICE_STREAM_ID_PROP_NAME} if set.
         */
        public static int serviceStreamId()
        {
            return Integer.getInteger(SERVICE_STREAM_ID_PROP_NAME, SERVICE_STREAM_ID_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #SNAPSHOT_CHANNEL_DEFAULT} or system property {@link #SNAPSHOT_CHANNEL_PROP_NAME} if set.
         */
        public static String snapshotChannel()
        {
            return System.getProperty(SNAPSHOT_CHANNEL_PROP_NAME, SNAPSHOT_CHANNEL_DEFAULT);
        }

        /**
         * The value {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME}
         * if set.
         *
         * @return {@link #SNAPSHOT_STREAM_ID_DEFAULT} or system property {@link #SNAPSHOT_STREAM_ID_PROP_NAME} if set.
         */
        public static int snapshotStreamId()
        {
            return Integer.getInteger(SNAPSHOT_STREAM_ID_PROP_NAME, SNAPSHOT_STREAM_ID_DEFAULT);
        }

        /**
         * Default {@link IdleStrategy} to be employed for cluster agents.
         */
        @Config(id = "CLUSTER_IDLE_STRATEGY")
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

        /**
         * {@link IdleStrategy} to be employed for cluster agents.
         */
        @Config
        public static final String CLUSTER_IDLE_STRATEGY_PROP_NAME = "aeron.cluster.idle.strategy";

        /**
         * Property to configure if this node should take standby snapshots. The default for this property is
         * <code>false</code>.
         */
        @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
        public static final String STANDBY_SNAPSHOT_ENABLED_PROP_NAME = "aeron.cluster.standby.snapshot.enabled";

        /**
         * Create a supplier of {@link IdleStrategy}s that will use the system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return () ->
            {
                final String name = System.getProperty(CLUSTER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                return io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
            };
        }

        /**
         * The value {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         *
         * @return {@link #CLUSTER_DIR_DEFAULT} or system property {@link #CLUSTER_DIR_PROP_NAME} if set.
         */
        public static String clusterDirName()
        {
            return System.getProperty(CLUSTER_DIR_PROP_NAME, CLUSTER_DIR_DEFAULT);
        }

        /**
         * The value of system property {@link #CLUSTER_DIR_PROP_NAME} if set or null.
         *
         * @return {@link #CLUSTER_DIR_PROP_NAME} if set or null.
         */
        public static String clusterServicesDirName()
        {
            return System.getProperty(CLUSTER_SERVICES_DIR_PROP_NAME);
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength()
        {
            return getSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
        }

        /**
         * The value {@link #RESPONDER_SERVICE_DEFAULT} or system property {@link #RESPONDER_SERVICE_PROP_NAME} if set.
         *
         * @return {@link #RESPONDER_SERVICE_DEFAULT} or system property {@link #RESPONDER_SERVICE_PROP_NAME} if set.
         */
        public static boolean isRespondingService()
        {
            final String property = System.getProperty(RESPONDER_SERVICE_PROP_NAME);
            if (null == property)
            {
                return RESPONDER_SERVICE_DEFAULT;
            }

            return "true".equals(property);
        }

        /**
         * The value {@link #LOG_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #LOG_FRAGMENT_LIMIT_PROP_NAME} if set.
         *
         * @return {@link #LOG_FRAGMENT_LIMIT_DEFAULT} or system property
         * {@link #LOG_FRAGMENT_LIMIT_PROP_NAME} if set.
         */
        public static int logFragmentLimit()
        {
            return Integer.getInteger(LOG_FRAGMENT_LIMIT_PROP_NAME, LOG_FRAGMENT_LIMIT_DEFAULT);
        }

        /**
         * Get threshold value for the container work cycle threshold to track for being exceeded.
         *
         * @return threshold value in nanoseconds.
         */
        public static long cycleThresholdNs()
        {
            return getDurationInNanos(CYCLE_THRESHOLD_PROP_NAME, CYCLE_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get threshold value, which is used for monitoring snapshot duration breaches of its predefined
         * threshold.
         *
         * @return threshold value in nanoseconds.
         */
        public static long snapshotDurationThresholdNs()
        {
            return getDurationInNanos(SNAPSHOT_DURATION_THRESHOLD_PROP_NAME, SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS);
        }

        /**
         * Get the configuration value to determine if this node should take standby snapshots be enabled.
         *
         * @return configuration value for standby snapshots being enabled.
         */
        public static boolean standbySnapshotEnabled()
        {
            return Boolean.getBoolean(STANDBY_SNAPSHOT_ENABLED_PROP_NAME);
        }

        /**
         * Create a new {@link ClusteredService} based on the configured {@link #SERVICE_CLASS_NAME_PROP_NAME}.
         *
         * @return a new {@link ClusteredService} based on the configured {@link #SERVICE_CLASS_NAME_PROP_NAME}.
         */
        public static ClusteredService newClusteredService()
        {
            final String className = System.getProperty(Configuration.SERVICE_CLASS_NAME_PROP_NAME);
            if (null == className)
            {
                throw new ClusterException("either a instance or class name for the service must be provided");
            }

            try
            {
                return (ClusteredService)Class.forName(className).getConstructor().newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
                return null;
            }
        }

        /**
         * Create a new {@link DelegatingErrorHandler} defined by {@link #DELEGATING_ERROR_HANDLER_PROP_NAME}.
         *
         * @return a new {@link DelegatingErrorHandler} defined by {@link #DELEGATING_ERROR_HANDLER_PROP_NAME} or
         * null if property not set.
         */
        public static DelegatingErrorHandler newDelegatingErrorHandler()
        {
            final String className = System.getProperty(Configuration.DELEGATING_ERROR_HANDLER_PROP_NAME);
            if (null != className)
            {
                try
                {
                    return (DelegatingErrorHandler)Class.forName(className).getConstructor().newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            }

            return null;
        }

        /**
         * Get the alternative directory to be used for storing the Cluster component's mark file.
         *
         * @return the directory to be used for storing the archive mark file.
         */
        public static String markFileDir()
        {
            return System.getProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    /**
     * The context will be owned by {@link ClusteredServiceAgent} after a successful
     * {@link ClusteredServiceContainer#launch(Context)} and closed via {@link ClusteredServiceContainer#close()}.
     */
    public static final class Context implements Cloneable
    {
        private static final VarHandle IS_CONCLUDED_VH;

        static
        {
            try
            {
                IS_CONCLUDED_VH = MethodHandles.lookup().findVarHandle(Context.class, "isConcluded", boolean.class);
            }
            catch (final ReflectiveOperationException ex)
            {
                throw new ExceptionInInitializerError(ex);
            }
        }

        private volatile boolean isConcluded;
        private int appVersion = SemanticVersion.compose(0, 0, 1);
        private int clusterId = Configuration.clusterId();
        private int serviceId = Configuration.serviceId();
        private String serviceName = System.getProperty(SERVICE_NAME_PROP_NAME);
        private String replayChannel = Configuration.replayChannel();
        private int replayStreamId = Configuration.replayStreamId();
        private String controlChannel = Configuration.controlChannel();
        private int consensusModuleStreamId = Configuration.consensusModuleStreamId();
        private int serviceStreamId = Configuration.serviceStreamId();
        private String snapshotChannel = Configuration.snapshotChannel();
        private int snapshotStreamId = Configuration.snapshotStreamId();
        private int errorBufferLength = Configuration.errorBufferLength();
        private boolean isRespondingService = Configuration.isRespondingService();
        private int logFragmentLimit = Configuration.logFragmentLimit();
        private long cycleThresholdNs = Configuration.cycleThresholdNs();
        private long snapshotDurationThresholdNs = Configuration.snapshotDurationThresholdNs();
        private boolean standbySnapshotEnabled = Configuration.standbySnapshotEnabled();

        private CountDownLatch abortLatch;
        private ThreadFactory threadFactory;
        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private DelegatingErrorHandler delegatingErrorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;
        private AeronArchive.Context archiveContext;
        private String clusterDirectoryName = Configuration.clusterDirName();
        private File clusterDir;
        private File markFileDir;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private DutyCycleTracker dutyCycleTracker;
        private SnapshotDurationTracker snapshotDurationTracker;
        private AppVersionValidator appVersionValidator;
        private boolean ownsAeronClient;

        private ClusteredService clusteredService;
        private Runnable terminationHook;
        private ClusterMarkFile markFile;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

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
        @SuppressWarnings("MethodLength")
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

                ownsAeronClient = true;
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
                        "Cluster container work cycle time exceeded count: threshold=" +
                            SystemUtil.formatDuration(cycleThresholdNs),
                        AeronCounters.CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID,
                        clusterId,
                        serviceId),
                    cycleThresholdNs);
            }

            // 9.4 Snapshot Duration Tracker（用于监控快照耗时）
            if (null == snapshotDurationTracker)
            {
                snapshotDurationTracker = new SnapshotDurationTracker(
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Clustered service max snapshot duration in ns",
                        AeronCounters.CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID,
                        clusterId,
                        serviceId
                    ),
                    ClusterCounters.allocateServiceCounter(
                        aeron,
                        tempBuffer,
                        "Clustered service max snapshot duration exceeded count: threshold=" +
                            SystemUtil.formatDuration(snapshotDurationThresholdNs),
                        AeronCounters.CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID,
                        clusterId,
                        serviceId
                    ),
                    snapshotDurationThresholdNs);
            }

            // ==================== 步骤 10: 创建 AeronArchive.Context ====================
            // AeronArchive 用于日志回放和快照加载
            if (null == archiveContext)
            {
                archiveContext = new AeronArchive.Context()
                    .controlRequestChannel(AeronArchive.Configuration.localControlChannel())          // ← Archive 控制通道
                    .controlResponseChannel(AeronArchive.Configuration.localControlChannel())         // ← Archive 响应通道
                    .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())        // ← 控制流 ID
                    .controlResponseStreamId(
                        clusterId * 100 + 100 + AeronArchive.Configuration.controlResponseStreamId() + (serviceId + 1));
            }

            // 校验 Archive 控制通道必须是 IPC
            if (!archiveContext.controlRequestChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            if (!archiveContext.controlResponseChannel().startsWith(CommonContext.IPC_CHANNEL))
            {
                throw new ClusterException("local archive control must be IPC");
            }

            // 配置 Archive Context
            archiveContext
                .aeron(aeron)
                .ownsAeronClient(false)
                .lock(NoOpLock.INSTANCE)
                .errorHandler(countedErrorHandler)
                .controlRequestChannel(addAliasIfAbsent(
                archiveContext.controlRequestChannel(),
                "sc-" + serviceId + "-archive-ctrl-req-cluster-" + clusterId))
                .controlResponseChannel(addAliasIfAbsent(
                archiveContext.controlResponseChannel(),
                "sc-" + serviceId + "-archive-ctrl-resp-cluster-" + clusterId))
                .clientName(serviceName);

            // ==================== 步骤 11: 设置终止钩子 ====================
            if (null == terminationHook)
            {
                terminationHook = () -> {};
            }

            // ==================== 步骤 12: 校验 ClusteredService ====================
            // 必须提供 ClusteredService 实例（这是业务逻辑的核心）
            if (null == clusteredService)
            {
                clusteredService = Configuration.newClusteredService();
            }

            // ==================== 步骤 13: 初始化终止同步器 ====================
            abortLatch = new CountDownLatch(!aeron.context().useConductorAgentInvoker() ? 1 : 0);

            // ==================== 步骤 14: 完成 Mark File 配置 ====================
            concludeMarkFile();

            if (CommonContext.shouldPrintConfigurationOnStart())
            {
                System.out.println(this);
            }
        }

        /**
         * Has the context had the {@link #conclude()} method called.
         *
         * @return true of the {@link #conclude()} method has been called.
         */
        public boolean isConcluded()
        {
            return isConcluded;
        }

        /**
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @param appVersion for user application.
         * @return this for a fluent API.
         */
        public Context appVersion(final int appVersion)
        {
            this.appVersion = appVersion;
            return this;
        }

        /**
         * User assigned application version which appended to the log as the appVersion in new leadership events.
         * <p>
         * This can be validated using {@link org.agrona.SemanticVersion} to ensure only application nodes of the same
         * major version communicate with each other.
         *
         * @return appVersion for user application.
         */
        public int appVersion()
        {
            return appVersion;
        }

        /**
         * User assigned application version validator implementation used to check version compatibility.
         * <p>
         * The default validator uses {@link org.agrona.SemanticVersion} semantics.
         *
         * @param appVersionValidator for user application.
         * @return this for fluent API.
         */
        public Context appVersionValidator(final AppVersionValidator appVersionValidator)
        {
            this.appVersionValidator = appVersionValidator;
            return this;
        }

        /**
         * User assigned application version validator implementation used to check version compatibility.
         * <p>
         * The default is to use {@link org.agrona.SemanticVersion} major version for checking compatibility.
         *
         * @return AppVersionValidator in use.
         */
        public AppVersionValidator appVersionValidator()
        {
            return appVersionValidator;
        }

        /**
         * Set the id for this cluster instance. This must match with the Consensus Module.
         *
         * @param clusterId for this clustered instance.
         * @return this for a fluent API
         * @see Configuration#CLUSTER_ID_PROP_NAME
         */
        public Context clusterId(final int clusterId)
        {
            this.clusterId = clusterId;
            return this;
        }

        /**
         * Get the id for this cluster instance. This must match with the Consensus Module.
         *
         * @return the id for this cluster instance.
         * @see Configuration#CLUSTER_ID_PROP_NAME
         */
        @Config
        public int clusterId()
        {
            return clusterId;
        }

        /**
         * Set the id for this clustered service. Services should be numbered from 0 and be contiguous.
         *
         * @param serviceId for this clustered service.
         * @return this for a fluent API
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        public Context serviceId(final int serviceId)
        {
            this.serviceId = serviceId;
            return this;
        }

        /**
         * Get the id for this clustered service. Services should be numbered from 0 and be contiguous.
         *
         * @return the id for this clustered service.
         * @see Configuration#SERVICE_ID_PROP_NAME
         */
        @Config
        public int serviceId()
        {
            return serviceId;
        }

        /**
         * Set the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @param serviceName for a clustered service to be the role for the {@link Agent}.
         * @return this for a fluent API.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        public Context serviceName(final String serviceName)
        {
            this.serviceName = serviceName;
            return this;
        }

        /**
         * Get the name for a clustered service to be the {@link Agent#roleName()} for the {@link Agent}.
         *
         * @return the name for a clustered service to be the role of the {@link Agent}.
         * @see Configuration#SERVICE_NAME_PROP_NAME
         */
        @Config
        public String serviceName()
        {
            return serviceName;
        }

        /**
         * Set the channel parameter for the cluster log and snapshot replay channel.
         *
         * @param channel parameter for the cluster log replay channel.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        public Context replayChannel(final String channel)
        {
            replayChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for the cluster log and snapshot replay channel.
         *
         * @return the channel parameter for the cluster replay channel.
         * @see Configuration#REPLAY_CHANNEL_PROP_NAME
         */
        @Config
        public String replayChannel()
        {
            return replayChannel;
        }

        /**
         * Set the stream id for the cluster log and snapshot replay channel.
         *
         * @param streamId for the cluster log replay channel.
         * @return this for a fluent API
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        public Context replayStreamId(final int streamId)
        {
            replayStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for the cluster log and snapshot replay channel.
         *
         * @return the stream id for the cluster log replay channel.
         * @see Configuration#REPLAY_STREAM_ID_PROP_NAME
         */
        @Config
        public int replayStreamId()
        {
            return replayStreamId;
        }

        /**
         * Set the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @param channel parameter for sending messages to the Consensus Module.
         * @return this for a fluent API.
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public Context controlChannel(final String channel)
        {
            controlChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for bidirectional communications between the consensus module and services.
         *
         * @return the channel parameter for sending messages to the Consensus Module.
         * @see Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        @Config
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the stream id for communications from the consensus module and to the services.
         *
         * @param streamId for communications from the consensus module and to the services.
         * @return this for a fluent API
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        public Context serviceStreamId(final int streamId)
        {
            serviceStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the consensus module and to the services.
         *
         * @return the stream id for communications from the consensus module and to the services.
         * @see Configuration#SERVICE_STREAM_ID_PROP_NAME
         */
        @Config
        public int serviceStreamId()
        {
            return serviceStreamId;
        }

        /**
         * Set the stream id for communications from the services to the consensus module.
         *
         * @param streamId for communications from the services to the consensus module.
         * @return this for a fluent API
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        public Context consensusModuleStreamId(final int streamId)
        {
            consensusModuleStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for communications from the services to the consensus module.
         *
         * @return the stream id for communications from the services to the consensus module.
         * @see Configuration#CONSENSUS_MODULE_STREAM_ID_PROP_NAME
         */
        @Config
        public int consensusModuleStreamId()
        {
            return consensusModuleStreamId;
        }

        /**
         * Set the channel parameter for snapshot recordings.
         *
         * @param channel parameter for snapshot recordings
         * @return this for a fluent API.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        public Context snapshotChannel(final String channel)
        {
            snapshotChannel = channel;
            return this;
        }

        /**
         * Get the channel parameter for snapshot recordings.
         *
         * @return the channel parameter for snapshot recordings.
         * @see Configuration#SNAPSHOT_CHANNEL_PROP_NAME
         */
        @Config
        public String snapshotChannel()
        {
            return snapshotChannel;
        }

        /**
         * Set the stream id for snapshot recordings.
         *
         * @param streamId for snapshot recordings.
         * @return this for a fluent API
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        public Context snapshotStreamId(final int streamId)
        {
            snapshotStreamId = streamId;
            return this;
        }

        /**
         * Get the stream id for snapshot recordings.
         *
         * @return the stream id for snapshot recordings.
         * @see Configuration#SNAPSHOT_STREAM_ID_PROP_NAME
         */
        @Config
        public int snapshotStreamId()
        {
            return snapshotStreamId;
        }

        /**
         * Set if this a service that responds to client requests.
         *
         * @param isRespondingService true if this service responds to client requests, otherwise false.
         * @return this for a fluent API.
         * @see Configuration#RESPONDER_SERVICE_PROP_NAME
         */
        public Context isRespondingService(final boolean isRespondingService)
        {
            this.isRespondingService = isRespondingService;
            return this;
        }

        /**
         * Set the fragment limit to be used when polling the log {@link Subscription}.
         *
         * @param logFragmentLimit for this clustered service.
         * @return this for a fluent API
         * @see Configuration#LOG_FRAGMENT_LIMIT_DEFAULT
         */
        public Context logFragmentLimit(final int logFragmentLimit)
        {
            this.logFragmentLimit = logFragmentLimit;
            return this;
        }

        /**
         * Get the fragment limit to be used when polling the log {@link Subscription}.
         *
         * @return the fragment limit to be used when polling the log {@link Subscription}.
         * @see Configuration#LOG_FRAGMENT_LIMIT_PROP_NAME
         */
        @Config
        public int logFragmentLimit()
        {
            return logFragmentLimit;
        }

        /**
         * Is this a service that responds to client requests?
         *
         * @return true if this service responds to client requests, otherwise false.
         * @see Configuration#RESPONDER_SERVICE_PROP_NAME
         */
        @Config(id = "RESPONDER_SERVICE")
        public boolean isRespondingService()
        {
            return isRespondingService;
        }

        /**
         * Get the thread factory used for creating threads.
         *
         * @return thread factory used for creating threads.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads.
         *
         * @param threadFactory used for creating threads
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for the idle strategy for the agent duty cycle.
         *
         * @param idleStrategySupplier supplier for the idle strategy for the agent duty cycle.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} based on configured supplier.
         *
         * @return a new {@link IdleStrategy} based on configured supplier.
         */
        @Config(id = "CLUSTER_IDLE_STRATEGY")
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the container.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the container.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time within the container.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time within the container.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         *
         * @return the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         *
         * @param errorHandler the error handler to be used by the {@link ClusteredServiceContainer}.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer} which will
         * delegate to {@link #errorHandler()} as next in the chain.
         *
         * @return the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer}.
         * @see Configuration#DELEGATING_ERROR_HANDLER_PROP_NAME
         */
        @Config
        public DelegatingErrorHandler delegatingErrorHandler()
        {
            return delegatingErrorHandler;
        }

        /**
         * Set the {@link DelegatingErrorHandler} to be used by the {@link ClusteredServiceContainer} which will
         * delegate to {@link #errorHandler()} as next in the chain.
         *
         * @param delegatingErrorHandler the error handler to be used by the {@link ClusteredServiceContainer}.
         * @return this for a fluent API
         * @see Configuration#DELEGATING_ERROR_HANDLER_PROP_NAME
         */
        public Context delegatingErrorHandler(final DelegatingErrorHandler delegatingErrorHandler)
        {
            this.delegatingErrorHandler = delegatingErrorHandler;
            return this;
        }

        /**
         * Get the error counter that will record the number of errors the container has observed.
         *
         * @return the error counter that will record the number of errors the container has observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors the cluster node has observed.
         *
         * @param errorCounter the error counter that will record the number of errors the cluster node has observed.
         * @return this for a fluent API.
         */
        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
            return this;
        }

        /**
         * Non-default for context.
         *
         * @param countedErrorHandler to override the default.
         * @return this for a fluent API.
         */
        public Context countedErrorHandler(final CountedErrorHandler countedErrorHandler)
        {
            this.countedErrorHandler = countedErrorHandler;
            return this;
        }

        /**
         * The {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         *
         * @return {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         */
        public CountedErrorHandler countedErrorHandler()
        {
            return countedErrorHandler;
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * An {@link Aeron} client for the container.
         *
         * @return {@link Aeron} client for the container
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Provide an {@link Aeron} client for the container
         * <p>
         * If not provided then one will be created.
         *
         * @param aeron client for the container
         * @return this for a fluent API.
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * The service this container holds.
         *
         * @return service this container holds.
         */
        @Config(id = "SERVICE_CLASS_NAME")
        public ClusteredService clusteredService()
        {
            return clusteredService;
        }

        /**
         * Set the service this container is to hold.
         *
         * @param clusteredService this container is to hold.
         * @return this for fluent API.
         */
        public Context clusteredService(final ClusteredService clusteredService)
        {
            this.clusteredService = clusteredService;
            return this;
        }

        /**
         * Set the context that should be used for communicating with the local Archive.
         *
         * @param archiveContext that should be used for communicating with the local Archive.
         * @return this for a fluent API.
         */
        public Context archiveContext(final AeronArchive.Context archiveContext)
        {
            this.archiveContext = archiveContext;
            return this;
        }

        /**
         * Get the context that should be used for communicating with the local Archive.
         *
         * @return the context that should be used for communicating with the local Archive.
         */
        public AeronArchive.Context archiveContext()
        {
            return archiveContext;
        }

        /**
         * Set the directory name to use for the consensus module directory.
         *
         * @param clusterDirectoryName to use.
         * @return this for a fluent API.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDirectoryName(final String clusterDirectoryName)
        {
            this.clusterDirectoryName = clusterDirectoryName;
            return this;
        }

        /**
         * The directory name to use for the cluster directory.
         *
         * @return directory name for the cluster directory.
         * @see Configuration#CLUSTER_DIR_PROP_NAME
         */
        @Config(id = "CLUSTER_DIR")
        public String clusterDirectoryName()
        {
            return clusterDirectoryName;
        }

        /**
         * Set the directory to use for the cluster directory.
         *
         * @param clusterDir to use.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public Context clusterDir(final File clusterDir)
        {
            this.clusterDir = clusterDir;
            return this;
        }

        /**
         * The directory used for the cluster directory.
         *
         * @return directory for the cluster directory.
         * @see ClusteredServiceContainer.Configuration#CLUSTER_DIR_PROP_NAME
         */
        public File clusterDir()
        {
            return clusterDir;
        }

        /**
         * Get the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         * cluster-mark-service-0.dat}). It defaults to {@link #clusterDir()} if it is not set explicitly via the {@link
         * ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME}.
         *
         * @return the directory in which the ClusteredServiceContainer will store mark file (i.e.
         * {@code cluster-mark-service-0.dat}).
         * @see ClusteredServiceContainer.Configuration#MARK_FILE_DIR_PROP_NAME
         * @see #clusterDir()
         */
        @Config
        public File markFileDir()
        {
            return markFileDir;
        }

        /**
         * Set the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         * cluster-mark-service-0.dat}).
         *
         * @param markFileDir the directory in which the ClusteredServiceContainer will store mark file (i.e. {@code
         *                    cluster-mark-service-0.dat}).
         * @return this for a fluent API.
         */
        public ClusteredServiceContainer.Context markFileDir(final File markFileDir)
        {
            this.markFileDir = markFileDir;
            return this;
        }

        /**
         * Set the {@link Runnable} that is called when container is instructed to terminate.
         *
         * @param terminationHook that can be used to terminate a service container.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when container is instructed to terminate.
         *
         * @return the {@link Runnable} that can be used to terminate a service container.
         */
        public Runnable terminationHook()
        {
            return terminationHook;
        }

        /**
         * Set the {@link ClusterMarkFile} in use.
         *
         * @param markFile to use.
         * @return this for a fluent API.
         */
        public Context clusterMarkFile(final ClusterMarkFile markFile)
        {
            this.markFile = markFile;
            return this;
        }

        /**
         * The {@link ClusterMarkFile} in use.
         *
         * @return {@link ClusterMarkFile} in use.
         */
        public ClusterMarkFile clusterMarkFile()
        {
            return markFile;
        }

        /**
         * Set the error buffer length in bytes to use.
         *
         * @param errorBufferLength in bytes to use.
         * @return this for a fluent API.
         */
        public Context errorBufferLength(final int errorBufferLength)
        {
            this.errorBufferLength = errorBufferLength;
            return this;
        }

        /**
         * The error buffer length in bytes.
         *
         * @return error buffer length in bytes.
         */
        @Config(id = "SERVICE_ERROR_BUFFER_LENGTH")
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Set the {@link DistinctErrorLog} in use.
         *
         * @param errorLog to use.
         * @return this for a fluent API.
         */
        public Context errorLog(final DistinctErrorLog errorLog)
        {
            this.errorLog = errorLog;
            return this;
        }

        /**
         * The {@link DistinctErrorLog} in use.
         *
         * @return {@link DistinctErrorLog} in use.
         */
        public DistinctErrorLog errorLog()
        {
            return errorLog;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @return the {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context nanoClock(final NanoClock clock)
        {
            nanoClock = clock;
            return this;
        }

        /**
         * Set a threshold for the container work cycle time which when exceed it will increment the
         * counter.
         *
         * @param thresholdNs value in nanoseconds
         * @return this for fluent API.
         * @see Configuration#CYCLE_THRESHOLD_PROP_NAME
         * @see Configuration#CYCLE_THRESHOLD_DEFAULT_NS
         */
        public Context cycleThresholdNs(final long thresholdNs)
        {
            this.cycleThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for the container work cycle time which when exceed it will increment the
         * counter.
         *
         * @return threshold to track for the container work cycle time.
         */
        @Config(id = "SERVICE_CYCLE_THRESHOLD")
        public long cycleThresholdNs()
        {
            return cycleThresholdNs;
        }

        /**
         * Set a duty cycle tracker to be used for tracking the duty cycle time of the container.
         *
         * @param dutyCycleTracker to use for tracking.
         * @return this for fluent API.
         */
        public Context dutyCycleTracker(final DutyCycleTracker dutyCycleTracker)
        {
            this.dutyCycleTracker = dutyCycleTracker;
            return this;
        }

        /**
         * The duty cycle tracker used to track the container duty cycle.
         *
         * @return the duty cycle tracker.
         */
        public DutyCycleTracker dutyCycleTracker()
        {
            return dutyCycleTracker;
        }

        /**
         * Set a threshold for snapshot duration which when exceeded will result in a counter increment.
         *
         * @param thresholdNs value in nanoseconds.
         * @return this for fluent API.
         * @see Configuration#SNAPSHOT_DURATION_THRESHOLD_PROP_NAME
         * @see Configuration#SNAPSHOT_DURATION_THRESHOLD_DEFAULT_NS
         * @since 1.44.0
         */
        public Context snapshotDurationThresholdNs(final long thresholdNs)
        {
            this.snapshotDurationThresholdNs = thresholdNs;
            return this;
        }

        /**
         * Threshold for snapshot duration which when exceeded will result in a counter increment.
         *
         * @return threshold value in nanoseconds.
         * @since 1.44.0
         */
        @Config
        public long snapshotDurationThresholdNs()
        {
            return snapshotDurationThresholdNs;
        }

        /**
         * Set snapshot duration tracker used for monitoring snapshot duration.
         *
         * @param snapshotDurationTracker snapshot duration tracker.
         * @return this for fluent API.
         * @since 1.44.0
         */
        public Context snapshotDurationTracker(final SnapshotDurationTracker snapshotDurationTracker)
        {
            this.snapshotDurationTracker = snapshotDurationTracker;
            return this;
        }

        /**
         * Get snapshot duration tracker used for monitoring snapshot duration.
         *
         * @return snapshot duration tracker.
         * @since 1.44.0
         */
        public SnapshotDurationTracker snapshotDurationTracker()
        {
            return snapshotDurationTracker;
        }

        /**
         * Delete the cluster container directory.
         */
        public void deleteDirectory()
        {
            if (null != clusterDir)
            {
                IoUtil.delete(clusterDir, false);
            }
        }

        /**
         * Indicates if this node should take standby snapshots.
         *
         * @return <code>true</code> if this should take standby snapshots, <code>false</code> otherwise.
         * @see ClusteredServiceContainer.Configuration#STANDBY_SNAPSHOT_ENABLED_PROP_NAME
         * @see ClusteredServiceContainer.Configuration#standbySnapshotEnabled()
         */
        @Config
        public boolean standbySnapshotEnabled()
        {
            return standbySnapshotEnabled;
        }

        /**
         * Indicates if this node should take standby snapshots.
         *
         * @param standbySnapshotEnabled if this node should take standby snapshots.
         * @return this for a fluent API.
         * @see ClusteredServiceContainer.Configuration#STANDBY_SNAPSHOT_ENABLED_PROP_NAME
         * @see ClusteredServiceContainer.Configuration#standbySnapshotEnabled()
         */
        public ClusteredServiceContainer.Context standbySnapshotEnabled(final boolean standbySnapshotEnabled)
        {
            this.standbySnapshotEnabled = standbySnapshotEnabled;
            return this;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            final ErrorHandler errorHandler = countedErrorHandler();
            if (ownsAeronClient)
            {
                CloseHelper.close(errorHandler, aeron);
            }

            CloseHelper.close(markFile);
        }

        CountDownLatch abortLatch()
        {
            return abortLatch;
        }

        private void concludeMarkFile()
        {
            ClusterMarkFile.checkHeaderLength(
                aeron.context().aeronDirectoryName(), controlChannel(), null, serviceName, null);

            final MarkFileHeaderEncoder encoder = markFile.encoder();

            encoder
                .archiveStreamId(archiveContext.controlRequestStreamId())
                .serviceStreamId(serviceStreamId)
                .consensusModuleStreamId(consensusModuleStreamId)
                .ingressStreamId(Aeron.NULL_VALUE)
                .memberId(Aeron.NULL_VALUE)
                .serviceId(serviceId)
                .clusterId(clusterId)
                .aeronDirectory(aeron.context().aeronDirectoryName())
                .controlChannel(controlChannel)
                .ingressChannel(null)
                .serviceName(serviceName)
                .authenticator(null);

            markFile.signalReady(epochClock.time());
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "ClusteredServiceContainer.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    ownsAeronClient=" + ownsAeronClient +
                "\n    aeronDirectoryName='" + aeronDirectoryName + '\'' +
                "\n    aeron=" + aeron +
                "\n    archiveContext=" + archiveContext +
                "\n    clusterDirectoryName='" + clusterDirectoryName + '\'' +
                "\n    clusterDir=" + clusterDir +
                "\n    appVersion=" + appVersion +
                "\n    clusterId=" + clusterId +
                "\n    serviceId=" + serviceId +
                "\n    serviceName='" + serviceName + '\'' +
                "\n    replayChannel='" + replayChannel + '\'' +
                "\n    replayStreamId=" + replayStreamId +
                "\n    controlChannel='" + controlChannel + '\'' +
                "\n    consensusModuleStreamId=" + consensusModuleStreamId +
                "\n    serviceStreamId=" + serviceStreamId +
                "\n    snapshotChannel='" + snapshotChannel + '\'' +
                "\n    snapshotStreamId=" + snapshotStreamId +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    isRespondingService=" + isRespondingService +
                "\n    logFragmentLimit=" + logFragmentLimit +
                "\n    abortLatch=" + abortLatch +
                "\n    threadFactory=" + threadFactory +
                "\n    idleStrategySupplier=" + idleStrategySupplier +
                "\n    epochClock=" + epochClock +
                "\n    errorLog=" + errorLog +
                "\n    errorHandler=" + errorHandler +
                "\n    delegatingErrorHandler=" + delegatingErrorHandler +
                "\n    errorCounter=" + errorCounter +
                "\n    countedErrorHandler=" + countedErrorHandler +
                "\n    clusteredService=" + clusteredService +
                "\n    terminationHook=" + terminationHook +
                "\n    cycleThresholdNs=" + cycleThresholdNs +
                "\n    dutyCyleTracker=" + dutyCycleTracker +
                "\n    snapshotDurationThresholdNs=" + snapshotDurationThresholdNs +
                "\n    snapshotDurationTracker=" + snapshotDurationTracker +
                "\n    markFile=" + markFile +
                "\n}";
        }
    }
}

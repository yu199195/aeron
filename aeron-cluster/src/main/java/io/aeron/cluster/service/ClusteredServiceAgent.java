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
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.DirectBufferVector;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.driver.Configuration;
import io.aeron.driver.DutyCycleTracker;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.SemanticVersion;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_DEFAULT;
import static io.aeron.cluster.ConsensusModule.CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT;
import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_UPDATE_INTERVAL_NS;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;

abstract class ClusteredServiceAgentLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class ClusteredServiceAgentHotFields extends ClusteredServiceAgentLhsPadding
{
    static final int LIFECYCLE_CALLBACK_NONE = 0;
    static final int LIFECYCLE_CALLBACK_ON_START = 1;
    static final int LIFECYCLE_CALLBACK_ON_TERMINATE = 2;
    static final int LIFECYCLE_CALLBACK_ON_ROLE_CHANGE = 3;
    static final int LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK = 4;

    static String lifecycleName(final int activeLifecycleCallback)
    {
        switch (activeLifecycleCallback)
        {
            case LIFECYCLE_CALLBACK_NONE:
                return "none";
            case LIFECYCLE_CALLBACK_ON_START:
                return "onStart";
            case LIFECYCLE_CALLBACK_ON_TERMINATE:
                return "onTerminate";
            case LIFECYCLE_CALLBACK_ON_ROLE_CHANGE:
                return "onRoleChange";
            case LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK:
                return "doBackgroundWork";
            default:
                return "unknown";
        }
    }

    int activeLifecycleCallback;
}

abstract class ClusteredServiceAgentRhsPadding extends ClusteredServiceAgentHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

final class ClusteredServiceAgent extends ClusteredServiceAgentRhsPadding implements Agent, Cluster, IdleStrategy
{
    private static final long ONE_MILLISECOND_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final long MARK_FILE_UPDATE_INTERVAL_MS =
        TimeUnit.NANOSECONDS.toMillis(MARK_FILE_UPDATE_INTERVAL_NS);

    private volatile boolean isAbort;
    private boolean isServiceActive;
    private final int serviceId;
    private int memberId = NULL_VALUE;
    private long closeHandlerRegistrationId;
    private long ackId = 0;
    private long terminationPosition = NULL_POSITION;
    private long markFileUpdateDeadlineMs;
    private long lastSlowTickNs;
    private long clusterTime;
    private long logPosition = NULL_POSITION;

    private final IdleStrategy idleStrategy;
    private final ClusterMarkFile markFile;
    private final ClusteredServiceContainer.Context ctx;
    private final Aeron aeron;
    private final AgentInvoker aeronAgentInvoker;
    private final ClusteredService service;
    private final ConsensusModuleProxy consensusModuleProxy;
    private final ServiceAdapter serviceAdapter;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final UnsafeBuffer messageBuffer = new UnsafeBuffer(new byte[Configuration.MAX_UDP_PAYLOAD_LENGTH]);
    private final UnsafeBuffer headerBuffer = new UnsafeBuffer(
        messageBuffer,
        DataHeaderFlyweight.HEADER_LENGTH,
        Configuration.MAX_UDP_PAYLOAD_LENGTH - DataHeaderFlyweight.HEADER_LENGTH);
    private final DirectBufferVector headerVector = new DirectBufferVector(headerBuffer, 0, SESSION_HEADER_LENGTH);
    private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
    private final ArrayList<ContainerClientSession> sessions = new ArrayList<>();
    private final Long2ObjectHashMap<ContainerClientSession> sessionByIdMap = new Long2ObjectHashMap<>();
    private final Collection<ClientSession> unmodifiableClientSessions = Collections.unmodifiableCollection(sessions);
    private final BoundedLogAdapter logAdapter;
    private final DutyCycleTracker dutyCycleTracker;
    private final SnapshotDurationTracker snapshotDurationTracker;
    private final String subscriptionAlias;
    private final int standbySnapshotFlags;

    private ReadableCounter commitPosition;
    private ActiveLogEvent activeLogEvent;
    private Role role = Role.FOLLOWER;
    private TimeUnit timeUnit = null;
    private long requestedAckPosition = NULL_POSITION;

    /**
     * ClusteredServiceAgent 构造函数：创建 Service 的核心代理。
     * <p>
     * 这是 ClusteredServiceContainer 的核心组件，负责：
     * <ul>
     *   <li>接收 ConsensusModule 的命令（通过 serviceAdapter）</li>
     *   <li>消费日志，调用 ClusteredService 回调方法</li>
     *   <li>管理客户端会话（创建、关闭、消息发送）</li>
     *   <li>执行快照保存和加载</li>
     *   <li>与 ConsensusModule 双向通信</li>
     * </ul>
     * <p>
     * <b>通信架构</b>：
     * <pre>
     * ConsensusModule                          ClusteredServiceAgent
     *        │                                          │
     *        ├─> serviceControlPublication             │
     *        │        └─────────────────────────────> serviceAdapter (订阅)
     *        │                                          │
     *        │   发送命令：                              │   接收命令：
     *        │   ├─ JOIN_LOG                           ├─ onJoinLog()
     *        │   ├─ SERVICE_TERMINATION_POSITION       ├─ onServiceTerminationPosition()
     *        │   └─ REQUEST_SERVICE_ACK                └─ onRequestServiceAck()
     *        │                                          │
     *        │ <──────────────────────────────────── consensusModuleProxy (发布)
     *        │                                          │
     *        └─ 接收 ACK：                             └─> 发送 ACK：
     *           - SERVICE_ACK                              - consensusModuleProxy.ack()
     * </pre>
     *
     * @param ctx ClusteredServiceContainer 配置上下文（已完成 conclude()）
     */
    ClusteredServiceAgent(final ClusteredServiceContainer.Context ctx)
    {
        // ==================== 步骤 1: 创建日志适配器 ====================
        // logAdapter 用于消费 Leader 发布的日志消息
        // - ctx.logFragmentLimit(): 每次 poll() 最多处理的消息数量（默认 50）
        logAdapter = new BoundedLogAdapter(this, ctx.logFragmentLimit());

        // ==================== 步骤 2: 保存配置引用 ====================
        this.ctx = ctx;

        // ==================== 步骤 3: 初始化基础组件 ====================
        markFile = ctx.clusterMarkFile();  // ← Mark File（进程协调、活跃性检测）
        aeron = ctx.aeron();  // ← Aeron 客户端（用于通信）
        aeronAgentInvoker = ctx.aeron().conductorAgentInvoker();  // ← Aeron 代理调用器（用于手动驱动）
        service = ctx.clusteredService();  // ← 业务逻辑实现（ClusteredService 接口）
        idleStrategy = ctx.idleStrategy();  // ← 空闲策略（无工作时降低 CPU 占用）
        serviceId = ctx.serviceId();  // ← Service ID（0-9，每个节点可运行多个 Service）
        epochClock = ctx.epochClock();  // ← 纪元时钟（用于时间戳）
        nanoClock = ctx.nanoClock();  // ← 纳秒时钟（用于高精度计时）
        dutyCycleTracker = ctx.dutyCycleTracker();  // ← Duty Cycle 追踪器（监控 doWork() 耗时）
        snapshotDurationTracker = ctx.snapshotDurationTracker();  // ← 快照耗时追踪器（监控快照性能）
        subscriptionAlias = "log-sc-" + ctx.serviceId();  // ← 日志订阅别名（便于识别）

        // ==================== 步骤 4: 创建与 ConsensusModule 的双向通信通道 ====================
        final String channel = ctx.controlChannel();  // ← 控制通道（IPC，通常是 "aeron:ipc?term-length=128k"）

        // 4.1 创建 ConsensusModule Publication（Service → ConsensusModule）
        // 用途：向 ConsensusModule 发送 ACK 消息
        // - SERVICE_ACK: 确认命令执行完成
        // - SNAPSHOT_COMPLETE: 确认快照保存完成
        consensusModuleProxy = new ConsensusModuleProxy(aeron.addPublication(channel, ctx.consensusModuleStreamId()));

        // 4.2 创建 Service Subscription（ConsensusModule → Service）
        // 用途：接收 ConsensusModule 发送的命令
        // - JOIN_LOG: 加入日志消费
        // - SERVICE_TERMINATION_POSITION: 终止位置
        // - REQUEST_SERVICE_ACK: 请求确认
        serviceAdapter = new ServiceAdapter(aeron.addSubscription(channel, ctx.serviceStreamId()), this);

        // ==================== 步骤 5: 初始化会话消息编码器 ====================
        // 用于向客户端发送响应消息时的头部编码
        sessionMessageHeaderEncoder.wrapAndApplyHeader(headerBuffer, 0, new MessageHeaderEncoder());

        // ==================== 步骤 6: 设置快照标志 ====================
        // standbySnapshotEnabled: 是否在 Follower 状态下也执行快照
        // - true: CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT（Follower 也快照）
        // - false: CLUSTER_ACTION_FLAGS_DEFAULT（只有 Leader 快照）
        this.standbySnapshotFlags = ctx.standbySnapshotEnabled() ? CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT :
            CLUSTER_ACTION_FLAGS_DEFAULT;
    }

    /**
     * Agent 启动回调：在 AgentRunner 启动线程后调用。
     * <p>
     * 这是 ClusteredServiceAgent 的初始化入口，负责：
     * <ul>
     *   <li>注册关闭和计数器不可用处理器</li>
     *   <li>等待 commit position 计数器就绪</li>
     *   <li>恢复 Service 状态（加载快照、调用 service.onStart()）</li>
     *   <li>向 ConsensusModule 发送就绪 ACK</li>
     * </ul>
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 注册处理器（关闭处理器、计数器不可用处理器）
     * 2. 等待 commit position 计数器（ConsensusModule 创建）
     * 3. 恢复状态：
     *    ├─ 读取 Recovery State 计数器
     *    ├─ 加载快照（如果存在）
     *    ├─ 调用 service.onStart(this, snapshotImage)
     *    └─ 发送就绪 ACK 给 ConsensusModule
     * 4. 标记 Service 为活跃状态
     * </pre>
     * <p>
     * <b>注意</b>：此方法在 Agent 线程上执行，不是主线程。
     */
    public void onStart()
    {
        // ==================== 步骤 1: 注册关闭处理器 ====================
        // 当 Aeron 客户端关闭时，会触发 abort() 方法
        closeHandlerRegistrationId = aeron.addCloseHandler(this::abort);

        // ==================== 步骤 2: 注册计数器不可用处理器 ====================
        // 当 commit position 计数器关闭时，Service 需要终止
        aeron.addUnavailableCounterHandler(this::counterUnavailable);

        // ==================== 步骤 3: 等待 commit position 计数器 ====================
        // commit position 由 ConsensusModule 创建，Service 需要等待其就绪
        // 用途：Service 只消费已提交的日志消息（不消费未提交的）
        final CountersReader counters = aeron.countersReader();
        commitPosition = awaitCommitPositionCounter(counters, ctx.clusterId());

        // ==================== 步骤 4: 恢复 Service 状态 ====================
        // recoverState() 会：
        // - 读取 Recovery State 计数器（包含 logPosition、timestamp、leadershipTermId）
        // - 加载最新快照（如果存在）
        // - 调用 service.onStart(this, snapshotImage)（业务逻辑恢复状态）
        // - 发送就绪 ACK 给 ConsensusModule
        recoverState(counters);

        // ==================== 步骤 5: 初始化 Duty Cycle Tracker ====================
        // 开始追踪 doWork() 循环的耗时
        dutyCycleTracker.update(nanoClock.nanoTime());

        // ==================== 步骤 6: 标记 Service 为活跃状态 ====================
        // 此后可以开始接收命令和消费日志
        isServiceActive = true;
    }

    public void onClose()
    {
        aeron.removeCloseHandler(closeHandlerRegistrationId);

        if (isAbort)
        {
            ctx.abortLatch().countDown();
        }
        else
        {
            final CountedErrorHandler errorHandler = ctx.countedErrorHandler();
            if (isServiceActive)
            {
                isServiceActive = false;
                try
                {
                    service.onTerminate(this);
                }
                catch (final Exception ex)
                {
                    errorHandler.onError(ex);
                }
            }

            CloseHelper.close(errorHandler, logAdapter);

            if (!ctx.ownsAeronClient() && !aeron.isClosed())
            {
                CloseHelper.close(errorHandler, serviceAdapter);
                CloseHelper.close(errorHandler, consensusModuleProxy);
                disconnectEgress(errorHandler);
            }
        }

        markFile.signalTerminated();
        ctx.close();
    }

    /**
     * Agent 主工作循环：执行 Service 的核心业务逻辑。
     * <p>
     * 这是 ClusteredServiceAgent 的<b>核心方法</b>，在独立线程上持续调用（约 1000 次/秒）。
     * 负责：
     * <ul>
     *   <li>接收 ConsensusModule 的命令（通过 pollServiceAdapter()）</li>
     *   <li>消费已提交的日志消息（通过 logAdapter.poll()）</li>
     *   <li>调用业务逻辑的后台工作（service.doBackgroundWork()）</li>
     *   <li>更新 Mark File 活跃时间戳（用于活跃性检测）</li>
     * </ul>
     * <p>
     * <b>执行频率</b>：
     * <pre>
     * while (running) {
     *     workCount = agent.doWork();  // ← 本方法
     *     if (workCount == 0) {
     *         idleStrategy.idle();  // ← 无工作时降低 CPU 占用
     *     } else {
     *         idleStrategy.reset();  // ← 有工作时保持高效率
     *     }
     * }
     * </pre>
     * <p>
     * <b>工作流程</b>：
     * <pre>
     * 1. 更新 Duty Cycle Tracker（追踪循环耗时）
     * 2. 检查时钟 tick（每 1ms 执行一次）：
     *    ├─ 调用 Aeron AgentInvoker（手动驱动 Aeron）
     *    ├─ 更新 Mark File 时间戳（每秒更新一次）
     *    └─ 接收 ConsensusModule 命令：
     *       ├─ JOIN_LOG → joinActiveLog()（订阅日志）
     *       ├─ SERVICE_TERMINATION_POSITION → terminate()（终止 Service）
     *       └─ REQUEST_SERVICE_ACK → consensusModuleProxy.ack()（发送确认）
     * 3. 消费日志（如果已订阅）：
     *    └─ logAdapter.poll(commitPosition) → 调用 ClusteredService 回调：
     *       ├─ onSessionMessage()（客户端消息）
     *       ├─ onTimerEvent()（定时器事件）
     *       ├─ onSessionOpen()（会话打开）
     *       ├─ onSessionClose()（会话关闭）
     *       ├─ onServiceAction()（集群动作，如 SNAPSHOT）
     *       └─ onNewLeadershipTermEvent()（新 Leader 上任）
     * 4. 调用后台工作：
     *    └─ service.doBackgroundWork(nowNs)（业务逻辑自定义）
     * </pre>
     *
     * @return 本次执行的工作量（0 表示无工作，>0 表示有工作完成）
     */
    public int doWork()
    {
        // ==================== 工作量计数器 ====================
        // 用于 idle 策略判断：
        // - 0: 无工作，调用 idleStrategy.idle()（降低 CPU 占用）
        // - >0: 有工作，调用 idleStrategy.reset()（保持高效率）
        int workCount = 0;

        // ==================== 记录当前时间 ====================
        final long nowNs = nanoClock.nanoTime();

        // ==================== 更新 Duty Cycle Tracker ====================
        // 追踪本次循环的开始时间，用于监控性能
        dutyCycleTracker.measureAndUpdate(nowNs);

        try
        {
            // ==================== 步骤 1: 检查时钟 tick（每 1ms 执行一次） ====================
            // checkForClockTick() 会：
            // - 检查 Aeron 是否关闭（关闭则抛出异常）
            // - 调用 Aeron AgentInvoker（手动驱动 Aeron 事件循环）
            // - 更新 Mark File 时间戳（每秒更新一次，供监控工具检测活跃性）
            // - 返回 true（表示本次是 tick 时刻）
            if (checkForClockTick(nowNs))
            {
                // ==================== 步骤 2: 接收 ConsensusModule 命令 ====================
                // pollServiceAdapter() 会：
                // - 从 serviceAdapter 接收命令（JOIN_LOG、SERVICE_TERMINATION_POSITION、REQUEST_SERVICE_ACK）
                // - 处理 activeLogEvent（如果有，调用 joinActiveLog()）
                // - 检查终止位置（如果达到，调用 terminate()）
                // - 检查请求的 ACK 位置（如果达到，发送 ACK）
                workCount += pollServiceAdapter();
            }

            // ==================== 步骤 3: 消费日志消息 ====================
            if (null != logAdapter.image())  // ← 检查是否已订阅日志
            {
                // 从日志中消费消息（最多 commitPosition.get() 位置）
                // commitPosition 由 ConsensusModule 更新，表示已提交的位置
                // Service 只消费已提交的消息（确保一致性）
                final int polled = logAdapter.poll(commitPosition.get());
                workCount += polled;

                // 如果本次没有消费到消息，且日志已结束，关闭日志
                if (0 == polled && logAdapter.isDone())
                {
                    closeLog();  // ← 关闭日志订阅，断开客户端会话连接
                }
            }

            // ==================== 步骤 4: 调用业务逻辑的后台工作 ====================
            // service.doBackgroundWork() 可用于：
            // - 执行定期任务（如统计、清理）
            // - 处理异步操作
            // - 自定义监控逻辑
            workCount += invokeBackgroundWork(nowNs);
        }
        catch (final AgentTerminationException ex)
        {
            // ==================== 异常处理：Agent 终止 ====================
            // 执行终止钩子（用户自定义清理逻辑）
            runTerminationHook();
            // 重新抛出异常，让 AgentRunner 停止循环
            throw ex;
        }

        // ==================== 返回工作量 ====================
        // AgentRunner 根据 workCount 决定是否执行 idle 策略
        return workCount;
    }

    public String roleName()
    {
        return ctx.serviceName();
    }

    public Cluster.Role role()
    {
        return role;
    }

    public int memberId()
    {
        return memberId;
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClusteredServiceContainer.Context context()
    {
        return ctx;
    }

    public ClientSession getClientSession(final long clusterSessionId)
    {
        return sessionByIdMap.get(clusterSessionId);
    }

    public Collection<ClientSession> clientSessions()
    {
        return unmodifiableClientSessions;
    }

    public void forEachClientSession(final Consumer<? super ClientSession> action)
    {
        sessions.forEach(action);
    }

    public boolean closeClientSession(final long clusterSessionId)
    {
        checkForValidInvocation();

        final ContainerClientSession clientSession = sessionByIdMap.get(clusterSessionId);
        if (clientSession == null)
        {
            throw new ClusterException("unknown clusterSessionId: " + clusterSessionId);
        }

        if (clientSession.isClosing())
        {
            return true;
        }

        int attempts = 3;
        do
        {
            if (consensusModuleProxy.closeSession(clusterSessionId))
            {
                clientSession.markClosing();
                return true;
            }
            idle();
        }
        while (--attempts > 0);

        return false;
    }

    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    public long time()
    {
        return clusterTime;
    }

    public long logPosition()
    {
        return logPosition;
    }

    public boolean scheduleTimer(final long correlationId, final long deadline)
    {
        checkForValidInvocation();

        return consensusModuleProxy.scheduleTimer(correlationId, deadline);
    }

    public boolean cancelTimer(final long correlationId)
    {
        checkForValidInvocation();

        return consensusModuleProxy.cancelTimer(correlationId);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());

        return consensusModuleProxy.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length);
    }

    public long offer(final DirectBufferVector[] vectors)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());
        vectors[0] = headerVector;

        return consensusModuleProxy.offer(vectors);
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkForValidInvocation();
        sessionMessageHeaderEncoder.clusterSessionId(context().serviceId());

        return consensusModuleProxy.tryClaim(length + SESSION_HEADER_LENGTH, bufferClaim, headerBuffer);
    }

    public IdleStrategy idleStrategy()
    {
        return this;
    }

    public void reset()
    {
        idleStrategy.reset();
    }

    public void idle()
    {
        idleStrategy.idle();
        doIdleWork();
    }

    public void idle(final int workCount)
    {
        idleStrategy.idle(workCount);
        if (workCount <= 0)
        {
            doIdleWork();
        }
    }

    private void doIdleWork()
    {
        if (Thread.currentThread().isInterrupted())
        {
            throw new AgentTerminationException("interrupted");
        }

        final long nowNs = nanoClock.nanoTime();

        checkForClockTick(nowNs);

        if (isServiceActive)
        {
            invokeBackgroundWork(nowNs);
        }
    }

    void onJoinLog(
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int logSessionId,
        final int logStreamId,
        final boolean isStartup,
        final Cluster.Role role,
        final String logChannel)
    {
        logAdapter.maxLogPosition(logPosition);
        activeLogEvent = new ActiveLogEvent(
            logPosition,
            maxLogPosition,
            memberId,
            logSessionId,
            logStreamId,
            isStartup,
            role,
            logChannel);
    }

    void onServiceTerminationPosition(final long logPosition)
    {
        terminationPosition = logPosition;
    }

    void onRequestServiceAck(final long logPosition)
    {
        requestedAckPosition = logPosition;
    }

    void onSessionMessage(
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        final ClientSession clientSession = sessionByIdMap.get(clusterSessionId);

        service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header);
    }

    void onTimerEvent(final long logPosition, final long correlationId, final long timestamp)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        service.onTimerEvent(correlationId, timestamp);
    }

    void onSessionOpen(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        if (sessionByIdMap.containsKey(clusterSessionId))
        {
            throw new ClusterException("clashing open clusterSessionId=" + clusterSessionId +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition);
        }

        final ContainerClientSession session = new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        if (Role.LEADER == role && ctx.isRespondingService())
        {
            session.connect(aeron);
        }

        addSession(session);
        service.onSessionOpen(session, timestamp);
    }

    void onSessionClose(
        final long leadershipTermId,
        final long logPosition,
        final long clusterSessionId,
        final long timestamp,
        final CloseReason closeReason)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;

        final ContainerClientSession session = sessionByIdMap.remove(clusterSessionId);
        if (null == session)
        {
            ctx.countedErrorHandler().onError(new ClusterEvent(
                "unknown session close: clusterSessionId=" + clusterSessionId + " closeReason=" + closeReason +
                " leadershipTermId=" + leadershipTermId + " logPosition=" + logPosition));
        }
        else
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                if (sessions.get(i).id() == clusterSessionId)
                {
                    sessions.remove(i);
                    break;
                }
            }

            session.disconnect(ctx.countedErrorHandler());
            service.onSessionClose(session, timestamp, closeReason);
        }
    }

    void onServiceAction(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final ClusterAction action,
        final int flags)
    {
        this.logPosition = logPosition;
        clusterTime = timestamp;
        executeAction(action, logPosition, leadershipTermId, flags);
    }

    void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            ctx.countedErrorHandler().onError(new ClusterException(
                "incompatible version: " + SemanticVersion.toString(ctx.appVersion()) +
                " log=" + SemanticVersion.toString(appVersion)));
            throw new AgentTerminationException();
        }

        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);
        this.logPosition = logPosition;
        clusterTime = timestamp;
        this.timeUnit = timeUnit;

        service.onNewLeadershipTermEvent(
            leadershipTermId,
            logPosition,
            timestamp,
            termBaseLogPosition,
            leaderMemberId,
            logSessionId,
            timeUnit,
            appVersion);
    }

    void addSession(
        final long clusterSessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal)
    {
        final ContainerClientSession session = new ContainerClientSession(
            clusterSessionId, responseStreamId, responseChannel, encodedPrincipal, this);

        addSession(session);
    }

    private void addSession(final ContainerClientSession session)
    {
        final long clusterSessionId = session.id();
        sessionByIdMap.put(clusterSessionId, session);

        final int size = sessions.size();
        int addIndex = size;
        for (int i = size - 1; i >= 0; i--)
        {
            if (sessions.get(i).id() < clusterSessionId)
            {
                addIndex = i + 1;
                break;
            }
        }

        if (size == addIndex)
        {
            sessions.add(session);
        }
        else
        {
            sessions.add(addIndex, session);
        }
    }

    void handleError(final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
    }

    long offer(
        final long clusterSessionId,
        final Publication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        return publication.offer(headerBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);
    }

    long offer(final long clusterSessionId, final Publication publication, final DirectBufferVector[] vectors)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        sessionMessageHeaderEncoder
            .clusterSessionId(clusterSessionId)
            .timestamp(clusterTime);

        vectors[0] = headerVector;

        return publication.offer(vectors, null);
    }

    long tryClaim(
        final long clusterSessionId,
        final Publication publication,
        final int length,
        final BufferClaim bufferClaim)
    {
        checkForValidInvocation();

        if (Cluster.Role.LEADER != role)
        {
            final int maxPayloadLength = headerBuffer.capacity() - SESSION_HEADER_LENGTH;
            if (length > maxPayloadLength)
            {
                throw new IllegalArgumentException(
                    "claim exceeds maxPayloadLength=" + maxPayloadLength + ", length=" + length);
            }

            bufferClaim.wrap(
                messageBuffer, 0, DataHeaderFlyweight.HEADER_LENGTH + SESSION_HEADER_LENGTH + length);
            return ClientSession.MOCKED_OFFER;
        }

        if (null == publication)
        {
            return Publication.NOT_CONNECTED;
        }

        final long offset = publication.tryClaim(SESSION_HEADER_LENGTH + length, bufferClaim);
        if (offset > 0)
        {
            sessionMessageHeaderEncoder
                .clusterSessionId(clusterSessionId)
                .timestamp(clusterTime);

            bufferClaim.putBytes(headerBuffer, 0, SESSION_HEADER_LENGTH);
        }

        return offset;
    }

    private void role(final Role newRole)
    {
        if (newRole != role)
        {
            role = newRole;
            activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_ROLE_CHANGE;
            try
            {
                service.onRoleChange(newRole);
            }
            finally
            {
                activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
            }
        }
    }

    /**
     * 恢复 Service 状态：从快照加载业务状态，调用 service.onStart()。
     * <p>
     * 这是 Service 启动的核心逻辑，负责：
     * <ul>
     *   <li>读取 Recovery State 计数器（ConsensusModule 创建）</li>
     *   <li>从快照恢复业务状态（如果存在）</li>
     *   <li>调用 service.onStart() 让业务逻辑完成初始化</li>
     *   <li>向 ConsensusModule 发送就绪 ACK</li>
     * </ul>
     * <p>
     * <b>Recovery State 计数器</b>：
     * <pre>
     * ConsensusModule 在启动时创建 Recovery State 计数器，包含：
     * - logPosition: Service 应该从哪个位置开始消费日志
     * - timestamp: 集群的当前时间
     * - leadershipTermId: 当前 Leadership Term ID
     * - snapshotRecordingId[]: 每个 Service 的快照 Recording ID（数组长度 = MAX_SERVICE_COUNT）
     * </pre>
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 等待 Recovery State 计数器就绪（ConsensusModule 创建）
     * 2. 从计数器读取恢复信息：
     *    ├─ logPosition（日志位置）
     *    ├─ timestamp（集群时间）
     *    ├─ leadershipTermId（Leadership Term ID）
     *    └─ snapshotRecordingId（快照 Recording ID）
     * 3. 加载快照（如果存在）：
     *    ├─ 连接 AeronArchive
     *    ├─ 开始回放快照（archive.startReplay()）
     *    ├─ 从 Image 读取快照数据
     *    │   ├─ 恢复会话列表（sessions）
     *    │   └─ 恢复 timeUnit
     *    └─ 调用 service.onStart(this, snapshotImage)
     * 4. 发送就绪 ACK 给 ConsensusModule
     * </pre>
     *
     * @param counters Aeron 计数器读取器
     */
    private void recoverState(final CountersReader counters)
    {
        // ==================== 步骤 1: 等待 Recovery State 计数器 ====================
        // ConsensusModule 在启动时创建 Recovery State 计数器
        // Service 需要等待其就绪才能开始恢复
        final int recoveryCounterId = awaitRecoveryCounter(counters);

        // ==================== 步骤 2: 读取恢复信息 ====================

        // 2.1 读取日志位置（Service 应该从哪个位置开始消费）
        // - 如果是首次启动：logPosition = 0
        // - 如果是重启：logPosition = 上次快照时的日志位置
        logPosition = RecoveryState.getLogPosition(counters, recoveryCounterId);

        // 2.2 读取集群时间（用于时间戳）
        clusterTime = RecoveryState.getTimestamp(counters, recoveryCounterId);

        // 2.3 读取 Leadership Term ID（用于消息头编码）
        // - NULL_VALUE: 表示集群首次启动，没有历史数据
        // - 非 NULL_VALUE: 表示集群重启，有历史快照
        final long leadershipTermId = RecoveryState.getLeadershipTermId(counters, recoveryCounterId);
        sessionMessageHeaderEncoder.leadershipTermId(leadershipTermId);

        // ==================== 步骤 3: 调用 service.onStart() ====================
        // activeLifecycleCallback 用于检测非法调用：
        // - 在 onStart() 中不允许发送消息或调度定时器
        activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_START;
        try
        {
            if (NULL_VALUE != leadershipTermId)  // ← 有历史数据，需要加载快照
            {
                // ==================== 步骤 3.1: 加载快照 ====================
                // 从 Archive 加载快照并恢复状态：
                // - 读取 snapshotRecordingId（每个 Service 有独立的快照）
                // - 连接 AeronArchive，开始回放
                // - 恢复会话列表（sessions）、timeUnit
                // - 调用 service.onStart(this, snapshotImage)（业务逻辑恢复状态）
                loadSnapshot(RecoveryState.getSnapshotRecordingId(counters, recoveryCounterId, serviceId));
            }
            else  // ← 首次启动，没有快照
            {
                // ==================== 步骤 3.2: 首次启动 ====================
                // 直接调用 service.onStart(this, null)
                // - null: 表示没有快照，业务逻辑需要从头初始化
                service.onStart(this, null);
            }
        }
        finally
        {
            // 恢复生命周期回调标志
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
        }

        // ==================== 步骤 4: 发送就绪 ACK 给 ConsensusModule ====================
        // 通知 ConsensusModule：Service 已就绪，可以开始发送命令
        // ACK 消息包含：
        // - logPosition: Service 当前的日志位置
        // - clusterTime: Service 当前的集群时间
        // - ackId: 确认 ID（递增）
        // - aeron.clientId(): Aeron 客户端 ID
        // - serviceId: Service ID
        final long id = ackId++;
        while (!consensusModuleProxy.ack(logPosition, clusterTime, id, aeron.clientId(), serviceId))
        {
            idle();  // ← 如果发送失败，重试（可能是 ConsensusModule 还未准备好）
        }
    }

    private int awaitRecoveryCounter(final CountersReader counters)
    {
        idleStrategy.reset();
        int counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = RecoveryState.findCounterId(counters, ctx.clusterId());
        }

        return counterId;
    }

    private void closeLog()
    {
        logPosition = Math.max(logAdapter.image().position(), logPosition);
        CloseHelper.close(ctx.countedErrorHandler(), logAdapter);
        disconnectEgress(ctx.countedErrorHandler());
        role(Role.FOLLOWER);
    }

    private void disconnectEgress(final CountedErrorHandler errorHandler)
    {
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            sessions.get(i).disconnect(errorHandler);
        }
    }

    private void joinActiveLog(final ActiveLogEvent activeLog)
    {
        if (Role.LEADER != activeLog.role)
        {
            disconnectEgress(ctx.countedErrorHandler());
        }

        final String channel = new ChannelUriStringBuilder(activeLog.channel)
            .alias(subscriptionAlias)
            .build();

        Subscription logSubscription = aeron.addSubscription(channel, activeLog.streamId);
        try
        {
            final Image image = awaitImage(activeLog.sessionId, logSubscription);
            if (image.joinPosition() != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for joining image: " +
                    "expectedPosition=" + logPosition + " joinPosition=" + image.joinPosition());
            }

            if (activeLog.logPosition != logPosition)
            {
                throw new ClusterException("Cluster log must be contiguous for active log event: " +
                    "expectedPosition=" + logPosition + " eventPosition=" + activeLog.logPosition);
            }

            logAdapter.image(image);
            logAdapter.maxLogPosition(activeLog.maxLogPosition);
            logSubscription = null;

            final long id = ackId++;
            while (!consensusModuleProxy.ack(activeLog.logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
        }
        finally
        {
            CloseHelper.quietClose(logSubscription);
        }

        memberId = activeLog.memberId;
        markFile.memberId(memberId);

        if (Role.LEADER == activeLog.role)
        {
            for (int i = 0, size = sessions.size(); i < size; i++)
            {
                final ContainerClientSession session = sessions.get(i);

                if (ctx.isRespondingService() && !activeLog.isStartup)
                {
                    session.connect(aeron);
                }

                session.resetClosing();
            }
        }

        role(activeLog.role);
    }

    private Image awaitImage(final int sessionId, final Subscription subscription)
    {
        idleStrategy.reset();
        Image image;
        while ((image = subscription.imageBySessionId(sessionId)) == null)
        {
            idle();
        }

        return image;
    }

    private ReadableCounter awaitCommitPositionCounter(final CountersReader counters, final int clusterId)
    {
        idleStrategy.reset();
        int counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            counterId = ClusterCounters.find(counters, COMMIT_POSITION_TYPE_ID, clusterId);
        }

        return new ReadableCounter(counters, counters.getCounterRegistrationId(counterId), counterId);
    }

    private void loadSnapshot(final long recordingId)
    {
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone()))
        {
            final String channel = ctx.replayChannel();
            final int streamId = ctx.replayStreamId();
            final int sessionId = (int)archive.startReplay(recordingId, 0, NULL_VALUE, channel, streamId);

            final String replaySessionChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replaySessionChannel, streamId))
            {
                final Image image = awaitImage(sessionId, subscription);
                loadState(image, archive);
                service.onStart(this, image);
            }
        }
    }

    private void loadState(final Image image, final AeronArchive archive)
    {
        final ServiceSnapshotLoader snapshotLoader = new ServiceSnapshotLoader(image, this);
        while (true)
        {
            final int fragments = snapshotLoader.poll();
            if (snapshotLoader.isDone())
            {
                break;
            }

            if (0 == fragments)
            {
                archive.checkForErrorResponse();
                if (image.isClosed())
                {
                    throw new ClusterException("snapshot ended unexpectedly: " + image);
                }
            }

            idle(fragments);
        }

        final int appVersion = snapshotLoader.appVersion();
        if (!ctx.appVersionValidator().isVersionCompatible(ctx.appVersion(), appVersion))
        {
            throw new ClusterException(
                "incompatible app version: " + SemanticVersion.toString(ctx.appVersion()) +
                " snapshot=" + SemanticVersion.toString(appVersion));
        }

        timeUnit = snapshotLoader.timeUnit();
    }

    /**
     * 执行快照保存：将集群状态和业务状态保存到 AeronArchive。
     * <p>
     * 这是快照保存的<b>核心方法</b>，负责：
     * <ul>
     *   <li>连接 AeronArchive（快照录制引擎）</li>
     *   <li>创建快照 publication（用于写入快照数据）</li>
     *   <li>录制快照到磁盘（零拷贝，高性能）</li>
     *   <li>保存集群状态（会话列表）</li>
     *   <li>调用业务逻辑保存自定义状态（service.onTakeSnapshot()）</li>
     *   <li>等待录制完成并返回 recordingId</li>
     * </ul>
     * <p>
     * <b>快照保存流程</b>：
     * <pre>
     * 1. 连接 AeronArchive（快照录制引擎）
     * 2. 创建快照 publication（IPC 通道，高性能）
     * 3. 开始录制快照到磁盘
     *    - archive.startRecording() → 后台录制
     *    - 使用零拷贝机制，不阻塞主线程
     * 4. 等待录制计数器就绪
     *    - RecordingPos 计数器用于跟踪录制进度
     * 5. 保存快照内容
     *    ├─ snapshotState() → 保存集群状态（会话列表）
     *    └─ service.onTakeSnapshot() → 保存业务状态（自定义数据）
     * 6. 等待录制完成
     *    - 轮询 RecordingPos 计数器，等待所有数据写入磁盘
     * 7. 返回 recordingId
     *    - 用于恢复时定位快照文件
     * </pre>
     * <p>
     * <b>重要特性</b>：
     * <ul>
     *   <li><b>非阻塞</b>：快照在后台录制，主线程继续处理消息</li>
     *   <li><b>零拷贝</b>：使用 Aeron 零拷贝机制，高性能</li>
     *   <li><b>原子性</b>：快照要么完全成功，要么失败（不会出现部分快照）</li>
     *   <li><b>可恢复</b>：recordingId 用于恢复时定位快照文件</li>
     * </ul>
     * <p>
     * <b>异常处理</b>：
     * <ul>
     *   <li>STORAGE_SPACE: 磁盘空间不足 → 终止 Agent（无法恢复）</li>
     *   <li>其他异常：重新抛出，由调用者处理</li>
     * </ul>
     *
     * @param logPosition 快照对应的日志位置（快照完成后，此位置之前的日志可以删除）
     * @param leadershipTermId 当前 Leadership Term ID
     * @return recordingId 快照的 Recording ID（用于恢复）
     * @throws AgentTerminationException 磁盘空间不足
     * @throws ArchiveException Archive 操作失败
     */
    private long onTakeSnapshot(final long logPosition, final long leadershipTermId)
    {
        // ==================== 步骤 1: 连接 AeronArchive 和创建快照 Publication ====================
        // 使用 try-with-resources 确保资源正确关闭
        // - AeronArchive: 快照录制引擎（负责将数据写入磁盘）
        // - ExclusivePublication: 快照 publication（用于写入快照数据）
        try (AeronArchive archive = AeronArchive.connect(ctx.archiveContext().clone());
            ExclusivePublication publication = aeron.addExclusivePublication(
                ctx.snapshotChannel(), ctx.snapshotStreamId()))
        {
            // ==================== 步骤 2: 开始录制快照 ====================
            // 将快照 publication 的所有消息录制到磁盘
            // - channel: 添加 sessionId 以唯一标识此次录制
            // - streamId: 快照流 ID（与日志流不同）
            // - LOCAL: 本地录制（不需要网络传输）
            // - true: 自动启动录制
            final String channel = ChannelUri.addSessionId(ctx.snapshotChannel(), publication.sessionId());
            archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);

            // ==================== 步骤 3: 等待录制计数器就绪 ====================
            // RecordingPos 计数器用于跟踪录制进度：
            // - counterId: 计数器 ID
            // - recordingId: 快照的 Recording ID（用于恢复）
            final CountersReader counters = aeron.countersReader();
            final int counterId = awaitRecordingCounter(publication.sessionId(), counters, archive);
            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            // ==================== 步骤 4: 保存集群状态（会话列表） ====================
            // snapshotState() 会保存：
            // - 快照元数据（BEGIN）
            // - 所有客户端会话（sessions）
            // - 快照元数据（END）
            snapshotState(publication, logPosition, leadershipTermId);

            // 检查时钟 tick（更新活跃时间戳）
            checkForClockTick(nanoClock.nanoTime());

            // 检查 Archive 是否有错误响应
            archive.checkForErrorResponse();

            // ==================== 步骤 5: 调用业务逻辑保存自定义状态 ====================
            // service.onTakeSnapshot() 由业务逻辑实现：
            // - 保存业务状态（如 counter、orders、users 等）
            // - 通过 publication.offer() 写入快照数据
            // - 数据会自动录制到磁盘（Archive 后台录制）
            // <p>
            // <b>重要</b>：这是非阻塞的！
            // - publication.offer() 立即返回，不等待数据写入磁盘
            // - Archive 在后台将数据写入磁盘
            // - 主线程继续执行，不会被阻塞
            service.onTakeSnapshot(publication);

            // ==================== 步骤 6: 等待录制完成 ====================
            // 轮询 RecordingPos 计数器，等待所有数据写入磁盘
            // - position: publication 的当前位置（总共写入的字节数）
            // - counters.getCounterValue(counterId): 已录制的字节数
            // - 当 已录制字节数 >= 总字节数 时，录制完成
            awaitRecordingComplete(recordingId, publication.position(), counters, counterId, archive);

            // ==================== 返回 recordingId ====================
            // recordingId 用于恢复时定位快照文件
            // - ConsensusModule 会记录此 recordingId
            // - 重启时，从 RecoveryState 读取 recordingId，加载快照
            return recordingId;
        }
        catch (final ArchiveException ex)
        {
            // ==================== 异常处理：磁盘空间不足 ====================
            // 如果磁盘空间不足，无法继续运行，终止 Agent
            if (ex.errorCode() == ArchiveException.STORAGE_SPACE)
            {
                throw new AgentTerminationException(ex);
            }

            // 其他异常：重新抛出
            throw ex;
        }
    }

    private void awaitRecordingComplete(
        final long recordingId,
        final long position,
        final CountersReader counters,
        final int counterId,
        final AeronArchive archive)
    {
        idleStrategy.reset();
        while (counters.getCounterValue(counterId) < position)
        {
            idle();
            archive.checkForErrorResponse();

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording stopped unexpectedly: " + recordingId);
            }
        }
    }

    /**
     * 保存集群状态到快照：写入快照元数据和会话列表。
     * <p>
     * 这是保存集群状态的核心方法，负责：
     * <ul>
     *   <li>写入快照元数据（BEGIN）：logPosition、leadershipTermId、timeUnit、appVersion</li>
     *   <li>写入所有客户端会话：clusterSessionId、responseChannel、encodedPrincipal 等</li>
     *   <li>写入快照元数据（END）：与 BEGIN 相同，用于校验完整性</li>
     * </ul>
     * <p>
     * <b>快照内容结构</b>：
     * <pre>
     * ┌─────────────────────────────────────────────────────┐
     * │  SnapshotMark (BEGIN)                               │
     * │  - snapshotTypeId: SNAPSHOT_TYPE_ID (2)             │
     * │  - logPosition: 快照对应的日志位置                   │
     * │  - leadershipTermId: 当前 Term ID                    │
     * │  - index: 0 (快照索引，保留字段)                     │
     * │  - timeUnit: 集群时间单位 (MILLISECONDS/...)         │
     * │  - appVersion: 应用版本号 (如 1.2.3 → 0x01020300)    │
     * ├─────────────────────────────────────────────────────┤
     * │  ClientSession[] (所有客户端会话)                    │
     * │  - clusterSessionId: 100                            │
     * │  - correlationId: 关联 ID                           │
     * │  - openedLogPosition: 会话打开时的日志位置           │
     * │  - closedLogPosition: 会话关闭时的日志位置（未关闭则为 NULL）│
     * │  - responseStreamId: 响应通道 stream ID             │
     * │  - responseChannel: 响应通道 URL                    │
     * │  - encodedPrincipal: 认证凭证（序列化的用户信息）    │
     * │                                                     │
     * │  - clusterSessionId: 101                            │
     * │  - ...                                              │
     * ├─────────────────────────────────────────────────────┤
     * │  SnapshotMark (END)                                 │
     * │  - 与 BEGIN 相同的数据（用于校验快照完整性）         │
     * └─────────────────────────────────────────────────────┘
     * </pre>
     * <p>
     * <b>注意</b>：业务状态由 service.onTakeSnapshot() 单独写入，不在此方法中。
     *
     * @param publication 快照 publication（用于写入快照数据）
     * @param logPosition 快照对应的日志位置
     * @param leadershipTermId 当前 Leadership Term ID
     */
    private void snapshotState(
        final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
    {
        // ==================== 创建快照保存器 ====================
        // ServiceSnapshotTaker 是快照保存的工具类：
        // - publication: 快照 publication（用于写入数据）
        // - idleStrategy: 空闲策略（写入失败时降低 CPU 占用）
        // - aeronAgentInvoker: Aeron 代理调用器（用于手动驱动）
        final ServiceSnapshotTaker snapshotTaker = new ServiceSnapshotTaker(
            publication, idleStrategy, aeronAgentInvoker);

        // ==================== 步骤 1: 写入快照元数据（BEGIN） ====================
        // markBegin() 会写入快照的开始标记，包含：
        // - SNAPSHOT_TYPE_ID (2): 快照类型 ID（区分不同类型的快照）
        // - logPosition: 快照对应的日志位置
        //   - 快照完成后，logPosition 之前的日志可以删除
        //   - 恢复时，从 logPosition 开始回放日志
        // - leadershipTermId: 当前 Leadership Term ID
        //   - 用于校验快照是否来自当前 Term
        // - 0: 快照索引（保留字段，当前未使用）
        // - timeUnit: 集群时间单位（MILLISECONDS/MICROSECONDS/NANOSECONDS）
        //   - 所有节点使用相同的时间单位
        // - ctx.appVersion(): 应用版本号
        //   - 用于兼容性检查（防止不兼容的版本加载快照）
        snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());

        // ==================== 步骤 2: 写入所有客户端会话 ====================
        // 遍历所有活跃会话，将每个会话的信息写入快照：
        // - clusterSessionId: 会话 ID（全局唯一）
        // - correlationId: 关联 ID（客户端连接请求的关联 ID）
        // - openedLogPosition: 会话打开时的日志位置
        // - closedLogPosition: 会话关闭时的日志位置（未关闭则为 NULL）
        // - responseStreamId: 响应通道的 stream ID
        // - responseChannel: 响应通道的 URL（如 "aeron:udp?endpoint=localhost:20001"）
        // - encodedPrincipal: 认证凭证（序列化的用户信息）
        // <p>
        // 恢复时，会根据这些信息重建会话对象：
        // - Leader: 重新连接响应通道，准备向客户端发送响应
        // - Follower: 只创建会话对象，不连接（Follower 不发送响应）
        for (int i = 0, size = sessions.size(); i < size; i++)
        {
            snapshotTaker.snapshotSession(sessions.get(i));
        }

        // ==================== 步骤 3: 写入快照元数据（END） ====================
        // markEnd() 会写入快照的结束标记，包含与 BEGIN 相同的数据：
        // - 用于校验快照完整性（BEGIN 和 END 必须一致）
        // - 如果不一致，说明快照损坏，恢复时会失败
        snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, timeUnit, ctx.appVersion());
    }

    private void executeAction(
        final ClusterAction action,
        final long logPosition,
        final long leadershipTermId,
        final int flags)
    {
        if (ClusterAction.SNAPSHOT == action && shouldSnapshot(flags))
        {
            long recordingId = NULL_VALUE;
            Exception exception = null;
            snapshotDurationTracker.onSnapshotBegin(nanoClock.nanoTime());
            try
            {
                recordingId = onTakeSnapshot(logPosition, leadershipTermId);
            }
            catch (final Exception ex)
            {
                exception = ex;
            }
            finally
            {
                snapshotDurationTracker.onSnapshotEnd(nanoClock.nanoTime());
            }

            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, recordingId, serviceId))
            {
                idle();
            }

            if (null != exception)
            {
                LangUtil.rethrowUnchecked(exception);
            }
        }
    }

    private boolean shouldSnapshot(final int flags)
    {
        return CLUSTER_ACTION_FLAGS_DEFAULT == flags || 0 != (flags & standbySnapshotFlags);
    }

    private int awaitRecordingCounter(final int sessionId, final CountersReader counters, final AeronArchive archive)
    {
        idleStrategy.reset();
        final long archiveId = archive.archiveId();
        int counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        while (NULL_COUNTER_ID == counterId)
        {
            idle();
            archive.checkForErrorResponse();
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId, archiveId);
        }

        return counterId;
    }

    private boolean checkForClockTick(final long nowNs)
    {
        if (isAbort || aeron.isClosed())
        {
            isAbort = true;
            throw new AgentTerminationException("unexpected Aeron close");
        }

        if (nowNs - lastSlowTickNs > ONE_MILLISECOND_NS)
        {
            lastSlowTickNs = nowNs;

            if (null != aeronAgentInvoker)
            {
                aeronAgentInvoker.invoke();
                if (isAbort || aeron.isClosed())
                {
                    isAbort = true;
                    throw new AgentTerminationException("unexpected Aeron close");
                }
            }

            if (null != commitPosition && commitPosition.isClosed())
            {
                ctx.errorLog().record(new AeronEvent(
                    "commit-pos counter unexpectedly closed, terminating", AeronException.Category.WARN));

                throw new ClusterTerminationException(true);
            }

            final long nowMs = epochClock.time();
            if (nowMs >= markFileUpdateDeadlineMs)
            {
                markFileUpdateDeadlineMs = nowMs + MARK_FILE_UPDATE_INTERVAL_MS;
                markFile.updateActivityTimestamp(nowMs);
            }

            return true;
        }

        return false;
    }

    private int pollServiceAdapter()
    {
        int workCount = 0;

        workCount += serviceAdapter.poll();

        if (null != activeLogEvent && null == logAdapter.image())
        {
            final ActiveLogEvent event = activeLogEvent;
            activeLogEvent = null;
            joinActiveLog(event);
        }

        if (NULL_POSITION != terminationPosition && logPosition >= terminationPosition)
        {
            if (logPosition > terminationPosition)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent(
                    "service terminate: logPosition=" + logPosition + " > terminationPosition=" + terminationPosition));
            }

            terminate(logPosition == terminationPosition);
        }

        if (NULL_POSITION != requestedAckPosition && logPosition >= requestedAckPosition)
        {
            if (logPosition > requestedAckPosition)
            {
                ctx.countedErrorHandler().onError(new ClusterEvent(
                    "invalid ack request: logPosition=" + logPosition +
                    " > requestedAckPosition=" + requestedAckPosition));
            }

            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                idle();
            }
            requestedAckPosition = NULL_POSITION;
        }

        return workCount;
    }

    private void terminate(final boolean isTerminationExpected)
    {
        isServiceActive = false;
        activeLifecycleCallback = LIFECYCLE_CALLBACK_ON_TERMINATE;
        try
        {
            service.onTerminate(this);
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
        finally
        {
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
        }

        try
        {
            int attempts = 5;
            final long id = ackId++;
            while (!consensusModuleProxy.ack(logPosition, clusterTime, id, NULL_VALUE, serviceId))
            {
                if (0 == --attempts)
                {
                    break;
                }
                idle();
            }
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }

        terminationPosition = NULL_VALUE;
        throw new ClusterTerminationException(isTerminationExpected);
    }

    private void checkForValidInvocation()
    {
        if (LIFECYCLE_CALLBACK_NONE != activeLifecycleCallback)
        {
            throw new ClusterException(
                "sending messages or scheduling timers is not allowed from " + lifecycleName(activeLifecycleCallback));
        }
    }

    private void abort()
    {
        isAbort = true;

        try
        {
            if (!ctx.abortLatch().await(AgentRunner.RETRY_CLOSE_TIMEOUT_MS * 3L, TimeUnit.MILLISECONDS))
            {
                ctx.countedErrorHandler().onError(
                    new TimeoutException("awaiting abort latch", AeronException.Category.WARN));
            }
        }
        catch (final InterruptedException ignore)
        {
            Thread.currentThread().interrupt();
        }
    }

    private void counterUnavailable(final CountersReader countersReader, final long registrationId, final int counterId)
    {
        final ReadableCounter commitPosition = this.commitPosition;
        if (null != commitPosition &&
            commitPosition.counterId() == counterId &&
            commitPosition.registrationId() == registrationId)
        {
            commitPosition.close();
        }
    }

    private int invokeBackgroundWork(final long nowNs)
    {
        activeLifecycleCallback = LIFECYCLE_CALLBACK_DO_BACKGROUND_WORK;
        try
        {
            return service.doBackgroundWork(nowNs);
        }
        finally
        {
            activeLifecycleCallback = LIFECYCLE_CALLBACK_NONE;
        }
    }

    private void runTerminationHook()
    {
        try
        {
            ctx.terminationHook().run();
        }
        catch (final Exception ex)
        {
            ctx.countedErrorHandler().onError(ex);
        }
    }

    private void logAck(
        final int memberId,
        final long logPosition,
        final long clusterTime,
        final long id,
        final long recordingId,
        final int serviceId)
    {
    }
}

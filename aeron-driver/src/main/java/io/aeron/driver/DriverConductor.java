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
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.buffer.LogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.media.ControlMode;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.ClientHeartbeatTimestamp;
import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiveChannelStatus;
import io.aeron.driver.status.ReceiveLocalSocketAddress;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverNaksSent;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SendChannelStatus;
import io.aeron.driver.status.SendLocalSocketAddress;
import io.aeron.driver.status.SenderBpe;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderNaksReceived;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.UnsafeBufferPosition;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.CONTROL_MODE_RESPONSE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.InferableBoolean;
import static io.aeron.CommonContext.InferableBoolean.FORCE_TRUE;
import static io.aeron.CommonContext.InferableBoolean.INFER;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_PARAM_NAME;
import static io.aeron.CommonContext.MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME;
import static io.aeron.CommonContext.MTU_LENGTH_PARAM_NAME;
import static io.aeron.CommonContext.RECEIVER_WINDOW_LENGTH_PARAM_NAME;
import static io.aeron.CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SOCKET_RCVBUF_PARAM_NAME;
import static io.aeron.CommonContext.SOCKET_SNDBUF_PARAM_NAME;
import static io.aeron.ErrorCode.GENERIC_ERROR;
import static io.aeron.ErrorCode.UNKNOWN_COUNTER;
import static io.aeron.ErrorCode.UNKNOWN_PUBLICATION;
import static io.aeron.ErrorCode.UNKNOWN_SUBSCRIPTION;
import static io.aeron.driver.PublicationParams.PROTOTYPE_VALUE_CORRELATION_ID;
import static io.aeron.driver.PublicationParams.confirmMatch;
import static io.aeron.driver.PublicationParams.getPublicationParams;
import static io.aeron.driver.PublicationParams.validateMtuForSndbuf;
import static io.aeron.driver.PublicationParams.validateSpiesSimulateConnection;
import static io.aeron.driver.SubscriptionParams.validateInitialWindowForRcvBuf;
import static io.aeron.driver.status.SystemCounterDescriptor.ERRORS;
import static io.aeron.driver.status.SystemCounterDescriptor.FREE_FAILS;
import static io.aeron.driver.status.SystemCounterDescriptor.IMAGES_REJECTED;
import static io.aeron.driver.status.SystemCounterDescriptor.INVALID_PACKETS;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;
import static io.aeron.driver.status.SystemCounterDescriptor.RETRANSMIT_OVERFLOW;
import static io.aeron.driver.status.SystemCounterDescriptor.UNBLOCKED_COMMANDS;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_BUFFER_TYPE_PUBLICATION_IMAGE;
import static io.aeron.logbuffer.LogBufferDescriptor.PARTITION_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.activeTermCount;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.correlationId;
import static io.aeron.logbuffer.LogBufferDescriptor.endOfStreamPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.entityTag;
import static io.aeron.logbuffer.LogBufferDescriptor.group;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByTerm;
import static io.aeron.logbuffer.LogBufferDescriptor.initialTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.initialiseTailWithTermId;
import static io.aeron.logbuffer.LogBufferDescriptor.isPublicationRevoked;
import static io.aeron.logbuffer.LogBufferDescriptor.isResponse;
import static io.aeron.logbuffer.LogBufferDescriptor.lingerTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.maxResend;
import static io.aeron.logbuffer.LogBufferDescriptor.mtuLength;
import static io.aeron.logbuffer.LogBufferDescriptor.nextPartitionIndex;
import static io.aeron.logbuffer.LogBufferDescriptor.osDefaultSocketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osDefaultSocketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osMaxSocketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.osMaxSocketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.packTail;
import static io.aeron.logbuffer.LogBufferDescriptor.pageSize;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static io.aeron.logbuffer.LogBufferDescriptor.publicationWindowLength;
import static io.aeron.logbuffer.LogBufferDescriptor.rawTail;
import static io.aeron.logbuffer.LogBufferDescriptor.receiverWindowLength;
import static io.aeron.logbuffer.LogBufferDescriptor.rejoin;
import static io.aeron.logbuffer.LogBufferDescriptor.reliable;
import static io.aeron.logbuffer.LogBufferDescriptor.responseCorrelationId;
import static io.aeron.logbuffer.LogBufferDescriptor.signalEos;
import static io.aeron.logbuffer.LogBufferDescriptor.socketRcvbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.socketSndbufLength;
import static io.aeron.logbuffer.LogBufferDescriptor.sparse;
import static io.aeron.logbuffer.LogBufferDescriptor.spiesSimulateConnection;
import static io.aeron.logbuffer.LogBufferDescriptor.storeDefaultFrameHeader;
import static io.aeron.logbuffer.LogBufferDescriptor.termLength;
import static io.aeron.logbuffer.LogBufferDescriptor.tether;
import static io.aeron.logbuffer.LogBufferDescriptor.type;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredLingerTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredRestingTimeoutNs;
import static io.aeron.logbuffer.LogBufferDescriptor.untetheredWindowLimitTimeoutNs;
import static io.aeron.protocol.DataHeaderFlyweight.createDefaultHeader;
import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;

/**
 * Driver Conductor that takes commands from publishers and subscribers, and orchestrates the media driver.
 *
 * 【源码解析】DriverConductor —— Media Driver 的"大脑"，三大核心 Agent 之一。
 *
 * 职责：
 * 1. 消费 Client 通过 CnC to-driver RingBuffer 发来的命令（ADD_PUBLICATION / ADD_SUBSCRIPTION / REMOVE 等）
 * 2. 创建和管理所有 Publication、Subscription、Image、ChannelEndpoint 等资源的生命周期
 * 3. 通过 ReceiverProxy / SenderProxy 将操作委派给 Receiver 和 Sender Agent（线程安全的命令队列）
 * 4. 通过 ClientProxy 将响应和事件写入 CnC to-clients BroadcastBuffer 回传给 Client
 * 5. 定期执行定时器任务：心跳检查、客户端活性检测、资源清理、流位置追踪等
 *
 * 线程模型：在 DEDICATED 模式下独占一个线程；在 SHARED 模式下与 Sender/Receiver 共享线程。
 * 实现 Agent 接口，核心驱动方法是 doWork()，由 AgentRunner 循环调用。
 */
public final class DriverConductor implements Agent
{
    // 时钟更新最小间隔 1ms，减少 nanoClock 系统调用频率
    private static final long CLOCK_UPDATE_INTERNAL_NS = TimeUnit.MILLISECONDS.toNanos(1);
    private static final String[] INVALID_DESTINATION_KEYS = {
        MTU_LENGTH_PARAM_NAME,
        RECEIVER_WINDOW_LENGTH_PARAM_NAME,
        SOCKET_RCVBUF_PARAM_NAME,
        SOCKET_SNDBUF_PARAM_NAME,
        RESPONSE_CORRELATION_ID_PARAM_NAME
    };

    static final long EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 1;

    // 随机 sessionId 起点，每次创建 Publication 递增，避免与其他 Driver 实例冲突
    private int nextSessionId = BitUtil.generateRandomisedId();
    private final long timerIntervalNs;          // 定时器检查间隔（心跳、客户端活性、资源清理）
    private final long clientLivenessTimeoutNs;  // 客户端存活超时，超过此时间未心跳则视为断连
    private long timeOfLastToDriverPositionChangeNs; // 上次 to-driver 命令位置变化的纳秒时间戳
    private long lastCommandConsumerPosition;    // to-driver RingBuffer 的消费位置，用于检测命令卡住
    private long timerCheckDeadlineNs;           // 下一次定时器检查的截止时间
    private long clockUpdateDeadlineNs;          // 下一次缓存时钟更新的截止时间

    private final Context ctx;                   // MediaDriver 上下文，包含所有配置和共享对象
    private final LogFactory logFactory;         // 创建 LogBuffer（Term Buffer）的工厂
    private final ReceiverProxy receiverProxy;   // 向 Receiver Agent 发送命令的代理（线程安全队列）
    private final SenderProxy senderProxy;       // 向 Sender Agent 发送命令的代理（线程安全队列）
    private final ClientProxy clientProxy;       // 向 Client 回写响应/事件的代理（写入 CnC to-clients BroadcastBuffer）
    private final RingBuffer toDriverCommands;   // CnC to-driver 环形缓冲区，Client → Driver 的命令通道
    private final ClientCommandAdapter clientCommandAdapter; // 命令适配器：从 RingBuffer 读命令并分发到对应处理方法
    private final ManyToOneConcurrentLinkedQueue<Runnable> driverCmdQueue; // Receiver/Sender → Conductor 的内部命令队列

    // === 资源注册表 ===
    private final Object2ObjectHashMap<String, SendChannelEndpoint> sendChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();             // 发送端 channel → endpoint 映射（按 canonical 地址去重）
    private final Object2ObjectHashMap<String, ReceiveChannelEndpoint> receiveChannelEndpointByChannelMap =
        new Object2ObjectHashMap<>();             // 接收端 channel → endpoint 映射
    private final ArrayList<NetworkPublication> networkPublications = new ArrayList<>();   // 所有网络 Publication
    private final ArrayList<IpcPublication> ipcPublications = new ArrayList<>();           // 所有 IPC Publication
    private final ArrayList<PublicationImage> publicationImages = new ArrayList<>();       // 所有接收到的 PublicationImage
    private final ArrayList<PublicationLink> publicationLinks = new ArrayList<>();         // Client ↔ Publication 的关联
    private final ArrayList<SubscriptionLink> subscriptionLinks = new ArrayList<>();       // Client ↔ Subscription 的关联
    private final ArrayList<CounterLink> counterLinks = new ArrayList<>();                 // Client ↔ Counter 的关联
    private final ArrayList<AeronClient> clients = new ArrayList<>();                      // 已注册的 Aeron Client 列表
    private final ArrayDeque<DriverManagedResource> endOfLifeResources = new ArrayDeque<>(); // 待释放的生命周期结束资源
    private final ObjectHashSet<SessionKey> activeSessionSet = new ObjectHashSet<>();      // 活跃 sessionId 集合（防冲突）

    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final CachedEpochClock cachedEpochClock;   // 缓存的毫秒级时钟，减少系统调用
    private final CachedNanoClock cachedNanoClock;     // 缓存的纳秒级时钟
    private final CountersManager countersManager;     // CnC Counters 管理器，分配/释放计数器
    private final NetworkPublicationThreadLocals networkPublicationThreadLocals = new NetworkPublicationThreadLocals();
    private final MutableDirectBuffer tempBuffer;
    private final DataHeaderFlyweight defaultDataHeader = new DataHeaderFlyweight(createDefaultHeader(0, 0, 0));
    private final AtomicCounter errorCounter;          // 全局错误计数
    private final AtomicCounter imagesRejected;        // 被拒绝的 Image 计数
    private final DutyCycleTracker dutyCycleTracker;   // 工作周期耗时追踪器
    private final Executor asyncTaskExecutor;          // 异步任务执行器（DNS 解析等）
    private final boolean asyncExecutionDisabled;      // 是否禁用异步执行
    private boolean asyncClientCommandInFlight;        // 是否有异步客户端命令正在执行中
    private TimeTrackingNameResolver nameResolver;     // 名称解析器（支持 DNS 动态解析）

    /**
     * 【构造函数】从 MediaDriver.Context 中提取所有必要的依赖注入到 Conductor 中。
     * 注意：此时三大 Agent 尚未启动，这里只做字段初始化。
     */
    DriverConductor(final MediaDriver.Context ctx)
    {
        this.ctx = ctx;
        timerIntervalNs = ctx.timerIntervalNs();           // 定时器间隔，默认 1 秒
        clientLivenessTimeoutNs = ctx.clientLivenessTimeoutNs(); // 客户端存活超时，默认 10 秒
        driverCmdQueue = ctx.driverCommandQueue();         // Receiver/Sender → Conductor 的内部命令队列
        receiverProxy = ctx.receiverProxy();               // 向 Receiver 下发命令的代理
        senderProxy = ctx.senderProxy();                   // 向 Sender 下发命令的代理
        logFactory = ctx.logFactory();                     // LogBuffer 工厂
        epochClock = ctx.epochClock();
        nanoClock = ctx.nanoClock();
        cachedEpochClock = ctx.cachedEpochClock();
        cachedNanoClock = ctx.cachedNanoClock();
        toDriverCommands = ctx.toDriverCommands();         // CnC to-driver RingBuffer
        clientProxy = ctx.clientProxy();                   // 向 Client 写事件的代理
        tempBuffer = ctx.tempBuffer();
        errorCounter = ctx.systemCounters().get(ERRORS);
        imagesRejected = ctx.systemCounters().get(IMAGES_REJECTED);
        dutyCycleTracker = ctx.conductorDutyCycleTracker();

        asyncTaskExecutor = ctx.asyncTaskExecutor();
        asyncExecutionDisabled = ctx.asyncTaskExecutorThreads() <= 0;

        countersManager = ctx.countersManager();

        // ClientCommandAdapter 负责从 toDriverCommands 中读取命令并路由到对应的 onXxx 处理方法
        clientCommandAdapter = new ClientCommandAdapter(
            errorCounter,
            ctx.errorHandler(),
            toDriverCommands,
            clientProxy,
            this);

        // 记录初始消费位置，用于后续检测命令通道是否卡住
        lastCommandConsumerPosition = toDriverCommands.consumerPosition();
    }

    /**
     * {@inheritDoc}
     */
    /**
     * 【Agent 启动回调】在 AgentRunner 的线程中，第一次调用 doWork() 之前被调用。
     * 初始化缓存时钟、各种截止时间、名称解析器。
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);               // 初始化缓存的纳秒时钟
        cachedEpochClock.update(epochClock.time());   // 初始化缓存的毫秒时钟
        dutyCycleTracker.update(nowNs);               // 初始化工作周期追踪
        timerCheckDeadlineNs = nowNs + timerIntervalNs;           // 设置第一次定时器检查的时间
        clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS; // 设置第一次时钟更新的时间
        timeOfLastToDriverPositionChangeNs = nowNs;

        // 初始化名称解析器：如果配置了 resolverInterface 则使用 DriverNameResolver（支持集群名称解析），否则用简单的 nameResolver
        nameResolver = new TimeTrackingNameResolver(
            null == ctx.resolverInterface() ? ctx.nameResolver() : new DriverNameResolver(ctx),
            nanoClock,
            ctx.nameResolverTimeTracker());

        final SystemCounters systemCounters = ctx.systemCounters();
        systemCounters.get(RESOLUTION_CHANGES).appendToLabel(": driverName=" + ctx.resolverName());
    }

    /**
     * {@inheritDoc}
     */
    /**
     * 【Agent 关闭回调】Driver 关闭时的资源清理：
     * 1. 停止异步任务执行器
     * 2. 关闭名称解析器
     * 3. 关闭所有 ChannelEndpoint（释放 UDP Socket）
     * 4. 释放所有 Publication/Image 的 LogBuffer（mmap 内存）
     * 5. 清除心跳并刷盘 CnC 文件
     * 6. 关闭 Context（释放 CnC mmap 等）
     */
    public void onClose()
    {
        if (asyncTaskExecutor instanceof ExecutorService)
        {
            try
            {
                final ExecutorService executor = (ExecutorService)asyncTaskExecutor;
                executor.shutdownNow();
                if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    ctx.errorHandler().onError(new AeronEvent("failed to shutdown async task executor"));
                }
            }
            catch (final Exception e)
            {
                ctx.errorHandler().onError(e);
            }
        }
        CloseHelper.close(ctx.errorHandler(), nameResolver);
        CloseHelper.closeAll(receiveChannelEndpointByChannelMap.values());  // 关闭所有接收端 endpoint
        CloseHelper.closeAll(sendChannelEndpointByChannelMap.values());    // 关闭所有发送端 endpoint
        publicationImages.forEach(PublicationImage::free);                 // 释放所有 Image 的 LogBuffer
        networkPublications.forEach(NetworkPublication::free);             // 释放所有网络 Publication 的 LogBuffer
        ipcPublications.forEach(IpcPublication::free);                     // 释放所有 IPC Publication 的 LogBuffer
        freeEndOfLifeResources(Integer.MAX_VALUE);                         // 释放所有排队中的待清理资源
        toDriverCommands.consumerHeartbeatTime(NULL_VALUE);                // 清除心跳，通知 Client: Driver 已关闭
        ctx.cncByteBuffer().force();                                       // 刷盘 CnC 文件
        ctx.close();                                                       // 释放 Context 持有的所有 mmap / fd
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "driver-conductor";
    }

    /**
     * {@inheritDoc}
     */
    /**
     * 【核心工作循环】由 AgentRunner 反复调用，每次执行以下 6 步：
     *
     * 1. processTimers —— 定时任务：客户端心跳检查、Publication/Image 清理、position 追踪
     * 2. clientCommandAdapter.receive —— 从 CnC to-driver RingBuffer 读取并处理 Client 命令
     *    （若有异步命令正在飞行中则跳过，防止乱序）
     * 3. drainCommandQueue —— 处理来自 Receiver/Sender 通过 driverCmdQueue 发来的内部命令
     * 4. trackStreamPositions —— 追踪所有 Publication/Image 的 position，更新流控相关计数器
     * 5. nameResolver.doWork —— 驱动名称解析器（DNS 刷新等）
     * 6. freeEndOfLifeResources —— 释放达到 linger 超时的已关闭资源
     *
     * @return workCount > 0 表示做了有意义的工作，IdleStrategy 据此决定是否空闲等待
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        trackTime(nowNs);   // 更新缓存时钟（cachedEpochClock / cachedNanoClock）

        int workCount = 0;
        workCount += processTimers(nowNs);                              // 步骤 1：定时器
        if (!asyncClientCommandInFlight)
        {
            workCount += clientCommandAdapter.receive();                // 步骤 2：消费 Client 命令
        }
        workCount += drainCommandQueue();                               // 步骤 3：内部命令队列
        workCount += trackStreamPositions(workCount, nowNs);            // 步骤 4：流位置追踪
        workCount += nameResolver.doWork(cachedEpochClock.time());      // 步骤 5：名称解析
        workCount += freeEndOfLifeResources(ctx.resourceFreeLimit());   // 步骤 6：资源回收

        return workCount;
    }

    boolean notAcceptingClientCommands()
    {
        return senderProxy.isApplyingBackpressure() || receiverProxy.isApplyingBackpressure();
    }

    @SuppressWarnings("MethodLength")
    void onCreatePublicationImage(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final int termBufferLength,
        final int senderMtuLength,
        final int transportIndex,
        final short flags,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final ReceiveChannelEndpoint channelEndpoint)
    {
        Configuration.validateMtuLength(senderMtuLength);

        final UdpChannel subscriptionChannel = channelEndpoint.subscriptionUdpChannel();

        final SubscriptionParams subscriptionParams =
            SubscriptionParams.getSubscriptionParams(subscriptionChannel.channelUri(), ctx, termBufferLength);

        Configuration.validateInitialWindowLength(subscriptionParams.receiverWindowLength, senderMtuLength);

        final long joinPosition = computePosition(
            activeTermId, termOffset, LogBufferDescriptor.positionBitsToShift(termBufferLength), initialTermId);
        final ArrayList<SubscriberPosition> subscriberPositions = createSubscriberPositions(
            sessionId, streamId, channelEndpoint, joinPosition);

        if (!subscriberPositions.isEmpty())
        {
            RawLog rawLog = null;
            CongestionControl congestionControl = null;
            UnsafeBufferPosition hwmPos = null;
            UnsafeBufferPosition rcvPos = null;
            AtomicCounter rcvNaksSent = null;

            try
            {
                final long registrationId = toDriverCommands.nextCorrelationId();
                final SubscriptionLink subscription = subscriberPositions.get(0).subscription();
                final boolean isMulticastSemantics =
                    isMulticastSemantics(subscriptionChannel, subscription.group(), flags);
                final boolean isReliable = subscription.isReliable();
                final boolean isSparse = isOldestSubscriptionSparse(subscriberPositions);

                rawLog = newPublicationImageLog(
                    sessionId,
                    streamId,
                    initialTermId,
                    termBufferLength,
                    isReliable,
                    isSparse,
                    senderMtuLength,
                    channelEndpoint.socketRcvbufLength(),
                    channelEndpoint.socketSndbufLength(),
                    termOffset,
                    subscriptionParams,
                    registrationId,
                    isMulticastSemantics);

                congestionControl = ctx.congestionControlSupplier().newInstance(
                    registrationId,
                    subscriptionChannel,
                    streamId,
                    sessionId,
                    termBufferLength,
                    senderMtuLength,
                    controlAddress,
                    sourceAddress,
                    ctx.receiverCachedNanoClock(),
                    ctx,
                    countersManager);

                final String uri = subscription.channel();
                final long clientId = subscription.aeronClient().clientId();
                hwmPos = ReceiverHwm.allocate(
                    tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);
                rcvPos = ReceiverPos.allocate(
                    tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);
                rcvNaksSent = ReceiverNaksSent.allocate(
                    tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, uri);

                final String sourceIdentity = Configuration.sourceIdentity(sourceAddress);

                final PublicationImage image = new PublicationImage(
                    registrationId,
                    ctx,
                    channelEndpoint,
                    transportIndex,
                    controlAddress,
                    sessionId,
                    streamId,
                    initialTermId,
                    activeTermId,
                    termOffset,
                    flags,
                    isReliable,
                    subscriptionParams.untetheredWindowLimitTimeoutNs,
                    subscriptionParams.untetheredLingerTimeoutNs,
                    subscriptionParams.untetheredRestingTimeoutNs,
                    rawLog,
                    resolveDelayGenerator(ctx, subscriptionChannel, isMulticastSemantics, isReliable),
                    subscriberPositions,
                    hwmPos,
                    rcvPos,
                    rcvNaksSent,
                    sourceIdentity,
                    congestionControl);

                channelEndpoint.incRefImages();
                publicationImages.add(image);
                receiverProxy.newPublicationImage(channelEndpoint, image);

                for (int i = 0, size = subscriberPositions.size(); i < size; i++)
                {
                    final SubscriberPosition position = subscriberPositions.get(i);
                    position.addLink(image);

                    final int positionCounterId = position.positionCounterId();
                    countersManager.setCounterReferenceId(positionCounterId, registrationId);

                    clientProxy.onAvailableImage(
                        registrationId,
                        streamId,
                        sessionId,
                        position.subscription().registrationId(),
                        positionCounterId,
                        rawLog.fileName(),
                        sourceIdentity);
                }
            }
            catch (final Exception ex)
            {
                subscriberPositions.forEach((subscriberPosition) -> subscriberPosition.position().close());
                CloseHelper.quietCloseAll(rawLog, congestionControl, hwmPos, rcvPos, rcvNaksSent);
                throw ex;
            }
        }
    }

    void onPublicationError(
        final long registrationId,
        final long destinationRegistrationId,
        final int sessionId,
        final int streamId,
        final long receiverId,
        final long groupId,
        final InetSocketAddress srcAddress,
        final int errorCode,
        final String errorMessage)
    {
        recordError(new AeronEvent(
            "onPublicationError: " +
            "registrationId=" + registrationId +
            ", destinationRegistrationId=" + destinationRegistrationId +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", receiverId=" + receiverId +
            ", groupId=" + groupId +
            ", errorCode=" + errorCode +
            ", errorMessage=" + errorMessage,
            AeronException.Category.WARN));
        clientProxy.onPublicationErrorFrame(
            registrationId,
            destinationRegistrationId,
            sessionId,
            streamId,
            receiverId,
            groupId,
            srcAddress,
            errorCode,
            errorMessage);
    }

    void onReResolveEndpoint(
        final String endpoint, final SendChannelEndpoint channelEndpoint, final InetSocketAddress address)
    {
        executeAsyncTask(
            () -> UdpChannel.resolve(endpoint, ENDPOINT_PARAM_NAME, true, nameResolver),
            (asyncResult) ->
            {
                try
                {
                    final InetSocketAddress newAddress = asyncResult.get();
                    if (newAddress.isUnresolved())
                    {
                        recordError(new AeronEvent("could not re-resolve: endpoint=" + endpoint));
                    }
                    else if (!address.equals(newAddress))
                    {
                        senderProxy.onResolutionChange(channelEndpoint, endpoint, newAddress);
                    }
                }
                catch (final Exception ex)
                {
                    recordError(ex);
                }
            });
    }

    void onReResolveControl(
        final String control,
        final UdpChannel udpChannel,
        final ReceiveChannelEndpoint channelEndpoint,
        final InetSocketAddress address)
    {
        executeAsyncTask(
            () -> UdpChannel.resolve(control, MDC_CONTROL_PARAM_NAME, true, nameResolver),
            (asyncResult) ->
            {
                try
                {
                    final InetSocketAddress newAddress = asyncResult.get();
                    if (newAddress.isUnresolved())
                    {
                        recordError(new AeronEvent("could not re-resolve: control=" + control));
                    }
                    else if (!address.equals(newAddress))
                    {
                        receiverProxy.onResolutionChange(channelEndpoint, udpChannel, newAddress);
                    }
                }
                catch (final Exception ex)
                {
                    recordError(ex);
                }
            });
    }

    IpcPublication getSharedIpcPublication(final long streamId, final long responseCorrelationId)
    {
        return findSharedIpcPublication(ipcPublications, streamId, responseCorrelationId);
    }

    IpcPublication getIpcPublication(final long registrationId)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.registrationId() == registrationId)
            {
                return publication;
            }
        }

        return null;
    }

    NetworkPublication findNetworkPublicationByTag(final long tag)
    {
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    IpcPublication findIpcPublicationByTag(final long tag)
    {
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            final long publicationTag = publication.tag();
            if (publicationTag == tag && publicationTag != ChannelUri.INVALID_TAG)
            {
                return publication;
            }
        }

        return null;
    }

    /**
     * 【处理 ADD_PUBLICATION 命令（网络类型）】
     * 当 Client 调用 aeron.addPublication("aeron:udp?endpoint=...", streamId) 时触发。
     *
     * 核心流程：
     * 1. 异步解析 channel URI（可能涉及 DNS），得到 UdpChannel
     * 2. 获取或创建 SendChannelEndpoint（复用同一 UDP Socket 发送多个 stream）
     * 3. 非 exclusive 模式下尝试复用已有 Publication（相同 streamId + endpoint）
     * 4. 若无可复用的，创建新 NetworkPublication（分配 LogBuffer、sessionId、Counters）
     * 5. 通过 clientProxy 向 Client 回写 PUBLICATION_READY 事件
     * 6. 新 Publication 创建后，自动 link 已有的 Spy Subscription
     */
    void onAddNetworkPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        executeAsyncClientTask(
            correlationId,
            () -> UdpChannel.parse(channel, nameResolver, false),  // 异步: 解析 URI，可能触发 DNS
            (asyncResult) ->
            {
                final UdpChannel udpChannel = asyncResult.get();
                final ChannelUri channelUri = udpChannel.channelUri();
                // 从 URI 中提取 Publication 参数（termLength, mtu, initialTermId 等）
                final PublicationParams params =
                    getPublicationParams(channelUri, ctx, this, streamId, udpChannel.canonicalForm());
                validateExperimentalFeatures(ctx.enableExperimentalFeatures(), udpChannel);
                validateEndpointForPublication(udpChannel);
                validateControlForPublication(udpChannel);
                validateResponseSubscription(params);

                // 获取或创建 SendChannelEndpoint：同一 UDP 地址复用同一个 endpoint（底层同一个 DatagramChannel）
                final SendChannelEndpoint channelEndpoint =
                    getOrCreateSendChannelEndpoint(params, udpChannel, correlationId);

                // 非 exclusive 模式：尝试找到相同 streamId + endpoint 的已有 Publication 进行复用
                NetworkPublication publication = null;
                if (!isExclusive)
                {
                    publication =
                        findPublication(networkPublications, streamId, channelEndpoint, params.responseCorrelationId);
                }

                final PublicationImage responsePublicationImage = findResponsePublicationImage(params);

                boolean isNewPublication = false;
                if (null == publication)
                {
                    // 新建 Publication：检查 session 冲突，分配 LogBuffer 和 Counters
                    checkForSessionClash(params.sessionId, streamId, udpChannel.canonicalForm(), channel);
                    publication = newNetworkPublication(
                        correlationId, clientId, streamId, channel, udpChannel, channelEndpoint, params, isExclusive);
                    isNewPublication = true;
                }
                else
                {
                    // 复用已有 Publication，校验参数一致性
                    confirmMatch(
                        channelUri,
                        params,
                        publication.rawLog(),
                        publication.sessionId(),
                        publication.channel(),
                        publication.initialTermId(),
                        publication.startingTermId(),
                        publication.startingTermOffset());

                    validateSpiesSimulateConnection(
                        params, publication.spiesSimulateConnection(), channel, publication.channel());
                }

                // 记录 Client ↔ Publication 的关联，用于后续 Client 断连时清理
                publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

                // 通过 CnC to-clients BroadcastBuffer 向 Client 发送 PUBLICATION_READY 事件
                clientProxy.onPublicationReady(
                    correlationId,
                    publication.registrationId(),
                    streamId,
                    publication.sessionId(),
                    publication.rawLog().fileName(),    // LogBuffer 文件路径，Client 会 mmap 这个文件来写数据
                    publication.publisherLimitId(),     // publisherLimit 计数器 ID，用于流控
                    channelEndpoint.statusIndicatorCounterId(),
                    isExclusive);

                // 新 Publication 创建后，自动 link 匹配的 Spy Subscription（spy: 订阅发送端的数据副本）
                if (isNewPublication)
                {
                    linkSpies(subscriptionLinks, publication);
                }

                if (null != responsePublicationImage)
                {
                    responsePublicationImage.responseSessionId(publication.sessionId());
                }
            });
    }

    private PublicationImage findResponsePublicationImage(final PublicationParams params)
    {
        if (!params.isResponse)
        {
            return null;
        }

        if (NULL_VALUE == params.responseCorrelationId)
        {
            throw new IllegalArgumentException(
                "control-mode=response was specified, but no response-correlation-id set");
        }

        if (PROTOTYPE_VALUE_CORRELATION_ID == params.responseCorrelationId)
        {
            return null;
        }

        for (final PublicationImage publicationImage : publicationImages)
        {
            if (publicationImage.correlationId() == params.responseCorrelationId)
            {
                if (publicationImage.hasSendResponseSetup())
                {
                    return publicationImage;
                }
                else
                {
                    throw new IllegalArgumentException(
                        "image.correlationId=" + params.responseCorrelationId + " did not request a response channel");
                }
            }
        }

        throw new IllegalArgumentException("image.correlationId=" + params.responseCorrelationId + " not found");
    }

    private PublicationImage findPublicationImage(final long correlationId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (correlationId == publicationImage.correlationId())
            {
                return publicationImage;
            }
        }

        return null;
    }

    void responseSetup(final long responseCorrelationId, final int responseSessionId)
    {
        for (int i = 0, subscriptionLinksSize = subscriptionLinks.size(); i < subscriptionLinksSize; i++)
        {
            final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
            if (subscriptionLink.registrationId() == responseCorrelationId &&
                subscriptionLink instanceof final NetworkSubscriptionLink link)
            {
                if (subscriptionLink.hasSessionId())
                {
                    receiverProxy.requestSetup(
                        subscriptionLink.channelEndpoint(), subscriptionLink.streamId(), subscriptionLink.sessionId());
                }
                else
                {
                    link.sessionId(responseSessionId);
                    addNetworkSubscriptionToReceiver(link);
                    link.channelEndpoint().decResponseRefToStream(subscriptionLink.streamId);
                }

                break;
            }
        }
    }

    void responseConnected(final long responseCorrelationId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (publicationImage.correlationId() == responseCorrelationId)
            {
                if (publicationImage.hasSendResponseSetup())
                {
                    publicationImage.responseSessionId(null);
                }
            }
        }
    }

    private void validateResponseSubscription(final PublicationParams params)
    {
        if (!params.isResponse && NULL_VALUE != params.responseCorrelationId)
        {
            for (final SubscriptionLink subscriptionLink : subscriptionLinks)
            {
                if (params.responseCorrelationId == subscriptionLink.registrationId())
                {
                    return;
                }
            }

            throw new IllegalArgumentException(
                "unable to find response subscription for response-correlation-id=" + params.responseCorrelationId);
        }
    }

    void cleanupSpies(final NetworkPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
                link.unlink(publication);
            }
        }
    }

    void notifyUnavailableImageLink(final long resourceId, final SubscriptionLink link)
    {
        clientProxy.onUnavailableImage(resourceId, link.registrationId(), link.streamId(), link.channel());
    }

    void notifyAvailableImageLink(
        final long resourceId,
        final int sessionId,
        final SubscriptionLink link,
        final int positionCounterId,
        final long joinPosition,
        final String logFileName,
        final String sourceIdentity)
    {
        countersManager.setCounterValue(positionCounterId, joinPosition);

        final int streamId = link.streamId();
        clientProxy.onAvailableImage(
            resourceId, streamId, sessionId, link.registrationId(), positionCounterId, logFileName, sourceIdentity);
    }

    void cleanupPublication(final NetworkPublication publication)
    {
        senderProxy.removeNetworkPublication(publication);

        final SendChannelEndpoint channelEndpoint = publication.channelEndpoint();
        if (channelEndpoint.shouldBeClosed())
        {
            senderProxy.closeSendChannelEndpoint(channelEndpoint);
        }

        final String channel = channelEndpoint.udpChannel().canonicalForm();
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), channel));
    }

    void sendChannelEndpointClosed(final SendChannelEndpoint channelEndpoint)
    {
        final String channel = channelEndpoint.udpChannel().canonicalForm();
        sendChannelEndpointByChannelMap.remove(channel);
        channelEndpoint.close();
    }

    void cleanupSubscriptionLink(final SubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();
        if (null != channelEndpoint)
        {
            if (subscription.hasSessionId())
            {
                if (0 == channelEndpoint.decRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
                {
                    receiverProxy.removeSubscription(
                        channelEndpoint, subscription.streamId(), subscription.sessionId());
                }
            }
            else if (subscription.isResponse())
            {
                channelEndpoint.decResponseRefToStream(subscription.streamId());
            }
            else
            {
                if (0 == channelEndpoint.decRefToStream(subscription.streamId()))
                {
                    receiverProxy.removeSubscription(channelEndpoint, subscription.streamId());
                }
            }

            tryCloseReceiveChannelEndpoint(channelEndpoint);
        }
    }

    void transitionToLinger(final PublicationImage image)
    {
        boolean rejoin = true;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(image))
            {
                rejoin = link.isRejoin();
                notifyUnavailableImageLink(image.correlationId(), link);
            }
        }

        if (rejoin)
        {
            receiverProxy.removeCoolDown(image.channelEndpoint(), image.sessionId(), image.streamId());
        }
    }

    void transitionToLinger(final IpcPublication publication)
    {
        activeSessionSet.remove(new SessionKey(publication.sessionId(), publication.streamId(), IPC_MEDIA));

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
            }
        }
    }

    void cleanupImage(final PublicationImage image)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(image);
        }
    }

    void cleanupIpcPublication(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            subscriptionLinks.get(i).unlink(publication);
        }
    }

    void unlinkIpcSubscriptions(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink link = subscriptionLinks.get(i);
            if (link.isLinked(publication))
            {
                notifyUnavailableImageLink(publication.registrationId(), link);
                link.unlink(publication);
            }
        }
    }

    void tryCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (channelEndpoint.shouldBeClosed())
        {
            receiverProxy.closeReceiveChannelEndpoint(channelEndpoint);
        }
    }

    void receiveChannelEndpointClosed(final ReceiveChannelEndpoint channelEndpoint)
    {
        final String channel = channelEndpoint.subscriptionUdpChannel().canonicalForm();
        receiveChannelEndpointByChannelMap.remove(channel);
        channelEndpoint.close();
    }

    void clientTimeout(final long clientId)
    {
        clientProxy.onClientTimeout(clientId);
    }

    void unavailableCounter(final long registrationId, final int counterId)
    {
        clientProxy.onUnavailableCounter(registrationId, counterId);
    }

    /**
     * 【处理 ADD_PUBLICATION 命令（IPC 类型）】
     * 当 Client 调用 aeron.addPublication("aeron:ipc", streamId) 时触发。
     *
     * IPC Publication 不走网络，发送端和接收端通过共享同一个 LogBuffer 文件实现零拷贝通信。
     * 流程与网络 Publication 类似，但不需要 ChannelEndpoint 和 UDP Socket。
     */
    void onAddIpcPublication(
        final String channel,
        final int streamId,
        final long correlationId,
        final long clientId,
        final boolean isExclusive)
    {
        IpcPublication publication = null;
        final ChannelUri channelUri = parseUri(channel);
        final PublicationParams params = getPublicationParams(channelUri, ctx, this, streamId, IPC_MEDIA);

        // 非 exclusive 模式：复用相同 streamId 的已有 IPC Publication
        if (!isExclusive)
        {
            publication = findSharedIpcPublication(ipcPublications, streamId, params.responseCorrelationId);
        }

        boolean isNewPublication = false;
        if (null == publication)
        {
            checkForSessionClash(params.sessionId, streamId, IPC_MEDIA, channel);
            publication = addIpcPublication(correlationId, clientId, streamId, channel, isExclusive, params);
            isNewPublication = true;
        }
        else
        {
            confirmMatch(
                channelUri,
                params,
                publication.rawLog(),
                publication.sessionId(),
                publication.channel(),
                publication.initialTermId(),
                publication.startingTermId(),
                publication.startingTermOffset());
        }

        publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

        // 回写 PUBLICATION_READY：Client 拿到 logFileName 后 mmap 该文件即可直接写消息
        clientProxy.onPublicationReady(
            correlationId,
            publication.registrationId(),
            streamId,
            publication.sessionId(),
            publication.rawLog().fileName(),
            publication.publisherLimitId(),
            ChannelEndpointStatus.NO_ID_ALLOCATED,  // IPC 无网络 endpoint
            isExclusive);

        // 新 IPC Publication 创建后，自动 link 所有匹配的 IPC Subscription
        if (isNewPublication)
        {
            linkIpcSubscriptions(publication);
        }
    }

    void onRemovePublication(final long registrationId, final long correlationId, final boolean revoke)
    {
        PublicationLink publicationLink = null;
        final ArrayList<PublicationLink> publicationLinks = this.publicationLinks;
        for (int i = 0, size = publicationLinks.size(); i < size; i++)
        {
            final PublicationLink publication = publicationLinks.get(i);
            if (registrationId == publication.registrationId())
            {
                publicationLink = publication;
                fastUnorderedRemove(publicationLinks, i);
                break;
            }
        }

        if (null == publicationLink)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        if (revoke)
        {
            publicationLink.revoke();
        }
        publicationLink.close();
        clientProxy.operationSucceeded(correlationId);
    }

    void onAddSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        final ChannelUri channelUri = parseUri(destinationChannel);
        validateDestinationUri(channelUri, destinationChannel);
        validateSendDestinationUri(channelUri, destinationChannel);

        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(channelUri, nameResolver);
        senderProxy.addDestination(sendChannelEndpoint, channelUri, dstAddress, correlationId);
        clientProxy.operationSucceeded(correlationId);
    }

    void onRemoveSendDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (registrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_PUBLICATION, "unknown publication: " + registrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        final ChannelUri channelUri = parseUri(destinationChannel);
        final InetSocketAddress dstAddress = UdpChannel.destinationAddress(channelUri, nameResolver);
        senderProxy.removeDestination(sendChannelEndpoint, channelUri, dstAddress);
        clientProxy.operationSucceeded(correlationId);
    }

    void onRemoveSendDestination(
        final long publicationRegistrationId, final long destinationRegistrationId, final long correlationId)
    {
        SendChannelEndpoint sendChannelEndpoint = null;

        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            final NetworkPublication publication = networkPublications.get(i);

            if (publicationRegistrationId == publication.registrationId())
            {
                sendChannelEndpoint = publication.channelEndpoint();
                break;
            }
        }

        if (null == sendChannelEndpoint)
        {
            throw new ControlProtocolException(
                UNKNOWN_PUBLICATION, "unknown publication: " + publicationRegistrationId);
        }

        sendChannelEndpoint.validateAllowsManualControl();

        senderProxy.removeDestination(sendChannelEndpoint, destinationRegistrationId);
        clientProxy.operationSucceeded(correlationId);
    }

    /**
     * 【处理 ADD_SUBSCRIPTION 命令（网络类型）】
     * 当 Client 调用 aeron.addSubscription("aeron:udp?endpoint=localhost:40123", streamId) 时触发。
     * <p>
     * 完整流程：
     * <pre>
     *  1. UdpChannel.parse()     ─ 解析 URI → 得到 remoteData=localhost:40123 等地址信息
     *  2. getOrCreateReceive     ─ 获取/创建 ReceiveChannelEndpoint:
     *     ChannelEndpoint()          a. new ReceiveChannelEndpoint(udpChannel, dispatcher, ...)
     *                                b. openChannel() → DatagramChannel.open() + bind(localhost:40123)
     *                                c. receiverProxy.registerReceiveChannelEndpoint()
     *                                   → Receiver 线程中: channel.register(selector, OP_READ)
     *  3. addNetworkSubscription ─ 通过 ReceiverProxy 将 streamId 注册到 DataPacketDispatcher
     *     ToReceiver()              Receiver 线程收包后按 streamId 分发到 PublicationImage
     *  4. clientProxy.on         ─ 向 Client 回写 ON_SUBSCRIPTION_READY（通过 CnC toClientsBuffer）
     *     SubscriptionReady()
     *  5. linkMatchingImages()   ─ 如果已有匹配的 Publisher，立即发送 ON_AVAILABLE_IMAGE
     * </pre>
     */
    void onAddNetworkSubscription(
        final String channel, final int streamId, final long registrationId, final long clientId)
    {
        executeAsyncClientTask(
            registrationId,
            // ① 异步解析 channel URI
            //    "aeron:udp?endpoint=localhost:40123" → UdpChannel 对象
            //    其中 remoteData = InetSocketAddress("localhost", 40123) → 这就是要绑定的 UDP 地址
            () -> UdpChannel.parse(channel, nameResolver, false),
            (asyncResult) ->
            {
                final UdpChannel udpChannel = asyncResult.get();
                final ControlMode controlMode = udpChannel.controlMode();

                validateExperimentalFeatures(ctx.enableExperimentalFeatures(), udpChannel);
                validateControlForSubscription(udpChannel);
                validateTimestampConfiguration(udpChannel);

                final SubscriptionParams params =
                    SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
                checkForClashingSubscription(params, udpChannel, streamId);

                // ② 获取或创建 ReceiveChannelEndpoint
                //    如果 localhost:40123 已有 endpoint → 复用（多个 subscription 共享同一 UDP socket）
                //    如果没有 → 创建新的 DatagramChannel, bind 端口, 注册到 Receiver 的 NIO Selector
                final ReceiveChannelEndpoint channelEndpoint = getOrCreateReceiveChannelEndpoint(
                    params, udpChannel, registrationId);

                // ③ 创建 NetworkSubscriptionLink：关联 channelEndpoint + streamId + client
                final NetworkSubscriptionLink subscription = new NetworkSubscriptionLink(
                    registrationId, channelEndpoint, streamId, channel, getOrAddClient(clientId), params);

                subscriptionLinks.add(subscription);

                if (ControlMode.RESPONSE == controlMode)
                {
                    channelEndpoint.incResponseRefToStream(subscription.streamId);
                }
                else
                {
                    // ④ 将 streamId 注册到 Receiver 线程的 DataPacketDispatcher
                    //    Receiver 收到 UDP 包后，根据包头中的 streamId 分发到对应的处理器
                    //    如果是该 endpoint 上第一个该 streamId 的 subscription，才需要真正注册
                    addNetworkSubscriptionToReceiver(subscription);
                }

                // ⑤ 向 Client 回写 ON_SUBSCRIPTION_READY
                clientProxy.onSubscriptionReady(registrationId, channelEndpoint.statusIndicatorCounter().id());

                // ⑥ 检查是否已有匹配的 Publisher 正在发送，如果有则立即通知 Client "Image 可用"
                linkMatchingImages(subscription);
            });
    }

    private void addNetworkSubscriptionToReceiver(final NetworkSubscriptionLink subscription)
    {
        final ReceiveChannelEndpoint channelEndpoint = subscription.channelEndpoint();

        if (subscription.hasSessionId())
        {
            if (1 == channelEndpoint.incRefToStreamAndSession(subscription.streamId(), subscription.sessionId()))
            {
                receiverProxy.addSubscription(channelEndpoint, subscription.streamId(), subscription.sessionId());
            }
        }
        else
        {
            if (1 == channelEndpoint.incRefToStream(subscription.streamId()))
            {
                receiverProxy.addSubscription(channelEndpoint, subscription.streamId());
            }
        }
    }

    /**
     * 【处理 ADD_SUBSCRIPTION 命令（IPC 类型）】
     * 当 Client 调用 aeron.addSubscription("aeron:ipc", streamId) 时触发。
     *
     * IPC Subscription 直接关联同一 Driver 内的 IPC Publication，
     * 读写双方通过 mmap 同一个 LogBuffer 文件实现零拷贝。
     */
    void onAddIpcSubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        final SubscriptionParams params = SubscriptionParams.getSubscriptionParams(parseUri(channel), ctx, 0);
        final IpcSubscriptionLink subscriptionLink = new IpcSubscriptionLink(
            registrationId, streamId, channel, getOrAddClient(clientId), params);

        subscriptionLinks.add(subscriptionLink);
        clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

        // 遍历所有 IPC Publication，找到匹配的并立即通知 Client "Image 可用"
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                // 通知 Client: 一个 Image 已可用，附带 LogBuffer 文件路径供 Client mmap
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    streamId,
                    publication.sessionId(),
                    registrationId,
                    linkIpcSubscription(publication, subscriptionLink).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    void onAddSpySubscription(final String channel, final int streamId, final long registrationId, final long clientId)
    {
        executeAsyncClientTask(
            registrationId,
            () -> UdpChannel.parse(channel, nameResolver, false),
            (asyncResult) ->
            {
                final UdpChannel udpChannel = asyncResult.get();
                final SubscriptionParams params =
                    SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
                final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
                    registrationId, udpChannel, streamId, getOrAddClient(clientId), params);

                subscriptionLinks.add(subscriptionLink);
                clientProxy.onSubscriptionReady(registrationId, ChannelEndpointStatus.NO_ID_ALLOCATED);

                for (int i = 0, size = networkPublications.size(); i < size; i++)
                {
                    final NetworkPublication publication = networkPublications.get(i);
                    if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
                    {
                        clientProxy.onAvailableImage(
                            publication.registrationId(),
                            streamId,
                            publication.sessionId(),
                            registrationId,
                            linkSpy(publication, subscriptionLink).id(),
                            publication.rawLog().fileName(),
                            IPC_CHANNEL);
                    }
                }
            });
    }

    void onRemoveSubscription(final long registrationId, final long correlationId)
    {
        boolean isAnySubscriptionFound = false;
        for (int lastIndex = subscriptionLinks.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId)
            {
                fastUnorderedRemove(subscriptionLinks, i, lastIndex--);

                subscription.close();
                cleanupSubscriptionLink(subscription);
                isAnySubscriptionFound = true;
            }
        }

        if (!isAnySubscriptionFound)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        clientProxy.operationSucceeded(correlationId);
    }

    /**
     * 【心跳处理】Client 定期通过 CnC to-driver RingBuffer 发送 KEEPALIVE 命令。
     * Conductor 收到后更新该 Client 的最后心跳时间。
     * 若超过 clientLivenessTimeoutNs 未收到心跳，processTimers 中会触发 Client 超时清理。
     */
    void onClientKeepalive(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.timeOfLastKeepaliveMs(cachedEpochClock.time());
        }
    }

    void onAddCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long correlationId,
        final long clientId)
    {
        final AeronClient client = getOrAddClient(clientId);
        final AtomicCounter counter = countersManager.newCounter(
            typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

        countersManager.setCounterRegistrationId(counter.id(), correlationId);
        countersManager.setCounterOwnerId(counter.id(), clientId);
        counterLinks.add(new CounterLink(counter, correlationId, client));
        clientProxy.onCounterReady(correlationId, counter.id());
    }

    void onAddStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId,
        final long correlationId,
        final long clientId)
    {
        getOrAddClient(clientId);

        final int counterId = countersManager.findByTypeIdAndRegistrationId(typeId, registrationId);
        if (CountersReader.NULL_COUNTER_ID != counterId)
        {
            if (NULL_VALUE != countersManager.getCounterOwnerId(counterId))
            {
                clientProxy.onError(correlationId, GENERIC_ERROR, "cannot add static counter, because a " +
                    "non-static counter exists (counterId=" + counterId + ") for typeId=" + typeId + " and " +
                    "registrationId=" + registrationId);
            }
            else
            {
                clientProxy.onStaticCounter(correlationId, counterId);
            }
        }
        else
        {
            final AtomicCounter counter = countersManager.newCounter(
                typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);

            countersManager.setCounterRegistrationId(counter.id(), registrationId);
            countersManager.setCounterOwnerId(counter.id(), NULL_VALUE);
            clientProxy.onStaticCounter(correlationId, counter.id());
        }
    }

    void onRemoveCounter(final long registrationId, final long correlationId)
    {
        CounterLink counterLink = null;
        final ArrayList<CounterLink> counterLinks = this.counterLinks;
        for (int i = 0, size = counterLinks.size(); i < size; i++)
        {
            final CounterLink link = counterLinks.get(i);
            if (registrationId == link.registrationId())
            {
                counterLink = link;
                fastUnorderedRemove(counterLinks, i);
                break;
            }
        }

        if (null == counterLink)
        {
            throw new ControlProtocolException(UNKNOWN_COUNTER, "unknown counter: " + registrationId);
        }

        clientProxy.operationSucceeded(correlationId);
        clientProxy.onUnavailableCounter(registrationId, counterLink.counterId());
        counterLink.close();
    }

    void onClientClose(final long clientId)
    {
        final AeronClient client = findClient(clients, clientId);
        if (null != client)
        {
            client.onClosedByCommand();
        }
    }

    void onAddRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        if (destinationChannel.startsWith(IPC_CHANNEL))
        {
            onAddRcvIpcDestination(registrationId, destinationChannel, correlationId);
        }
        else if (destinationChannel.startsWith(SPY_QUALIFIER))
        {
            onAddRcvSpyDestination(registrationId, destinationChannel, correlationId);
        }
        else
        {
            onAddRcvNetworkDestination(registrationId, destinationChannel, correlationId);
        }
    }

    void onAddRcvIpcDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        final SubscriptionParams params =
            SubscriptionParams.getSubscriptionParams(parseUri(destinationChannel), ctx, 0);
        final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

        if (null == mdsSubscriptionLink)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown MDS subscription: " + registrationId);
        }

        final IpcSubscriptionLink subscriptionLink = new IpcSubscriptionLink(
            registrationId,
            mdsSubscriptionLink.streamId(),
            destinationChannel,
            mdsSubscriptionLink.aeronClient(),
            params);

        subscriptionLinks.add(subscriptionLink);
        clientProxy.operationSucceeded(correlationId);

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    mdsSubscriptionLink.streamId(),
                    publication.sessionId(),
                    registrationId,
                    linkIpcSubscription(publication, subscriptionLink).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    void onAddRcvSpyDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        executeAsyncClientTask(
            correlationId,
            () -> UdpChannel.parse(destinationChannel, nameResolver, false),
            (asyncResult) ->
            {
                final UdpChannel udpChannel = asyncResult.get();
                final SubscriptionParams params =
                    SubscriptionParams.getSubscriptionParams(udpChannel.channelUri(), ctx, 0);
                final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

                if (null == mdsSubscriptionLink)
                {
                    throw new ControlProtocolException(
                        UNKNOWN_SUBSCRIPTION, "unknown MDS subscription: " + registrationId);
                }

                final SpySubscriptionLink subscriptionLink = new SpySubscriptionLink(
                    registrationId,
                    udpChannel,
                    mdsSubscriptionLink.streamId(),
                    mdsSubscriptionLink.aeronClient(),
                    params);

                subscriptionLinks.add(subscriptionLink);
                clientProxy.operationSucceeded(correlationId);

                for (int i = 0, size = networkPublications.size(); i < size; i++)
                {
                    final NetworkPublication publication = networkPublications.get(i);
                    if (subscriptionLink.matches(publication) && publication.isAcceptingSubscriptions())
                    {
                        clientProxy.onAvailableImage(
                            publication.registrationId(),
                            mdsSubscriptionLink.streamId(),
                            publication.sessionId(),
                            registrationId,
                            linkSpy(publication, subscriptionLink).id(),
                            publication.rawLog().fileName(),
                            IPC_CHANNEL);
                    }
                }
            });
    }

    void onAddRcvNetworkDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        executeAsyncClientTask(
            correlationId,
            () -> UdpChannel.parse(destinationChannel, nameResolver, true),
            (asyncResult) ->
            {
                final UdpChannel udpChannel = asyncResult.get();
                validateDestinationUri(udpChannel.channelUri(), destinationChannel);

                final SubscriptionLink mdsSubscriptionLink = findMdsSubscriptionLink(subscriptionLinks, registrationId);

                if (null == mdsSubscriptionLink)
                {
                    throw new ControlProtocolException(
                        UNKNOWN_SUBSCRIPTION, "unknown MDS subscription: " + registrationId);
                }

                final ReceiveChannelEndpoint receiveChannelEndpoint = mdsSubscriptionLink.channelEndpoint();

                AtomicCounter localSocketAddressIndicator = null;
                ReceiveDestinationTransport transport = null;

                try
                {
                    localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
                        tempBuffer,
                        countersManager,
                        registrationId,
                        receiveChannelEndpoint.statusIndicatorCounter().id());

                    transport = new ReceiveDestinationTransport(
                        udpChannel, ctx, localSocketAddressIndicator, receiveChannelEndpoint);

                    transport.openChannel(null);
                }
                catch (final Exception ex)
                {
                    CloseHelper.closeAll(localSocketAddressIndicator, transport);
                    throw ex;
                }

                receiverProxy.addDestination(receiveChannelEndpoint, transport);
                clientProxy.operationSucceeded(correlationId);
            });
    }

    void onRemoveRcvDestination(final long registrationId, final String destinationChannel, final long correlationId)
    {
        if (destinationChannel.startsWith(IPC_CHANNEL) || destinationChannel.startsWith(SPY_QUALIFIER))
        {
            onRemoveRcvIpcOrSpyDestination(registrationId, destinationChannel, correlationId);
        }
        else
        {
            onRemoveRcvNetworkDestination(registrationId, destinationChannel, correlationId);
        }
    }

    void onRemoveRcvIpcOrSpyDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        final SubscriptionLink subscription =
            removeSubscriptionLink(subscriptionLinks, registrationId, destinationChannel);

        if (null == subscription)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        subscription.close();
        cleanupSubscriptionLink(subscription);
        clientProxy.operationSucceeded(correlationId);
        subscription.notifyUnavailableImages(this);
    }

    void onRemoveRcvNetworkDestination(
        final long registrationId, final String destinationChannel, final long correlationId)
    {
        ReceiveChannelEndpoint receiveChannelEndpoint = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
            if (registrationId == subscriptionLink.registrationId())
            {
                receiveChannelEndpoint = subscriptionLink.channelEndpoint();
                break;
            }
        }

        if (null == receiveChannelEndpoint)
        {
            throw new ControlProtocolException(UNKNOWN_SUBSCRIPTION, "unknown subscription: " + registrationId);
        }

        receiveChannelEndpoint.validateAllowsDestinationControl();

        final ReceiveChannelEndpoint endpoint = receiveChannelEndpoint;
        executeAsyncClientTask(
            correlationId,
            () -> UdpChannel.parse(destinationChannel, nameResolver, true),
            (asyncResult) ->
            {
                receiverProxy.removeDestination(endpoint, asyncResult.get());
                clientProxy.operationSucceeded(correlationId);
            });
    }

    void closeReceiveDestination(final ReceiveDestinationTransport destinationTransport)
    {
        destinationTransport.close();
    }

    void onTerminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        if (ctx.terminationValidator().allowTermination(ctx.aeronDirectory(), tokenBuffer, tokenOffset, tokenLength))
        {
            ctx.terminationHook().run();
        }
    }

    void onRejectImage(
        final long correlationId,
        final long imageCorrelationId,
        final long position,
        final String reason)
    {
        if (reason.length() > ErrorFlyweight.MAX_ERROR_MESSAGE_LENGTH)
        {
            throw new ControlProtocolException(GENERIC_ERROR, "Invalidation reason must be " +
                ErrorFlyweight.MAX_ERROR_MESSAGE_LENGTH + " bytes or less");
        }

        final PublicationImage publicationImage = findPublicationImage(imageCorrelationId);

        if (null == publicationImage)
        {
            final IpcPublication foundPublication = getIpcPublication(imageCorrelationId);

            if (null == foundPublication)
            {
                throw new ControlProtocolException(
                    GENERIC_ERROR, "Unable to resolve image for correlationId=" + imageCorrelationId);
            }

            foundPublication.reject(position, reason, this, cachedNanoClock.nanoTime());
        }
        else
        {
            receiverProxy.rejectImage(imageCorrelationId, position, reason);
        }

        imagesRejected.incrementRelease();

        clientProxy.operationSucceeded(correlationId);
    }

    void onNextAvailableSessionId(final long correlationId, final int streamId)
    {
        outer:
        while (true)
        {
            final int sessionId = advanceSessionId();

            for (final SessionKey key : activeSessionSet)
            {
                if (streamId == key.streamId && sessionId == key.sessionId)
                {
                    continue outer;
                }
            }

            clientProxy.onNextAvailableSessionId(correlationId, sessionId);
            break;
        }
    }

    int nextAvailableSessionId(final int streamId, final String channel)
    {
        final SessionKey sessionKey = new SessionKey(streamId, channel);
        while (true)
        {
            final int sessionId = advanceSessionId();

            sessionKey.sessionId = sessionId;
            if (!activeSessionSet.contains(sessionKey))
            {
                return sessionId;
            }
        }
    }

    private int advanceSessionId()
    {
        int sessionId = nextSessionId++;

        if (ctx.publicationReservedSessionIdLow() <= sessionId &&
            sessionId <= ctx.publicationReservedSessionIdHigh())
        {
            nextSessionId = ctx.publicationReservedSessionIdHigh() + 1;
            sessionId = nextSessionId++;
        }
        return sessionId;
    }

    private void heartbeatAndCheckTimers(final long nowNs)
    {
        final long nowMs = cachedEpochClock.time();
        toDriverCommands.consumerHeartbeatTime(nowMs);

        checkManagedResources(clients, nowNs, nowMs);
        checkManagedResources(publicationLinks, nowNs, nowMs);
        checkManagedResources(networkPublications, nowNs, nowMs);
        checkManagedResources(subscriptionLinks, nowNs, nowMs);
        checkManagedResources(publicationImages, nowNs, nowMs);
        checkManagedResources(ipcPublications, nowNs, nowMs);
        checkManagedResources(counterLinks, nowNs, nowMs);
    }

    private void checkForBlockedToDriverCommands(final long nowNs)
    {
        final long consumerPosition = toDriverCommands.consumerPosition();

        if (consumerPosition == lastCommandConsumerPosition && toDriverCommands.producerPosition() > consumerPosition)
        {
            if ((timeOfLastToDriverPositionChangeNs + clientLivenessTimeoutNs) - nowNs < 0)
            {
                if (toDriverCommands.unblock())
                {
                    ctx.systemCounters().get(UNBLOCKED_COMMANDS).incrementRelease();
                }
            }
        }
        else
        {
            timeOfLastToDriverPositionChangeNs = nowNs;
            lastCommandConsumerPosition = consumerPosition;
        }
    }

    private static ChannelUri parseUri(final String channel)
    {
        try
        {
            return ChannelUri.parse(channel);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    private ArrayList<SubscriberPosition> createSubscriberPositions(
        final int sessionId, final int streamId, final ReceiveChannelEndpoint channelEndpoint, final long joinPosition)
    {
        final ArrayList<SubscriberPosition> subscriberPositions = new ArrayList<>();

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(channelEndpoint, streamId, sessionId))
            {
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    subscription.aeronClient().clientId(),
                    subscription.registrationId(),
                    sessionId,
                    streamId,
                    subscription.channel(),
                    joinPosition);

                position.setRelease(joinPosition);
                subscriberPositions.add(new SubscriberPosition(subscription, null, position));
            }
        }

        return subscriberPositions;
    }

    private void executeAsyncClientTask(
        final long correlationId,
        final Supplier<UdpChannel> asyncTask,
        final Consumer<Supplier<UdpChannel>> command)
    {
        if (asyncExecutionDisabled)
        {
            command.accept(asyncTask);
        }
        else
        {
            asyncClientCommandInFlight = true;
            asyncTaskExecutor.execute(() ->
            {
                final AsyncResult<UdpChannel> asyncResult = AsyncResult.of(asyncTask);
                addToCommandQueue(() ->
                {
                    try
                    {
                        command.accept(asyncResult);
                    }
                    catch (final Exception ex)
                    {
                        clientCommandAdapter.onError(correlationId, ex);
                    }
                    finally
                    {
                        asyncClientCommandInFlight = false;
                    }
                });
            });
        }
    }

    private <T> void executeAsyncTask(final Supplier<T> supplier, final Consumer<Supplier<T>> command)
    {
        if (asyncExecutionDisabled)
        {
            command.accept(supplier);
        }
        else
        {
            asyncTaskExecutor.execute(() ->
            {
                final AsyncResult<T> asyncResult = AsyncResult.of(supplier);
                addToCommandQueue(() -> command.accept(asyncResult));
            });
        }
    }

    private void addToCommandQueue(final Runnable cmd)
    {
        if (!driverCmdQueue.offer(cmd))
        {
            // unreachable for ManyToOneConcurrentLinkedQueue
            throw new IllegalStateException(driverCmdQueue.getClass().getSimpleName() + ".offer failed!");
        }
    }

    private static NetworkPublication findPublication(
        final ArrayList<NetworkPublication> publications,
        final int streamId,
        final SendChannelEndpoint channelEndpoint,
        final long responseCorrelationId)
    {
        for (int i = 0, size = publications.size(); i < size; i++)
        {
            final NetworkPublication publication = publications.get(i);

            if (streamId == publication.streamId() &&
                channelEndpoint == publication.channelEndpoint() &&
                NetworkPublication.State.ACTIVE == publication.state() &&
                !publication.isExclusive() &&
                publication.responseCorrelationId() == responseCorrelationId)
            {
                return publication;
            }
        }

        return null;
    }

    /**
     * 创建一个新的 NetworkPublication 实例。
     * 包括：分配 LogBuffer 文件、创建 FlowControl、分配各种 Counter、构造 Publication 对象，
     * 并注册到 Sender 线程以开始网络发送。
     */
    @SuppressWarnings("MethodLength")
    private NetworkPublication newNetworkPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final UdpChannel udpChannel,
        final SendChannelEndpoint channelEndpoint,
        final PublicationParams params,
        final boolean isExclusive)
    {
        if (params.isResponse &&
            PROTOTYPE_VALUE_CORRELATION_ID == params.responseCorrelationId)
        {
            params.termLength = TERM_MIN_LENGTH;
        }

        final String canonicalForm = udpChannel.canonicalForm();

        // 创建 FlowControl 实例：控制发送速率，防止接收端被淹没
        // 多播/多目标 → multicastFlowControlSupplier（如 MinMulticastFlowControl，取最慢接收者的位置）
        // 单播 → unicastFlowControlSupplier（如 MaxFlowControl，只跟踪单个接收者）
        final FlowControl flowControl = udpChannel.isMulticast() || udpChannel.isMultiDestination() ?
            ctx.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
            ctx.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);
        flowControl.initialize(
            ctx,
            countersManager,
            udpChannel,
            streamId,
            params.sessionId,
            registrationId,
            params.initialTermId,
            params.termLength);

        final int termOffset = params.termOffset;

        // 分配 RawLog（LogBuffer 文件）：在 aeronDir/publications/ 目录下创建内存映射文件
        // 文件包含 3 个 Term Buffer（轮转写入）+ 1 个 Log Metadata Buffer
        final RawLog rawLog = newNetworkPublicationLog(
            isExclusive,
            params.sessionId,
            streamId,
            params.initialTermId,
            registrationId,
            channelEndpoint.socketRcvbufLength(),
            channelEndpoint.socketSndbufLength(),
            termOffset,
            params,
            udpChannel.hasGroupSemantics());

        // 以下 Counter 都分配在 CnC 文件的 counters 区域，Client 和 Driver 通过 mmap 共享访问
        UnsafeBufferPosition publisherPos = null;
        UnsafeBufferPosition publisherLmt = null;
        UnsafeBufferPosition senderPos = null;
        UnsafeBufferPosition senderLmt = null;
        AtomicCounter senderBpe = null;
        AtomicCounter senderNaksReceived = null;
        try
        {
            // publisherPos：发布者已写入的位置（Client offer() 时更新）
            publisherPos = PublisherPos.allocate(
                tempBuffer,
                countersManager,
                clientId,
                registrationId,
                params.sessionId,
                streamId,
                channel,
                isExclusive);
            // publisherLimit：发布者可写入的最大位置（Driver 基于流控计算后更新，Client offer() 时读取检查）
            publisherLmt = PublisherLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            // senderPos：Sender 线程已发送到网络的位置
            senderPos = SenderPos.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            // senderLimit：Sender 线程可发送的最大位置（由 FlowControl 根据接收端反馈计算）
            senderLmt = SenderLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            // senderBpe：背压事件计数（Sender 尝试发送但被流控限制时递增）
            senderBpe = SenderBpe.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);
            // senderNaksReceived：收到的 NAK（否定确认）计数，用于触发重传
            senderNaksReceived = SenderNaksReceived.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);

            final AtomicCounter retransmitOverflowCounter = ctx.systemCounters().get(RETRANSMIT_OVERFLOW);

            // 如果 Client 指定了起始位置，则初始化所有位置 Counter
            if (params.hasPosition)
            {
                final int bits = LogBufferDescriptor.positionBitsToShift(params.termLength);
                final long position = computePosition(params.termId, params.termOffset, bits, params.initialTermId);
                publisherPos.setRelease(position);
                publisherLmt.setRelease(position);
                senderPos.setRelease(position);
                senderLmt.setRelease(position);
            }

            // RetransmitHandler：当收到接收端的 NAK 时负责调度重传
            final RetransmitHandler retransmitHandler = new RetransmitHandler(
                ctx.senderCachedNanoClock(),
                ctx.systemCounters().get(INVALID_PACKETS),
                ctx.retransmitUnicastDelayGenerator(),
                ctx.retransmitUnicastLingerGenerator(),
                udpChannel.hasGroupSemantics(),
                params.maxResend,
                retransmitOverflowCounter);

            // 构造 NetworkPublication 对象，汇聚上述所有资源
            final NetworkPublication publication = new NetworkPublication(
                registrationId,
                ctx,
                params,
                channelEndpoint,
                rawLog,
                params.publicationWindowLength,
                publisherPos,
                publisherLmt,
                senderPos,
                senderLmt,
                senderBpe,
                senderNaksReceived,
                params.sessionId,
                streamId,
                params.initialTermId,
                flowControl,
                retransmitHandler,
                networkPublicationThreadLocals,
                isExclusive);

            channelEndpoint.incRef();                      // endpoint 引用计数+1（共享 UDP Socket）
            networkPublications.add(publication);           // 加入 Conductor 的 publication 列表
            senderProxy.newNetworkPublication(publication); // 通知 Sender 线程：有新 Publication 需要发送
            activeSessionSet.add(new SessionKey(params.sessionId, streamId, canonicalForm));

            return publication;
        }
        catch (final Exception ex)
        {
            // 创建失败时关闭已分配的资源，避免泄漏
            CloseHelper.quietCloseAll(
                rawLog, publisherPos, publisherLmt, senderPos, senderLmt, senderBpe, senderNaksReceived);
            throw ex;
        }
    }

    /**
     * 【LogBuffer 文件创建点】为网络 Publication 分配 LogBuffer 文件并初始化元数据。
     * <p>
     * 创建链路：
     * <pre>
     *   logFactory.newPublication(registrationId, termLength, isSparse)
     *     → FileStoreLogFactory.newInstance()
     *       → new MappedRawLog(file, ...)  ← 在 {aeronDir}/publications/{registrationId}.logbuffer 创建文件并 mmap
     * </pre>
     * 创建完成后，Driver 在此文件上初始化 Log Metadata（sessionId、streamId、initialTermId、mtu 等），
     * 然后通过 PUBLICATION_READY 响应将文件路径发送给 Client，Client 再做 mmap 映射实现共享。
     */
    private RawLog newNetworkPublicationLog(
        final boolean isExclusive,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId,
        final int socketRcvBufLength,
        final int socketSndBufLength,
        final int termOffset,
        final PublicationParams params,
        final boolean hasGroupSemantics)
    {
        // ★ 这里是 LogBuffer 文件实际创建的地方：创建文件 → 设置大小 → mmap 映射
        final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);
        final int receiverWindowLength = 0;
        final boolean tether = false;
        final boolean rejoin = false;
        final boolean reliable = false;
        initLogMetadata(
            isExclusive ? LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION : LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION,
            sessionId,
            streamId,
            initialTermId,
            params.mtuLength,
            registrationId,
            socketRcvBufLength,
            socketSndBufLength,
            termOffset,
            receiverWindowLength,
            tether,
            rejoin,
            reliable,
            params.isSparse,
            hasGroupSemantics,
            params.isResponse,
            params.publicationWindowLength,
            params.untetheredWindowLimitTimeoutNs,
            params.untetheredLingerTimeoutNs,
            params.untetheredRestingTimeoutNs,
            params.maxResend,
            params.lingerTimeoutNs,
            params.signalEos,
            params.spiesSimulateConnection,
            params.entityTag,
            params.responseCorrelationId,
            rawLog);
        initialisePositionCounters(initialTermId, params, rawLog.metaData());

        return rawLog;
    }

    private RawLog newIpcPublicationLog(
        final boolean isExclusive,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final long registrationId,
        final int termOffset,
        final PublicationParams params)
    {
        final RawLog rawLog = logFactory.newPublication(registrationId, params.termLength, params.isSparse);

        final int socketRcvBufLength = 0;
        final int socketSndbufLength = 0;
        final int receiverWindowLength = 0;
        final boolean tether = false;
        final boolean rejoin = false;
        final boolean reliable = false;
        final boolean group = false;
        initLogMetadata(
            isExclusive ? LOG_BUFFER_TYPE_EXCLUSIVE_PUBLICATION : LOG_BUFFER_TYPE_CONCURRENT_PUBLICATION,
            sessionId,
            streamId,
            initialTermId,
            params.mtuLength,
            registrationId,
            socketRcvBufLength,
            socketSndbufLength,
            termOffset,
            receiverWindowLength,
            tether,
            rejoin,
            reliable,
            params.isSparse,
            group,
            params.isResponse,
            params.publicationWindowLength,
            params.untetheredWindowLimitTimeoutNs,
            params.untetheredLingerTimeoutNs,
            params.untetheredRestingTimeoutNs,
            params.maxResend,
            params.lingerTimeoutNs,
            params.signalEos,
            params.spiesSimulateConnection,
            params.entityTag,
            params.responseCorrelationId,
            rawLog);
        initialisePositionCounters(initialTermId, params, rawLog.metaData());

        return rawLog;
    }

    private void initLogMetadata(
        final byte type,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int mtuLength,
        final long registrationId,
        final int socketRcvBufLength,
        final int socketSndbufLength,
        final int termOffset,
        final int receiverWindowLength,
        final boolean tether,
        final boolean rejoin,
        final boolean reliable,
        final boolean sparse,
        final boolean group,
        final boolean isResponse,
        final int publicationWindowLength,
        final long untetheredWindowLimitTimeoutNs,
        final long untetheredLingerTimeoutNs,
        final long untetheredRestingTimeoutNs,
        final int maxResend,
        final long lingerTimeoutNs,
        final boolean signalEos,
        final boolean spiesSimulateConnection,
        final long entityTag,
        final long responseCorrelationId,
        final RawLog rawLog)
    {
        final UnsafeBuffer logMetaData = rawLog.metaData();

        defaultDataHeader
            .sessionId(sessionId)
            .streamId(streamId)
            .termId(initialTermId)
            .termOffset(termOffset);
        storeDefaultFrameHeader(logMetaData, defaultDataHeader);

        correlationId(logMetaData, registrationId);
        initialTermId(logMetaData, initialTermId);
        mtuLength(logMetaData, mtuLength);
        termLength(logMetaData, rawLog.termLength());
        pageSize(logMetaData, ctx.filePageSize());

        publicationWindowLength(logMetaData, publicationWindowLength);
        receiverWindowLength(logMetaData, receiverWindowLength);
        socketSndbufLength(logMetaData, socketSndbufLength);
        osDefaultSocketSndbufLength(logMetaData, ctx.osDefaultSocketSndbufLength());
        osMaxSocketSndbufLength(logMetaData, ctx.osMaxSocketSndbufLength());
        socketRcvbufLength(logMetaData, socketRcvBufLength);
        osDefaultSocketRcvbufLength(logMetaData, ctx.osDefaultSocketRcvbufLength());
        osMaxSocketRcvbufLength(logMetaData, ctx.osMaxSocketRcvbufLength());
        maxResend(logMetaData, maxResend);

        rejoin(logMetaData, rejoin);
        reliable(logMetaData, reliable);
        sparse(logMetaData, sparse);
        signalEos(logMetaData, signalEos);
        spiesSimulateConnection(logMetaData, spiesSimulateConnection);
        tether(logMetaData, tether);
        isPublicationRevoked(logMetaData, false);
        group(logMetaData, group);
        isResponse(logMetaData, isResponse);
        type(logMetaData, type);

        entityTag(logMetaData, entityTag);
        responseCorrelationId(logMetaData, responseCorrelationId);
        untetheredWindowLimitTimeoutNs(logMetaData, untetheredWindowLimitTimeoutNs);
        untetheredLingerTimeoutNs(logMetaData, untetheredLingerTimeoutNs);
        untetheredRestingTimeoutNs(logMetaData, untetheredRestingTimeoutNs);
        lingerTimeoutNs(logMetaData, lingerTimeoutNs);

        // Acts like a release fence; so this should be the last statement to ensure that all above writes
        // are ordered before the eos-position.
        endOfStreamPosition(logMetaData, Long.MAX_VALUE);
    }


    private static void initialisePositionCounters(
        final int initialTermId, final PublicationParams params, final UnsafeBuffer logMetaData)
    {
        if (params.hasPosition)
        {
            final int termId = params.termId;
            final int termCount = termId - initialTermId;
            int activeIndex = indexByTerm(initialTermId, termId);

            rawTail(logMetaData, activeIndex, packTail(termId, params.termOffset));
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (termId + i) - PARTITION_COUNT;
                activeIndex = nextPartitionIndex(activeIndex);
                initialiseTailWithTermId(logMetaData, activeIndex, expectedTermId);
            }

            activeTermCount(logMetaData, termCount);
        }
        else
        {
            initialiseTailWithTermId(logMetaData, 0, initialTermId);
            for (int i = 1; i < PARTITION_COUNT; i++)
            {
                final int expectedTermId = (initialTermId + i) - PARTITION_COUNT;
                initialiseTailWithTermId(logMetaData, i, expectedTermId);
            }
        }
    }

    private RawLog newPublicationImageLog(
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int termBufferLength,
        final boolean isReliable,
        final boolean isSparse,
        final int senderMtuLength,
        final int socketRcvBufLength,
        final int socketSndBufLength,
        final int termOffset,
        final SubscriptionParams params,
        final long correlationId,
        final boolean hasGroupSemantics)
    {
        final RawLog rawLog = logFactory.newImage(correlationId, termBufferLength, isSparse);

        final int publicationWindowLength = 0;
        final int maxResend = 0;
        final long lingerTimeoutNs = 0;
        final boolean signalEos = false;
        final boolean spiesSimulateConnection = false;
        final long entityTag = 0;
        final long responseCorrelationId = 0;
        initLogMetadata(
            LOG_BUFFER_TYPE_PUBLICATION_IMAGE,
            sessionId,
            streamId,
            initialTermId,
            senderMtuLength,
            correlationId,
            socketRcvBufLength,
            socketSndBufLength,
            termOffset,
            params.receiverWindowLength,
            params.isTether,
            params.isRejoin,
            isReliable,
            isSparse,
            hasGroupSemantics,
            params.isResponse,
            publicationWindowLength,
            params.untetheredWindowLimitTimeoutNs,
            params.untetheredLingerTimeoutNs,
            params.untetheredRestingTimeoutNs,
            maxResend,
            lingerTimeoutNs,
            signalEos,
            spiesSimulateConnection,
            entityTag,
            responseCorrelationId,
            rawLog);

        return rawLog;
    }

    /**
     * 【UDP Socket 创建入口】获取或创建发送端的 ChannelEndpoint。
     * <p>
     * 同一个 UDP 地址（endpoint）复用同一个 SendChannelEndpoint（共享底层 DatagramChannel），
     * 多个不同 streamId 的 Publication 可以复用同一个 endpoint 发送数据。
     * <p>
     * 当需要新建时的完整链路：
     * <pre>
     *   getOrCreateSendChannelEndpoint()
     *     → ctx.sendChannelEndpointSupplier().newInstance()  ← 创建 SendChannelEndpoint 对象
     *     → channelEndpoint.openChannel()                    ← 打开 UDP Socket
     *       → openDatagramChannel()                          ← DatagramChannel.open() + bind()
     *     → senderProxy.registerSendChannelEndpoint()        ← 注册到 Sender 线程的 Selector
     *       → controlTransportPoller.registerForRead()       ← 用 NIO Selector 监听控制消息（NAK/SM）
     * </pre>
     */
    private SendChannelEndpoint getOrCreateSendChannelEndpoint(
        final PublicationParams params, final UdpChannel udpChannel, final long registrationId)
    {
        // 先查找是否已有相同地址的 endpoint 可以复用
        SendChannelEndpoint channelEndpoint = findExistingSendChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            // 没有可复用的，需要新建 endpoint 和底层 UDP Socket
            AtomicCounter statusIndicator = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                // 分配通道状态计数器（Client 可通过该计数器监控通道健康度）
                statusIndicator = SendChannelStatus.allocate(
                    tempBuffer, countersManager, registrationId, udpChannel.originalUriString());

                // 创建 SendChannelEndpoint 对象（此时还没有打开 Socket）
                channelEndpoint = ctx.sendChannelEndpointSupplier().newInstance(udpChannel, statusIndicator, ctx);

                localSocketAddressIndicator = SendLocalSocketAddress.allocate(
                    tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounterId());

                channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);
                channelEndpoint.allocateDestinationsCounterForMdc(
                    tempBuffer, countersManager, registrationId, udpChannel.originalUriString());

                validateMtuForSndbuf(
                    params, channelEndpoint.socketSndbufLength(), ctx, udpChannel.originalUriString(), null);

                // ★ 打开 UDP Socket：内部调用 DatagramChannel.open() + bind(bindAddress)
                // 对于单播：bind 到本地端口（可能由 PortManager 自动分配临时端口）
                // 对于多播：bind + join 多播组
                channelEndpoint.openChannel();
                channelEndpoint.indicateActive();

                // 将 endpoint 注册到 Sender 线程的 ControlTransportPoller（NIO Selector），
                // 用于监听来自接收端的控制消息（Status Message / NAK）
                senderProxy.registerSendChannelEndpoint(channelEndpoint);
                // 缓存 endpoint，后续相同地址的 Publication 可以复用
                sendChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(statusIndicator, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateChannelSendTimestampOffset(udpChannel, channelEndpoint);
            validateMtuForSndbuf(
                params,
                channelEndpoint.socketSndbufLength(),
                ctx,
                udpChannel.originalUriString(),
                channelEndpoint.originalUriString());
            validateChannelBufferLength(
                SOCKET_RCVBUF_PARAM_NAME,
                udpChannel.socketRcvbufLength(),
                channelEndpoint.socketRcvbufLength(),
                udpChannel.originalUriString(),
                channelEndpoint.originalUriString());
            validateChannelBufferLength(
                SOCKET_SNDBUF_PARAM_NAME,
                udpChannel.socketSndbufLength(),
                channelEndpoint.socketSndbufLength(),
                udpChannel.originalUriString(),
                channelEndpoint.originalUriString());
        }

        return channelEndpoint;
    }

    private void validateChannelSendTimestampOffset(
        final UdpChannel udpChannel, final SendChannelEndpoint channelEndpoint)
    {
        if (udpChannel.channelSendTimestampOffset() != channelEndpoint.udpChannel().channelSendTimestampOffset())
        {
            throw new InvalidChannelException(
                "option conflicts with existing subscription: " + CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME + "=" +
                    udpChannel.channelSendTimestampOffset() +
                    " existingChannel=" + channelEndpoint.originalUriString() + " channel=" +
                    udpChannel.originalUriString());
        }
    }

    private void validateReceiveTimestampOffset(
        final UdpChannel udpChannel, final ReceiveChannelEndpoint channelEndpoint)
    {
        if (udpChannel.channelReceiveTimestampOffset() !=
            channelEndpoint.subscriptionUdpChannel().channelReceiveTimestampOffset())
        {
            throw new InvalidChannelException(
                "option conflicts with existing subscription: " + CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME + "=" +
                    udpChannel.channelReceiveTimestampOffset() +
                    " existingChannel=" + channelEndpoint.originalUriString() + " channel=" +
                    udpChannel.originalUriString());
        }
    }

    private SendChannelEndpoint findExistingSendChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final SendChannelEndpoint endpoint : sendChannelEndpointByChannelMap.values())
            {
                if (endpoint.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }

            if (!udpChannel.hasExplicitControl() && !udpChannel.isManualControlMode() &&
                !udpChannel.channelUri().containsKey(ENDPOINT_PARAM_NAME))
            {
                throw new InvalidChannelException(
                    "URI must have explicit control, endpoint, or be manual control-mode when original: channel=" +
                        udpChannel.originalUriString());
            }
        }

        SendChannelEndpoint endpoint = sendChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.udpChannel().hasTag() && udpChannel.hasTag() &&
            endpoint.udpChannel().tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        return endpoint;
    }

    private void checkForClashingSubscription(
        final SubscriptionParams params, final UdpChannel udpChannel, final int streamId)
    {
        final ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null != channelEndpoint)
        {
            validateReceiveTimestampOffset(udpChannel, channelEndpoint);

            for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
            {
                final SubscriptionLink subscription = subscriptionLinks.get(i);
                final boolean matchesTag = !udpChannel.hasTag() || channelEndpoint.matchesTag(udpChannel);

                if (matchesTag && subscription.matches(channelEndpoint, streamId, params))
                {
                    if (params.isReliable != subscription.isReliable())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: reliable=" + params.isReliable +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }

                    if (params.isRejoin != subscription.isRejoin())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: rejoin=" + params.isRejoin +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }

                    if (params.isResponse != subscription.isResponse())
                    {
                        throw new InvalidChannelException(
                            "option conflicts with existing subscription: isResponse=" + params.isResponse +
                                " existingChannel=" + subscription.channel() + " channel=" +
                                udpChannel.originalUriString());
                    }
                }
            }
        }
    }

    private void linkMatchingImages(final SubscriptionLink subscriptionLink)
    {
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            final PublicationImage image = publicationImages.get(i);
            if (subscriptionLink.matches(image) && image.isAcceptingSubscriptions())
            {
                final long registrationId = subscriptionLink.registrationId();
                final long joinPosition = image.joinPosition();
                final int sessionId = image.sessionId();
                final int streamId = subscriptionLink.streamId();
                final Position position = SubscriberPos.allocate(
                    tempBuffer,
                    countersManager,
                    subscriptionLink.aeronClient().clientId(),
                    registrationId,
                    sessionId,
                    streamId,
                    subscriptionLink.channel(),
                    joinPosition);

                countersManager.setCounterReferenceId(position.id(), image.correlationId());

                position.setRelease(joinPosition);
                subscriptionLink.link(image, position);
                image.addSubscriber(subscriptionLink, position, cachedNanoClock.nanoTime());

                clientProxy.onAvailableImage(
                    image.correlationId(),
                    streamId,
                    sessionId,
                    registrationId,
                    position.id(),
                    image.rawLog().fileName(),
                    image.sourceIdentity());
            }
        }
    }

    void linkIpcSubscriptions(final IpcPublication publication)
    {
        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.matches(publication) &&
                !subscription.isLinked(publication) &&
                publication.isAcceptingSubscriptions())
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    publication.streamId(),
                    publication.sessionId(),
                    subscription.registrationId,
                    linkIpcSubscription(publication, subscription).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    private Position linkIpcSubscription(final IpcPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.joinPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int sessionId = publication.sessionId();
        final int streamId = subscription.streamId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        countersManager.setCounterReferenceId(position.id(), publication.registrationId());

        position.setRelease(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    private Position linkSpy(final NetworkPublication publication, final SubscriptionLink subscription)
    {
        final long joinPosition = publication.consumerPosition();
        final long registrationId = subscription.registrationId();
        final long clientId = subscription.aeronClient().clientId();
        final int streamId = publication.streamId();
        final int sessionId = publication.sessionId();
        final String channel = subscription.channel();

        final Position position = SubscriberPos.allocate(
            tempBuffer, countersManager, clientId, registrationId, sessionId, streamId, channel, joinPosition);

        countersManager.setCounterReferenceId(position.id(), publication.registrationId());

        position.setRelease(joinPosition);
        subscription.link(publication, position);
        publication.addSubscriber(subscription, position, cachedNanoClock.nanoTime());

        return position;
    }

    /**
     * 获取或创建 ReceiveChannelEndpoint —— 这是 UDP 端口监听的核心入口。
     * <p>
     * 同一 UDP 地址（如 localhost:40123）复用同一个 ReceiveChannelEndpoint（共享 DatagramChannel）。
     * 多个 streamId 的 subscription 可以共享一个 endpoint，通过 DataPacketDispatcher 按 streamId 分发。
     * <p>
     * 当创建新 endpoint 时，完整流程：
     * <ol>
     *   <li>new ReceiveChannelEndpoint → 内部构造 UdpChannelTransport（持有 bindAddress）</li>
     *   <li>openChannel() → openDatagramChannel()：创建 DatagramChannel 并 bind 到 UDP 端口</li>
     *   <li>receiverProxy.registerReceiveChannelEndpoint() → Receiver 线程中注册到 NIO Selector</li>
     * </ol>
     * 之后 Receiver 线程在 doWork() 循环中通过 Selector 轮询该 DatagramChannel 收包。
     */
    private ReceiveChannelEndpoint getOrCreateReceiveChannelEndpoint(
        final SubscriptionParams params, final UdpChannel udpChannel, final long registrationId)
    {
        // 先查找是否已有同一 UDP 地址的 endpoint（复用）
        ReceiveChannelEndpoint channelEndpoint = findExistingReceiveChannelEndpoint(udpChannel);
        if (null == channelEndpoint)
        {
            // ========== 新建 ReceiveChannelEndpoint ==========
            AtomicCounter channelStatus = null;
            AtomicCounter localSocketAddressIndicator = null;
            try
            {
                final String channel = udpChannel.originalUriString();
                channelStatus = ReceiveChannelStatus.allocate(tempBuffer, countersManager, registrationId, channel);

                // ① 创建 DataPacketDispatcher：负责按 streamId+sessionId 将 UDP 数据包分发到对应的 PublicationImage
                final DataPacketDispatcher dispatcher = new DataPacketDispatcher(
                    ctx.driverConductorProxy(), receiverProxy.receiver(), ctx.streamSessionLimit());

                // ② 创建 ReceiveChannelEndpoint（继承自 UdpChannelTransport）
                //    构造函数中传入 udpChannel.remoteData() 作为 bindAddress（即 "endpoint=localhost:40123" 解析出的地址）
                //    此时 DatagramChannel 尚未创建
                channelEndpoint = ctx.receiveChannelEndpointSupplier().newInstance(
                    udpChannel, dispatcher, channelStatus, ctx);

                if (!udpChannel.isManualControlMode())
                {
                    localSocketAddressIndicator = ReceiveLocalSocketAddress.allocate(
                        tempBuffer, countersManager, registrationId, channelEndpoint.statusIndicatorCounter().id());

                    channelEndpoint.localSocketAddressIndicator(localSocketAddressIndicator);
                }

                validateInitialWindowForRcvBuf(params, channel, channelEndpoint.socketRcvbufLength(), ctx, null);

                // ③ ★ 核心：打开 DatagramChannel 并绑定 UDP 端口 ★
                //    openChannel() → openDatagramChannel()：
                //      DatagramChannel.open() → channel.bind(bindAddress) → channel.configureBlocking(false)
                //    绑定的地址就是 URI 中 endpoint 参数指定的 "localhost:40123"
                channelEndpoint.openChannel();
                channelEndpoint.indicateActive();

                // ④ 将 endpoint 注册到 Receiver 线程
                //    Receiver.onRegisterReceiveChannelEndpoint()：
                //      dataTransportPoller.registerForRead(endpoint)：
                //        DatagramChannel.register(selector, OP_READ) ← 注册到 NIO Selector
                //    之后 Receiver 线程的 doWork() 循环中 selector.selectNow() 即可轮询到达的 UDP 包
                receiverProxy.registerReceiveChannelEndpoint(channelEndpoint);
                receiveChannelEndpointByChannelMap.put(udpChannel.canonicalForm(), channelEndpoint);
            }
            catch (final Exception ex)
            {
                CloseHelper.closeAll(channelStatus, localSocketAddressIndicator, channelEndpoint);
                throw ex;
            }
        }
        else
        {
            validateInitialWindowForRcvBuf(
                params,
                udpChannel.originalUriString(),
                channelEndpoint.socketRcvbufLength(),
                ctx,
                channelEndpoint.originalUriString());
            validateChannelBufferLength(
                SOCKET_RCVBUF_PARAM_NAME,
                udpChannel.socketRcvbufLength(),
                channelEndpoint.socketRcvbufLength(),
                udpChannel.originalUriString(),
                channelEndpoint.originalUriString());
            validateChannelBufferLength(
                SOCKET_SNDBUF_PARAM_NAME,
                udpChannel.socketSndbufLength(),
                channelEndpoint.socketSndbufLength(),
                udpChannel.originalUriString(),
                channelEndpoint.originalUriString());
        }

        return channelEndpoint;
    }

    private ReceiveChannelEndpoint findExistingReceiveChannelEndpoint(final UdpChannel udpChannel)
    {
        if (udpChannel.hasTag())
        {
            for (final ReceiveChannelEndpoint endpoint : receiveChannelEndpointByChannelMap.values())
            {
                if (endpoint.matchesTag(udpChannel))
                {
                    return endpoint;
                }
            }
        }

        ReceiveChannelEndpoint endpoint = receiveChannelEndpointByChannelMap.get(udpChannel.canonicalForm());
        if (null != endpoint && endpoint.hasTag() && udpChannel.hasTag() && endpoint.tag() != udpChannel.tag())
        {
            endpoint = null;
        }

        return endpoint;
    }

    private AeronClient getOrAddClient(final long clientId)
    {
        AeronClient client = findClient(clients, clientId);
        if (null == client)
        {
            final AtomicCounter counter = ClientHeartbeatTimestamp.allocate(tempBuffer, countersManager, clientId);
            final int counterId = counter.id();

            counter.setRelease(cachedEpochClock.time());
            countersManager.setCounterRegistrationId(counterId, clientId);
            countersManager.setCounterOwnerId(counterId, clientId);

            client = new AeronClient(
                clientId,
                clientLivenessTimeoutNs,
                ctx.systemCounters().get(SystemCounterDescriptor.CLIENT_TIMEOUTS),
                counter);
            clients.add(client);

            clientProxy.onCounterReady(clientId, counterId);
        }

        return client;
    }

    private IpcPublication addIpcPublication(
        final long registrationId,
        final long clientId,
        final int streamId,
        final String channel,
        final boolean isExclusive,
        final PublicationParams params)
    {
        final int termOffset = params.termOffset;

        final RawLog rawLog = newIpcPublicationLog(
            isExclusive, params.sessionId, streamId, params.initialTermId, registrationId, termOffset, params);

        UnsafeBufferPosition publisherPosition = null;
        UnsafeBufferPosition publisherLimit = null;
        try
        {
            publisherPosition = PublisherPos.allocate(
                tempBuffer,
                countersManager,
                clientId,
                registrationId,
                params.sessionId,
                streamId,
                channel,
                isExclusive);
            publisherLimit = PublisherLimit.allocate(
                tempBuffer, countersManager, clientId, registrationId, params.sessionId, streamId, channel);

            if (params.hasPosition)
            {
                final int positionBitsToShift = positionBitsToShift(params.termLength);
                final long position = computePosition(
                    params.termId, params.termOffset, positionBitsToShift, params.initialTermId);
                publisherPosition.setRelease(position);
                publisherLimit.setRelease(position);
            }

            final IpcPublication publication = new IpcPublication(
                registrationId,
                channel,
                ctx,
                params.entityTag,
                params.sessionId,
                streamId,
                publisherPosition,
                publisherLimit,
                rawLog,
                isExclusive,
                params);

            findAndUpdateResponseIpcSubscription(params, publication);

            ipcPublications.add(publication);
            activeSessionSet.add(new SessionKey(params.sessionId, streamId, IPC_MEDIA));

            return publication;
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(rawLog, publisherPosition, publisherLimit);
            throw ex;
        }
    }

    private void findAndUpdateResponseIpcSubscription(final PublicationParams params, final IpcPublication publication)
    {
        if (NULL_VALUE != params.responseCorrelationId)
        {
            for (final IpcPublication ipcPublication : ipcPublications)
            {
                if (ipcPublication.registrationId() == params.responseCorrelationId)
                {
                    for (int i = 0, n = subscriptionLinks.size(); i < n; i++)
                    {
                        final SubscriptionLink subscriptionLink = subscriptionLinks.get(i);
                        if (ipcPublication.responseCorrelationId() == subscriptionLink.registrationId &&
                            subscriptionLink instanceof IpcSubscriptionLink)
                        {
                            subscriptionLink.sessionId(publication.sessionId());
                            break;
                        }
                    }

                    break;
                }
            }
        }
    }

    private static AeronClient findClient(final ArrayList<AeronClient> clients, final long clientId)
    {
        AeronClient aeronClient = null;

        for (int i = 0, size = clients.size(); i < size; i++)
        {
            final AeronClient client = clients.get(i);
            if (client.clientId() == clientId)
            {
                aeronClient = client;
                break;
            }
        }

        return aeronClient;
    }

    private static SubscriptionLink findMdsSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId && subscription.supportsMds())
            {
                subscriptionLink = subscription;
                break;
            }
        }

        return subscriptionLink;
    }

    private static SubscriptionLink removeSubscriptionLink(
        final ArrayList<SubscriptionLink> subscriptionLinks, final long registrationId, final String channel)
    {
        SubscriptionLink subscriptionLink = null;

        for (int i = 0, size = subscriptionLinks.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriptionLinks.get(i);
            if (subscription.registrationId() == registrationId && subscription.channel().equals(channel))
            {
                subscriptionLink = subscription;
                fastUnorderedRemove(subscriptionLinks, i);
                break;
            }
        }

        return subscriptionLink;
    }

    private static IpcPublication findSharedIpcPublication(
        final ArrayList<IpcPublication> ipcPublications,
        final long streamId,
        final long responseCorrelationId)
    {
        IpcPublication ipcPublication = null;

        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            final IpcPublication publication = ipcPublications.get(i);
            if (publication.streamId() == streamId &&
                !publication.isExclusive() &&
                IpcPublication.State.ACTIVE == publication.state() &&
                publication.responseCorrelationId() == responseCorrelationId)
            {
                ipcPublication = publication;
                break;
            }
        }

        return ipcPublication;
    }

    private void checkForSessionClash(
        final int sessionId, final int streamId, final String channel, final String originalChannel)
    {
        if (activeSessionSet.contains(new SessionKey(sessionId, streamId, channel)))
        {
            throw new InvalidChannelException("existing publication has clashing sessionId=" + sessionId +
                " for streamId=" + streamId + " channel=" + originalChannel);
        }
    }

    private <T extends DriverManagedResource> void checkManagedResources(
        final ArrayList<T> list, final long nowNs, final long nowMs)
    {
        for (int lastIndex = list.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final DriverManagedResource resource = list.get(i);

            resource.onTimeEvent(nowNs, nowMs, this);

            if (resource.hasReachedEndOfLife())
            {
                CloseHelper.close(ctx.errorHandler(), resource::close);
                endOfLifeResources.add(resource);
                fastUnorderedRemove(list, i, lastIndex--);
            }
        }
    }

    private int freeEndOfLifeResources(final int freeLimit)
    {
        int workCount = 0;

        for (int i = 0; i < freeLimit; i++)
        {
            final DriverManagedResource resource = endOfLifeResources.pollFirst();
            if (null == resource)
            {
                break;
            }

            if (resource.free())
            {
                workCount++;
            }
            else
            {
                ctx.systemCounters().get(FREE_FAILS).incrementRelease();
                endOfLifeResources.addLast(resource);
            }
        }

        return workCount;
    }

    private void linkSpies(final ArrayList<SubscriptionLink> links, final NetworkPublication publication)
    {
        for (int i = 0, size = links.size(); i < size; i++)
        {
            final SubscriptionLink subscription = links.get(i);
            if (subscription.matches(publication) && !subscription.isLinked(publication))
            {
                clientProxy.onAvailableImage(
                    publication.registrationId(),
                    publication.streamId(),
                    publication.sessionId(),
                    subscription.registrationId(),
                    linkSpy(publication, subscription).id(),
                    publication.rawLog().fileName(),
                    IPC_CHANNEL);
            }
        }
    }

    private void trackTime(final long nowNs)
    {
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.measureAndUpdate(nowNs);

        if (clockUpdateDeadlineNs - nowNs < 0)
        {
            clockUpdateDeadlineNs = nowNs + CLOCK_UPDATE_INTERNAL_NS;
            cachedEpochClock.update(epochClock.time());
        }
    }

    private int processTimers(final long nowNs)
    {
        int workCount = 0;

        if (timerCheckDeadlineNs - nowNs < 0)
        {
            timerCheckDeadlineNs = nowNs + timerIntervalNs;
            heartbeatAndCheckTimers(nowNs);
            checkForBlockedToDriverCommands(nowNs);
            workCount = 1;
        }

        return workCount;
    }

    private static boolean isOldestSubscriptionSparse(final ArrayList<SubscriberPosition> subscriberPositions)
    {
        final SubscriberPosition subscriberPosition = subscriberPositions.get(0);
        long regId = subscriberPosition.subscription().registrationId();
        boolean isSparse = subscriberPosition.subscription().isSparse();

        for (int i = 1, size = subscriberPositions.size(); i < size; i++)
        {
            final SubscriptionLink subscription = subscriberPositions.get(i).subscription();
            if (subscription.registrationId() < regId)
            {
                isSparse = subscription.isSparse();
                regId = subscription.registrationId();
            }
        }

        return isSparse;
    }

    private int trackStreamPositions(final int existingWorkCount, final long nowNs)
    {
        int workCount = existingWorkCount;

        final ArrayList<PublicationImage> publicationImages = this.publicationImages;
        for (int i = 0, size = publicationImages.size(); i < size; i++)
        {
            workCount += publicationImages.get(i).trackRebuild(nowNs);
        }

        final ArrayList<NetworkPublication> networkPublications = this.networkPublications;
        for (int i = 0, size = networkPublications.size(); i < size; i++)
        {
            workCount += networkPublications.get(i).updatePublisherPositionAndLimit();
        }

        final ArrayList<IpcPublication> ipcPublications = this.ipcPublications;
        for (int i = 0, size = ipcPublications.size(); i < size; i++)
        {
            workCount += ipcPublications.get(i).updatePublisherPositionAndLimit();
        }

        return workCount;
    }

    private int drainCommandQueue()
    {
        int workCount = 0;
        for (int i = 0; i < Configuration.COMMAND_DRAIN_LIMIT; i++)
        {
            final Runnable command = driverCmdQueue.poll();
            if (null != command)
            {
                command.run();
                workCount++;
            }
            else
            {
                break;
            }
        }
        return workCount;
    }

    private static void validateChannelBufferLength(
        final String paramName,
        final int newLength,
        final int existingLength,
        final String channel,
        final String existingChannel)
    {
        if (0 != newLength && newLength != existingLength)
        {
            final Object existingValue = 0 == existingLength ? "OS default" : existingLength;
            throw new InvalidChannelException(
                paramName + "=" + newLength + " does not match existing value of " + existingValue +
                    ": existingChannel=" + existingChannel + " channel=" + channel);
        }
    }

    private static void validateEndpointForPublication(final UdpChannel udpChannel)
    {
        if (!udpChannel.isMultiDestination() && udpChannel.hasExplicitEndpoint() &&
            0 == udpChannel.remoteData().getPort())
        {
            throw new IllegalArgumentException(
                ENDPOINT_PARAM_NAME + " has port=0 for publication: channel=" + udpChannel.originalUriString());
        }
    }

    private static void validateControlForPublication(final UdpChannel udpChannel)
    {
        if (udpChannel.isDynamicControlMode() && !udpChannel.hasExplicitControl())
        {
            throw new IllegalArgumentException(
                "'control-mode=dynamic' requires that 'control' parameter is set, channel=" +
                    udpChannel.originalUriString());
        }

        if (udpChannel.hasExplicitControl() && !udpChannel.hasExplicitEndpoint() &&
            ControlMode.NONE == udpChannel.controlMode())
        {
            throw new IllegalArgumentException(
                "'control' parameter requires that either 'endpoint' or 'control-mode' is specified, channel=" +
                    udpChannel.originalUriString());
        }
    }

    private static void validateControlForSubscription(final UdpChannel udpChannel)
    {
        if (udpChannel.hasExplicitControl() &&
            0 == udpChannel.localControl().getPort())
        {
            throw new IllegalArgumentException(MDC_CONTROL_PARAM_NAME + " has port=0 for subscription: channel=" +
                udpChannel.originalUriString());
        }
    }

    private static void validateTimestampConfiguration(final UdpChannel udpChannel)
    {
        if (null != udpChannel.channelUri().get(MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME))
        {
            throw new InvalidChannelException(
                "Media timestamps '" + MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME +
                    "' are not supported in the Java driver: channel=" + udpChannel.originalUriString());
        }
    }

    private static void validateDestinationUri(final ChannelUri uri, final String destinationUri)
    {
        if (SPY_QUALIFIER.equals(uri.prefix()))
        {
            throw new InvalidChannelException("Aeron spies are invalid as send destinations: channel=" +
                destinationUri);
        }

        for (final String invalidKey : INVALID_DESTINATION_KEYS)
        {
            if (uri.containsKey(invalidKey))
            {
                throw new InvalidChannelException(
                    "destinations must not contain the key: " + invalidKey + " channel=" + destinationUri);
            }
        }

        if (Objects.equals(CONTROL_MODE_RESPONSE, uri.get(MDC_CONTROL_MODE_PARAM_NAME)))
        {
            throw new InvalidChannelException("destinations may not specify " +
                MDC_CONTROL_MODE_PARAM_NAME + "=" + CONTROL_MODE_RESPONSE);
        }
    }

    private static void validateSendDestinationUri(final ChannelUri uri, final String destinationUri)
    {
        final String endpoint = uri.get(ENDPOINT_PARAM_NAME);

        if (null != endpoint && endpoint.endsWith(":0"))
        {
            throw new InvalidChannelException(ENDPOINT_PARAM_NAME + " has port=0 for send destination: channel=" +
                destinationUri);
        }
    }

    @SuppressWarnings({ "unused", "UnnecessaryReturnStatement" })
    private static void validateExperimentalFeatures(final boolean enableExperimentalFeatures, final UdpChannel channel)
    {
        if (enableExperimentalFeatures)
        {
            return;
        }

        /*
         * Put experimental feature validation here.
         */
    }

    static FeedbackDelayGenerator resolveDelayGenerator(
        final Context ctx,
        final UdpChannel channel,
        final boolean isMulticastSemantics,
        final boolean isReliable)
    {
        if (!isReliable)
        {
            return StaticDelayGenerator.ZERO_DELAY_GENERATOR;
        }

        if (isMulticastSemantics)
        {
            return ctx.multicastFeedbackDelayGenerator();
        }

        final Long nakDelayNs = channel.nakDelayNs();
        if (null != nakDelayNs)
        {
            final long retryDelayNs =
                Math.max(nakDelayNs, Configuration.NAK_UNICAST_DELAY_MIN_VALUE_NS) * ctx.nakUnicastRetryDelayRatio();
            return new StaticDelayGenerator(nakDelayNs, retryDelayNs);
        }
        else
        {
            return ctx.unicastFeedbackDelayGenerator();
        }
    }

    static boolean isMulticastSemantics(
        final UdpChannel channel,
        final InferableBoolean receiverGroupConsideration,
        final short flags)
    {
        final boolean isGroupFromFlag = (flags & SetupFlyweight.GROUP_FLAG) == SetupFlyweight.GROUP_FLAG;

        return receiverGroupConsideration == INFER ?
            channel.isMulticast() || isGroupFromFlag :
            receiverGroupConsideration == FORCE_TRUE;
    }

    private interface AsyncResult<T> extends Supplier<T>
    {
        T get();

        static <T> AsyncResult<T> of(final Supplier<T> supplier)
        {
            try
            {
                final T value = supplier.get();
                return () -> value;
            }
            catch (final Throwable t)
            {
                return () ->
                {
                    LangUtil.rethrowUnchecked(t);
                    return null;
                };
            }
        }
    }

    private void recordError(final Exception ex)
    {
        ctx.errorHandler().onError(ex);
        errorCounter.increment();
    }
}

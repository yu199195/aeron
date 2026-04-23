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

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.LogBufferUnblocker;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import static io.aeron.driver.Configuration.PUBLICATION_HEARTBEAT_TIMEOUT_NS;
import static io.aeron.driver.Configuration.PUBLICATION_SETUP_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.HEARTBEATS_SENT;
import static io.aeron.driver.status.SystemCounterDescriptor.RETRANSMITS_SENT;
import static io.aeron.driver.status.SystemCounterDescriptor.RETRANSMITTED_BYTES;
import static io.aeron.driver.status.SystemCounterDescriptor.SENDER_FLOW_CONTROL_LIMITS;
import static io.aeron.driver.status.SystemCounterDescriptor.SHORT_SENDS;
import static io.aeron.driver.status.SystemCounterDescriptor.UNBLOCKED_PUBLICATIONS;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATIONS_REVOKED;
import static io.aeron.logbuffer.LogBufferDescriptor.activeTermCount;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.endOfStreamPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.rawTailVolatile;
import static io.aeron.logbuffer.LogBufferDescriptor.termId;
import static io.aeron.logbuffer.LogBufferDescriptor.termOffset;
import static io.aeron.logbuffer.TermScanner.available;
import static io.aeron.logbuffer.TermScanner.padding;
import static io.aeron.logbuffer.TermScanner.scanForAvailability;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_AND_END_FLAGS;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_END_AND_EOS_FLAGS;
import static io.aeron.protocol.DataHeaderFlyweight.BEGIN_END_EOS_AND_REVOKED_FLAGS;
import static io.aeron.protocol.StatusMessageFlyweight.END_OF_STREAM_FLAG;
import static org.agrona.BitUtil.SIZE_OF_LONG;

class NetworkPublicationPadding1
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class NetworkPublicationConductorFields extends NetworkPublicationPadding1
{
    static final ReadablePosition[] EMPTY_POSITIONS = new ReadablePosition[0];

    long cleanPosition;
    long timeOfLastActivityNs;
    long lastSenderPosition;
    int refCount = 0;
    ReadablePosition[] spyPositions = EMPTY_POSITIONS;
    final ArrayList<UntetheredSubscription> untetheredSubscriptions = new ArrayList<>();
}

class NetworkPublicationPadding2 extends NetworkPublicationConductorFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

class NetworkPublicationSenderFields extends NetworkPublicationPadding2
{
    long timeOfLastDataOrHeartbeatNs;
    long timeOfLastSetupNs;
    long timeOfLastStatusMessageNs;
    long timeOfLastUpdateReceivers;
    boolean trackSenderLimits = false;
    boolean isSetupElicited = false;
    boolean hasInitialConnection = false;
    byte extraPaddingByteForAlignment;
    InetSocketAddress endpointAddress;
}

class NetworkPublicationPadding3 extends NetworkPublicationSenderFields
{
    byte p128, p129, p130, p131, p132, p133, p134, p135, p136, p137, p138, p139, p140, p142, p143, p144;
    byte p145, p146, p147, p148, p149, p150, p151, p152, p153, p154, p155, p156, p157, p158, p159, p160;
    byte p161, p162, p163, p164, p165, p166, p167, p168, p169, p170, p171, p172, p173, p174, p175, p176;
    byte p177, p178, p179, p180, p181, p182, p183, p184, p185, p186, p187, p189, p190, p191, p192, p193;
}

/**
 * 【源码解析】NetworkPublication —— Driver 侧的网络发布端，是 Aeron 可靠 UDP 传输的核心枢纽。
 * <p>
 * 职责：
 * 1. 【数据发送】从 LogBuffer（Term Buffer）中读取应用线程 offer 的数据帧，通过 UDP 发送到网络
 * 2. 【流控】处理接收端的 Status Message(SM)，更新 senderLimit，驱动端到端流控
 * 3. 【NAK 重传】处理接收端的 NAK，从 Term Buffer 中重传丢失的数据帧
 * 4. 【连接管理】发送 SETUP 帧建立流、发送心跳帧维持活性
 * 5. 【拥塞控制】通过 FlowControl 策略根据 SM 中的 receiverWindowLength 限制发送速率
 * <p>
 * 线程模型：
 * - Sender 线程调用 send()、sendData()、heartbeatMessageCheck()、resend()
 * - Conductor 线程调用 updatePublisherPositionAndLimit()、onStatusMessage()、onNak()
 * - 通过 cache line padding 隔离不同线程访问的字段，避免 false sharing
 * <p>
 * 关键 Position 链（端到端流控）：
 * publisherPos → senderPosition → senderLimit ← SM(consumptionPos + window)
 *                                                     ↑ FlowControl 计算
 * publisherLimit ← Conductor 根据 senderPosition + spy 位置计算
 * <p>
 * 【Archive 回放与本类的衔接】（{@code aeron-driver} 不依赖 archive 模块，此处不引用具体回放类名）
 * Archive 等服务进程通过 {@link io.aeron.ExclusivePublication#offerBlock(org.agrona.MutableDirectBuffer, int, int)} 写入的 term，与本
 * {@code NetworkPublication} 所绑定的 {@link RawLog} 为同一映射内存；{@link Sender} 在
 * {@link Sender#doWork()} 中调用 {@link #send(long)} → {@link #sendData} → {@link #doSend} →
 * {@link SendChannelEndpoint}，最终在 {@link java.nio.channels.DatagramChannel} 上发出 UDP。
 */
public final class NetworkPublication
    extends NetworkPublicationPadding3
    implements RetransmitSender, DriverManagedResource, Subscribable
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        ACTIVE,    // 正常活跃
        DRAINING,  // 关闭中，等待数据发送完毕
        LINGER,    // 数据发完，短暂保留以处理迟到的 NAK
        DONE       // 完全关闭，可回收
    }

    private final long registrationId;
    private final long unblockTimeoutNs;
    private final long connectionTimeoutNs;
    private final long lingerTimeoutNs;
    private final long untetheredWindowLimitTimeoutNs;
    private final long untetheredLingerTimeoutNs;
    private final long untetheredRestingTimeoutNs;
    private final long tag;
    private final long responseCorrelationId;
    private final int positionBitsToShift;
    private final int initialTermId;
    private final int startingTermId;
    private final int startingTermOffset;
    private final int termBufferLength;
    private final int termLengthMask;
    private final int mtuLength;
    private final int termWindowLength;
    private final int sessionId;
    private final int streamId;
    private final boolean isExclusive;
    private final boolean signalEos;
    private final boolean isResponse;
    private final boolean spiesSimulateConnection;
    private volatile boolean hasSpies;
    private volatile boolean hasReceivers;
    private volatile boolean isConnected;
    private volatile boolean isEndOfStream;
    private volatile boolean hasSenderReleased;
    private volatile boolean hasReceivedUnicastEos;
    private State state = State.ACTIVE;

    private final FlowControl flowControl;
    private final UnsafeBuffer[] termBuffers;
    private final ByteBuffer[] sendBuffers;
    private final ErrorHandler errorHandler;
    private final Position publisherPos;
    private final Position publisherLimit;
    private final Position senderPosition;
    private final Position senderLimit;
    private final SendChannelEndpoint channelEndpoint;
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight heartbeatDataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementHeader;
    private final CachedNanoClock cachedNanoClock;
    private final RetransmitHandler retransmitHandler;
    private final UnsafeBuffer metaDataBuffer;
    private final RawLog rawLog;
    private final AtomicCounter heartbeatsSent;
    private final AtomicCounter retransmitsSent;
    private final AtomicCounter retransmittedBytes;
    private final AtomicCounter senderFlowControlLimits;
    private final AtomicCounter senderBpe;
    private final AtomicCounter senderNaksReceived;
    private final AtomicCounter shortSends;
    private final AtomicCounter unblockedPublications;
    private final AtomicCounter publicationsRevoked;
    private final ReceiverLivenessTracker livenessTracker = new ReceiverLivenessTracker();

    NetworkPublication(
        final long registrationId,
        final MediaDriver.Context ctx,
        final PublicationParams params,
        final SendChannelEndpoint channelEndpoint,
        final RawLog rawLog,
        final int termWindowLength,
        final Position publisherPos,
        final Position publisherLimit,
        final Position senderPosition,
        final Position senderLimit,
        final AtomicCounter senderBpe,
        final AtomicCounter senderNaksReceived,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final FlowControl flowControl,
        final RetransmitHandler retransmitHandler,
        final NetworkPublicationThreadLocals threadLocals,
        final boolean isExclusive)
    {
        this.registrationId = registrationId;
        this.unblockTimeoutNs = ctx.publicationUnblockTimeoutNs();
        this.connectionTimeoutNs = ctx.publicationConnectionTimeoutNs();
        this.lingerTimeoutNs = params.lingerTimeoutNs;
        this.untetheredWindowLimitTimeoutNs = params.untetheredWindowLimitTimeoutNs;
        this.untetheredLingerTimeoutNs = params.untetheredLingerTimeoutNs;
        this.untetheredRestingTimeoutNs = params.untetheredRestingTimeoutNs;
        this.tag = params.entityTag;
        this.channelEndpoint = channelEndpoint;
        this.rawLog = rawLog;
        this.cachedNanoClock = ctx.senderCachedNanoClock();
        this.senderPosition = senderPosition;
        this.senderLimit = senderLimit;
        this.senderNaksReceived = senderNaksReceived;
        this.flowControl = flowControl;
        this.retransmitHandler = retransmitHandler;
        this.publisherPos = publisherPos;
        this.publisherLimit = publisherLimit;
        this.mtuLength = params.mtuLength;
        this.initialTermId = initialTermId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.spiesSimulateConnection = params.spiesSimulateConnection;
        this.signalEos = params.signalEos;
        this.isExclusive = isExclusive;
        this.startingTermId = params.termId;
        this.startingTermOffset = params.termOffset;
        this.isResponse = params.isResponse;
        this.responseCorrelationId = params.responseCorrelationId;

        metaDataBuffer = rawLog.metaData();
        setupBuffer = threadLocals.setupBuffer();
        setupHeader = threadLocals.setupHeader();
        heartbeatBuffer = threadLocals.heartbeatBuffer();
        heartbeatDataHeader = threadLocals.heartbeatDataHeader();
        rttMeasurementBuffer = threadLocals.rttMeasurementBuffer();
        rttMeasurementHeader = threadLocals.rttMeasurementHeader();

        final SystemCounters systemCounters = ctx.systemCounters();
        heartbeatsSent = systemCounters.get(HEARTBEATS_SENT);
        shortSends = systemCounters.get(SHORT_SENDS);
        retransmitsSent = systemCounters.get(RETRANSMITS_SENT);
        retransmittedBytes = systemCounters.get(RETRANSMITTED_BYTES);
        senderFlowControlLimits = systemCounters.get(SENDER_FLOW_CONTROL_LIMITS);
        unblockedPublications = systemCounters.get(UNBLOCKED_PUBLICATIONS);
        publicationsRevoked = systemCounters.get(PUBLICATIONS_REVOKED);
        this.senderBpe = senderBpe;

        termBuffers = rawLog.termBuffers();
        for (final UnsafeBuffer termBuffer : termBuffers)
        {
            termBuffer.verifyAlignment();
        }

        sendBuffers = rawLog.sliceTerms();
        errorHandler = ctx.errorHandler();

        final int termLength = rawLog.termLength();
        termBufferLength = termLength;
        termLengthMask = termLength - 1;

        final long nowNs = cachedNanoClock.nanoTime();
        timeOfLastDataOrHeartbeatNs = nowNs - PUBLICATION_HEARTBEAT_TIMEOUT_NS - 1;
        timeOfLastSetupNs = nowNs - PUBLICATION_SETUP_TIMEOUT_NS - 1;

        positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        this.termWindowLength = termWindowLength;

        lastSenderPosition = senderPosition.get();
        cleanPosition = lastSenderPosition;
        timeOfLastActivityNs = nowNs;
    }

    /**
     * {@inheritDoc}
     */
    public boolean free()
    {
        return rawLog.free();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, publisherPos);
        CloseHelper.close(errorHandler, publisherLimit);
        CloseHelper.close(errorHandler, senderPosition);
        CloseHelper.close(errorHandler, senderLimit);
        CloseHelper.close(errorHandler, senderBpe);
        CloseHelper.close(errorHandler, senderNaksReceived);
        CloseHelper.closeAll(errorHandler, spyPositions);

        for (int i = 0, size = untetheredSubscriptions.size(); i < size; i++)
        {
            final UntetheredSubscription untetheredSubscription = untetheredSubscriptions.get(i);
            if (UntetheredSubscription.State.RESTING == untetheredSubscription.state)
            {
                CloseHelper.close(errorHandler, untetheredSubscription.position);
            }
        }

        CloseHelper.close(flowControl);
    }

    /**
     * Time of the last status message a from a receiver.
     *
     * @return this of the last status message a from a receiver.
     */
    public long timeOfLastStatusMessageNs()
    {
        return timeOfLastStatusMessageNs;
    }

    /**
     * Channel URI string for this publication.
     *
     * @return channel URI string for this publication.
     */
    public String channel()
    {
        return channelEndpoint.originalUriString();
    }

    /**
     * Session id allocated to this stream.
     *
     * @return session id allocated to this stream.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * Stream id within the channel.
     *
     * @return stream id within the channel.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Trigger the sending of a SETUP frame so a connection can be established.
     *
     * @param msg        that triggers the SETUP.
     * @param srcAddress of the source that triggers the SETUP.
     */
    public void triggerSendSetupFrame(final StatusMessageFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (!isEndOfStream)
        {
            timeOfLastStatusMessageNs = cachedNanoClock.nanoTime();
            isSetupElicited = true;
            flowControl.onTriggerSendSetup(msg, srcAddress, timeOfLastStatusMessageNs);

            if (isResponse)
            {
                this.endpointAddress = srcAddress;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public long subscribableRegistrationId()
    {
        return registrationId;
    }

    /**
     * {@inheritDoc}
     */
    public void addSubscriber(
        final SubscriptionLink subscriptionLink, final ReadablePosition position, final long nowNs)
    {
        addSpyPosition(position);

        if (!subscriptionLink.isTether())
        {
            untetheredSubscriptions.add(new UntetheredSubscription(subscriptionLink, position, nowNs));
        }

        if (spiesSimulateConnection)
        {
            updateConnectedState(true);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeSubscriber(final SubscriptionLink subscriptionLink, final ReadablePosition position)
    {
        removeSpyPosition(position);
        position.close();

        if (!subscriptionLink.isTether())
        {
            for (int lastIndex = untetheredSubscriptions.size() - 1, i = lastIndex; i >= 0; i--)
            {
                if (untetheredSubscriptions.get(i).subscriptionLink == subscriptionLink)
                {
                    ArrayListUtil.fastUnorderedRemove(untetheredSubscriptions, i, lastIndex);
                    break;
                }
            }
        }

        if (spiesSimulateConnection)
        {
            updateConnectedState(hasSubscribers());
        }
    }

    /**
     * 【NAK 处理入口】收到接收端发来的 NAK 帧时由 SendChannelEndpoint 调用。
     * <p>
     * NAK 含义："我在 termId 的 [termOffset, termOffset+length) 没收到数据，请重传。"
     * <p>
     * 处理链：onNak() → RetransmitHandler.onNak() → (延迟或立即) → this.resend()
     *
     * @param termId     丢失数据所在的 term ID
     * @param termOffset 丢失数据的起始偏移
     * @param length     丢失数据的长度
     */
    public void onNak(final int termId, final int termOffset, final int length)
    {
        senderNaksReceived.incrementRelease();
        // 委托给 RetransmitHandler：校验合法性 → 去重 → 延迟或立即调用 this.resend()
        retransmitHandler.onNak(termId, termOffset, length, termBufferLength, mtuLength, flowControl, this);
    }

    /**
     * 【Status Message 处理入口】收到接收端发来的 SM 时调用。这是流控的核心驱动入口。
     * <p>
     * SM 包含：
     * - consumptionTermId + consumptionTermOffset：接收端已消费到的位置
     * - receiverWindowLength：接收端愿意接收的窗口大小（由 CongestionControl 决定）
     * - receiverId：接收端标识
     * - flags：END_OF_STREAM_FLAG 表示接收端关闭
     * <p>
     * 处理逻辑：
     * 1. 更新接收端活性追踪（livenessTracker）
     * 2. 通过 FlowControl.onStatusMessage() 计算新的 senderLimit
     *    senderLimit = max(当前limit, consumptionPosition + receiverWindowLength)
     * 3. 更新连接状态
     *
     * @param msg            SM 帧的 flyweight
     * @param srcAddress     发送 SM 的接收端地址
     * @param conductorProxy Conductor 代理（用于通知连接状态变化）
     */
    public void onStatusMessage(
        final StatusMessageFlyweight msg,
        final InetSocketAddress srcAddress,
        final DriverConductorProxy conductorProxy)
    {
        final boolean isEos = END_OF_STREAM_FLAG == (msg.flags() & END_OF_STREAM_FLAG);
        final long timeNs = cachedNanoClock.nanoTime();

        if (isEos)
        {
            // 接收端通知流结束，从活性追踪中移除
            livenessTracker.onRemoteClose(msg.receiverId());

            if (!channelEndpoint.udpChannel().isMulticast() &&
                !channelEndpoint.udpChannel().isMultiDestination())
            {
                hasReceivedUnicastEos = true;
            }
        }
        else
        {
            // 正常 SM：更新接收端活性时间戳
            livenessTracker.onStatusMessage(msg.receiverId(), timeNs);
        }

        final boolean isLive = livenessTracker.hasReceivers();
        final boolean existingHasReceivers = hasReceivers;

        if (!existingHasReceivers && isLive)
        {
            conductorProxy.responseConnected(responseCorrelationId);
        }

        if (existingHasReceivers != isLive)
        {
            hasReceivers = isLive;
        }

        if (!hasInitialConnection)
        {
            hasInitialConnection = true; // 收到第一个 SM → 连接已建立
        }

        timeOfLastStatusMessageNs = timeNs;

        // 【核心流控更新】通过 FlowControl 策略计算新的 senderLimit
        // UnicastFlowControl: max(senderLimit, consumptionPosition + receiverWindowLength)
        senderLimit.setRelease(flowControl.onStatusMessage(
            msg,
            srcAddress,
            senderLimit.get(),
            initialTermId,
            positionBitsToShift,
            timeNs));

        updateConnectedState(hasSubscribers());
    }

    /**
     * Process an error message from a receiver.
     *
     * @param msg                       flyweight over the network packet.
     * @param srcAddress                that the setup message has come from.
     * @param destinationRegistrationId registrationId of the relevant MDC destination or {@link Aeron#NULL_VALUE}
     * @param conductorProxy            to send messages back to the conductor.
     */
    public void onError(
        final ErrorFlyweight msg,
        final InetSocketAddress srcAddress,
        final long destinationRegistrationId,
        final DriverConductorProxy conductorProxy)
    {
        flowControl.onError(msg, srcAddress, cachedNanoClock.nanoTime());
        if (livenessTracker.onRemoteClose(msg.receiverId()))
        {
            conductorProxy.onPublicationError(
                registrationId,
                destinationRegistrationId,
                msg.sessionId(),
                msg.streamId(),
                msg.receiverId(),
                msg.groupTag(),
                srcAddress,
                msg.errorCode(),
                msg.errorMessage());
        }
    }

    /**
     * Process RTT (Round Trip Timing) message from a receiver.
     *
     * @param msg        flyweight over the network packet.
     * @param srcAddress that the RTT message has come from.
     */
    public void onRttMeasurement(final RttMeasurementFlyweight msg, final InetSocketAddress srcAddress)
    {
        if (RttMeasurementFlyweight.REPLY_FLAG == (msg.flags() & RttMeasurementFlyweight.REPLY_FLAG))
        {
            rttMeasurementBuffer.clear();
            rttMeasurementHeader
                .receiverId(msg.receiverId())
                .echoTimestampNs(msg.echoTimestampNs())
                .receptionDelta(0)
                .sessionId(sessionId)
                .streamId(streamId)
                .flags((short)0x0);

            final int bytesSent = doSend(rttMeasurementBuffer);
            if (RttMeasurementFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }
        }

        // handling of RTT measurements would be done in an else clause here.
    }

    /**
     * 统一出口：把待发送的 {@link ByteBuffer}（通常为 term 的 mmap slice，或 SETUP/心跳等小缓冲区）交给
     * {@link SendChannelEndpoint}。Response 模式且已解析出对端地址时用
     * {@link SendChannelEndpoint#send(ByteBuffer, java.net.InetSocketAddress)}；否则用已与远端
     * {@code connect} 的 {@link SendChannelEndpoint#send(ByteBuffer)}，减少每包路由开销。
     */
    private int doSend(final ByteBuffer message)
    {
        if (isResponse)
        {
            if (null != endpointAddress)
            {
                return channelEndpoint.send(message, endpointAddress);
            }
            else
            {
                return 0;
            }
        }
        else
        {
            return channelEndpoint.send(message);
        }
    }

    /**
     * 【重传执行】由 RetransmitHandler 超时或立即触发，从 Term Buffer 中重传指定范围的数据帧。
     * <p>
     * 重传安全窗口校验：
     * resendPosition 必须在 [bottomResendWindow, senderPosition) 范围内，
     * 否则可能读到已被清理（zeroed）的旧数据。
     * bottomResendWindow = senderPosition - termBufferLength/2 - maxMessageLength
     * <p>
     * 重传逻辑：
     * 1. 计算 resendPosition 对应的 term index 和 offset
     * 2. 逐 MTU 扫描已完成帧（scanForAvailability），通过 UDP 发送
     * 3. 直到 remainingBytes 耗尽或遇到未完成帧
     * <p>
     * 与正常发送的区别：重传不更新 senderPosition（它只是补发旧数据）。
     *
     * @param termId     要重传的 term ID
     * @param termOffset 要重传的起始偏移
     * @param length     要重传的总长度
     */
    public void resend(final int termId, final int termOffset, final int length)
    {
        channelEndpoint.resendHook(sessionId, streamId, termId, termOffset, length);

        final long senderPosition = this.senderPosition.get();
        final long resendPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        // 安全窗口下界：senderPosition 之前的半个 term + maxMessageLength 是可重传的最远距离
        final long bottomResendWindow =
            senderPosition - (termBufferLength >> 1) - FrameDescriptor.computeMaxMessageLength(termBufferLength);

        // 安全检查：resendPosition 必须在可重传窗口内
        if (bottomResendWindow <= resendPosition && resendPosition < senderPosition)
        {
            final int activeIndex = indexByPosition(resendPosition, positionBitsToShift);
            final UnsafeBuffer termBuffer = termBuffers[activeIndex];
            final ByteBuffer sendBuffer = sendBuffers[activeIndex];

            int remainingBytes = length;
            int totalBytesSent = 0;
            int bytesSent = 0;
            int offset = termOffset;
            do
            {
                offset += bytesSent;

                // 逐 MTU 扫描已完成帧
                final long scanOutcome = scanForAvailability(termBuffer, offset, Math.min(mtuLength, remainingBytes));
                final int available = available(scanOutcome);
                if (available <= 0)
                {
                    break; // 该位置的帧尚未完成，停止重传
                }

                // 零拷贝发送：直接从 mmap ByteBuffer 的 slice 发出
                sendBuffer.limit(offset + available).position(offset);

                if (available != doSend(sendBuffer))
                {
                    shortSends.increment();
                    break;
                }

                bytesSent = available + padding(scanOutcome);
                remainingBytes -= bytesSent;
                totalBytesSent += bytesSent;
            }
            while (remainingBytes > 0);

            if (totalBytesSent > 0)
            {
                retransmitsSent.incrementRelease();
                retransmittedBytes.getAndAddRelease(totalBytesSent);
            }
        }
    }

    /**
     * 【Sender 线程调用的核心方法】每个 Publication 每次被 Sender 轮询时执行。
     * <p>
     * 完整流程：
     * 1. SETUP 检查：若尚未建立连接或被请求发 SETUP → 发送 SETUP 帧
     * 2. sendData()：从 LogBuffer 读取数据帧，通过 UDP 发送（受 senderLimit 流控限制）
     * 3. 若没发数据 → 心跳检查：发送心跳帧（DATA 帧但 length=0）维持连接活性
     * 4. FlowControl.onIdle()：空闲时更新 senderLimit（多播策略可能在此做超时清理）
     * 5. processTimeouts()：处理 RetransmitHandler 中延迟的重传动作
     *
     * @param nowNs 当前时间
     * @return 本次发送的字节数
     */
    int send(final long nowNs)
    {
        final long senderPosition = this.senderPosition.get();
        final int activeTermId = computeTermIdFromPosition(senderPosition, positionBitsToShift, initialTermId);
        final int termOffset = (int)senderPosition & termLengthMask;

        // 步骤 1：连接建立 —— 发送 SETUP 帧
        if (!hasInitialConnection || isSetupElicited)
        {
            setupMessageCheck(nowNs, activeTermId, termOffset);
        }

        // 步骤 2：发送数据
        int bytesSent = sendData(nowNs, senderPosition, termOffset);

        if (0 == bytesSent)
        {
            // 步骤 3：无数据可发 → 心跳检查
            bytesSent = heartbeatMessageCheck(nowNs, activeTermId, termOffset);

            // 步骤 4：空闲时流控维护
            if (spiesSimulateConnection && hasSpies && !hasReceivers)
            {
                // Spy 模拟连接：直接推进 senderPosition 到最快的 spy
                final long newSenderPosition = maxSpyPosition(senderPosition);
                this.senderPosition.setRelease(newSenderPosition);
                senderLimit.setRelease(flowControl.onIdle(nowNs, newSenderPosition, newSenderPosition, isEndOfStream));
            }
            else
            {
                senderLimit.setRelease(flowControl.onIdle(nowNs, senderLimit.get(), senderPosition, isEndOfStream));
            }

            updateHasReceivers(nowNs);
        }

        // 步骤 5：处理重传超时（DELAYED → resend、LINGERING → 释放槽位）
        retransmitHandler.processTimeouts(nowNs, this);

        return bytesSent;
    }

    SendChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    RawLog rawLog()
    {
        return rawLog;
    }

    int publisherLimitId()
    {
        return publisherLimit.id();
    }

    long tag()
    {
        return tag;
    }

    int termBufferLength()
    {
        return termBufferLength;
    }

    int mtuLength()
    {
        return mtuLength;
    }

    long registrationId()
    {
        return registrationId;
    }

    boolean isExclusive()
    {
        return isExclusive;
    }

    boolean spiesSimulateConnection()
    {
        return spiesSimulateConnection;
    }

    int initialTermId()
    {
        return initialTermId;
    }

    int startingTermId()
    {
        return startingTermId;
    }

    int startingTermOffset()
    {
        return startingTermOffset;
    }

    boolean isAcceptingSubscriptions()
    {
        return State.ACTIVE == state ||
            (State.DRAINING == state && hasSpies && producerPosition() > senderPosition.getVolatile());
    }

    /**
     * 【Conductor 线程调用】更新 publisherPos 和 publisherLimit，实现应用端背压控制。
     * <p>
     * publisherLimit 决定应用线程 offer() 能写到多远：
     * publisherLimit = min(所有消费者位置) + termWindowLength
     * <p>
     * "所有消费者" 包括 senderPosition（远端接收端进度）和所有 spy 的位置。
     * 这保证了写入不会超过最慢的消费者一个 termWindowLength（通常 = termBufferLength/2），
     * 避免覆盖尚未被消费的数据。
     * <p>
     * 此外还负责 cleanBuffer：清零已被所有消费者消费过的 term 区域（为下次 term 轮转做准备）。
     *
     * @return 1 如果 limit 发生变化，否则 0
     */
    int updatePublisherPositionAndLimit()
    {
        int workCount = 0;

        if (State.ACTIVE == state)
        {
            final long producerPosition = producerPosition();
            final long senderPosition = this.senderPosition.getVolatile();

            publisherPos.setRelease(producerPosition);

            if (hasSubscribers())
            {
                // 找到最慢的消费者（senderPosition 或最慢的 spy）
                long minConsumerPosition = senderPosition;
                for (final ReadablePosition spyPosition : spyPositions)
                {
                    minConsumerPosition = Math.min(minConsumerPosition, spyPosition.getVolatile());
                }

                // publisherLimit = 最慢消费者 + termWindowLength
                final long newLimitPosition = minConsumerPosition + termWindowLength;
                if (newLimitPosition > publisherLimit.get())
                {
                    // 清理已消费的 term 区域（防止脏数据影响下次 term 轮转）
                    cleanBufferTo(minConsumerPosition - termBufferLength);
                    final long cleanPosition = this.cleanPosition;
                    final int dirtyTermId =
                        computeTermIdFromPosition(cleanPosition, positionBitsToShift, initialTermId);
                    final int activeTermId =
                        computeTermIdFromPosition(newLimitPosition, positionBitsToShift, initialTermId);
                    // 防止 limit 跑到未清理的 term 上（term gap < 2 才安全）
                    final int termGap = activeTermId - dirtyTermId;
                    if (termGap < 2 || (2 == termGap && 0 != (int)(cleanPosition & termLengthMask)))
                    {
                        publisherLimit.setRelease(newLimitPosition);
                    }
                    workCount = 1;
                }
            }
            else if (publisherLimit.get() > senderPosition)
            {
                // 无订阅者：收回 limit 到 senderPosition（停止写入）
                updateConnectedState(false);
                publisherLimit.setRelease(senderPosition);
                cleanBufferTo(senderPosition - termBufferLength);
                workCount = 1;
            }
        }

        return workCount;
    }

    boolean hasSpies()
    {
        return hasSpies;
    }

    void updateHasReceivers(final long timeNs)
    {
        livenessTracker.onIdle(timeNs, connectionTimeoutNs);
        final boolean isLive = livenessTracker.hasReceivers();

        if (hasReceivers != isLive)
        {
            hasReceivers = isLive;
        }

        timeOfLastUpdateReceivers = timeNs;
    }

    /**
     * 【数据发送核心】从 Term Buffer 读取已完成的帧，通过 UDP 发送到网络。
     * <p>
     * 流控检查：availableWindow = senderLimit - senderPosition
     * - > 0：有窗口余量，可发送（但不超过 MTU）
     * - ≤ 0：流控限制，不发送（senderFlowControlLimits++）
     * <p>
     * 扫描 + 发送：
     * 1. scanForAvailability(termBuffer, termOffset, scanLimit) → 找出连续已完成帧
     * 2. 零拷贝发送：sendBuffer.limit/position → channelEndpoint.send(sendBuffer) → DatagramChannel.write()
     * 3. 成功后更新 senderPosition
     *
     * @param nowNs          当前时间
     * @param senderPosition 当前发送位置（绝对 position）
     * @param termOffset     发送位置在 term 内的偏移
     * @return 发送字节数
     */
    private int sendData(final long nowNs, final long senderPosition, final int termOffset)
    {
        int bytesSent = 0;
        // 流控检查：senderLimit 由 SM → FlowControl.onStatusMessage() 更新
        final int availableWindow = (int)(senderLimit.get() - senderPosition);
        if (availableWindow > 0)
        {
            // 本次最多扫描/发送 min(可用窗口, MTU) 字节
            final int scanLimit = Math.min(availableWindow, mtuLength);
            final int activeIndex = indexByPosition(senderPosition, positionBitsToShift);

            // 扫描 Term Buffer 中已完成的帧
            final long scanOutcome = scanForAvailability(termBuffers[activeIndex], termOffset, scanLimit);
            final int available = available(scanOutcome);
            if (available > 0)
            {
                // 零拷贝发送：直接从 mmap 的 ByteBuffer slice 发出
                final ByteBuffer sendBuffer = sendBuffers[activeIndex];
                sendBuffer.limit(termOffset + available).position(termOffset);

                if (available == doSend(sendBuffer))
                {
                    timeOfLastDataOrHeartbeatNs = nowNs;
                    trackSenderLimits = true;

                    // 推进 senderPosition（包括 padding 部分）
                    bytesSent = available + padding(scanOutcome);
                    this.senderPosition.setRelease(senderPosition + bytesSent);
                }
                else
                {
                    shortSends.increment(); // 操作系统发送缓冲区满，部分发送
                }
            }
            else if (available < 0)
            {
                // available < 0：有数据但单帧超过 scanLimit → 流控限制
                if (trackSenderLimits)
                {
                    trackSenderLimits = false;
                    senderBpe.incrementRelease();
                    senderFlowControlLimits.incrementRelease();
                }
            }
        }
        else if (trackSenderLimits)
        {
            // 流控窗口耗尽：senderPosition 已追上 senderLimit
            trackSenderLimits = false;
            senderBpe.incrementRelease();
            senderFlowControlLimits.incrementRelease();
        }

        return bytesSent;
    }

    /**
     * 【SETUP 帧发送检查】周期性发送 SETUP 帧以建立或恢复与接收端的连接。
     * <p>
     * SETUP 帧告诉接收端：
     * - sessionId / streamId：流标识
     * - initialTermId / termLength / mtuLength：流参数（接收端据此初始化 LogBuffer）
     * - activeTermId / termOffset：当前发送位置（接收端据此设置初始接收位置）
     * <p>
     * 发送时机：
     * - hasInitialConnection == false：首次建连，周期性发 SETUP 直到收到 SM
     * - isSetupElicited == true：接收端通过 SM 请求重发 SETUP（接收端可能刚启动/重建）
     */
    private void setupMessageCheck(final long nowNs, final int activeTermId, final int termOffset)
    {
        if ((timeOfLastSetupNs + PUBLICATION_SETUP_TIMEOUT_NS) - nowNs < 0)
        {
            timeOfLastSetupNs = nowNs;

            final int flags =
                (isSendResponseSetupFlag() ? SetupFlyweight.SEND_RESPONSE_SETUP_FLAG : 0) |
                (hasGroupSemantics() ? SetupFlyweight.GROUP_FLAG : 0);

            setupBuffer.clear();
            setupHeader
                .activeTermId(activeTermId)
                .termOffset(termOffset)
                .sessionId(sessionId)
                .streamId(streamId)
                .initialTermId(initialTermId)
                .termLength(termBufferLength)
                .mtuLength(mtuLength)
                .ttl(channelEndpoint.multicastTtl())
                .flags((short)(flags & 0xFFFF));

            if (isSetupElicited)
            {
                flowControl.onSetup(setupHeader, senderLimit.get(), senderPosition.get(), positionBitsToShift, nowNs);
            }

            if (SetupFlyweight.HEADER_LENGTH != doSend(setupBuffer))
            {
                shortSends.increment();
            }

            if (isSetupElicited && hasReceivers)
            {
                isSetupElicited = false; // 已有接收端响应，停止周期发 SETUP
            }
        }
    }

    /**
     * 【心跳帧发送检查】在没有数据可发时，周期性发送心跳帧维持连接活性。
     * <p>
     * 心跳帧 = DATA 帧但 frameLength == HEADER_LENGTH（32字节，无负载）。
     * 接收端根据心跳更新最后收到数据的时间戳，避免超时断连。
     * <p>
     * 特殊 flags：
     * - BEGIN_AND_END_FLAGS：正常心跳
     * - BEGIN_END_AND_EOS_FLAGS：流结束的心跳
     * - BEGIN_END_EOS_AND_REVOKED_FLAGS：Publication 被撤销的心跳
     */
    private int heartbeatMessageCheck(
        final long nowNs, final int activeTermId, final int termOffset)
    {
        int bytesSent = 0;

        if (hasInitialConnection && (timeOfLastDataOrHeartbeatNs + PUBLICATION_HEARTBEAT_TIMEOUT_NS) - nowNs < 0)
        {
            final short flags;

            if (LogBufferDescriptor.isPublicationRevoked(metaDataBuffer))
            {
                flags = BEGIN_END_EOS_AND_REVOKED_FLAGS;
            }
            else if (signalEos && isEndOfStream)
            {
                flags = BEGIN_END_AND_EOS_FLAGS;
            }
            else
            {
                flags = BEGIN_AND_END_FLAGS;
            }

            heartbeatBuffer.clear();
            heartbeatDataHeader
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(activeTermId)
                .termOffset(termOffset)
                .flags(flags);

            bytesSent = doSend(heartbeatBuffer); // 发送心跳（DATA 帧，长度 = 32B）
            if (DataHeaderFlyweight.HEADER_LENGTH != bytesSent)
            {
                shortSends.increment();
            }

            timeOfLastDataOrHeartbeatNs = nowNs;
            heartbeatsSent.incrementRelease();
        }

        return bytesSent;
    }

    private void cleanBufferTo(final long position)
    {
        final long cleanPosition = this.cleanPosition;
        if (position > cleanPosition)
        {
            final UnsafeBuffer dirtyTermBuffer = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
            final int bytesForCleaning = (int)(position - cleanPosition);
            final int termOffset = (int)cleanPosition & termLengthMask;
            final int length = Math.min(bytesForCleaning, termBufferLength - termOffset);

            dirtyTermBuffer.setMemory(termOffset + SIZE_OF_LONG, length - SIZE_OF_LONG, (byte)0);
            dirtyTermBuffer.putLongRelease(termOffset, 0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    private void checkForBlockedPublisher(final long producerPosition, final long senderPosition, final long nowNs)
    {
        if (senderPosition == lastSenderPosition && isPossiblyBlocked(producerPosition, senderPosition))
        {
            if ((timeOfLastActivityNs + unblockTimeoutNs) - nowNs < 0)
            {
                if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                {
                    unblockedPublications.incrementRelease();
                }
            }
        }
        else
        {
            timeOfLastActivityNs = nowNs;
            lastSenderPosition = senderPosition;
        }
    }

    private boolean isPossiblyBlocked(final long producerPosition, final long consumerPosition)
    {
        final int producerTermCount = activeTermCount(metaDataBuffer);
        final int expectedTermCount = (int)(consumerPosition >> positionBitsToShift);

        if (producerTermCount != expectedTermCount)
        {
            return true;
        }

        return producerPosition > consumerPosition;
    }

    private boolean spiesFinishedConsuming(final DriverConductor conductor, final long eosPosition)
    {
        if (hasSpies)
        {
            for (final ReadablePosition spyPosition : spyPositions)
            {
                if (spyPosition.getVolatile() < eosPosition)
                {
                    return false;
                }
            }

            hasSpies = false;
            conductor.cleanupSpies(this);
        }

        return true;
    }

    private long maxSpyPosition(final long senderPosition)
    {
        long position = senderPosition;

        for (final ReadablePosition spyPosition : spyPositions)
        {
            position = Math.max(position, spyPosition.getVolatile());
        }

        return position;
    }

    private void updateConnectedState(final boolean newConnectedState)
    {
        if (newConnectedState != isConnected)
        {
            LogBufferDescriptor.isConnected(metaDataBuffer, newConnectedState);
            isConnected = newConnectedState;
        }
    }

    private boolean hasSubscribers()
    {
        return (spiesSimulateConnection && hasSpies) || (hasReceivers && flowControl.hasRequiredReceivers());
    }

    private void checkUntetheredSubscriptions(final long nowNs, final DriverConductor conductor)
    {
        final ArrayList<UntetheredSubscription> untetheredSubscriptions = this.untetheredSubscriptions;
        final int untetheredSubscriptionsSize = untetheredSubscriptions.size();
        if (untetheredSubscriptionsSize > 0)
        {
            final long senderPosition = this.senderPosition.getVolatile();
            final long untetheredWindowLimit = (senderPosition - termWindowLength) + (termWindowLength >> 2);

            for (int lastIndex = untetheredSubscriptionsSize - 1, i = lastIndex; i >= 0; i--)
            {
                final UntetheredSubscription untethered = untetheredSubscriptions.get(i);
                if (UntetheredSubscription.State.ACTIVE == untethered.state)
                {
                    if (untethered.position.getVolatile() > untetheredWindowLimit)
                    {
                        untethered.timeOfLastUpdateNs = nowNs;
                    }
                    else if ((untethered.timeOfLastUpdateNs + untetheredWindowLimitTimeoutNs) - nowNs <= 0)
                    {
                        conductor.notifyUnavailableImageLink(registrationId, untethered.subscriptionLink);
                        untethered.state(UntetheredSubscription.State.LINGER, nowNs, streamId, sessionId);
                    }
                }
                else if (UntetheredSubscription.State.LINGER == untethered.state)
                {
                    if ((untethered.timeOfLastUpdateNs + untetheredLingerTimeoutNs) - nowNs <= 0)
                    {
                        removeSpyPosition(untethered.position);
                        if (untethered.subscriptionLink.isRejoin())
                        {
                            untethered.state(UntetheredSubscription.State.RESTING, nowNs, streamId, sessionId);
                        }
                        else
                        {
                            ArrayListUtil.fastUnorderedRemove(untetheredSubscriptions, i, lastIndex--);
                            untethered.position.close();
                        }
                    }
                }
                else if (UntetheredSubscription.State.RESTING == untethered.state)
                {
                    if ((untethered.timeOfLastUpdateNs + untetheredRestingTimeoutNs) - nowNs <= 0)
                    {
                        addSpyPosition(untethered.position);
                        conductor.notifyAvailableImageLink(
                            registrationId,
                            sessionId,
                            untethered.subscriptionLink,
                            untethered.position.id(),
                            senderPosition,
                            rawLog.fileName(),
                            CommonContext.IPC_CHANNEL);
                        untethered.state(UntetheredSubscription.State.ACTIVE, nowNs, streamId, sessionId);
                    }
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        switch (state)
        {
            case ACTIVE:
            {
                if (LogBufferDescriptor.isPublicationRevoked(metaDataBuffer))
                {
                    final long revokedPos = producerPosition();
                    publisherLimit.setRelease(revokedPos);
                    endOfStreamPosition(metaDataBuffer, revokedPos);
                    updateConnectedState(false);
                    isConnected = false;

                    isEndOfStream = true;

                    conductor.cleanupSpies(this);

                    state = State.LINGER;

                    logRevoke(revokedPos, sessionId(), streamId(), channel());
                    publicationsRevoked.increment();
                }
                else
                {
                    checkUntetheredSubscriptions(timeNs, conductor);
                    updateConnectedState(hasSubscribers());
                    final long producerPosition = producerPosition();
                    publisherPos.setRelease(producerPosition);
                    if (!isExclusive)
                    {
                        checkForBlockedPublisher(producerPosition, senderPosition.getVolatile(), timeNs);
                    }
                }
                break;
            }

            case DRAINING:
            {
                final long producerPosition = producerPosition();
                publisherPos.setRelease(producerPosition);
                final long senderPosition = this.senderPosition.getVolatile();
                if (producerPosition > senderPosition)
                {
                    if (LogBufferUnblocker.unblock(termBuffers, metaDataBuffer, senderPosition, termBufferLength))
                    {
                        unblockedPublications.incrementRelease();
                        break;
                    }

                    if (hasReceivers)
                    {
                        break;
                    }
                }
                else
                {
                    isEndOfStream = true;
                }

                if (spiesFinishedConsuming(conductor, producerPosition))
                {
                    timeOfLastActivityNs = timeNs;
                    state = State.LINGER;
                }
                break;
            }

            case LINGER:
                if (0 == refCount &&
                    (hasReceivedUnicastEos || (timeOfLastActivityNs + lingerTimeoutNs) - timeNs < 0))
                {
                    channelEndpoint.decRef();
                    conductor.cleanupPublication(this);
                    timeOfLastActivityNs = timeNs;
                    state = State.DONE;
                }
                break;

            case DONE:
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return hasSenderReleased;
    }

    /**
     * Get the response correlation id for the publication.
     *
     * @return the response correlation id for the publication.
     */
    public long responseCorrelationId()
    {
        return responseCorrelationId;
    }

    void revoke()
    {
        LogBufferDescriptor.isPublicationRevoked(metaDataBuffer, true);
    }

    void decRef()
    {
        if (0 == --refCount)
        {
            final long producerPosition = producerPosition();
            publisherLimit.setRelease(producerPosition);
            endOfStreamPosition(metaDataBuffer, producerPosition);

            if (!LogBufferDescriptor.isPublicationRevoked(metaDataBuffer))
            {
                if (senderPosition.getVolatile() >= producerPosition)
                {
                    isEndOfStream = true;
                }

                state = State.DRAINING;
            }
        }
    }

    void incRef()
    {
        ++refCount;
    }

    State state()
    {
        return state;
    }

    void senderRelease()
    {
        hasSenderReleased = true;
    }

    long producerPosition()
    {
        final long rawTail = rawTailVolatile(metaDataBuffer);
        final int termOffset = termOffset(rawTail, termBufferLength);

        return computePosition(termId(rawTail), termOffset, positionBitsToShift, initialTermId);
    }

    long consumerPosition()
    {
        return senderPosition.getVolatile();
    }

    private void addSpyPosition(final ReadablePosition position)
    {
        spyPositions = ArrayUtil.add(spyPositions, position);
        hasSpies = true;
    }

    private void removeSpyPosition(final ReadablePosition position)
    {
        spyPositions = ArrayUtil.remove(spyPositions, position);
        hasSpies = 0 != spyPositions.length;
    }

    private boolean isSendResponseSetupFlag()
    {
        return !isResponse && Aeron.NULL_VALUE != responseCorrelationId;
    }

    private boolean hasGroupSemantics()
    {
        return channelEndpoint().udpChannel().hasGroupSemantics();
    }

    private static void logRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
    }
}

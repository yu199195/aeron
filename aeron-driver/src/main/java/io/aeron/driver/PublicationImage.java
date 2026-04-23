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

import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.ImageConnection;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.logbuffer.TermRebuilder;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.Position;
import org.agrona.concurrent.status.ReadablePosition;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;

import static io.aeron.ErrorCode.IMAGE_REJECTED;
import static io.aeron.driver.LossDetector.lossFound;
import static io.aeron.driver.LossDetector.rebuildOffset;
import static io.aeron.driver.status.SystemCounterDescriptor.FLOW_CONTROL_OVER_RUNS;
import static io.aeron.driver.status.SystemCounterDescriptor.FLOW_CONTROL_UNDER_RUNS;
import static io.aeron.driver.status.SystemCounterDescriptor.HEARTBEATS_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.LOSS_GAP_FILLS;
import static io.aeron.driver.status.SystemCounterDescriptor.NAK_MESSAGES_SENT;
import static io.aeron.driver.status.SystemCounterDescriptor.PUBLICATION_IMAGES_REVOKED;
import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_SENT;
import static io.aeron.logbuffer.LogBufferDescriptor.LOG_ACTIVE_TRANSPORT_COUNT;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByPosition;
import static io.aeron.logbuffer.LogBufferDescriptor.indexByTerm;
import static io.aeron.logbuffer.LogBufferDescriptor.isPublicationRevoked;
import static io.aeron.logbuffer.TermGapFiller.tryFillGap;
import static io.aeron.protocol.SetupFlyweight.SEND_RESPONSE_SETUP_FLAG;
import static org.agrona.BitUtil.SIZE_OF_LONG;

class PublicationImagePadding1
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class PublicationImageConductorFields extends PublicationImagePadding1
{
    long cleanPosition;
    final ArrayList<UntetheredSubscription> untetheredSubscriptions = new ArrayList<>();
    ReadablePosition[] subscriberPositions;
    int lossReportTermId;
    int lossReportTermOffset;
    int lossReportLength;
    LossReport lossReport;
    LossReport.ReportEntry reportEntry;
    volatile Integer responseSessionId = null;
}

class PublicationImagePadding2 extends PublicationImageConductorFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

class PublicationImageReceiverFields extends PublicationImagePadding2
{
    boolean isEndOfStream = false;
    boolean isSendingEosSm = false;
    long timeOfLastPacketNs;
    ImageConnection[] imageConnections = new ImageConnection[1];
    String rejectionReason = null;
}

class PublicationImagePadding3 extends PublicationImageReceiverFields
{
    byte p128, p129, p130, p131, p132, p133, p134, p135, p136, p137, p138, p139, p140, p142, p143, p144;
    byte p145, p146, p147, p148, p149, p150, p151, p152, p153, p154, p155, p156, p157, p158, p159, p160;
    byte p161, p162, p163, p164, p165, p166, p167, p168, p169, p170, p171, p172, p173, p174, p175, p176;
    byte p177, p178, p179, p180, p181, p182, p183, p184, p185, p186, p187, p189, p190, p191, p192, p193;
}

/**
 * 【源码解析】PublicationImage —— 接收端的"流镜像"，Aeron 可靠接收的核心组件。
 * <p>
 * 一个 PublicationImage 对应一个远端 NetworkPublication 的本地接收副本。
 * 它维护接收端的 Term Buffer（与发送端结构相同），并驱动以下可靠性机制：
 * <p>
 * 1. 【数据插入】Receiver 线程收到 UDP 数据帧 → insertPacket() → TermRebuilder.insert()
 *    将数据写入本地 Term Buffer，推进 hwmPosition（高水位线）
 * <p>
 * 2. 【丢包检测与 NAK】Conductor 线程调用 trackRebuild()：
 *    - LossDetector.scan() 扫描 rebuildPosition 到 hwmPosition 之间的空洞
 *    - 空洞超时 → onGapDetected() → 将 NAK 信息写入 loss* 字段
 *    - Receiver 线程 processPendingLoss() → 通过 channelEndpoint.sendNakMessage() 发送 NAK
 * <p>
 * 3. 【Status Message 发送】Conductor 线程 trackRebuild() 中根据拥塞控制计算 receiverWindowLength，
 *    调用 scheduleStatusMessage() → Receiver 线程 sendPendingStatusMessage() → 发送 SM
 *    SM 中的 consumptionPosition + receiverWindowLength 反馈给发送端，驱动端到端流控
 * <p>
 * 4. 【拥塞控制】CongestionControl.onTrackRebuild() 根据当前接收状态动态调整 receiverWindowLength
 * <p>
 * 5. 【RTT 测量】CongestionControl.shouldMeasureRtt() → 发送 RTTM 帧 → 收到回复后计算 RTT
 * <p>
 * 线程模型（跨线程协调使用 VarHandle 实现无锁通信）：
 * - Conductor 线程：trackRebuild()（丢包检测 + 拥塞控制 + 调度 SM/NAK）
 * - Receiver 线程：insertPacket()（插入数据）、sendPendingStatusMessage()、processPendingLoss()
 * <p>
 * 关键 Position：
 * - hwmPosition：收到的最高数据位置（可能有空洞）
 * - rebuildPosition：已连续重建（无空洞）到的位置
 * - subscriberPositions[]：各订阅者已消费到的位置
 */
public final class PublicationImage
    extends PublicationImagePadding3
    implements LossHandler, DriverManagedResource, Subscribable
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT,      // 初始化，等待接收数据
        ACTIVE,    // 正常接收中
        DRAINING,  // 流结束，等待所有数据被消费
        LINGER,    // 数据消费完毕，短暂保留
        DONE       // 完全关闭，可回收
    }

    // expected minimum number of SMs with EOS bit set sent during draining.
    private static final long SM_EOS_MULTIPLE = 5;

    private static final VarHandle BEGIN_SM_CHANGE_VH;
    private static final VarHandle END_SM_CHANGE_VH;
    private static final VarHandle BEGIN_LOSS_CHANGE_VH;
    private static final VarHandle END_LOSS_CHANGE_VH;

    static
    {
        try
        {
            final MethodHandles.Lookup lookup = MethodHandles.lookup();
            BEGIN_SM_CHANGE_VH = lookup
                .findVarHandle(PublicationImage.class, "beginSmChange", long.class);
            END_SM_CHANGE_VH = lookup
                .findVarHandle(PublicationImage.class, "endSmChange", long.class);
            BEGIN_LOSS_CHANGE_VH = lookup
                .findVarHandle(PublicationImage.class, "beginLossChange", long.class);
            END_LOSS_CHANGE_VH = lookup
                .findVarHandle(PublicationImage.class, "endLossChange", long.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private volatile long beginSmChange;
    private volatile long endSmChange;
    private long nextSmPosition;
    private int nextSmReceiverWindowLength;

    private long lastSmChangeNumber;
    private long lastSmPosition;
    private long lastOverrunThreshold;
    private long nextSmDeadlineNs;
    private final long smTimeoutNs;
    private final long maxReceiverWindowLength;

    volatile long beginLossChange;
    volatile long endLossChange;
    int lossTermId;
    int lossTermOffset;
    int lossLength;
    private long lastLossChangeNumber;

    private volatile long timeOfLastStateChangeNs;

    private final long correlationId;
    private final long imageLivenessTimeoutNs;
    private final long untetheredWindowLimitTimeoutNs;
    private final long untetheredLingerTimeoutNs;
    private final long untetheredRestingTimeoutNs;
    private final int sessionId;
    private final int streamId;
    private final int positionBitsToShift;
    private final int termLengthMask;
    private final int initialTermId;
    private final short flags;
    private final boolean isReliable;
    private boolean smEnabled = true;

    private boolean isRebuilding = true;
    private volatile boolean isReceiverReleaseTriggered = false;
    private volatile boolean hasReceiverReleased = false;
    private volatile State state = State.INIT;

    private final CachedNanoClock cachedNanoClock;
    private final ReceiveChannelEndpoint channelEndpoint;
    private final UnsafeBuffer[] termBuffers;
    private final Position hwmPosition;
    private final LossDetector lossDetector;
    private final CongestionControl congestionControl;
    private final ErrorHandler errorHandler;
    private final Position rebuildPosition;
    private final String sourceIdentity;
    private final AtomicCounter heartbeatsReceived;
    private final AtomicCounter statusMessagesSent;
    private final AtomicCounter nakMessagesSent;
    private final AtomicCounter receiverNaksSent;
    private final AtomicCounter flowControlUnderRuns;
    private final AtomicCounter flowControlOverRuns;
    private final AtomicCounter lossGapFills;
    private final AtomicCounter publicationImagesRevoked;
    private final EpochClock epochClock;
    private final NanoClock nanoClock;
    private final RawLog rawLog;

    PublicationImage(
        final long correlationId,
        final MediaDriver.Context ctx,
        final ReceiveChannelEndpoint channelEndpoint,
        final int transportIndex,
        final InetSocketAddress controlAddress,
        final int sessionId,
        final int streamId,
        final int initialTermId,
        final int activeTermId,
        final int termOffset,
        final short flags,
        final boolean isReliable,
        final long untetheredWindowLimitTimeoutNs,
        final long untetheredLingerTimeoutNs,
        final long untetheredRestingTimeoutNs,
        final RawLog rawLog,
        final FeedbackDelayGenerator lossFeedbackDelayGenerator,
        final ArrayList<SubscriberPosition> subscriberPositions,
        final Position hwmPosition,
        final Position rebuildPosition,
        final AtomicCounter receiverNaksSent,
        final String sourceIdentity,
        final CongestionControl congestionControl)
    {
        this.correlationId = correlationId;
        this.imageLivenessTimeoutNs = ctx.imageLivenessTimeoutNs();
        this.receiverNaksSent = receiverNaksSent;
        this.untetheredWindowLimitTimeoutNs = untetheredWindowLimitTimeoutNs;
        this.untetheredLingerTimeoutNs = untetheredLingerTimeoutNs;
        this.untetheredRestingTimeoutNs = untetheredRestingTimeoutNs;
        this.smTimeoutNs = ctx.statusMessageTimeoutNs();
        this.channelEndpoint = channelEndpoint;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.flags = flags;
        this.rawLog = rawLog;
        this.hwmPosition = hwmPosition;
        this.rebuildPosition = rebuildPosition;
        this.sourceIdentity = sourceIdentity;
        this.initialTermId = initialTermId;
        this.congestionControl = congestionControl;
        this.errorHandler = ctx.errorHandler();
        this.lossReport = ctx.lossReport();

        this.nanoClock = ctx.nanoClock();
        this.epochClock = ctx.epochClock();
        this.cachedNanoClock = ctx.receiverCachedNanoClock();

        final long nowNs = cachedNanoClock.nanoTime();
        this.timeOfLastStateChangeNs = nowNs;
        this.timeOfLastPacketNs = nowNs;

        this.subscriberPositions = positionArray(subscriberPositions, nowNs);
        this.isReliable = isReliable;

        final SystemCounters systemCounters = ctx.systemCounters();
        heartbeatsReceived = systemCounters.get(HEARTBEATS_RECEIVED);
        statusMessagesSent = systemCounters.get(STATUS_MESSAGES_SENT);
        nakMessagesSent = systemCounters.get(NAK_MESSAGES_SENT);
        flowControlUnderRuns = systemCounters.get(FLOW_CONTROL_UNDER_RUNS);
        flowControlOverRuns = systemCounters.get(FLOW_CONTROL_OVER_RUNS);
        lossGapFills = systemCounters.get(LOSS_GAP_FILLS);
        publicationImagesRevoked = systemCounters.get(PUBLICATION_IMAGES_REVOKED);

        imageConnections = ArrayUtil.ensureCapacity(imageConnections, transportIndex + 1);
        imageConnections[transportIndex] = new ImageConnection(nowNs, controlAddress);

        termBuffers = rawLog.termBuffers();
        lossDetector = new LossDetector(lossFeedbackDelayGenerator, this);

        final int termLength = rawLog.termLength();
        termLengthMask = termLength - 1;
        positionBitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);

        nextSmReceiverWindowLength = congestionControl.initialWindowLength();
        maxReceiverWindowLength = congestionControl.maxWindowLength();
        final long position = computePosition(activeTermId, termOffset, positionBitsToShift, initialTermId);
        nextSmPosition = position;
        lastSmPosition = position;
        lastOverrunThreshold = position + (termLength >> 1);
        cleanPosition = position;

        hwmPosition.setRelease(position);
        rebuildPosition.setRelease(position);
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
        CloseHelper.close(errorHandler, hwmPosition);
        CloseHelper.close(errorHandler, rebuildPosition);
        CloseHelper.close(errorHandler, receiverNaksSent);
        CloseHelper.closeAll(errorHandler, subscriberPositions);

        for (int i = 0, size = untetheredSubscriptions.size(); i < size; i++)
        {
            final UntetheredSubscription untetheredSubscription = untetheredSubscriptions.get(i);
            if (UntetheredSubscription.State.RESTING == untetheredSubscription.state)
            {
                CloseHelper.close(errorHandler, untetheredSubscription.position);
            }
        }

        CloseHelper.close(errorHandler, congestionControl);
    }

    /**
     * The correlation id assigned by the driver when created.
     *
     * @return the correlation id assigned by the driver when created.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * The session id of the channel from a publisher.
     *
     * @return session id of the channel from a publisher.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * The stream id of this image within a channel.
     *
     * @return stream id of this image within a channel.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Get the string representation of the channel URI.
     *
     * @return the string representation of the channel URI.
     */
    public String channel()
    {
        return channelEndpoint.originalUriString();
    }

    /**
     * {@inheritDoc}
     */
    public long subscribableRegistrationId()
    {
        return correlationId;
    }

    /**
     * {@inheritDoc}
     */
    public void addSubscriber(
        final SubscriptionLink subscriptionLink, final ReadablePosition subscriberPosition, final long nowNs)
    {
        subscriberPositions = ArrayUtil.add(subscriberPositions, subscriberPosition);
        if (!subscriptionLink.isTether())
        {
            untetheredSubscriptions.add(new UntetheredSubscription(subscriptionLink, subscriberPosition, nowNs));
        }
    }

    /**
     * {@inheritDoc}
     */
    public void removeSubscriber(final SubscriptionLink subscriptionLink, final ReadablePosition subscriberPosition)
    {
        subscriberPositions = ArrayUtil.remove(subscriberPositions, subscriberPosition);
        subscriberPosition.close();

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

        if (0 == subscriberPositions.length)
        {
            isRebuilding = false;
        }
    }

    /**
     * 【LossHandler 回调】Conductor 线程中 LossDetector 检测到空洞超时后调用。
     * <p>
     * 将丢包信息写入 loss* 字段，供 Receiver 线程 processPendingLoss() 读取并发送 NAK。
     * <p>
     * 跨线程无锁协议（类似 SeqLock）：
     * 1. beginLossChange++ (release)
     * 2. storeStoreFence()
     * 3. 写入 lossTermId / lossTermOffset / lossLength
     * 4. endLossChange++ (release)
     * <p>
     * Receiver 线程读取时：先读 endLossChange → 读数据 → loadLoadFence → 读 beginLossChange，
     * 两者相等则数据一致。
     */
    public void onGapDetected(final int termId, final int termOffset, final int length)
    {
        final long changeNumber = (long)BEGIN_LOSS_CHANGE_VH.get(this) + 1;

        BEGIN_LOSS_CHANGE_VH.setRelease(this, changeNumber);
        VarHandle.storeStoreFence();

        lossTermId = termId;
        lossTermOffset = termOffset;
        lossLength = length;

        END_LOSS_CHANGE_VH.setRelease(this, changeNumber);

        final int lossReportEndOffset;
        if (termId != lossReportTermId ||
            termOffset >= (lossReportEndOffset = (lossReportTermOffset + lossReportLength)))
        {
            reportLoss(termId, termOffset, length, length);
        }
        else if (termOffset + length > lossReportEndOffset)
        {
            reportLoss(termId, termOffset, length, length - (lossReportEndOffset - termOffset));
        }
    }

    /**
     * Source identity for this stream.
     *
     * @return source identity for a source address.
     * @see Configuration#sourceIdentity(InetSocketAddress)
     */
    String sourceIdentity()
    {
        return sourceIdentity;
    }

    /**
     * Return the {@link ReceiveChannelEndpoint} that the image is attached to.
     *
     * @return {@link ReceiveChannelEndpoint} that the image is attached to.
     */
    ReceiveChannelEndpoint channelEndpoint()
    {
        return channelEndpoint;
    }

    /**
     * Remove this image from the {@link DataPacketDispatcher} so it will process no further packets from the network.
     * Called from the {@link Receiver} thread.
     */
    void removeFromDispatcher()
    {
        channelEndpoint.dispatcher().removePublicationImage(this);
    }

    /**
     * Get the {@link RawLog} the back this image.
     *
     * @return the {@link RawLog} the back this image.
     */
    RawLog rawLog()
    {
        return rawLog;
    }

    /**
     * Activate this image from the {@link Receiver}.
     */
    void activate()
    {
        timeOfLastStateChangeNs = cachedNanoClock.nanoTime();
        state(State.ACTIVE);
    }

    /**
     * Deactivate image by setting state to {@link State#DRAINING} if currently {@link State#ACTIVE} from the
     * {@link Receiver}.
     */
    void deactivate()
    {
        if (State.ACTIVE == state)
        {
            final long nowNs = cachedNanoClock.nanoTime();

            isRebuilding = false;
            timeOfLastStateChangeNs = nowNs;

            if (!isSendingEosSm)
            {
                isSendingEosSm = !isEndOfStream || rebuildPosition.getVolatile() == hwmPosition.get();
            }

            if (isSendingEosSm)
            {
                nextSmDeadlineNs = nowNs - 1;
            }

            state(State.DRAINING);
        }
    }

    void receiverRelease()
    {
        hasReceiverReleased = true;
    }

    void addDestination(final int transportIndex, final ReceiveDestinationTransport transport)
    {
        imageConnections = ArrayUtil.ensureCapacity(imageConnections, transportIndex + 1);

        if (transport.isMulticast())
        {
            imageConnections[transportIndex] = new ImageConnection(
                cachedNanoClock.nanoTime(), transport.udpChannel().remoteControl());
        }
        else if (transport.hasExplicitControl())
        {
            imageConnections[transportIndex] = new ImageConnection(
                cachedNanoClock.nanoTime(), transport.explicitControlAddress());
        }
    }

    void removeDestination(final int transportIndex)
    {
        imageConnections[transportIndex] = null;
        updateActiveTransportCount();
    }

    void addDestinationConnectionIfUnknown(final int transportIndex, final InetSocketAddress remoteAddress)
    {
        trackConnection(transportIndex, remoteAddress, cachedNanoClock.nanoTime());
    }

    /**
     * 【Conductor 线程调用的核心方法】跟踪接收端重建进度，驱动丢包检测和 SM 调度。
     * <p>
     * 完整流程：
     * 1. 收集所有订阅者位置 → 计算 minSubscriberPosition（用于 SM 的 consumptionPosition）
     * 2. LossDetector.scan()：在 rebuildPosition → hwmPosition 之间扫描空洞
     *    → 发现空洞超时 → onGapDetected() → 记录 NAK 信息
     * 3. 推进 rebuildPosition 到最新的无空洞位置
     * 4. CongestionControl.onTrackRebuild()：计算新的 receiverWindowLength
     * 5. 判断是否需要发送 SM：
     *    - 拥塞控制强制发送
     *    - 订阅者位置推进超过窗口 1/4（threshold）
     *    - 窗口大小变化
     *    满足条件 → scheduleStatusMessage() → Receiver 线程稍后发送
     *
     * @param nowNs 当前时间
     * @return workCount
     */
    int trackRebuild(final long nowNs)
    {
        int workCount = 0;

        if (isRebuilding)
        {
            final long hwmPosition = this.hwmPosition.getVolatile();
            long minSubscriberPosition = Long.MAX_VALUE;
            long maxSubscriberPosition = 0;

            // 收集所有订阅者位置
            for (final ReadablePosition subscriberPosition : subscriberPositions)
            {
                final long position = subscriberPosition.getVolatile();
                minSubscriberPosition = Math.min(minSubscriberPosition, position);
                maxSubscriberPosition = Math.max(maxSubscriberPosition, position);
            }

            // 从最快订阅者位置开始扫描（避免重复扫描已消费区域）
            final long rebuildPosition = Math.max(this.rebuildPosition.get(), maxSubscriberPosition);
            // 丢包检测：扫描 rebuildPosition → hwmPosition 之间的空洞
            final long scanOutcome = lossDetector.scan(
                termBuffers[indexByPosition(rebuildPosition, positionBitsToShift)],
                rebuildPosition,
                hwmPosition,
                nowNs,
                termLengthMask,
                positionBitsToShift,
                initialTermId);

            // 推进 rebuildPosition 到扫描结果（无空洞的连续位置）
            final int rebuildTermOffset = (int)(rebuildPosition & termLengthMask);
            final long newRebuildPosition = (rebuildPosition - rebuildTermOffset) + rebuildOffset(scanOutcome);
            this.rebuildPosition.proposeMaxRelease(newRebuildPosition);

            // 拥塞控制：根据当前状态计算新的 receiverWindowLength
            final long ccOutcome = congestionControl.onTrackRebuild(
                nowNs,
                minSubscriberPosition,
                nextSmPosition,
                hwmPosition,
                rebuildPosition,
                newRebuildPosition,
                lossFound(scanOutcome));

            final int windowLength = CongestionControl.receiverWindowLength(ccOutcome);
            final int threshold = CongestionControl.threshold(windowLength); // = windowLength / 4

            // 判断是否需要调度发送 SM
            if (CongestionControl.shouldForceStatusMessage(ccOutcome) ||       // 拥塞控制强制
                (minSubscriberPosition > (nextSmPosition + threshold)) ||       // 消费推进超过阈值
                windowLength != nextSmReceiverWindowLength)                     // 窗口大小变化
            {
                cleanBufferTo(minSubscriberPosition - (termLengthMask + 1));
                scheduleStatusMessage(minSubscriberPosition, windowLength);     // → Receiver 线程发送
                workCount += 1;
            }
        }

        return workCount;
    }

    /**
     * 【数据帧插入 - Receiver 线程】收到 UDP 数据帧后，将其写入本地 Term Buffer。
     * <p>
     * 处理逻辑：
     * 1. 计算帧的绝对 position
     * 2. 流控检查：isFlowControlOverRun（超过接收窗口上限）/ isFlowControlUnderRun（过于落后）
     * 3. 心跳帧：不插入数据，仅更新时间戳和 hwmPosition
     * 4. 数据帧：通过 TermRebuilder.insert() 写入 Term Buffer，推进 hwmPosition
     * <p>
     * TermRebuilder.insert() 是接收端的"两阶段提交"——
     * 直接将收到的帧（含帧头和负载）写入 Term Buffer 的对应位置，
     * 最后通过 frameLengthOrdered() 设置正值帧长度（release 语义），标记帧完成。
     *
     * @param termId         数据帧所属的 term ID
     * @param termOffset     数据帧在 term 中的偏移
     * @param buffer         数据帧 buffer
     * @param length         数据帧长度
     * @param transportIndex 接收该帧的传输通道索引
     * @param srcAddress     发送端地址
     * @return 插入的字节数
     */
    int insertPacket(
        final int termId,
        final int termOffset,
        final UnsafeBuffer buffer,
        final int length,
        final int transportIndex,
        final InetSocketAddress srcAddress)
    {
        if (null != rejectionReason)
        {
            return 0;
        }

        final boolean isHeartbeat = DataHeaderFlyweight.isHeartbeat(buffer, length);
        final long packetPosition = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        final long proposedPosition = isHeartbeat ? packetPosition : packetPosition + length;

        if (!isFlowControlOverRun(proposedPosition))
        {
            if (isHeartbeat)
            {
                final long potentialWindowBottom = lastSmPosition - (termLengthMask + 1);
                final long publicationWindowBottom = potentialWindowBottom < 0 ? 0 : potentialWindowBottom;

                if (packetPosition >= publicationWindowBottom)
                {
                    final long nowNs = cachedNanoClock.nanoTime();
                    timeOfLastPacketNs = nowNs;
                    final ImageConnection imageConnection = trackConnection(transportIndex, srcAddress, nowNs);

                    if (DataHeaderFlyweight.isEndOfStream(buffer))
                    {
                        imageConnection.eosPosition = packetPosition;
                        imageConnection.isEos = true;

                        if (!this.isEndOfStream && isAllConnectedEos())
                        {
                            final long eosPosition = findEosPosition();

                            if (DataHeaderFlyweight.isRevoked(buffer))
                            {
                                isPublicationRevoked(rawLog.metaData(), true);

                                logRevoke(eosPosition, sessionId(), streamId(), channel());
                                publicationImagesRevoked.increment();
                            }

                            LogBufferDescriptor.endOfStreamPosition(rawLog.metaData(), eosPosition);
                            this.isEndOfStream = true;
                        }
                    }

                    hwmPosition.proposeMaxRelease(proposedPosition);
                    heartbeatsReceived.incrementRelease();
                }
                else
                {
                    flowControlUnderRuns.incrementRelease();
                }
            }
            else if (!isFlowControlUnderRun(packetPosition))
            {
                final long nowNs = cachedNanoClock.nanoTime();
                timeOfLastPacketNs = nowNs;
                trackConnection(transportIndex, srcAddress, nowNs);

                final UnsafeBuffer termBuffer = termBuffers[indexByPosition(packetPosition, positionBitsToShift)];
                TermRebuilder.insert(termBuffer, termOffset, buffer, length);

                hwmPosition.proposeMaxRelease(proposedPosition);
            }
            else if (packetPosition >= (lastSmPosition - maxReceiverWindowLength))
            {
                trackConnection(transportIndex, srcAddress, cachedNanoClock.nanoTime());
            }
        }

        return length;
    }

    private static void logRevoke(
        final long revokedPos,
        final int sessionId,
        final int streamId,
        final String channel)
    {
    }

    /**
     * To be called from the {@link Receiver} to see if image should be dispatched to.
     *
     * @param nowNs current time to check against for activity.
     * @return true if the image should be retained otherwise false.
     */
    boolean isConnected(final long nowNs)
    {
        return ((timeOfLastPacketNs + imageLivenessTimeoutNs) - nowNs >= 0) && !isReceiverReleaseTriggered;
    }

    boolean isEndOfStream()
    {
        return isEndOfStream;
    }

    /**
     * Check for EOS from publication and switch to {@link State#DRAINING} if at end of stream position.
     *
     * @param nowNs current time of use.
     */
    void checkEosForDrainTransition(final long nowNs)
    {
        if (!isSendingEosSm)
        {
            if (isEndOfStream && rebuildPosition.getVolatile() == hwmPosition.get() && State.ACTIVE == state)
            {
                isRebuilding = false;
                timeOfLastStateChangeNs = nowNs;

                isSendingEosSm = true;
                nextSmDeadlineNs = nowNs - 1;
                state(State.DRAINING);
            }
        }
    }

    /**
     * 【SM 发送 - Receiver 线程】发送 Conductor 线程调度的 Status Message。
     * <p>
     * SM 包含：
     * - consumptionTermId + consumptionTermOffset：最慢订阅者的消费位置（发送端据此回收 buffer）
     * - receiverWindowLength：接收窗口大小（由 CongestionControl 计算）
     * <p>
     * 发送时机：
     * - Conductor 调用 scheduleStatusMessage() 更新 nextSmPosition/nextSmReceiverWindowLength
     *   （通过 SeqLock 无锁协议跨线程传递）
     * - SM 周期超时（smTimeoutNs）
     * <p>
     * SM → 发送端 → FlowControl.onStatusMessage() → 更新 senderLimit → 控制发送速率
     *
     * @param nowNs 当前时间
     * @return workCount
     */
    int sendPendingStatusMessage(final long nowNs)
    {
        int workCount = 0;
        final long changeNumber = (long)END_SM_CHANGE_VH.getAcquire(this);
        final boolean hasSmTimedOut = smEnabled && nextSmDeadlineNs - nowNs < 0;

        if (null != rejectionReason)
        {
            if (hasSmTimedOut)
            {
                channelEndpoint.sendErrorFrame(
                    imageConnections, sessionId, streamId, IMAGE_REJECTED.value(), rejectionReason);

                nextSmDeadlineNs = nowNs + smTimeoutNs;
                workCount++;
            }

            return workCount;
        }

        final Integer responseSessionId;
        if (hasSmTimedOut && null != (responseSessionId = this.responseSessionId))
        {
            channelEndpoint.sendResponseSetup(imageConnections, sessionId, streamId, responseSessionId);
        }

        if (changeNumber != lastSmChangeNumber || hasSmTimedOut)
        {
            final long smPosition = nextSmPosition;
            final int receiverWindowLength = nextSmReceiverWindowLength;

            VarHandle.loadLoadFence();

            if (changeNumber == (long)BEGIN_SM_CHANGE_VH.getAcquire(this))
            {
                final int termId = computeTermIdFromPosition(smPosition, positionBitsToShift, initialTermId);
                final int termOffset = (int)smPosition & termLengthMask;
                final int termLength = termLengthMask + 1;
                final short flags = isSendingEosSm ? StatusMessageFlyweight.END_OF_STREAM_FLAG : 0;

                channelEndpoint.sendStatusMessage(
                    imageConnections, sessionId, streamId, termId, termOffset, receiverWindowLength, flags);

                statusMessagesSent.incrementRelease();

                lastSmPosition = smPosition;
                lastOverrunThreshold = smPosition + (termLength >> 1);
                lastSmChangeNumber = changeNumber;
                nextSmDeadlineNs = nowNs + smTimeoutNs;

                updateActiveTransportCount();
            }

            workCount = 1;
        }

        return workCount;
    }

    /**
     * 【NAK 发送 - Receiver 线程】处理 Conductor 线程检测到的丢包，发送 NAK 帧到发送端。
     * <p>
     * Conductor 线程在 onGapDetected() 中将丢包信息写入 loss* 字段（SeqLock 协议）。
     * Receiver 线程在此方法中读取并发送 NAK：
     * - 可靠模式（isReliable）：发送 NAK(termId, termOffset, length) → 发送端重传
     * - 不可靠模式：用 PADDING 帧填充空洞（tryFillGap），跳过丢失的数据
     * <p>
     * NAK 帧格式：
     * | frameLength | version | flags | type=NAK | sessionId | streamId | termId | termOffset | length |
     *
     * @return workCount
     */
    int processPendingLoss()
    {
        if (isPublicationRevoked(rawLog.metaData()))
        {
            return 0;
        }

        int workCount = 0;
        final long changeNumber = (long)END_LOSS_CHANGE_VH.getAcquire(this);

        if (changeNumber != lastLossChangeNumber)
        {
            final int termId = lossTermId;
            final int termOffset = lossTermOffset;
            final int length = lossLength;

            VarHandle.loadLoadFence();

            if (changeNumber == (long)BEGIN_LOSS_CHANGE_VH.getAcquire(this))
            {
                if (isReliable)
                {
                    // 可靠模式：发送 NAK → 发送端重传丢失的数据
                    channelEndpoint.sendNakMessage(imageConnections, sessionId, streamId, termId, termOffset, length);
                    nakMessagesSent.incrementRelease();
                    receiverNaksSent.incrementRelease();
                }
                else
                {
                    // 不可靠模式：用 PADDING 帧填充空洞，跳过丢失数据
                    final UnsafeBuffer termBuffer = termBuffers[indexByTerm(initialTermId, termId)];
                    if (tryFillGap(rawLog.metaData(), termBuffer, termId, termOffset, length))
                    {
                        lossGapFills.incrementRelease();
                    }
                }

                lastLossChangeNumber = changeNumber;
            }

            workCount = 1;
        }

        return workCount;
    }

    /**
     * 【RTT 测量发起 - Receiver 线程】由 CongestionControl 决定是否发起 RTT 测量。
     * <p>
     * StaticWindowCongestionControl 不测量 RTT（shouldMeasureRtt 返回 false）。
     * CubicCongestionControl 会周期性发起 RTT 测量，用于动态调整窗口大小。
     * <p>
     * 测量流程：
     * 1. 接收端发送 RTTM 帧（flags=REPLY_FLAG, echoTimestampNs=当前时间）
     * 2. 发送端收到 → 回复 RTTM 帧（echoTimestampNs 原样返回）
     * 3. 接收端收到回复 → rttNs = nowNs - echoTimestampNs - receptionDelta
     * 4. congestionControl.onRttMeasurement(rttNs) → 用于窗口调整
     *
     * @param nowNs 当前时间
     * @return workCount
     */
    int initiateAnyRttMeasurements(final long nowNs)
    {
        int workCount = 0;

        if (congestionControl.shouldMeasureRtt(nowNs))
        {
            final long preciseTimeNs = nanoClock.nanoTime();

            channelEndpoint.sendRttMeasurement(imageConnections, sessionId, streamId, preciseTimeNs, 0, true);
            congestionControl.onRttMeasurementSent(preciseTimeNs);

            workCount = 1;
        }

        return workCount;
    }

    /**
     * Called from the {@link Receiver} upon receiving an RTT Measurement that is a reply.
     *
     * @param header         of the measurement message.
     * @param transportIndex that the RTT Measurement came in on.
     * @param srcAddress     from the sender requesting the measurement
     */
    void onRttMeasurement(
        final RttMeasurementFlyweight header, final int transportIndex, final InetSocketAddress srcAddress)
    {
        final long nowNs = nanoClock.nanoTime();
        final long rttInNs = nowNs - header.echoTimestampNs() - header.receptionDelta();

        congestionControl.onRttMeasurement(nowNs, rttInNs, srcAddress);
    }

    boolean isAcceptingSubscriptions()
    {
        if (subscriberPositions.length > 0)
        {
            final State state = this.state;
            return State.INIT == state || State.ACTIVE == state || (State.DRAINING == state && !isDrained());
        }
        return false;
    }

    long joinPosition()
    {
        long position = rebuildPosition.get();

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            position = Math.min(subscriberPosition.getVolatile(), position);
        }

        return position;
    }

    boolean hasSendResponseSetup()
    {
        return 0 != (SEND_RESPONSE_SETUP_FLAG & flags);
    }

    void responseSessionId(final Integer responseSessionId)
    {
        this.responseSessionId = responseSessionId;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("fallthrough")
    public void onTimeEvent(final long timeNs, final long timesMs, final DriverConductor conductor)
    {
        switch (state)
        {
            case ACTIVE:
                if (isPublicationRevoked(rawLog.metaData()))
                {
                    state(State.DRAINING);
                }
                else
                {
                    checkUntetheredSubscriptions(timeNs, conductor);
                }
                break;

            case DRAINING:
                if ((isDrained() && ((timeOfLastStateChangeNs + (SM_EOS_MULTIPLE * smTimeoutNs)) - timeNs < 0)) ||
                    isPublicationRevoked(rawLog.metaData()))
                {
                    if (isPublicationRevoked(rawLog.metaData()))
                    {
                        isRebuilding = false;
                        isSendingEosSm = true;

                        nextSmDeadlineNs = timeNs - 1;
                    }

                    conductor.transitionToLinger(this);

                    isReceiverReleaseTriggered = true;
                    channelEndpoint.decRefImages();
                    conductor.tryCloseReceiveChannelEndpoint(channelEndpoint);

                    timeOfLastStateChangeNs = timeNs;
                    state(State.LINGER);
                }
                break;

            case LINGER:
                if (hasNoSubscribers() || ((timeOfLastStateChangeNs + imageLivenessTimeoutNs) - timeNs < 0))
                {
                    conductor.cleanupImage(this);
                    timeOfLastStateChangeNs = timeNs;
                    state(State.DONE);
                }
                break;

            case DONE:
            case INIT:
                break;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return hasReceiverReleased && State.DONE == state;
    }

    void reject(final String reason)
    {
        rejectionReason = reason;
    }

    void stopStatusMessagesIfNotActive()
    {
        if (State.ACTIVE != state)
        {
            smEnabled = false;
        }
    }

    private void reportLoss(final int termId, final int termOffset, final int length, final int bytesLost)
    {
        if (null != reportEntry)
        {
            reportEntry.recordObservation(bytesLost, epochClock.time());
        }
        else if (null != lossReport)
        {
            reportEntry = lossReport.createEntry(
                bytesLost, epochClock.time(), sessionId, streamId, channel(), sourceIdentity);

            if (null == reportEntry)
            {
                lossReport = null;
            }
        }

        lossReportTermId = termId;
        lossReportTermOffset = termOffset;
        lossReportLength = length;
    }

    private void state(final State state)
    {
        this.state = state;
    }

    private boolean isDrained()
    {
        final long rebuildPosition = this.rebuildPosition.get();

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            if (subscriberPosition.getVolatile() < rebuildPosition)
            {
                return false;
            }
        }

        return true;
    }

    private boolean hasNoSubscribers()
    {
        return subscriberPositions.length == 0;
    }

    private boolean isFlowControlUnderRun(final long packetPosition)
    {
        final boolean isFlowControlUnderRun = packetPosition < lastSmPosition;

        if (isFlowControlUnderRun)
        {
            flowControlUnderRuns.incrementRelease();
        }

        return isFlowControlUnderRun;
    }

    private boolean isFlowControlOverRun(final long proposedPosition)
    {
        final boolean isFlowControlOverRun = proposedPosition > lastOverrunThreshold;

        if (isFlowControlOverRun)
        {
            flowControlOverRuns.incrementRelease();
        }

        return isFlowControlOverRun;
    }

    private void cleanBufferTo(final long position)
    {
        final long cleanPosition = this.cleanPosition;
        if (position > cleanPosition)
        {
            final int bytesForCleaning = (int)(position - cleanPosition);
            final UnsafeBuffer dirtyTermBuffer = termBuffers[indexByPosition(cleanPosition, positionBitsToShift)];
            final int termOffset = (int)cleanPosition & termLengthMask;
            final int length = Math.min(bytesForCleaning, dirtyTermBuffer.capacity() - termOffset);

            dirtyTermBuffer.setMemory(termOffset, length - SIZE_OF_LONG, (byte)0);
            dirtyTermBuffer.putLongRelease(termOffset + (length - SIZE_OF_LONG), 0);
            this.cleanPosition = cleanPosition + length;
        }
    }

    private ImageConnection trackConnection(
        final int transportIndex, final InetSocketAddress srcAddress, final long nowNs)
    {
        ImageConnection[] imageConnections = this.imageConnections;
        if (transportIndex >= imageConnections.length)
        {
            this.imageConnections = imageConnections = Arrays.copyOf(imageConnections, transportIndex + 1);
        }

        ImageConnection imageConnection = imageConnections[transportIndex];
        if (null == imageConnection)
        {
            imageConnection = new ImageConnection(nowNs, srcAddress);
            imageConnections[transportIndex] = imageConnection;
        }

        imageConnection.timeOfLastActivityNs = nowNs;
        imageConnection.timeOfLastFrameNs = nowNs;
        return imageConnection;
    }

    private boolean isAllConnectedEos()
    {
        for (int i = 0, length = imageConnections.length; i < length; i++)
        {
            final ImageConnection imageConnection = imageConnections[i];

            if (null != imageConnection && !imageConnection.isEos)
            {
                return false;
            }
            else if (null == imageConnection && channelEndpoint.hasDestination(i))
            {
                return false;
            }
        }

        return true;
    }

    private long findEosPosition()
    {
        long eosPosition = 0;

        for (final ImageConnection imageConnection : imageConnections)
        {
            if (null != imageConnection && imageConnection.eosPosition > eosPosition)
            {
                eosPosition = imageConnection.eosPosition;
            }
        }

        return eosPosition;
    }

    private void scheduleStatusMessage(final long smPosition, final int receiverWindowLength)
    {
        final long changeNumber = (long)BEGIN_SM_CHANGE_VH.get(this) + 1;

        BEGIN_SM_CHANGE_VH.setRelease(this, changeNumber);
        VarHandle.storeStoreFence();

        nextSmPosition = smPosition;
        nextSmReceiverWindowLength = receiverWindowLength;

        END_SM_CHANGE_VH.setRelease(this, changeNumber);
    }

    private void checkUntetheredSubscriptions(final long nowNs, final DriverConductor conductor)
    {
        final ArrayList<UntetheredSubscription> untetheredSubscriptions = this.untetheredSubscriptions;
        final int untetheredSubscriptionsSize = untetheredSubscriptions.size();
        if (untetheredSubscriptionsSize > 0)
        {
            final long untetheredWindowLimit = untetheredWindowLimit();

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
                        conductor.notifyUnavailableImageLink(correlationId, untethered.subscriptionLink);
                        untethered.state(UntetheredSubscription.State.LINGER, nowNs, streamId, sessionId);
                    }
                }
                else if (UntetheredSubscription.State.LINGER == untethered.state)
                {
                    if ((untethered.timeOfLastUpdateNs + untetheredLingerTimeoutNs) - nowNs <= 0)
                    {
                        subscriberPositions = ArrayUtil.remove(subscriberPositions, untethered.position);
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
                        final long joinPosition = joinPosition();
                        subscriberPositions = ArrayUtil.add(subscriberPositions, untethered.position);
                        conductor.notifyAvailableImageLink(
                            correlationId,
                            sessionId,
                            untethered.subscriptionLink,
                            untethered.position.id(),
                            joinPosition,
                            rawLog.fileName(),
                            sourceIdentity);
                        untethered.state(UntetheredSubscription.State.ACTIVE, nowNs, streamId, sessionId);
                    }
                }
            }
        }
    }

    private long untetheredWindowLimit()
    {
        long maxConsumerPosition = 0;

        for (final ReadablePosition subscriberPosition : subscriberPositions)
        {
            final long position = subscriberPosition.getVolatile();
            if (position > maxConsumerPosition)
            {
                maxConsumerPosition = position;
            }
        }

        final int windowLength = nextSmReceiverWindowLength;

        return (maxConsumerPosition - windowLength) + (windowLength >> 2);
    }

    private void updateActiveTransportCount()
    {
        final long nowNs = cachedNanoClock.nanoTime();
        int activeTransportCount = 0;

        for (final ImageConnection imageConnection : imageConnections)
        {
            if (null != imageConnection && ((imageConnection.timeOfLastFrameNs + imageLivenessTimeoutNs) - nowNs > 0))
            {
                activeTransportCount++;
            }
        }

        final UnsafeBuffer metaDataBuffer = rawLog.metaData();
        if (metaDataBuffer.getInt(LOG_ACTIVE_TRANSPORT_COUNT) != activeTransportCount)
        {
            LogBufferDescriptor.activeTransportCount(metaDataBuffer, activeTransportCount);
        }
    }

    private ReadablePosition[] positionArray(final ArrayList<SubscriberPosition> subscriberPositions, final long nowNs)
    {
        final int size = subscriberPositions.size();
        final ReadablePosition[] positions = new ReadablePosition[subscriberPositions.size()];

        for (int i = 0; i < size; i++)
        {
            final SubscriberPosition subscriberPosition = subscriberPositions.get(i);
            positions[i] = subscriberPosition.position();

            if (!subscriberPosition.subscription().isTether())
            {
                untetheredSubscriptions.add(new UntetheredSubscription(
                    subscriberPosition.subscription(), subscriberPosition.position(), nowNs));
            }
        }

        return positions;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "PublicationImage{" +
            "state=" + state +
            ", sourceIdentity='" + sourceIdentity + '\'' +
            ", streamId=" + streamId +
            ", sessionId=" + sessionId +
            '}';
    }
}

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

import io.aeron.driver.media.DataTransportPoller;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.ReceiveDestinationTransport;
import io.aeron.driver.media.UdpChannel;
import org.agrona.collections.ArrayListUtil;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.ArrayList;

import static io.aeron.driver.Configuration.PENDING_SETUPS_TIMEOUT_NS;
import static io.aeron.driver.status.SystemCounterDescriptor.BYTES_RECEIVED;
import static io.aeron.driver.status.SystemCounterDescriptor.RESOLUTION_CHANGES;

/**
 * Agent that receives messages streams and rebuilds {@link PublicationImage}s, plus iterates over them sending status
 * and control messages back to the {@link Sender}.
 *
 * 【源码解析】Receiver —— Media Driver 的"接收引擎"，三大核心 Agent 之一。
 *
 * 职责：
 * 1. 通过 DataTransportPoller 从 UDP Socket 接收数据帧（Data Frame / Setup Frame）
 * 2. 将收到的帧分发到对应的 PublicationImage（DataPacketDispatcher 负责路由）
 * 3. 遍历所有 PublicationImage，执行：
 *    - 发送 Status Message（NAK / SM）给 Sender 进行流控
 *    - 处理丢包重传请求
 *    - 发起 RTT 测量
 * 4. 清理不再连接的 Image
 * 5. 检查 pending 的 Setup 消息（多播/MDC 场景下的握手消息）
 *
 * 线程模型：在 DEDICATED 模式下独占一个线程；Conductor 通过 ReceiverProxy 命令队列向 Receiver 发送指令。
 */
public final class Receiver implements Agent
{
    private static final PublicationImage[] EMPTY_IMAGES = new PublicationImage[0];

    private final long reResolutionCheckIntervalNs;        // DNS 重新解析检查间隔
    private long reResolutionDeadlineNs;
    private final DataTransportPoller dataTransportPoller;  // 数据传输轮询器：封装了 Selector，从注册的 ChannelEndpoint 读取 UDP 数据
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue; // Conductor → Receiver 的命令队列（线程安全）
    private final AtomicCounter totalBytesReceived;        // 累计接收字节数计数器
    private final AtomicCounter resolutionChanges;         // DNS 解析变化次数
    private final NanoClock nanoClock;
    private final CachedNanoClock cachedNanoClock;         // 缓存时钟，减少系统调用
    private PublicationImage[] publicationImages = EMPTY_IMAGES; // 当前活跃的 PublicationImage 数组（无锁结构）
    private final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = new ArrayList<>(); // 等待处理的 Setup 消息
    private final DriverConductorProxy conductorProxy;     // 向 Conductor 发送通知的代理
    private final DutyCycleTracker dutyCycleTracker;       // 工作周期耗时追踪

    /**
     * 【构造函数】从 MediaDriver.Context 提取依赖。
     * DataTransportPoller 是数据接收的核心组件，commandQueue 用于接收 Conductor 的指令。
     */
    Receiver(final MediaDriver.Context ctx)
    {
        dataTransportPoller = ctx.dataTransportPoller();       // 数据传输轮询器（内含 Selector）
        commandQueue = ctx.receiverCommandQueue();             // Conductor → Receiver 命令队列
        totalBytesReceived = ctx.systemCounters().get(BYTES_RECEIVED);
        resolutionChanges = ctx.systemCounters().get(RESOLUTION_CHANGES);
        nanoClock = ctx.nanoClock();
        cachedNanoClock = ctx.receiverCachedNanoClock();
        conductorProxy = ctx.driverConductorProxy();           // Receiver → Conductor 通知代理
        reResolutionCheckIntervalNs = ctx.reResolutionCheckIntervalNs();
        dutyCycleTracker = ctx.receiverDutyCycleTracker();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.update(nowNs);
        reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        dataTransportPoller.close();
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "receiver";
    }

    /**
     * {@inheritDoc}
     */
    /**
     * 【核心工作循环】由 AgentRunner 反复调用，每次执行以下步骤：
     *
     * 1. 排空 Conductor 发来的命令（addSubscription / removeSubscription / newPublicationImage 等）
     * 2. 从网络轮询数据（dataTransportPoller.pollTransports）—— 实际的 UDP 收包入口
     * 3. 遍历所有 PublicationImage：
     *    - 仍然连接的 Image: 发送 Status Message（流控反馈）、处理丢包、RTT 测量
     *    - 已断连的 Image: 从数组中移除，释放资源
     * 4. 检查 pending 的 Setup 消息（超时重发或清理）
     * 5. 定期检查 DNS 重新解析
     *
     * @return workCount > 0 表示有工作完成，IdleStrategy 据此决定是否休眠
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.measureAndUpdate(nowNs);

        // 步骤 1：排空 Conductor 发来的命令（如：注册/注销 subscription、新增 Image 等）
        int workCount = commandQueue.drain(CommandProxy.RUN_TASK, Configuration.COMMAND_DRAIN_LIMIT);

        // 步骤 2：从所有已注册的 UDP Socket 接收数据帧，分发到对应的 PublicationImage
        final int bytesReceived = dataTransportPoller.pollTransports();
        totalBytesReceived.getAndAddOrdered(bytesReceived);

        // 步骤 3：遍历所有 PublicationImage（倒序遍历，便于移除）
        final PublicationImage[] publicationImages = this.publicationImages;
        for (int lastIndex = publicationImages.length - 1, i = lastIndex; i >= 0; i--)
        {
            final PublicationImage image = publicationImages[i];
            if (image.isConnected(nowNs))
            {
                // 检查是否到达 End-of-Stream 可以进入 drain 状态
                image.checkEosForDrainTransition(nowNs);
                // 发送 Status Message（SM）给 Sender，包含接收端 position，实现流控
                workCount += image.sendPendingStatusMessage(nowNs);
                // 处理丢包：发送 NAK（Negative Acknowledgment）请求 Sender 重传
                workCount += image.processPendingLoss();
                // RTT 测量：发送 RTT 请求帧
                workCount += image.initiateAnyRttMeasurements(nowNs);
            }
            else
            {
                // Image 已断连：从数组中移除，从 Dispatcher 中注销，释放 Receiver 端资源
                this.publicationImages = 1 == this.publicationImages.length ?
                    EMPTY_IMAGES : ArrayUtil.remove(this.publicationImages, i);
                image.removeFromDispatcher();
                image.receiverRelease();
            }
        }

        // 步骤 4：检查 Setup 消息超时（多播/MDC 场景下的握手机制）
        checkPendingSetupMessages(nowNs);

        // 步骤 5：定期 DNS 重新解析检查
        if (reResolutionCheckIntervalNs > 0 && (reResolutionDeadlineNs - nowNs) < 0)
        {
            reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
            dataTransportPoller.checkForReResolutions(nowNs, conductorProxy);
        }

        return workCount + bytesReceived;
    }

    void addPendingSetupMessage(
        final int sessionId,
        final int streamId,
        final int transportIndex,
        final ReceiveChannelEndpoint channelEndpoint,
        final boolean periodic,
        final InetSocketAddress controlAddress)
    {
        final PendingSetupMessageFromSource cmd = new PendingSetupMessageFromSource(
            sessionId, streamId, transportIndex, channelEndpoint, periodic, controlAddress);

        cmd.timeOfStatusMessageNs(cachedNanoClock.nanoTime());
        pendingSetupMessages.add(cmd);
    }

    /**
     * 【添加订阅 - 不指定 session】由 Conductor 通过 ReceiverProxy 命令队列触发。
     * 在 DataPacketDispatcher 中注册 streamId，使后续收到的该 stream 的数据帧能被分发。
     */
    void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId);
    }

    /**
     * 【添加订阅 - 指定 session】用于精确过滤特定 session 的数据。
     * 如果 endpoint 有 explicit control 地址（MDC 模式），主动发送 Setup-Eliciting SM 请求发送端发起 Setup。
     */
    void onAddSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        channelEndpoint.dispatcher().addSubscription(streamId, sessionId);
        if (channelEndpoint.hasExplicitControl())
        {
            channelEndpoint.sendSetupElicitingStatusMessage(
                0, channelEndpoint.explicitControlAddress(), sessionId, streamId);
        }
    }

    void onRequestSetup(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        if (channelEndpoint.hasExplicitControl())
        {
            channelEndpoint.sendSetupElicitingStatusMessage(
                0, channelEndpoint.explicitControlAddress(), sessionId, streamId);
        }
    }

    void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId);
    }

    void onRemoveSubscription(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        channelEndpoint.dispatcher().removeSubscription(streamId, sessionId);

        final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = this.pendingSetupMessages;
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint &&
                pending.streamId() == streamId &&
                pending.sessionId() == sessionId)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                pending.removeFromDataPacketDispatcher();
            }
        }
    }

    /**
     * 【新增 PublicationImage】当 Receiver 收到来自新发送端的 Setup 帧后，Conductor 创建 PublicationImage 并通知 Receiver。
     * 将新 Image 添加到 Receiver 的轮询数组中，同时注册到 Dispatcher 以接收后续数据帧。
     */
    void onNewPublicationImage(final ReceiveChannelEndpoint channelEndpoint, final PublicationImage image)
    {
        disconnectInactiveImage(channelEndpoint, image.streamId(), image.sessionId());
        publicationImages = ArrayUtil.add(publicationImages, image);
        channelEndpoint.dispatcher().addPublicationImage(image);
    }

    /**
     * 【注册接收端 ChannelEndpoint】将该 endpoint 的 UDP Socket 注册到 DataTransportPoller（内含 Selector），
     * 使后续 pollTransports() 能从该 Socket 读取数据。
     * 如果是 MDC explicit control 模式，还会主动发送 Setup-Eliciting SM。
     */
    void onRegisterReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        if (!channelEndpoint.hasDestinationControl())
        {
            dataTransportPoller.registerForRead(channelEndpoint, channelEndpoint, 0);

            if (channelEndpoint.hasExplicitControl())
            {
                addPendingSetupMessage(0, 0, 0, channelEndpoint, true, channelEndpoint.explicitControlAddress());
                channelEndpoint.sendSetupElicitingStatusMessage(0, channelEndpoint.explicitControlAddress(), 0, 0);
            }
        }
    }

    /**
     * 【关闭接收端 ChannelEndpoint】从 DataTransportPoller 中注销所有该 endpoint 的读取注册，
     * 清理相关 pending setup 消息，并通知 Conductor 该 endpoint 已关闭。
     */
    void onCloseReceiveChannelEndpoint(final ReceiveChannelEndpoint channelEndpoint)
    {
        dataTransportPoller.cancelReadForAllTransports(channelEndpoint);

        final ArrayList<PendingSetupMessageFromSource> pendingSetupMessages = this.pendingSetupMessages;
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint)
            {
                ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                pending.removeFromDataPacketDispatcher();
            }
        }

        conductorProxy.receiveChannelEndpointClosed(channelEndpoint);
    }

    void onRemoveCoolDown(final ReceiveChannelEndpoint channelEndpoint, final int sessionId, final int streamId)
    {
        channelEndpoint.dispatcher().removeCoolDown(sessionId, streamId);
    }

    void onAddDestination(final ReceiveChannelEndpoint channelEndpoint, final ReceiveDestinationTransport transport)
    {
        final int transportIndex = channelEndpoint.addDestination(transport);
        dataTransportPoller.registerForRead(channelEndpoint, transport, transportIndex);

        if (transport.hasExplicitControl())
        {
            addPendingSetupMessage(0, 0, transportIndex, channelEndpoint, true, transport.explicitControlAddress());
            channelEndpoint.sendSetupElicitingStatusMessage(transportIndex, transport.explicitControlAddress(), 0, 0);
        }

        for (final PublicationImage image : publicationImages)
        {
            if (channelEndpoint == image.channelEndpoint())
            {
                image.addDestination(transportIndex, transport);
            }
        }
    }

    void onRemoveDestination(final ReceiveChannelEndpoint channelEndpoint, final UdpChannel udpChannel)
    {
        final int transportIndex = channelEndpoint.destination(udpChannel);
        if (ArrayUtil.UNKNOWN_INDEX != transportIndex)
        {
            final ReceiveDestinationTransport transport = channelEndpoint.destination(transportIndex);

            dataTransportPoller.cancelRead(channelEndpoint, transport);
            channelEndpoint.removeDestination(transportIndex);

            for (final PublicationImage image : publicationImages)
            {
                if (channelEndpoint == image.channelEndpoint())
                {
                    image.removeDestination(transportIndex);
                }
            }

            conductorProxy.closeReceiveDestination(transport);
        }
    }

    void onResolutionChange(
        final ReceiveChannelEndpoint channelEndpoint, final UdpChannel channel, final InetSocketAddress newAddress)
    {
        final int transportIndex = channelEndpoint.hasDestinationControl() ? channelEndpoint.destination(channel) : 0;

        for (int i = 0, size = pendingSetupMessages.size(); i < size; i++)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if (pending.channelEndpoint() == channelEndpoint &&
                pending.isPeriodic() &&
                pending.transportIndex() == transportIndex)
            {
                pending.controlAddress(newAddress);
                resolutionChanges.getAndAddRelease(1);
            }
        }

        channelEndpoint.updateControlAddress(transportIndex, newAddress);
    }

    void onRejectImage(final long imageCorrelationId, final long position, final String reason)
    {
        for (final PublicationImage image : publicationImages)
        {
            if (imageCorrelationId == image.correlationId())
            {
                image.reject(reason);
                break;
            }
        }
    }

    /**
     * 【检查 Pending Setup 消息超时】
     * - 非周期性的 pending: 超时后直接移除（一次性 setup 请求，发送端未响应则放弃）
     * - 周期性的 pending（MDC explicit control 模式）: 超时后重新发送 Setup-Eliciting SM
     */
    private void checkPendingSetupMessages(final long nowNs)
    {
        for (int lastIndex = pendingSetupMessages.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final PendingSetupMessageFromSource pending = pendingSetupMessages.get(i);

            if ((pending.timeOfStatusMessageNs() + PENDING_SETUPS_TIMEOUT_NS) - nowNs < 0)
            {
                if (!pending.isPeriodic())
                {
                    ArrayListUtil.fastUnorderedRemove(pendingSetupMessages, i, lastIndex--);
                    pending.removeFromDataPacketDispatcher();
                }
                else if (pending.shouldElicitSetupMessage())
                {
                    pending.timeOfStatusMessageNs(nowNs);
                    pending.channelEndpoint().sendSetupElicitingStatusMessage(
                        pending.transportIndex(), pending.controlAddress(), pending.sessionId(), pending.streamId());
                }
            }
        }
    }

    void disconnectInactiveImage(
        final ReceiveChannelEndpoint channelEndpoint,
        final int streamId,
        final int sessionId)
    {
        for (final PublicationImage publicationImage : publicationImages)
        {
            if (publicationImage.channelEndpoint() == channelEndpoint &&
                publicationImage.streamId() == streamId &&
                publicationImage.sessionId() == sessionId)
            {
                publicationImage.stopStatusMessagesIfNotActive();
            }
        }
    }
}

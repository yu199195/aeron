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

import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;

import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

/**
 * 【源码解析】UnicastFlowControl —— 单播模式下的发送端流控策略。
 * <p>
 * 原理：接收端周期性发送 Status Message（SM），包含 consumptionPosition 和 receiverWindowLength。
 * 本流控策略根据 SM 计算 senderLimit = consumptionPosition + receiverWindowLength，
 * 发送端只在 senderPosition < senderLimit 时才发数据。
 * <p>
 * 公式：senderLimit = max(当前senderLimit, consumptionPosition + receiverWindowLength)
 * - 取 max 是因为 SM 可能乱序到达，不应让 limit 倒退
 * - consumptionPosition：接收端已消费到的位置
 * - receiverWindowLength：接收端愿意接受的窗口大小（由 CongestionControl 决定）
 * <p>
 * 单播特点：
 * - 不追踪接收端身份（hasRequiredReceivers 始终返回 true）
 * - onIdle 时保持 senderLimit 不变（不做超时收缩）
 */
public class UnicastFlowControl implements FlowControl
{
    /**
     * 重传窗口倍数：maxRetransmissionLength = receiverWindowLength × retransmitReceiverWindowMultiple
     */
    private int retransmitReceiverWindowMultiple;

    /**
     * 【核心方法】处理接收端发来的 Status Message，计算新的 senderLimit。
     * <p>
     * SM 包含：consumptionTermId + consumptionTermOffset（接收端消费到的位置）
     *          + receiverWindowLength（接收端愿意接收的窗口大小）
     * <p>
     * 计算：position = computePosition(consumptionTermId, consumptionTermOffset, ...)
     * 新 limit = max(当前 senderLimit, position + receiverWindowLength)
     * <p>
     * 这个 limit 最终传导到：
     * senderLimit → NetworkPublication.senderLimit → positionLimit → 应用 offer() 背压
     */
    public long onStatusMessage(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long senderLimit,
        final int initialTermId,
        final int positionBitsToShift,
        final long timeNs)
    {
        // 从 SM 中提取接收端的消费位置（绝对 position）
        final long position = computePosition(
            flyweight.consumptionTermId(),
            flyweight.consumptionTermOffset(),
            positionBitsToShift,
            initialTermId);

        // 新 limit = 消费位置 + 接收窗口；取 max 防止 SM 乱序导致 limit 倒退
        return Math.max(senderLimit, position + flyweight.receiverWindowLength());
    }

    /**
     * {@inheritDoc}
     */
    public void onTriggerSendSetup(
        final StatusMessageFlyweight flyweight,
        final InetSocketAddress receiverAddress,
        final long timeNs)
    {
    }

    /**
     * {@inheritDoc}
     */
    public long onSetup(
        final SetupFlyweight flyweight,
        final long senderLimit,
        final long senderPosition,
        final int positionBitsToShift,
        final long timeNs)
    {
        return senderLimit;
    }

    /**
     * {@inheritDoc}
     */
    public void onError(final ErrorFlyweight errorFlyweight, final InetSocketAddress receiverAddress, final long timeNs)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void initialize(
        final MediaDriver.Context context,
        final CountersManager countersManager,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final long registrationId,
        final int initialTermId,
        final int termBufferLength)
    {
        retransmitReceiverWindowMultiple = FlowControl.retransmitReceiverWindowMultiple(
            udpChannel,
            context.unicastFlowControlRetransmitReceiverWindowMultiple()
        );
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }

    /**
     * {@inheritDoc}
     */
    public long onIdle(final long timeNs, final long senderLimit, final long senderPosition, final boolean isEos)
    {
        return senderLimit;
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasRequiredReceivers()
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int maxRetransmissionLength(
        final int termOffset,
        final int resendLength,
        final int termBufferLength,
        final int mtuLength)
    {
        return FlowControl.calculateRetransmissionLength(
            resendLength, termBufferLength, termOffset, retransmitReceiverWindowMultiple);
    }
}

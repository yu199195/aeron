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

import java.net.InetSocketAddress;

/**
 * 【源码解析】CongestionControl —— 接收端拥塞控制策略接口。
 * <p>
 * 职责：在接收端动态计算 receiverWindowLength（接收窗口大小），
 * 该值通过 Status Message（SM）反馈给发送端，从而控制发送速率。
 * <p>
 * 工作原理：
 * 1. 接收端的 PublicationImage.trackRebuild() 被 Conductor 周期性调用
 * 2. 调用 CongestionControl.onTrackRebuild() → 返回 receiverWindowLength
 * 3. receiverWindowLength 写入下一个 SM 帧的 receiverWindowLength 字段
 * 4. 发送端 FlowControl.onStatusMessage() 计算 senderLimit = consumptionPos + receiverWindowLength
 * 5. 发送端 senderPosition < senderLimit 时才能发数据
 * <p>
 * 这形成了接收端驱动的端到端拥塞控制闭环。
 * <p>
 * 内置实现：
 * - StaticWindowCongestionControl：固定窗口（默认），不动态调整
 * - CubicCongestionControl（aeron-driver 额外模块）：类 TCP CUBIC 算法，根据 RTT 和丢包动态调整
 * <p>
 * 可插拔：通过 MediaDriver.Context.congestionControlSupplier() 配置。
 */
public interface CongestionControl extends AutoCloseable
{
    /**
     * Bit flag for if a status message should be forced out.
     */
    int FORCE_STATUS_MESSAGE_BIT = 0x1;

    /**
     * Pack values into a long, so they can be returned on the stack without allocation.
     *
     * @param receiverWindowLength to go in the lower bits.
     * @param forceStatusMessage   to go in the higher bits.
     * @return the packed value.
     */
    static long packOutcome(final int receiverWindowLength, final boolean forceStatusMessage)
    {
        final int flags = forceStatusMessage ? FORCE_STATUS_MESSAGE_BIT : 0x0;

        return ((long)flags << 32) | receiverWindowLength;
    }

    /**
     * Extract the receiver window length from the packed value.
     *
     * @param outcome as the packed value.
     * @return the receiver window length.
     */
    static int receiverWindowLength(final long outcome)
    {
        return (int)(outcome & 0xFFFFFFFFL);
    }

    /**
     * Extract the boolean value for if a status message should be forced from the packed value.
     *
     * @param outcome which is packed containing the force status message flag.
     * @return true if the force status message bit has been set.
     */
    static boolean shouldForceStatusMessage(final long outcome)
    {
        return ((int)(outcome >>> 32) & FORCE_STATUS_MESSAGE_BIT) == FORCE_STATUS_MESSAGE_BIT;
    }

    /**
     * Threshold increment in a window after which a status message should be scheduled.
     *
     * @param windowLength to calculate the threshold from.
     * @return the threshold in the window after which a status message should be scheduled.
     */
    static int threshold(final int windowLength)
    {
        return windowLength >> 2;
    }

    /**
     * Polled by {@link Receiver} to determine when to initiate an RTT measurement to a Sender.
     *
     * @param nowNs in nanoseconds
     * @return true for should measure RTT now or false for no measurement
     */
    boolean shouldMeasureRtt(long nowNs);

    /**
     * Called by {@link Receiver} to record that a measurement request has been sent.
     *
     * @param nowNs in nanoseconds.
     */
    void onRttMeasurementSent(long nowNs);

    /**
     * Called by {@link Receiver} on reception of an RTT Measurement.
     *
     * @param nowNs      in nanoseconds
     * @param rttNs      to the Sender in nanoseconds
     * @param srcAddress of the Sender
     */
    void onRttMeasurement(long nowNs, long rttNs, InetSocketAddress srcAddress);

    /**
     * 【核心方法】由 Conductor 在 PublicationImage.trackRebuild() 中调用，
     * 根据当前接收状态计算新的 receiverWindowLength。
     * <p>
     * 拥塞控制算法的输入：
     * - newConsumptionPosition：订阅者已消费到的位置
     * - lastSmPosition：上次 SM 发送时的位置
     * - hwmPosition：接收到的最高水位线
     * - startingRebuildPosition / endingRebuildPosition：本轮重建进度
     * - lossOccurred：本轮是否检测到丢包
     * <p>
     * 返回值打包：packOutcome(receiverWindowLength, forceStatusMessage)
     * - receiverWindowLength：放入 SM 反馈给发送端
     * - forceStatusMessage：true 则立即发 SM（例如窗口大小突变时）
     *
     * @return 打包的拥塞控制结果
     */
    long onTrackRebuild(
        long nowNs,
        long newConsumptionPosition,
        long lastSmPosition,
        long hwmPosition,
        long startingRebuildPosition,
        long endingRebuildPosition,
        boolean lossOccurred);

    /**
     * Called by {@link DriverConductor} to initialise window length for a new {@link PublicationImage}.
     *
     * @return initial window length for flow and congestion control.
     */
    int initialWindowLength();

    /**
     * Called by {@link DriverConductor} limit the window length for a new {@link PublicationImage}.
     *
     * @return maximum window length for flow and congestion control.
     */
    int maxWindowLength();

    /**
     * {@inheritDoc}
     */
    void close();
}

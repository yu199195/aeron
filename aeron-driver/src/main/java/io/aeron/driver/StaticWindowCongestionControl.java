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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;

/**
 * 【源码解析】StaticWindowCongestionControl —— 固定窗口拥塞控制（Aeron 默认策略）。
 * <p>
 * 算法极简：在初始化时计算一个固定的 receiverWindowLength，此后永远返回该值。
 * windowLength = min(initialWindowLength, termLength / 2)
 * <p>
 * 特点：
 * - 不根据 RTT、丢包率等网络状态动态调整窗口
 * - 不测量 RTT（shouldMeasureRtt() 返回 false）
 * - 适用于低延迟、网络条件可控的场景（如数据中心内部）
 * <p>
 * 对比 CubicCongestionControl（可选策略）：
 * - Cubic 会根据 RTT 和丢包动态调整窗口（类 TCP CUBIC）
 * - 适用于广域网或网络条件不可控的场景
 */
public class StaticWindowCongestionControl implements CongestionControl
{
    /**
     * URI param value to identify this {@link CongestionControl} strategy.
     */
    public static final String CC_PARAM_VALUE = "static";

    private final long ccOutcome;

    /**
     * Construct a new {@link CongestionControl} instance for a received stream image using a static window algorithm.
     *
     * @param registrationId  for the publication image.
     * @param udpChannel      for the publication image.
     * @param streamId        for the publication image.
     * @param sessionId       for the publication image.
     * @param termLength      for the publication image.
     * @param senderMtuLength for the publication image.
     * @param controlAddress  for the publication image.
     * @param sourceAddress   for the publication image.
     * @param nanoClock       for the precise timing.
     * @param context         for configuration options applied in the driver.
     * @param countersManager for the driver.
     */
    public StaticWindowCongestionControl(
        final long registrationId,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final int termLength,
        final int senderMtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final NanoClock nanoClock,
        final MediaDriver.Context context,
        final CountersManager countersManager)
    {
        final int initialWindowLength = udpChannel.receiverWindowLength() != 0 ?
            udpChannel.receiverWindowLength() : context.initialWindowLength();

        ccOutcome = CongestionControl.packOutcome(
            Configuration.receiverWindowLength(termLength, initialWindowLength), false);
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
    public boolean shouldMeasureRtt(final long nowNs)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void onRttMeasurementSent(final long nowNs)
    {
    }

    /**
     * {@inheritDoc}
     */
    public void onRttMeasurement(final long nowNs, final long rttNs, final InetSocketAddress srcAddress)
    {
    }

    /**
     * {@inheritDoc}
     */
    public long onTrackRebuild(
        final long nowNs,
        final long newConsumptionPosition,
        final long lastSmPosition,
        final long hwmPosition,
        final long startingRebuildPosition,
        final long endingRebuildPosition,
        final boolean lossOccurred)
    {
        return ccOutcome;
    }

    /**
     * {@inheritDoc}
     */
    public int initialWindowLength()
    {
        return CongestionControl.receiverWindowLength(ccOutcome);
    }

    /**
     * {@inheritDoc}
     */
    public int maxWindowLength()
    {
        return CongestionControl.receiverWindowLength(ccOutcome);
    }
}

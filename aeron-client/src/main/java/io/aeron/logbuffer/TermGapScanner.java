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
package io.aeron.logbuffer;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthVolatile;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;

/**
 * 【源码解析】TermGapScanner —— 接收端丢包检测的底层扫描器。
 * <p>
 * 职责：在接收端的 Term Buffer 中，从 rebuildPosition 到 hwmPosition（高水位线）之间
 * 扫描"空洞"——即 UDP 丢包导致的帧缺失。
 * <p>
 * 工作原理：
 * 1. 从 termOffset 开始逐帧读取 frameLengthVolatile()
 * 2. 正值 → 帧已到达，继续向前
 * 3. ≤ 0 → 发现空洞起点！接下来确定空洞大小
 * 4. 空洞大小确定后，通过 GapHandler.onGap() 回调报告
 * <p>
 * 空洞检测示意：
 * <pre>
 *   Term Buffer（接收端）：
 *   ┌────────┬────────┬────────┬────────┬────────┬────────┐
 *   │Frame 1 │Frame 2 │ 空洞   │ 空洞   │Frame 5 │Frame 6 │
 *   │ len>0  │ len>0  │ len≤0  │ len≤0  │ len>0  │ len>0  │
 *   └────────┴────────┴────────┴────────┴────────┴────────┘
 *                      ↑ gapBeginOffset          ↑ 空洞结束
 *                      onGap(termId, gapBeginOffset, gapLength)
 * </pre>
 * <p>
 * 该扫描结果会传给 LossDetector，由其决定是否/何时发送 NAK 请求重传。
 */
public class TermGapScanner
{
    /**
     * 空洞回调接口。LossDetector 实现此接口以接收空洞通知。
     */
    @FunctionalInterface
    public interface GapHandler
    {
        /**
         * 在正在重建的 Term Buffer 中检测到空洞。
         *
         * @param termId 当前扫描的 term ID。
         * @param offset 空洞起始偏移。
         * @param length 空洞的字节长度。
         */
        void onGap(int termId, int offset, int length);
    }

    /**
     * 【核心扫描方法】从 termOffset 到 limitOffset 扫描空洞。
     * <p>
     * 算法分两步：
     * 第一步（跳过已到达帧）：逐帧读 frameLengthVolatile，正值则 offset 前进，≤0 则停止
     * 第二步（确定空洞大小）：以 HEADER_LENGTH(32B) 为步长向前探测，找到下一个有效帧
     * <p>
     * 为什么按 HEADER_LENGTH 步进？因为所有帧都按 FRAME_ALIGNMENT(32B) 对齐，
     * 空洞中的每个"帧槽位"至少有 32 字节，从帧头位置读 frameLength 即可判断是否有效。
     *
     * @param termBuffer  接收端的 term buffer
     * @param termId      当前 term 的 ID（用于 NAK 帧的 termId 字段）
     * @param termOffset  扫描起始偏移（= rebuildPosition 的 term 内偏移）
     * @param limitOffset 扫描结束偏移（= hwmPosition 的 term 内偏移）
     * @param handler     空洞回调（通常是 LossDetector）
     * @return 最后一个连续帧的结束偏移（即空洞起始位置，或 limitOffset 表示无空洞）
     */
    public static int scanForGap(
        final UnsafeBuffer termBuffer,
        final int termId,
        final int termOffset,
        final int limitOffset,
        final GapHandler handler)
    {
        // 第一步：跳过所有已到达的连续帧
        int offset = termOffset;
        do
        {
            final int frameLength = frameLengthVolatile(termBuffer, offset);
            if (frameLength <= 0)
            {
                break; // 发现空洞起点：该位置的帧未到达
            }

            offset += align(frameLength, FRAME_ALIGNMENT);
        }
        while (offset < limitOffset);

        // 第二步：如果在 limitOffset 之前发现空洞，确定空洞大小
        final int gapBeginOffset = offset;
        if (offset < limitOffset)
        {
            // 从空洞起点向前探测，以 HEADER_LENGTH(32B) 为步长
            offset += HEADER_LENGTH;
            while (offset < limitOffset)
            {
                if (0 != frameLengthVolatile(termBuffer, offset))
                {
                    break; // 找到下一个有效帧 → 空洞结束
                }
                offset += HEADER_LENGTH;
            }

            final int gapLength = offset - gapBeginOffset;
            handler.onGap(termId, gapBeginOffset, gapLength); // 报告空洞：将触发 NAK 流程
        }

        return gapBeginOffset;
    }
}

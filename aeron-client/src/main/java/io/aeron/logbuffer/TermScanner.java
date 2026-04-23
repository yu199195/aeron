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
import static io.aeron.logbuffer.FrameDescriptor.isPaddingFrame;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;

/**
 * 【源码解析】TermScanner —— Sender 线程用来扫描 Term Buffer 中已完成帧的核心工具。
 * <p>
 * 工作原理：从指定 offset 开始，通过 frameLengthVolatile() 逐帧读取帧长度：
 * - 正值 → 帧已完成（应用线程已调用 frameLengthOrdered 发布），可以发送
 * - ≤ 0 → 帧尚未完成（仍处于两阶段提交的"负长度占位"阶段），停止扫描
 * <p>
 * 多个小帧可以被累加到一个 UDP 包中发送（批处理），上限为 maxLength（通常 = MTU）。
 * 这使得 Aeron 在小消息场景下能高效利用 MTU。
 */
public final class TermScanner
{
    /**
     * 【核心扫描方法】从 offset 开始扫描 Term Buffer，找出可以一次 UDP 发送的连续已完成帧。
     * <p>
     * 扫描逻辑：
     * 1. 逐帧读取 frameLengthVolatile()，正值表示帧已完成
     * 2. 累加已完成帧的对齐长度到 available
     * 3. 遇到以下任一条件停止：
     *    - frameLength ≤ 0（帧未完成或空位）
     *    - 遇到 PADDING 帧（term 末尾填充）
     *    - available 超过 limit（通常为 MTU）
     * 4. 若单帧就超过 limit：返回 -available（负值标识"有数据但单帧太大"）
     *
     * @param termBuffer 要扫描的 term buffer（与应用线程共享的 mmap 内存）
     * @param offset     扫描起始偏移（= senderPosition 对应的 term 内偏移）
     * @param maxLength  最大扫描长度（通常 = min(availableWindow, mtuLength)）
     * @return 打包结果：低 32 位 = available 字节数，高 32 位 = padding 字节数
     */
    public static long scanForAvailability(
        final UnsafeBuffer termBuffer, final int offset, final int maxLength)
    {
        final int limit = Math.min(maxLength, termBuffer.capacity() - offset);
        int available = 0;
        int padding = 0;

        do
        {
            final int termOffset = offset + available;
            final int frameLength = frameLengthVolatile(termBuffer, termOffset); // volatile 读：与 offer 的 release 配对
            if (frameLength <= 0)
            {
                break; // 帧未完成（负长度占位阶段）或空位，停止扫描
            }

            int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT); // 32 字节对齐
            if (isPaddingFrame(termBuffer, termOffset))
            {
                // PADDING 帧：term 末尾的填充，只需发送帧头部分（32B），其余为 padding 值
                padding = alignedFrameLength - HEADER_LENGTH;
                alignedFrameLength = HEADER_LENGTH;
            }

            available += alignedFrameLength;

            if (available > limit)
            {
                // 超过 MTU 限制：
                // - 若只有一帧且超限 → 返回负值（-available），表示有数据但单帧大于 MTU
                // - 若有多帧 → 退回最后一帧，只发前面能放进 MTU 的部分
                available = alignedFrameLength == available ? -available : available - alignedFrameLength;
                padding = 0;
                break;
            }
        }
        while (0 == padding && available < limit); // 未遇到 padding 且未达到 limit 则继续

        return pack(padding, available);
    }

    /**
     * Pack the values for available and padding into a long for returning on the stack.
     *
     * @param padding   value to be packed.
     * @param available value to be packed.
     * @return a long with both ints packed into it.
     */
    public static long pack(final int padding, final int available)
    {
        return ((long)padding << 32) | available;
    }

    /**
     * The number of bytes that are available to be read after a scan.
     *
     * @param result into which the padding value has been packed.
     * @return the count of bytes that are available to be read.
     */
    public static int available(final long result)
    {
        return (int)result;
    }

    /**
     * The count of bytes that should be added for padding to the position on top of what is available.
     *
     * @param result into which the padding value has been packed.
     * @return the count of bytes that should be added for padding to the position on top of what is available.
     */
    public static int padding(final long result)
    {
        return (int)(result >>> 32);
    }
}

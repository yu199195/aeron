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
package io.aeron;

import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.ReadablePosition;

import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.RESERVED_VALUE_OFFSET;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.align;

/**
 * Aeron publisher API for sending messages to subscribers of a given channel and streamId pair. {@link Publication}s
 * are created via the {@link Aeron#addPublication(String, int)} method, and messages are sent via one of the
 * {@link #offer(DirectBuffer)} methods, or a {@link #tryClaim(int, BufferClaim)} and {@link BufferClaim#commit()}
 * method combination.
 * <p>
 * The APIs for tryClaim and offer are non-blocking and thread safe.
 * <p>
 * <b>Note:</b> Instances are threadsafe and can be shared between publishing threads.
 *
 * @see Aeron#addPublication(String, int)
 * @see BufferClaim
 */
public final class ConcurrentPublication extends Publication
{
    ConcurrentPublication(
        final ClientConductor clientConductor,
        final String channel,
        final int streamId,
        final int sessionId,
        final ReadablePosition positionLimit,
        final int channelStatusId,
        final LogBuffers logBuffers,
        final long originalRegistrationId,
        final long registrationId)
    {
        super(
            clientConductor,
            channel,
            streamId,
            sessionId,
            positionLimit,
            channelStatusId,
            logBuffers,
            originalRegistrationId,
            registrationId);
    }

    /**
     * {@inheritDoc}
     */
    public long availableWindow()
    {
        if (isClosed)
        {
            return CLOSED;
        }

        return positionLimit.getVolatile() - position();
    }

    /**
     * 非阻塞写入一条消息到当前 term buffer：先读 positionLimit（流控上限）、当前 term 与 tail，
     * 若 position < limit 则按长度选择整条写入或分片写入，并 CAS 更新 tail；否则返回背压状态。
     * 消息长度 ≤ maxPayloadLength 时单帧写入（appendUnfragmentedMessage），否则多帧（appendFragmentedMessage）。
     *
     * @param buffer                消息所在 buffer。
     * @param offset                消息起始偏移。
     * @param length                消息字节长度。
     * @param reservedValueSupplier 可选的帧 reserved 值提供者。
     * @return 成功为新的 stream position，失败为 NOT_CONNECTED/BACK_PRESSURED/ADMIN_ACTION/CLOSED/MAX_POSITION_EXCEEDED。
     */
    public long offer(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;                                      // 默认返回 CLOSED，表示 Publication 已关闭
        if (!isClosed)                                                  // 检查 Publication 是否已被关闭
        {
            final long limit = positionLimit.getVolatile();             // volatile 读取流控上限：订阅者通过 SM 通告的可接受 position 上限
            final int termCount = activeTermCount(logMetaDataBuffer);   // volatile 读取当前活跃 term 计数（由 rotateLog 递增）
            final int index = indexByTermCount(termCount);              // termCount % 3 → 对应 term buffer 数组下标（0/1/2）
            final UnsafeBuffer termBuffer = termBuffers[index];         // 取出当前活跃的 term buffer
            final int tailCounterOffset =                               // 计算该 partition 的 tail counter 在 metadata 中的偏移
                TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail =                                        // volatile 读取当前 term 的 rawTail（高32位=termId, 低32位=termOffset）
                logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset =                                      // 从 rawTail 低 32 位提取 termOffset，截断不超过 termLength
                termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);                         // 从 rawTail 高 32 位提取 termId

            if (termCount != (termId - initialTermId))                  // 校验 termCount 与 termId 是否一致
            {
                return ADMIN_ACTION;                                    // 不一致说明 term 正在轮转，返回 ADMIN_ACTION 让调用方重试
            }

            final long position = computePosition(                      // 根据 termId + termOffset 计算当前绝对 position（字节数）
                termId, termOffset, positionBitsToShift, initialTermId);

            if (position < limit)                                       // 若当前 position 在流控窗口内，允许写入
            {
                if (length <= maxPayloadLength)                         // 消息长度 ≤ 单帧最大载荷（MTU - 帧头）→ 单帧写入
                {
                    checkPositiveLength(length);                        // 校验 length 不为负
                    newPosition = appendUnfragmentedMessage(            // 单帧写入：原子推进 tail → 写帧头 → 拷贝 payload → 发布帧
                        termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
                }
                else                                                    // 消息长度 > maxPayloadLength → 需要分片写入
                {
                    checkMaxMessageLength(length);                      // 校验不超过 maxMessageLength（termLength/8, 上限 16MB）
                    newPosition = appendFragmentedMessage(              // 分片写入：一次性预留空间 → 循环写多个 fragment
                        termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
                }
            }
            else                                                        // position >= limit：流控窗口已满或无订阅者
            {
                newPosition = backPressureStatus(position, length);     // 判断具体原因：MAX_POSITION_EXCEEDED / BACK_PRESSURED / NOT_CONNECTED
            }
        }

        return newPosition;                                             // 返回新的 position（成功）或负值错误码（失败）
    }


    /**
     * Non-blocking publish of a message composed of two parts, e.g. a header and encapsulated payload.
     *
     * @param bufferOne             containing the first part of the message.
     * @param offsetOne             at which the first part of the message begins.
     * @param lengthOne             of the first part of the message.
     * @param bufferTwo             containing the second part of the message.
     * @param offsetTwo             at which the second part of the message begins.
     * @param lengthTwo             of the second part of the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        long newPosition = CLOSED;
        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            final int length = validateAndComputeLength(lengthOne, lengthTwo);
            if (position < limit)
            {
                if (length <= maxPayloadLength)
                {
                    newPosition = appendUnfragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne,
                        offsetOne,
                        lengthOne,
                        bufferTwo,
                        offsetTwo,
                        lengthTwo,
                        reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    newPosition = appendFragmentedMessage(
                        termBuffer,
                        tailCounterOffset,
                        bufferOne,
                        offsetOne,
                        lengthOne,
                        bufferTwo,
                        offsetTwo,
                        lengthTwo,
                        reservedValueSupplier);
                }
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Non-blocking publish by gathering buffer vectors into a message.
     *
     * @param vectors               which make up the message.
     * @param reservedValueSupplier {@link ReservedValueSupplier} for the frame.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     */
    public long offer(final DirectBufferVector[] vectors, final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = DirectBufferVector.validateAndComputeLength(vectors);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            if (position < limit)
            {
                if (length <= maxPayloadLength)
                {
                    newPosition = appendUnfragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }
                else
                {
                    checkMaxMessageLength(length);
                    newPosition = appendFragmentedMessage(
                        termBuffer, tailCounterOffset, vectors, length, reservedValueSupplier);
                }
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
     * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
     * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress or
     * to reach end-of-stream (EOS).
     * <pre>{@code
     *     final BufferClaim bufferClaim = new BufferClaim(); // Can be stored and reused to avoid allocation
     *
     *     if (publication.tryClaim(messageLength, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *
     *              // Work with buffer directly or wrap with a flyweight
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * }</pre>
     *
     * @param length      of the range to claim, in bytes.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value of {@link #NOT_CONNECTED},
     * {@link #BACK_PRESSURED}, {@link #ADMIN_ACTION}, {@link #CLOSED}, or {@link #MAX_POSITION_EXCEEDED}.
     * @throws IllegalArgumentException if the length is greater than {@link #maxPayloadLength()} within an MTU.
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        checkPayloadLength(length);
        long newPosition = CLOSED;

        if (!isClosed)
        {
            final long limit = positionLimit.getVolatile();
            final int termCount = activeTermCount(logMetaDataBuffer);
            final int index = indexByTermCount(termCount);
            final UnsafeBuffer termBuffer = termBuffers[index];
            final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
            final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);
            final int termOffset = termOffset(rawTail, termBuffer.capacity());
            final int termId = termId(rawTail);

            if (termCount != (termId - initialTermId))
            {
                return ADMIN_ACTION;
            }

            final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);

            if (position < limit)
            {
                newPosition = claim(termBuffer, tailCounterOffset, length, bufferClaim);
            }
            else
            {
                newPosition = backPressureStatus(position, length);
            }
        }

        return newPosition;
    }

    /**
     * 【单帧写入 - 单 buffer】消息长度 ≤ maxPayloadLength 时走此路径，一条消息写成一个完整帧。
     * 核心步骤：
     * 1. getAndAddLong 原子地将 tail 推进 alignedLength，"抢占"一段 term 空间（CAS-free，用 FAA）
     * 2. 若推进后越界 → handleEndOfLog 处理 term 结尾 padding + 轮转
     * 3. 否则：写帧头 → 拷贝 payload → 设置 reservedValue → frameLengthOrdered 让帧对消费者可见
     */
    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;                 // 帧总长 = 消息体长度 + 32字节帧头
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT); // 按 32 字节（FRAME_ALIGNMENT）向上对齐，确保帧边界对齐
        final int termLength = termBuffer.capacity();                   // 当前 term buffer 的容量（即 term 长度）

        final long rawTail =                                            // 原子 FAA（Fetch-And-Add）：将 tail 推进 alignedLength，
            logMetaDataBuffer.getAndAddLong(tailCounterOffset,          //   返回推进前的 rawTail（高32位=termId, 低32位=旧offset）
                alignedLength);                                         //   这一步完成后，当前线程"独占"了 [旧offset, 旧offset+alignedLength) 这段空间
        final int termId = termId(rawTail);                             // 从旧 rawTail 提取 termId
        final int termOffset = termOffset(rawTail, termLength);         // 从旧 rawTail 提取 termOffset（截断不超过 termLength）

        final int resultingOffset = termOffset + alignedLength;         // 推进后的 offset = 旧 offset + 对齐后帧长
        final long position = computePosition(                          // 计算推进后的绝对 position（用于返回给调用方）
            termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)                               // 推进后越过了 term 边界 → 当前 term 空间不够
        {
            return handleEndOfLog(                                      // 处理 term 结尾：写 padding + 轮转到下一个 term
                termBuffer, termLength, termId, termOffset, position);
        }
        else                                                            // 空间足够，开始写帧
        {
            headerWriter.write(termBuffer, termOffset,                  // 步骤1：写 32 字节帧头（version/flags/type/sessionId/streamId/termId/termOffset）
                frameLength, termId);                                   //   注意：此时帧头中的 length 字段为负值（表示帧尚未完成）
            termBuffer.putBytes(                                        // 步骤2：将消息 payload 拷贝到帧头之后
                termOffset + HEADER_LENGTH, buffer, offset, length);

            if (null != reservedValueSupplier)                          // 步骤3（可选）：若提供了 reservedValueSupplier
            {
                final long reservedValue =                              //   调用 supplier 获取 reserved 值
                    reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(                                     //   写入帧头的 reserved 字段（偏移 24 字节处）
                    termOffset + RESERVED_VALUE_OFFSET,
                    reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);    // 步骤4：以 release 语义写入正的 frameLength → 帧对消费者可见（发布屏障）
        }

        return position;                                                // 返回写入后的绝对 position
    }

    /**
     * 【分片写入 - 单 buffer】消息长度 > maxPayloadLength 时走此路径，将消息拆成多个帧（fragment）。
     * 核心步骤：
     * 1. computeFragmentedFrameLength 计算分片后总帧长（含每个 fragment 的帧头开销）
     * 2. getAndAddLong 原子推进 tail，一次性预留所有 fragment 的空间
     * 3. 若越界 → handleEndOfLog
     * 4. 否则循环写每个 fragment：
     *    - 写帧头 → 拷贝该 fragment 的 payload 部分
     *    - 首帧加 BEGIN_FRAG_FLAG，尾帧加 END_FRAG_FLAG（frameFlags）
     *    - frameLengthOrdered 发布每个 fragment 让消费者可见
     */
    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength =                                        // 计算分片后所有 fragment 的总帧长（含每个 fragment 的帧头开销 + 对齐填充）
            computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();                   // 当前 term buffer 容量

        final long rawTail =                                            // 原子 FAA：一次性将 tail 推进 framedLength，预留所有 fragment 的空间
            logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);                             // 从旧 rawTail 提取 termId
        final int termOffset = termOffset(rawTail, termLength);         // 从旧 rawTail 提取 termOffset

        final int resultingOffset = termOffset + framedLength;          // 推进后的 offset
        final long position = computePosition(                          // 计算推进后的绝对 position
            termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)                               // 预留空间越过 term 边界 → term 不够用
        {
            return handleEndOfLog(                                      // 写 padding + 轮转 term
                termBuffer, termLength, termId, termOffset, position);
        }
        else                                                            // 空间足够，开始逐 fragment 写入
        {
            int frameOffset = termOffset;                               // 当前 fragment 在 term 中的写入起始偏移
            byte flags = BEGIN_FRAG_FLAG;                               // 第一个 fragment 标记为 BEGIN（消息首片）
            int remaining = length;                                     // 剩余未写入的消息字节数

            do
            {
                final int bytesToWrite =                                // 本 fragment 要写入的 payload 字节数 = min(剩余, maxPayloadLength)
                    Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;   // 本 fragment 的帧总长 = payload + 32字节帧头
                final int alignedLength =                               // 按 FRAME_ALIGNMENT(32) 对齐后的帧长
                    align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(                                     // 写本 fragment 的 32 字节帧头（length 先为负值占位）
                    termBuffer, frameOffset, frameLength, termId);
                termBuffer.putBytes(                                    // 拷贝本 fragment 的 payload 到帧头之后
                    frameOffset + HEADER_LENGTH,
                    buffer,
                    offset + (length - remaining),                      // 源 buffer 中本 fragment 的起始位置
                    bytesToWrite);

                if (remaining <= maxPayloadLength)                      // 若这是最后一个 fragment
                {
                    flags |= END_FRAG_FLAG;                             // 追加 END 标志（消息尾片）
                }

                frameFlags(termBuffer, frameOffset, flags);             // 将 flags（BEGIN/END/两者都有/都没有）写入帧头 flags 字段

                if (null != reservedValueSupplier)                      // 若提供了 reservedValueSupplier
                {
                    final long reservedValue =                          // 获取 reserved 值
                        reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(                                 // 写入帧头 reserved 字段
                        frameOffset + RESERVED_VALUE_OFFSET,
                        reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(                                     // 以 release 语义写入正的 frameLength → 本 fragment 对消费者可见
                    termBuffer, frameOffset, frameLength);

                flags = 0;                                              // 后续 fragment 既非首片也非尾片（flags=0），直到最后一片才加 END
                frameOffset += alignedLength;                           // 推进到下一个 fragment 的写入位置
                remaining -= bytesToWrite;                              // 减去已写入的字节数
            }
            while (remaining > 0);                                      // 还有剩余 payload 则继续写下一个 fragment
        }

        return position;                                                // 返回写入后的绝对 position
    }

    /**
     * 【单帧写入 - 双 buffer】与单 buffer 版本逻辑相同，只是 payload 由两段 buffer 拼接而成。
     */
    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = lengthOne + lengthTwo + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            termBuffer.putBytes(termOffset + HEADER_LENGTH, bufferOne, offsetOne, lengthOne);
            termBuffer.putBytes(termOffset + HEADER_LENGTH + lengthOne, bufferTwo, offsetTwo, lengthTwo);

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    /**
     * 【分片写入 - 双 buffer】与单 buffer 版本逻辑相同，只是 payload 从两段 buffer 中交替拷贝。
     */
    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBuffer bufferOne,
        final int offsetOne,
        final int lengthOne,
        final DirectBuffer bufferTwo,
        final int offsetTwo,
        final int lengthTwo,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int length = lengthOne + lengthTwo;
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + framedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int positionOne = 0;
            int positionTwo = 0;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(termBuffer, frameOffset, frameLength, termId);

                int bytesWritten = 0;
                int payloadOffset = frameOffset + HEADER_LENGTH;
                do
                {
                    final int remainingOne = lengthOne - positionOne;
                    if (remainingOne > 0)
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, remainingOne);
                        termBuffer.putBytes(payloadOffset, bufferOne, offsetOne + positionOne, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionOne += numBytes;
                    }
                    else
                    {
                        final int numBytes = Math.min(bytesToWrite - bytesWritten, lengthTwo - positionTwo);
                        termBuffer.putBytes(payloadOffset, bufferTwo, offsetTwo + positionTwo, numBytes);

                        bytesWritten += numBytes;
                        payloadOffset += numBytes;
                        positionTwo += numBytes;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return position;
    }

    /**
     * 【单帧写入 - vector 数组】与单 buffer 版本逻辑相同，只是 payload 由多个 DirectBufferVector 拼接而成。
     */
    private long appendUnfragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);

            int offset = termOffset + HEADER_LENGTH;
            for (final DirectBufferVector vector : vectors)
            {
                termBuffer.putBytes(offset, vector.buffer(), vector.offset(), vector.length());
                offset += vector.length();
            }

            if (null != reservedValueSupplier)
            {
                final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
                termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
            }

            frameLengthOrdered(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    /**
     * 【分片写入 - vector 数组】与单 buffer 版本逻辑相同，只是 payload 从多个 DirectBufferVector 中交替拷贝。
     */
    private long appendFragmentedMessage(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final DirectBufferVector[] vectors,
        final int length,
        final ReservedValueSupplier reservedValueSupplier)
    {
        final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + framedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            int frameOffset = termOffset;
            byte flags = BEGIN_FRAG_FLAG;
            int remaining = length;
            int vectorIndex = 0;
            int vectorOffset = 0;

            do
            {
                final int bytesToWrite = Math.min(remaining, maxPayloadLength);
                final int frameLength = bytesToWrite + HEADER_LENGTH;
                final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                headerWriter.write(termBuffer, frameOffset, frameLength, termId);

                int bytesWritten = 0;
                int payloadOffset = frameOffset + HEADER_LENGTH;
                do
                {
                    final DirectBufferVector vector = vectors[vectorIndex];
                    final int vectorRemaining = vector.length() - vectorOffset;
                    final int numBytes = Math.min(bytesToWrite - bytesWritten, vectorRemaining);

                    termBuffer.putBytes(payloadOffset, vector.buffer(), vector.offset() + vectorOffset, numBytes);

                    bytesWritten += numBytes;
                    payloadOffset += numBytes;
                    vectorOffset += numBytes;

                    if (vectorRemaining <= numBytes)
                    {
                        vectorIndex++;
                        vectorOffset = 0;
                    }
                }
                while (bytesWritten < bytesToWrite);

                if (remaining <= maxPayloadLength)
                {
                    flags |= END_FRAG_FLAG;
                }

                frameFlags(termBuffer, frameOffset, flags);

                if (null != reservedValueSupplier)
                {
                    final long reservedValue = reservedValueSupplier.get(termBuffer, frameOffset, frameLength);
                    termBuffer.putLong(frameOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
                }

                frameLengthOrdered(termBuffer, frameOffset, frameLength);

                flags = 0;
                frameOffset += alignedLength;
                remaining -= bytesToWrite;
            }
            while (remaining > 0);
        }

        return position;
    }

    /**
     * 【零拷贝预留空间】在 term buffer 中原子预留一段空间，返回 BufferClaim 供调用方直接写入后 commit。
     * 与 appendUnfragmentedMessage 类似，但不拷贝 payload，由应用自行填充。
     */
    private long claim(
        final UnsafeBuffer termBuffer,
        final int tailCounterOffset,
        final int length,
        final BufferClaim bufferClaim)
    {
        final int frameLength = length + HEADER_LENGTH;
        final int alignedLength = align(frameLength, FRAME_ALIGNMENT);
        final int termLength = termBuffer.capacity();

        final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
        final int termId = termId(rawTail);
        final int termOffset = termOffset(rawTail, termLength);

        final int resultingOffset = termOffset + alignedLength;
        final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
        if (resultingOffset > termLength)
        {
            return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
        }
        else
        {
            headerWriter.write(termBuffer, termOffset, frameLength, termId);
            bufferClaim.wrap(termBuffer, termOffset, frameLength);
        }

        return position;
    }

    /**
     * 处理 term 写满（tail 推进后越过 termLength）的善后逻辑：
     * 1. 若 termOffset < termLength（当前线程是第一个越界的），在剩余空间写入 PADDING 帧填充到 term 结尾，
     *    让消费者能跳过这段空白区域。
     * 2. 若 position 已达到 maxPossiblePosition（整条流用尽），返回 MAX_POSITION_EXCEEDED。
     * 3. 否则调用 rotateLog 将 activeTermCount 推进到下一个 term，并返回 ADMIN_ACTION（调用方需重试 offer）。
     */
    private long handleEndOfLog(
        final UnsafeBuffer termBuffer,
        final int termLength,
        final int termId,
        final int termOffset,
        final long position)
    {
        if (termOffset < termLength)                                    // 若当前线程是第一个使 tail 越过 termLength 的（旧 offset 还在 term 内）
        {
            final int paddingLength = termLength - termOffset;          // padding 长度 = term 剩余空间
            headerWriter.write(                                         // 在剩余空间写一个 padding 帧头
                termBuffer, termOffset, paddingLength, termId);
            frameType(termBuffer, termOffset, PADDING_FRAME_TYPE);      // 将帧类型设为 PADDING（0x0002），消费者据此跳过
            frameLengthOrdered(                                         // 以 release 语义写入 paddingLength → padding 帧对消费者可见
                termBuffer, termOffset, paddingLength);
        }
        // 若 termOffset >= termLength，说明其他线程已写过 padding，当前线程无需重复

        if (position >= maxPossiblePosition)                            // 检查流是否已用尽（position 达到 termLength * 2^31）
        {
            return MAX_POSITION_EXCEEDED;                               // 流耗尽，不可继续，调用方应关闭并重建 Publication
        }

        rotateLog(logMetaDataBuffer,                                    // CAS 推进 activeTermCount 并初始化下一个 term 的 tail
            termId - initialTermId, termId);                            //   参数: termCount = termId - initialTermId, termId

        return ADMIN_ACTION;                                            // 返回 ADMIN_ACTION，提示调用方 term 已轮转，重试 offer 即可
    }
}

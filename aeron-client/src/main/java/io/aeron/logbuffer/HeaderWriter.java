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

import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static java.lang.Integer.reverseBytes;
import static io.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;
import static io.aeron.protocol.HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET;
import static io.aeron.protocol.HeaderFlyweight.VERSION_FIELD_OFFSET;

/**
 * Utility for applying a header to a message in a term buffer.
 * <p>
 * This class is designed to be thread safe to be used across multiple producers and makes the header
 * visible in the correct order for consumers.
 */
public class HeaderWriter
{
    final long versionFlagsType;
    final long sessionId;
    final long streamId;

    HeaderWriter(final long versionFlagsType, final long sessionId, final long streamId)
    {
        this.versionFlagsType = versionFlagsType;
        this.sessionId = sessionId;
        this.streamId = streamId;
    }

    HeaderWriter(final UnsafeBuffer defaultHeader)
    {
        versionFlagsType = ((long)defaultHeader.getInt(VERSION_FIELD_OFFSET)) << 32;
        sessionId = ((long)defaultHeader.getInt(SESSION_ID_FIELD_OFFSET)) << 32;
        streamId = defaultHeader.getInt(STREAM_ID_FIELD_OFFSET) & 0xFFFF_FFFFL;
    }

    /**
     * 【工厂方法：创建平台字节序适配的 HeaderWriter】
     * 根据当前平台字节序选择小端（HeaderWriter）或大端（NativeBigEndianHeaderWriter）实现。
     * Publication 构造时调用一次，后续 offer 每次写帧头都用这个实例。
     *
     * @param defaultHeader for the stream.
     * @return a new {@link HeaderWriter} that is {@link ByteOrder} specific to the platform.
     */
    public static HeaderWriter newInstance(final UnsafeBuffer defaultHeader)
    {
        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN)
        {
            return new HeaderWriter(defaultHeader);
        }
        else
        {
            return new NativeBigEndianHeaderWriter(defaultHeader);
        }
    }

    /**
     * 【写帧头到 term buffer（小端版本）】在 appendUnfragmentedMessage / appendFragmentedMessage 中、
     * 拷贝 payload 之前调用。只需 3 次 putLong 即可填完 32 字节帧头（极致减少指令数）。
     * <p>
     * 写入顺序的设计考量：
     * 1. 第一个 putLongRelease 写入 version+flags+type 和一个 **负的** frameLength（-length）。
     *    负值表示"帧尚未完成"，消费者看到负值会跳过。这保证了帧头可见但帧体尚未就绪时消费者不会误读。
     * 2. storeStoreFence 确保后续写入在第一步之后。
     * 3. 填入 sessionId + termOffset，以及 streamId + termId。
     * 4. 最终由 frameLengthOrdered 将 length 从负值改为正值（release 语义），标志帧完全可读。
     *
     * @param termBuffer to be written to.
     * @param offset     at which the header should be written.
     * @param length     of the fragment including the header.
     * @param termId     of the current term buffer.
     */
    public void write(final UnsafeBuffer termBuffer, final int offset, final int length, final int termId)
    {
        termBuffer.putLongRelease(                                      // 第1次写（release 语义）：帧头前 8 字节
            offset + FRAME_LENGTH_FIELD_OFFSET,                         //   offset 0-3: frameLength（取负值 -length，表示帧尚未完成）
            versionFlagsType | ((-length) & 0xFFFF_FFFFL));             //   offset 4-7: version + flags + type（从 defaultHeader 模板中预填）
        VarHandle.storeStoreFence();                                    // store-store 屏障：确保上面的写入先于下面的写入

        termBuffer.putLong(                                             // 第2次写：帧头 8-15 字节
            offset + TERM_OFFSET_FIELD_OFFSET,                          //   offset 8-11: termOffset（当前帧在 term 中的起始偏移）
            sessionId | offset);                                        //   offset 12-15: sessionId（从 defaultHeader 模板中预填）
        termBuffer.putLong(                                             // 第3次写：帧头 16-23 字节
            offset + STREAM_ID_FIELD_OFFSET,                            //   offset 16-19: streamId（从 defaultHeader 模板中预填）
            (((long)termId) << 32) | streamId);                         //   offset 20-23: termId（当前 term 的标识）
    }
}

final class NativeBigEndianHeaderWriter extends HeaderWriter
{
    NativeBigEndianHeaderWriter(final UnsafeBuffer defaultHeader)
    {
        super(
            defaultHeader.getInt(VERSION_FIELD_OFFSET) & 0xFFFF_FFFFL,
            defaultHeader.getInt(SESSION_ID_FIELD_OFFSET) & 0xFFFF_FFFFL,
            ((long)defaultHeader.getInt(STREAM_ID_FIELD_OFFSET)) << 32);
    }

    public void write(final UnsafeBuffer termBuffer, final int offset, final int length, final int termId)
    {
        termBuffer.putLongRelease(
            offset + FRAME_LENGTH_FIELD_OFFSET, ((((long)reverseBytes(-length))) << 32) | versionFlagsType);
        VarHandle.storeStoreFence();

        termBuffer.putLong(offset + TERM_OFFSET_FIELD_OFFSET, ((((long)reverseBytes(offset))) << 32) | sessionId);
        termBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, streamId | (reverseBytes(termId) & 0xFFFF_FFFFL));
    }
}

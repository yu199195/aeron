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

import io.aeron.protocol.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Description of the structure for message framing in a log buffer.
 * <p>
 * All messages are logged in frames that have a minimum header layout as follows plus a reserve then
 * the encoded message follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |R|                       Frame Length                          |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |  Version      |B|E| Flags     |             Type              |
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 *  |R|                       Term Offset                           |
 *  +-+-------------------------------------------------------------+
 *  |                      Additional Fields                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                        Encoded Message                       ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 * <p>
 * The (B)egin and (E)nd flags are used for message fragmentation. R is for reserved bit.
 * Both (B)egin and (E)nd flags are set for a message that does not span frames.
 */
public class FrameDescriptor
{
    /**
     * Set a pragmatic maximum message length regardless of term length to encourage better design.
     * Messages larger than half the cache size should be broken up into chunks and streamed.
     */
    public static final int MAX_MESSAGE_LENGTH = 16 * 1024 * 1024;

    /**
     * Alignment as a multiple of bytes for each frame. The length field will store the unaligned length in bytes.
     */
    public static final int FRAME_ALIGNMENT = 32;

    /**
     * Beginning fragment of a frame.
     */
    public static final byte BEGIN_FRAG_FLAG = (byte)0b1000_0000;

    /**
     * End fragment of a frame.
     */
    public static final byte END_FRAG_FLAG = (byte)0b0100_0000;

    /**
     * Unfragmented frame.
     */
    public static final byte UNFRAGMENTED = BEGIN_FRAG_FLAG | END_FRAG_FLAG;

    /**
     * Offset within a frame at which the version field begins.
     */
    public static final int VERSION_OFFSET = DataHeaderFlyweight.VERSION_FIELD_OFFSET;

    /**
     * Offset within a frame at which the flags field begins.
     */
    public static final int FLAGS_OFFSET = DataHeaderFlyweight.FLAGS_FIELD_OFFSET;

    /**
     * Offset within a frame at which the type field begins.
     */
    public static final int TYPE_OFFSET = DataHeaderFlyweight.TYPE_FIELD_OFFSET;

    /**
     * Offset within a frame at which the term offset field begins.
     */
    public static final int TERM_OFFSET = DataHeaderFlyweight.TERM_OFFSET_FIELD_OFFSET;

    /**
     * Offset within a frame at which the term id field begins.
     */
    public static final int TERM_ID_OFFSET = DataHeaderFlyweight.TERM_ID_FIELD_OFFSET;

    /**
     * Offset within a frame at which the session id field begins.
     */
    public static final int SESSION_ID_OFFSET = DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;

    /**
     * Padding frame type to indicate the message should be ignored.
     */
    public static final int PADDING_FRAME_TYPE = HeaderFlyweight.HDR_TYPE_PAD;

    /**
     * 【计算最大消息长度】单条消息允许的最大字节数 = min(termLength/8, 16MB)。
     * 超过此长度的消息会被 checkMaxMessageLength 拒绝。
     *
     * @param termLength of the log buffer.
     * @return the maximum supported length for a message.
     */
    public static int computeMaxMessageLength(final int termLength)
    {
        return Math.min(termLength >> 3, MAX_MESSAGE_LENGTH);           // termLength/8 与 16MB 取较小值 → 单条消息的最大字节数
    }

    /**
     * The buffer offset at which the length field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the length field begins.
     */
    public static int lengthOffset(final int termOffset)
    {
        return termOffset;
    }

    /**
     * The buffer offset at which the version field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the version field begins.
     */
    public static int versionOffset(final int termOffset)
    {
        return termOffset + VERSION_OFFSET;
    }

    /**
     * The buffer offset at which the flags field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the flags field begins.
     */
    public static int flagsOffset(final int termOffset)
    {
        return termOffset + FLAGS_OFFSET;
    }

    /**
     * The buffer offset at which the type field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the type field begins.
     */
    public static int typeOffset(final int termOffset)
    {
        return termOffset + TYPE_OFFSET;
    }

    /**
     * The buffer offset at which the term offset field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the term offset field begins.
     */
    public static int termOffsetOffset(final int termOffset)
    {
        return termOffset + TERM_OFFSET;
    }

    /**
     * The buffer offset at which the term id field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the term id field begins.
     */
    public static int termIdOffset(final int termOffset)
    {
        return termOffset + TERM_ID_OFFSET;
    }

    /**
     * The buffer offset at which the session id field begins.
     *
     * @param termOffset at which the frame begins.
     * @return the offset at which the session id field begins.
     */
    public static int sessionIdOffset(final int termOffset)
    {
        return termOffset + SESSION_ID_OFFSET;
    }

    /**
     * Read the type of the frame from header.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value of the frame type header.
     */
    public static int frameVersion(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getByte(versionOffset(termOffset));
    }

    /**
     * Get the flags field for a frame.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value of the flags.
     */
    public static byte frameFlags(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getByte(flagsOffset(termOffset));
    }

    /**
     * Read the type of the frame from header.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value of the frame type header.
     */
    public static int frameType(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getShort(typeOffset(termOffset), LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * Is the frame starting at the termOffset a padding frame at the end of a buffer?
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return true if the frame is a padding frame otherwise false.
     */
    public static boolean isPaddingFrame(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getShort(typeOffset(termOffset)) == PADDING_FRAME_TYPE;
    }

    /**
     * Get the length of a frame from the header.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value for the frame length.
     */
    public static int frameLength(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getInt(termOffset, LITTLE_ENDIAN);
    }

    /**
     * Get the term id of a frame from the header.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value for the term id field.
     */
    public static int frameTermId(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getInt(termIdOffset(termOffset), LITTLE_ENDIAN);
    }

    /**
     * Get the session id of a frame from the header.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value for the session id field.
     */
    public static int frameSessionId(final UnsafeBuffer buffer, final int termOffset)
    {
        return buffer.getInt(sessionIdOffset(termOffset), LITTLE_ENDIAN);
    }

    /**
     * Get the length of a frame from the header as a volatile read.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @return the value for the frame length.
     */
    public static int frameLengthVolatile(final UnsafeBuffer buffer, final int termOffset)
    {
        int frameLength = buffer.getIntVolatile(termOffset);

        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
        {
            frameLength = Integer.reverseBytes(frameLength);
        }

        return frameLength;
    }

    /**
     * 【以 release 语义写入帧长度】这是帧写入的最后一步——将 frameLength 以 putIntRelease（store-release）
     * 写入帧头的第一个 int 字段。消费者通过 volatile 读该字段判断帧是否可见（length > 0 且非负值表示可读）。
     * 此操作相当于一个"发布屏障"：保证此前写入的帧头、payload 等数据对消费者可见。
     *
     * @param buffer      containing the frame.
     * @param termOffset  at which a frame begins.
     * @param frameLength field to be set for the frame.
     */
    public static void frameLengthOrdered(final UnsafeBuffer buffer, final int termOffset, final int frameLength)
    {
        int length = frameLength;                                       // 准备写入的帧长度值
        if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)                   // 若当前平台不是小端字节序
        {
            length = Integer.reverseBytes(frameLength);                 // 翻转字节序（Aeron 协议统一使用小端）
        }

        buffer.putIntRelease(termOffset, length);                       // 以 release 语义写入帧头偏移 0 处（即 frameLength 字段）
                                                                        // 这是帧写入的最后一步：从负值翻为正值，标志帧完全可读（发布屏障）
    }

    /**
     * 【写入帧类型】设置帧头中的 type 字段（如 DATA / PADDING / NAK / SM 等）。
     * handleEndOfLog 中用它将填充帧标记为 PADDING_FRAME_TYPE，让消费者跳过 term 末尾空白区。
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @param type       type value for the frame.
     */
    public static void frameType(final UnsafeBuffer buffer, final int termOffset, final int type)
    {
        buffer.putShort(typeOffset(termOffset),                         // 在帧头的 type 字段偏移处（termOffset + 6）
            (short)type, LITTLE_ENDIAN);                                // 以小端写入帧类型（如 DATA=0x0001, PADDING=0x0002, SM, NAK 等）
    }

    /**
     * 【写入帧标志位】设置帧头中的 flags 字段。分片写入时用于标记 BEGIN_FRAG_FLAG / END_FRAG_FLAG，
     * 消费者据此判断是完整帧还是某条大消息的首片 / 中间片 / 尾片。
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @param flags      value for the frame.
     */
    public static void frameFlags(final UnsafeBuffer buffer, final int termOffset, final byte flags)
    {
        buffer.putByte(flagsOffset(termOffset), flags);                 // 在帧头的 flags 字段偏移处（termOffset + 5）写入标志位
                                                                        // BEGIN_FRAG=0x80（消息首片）, END_FRAG=0x40（消息尾片）
    }

    /**
     * Write the term offset field for a frame.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     */
    public static void frameTermOffset(final UnsafeBuffer buffer, final int termOffset)
    {
        buffer.putInt(termOffsetOffset(termOffset), termOffset, LITTLE_ENDIAN);
    }

    /**
     * Write the term id field for a frame.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @param termId     value for the frame.
     */
    public static void frameTermId(final UnsafeBuffer buffer, final int termOffset, final int termId)
    {
        buffer.putInt(termIdOffset(termOffset), termId, LITTLE_ENDIAN);
    }

    /**
     * Write the session id field for a frame.
     *
     * @param buffer     containing the frame.
     * @param termOffset at which a frame begins.
     * @param sessionId  value for the frame.
     */
    public static void frameSessionId(final UnsafeBuffer buffer, final int termOffset, final int sessionId)
    {
        buffer.putInt(sessionIdOffset(termOffset), sessionId, LITTLE_ENDIAN);
    }
}

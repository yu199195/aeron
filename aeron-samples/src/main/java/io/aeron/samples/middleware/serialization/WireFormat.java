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
package io.aeron.samples.middleware.serialization;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * 中间件线格式（Wire Format）。
 * <p>
 * 定义多类型通道下每条消息的二进制布局，由中间件负责解析，
 * 业务 Codec 只处理 payload 部分。
 * <pre>
 *   [0..3] typeId   (int32) 消息类型 ID
 *   [4..7] length   (int32) payload 字节长度
 *   [8..n] payload  业务序列化后的字节
 * </pre>
 */
public final class WireFormat
{
    /** Header 固定长度：typeId(4) + length(4) = 8 字节 */
    public static final int HEADER_LENGTH = 8;

    private static final int TYPE_ID_OFFSET = 0;
    private static final int LENGTH_OFFSET = 4;

    private WireFormat()
    {
    }

    public static void writeHeader(
        final MutableDirectBuffer buffer,
        final int offset,
        final int typeId,
        final int payloadLength)
    {
        buffer.putInt(offset + TYPE_ID_OFFSET, typeId, LITTLE_ENDIAN);
        buffer.putInt(offset + LENGTH_OFFSET, payloadLength, LITTLE_ENDIAN);
    }

    public static void writeTypeId(final MutableDirectBuffer buffer, final int offset, final int typeId)
    {
        buffer.putInt(offset + TYPE_ID_OFFSET, typeId, LITTLE_ENDIAN);
    }

    public static void writePayloadLength(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        buffer.putInt(offset + LENGTH_OFFSET, length, LITTLE_ENDIAN);
    }

    public static int readTypeId(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + TYPE_ID_OFFSET, LITTLE_ENDIAN);
    }

    public static int readPayloadLength(final DirectBuffer buffer, final int offset)
    {
        return buffer.getInt(offset + LENGTH_OFFSET, LITTLE_ENDIAN);
    }

    public static int readHeader(final DirectBuffer buffer, final int offset, final int[] outTypeIdAndLength)
    {
        outTypeIdAndLength[0] = readTypeId(buffer, offset);
        outTypeIdAndLength[1] = readPayloadLength(buffer, offset);
        return HEADER_LENGTH;
    }
}

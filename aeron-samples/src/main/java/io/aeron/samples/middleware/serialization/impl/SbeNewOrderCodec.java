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
package io.aeron.samples.middleware.serialization.impl;

import io.aeron.samples.middleware.serialization.MessageCodec;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * SBE 格式的 NewOrder 编解码器。
 * <p>
 * 将业务实体 {@link NewOrder} 与 SBE 二进制格式互转。
 * 布局与 trading-codecs.xml 中 NewOrder 一致，可与 SbeTool 生成的代码兼容。
 */
public final class SbeNewOrderCodec implements MessageCodec<NewOrder>
{
    public static final int TYPE_ID = 1;

    private static final int HEADER_SIZE = 8;
    private static final int BLOCK_LENGTH = 30;

    private static final int ORDER_ID_OFFSET = 0;
    private static final int ACCOUNT_ID_OFFSET = 8;
    private static final int PRICE_OFFSET = 16;
    private static final int QUANTITY_OFFSET = 24;
    private static final int SIDE_OFFSET = 28;
    private static final int ORDER_TYPE_OFFSET = 29;

    /** 保守估计：header + 固定 30 + symbol 最长 64 字节 */
    private static final int MAX_ENCODED_LENGTH = HEADER_SIZE + BLOCK_LENGTH + 4 + 64;

    @Override
    public int typeId()
    {
        return TYPE_ID;
    }

    @Override
    public Class<NewOrder> messageType()
    {
        return NewOrder.class;
    }

    @Override
    public int encode(final NewOrder message, final MutableDirectBuffer buffer, final int offset)
    {
        // SBE MessageHeader
        buffer.putShort(offset + 0, (short)BLOCK_LENGTH, LITTLE_ENDIAN);
        buffer.putShort(offset + 2, (short)TYPE_ID, LITTLE_ENDIAN);
        buffer.putShort(offset + 4, (short)200, LITTLE_ENDIAN);  // schemaId
        buffer.putShort(offset + 6, (short)1, LITTLE_ENDIAN);    // version

        final int bodyOffset = offset + HEADER_SIZE;
        buffer.putLong(bodyOffset + ORDER_ID_OFFSET, message.getOrderId(), LITTLE_ENDIAN);
        buffer.putLong(bodyOffset + ACCOUNT_ID_OFFSET, message.getAccountId(), LITTLE_ENDIAN);
        buffer.putLong(bodyOffset + PRICE_OFFSET, message.getPrice(), LITTLE_ENDIAN);
        buffer.putInt(bodyOffset + QUANTITY_OFFSET, message.getQuantity(), LITTLE_ENDIAN);
        buffer.putByte(bodyOffset + SIDE_OFFSET, (byte)message.getSide().getCode());
        buffer.putByte(bodyOffset + ORDER_TYPE_OFFSET, (byte)message.getOrderType().getCode());

        final byte[] symbolBytes = message.getSymbol().getBytes();
        final int varOffset = bodyOffset + BLOCK_LENGTH;
        buffer.putInt(varOffset, symbolBytes.length, LITTLE_ENDIAN);
        buffer.putBytes(varOffset + 4, symbolBytes);

        return HEADER_SIZE + BLOCK_LENGTH + 4 + symbolBytes.length;
    }

    @Override
    public NewOrder decode(final DirectBuffer buffer, final int offset, final int length)
    {
        final int blockLength = buffer.getShort(offset + 0, LITTLE_ENDIAN) & 0xFFFF;
        final int bodyOffset = offset + HEADER_SIZE;

        final NewOrder order = new NewOrder();
        order.setOrderId(buffer.getLong(bodyOffset + ORDER_ID_OFFSET, LITTLE_ENDIAN));
        order.setAccountId(buffer.getLong(bodyOffset + ACCOUNT_ID_OFFSET, LITTLE_ENDIAN));
        order.setPrice(buffer.getLong(bodyOffset + PRICE_OFFSET, LITTLE_ENDIAN));
        order.setQuantity(buffer.getInt(bodyOffset + QUANTITY_OFFSET, LITTLE_ENDIAN));
        order.setSide(NewOrder.Side.fromCode(buffer.getByte(bodyOffset + SIDE_OFFSET) & 0xFF));
        order.setOrderType(NewOrder.OrderType.fromCode(buffer.getByte(bodyOffset + ORDER_TYPE_OFFSET) & 0xFF));

        final int varOffset = bodyOffset + blockLength;
        final int symbolLen = buffer.getInt(varOffset, LITTLE_ENDIAN);
        final byte[] symbolBytes = new byte[symbolLen];
        buffer.getBytes(varOffset + 4, symbolBytes);
        order.setSymbol(new String(symbolBytes));

        return order;
    }

    @Override
    public int maxEncodedLength()
    {
        return MAX_ENCODED_LENGTH;
    }
}

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

/**
 * JSON 格式的 NewOrder 编解码器示例。
 * <p>
 * 使用 {@link JsonMessageCodec}，业务提供 toJson/fromJson。
 * 本示例用简单拼接格式模拟（生产环境可接入 Jackson）：
 * <pre>
 *   orderId|accountId|price|quantity|side|orderType|symbol
 * </pre>
 */
public final class JsonNewOrderCodec implements MessageCodec<NewOrder>
{
    private static final int TYPE_ID = 1;
    private static final int MAX_LENGTH = 256;

    private final MessageCodec<NewOrder> delegate;

    public JsonNewOrderCodec()
    {
        delegate = new JsonMessageCodec<>(
            TYPE_ID,
            NewOrder.class,
            order -> order.getOrderId() + "|" + order.getAccountId() + "|" +
                order.getPrice() + "|" + order.getQuantity() + "|" +
                order.getSide().name() + "|" + order.getOrderType().name() + "|" +
                order.getSymbol(),
            s ->
            {
                final String[] parts = s.split("\\|", 7);
                final NewOrder order = new NewOrder();
                order.setOrderId(Long.parseLong(parts[0]));
                order.setAccountId(Long.parseLong(parts[1]));
                order.setPrice(Long.parseLong(parts[2]));
                order.setQuantity(Integer.parseInt(parts[3]));
                order.setSide(NewOrder.Side.valueOf(parts[4]));
                order.setOrderType(NewOrder.OrderType.valueOf(parts[5]));
                order.setSymbol(parts.length > 6 ? parts[6] : "");
                return order;
            },
            MAX_LENGTH);
    }

    @Override
    public int typeId()
    {
        return delegate.typeId();
    }

    @Override
    public Class<NewOrder> messageType()
    {
        return delegate.messageType();
    }

    @Override
    public int encode(final NewOrder message, final org.agrona.MutableDirectBuffer buffer, final int offset)
    {
        return delegate.encode(message, buffer, offset);
    }

    @Override
    public NewOrder decode(final org.agrona.DirectBuffer buffer, final int offset, final int length)
    {
        return delegate.decode(buffer, offset, length);
    }

    @Override
    public int maxEncodedLength()
    {
        return delegate.maxEncodedLength();
    }
}

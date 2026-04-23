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
package io.aeron.samples.cluster.messages;

/**
 * Order 消息 POJO。
 */
public class Order
{
    public final long orderId;
    public final String symbol;
    public final double price;
    public final int quantity;
    public final char side;
    public final long timestamp;

    public Order(
        final long orderId,
        final String symbol,
        final double price,
        final int quantity,
        final char side,
        final long timestamp)
    {
        this.orderId = orderId;
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.side = side;
        this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
        return String.format("Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}",
            orderId, symbol, price, quantity, side);
    }
}

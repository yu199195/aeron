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

/**
 * 业务实体示例：新订单。
 * <p>
 * 业务定义自己的 POJO，通过不同的 Codec（SBE/JSON）序列化。
 */
public final class NewOrder
{
    private long orderId;
    private long accountId;
    private long price;
    private int quantity;
    private Side side;
    private OrderType orderType;
    private String symbol;

    public NewOrder()
    {
    }

    public NewOrder(
        final long orderId,
        final long accountId,
        final long price,
        final int quantity,
        final Side side,
        final OrderType orderType,
        final String symbol)
    {
        this.orderId = orderId;
        this.accountId = accountId;
        this.price = price;
        this.quantity = quantity;
        this.side = side;
        this.orderType = orderType;
        this.symbol = symbol;
    }

    public long getOrderId()
    {
        return orderId;
    }

    public void setOrderId(final long orderId)
    {
        this.orderId = orderId;
    }

    public long getAccountId()
    {
        return accountId;
    }

    public void setAccountId(final long accountId)
    {
        this.accountId = accountId;
    }

    public long getPrice()
    {
        return price;
    }

    public void setPrice(final long price)
    {
        this.price = price;
    }

    public int getQuantity()
    {
        return quantity;
    }

    public void setQuantity(final int quantity)
    {
        this.quantity = quantity;
    }

    public Side getSide()
    {
        return side;
    }

    public void setSide(final Side side)
    {
        this.side = side;
    }

    public OrderType getOrderType()
    {
        return orderType;
    }

    public void setOrderType(final OrderType orderType)
    {
        this.orderType = orderType;
    }

    public String getSymbol()
    {
        return symbol;
    }

    public void setSymbol(final String symbol)
    {
        this.symbol = symbol;
    }

    public enum Side
    {
        BUY(0),
        SELL(1);

        private final int code;

        Side(final int code)
        {
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }

        public static Side fromCode(final int code)
        {
            return code == 0 ? BUY : SELL;
        }
    }

    public enum OrderType
    {
        MARKET(0),
        LIMIT(1);

        private final int code;

        OrderType(final int code)
        {
            this.code = code;
        }

        public int getCode()
        {
            return code;
        }

        public static OrderType fromCode(final int code)
        {
            return code == 0 ? MARKET : LIMIT;
        }
    }
}

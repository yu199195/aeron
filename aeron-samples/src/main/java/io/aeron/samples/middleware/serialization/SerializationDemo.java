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

import io.aeron.samples.middleware.serialization.impl.NewOrder;
import io.aeron.samples.middleware.serialization.impl.SbeNewOrderCodec;

/**
 * 序列化抽象使用示例。
 * <p>
 * 业务只需：注册 Codec、send 对象、subscribe 接收。无需 buffer、offset、encode/decode。
 */
public final class SerializationDemo
{
    public static void main(final String[] args)
    {
        // 1. 创建传输层（Demo 用内存队列；生产环境可对接 Aeron/Netty）
        final InMemoryTransport transport = new InMemoryTransport();

        // 2. 创建客户端
        final MessageClient client = MessageClient.create(transport);

        // 3. 注册 Codec（业务选择 SBE 或 JSON）
        client.register(new SbeNewOrderCodec());

        // 4. 订阅：回调中收到的是已解码的对象
        client.subscribe(msg ->
        {
            if (msg instanceof NewOrder order)
            {
                System.out.println("收到: " + order.getSymbol() + " " +
                    order.getQuantity() + "@$" + (order.getPrice() / 100.0));
            }
        });

        // 5. 发送：直接传业务对象
        client.send(new NewOrder(
            1001L, 88888L, 15000L, 100,
            NewOrder.Side.BUY, NewOrder.OrderType.LIMIT, "AAPL"));

        // 6. 轮询接收（Demo 用内存传输，需主动 poll）
        transport.poll();

        System.out.println("完成。业务无需关心 encode/decode、buffer、WireFormat。");
    }
}

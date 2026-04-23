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

/**
 * 消息客户端（业务入口）。
 * <p>
 * 业务只需：注册 Codec、发送对象、订阅接收。无需接触 buffer、offset、编解码等底层细节。
 * <pre>
 * MessageClient client = MessageClient.create(transport);
 * client.register(new SbeNewOrderCodec());
 * client.subscribe(msg -> { ... });  // msg 已是解码后的对象
 * client.send(new NewOrder(...));    // 直接传对象
 * </pre>
 */
public interface MessageClient
{
    /**
     * 注册消息编解码器。
     *
     * @param codec 编解码器
     */
    void register(MessageCodec<?> codec);

    /**
     * 发送消息。
     *
     * @param message 业务对象，中间件内部完成编码与发送
     */
    void send(Object message);

    /**
     * 订阅消息。收到数据时，回调中传入已解码的业务对象。
     *
     * @param handler 接收回调
     */
    void subscribe(MessageHandler handler);

    /**
     * 创建客户端。
     *
     * @param transport 传输层（Aeron、Netty 等）
     * @return 客户端实例
     */
    static MessageClient create(final MessageTransport transport)
    {
        return new MessageClientImpl(transport);
    }
}

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

/**
 * 传输层抽象。
 * <p>
 * 中间件内部使用，业务不感知。可对接 Aeron、Netty、内存队列等。
 */
public interface MessageTransport
{
    /**
     * 发送字节数据。
     *
     * @param buffer 数据 buffer
     * @param offset 起始偏移
     * @param length 字节长度
     */
    void send(DirectBuffer buffer, int offset, int length);

    /**
     * 设置接收回调。收到数据时，transport 将调用此回调。
     *
     * @param callback 回调，参数为 (buffer, offset, length)
     */
    void setReceiveCallback(ReceiveCallback callback);

    /**
     * 接收回调。
     */
    @FunctionalInterface
    interface ReceiveCallback
    {
        void onReceive(DirectBuffer buffer, int offset, int length);
    }
}

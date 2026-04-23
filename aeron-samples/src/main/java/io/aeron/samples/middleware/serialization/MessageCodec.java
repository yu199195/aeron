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

/**
 * 带类型标识的消息编解码器。
 * <p>
 * 在 {@link MessageSerializer} 基础上增加 typeId，用于多类型通道的消息路由：
 * 中间件在 wire 层写入/解析 typeId，根据 typeId 查找对应 Codec 完成编解码。
 * <p>
 * typeId 可与 SBE templateId、Protobuf message type 等对齐，由业务约定。
 *
 * @param <T> 消息类型
 */
public interface MessageCodec<T> extends MessageSerializer<T>
{
    /**
     * 消息类型 ID。
     * <p>
     * 用于 wire 层标识消息类型，中间件据此分发到正确的 Codec。
     * 建议与 SBE templateId、Protobuf 等保持一致。
     *
     * @return 类型 ID（建议 1..65535，0 保留）
     */
    int typeId();

    /**
     * 对应的 Java 消息类型。
     * <p>
     * 用于注册表按消息对象查找 Codec，以及反射/泛型场景。
     *
     * @return 消息 Class
     */
    Class<T> messageType();
}

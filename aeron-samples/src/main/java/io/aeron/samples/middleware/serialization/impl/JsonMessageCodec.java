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

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * 基于「序列化函数」的 JSON 编解码器。
 * <p>
 * 业务提供 T → String 和 String → T 的转换逻辑，可接入 Jackson、Gson 等。
 * 本实现不依赖具体 JSON 库，由业务自行实现。
 *
 * @param <T> 消息类型
 */
public final class JsonMessageCodec<T> implements MessageCodec<T>
{
    private final int typeId;
    private final Class<T> messageType;
    private final Function<T, String> toJson;
    private final Function<String, T> fromJson;
    private final int maxEncodedLength;

    /**
     * @param typeId         消息类型 ID
     * @param messageType    消息 Class
     * @param toJson         T → JSON String，业务可用 ObjectMapper.writeValueAsString
     * @param fromJson       JSON String → T，业务可用 ObjectMapper.readValue
     * @param maxEncodedLength 最大编码长度（JSON 通常较长，需预估）
     */
    public JsonMessageCodec(
        final int typeId,
        final Class<T> messageType,
        final Function<T, String> toJson,
        final Function<String, T> fromJson,
        final int maxEncodedLength)
    {
        this.typeId = typeId;
        this.messageType = messageType;
        this.toJson = toJson;
        this.fromJson = fromJson;
        this.maxEncodedLength = maxEncodedLength;
    }

    @Override
    public int typeId()
    {
        return typeId;
    }

    @Override
    public Class<T> messageType()
    {
        return messageType;
    }

    @Override
    public int encode(final T message, final MutableDirectBuffer buffer, final int offset)
    {
        final byte[] bytes = toJson.apply(message).getBytes(StandardCharsets.UTF_8);
        buffer.putBytes(offset, bytes);
        return bytes.length;
    }

    @Override
    public T decode(final DirectBuffer buffer, final int offset, final int length)
    {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return fromJson.apply(new String(bytes, StandardCharsets.UTF_8));
    }

    @Override
    public int maxEncodedLength()
    {
        return maxEncodedLength;
    }
}

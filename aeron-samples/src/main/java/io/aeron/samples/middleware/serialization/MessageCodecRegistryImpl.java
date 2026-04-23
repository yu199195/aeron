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

import java.util.HashMap;
import java.util.Map;

/**
 * {@link MessageCodecRegistry} 的默认实现。
 */
public final class MessageCodecRegistryImpl implements MessageCodecRegistry
{
    private final Map<Integer, MessageCodec<?>> byTypeId = new HashMap<>();
    private final Map<Class<?>, MessageCodec<?>> byMessageType = new HashMap<>();

    @Override
    public void register(final MessageCodec<?> codec)
    {
        final int typeId = codec.typeId();
        final Class<?> msgType = codec.messageType();

        if (byTypeId.containsKey(typeId))
        {
            throw new IllegalArgumentException("typeId already registered: " + typeId);
        }
        if (byMessageType.containsKey(msgType))
        {
            throw new IllegalArgumentException("messageType already registered: " + msgType.getName());
        }

        byTypeId.put(typeId, codec);
        byMessageType.put(msgType, codec);
    }

    @Override
    public MessageCodec<?> getByTypeId(final int typeId)
    {
        return byTypeId.get(typeId);
    }

    @Override
    public MessageCodec<?> getByMessage(final Object message)
    {
        return message != null ? byMessageType.get(message.getClass()) : null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int encode(final Object message, final org.agrona.MutableDirectBuffer buffer, final int offset)
    {
        final MessageCodec<Object> codec = (MessageCodec<Object>)getByMessage(message);
        if (codec == null)
        {
            throw new IllegalArgumentException("No codec for message type: " +
                (message != null ? message.getClass().getName() : "null"));
        }
        return codec.encode(message, buffer, offset);
    }

    @Override
    public Object decode(final int typeId, final org.agrona.DirectBuffer buffer, final int offset, final int length)
    {
        final MessageCodec<?> codec = getByTypeId(typeId);
        if (codec == null)
        {
            throw new IllegalArgumentException("No codec for typeId: " + typeId);
        }
        return codec.decode(buffer, offset, length);
    }
}

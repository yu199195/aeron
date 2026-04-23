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

import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * {@link MessageClient} 实现。
 * <p>
 * 内部封装：编码、Wire 格式、解码，业务完全无感知。
 */
public final class MessageClientImpl implements MessageClient
{
    private final MessageCodecRegistry registry;
    private final MessageTransport transport;
    private final UnsafeBuffer sendBuffer;
    private MessageHandler handler;

    public MessageClientImpl(final MessageTransport transport)
    {
        this.registry = new MessageCodecRegistryImpl();
        this.transport = transport;
        this.sendBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, BitUtil.CACHE_LINE_LENGTH));

        transport.setReceiveCallback(this::onReceive);
    }

    @Override
    public void register(final MessageCodec<?> codec)
    {
        registry.register(codec);
    }

    @Override
    public void send(final Object message)
    {
        final MessageCodec<?> codec = registry.getByMessage(message);
        if (codec == null)
        {
            throw new IllegalArgumentException("No codec for: " + message.getClass().getName());
        }

        final int payloadOffset = WireFormat.HEADER_LENGTH;
        final int payloadLen = registry.encode(message, sendBuffer, payloadOffset);
        WireFormat.writeHeader(sendBuffer, 0, codec.typeId(), payloadLen);

        transport.send(sendBuffer, 0, payloadOffset + payloadLen);
    }

    @Override
    public void subscribe(final MessageHandler h)
    {
        this.handler = h;
    }

    private void onReceive(final DirectBuffer buffer, final int offset, final int length)
    {
        if (handler == null || length < WireFormat.HEADER_LENGTH)
        {
            return;
        }

        final int typeId = WireFormat.readTypeId(buffer, offset);
        final int payloadLen = WireFormat.readPayloadLength(buffer, offset);
        final int payloadOffset = offset + WireFormat.HEADER_LENGTH;

        if (payloadOffset + payloadLen > offset + length)
        {
            return;
        }

        final Object message = registry.decode(typeId, buffer, payloadOffset, payloadLen);
        handler.onMessage(message);
    }
}

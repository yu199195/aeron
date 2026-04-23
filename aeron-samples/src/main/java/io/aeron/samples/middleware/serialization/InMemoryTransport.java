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

import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 内存传输实现（用于 Demo、单机测试）。
 * <p>
 * 发送的数据入队，{@link #poll()} 时出队并回调。可模拟同一进程内的收发。
 */
public final class InMemoryTransport implements MessageTransport
{
    private final BlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    private final UnsafeBuffer receiveBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, 64));
    private volatile MessageTransport.ReceiveCallback callback;

    @Override
    public void send(final DirectBuffer buffer, final int offset, final int length)
    {
        final byte[] copy = new byte[length];
        buffer.getBytes(offset, copy);
        queue.offer(copy);
    }

    @Override
    public void setReceiveCallback(final MessageTransport.ReceiveCallback cb)
    {
        this.callback = cb;
    }

    /**
     * 轮询接收。由调用方（如 Demo 主循环）定期调用。
     */
    public void poll()
    {
        final MessageTransport.ReceiveCallback cb = callback;
        if (cb == null)
        {
            return;
        }

        byte[] data = queue.poll();
        if (data != null)
        {
            receiveBuffer.wrap(data);
            cb.onReceive(receiveBuffer, 0, data.length);
        }
    }
}

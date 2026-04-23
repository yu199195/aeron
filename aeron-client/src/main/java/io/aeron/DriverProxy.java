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
package io.aeron;

import io.aeron.command.*;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 * <p>
 * For writing commands into the client conductor buffer.
 * <p>
 * <b>Note:</b> this class is not thread safe and is expecting to be called within {@link Aeron.Context#clientLock()}.
 */
public final class DriverProxy
{
    private final long clientId;
    private final PublicationMessageFlyweight publicationMessageFlyweight = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessageFlyweight = new SubscriptionMessageFlyweight();
    private final RemoveCounterFlyweight removeCounterFlyweight = new RemoveCounterFlyweight();
    private final RemovePublicationFlyweight removePublicationFlyweight = new RemovePublicationFlyweight();
    private final RemoveSubscriptionFlyweight removeSubscriptionFlyweight = new RemoveSubscriptionFlyweight();
    private final DestinationMessageFlyweight destinationMessageFlyweight = new DestinationMessageFlyweight();
    private final DestinationByIdMessageFlyweight destinationByIdMessageFlyweight =
        new DestinationByIdMessageFlyweight();
    private final CounterMessageFlyweight counterMessageFlyweight = new CounterMessageFlyweight();
    private final StaticCounterMessageFlyweight staticCounterMessageFlyweight = new StaticCounterMessageFlyweight();
    private final RejectImageFlyweight rejectImageFlyweight = new RejectImageFlyweight();
    private final GetNextAvailableSessionIdMessageFlyweight getNextAvailableSessionIdMessageFlyweight =
        new GetNextAvailableSessionIdMessageFlyweight();
    private final RingBuffer toDriverCommandBuffer;

    /**
     * Create a proxy to a media driver which sends commands via a {@link RingBuffer}.
     *
     * @param toDriverCommandBuffer to send commands via.
     * @param clientId              to represent the client.
     */
    public DriverProxy(final RingBuffer toDriverCommandBuffer, final long clientId)
    {
        this.toDriverCommandBuffer = toDriverCommandBuffer;
        this.clientId = clientId;
    }

    /**
     * Time of the last heartbeat to indicate the driver is alive.
     *
     * @return time of the last heartbeat to indicate the driver is alive.
     */
    public long timeOfLastDriverKeepaliveMs()
    {
        return toDriverCommandBuffer.consumerHeartbeatTime();
    }

    /**
     * 向 Driver 发送 ADD_PUBLICATION 命令。
     * 实际上是将命令编码后写入 CnC 文件中的 to-driver ManyToOneRingBuffer。
     * Driver Conductor 线程会周期性消费该 RingBuffer 中的消息来处理命令。
     *
     * @param channel  通道 URI 字符串（如 "aeron:udp?endpoint=localhost:40123" 或 "aeron:ipc"）
     * @param streamId 通道内的流 ID
     * @return correlationId（命令唯一标识，Client 用它匹配 Driver 的响应）
     */
    public long addPublication(final String channel, final int streamId)
    {
        // 从 RingBuffer metadata 区域通过 CAS 原子递增获取全局唯一的 correlationId
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        // 计算消息总长度：固定头部字段 + channel 字符串的变长部分
        final int length = PublicationMessageFlyweight.computeLength(channel.length());
        // 在 RingBuffer 中 claim 一块空间，msgTypeId = ADD_PUBLICATION 供 Driver 端按类型分发
        // 返回值 < 0 表示 RingBuffer 已满（Client 命令发送过快或 Driver 消费不及时）
        final int index = toDriverCommandBuffer.tryClaim(ADD_PUBLICATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add publication command");
        }

        // 通过 Flyweight 模式直接在 RingBuffer 底层 buffer 上零拷贝编码消息字段，
        // 避免额外的对象分配和序列化开销
        publicationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)  // 包裹到 claim 的起始位置
            .streamId(streamId)                            // 流 ID
            .channel(channel)                              // 通道 URI 字符串
            .clientId(clientId)                            // 标识发送命令的 Client
            .correlationId(correlationId);                 // 命令唯一标识

        // 提交消息：设置 record header 的长度字段为正值，使 Driver 可见并可消费
        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to add a non-concurrent, i.e. exclusive, publication.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
    public long addExclusivePublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = PublicationMessageFlyweight.computeLength(channel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_EXCLUSIVE_PUBLICATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add exclusive publication command");
        }

        publicationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .streamId(streamId)
            .channel(channel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to remove a publication by its registration id.
     *
     * @param registrationId for the publication to be removed.
     * @param revoke whether the publication is being revoked.
     * @return the correlation id for the command.
     */
    public long removePublication(final long registrationId, final boolean revoke)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_PUBLICATION, RemovePublicationFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("failed to write remove publication command");
        }

        removePublicationFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .revoke(revoke)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * 向 Driver 发送 ADD_SUBSCRIPTION 命令。
     * <p>
     * 底层机制：通过 CnC 共享内存中的 ManyToOneRingBuffer（toDriverCommandBuffer）写入命令。
     * 采用 tryClaim → 填充 → commit 的三阶段无锁写入协议：
     * <ol>
     *   <li>tryClaim：用 CAS 在 ring buffer 中原子地预留一段空间，返回写入位置 index</li>
     *   <li>Flyweight 填充：将命令字段直接写入 ring buffer 底层的 mmap 内存（零拷贝）</li>
     *   <li>commit：写入消息头的 length 字段（volatile write），使 Driver 的 Conductor 线程可见</li>
     * </ol>
     *
     * @param channel  通道 URI，如 "aeron:udp?endpoint=localhost:40123"
     * @param streamId 通道内的流 ID
     * @return correlationId，全局唯一标识此命令，用于匹配 Driver 的异步响应
     */
    public long addSubscription(final String channel, final int streamId)
    {
        final long registrationId = Aeron.NULL_VALUE;
        // 原子递增分配一个全局唯一的 correlationId（存储在 ring buffer trailer 的 correlation counter 中）
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        // 计算 SubscriptionMessageFlyweight 序列化后的字节长度（固定头 + channel 字符串长度）
        final int length = SubscriptionMessageFlyweight.computeLength(channel.length());

        // tryClaim：在 ring buffer 中用 CAS 原子预留 length 字节空间
        // 成功返回写入起始 index；失败（buffer 满）返回负值
        // 消息类型 ADD_SUBSCRIPTION (0x04) 会写入消息头，Driver 据此分发到对应的处理方法
        final int index = toDriverCommandBuffer.tryClaim(ADD_SUBSCRIPTION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add subscription command");
        }

        // 用 Flyweight 模式直接在 ring buffer 的 mmap 内存上序列化命令字段（零拷贝，无中间对象）
        // Flyweight 消息布局：
        //   [clientId (8B)] [correlationId (8B)] [registrationCorrelationId (8B)] [streamId (4B)] [channel (变长)]
        subscriptionMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .streamId(streamId)
            .channel(channel)
            .clientId(clientId)
            .correlationId(correlationId);

        // commit：对消息头的 length 字段执行 volatile write（putIntOrdered），
        // 构成 happens-before 关系，保证 Driver 读到 length 时所有字段已可见
        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to remove a subscription by its registration id.
     *
     * @param registrationId for the subscription to be removed.
     * @return the correlation id for the command.
     */
    public long removeSubscription(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_SUBSCRIPTION, RemoveSubscriptionFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("failed to write remove subscription command");
        }

        removeSubscriptionFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a destination to the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add destination command");
        }

        destinationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel used for the {@link #addDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write remove destination command");
        }

        destinationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the send channel of an existing MDC Publication.
     *
     * @param publicationRegistrationId  of the Publication.
     * @param destinationRegistrationId used for the {@link #addDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeDestination(final long publicationRegistrationId, final long destinationRegistrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(
            REMOVE_DESTINATION_BY_ID, DestinationByIdMessageFlyweight.MESSAGE_LENGTH);
        if (index < 0)
        {
            throw new AeronException("failed to write remove destination command");
        }

        destinationByIdMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .resourceRegistrationId(publicationRegistrationId)
            .destinationRegistrationId(destinationRegistrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a destination to the receive channel endpoint of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_RCV_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add rcv destination command");
        }

        destinationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the receive channel endpoint of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel used for the {@link #addRcvDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_RCV_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("failed to write remove rcv destination command");
        }

        destinationMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a new counter with a type id plus the label and key are provided in buffers.
     *
     * @param typeId      for associating with the counter.
     * @param keyBuffer   containing the metadata key.
     * @param keyOffset   offset at which the key begins.
     * @param keyLength   length in bytes for the key.
     * @param labelBuffer containing the label.
     * @param labelOffset offset at which the label begins.
     * @param labelLength length in bytes for the label.
     * @return the correlation id for the command.
     */
    public long addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = CounterMessageFlyweight.computeLength(keyLength, labelLength);
        final int index = toDriverCommandBuffer.tryClaim(ADD_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add counter command");
        }

        counterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(keyBuffer, keyOffset, keyLength)
            .labelBuffer(labelBuffer, labelOffset, labelLength)
            .typeId(typeId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a new counter with a type id and label, the key will be blank.
     *
     * @param typeId for associating with the counter.
     * @param label  that is human-readable for the counter.
     * @return the correlation id for the command.
     */
    public long addCounter(final int typeId, final String label)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = CounterMessageFlyweight.computeLength(0, label.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add counter command");
        }

        counterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(null, 0, 0)
            .label(label)
            .typeId(typeId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the media driver to remove an existing counter by its registration id.
     *
     * @param registrationId of counter to remove.
     * @return the correlation id for the command.
     */
    public long removeCounter(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_COUNTER, RemoveCounterFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("failed to write remove counter command");
        }

        removeCounterFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Notify the media driver that this client is closing.
     */
    public void clientClose()
    {
        final int index = toDriverCommandBuffer.tryClaim(CLIENT_CLOSE, CorrelatedMessageFlyweight.LENGTH);
        if (index > 0)
        {
            new CorrelatedMessageFlyweight()
                .wrap(toDriverCommandBuffer.buffer(), index)
                .clientId(clientId)
                .correlationId(Aeron.NULL_VALUE);

            toDriverCommandBuffer.commit(index);
        }
    }

    /**
     * Instruct the media driver to terminate.
     *
     * @param tokenBuffer containing the authentication token.
     * @param tokenOffset at which the token begins.
     * @param tokenLength in bytes.
     * @return true is successfully sent.
     */
    public boolean terminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        final int length = TerminateDriverFlyweight.computeLength(tokenLength);
        final int index = toDriverCommandBuffer.tryClaim(TERMINATE_DRIVER, length);
        if (index > 0)
        {
            new TerminateDriverFlyweight()
                .wrap(toDriverCommandBuffer.buffer(), index)
                .tokenBuffer(tokenBuffer, tokenOffset, tokenLength)
                .clientId(clientId)
                .correlationId(Aeron.NULL_VALUE);

            toDriverCommandBuffer.commit(index);
            return true;
        }

        return false;
    }

    /**
     * Reject a specific image.
     *
     * @param imageCorrelationId of the image to be invalidated
     * @param position      of the image when invalidation occurred
     * @param reason        user supplied reason for invalidation, reported back to publication
     * @return              the correlationId of the request for invalidation.
     */
    public long rejectImage(
        final long imageCorrelationId,
        final long position,
        final String reason)
    {
        final int length = RejectImageFlyweight.computeLength(reason);
        final int index = toDriverCommandBuffer.tryClaim(REJECT_IMAGE, length);

        if (index < 0)
        {
            throw new AeronException("failed to write reject image command");
        }

        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        rejectImageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .clientId(clientId)
            .correlationId(correlationId)
            .imageCorrelationId(imageCorrelationId)
            .position(position)
            .reason(reason);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }


    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DriverProxy{" +
            "clientId=" + clientId +
            '}';
    }

    long addStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = StaticCounterMessageFlyweight.computeLength(keyLength, labelLength);
        final int index = toDriverCommandBuffer.tryClaim(ADD_STATIC_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add counter command");
        }

        staticCounterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(keyBuffer, keyOffset, keyLength)
            .labelBuffer(labelBuffer, labelOffset, labelLength)
            .typeId(typeId)
            .registrationId(registrationId)
            .correlationId(correlationId)
            .clientId(clientId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    long addStaticCounter(final int typeId, final String label, final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = StaticCounterMessageFlyweight.computeLength(0, label.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_STATIC_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("failed to write add counter command");
        }

        staticCounterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(null, 0, 0)
            .label(label)
            .typeId(typeId)
            .registrationId(registrationId)
            .correlationId(correlationId)
            .clientId(clientId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    long nextAvailableSessionId(final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(
            GET_NEXT_AVAILABLE_SESSION_ID, GetNextAvailableSessionIdMessageFlyweight.LENGTH);
        if (index < 0)
        {
            throw new AeronException("failed to write next session id command");
        }

        getNextAvailableSessionIdMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .streamId(streamId)
            .correlationId(correlationId)
            .clientId(clientId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }
}

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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ErrorCode;
import io.aeron.command.*;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;

import java.net.InetSocketAddress;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Proxy for communicating from the driver to the client conductor.
 */
final class ClientProxy
{
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);
    private final BroadcastTransmitter transmitter;

    private final ErrorResponseFlyweight errorResponse = new ErrorResponseFlyweight();
    private final PublicationErrorFrameFlyweight publicationErrorFrame = new PublicationErrorFrameFlyweight();
    private final PublicationBuffersReadyFlyweight publicationReady = new PublicationBuffersReadyFlyweight();
    private final SubscriptionReadyFlyweight subscriptionReady = new SubscriptionReadyFlyweight();
    private final ImageBuffersReadyFlyweight imageReady = new ImageBuffersReadyFlyweight();
    private final OperationSucceededFlyweight operationSucceeded = new OperationSucceededFlyweight();
    private final ImageMessageFlyweight imageMessage = new ImageMessageFlyweight();
    private final CounterUpdateFlyweight counterUpdate = new CounterUpdateFlyweight();
    private final ClientTimeoutFlyweight clientTimeout = new ClientTimeoutFlyweight();
    private final StaticCounterFlyweight staticCounter = new StaticCounterFlyweight();
    private final NextAvailableSessionIdFlyweight nextSessionId = new NextAvailableSessionIdFlyweight();

    ClientProxy(final BroadcastTransmitter transmitter)
    {
        this.transmitter = transmitter;

        errorResponse.wrap(buffer, 0);
        publicationErrorFrame.wrap(buffer, 0);
        imageReady.wrap(buffer, 0);
        publicationReady.wrap(buffer, 0);
        subscriptionReady.wrap(buffer, 0);
        operationSucceeded.wrap(buffer, 0);
        imageMessage.wrap(buffer, 0);
        counterUpdate.wrap(buffer, 0);
        clientTimeout.wrap(buffer, 0);
        staticCounter.wrap(buffer, 0);
        nextSessionId.wrap(buffer, 0);
    }

    void onError(final long correlationId, final ErrorCode errorCode, final String errorMessage)
    {
        errorResponse
            .offendingCommandCorrelationId(correlationId)
            .errorCode(errorCode)
            .errorMessage(errorMessage);

        transmit(ON_ERROR, buffer, 0, errorResponse.length());
    }

    void onPublicationErrorFrame(
        final long registrationId,
        final long destinationRegistrationId, final int sessionId,
        final int streamId,
        final long receiverId,
        final Long groupTag,
        final InetSocketAddress srcAddress,
        final int errorCode,
        final String errorMessage)
    {
        publicationErrorFrame
            .registrationId(registrationId)
            .destinationRegistrationId(destinationRegistrationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .receiverId(receiverId)
            .groupTag(null == groupTag ? Aeron.NULL_VALUE : groupTag)
            .sourceAddress(srcAddress)
            .errorCode(ErrorCode.get(errorCode))
            .errorMessage(errorMessage);

        transmit(ON_PUBLICATION_ERROR, buffer, 0, publicationErrorFrame.length());
    }

    void onAvailableImage(
        final long correlationId,
        final int streamId,
        final int sessionId,
        final long subscriptionRegistrationId,
        final int positionCounterId,
        final String logFileName,
        final String sourceIdentity)
    {
        imageReady
            .correlationId(correlationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .subscriptionRegistrationId(subscriptionRegistrationId)
            .subscriberPositionId(positionCounterId)
            .logFileName(logFileName)
            .sourceIdentity(sourceIdentity);

        transmit(ON_AVAILABLE_IMAGE, buffer, 0, imageReady.length());
    }

    /**
     * 通过 CnC to-clients BroadcastBuffer 向 Client 发送 PUBLICATION_READY 事件。
     * Client 端的 awaitResponse() 循环会从 BroadcastReceiver 中读取此消息，
     * 然后用 logFileName 做 mmap 映射来构造 ConcurrentPublication。
     *
     * @param correlationId        Client 命令的 correlationId，用于匹配请求-响应
     * @param registrationId       Publication 的注册 ID（首次创建时的 correlationId）
     * @param streamId             流 ID
     * @param sessionId            会话 ID（唯一标识一个 Publication 实例）
     * @param logFileName          LogBuffer 文件的绝对路径，Client 会 mmap 此文件来写数据
     * @param positionCounterId    publisherLimit 计数器 ID，Client 用于流控检查
     * @param channelStatusCounterId 通道状态计数器 ID
     * @param isExclusive          是否独占 Publication
     */
    void onPublicationReady(
        final long correlationId,
        final long registrationId,
        final int streamId,
        final int sessionId,
        final String logFileName,
        final int positionCounterId,
        final int channelStatusCounterId,
        final boolean isExclusive)
    {
        // 通过 Flyweight 直接在 buffer 上编码响应消息（零拷贝）
        publicationReady
            .correlationId(correlationId)
            .registrationId(registrationId)
            .sessionId(sessionId)
            .streamId(streamId)
            .publicationLimitCounterId(positionCounterId)
            .channelStatusCounterId(channelStatusCounterId)
            .logFileName(logFileName);

        // 根据是否 exclusive 选择消息类型，Client 端据此调用不同的回调
        // ON_PUBLICATION_READY → onNewPublication()（构造 ConcurrentPublication）
        // ON_EXCLUSIVE_PUBLICATION_READY → onNewExclusivePublication()（构造 ExclusivePublication）
        final int msgTypeId = isExclusive ? ON_EXCLUSIVE_PUBLICATION_READY : ON_PUBLICATION_READY;
        // 写入 to-clients BroadcastBuffer，所有连接的 Client 都可以收到（one-to-many broadcast）
        transmit(msgTypeId, buffer, 0, publicationReady.length());
    }

    void onSubscriptionReady(final long correlationId, final int channelStatusCounterId)
    {
        subscriptionReady
            .correlationId(correlationId)
            .channelStatusCounterId(channelStatusCounterId);

        transmit(ON_SUBSCRIPTION_READY, buffer, 0, SubscriptionReadyFlyweight.LENGTH);
    }

    void operationSucceeded(final long correlationId)
    {
        operationSucceeded.correlationId(correlationId);

        transmit(ON_OPERATION_SUCCESS, buffer, 0, OperationSucceededFlyweight.LENGTH);
    }

    void onUnavailableImage(
        final long correlationId, final long subscriptionRegistrationId, final int streamId, final String channel)
    {
        imageMessage
            .correlationId(correlationId)
            .subscriptionRegistrationId(subscriptionRegistrationId)
            .streamId(streamId)
            .channel(channel);

        transmit(ON_UNAVAILABLE_IMAGE, buffer, 0, imageMessage.length());
    }

    void onCounterReady(final long correlationId, final int counterId)
    {
        counterUpdate
            .correlationId(correlationId)
            .counterId(counterId);

        transmit(ON_COUNTER_READY, buffer, 0, CounterUpdateFlyweight.LENGTH);
    }

    void onStaticCounter(final long correlationId, final int counterId)
    {
        staticCounter
            .correlationId(correlationId)
            .counterId(counterId);

        transmit(ON_STATIC_COUNTER, buffer, 0, StaticCounterFlyweight.LENGTH);
    }

    void onUnavailableCounter(final long registrationId, final int counterId)
    {
        counterUpdate
            .correlationId(registrationId)
            .counterId(counterId);

        transmit(ON_UNAVAILABLE_COUNTER, buffer, 0, CounterUpdateFlyweight.LENGTH);
    }

    void onClientTimeout(final long clientId)
    {
        clientTimeout.clientId(clientId);

        transmit(ON_CLIENT_TIMEOUT, buffer, 0, ClientTimeoutFlyweight.LENGTH);
    }

    void onNextAvailableSessionId(final long correlationId, final int sessionId)
    {
        nextSessionId
            .correlationId(correlationId)
            .nextSessionId(sessionId);

        transmit(ON_NEXT_AVAILABLE_SESSION_ID, buffer, 0, NextAvailableSessionIdFlyweight.LENGTH);
    }

    private void transmit(final int msgTypeId, final DirectBuffer buffer, final int index, final int length)
    {
        transmitter.transmit(msgTypeId, buffer, index, length);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ClientProxy{}";
    }
}

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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import io.aeron.security.NullCredentialsSupplier;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Long2ObjectHashMap;

import static io.aeron.CommonContext.RESPONSE_CORRELATION_ID_PARAM_NAME;

class ControlSessionAdapter implements FragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;
    private static final int FILE_IO_MAX_LENGTH_VERSION = 7;
    private static final int SESSION_ID_VERSION = 8;
    private static final int ENCODED_CREDENTIALS_VERSION = 8;
    private static final int REPLAY_TOKEN_VERSION = 10;

    private final ControlRequestDecoders decoders;
    private final AuthorisationService authorisationService;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Long2ObjectHashMap<SessionInfo> controlSessionByIdMap = new Long2ObjectHashMap<>();
    private final Subscription controlSubscription;
    private final Subscription localControlSubscription;
    private final ArchiveConductor conductor;

    ControlSessionAdapter(
        final ControlRequestDecoders decoders,
        final Subscription controlSubscription,
        final Subscription localControlSubscription,
        final ArchiveConductor conductor,
        final AuthorisationService authorisationService)
    {
        this.decoders = decoders;
        this.controlSubscription = controlSubscription;
        this.localControlSubscription = localControlSubscription;
        this.conductor = conductor;
        this.authorisationService = authorisationService;
    }

    /**
     * 轮询控制 Subscription（外部 + 本地），将分片交给 {@link FragmentAssembler} 组装；组装完整消息后回调
     * {@link #onFragment(DirectBuffer, int, int, Header)}。
     */
    public int poll()
    {
        int fragmentsRead = 0;

        if (null != controlSubscription)
        {
            // 远端/可配置控制流；与 local 二选一或并存，由 Archive 配置决定。
            fragmentsRead += controlSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
        }

        // 本进程内控制通道（例如 embedded archive 与 client 同 JVM）。
        fragmentsRead += localControlSubscription.poll(fragmentAssembler, FRAGMENT_LIMIT);

        return fragmentsRead;
    }

    /**
     * Aeron Archive 控制通道上的入站消息回调：由 {@link #poll()} 经 {@link FragmentAssembler} 拼出完整帧后调用。
     * <p>
     * 流程：用 {@link MessageHeaderDecoder} 解析 SBE 消息头并校验 schemaId；再按 {@code templateId} 选择解码器
     * {@code wrap} 负载；除 {@code Connect}/{@code AuthConnect} 新建会话外，通常经 {@link #getControlSession}
     * 校验 {@code controlSessionId} 与授权后，把请求转给 {@link ControlSession} 或 {@link ArchiveConductor}。
     * {@code Replay}/{@code BoundedReplay} 在 response 控制模式下会经 {@link #setupSessionAndChannelForReplay}
     * 解析 replay token 并把本 Image 的 correlation id 写入 channel，供驱动把回放发往正确 publication。
     */
    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        // 每条控制消息前导均为 SBE MessageHeader；wrap 后可读 schemaId、templateId、blockLength、version 等。
        final MessageHeaderDecoder headerDecoder = decoders.header;
        headerDecoder.wrap(buffer, offset);

        final int schemaId = headerDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        // templateId 决定具体请求类型；负载从 MessageHeader 之后开始，解码器需与 header 的 blockLength/version 一致。
        final int templateId = headerDecoder.templateId();
        switch (templateId)
        {
            // --- 会话生命周期：连接（无 controlSessionId，新建会话并登记）---
            case ConnectRequestDecoder.TEMPLATE_ID:
            {
                final ConnectRequestDecoder decoder = decoders.connectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                // Header.context() 为收到该消息的 Image，用于会话与 Image 绑定及后续按 Image 中止等。
                final Image image = (Image)header.context();

                final ControlSession session = conductor.newControlSession(
                    image,
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    decoder.responseChannel(),
                    ArrayUtil.EMPTY_BYTE_ARRAY,
                    "",
                    this);
                // 服务端分配的 controlSessionId -> 该客户端 Image 与控制会话，供后续请求路由与授权。
                controlSessionByIdMap.put(session.sessionId(), new SessionInfo(image, session));
                break;
            }

            // 客户端主动关闭控制会话：按 controlSessionId 找到会话并 abort。
            case CloseSessionRequestDecoder.TEMPLATE_ID:
            {
                final CloseSessionRequestDecoder decoder = decoders.closeSessionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
                if (null != info)
                {
                    info.controlSession.abort(ControlSession.SESSION_CLOSED_MSG);
                }
                break;
            }

            // --- 录制：开始/停止/按 subscription 或 recordingId 停止等 ---
            case StartRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StartRecordingRequestDecoder decoder = decoders.startRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingRequestDecoder decoder = decoders.stopRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecording(correlationId, decoder.streamId(), decoder.channel());
                }
                break;
            }

            // --- 回放：普通回放；可能带 fileIoMaxLength、replayToken（高版本协议）---
            case ReplayRequestDecoder.TEMPLATE_ID:
            {
                final ReplayRequestDecoder decoder = decoders.replayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                // 协议版本足够时才读 fileIoMaxLength / replayToken，否则用 NULL，保持与老客户端兼容。
                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;
                final long recordingId = decoder.recordingId();
                final long position = decoder.position();
                final long replayLength = decoder.length();
                final int replayStreamId = decoder.replayStreamId();
                final long replayToken = REPLAY_TOKEN_VERSION <= headerDecoder.version() ?
                    decoder.replayToken() : Aeron.NULL_VALUE;

                final String replayChannel = decoder.replayChannel();
                final ChannelUri channelUri = ChannelUri.parse(replayChannel);
                final Image image = (Image)header.context();
                // response 模式 + 有效 replayToken 时，会话来自 token 映射且 channel 会注入 response correlation id。
                final ControlSession controlSession = setupSessionAndChannelForReplay(
                    channelUri,
                    replayToken,
                    recordingId,
                    correlationId,
                    controlSessionId,
                    templateId,
                    image.correlationId());

                if (null != controlSession)
                {
                    controlSession.onStartReplay(
                        correlationId,
                        recordingId,
                        position,
                        replayLength,
                        fileIoMaxLength,
                        replayStreamId,
                        channelUri.toString());
                }
                break;
            }

            // 停止指定 replaySessionId 的回放。
            case StopReplayRequestDecoder.TEMPLATE_ID:
            {
                final StopReplayRequestDecoder decoder = decoders.stopReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplay(correlationId, decoder.replaySessionId());
                }
                break;
            }

            // --- 目录与查询：列表、按 URI 过滤、单条 recording、subscription 列表等 ---
            case ListRecordingsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsRequestDecoder decoder = decoders.listRecordingsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordings(correlationId, decoder.fromRecordingId(), decoder.recordCount());
                }
                break;
            }

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingsForUriRequestDecoder decoder = decoders.listRecordingsForUriRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    // channel 为变长字段：先取长度再拷入 byte[]，供会话侧按字节比较/匹配。
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];
                    decoder.getChannel(bytes, 0, channelLength);

                    controlSession.onListRecordingsForUri(
                        correlationId,
                        decoder.fromRecordingId(),
                        decoder.recordCount(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingRequestDecoder decoder = decoders.listRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecording(correlationId, decoder.recordingId());
                }
                break;
            }

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequestDecoder decoder = decoders.extendRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        false,
                        decoder.channel());
                }
                break;
            }

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
            {
                final RecordingPositionRequestDecoder decoder = decoders.recordingPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetRecordingPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
            {
                final TruncateRecordingRequestDecoder decoder = decoders.truncateRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onTruncateRecording(correlationId, decoder.recordingId(), decoder.position());
                }
                break;
            }

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingSubscriptionRequestDecoder decoder = decoders.stopRecordingSubscriptionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingSubscription(correlationId, decoder.subscriptionId());
                }
                break;
            }

            case StopPositionRequestDecoder.TEMPLATE_ID:
            {
                final StopPositionRequestDecoder decoder = decoders.stopPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStopPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case FindLastMatchingRecordingRequestDecoder.TEMPLATE_ID:
            {
                final FindLastMatchingRecordingRequestDecoder decoder = decoders.findLastMatchingRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    // 与 ListRecordingsForUri 相同：channel 为变长，读出为 byte[] 供匹配。
                    final int channelLength = decoder.channelLength();
                    final byte[] bytes = 0 == channelLength ? ArrayUtil.EMPTY_BYTE_ARRAY : new byte[channelLength];

                    decoder.getChannel(bytes, 0, channelLength);
                    controlSession.onFindLastMatchingRecording(
                        correlationId,
                        decoder.minRecordingId(),
                        decoder.sessionId(),
                        decoder.streamId(),
                        bytes);
                }
                break;
            }

            case ListRecordingSubscriptionsRequestDecoder.TEMPLATE_ID:
            {
                final ListRecordingSubscriptionsRequestDecoder decoder = decoders.listRecordingSubscriptionsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onListRecordingSubscriptions(
                        correlationId,
                        decoder.pseudoIndex(),
                        decoder.subscriptionCount(),
                        decoder.applyStreamId() == BooleanType.TRUE,
                        decoder.streamId(),
                        decoder.channel());
                }
                break;
            }

            // 有界回放：除与普通回放相同参数外，还用 limitCounterId 约束回放上界。
            case BoundedReplayRequestDecoder.TEMPLATE_ID:
            {
                final BoundedReplayRequestDecoder decoder = decoders.boundedReplayRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long position = decoder.position();
                final long replayLength = decoder.length();
                final long recordingId = decoder.recordingId();
                final int limitCounterId = decoder.limitCounterId();
                final int replayStreamId = decoder.replayStreamId();
                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;
                final long replayToken = REPLAY_TOKEN_VERSION <= headerDecoder.version() ?
                    decoder.replayToken() : Aeron.NULL_VALUE;

                final String replayChannel = decoder.replayChannel();

                final Image image = (Image)header.context();

                final ChannelUri channelUri = ChannelUri.parse(replayChannel);
                // 与普通 Replay 相同：response + token 时走 token 会话并在 URI 注入 response correlation。
                final ControlSession controlSession = setupSessionAndChannelForReplay(
                    channelUri,
                    replayToken,
                    recordingId,
                    correlationId,
                    controlSessionId,
                    templateId,
                    image.correlationId());

                if (null != controlSession)
                {
                    controlSession.onStartBoundedReplay(
                        correlationId,
                        recordingId,
                        position,
                        replayLength,
                        limitCounterId,
                        fileIoMaxLength,
                        replayStreamId,
                        channelUri.toString());
                }
                break;
            }

            case StopAllReplaysRequestDecoder.TEMPLATE_ID:
            {
                final StopAllReplaysRequestDecoder decoder = decoders.stopAllReplaysRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopAllReplays(correlationId, decoder.recordingId());
                }
                break;
            }

            // --- 复制（replication）：从远端 archive 拉取录制；多种 Replicate 变体参数不同 ---
            case ReplicateRequestDecoder.TEMPLATE_ID:
            {
                final ReplicateRequestDecoder decoder = decoders.replicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        "",
                        NullCredentialsSupplier.NULL_CREDENTIAL,
                        "");
                }
                break;
            }

            case StopReplicationRequestDecoder.TEMPLATE_ID:
            {
                final StopReplicationRequestDecoder decoder = decoders.stopReplicationRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopReplication(correlationId, decoder.replicationId());
                }
                break;
            }

            // --- 录制元数据：起止位置、截断、分段 attach/detach/migrate/purge 等 ---
            case StartPositionRequestDecoder.TEMPLATE_ID:
            {
                final StartPositionRequestDecoder decoder = decoders.startPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetStartPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case DetachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DetachSegmentsRequestDecoder decoder = decoders.detachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDetachSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case DeleteDetachedSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final DeleteDetachedSegmentsRequestDecoder decoder = decoders.deleteDetachedSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onDeleteDetachedSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case PurgeSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final PurgeSegmentsRequestDecoder decoder = decoders.purgeSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeSegments(correlationId, decoder.recordingId(), decoder.newStartPosition());
                }
                break;
            }

            case AttachSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final AttachSegmentsRequestDecoder decoder = decoders.attachSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onAttachSegments(correlationId, decoder.recordingId());
                }
                break;
            }

            case MigrateSegmentsRequestDecoder.TEMPLATE_ID:
            {
                final MigrateSegmentsRequestDecoder decoder = decoders.migrateSegmentsRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onMigrateSegments(correlationId, decoder.srcRecordingId(), decoder.dstRecordingId());
                }
                break;
            }

            // --- 带认证的连接：携带编码凭据与 clientInfo，建立控制会话 ---
            case AuthConnectRequestDecoder.TEMPLATE_ID:
            {
                final AuthConnectRequestDecoder decoder = decoders.authConnectRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final String responseChannel = decoder.responseChannel();
                final int credentialsLength = decoder.encodedCredentialsLength();
                final byte[] credentials;

                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                }
                else
                {
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                    decoder.skipEncodedCredentials();
                }
                final String clientInfo = decoder.clientInfo();

                final Image image = (Image)header.context();

                // 凭据交给 conductor 做认证流程；成功后同样登记 controlSessionByIdMap。
                final ControlSession session = conductor.newControlSession(
                    image,
                    decoder.correlationId(),
                    decoder.responseStreamId(),
                    decoder.version(),
                    responseChannel,
                    credentials,
                    clientInfo,
                    this);
                controlSessionByIdMap.put(session.sessionId(), new SessionInfo(image, session));
                break;
            }

            // 质询-响应：客户端对 challenge 的应答，凭据经 decode 后交给已有会话。
            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                final ChallengeResponseDecoder decoder = decoders.challengeResponse;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
                if (null != info)
                {
                    final int credentialsLength = decoder.encodedCredentialsLength();
                    final byte[] credentials;

                    if (credentialsLength > 0)
                    {
                        credentials = new byte[credentialsLength];
                        decoder.getEncodedCredentials(credentials, 0, credentialsLength);
                    }
                    else
                    {
                        credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                    }

                    info.controlSession.onChallengeResponse(decoder.correlationId(), credentials);
                }
                break;
            }

            // 保活：刷新会话活动时间，避免服务端超时关闭。
            case KeepAliveRequestDecoder.TEMPLATE_ID:
            {
                final KeepAliveRequestDecoder decoder = decoders.keepAliveRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onKeepAlive(correlationId);
                }
                break;
            }

            // 复制请求扩展：携带 channelTagId / subscriptionTagId 等，便于按标签路由。
            case TaggedReplicateRequestDecoder.TEMPLATE_ID:
            {
                final TaggedReplicateRequestDecoder decoder = decoders.taggedReplicateRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        AeronArchive.NULL_POSITION,
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        Aeron.NULL_VALUE,
                        Aeron.NULL_VALUE,
                        decoder.srcControlChannel(),
                        decoder.liveDestination(),
                        "",
                        NullCredentialsSupplier.NULL_CREDENTIAL,
                        "");
                }
                break;
            }

            case StartRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final StartRecordingRequest2Decoder decoder = decoders.startRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStartRecording(
                        correlationId,
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case ExtendRecordingRequest2Decoder.TEMPLATE_ID:
            {
                final ExtendRecordingRequest2Decoder decoder = decoders.extendRecordingRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onExtendRecording(
                        correlationId,
                        decoder.recordingId(),
                        decoder.streamId(),
                        decoder.sourceLocation(),
                        decoder.autoStop() == BooleanType.TRUE,
                        decoder.channel());
                }
                break;
            }

            case StopRecordingByIdentityRequestDecoder.TEMPLATE_ID:
            {
                final StopRecordingByIdentityRequestDecoder decoder = decoders.stopRecordingByIdentityRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onStopRecordingByIdentity(correlationId, decoder.recordingId());
                }
                break;
            }

            // Replicate 第二代：stopPosition、fileIoMaxLength、replicationSessionId、编码凭据、srcResponseChannel 等。
            case ReplicateRequest2Decoder.TEMPLATE_ID:
            {
                final ReplicateRequest2Decoder decoder = decoders.replicateRequest2;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                final int fileIoMaxLength = FILE_IO_MAX_LENGTH_VERSION <= headerDecoder.version() ?
                    decoder.fileIoMaxLength() : Aeron.NULL_VALUE;

                final int sessionId = SESSION_ID_VERSION <= headerDecoder.version() ?
                    decoder.replicationSessionId() : Aeron.NULL_VALUE;

                final String srcControlChannel = decoder.srcControlChannel();
                final String liveDestination = decoder.liveDestination();
                final String replicationChannel = decoder.replicationChannel();
                final byte[] encodedCredentials;
                if (ENCODED_CREDENTIALS_VERSION <= headerDecoder.version())
                {
                    encodedCredentials = new byte[decoder.encodedCredentialsLength()];
                    decoder.getEncodedCredentials(encodedCredentials, 0, decoder.encodedCredentialsLength());
                }
                else
                {
                    encodedCredentials = NullCredentialsSupplier.NULL_CREDENTIAL;
                }
                final String srcResponseChannel = decoder.srcResponseChannel();

                if (null != controlSession)
                {
                    controlSession.onReplicate(
                        correlationId,
                        decoder.srcRecordingId(),
                        decoder.dstRecordingId(),
                        decoder.stopPosition(),
                        decoder.channelTagId(),
                        decoder.subscriptionTagId(),
                        decoder.srcControlStreamId(),
                        fileIoMaxLength,
                        sessionId,
                        srcControlChannel,
                        liveDestination,
                        replicationChannel,
                        encodedCredentials,
                        srcResponseChannel);
                }
                break;
            }

            case PurgeRecordingRequestDecoder.TEMPLATE_ID:
            {
                final PurgeRecordingRequestDecoder decoder = decoders.purgeRecordingRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onPurgeRecording(correlationId, decoder.recordingId());
                }
                break;
            }

            case MaxRecordedPositionRequestDecoder.TEMPLATE_ID:
            {
                final MaxRecordedPositionRequestDecoder decoder = decoders.maxRecordedPositionRequest;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onGetMaxRecordedPosition(correlationId, decoder.recordingId());
                }
                break;
            }

            case ArchiveIdRequestDecoder.TEMPLATE_ID:
            {
                final ArchiveIdRequestDecoder decoder = decoders.archiveIdRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);

                if (null != controlSession)
                {
                    controlSession.onArchiveId(correlationId);
                }
                break;
            }

            // 申请 replay token：用于后续在 response 模式的 replay channel 上与预建会话关联。
            case ReplayTokenRequestDecoder.TEMPLATE_ID:
            {
                final ReplayTokenRequestDecoder decoder = decoders.replayTokenRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long recordingId = decoder.recordingId();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);
                if (null != controlSession)
                {
                    // conductor 生成并登记 token；客户端在 Replay 请求中带回，setupSessionAndChannelForReplay 解析。
                    final long replayToken = conductor.generateReplayToken(controlSession, recordingId);
                    controlSession.sendOkResponse(correlationId, replayToken);
                }

                break;
            }

            // 更新正在录制会话的 channel（例如动态重定向）；具体策略由 ArchiveConductor 实现。
            case UpdateChannelRequestDecoder.TEMPLATE_ID:
            {
                final UpdateChannelRequestDecoder decoder = decoders.updateChannelRequestDecoder;
                decoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                final long controlSessionId = decoder.controlSessionId();
                final long correlationId = decoder.correlationId();
                final long recordingId = decoder.recordingId();
                final String channel = decoder.channel();
                final ControlSession controlSession = getControlSession(correlationId, controlSessionId, templateId);
                if (null != controlSession)
                {
                    conductor.updateChannel(correlationId, recordingId, channel, controlSession);
                }

                break;
            }
        }
    }

    void abortControlSessionByImage(final Image image)
    {
        for (final SessionInfo info : controlSessionByIdMap.values())
        {
            if (info.image == image && !info.controlSession.isDone())
            {
                final Subscription subscription = image.subscription();
                info.controlSession.abort(ControlSession.REQUEST_IMAGE_NOT_AVAILABLE_MSG +
                    " : controlSessionId=" + info.controlSession.sessionId() +
                    " image.correlationId=" + image.correlationId() +
                    " sessionId=" + image.sessionId() +
                    " streamId=" + subscription.streamId() +
                    " channel=" + subscription.channel());
                break;
            }
        }
    }

    void removeControlSession(final long controlSessionId, final boolean sessionAborted, final String abortReason)
    {
        final SessionInfo sessionInfo = controlSessionByIdMap.remove(controlSessionId);
        if (null != sessionInfo && sessionAborted)
        {
            sessionInfo.image.reject(abortReason);
        }
        conductor.removeReplayTokensForSession(controlSessionId);
        final Counter controlSessionsCounter = conductor.context().controlSessionsCounter();
        if (!controlSessionsCounter.isClosed())
        {
            controlSessionsCounter.decrementRelease();
        }
    }

    /**
     * 为回放请求解析出要派发的 {@link ControlSession}，并在「response 控制模式 + 有效 replayToken」时
     * 把当前控制 Image 的 correlation id 写入 channel（{@link io.aeron.CommonContext#RESPONSE_CORRELATION_ID_PARAM_NAME}），
     * 使 Media Driver 将回放数据发到客户端预先 add 的 publication。
     */
    private ControlSession setupSessionAndChannelForReplay(
        final ChannelUri channelUri,
        final long replayToken,
        final long recordingId,
        final long correlationId,
        final long controlSessionId,
        final int templateId,
        final long imageCorrelationId)
    {
        final ControlSession controlSession;
        if (channelUri.hasControlModeResponse() && Aeron.NULL_VALUE != replayToken)
        {
            // token 在 ReplayTokenRequest 中生成并绑定会话；此处取出会话，失败表示过期或非法 token。
            controlSession = conductor.getReplaySession(replayToken, recordingId);
            if (null == controlSession)
            {
                throw new ArchiveException("Unknown session or token timeout for replayToken=" + replayToken);
            }

            // 将本控制 Image 与客户端 response publication 关联，供驱动端解析并定向发送。
            channelUri.put(RESPONSE_CORRELATION_ID_PARAM_NAME, Long.toString(imageCorrelationId));
        }
        else
        {
            // 非 response+token：按 controlSessionId 查表并做授权（与普通控制请求一致）。
            controlSession = getControlSession(correlationId, controlSessionId, templateId);
        }
        return controlSession;
    }

    /**
     * 根据 {@code controlSessionId} 查找 {@link SessionInfo}，对当前请求的 {@code templateId} 做
     * {@link AuthorisationService#isAuthorised}；未授权则发错误响应并返回 {@code null}，未知会话记日志并返回 {@code null}。
     */
    private ControlSession getControlSession(
        final long correlationId, final long controlSessionId, final int templateId)
    {
        final SessionInfo info = controlSessionByIdMap.get(controlSessionId);
        if (null != info)
        {
            final ControlSession controlSession = info.controlSession;
            final byte[] principal = controlSession.encodedPrincipal();
            if (!authorisationService.isAuthorised(MessageHeaderDecoder.SCHEMA_ID, templateId, null, principal))
            {
                conductor.logWarning("unauthorised archive action=" + templateId +
                    " controlSessionId=" + controlSessionId + " source=" + info.image.sourceIdentity());

                controlSession.sendErrorResponse(
                    correlationId, ArchiveException.UNAUTHORISED_ACTION, "unauthorised action");

                return null;
            }
            return controlSession;
        }
        else
        {
            // 未 Connect / 会话已移除 / 错误的 controlSessionId：不向未知 peer 泄露细节，仅记内部日志。
            conductor.logWarning("control request for unknown session:" +
                " controlSessionId=" + controlSessionId +
                " templateId=" + templateId);
            return null;
        }
    }

    /** 控制会话与收到其 Connect 的 {@link Image} 绑定，便于按 Image 失效时中止会话。 */
    private record SessionInfo(Image image, ControlSession controlSession)
    {
    }
}

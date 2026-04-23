/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.CloseHelper;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLength;
import static io.aeron.logbuffer.FrameDescriptor.frameSessionId;
import static io.aeron.logbuffer.FrameDescriptor.frameType;
import static io.aeron.protocol.DataHeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.DataHeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.DataHeaderFlyweight.SESSION_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.STREAM_ID_FIELD_OFFSET;
import static io.aeron.protocol.DataHeaderFlyweight.streamId;
import static io.aeron.protocol.DataHeaderFlyweight.termId;
import static io.aeron.protocol.DataHeaderFlyweight.termOffset;
import static java.lang.Math.min;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.file.StandardOpenOption.READ;
import static org.agrona.BitUtil.align;

/**
 * Archive 侧「回放会话」：在 {@link ArchiveConductor} 收到 Replay/BoundedReplay 等请求后创建，按状态机驱动。
 * <p>
 * <b>与 UDP 的关系（本类不直接发 UDP）</b>：回放数据通过 {@link ExclusivePublication#offerBlock} /
 * {@link ExclusivePublication#appendPadding} 写入该 publication 对应的<strong>日志缓冲区（term buffer）</strong>。
 * 运行在独立进程或同进程内的 <strong>Media Driver</strong> 中，{@link io.aeron.driver.Sender} 线程在
 * {@link io.aeron.driver.Sender#doWork()} 里轮询每个 {@link io.aeron.driver.NetworkPublication}，调用
 * {@link io.aeron.driver.NetworkPublication#send(long)}，从与本 {@link ExclusivePublication} 映射的同一
 * {@link io.aeron.driver.buffer.RawLog} term 中读出帧，再经 {@link io.aeron.driver.media.SendChannelEndpoint#send(java.nio.ByteBuffer)}
 * → {@link java.nio.channels.DatagramChannel} 进入内核 UDP。
 * <p>
 * 因此「磁盘 → 网络」：本类读 segment、改帧头、{@code offerBlock} → Sender → NetworkPublication → SendChannelEndpoint → UDP。
 *
 * @see io.aeron.driver.Sender
 * @see io.aeron.driver.NetworkPublication
 * @see io.aeron.driver.media.SendChannelEndpoint
 * <p>
 * 会话阶段（英文要点保留）：
 * <ul>
 * <li>校验参数；segment 就绪且 {@link Publication#isConnected()} 后再进入 REPLAY。</li>
 * <li>连接超时见 {@link Archive.Configuration#CONNECT_TIMEOUT_PROP_NAME}。</li>
 * <li>连接建立后通过 control session 异步发送 OK（含 replaySessionId）。</li>
 * <li>在 REPLAY 状态从录制文件按 term 读取 DATA 帧，写入 {@link ExclusivePublication}。</li>
 * <li>中止时进入 INACTIVE/DONE，必要时 {@link ExclusivePublication#revoke()}。</li>
 * </ul>
 */
class ReplaySession implements Session, AutoCloseable
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT, REPLAY, INACTIVE, DONE
    }

    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(READ);

    private final long connectDeadlineMs;
    private final long correlationId;
    private final long sessionId;
    private final long recordingId;
    private final long startPosition;
    private long replayPosition;
    private long stopPosition;
    private long replayLimit;
    private long segmentFileBasePosition;
    private int termBaseSegmentOffset;
    private int termOffset;
    private final int streamId;
    private final int termLength;
    private final int segmentLength;

    private final long replayBufferAddress;
    private final Checksum checksum;

    private final ExclusivePublication publication;
    private final ControlSession controlSession;
    private final CachedEpochClock epochClock;

    private final NanoClock nanoClock;
    final ArchiveConductor.Replayer replayer;
    private final File archiveDir;
    private final CountersReader countersReader;
    private final Counter limitPosition;
    private final UnsafeBuffer replayBuffer;
    private FileChannel fileChannel;
    private File segmentFile;
    private State state = State.INIT;
    private String errorMessage = null;
    private boolean revokePublication;
    private volatile boolean isAborted;

    ReplaySession(
        final long correlationId,
        final long recordingId,
        final long replayPosition,
        final long replayLength,
        final long startPosition,
        final long stopPosition,
        final int segmentFileLength,
        final int termBufferLength,
        final int streamId,
        final long replaySessionId,
        final long connectTimeoutMs,
        final ControlSession controlSession,
        final UnsafeBuffer replayBuffer,
        final File archiveDir,
        final CachedEpochClock epochClock,
        final NanoClock nanoClock,
        final ExclusivePublication publication,
        final CountersReader countersReader,
        final Counter replayLimitPosition,
        final Checksum checksum,
        final ArchiveConductor.Replayer replayer)
    {
        this.controlSession = controlSession;
        this.sessionId = replaySessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.segmentLength = segmentFileLength;
        this.termLength = termBufferLength;
        this.streamId = streamId;
        this.epochClock = epochClock;
        this.nanoClock = nanoClock;
        this.archiveDir = archiveDir;
        this.publication = publication;
        this.countersReader = countersReader;
        this.limitPosition = replayLimitPosition;
        this.replayBuffer = replayBuffer;
        this.replayBufferAddress = replayBuffer.addressOffset();
        this.checksum = checksum;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.replayer = replayer;

        segmentFileBasePosition = AeronArchive.segmentFileBasePosition(
            startPosition, replayPosition, termLength, segmentLength);
        this.replayPosition = replayPosition;
        replayLimit = replayPosition + replayLength;

        segmentFile = new File(archiveDir, segmentFileName(recordingId, segmentFileBasePosition));
        connectDeadlineMs = epochClock.time() + connectTimeoutMs;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        final CountedErrorHandler errorHandler = controlSession.archiveConductor().context().countedErrorHandler();
        if (revokePublication)
        {
            try
            {
                publication.revoke();
            }
            catch (final Exception ex)
            {
                errorHandler.onError(ex);
            }
        }
        else
        {
            CloseHelper.close(errorHandler, publication);
        }
        CloseHelper.close(errorHandler, fileChannel);
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return sessionId;
    }

    /**
     * Archive 代理线程每轮调用：INIT 阶段打开 segment、等订阅方连上 publication；REPLAY 阶段读盘并 offer 到 publication。
     * INACTIVE 时关文件并转入 DONE。
     */
    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            revokePublication = true;
            state(State.INACTIVE, "replay aborted");
        }

        try
        {
            // 等待 segment 文件出现、校验起始帧、发 OK；直到 publication 已连接才切到 REPLAY。
            if (State.INIT == state)
            {
                workCount += init();
            }

            // 从录制文件读入 replayBuffer，修补帧头后经 publication 交给 Driver，最终由 Driver 按 channel 发 UDP（若 channel 为 udp）。
            if (State.REPLAY == state)
            {
                workCount += replay();
            }
        }
        catch (final IOException ex)
        {
            raiseError(ex.toString(), ex);
        }

        if (State.INACTIVE == state)
        {
            closeRecordingSegment();
            state(State.DONE, "");
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
    {
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return state == State.DONE;
    }

    long recordingId()
    {
        return recordingId;
    }

    State state()
    {
        return state;
    }

    String replayChannel()
    {
        return publication.channel();
    }

    int replayStreamId()
    {
        return publication.streamId();
    }

    long segmentFileBasePosition()
    {
        return segmentFileBasePosition;
    }

    void sendPendingError()
    {
        if (null != errorMessage)
        {
            onPendingError(sessionId, recordingId, errorMessage);
            controlSession.sendErrorResponse(correlationId, ArchiveException.GENERIC, errorMessage);
        }
    }

    @SuppressWarnings("unused")
    void onPendingError(final long sessionId, final long recordingId, final String errorMessage)
    {
        // Hook for Agent logging
    }

    /**
     * INIT：打开当前 segment、校验 replay 起点是否与录制时 stream/term 一致；先发控制面 OK，再阻塞直到
     * {@link Publication#isConnected()}（表示至少有一个 Image/订阅方与本次 replay publication 匹配），避免无人接收仍往 term 里写。
     */
    private int init() throws IOException
    {
        if (null == fileChannel)
        {
            if (!segmentFile.exists())
            {
                if (epochClock.time() > connectDeadlineMs)
                {
                    raiseError("recording segment file not created", null);
                }

                return 0;
            }
            else
            {
                // 将全局 replayPosition 映射到 segment 内字节偏移与 term 内 offset，与录制时 log 布局一致。
                final long startTermBasePosition = startPosition - (startPosition & (termLength - 1));
                final int segmentOffset = (int)((replayPosition - startTermBasePosition) & (segmentLength - 1));
                final int termId = LogBufferDescriptor.computeTermIdFromPosition(
                    replayPosition, publication.positionBitsToShift(), publication.initialTermId());

                openRecordingSegment();

                termOffset = (int)(replayPosition & (termLength - 1));
                termBaseSegmentOffset = segmentOffset - termOffset;

                if (replayPosition > startPosition && replayPosition != stopPosition)
                {
                    // 读首个数据帧头，确认 termOffset/termId/streamId 与推导一致，避免从半截帧开始回放。
                    if (notHeaderAligned(fileChannel, replayBuffer, segmentOffset, termOffset, termId, streamId))
                    {
                        raiseError("replayPosition=" + framePosition(0) + " does not point to a valid frame", null);
                        return 0;
                    }
                }

                // 控制通道异步回复：客户端拿到 replaySessionId 后可在数据面订阅 replayChannel/replayStreamId。
                controlSession.asyncSendOkResponse(correlationId, sessionId);
            }
        }

        // 无订阅方则 publication 未连接；超时失败，避免 Driver 侧无发送目标。
        if (!publication.isConnected())
        {
            if (epochClock.time() > connectDeadlineMs)
            {
                raiseError("no connection established for replayChannel=" + publication.channel() +
                    ", replayStreamId=" + publication.streamId(), null);
            }

            return 0;
        }

        state(State.REPLAY, "");

        return 1;
    }

    /**
     * REPLAY：从 segment 文件读出与录制时相同布局的帧流，把 DATA 帧批量写入 {@link ExclusivePublication}。
     * <p>
     * 网络路径：{@code offerBlock} / {@code appendPadding} 只把数据放入本 publication 的 term；之后由 Media Driver
     * 的发送路径（若 channel 为 UDP）把帧发到订阅方。录制时的 sessionId/streamId 与当前 publication 不同，故必须在
     * 缓冲区内覆写帧头中的 sessionId、streamId，否则接收端无法按新 subscription 解析。
     */
    @SuppressWarnings("methodlength")
    private int replay() throws IOException
    {
        if (!publication.isConnected())
        {
            revokePublication = true;
            state(State.INACTIVE, "publication is not connected");
            return 0;
        }

        if (startPosition == stopPosition && 0 == replayLimit)
        {
            state(State.INACTIVE, "empty replay");
            return 0;
        }

        // 有界回放：依赖 limit counter 扩展 stopPosition；未扩展则先等待。
        if (null != limitPosition && replayPosition >= stopPosition && notExtended(replayPosition, stopPosition))
        {
            return 0;
        }

        // 当前 term 写满后推进到下一 term，必要时换下一个 segment 文件。
        if (termOffset == termLength)
        {
            nextTerm();
        }

        int workCount = 0;
        // publication 背压：窗口为 0 时不读盘，避免大块缓冲无法 offer。
        if (publication.availableWindow() > 0)
        {
            final long startNs = nanoClock.nanoTime();
            final int bytesRead = readRecording(stopPosition - replayPosition);
            if (bytesRead > 0)
            {
                // 使用「当前 replay publication」的 session/stream，替换录制文件里旧值，供接收端 Image 过滤。
                final int sessionId = publication.sessionId();
                final int streamId = publication.streamId();
                final int remaining = (int)Math.min(replayLimit - replayPosition, LogBufferDescriptor.TERM_MAX_LENGTH);
                int batchOffset = 0;
                int paddingFrameLength = 0;

                // 按帧遍历本批读入的字节：连续 DATA 帧合并一次 offerBlock；遇 PAD 则单独 appendPadding。
                while (batchOffset < bytesRead && batchOffset < remaining)
                {
                    final int frameLength = frameLength(replayBuffer, batchOffset);
                    if (frameLength <= 0)
                    {
                        raiseError("unexpected end of recording at position=" + framePosition(batchOffset), null);
                    }

                    final int frameType = frameType(replayBuffer, batchOffset);
                    final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

                    if (HDR_TYPE_DATA == frameType)
                    {
                        // 帧跨 read 边界则留到下一轮，保证 offer 的是完整帧序列。
                        if (batchOffset + alignedLength > bytesRead)
                        {
                            break;
                        }

                        if (null != checksum)
                        {
                            verifyChecksum(checksum, batchOffset, alignedLength);
                        }

                        replayBuffer.putInt(batchOffset + SESSION_ID_FIELD_OFFSET, sessionId, LITTLE_ENDIAN);
                        replayBuffer.putInt(batchOffset + STREAM_ID_FIELD_OFFSET, streamId, LITTLE_ENDIAN);
                        batchOffset += alignedLength;
                    }
                    else if (HDR_TYPE_PAD == frameType)
                    {
                        paddingFrameLength = frameLength;
                        break;
                    }
                }

                final long readTimeNs = nanoClock.nanoTime() - startNs;
                replayer.bytesRead(bytesRead);
                replayer.readTimeNs(readTimeNs);

                if (batchOffset > 0)
                {
                    // 整块拷贝进 publication term；返回 position > 0 表示已提交，Driver 随后可发送（含 UDP）。
                    final long position = publication.offerBlock(replayBuffer, 0, batchOffset);
                    if (hasPublicationAdvanced(position, batchOffset))
                    {
                        workCount++;
                    }
                    else
                    {
                        paddingFrameLength = 0;
                    }
                }

                if (paddingFrameLength > 0)
                {
                    // PAD 无 payload，用 appendPadding 占满对齐长度，保持 term 布局与流控语义。
                    final long position = publication.appendPadding(paddingFrameLength - HEADER_LENGTH);
                    if (hasPublicationAdvanced(position, align(paddingFrameLength, FRAME_ALIGNMENT)))
                    {
                        workCount++;
                    }
                }
            }
        }

        return workCount;
    }

    private String framePosition(final int frameOffset)
    {
        final long pos = segmentFileBasePosition + termBaseSegmentOffset + termOffset + frameOffset;
        return pos +
            " (segmentFilePosition=" + segmentFileBasePosition +
            ", segmentOffset=" + termBaseSegmentOffset +
            ", termOffset=" + termOffset +
            ", frameOffset=" + frameOffset + ")";
    }

    /** offerBlock/appendPadding 返回值 &gt;0 表示 term 位置前进；据此维护 replayPosition 与 term 内偏移。 */
    private boolean hasPublicationAdvanced(final long position, final int alignedLength)
    {
        if (position > 0)
        {
            termOffset += alignedLength;
            replayPosition += alignedLength;

            if (replayPosition >= replayLimit)
            {
                state(State.INACTIVE, "position (" + replayPosition + ") past limit (" + replayLimit + ")");
            }

            return true;
        }
        else if (Publication.CLOSED == position || Publication.NOT_CONNECTED == position)
        {
            revokePublication = true;
            state(State.INACTIVE, "stream closed before replay complete");
        }

        return false;
    }

    private void verifyChecksum(final Checksum checksum, final int frameOffset, final int alignedLength)
    {
        final int computedChecksum = checksum.compute(
            replayBufferAddress, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
        final int recordedChecksum = frameSessionId(replayBuffer, frameOffset);

        if (computedChecksum != recordedChecksum)
        {
            final String message = "CRC checksum mismatch at position=" + framePosition(frameOffset) +
                ": recorded checksum=" + recordedChecksum + ", computed checksum=" + computedChecksum;
            raiseError(message, null);
        }
    }

    /**
     * 从当前 segment 文件的 {@code termBaseSegmentOffset + termOffset} 起，最多读满本 term 剩余或 buffer 容量。
     * 读入的是录制时的原始 log 帧（含 data header），尚未经过 publication 发送。
     */
    private int readRecording(final long availableReplay) throws IOException
    {
        final int limit = min((int)min(availableReplay, replayBuffer.capacity()), termLength - termOffset);
        final ByteBuffer byteBuffer = replayBuffer.byteBuffer();
        byteBuffer.clear().limit(limit);

        int position = termBaseSegmentOffset + termOffset;
        do
        {
            final int bytesRead = fileChannel.read(byteBuffer, position);
            if (bytesRead <= 0)
            {
                break;
            }

            position += bytesRead;
        }
        while (byteBuffer.remaining() > 0);

        return limit;
    }

    private void raiseError(final String errorMessage, final Throwable cause)
    {
        revokePublication = true;
        this.errorMessage = errorMessage +
            ", recordingId=" + recordingId +
            ", replaySessionId=" + sessionId +
            ", segmentFile=" + segmentFileName(recordingId, segmentFileBasePosition);
        state(State.INACTIVE, errorMessage);
        throw new ArchiveException(this.errorMessage, cause, ArchiveException.GENERIC);
    }

    private boolean notExtended(final long replayPosition, final long oldStopPosition)
    {
        final Counter limitPosition = this.limitPosition;
        final long currentLimitPosition = limitPosition.get();
        long newStopPosition = oldStopPosition;

        if (limitPosition.isClosed())
        {
            if (countersReader.getCounterRegistrationId(limitPosition.id()) == limitPosition.registrationId())
            {
                replayLimit = currentLimitPosition;
                newStopPosition = Math.max(oldStopPosition, currentLimitPosition);
            }
            else if (replayLimit >= oldStopPosition)
            {
                replayLimit = oldStopPosition;
            }
        }
        else
        {
            newStopPosition = currentLimitPosition;
        }

        if (replayPosition >= replayLimit)
        {
            state(State.INACTIVE, "position (" + replayPosition + ") past limit (" + replayLimit + ") (notExtended)");
        }
        else if (newStopPosition > oldStopPosition)
        {
            stopPosition = newStopPosition;
            return false;
        }

        return true;
    }

    /** 当前 term 已满：segment 内推进 term；若跨 segment 则关闭当前文件并打开下一段 segment。 */
    private void nextTerm() throws IOException
    {
        termOffset = 0;
        termBaseSegmentOffset += termLength;

        if (termBaseSegmentOffset == segmentLength)
        {
            closeRecordingSegment();
            segmentFileBasePosition += segmentLength;
            openRecordingSegment();
            termBaseSegmentOffset = 0;
        }
    }

    private void closeRecordingSegment()
    {
        CloseHelper.close(fileChannel);
        fileChannel = null;
        segmentFile = null;
    }

    /** 按 recordingId + segment 起始 position 打开磁盘上的录制 segment（只读 mmap/文件通道）。 */
    private void openRecordingSegment() throws IOException
    {
        if (null == segmentFile)
        {
            final String segmentFileName = segmentFileName(recordingId, segmentFileBasePosition);
            segmentFile = new File(archiveDir, segmentFileName);

            if (!segmentFile.exists())
            {
                raiseError("recording segment not found", null);
                return;
            }
        }

        fileChannel = FileChannel.open(segmentFile.toPath(), FILE_OPTIONS);
    }

    static boolean notHeaderAligned(
        final FileChannel channel,
        final UnsafeBuffer buffer,
        final int segmentOffset,
        final int termOffset,
        final int termId,
        final int streamId) throws IOException
    {
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.clear().limit(HEADER_LENGTH);
        if (HEADER_LENGTH != channel.read(byteBuffer, segmentOffset))
        {
            throw new IOException("failed to read fragment header");
        }

        return isInvalidHeader(buffer, streamId, termId, termOffset);
    }

    private void state(final State newState, final String reason)
    {
        logStateChange(state, newState, sessionId, recordingId, replayPosition, null == reason ? "" : reason);
        state = newState;
    }

    @SuppressWarnings("unused")
    private void logStateChange(
        final State oldState,
        final State newState,
        final long sessionId,
        final long recordingId,
        final long position,
        final String reason)
    {
        //System.out.println("ReplaySession: " + state + " -> " + newState);
    }

    static boolean isInvalidHeader(
        final UnsafeBuffer buffer, final int streamId, final int termId, final int termOffset)
    {
        return
            termOffset(buffer, 0) != termOffset ||
            termId(buffer, 0) != termId ||
            streamId(buffer, 0) != streamId;
    }
}

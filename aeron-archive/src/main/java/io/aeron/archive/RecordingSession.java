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
import io.aeron.archive.client.ArchiveException;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.concurrent.CountedErrorHandler;

import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Consumes an {@link Image} and records data to file using a {@link RecordingWriter}.
 */
class RecordingSession implements Session
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT, RECORDING, INACTIVE, STOPPED
    }

    private final boolean isAutoStop;
    private volatile boolean isAborted = false;
    private final long correlationId;
    private final long recordingId;
    private long progressEventPosition;
    private final int blockLengthLimit;
    private final RecordingEventsProxy recordingEventsProxy;
    private final Image image;
    private final Counter position;
    private final RecordingWriter recordingWriter;
    private final String originalChannel;
    private final ControlSession controlSession;
    private final CountedErrorHandler countedErrorHandler;
    private State state = State.INIT;
    private String errorMessage;
    private String abortReason;
    private int errorCode = ArchiveException.GENERIC;

    RecordingSession(
        final long correlationId,
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final String originalChannel,
        final RecordingEventsProxy recordingEventsProxy,
        final Image image,
        final Counter position,
        final Archive.Context ctx,
        final ControlSession controlSession,
        final boolean isAutoStop,
        final ArchiveConductor.Recorder recorder)
    {
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.originalChannel = originalChannel;
        this.recordingEventsProxy = recordingEventsProxy;
        this.image = image;
        this.position = position;
        this.controlSession = controlSession;
        this.isAutoStop = isAutoStop;
        this.countedErrorHandler = ctx.countedErrorHandler();
        this.progressEventPosition = image.joinPosition();

        blockLengthLimit = Math.min(image.termBufferLength(), ctx.fileIoMaxLength());
        recordingWriter = new RecordingWriter(recordingId, startPosition, segmentLength, image, ctx, recorder);
    }

    /**
     * {@inheritDoc}
     */
    public long sessionId()
    {
        return recordingId;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isDone()
    {
        return state == State.STOPPED;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(final String reason)
    {
        abortReason = reason;
        isAborted = true;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        recordingWriter.close();
        CloseHelper.close(countedErrorHandler, position);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (isAborted)
        {
            state(State.INACTIVE, abortReason);
        }

        if (State.INIT == state)
        {
            workCount += init();
        }

        if (State.RECORDING == state)
        {
            workCount += record();
        }

        if (State.INACTIVE == state)
        {
            state(State.STOPPED, "");
            recordingWriter.close();
            workCount++;

            if (null != recordingEventsProxy)
            {
                recordingEventsProxy.stopped(recordingId, image.joinPosition(), position.getPlain());
            }
        }

        return workCount;
    }

    void abortClose()
    {
        recordingWriter.close();
    }

    long correlationId()
    {
        return correlationId;
    }

    Counter recordingPosition()
    {
        return position;
    }

    long recordedPosition()
    {
        if (position.isClosed())
        {
            return NULL_POSITION;
        }

        return position.get();
    }

    Subscription subscription()
    {
        return image.subscription();
    }

    ControlSession controlSession()
    {
        return controlSession;
    }

    boolean isAutoStop()
    {
        return isAutoStop;
    }

    void sendPendingError()
    {
        if (null != errorMessage)
        {
            controlSession.sendErrorResponse(correlationId, errorCode, errorMessage);
        }
    }

    private int init()
    {
        try
        {
            recordingWriter.init();
        }
        catch (final Exception ex)
        {
            errorMessage = ex.getClass().getName() + ": " + ex.getMessage();
            recordingWriter.close();
            state(State.STOPPED, errorMessage);
            LangUtil.rethrowUnchecked(ex);
        }

        if (null != recordingEventsProxy)
        {
            recordingEventsProxy.started(
                recordingId,
                image.joinPosition(),
                image.sessionId(),
                image.subscription().streamId(),
                originalChannel,
                image.sourceIdentity());
        }
        state(State.RECORDING, "");

        return 1;
    }

    /**
     * 录制态下每轮 {@link ArchiveConductor.Recorder#doWork()} 调用一次：从 {@link Image} 的 term buffer
     * 批量取出「连续完整 frame」组成的块，交给 {@link RecordingWriter}（实现 {@link io.aeron.logbuffer.BlockHandler}）落盘。
     * <p>
     * <b>{@link Image#blockPoll(io.aeron.logbuffer.BlockHandler, int)} 原理概要}</b>（实现见 aeron-client {@code Image.java}）：
     * <ol>
     *     <li>用 {@link Image} 的 subscriber position 定位当前 term 内偏移，计算本次最多扫描上界
     *         {@code min(offset + blockLengthLimit, term 末尾)}；{@code blockLengthLimit} 在构造时为
     *         {@code min(termBufferLength, ctx.fileIoMaxLength())}，避免单次 I/O 过大。</li>
     *     <li>{@link io.aeron.logbuffer.TermBlockScanner#scan} 从 offset 起顺序读各 frame 的 length，
     *         只拼接<strong>完整 frame</strong>；遇 padding frame 或半帧超出 limit 则停止，保证回调拿到的是合法块。</li>
     *     <li>若有可读字节：调用 {@code handler.onBlock(termBuffer, offset, length, sessionId, termId)}，
     *         此处 handler 即 {@link RecordingWriter#onBlock}，将块写入 segment 文件；最后在 {@code finally} 里
     *         把 subscriber position 前进 {@code length}，与逐条 {@link Image#poll} 一样参与流控。</li>
     *     <li>返回值为本轮消费的<strong>字节数</strong>（非条数）；0 表示当前没有新数据或 Image 已关闭。</li>
     * </ol>
     */
    private int record()
    {
        try
        {
            // blockPoll：TermBlockScanner 扫描 term buffer → RecordingWriter.onBlock 写盘 → 推进 subscriber position
            final int workCount = image.blockPoll(recordingWriter, blockLengthLimit);
            if (workCount > 0)
            {
                // RecordingPos 计数器：与 catalog / 客户端查询的「已录制位置」对齐（writer 内维护 stream position）
                this.position.setRelease(recordingWriter.position());
            }
            else if (image.isEndOfStream() || image.isClosed())
            {
                // 无新数据且流结束或 Image 关闭：结束录制会话，doWork 后续将进入 INACTIVE → STOPPED
                state(
                    State.INACTIVE,
                    "image.isEndOfStream=" + image.isEndOfStream() + ", image.isClosed=" + image.isClosed());
            }

            if (null != recordingEventsProxy)
            {
                final long recordedPosition = recordingWriter.position();
                if (progressEventPosition < recordedPosition)
                {
                    if (recordingEventsProxy.progress(recordingId, image.joinPosition(), recordedPosition))
                    {
                        progressEventPosition = recordedPosition;
                    }
                }
            }

            return workCount;
        }
        catch (final ArchiveException ex)
        {
            countedErrorHandler.onError(ex);
            errorMessage = ex.getMessage();
            errorCode = ex.errorCode();
            state(State.INACTIVE, errorMessage);
        }
        catch (final Exception ex)
        {
            countedErrorHandler.onError(ex);
            errorMessage = ex.getClass().getName() + ": " + ex.getMessage();
            state(State.INACTIVE, errorMessage);
        }

        return 1;
    }

    private void state(final State newState, final String reason)
    {
        logStateChange(
            state,
            newState,
            recordingId,
            null != image ? image.position() : NULL_POSITION,
            null == reason ? "" : reason);
        state = newState;
    }

    @SuppressWarnings("unused")
    private void logStateChange(
        final State oldState,
        final State newState,
        final long recordingId,
        final long position,
        final String reason)
    {
        //System.out.println("RecordingSession: " + state + " -> " + newState);
    }
}

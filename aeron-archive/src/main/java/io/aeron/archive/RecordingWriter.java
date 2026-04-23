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

import io.aeron.Image;
import io.aeron.archive.checksum.Checksum;
import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.StorageSpaceException;
import io.aeron.logbuffer.BlockHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;

import static io.aeron.archive.client.AeronArchive.segmentFileBasePosition;
import static io.aeron.logbuffer.FrameDescriptor.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;

/**
 * Responsible for writing out a recording into the file system. A recording has descriptor file and a set of data files
 * written into the archive folder.
 * <p>
 * <b>Design note:</b> While this class is notionally closely related to the {@link RecordingSession} it is separated
 * from it for the following reasons:
 * <ul>
 * <li>Easier testing and in particular simplified re-use in testing.</li>
 * <li>Isolation of an external relationship, namely the file system.</li>
 * </ul>
 */
final class RecordingWriter implements BlockHandler, AutoCloseable
{
    private final long recordingId;
    private final int segmentLength;
    private final boolean forceWrites;
    private final boolean forceMetadata;
    private final UnsafeBuffer checksumBuffer;
    private final Checksum checksum;
    private final FileChannel archiveDirChannel;
    private final File archiveDir;
    private final CountedErrorHandler countedErrorHandler;
    private final NanoClock nanoClock;
    private final Archive.Context ctx;

    private final ArchiveConductor.Recorder recorder;

    private long segmentBasePosition;
    private int segmentOffset;
    private FileChannel recordingFileChannel;

    private boolean isClosed = false;

    RecordingWriter(
        final long recordingId,
        final long startPosition,
        final int segmentLength,
        final Image image,
        final Archive.Context ctx,
        final ArchiveConductor.Recorder recorder)
    {
        this.recordingId = recordingId;
        this.archiveDirChannel = ctx.archiveDirChannel();
        this.segmentLength = segmentLength;

        archiveDir = ctx.archiveDir();
        forceWrites = ctx.fileSyncLevel() > 0;
        forceMetadata = ctx.fileSyncLevel() > 1;

        countedErrorHandler = ctx.countedErrorHandler();
        checksumBuffer = ctx.recordChecksumBuffer();
        checksum = ctx.recordChecksum();
        nanoClock = ctx.nanoClock();
        this.ctx = ctx;
        this.recorder = recorder;

        final int termLength = image.termBufferLength();
        final long joinPosition = image.joinPosition();
        segmentBasePosition = segmentFileBasePosition(startPosition, joinPosition, termLength, segmentLength);
        segmentOffset = (int)(joinPosition - segmentBasePosition);
    }

    /**
     * {@link io.aeron.Image#blockPoll} / {@link io.aeron.Subscription#blockPoll} 在扫出一段连续 term 字节后调用；
     * 本方法把该块<strong>原样（或带可选校验增强）</strong>写入当前 segment 文件，并推进逻辑 position。
     * <p>
     * 参数与块语义（与 {@link io.aeron.logbuffer.BlockHandler} 一致）：
     * <ul>
     *     <li>{@code termBuffer}/{@code termOffset}/{@code length}：当前 term 内一块数据，含各 frame 头，
     *         已由 {@link io.aeron.logbuffer.TermBlockScanner} 保证边界落在完整 frame 上。</li>
     *     <li>{@code sessionId}/{@code termId}：块内首帧所属流会话与 term；落盘时若未启用 checksum 则不再单独使用，
     *         数据已在 buffer 中。</li>
     * </ul>
     * <p>
     * Padding frame：块可能仅为 padding；此时只保证头有效，实际写入长度按 {@link io.aeron.protocol.DataHeaderFlyweight#HEADER_LENGTH}，
     * 但 {@code segmentOffset} 仍按本块完整 {@code length} 增加，与 stream position 一致。
     * <p>
     * {@link #position()} = {@code segmentBasePosition + segmentOffset}，供 {@link RecordingSession} 更新 {@link io.aeron.archive.status.RecordingPos}。
     */
    public void onBlock(
        final DirectBuffer termBuffer, final int termOffset, final int length, final int sessionId, final int termId)
    {
        try
        {
            // 块首帧类型：padding 只落盘头长度（块内可能声明更大范围，Archive 与 TermBlockScanner 约定一致）
            final boolean isPaddingFrame = termBuffer.getShort(typeOffset(termOffset)) == PADDING_FRAME_TYPE;
            final int dataLength = isPaddingFrame ? HEADER_LENGTH : length;
            final ByteBuffer byteBuffer;

            final long startNs = nanoClock.nanoTime();
            if (null == checksum || isPaddingFrame)
            {
                // 零拷贝视图：直接映射到底层 mmap 的 ByteBuffer，从 term 写入文件通道
                byteBuffer = termBuffer.byteBuffer();
                byteBuffer.limit(termOffset + dataLength).position(termOffset);
            }
            else
            {
                // 拷贝到专用 buffer，逐帧计算校验并写回帧头后再写盘（避免改共享 term 映射）
                checksumBuffer.putBytes(0, termBuffer, termOffset, dataLength);
                computeChecksum(checksum, checksumBuffer, dataLength);
                byteBuffer = checksumBuffer.byteBuffer();
                byteBuffer.limit(dataLength).position(0);
            }

            // 当前 segment 文件内偏移；续写同一 segment（可能跨多次 blockPoll）
            int fileOffset = segmentOffset;
            do
            {
                fileOffset += recordingFileChannel.write(byteBuffer, fileOffset);
            }
            while (byteBuffer.remaining() > 0);

            if (forceWrites)
            {
                // fileSyncLevel：刷盘保证崩溃后可见性；forceMetadata 是否连元数据一起 fsync
                recordingFileChannel.force(forceMetadata);
            }

            final long writeTimeNs = nanoClock.nanoTime() - startNs;
            recorder.bytesWritten(dataLength);
            recorder.writeTimeNs(writeTimeNs);

            // 逻辑 stream position 前进整块长度（含 padding 全长），与 Image subscriber position 推进一致
            segmentOffset += length;
            if (segmentOffset >= segmentLength)
            {
                // 单文件达到 segmentLength：关旧文件、抬高 segmentBasePosition、开新 segment 文件
                onFileRollOver();
            }
        }
        catch (final ClosedByInterruptException ex)
        {
            close();
            throw new ArchiveException("file closed by interrupt, recording aborted", ex, ArchiveException.GENERIC);
        }
        catch (final IOException ex)
        {
            close();
            checkErrorType(ex, length);
        }
        catch (final Exception ex)
        {
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.close(countedErrorHandler, recordingFileChannel);
        }
    }

    long position()
    {
        return segmentBasePosition + segmentOffset;
    }

    void init() throws IOException
    {
        openRecordingSegmentFile(new File(archiveDir, Archive.segmentFileName(recordingId, segmentBasePosition)));

        if (segmentOffset != 0)
        {
            recordingFileChannel.position(segmentOffset);
        }
    }

    private void computeChecksum(final Checksum checksum, final UnsafeBuffer buffer, final int length)
    {
        final long address = buffer.addressOffset();
        int frameOffset = 0;

        while (frameOffset < length)
        {
            final int alignedLength = align(frameLength(buffer, frameOffset), FRAME_ALIGNMENT);
            final int computedChecksum = checksum.compute(
                address, frameOffset + HEADER_LENGTH, alignedLength - HEADER_LENGTH);
            frameSessionId(buffer, frameOffset, computedChecksum);
            frameOffset += alignedLength;
        }
    }

    private void openRecordingSegmentFile(final File segmentFile)
    {
        RandomAccessFile recordingFile = null;
        try
        {
            recordingFile = new RandomAccessFile(segmentFile, "rw");
            recordingFile.setLength(segmentLength);
            recordingFileChannel = recordingFile.getChannel();
            if (forceWrites && null != archiveDirChannel)
            {
                archiveDirChannel.force(forceMetadata);
            }
        }
        catch (final IOException ex)
        {
            CloseHelper.close(recordingFile);
            close();
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void onFileRollOver()
    {
        CloseHelper.close(recordingFileChannel);
        segmentOffset = 0;
        segmentBasePosition += segmentLength;

        final File file = new File(archiveDir, Archive.segmentFileName(recordingId, segmentBasePosition));
        if (file.exists())
        {
            throw new ArchiveException("segment file already exists: " + file);
        }

        openRecordingSegmentFile(file);
    }

    private void checkErrorType(final IOException ex, final int writeLength)
    {
        boolean isLowStorageSpace = false;
        IOException suppressed = null;

        try
        {
            isLowStorageSpace = StorageSpaceException.isStorageSpaceError(ex) ||
                ctx.archiveFileStore().getUsableSpace() < writeLength;
        }
        catch (final IOException ex2)
        {
            suppressed = ex2;
        }

        final int errorCode = isLowStorageSpace ? ArchiveException.STORAGE_SPACE : ArchiveException.GENERIC;
        final ArchiveException error = new ArchiveException("java.io.IOException - " + ex.getMessage(), ex, errorCode);

        if (null != suppressed)
        {
            error.addSuppressed(suppressed);
        }

        throw error;
    }
}

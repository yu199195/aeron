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
package io.aeron.driver.buffer;

import io.aeron.exceptions.AeronException;
import org.agrona.BufferUtil;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static io.aeron.logbuffer.LogBufferDescriptor.*;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;
import static org.agrona.BitUtil.align;

/**
 * 【LogBuffer 文件的实际创建和 mmap 映射】
 * 在构造函数中完成以下工作：
 * <pre>
 *   1. FileChannel.open(CREATE_NEW) → 在磁盘上创建新文件
 *   2. truncate(logLength)          → 设置文件大小（3 × termLength + metadata）
 *   3. FileChannel.map(READ_WRITE)  → mmap 映射到进程虚拟内存
 *   4. 将映射区域切分为 3 个 termBuffer + 1 个 logMetaDataBuffer
 * </pre>
 * 文件布局：
 * <pre>
 *   ┌─────────────────────┐  offset 0
 *   │ Term Buffer 0       │  大小 = termLength（默认 16MB）
 *   ├─────────────────────┤  offset termLength
 *   │ Term Buffer 1       │  大小 = termLength
 *   ├─────────────────────┤  offset 2 × termLength
 *   │ Term Buffer 2       │  大小 = termLength
 *   ├─────────────────────┤  offset 3 × termLength
 *   │ Log Metadata Buffer │  大小 = LOG_META_DATA_LENGTH
 *   └─────────────────────┘
 * </pre>
 * Client 端收到 PUBLICATION_READY 后，用同一个文件路径调用 logBuffersFactory.map()
 * 做 mmap 映射，从而与 Driver 共享同一块物理内存，实现零拷贝通信。
 */
class MappedRawLog implements RawLog
{
    private static final int ONE_GIG = 1 << 30;
    private static final EnumSet<StandardOpenOption> FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE);
    private static final EnumSet<StandardOpenOption> SPARSE_FILE_OPTIONS = EnumSet.of(CREATE_NEW, READ, WRITE, SPARSE);

    private final int termLength;
    private final long logLength;
    private final UnsafeBuffer[] termBuffers = new UnsafeBuffer[PARTITION_COUNT];
    private final UnsafeBuffer logMetaDataBuffer;
    private final ErrorHandler errorHandler;
    private final AtomicCounter mappedBytesCounter;
    private File logFile;
    private MappedByteBuffer[] mappedBuffers;

    MappedRawLog(
        final File location,
        final boolean useSparseFiles,
        final long logLength,
        final int termLength,
        final int filePageSize,
        final ErrorHandler errorHandler,
        final AtomicCounter mappedBytesCounter)
    {
        this.termLength = termLength;
        this.errorHandler = errorHandler;
        this.logFile = location;
        this.logLength = logLength;
        this.mappedBytesCounter = mappedBytesCounter;

        // 稀疏文件模式（SPARSE）：不预分配物理页面，首次写入时才触发分配，节省初始内存
        final EnumSet<StandardOpenOption> options = useSparseFiles ? SPARSE_FILE_OPTIONS : FILE_OPTIONS;
        try
        {
            // ★ 关键：CREATE_NEW 标志确保创建全新文件；如果已存在则抛异常
            try (FileChannel logChannel = FileChannel.open(logFile.toPath(), options))
            {
                // 设置文件大小（此时文件内容全为 0）
                logChannel.truncate(logLength);
                if (logLength <= Integer.MAX_VALUE)
                {
                    // 文件 ≤ 2GB：整个文件映射为一个 MappedByteBuffer
                    final MappedByteBuffer mappedBuffer = logChannel.map(READ_WRITE, 0, logLength);
                    mappedBuffer.order(ByteOrder.LITTLE_ENDIAN);
                    mappedBuffers = new MappedByteBuffer[]{ mappedBuffer };

                    // 将映射区域切分为 3 个 Term Buffer（PARTITION_COUNT = 3）
                    for (int i = 0; i < PARTITION_COUNT; i++)
                    {
                        termBuffers[i] = new UnsafeBuffer(mappedBuffer, i * termLength, termLength);
                    }

                    // 文件末尾是 Log Metadata Buffer（存放 activeTermCount、tailPosition 等元数据）
                    logMetaDataBuffer = new UnsafeBuffer(
                        mappedBuffer, (int)(logLength - LOG_META_DATA_LENGTH), LOG_META_DATA_LENGTH);
                }
                else
                {
                    // 文件 > 2GB：每个 term 单独映射（MappedByteBuffer 最大 Integer.MAX_VALUE）
                    mappedBuffers = new MappedByteBuffer[PARTITION_COUNT + 1];

                    for (int i = 0; i < PARTITION_COUNT; i++)
                    {
                        final MappedByteBuffer buffer = logChannel.map(READ_WRITE, termLength * (long)i, termLength);
                        buffer.order(ByteOrder.LITTLE_ENDIAN);
                        mappedBuffers[i] = buffer;
                        termBuffers[i] = new UnsafeBuffer(buffer, 0, termLength);
                    }

                    final int metaDataMappingLength = align(LOG_META_DATA_LENGTH, filePageSize);
                    final long metaDataSectionOffset = termLength * (long)PARTITION_COUNT;

                    final MappedByteBuffer metaDataMappedBuffer = logChannel.map(
                        READ_WRITE, metaDataSectionOffset, metaDataMappingLength);
                    metaDataMappedBuffer.order(ByteOrder.LITTLE_ENDIAN);

                    mappedBuffers[LOG_META_DATA_SECTION_INDEX] = metaDataMappedBuffer;
                    logMetaDataBuffer = new UnsafeBuffer(
                        metaDataMappedBuffer,
                        metaDataMappingLength - LOG_META_DATA_LENGTH,
                        LOG_META_DATA_LENGTH);
                }

                if (!useSparseFiles)
                {
                    // 非稀疏文件模式：预触摸所有页面，迫使 OS 分配物理内存，
                    // 避免后续首次写入时产生 page fault 导致延迟抖动
                    preTouchPages(termBuffers, termLength, filePageSize);
                }

                // 更新全局计数器：Driver 总共映射了多少字节的 LogBuffer
                mappedBytesCounter.getAndAddRelease(logLength);
            }
        }
        catch (final IOException ex)
        {
            IoUtil.delete(logFile, true);
            throw new UncheckedIOException(ex);
        }
    }

    public int termLength()
    {
        return termLength;
    }

    public boolean free()
    {
        final MappedByteBuffer[] mappedBuffers = this.mappedBuffers;
        if (null != mappedBuffers)
        {
            this.mappedBuffers = null;
            for (int i = 0; i < mappedBuffers.length; i++)
            {
                BufferUtil.free(mappedBuffers[i]);
            }

            mappedBytesCounter.getAndAddRelease(-logLength);

            logMetaDataBuffer.wrap(0, 0);
            for (int i = 0; i < termBuffers.length; i++)
            {
                termBuffers[i].wrap(0, 0);
            }
        }

        if (null != logFile)
        {
            if (!logFile.delete() && logFile.exists())
            {
                return false;
            }

            logFile = null;
        }

        return true;
    }

    public void close()
    {
        if (!free())
        {
            errorHandler.onError(new AeronException("unable to delete " + logFile, AeronException.Category.WARN));
        }
    }

    public UnsafeBuffer[] termBuffers()
    {
        return termBuffers;
    }

    public UnsafeBuffer metaData()
    {
        return logMetaDataBuffer;
    }

    public ByteBuffer[] sliceTerms()
    {
        final ByteBuffer[] terms = new ByteBuffer[PARTITION_COUNT];

        if (termLength < ONE_GIG)
        {
            final MappedByteBuffer buffer = mappedBuffers[0];
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                buffer.limit((termLength * i) + termLength).position(termLength * i);
                terms[i] = buffer.slice();
            }
        }
        else
        {
            for (int i = 0; i < PARTITION_COUNT; i++)
            {
                terms[i] = mappedBuffers[i].duplicate();
            }
        }

        return terms;
    }

    public String fileName()
    {
        return logFile.getAbsolutePath();
    }

    private static void preTouchPages(final UnsafeBuffer[] buffers, final int length, final int pageSize)
    {
        for (final UnsafeBuffer buffer : buffers)
        {
            for (long i = 0; i < length; i += pageSize)
            {
                buffer.putByte((int)i, (byte)0);
            }
        }
    }
}

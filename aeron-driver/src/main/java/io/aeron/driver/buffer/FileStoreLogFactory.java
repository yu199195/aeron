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
import io.aeron.exceptions.StorageSpaceException;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;

import static io.aeron.logbuffer.LogBufferDescriptor.computeLogLength;

/**
 * LogBuffer 文件工厂：负责在磁盘上创建 Publication / Image 的 LogBuffer 文件。
 * <p>
 * 文件目录结构：
 * <pre>
 *   {aeronDir}/
 *     ├── publications/         ← Publication 的 LogBuffer 文件
 *     │   ├── {correlationId}.logbuffer
 *     │   └── ...
 *     └── images/               ← Subscription Image 的 LogBuffer 文件
 *         ├── {correlationId}.logbuffer
 *         └── ...
 * </pre>
 * 创建时机：Driver 处理 ADD_PUBLICATION 命令时，在 DriverConductor.newNetworkPublication()
 * 或 newIpcPublication() 中调用 logFactory.newPublication()。
 * <p>
 * aeronDir 在 Linux 默认为 /dev/shm/aeron-{user}（tmpfs 内存文件系统），
 * 这使得 mmap 映射后的读写实际上直接操作物理内存，避免磁盘 I/O。
 */
public class FileStoreLogFactory implements LogFactory
{
    private static final String PUBLICATIONS = "publications";
    private static final String IMAGES = "images";

    private final long lowStorageWarningThreshold;
    private final int filePageSize;
    private final boolean checkStorage;
    private final ErrorHandler errorHandler;
    private final File publicationsDir;
    private final File imagesDir;
    private final FileStore fileStore;
    private final AtomicCounter mappedBytesCounter;

    /**
     * Construct a {@link LogFactory} over a file store.
     *
     * @param dataDirectoryName          where the log buffers will be created.
     * @param filePageSize               of the filesystem.
     * @param checkStorage               for sufficient space before allocating files.
     * @param lowStorageWarningThreshold when warnings about remaining space will begin.
     * @param errorHandler               to call when an error is encountered.
     * @param mappedBytesCounter         used to keep track of how many bytes are mapped by the driver.
     */
    public FileStoreLogFactory(
        final String dataDirectoryName,
        final int filePageSize,
        final boolean checkStorage,
        final long lowStorageWarningThreshold,
        final ErrorHandler errorHandler,
        final AtomicCounter mappedBytesCounter)
    {
        this.filePageSize = filePageSize;
        this.lowStorageWarningThreshold = lowStorageWarningThreshold;
        this.checkStorage = checkStorage;
        this.errorHandler = errorHandler;
        this.mappedBytesCounter = mappedBytesCounter;

        final File dataDir = new File(dataDirectoryName);

        publicationsDir = new File(dataDir, PUBLICATIONS);
        imagesDir = new File(dataDir, IMAGES);

        IoUtil.ensureDirectoryExists(publicationsDir, PUBLICATIONS);
        IoUtil.ensureDirectoryExists(imagesDir, IMAGES);

        try
        {
            fileStore = checkStorage ? Files.getFileStore(dataDir.toPath()) : null;
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
    }

    /**
     * 【LogBuffer 文件创建入口】在 publications/ 目录下为新 Publication 创建 LogBuffer 文件。
     * <p>
     * 调用链路：
     * <pre>
     *   DriverConductor.newNetworkPublication() / addIpcPublication()
     *     → newNetworkPublicationLog() / newIpcPublicationLog()
     *       → logFactory.newPublication(correlationId, termLength, isSparse)  ← 就是这里
     *         → newInstance(publicationsDir, ...)
     *           → new MappedRawLog(location, ...)  ← 实际创建文件并 mmap
     * </pre>
     * 生成的文件路径为：{aeronDir}/publications/{correlationId}.logbuffer
     * <p>
     * 文件大小 = termLength × 3 + LOG_META_DATA_LENGTH（默认 16MB × 3 + metadata ≈ 48MB+）
     *
     * @param correlationId    Publication 的注册 ID，用作文件名
     * @param termBufferLength 每个 term buffer 的长度（默认 16MB）
     * @param useSparseFiles   是否使用稀疏文件（延迟分配物理页面）
     * @return 新创建并已 mmap 映射的 {@link RawLog}
     */
    public RawLog newPublication(final long correlationId, final int termBufferLength, final boolean useSparseFiles)
    {
        return newInstance(publicationsDir, correlationId, termBufferLength, useSparseFiles);
    }

    /**
     * Create new {@link RawLog} in the rebuilt publication images directory for the supplied triplet.
     *
     * @param correlationId    to use to distinguish this connection
     * @param termBufferLength to use for the log buffer
     * @param useSparseFiles   for the log buffer.
     * @return the newly allocated {@link RawLog}
     */
    public RawLog newImage(final long correlationId, final int termBufferLength, final boolean useSparseFiles)
    {
        return newInstance(imagesDir, correlationId, termBufferLength, useSparseFiles);
    }

    private RawLog newInstance(
        final File rootDir,
        final long correlationId,
        final int termLength,
        final boolean useSparseFiles)
    {
        // 计算文件总长度：3 个 term buffer + log metadata，按 filePageSize 对齐
        final long logLength = computeLogLength(termLength, filePageSize);
        // 检查磁盘剩余空间是否足够
        checkStorage(logLength);

        // 文件路径：{rootDir}/{correlationId}.logbuffer
        final File location = streamLocation(rootDir, correlationId);

        // 创建文件并做 mmap 映射（FileChannel.open → truncate → map），详见 MappedRawLog 构造函数
        return new MappedRawLog(
            location, useSparseFiles, logLength, termLength, filePageSize, errorHandler, mappedBytesCounter);
    }

    private void checkStorage(final long logLength)
    {
        if (checkStorage)
        {
            final long usableSpace = getUsableSpace();

            if (usableSpace < logLength)
            {
                throw new StorageSpaceException(
                    "insufficient usable storage for new log of length=" + logLength + " usable=" + usableSpace +
                    " in " + fileStore);
            }

            if (usableSpace <= lowStorageWarningThreshold)
            {
                final String msg =
                    "space is running low: threshold=" + lowStorageWarningThreshold +
                    " usable=" + usableSpace + " in " + fileStore;

                errorHandler.onError(new AeronException(msg, AeronException.Category.WARN));
            }
        }
    }

    private long getUsableSpace()
    {
        long usableSpace = 0;

        try
        {
            usableSpace = fileStore.getUsableSpace();
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return usableSpace;
    }

    private static File streamLocation(final File rootDir, final long correlationId)
    {
        final String fileName = correlationId + ".logbuffer";

        return new File(rootDir, fileName);
    }
}

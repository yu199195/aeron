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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 带 Archive 录制功能的发送端 Demo。整体流程：
 * <ol>
 *     <li><b>发送 + 录制</b>：从控制台读取输入，通过 UDP 发送给接收端，
 *     同时 Archive 以 {@link SourceLocation#LOCAL} spy 方式自动录制所有消息到本地磁盘。</li>
 *     <li><b>保持 Archive 服务</b>：发送结束后，Archive 服务继续运行，
 *     等待 {@link ArchiveReceiver} 通过 Archive Client 连接并请求回放。</li>
 * </ol>
 * <p>
 * 使用嵌入式 {@link ArchivingMediaDriver}（MediaDriver + Archive 一体化）。
 * Archive 对外暴露 UDP 控制通道（默认 {@code localhost:8010}），
 * 供远端的 {@link AeronArchive} 客户端连接、查询录制、请求回放等。
 * <p>
 * 回放时 Archive 会读取磁盘上的录制数据，以新的 ExclusivePublication 通过 UDP 发送给请求方。
 * 整个回放链路：Archive 磁盘 → sender MediaDriver → UDP → receiver MediaDriver → Subscription。
 * <p>
 * 使用方式：
 * <ol>
 *     <li>启动本程序，输入消息后按回车发送</li>
 *     <li>Ctrl+D 结束发送（Archive 服务保持运行）</li>
 *     <li>启动 {@link ArchiveReceiver}，它会连接 Archive 请求回放</li>
 *     <li>Ctrl+C 彻底关闭本程序</li>
 * </ol>
 */
public class ArchiveSender
{
    public static final String CHANNEL = "aeron:udp?endpoint=localhost:40456";
    public static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    public static final String CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8010";
    public static final String CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int STREAM_ID = 10;
    private static final int MAX_MESSAGE_BYTES = 1024;

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     * @throws InterruptedException if the thread sleep delay is interrupted.
     */
    @SuppressWarnings("try")
    public static void main(final String[] args) throws InterruptedException
    {
        System.out.println("========================================");
        System.out.println("  ArchiveSender 发送端（录制 + Archive 服务）");
        System.out.println("  数据通道: " + CHANNEL + "  streamId=" + STREAM_ID);
        System.out.println("  Archive 控制: localhost:8010 (默认)");
        System.out.println("========================================");
        System.out.println("  输入内容后按回车发送，支持中文。");
        System.out.println("  Ctrl+D 结束发送（Archive 保持运行等待回放请求）");
        System.out.println("  Ctrl+C 彻底关闭");
        System.out.println("========================================");
        System.out.println();

        final AtomicBoolean running = new AtomicBoolean(true);
        final UnsafeBuffer buffer = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(MAX_MESSAGE_BYTES, BitUtil.CACHE_LINE_LENGTH));

        final File archiveDir = new File(System.getProperty("java.io.tmpdir"), "aeron-archive-sender");

        final MediaDriver.Context driverCtx = new MediaDriver.Context();
        driverCtx
            .aeronDirectoryName(driverCtx.aeronDirectoryName() + "-archive-sender")
            .spiesSimulateConnection(true)
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(Throwable::printStackTrace)
            .dirDeleteOnStart(true);

        /*
         * controlChannel：Archive 服务监听的控制通道，供 ArchiveReceiver 的 AeronArchive Client 连接。
         * 代码中无默认值，必须显式设置，否则 Archive 启动时会抛 ConfigurationException。
         * 与 ArchiveReceiver 的 controlRequestChannel 需一致（localhost:8010）。
         */
        final Archive.Context archiveCtx = new Archive.Context()
            .archiveDir(archiveDir)
            .controlChannel(CONTROL_REQUEST_CHANNEL)
                .replicationChannel(REPLICATION_CHANNEL)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .recordingEventsEnabled(false)
            .deleteArchiveOnStart(true);

        /*
         * ArchivingMediaDriver.launch(driverCtx, archiveCtx)
         * 在当前进程内以嵌入式方式启动 MediaDriver + Archive 一体化服务：
         * 1) 先 launch MediaDriver（创建 CnC、Conductor/Receiver/Sender 三大 Agent、启动工作线程）
         * 2) 再 launch Archive（共享 MediaDriver 的 AgentInvoker、aeronDirectoryName、errorHandler/errorCounter）
         * 3) Archive 与 MediaDriver 同进程运行，通过 IPC 与 Aeron Client 通信；Archive 监听 controlChannel（如 localhost:8010）
         * 关闭顺序：try-with-resources 退出时先 close Archive，再 close MediaDriver。
         */
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            ArchivingMediaDriver archivingDriver = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(archivingDriver.mediaDriver().aeronDirectoryName());

            /*
             * AeronArchive.connect(ctx)
             * 创建与本地/远程 Archive 的控制会话（Control Session）：
             * 1) ctx.conclude() 校验配置并填充默认值
             * 2) 通过 asyncConnect → poll 循环：订阅 controlResponseChannel、添加 controlRequest Publication、
             *    发送 connect 请求、等待 Archive 返回 controlSessionId
             * 3) 成功后返回 AeronArchive 实例，可调用 startRecording/stopRecording、replay 等
             * 需保证 aeron 的 aeronDirectoryName 与 ArchivingMediaDriver 的 MediaDriver 目录一致（同进程嵌入时）。
             */
            try (Aeron aeron = Aeron.connect(aeronCtx);
                AeronArchive archive = AeronArchive.connect(
                    new AeronArchive.Context().
                            controlRequestChannel(CONTROL_REQUEST_CHANNEL)
                            .controlResponseChannel(CONTROL_RESPONSE_CHANNEL).
                            aeron(aeron)))
            {
                /*
                 * startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL) 端到端流程（与源码对应）：
                 *
                 * 【客户端 AeronArchive】
                 * io.aeron.archive.client.AeronArchive#startRecording(String,int,SourceLocation)
                 *   → 分配 correlationId，调用 ArchiveProxy.startRecording 将 StartRecordingRequest（SBE）
                 *     写入 controlRequest Publication（本例 UDP localhost:8010 → Archive 控制端口）。
                 *   → pollForResponse：在 controlResponse Subscription 上轮询，直到收到匹配 correlationId 的 OK，
                 *     返回值为「录制用 Subscription 的 registrationId」（本 Demo 未使用返回值）。
                 *
                 * 【Archive 侧 Conductor 线程】
                 * io.aeron.archive.ArchiveConductor#doWork → controlSessionAdapter.poll()
                 * io.aeron.archive.ControlSessionAdapter（FragmentHandler）解析模板 StartRecordingRequest
                 *   → ControlSession#onStartRecording → ArchiveConductor#startRecording(...)
                 *   → 对 LOCAL + UDP：在 stripped channel 前加 CommonContext.SPY_PREFIX（"aeron-spy:"），
                 *     即 spy 订阅同一 MediaDriver 内 Publication 的日志，不经网络复制。
                 *   → aeron.addSubscription(spyChannel, streamId, AvailableImageHandler, null)，
                 *     并 sendOkResponse(correlationId, subscription.registrationId())。
                 *
                 * 【真正开始写盘】要等下面 addPublication(CHANNEL, STREAM_ID) 建立 Publication 后，
                 * Image 可用时 AvailableImageHandler 将任务投递 taskQueue → startRecordingSession：
                 * catalog 新增 recording、分配 RecordingPos 计数器、构造 RecordingSession、recorder.addSession，
                 * 并 sendSignal(START)。因此本类在 addPublication 之后轮询 RecordingPos 等待 recordingId。
                 *
                 * SourceLocation.LOCAL：本地 spy，直接读共享 term buffer；配合 driverCtx.spiesSimulateConnection(true)，
                 * spy 视为已连接，避免 publication.offer() 长期 NOT_CONNECTED。
                 */
                archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);

                long sendCount = 0;
                long recordingId;

                try (Publication publication = aeron.addPublication(CHANNEL, STREAM_ID))
                {
                    final CountersReader counters = aeron.countersReader();
                    final long archiveId = archive.archiveId();

                    int counterId;
                    while (CountersReader.NULL_COUNTER_ID ==
                        (counterId = RecordingPos.findCounterIdBySession(
                            counters, publication.sessionId(), archiveId)))
                    {
                        if (!running.get())
                        {
                            return;
                        }
                        Thread.yield();
                    }

                    recordingId = RecordingPos.getRecordingId(counters, counterId);
                    System.out.println("  Archive 录制已启动: recordingId=" + recordingId);
                    System.out.println();

                    // ==================== 发送阶段 ====================

                    try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name()))
                    {
                        while (running.get() && scanner.hasNextLine())
                        {
                            System.out.print("> ");
                            System.out.flush();
                            final String line = scanner.nextLine();
                            if (line == null)
                            {
                                break;
                            }

                            final byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                            if (bytes.length > MAX_MESSAGE_BYTES)
                            {
                                System.out.println("  [跳过] 内容过长，最多 " + MAX_MESSAGE_BYTES + " 字节");
                                continue;
                            }

                            buffer.putBytes(0, bytes);
                            final int length = bytes.length;

                            long result;
                            while ((result = publication.offer(buffer, 0, length)) < 0)
                            {
                                if (result == Publication.BACK_PRESSURED)
                                {
                                    Thread.sleep(1);
                                }
                                else if (result == Publication.NOT_CONNECTED)
                                {
                                    Thread.sleep(100);
                                }
                                else
                                {
                                    break;
                                }
                            }

                            if (result > 0)
                            {
                                sendCount++;
                                System.out.println("  ✓ 已发送并录制 (" + length + " 字节)");
                            }
                        }
                    }

                    System.out.println();
                    System.out.println("共发送 " + sendCount + " 条消息。等待 Archive 写入完成...");
                    // RecordingPos 计数器追上 publication.position()：确保 spy→RecordingSession 已刷盘到 catalog 可见范围。
                    final YieldingIdleStrategy idle = YieldingIdleStrategy.INSTANCE;
                    idle.reset();
                    while (counters.getCounterValue(counterId) < publication.position())
                    {
                        if (!RecordingPos.isActive(counters, counterId, recordingId))
                        {
                            break;
                        }
                        idle.idle();
                    }
                }

                // 停止 spy 录制；catalog 中该 recording 置为完成，receiver 的 listRecordings 才能稳定看到 stopPosition。
                archive.stopRecording(CHANNEL, STREAM_ID);

                // ==================== 等待回放请求 ====================

                System.out.println("  录制完成: recordingId=" + recordingId);
                System.out.println();
                System.out.println("========================================");
                System.out.println("  Archive 服务继续运行 (控制端口 localhost:8010)");
                System.out.println("  现在可以启动 ArchiveReceiver 连接并请求回放。");
                System.out.println("  按 Ctrl+C 关闭。");
                System.out.println("========================================");

                /*
                 * 发送阶段结束后保持进程存活，让 Archive 服务持续对外提供服务。
                 * ArchiveReceiver 通过 UDP 控制通道连接到此 Archive，查询录制记录并请求回放。
                 * 回放时 Archive 读取磁盘数据，通过 sender 的 MediaDriver 以 UDP 发给 receiver。
                 */
                while (running.get())
                {
                    Thread.sleep(1000);
                }
            }
        }

        System.out.println();
        System.out.println("ArchiveSender 已退出。");
    }
}

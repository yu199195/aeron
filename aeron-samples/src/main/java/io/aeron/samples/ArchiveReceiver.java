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
import io.aeron.ChannelUri;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 接收端 Demo：先订阅实时数据，输入 {@code replay} 后连接 Archive 请求回放。
 * <p>
 * 流程：
 * <ol>
 *     <li><b>实时接收</b>：订阅 UDP 通道，持续接收 {@link ArchiveSender} 发送的实时消息</li>
 *     <li><b>回放指令</b>：在控制台输入 {@code replay} 后回车</li>
 *     <li><b>连接 Archive</b>：通过 AeronArchive Client 连接到发送端的 Archive 服务</li>
 *     <li><b>请求回放</b>：查询录制记录，请求回放，接收并打印历史消息</li>
 *     <li><b>继续实时</b>：回放完成后继续接收实时数据，可再次输入 {@code replay}</li>
 * </ol>
 * <p>
 * 控制台命令：
 * <ul>
 *     <li>{@code replay} — 连接 Archive 并回放已录制的消息</li>
 *     <li>{@code quit} 或 {@code exit} — 退出程序</li>
 * </ul>
 */
public class ArchiveReceiver
{
    // 与 ArchiveSender 一致：实时业务流 streamId，对应录制时的 channel/stream。
    private static final int STREAM_ID = 10;
    private static final int FRAGMENT_LIMIT = 10;

    // 回放专用：与实时通道 endpoint 不同（40457），避免与 sender 的 publication 冲突；Archive 会向此 UDP 发回放数据。
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:40457";
    // 回放使用独立 streamId，与实时订阅（10）区分，同一 Aeron 实例上可同时存在两条逻辑流。
    private static final int REPLAY_STREAM_ID = 11;
    private static final String CMD_REPLAY = "replay";
    private static final String CMD_QUIT = "quit";
    private static final String CMD_EXIT = "exit";

    public static void main(final String[] args)
    {
        System.out.println("========================================");
        System.out.println("  ArchiveReceiver 接收端");
        System.out.println("  实时通道: " + ArchiveSender.CHANNEL + "  streamId=" + STREAM_ID);
        System.out.println("========================================");
        System.out.println("  命令: replay=请求回放 | quit/exit=退出");
        System.out.println("  等待实时消息中...");
        System.out.println("========================================");
        System.out.println();

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean replayRequested = new AtomicBoolean(false);

        // 接收端仅启动 MediaDriver（无 Archive）；回放数据由「发送端进程内的 Archive」经 UDP 灌入本机 driver。
        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver driver = MediaDriver.launchEmbedded(
                new MediaDriver.Context().terminationHook(barrier::signalAll)))
        {
            final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(driver.aeronDirectoryName());

            try (Aeron aeron = Aeron.connect(aeronCtx))
            {
                // 独立线程读控制台，避免阻塞 subscription.poll()；输入 replay 时置位 replayRequested。
                final Thread inputThread = new Thread(() ->
                {
                    try (Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name()))
                    {
                        while (running.get() && scanner.hasNextLine())
                        {
                            final String line = scanner.nextLine();
                            if (line == null)
                            {
                                break;
                            }
                            final String cmd = line.trim().toLowerCase();
                            if (CMD_REPLAY.equals(cmd))
                            {
                                replayRequested.set(true);
                                System.out.println();
                                System.out.println("  [指令] 收到回放请求，正在连接 Archive...");
                            }
                            else if (CMD_QUIT.equals(cmd) || CMD_EXIT.equals(cmd))
                            {
                                running.set(false);
                                break;
                            }
                            else if (!cmd.isEmpty())
                            {
                                System.out.println("  [提示] 未知命令，输入 replay 请求回放，quit 退出");
                            }
                        }
                    }
                }, "archive-receiver-input");
                inputThread.setDaemon(true);
                inputThread.start();

                final AtomicLong liveCount = new AtomicLong(0);
                // 实时流 FragmentHandler：与 sender 的 publication.offer 对应，经 UDP 到达本 subscription。
                final FragmentHandler liveHandler = (buffer, offset, length, header) ->
                {
                    final byte[] bytes = new byte[length];
                    buffer.getBytes(offset, bytes);
                    final String message = new String(bytes, StandardCharsets.UTF_8);
                    System.out.println("  [实时 #" + liveCount.incrementAndGet() + "] " + message);
                };

                try (Subscription subscription = aeron.addSubscription(ArchiveSender.CHANNEL, STREAM_ID))
                {
                    final SleepingIdleStrategy idle =
                        new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1));

                    while (running.get())
                    {
                        // 主线程：优先处理回放请求（同步执行 doReplay），否则轮询实时 Image。
                        if (replayRequested.getAndSet(false))
                        {
                            // 阻塞式回放：期间不 poll 实时 subscription；回放结束后再继续实时循环。
                            doReplay(aeron);
                            System.out.println();
                            System.out.println("  回放完成，继续接收实时消息。输入 replay 可再次回放。");
                            System.out.println();
                            continue;
                        }

                        final int fragmentsRead = subscription.poll(liveHandler, FRAGMENT_LIMIT);
                        idle.idle(fragmentsRead);
                    }
                }
            }
        }

        System.out.println();
        System.out.println("ArchiveReceiver 已退出。");
    }

    /**
     * 回放路径概要：
     * <ol>
     *     <li>AeronArchive.connect：经 UDP 连到 sender 上 Archive 的 control（8010），建立 control session。</li>
     *     <li>listRecordingsForUri：拉 catalog 中与实时通道匹配的 recordingId。</li>
     *     <li>startReplay：发 Replay 请求；Archive 侧 asyncAddExclusivePublication + ReplaySession 读盘并 offer。</li>
     *     <li>本机 addSubscription(replayChannel+sessionId, REPLAY_STREAM_ID)：接收回放 UDP，poll 至 Image 关闭。</li>
     * </ol>
     */
    private static void doReplay(final Aeron aeron)
    {
        // controlRequestChannel 指向「运行 Archive 的那台机器」的控制端口（本 demo 为 sender 的 localhost:8010）。
        // controlResponseChannel 使用 ephemeral（localhost:0），Archive 将控制响应发回该动态端口。
        try (AeronArchive archive = AeronArchive.connect(
            new AeronArchive.Context()
                .controlRequestChannel(ArchiveSender.CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(ArchiveSender.REPLICATION_CHANNEL)
                .aeron(aeron)))
        {
            final long recordingId = findLatestRecording(archive);
            if (recordingId < 0)
            {
                System.out.println("  未找到录制记录。请先用 ArchiveSender 发送并录制消息。");
                return;
            }

            System.out.println("  找到录制: recordingId=" + recordingId);

            // position=0：从录制起点回放；length=Long.MAX_VALUE：尽可能多读（已结束录制则读到 stopPosition）。
            // 返回值低 32 位为 replay Publication 的 sessionId，需写进 channel 才能与 Archive 创建的 ExclusivePublication 对齐。
            final long sessionId = archive.startReplay(
                recordingId, 0L, Long.MAX_VALUE, REPLAY_CHANNEL, REPLAY_STREAM_ID);
            final String replayChannel = ChannelUri.addSessionId(REPLAY_CHANNEL, (int)sessionId);

            final AtomicLong replayCount = new AtomicLong(0);
            final FragmentHandler replayHandler = (buffer, offset, length, header) ->
            {
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                final String message = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("  [回放 #" + replayCount.incrementAndGet() + "] " + message);
            };

            // 订阅「带 session-id 的回放通道」：与 Archive 侧 replay publication 的 session 一一对应。
            try (Subscription subscription = aeron.addSubscription(replayChannel, REPLAY_STREAM_ID))
            {
                final SleepingIdleStrategy idle =
                    new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1));

                // 等待 driver 与 sender 侧 replay publication 完成建链（UDP unicast 需要双方就绪）。
                while (!subscription.isConnected())
                {
                    idle.idle();
                }

                final Image image = subscription.imageAtIndex(0);

                // ReplaySession 读完 replayLength 后会关闭 publication，此处 Image 关闭即表示回放结束。
                while (true)
                {
                    final int fragments = image.poll(replayHandler, FRAGMENT_LIMIT);
                    if (0 == fragments && image.isClosed())
                    {
                        break;
                    }
                    idle.idle(fragments);
                }
            }

            System.out.println("  回放完成，共 " + replayCount.get() + " 条消息。");
        }
        catch (final Exception ex)
        {
            System.err.println("  回放失败: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 遍历 Archive catalog 中与「实时 publication 相同的 uri + streamId」的录制；consumer 每次回调更新 last，
     * 因此得到的是列表中最后一条匹配的 recordingId（通常即最近一次录制）。
     */
    private static long findLatestRecording(final AeronArchive archive)
    {
        final MutableLong lastRecordingId = new MutableLong(-1);

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        // fromRecordingId=0，count=100：简单 demo 够用；生产环境宜分页或按 recordingId 精确查询。
        return archive.listRecordingsForUri(0L, 100, ArchiveSender.CHANNEL, STREAM_ID, consumer) > 0
            ? lastRecordingId.get()
            : -1;
    }
}

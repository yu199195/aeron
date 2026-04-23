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
package io.aeron.samples.cluster;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 集群消息回放客户端：从 Archive 中回放历史消息。
 * <p>
 * 本类演示如何：
 * <ul>
 *   <li><b>连接到 Archive</b>：连接到集群节点的 Archive 服务</li>
 *   <li><b>查询录制</b>：列出所有可用的录制（Recording）</li>
 *   <li><b>回放消息</b>：从指定位置回放历史消息</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 列出所有录制
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient list
 *
 * # 回放指定录制（从起始位置）
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay <recordingId>
 *
 * # 从指定位置回放
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay <recordingId> <position> <length>
 *
 * # 示例
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient list
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay 0
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay 0 0 10000
 * </pre>
 * <p>
 * <b>注意</b>：
 * <ul>
 *   <li>需要集群节点正在运行（Archive 服务可访问）</li>
 *   <li>Archive 控制通道默认为 localhost:9001（Node 0）</li>
 *   <li>如果连接其他节点，修改 ARCHIVE_CONTROL_CHANNEL</li>
 * </ul>
 */
public class ClusterReplayClient implements AutoCloseable
{
    /** Archive 控制通道（连接到 Node 0 的 Archive） */
    private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9001";

    /** 回放通道（用于接收回放数据） */
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /** 回放流 ID */
    private static final int REPLAY_STREAM_ID = 101;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    /**
     * 主入口。
     *
     * @param args 命令行参数
     *             - list: 列出所有录制
     *             - replay <recordingId> [position] [length]: 回放指定录制
     */
    public static void main(final String[] args)
    {
        if (args.length < 1)
        {
            printUsage();
            System.exit(1);
        }

        final String command = args[0];

        System.out.println("========================================");
        System.out.println("  Aeron Cluster Replay Client");
        System.out.println("========================================");
        System.out.println("Archive 控制通道: " + ARCHIVE_CONTROL_CHANNEL);
        System.out.println();

        try (ClusterReplayClient client = new ClusterReplayClient())
        {
            switch (command.toLowerCase())
            {
                case "list":
                    client.listRecordings();
                    break;

                case "replay":
                    if (args.length < 2)
                    {
                        System.err.println("错误: replay 命令需要 recordingId 参数");
                        printUsage();
                        System.exit(1);
                    }

                    final long recordingId = Long.parseLong(args[1]);
                    long position = 0;
                    long length = Long.MAX_VALUE;

                    if (args.length >= 3)
                    {
                        position = Long.parseLong(args[2]);
                    }

                    if (args.length >= 4)
                    {
                        length = Long.parseLong(args[3]);
                    }

                    client.replayRecording(recordingId, position, length);
                    break;

                default:
                    System.err.println("错误: 未知命令 '" + command + "'");
                    printUsage();
                    System.exit(1);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    /**
     * 打印使用说明。
     */
    private static void printUsage()
    {
        System.out.println("用法:");
        System.out.println("  java ClusterReplayClient list");
        System.out.println("  java ClusterReplayClient replay <recordingId> [position] [length]");
        System.out.println();
        System.out.println("命令:");
        System.out.println("  list                           - 列出所有录制");
        System.out.println("  replay <recordingId>           - 回放整个录制");
        System.out.println("  replay <recordingId> <pos>     - 从指定位置回放到结尾");
        System.out.println("  replay <recordingId> <pos> <len> - 从指定位置回放指定长度");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  java ClusterReplayClient list");
        System.out.println("  java ClusterReplayClient replay 0");
        System.out.println("  java ClusterReplayClient replay 0 0 10000");
    }

    /**
     * 构造客户端并连接到 Archive。
     */
    public ClusterReplayClient()
    {
        System.out.println("[Replay] 启动 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Replay] 启动 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Replay] 连接到 Archive...");
        aeronArchive = AeronArchive.connect(new AeronArchive.Context()
            .aeron(aeron)
            .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0"));

        System.out.println("[Replay] 已连接到 Archive");
        System.out.println();
    }

    /**
     * 列出所有录制。
     */
    private void listRecordings()
    {
        System.out.println("========================================");
        System.out.println("  可用的录制 (Recordings)");
        System.out.println("========================================");

        final long fromRecordingId = 0L;
        final int recordCount = 100;

        final RecordingDescriptorConsumer consumer = (
            controlSessionId,
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
            sourceIdentity) ->
        {
            System.out.println();
            System.out.println("Recording ID: " + recordingId);
            System.out.println("  Channel:    " + originalChannel);
            System.out.println("  Stream ID:  " + streamId);
            System.out.println("  Start Pos:  " + startPosition);
            System.out.println("  Stop Pos:   " + stopPosition);
            System.out.println("  Length:     " + (stopPosition - startPosition) + " bytes");
            System.out.println("  Start Time: " + new java.util.Date(startTimestamp));
            if (stopTimestamp != 0)
            {
                System.out.println("  Stop Time:  " + new java.util.Date(stopTimestamp));
            }
            else
            {
                System.out.println("  Stop Time:  [正在录制中]");
            }
        };

        final int foundCount = aeronArchive.listRecordings(fromRecordingId, recordCount, consumer);

        System.out.println();
        System.out.println("========================================");
        System.out.println("共找到 " + foundCount + " 个录制");
        System.out.println("========================================");
    }

    /**
     * 回放指定的录制。
     *
     * @param recordingId 录制 ID
     * @param position    起始位置
     * @param length      回放长度
     */
    private void replayRecording(final long recordingId, final long position, final long length)
    {
        System.out.println("========================================");
        System.out.println("  回放录制");
        System.out.println("========================================");
        System.out.println("Recording ID: " + recordingId);
        System.out.println("Position:     " + position);
        System.out.println("Length:       " + (length == Long.MAX_VALUE ? "全部" : String.valueOf(length)));
        System.out.println();

        // 启动回放
        final long replaySessionId = aeronArchive.startReplay(
            recordingId,
            position,
            length,
            REPLAY_CHANNEL,
            REPLAY_STREAM_ID);

        System.out.println("[Replay] 回放会话已启动，会话 ID: " + replaySessionId);

        // 订阅回放通道
        final Subscription subscription = aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);

        System.out.println("[Replay] 等待回放流连接...");
        while (!subscription.isConnected())
        {
            idleStrategy.idle();
        }

        System.out.println("[Replay] 回放流已连接，开始接收数据...");
        System.out.println("========================================");
        System.out.println();

        // 创建消息处理器
        final FragmentHandler fragmentHandler = new FragmentAssembler(this::onReplayedMessage);

        // 接收回放的消息
        long messagesReceived = 0;
        int idleCount = 0;
        final int maxIdleCount = 100;  // 最多空闲 100 次（约 1 秒）

        while (idleCount < maxIdleCount)
        {
            final int fragmentsRead = subscription.poll(fragmentHandler, 10);

            if (fragmentsRead > 0)
            {
                messagesReceived += fragmentsRead;
                idleCount = 0;  // 重置空闲计数
            }
            else
            {
                idleCount++;
                idleStrategy.idle();
            }

            // 检查回放是否完成
            if (!subscription.isConnected())
            {
                System.out.println();
                System.out.println("[Replay] 回放流已断开");
                break;
            }
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("回放完成，共接收 " + messagesReceived + " 条消息");
        System.out.println("========================================");

        CloseHelper.close(subscription);
    }

    /**
     * 处理回放的消息。
     */
    private void onReplayedMessage(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        // 尝试解码为字符串（假设消息是 UTF-8 文本）
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);

        try
        {
            final String message = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("[Replayed] " + message);
        }
        catch (final Exception ex)
        {
            // 如果不是文本，显示字节数
            System.out.println("[Replayed] Binary message (" + length + " bytes) at position " +
                header.position());
        }
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Replay] 关闭连接...");
        CloseHelper.closeAll(aeronArchive, aeron, mediaDriver);
    }
}

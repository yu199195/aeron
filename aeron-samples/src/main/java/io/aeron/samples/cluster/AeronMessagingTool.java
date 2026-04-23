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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * Aeron 消息工具：支持指定 streamId 的消息收发和回放。
 * <p>
 * 本工具支持：
 * <ul>
 *   <li><b>发送消息</b>：向指定 channel 和 streamId 发送消息</li>
 *   <li><b>接收消息</b>：从指定 channel 和 streamId 接收消息</li>
 *   <li><b>列出录制</b>：按 streamId 过滤 Archive 录制</li>
 *   <li><b>回放消息</b>：回放指定 streamId 的历史消息</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 启动发送者（发送到 streamId=100）
 * java AeronMessagingTool sender aeron:ipc 100
 *
 * # 启动接收者（接收 streamId=100）
 * java AeronMessagingTool receiver aeron:ipc 100
 *
 * # 列出 streamId=100 的录制
 * java AeronMessagingTool list 100
 *
 * # 回放 streamId=100 的录制
 * java AeronMessagingTool replay &lt;recordingId&gt; 100
 * </pre>
 * <p>
 * <b>注意</b>：
 * <ul>
 *   <li>发送者和接收者必须使用相同的 channel 和 streamId</li>
 *   <li>Archive 控制通道默认为 UDP 9001（可配置）</li>
 * </ul>
 */
public class AeronMessagingTool implements AutoCloseable
{
    /** Archive 控制通道（远程连接 Node 0） */
    private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9001";

    /** 回放通道 */
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int REPLAY_STREAM_ID = 999;  // 回放专用 streamId

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    /**
     * 主入口。
     */
    public static void main(final String[] args)
    {
        if (args.length < 1)
        {
            printUsage();
            System.exit(1);
        }

        final String mode = args[0].toLowerCase();

        try (AeronMessagingTool tool = new AeronMessagingTool())
        {
            switch (mode)
            {
                case "sender":
                    if (args.length < 3)
                    {
                        System.err.println("用法: sender <channel> <streamId>");
                        System.exit(1);
                    }
                    tool.runSender(args[1], Integer.parseInt(args[2]));
                    break;

                case "receiver":
                    if (args.length < 3)
                    {
                        System.err.println("用法: receiver <channel> <streamId>");
                        System.exit(1);
                    }
                    tool.runReceiver(args[1], Integer.parseInt(args[2]));
                    break;

                case "list":
                    final int filterStreamId = args.length > 1 ? Integer.parseInt(args[1]) : -1;
                    tool.listRecordings(filterStreamId);
                    break;

                case "replay":
                    if (args.length < 3)
                    {
                        System.err.println("用法: replay <recordingId> <streamId>");
                        System.exit(1);
                    }
                    final long recordingId = Long.parseLong(args[1]);
                    final int streamId = Integer.parseInt(args[2]);
                    tool.replayRecording(recordingId, streamId);
                    break;

                default:
                    System.err.println("未知模式: " + mode);
                    printUsage();
                    System.exit(1);
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void printUsage()
    {
        System.out.println("Aeron 消息工具");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  sender <channel> <streamId>        - 发送消息");
        System.out.println("  receiver <channel> <streamId>      - 接收消息");
        System.out.println("  list [streamId]                    - 列出录制（可选过滤 streamId）");
        System.out.println("  replay <recordingId> <streamId>    - 回放录制");
        System.out.println();
        System.out.println("Channel 示例:");
        System.out.println("  aeron:ipc                          - 本地 IPC");
        System.out.println("  aeron:udp?endpoint=localhost:9000  - UDP");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 发送消息到 streamId=100");
        System.out.println("  java AeronMessagingTool sender aeron:ipc 100");
        System.out.println();
        System.out.println("  # 接收 streamId=100 的消息");
        System.out.println("  java AeronMessagingTool receiver aeron:ipc 100");
        System.out.println();
        System.out.println("  # 列出 streamId=100 的录制");
        System.out.println("  java AeronMessagingTool list 100");
        System.out.println();
        System.out.println("  # 回放录制 ID=0");
        System.out.println("  java AeronMessagingTool replay 0 100");
    }

    public AeronMessagingTool()
    {
        System.out.println("[Tool] 启动 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Tool] 启动 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Tool] 准备就绪");
        System.out.println();
    }

    /**
     * 发送者模式：交互式发送消息。
     */
    private void runSender(final String channel, final int streamId)
    {
        System.out.println("========================================");
        System.out.println("  发送者模式");
        System.out.println("========================================");
        System.out.println("Channel:   " + channel);
        System.out.println("Stream ID: " + streamId);
        System.out.println();

        System.out.println("[Sender] 创建 Publication...");
        final Publication publication = aeron.addPublication(channel, streamId);

        System.out.println("[Sender] 等待连接...");
        while (!publication.isConnected())
        {
            idleStrategy.idle();
        }

        System.out.println("[Sender] 已连接，可以发送消息");
        System.out.println("输入消息（输入 'exit' 退出）:");
        System.out.println("========================================");
        System.out.println();

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        try (Scanner scanner = new Scanner(System.in))
        {
            while (true)
            {
                System.out.print("> ");
                System.out.flush();

                if (!scanner.hasNextLine())
                {
                    break;
                }

                final String input = scanner.nextLine().trim();

                if (input.isEmpty())
                {
                    continue;
                }

                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit"))
                {
                    break;
                }

                // 发送消息
                final byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
                buffer.putBytes(0, bytes);

                long result;
                int attempts = 0;
                while ((result = publication.offer(buffer, 0, bytes.length)) < 0 && attempts++ < 3)
                {
                    if (result == Publication.NOT_CONNECTED)
                    {
                        System.err.println("[Error] 未连接");
                        Thread.sleep(100);
                    }
                    else if (result == Publication.BACK_PRESSURED)
                    {
                        System.err.println("[Error] 背压");
                        idleStrategy.idle();
                    }
                }

                if (result > 0)
                {
                    System.out.println("[Sent] 位置: " + result);
                }
                else
                {
                    System.err.println("[Error] 发送失败: " + result);
                }
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            CloseHelper.close(publication);
        }

        System.out.println();
        System.out.println("[Sender] 已退出");
    }

    /**
     * 接收者模式：持续接收消息。
     */
    private void runReceiver(final String channel, final int streamId)
    {
        System.out.println("========================================");
        System.out.println("  接收者模式");
        System.out.println("========================================");
        System.out.println("Channel:   " + channel);
        System.out.println("Stream ID: " + streamId);
        System.out.println();

        System.out.println("[Receiver] 创建 Subscription...");
        final Subscription subscription = aeron.addSubscription(channel, streamId);

        System.out.println("[Receiver] 等待连接...");
        while (!subscription.isConnected())
        {
            idleStrategy.idle();
        }

        System.out.println("[Receiver] 已连接，等待消息...");
        System.out.println("按 Ctrl+C 退出");
        System.out.println("========================================");
        System.out.println();

        final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) ->
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final String message = new String(bytes, StandardCharsets.UTF_8);

            System.out.printf("[Received] Position: %d, Length: %d%n", header.position(), length);
            System.out.println("           Message: " + message);
        });

        long messagesReceived = 0;

        try
        {
            while (true)
            {
                final int fragmentsRead = subscription.poll(fragmentHandler, 10);

                if (fragmentsRead > 0)
                {
                    messagesReceived += fragmentsRead;
                }
                else
                {
                    idleStrategy.idle();
                }

                // 检查连接状态
                if (!subscription.isConnected())
                {
                    System.out.println();
                    System.out.println("[Receiver] 连接已断开");
                    break;
                }
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            CloseHelper.close(subscription);
        }

        System.out.println();
        System.out.println("[Receiver] 共接收 " + messagesReceived + " 条消息");
    }

    /**
     * 列出录制，支持按 streamId 过滤。
     */
    private void listRecordings(final int filterStreamId)
    {
        ensureArchiveConnected();

        System.out.println("========================================");
        System.out.println("  Archive 录制列表");
        System.out.println("========================================");
        if (filterStreamId >= 0)
        {
            System.out.println("过滤 Stream ID: " + filterStreamId);
        }
        System.out.println();

        final RecordingDescriptorConsumer consumer = (
            controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
            startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
            mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
        {
            // 过滤 streamId
            if (filterStreamId >= 0 && streamId != filterStreamId)
            {
                return;
            }

            System.out.println("Recording ID: " + recordingId);
            System.out.println("  Channel:    " + originalChannel);
            System.out.println("  Stream ID:  " + streamId);
            System.out.println("  Session ID: " + sessionId);
            System.out.println("  Position:   " + startPosition + " - " + stopPosition);
            System.out.println("  Length:     " + (stopPosition - startPosition) + " bytes");
            System.out.println("  Start:      " + new java.util.Date(startTimestamp));
            if (stopTimestamp != 0)
            {
                System.out.println("  Stop:       " + new java.util.Date(stopTimestamp));
            }
            else
            {
                System.out.println("  Stop:       [正在录制中]");
            }
            System.out.println();
        };

        final int count = aeronArchive.listRecordings(0L, 100, consumer);

        System.out.println("========================================");
        System.out.println("共找到 " + count + " 个录制");
        System.out.println("========================================");
    }

    /**
     * 回放指定 streamId 的录制。
     */
    private void replayRecording(final long recordingId, final int originalStreamId)
    {
        ensureArchiveConnected();

        System.out.println("========================================");
        System.out.println("  回放录制");
        System.out.println("========================================");
        System.out.println("Recording ID:       " + recordingId);
        System.out.println("Original Stream ID: " + originalStreamId);
        System.out.println();

        // 启动回放（使用独立的回放 streamId）
        System.out.println("[Replay] 启动回放...");
        final long replaySessionId = aeronArchive.startReplay(
            recordingId, 0, Long.MAX_VALUE, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        System.out.println("[Replay] 回放会话 ID: " + replaySessionId);

        // 订阅回放通道
        final Subscription subscription = aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);

        System.out.println("[Replay] 等待连接...");
        int waitCount = 0;
        while (!subscription.isConnected() && waitCount++ < 100)
        {
            idleStrategy.idle();
        }

        if (!subscription.isConnected())
        {
            System.err.println("[Error] 回放连接超时");
            CloseHelper.close(subscription);
            return;
        }

        System.out.println("[Replay] 开始接收回放数据...");
        System.out.println("========================================");
        System.out.println();

        final FragmentHandler fragmentHandler = new FragmentAssembler((buffer, offset, length, header) ->
        {
            try
            {
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                final String message = new String(bytes, StandardCharsets.UTF_8);

                System.out.printf("[Replayed] Position: %d%n", header.position());
                System.out.println("           Message: " + message);
            }
            catch (final Exception ex)
            {
                System.out.printf("[Replayed] Binary (%d bytes) at position %d%n", length, header.position());
            }
        });

        long messagesReplayed = 0;
        int idleCount = 0;
        final int maxIdleCount = 50;

        while (idleCount < maxIdleCount)
        {
            final int fragmentsRead = subscription.poll(fragmentHandler, 10);

            if (fragmentsRead > 0)
            {
                messagesReplayed += fragmentsRead;
                idleCount = 0;
            }
            else
            {
                idleCount++;
                idleStrategy.idle();
            }

            if (!subscription.isConnected())
            {
                System.out.println();
                System.out.println("[Replay] 回放流已断开");
                break;
            }
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("回放完成，共 " + messagesReplayed + " 条消息");
        System.out.println("========================================");

        CloseHelper.close(subscription);
    }

    /**
     * 确保 Archive 连接。
     */
    private void ensureArchiveConnected()
    {
        if (null == aeronArchive)
        {
            System.out.println("[Archive] 连接到 Archive...");
            System.out.println("[Archive] 控制通道: " + ARCHIVE_CONTROL_CHANNEL);

            aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(ARCHIVE_CONTROL_CHANNEL)
                .controlResponseChannel("aeron:udp?endpoint=localhost:0"));

            System.out.println("[Archive] 已连接");
        }
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Tool] 关闭...");
        CloseHelper.closeAll(aeronArchive, aeron, mediaDriver);
    }
}

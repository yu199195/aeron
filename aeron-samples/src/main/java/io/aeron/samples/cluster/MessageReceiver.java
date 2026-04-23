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
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * 消息接收者和回放工具：接收和回放 Order 和 User 消息。
 * <p>
 * <b>支持的功能</b>：
 * <ul>
 *   <li>实时接收消息（多个 streamId）</li>
 *   <li>列出 Archive 录制（按 streamId 过滤）</li>
 *   <li>回放历史消息</li>
 * </ul>
 * <p>
 * <b>部署模式</b>：
 * <ul>
 *   <li>创建独立的 MediaDriver</li>
 *   <li>通过 UDP 从集群节点接收消息</li>
 *   <li>通过 UDP 连接到远程 Archive 进行回放</li>
 *   <li>支持远程集群部署</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 接收实时消息（UDP，订阅本地集群）
 * java MessageReceiver receive "aeron:udp?endpoint=localhost:9010|control-mode=dynamic" 100,200
 *
 * # 接收远程集群消息
 * java MessageReceiver receive "aeron:udp?endpoint=192.168.1.100:9010" 100,200
 *
 * # 列出录制（需指定 Archive 地址，默认 localhost:9001）
 * java MessageReceiver list [streamId]
 *
 * # 回放录制
 * java MessageReceiver replay &lt;recordingId&gt;
 *
 * # 交互模式
 * java MessageReceiver interactive [channel]
 * </pre>
 */
public class MessageReceiver implements AutoCloseable
{
    /** Archive 控制通道 */
    private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9001";

    /** 回放通道 */
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int REPLAY_STREAM_ID = 999;

    /** 消息类型 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private AeronArchive aeronArchive;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    public static void main(final String[] args)
    {
        if (args.length < 1)
        {
            printUsage();
            System.exit(1);
        }

        final String mode = args[0].toLowerCase();

        try (MessageReceiver receiver = new MessageReceiver())
        {
            switch (mode)
            {
                case "receive":
                    // 默认使用 UDP 订阅本地集群
                    final String channel = args.length > 1 ? args[1] : "aeron:udp?endpoint=localhost:9010|control-mode=dynamic";
                    final String streamIdsStr = args.length > 2 ? args[2] : "100,200";
                    final int[] streamIds = parseStreamIds(streamIdsStr);
                    receiver.receiveMessages(channel, streamIds);
                    break;

                case "list":
                    final int filterStreamId = args.length > 1 ? Integer.parseInt(args[1]) : -1;
                    receiver.listRecordings(filterStreamId);
                    break;

                case "replay":
                    if (args.length < 2)
                    {
                        System.err.println("用法: replay <recordingId>");
                        System.exit(1);
                    }
                    receiver.replayRecording(Long.parseLong(args[1]));
                    break;

                case "interactive":
                    final String interactiveChannel = args.length > 1 ? args[1] : "aeron:udp?endpoint=localhost:9010|control-mode=dynamic";
                    receiver.runInteractive(interactiveChannel);
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
        System.out.println("消息接收者和回放工具（远程模式）");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  receive [channel] [streamIds]   - 接收实时消息");
        System.out.println("  list [streamId]                 - 列出录制");
        System.out.println("  replay <recordingId>            - 回放录制");
        System.out.println("  interactive [channel]           - 交互模式");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 从本地集群接收");
        System.out.println("  java MessageReceiver receive \"aeron:udp?endpoint=localhost:9010\" 100,200");
        System.out.println();
        System.out.println("  # 从远程集群接收");
        System.out.println("  java MessageReceiver receive \"aeron:udp?endpoint=192.168.1.100:9010\" 100,200");
        System.out.println();
        System.out.println("  # 列出录制（需先配置 ARCHIVE_CONTROL_CHANNEL）");
        System.out.println("  java MessageReceiver list 100");
        System.out.println();
        System.out.println("  # 回放录制");
        System.out.println("  java MessageReceiver replay 5");
    }

    private static int[] parseStreamIds(final String streamIdsStr)
    {
        final String[] parts = streamIdsStr.split(",");
        final int[] streamIds = new int[parts.length];
        for (int i = 0; i < parts.length; i++)
        {
            streamIds[i] = Integer.parseInt(parts[i].trim());
        }
        return streamIds;
    }

    public MessageReceiver()
    {
        // 创建独立的 MediaDriver，用于远程接收
        System.out.println("[Receiver] 启动独立 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Receiver] 连接 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Receiver] 准备就绪");
        System.out.println();
    }

    /**
     * 接收实时消息。
     */
    private void receiveMessages(final String channel, final int[] streamIds)
    {
        System.out.println("========================================");
        System.out.println("  接收实时消息");
        System.out.println("========================================");
        System.out.println("Channel:    " + channel);
        System.out.print("Stream IDs: ");
        for (int i = 0; i < streamIds.length; i++)
        {
            System.out.print(streamIds[i]);
            if (i < streamIds.length - 1)
            {
                System.out.print(", ");
            }
        }
        System.out.println();
        System.out.println();

        // 创建多个订阅
        final Subscription[] subscriptions = new Subscription[streamIds.length];
        for (int i = 0; i < streamIds.length; i++)
        {
            System.out.println("[Receiver] 订阅 Stream ID " + streamIds[i] + "...");
            subscriptions[i] = aeron.addSubscription(channel, streamIds[i]);
        }

        System.out.println("[Receiver] 等待连接...");
        boolean allConnected = false;
        while (!allConnected)
        {
            allConnected = true;
            for (final Subscription subscription : subscriptions)
            {
                if (!subscription.isConnected())
                {
                    allConnected = false;
                    break;
                }
            }
            if (!allConnected)
            {
                idleStrategy.idle();
            }
        }

        System.out.println("[Receiver] 已连接，等待消息...");
        System.out.println("按 Ctrl+C 退出");
        System.out.println("========================================");
        System.out.println();

        final FragmentHandler fragmentHandler = new FragmentAssembler(this::onMessage);

        try
        {
            long messagesReceived = 0;

            while (true)
            {
                int totalFragments = 0;

                for (final Subscription subscription : subscriptions)
                {
                    final int fragments = subscription.poll(fragmentHandler, 10);
                    totalFragments += fragments;
                }

                if (totalFragments > 0)
                {
                    messagesReceived += totalFragments;
                }
                else
                {
                    idleStrategy.idle();
                }
            }
        }
        finally
        {
            for (final Subscription subscription : subscriptions)
            {
                CloseHelper.close(subscription);
            }
        }
    }

    /**
     * 交互模式。
     */
    private void runInteractive(final String channel)
    {
        System.out.println("========================================");
        System.out.println("  交互模式");
        System.out.println("========================================");
        System.out.println("Channel: " + channel);
        System.out.println();
        System.out.println("命令:");
        System.out.println("  list [streamId]       - 列出录制");
        System.out.println("  replay <recordingId>  - 回放录制");
        System.out.println("  help                  - 显示帮助");
        System.out.println("  exit                  - 退出");
        System.out.println();

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

                final String[] parts = input.split("\\s+");
                final String command = parts[0].toLowerCase();

                try
                {
                    if (command.equals("exit") || command.equals("quit"))
                    {
                        break;
                    }
                    else if (command.equals("help"))
                    {
                        printInteractiveHelp();
                    }
                    else if (command.equals("list"))
                    {
                        final int streamId = parts.length > 1 ? Integer.parseInt(parts[1]) : -1;
                        listRecordings(streamId);
                    }
                    else if (command.equals("replay"))
                    {
                        if (parts.length < 2)
                        {
                            System.err.println("[Error] 用法: replay <recordingId>");
                            continue;
                        }
                        replayRecording(Long.parseLong(parts[1]));
                    }
                    else
                    {
                        System.err.println("[Error] 未知命令: " + command);
                    }
                }
                catch (final Exception ex)
                {
                    System.err.println("[Error] " + ex.getMessage());
                }
            }
        }

        System.out.println();
        System.out.println("[Receiver] 已退出");
    }

    private void printInteractiveHelp()
    {
        System.out.println();
        System.out.println("可用命令:");
        System.out.println("  list                 - 列出所有录制");
        System.out.println("  list <streamId>      - 列出指定 streamId 的录制");
        System.out.println("  replay <recordingId> - 回放指定录制");
        System.out.println("  help                 - 显示此帮助");
        System.out.println("  exit                 - 退出");
        System.out.println();
    }

    /**
     * 列出录制。
     */
    private void listRecordings(final int filterStreamId)
    {
        ensureArchiveConnected();

        System.out.println();
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
            if (filterStreamId >= 0 && streamId != filterStreamId)
            {
                return;
            }

            final String streamType = getStreamTypeName(streamId);

            System.out.printf("Recording ID: %d (%s)%n", recordingId, streamType);
            System.out.printf("  Channel:    %s%n", originalChannel);
            System.out.printf("  Stream ID:  %d%n", streamId);
            System.out.printf("  Position:   %d - %d (%d bytes)%n", startPosition, stopPosition, stopPosition - startPosition);
            System.out.printf("  Start:      %s%n", new java.util.Date(startTimestamp));
            if (stopTimestamp != 0)
            {
                System.out.printf("  Stop:       %s%n", new java.util.Date(stopTimestamp));
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
        System.out.println();
    }

    /**
     * 回放录制。
     */
    private void replayRecording(final long recordingId)
    {
        ensureArchiveConnected();

        System.out.println();
        System.out.println("========================================");
        System.out.println("  回放录制 " + recordingId);
        System.out.println("========================================");

        final long replaySessionId = aeronArchive.startReplay(
            recordingId, 0, Long.MAX_VALUE, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        System.out.println("[Replay] 回放会话 ID: " + replaySessionId);

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

        System.out.println("[Replay] 开始接收...");
        System.out.println();

        final FragmentHandler fragmentHandler = new FragmentAssembler(this::onMessage);

        long messagesReplayed = 0;
        int idleCount = 0;

        while (idleCount < 50)
        {
            final int fragments = subscription.poll(fragmentHandler, 10);

            if (fragments > 0)
            {
                messagesReplayed += fragments;
                idleCount = 0;
            }
            else
            {
                idleCount++;
                idleStrategy.idle();
            }

            if (!subscription.isConnected())
            {
                break;
            }
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("回放完成，共 " + messagesReplayed + " 条消息");
        System.out.println("========================================");
        System.out.println();

        CloseHelper.close(subscription);
    }

    /**
     * 处理接收到的消息。
     */
    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        try
        {
            final byte msgType = buffer.getByte(offset);

            if (msgType == MSG_TYPE_ORDER)
            {
                final MessageSender.Order order = decodeOrder(buffer, offset);
                System.out.printf("[Order] Position: %d, Stream ID: %d%n", header.position(), header.streamId());
                System.out.println("        " + order);
            }
            else if (msgType == MSG_TYPE_USER)
            {
                final MessageSender.User user = decodeUser(buffer, offset);
                System.out.printf("[User]  Position: %d, Stream ID: %d%n", header.position(), header.streamId());
                System.out.println("        " + user);
            }
            else
            {
                System.out.printf("[Unknown] Type: %d, Position: %d, Length: %d%n",
                    msgType, header.position(), length);
            }
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] 解码失败: " + ex.getMessage());
        }
    }

    /**
     * 解码 Order 消息。
     */
    private MessageSender.Order decodeOrder(final DirectBuffer buffer, int offset)
    {
        offset += 1;  // Skip msg type

        final long orderId = buffer.getLong(offset);
        offset += 8;

        final long priceLong = buffer.getLong(offset);
        offset += 8;
        final double price = priceLong / 100.0;

        final int quantity = buffer.getInt(offset);
        offset += 4;

        final char side = (char) buffer.getByte(offset);
        offset += 1;

        final long timestamp = buffer.getLong(offset);
        offset += 8;

        final int symbolLength = buffer.getInt(offset);
        offset += 4;
        final byte[] symbolBytes = new byte[symbolLength];
        buffer.getBytes(offset, symbolBytes);
        final String symbol = new String(symbolBytes, StandardCharsets.UTF_8);

        return new MessageSender.Order(orderId, symbol, price, quantity, side, timestamp);
    }

    /**
     * 解码 User 消息。
     */
    private MessageSender.User decodeUser(final DirectBuffer buffer, int offset)
    {
        offset += 1;  // Skip msg type

        final long userId = buffer.getLong(offset);
        offset += 8;

        final int age = buffer.getInt(offset);
        offset += 4;

        final long balanceLong = buffer.getLong(offset);
        offset += 8;
        final double balance = balanceLong / 100.0;

        final boolean isActive = buffer.getByte(offset) != 0;
        offset += 1;

        final int usernameLength = buffer.getInt(offset);
        offset += 4;
        final byte[] usernameBytes = new byte[usernameLength];
        buffer.getBytes(offset, usernameBytes);
        offset += usernameLength;
        final String username = new String(usernameBytes, StandardCharsets.UTF_8);

        final int emailLength = buffer.getInt(offset);
        offset += 4;
        final byte[] emailBytes = new byte[emailLength];
        buffer.getBytes(offset, emailBytes);
        final String email = new String(emailBytes, StandardCharsets.UTF_8);

        return new MessageSender.User(userId, username, email, age, balance, isActive);
    }

    private String getStreamTypeName(final int streamId)
    {
        if (streamId == 100)
        {
            return "Order";
        }
        else if (streamId == 200)
        {
            return "User";
        }
        else
        {
            return "Unknown";
        }
    }

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
        System.out.println("[Receiver] 关闭...");
        CloseHelper.closeAll(aeronArchive, aeron, mediaDriver);
    }
}

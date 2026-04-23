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
 * 外部订阅者：订阅集群转发的消息。
 * <p>
 * <b>功能</b>：
 * <ul>
 *   <li>订阅特定 streamId 的消息（经过集群共识）</li>
 *   <li>实时接收集群转发的消息</li>
 *   <li>支持订阅多个 streamId</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 订阅 Order 消息（streamId 100）
 * java ExternalSubscriber "aeron:udp?endpoint=localhost:9010" 100
 *
 * # 订阅 User 消息（streamId 200）
 * java ExternalSubscriber "aeron:udp?endpoint=localhost:9010" 200
 *
 * # 订阅多个 streamId
 * java ExternalSubscriber "aeron:udp?endpoint=localhost:9010" 100,200
 *
 * # 远程订阅
 * java ExternalSubscriber "aeron:udp?endpoint=192.168.1.100:9010" 100
 * </pre>
 * <p>
 * <b>注意</b>：
 * <ul>
 *   <li>必须先启动集群节点（使用 ForwardingClusteredService）</li>
 *   <li>消息已经过 Raft 共识</li>
 *   <li>只接收消息，不能回复</li>
 * </ul>
 */
public class ExternalSubscriber implements AutoCloseable
{
    /** 消息类型 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            printUsage();
            System.exit(1);
        }

        final String channel = args[0];
        final String streamIdsStr = args[1];
        final int[] streamIds = parseStreamIds(streamIdsStr);

        System.out.println("========================================");
        System.out.println("  外部订阅者");
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
        System.out.println("说明:");
        System.out.println("  - streamId 100: Order 消息");
        System.out.println("  - streamId 200: User 消息");
        System.out.println("  - 消息已经过集群 Raft 共识");
        System.out.println();

        try (ExternalSubscriber subscriber = new ExternalSubscriber())
        {
            subscriber.subscribe(channel, streamIds);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void printUsage()
    {
        System.out.println("外部订阅者：订阅集群转发的消息");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java ExternalSubscriber <channel> <streamIds>");
        System.out.println();
        System.out.println("参数:");
        System.out.println("  channel   - 订阅 channel");
        System.out.println("  streamIds - 要订阅的 streamId（逗号分隔）");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 订阅 Order 消息");
        System.out.println("  java ExternalSubscriber \"aeron:udp?endpoint=localhost:9010\" 100");
        System.out.println();
        System.out.println("  # 订阅 User 消息");
        System.out.println("  java ExternalSubscriber \"aeron:udp?endpoint=localhost:9010\" 200");
        System.out.println();
        System.out.println("  # 订阅所有消息");
        System.out.println("  java ExternalSubscriber \"aeron:udp?endpoint=localhost:9010\" 100,200");
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

    public ExternalSubscriber()
    {
        System.out.println("[Subscriber] 启动 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Subscriber] 连接 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Subscriber] 准备就绪");
        System.out.println();
    }

    private void subscribe(final String channel, final int[] streamIds)
    {
        // 创建订阅
        final Subscription[] subscriptions = new Subscription[streamIds.length];
        for (int i = 0; i < streamIds.length; i++)
        {
            System.out.println("[Subscriber] 订阅 Stream ID " + streamIds[i] + "...");
            subscriptions[i] = aeron.addSubscription(channel, streamIds[i]);
        }

        System.out.println("[Subscriber] 等待连接...");
        boolean allConnected = false;
        int waitCount = 0;
        while (!allConnected && waitCount++ < 100)
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

        if (allConnected)
        {
            System.out.println("[Subscriber] 已连接到集群转发服务");
        }
        else
        {
            System.out.println("[Subscriber] 未检测到转发服务（这是正常的，等待消息...）");
        }

        System.out.println("[Subscriber] 等待消息...");
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

    private void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        try
        {
            final byte msgType = buffer.getByte(offset);

            if (msgType == MSG_TYPE_ORDER)
            {
                decodeAndPrintOrder(buffer, offset, header);
            }
            else if (msgType == MSG_TYPE_USER)
            {
                decodeAndPrintUser(buffer, offset, header);
            }
            else
            {
                System.out.printf("[Unknown] Type: %d, Position: %d, StreamId: %d%n",
                    msgType, header.position(), header.streamId());
            }
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] 解码失败: " + ex.getMessage());
        }
    }

    private void decodeAndPrintOrder(final DirectBuffer buffer, int offset, final Header header)
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

        System.out.printf("[Order] Position: %d, StreamId: %d%n", header.position(), header.streamId());
        System.out.printf("        Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}%n",
            orderId, symbol, price, quantity, side);
        System.out.println();
    }

    private void decodeAndPrintUser(final DirectBuffer buffer, int offset, final Header header)
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

        System.out.printf("[User] Position: %d, StreamId: %d%n", header.position(), header.streamId());
        System.out.printf("       User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}%n",
            userId, username, email, age, balance, isActive);
        System.out.println();
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Subscriber] 关闭...");
        CloseHelper.closeAll(aeron, mediaDriver);
    }
}

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
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * 消息发送者：支持发送 Order 和 User 对象到不同的 streamId。
 * <p>
 * <b>StreamId 分配</b>：
 * <ul>
 *   <li>Stream ID 100: Order 消息</li>
 *   <li>Stream ID 200: User 消息</li>
 * </ul>
 * <p>
 * <b>部署模式</b>：
 * <ul>
 *   <li>创建独立的 MediaDriver</li>
 *   <li>通过 UDP 多播/单播发送消息到集群节点</li>
 *   <li>支持远程集群部署</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 发送到本地集群（UDP）
 * java MessageSender "aeron:udp?endpoint=localhost:9010|control-mode=dynamic"
 *
 * # 发送到远程集群
 * java MessageSender "aeron:udp?endpoint=192.168.1.100:9010"
 *
 * # 使用多播
 * java MessageSender "aeron:udp?endpoint=224.0.1.1:9010|interface=192.168.1.2"
 * </pre>
 * <p>
 * <b>交互命令</b>：
 * <pre>
 * > order &lt;orderId&gt; &lt;symbol&gt; &lt;price&gt; &lt;quantity&gt; &lt;side&gt;
 * > user &lt;userId&gt; &lt;username&gt; &lt;email&gt; &lt;age&gt; &lt;balance&gt;
 * > help
 * > exit
 * </pre>
 */
public class MessageSender implements AutoCloseable
{
    /** Order 消息的 streamId */
    public static final int ORDER_STREAM_ID = 100;

    /** User 消息的 streamId */
    public static final int USER_STREAM_ID = 200;

    /** 消息类型标识 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final Publication orderPublication;
    private final Publication userPublication;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    public static void main(final String[] args)
    {
        // 默认发送到本地集群的 UDP 端口（与 ClusterNodeWithConfig 的配置对应）
        final String channel = args.length > 0 ? args[0] : "aeron:udp?endpoint=localhost:9010|control-mode=dynamic";

        System.out.println("========================================");
        System.out.println("  消息发送者（远程模式）");
        System.out.println("========================================");
        System.out.println("Channel: " + channel);
        System.out.println("Order Stream ID: " + ORDER_STREAM_ID);
        System.out.println("User Stream ID:  " + USER_STREAM_ID);
        System.out.println();
        System.out.println("注意: 需要集群订阅这些 streamId 才能接收消息");
        System.out.println();

        try (MessageSender sender = new MessageSender(channel))
        {
            sender.runInteractive();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public MessageSender(final String channel)
    {
        // 创建独立的 MediaDriver，用于远程连接
        System.out.println("[Sender] 启动独立 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Sender] 连接 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Sender] 创建 Publications...");
        System.out.println("  Order Publication: " + channel + " (Stream ID: " + ORDER_STREAM_ID + ")");
        System.out.println("  User Publication:  " + channel + " (Stream ID: " + USER_STREAM_ID + ")");

        orderPublication = aeron.addPublication(channel, ORDER_STREAM_ID);
        userPublication = aeron.addPublication(channel, USER_STREAM_ID);

        System.out.println("[Sender] 等待连接...");
        System.out.println("  提示: UDP 模式下不需要等待订阅者连接");

        // UDP 模式下，Publication 可能不会立即显示为 connected
        // 但消息仍然可以发送（单播需要订阅者，多播不需要）
        int waitCount = 0;
        while ((!orderPublication.isConnected() && !userPublication.isConnected()) && waitCount++ < 50)
        {
            idleStrategy.idle();
        }

        if (orderPublication.isConnected() || userPublication.isConnected())
        {
            System.out.println("[Sender] 已检测到订阅者");
        }
        else
        {
            System.out.println("[Sender] 未检测到订阅者（UDP 模式下这是正常的）");
            System.out.println("[Sender] 消息将发送到网络，等待订阅者接收");
        }

        System.out.println("[Sender] 准备就绪");
        System.out.println();
    }

    private void runInteractive()
    {
        System.out.println("========================================");
        System.out.println("  交互式命令");
        System.out.println("========================================");
        printHelp();
        System.out.println("开始输入命令:");
        System.out.println("========================================");
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
                        printHelp();
                    }
                    else if (command.equals("order"))
                    {
                        handleOrderCommand(parts);
                    }
                    else if (command.equals("user"))
                    {
                        handleUserCommand(parts);
                    }
                    else
                    {
                        System.err.println("[Error] 未知命令: " + command);
                        System.err.println("输入 'help' 查看帮助");
                    }
                }
                catch (final Exception ex)
                {
                    System.err.println("[Error] " + ex.getMessage());
                }
            }
        }

        System.out.println();
        System.out.println("[Sender] 已退出");
    }

    private void handleOrderCommand(final String[] parts)
    {
        if (parts.length < 6)
        {
            System.err.println("[Error] 用法: order <orderId> <symbol> <price> <quantity> <side>");
            System.err.println("示例: order 1001 AAPL 150.50 100 B");
            return;
        }

        final long orderId = Long.parseLong(parts[1]);
        final String symbol = parts[2];
        final double price = Double.parseDouble(parts[3]);
        final int quantity = Integer.parseInt(parts[4]);
        final char side = parts[5].charAt(0);

        final Order order = new Order(orderId, symbol, price, quantity, side, System.currentTimeMillis());

        System.out.println("[Sending Order] " + order);

        final int length = encodeOrder(buffer, 0, order);
        final long position = offer(orderPublication, buffer, 0, length);

        if (position > 0)
        {
            System.out.println("[Sent] Position: " + position + " (Stream ID: " + ORDER_STREAM_ID + ")");
        }
        else
        {
            System.err.println("[Error] 发送失败: " + position);
        }
    }

    private void handleUserCommand(final String[] parts)
    {
        if (parts.length < 6)
        {
            System.err.println("[Error] 用法: user <userId> <username> <email> <age> <balance>");
            System.err.println("示例: user 1001 alice alice@example.com 25 1000.50");
            return;
        }

        final long userId = Long.parseLong(parts[1]);
        final String username = parts[2];
        final String email = parts[3];
        final int age = Integer.parseInt(parts[4]);
        final double balance = Double.parseDouble(parts[5]);

        final User user = new User(userId, username, email, age, balance, true);

        System.out.println("[Sending User] " + user);

        final int length = encodeUser(buffer, 0, user);
        final long position = offer(userPublication, buffer, 0, length);

        if (position > 0)
        {
            System.out.println("[Sent] Position: " + position + " (Stream ID: " + USER_STREAM_ID + ")");
        }
        else
        {
            System.err.println("[Error] 发送失败: " + position);
        }
    }

    private long offer(final Publication publication, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        long result;
        int attempts = 0;

        while ((result = publication.offer(buffer, offset, length)) < 0 && attempts++ < 3)
        {
            if (result == Publication.NOT_CONNECTED)
            {
                System.err.println("[Retry] 未连接");
                try
                {
                    Thread.sleep(100);
                }
                catch (final InterruptedException ex)
                {
                    Thread.currentThread().interrupt();
                }
            }
            else if (result == Publication.BACK_PRESSURED)
            {
                System.err.println("[Retry] 背压");
                idleStrategy.idle();
            }
            else
            {
                break;
            }
        }

        return result;
    }

    /**
     * 编码 Order 消息（简化的 SBE 风格编码）。
     */
    private int encodeOrder(final MutableDirectBuffer buffer, int offset, final Order order)
    {
        final int startOffset = offset;

        // Message type
        buffer.putByte(offset, MSG_TYPE_ORDER);
        offset += 1;

        // Order fields
        buffer.putLong(offset, order.orderId);
        offset += 8;

        buffer.putLong(offset, (long) (order.price * 100));  // 价格乘以100存储（避免浮点）
        offset += 8;

        buffer.putInt(offset, order.quantity);
        offset += 4;

        buffer.putByte(offset, (byte) order.side);
        offset += 1;

        buffer.putLong(offset, order.timestamp);
        offset += 8;

        // Variable length symbol
        final byte[] symbolBytes = order.symbol.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(offset, symbolBytes.length);
        offset += 4;
        buffer.putBytes(offset, symbolBytes);
        offset += symbolBytes.length;

        return offset - startOffset;
    }

    /**
     * 编码 User 消息（简化的 SBE 风格编码）。
     */
    private int encodeUser(final MutableDirectBuffer buffer, int offset, final User user)
    {
        final int startOffset = offset;

        // Message type
        buffer.putByte(offset, MSG_TYPE_USER);
        offset += 1;

        // User fields
        buffer.putLong(offset, user.userId);
        offset += 8;

        buffer.putInt(offset, user.age);
        offset += 4;

        buffer.putLong(offset, (long) (user.balance * 100));  // 余额乘以100存储
        offset += 8;

        buffer.putByte(offset, (byte) (user.isActive ? 1 : 0));
        offset += 1;

        // Variable length username
        final byte[] usernameBytes = user.username.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(offset, usernameBytes.length);
        offset += 4;
        buffer.putBytes(offset, usernameBytes);
        offset += usernameBytes.length;

        // Variable length email
        final byte[] emailBytes = user.email.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(offset, emailBytes.length);
        offset += 4;
        buffer.putBytes(offset, emailBytes);
        offset += emailBytes.length;

        return offset - startOffset;
    }

    private void printHelp()
    {
        System.out.println("可用命令:");
        System.out.println("  order <orderId> <symbol> <price> <qty> <side>");
        System.out.println("    - 发送 Order 消息到 Stream ID " + ORDER_STREAM_ID);
        System.out.println("    - side: B=买入, S=卖出");
        System.out.println("    - 示例: order 1001 AAPL 150.50 100 B");
        System.out.println();
        System.out.println("  user <userId> <username> <email> <age> <balance>");
        System.out.println("    - 发送 User 消息到 Stream ID " + USER_STREAM_ID);
        System.out.println("    - 示例: user 2001 alice alice@example.com 25 1000.50");
        System.out.println();
        System.out.println("  help  - 显示此帮助");
        System.out.println("  exit  - 退出程序");
        System.out.println();
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Sender] 关闭...");
        CloseHelper.closeAll(orderPublication, userPublication, aeron, mediaDriver);
    }

    /**
     * Order POJO.
     */
    public static class Order
    {
        public final long orderId;
        public final String symbol;
        public final double price;
        public final int quantity;
        public final char side;
        public final long timestamp;

        public Order(final long orderId, final String symbol, final double price,
                     final int quantity, final char side, final long timestamp)
        {
            this.orderId = orderId;
            this.symbol = symbol;
            this.price = price;
            this.quantity = quantity;
            this.side = side;
            this.timestamp = timestamp;
        }

        @Override
        public String toString()
        {
            return String.format("Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}",
                orderId, symbol, price, quantity, side);
        }
    }

    /**
     * User POJO.
     */
    public static class User
    {
        public final long userId;
        public final String username;
        public final String email;
        public final int age;
        public final double balance;
        public final boolean isActive;

        public User(final long userId, final String username, final String email,
                    final int age, final double balance, final boolean isActive)
        {
            this.userId = userId;
            this.username = username;
            this.email = email;
            this.age = age;
            this.balance = balance;
            this.isActive = isActive;
        }

        @Override
        public String toString()
        {
            return String.format("User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}",
                userId, username, email, age, balance, isActive);
        }
    }
}

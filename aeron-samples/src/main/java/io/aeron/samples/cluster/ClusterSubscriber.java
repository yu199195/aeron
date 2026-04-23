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

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 集群订阅者：通过 AeronCluster 订阅集群消息。
 * <p>
 * <b>功能</b>：
 * <ul>
 *   <li>通过 AeronCluster 连接到集群</li>
 *   <li>发送订阅命令：SUBSCRIBE ORDER 或 SUBSCRIBE USER</li>
 *   <li>实时接收集群推送的消息</li>
 *   <li>支持 leader 切换</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 订阅 Order 消息
 * java ClusterSubscriber "0=localhost:9002,1=localhost:9102,2=localhost:9202" ORDER
 *
 * # 订阅 User 消息
 * java ClusterSubscriber "0=localhost:9002,1=localhost:9102,2=localhost:9202" USER
 *
 * # 订阅所有消息
 * java ClusterSubscriber "0=localhost:9002,1=localhost:9102,2=localhost:9202" ORDER,USER
 *
 * # 远程集群
 * java ClusterSubscriber "0=192.168.1.100:9002,1=192.168.1.101:9102,2=192.168.1.102:9202" ORDER
 * </pre>
 * <p>
 * <b>优点</b>：
 * <ul>
 *   <li>✅ 使用标准的 AeronCluster 协议</li>
 *   <li>✅ 无需硬编码 channel 地址</li>
 *   <li>✅ 自动 leader 切换</li>
 *   <li>✅ 适用于 K8s 部署</li>
 *   <li>✅ 消息已经过 Raft 共识</li>
 * </ul>
 */
public class ClusterSubscriber implements AutoCloseable
{
    /** 默认的 ingress endpoints */
    private static final String DEFAULT_INGRESS_ENDPOINTS = "0=localhost:9002,1=localhost:9102,2=localhost:9202";

    /** 消息类型 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    private final MediaDriver mediaDriver;
    private final AeronCluster aeronCluster;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

    private long messagesReceived = 0;

    public static void main(final String[] args)
    {
        if (args.length < 1)
        {
            printUsage();
            System.exit(1);
        }

        final String ingressEndpoints = args.length > 1 ? args[0] : DEFAULT_INGRESS_ENDPOINTS;
        final String subscriptionTypes = args.length > 1 ? args[1] : args[0];

        System.out.println("========================================");
        System.out.println("  集群订阅者");
        System.out.println("  (使用 AeronCluster)");
        System.out.println("========================================");
        System.out.println("Ingress Endpoints: " + ingressEndpoints);
        System.out.println("订阅类型: " + subscriptionTypes);
        System.out.println();
        System.out.println("优点:");
        System.out.println("  - 使用标准的 AeronCluster 协议");
        System.out.println("  - 无需硬编码地址");
        System.out.println("  - 自动 leader 切换");
        System.out.println("  - 适用于 K8s 部署");
        System.out.println();

        try (ClusterSubscriber subscriber = new ClusterSubscriber(ingressEndpoints))
        {
            subscriber.subscribe(subscriptionTypes);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    private static void printUsage()
    {
        System.out.println("集群订阅者：通过 AeronCluster 订阅集群消息");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java ClusterSubscriber [ingressEndpoints] <subscriptionTypes>");
        System.out.println();
        System.out.println("参数:");
        System.out.println("  ingressEndpoints   - 集群 ingress endpoints（可选，默认本地 3 节点）");
        System.out.println("  subscriptionTypes  - 要订阅的消息类型（ORDER, USER, 或 ORDER,USER）");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 订阅 Order 消息（本地集群）");
        System.out.println("  java ClusterSubscriber ORDER");
        System.out.println();
        System.out.println("  # 订阅 User 消息");
        System.out.println("  java ClusterSubscriber USER");
        System.out.println();
        System.out.println("  # 订阅所有消息");
        System.out.println("  java ClusterSubscriber ORDER,USER");
        System.out.println();
        System.out.println("  # 远程集群");
        System.out.println("  java ClusterSubscriber \"0=192.168.1.100:9002,1=192.168.1.101:9102\" ORDER");
    }

    public ClusterSubscriber(final String ingressEndpoints)
    {
        System.out.println("[Subscriber] 启动 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Subscriber] 连接到集群...");
        System.out.println("[Subscriber] Ingress Endpoints: " + ingressEndpoints);

        // 创建 Egress 监听器
        final EgressListener egressListener = new EgressListener()
        {
            @Override
            public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
                messagesReceived++;
                handleMessage(buffer, offset, length);
            }

            @Override
            public void onSessionEvent(
                final long correlationId,
                final long clusterSessionId,
                final long leadershipTermId,
                final int leaderMemberId,
                final EventCode code,
                final String detail)
            {
                System.out.println("[Session Event] code=" + code + ", detail=" + detail + ", leader=" + leaderMemberId);
            }

            @Override
            public void onNewLeader(
                final long clusterSessionId,
                final long leadershipTermId,
                final int leaderMemberId,
                final String ingressEndpoints)
            {
                System.out.println("[New Leader] memberId=" + leaderMemberId + ", term=" + leadershipTermId);
            }
        };

        aeronCluster = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .ingressChannel("aeron:udp")
            .ingressEndpoints(ingressEndpoints)
            .egressListener(egressListener)
            .errorHandler(throwable -> {
                System.err.println("[Error] " + throwable.getMessage());
                throwable.printStackTrace();
            }));

        System.out.println("[Subscriber] 已连接到集群");
        System.out.println("[Subscriber] Session ID: " + aeronCluster.clusterSessionId());
        System.out.println("[Subscriber] Leader Member ID: " + aeronCluster.leaderMemberId());
        System.out.println();
    }

    private void subscribe(final String subscriptionTypes)
    {
        final String[] types = subscriptionTypes.split(",");

        for (final String type : types)
        {
            final String trimmedType = type.trim().toUpperCase();
            System.out.println("[Subscriber] 发送订阅请求: " + trimmedType);

            final String command = "SUBSCRIBE " + trimmedType;
            final byte[] bytes = command.getBytes(StandardCharsets.UTF_8);
            buffer.putBytes(0, bytes);

            long result;
            int attempts = 0;
            while ((result = aeronCluster.offer(buffer, 0, bytes.length)) < 0 && attempts++ < 3)
            {
                aeronCluster.pollEgress();
                idleStrategy.idle();
            }

            if (result > 0)
            {
                System.out.println("[Subscriber] 订阅请求已发送: " + trimmedType);
            }
            else
            {
                System.err.println("[Subscriber] 订阅请求发送失败: " + trimmedType);
            }

            // 等待响应
            for (int i = 0; i < 10; i++)
            {
                aeronCluster.pollEgress();
                idleStrategy.idle();
            }
        }

        System.out.println("[Subscriber] 等待消息推送...");
        System.out.println("按 Ctrl+C 退出");
        System.out.println("========================================");
        System.out.println();

        // 持续轮询 egress
        try
        {
            while (true)
            {
                aeronCluster.pollEgress();
                idleStrategy.idle();
            }
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] " + ex.getMessage());
        }

        System.out.println();
        System.out.println("========================================");
        System.out.println("订阅结束");
        System.out.println("共接收 " + messagesReceived + " 条消息");
        System.out.println("========================================");
    }

    private void handleMessage(final DirectBuffer buffer, final int offset, final int length)
    {
        if (length == 0)
        {
            return;
        }

        final byte firstByte = buffer.getByte(offset);

        // 如果是文本响应
        if (firstByte > 32 && firstByte < 127)
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final String message = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("[Response] " + message);
            return;
        }

        // 二进制消息
        if (firstByte == MSG_TYPE_ORDER)
        {
            decodeAndPrintOrder(buffer, offset);
        }
        else if (firstByte == MSG_TYPE_USER)
        {
            decodeAndPrintUser(buffer, offset);
        }
        else
        {
            System.out.printf("[Unknown] Type: %d, Length: %d%n", firstByte, length);
        }
    }

    private void decodeAndPrintOrder(final DirectBuffer buffer, int offset)
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

        System.out.printf("[Order] Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}%n",
            orderId, symbol, price, quantity, side);
    }

    private void decodeAndPrintUser(final DirectBuffer buffer, int offset)
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

        System.out.printf("[User] User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}%n",
            userId, username, email, age, balance, isActive);
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Subscriber] 关闭...");
        CloseHelper.closeAll(aeronCluster, mediaDriver);
    }
}

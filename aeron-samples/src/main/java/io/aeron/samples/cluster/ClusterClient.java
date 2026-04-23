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
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 集群客户端：连接到 Aeron Cluster，从控制台读取消息并发送到集群。
 * <p>
 * 启动后保持运行，等待用户在控制台输入消息：
 * <ul>
 *   <li>输入任意文本 + Enter → 发送到集群</li>
 *   <li>输入 {@code quit} 或 {@code exit} → 优雅退出</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterClient
 * &gt; Hello Cluster!
 * [Client] 收到响应: [Echo #1] Hello Cluster!
 * &gt; quit
 * </pre>
 */
public class ClusterClient implements AutoCloseable
{
    /** 集群所有节点的 ingress 端点（格式：memberId=host:port） */
    private static final String INGRESS_ENDPOINTS = "0=localhost:9002,1=localhost:9102,2=localhost:9202";

    // 提示：也可以使用 ClusterConfig 自动生成：
    // ClusterConfig.ingressEndpoints(Arrays.asList("localhost", "localhost", "localhost"), 9000, ClusterConfig.CLIENT_FACING_PORT_OFFSET)

    /** 客户端本地 MediaDriver（用于与集群通信） */
    private final MediaDriver mediaDriver;

    /** AeronCluster 客户端实例 */
    private final AeronCluster aeronCluster;

    /** Idle 策略（用于等待响应） */
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    /** 控制 egress 轮询线程的退出标志 */
    private final AtomicBoolean running = new AtomicBoolean(true);

    /**
     * 主入口：启动客户端，从控制台读取消息并发送到集群，直到输入 quit/exit。
     */
    public static void main(final String[] args)
    {
        System.out.println("========================================");
        System.out.println("  Aeron Cluster Client (Interactive)");
        System.out.println("========================================");
        System.out.println("Ingress 端点: " + INGRESS_ENDPOINTS);
        System.out.println("输入消息后按 Enter 发送，输入 quit 或 exit 退出");
        System.out.println();

        try (ClusterClient client = new ClusterClient())
        {
            client.runInteractive();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    /**
     * 交互式控制台循环：
     * <ul>
     *   <li>独立 daemon 线程持续 pollEgress，确保响应及时打印</li>
     *   <li>主线程阻塞读取 stdin，每行作为一条消息发送</li>
     * </ul>
     */
    private void runInteractive()
    {
        // 后台线程：持续轮询 egress，接收集群响应
        final Thread pollThread = new Thread(() ->
        {
            while (running.get())
            {
                aeronCluster.pollEgress();
                idleStrategy.idle();
            }
        }, "egress-poller");
        pollThread.setDaemon(true);
        pollThread.start();

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("> ");
        System.out.flush();

        try
        {
            String line;
            while ((line = reader.readLine()) != null)
            {
                final String trimmed = line.trim();
                if (trimmed.equalsIgnoreCase("quit") || trimmed.equalsIgnoreCase("exit"))
                {
                    System.out.println("[Client] 正在退出...");
                    break;
                }
                if (!trimmed.isEmpty())
                {
                    sendMessage(trimmed);
                }
                System.out.print("> ");
                System.out.flush();
            }
        }
        catch (final Exception ex)
        {
            System.err.println("[Client] 读取输入异常: " + ex.getMessage());
        }
        finally
        {
            running.set(false);
        }
    }

    /**
     * 构造客户端：启动 MediaDriver 并连接到集群。
     */
    public ClusterClient()
    {
        System.out.println("[Client] 启动 MediaDriver...");

        // 启动客户端 MediaDriver（嵌入式）
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Client] 连接到集群...");

        // 连接到集群
        aeronCluster = AeronCluster.connect(new AeronCluster.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .ingressChannel("aeron:udp")
            .ingressEndpoints(INGRESS_ENDPOINTS)
                .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(new ClientEgressListener()));

        System.out.println("[Client] 已连接到集群，会话 ID: " + aeronCluster.clusterSessionId());
        System.out.println();
    }

    /**
     * 发送单条消息到集群，处理背压重试。
     *
     * @param message 要发送的字符串消息
     */
    private void sendMessage(final String message)
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        buffer.putBytes(0, bytes);

        System.out.println("[Client] 发送消息: " + message);

        long result;
        int attempts = 0;
        while ((result = aeronCluster.offer(buffer, 0, bytes.length)) < 0 && attempts++ < 3)
        {
            if (result == io.aeron.Publication.NOT_CONNECTED)
            {
                System.err.println("[Client] 未连接到 Leader，等待重连...");
                idleStrategy.idle();
            }
            else if (result == io.aeron.Publication.BACK_PRESSURED)
            {
                System.err.println("[Client] 背压，等待重试...");
                idleStrategy.idle();
            }
            else
            {
                System.err.println("[Client] 发送失败: " + result);
                break;
            }
        }

        if (result <= 0)
        {
            System.err.println("[Client] 消息发送失败，返回值: " + result);
        }
    }

    /**
     * Egress 监听器：处理来自集群的响应消息。
     */
    private static class ClientEgressListener implements EgressListener
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
            // 解码响应消息
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final String response = new String(bytes, StandardCharsets.UTF_8);

            System.out.println("[Client] 收到响应: " + response);
        }

        @Override
        public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final io.aeron.cluster.codecs.EventCode code,
            final String detail)
        {
            System.out.println("[Client] 会话事件: " + code + " - " + detail +
                " (Leader: " + leaderMemberId + ", Term: " + leadershipTermId + ")");
        }

        @Override
        public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints)
        {
            System.out.println("[Client] 新 Leader 选出: memberId=" + leaderMemberId +
                ", term=" + leadershipTermId + ", endpoints=" + ingressEndpoints);
        }
    }

    /**
     * 关闭客户端与 MediaDriver。
     */
    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Client] 关闭连接...");
        CloseHelper.closeAll(aeronCluster, mediaDriver);
    }

}

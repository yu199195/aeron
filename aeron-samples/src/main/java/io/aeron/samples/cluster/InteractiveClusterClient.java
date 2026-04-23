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
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
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
 * 交互式集群客户端：从控制台输入消息发送到 Aeron Cluster，支持历史消息回放。
 * <p>
 * 本类演示如何：
 * <ul>
 *   <li><b>交互式输入</b>：从标准输入读取用户输入的消息</li>
 *   <li><b>连接到集群</b>：通过 ingress 端点连接（自动发现 Leader）</li>
 *   <li><b>发送消息</b>：向集群发送用户输入的消息</li>
 *   <li><b>接收响应</b>：实时显示来自集群的响应</li>
 *   <li><b>回放历史</b>：从 Archive 回放历史消息</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 启动交互式客户端
 * java -cp aeron-all.jar io.aeron.samples.cluster.InteractiveClusterClient
 *
 * # 可用命令：
 * > help                    - 显示帮助
 * > session                 - 显示会话信息
 * > list                    - 列出所有录制
 * > replay &lt;recordingId&gt;    - 回放指定录制
 * > replay &lt;id&gt; &lt;pos&gt; &lt;len&gt; - 从指定位置回放
 * > exit                    - 退出
 *
 * # 发送消息示例：
 * > Hello Cluster!
 * [Client] 发送消息: Hello Cluster!
 * [Client] 收到响应: [Echo #1] Hello Cluster!
 *
 * # 回放示例：
 * > list
 * Recording ID: 0, Length: 5120 bytes
 *
 * > replay 0
 * [Replayed] Hello Cluster!
 * [Replayed] 测试消息
 * </pre>
 */
public class InteractiveClusterClient implements AutoCloseable
{
    /** 集群所有节点的 ingress 端点（格式：memberId=host:port） */
    private static final String INGRESS_ENDPOINTS = "0=localhost:9002,1=localhost:9102,2=localhost:9202";

    /** Archive 控制通道（使用 IPC 连接本地 Archive，或指定节点的 UDP 端口） */
    private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:ipc";  // 本地 IPC 连接
    // 如果要连接远程 Archive，使用: "aeron:udp?endpoint=localhost:9001"

    /** 回放通道 */
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final int REPLAY_STREAM_ID = 101;

    /** 客户端本地 MediaDriver（用于与集群通信） */
    private final MediaDriver mediaDriver;

    /** Aeron 实例 */
    private final Aeron aeron;

    /** AeronCluster 客户端实例 */
    private final AeronCluster aeronCluster;

    /** AeronArchive 客户端实例（用于回放） */
    private AeronArchive aeronArchive;

    /** Idle 策略（用于等待响应） */
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));

    /**
     * 主入口：启动交互式客户端。
     *
     * @param args 命令行参数（未使用）
     */
    public static void main(final String[] args)
    {
        System.out.println("========================================");
        System.out.println("  Aeron Cluster 交互式客户端");
        System.out.println("========================================");
        System.out.println("Ingress 端点: " + INGRESS_ENDPOINTS);
        System.out.println();

        try (InteractiveClusterClient client = new InteractiveClusterClient())
        {
            client.runInteractiveMode();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    /**
     * 构造客户端：启动 MediaDriver 并连接到集群。
     */
    public InteractiveClusterClient()
    {
        System.out.println("[Client] 启动 MediaDriver...");

        // 启动客户端 MediaDriver（嵌入式）
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Client] 启动 Aeron...");
        aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName()));

        System.out.println("[Client] 连接到集群...");

        // 连接到集群
        aeronCluster = AeronCluster.connect(new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp").ingressStreamId(100)
            .ingressEndpoints(INGRESS_ENDPOINTS)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .egressListener(new ClientEgressListener()));
        System.out.println("[Client] 已连接到集群，会话 ID: " + aeronCluster.clusterSessionId());
        System.out.println();
    }

    /**
     * 交互式控制台模式：从标准输入读取消息并发送。
     */
    private void runInteractiveMode()
    {
        System.out.println("========================================");
        System.out.println("  交互式模式 (输入命令或消息)");
        System.out.println("========================================");
        System.out.println("命令:");
        System.out.println("  exit / quit              - 退出客户端");
        System.out.println("  help                     - 显示帮助");
        System.out.println("  session                  - 显示会话信息");
        System.out.println("  list                     - 列出所有录制");
        System.out.println("  replay <id>              - 回放整个录制");
        System.out.println("  replay <id> <pos> <len>  - 从指定位置回放");
        System.out.println("  其他输入                 - 作为消息发送到集群");
        System.out.println();
        System.out.println("开始输入消息或命令（按 Ctrl+D 或输入 'exit' 退出）:");
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
                    break;  // EOF (Ctrl+D)
                }

                final String input = scanner.nextLine().trim();

                if (input.isEmpty())
                {
                    continue;
                }

                // 处理命令
                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit"))
                {
                    System.out.println("[Client] 退出中...");
                    break;
                }
                else if (input.equalsIgnoreCase("help"))
                {
                    printHelp();
                    continue;
                }
                else if (input.equalsIgnoreCase("session"))
                {
                    printSessionInfo();
                    continue;
                }
                else if (input.equalsIgnoreCase("list"))
                {
                    listRecordings();
                    continue;
                }
                else if (input.toLowerCase().startsWith("replay "))
                {
                    handleReplayCommand(input);
                    continue;
                }

                // 发送消息到集群
                sendMessage(input);

                // Poll egress 以接收响应（尝试多次）
                for (int i = 0; i < 20; i++)
                {
                    if (aeronCluster.pollEgress() > 0)
                    {
                        break;  // 收到响应，退出轮询
                    }
                    idleStrategy.idle();
                }
            }
        }

        System.out.println();
        System.out.println("[Client] 交互式会话结束");
    }

    /**
     * 打印帮助信息。
     */
    private void printHelp()
    {
        System.out.println();
        System.out.println("========================================");
        System.out.println("  可用命令");
        System.out.println("========================================");
        System.out.println("  exit / quit              - 退出客户端");
        System.out.println("  help                     - 显示此帮助");
        System.out.println("  session                  - 显示当前会话信息");
        System.out.println();
        System.out.println("  list                     - 列出所有 Archive 录制");
        System.out.println("  replay <recordingId>     - 回放整个录制");
        System.out.println("  replay <id> <pos>        - 从指定位置回放到结尾");
        System.out.println("  replay <id> <pos> <len>  - 回放指定位置和长度");
        System.out.println();
        System.out.println("  直接输入文本即可发送到集群");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  > Hello Cluster!         - 发送消息");
        System.out.println("  > list                   - 查看录制");
        System.out.println("  > replay 0               - 回放录制 0");
        System.out.println("  > replay 0 1024 2048     - 回放部分数据");
        System.out.println("========================================");
        System.out.println();
    }

    /**
     * 打印会话信息。
     */
    private void printSessionInfo()
    {
        System.out.println();
        System.out.println("========================================");
        System.out.println("  会话信息");
        System.out.println("========================================");
        System.out.println("  会话 ID:      " + aeronCluster.clusterSessionId());
        System.out.println("  Leader ID:    " + aeronCluster.leaderMemberId());
        System.out.println("  已关闭:       " + aeronCluster.isClosed());
        System.out.println("========================================");
        System.out.println();
    }

    /**
     * 发送单条消息到集群。
     *
     * @param message 要发送的字符串消息
     */
    private void sendMessage(final String message)
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        buffer.putBytes(0, bytes);

        System.out.println("[Client] 发送消息: " + message);

        // 尝试发送，处理背压
        long result;
        int attempts = 0;
        while ((result = aeronCluster.offer(buffer, 0, bytes.length)) < 0 && attempts++ < 3)
        {
            if (result == io.aeron.Publication.NOT_CONNECTED)
            {
                System.err.println("[Client] 未连接到 Leader，等待重连...");
                sleep(1000);
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

        if (result > 0)
        {
            // 发送成功
        }
        else
        {
            System.err.println("[Client] 消息发送失败");
        }
    }

    /**
     * 列出所有录制。
     */
    private void listRecordings()
    {
        try
        {
            ensureArchiveConnected();

            System.out.println();
            System.out.println("========================================");
            System.out.println("  可用的录制 (Recordings)");
            System.out.println("========================================");

            final RecordingDescriptorConsumer consumer = (
                controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp,
                startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength,
                mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
            {
                System.out.println();
                System.out.printf("Recording ID: %d%n", recordingId);
                System.out.printf("  Channel:    %s%n", originalChannel);
                System.out.printf("  Stream ID:  %d%n", streamId);
                System.out.printf("  Position:   %d - %d%n", startPosition, stopPosition);
                System.out.printf("  Length:     %d bytes%n", stopPosition - startPosition);
                System.out.printf("  Start:      %s%n", new java.util.Date(startTimestamp));
                if (stopTimestamp != 0)
                {
                    System.out.printf("  Stop:       %s%n", new java.util.Date(stopTimestamp));
                }
                else
                {
                    System.out.println("  Stop:       [正在录制中]");
                }
            };

            final int count = aeronArchive.listRecordings(0L, 100, consumer);

            System.out.println();
            System.out.println("========================================");
            System.out.println("共找到 " + count + " 个录制");
            System.out.println("========================================");
            System.out.println();
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] 列出录制失败: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 处理回放命令。
     */
    private void handleReplayCommand(final String command)
    {
        try
        {
            final String[] parts = command.split("\\s+");

            if (parts.length < 2)
            {
                System.err.println("[Error] 用法: replay <recordingId> [position] [length]");
                return;
            }

            final long recordingId = Long.parseLong(parts[1]);
            long position = 0;
            long length = Long.MAX_VALUE;

            if (parts.length >= 3)
            {
                position = Long.parseLong(parts[2]);
            }

            if (parts.length >= 4)
            {
                length = Long.parseLong(parts[3]);
            }

            replayRecording(recordingId, position, length);
        }
        catch (final NumberFormatException ex)
        {
            System.err.println("[Error] 无效的数字参数");
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] 回放失败: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 回放指定的录制。
     */
    private void replayRecording(final long recordingId, final long position, final long length)
    {
        try
        {
            ensureArchiveConnected();

            System.out.println();
            System.out.println("========================================");
            System.out.println("  回放录制 " + recordingId);
            System.out.println("========================================");
            System.out.printf("Position: %d%n", position);
            System.out.printf("Length:   %s%n", length == Long.MAX_VALUE ? "全部" : String.valueOf(length));
            System.out.println();

            // 启动回放
            final long replaySessionId = aeronArchive.startReplay(
                recordingId, position, length, REPLAY_CHANNEL, REPLAY_STREAM_ID);

            System.out.println("[Replay] 回放会话已启动，会话 ID: " + replaySessionId);

            // 订阅回放通道
            final Subscription subscription = aeron.addSubscription(REPLAY_CHANNEL, REPLAY_STREAM_ID);

            System.out.println("[Replay] 等待回放流连接...");
            int waitCount = 0;
            while (!subscription.isConnected() && waitCount++ < 100)
            {
                idleStrategy.idle();
            }

            if (!subscription.isConnected())
            {
                System.err.println("[Error] 回放流连接超时");
                CloseHelper.close(subscription);
                return;
            }

            System.out.println("[Replay] 回放流已连接，开始接收数据...");
            System.out.println("========================================");
            System.out.println();

            // 创建消息处理器
            final FragmentHandler fragmentHandler = new FragmentAssembler(this::onReplayedMessage);

            // 接收回放的消息
            long messagesReceived = 0;
            int idleCount = 0;
            final int maxIdleCount = 50;  // 最多空闲 50 次（约 500ms）

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
            System.out.println();

            CloseHelper.close(subscription);
        }
        catch (final Exception ex)
        {
            System.err.println("[Error] 回放失败: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 处理回放的消息。
     */
    private void onReplayedMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        try
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(offset, bytes);
            final String message = new String(bytes, StandardCharsets.UTF_8);
            System.out.println("[Replayed] " + message);
        }
        catch (final Exception ex)
        {
            System.out.printf("[Replayed] Binary message (%d bytes) at position %d%n", length, header.position());
        }
    }

    /**
     * 确保 Archive 连接已建立。
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

            System.out.println("[Archive] 已连接到 Archive");
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
        CloseHelper.closeAll(aeronArchive, aeronCluster, aeron, mediaDriver);
    }

    /**
     * 睡眠指定毫秒数。
     *
     * @param millis 毫秒数
     */
    private static void sleep(final long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
        }
    }
}

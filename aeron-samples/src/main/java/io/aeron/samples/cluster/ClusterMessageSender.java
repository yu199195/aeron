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

import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.messages.Order;
import io.aeron.samples.cluster.messages.User;
import io.aeron.samples.cluster.sbe.MessageHeaderDecoder;
import io.aeron.samples.cluster.sbe.MessageHeaderEncoder;
import io.aeron.samples.cluster.sbe.OrderDecoder;
import io.aeron.samples.cluster.sbe.OrderEncoder;
import io.aeron.samples.cluster.sbe.UserDecoder;
import io.aeron.samples.cluster.sbe.UserEncoder;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

/**
 * 集群消息发送者：使用 AeronCluster 客户端连接到集群。
 * <p>
 * <b>与 MessageSender 的区别</b>：
 * <ul>
 *   <li>使用 {@link AeronCluster} 连接到集群（正规方式）</li>
 *   <li>消息经过 Raft 共识</li>
 *   <li>ClusteredService 的 onSessionMessage 会收到</li>
 *   <li>不能使用自定义 streamId（集群内部管理）</li>
 *   <li>Archive 自动录制</li>
 * </ul>
 * <p>
 * <b>Archive 回放</b>：
 * <ul>
 *   <li>{@code replay <nodeId>}：从头全量回放指定节点 Archive 中的最新录制</li>
 *   <li>{@code replay <nodeId> resume}：断点续播，从上次记录的 position 继续</li>
 *   <li>断点位置保存在 {@code cluster-replay-checkpoint.properties} 文件中</li>
 *   <li>nodeId 对应 Archive 端口：0→8010, 1→8011, 2→8012（见 ClusterNode）</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 连接到本地集群
 * java ClusterMessageSender
 *
 * # 连接到远程集群
 * java ClusterMessageSender "0=192.168.1.100:9002,1=192.168.1.101:9102,2=192.168.1.102:9202"
 * </pre>
 * <p>
 * <b>消息格式</b>：
 * <pre>
 * [消息类型(1字节)][消息数据]
 *
 * Order: [1][orderId(8)][price(8)][quantity(4)][side(1)][timestamp(8)][symbolLen(4)][symbol(var)]
 * User:  [2][userId(8)][age(4)][balance(8)][active(1)][usernameLen(4)][username(var)][emailLen(4)][email(var)]
 * </pre>
 */
public class ClusterMessageSender implements AutoCloseable
{
    /** 默认的 ingress endpoints（本地 3 节点集群） */
    private static final String DEFAULT_INGRESS_ENDPOINTS = "0=localhost:9002,1=localhost:9102,2=localhost:9202";

    /**
     * Archive 控制端口基础值：nodeId 0→8010, 1→8011, 2→8012（与 ClusterNode 保持一致）。
     */
    private static final int ARCHIVE_CONTROL_PORT_BASE = 8010;

    /** 回放用的本地通道（ephemeral 端口，避免与集群端口冲突） */
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /** 回放专用 streamId */
    private static final int REPLAY_STREAM_ID = 101;

    /** 断点文件路径 */
    private static final String CHECKPOINT_FILE = "cluster-replay-checkpoint.properties";

    /** 断点 key 格式：recording.<recordingId>.position */
    private static final String CHECKPOINT_KEY_PREFIX = "recording.";
    private static final String CHECKPOINT_KEY_SUFFIX = ".position";

    private final MediaDriver mediaDriver;
    private final AeronCluster aeronCluster;
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.MILLISECONDS.toNanos(10));
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    // SBE 编解码器（flyweight，复用同一实例，无对象分配）
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final OrderEncoder orderEncoder = new OrderEncoder();
    private final UserEncoder userEncoder = new UserEncoder();
    private final OrderDecoder orderDecoder = new OrderDecoder();
    private final UserDecoder userDecoder = new UserDecoder();

    public static void main(final String[] args)
    {
        final String ingressEndpoints = args.length > 0 ? args[0] : DEFAULT_INGRESS_ENDPOINTS;

        System.out.println("========================================");
        System.out.println("  集群消息发送者");
        System.out.println("  (使用 AeronCluster 客户端)");
        System.out.println("========================================");
        System.out.println("Ingress Endpoints: " + ingressEndpoints);
        System.out.println();
        System.out.println("特点:");
        System.out.println("  - 消息经过 Raft 共识");
        System.out.println("  - ClusteredService 会收到消息");
        System.out.println("  - Archive 自动录制");
        System.out.println("  - 自动 leader 切换");
        System.out.println();

        try (ClusterMessageSender sender = new ClusterMessageSender(ingressEndpoints))
        {
            sender.runInteractive();
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public ClusterMessageSender(final String ingressEndpoints)
    {
        System.out.println("[Sender] 启动 MediaDriver...");
        mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .dirDeleteOnStart(true));

        System.out.println("[Sender] 连接到集群...");
        System.out.println("[Sender] Ingress Endpoints: " + ingressEndpoints);

        // 创建 Egress 监听器（接收集群响应）
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
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                final String response = new String(bytes, StandardCharsets.UTF_8);
                System.out.println("[Response] " + response);
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

        System.out.println("[Sender] 已连接到集群");
        System.out.println("[Sender] Session ID: " + aeronCluster.clusterSessionId());
        System.out.println("[Sender] Leader Member ID: " + aeronCluster.leaderMemberId());
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
                // 轮询 egress 消息
                aeronCluster.pollEgress();

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
                    else if (command.equals("status"))
                    {
                        printStatus();
                    }
                    else if (command.equals("order"))
                    {
                        handleOrderCommand(parts);
                    }
                    else if (command.equals("user"))
                    {
                        handleUserCommand(parts);
                    }
                    else if (command.equals("replay"))
                    {
                        handleReplayCommand(parts);
                    }
                    else if (command.equals("recordings"))
                    {
                        handleRecordingsCommand(parts);
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

                // 处理完命令后再次轮询
                aeronCluster.pollEgress();
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
        final long result = offerToCluster(buffer, 0, length);

        if (result > 0)
        {
            System.out.println("[Sent] Position: " + result);
            refreshLeaderRecordingIds();
        }
        else
        {
            System.err.println("[Error] 发送失败: " + result);
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
        final long result = offerToCluster(buffer, 0, length);

        if (result > 0)
        {
            System.out.println("[Sent] Position: " + result);
            refreshLeaderRecordingIds();
        }
        else
        {
            System.err.println("[Error] 发送失败: " + result);
        }
    }

    private long offerToCluster(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        long result;
        int attempts = 0;

        while ((result = aeronCluster.offer(buffer, offset, length)) < 0 && attempts++ < 3)
        {
            if (result == Publication.CLOSED)
            {
                System.err.println("[Retry] Publication closed");
                throw new IllegalStateException("Cluster session closed");
            }
            else
            {
                System.err.println("[Retry] Back pressure, attempt " + attempts);
                idleStrategy.idle();
                aeronCluster.pollEgress();
            }
        }

        return result;
    }

    /**
     * 用 SBE Encoder 将 Order 序列化到 buffer。
     * 布局：[MessageHeader 8B][fixed fields 29B][symbol var]
     */
    private int encodeOrder(final MutableDirectBuffer buffer, final int offset, final Order order)
    {
        final byte[] symbolBytes = order.symbol.getBytes(StandardCharsets.UTF_8);

        orderEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder)
            .orderId(order.orderId)
            .price((long)(order.price * 100))
            .quantity(order.quantity)
            .side((byte) order.side)
            .timestamp(order.timestamp)
            .putSymbol(symbolBytes, 0, symbolBytes.length);

        return MessageHeaderEncoder.ENCODED_LENGTH + orderEncoder.encodedLength();
    }

    /**
     * 用 SBE Encoder 将 User 序列化到 buffer。
     * 布局：[MessageHeader 8B][fixed fields 21B][username var][email var]
     */
    private int encodeUser(final MutableDirectBuffer buffer, final int offset, final User user)
    {
        final byte[] usernameBytes = user.username.getBytes(StandardCharsets.UTF_8);
        final byte[] emailBytes = user.email.getBytes(StandardCharsets.UTF_8);

        userEncoder.wrapAndApplyHeader(buffer, offset, headerEncoder)
            .userId(user.userId)
            .age(user.age)
            .balance((long)(user.balance * 100))
            .isActive((short)(user.isActive ? 1 : 0))
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putEmail(emailBytes, 0, emailBytes.length);

        return MessageHeaderEncoder.ENCODED_LENGTH + userEncoder.encodedLength();
    }

    private void printStatus()
    {
        System.out.println();
        System.out.println("集群状态:");
        System.out.println("  Session ID:      " + aeronCluster.clusterSessionId());
        System.out.println("  Leader Member:   " + aeronCluster.leaderMemberId());
        System.out.println("  Leadership Term: " + aeronCluster.leadershipTermId());
        System.out.println("  Is Closed:       " + aeronCluster.isClosed());
        System.out.println();
    }

    private void printHelp()
    {
        System.out.println("可用命令:");
        System.out.println("  order <orderId> <symbol> <price> <qty> <side>");
        System.out.println("    - 发送 Order 消息到集群");
        System.out.println("    - side: B=买入, S=卖出");
        System.out.println("    - 示例: order 1001 AAPL 150.50 100 B");
        System.out.println();
        System.out.println("  user <userId> <username> <email> <age> <balance>");
        System.out.println("    - 发送 User 消息到集群");
        System.out.println("    - 示例: user 2001 alice alice@example.com 25 1000.50");
        System.out.println();
        System.out.println("  recordings                - 列出 leader Archive 的所有录制（含 recordingId）");
        System.out.println();
        System.out.println("  replay                    - 从头回放 leader Archive 中最新录制");
        System.out.println("  replay resume             - 断点续播最新录制");
        System.out.println("  replay <recordingId>      - 从头回放指定 recordingId");
        System.out.println("  replay <recordingId> resume - 断点续播指定 recordingId");
        System.out.println("    - 均自动连接当前 leader 节点 Archive");
        System.out.println("    - 示例: replay 3");
        System.out.println("    - 示例: replay 3 resume");
        System.out.println();
        System.out.println("  status - 显示集群状态");
        System.out.println("  help   - 显示此帮助");
        System.out.println("  exit   - 退出程序");
        System.out.println();
    }

    // ==================== Archive 回放 ====================

    /**
     * 处理 recordings 命令：列出当前 leader 节点 Archive 中的所有录制记录。
     * <p>
     * 自动从 {@link AeronCluster#leaderMemberId()} 获取 leader，无需手动指定 nodeId。
     * 同时把查询到的 recordingId 保存到断点文件，方便后续 replay 时指定。
     */
    private void handleRecordingsCommand(final String[] parts)
    {
        final int nodeId = aeronCluster.leaderMemberId();
        final String controlChannel = "aeron:udp?endpoint=localhost:" + (ARCHIVE_CONTROL_PORT_BASE + nodeId);
        System.out.println("[Recordings] 查询 Leader(Node " + nodeId + ") Archive: " + controlChannel);

        try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .controlRequestChannel(controlChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            System.out.println();
            System.out.println("RecordingId  StreamId  StartPos      StopPos       Status   Channel");
            System.out.println("------------ --------- ------------- ------------- -------- -------");

            final int[] count = {0};
            archive.listRecordings(0, 1000,
                (controlSessionId, correlationId, recordingId,
                startTimestamp, stopTimestamp,
                startPosition, stopPosition,
                initialTermId, segmentFileLength, termBufferLength, mtuLength,
                sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) ->
                {
                    final String status = stopPosition == -1 ? "active  " : "stopped ";
                    System.out.printf("%-12d %-9d %-13d %-13d %s %s%n",
                        recordingId, streamId, startPosition,
                        stopPosition == -1 ? 0 : stopPosition,
                        status, strippedChannel);

                    // 保存到断点文件，记录已知的 recordingId（position 初始化为 0，不覆盖已有断点）
                    saveRecordingIdIfAbsent(recordingId);
                    count[0]++;
                });

            System.out.println();
            if (count[0] == 0)
            {
                System.out.println("[Recordings] Node " + nodeId + " Archive 中暂无录制记录");
            }
            else
            {
                System.out.println("[Recordings] 共 " + count[0] + " 条录制记录");
                System.out.println("[Recordings] 已保存到断点文件: " + CHECKPOINT_FILE);
                System.out.println("[Recordings] 使用方式: replay <recordingId> [resume]");
            }
            System.out.println();
        }
        catch (final Exception ex)
        {
            System.err.println("[Recordings Error] " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 处理 replay 命令。自动连接当前 leader 节点的 Archive，无需手动指定 nodeId。
     *
     * <pre>
     * replay                       从头回放最新录制
     * replay resume                断点续播最新录制
     * replay &lt;recordingId&gt;         从头回放指定录制
     * replay &lt;recordingId&gt; resume  断点续播指定录制
     * </pre>
     */
    private void handleReplayCommand(final String[] parts)
    {
        // 解析参数：parts[1] 可能是 recordingId（数字）或 "resume"，均可选
        long specifiedRecordingId = -1;
        boolean resume = false;
        if (parts.length >= 2)
        {
            if (parts[1].equalsIgnoreCase("resume"))
            {
                resume = true;
            }
            else
            {
                specifiedRecordingId = Long.parseLong(parts[1]);
                if (parts.length >= 3 && parts[2].equalsIgnoreCase("resume"))
                {
                    resume = true;
                }
            }
        }

        final int nodeId = aeronCluster.leaderMemberId();
        final String controlChannel = "aeron:udp?endpoint=localhost:" + (ARCHIVE_CONTROL_PORT_BASE + nodeId);
        System.out.println("[Replay] 连接到 Leader(Node " + nodeId + ") Archive: " + controlChannel);

        try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .controlRequestChannel(controlChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            final long recordingId;
            if (specifiedRecordingId >= 0)
            {
                recordingId = specifiedRecordingId;
                System.out.println("[Replay] 使用指定 recordingId=" + recordingId);
            }
            else
            {
                recordingId = findLatestRecording(archive);
                if (recordingId < 0)
                {
                    System.out.println("[Replay] 未找到录制记录，提示: 先运行 recordings 查看可用录制");
                    return;
                }
                System.out.println("[Replay] 自动选择最新录制: recordingId=" + recordingId);
            }

            final long startPosition = resume ? loadCheckpoint(recordingId) : 0L;
            System.out.println("[Replay] 模式: " + (resume ? "断点续播" : "从头全量回放") +
                ", startPosition=" + startPosition);

            doReplay(archive, recordingId, startPosition);
        }
        catch (final Exception ex)
        {
            System.err.println("[Replay Error] " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    /**
     * 执行回放，从 startPosition 到录制末尾。
     * 每处理一条消息都更新断点文件，以支持下次断点续播。
     */
    private void doReplay(final AeronArchive archive, final long recordingId, final long startPosition)
    {
        // startReplay 返回值低 32 位是 replay publication 的 sessionId，
        // 必须加入 channel URI 确保只订阅这条 replay 流（避免与其他流混淆）。
        final long replaySessionId = archive.startReplay(
            recordingId, startPosition, Long.MAX_VALUE, REPLAY_CHANNEL, REPLAY_STREAM_ID);

        final String replayChannelWithSession = ChannelUri.addSessionId(REPLAY_CHANNEL, (int) replaySessionId);
        System.out.println("[Replay] 回放通道: " + replayChannelWithSession);

        final long[] replayCount = {0};
        final long[] lastPosition = {startPosition};

        final FragmentHandler handler = (buf, offset, length, header) ->
        {
            replayCount[0]++;
            lastPosition[0] = header.position();

            // 用 SBE MessageHeader 解析 templateId，替代原来的手动 msgType 字节
            headerDecoder.wrap(buf, offset);
            final int templateId = headerDecoder.templateId();

            if (templateId == OrderDecoder.TEMPLATE_ID)
            {
                printReplayOrder(buf, offset, replayCount[0]);
            }
            else if (templateId == UserDecoder.TEMPLATE_ID)
            {
                printReplayUser(buf, offset, replayCount[0]);
            }
            else
            {
                // 其他消息（如 Raft 内部消息）直接打印 templateId 和长度
                System.out.println("[Replay #" + replayCount[0] + "] templateId=" + templateId +
                    ", length=" + length + ", position=" + header.position());
            }

            // 每条消息都更新断点，保证即使中途退出也不丢进度
            saveCheckpoint(recordingId, lastPosition[0]);
        };

        final SleepingIdleStrategy replayIdle = new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1));

        try (io.aeron.Aeron aeron = io.aeron.Aeron.connect(
            new io.aeron.Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Subscription subscription = aeron.addSubscription(replayChannelWithSession, REPLAY_STREAM_ID))
        {
            System.out.println("[Replay] 等待回放流建连...");
            while (!subscription.isConnected())
            {
                replayIdle.idle();
            }

            final io.aeron.Image image = subscription.imageAtIndex(0);
            System.out.println("[Replay] 开始接收...");

            // Image 关闭（Archive replay publication 关闭）即表示回放结束
            while (true)
            {
                final int fragments = image.poll(handler, 10);
                if (fragments == 0 && image.isClosed())
                {
                    break;
                }
                replayIdle.idle(fragments);
            }
        }

        System.out.println("[Replay] 完成，共回放 " + replayCount[0] + " 条消息，最终 position=" + lastPosition[0]);
    }

    /**
     * 遍历 Archive catalog，取 recordingId 最大的一条（即最新录制）。
     * <p>
     * {@code listRecordings} 是分页接口，每次最多返回 count 条；
     * count=1000 对 demo 场景足够。生产环境录制数量巨大时，应循环直到返回 0。
     */
    private long findLatestRecording(final AeronArchive archive)
    {
        final MutableLong latestRecordingId = new MutableLong(-1);

        archive.listRecordings(0, 1000,
            (controlSessionId, correlationId, recordingId,
            startTimestamp, stopTimestamp,
            startPosition, stopPosition,
            initialTermId, segmentFileLength, termBufferLength, mtuLength,
            sessionId, streamId,
            strippedChannel, originalChannel, sourceIdentity) ->
            {
                if (recordingId > latestRecordingId.get())
                {
                    latestRecordingId.set(recordingId);
                }
            });

        return latestRecordingId.get();
    }

    // ==================== 断点持久化 ====================

    /**
     * 从断点文件加载上次消费到的 position。
     * 如果文件不存在或没有该 recordingId 的记录，返回 0（从头播放）。
     */
    private long loadCheckpoint(final long recordingId)
    {
        final File file = new File(CHECKPOINT_FILE);
        if (!file.exists())
        {
            System.out.println("[Checkpoint] 无断点文件，从 position=0 开始");
            return 0L;
        }

        final Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(file))
        {
            props.load(fis);
        }
        catch (final IOException ex)
        {
            System.err.println("[Checkpoint] 读取断点文件失败: " + ex.getMessage());
            return 0L;
        }

        final String key = CHECKPOINT_KEY_PREFIX + recordingId + CHECKPOINT_KEY_SUFFIX;
        final String value = props.getProperty(key);
        if (value == null)
        {
            System.out.println("[Checkpoint] recordingId=" + recordingId + " 无断点记录，从 position=0 开始");
            return 0L;
        }

        final long position = Long.parseLong(value);
        System.out.println("[Checkpoint] 加载断点: recordingId=" + recordingId + ", position=" + position);
        return position;
    }

    /**
     * 将当前消费到的 position 写入断点文件。
     * 使用 Properties 格式，key 为 {@code recording.<recordingId>.position}。
     */
    private void saveCheckpoint(final long recordingId, final long position)
    {
        final File file = new File(CHECKPOINT_FILE);
        final Properties props = new Properties();

        // 先加载现有断点（避免覆盖其他 recordingId 的记录）
        if (file.exists())
        {
            try (FileInputStream fis = new FileInputStream(file))
            {
                props.load(fis);
            }
            catch (final IOException ex)
            {
                System.err.println("[Checkpoint] 读取已有断点失败: " + ex.getMessage());
            }
        }

        final String key = CHECKPOINT_KEY_PREFIX + recordingId + CHECKPOINT_KEY_SUFFIX;
        props.setProperty(key, String.valueOf(position));

        try (FileOutputStream fos = new FileOutputStream(file))
        {
            props.store(fos, "Cluster replay checkpoint - recordingId=" + recordingId);
        }
        catch (final IOException ex)
        {
            System.err.println("[Checkpoint] 写入断点失败: " + ex.getMessage());
        }
    }

    /**
     * 仅在断点文件中不存在该 recordingId 时，初始化其 position=0。
     * 不覆盖已有断点，避免破坏进度。
     */
    private void saveRecordingIdIfAbsent(final long recordingId)
    {
        final File file = new File(CHECKPOINT_FILE);
        final Properties props = new Properties();
        if (file.exists())
        {
            try (FileInputStream fis = new FileInputStream(file))
            {
                props.load(fis);
            }
            catch (final IOException ex)
            {
                System.err.println("[Checkpoint] 读取断点文件失败: " + ex.getMessage());
                return;
            }
        }

        final String key = CHECKPOINT_KEY_PREFIX + recordingId + CHECKPOINT_KEY_SUFFIX;
        if (props.containsKey(key))
        {
            return; // 已有断点，不覆盖
        }

        props.setProperty(key, "0");
        try (FileOutputStream fos = new FileOutputStream(file))
        {
            props.store(fos, "Cluster replay checkpoint");
        }
        catch (final IOException ex)
        {
            System.err.println("[Checkpoint] 写入断点文件失败: " + ex.getMessage());
        }
    }

    /**
     * 发送消息成功后，自动连接当前 leader 节点的 Archive，查询所有录制记录并保存 recordingId。
     * <p>
     * 这样用户在发完消息后可以立刻用 recordings 或 replay 命令找到对应的 recordingId。
     * 连接失败时仅打印警告，不中断主流程。
     */
    private void refreshLeaderRecordingIds()
    {
        final int leaderMemberId = aeronCluster.leaderMemberId();
        final String controlChannel = "aeron:udp?endpoint=localhost:" + (ARCHIVE_CONTROL_PORT_BASE + leaderMemberId);

        try (AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .controlRequestChannel(controlChannel)
            .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            final int[] found = {0};
            archive.listRecordings(0, 1000,
                (controlSessionId, correlationId, recordingId,
                startTimestamp, stopTimestamp,
                startPosition, stopPosition,
                initialTermId, segmentFileLength, termBufferLength, mtuLength,
                sessionId, streamId,
                strippedChannel, originalChannel, sourceIdentity) ->
                {
                    saveRecordingIdIfAbsent(recordingId);
                    found[0]++;
                });

            if (found[0] > 0)
            {
                System.out.println("[Archive] Leader(Node " + leaderMemberId + ") 有 " + found[0] +
                    " 条录制，recordingId 已保存到 " + CHECKPOINT_FILE);
                System.out.println("[Archive] 可运行 recordings 查看详情，或直接 replay 回放");
            }
        }
        catch (final Exception ex)
        {
            System.err.println("[Archive] 查询 Leader Archive 失败（不影响消息发送）: " + ex.getMessage());
        }
    }

    // ==================== 回放消息解码（打印） ====================

    /**
     * 用 SBE Decoder 解析并打印回放的 Order 消息。
     */
    private void printReplayOrder(final DirectBuffer buf, final int offset, final long seq)
    {
        orderDecoder.wrapAndApplyHeader(buf, offset, headerDecoder);

        final int symbolLen = orderDecoder.symbolLength();
        final byte[] symbolBytes = new byte[symbolLen];
        orderDecoder.getSymbol(symbolBytes, 0, symbolLen);
        final String symbol = new String(symbolBytes, StandardCharsets.UTF_8);

        System.out.printf("[Replay #%d] Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c, ts=%d}%n",
            seq,
            orderDecoder.orderId(),
            symbol,
            orderDecoder.price() / 100.0,
            orderDecoder.quantity(),
            (char) orderDecoder.side(),
            orderDecoder.timestamp());
    }

    /**
     * 用 SBE Decoder 解析并打印回放的 User 消息。
     */
    private void printReplayUser(final DirectBuffer buf, final int offset, final long seq)
    {
        userDecoder.wrapAndApplyHeader(buf, offset, headerDecoder);

        final int usernameLen = userDecoder.usernameLength();
        final byte[] usernameBytes = new byte[usernameLen];
        userDecoder.getUsername(usernameBytes, 0, usernameLen);
        final String username = new String(usernameBytes, StandardCharsets.UTF_8);

        final int emailLen = userDecoder.emailLength();
        final byte[] emailBytes = new byte[emailLen];
        userDecoder.getEmail(emailBytes, 0, emailLen);
        final String email = new String(emailBytes, StandardCharsets.UTF_8);

        System.out.printf("[Replay #%d] User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}%n",
            seq,
            userDecoder.userId(),
            username,
            email,
            userDecoder.age(),
            userDecoder.balance() / 100.0,
            userDecoder.isActive() != 0);
    }

    @Override
    public void close()
    {
        System.out.println();
        System.out.println("[Sender] 关闭...");
        CloseHelper.closeAll(aeronCluster, mediaDriver);
    }
}

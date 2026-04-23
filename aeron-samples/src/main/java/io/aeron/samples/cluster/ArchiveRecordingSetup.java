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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingSubscriptionDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;

/**
 * Archive 录制配置工具：配置 Archive 自动录制外部消息。
 * <p>
 * <b>功能</b>：
 * <ul>
 *   <li>连接到集群节点的 Archive</li>
 *   <li>配置自动录制 streamId 100（Order 消息）</li>
 *   <li>配置自动录制 streamId 200（User 消息）</li>
 *   <li>列出当前的录制订阅</li>
 * </ul>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 配置本地集群 Node 0 的 Archive
 * java ArchiveRecordingSetup localhost 9001
 *
 * # 配置远程集群
 * java ArchiveRecordingSetup 192.168.1.100 9001
 * </pre>
 * <p>
 * <b>说明</b>：
 * <ul>
 *   <li>一次性配置，重启后需要重新运行</li>
 *   <li>录制会保存到 Archive 的存储目录</li>
 *   <li>MessageReceiver 可以回放这些录制</li>
 * </ul>
 */
public class ArchiveRecordingSetup
{
    /** 要录制的 channel（需要与 MessageSender 的 channel 匹配） */
    private static final String RECORDING_CHANNEL = "aeron:udp?endpoint=localhost:9010";

    /** Order 消息的 streamId */
    private static final int ORDER_STREAM_ID = 100;

    /** User 消息的 streamId */
    private static final int USER_STREAM_ID = 200;

    public static void main(final String[] args)
    {
        if (args.length < 1)
        {
            printUsage();
            System.exit(1);
        }

        final String archiveHost = args[0];
        final int archivePort = args.length > 1 ? Integer.parseInt(args[1]) : 9001;

        System.out.println("========================================");
        System.out.println("  Archive 录制配置工具");
        System.out.println("========================================");
        System.out.println("Archive 地址: " + archiveHost + ":" + archivePort);
        System.out.println("录制 Channel: " + RECORDING_CHANNEL);
        System.out.println("录制 Stream IDs: " + ORDER_STREAM_ID + ", " + USER_STREAM_ID);
        System.out.println();

        try
        {
            setupRecordings(archiveHost, archivePort);
            System.out.println();
            System.out.println("========================================");
            System.out.println("配置完成！");
            System.out.println("========================================");
            System.out.println();
            System.out.println("提示:");
            System.out.println("1. 现在可以启动 MessageSender 发送消息");
            System.out.println("2. Archive 会自动录制 streamId 100, 200 的消息");
            System.out.println("3. 使用 MessageReceiver 可以回放这些消息");
        }
        catch (final Exception ex)
        {
            System.err.println();
            System.err.println("========================================");
            System.err.println("配置失败！");
            System.err.println("========================================");
            ex.printStackTrace();
            System.exit(1);
        }
    }

    private static void printUsage()
    {
        System.out.println("Archive 录制配置工具");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  java ArchiveRecordingSetup <archiveHost> [archivePort]");
        System.out.println();
        System.out.println("参数:");
        System.out.println("  archiveHost  - Archive 服务器地址（必需）");
        System.out.println("  archivePort  - Archive 控制端口（默认 9001）");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  # 配置本地 Archive");
        System.out.println("  java ArchiveRecordingSetup localhost");
        System.out.println();
        System.out.println("  # 配置远程 Archive");
        System.out.println("  java ArchiveRecordingSetup 192.168.1.100 9001");
    }

    private static void setupRecordings(final String archiveHost, final int archivePort)
    {
        System.out.println("[Setup] 连接到 Archive...");

        final String controlChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint(archiveHost + ":" + archivePort)
            .build();

        System.out.println("[Setup] Archive 控制通道: " + controlChannel);

        try (Aeron aeron = Aeron.connect();
             AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                 .aeron(aeron)
                 .controlRequestChannel(controlChannel)
                 .controlResponseChannel("aeron:udp?endpoint=localhost:0")))
        {
            System.out.println("[Setup] 已连接到 Archive");
            System.out.println();

            // 列出现有的录制订阅
            System.out.println("[Setup] 检查现有的录制订阅...");
            listRecordingSubscriptions(archive);
            System.out.println();

            // 配置 Order 消息录制
            System.out.println("[Setup] 配置 Order 消息录制 (Stream ID: " + ORDER_STREAM_ID + ")...");
            final long orderSubscriptionId = archive.startRecording(
                RECORDING_CHANNEL,
                ORDER_STREAM_ID,
                SourceLocation.REMOTE,  // 接收远程发送的消息
                true                     // autoStop: false（持续录制）
            );
            System.out.println("[Setup] Order 录制订阅 ID: " + orderSubscriptionId);

            // 配置 User 消息录制
            System.out.println("[Setup] 配置 User 消息录制 (Stream ID: " + USER_STREAM_ID + ")...");
            final long userSubscriptionId = archive.startRecording(
                RECORDING_CHANNEL,
                USER_STREAM_ID,
                SourceLocation.REMOTE,
                true
            );
            System.out.println("[Setup] User 录制订阅 ID: " + userSubscriptionId);
            System.out.println();

            // 再次列出录制订阅，验证配置
            System.out.println("[Setup] 验证配置...");
            listRecordingSubscriptions(archive);
        }
    }

    private static void listRecordingSubscriptions(final AeronArchive archive)
    {
        final RecordingSubscriptionDescriptorConsumer consumer = (
            controlSessionId, correlationId, subscriptionId, streamId, strippedChannel) ->
        {
            System.out.println("  录制订阅:");
            System.out.println("    Subscription ID: " + subscriptionId);
            System.out.println("    Stream ID:       " + streamId);
            System.out.println("    Channel:         " + strippedChannel);
            System.out.println();
        };

        final int count = archive.listRecordingSubscriptions(
            0,              // pseudoIndex
            5,              // subscriptionCount
            ".*",           // channelFragment（匹配所有）
            -1,             // streamId（-1 匹配所有）
            true,           // applyStreamId
            consumer);

        if (count == 0)
        {
            System.out.println("  （无录制订阅）");
        }
        else
        {
            System.out.println("  共 " + count + " 个录制订阅");
        }
    }
}

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
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 转发集群服务：处理集群消息并转发到外部订阅者。
 * <p>
 * <b>功能</b>：
 * <ul>
 *   <li>接收集群客户端消息（经过 Raft 共识）</li>
 *   <li>根据消息类型转发到不同的 streamId</li>
 *   <li>外部订阅者可以订阅特定类型的消息</li>
 *   <li>支持回复客户端</li>
 * </ul>
 * <p>
 * <b>转发规则</b>：
 * <ul>
 *   <li>Order 消息（类型 1） → streamId 100</li>
 *   <li>User 消息（类型 2） → streamId 200</li>
 *   <li>纯文本消息 → 不转发，仅回复</li>
 * </ul>
 * <p>
 * <b>转发 Channel</b>：
 * <pre>
 * aeron:udp?endpoint=localhost:9010
 * </pre>
 * <p>
 * 外部订阅者可以通过以下方式订阅：
 * <pre>
 * Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:9010", 100);  // Order
 * Subscription sub = aeron.addSubscription("aeron:udp?endpoint=localhost:9010", 200);  // User
 * </pre>
 */
public class ForwardingClusteredService implements ClusteredService
{
    /** 转发 channel（外部订阅者监听此地址） */
    private static final String FORWARD_CHANNEL = "aeron:udp?endpoint=localhost:9010";

    /** Order 消息的 streamId */
    private static final int ORDER_STREAM_ID = 100;

    /** User 消息的 streamId */
    private static final int USER_STREAM_ID = 200;

    /** 消息类型 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    private Cluster cluster;
    private Aeron aeron;

    /** 转发 Publication（Order 消息） */
    private Publication orderPublication;

    /** 转发 Publication（User 消息） */
    private Publication userPublication;

    /** 全局消息计数 */
    private long globalMessageCount = 0;

    /** 每个会话的消息计数 */
    private final Long2LongHashMap sessionMessageCounts = new Long2LongHashMap(-1);

    /** 响应缓冲区 */
    private final ExpandableArrayBuffer responseBuffer = new ExpandableArrayBuffer();

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.aeron = cluster.aeron();

        System.out.println("  [ForwardingService] 正在启动...");

        // 创建转发 Publications
        System.out.println("  [ForwardingService] 创建转发 Publications...");
        System.out.println("    Order → " + FORWARD_CHANNEL + " (Stream ID: " + ORDER_STREAM_ID + ")");
        System.out.println("    User  → " + FORWARD_CHANNEL + " (Stream ID: " + USER_STREAM_ID + ")");

        orderPublication = aeron.addPublication(FORWARD_CHANNEL, ORDER_STREAM_ID);
        userPublication = aeron.addPublication(FORWARD_CHANNEL, USER_STREAM_ID);

        // 从快照恢复状态
        if (snapshotImage != null)
        {
            System.out.println("  [ForwardingService] 从快照恢复状态...");
            loadSnapshot(snapshotImage);
            System.out.println("  [ForwardingService] 恢复完成: globalCount=" + globalMessageCount +
                ", sessions=" + sessionMessageCounts.size());
        }

        System.out.println("  [ForwardingService] 已启动");
        System.out.println();
        System.out.println("  [提示] 外部订阅者可以通过以下方式接收消息:");
        System.out.println("    java ExternalSubscriber " + FORWARD_CHANNEL + " 100  # Order 消息");
        System.out.println("    java ExternalSubscriber " + FORWARD_CHANNEL + " 200  # User 消息");
        System.out.println();
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("  [ForwardingService] 会话打开: sessionId=" + session.id());
        sessionMessageCounts.put(session.id(), 0L);

        final String welcome = "欢迎连接到转发集群! sessionId=" + session.id();
        sendResponse(session, welcome);
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("  [ForwardingService] 会话关闭: sessionId=" + session.id() + ", reason=" + closeReason);
        sessionMessageCounts.remove(session.id());
    }

    @Override
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        globalMessageCount++;
        final long sessionCount = sessionMessageCounts.get(session.id()) + 1;
        sessionMessageCounts.put(session.id(), sessionCount);

        // 检查消息类型
        if (length > 0)
        {
            final byte msgType = buffer.getByte(offset);

            if (msgType == MSG_TYPE_ORDER)
            {
                handleOrderMessage(session, buffer, offset, length, sessionCount);
                return;
            }
            else if (msgType == MSG_TYPE_USER)
            {
                handleUserMessage(session, buffer, offset, length, sessionCount);
                return;
            }
        }

        // 默认：纯文本消息
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        final String message = new String(bytes, StandardCharsets.UTF_8);

        System.out.println("  [ForwardingService] 收到文本消息 #" + globalMessageCount + ": " + message);

        final String response = String.format("[Echo #%d] %s", globalMessageCount, message);
        sendResponse(session, response);
    }

    private void handleOrderMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long sessionCount)
    {
        // 解码 Order
        int pos = offset + 1;  // Skip msg type
        final long orderId = buffer.getLong(pos);
        pos += 8;
        final long priceLong = buffer.getLong(pos);
        pos += 8;
        final double price = priceLong / 100.0;
        final int quantity = buffer.getInt(pos);
        pos += 4;
        final char side = (char) buffer.getByte(pos);
        pos += 1;
        final long ts = buffer.getLong(pos);
        pos += 8;
        final int symbolLength = buffer.getInt(pos);
        pos += 4;
        final byte[] symbolBytes = new byte[symbolLength];
        buffer.getBytes(pos, symbolBytes);
        final String symbol = new String(symbolBytes, StandardCharsets.UTF_8);

        System.out.println("  [ForwardingService] 收到 Order #" + globalMessageCount +
            String.format(": Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}",
                orderId, symbol, price, quantity, side));

        // 转发到外部订阅者（streamId 100）
        forwardMessage(orderPublication, buffer, offset, length, "Order", ORDER_STREAM_ID);

        // 回复客户端
        final String response = String.format(
            "[Order Processed #%d] orderId=%d, symbol=%s, price=%.2f, qty=%d, side=%c (全局: %d, 本会话: %d)",
            globalMessageCount, orderId, symbol, price, quantity, side, globalMessageCount, sessionCount);
        sendResponse(session, response);
    }

    private void handleUserMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long sessionCount)
    {
        // 解码 User
        int pos = offset + 1;  // Skip msg type
        final long userId = buffer.getLong(pos);
        pos += 8;
        final int age = buffer.getInt(pos);
        pos += 4;
        final long balanceLong = buffer.getLong(pos);
        pos += 8;
        final double balance = balanceLong / 100.0;
        final boolean isActive = buffer.getByte(pos) != 0;
        pos += 1;
        final int usernameLength = buffer.getInt(pos);
        pos += 4;
        final byte[] usernameBytes = new byte[usernameLength];
        buffer.getBytes(pos, usernameBytes);
        pos += usernameLength;
        final String username = new String(usernameBytes, StandardCharsets.UTF_8);
        final int emailLength = buffer.getInt(pos);
        pos += 4;
        final byte[] emailBytes = new byte[emailLength];
        buffer.getBytes(pos, emailBytes);
        final String email = new String(emailBytes, StandardCharsets.UTF_8);

        System.out.println("  [ForwardingService] 收到 User #" + globalMessageCount +
            String.format(": User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}",
                userId, username, email, age, balance, isActive));

        // 转发到外部订阅者（streamId 200）
        forwardMessage(userPublication, buffer, offset, length, "User", USER_STREAM_ID);

        // 回复客户端
        final String response = String.format(
            "[User Processed #%d] userId=%d, username=%s, email=%s (全局: %d, 本会话: %d)",
            globalMessageCount, userId, username, email, globalMessageCount, sessionCount);
        sendResponse(session, response);
    }

    /**
     * 转发消息到外部订阅者。
     */
    private void forwardMessage(
        final Publication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final String messageType,
        final int streamId)
    {
        long result;
        int attempts = 0;

        while ((result = publication.offer(buffer, offset, length)) < 0 && attempts++ < 3)
        {
            if (result == Publication.NOT_CONNECTED)
            {
                // 没有订阅者，这是正常的
                System.out.println("    [Forward] 没有 " + messageType + " 订阅者（streamId: " + streamId + "）");
                return;
            }
            else if (result == Publication.BACK_PRESSURED)
            {
                cluster.idleStrategy().idle();
            }
        }

        if (result > 0)
        {
            System.out.println("    [Forward] " + messageType + " → streamId " + streamId + " (position: " + result + ")");
        }
        else
        {
            System.err.println("    [Forward] 转发 " + messageType + " 失败: " + result);
        }
    }

    private void sendResponse(final ClientSession session, final String message)
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        responseBuffer.putBytes(0, bytes);

        long result;
        int retries = 0;
        while ((result = session.offer(responseBuffer, 0, bytes.length)) < 0 && retries++ < 3)
        {
            cluster.idleStrategy().idle();
        }

        if (result > 0)
        {
            System.out.println("    [Response] " + message);
        }
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        // 不使用定时器
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        System.out.println("  [ForwardingService] 开始保存快照: globalCount=" + globalMessageCount);

        responseBuffer.putLong(0, globalMessageCount);
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

        responseBuffer.putInt(0, sessionMessageCounts.size());
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Integer.BYTES);

        sessionMessageCounts.forEach((sessionId, count) ->
        {
            responseBuffer.putLong(0, sessionId);
            responseBuffer.putLong(Long.BYTES, count);
            offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES * 2);
        });

        System.out.println("  [ForwardingService] 快照保存完成");
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        System.out.println("  [ForwardingService] 角色变更: " + newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
        System.out.println("  [ForwardingService] 正在关闭...");
        CloseHelper.closeAll(orderPublication, userPublication);
    }

    @Override
    public void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        System.out.println("  [ForwardingService] 新的领导任期: termId=" + leadershipTermId + ", leaderId=" + leaderMemberId);
    }

    private void loadSnapshot(final Image snapshotImage)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

        pollSnapshot(snapshotImage, buffer, Long.BYTES);
        globalMessageCount = buffer.getLong(0);

        pollSnapshot(snapshotImage, buffer, Integer.BYTES);
        final int sessionCount = buffer.getInt(0);

        sessionMessageCounts.clear();
        for (int i = 0; i < sessionCount; i++)
        {
            pollSnapshot(snapshotImage, buffer, Long.BYTES * 2);
            final long sessionId = buffer.getLong(0);
            final long count = buffer.getLong(Long.BYTES);
            sessionMessageCounts.put(sessionId, count);
        }
    }

    private void offerToSnapshot(
        final ExclusivePublication publication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        while (publication.offer(buffer, offset, length) < 0)
        {
            cluster.idleStrategy().idle();
        }
    }

    private void pollSnapshot(final Image image, final MutableDirectBuffer buffer, final int expectedLength)
    {
        int bytesRead = 0;
        while (bytesRead < expectedLength)
        {
            final int fragments = image.poll((buf, offset, length, header) ->
            {
                buffer.putBytes(0, buf, offset, length);
            }, 1);

            if (fragments == 0)
            {
                if (image.isClosed() || image.isEndOfStream())
                {
                    throw new IllegalStateException("快照流意外关闭");
                }
                cluster.idleStrategy().idle();
            }
            else
            {
                bytesRead += expectedLength;
            }
        }
    }
}

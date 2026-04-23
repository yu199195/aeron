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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 发布-订阅集群服务：支持客户端订阅特定类型的消息。
 * <p>
 * <b>功能</b>：
 * <ul>
 *   <li>客户端通过 AeronCluster 连接到集群</li>
 *   <li>发送订阅请求：SUBSCRIBE ORDER 或 SUBSCRIBE USER</li>
 *   <li>ClusteredService 维护订阅者列表</li>
 *   <li>当收到消息时，推送给所有相关订阅者</li>
 * </ul>
 * <p>
 * <b>消息协议</b>：
 * <pre>
 * # 订阅 Order 消息
 * SUBSCRIBE ORDER
 *
 * # 订阅 User 消息
 * SUBSCRIBE USER
 *
 * # 取消订阅
 * UNSUBSCRIBE ORDER
 *
 * # 发送 Order 消息（SBE 编码）
 * [1][orderId][price]...
 *
 * # 发送 User 消息（SBE 编码）
 * [2][userId][age]...
 * </pre>
 * <p>
 * <b>优点</b>：
 * <ul>
 *   <li>✅ 所有通信通过 AeronCluster</li>
 *   <li>✅ 无需硬编码 channel 地址</li>
 *   <li>✅ 自动 leader 切换</li>
 *   <li>✅ 适用于 K8s 部署</li>
 * </ul>
 */
public class PubSubClusteredService implements ClusteredService
{
    /** 消息类型 */
    private static final byte MSG_TYPE_ORDER = 1;
    private static final byte MSG_TYPE_USER = 2;

    /** 订阅类型 */
    private static final int SUB_TYPE_ORDER = 1;
    private static final int SUB_TYPE_USER = 2;

    private Cluster cluster;
    private long globalMessageCount = 0;
    private final Long2LongHashMap sessionMessageCounts = new Long2LongHashMap(-1);
    private final ExpandableArrayBuffer responseBuffer = new ExpandableArrayBuffer();

    /**
     * 订阅者信息。
     */
    private static class Subscription
    {
        final long sessionId;
        final int subscriptionType;  // 1=Order, 2=User

        Subscription(final long sessionId, final int subscriptionType)
        {
            this.sessionId = sessionId;
            this.subscriptionType = subscriptionType;
        }
    }

    /** 所有订阅者（sessionId → Subscriptions） */
    private final Long2ObjectHashMap<List<Integer>> subscriptions = new Long2ObjectHashMap<>();

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;

        System.out.println("  [PubSubService] 正在启动...");

        if (snapshotImage != null)
        {
            System.out.println("  [PubSubService] 从快照恢复状态...");
            loadSnapshot(snapshotImage);
            System.out.println("  [PubSubService] 恢复完成: globalCount=" + globalMessageCount);
        }

        System.out.println("  [PubSubService] 已启动");
        System.out.println();
        System.out.println("  [提示] 客户端可以发送以下命令:");
        System.out.println("    SUBSCRIBE ORDER  - 订阅 Order 消息");
        System.out.println("    SUBSCRIBE USER   - 订阅 User 消息");
        System.out.println("    UNSUBSCRIBE ORDER");
        System.out.println("    UNSUBSCRIBE USER");
        System.out.println();
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("  [PubSubService] 会话打开: sessionId=" + session.id());
        sessionMessageCounts.put(session.id(), 0L);
        subscriptions.put(session.id(), new ArrayList<>());

        final String welcome = "欢迎连接到 PubSub 集群! sessionId=" + session.id() +
            "\n使用命令: SUBSCRIBE ORDER 或 SUBSCRIBE USER";
        sendResponse(session, welcome);
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("  [PubSubService] 会话关闭: sessionId=" + session.id() + ", reason=" + closeReason);
        sessionMessageCounts.remove(session.id());
        subscriptions.remove(session.id());
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

        // 检查是否是文本命令（订阅/取消订阅）
        if (length > 0)
        {
            final byte firstByte = buffer.getByte(offset);

            // 如果是 ASCII 字符，当作文本命令处理
            if (firstByte > 32 && firstByte < 127)
            {
                final byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                final String command = new String(bytes, StandardCharsets.UTF_8).trim();

                if (handleCommand(session, command))
                {
                    return;
                }
            }

            // 否则当作二进制消息（Order/User）
            if (firstByte == MSG_TYPE_ORDER)
            {
                handleOrderMessage(session, buffer, offset, length, sessionCount);
                return;
            }
            else if (firstByte == MSG_TYPE_USER)
            {
                handleUserMessage(session, buffer, offset, length, sessionCount);
                return;
            }
        }

        // 默认：回显
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        final String message = new String(bytes, StandardCharsets.UTF_8);
        System.out.println("  [PubSubService] 收到消息 #" + globalMessageCount + ": " + message);
        sendResponse(session, "[Echo] " + message);
    }

    /**
     * 处理订阅命令。
     */
    private boolean handleCommand(final ClientSession session, final String command)
    {
        if (command.startsWith("SUBSCRIBE "))
        {
            final String type = command.substring(10).trim();
            final List<Integer> sessionSubs = subscriptions.get(session.id());

            if (type.equals("ORDER"))
            {
                if (!sessionSubs.contains(SUB_TYPE_ORDER))
                {
                    sessionSubs.add(SUB_TYPE_ORDER);
                    System.out.println("  [PubSubService] 会话 " + session.id() + " 订阅 Order 消息");
                    sendResponse(session, "[OK] 已订阅 Order 消息");
                }
                else
                {
                    sendResponse(session, "[WARN] 已经订阅了 Order 消息");
                }
                return true;
            }
            else if (type.equals("USER"))
            {
                if (!sessionSubs.contains(SUB_TYPE_USER))
                {
                    sessionSubs.add(SUB_TYPE_USER);
                    System.out.println("  [PubSubService] 会话 " + session.id() + " 订阅 User 消息");
                    sendResponse(session, "[OK] 已订阅 User 消息");
                }
                else
                {
                    sendResponse(session, "[WARN] 已经订阅了 User 消息");
                }
                return true;
            }
        }
        else if (command.startsWith("UNSUBSCRIBE "))
        {
            final String type = command.substring(12).trim();
            final List<Integer> sessionSubs = subscriptions.get(session.id());

            if (type.equals("ORDER"))
            {
                sessionSubs.remove(Integer.valueOf(SUB_TYPE_ORDER));
                System.out.println("  [PubSubService] 会话 " + session.id() + " 取消订阅 Order 消息");
                sendResponse(session, "[OK] 已取消订阅 Order 消息");
                return true;
            }
            else if (type.equals("USER"))
            {
                sessionSubs.remove(Integer.valueOf(SUB_TYPE_USER));
                System.out.println("  [PubSubService] 会话 " + session.id() + " 取消订阅 User 消息");
                sendResponse(session, "[OK] 已取消订阅 User 消息");
                return true;
            }
        }

        return false;
    }

    private void handleOrderMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long sessionCount)
    {
        // 解码 Order
        int pos = offset + 1;
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

        System.out.println("  [PubSubService] 收到 Order #" + globalMessageCount +
            String.format(": Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c}",
                orderId, symbol, price, quantity, side));

        // 推送给所有订阅 Order 的会话
        broadcastToSubscribers(SUB_TYPE_ORDER, buffer, offset, length, "Order");

        // 回复发送者
        final String response = String.format(
            "[Order Published #%d] orderId=%d, symbol=%s, subscribers=%d",
            globalMessageCount, orderId, symbol, countSubscribers(SUB_TYPE_ORDER));
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
        int pos = offset + 1;
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

        System.out.println("  [PubSubService] 收到 User #" + globalMessageCount +
            String.format(": User{id=%d, username=%s, age=%d, balance=%.2f}",
                userId, username, age, balance));

        // 推送给所有订阅 User 的会话
        broadcastToSubscribers(SUB_TYPE_USER, buffer, offset, length, "User");

        // 回复发送者
        final String response = String.format(
            "[User Published #%d] userId=%d, username=%s, subscribers=%d",
            globalMessageCount, userId, username, countSubscribers(SUB_TYPE_USER));
        sendResponse(session, response);
    }

    /**
     * 广播消息给所有订阅者。
     */
    private void broadcastToSubscribers(
        final int subscriptionType,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final String messageType)
    {
        int delivered = 0;

        for (final ClientSession session : cluster.clientSessions())
        {
            final List<Integer> sessionSubs = subscriptions.get(session.id());
            if (sessionSubs != null && sessionSubs.contains(subscriptionType))
            {
                long result;
                int attempts = 0;
                while ((result = session.offer(buffer, offset, length)) < 0 && attempts++ < 3)
                {
                    cluster.idleStrategy().idle();
                }

                if (result > 0)
                {
                    delivered++;
                }
            }
        }

        System.out.println("    [Broadcast] " + messageType + " 已推送给 " + delivered + " 个订阅者");
    }

    private int countSubscribers(final int subscriptionType)
    {
        int count = 0;
        for (final List<Integer> subs : subscriptions.values())
        {
            if (subs.contains(subscriptionType))
            {
                count++;
            }
        }
        return count;
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
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        System.out.println("  [PubSubService] 开始保存快照");

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

        System.out.println("  [PubSubService] 快照保存完成");
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        System.out.println("  [PubSubService] 角色变更: " + newRole);
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
        System.out.println("  [PubSubService] 正在关闭");
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
        System.out.println("  [PubSubService] 新的领导任期: termId=" + leadershipTermId + ", leaderId=" + leaderMemberId);
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

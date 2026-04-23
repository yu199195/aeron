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
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.sbe.MessageHeaderDecoder;
import io.aeron.samples.cluster.sbe.OrderDecoder;
import io.aeron.samples.cluster.sbe.UserDecoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.status.CountersReader;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 简单的集群服务实现：Echo 服务 + 消息计数器。
 * <p>
 * 功能：
 * <ul>
 *   <li><b>Echo 回显</b>：接收客户端消息后，立即回显原内容</li>
 *   <li><b>全局计数</b>：维护集群级别的消息计数器</li>
 *   <li><b>会话计数</b>：为每个客户端会话维护独立的消息计数</li>
 *   <li><b>快照支持</b>：保存/恢复计数器状态</li>
 * </ul>
 * <p>
 * 状态：
 * <ul>
 *   <li>globalMessageCount：所有会话的消息总数</li>
 *   <li>sessionMessageCounts：每个会话的消息数（sessionId → count）</li>
 * </ul>
 */
public class SimpleClusteredService implements ClusteredService
{
    /** 集群对象，用于访问 Cluster API */
    private Cluster cluster;

    /** 全局消息计数（所有会话累计） */
    private long globalMessageCount = 0;

    /** 每个会话的消息计数（sessionId → count） */
    private final Long2LongHashMap sessionMessageCounts = new Long2LongHashMap(-1);

    /**
     * 最近一次快照时的 Raft log position（字节偏移）。
     * <p>
     * 含义：快照已经覆盖了 Raft log 中 [0, snapshotLogPosition) 的全部消息。
     * 如果要从快照恢复后继续回放 Archive，只需从 snapshotLogPosition 开始，
     * 无需重放已被快照吸收的历史消息。
     * <p>
     * 初始值为 -1 表示"尚未打过快照"。
     */
    private long snapshotLogPosition = -1;

    /**
     * 最近一次快照对应的 Archive recordingId。
     * <p>
     * 在 {@link #onTakeSnapshot} 中通过 {@link RecordingPos} 计数器查得，
     * 保存在快照数据里，方便恢复时知道快照来自哪条录制。
     * 初始值为 -1 表示"未知"。
     */
    private long snapshotRecordingId = -1;

    /** 响应缓冲区（预分配，避免 GC） */
    private final ExpandableArrayBuffer responseBuffer = new ExpandableArrayBuffer();

    // SBE 解码器（flyweight，复用同一实例，无对象分配）
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final OrderDecoder orderDecoder = new OrderDecoder();
    private final UserDecoder userDecoder = new UserDecoder();

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;

        // 从快照恢复状态
        if (snapshotImage != null)
        {
            System.out.println("  [Service] 从快照恢复状态...");
            loadSnapshot(snapshotImage);
            System.out.println("  [Service] 恢复完成: globalCount=" + globalMessageCount +
                ", sessions=" + sessionMessageCounts.size() +
                ", snapshotLogPosition=" + snapshotLogPosition +
                ", snapshotRecordingId=" + snapshotRecordingId);
            if (snapshotLogPosition >= 0)
            {
                System.out.println("  [Service] 提示: Archive 回放从 position=" + snapshotLogPosition +
                    " 开始即可（该位置之前已由快照覆盖）");
            }
        }
        else
        {
            System.out.println("  [Service] 首次启动，无快照可恢复");
        }

        System.out.println("  [Service] ClusteredService 已启动");
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
        System.out.println("  [Service] 会话打开: sessionId=" + session.id() +
            ", timestamp=" + timestamp);

        // 初始化该会话的计数器
        sessionMessageCounts.put(session.id(), 0L);

        // 发送欢迎消息
        final String welcome = "欢迎连接到集群! sessionId=" + session.id();
        sendResponse(session, welcome);
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
        System.out.println("  [Service] 会话关闭: sessionId=" + session.id() +
            ", reason=" + closeReason + ", timestamp=" + timestamp);

        // 清理会话计数器
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
        // 更新计数器
        globalMessageCount++;
        final long sessionCount = sessionMessageCounts.get(session.id()) + 1;
        sessionMessageCounts.put(session.id(), sessionCount);

        // 检查是否是 SBE 结构化消息（Order/User），通过 MessageHeader 中的 templateId 判断
        if (length >= MessageHeaderDecoder.ENCODED_LENGTH)
        {
            headerDecoder.wrap(buffer, offset);
            final int templateId = headerDecoder.templateId();

            if (templateId == OrderDecoder.TEMPLATE_ID)
            {
                handleOrderMessage(session, buffer, offset, sessionCount);
                return;
            }
            else if (templateId == UserDecoder.TEMPLATE_ID)
            {
                handleUserMessage(session, buffer, offset, sessionCount);
                return;
            }
        }

        // 默认：纯文本消息（向后兼容）
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        final String message = new String(bytes, StandardCharsets.UTF_8);

        System.out.println("  [Service] 收到消息 #" + globalMessageCount +
            " (session #" + sessionCount + "): " + message);

        // 构造响应：Echo + 计数信息
        final String response = String.format(
            "[Echo #%d] %s (全局: %d, 本会话: %d)",
            globalMessageCount,
            message,
            globalMessageCount,
            sessionCount);

        sendResponse(session, response);
    }

    /**
     * 处理 Order 消息（SBE 解码）。
     */
    private void handleOrderMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final long sessionCount)
    {
        orderDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long orderId = orderDecoder.orderId();
        final double price = orderDecoder.price() / 100.0;
        final int quantity = orderDecoder.quantity();
        final char side = (char) orderDecoder.side();
        final long timestamp = orderDecoder.timestamp();

        final int symbolLen = orderDecoder.symbolLength();
        final byte[] symbolBytes = new byte[symbolLen];
        orderDecoder.getSymbol(symbolBytes, 0, symbolLen);
        final String symbol = new String(symbolBytes, StandardCharsets.UTF_8);

        System.out.println("  [Service] 收到 Order #" + globalMessageCount +
            " (session #" + sessionCount + "): " +
            String.format("Order{id=%d, symbol=%s, price=%.2f, qty=%d, side=%c, ts=%d}",
                orderId, symbol, price, quantity, side, timestamp));

        // 构造响应
        final String response = String.format(
            "[Order Processed #%d] orderId=%d, symbol=%s, price=%.2f, qty=%d, side=%c (全局: %d, 本会话: %d)",
            globalMessageCount, orderId, symbol, price, quantity, side, globalMessageCount, sessionCount);

        sendResponse(session, response);
    }

    /**
     * 处理 User 消息（SBE 解码）。
     */
    private void handleUserMessage(
        final ClientSession session,
        final DirectBuffer buffer,
        final int offset,
        final long sessionCount)
    {
        userDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        final long userId = userDecoder.userId();
        final int age = userDecoder.age();
        final double balance = userDecoder.balance() / 100.0;
        final boolean isActive = userDecoder.isActive() != 0;

        final int usernameLen = userDecoder.usernameLength();
        final byte[] usernameBytes = new byte[usernameLen];
        userDecoder.getUsername(usernameBytes, 0, usernameLen);
        final String username = new String(usernameBytes, StandardCharsets.UTF_8);

        final int emailLen = userDecoder.emailLength();
        final byte[] emailBytes = new byte[emailLen];
        userDecoder.getEmail(emailBytes, 0, emailLen);
        final String email = new String(emailBytes, StandardCharsets.UTF_8);

        System.out.println("  [Service] 收到 User #" + globalMessageCount +
            " (session #" + sessionCount + "): " +
            String.format("User{id=%d, username=%s, email=%s, age=%d, balance=%.2f, active=%s}",
                userId, username, email, age, balance, isActive));

        // 构造响应
        final String response = String.format(
            "[User Processed #%d] userId=%d, username=%s, email=%s, age=%d, balance=%.2f (全局: %d, 本会话: %d)",
            globalMessageCount, userId, username, email, age, balance, globalMessageCount, sessionCount);

        sendResponse(session, response);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
        // 本 demo 不使用定时器
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        // cluster.logPosition()：当前 Raft log 的字节偏移，即"本次快照覆盖到哪里"。
        // 含义：[0, logPosition) 范围内的所有消息已被本快照吸收，
        // 恢复后 Archive 回放只需从 logPosition 开始。
        snapshotLogPosition = cluster.logPosition();

        // 通过 RecordingPos 计数器查出本次快照 publication 对应的 Archive recordingId。
        // snapshotPublication 正在被 Archive 以 spy 方式录制；
        // RecordingPos.findCounterIdBySession 在计数器中找到匹配 sessionId 的那个，
        // 再用 getRecordingId 从同一计数器读出 recordingId。
        // cluster.aeron() 返回 ClusteredServiceAgent 内部共享的 Aeron 实例。
        final CountersReader counters = cluster.aeron().countersReader();
        final int counterId = RecordingPos.findCounterIdBySession(
            counters, snapshotPublication.sessionId(), io.aeron.Aeron.NULL_VALUE);
        snapshotRecordingId = (counterId != CountersReader.NULL_COUNTER_ID)
            ? RecordingPos.getRecordingId(counters, counterId) : -1;

        System.out.println("  [Service] 开始保存快照: globalCount=" + globalMessageCount +
            ", sessions=" + sessionMessageCounts.size() +
            ", logPosition=" + snapshotLogPosition +
            ", snapshotRecordingId=" + snapshotRecordingId);

        // 1. 保存 logPosition（新增：用于 Archive 断点续播定位）
        responseBuffer.putLong(0, snapshotLogPosition);
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

        // 2. 保存 snapshotRecordingId（新增：记录快照来自哪条 Archive 录制）
        responseBuffer.putLong(0, snapshotRecordingId);
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

        // 3. 保存全局计数
        responseBuffer.putLong(0, globalMessageCount);
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

        // 4. 保存会话数量
        responseBuffer.putInt(0, sessionMessageCounts.size());
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Integer.BYTES);

        // 5. 保存每个会话的计数
        sessionMessageCounts.forEach((sessionId, count) ->
        {
            responseBuffer.putLong(0, sessionId);
            responseBuffer.putLong(Long.BYTES, count);
            offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES * 2);
        });

        System.out.println("  [Service] 快照保存完成");
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
        System.out.println("  [Service] 角色变更: " + newRole);

        if (newRole == Cluster.Role.LEADER)
        {
            System.out.println("  [Service] *** 本节点已成为 Leader ***");
        }
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
        System.out.println("  [Service] ClusteredService 正在关闭");
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
        System.out.println("  [Service] 新的领导任期: termId=" + leadershipTermId +
            ", leaderId=" + leaderMemberId);
    }

    /**
     * 从快照恢复状态。读取顺序必须与 {@link #onTakeSnapshot} 写入顺序完全一致。
     */
    private void loadSnapshot(final Image snapshotImage)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

        // 1. 恢复 logPosition（快照覆盖到的 Raft log 位置）
        pollSnapshot(snapshotImage, buffer, Long.BYTES);
        snapshotLogPosition = buffer.getLong(0);

        // 2. 恢复 snapshotRecordingId
        pollSnapshot(snapshotImage, buffer, Long.BYTES);
        snapshotRecordingId = buffer.getLong(0);

        // 3. 恢复全局计数
        pollSnapshot(snapshotImage, buffer, Long.BYTES);
        globalMessageCount = buffer.getLong(0);

        // 4. 恢复会话数量
        pollSnapshot(snapshotImage, buffer, Integer.BYTES);
        final int sessionCount = buffer.getInt(0);

        // 5. 恢复每个会话的计数
        sessionMessageCounts.clear();
        for (int i = 0; i < sessionCount; i++)
        {
            pollSnapshot(snapshotImage, buffer, Long.BYTES * 2);
            final long sessionId = buffer.getLong(0);
            final long count = buffer.getLong(Long.BYTES);
            sessionMessageCounts.put(sessionId, count);
        }
    }

    /**
     * 发送响应给客户端。
     */
    private void
    sendResponse(final ClientSession session, final String message)
    {
        final byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        responseBuffer.putBytes(0, bytes);

        // 尝试发送，处理背压
        long result;
        int retries = 0;
        while ((result = session.offer(responseBuffer, 0, bytes.length)) < 0 && retries++ < 3)
        {
            cluster.idleStrategy().idle();
        }

        if (result > 0)
        {
            System.out.println("  [Service] 已回复: " + message);
        }
        else
        {
            System.err.println("  [Service] 发送失败: " + result);
        }
    }

    /**
     * 将数据写入快照流（处理背压）。
     */
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

    /**
     * 从快照流读取指定长度的数据。
     */
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

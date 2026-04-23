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
package io.aeron.cluster;

import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.ClusterAction;
import io.aeron.cluster.codecs.ClusterActionRequestEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.NewLeadershipTermEventEncoder;
import io.aeron.cluster.codecs.SessionCloseEventEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionOpenEventEncoder;
import io.aeron.cluster.codecs.TimerEventEncoder;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.client.AeronCluster.SESSION_HEADER_LENGTH;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.BitUtil.align;

/**
 * 日志发布器：Leader 用于将消息写入集群日志的核心组件。
 * <p>
 * LogPublisher 是 Aeron Cluster 日志复制机制的 Leader 侧实现，负责：
 * <ul>
 *   <li>将客户端消息写入日志（SessionMessage）</li>
 *   <li>写入会话事件（SessionOpen、SessionClose）</li>
 *   <li>写入定时器事件（TimerEvent）</li>
 *   <li>写入集群动作（ClusterAction.SNAPSHOT）</li>
 *   <li>写入 Leadership Term 事件（NewLeadershipTermEvent）</li>
 * </ul>
 * <p>
 * <b>通信机制</b>：
 * <pre>
 * Leader LogPublisher
 *        │
 *        └─> ExclusivePublication (UDP multicast)
 *            - channel: aeron:udp?endpoint=224.0.1.1:20002|interface=localhost
 *            - streamId: 103
 *            │
 *            ├─> Follower 1 订阅（logSubscription）
 *            ├─> Follower 2 订阅（logSubscription）
 *            └─> Leader Service 订阅（logSubscription）
 * </pre>
 * <p>
 * <b>日志消息类型</b>：
 * <ul>
 *   <li>SessionMessage: 客户端业务消息</li>
 *   <li>SessionOpen: 会话打开事件</li>
 *   <li>SessionClose: 会话关闭事件</li>
 *   <li>TimerEvent: 定时器触发事件</li>
 *   <li>ClusterAction: 集群动作（如快照）</li>
 *   <li>NewLeadershipTermEvent: 新 Leader 上任事件</li>
 * </ul>
 * <p>
 * <b>重试机制</b>：所有写入操作最多重试 3 次（SEND_ATTEMPTS）
 */
final class LogPublisher
{
    // ==================== 常量定义 ====================
    // 发送重试次数：如果 publication.offer() 返回 NOT_CONNECTED 或 BACK_PRESSURED，最多重试 3 次
    private static final int SEND_ATTEMPTS = 3;

    // ==================== 消息编码器（复用，避免 GC） ====================
    // 各种消息类型的编码器，使用 SBE（Simple Binary Encoding）编码
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();  // ← 消息头编码器
    private final SessionMessageHeaderEncoder sessionHeaderEncoder = new SessionMessageHeaderEncoder();  // ← 会话消息头
    private final SessionOpenEventEncoder sessionOpenEventEncoder = new SessionOpenEventEncoder();  // ← 会话打开事件
    private final SessionCloseEventEncoder sessionCloseEventEncoder = new SessionCloseEventEncoder();  // ← 会话关闭事件
    private final TimerEventEncoder timerEventEncoder = new TimerEventEncoder();  // ← 定时器事件
    private final ClusterActionRequestEncoder clusterActionRequestEncoder = new ClusterActionRequestEncoder();  // ← 集群动作
    private final NewLeadershipTermEventEncoder newLeadershipTermEventEncoder = new NewLeadershipTermEventEncoder();  // ← Leadership Term 事件

    // ==================== 缓冲区（复用，避免 GC） ====================
    private final UnsafeBuffer sessionHeaderBuffer = new UnsafeBuffer(new byte[SESSION_HEADER_LENGTH]);  // ← 会话消息头缓冲区
    private final ExpandableArrayBuffer expandableArrayBuffer = new ExpandableArrayBuffer();  // ← 可扩展缓冲区（编码复杂消息）
    private final BufferClaim bufferClaim = new BufferClaim();  // ← 零拷贝写入（tryClaim）

    // ==================== 核心字段 ====================
    private final String destinationChannel;  // ← 目标通道（UDP multicast 地址）
    private ExclusivePublication publication;  // ← 独占发布器（Leader 独占，保证顺序）

    /**
     * 构造函数：创建日志发布器。
     *
     * @param destinationChannel 目标通道（如 "aeron:udp?endpoint=224.0.1.1:20002|interface=localhost"）
     */
    LogPublisher(final String destinationChannel)
    {
        this.destinationChannel = destinationChannel;
        // 预先初始化会话消息头编码器（复用，避免每次编码时重新初始化）
        sessionHeaderEncoder.wrapAndApplyHeader(sessionHeaderBuffer, 0, new MessageHeaderEncoder());
    }

    void publication(final ExclusivePublication publication)
    {
        if (null != this.publication)
        {
            this.publication.close();
        }
        this.publication = publication;
    }

    ExclusivePublication publication()
    {
        return publication;
    }

    void disconnect(final ErrorHandler errorHandler)
    {
        if (null != publication)
        {
            CloseHelper.close(errorHandler, publication);
            this.publication = null;
        }
    }

    long position()
    {
        if (null == publication)
        {
            return 0;
        }

        return publication.position();
    }

    int sessionId()
    {
        return publication.sessionId();
    }

    void addDestination(final String followerLogEndpoint)
    {
        if (null != publication)
        {
            publication.asyncAddDestination(ChannelUri.createDestinationUri(destinationChannel, followerLogEndpoint));
        }
    }

    /**
     * 追加客户端消息到日志：将客户端发送的业务消息写入集群日志。
     * <p>
     * 这是 Aeron Cluster 日志复制的<b>核心方法</b>，所有客户端消息都通过此方法写入日志。
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 编码消息头（SessionMessageHeader）
     *    - leadershipTermId: 当前 Leadership Term ID
     *    - clusterSessionId: 客户端会话 ID
     *    - timestamp: 确定性时间戳（集群时间）
     *
     * 2. 写入日志（publication.offer）
     *    - 发送到 UDP multicast（所有 Follower 和 Service 订阅）
     *    - 返回 logPosition（日志位置）
     *
     * 3. Archive 自动录制
     *    - AeronArchive 监听 publication，自动持久化到磁盘
     *
     * 4. Followers 接收并复制
     *    - 从 logSubscription 接收消息
     *    - 本地 Archive 也会录制
     *    - 发送 appendPosition 确认给 Leader
     *
     * 5. commitPosition 更新
     *    - Leader 收集 quorum 的 appendPosition
     *    - 更新 commitPosition counter
     *    - Service 可以消费到 commitPosition 的消息
     * </pre>
     * <p>
     * <b>消息格式</b>（SBE 编码）：
     * <pre>
     * ┌──────────────────────────────────────────────────────┐
     * │  SessionMessageHeader (32 bytes)                     │
     * ├──────────────────────────────────────────────────────┤
     * │  - leadershipTermId (8 bytes)                        │
     * │  - clusterSessionId (8 bytes)                        │
     * │  - timestamp (8 bytes)                               │
     * │  - padding (8 bytes)                                 │
     * ├──────────────────────────────────────────────────────┤
     * │  业务消息内容 (buffer[offset..offset+length])         │
     * └──────────────────────────────────────────────────────┘
     * </pre>
     * <p>
     * <b>重试机制</b>：最多重试 3 次，失败原因可能是：
     * <ul>
     *   <li>NOT_CONNECTED: 订阅者未连接（等待连接）</li>
     *   <li>BACK_PRESSURED: 网络拥塞（等待缓冲区可用）</li>
     *   <li>CLOSED: publication 已关闭（抛出异常）</li>
     *   <li>MAX_POSITION_EXCEEDED: 达到最大位置（抛出异常）</li>
     * </ul>
     *
     * @param leadershipTermId 当前 Leadership Term ID
     * @param clusterSessionId 客户端会话 ID
     * @param timestamp 确定性时间戳（集群时间，非系统时间）
     * @param buffer 消息内容缓冲区
     * @param offset 消息内容起始偏移
     * @param length 消息内容长度
     * @return 日志位置（logPosition），失败返回负值
     */
    long appendMessage(
        final long leadershipTermId,
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        // ==================== 步骤 1: 编码消息头 ====================
        // 将 leadershipTermId、clusterSessionId、timestamp 编码到 sessionHeaderBuffer
        // 这些信息用于：
        // - leadershipTermId: 确保消息来自当前 Leader（防止旧 Leader 的消息）
        // - clusterSessionId: 标识消息属于哪个客户端会话
        // - timestamp: 确定性时间戳（所有节点使用相同的时间）
        sessionHeaderEncoder
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(clusterSessionId)
            .timestamp(timestamp);

        // ==================== 步骤 2: 写入日志（重试机制） ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        long position;
        do
        {
            // 调用 publication.offer() 写入日志
            // 参数：
            // - sessionHeaderBuffer: 消息头（32 bytes）
            // - buffer: 消息内容
            // - null: 保留字段（未使用）
            // 返回值：
            // - > 0: 成功，返回 logPosition
            // - NOT_CONNECTED (-1): 订阅者未连接
            // - BACK_PRESSURED (-2): 网络拥塞
            // - CLOSED (-3): publication 已关闭
            // - MAX_POSITION_EXCEEDED (-4): 达到最大位置
            position = publication.offer(sessionHeaderBuffer, 0, SESSION_HEADER_LENGTH, buffer, offset, length, null);

            if (position > 0)  // ← 成功写入
            {
                break;
            }

            // ==================== 步骤 3: 检查失败原因 ====================
            // 如果 position 是 CLOSED 或 MAX_POSITION_EXCEEDED，抛出异常
            // 如果是 NOT_CONNECTED 或 BACK_PRESSURED，继续重试
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回日志位置 ====================
        // - 成功：返回 logPosition（> 0）
        // - 失败：返回负值（NOT_CONNECTED 或 BACK_PRESSURED）
        return position;
    }

    /**
     * 追加会话打开事件到日志：记录客户端会话打开事件。
     * <p>
     * 当客户端成功连接到集群并通过认证后，Leader 会调用此方法写入 SessionOpen 事件。
     * 所有节点执行此事件后，会创建会话对象，准备接收该客户端的消息。
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 编码 SessionOpenEvent：
     *    - leadershipTermId: 当前 Leadership Term ID
     *    - clusterSessionId: 分配给客户端的会话 ID
     *    - correlationId: 客户端连接请求的关联 ID
     *    - timestamp: 会话打开时间
     *    - responseStreamId: 响应通道的 stream ID
     *    - responseChannel: 响应通道的 URL
     *    - encodedPrincipal: 认证凭证（如用户名、权限等）
     *
     * 2. 写入日志（重试机制）：
     *    - 使用 expandableArrayBuffer（可扩展缓冲区，适合变长消息）
     *    - 最多重试 3 次
     *
     * 3. Service 消费此事件：
     *    - 调用 service.onSessionOpen(session, timestamp)
     *    - 创建会话对象，准备处理该客户端的消息
     * </pre>
     *
     * @param session 集群会话对象（包含会话 ID、响应通道等信息）
     * @param leadershipTermId 当前 Leadership Term ID
     * @param timestamp 会话打开时间戳（集群时间）
     * @return 日志位置（logPosition），失败返回负值
     */
    long appendSessionOpen(final ClusterSession session, final long leadershipTermId, final long timestamp)
    {
        long position;
        // 获取认证凭证（可能包含用户名、角色、权限等信息）
        final byte[] encodedPrincipal = session.encodedPrincipal();
        // 获取响应通道（Leader 用于向客户端发送响应的通道）
        final String channel = session.responseChannel();

        // ==================== 步骤 1: 编码 SessionOpenEvent ====================
        // 将会话打开事件编码到 expandableArrayBuffer
        // - wrapAndApplyHeader(): 写入 SBE 消息头
        // - leadershipTermId: 当前 Leadership Term ID（确保消息来自当前 Leader）
        // - clusterSessionId: 会话 ID（全局唯一，用于标识客户端）
        // - correlationId: 客户端连接请求的关联 ID（用于客户端匹配响应）
        // - timestamp: 会话打开时间（集群时间，非系统时间）
        // - responseStreamId: 响应通道的 stream ID
        // - responseChannel: 响应通道的 URL（如 "aeron:udp?endpoint=localhost:20001"）
        // - encodedPrincipal: 认证凭证（序列化的用户信息）
        sessionOpenEventEncoder
            .wrapAndApplyHeader(expandableArrayBuffer, 0, messageHeaderEncoder)
            .leadershipTermId(leadershipTermId)
            .clusterSessionId(session.id())
            .correlationId(session.correlationId())
            .timestamp(timestamp)
            .responseStreamId(session.responseStreamId())
            .responseChannel(channel)
            .putEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);

        // 计算消息总长度（消息头 + SessionOpenEvent 内容）
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + sessionOpenEventEncoder.encodedLength();

        // ==================== 步骤 2: 写入日志（重试机制） ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        do
        {
            // 使用 expandableArrayBuffer（可扩展缓冲区）
            // - 适合变长消息（responseChannel 和 encodedPrincipal 长度不固定）
            position = publication.offer(expandableArrayBuffer, 0, length, null);
            if (position > 0)  // ← 成功写入
            {
                break;
            }

            // 检查失败原因（CLOSED 或 MAX_POSITION_EXCEEDED 会抛出异常）
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回日志位置 ====================
        return position;
    }

    /**
     * 追加会话关闭事件到日志：记录客户端会话关闭事件。
     * <p>
     * 当客户端断开连接、超时、或被 Leader 主动关闭时，会调用此方法写入 SessionClose 事件。
     * 所有节点执行此事件后，会清理会话对象，释放相关资源。
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 使用 tryClaim() 零拷贝写入：
     *    - 直接在 publication 的缓冲区中编码消息
     *    - 避免额外的内存拷贝（性能优化）
     *    - 适合固定长度消息
     *
     * 2. 编码 SessionCloseEvent：
     *    - leadershipTermId: 当前 Leadership Term ID
     *    - clusterSessionId: 会话 ID
     *    - timestamp: 会话关闭时间
     *    - closeReason: 关闭原因（CLIENT_CLOSE、TIMEOUT、USER_ACTION 等）
     *
     * 3. 提交 BufferClaim：
     *    - bufferClaim.commit() 使消息对订阅者可见
     *
     * 4. Service 消费此事件：
     *    - 调用 service.onSessionClose(session, timestamp, closeReason)
     *    - 清理会话状态，断开连接
     * </pre>
     * <p>
     * <b>与 appendSessionOpen 的区别</b>：
     * <ul>
     *   <li>使用 tryClaim()（零拷贝）而不是 offer()（因为 SessionClose 是固定长度）</li>
     *   <li>返回 boolean 而不是 long（调用者通常不关心具体的 logPosition）</li>
     * </ul>
     *
     * @param memberId 当前节点的成员 ID（未使用，保留参数）
     * @param session 集群会话对象
     * @param leadershipTermId 当前 Leadership Term ID
     * @param timestamp 会话关闭时间戳（集群时间）
     * @param timeUnit 时间单位（未使用，保留参数）
     * @return true 成功写入，false 失败（需要重试）
     */
    boolean appendSessionClose(
        final int memberId,
        final ClusterSession session,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        // 计算消息总长度（消息头 + SessionCloseEvent 固定长度）
        // - BLOCK_LENGTH: SBE 编码的固定长度（不含变长字段）
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + SessionCloseEventEncoder.BLOCK_LENGTH;

        // ==================== 重试机制 ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        do
        {
            // ==================== 步骤 1: 使用 tryClaim() 零拷贝写入 ====================
            // tryClaim() 直接在 publication 的缓冲区中编码消息：
            // - 避免额外的内存拷贝（相比 offer()）
            // - 适合固定长度消息（SessionClose 不含变长字段）
            // - 返回值：
            //   - > 0: 成功，返回 logPosition
            //   - NOT_CONNECTED (-1): 订阅者未连接
            //   - BACK_PRESSURED (-2): 网络拥塞
            //   - CLOSED (-3): publication 已关闭
            //   - MAX_POSITION_EXCEEDED (-4): 达到最大位置
            final long position = publication.tryClaim(length, bufferClaim);
            if (position > 0)  // ← 成功 claim 缓冲区
            {
                // ==================== 步骤 2: 编码 SessionCloseEvent ====================
                // 直接在 bufferClaim 的缓冲区中编码消息
                // - leadershipTermId: 当前 Leadership Term ID
                // - clusterSessionId: 会话 ID
                // - timestamp: 会话关闭时间
                // - closeReason: 关闭原因（枚举值）：
                //   - CLIENT_CLOSE: 客户端主动关闭
                //   - TIMEOUT: 会话超时
                //   - USER_ACTION: Leader 主动关闭（如调用 closeSession()）
                sessionCloseEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .clusterSessionId(session.id())
                    .timestamp(timestamp)
                    .closeReason(session.closeReason());

                // ==================== 步骤 3: 提交 BufferClaim ====================
                // commit() 使消息对订阅者可见
                // - 在 commit() 之前，消息对订阅者不可见
                // - commit() 后，Followers 和 Service 可以接收到此事件
                bufferClaim.commit();
                return true;  // ← 成功写入
            }

            // 检查失败原因（CLOSED 或 MAX_POSITION_EXCEEDED 会抛出异常）
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回失败 ====================
        // 如果重试 3 次后仍失败，返回 false
        // 调用者可以决定是否再次重试或记录错误
        return false;
    }

    /**
     * 追加定时器事件到日志：记录定时器触发事件。
     * <p>
     * 当业务逻辑通过 cluster.scheduleTimer(correlationId, deadline) 调度定时器后，
     * Leader 会在指定时间到达时调用此方法写入 TimerEvent。所有节点执行此事件后，
     * 会调用业务逻辑的 onTimerEvent() 回调，实现分布式定时任务。
     * <p>
     * <b>定时器的工作原理</b>：
     * <pre>
     * 1. 业务逻辑调度定时器：
     *    cluster.scheduleTimer(42, clusterTime + 5000)
     *    └─> ConsensusModule 记录定时器（timerService）
     *
     * 2. Leader 检测定时器到期：
     *    consensusModule.doWork() → 检查 timerService
     *    └─> 调用 logPublisher.appendTimer(42, ..., clusterTime)
     *
     * 3. 所有节点消费 TimerEvent：
     *    service.onTimerEvent(42, clusterTime)
     *    └─> 业务逻辑执行定时任务（如清理过期数据）
     * </pre>
     * <p>
     * <b>关键特性</b>：
     * <ul>
     *   <li><b>确定性</b>：定时器使用集群时间，所有节点在相同的 logPosition 触发</li>
     *   <li><b>容错性</b>：定时器记录在日志中，Leader 故障后新 Leader 会继续触发</li>
     *   <li><b>幂等性</b>：correlationId 用于去重，业务逻辑需要处理重复触发</li>
     * </ul>
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>会话超时检测（如 30 秒无心跳则关闭会话）</li>
     *   <li>定期快照（如每 5 分钟保存快照）</li>
     *   <li>业务定时任务（如每天 00:00 清理过期订单）</li>
     * </ul>
     *
     * @param correlationId 定时器关联 ID（业务逻辑自定义，用于标识定时器）
     * @param leadershipTermId 当前 Leadership Term ID
     * @param timestamp 定时器触发时间戳（集群时间）
     * @return 日志位置（logPosition），失败返回负值
     */
    long appendTimer(final long correlationId, final long leadershipTermId, final long timestamp)
    {
        // 计算消息总长度（消息头 + TimerEvent 固定长度）
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + TimerEventEncoder.BLOCK_LENGTH;

        // ==================== 重试机制 ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        long position;
        do
        {
            // ==================== 步骤 1: 使用 tryClaim() 零拷贝写入 ====================
            // TimerEvent 是固定长度消息，使用 tryClaim() 可以避免额外的内存拷贝
            position = publication.tryClaim(length, bufferClaim);
            if (position > 0)  // ← 成功 claim 缓冲区
            {
                // ==================== 步骤 2: 编码 TimerEvent ====================
                // - leadershipTermId: 当前 Leadership Term ID
                // - correlationId: 定时器关联 ID（业务逻辑自定义）
                //   - 用于标识定时器（如 correlationId = 会话 ID，用于会话超时检测）
                //   - 用于去重（业务逻辑可以根据 correlationId 判断是否已处理）
                // - timestamp: 定时器触发时间（集群时间）
                //   - 所有节点在相同的 clusterTime 触发定时器
                //   - 确保所有节点执行相同的操作
                timerEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .correlationId(correlationId)
                    .timestamp(timestamp);

                // ==================== 步骤 3: 提交 BufferClaim ====================
                // commit() 使消息对订阅者可见
                bufferClaim.commit();
                break;  // ← 成功写入，退出循环
            }

            // 检查失败原因（CLOSED 或 MAX_POSITION_EXCEEDED 会抛出异常）
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回日志位置 ====================
        // - 成功：返回 logPosition（> 0）
        // - 失败：返回负值（NOT_CONNECTED 或 BACK_PRESSURED）
        return position;
    }

    /**
     * 追加集群动作到日志：触发集群级别的协调动作。
     * <p>
     * 集群动作（ClusterAction）是 Leader 触发的集群级别的协调命令，最常见的是 SNAPSHOT（快照）。
     * 当 Leader 决定保存快照时，会调用此方法写入 ClusterAction.SNAPSHOT，所有节点执行此动作后，
     * 会调用业务逻辑的 onTakeSnapshot() 保存快照。
     * <p>
     * <b>快照流程</b>：
     * <pre>
     * 1. Leader 触发快照：
     *    consensusModule.doWork() → takeSnapshot()
     *    └─> logPublisher.appendClusterAction(SNAPSHOT, ...)
     *
     * 2. Service 消费 ClusterAction：
     *    service.onServiceAction(SNAPSHOT, logPosition)
     *    └─> onTakeSnapshot()：保存业务状态到 AeronArchive
     *
     * 3. ConsensusModule 保存快照：
     *    consensusModule.onServiceAck(recordingId)
     *    └─> 记录快照位置，清理旧日志
     * </pre>
     * <p>
     * <b>为什么需要预计算 logPosition</b>：
     * <ul>
     *   <li>快照需要知道<b>精确的日志位置</b>（snapshot 完成后，logPosition 之前的日志可以删除）</li>
     *   <li>调用 tryClaim() 之前，logPosition 是未知的（因为还没写入）</li>
     *   <li>因此需要预先计算：currentPosition + 消息对齐后的长度 = 快照位置</li>
     * </ul>
     * <p>
     * <b>ClusterAction 类型</b>：
     * <ul>
     *   <li><b>SNAPSHOT</b>：保存快照（最常用）</li>
     *   <li><b>SHUTDOWN</b>：优雅关闭集群（未使用）</li>
     *   <li><b>SUSPEND</b>：暂停集群（未使用）</li>
     *   <li><b>RESUME</b>：恢复集群（未使用）</li>
     * </ul>
     * <p>
     * <b>flags 参数</b>：
     * <ul>
     *   <li>CLUSTER_ACTION_FLAGS_DEFAULT (0): 只有 Leader 执行快照</li>
     *   <li>CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT (1): Follower 也执行快照（standbySnapshotEnabled = true）</li>
     * </ul>
     *
     * @param leadershipTermId 当前 Leadership Term ID
     * @param timestamp 触发时间戳（集群时间）
     * @param action 集群动作类型（通常是 SNAPSHOT）
     * @param flags 动作标志（控制 Follower 是否执行）
     * @return true 成功写入，false 失败（需要重试）
     */
    boolean appendClusterAction(
        final long leadershipTermId,
        final long timestamp,
        final ClusterAction action,
        final int flags)
    {
        // 计算消息总长度（消息头 + ClusterActionRequest 固定长度）
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterActionRequestEncoder.BLOCK_LENGTH;
        // 计算完整消息长度（包含 Aeron 数据帧头）
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        // 对齐到 32 字节边界（Aeron 帧对齐要求）
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        // ==================== 重试机制 ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        do
        {
            // ==================== 预计算 logPosition ====================
            // 快照需要知道精确的日志位置：
            // - 快照完成后，logPosition 之前的日志可以删除
            // - 因此需要预先计算：currentPosition + 消息对齐后的长度 = 快照位置
            // - publication.position(): 当前日志位置
            // - alignedFragmentLength: 本消息对齐后的长度
            // - logPosition: 本消息写入后的日志位置
            final long logPosition = publication.position() + alignedFragmentLength;

            // ==================== 步骤 1: 使用 tryClaim() 零拷贝写入 ====================
            final long position = publication.tryClaim(length, bufferClaim);

            if (position > 0)  // ← 成功 claim 缓冲区
            {
                // ==================== 步骤 2: 编码 ClusterActionRequest ====================
                // - leadershipTermId: 当前 Leadership Term ID
                // - logPosition: 快照位置（预计算的值）
                //   - Service 消费此消息时，会在 logPosition 处保存快照
                //   - ConsensusModule 记录此位置，删除 logPosition 之前的日志
                // - timestamp: 快照触发时间（集群时间）
                // - action: 集群动作类型（通常是 SNAPSHOT）
                // - flags: 动作标志
                //   - CLUSTER_ACTION_FLAGS_DEFAULT (0): 只有 Leader 执行快照
                //   - CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT (1): Follower 也执行快照
                clusterActionRequestEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .action(action)
                    .flags(flags);

                // ==================== 步骤 3: 提交 BufferClaim ====================
                bufferClaim.commit();
                return true;  // ← 成功写入
            }

            // 检查失败原因（CLOSED 或 MAX_POSITION_EXCEEDED 会抛出异常）
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回失败 ====================
        return false;
    }

    /**
     * 追加新 Leadership Term 事件到日志：记录新 Leader 上任事件。
     * <p>
     * 当选举完成，新 Leader 上任后，会调用此方法写入 NewLeadershipTermEvent。
     * 所有节点消费此事件后，会更新本地状态，同步集群配置（时间单位、应用版本等）。
     * <p>
     * <b>NewLeadershipTermEvent 的作用</b>：
     * <ul>
     *   <li><b>标记新 Term</b>：递增 leadershipTermId，防止旧 Leader 的消息被接受</li>
     *   <li><b>同步配置</b>：所有节点使用相同的 timeUnit、appVersion</li>
     *   <li><b>记录基准位置</b>：termBaseLogPosition 是新 Term 的起始日志位置</li>
     *   <li><b>恢复关键信息</b>：Leader 故障重启后，从日志回放此事件，恢复 Term 信息</li>
     * </ul>
     * <p>
     * <b>执行流程</b>：
     * <pre>
     * 1. 选举完成：
     *    election.onCanvassPosition() → 收集到 quorum 票数
     *    └─> election.state(LEADER_READY)
     *
     * 2. Leader 写入 NewLeadershipTermEvent：
     *    consensusModule.consensusWork() → becomeLeader()
     *    └─> logPublisher.appendNewLeadershipTermEvent(...)
     *
     * 3. Service 消费此事件：
     *    service.onNewLeadershipTermEvent(leadershipTermId, timestamp, ...)
     *    └─> 更新 sessionMessageHeaderEncoder.leadershipTermId()
     *    └─> 更新 timeUnit（如 MILLISECONDS → MICROSECONDS）
     *
     * 4. Followers 同步：
     *    - 更新本地 leadershipTermId
     *    - 拒绝旧 Term 的消息（防止脑裂）
     * </pre>
     * <p>
     * <b>关键字段说明</b>：
     * <ul>
     *   <li><b>leadershipTermId</b>：新 Term ID（递增，类似 Raft 的 term）</li>
     *   <li><b>termBaseLogPosition</b>：新 Term 的起始日志位置（用于日志压缩）</li>
     *   <li><b>leaderMemberId</b>：新 Leader 的成员 ID</li>
     *   <li><b>logSessionId</b>：日志 publication 的会话 ID（用于订阅）</li>
     *   <li><b>timeUnit</b>：集群时间单位（MILLISECONDS/MICROSECONDS/NANOSECONDS）</li>
     *   <li><b>appVersion</b>：应用版本号（用于兼容性检查）</li>
     * </ul>
     * <p>
     * <b>为什么需要 termBaseLogPosition</b>：
     * <ul>
     *   <li>新 Leader 可能从旧日志中间开始（如 Follower 追赶后成为 Leader）</li>
     *   <li>termBaseLogPosition 标记新 Term 的起始位置</li>
     *   <li>用于日志压缩：可以删除 termBaseLogPosition 之前的旧 Term 日志</li>
     * </ul>
     * <p>
     * <b>为什么需要 appVersion</b>：
     * <ul>
     *   <li>防止不兼容的版本加入集群（如新版本引入不兼容的消息格式）</li>
     *   <li>Service 启动时会检查：ctx.appVersionValidator().isVersionCompatible()</li>
     *   <li>不兼容则拒绝启动，避免状态不一致</li>
     * </ul>
     *
     * @param leadershipTermId 新 Term ID（递增）
     * @param timestamp 新 Term 开始时间戳（集群时间）
     * @param termBaseLogPosition 新 Term 的起始日志位置
     * @param leaderMemberId 新 Leader 的成员 ID
     * @param logSessionId 日志 publication 的会话 ID
     * @param timeUnit 集群时间单位（MILLISECONDS/MICROSECONDS/NANOSECONDS）
     * @param appVersion 应用版本号（语义版本号，如 1.2.3 → 0x01020300）
     * @return true 成功写入，false 失败（需要重试）
     */
    boolean appendNewLeadershipTermEvent(
        final long leadershipTermId,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
        // 计算消息总长度（消息头 + NewLeadershipTermEvent 固定长度）
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + NewLeadershipTermEventEncoder.BLOCK_LENGTH;
        // 计算完整消息长度（包含 Aeron 数据帧头）
        final int fragmentLength = DataHeaderFlyweight.HEADER_LENGTH + length;
        // 对齐到 32 字节边界（Aeron 帧对齐要求）
        final int alignedFragmentLength = align(fragmentLength, FRAME_ALIGNMENT);

        // ==================== 重试机制 ====================
        int attempts = SEND_ATTEMPTS;  // ← 最多重试 3 次
        do
        {
            // ==================== 预计算 logPosition ====================
            // 同 appendClusterAction()，需要预先计算本消息写入后的日志位置
            // - 用于记录新 Term 的起始位置
            final long logPosition = publication.position() + alignedFragmentLength;

            // ==================== 步骤 1: 使用 tryClaim() 零拷贝写入 ====================
            final long position = publication.tryClaim(length, bufferClaim);

            if (position > 0)  // ← 成功 claim 缓冲区
            {
                // ==================== 步骤 2: 编码 NewLeadershipTermEvent ====================
                // - leadershipTermId: 新 Term ID（递增）
                //   - 所有后续消息都使用此 Term ID
                //   - Followers 拒绝旧 Term 的消息
                // - logPosition: 本消息写入后的日志位置（预计算）
                // - timestamp: 新 Term 开始时间（集群时间）
                // - termBaseLogPosition: 新 Term 的起始日志位置
                //   - 新 Leader 可能从旧日志中间开始
                //   - termBaseLogPosition 标记新 Term 的起点
                // - leaderMemberId: 新 Leader 的成员 ID（0-N）
                // - logSessionId: 日志 publication 的会话 ID
                //   - Followers 用此 ID 订阅日志
                // - timeUnit: 集群时间单位（MILLISECONDS/MICROSECONDS/NANOSECONDS）
                //   - 所有节点使用相同的时间单位
                //   - ClusterClock.map() 将 Java TimeUnit 转换为 SBE 枚举
                // - appVersion: 应用版本号（语义版本号）
                //   - 用于兼容性检查
                //   - 不兼容则拒绝启动
                newLeadershipTermEventEncoder.wrapAndApplyHeader(
                    bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .leadershipTermId(leadershipTermId)
                    .logPosition(logPosition)
                    .timestamp(timestamp)
                    .termBaseLogPosition(termBaseLogPosition)
                    .leaderMemberId(leaderMemberId)
                    .logSessionId(logSessionId)
                    .timeUnit(ClusterClock.map(timeUnit))
                    .appVersion(appVersion);

                // ==================== 步骤 3: 提交 BufferClaim ====================
                bufferClaim.commit();
                return true;  // ← 成功写入
            }

            // 检查失败原因（CLOSED 或 MAX_POSITION_EXCEEDED 会抛出异常）
            checkResult(position, publication);
        }
        while (--attempts > 0);

        // ==================== 返回失败 ====================
        return false;
    }

    private static void checkResult(final long position, final Publication publication)
    {
        if (Publication.CLOSED == position)
        {
            throw new ClusterException("log publication is closed");
        }

        if (Publication.MAX_POSITION_EXCEEDED == position)
        {
            throw new ClusterException(
                "log publication at max position: term-length=" + publication.termBufferLength());
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "LogPublisher{" +
            "destinationChannel='" + destinationChannel + '\'' +
            '}';
    }
}

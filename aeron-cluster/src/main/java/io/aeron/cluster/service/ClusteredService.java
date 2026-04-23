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
package io.aeron.cluster.service;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

/**
 * 集群服务接口：用户业务逻辑必须实现此接口才能在 Aeron Cluster 中运行。
 * <p>
 * ClusteredService 是 Aeron Cluster 的应用层接口，实现了确定性状态机模型：
 * <ul>
 *   <li>所有节点按相同顺序处理相同的日志消息</li>
 *   <li>保证业务逻辑在所有节点产生一致的状态</li>
 *   <li>支持快照与恢复，实现快速故障恢复</li>
 *   <li>提供定时器服务，支持分布式定时任务</li>
 * </ul>
 * <p>
 * <b>关键约束</b>（确保确定性）：
 * <ul>
 *   <li><b>只能在事件回调中修改状态</b>：{@link #onSessionMessage}、{@link #onTimerEvent}、{@link #onSessionOpen}、{@link #onSessionClose}</li>
 *   <li><b>禁止在生命周期方法中发送消息/调度定时器</b>：{@link #onStart}、{@link #onRoleChange}、{@link #onTakeSnapshot}、{@link #onTerminate}</li>
 *   <li><b>禁止使用非确定性输入</b>：不能使用 System.currentTimeMillis()、Random()（未提供种子）、网络IO等</li>
 *   <li><b>禁止在 doBackgroundWork 中修改状态</b>：此方法仅用于非确定性后台任务（如保活连接）</li>
 * </ul>
 * <p>
 * <b>典型实现示例</b>：
 * <pre>{@code
 * public class OrderBookService implements ClusteredService {
 *     private final Map<Long, Order> orders = new HashMap<>();
 *     private Cluster cluster;
 *
 *     public void onStart(Cluster cluster, Image snapshotImage) {
 *         this.cluster = cluster;
 *         if (snapshotImage != null) {
 *             // 从快照恢复状态
 *             loadSnapshot(snapshotImage);
 *         }
 *     }
 *
 *     public void onSessionMessage(ClientSession session, long timestamp,
 *                                   DirectBuffer buffer, int offset, int length, Header header) {
 *         // 处理客户端订单请求
 *         final int msgType = buffer.getInt(offset);
 *         if (msgType == NEW_ORDER) {
 *             Order order = decodeOrder(buffer, offset + 4);
 *             orders.put(order.id, order);
 *
 *             // 发送确认给客户端
 *             cluster.offer(session.id(), confirmationBuffer, 0, confirmationLength);
 *         }
 *     }
 *
 *     public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
 *         // 序列化订单簿状态到快照
 *         for (Order order : orders.values()) {
 *             encodeAndOffer(snapshotPublication, order);
 *         }
 *     }
 * }
 * }</pre>
 *
 * @see Cluster
 * @see ClientSession
 * @see ClusteredServiceContainer
 */
public interface ClusteredService
{
    /**
     * 服务启动事件：执行初始化逻辑并加载快照状态。
     * <p>
     * 此方法在 {@link ClusteredServiceContainer} 启动后、开始处理日志消息前调用。
     * <p>
     * <b>主要职责</b>：
     * <ul>
     *   <li>初始化业务状态（创建数据结构、连接外部系统等）</li>
     *   <li>从快照镜像加载已保存的状态（如果存在）</li>
     *   <li>准备好处理客户端消息</li>
     * </ul>
     * <p>
     * <b>重要约束</b>：
     * <ul>
     *   <li><b>禁止发送消息</b>：不能调用 {@link Cluster#offer} 或 {@link Cluster#scheduleTimer}</li>
     *   <li><b>可能耗时较长</b>：加载大型快照时应周期性调用 {@link Cluster#idleStrategy()#idle()}，避免阻塞过久</li>
     *   <li><b>快照可能为 null</b>：首次启动或未配置快照时，snapshotImage 为 null</li>
     * </ul>
     * <p>
     * <b>快照加载示例</b>：
     * <pre>{@code
     * public void onStart(Cluster cluster, Image snapshotImage) {
     *     this.cluster = cluster;
     *
     *     if (snapshotImage != null) {
     *         final FragmentHandler handler = (buffer, offset, length, header) -> {
     *             // 解码并恢复状态
     *             restoreStateFrom(buffer, offset, length);
     *         };
     *
     *         while (true) {
     *             final int fragments = snapshotImage.poll(handler, 10);
     *             if (fragments == 0) {
     *                 if (snapshotImage.isClosed() || snapshotImage.isEndOfStream()) {
     *                     break;
     *                 }
     *                 cluster.idleStrategy().idle();  // 避免 busy-spin
     *             }
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param cluster       集群对象，用于访问 Cluster API（获取时间、IdleStrategy 等）
     * @param snapshotImage 快照镜像，用于加载已保存的状态；首次启动时为 null
     */
    void onStart(Cluster cluster, Image snapshotImage);

    /**
     * 客户端会话打开事件：当客户端成功连接到集群时调用。
     * <p>
     * 此方法在客户端调用 {@link io.aeron.cluster.client.AeronCluster#connect()} 并通过认证后触发。
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>记录会话到内部映射表（session ID → user info）</li>
     *   <li>初始化会话相关状态（如购物车、订单簿订阅等）</li>
     *   <li>发送欢迎消息或初始数据</li>
     * </ul>
     * <p>
     * <b>注意事项</b>：
     * <ul>
     *   <li>所有节点都会收到此事件（保证状态一致性）</li>
     *   <li>可以在此方法中发送消息或调度定时器</li>
     *   <li>使用 {@code cluster.time()} 获取确定性时间戳</li>
     * </ul>
     *
     * @param session   已打开的客户端会话对象，包含 session ID 等信息
     * @param timestamp 会话打开时的集群时间戳（由 Cluster.time() 提供，确保所有节点一致）
     */
    void onSessionOpen(ClientSession session, long timestamp);

    /**
     * 客户端会话关闭事件：当客户端断开连接或会话超时时调用。
     * <p>
     * <b>触发条件</b>：
     * <ul>
     *   <li><b>CLIENT_ACTION</b>：客户端主动关闭连接</li>
     *   <li><b>SERVICE_ACTION</b>：服务端调用 {@link Cluster#closeClientSession(long)} 主动关闭</li>
     *   <li><b>TIMEOUT</b>：会话心跳超时（sessionTimeoutNs）</li>
     * </ul>
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>清理会话相关状态（从映射表移除、释放资源）</li>
     *   <li>记录审计日志</li>
     *   <li>处理未完成的业务逻辑（如取消订单）</li>
     * </ul>
     *
     * @param session     已关闭的会话对象
     * @param timestamp   会话关闭时的集群时间戳
     * @param closeReason 关闭原因（CLIENT_ACTION、SERVICE_ACTION 或 TIMEOUT）
     */
    void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason);

    /**
     * 客户端消息接收事件：这是处理业务逻辑的核心方法。
     * <p>
     * 当客户端调用 {@link io.aeron.cluster.client.AeronCluster#offer(DirectBuffer, int, int)} 发送消息后，
     * 消息经过 Leader 序列化、多数派提交后，所有节点会按相同顺序调用此方法。
     * <p>
     * <b>确定性保证</b>：
     * <ul>
     *   <li>所有节点以相同顺序处理相同的消息</li>
     *   <li>timestamp 在所有节点完全一致</li>
     *   <li>只能使用 cluster.time() 获取时间，禁止使用 System.currentTimeMillis()</li>
     * </ul>
     * <p>
     * <b>典型处理流程</b>：
     * <pre>{@code
     * public void onSessionMessage(ClientSession session, long timestamp,
     *                               DirectBuffer buffer, int offset, int length, Header header) {
     *     // 1. 解码消息类型
     *     final int msgType = buffer.getInt(offset);
     *
     *     // 2. 根据类型处理业务逻辑
     *     switch (msgType) {
     *         case NEW_ORDER:
     *             handleNewOrder(session, buffer, offset + 4, length - 4, timestamp);
     *             break;
     *         case CANCEL_ORDER:
     *             handleCancelOrder(session, buffer, offset + 4, length - 4, timestamp);
     *             break;
     *     }
     *
     *     // 3. 发送响应（可选）
     *     responseBuffer.putLong(0, timestamp);
     *     responseBuffer.putInt(8, ORDER_CONFIRMED);
     *     cluster.offer(session.id(), responseBuffer, 0, 12);
     * }
     * }</pre>
     * <p>
     * <b>性能优化</b>：
     * <ul>
     *   <li>避免在此方法中创建对象（预分配 buffer）</li>
     *   <li>使用 {@link org.agrona.concurrent.UnsafeBuffer} 零拷贝访问消息</li>
     *   <li>批量处理后再发送响应</li>
     * </ul>
     *
     * @param session   发送消息的客户端会话；如果是 Service 间通信则可能为 null
     * @param timestamp 消息被 Leader 序列化时的集群时间戳
     * @param buffer    包含消息内容的缓冲区
     * @param offset    消息在缓冲区中的起始位置
     * @param length    消息的长度（字节）
     * @param header    Aeron 消息头，包含 position、session ID 等元数据
     */
    void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * 定时器到期事件：当通过 {@link Cluster#scheduleTimer(long, long)} 调度的定时器到期时调用。
     * <p>
     * 定时器是集群范围的确定性定时器，所有节点会在相同的日志位置触发相同的定时器事件。
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>实现超时逻辑（订单过期、会话超时等）</li>
     *   <li>周期性任务（定时快照、定时清理等）</li>
     *   <li>延迟执行（异步回调）</li>
     * </ul>
     * <p>
     * <b>定时器调度示例</b>：
     * <pre>{@code
     * // 在 onSessionMessage 中调度 30 秒后的定时器
     * long orderId = 12345;
     * long deadlineMs = cluster.time() + TimeUnit.SECONDS.toMillis(30);
     * cluster.scheduleTimer(orderId, deadlineMs);
     *
     * // 30 秒后，onTimerEvent 会被调用
     * public void onTimerEvent(long correlationId, long timestamp) {
     *     long orderId = correlationId;
     *     if (orders.containsKey(orderId) && !orders.get(orderId).isProcessed()) {
     *         // 订单超时，执行取消逻辑
     *         cancelOrder(orderId, timestamp);
     *     }
     * }
     * }</pre>
     * <p>
     * <b>注意事项</b>：
     * <ul>
     *   <li>correlationId 由调用方指定，用于关联业务逻辑</li>
     *   <li>定时器精度取决于集群的 poll 频率，通常为毫秒级</li>
     *   <li>取消定时器使用 {@link Cluster#cancelTimer(long)}</li>
     * </ul>
     *
     * @param correlationId 调度定时器时指定的关联 ID（通常是订单 ID、会话 ID 等）
     * @param timestamp     定时器到期时的集群时间戳
     */
    void onTimerEvent(long correlationId, long timestamp);

    /**
     * 保存快照事件：服务应将当前状态序列化到提供的快照 Publication。
     * <p>
     * 此方法在以下情况被调用：
     * <ul>
     *   <li>达到配置的快照间隔（snapshotIntervalLength）</li>
     *   <li>管理员手动触发快照（AdminRequest.SNAPSHOT）</li>
     *   <li>节点关闭前保存最终状态</li>
     * </ul>
     * <p>
     * <b>快照保存流程</b>：
     * <pre>{@code
     * public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
     *     final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
     *
     *     // 序列化所有业务状态
     *     for (Order order : orders.values()) {
     *         orderEncoder.wrap(buffer, 0)
     *             .orderId(order.id)
     *             .quantity(order.quantity)
     *             .price(order.price);
     *
     *         // 发布到快照流（处理背压）
     *         while (snapshotPublication.offer(buffer, 0, orderEncoder.encodedLength()) < 0) {
     *             cluster.idleStrategy().idle();
     *         }
     *     }
     * }
     * }</pre>
     * <p>
     * <b>重要约束</b>：
     * <ul>
     *   <li><b>禁止发送消息</b>：不能调用 {@link Cluster#offer} 或 {@link Cluster#scheduleTimer}</li>
     *   <li><b>必须处理背压</b>：snapshotPublication.offer() 返回负值时需要重试</li>
     *   <li><b>可能耗时较长</b>：大型状态应周期性调用 {@link Cluster#idleStrategy()#idle()}</li>
     *   <li><b>确保完整性</b>：快照必须包含完整的业务状态，恢复时才能正确重建</li>
     * </ul>
     * <p>
     * <b>性能优化</b>：
     * <ul>
     *   <li>使用 SBE 或其他高效序列化格式</li>
     *   <li>预分配缓冲区，避免重复分配</li>
     *   <li>批量写入，减少 offer 调用次数</li>
     * </ul>
     *
     * @param snapshotPublication 快照 Publication，用于写入序列化后的状态数据
     */
    void onTakeSnapshot(ExclusivePublication snapshotPublication);

    /**
     * 角色变更事件：当节点在 Follower/Candidate/Leader 之间转换时调用。
     * <p>
     * <b>角色说明</b>：
     * <ul>
     *   <li><b>FOLLOWER</b>：跟随者，复制 Leader 的日志</li>
     *   <li><b>CANDIDATE</b>：候选人，正在参与选举</li>
     *   <li><b>LEADER</b>：领导者，处理客户端请求并序列化日志</li>
     * </ul>
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>记录审计日志</li>
     *   <li>更新监控指标</li>
     *   <li>调整外部系统连接（如只在 Leader 上连接外部数据库）</li>
     * </ul>
     * <p>
     * <b>重要约束</b>：
     * <ul>
     *   <li><b>禁止发送消息</b>：不能调用 {@link Cluster#offer} 或 {@link Cluster#scheduleTimer}</li>
     *   <li><b>禁止修改业务状态</b>：角色变更不应影响确定性状态</li>
     * </ul>
     * <p>
     * <b>示例</b>：
     * <pre>{@code
     * public void onRoleChange(Cluster.Role newRole) {
     *     logger.info("Node role changed to: {}", newRole);
     *
     *     if (newRole == Cluster.Role.LEADER) {
     *         // 成为 Leader，可能需要初始化某些资源
     *         externalConnectionManager.enableWrites();
     *     } else {
     *         // 降级为 Follower/Candidate
     *         externalConnectionManager.disableWrites();
     *     }
     * }
     * }</pre>
     *
     * @param newRole 节点新的角色（FOLLOWER、CANDIDATE 或 LEADER）
     */
    void onRoleChange(Cluster.Role newRole);

    /**
     * 终止事件：在服务容器关闭前调用，用于清理资源。
     * <p>
     * 此方法仅在服务成功启动（{@link #onStart} 完成）后才会被调用。
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>关闭外部连接（数据库、消息队列等）</li>
     *   <li>释放资源（线程池、内存缓冲区等）</li>
     *   <li>记录最终日志</li>
     * </ul>
     * <p>
     * <b>重要约束</b>：
     * <ul>
     *   <li><b>禁止发送消息</b>：不能调用 {@link Cluster#offer} 或 {@link Cluster#scheduleTimer}</li>
     *   <li><b>应快速完成</b>：避免长时间阻塞关闭流程</li>
     * </ul>
     *
     * @param cluster 集群对象，可用于获取配置信息
     */
    void onTerminate(Cluster cluster);

    /**
     * 新领导任期事件：当选举成功且新 Leader 进入新的 leadership term 时调用。
     * <p>
     * 此事件在以下情况触发：
     * <ul>
     *   <li>首次启动后选出 Leader</li>
     *   <li>Leader 故障后重新选举</li>
     *   <li>Leader 主动退位（如滚动升级）</li>
     * </ul>
     * <p>
     * <b>使用场景</b>：
     * <ul>
     *   <li>记录审计日志（记录 Leader 变更）</li>
     *   <li>更新监控指标</li>
     *   <li>检测 term ID 变化，处理跨 term 的业务逻辑</li>
     * </ul>
     * <p>
     * <b>注意事项</b>：
     * <ul>
     *   <li>此事件在所有节点上触发，时间戳和参数完全一致</li>
     *   <li>可选实现（默认为空方法）</li>
     *   <li>禁止修改业务状态或发送消息</li>
     * </ul>
     *
     * @param leadershipTermId    新的 leadership term ID（每次选举后递增）
     * @param logPosition         此消息到达时的日志位置
     * @param timestamp           新任期的起始时间戳
     * @param termBaseLogPosition 当前 term 的基准日志位置
     * @param leaderMemberId      新 Leader 的成员 ID
     * @param logSessionId        日志 publication 的 session ID
     * @param timeUnit            后续时间戳的时间单位（MILLIS/MICROS/NANOS）
     * @param appVersion          ConsensusModule 配置的应用版本号
     */
    default void onNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition,
        final int leaderMemberId,
        final int logSessionId,
        final TimeUnit timeUnit,
        final int appVersion)
    {
    }

    /**
     * 执行后台任务（非确定性）：用于不影响状态机模型的辅助操作。
     * <p>
     * <b>典型用途</b>：
     * <ul>
     *   <li>保持外部连接活跃（数据库心跳、消息队列 keepalive）</li>
     *   <li>更新监控指标（延迟统计、吞吐量计数）</li>
     *   <li>清理过期缓存</li>
     * </ul>
     * <p>
     * <b>严格禁止</b>：
     * <ul>
     *   <li><b>修改业务状态</b>：任何会影响确定性的操作</li>
     *   <li><b>发送消息</b>：{@link Cluster#offer} 或 {@link Cluster#scheduleTimer}</li>
     *   <li><b>长时间运行</b>：执行时间应为常量级（< 1ms），避免影响延迟</li>
     * </ul>
     * <p>
     * <b>时间戳说明</b>：
     * <ul>
     *   <li>nowNs 是非确定性的系统时间（类似 {@link System#nanoTime()}）</li>
     *   <li><b>不是</b> {@link Cluster#time()}，不能用于业务逻辑</li>
     *   <li>可用于测量本地耗时、限流等</li>
     * </ul>
     * <p>
     * <b>示例</b>：
     * <pre>{@code
     * private long lastHeartbeatNs = 0;
     *
     * public int doBackgroundWork(long nowNs) {
     *     // 每 5 秒发送一次心跳
     *     if (nowNs - lastHeartbeatNs > TimeUnit.SECONDS.toNanos(5)) {
     *         externalConnectionManager.sendHeartbeat();
     *         lastHeartbeatNs = nowNs;
     *         return 1;
     *     }
     *     return 0;
     * }
     * }</pre>
     *
     * @param nowNs 当前系统纳秒时间戳（用于测量耗时，非确定性）
     * @return 0 表示无工作；正数表示执行了工作（用于 idle strategy）
     * @since 1.40.0
     */
    default int doBackgroundWork(final long nowNs)
    {
        return 0;
    }
}

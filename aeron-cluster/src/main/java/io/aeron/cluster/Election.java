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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.Cluster;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.AgentTerminationException;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.CommonContext.UDP_MEDIA;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.cluster.ClusterMember.compareLog;
import static io.aeron.cluster.ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE;
import static io.aeron.cluster.ElectionState.CANDIDATE_BALLOT;
import static io.aeron.cluster.ElectionState.CANVASS;
import static io.aeron.cluster.ElectionState.CLOSED;
import static io.aeron.cluster.ElectionState.FOLLOWER_BALLOT;
import static io.aeron.cluster.ElectionState.FOLLOWER_CATCHUP;
import static io.aeron.cluster.ElectionState.FOLLOWER_CATCHUP_AWAIT;
import static io.aeron.cluster.ElectionState.FOLLOWER_CATCHUP_INIT;
import static io.aeron.cluster.ElectionState.FOLLOWER_LOG_AWAIT;
import static io.aeron.cluster.ElectionState.FOLLOWER_LOG_INIT;
import static io.aeron.cluster.ElectionState.FOLLOWER_LOG_REPLICATION;
import static io.aeron.cluster.ElectionState.FOLLOWER_READY;
import static io.aeron.cluster.ElectionState.FOLLOWER_REPLAY;
import static io.aeron.cluster.ElectionState.INIT;
import static io.aeron.cluster.ElectionState.LEADER_INIT;
import static io.aeron.cluster.ElectionState.LEADER_LOG_REPLICATION;
import static io.aeron.cluster.ElectionState.LEADER_READY;
import static io.aeron.cluster.ElectionState.LEADER_REPLAY;
import static io.aeron.cluster.ElectionState.NOMINATE;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * 集群选举过程实现类：负责确定新的 Leader 并协调 Follower 追赶日志。
 * <p>
 * 这是 Aeron Cluster 实现 Raft 共识算法的核心组件，处理以下职责：
 * <ul>
 *   <li><b>Leader 选举</b>：通过 Canvass → Nominate → Ballot 流程选出新 Leader</li>
 *   <li><b>日志复制</b>：Leader 发布日志，Follower 订阅并复制</li>
 *   <li><b>状态追赶</b>：Follower 从 Leader 同步落后的日志</li>
 *   <li><b>角色转换</b>：管理节点在 Follower/Candidate/Leader 之间的状态转换</li>
 * </ul>
 * <p>
 * <b>选举状态机</b>：
 * <pre>
 *     INIT → CANVASS → NOMINATE → CANDIDATE_BALLOT → LEADER_READY
 *                                ↘ FOLLOWER_BALLOT → FOLLOWER_READY
 * </pre>
 * <p>
 * <b>触发条件</b>：
 * <ul>
 *   <li>节点启动时无 Leader</li>
 *   <li>Leader 心跳超时 (leaderHeartbeatTimeoutNs)</li>
 *   <li>集群成员变更</li>
 * </ul>
 *
 * @see ElectionState
 * @see ConsensusModuleAgent
 */
class Election
{
    /** 集群所有成员数组（包括本节点） */
    private final ClusterMember[] clusterMembers;

    /** 当前节点的 ClusterMember 对象 */
    private final ClusterMember thisMember;

    /** 成员 ID 到 ClusterMember 的映射，用于快速查找 */
    private final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap;

    /** 共识消息发布器，用于发送选举相关消息（RequestVote、Vote 等） */
    private final ConsensusPublisher consensusPublisher;

    /** ConsensusModule 配置上下文 */
    private final ConsensusModule.Context ctx;

    /** ConsensusModule 代理，用于回调状态变更 */
    private final ConsensusModuleAgent consensusModuleAgent;

    /** 选举开始时的 leadership term ID（用于检测是否有新的 term） */
    private final long initialLogLeadershipTermId;

    /** 选举开始时的最后更新时间（纳秒） */
    private final long initialTimeOfLastUpdateNs;

    /** 选举开始时的 term 基准日志位置 */
    private final long initialTermBaseLogPosition;

    /** 是否是节点启动触发的选举（true=启动选举，false=运行时选举） */
    private final boolean isNodeStartup;

    /** 上次状态变更时间（纳秒），用于超时检测 */
    private long timeOfLastStateChangeNs;

    /** 上次收到更新的时间（纳秒），用于心跳检测 */
    private long timeOfLastUpdateNs;

    /** 上次提交位置更新的时间（纳秒） */
    private long timeOfLastCommitPositionUpdateNs;

    /** 提名阶段的截止时间（纳秒） */
    private long nominationDeadlineNs;

    /** 当前日志位置（已持久化的最高位置） */
    private long logPosition;

    /** 追加位置（Leader 已写入但可能未提交的位置） */
    private long appendPosition;

    /** 已通知给 Service 的提交位置 */
    private long notifiedCommitPosition;

    /** 追赶时的加入位置（Follower 从此位置开始追赶） */
    private long catchupJoinPosition = NULL_POSITION;

    /** 复制时的 leadership term ID */
    private long replicationLeadershipTermId = NULL_VALUE;

    /** 复制停止位置（复制到此位置后停止） */
    private long replicationStopPosition = NULL_POSITION;

    /** 复制截止时间（纳秒） */
    private long replicationDeadlineNs;

    /** 复制时的 term 基准日志位置 */
    private long replicationTermBaseLogPosition;

    /** Leader 的 recording ID（用于从 Archive 回放） */
    private long leaderRecordingId = NULL_VALUE;

    /** 当前 leadership term ID */
    private long leadershipTermId;

    /** 日志的 leadership term ID */
    private long logLeadershipTermId;

    /** 候选人的 term ID（自我提名时递增） */
    private long candidateTermId;

    /** 上次发布的提交位置（避免重复发送） */
    private long lastPublishedCommitPosition;

    /** 上次发布的追加位置（避免重复发送） */
    private long lastPublishedAppendPosition;

    /** 日志 session ID（用于订阅 Leader 的日志流） */
    private int logSessionId = NULL_SESSION_ID;

    /** 优雅关闭的 Leader ID（如果 Leader 主动关闭，记录其 ID） */
    private int gracefulClosedLeaderId;

    /** 是否是首次初始化（用于某些一次性逻辑） */
    private boolean isFirstInit = true;

    /** 是否是 Leader 启动（新当选的 Leader） */
    private boolean isLeaderStartup;

    /** 是否使用扩展拉票阶段（节点启动时为 true，给予更多时间） */
    private boolean isExtendedCanvass;

    /** Leader 成员对象（null 表示当前无 Leader） */
    private ClusterMember leaderMember = null;

    /** 当前选举状态 */
    private ElectionState state = INIT;

    /** 日志订阅（Follower 用于订阅 Leader 的日志流） */
    private Subscription logSubscription = null;

    /** 日志回放器（从 Archive 回放历史日志） */
    private LogReplay logReplay = null;

    /** 日志复制器（从 Leader 复制日志到本地 Archive） */
    private RecordingReplication logReplication = null;

    /**
     * 构造 Election 实例，初始化选举过程。
     * <p>
     * 此构造函数在 {@link ConsensusModuleAgent} 需要启动选举时调用，初始化所有必要的状态。
     *
     * @param isNodeStartup           是否是节点启动触发的选举（true=首次启动，false=运行时 Leader 失效）
     * @param gracefulClosedLeaderId  如果前任 Leader 优雅关闭，此为其 ID；否则为 NULL_VALUE
     * @param leadershipTermId        当前 leadership term ID
     * @param termBaseLogPosition     当前 term 的基准日志位置
     * @param logPosition             当前已持久化的日志位置
     * @param appendPosition          当前已追加的日志位置（可能未提交）
     * @param clusterMembers          集群所有成员数组
     * @param clusterMemberByIdMap    成员 ID 到 ClusterMember 的映射
     * @param thisMember              当前节点的 ClusterMember 对象
     * @param consensusPublisher      共识消息发布器
     * @param ctx                     ConsensusModule 配置上下文
     * @param consensusModuleAgent    ConsensusModule 代理
     */
    Election(
        final boolean isNodeStartup,
        final int gracefulClosedLeaderId,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long appendPosition,
        final ClusterMember[] clusterMembers,
        final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap,
        final ClusterMember thisMember,
        final ConsensusPublisher consensusPublisher,
        final ConsensusModule.Context ctx,
        final ConsensusModuleAgent consensusModuleAgent)
    {
        // 初始化选举类型（节点启动 vs 运行时选举）
        this.isNodeStartup = isNodeStartup;
        this.isExtendedCanvass = isNodeStartup;  // 节点启动时使用更长的拉票时间
        this.gracefulClosedLeaderId = gracefulClosedLeaderId;

        // 初始化日志位置信息
        this.logPosition = logPosition;
        this.appendPosition = appendPosition;
        this.logLeadershipTermId = leadershipTermId;
        this.initialLogLeadershipTermId = leadershipTermId;
        this.initialTermBaseLogPosition = termBaseLogPosition;

        // 初始化 term ID（候选人 term 从当前 term 开始）
        this.leadershipTermId = leadershipTermId;
        this.candidateTermId = leadershipTermId;

        // 初始化集群成员信息
        this.clusterMembers = clusterMembers;
        this.clusterMemberByIdMap = clusterMemberByIdMap;
        this.thisMember = thisMember;

        // 初始化通信与配置
        this.consensusPublisher = consensusPublisher;
        this.ctx = ctx;
        this.consensusModuleAgent = consensusModuleAgent;

        // 初始化时间戳（设置为 1 天前，确保首次检查会触发）
        final long nowNs = nowNs(ctx);
        this.initialTimeOfLastUpdateNs = nowNs - TimeUnit.DAYS.toNanos(1);
        this.timeOfLastUpdateNs = initialTimeOfLastUpdateNs;
        this.timeOfLastCommitPositionUpdateNs = initialTimeOfLastUpdateNs;

        // 验证必需参数并更新计数器
        Objects.requireNonNull(thisMember);
        ctx.electionStateCounter().setRelease(INIT.code());  // 设置初始状态为 INIT
        ctx.electionCounter().incrementRelease();             // 递增选举计数（用于监控）
    }

    /**
     * 获取当前的 Leader 成员。
     *
     * @return Leader 的 {@link ClusterMember} 对象；如果当前无 Leader 则返回 null
     */
    ClusterMember leader()
    {
        return leaderMember;
    }

    /**
     * 获取日志流的 session ID。
     * <p>
     * Follower 使用此 session ID 订阅 Leader 的日志 publication。
     *
     * @return 日志流的 session ID
     */
    int logSessionId()
    {
        return logSessionId;
    }

    /**
     * 获取当前的 leadership term ID。
     * <p>
     * Leadership term ID 在每次成功选举后递增，用于标识不同的领导任期。
     *
     * @return 当前 leadership term ID
     */
    long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * 获取当前的日志位置。
     * <p>
     * 表示本节点已持久化到 Archive 的最高日志位置。
     *
     * @return 当前日志位置（字节偏移）
     */
    long logPosition()
    {
        return logPosition;
    }

    /**
     * 判断是否是 Leader 启动状态。
     * <p>
     * 当节点刚当选为 Leader 时此标志为 true，用于触发 Leader 初始化逻辑。
     *
     * @return true 如果是 Leader 启动阶段
     */
    boolean isLeaderStartup()
    {
        return isLeaderStartup;
    }

    /**
     * 获取当前节点的成员 ID。
     *
     * @return 当前节点的成员 ID
     */
    int thisMemberId()
    {
        return thisMember.id();
    }

    /**
     * 执行选举状态机的一次工作循环。
     * <p>
     * 这是选举过程的核心驱动方法，根据当前状态 ({@link ElectionState}) 调用相应的处理方法。
     * 每次调用处理当前状态的逻辑，可能会触发状态转换。
     * <p>
     * <b>状态处理流程</b>：
     * <ul>
     *   <li><b>INIT</b>：初始化选举，进入 CANVASS 状态</li>
     *   <li><b>CANVASS</b>：向所有成员发送 CanvassPosition，收集日志位置信息</li>
     *   <li><b>NOMINATE</b>：根据日志完整性决定自我提名或等待他人提名</li>
     *   <li><b>CANDIDATE_BALLOT</b>：发送 RequestVote，等待多数派投票</li>
     *   <li><b>FOLLOWER_BALLOT</b>：处理来自候选人的投票请求，投票给合适的候选人</li>
     *   <li><b>LEADER_*</b>：Leader 各阶段（初始化、日志复制、回放、就绪）</li>
     *   <li><b>FOLLOWER_*</b>：Follower 各阶段（追赶、日志复制、回放、就绪）</li>
     * </ul>
     * <p>
     * 此方法被 {@link ConsensusModuleAgent#doWork()} 周期性调用。
     *
     * @param nowNs 当前时间（纳秒），用于超时检测
     * @return 本次循环执行的工作量（用于 idle strategy 判断）
     */
    /**
     * Election 的主工作循环：根据当前选举状态执行对应的逻辑。
     * <p>
     * 这是一个<b>状态机</b>，每次调用都会根据 {@code state} 执行对应的处理函数。
     * 状态机保证了选举流程的有序推进：
     * <ul>
     *   <li><b>选举路径</b>：INIT → CANVASS → NOMINATE → BALLOT → READY</li>
     *   <li><b>Leader 路径</b>：CANDIDATE_BALLOT → LEADER_LOG_REPLICATION → LEADER_REPLAY → LEADER_INIT → LEADER_READY</li>
     *   <li><b>Follower 路径</b>：FOLLOWER_BALLOT → FOLLOWER_LOG_REPLICATION → FOLLOWER_REPLAY → (追赶) → FOLLOWER_READY</li>
     * </ul>
     * <p>
     * <b>调用频率</b>：每次 ConsensusModuleAgent.doWork() 都会调用（约 1000 次/秒）
     * <p>
     * <b>工作量统计</b>：
     * <ul>
     *   <li>workCount == 0: 无事可做（idle），ConsensusModuleAgent 会执行 idle 策略</li>
     *   <li>workCount > 0: 有工作完成（发送消息、接收响应、状态转换等），保持高效率</li>
     * </ul>
     * <p>
     * <b>状态分类</b>（18 个状态）：
     * <ul>
     *   <li><b>1. 选举阶段</b>（5 个状态）：INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT, FOLLOWER_BALLOT</li>
     *   <li><b>2. Leader 阶段</b>（4 个状态）：LEADER_LOG_REPLICATION, LEADER_REPLAY, LEADER_INIT, LEADER_READY</li>
     *   <li><b>3. Follower 阶段</b>（8 个状态）：FOLLOWER_LOG_REPLICATION, FOLLOWER_REPLAY, FOLLOWER_CATCHUP_INIT,
     *       FOLLOWER_CATCHUP_AWAIT, FOLLOWER_CATCHUP, FOLLOWER_LOG_INIT, FOLLOWER_LOG_AWAIT, FOLLOWER_READY</li>
     *   <li><b>4. 终止阶段</b>（1 个状态）：CLOSED</li>
     * </ul>
     *
     * @param nowNs 当前集群时间（纳秒），用于超时判断和时间戳生成
     * @return 本次执行的工作量（0 表示无事可做，>0 表示有工作完成）
     * @see ConsensusModuleAgent#doWork() 主循环中调用此方法
     * @see ElectionState 所有选举状态的定义
     */
    int doWork(final long nowNs)
    {
        // ==================== 工作量计数器 ====================
        // workCount 用于记录本次执行的工作量：
        // - 0: 无事可做（idle）
        // - >0: 有工作完成（发送消息、接收响应、状态转换等）
        // ConsensusModuleAgent 根据 workCount 决定是否执行 idle 策略
        int workCount = 0;

        // ==================== 状态机核心：根据当前状态执行对应逻辑 ====================
        // 选举状态机有 18 个状态，分为 4 大类：
        // 1. 选举阶段：INIT, CANVASS, NOMINATE, CANDIDATE_BALLOT, FOLLOWER_BALLOT
        // 2. Leader 阶段：LEADER_LOG_REPLICATION, LEADER_REPLAY, LEADER_INIT, LEADER_READY
        // 3. Follower 阶段：FOLLOWER_LOG_REPLICATION, FOLLOWER_REPLAY, FOLLOWER_CATCHUP_*, FOLLOWER_LOG_*, FOLLOWER_READY
        // 4. 终止阶段：CLOSED

        switch (state)
        {
            // ==================== 阶段 1: 选举初始化 ====================
            case INIT:
                // 选举初始化：重置状态、设置超时、准备选举
                // 职责：
                // - 重置所有成员的投票状态
                // - 设置选举超时时间
                // - 生成新的 candidateTermId
                // - 立即转换到 CANVASS 状态
                workCount += init(nowNs);
                break;

            // ==================== 阶段 2: 询问阶段（Canvass） ====================
            case CANVASS:
                // 询问阶段：向所有节点发送 CanvassPosition 请求，收集日志位置信息
                // 职责：
                // - 向所有成员发送 CanvassPosition 消息（询问日志位置）
                // - 收集响应，判断自己的日志是否是最新的
                // - 超时后转换到 NOMINATE 状态
                //
                // 为什么需要 Canvass？
                // - 避免多次投票失败（预先知道谁的日志最新）
                // - 减少网络开销（不需要多轮投票）
                workCount += canvass(nowNs);
                break;

            // ==================== 阶段 3: 提名阶段（Nominate） ====================
            case NOMINATE:
                // 提名阶段：决定是否参选
                // 职责：
                // - 比较所有节点的日志位置（使用 compareLog()）
                // - 如果自己的日志最新，转换到 CANDIDATE_BALLOT（参选）
                // - 否则转换到 FOLLOWER_BALLOT（跟随）
                //
                // 决策依据：compareLog(logLeadershipTermId, logPosition)
                // 1. 先比较 leadershipTermId（term ID 越大越新）
                // 2. 再比较 logPosition（日志位置越大越新）
                workCount += nominate(nowNs);
                break;

            // ==================== 阶段 4a: 候选人投票阶段 ====================
            case CANDIDATE_BALLOT:
                // 候选人投票阶段：作为候选人，向所有节点发送投票请求
                // 职责：
                // - 向所有成员发送 RequestVote 消息（请求投票）
                // - 收集投票响应（Vote 消息）
                // - 如果获得多数票（quorum），转换到 LEADER_LOG_REPLICATION
                // - 如果超时未获得多数票，重新开始选举（回到 CANVASS）
                //
                // Quorum 计算：quorum = (clusterMembers.length / 2) + 1
                // 示例：3 节点集群，quorum = 2（需要 2 票）
                workCount += candidateBallot(nowNs);
                break;

            // ==================== 阶段 4b: 跟随者投票阶段 ====================
            case FOLLOWER_BALLOT:
                // 跟随者投票阶段：作为跟随者，等待投票结果
                // 职责：
                // - 等待接收 NewLeadershipTerm 消息（新 Leader 通知）
                // - 收到后转换到 FOLLOWER_LOG_REPLICATION
                // - 超时后重新开始选举（回到 CANVASS）
                //
                // 注意：在 Nominate 阶段已经决定不参选
                workCount += followerBallot(nowNs);
                break;

            // ==================== 阶段 5a: Leader 日志复制准备 ====================
            case LEADER_LOG_REPLICATION:
                // Leader 日志复制阶段：启动日志 publication，等待 Followers 订阅
                // 职责：
                // - 创建日志 publication（供 Followers 订阅）
                //   * channel: logChannel (UDP multicast)
                //   * streamId: logStreamId
                // - 开始 Archive 录制日志
                // - 等待 Followers 连接并开始复制
                // - 转换到 LEADER_REPLAY
                //
                // 关键操作：aeron.addExclusivePublication(logChannel, logStreamId)
                workCount += leaderLogReplication(nowNs);
                break;

            // ==================== 阶段 6a: Leader 日志重放 ====================
            case LEADER_REPLAY:
                // Leader 日志重放阶段：重放未提交的日志
                // 职责：
                // - 从上次提交位置（commitPosition）重放到当前位置（logPosition）
                // - 确保 Leader 状态与日志一致
                // - 调用 ClusteredService.onSessionMessage() 恢复业务状态
                // - 转换到 LEADER_INIT
                //
                // 为什么需要重放？
                // - Leader 可能在上次 term 中崩溃，导致部分日志未提交
                // - 重放确保 Leader 的业务状态与日志一致
                workCount += leaderReplay(nowNs);
                break;

            // ==================== 阶段 7a: Leader 初始化 ====================
            case LEADER_INIT:
                // Leader 初始化阶段：准备接受客户端请求
                // 职责：
                // - 创建 NewLeadershipTerm 事件，写入日志
                //   * leadershipTermId（新的 term ID）
                //   * leaderMemberId（本节点 ID）
                //   * logPosition（当前日志位置）
                // - 通知所有 Followers 新的 leadership term
                // - 转换到 LEADER_READY
                //
                // NewLeadershipTerm 的作用：
                // - 标记新的 leadership term 开始
                // - Followers 消费到此事件后知道新 Leader 已就绪
                workCount += leaderInit(nowNs);
                break;

            // ==================== 阶段 8a: Leader 就绪 ====================
            case LEADER_READY:
                // Leader 就绪阶段：等待所有 Followers 追赶到 leadershipTermId
                // 职责：
                // - 等待 quorum 的 Followers 追赶到 NewLeadershipTerm 位置
                // - 持续发送心跳消息（CommitPosition）
                // - 所有 Followers 就绪后，选举结束
                // - 返回到 ConsensusModuleAgent，切换到 ACTIVE 状态
                //
                // 结束条件：readyFollowers >= quorumThreshold
                workCount += leaderReady(nowNs);
                break;

            // ==================== 阶段 5b: Follower 日志复制准备 ====================
            case FOLLOWER_LOG_REPLICATION:
                // Follower 日志复制阶段：订阅 Leader 的日志
                // 职责：
                // - 订阅 Leader 的日志 publication
                //   * channel: Leader 的 logChannel
                //   * streamId: logStreamId
                // - 等待订阅成功
                // - 转换到 FOLLOWER_REPLAY
                //
                // 关键操作：aeron.addSubscription(logChannel, logStreamId)
                workCount += followerLogReplication(nowNs);
                break;

            // ==================== 阶段 6b: Follower 日志重放 ====================
            case FOLLOWER_REPLAY:
                // Follower 日志重放阶段：重放未提交的日志
                // 职责：
                // - 从上次提交位置重放到 Leader 通知的位置
                // - 确保 Follower 状态与日志一致
                // - 根据日志差距决定下一步：
                //   * 日志一致 → FOLLOWER_LOG_INIT（直接进入日志初始化）
                //   * 日志落后 → FOLLOWER_CATCHUP_INIT（需要从 Leader 追赶日志）
                //
                // 日志差距判断：logPosition < leaderLogPosition
                workCount += followerReplay(nowNs);
                break;

            // ==================== 阶段 7b: Follower 追赶初始化 ====================
            case FOLLOWER_CATCHUP_INIT:
                // Follower 追赶初始化阶段：准备从 Leader 同步落后的日志
                // 职责：
                // - 向 Leader 发送 CatchupRequest 请求
                //   * fromPosition: 需要追赶的起始位置
                // - 等待 Leader 创建 catchup publication
                // - 转换到 FOLLOWER_CATCHUP_AWAIT
                //
                // 为什么需要追赶？
                // - Follower 的日志落后于 Leader（如节点重启、网络延迟）
                // - 直接从 Archive 同步落后的日志，比逐条重放更快
                workCount += followerCatchupInit(nowNs);
                break;

            // ==================== 阶段 8b: Follower 等待追赶 ====================
            case FOLLOWER_CATCHUP_AWAIT:
                // Follower 等待追赶阶段：等待 Leader 提供的 catchup publication 就绪
                // 职责：
                // - 等待 Leader 返回 CatchupPosition 消息（包含 recordingId）
                // - 订阅 catchup publication
                // - 转换到 FOLLOWER_CATCHUP
                //
                // CatchupPosition 包含：
                // - recordingId: Leader 为此追赶创建的 Archive 录制 ID
                // - catchupEndpoint: catchup publication 的端点地址
                workCount += followerCatchupAwait(nowNs);
                break;

            // ==================== 阶段 9b: Follower 追赶中 ====================
            case FOLLOWER_CATCHUP:
                // Follower 追赶阶段：从 Leader 同步落后的日志
                // 职责：
                // - 从 catchup publication 接收日志
                // - 写入本地 Archive
                // - 重放追赶的日志，更新业务状态
                // - 追赶完成后转换到 FOLLOWER_LOG_INIT
                //
                // 追赶完成条件：catchupPosition >= targetPosition
                workCount += followerCatchup(nowNs);
                break;

            // ==================== 阶段 10b: Follower 日志初始化 ====================
            case FOLLOWER_LOG_INIT:
                // Follower 日志初始化阶段：准备接收 Leader 的实时日志
                // 职责：
                // - 从 Archive 加载 catchup 的日志（如果有）
                // - 准备订阅 Leader 的实时日志
                // - 转换到 FOLLOWER_LOG_AWAIT
                //
                // 此阶段确保 Follower 已追赶到 Leader 的最新位置
                workCount += followerLogInit(nowNs);
                break;

            // ==================== 阶段 11b: Follower 等待日志 ====================
            case FOLLOWER_LOG_AWAIT:
                // Follower 等待日志阶段：等待日志订阅就绪
                // 职责：
                // - 等待日志订阅连接到 Leader
                // - 等待 Image 可用（订阅建立完成）
                // - 转换到 FOLLOWER_READY
                //
                // Image: Aeron 中表示一个订阅连接
                workCount += followerLogAwait(nowNs);
                break;

            // ==================== 阶段 12b: Follower 就绪 ====================
            case FOLLOWER_READY:
                // Follower 就绪阶段：通知 Leader 自己已就绪
                // 职责：
                // - 向 Leader 发送 AppendPosition 消息（确认已就绪）
                //   * logPosition: 当前日志位置
                //   * leadershipTermId: 当前 leadership term ID
                // - 选举结束
                // - 返回到 ConsensusModuleAgent，切换到 ACTIVE 状态
                //
                // 结束标志：election = null
                workCount += followerReady(nowNs);
                break;

            // ==================== 终止状态 ====================
            case CLOSED:
                // 选举已关闭：不执行任何操作
                // 当 Election 对象被关闭时进入此状态
                // 原因：ConsensusModule 关闭、集群终止等
                break;
        }

        // ==================== 返回工作量 ====================
        // workCount 用于 ConsensusModuleAgent 的 idle 策略：
        // - workCount == 0: 调用 idleStrategy.idle()（降低 CPU 占用）
        // - workCount > 0: 调用 idleStrategy.reset()（保持高效率）
        return workCount;
    }

    void handleError(final long nowNs, final Throwable ex)
    {
        ctx.countedErrorHandler().onError(ex);
        logPosition = ctx.commitPositionCounter().getPlain();
        state(INIT, nowNs, ex.getMessage());

        if (ex instanceof AgentTerminationException || ex instanceof InterruptedException)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    void onRecordingSignal(
        final long correlationId, final long recordingId, final long position, final RecordingSignal signal)
    {
        if (INIT == state)
        {
            return;
        }

        if (null != logReplication)
        {
            logReplication.onSignal(correlationId, recordingId, position, signal);
            consensusModuleAgent.logRecordingId(logReplication.recordingId());
        }
    }

    void onCanvassPosition(
        final long logLeadershipTermId,
        final long logPosition,
        final long leadershipTermId,
        final int followerMemberId,
        final int protocolVersion)
    {
        if (INIT == state)
        {
            return;
        }

        if (followerMemberId == gracefulClosedLeaderId)
        {
            gracefulClosedLeaderId = NULL_VALUE;
        }

        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower && thisMember.id() != followerMemberId)
        {
            consensusModuleAgent.updateMemberLogPosition(follower, logLeadershipTermId, logPosition);

            if (logLeadershipTermId < this.leadershipTermId)
            {
                if (Cluster.Role.LEADER == consensusModuleAgent.role())
                {
                    final long nowNs = nowNs(ctx);
                    publishNewLeadershipTerm(
                        follower,
                        logLeadershipTermId,
                        consensusModuleAgent.quorumPositionBoundedByLeaderLog(
                            appendPosition, nowNs),
                        nanosToTimestamp(ctx, nowNs));
                }
            }
            else if (logLeadershipTermId > this.leadershipTermId)
            {
                switch (state)
                {
                    case LEADER_LOG_REPLICATION:
                    case LEADER_READY:
                        throw new ClusterEvent("potential new election in progress");

                    default:
                        break;
                }
            }
        }
    }

    void onRequestVote(
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int protocolVersion)
    {
        if (INIT == state)
        {
            return;
        }

        if (candidateId == thisMember.id())
        {
            return;
        }

        if (candidateTermId <= this.candidateTermId)
        {
            placeVote(candidateTermId, candidateId, false);
        }
        else if (compareLog(this.logLeadershipTermId, appendPosition, logLeadershipTermId, logPosition) > 0)
        {
            this.candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId, logPosition, ctx.epochClock().time());

            placeVote(candidateTermId, candidateId, false);

            final ClusterMember candidateMember = clusterMemberByIdMap.get(candidateId);
            if (null != candidateMember && Cluster.Role.LEADER == consensusModuleAgent.role())
            {
                final long nowNs = nowNs(ctx);
                publishNewLeadershipTerm(
                    candidateMember,
                    logLeadershipTermId,
                    consensusModuleAgent.quorumPositionBoundedByLeaderLog(appendPosition, nowNs),
                    nanosToTimestamp(ctx, nowNs));
            }
        }
        else if (CANVASS == state || NOMINATE == state || CANDIDATE_BALLOT == state || FOLLOWER_BALLOT == state)
        {
            final long nowNs = nowNs(ctx);
            this.candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId, logPosition, TimeUnit.MILLISECONDS.convert(nowNs, TimeUnit.NANOSECONDS));
            placeVote(candidateTermId, candidateId, true);
            state(FOLLOWER_BALLOT, nowNs, "");
        }
    }

    void onVote(
        final long logLeadershipTermId,
        final long logPosition,
        final long candidateTermId,
        final int candidateId,
        final int voterId,
        final boolean vote)
    {
        if (INIT == state)
        {
            return;
        }

        if (CANDIDATE_BALLOT == state &&
            candidateTermId == this.candidateTermId &&
            candidateId == thisMember.id())
        {
            final ClusterMember follower = clusterMemberByIdMap.get(voterId);
            if (null != follower)
            {
                follower
                    .candidateTermId(candidateTermId)
                    .vote(vote ? Boolean.TRUE : Boolean.FALSE);
                consensusModuleAgent.updateMemberLogPosition(follower, logLeadershipTermId, logPosition);
            }
        }
    }

    @SuppressWarnings("MethodLength")
    void onNewLeadershipTerm(
        final long logLeadershipTermId,
        final long nextLeadershipTermId,
        final long nextTermBaseLogPosition,
        final long nextLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long commitPosition,
        final long leaderRecordingId,
        final long timestamp,
        final int leaderMemberId,
        final int logSessionId,
        final boolean isStartup)
    {
        if (INIT == state)
        {
            return;
        }

        final ClusterMember leader = clusterMemberByIdMap.get(leaderMemberId);
        if (null == leader || (leaderMemberId == thisMember.id() && leadershipTermId == this.leadershipTermId))
        {
            return;
        }

        if (leaderMemberId == gracefulClosedLeaderId)
        {
            gracefulClosedLeaderId = NULL_VALUE;
        }

        if (((FOLLOWER_BALLOT == state || CANDIDATE_BALLOT == state) && leadershipTermId == candidateTermId) ||
            CANVASS == state)
        {
            if (logLeadershipTermId == this.logLeadershipTermId)
            {
                if (NULL_POSITION != nextTermBaseLogPosition && nextTermBaseLogPosition < appendPosition)
                {
                    onTruncateLogEntry(
                        thisMember.id(),
                        state,
                        logLeadershipTermId,
                        this.leadershipTermId,
                        candidateTermId,
                        ctx.commitPositionCounter().getPlain(),
                        this.logPosition,
                        appendPosition,
                        appendPosition,
                        nextTermBaseLogPosition);
                }

                this.leaderMember = leader;
                this.isLeaderStartup = isStartup;
                this.leadershipTermId = leadershipTermId;
                this.candidateTermId = max(leadershipTermId, candidateTermId);
                this.logSessionId = logSessionId;
                this.leaderRecordingId = leaderRecordingId;
                this.catchupJoinPosition = appendPosition < logPosition ? logPosition : NULL_POSITION;
                notifiedCommitPosition = max(notifiedCommitPosition, commitPosition);

                if (this.appendPosition < termBaseLogPosition)
                {
                    if (NULL_VALUE != nextLeadershipTermId)
                    {
                        if (appendPosition < nextTermBaseLogPosition)
                        {
                            replicationLeadershipTermId = logLeadershipTermId;
                            replicationStopPosition = nextTermBaseLogPosition;
                            // Here we should have an open, but uncommitted term so the base position
                            // is already known. We could look it up from the recording log only to write
                            // it back again...
                            replicationTermBaseLogPosition = NULL_VALUE;
                            state(FOLLOWER_LOG_REPLICATION, nowNs(ctx), "");
                        }
                        else if (appendPosition == nextTermBaseLogPosition)
                        {
                            if (NULL_POSITION != nextLogPosition)
                            {
                                replicationLeadershipTermId = nextLeadershipTermId;
                                replicationStopPosition = nextLogPosition;
                                replicationTermBaseLogPosition = nextTermBaseLogPosition;
                                state(FOLLOWER_LOG_REPLICATION, nowNs(ctx), "");
                            }
                        }
                    }
                    else
                    {
                        throw new ClusterException(
                            "invalid newLeadershipTerm - this.appendPosition=" + appendPosition +
                            " < termBaseLogPosition=" + termBaseLogPosition +
                            " and nextLeadershipTermId=" + nextLeadershipTermId +
                            ", logLeadershipTermId=" + logLeadershipTermId +
                            ", nextTermBaseLogPosition=" + nextTermBaseLogPosition +
                            ", nextLogPosition=" + nextLogPosition +
                            ", leadershipTermId=" + leadershipTermId +
                            ", termBaseLogPosition=" + termBaseLogPosition +
                            ", logPosition=" + logPosition +
                            ", commitPosition=" + commitPosition +
                            ", leaderRecordingId=" + leaderRecordingId +
                            ", leaderMemberId=" + leaderMemberId +
                            ", logSessionId=" + logSessionId +
                            ", isStartup=" + isStartup);
                    }
                }
                else
                {
                    state(FOLLOWER_REPLAY, nowNs(ctx), "");
                }
            }
            else
            {
                state(CANVASS, nowNs(ctx), "");
            }
        }

        if (FOLLOWER_LOG_REPLICATION == state &&
            logLeadershipTermId == this.logLeadershipTermId &&
            leaderMember.id() == leaderMemberId)
        {
            replicationDeadlineNs = nowNs(ctx) + ctx.leaderHeartbeatTimeoutNs();
        }
    }

    void onAppendPosition(
        final long leadershipTermId,
        final long logPosition,
        final int followerMemberId,
        final short flags)
    {
        if (INIT == state)
        {
            return;
        }

        if (leadershipTermId <= this.leadershipTermId)
        {
            final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
            if (null != follower)
            {
                consensusModuleAgent.updateMemberLogPosition(follower, leadershipTermId, logPosition);
                consensusModuleAgent.trackCatchupCompletion(follower, leadershipTermId, flags);
            }
        }
    }

    void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
    {
        final ElectionState state = this.state;
        if (INIT == state)
        {
            return;
        }

        // we do not check `leadershipTermId == this.leadershipTermId` here, because prior to fixes the leader was
        // sending wrong `leadershipTermId` value in the `CommitPosition` message (i.e. not matching the one sent
        // by the `NewLeadershipTerm` message).
        if (null != leaderMember && leaderMember.id() == leaderMemberId)
        {
            notifiedCommitPosition = max(notifiedCommitPosition, logPosition);
            if (FOLLOWER_LOG_REPLICATION == state)
            {
                replicationDeadlineNs = nowNs(ctx) + ctx.leaderHeartbeatTimeoutNs();
            }
        }
        else if (leadershipTermId > this.leadershipTermId && LEADER_READY == state)
        {
            throw new ClusterEvent("new leader detected due to commit position - " +
                " memberId=" + thisMemberId() +
                " this.leadershipTermId=" + this.leadershipTermId +
                " this.leaderMemberId=" + (null != leaderMember ? leaderMember.id() : NULL_VALUE) +
                " this.logPosition=" + this.logPosition +
                " newLeadershipTermId=" + leadershipTermId +
                " newLeaderMemberId=" + leaderMemberId +
                " newCommitPosition=" + logPosition +
                ")");
        }
    }

    void onReplayNewLeadershipTermEvent(
        final long leadershipTermId,
        final long logPosition,
        final long timestamp,
        final long termBaseLogPosition)
    {
        if (INIT == state)
        {
            return;
        }

        if (FOLLOWER_CATCHUP == state || FOLLOWER_REPLAY == state)
        {
            final long nowNs = timestampToNanos(ctx, timestamp);
            ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, NULL_VALUE, nowNs);
            this.logPosition = logPosition;
            this.logLeadershipTermId = leadershipTermId;
        }
    }

    void onTruncateLogEntry(
        final int memberId,
        final ElectionState state,
        final long logLeadershipTermId,
        final long leadershipTermId,
        final long candidateTermId,
        final long commitPosition,
        final long logPosition,
        final long appendPosition,
        final long oldPosition,
        final long newPosition)
    {
        consensusModuleAgent.truncateLogEntry(logLeadershipTermId, newPosition);
        this.appendPosition = newPosition;
        throw new ClusterEvent("Truncating Cluster Log - memberId=" + memberId +
            " state=" + state +
            " this.logLeadershipTermId=" + logLeadershipTermId +
            " this.leadershipTermId=" + leadershipTermId +
            " this.candidateTermId=" + candidateTermId +
            " this.commitPosition=" + commitPosition +
            " this.logPosition=" + logPosition +
            " this.appendPosition=" + appendPosition +
            " oldPosition=" + oldPosition +
            " newPosition=" + newPosition);
    }

    long notifiedCommitPosition()
    {
        return notifiedCommitPosition;
    }

    private int init(final long nowNs)
    {
        if (isFirstInit)
        {
            isFirstInit = false;
            if (!isNodeStartup)
            {
                prepareForNewLeadership(nowNs);
            }
        }
        else
        {
            stopLogReplication();
            stopCatchup();

            prepareForNewLeadership(nowNs);
            logSessionId = NULL_SESSION_ID;
            stopReplay();

            if (null != logSubscription)
            {
                CloseHelper.close(logSubscription);
                consensusModuleAgent.awaitLocalSocketsClosed(logSubscription.registrationId());
                logSubscription = null;
            }
        }

        notifiedCommitPosition = 0;
        candidateTermId = max(ctx.nodeStateFile().candidateTerm().candidateTermId(), leadershipTermId);

        if (clusterMembers.length == 1 && thisMember.id() == clusterMembers[0].id())
        {
            // short-circuit nominating self to a candidate and winning the ballot
            candidateTermId += 1;
            ctx.nodeStateFile().updateCandidateTermId(candidateTermId, logPosition, ctx.epochClock().time());
            leaderMember = thisMember;
            leadershipTermId = candidateTermId;
            state(LEADER_LOG_REPLICATION, nowNs, "single-node cluster leader");
        }
        else
        {
            state(CANVASS, nowNs, "");
        }

        return 1;
    }

    private int canvass(final long nowNs)
    {
        int workCount = 0;
        final long deadlineNs = isExtendedCanvass ?
            timeOfLastStateChangeNs + ctx.startupCanvassTimeoutNs() :
            consensusModuleAgent.timeOfLastLeaderUpdateNs() + ctx.leaderHeartbeatTimeoutNs();

        if (hasUpdateIntervalExpired(nowNs, ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishCanvassPosition();

            workCount++;
        }

        if (ctx.appointedLeaderId() != NULL_VALUE && ctx.appointedLeaderId() != thisMember.id())
        {
            return workCount;
        }

        if (ClusterMember.isUnanimousCandidate(clusterMembers, thisMember, gracefulClosedLeaderId) ||
            (nowNs >= deadlineNs && ClusterMember.isQuorumCandidate(clusterMembers, thisMember)))
        {
            final long delayNs = (long)(ctx.random().nextDouble() * (ctx.electionTimeoutNs() >> 1));
            nominationDeadlineNs = nowNs + delayNs;
            state(NOMINATE, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int nominate(final long nowNs)
    {
        if (nowNs >= nominationDeadlineNs)
        {
            candidateTermId = ctx.nodeStateFile().proposeMaxCandidateTermId(
                candidateTermId + 1, logPosition, ctx.epochClock().time());
            ClusterMember.becomeCandidate(clusterMembers, candidateTermId, thisMember.id());
            state(CANDIDATE_BALLOT, nowNs, "");

            return 1;
        }
        else if (hasUpdateIntervalExpired(nowNs, ctx.electionStatusIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishCanvassPosition();

            return 1;
        }

        return 0;
    }

    private int candidateBallot(final long nowNs)
    {
        int workCount = 0;

        if (ClusterMember.isUnanimousLeader(clusterMembers, candidateTermId, gracefulClosedLeaderId))
        {
            leaderMember = thisMember;
            leadershipTermId = candidateTermId;
            state(LEADER_LOG_REPLICATION, nowNs, "unanimous leader");
            workCount++;
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            if (ClusterMember.isQuorumLeader(clusterMembers, candidateTermId))
            {
                leaderMember = thisMember;
                leadershipTermId = candidateTermId;
                state(LEADER_LOG_REPLICATION, nowNs, "quorum leader");
            }
            else
            {
                state(CANVASS, nowNs, "");
            }

            workCount++;
        }
        else
        {
            for (final ClusterMember member : clusterMembers)
            {
                if (!member.isBallotSent())
                {
                    workCount++;
                    member.isBallotSent(consensusPublisher.requestVote(
                        member.publication(), logLeadershipTermId, appendPosition, candidateTermId, thisMember.id()));
                }
            }
        }

        return workCount;
    }

    private int followerBallot(final long nowNs)
    {
        int workCount = 0;

        if (nowNs >= (timeOfLastStateChangeNs + ctx.electionTimeoutNs()))
        {
            state(CANVASS, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int leaderLogReplication(final long nowNs)
    {
        int workCount = 0;

        thisMember.logPosition(appendPosition).timeOfLastAppendPositionNs(nowNs);

        final long quorumPosition = consensusModuleAgent.quorumPositionBoundedByLeaderLog(appendPosition, nowNs);
        workCount += publishNewLeadershipTermOnInterval(quorumPosition, nowNs);
        workCount += publishCommitPositionOnInterval(quorumPosition, nowNs);

        if (quorumPosition >= appendPosition)
        {
            workCount++;
            state(LEADER_REPLAY, nowNs, "");
        }

        return workCount;
    }

    private int leaderReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            if (logPosition < appendPosition)
            {
                logReplay = consensusModuleAgent.newLogReplay(logPosition, appendPosition);
            }
            else
            {
                state(LEADER_INIT, nowNs, "");
            }

            workCount++;
            isLeaderStartup = isNodeStartup;
            thisMember
                .leadershipTermId(leadershipTermId)
                .logPosition(appendPosition)
                .timeOfLastAppendPositionNs(nowNs);
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                stopReplay();
                logPosition = appendPosition;
                state(LEADER_INIT, nowNs, "");
            }
        }

        final long quorumPosition = consensusModuleAgent.quorumPositionBoundedByLeaderLog(appendPosition, nowNs);
        workCount += publishNewLeadershipTermOnInterval(quorumPosition, nowNs);
        workCount += publishCommitPositionOnInterval(quorumPosition, nowNs);

        return workCount;
    }

    private int leaderInit(final long nowNs)
    {
        consensusModuleAgent.joinLogAsLeader(leadershipTermId, logPosition, logSessionId, isLeaderStartup);
        updateRecordingLog(nowNs);
        state(LEADER_READY, nowNs, "");

        return 1;
    }

    private int leaderReady(final long nowNs)
    {
        final long quorumPosition = consensusModuleAgent.quorumPositionBoundedByLeaderLog(appendPosition, nowNs);
        int workCount = consensusModuleAgent.updateLeaderPosition(nowNs, appendPosition, quorumPosition);
        workCount += publishNewLeadershipTermOnInterval(quorumPosition, nowNs);

        if (ClusterMember.hasQuorumAtPosition(
                clusterMembers, leadershipTermId, logPosition, nowNs, ctx.leaderHeartbeatTimeoutNs()))
        {
            if (consensusModuleAgent.appendNewLeadershipTermEvent(nowNs))
            {
                consensusModuleAgent.electionComplete(nowNs);
                state(CLOSED, nowNs, "");
                workCount++;
            }
        }

        return workCount;
    }

    private int followerLogReplication(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplication)
        {
            if (appendPosition < replicationStopPosition)
            {
                logReplication = consensusModuleAgent.newLogReplication(
                    leaderMember.archiveEndpoint(),
                    leaderMember.archiveResponseEndpoint(),
                    leaderRecordingId,
                    replicationStopPosition,
                    nowNs);
                replicationDeadlineNs = nowNs + ctx.leaderHeartbeatTimeoutNs();
                workCount++;
            }
            else
            {
                updateRecordingLogForReplication(
                    replicationLeadershipTermId, replicationTermBaseLogPosition, replicationStopPosition, nowNs);
                state(CANVASS, nowNs, "");
            }
        }
        else
        {
            workCount += consensusModuleAgent.pollArchiveEvents();
            logReplication.poll(nowNs);
            final boolean replicationDone = logReplication.hasReplicationEnded() && logReplication.hasStopped();
            // Log replication runs concurrently, calling this after the check for completion ensures that the
            // last position at the end of the leadership is published as an appendPosition event.
            workCount += publishFollowerReplicationPosition(nowNs);

            if (replicationDone)
            {
                if (notifiedCommitPosition >= appendPosition)
                {
                    ConsensusModuleAgent.logReplicationEnded(
                        thisMember.id(),
                        "ELECTION",
                        logReplication.srcArchiveChannel(),
                        logReplication.recordingId(),
                        leaderRecordingId,
                        logReplication.position(),
                        logReplication.hasSynced());

                    appendPosition = logReplication.position();
                    stopLogReplication();
                    updateRecordingLogForReplication(
                        replicationLeadershipTermId, replicationTermBaseLogPosition, replicationStopPosition, nowNs);
                    state(CANVASS, nowNs, "");
                    workCount++;
                }
                else if (nowNs >= replicationDeadlineNs)
                {
                    throw new TimeoutException("timeout awaiting commit position", AeronException.Category.WARN);
                }
            }
        }

        return workCount;
    }

    private int followerReplay(final long nowNs)
    {
        int workCount = 0;

        if (null == logReplay)
        {
            if (logPosition < appendPosition)
            {
                if (0 == notifiedCommitPosition)
                {
                    return publishFollowerAppendPosition(nowNs);
                }
                else if (logPosition >= notifiedCommitPosition)
                {
                    state(CANVASS, nowNs, "log replay rejected: logPosition=" + logPosition + " is " +
                        (logPosition > notifiedCommitPosition ? "greater than" : "equal to") +
                        " quorumPosition=" + notifiedCommitPosition);
                }
                else
                {
                    logReplay =
                        consensusModuleAgent.newLogReplay(logPosition, min(appendPosition, notifiedCommitPosition));
                    workCount++;
                }
            }
            else
            {
                state(
                    NULL_POSITION != catchupJoinPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT,
                    nowNs,
                    "nothing to replay");
                workCount++;
            }
        }
        else
        {
            workCount += logReplay.doWork();
            if (logReplay.isDone())
            {
                logPosition = logReplay.position();
                stopReplay();

                if (logPosition == appendPosition)
                {
                    state(
                        NULL_POSITION != catchupJoinPosition ? FOLLOWER_CATCHUP_INIT : FOLLOWER_LOG_INIT,
                        nowNs,
                        "log replay done");
                }
                else
                {
                    state(CANVASS, nowNs, "incomplete log replay: logPosition=" + logPosition +
                        " appendPosition=" + appendPosition);
                }
            }
        }

        return workCount;
    }

    private int followerCatchupInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            logSubscription = addFollowerSubscription();
            addCatchupLogDestination();
        }

        String catchupEndpoint = null;
        final String endpoint = thisMember.catchupEndpoint();
        if (endpoint.endsWith(":0"))
        {
            final String resolvedEndpoint = logSubscription.resolvedEndpoint();
            if (null != resolvedEndpoint)
            {
                final int i = resolvedEndpoint.lastIndexOf(':');
                catchupEndpoint = endpoint.substring(0, endpoint.length() - 2) + resolvedEndpoint.substring(i);
            }
        }
        else
        {
            catchupEndpoint = endpoint;
        }

        if (null != catchupEndpoint && sendCatchupPosition(catchupEndpoint))
        {
            timeOfLastUpdateNs = nowNs;
            consensusModuleAgent.catchupInitiated(nowNs);
            state(FOLLOWER_CATCHUP_AWAIT, nowNs, "");
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to send catchup position", AeronException.Category.WARN);
        }

        return 1;
    }

    private int followerCatchupAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogJoinPosition("followerCatchupAwait", image.joinPosition());
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup, nowNs))
            {
                state(FOLLOWER_CATCHUP, nowNs, "");
                workCount++;
            }
            else if (ChannelEndpointStatus.ERRORED == logSubscription.channelStatus())
            {
                final String message = "failed to add catchup log as follower - " + logSubscription.channel();
                throw new ClusterException(message, AeronException.Category.WARN);
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join catchup log as follower", AeronException.Category.WARN);
            }
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join catchup log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerCatchup(final long nowNs)
    {
        int workCount = consensusModuleAgent.catchupPoll(notifiedCommitPosition, nowNs);

        if (null == consensusModuleAgent.liveLogDestination() &&
            consensusModuleAgent.isCatchupNearLive(max(catchupJoinPosition, notifiedCommitPosition)))
        {
            addLiveLogDestination();
            workCount++;
        }

        final long position = ctx.commitPositionCounter().getPlain();
        if (position >= catchupJoinPosition &&
            position >= notifiedCommitPosition &&
            null == consensusModuleAgent.catchupLogDestination() &&
            ConsensusModule.State.SNAPSHOT != consensusModuleAgent.state())
        {
            appendPosition = position;
            logPosition = position;
            state(FOLLOWER_LOG_INIT, nowNs, "");
            workCount++;
        }

        return workCount;
    }

    private int followerLogInit(final long nowNs)
    {
        if (null == logSubscription)
        {
            if (NULL_SESSION_ID != logSessionId)
            {
                logSubscription = addFollowerSubscription();
                addLiveLogDestination();
                state(FOLLOWER_LOG_AWAIT, nowNs, "");
            }
        }
        else
        {
            state(FOLLOWER_READY, nowNs, "");
        }

        return 1;
    }

    private int followerLogAwait(final long nowNs)
    {
        int workCount = 0;

        final Image image = logSubscription.imageBySessionId(logSessionId);
        if (null != image)
        {
            verifyLogJoinPosition("followerLogAwait", image.joinPosition());
            if (consensusModuleAgent.tryJoinLogAsFollower(image, isLeaderStartup, nowNs))
            {
                updateRecordingLog(nowNs);
                state(FOLLOWER_READY, nowNs, "");
                workCount++;
            }
            else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
            {
                throw new TimeoutException("failed to join live log as follower", AeronException.Category.WARN);
            }
        }
        else if (ChannelEndpointStatus.ERRORED == logSubscription.channelStatus())
        {
            final String message = "failed to add live log as follower - " + logSubscription.channel();
            throw new ClusterException(message, AeronException.Category.WARN);
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("failed to join live log", AeronException.Category.WARN);
        }

        return workCount;
    }

    private int followerReady(final long nowNs)
    {
        if (consensusPublisher.appendPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id(), APPEND_POSITION_FLAG_NONE))
        {
            consensusModuleAgent.electionComplete(nowNs);
            state(CLOSED, nowNs, "");
        }
        else if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
        {
            throw new TimeoutException("ready follower failed to notify leader", AeronException.Category.WARN);
        }

        return 1;
    }

    private void placeVote(final long candidateTermId, final int candidateId, final boolean vote)
    {
        final ClusterMember candidate = clusterMemberByIdMap.get(candidateId);
        if (null != candidate)
        {
            consensusPublisher.placeVote(
                candidate.publication(),
                candidateTermId,
                logLeadershipTermId,
                appendPosition,
                candidateId,
                thisMember.id(),
                vote);
        }
    }

    private int publishNewLeadershipTermOnInterval(final long quorumPosition, final long nowNs)
    {
        int workCount = 0;

        if (hasUpdateIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs()))
        {
            timeOfLastUpdateNs = nowNs;
            publishNewLeadershipTerm(quorumPosition, nanosToTimestamp(ctx, nowNs));
            workCount++;
        }

        return workCount;
    }

    private int publishCommitPositionOnInterval(final long quorumPosition, final long nowNs)
    {
        int workCount = 0;

        // We would stop sending `commitPosition` if `quorumPosition` regresses (goes backwards) during election. This
        // is fine, because the NewLeadershipTerm continues to be sent on a regular interval.
        if (lastPublishedCommitPosition < quorumPosition ||
            (lastPublishedCommitPosition == quorumPosition &&
            hasIntervalExpired(nowNs, timeOfLastCommitPositionUpdateNs, ctx.leaderHeartbeatIntervalNs())))
        {
            timeOfLastCommitPositionUpdateNs = nowNs;
            lastPublishedCommitPosition = quorumPosition;
            // use `leadershipTermId` of the election, i.e. match the value used for the `NewLeadershipTerm` event
            consensusModuleAgent.publishCommitPosition(quorumPosition, leadershipTermId);
            workCount++;
        }

        return workCount;
    }

    private void publishCanvassPosition()
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.id() != thisMember.id())
            {
                if (null == member.publication())
                {
                    ClusterMember.tryAddPublication(
                        member,
                        ctx.consensusStreamId(),
                        ctx.aeron(),
                        ctx.countedErrorHandler());
                }

                consensusPublisher.canvassPosition(
                    member.publication(), logLeadershipTermId, appendPosition, leadershipTermId, thisMember.id());
            }
        }
    }

    private void publishNewLeadershipTerm(final long quorumPosition, final long timestamp)
    {
        for (final ClusterMember member : clusterMembers)
        {
            publishNewLeadershipTerm(member, logLeadershipTermId, quorumPosition, timestamp);
        }
    }

    private void publishNewLeadershipTerm(
        final ClusterMember member,
        final long logLeadershipTermId,
        final long quorumPosition,
        final long timestamp)
    {
        if (member.id() != thisMember.id() && NULL_SESSION_ID != logSessionId)
        {
            final RecordingLog.Entry logNextTermEntry =
                ctx.recordingLog().findTermEntry(logLeadershipTermId + 1);

            final long nextLeadershipTermId = null != logNextTermEntry ?
                logNextTermEntry.leadershipTermId : leadershipTermId;
            final long nextTermBaseLogPosition = null != logNextTermEntry ?
                logNextTermEntry.termBaseLogPosition : appendPosition;

            final long nextLogPosition = null != logNextTermEntry ?
                (NULL_POSITION != logNextTermEntry.logPosition ? logNextTermEntry.logPosition : appendPosition) :
                NULL_POSITION;

            consensusPublisher.newLeadershipTerm(
                member.publication(),
                logLeadershipTermId,
                nextLeadershipTermId,
                nextTermBaseLogPosition,
                nextLogPosition,
                leadershipTermId,
                appendPosition,
                appendPosition,
                quorumPosition,
                consensusModuleAgent.logRecordingId(),
                timestamp,
                thisMember.id(),
                logSessionId,
                ctx.appVersion(),
                isLeaderStartup);
        }
    }

    private int publishFollowerReplicationPosition(final long nowNs)
    {
        final long position = logReplication.position();
        if (position > appendPosition ||
            (position == appendPosition && hasUpdateIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs())))
        {
            if (consensusPublisher.appendPosition(
                leaderMember.publication(), leadershipTermId, position, thisMember.id(), APPEND_POSITION_FLAG_NONE))
            {
                appendPosition = position;
                timeOfLastUpdateNs = nowNs;
                return 1;
            }
        }

        return 0;
    }

    private int publishFollowerAppendPosition(final long nowNs)
    {
        if (lastPublishedAppendPosition != appendPosition ||
            hasUpdateIntervalExpired(nowNs, ctx.leaderHeartbeatIntervalNs()))
        {
            if (consensusPublisher.appendPosition(
                leaderMember.publication(),
                leadershipTermId,
                appendPosition,
                thisMember.id(),
                APPEND_POSITION_FLAG_NONE))
            {
                lastPublishedAppendPosition = appendPosition;
                timeOfLastUpdateNs = nowNs;
                return 1;
            }
        }
        return 0;
    }

    private boolean sendCatchupPosition(final String catchupEndpoint)
    {
        return consensusPublisher.catchupPosition(
            leaderMember.publication(), leadershipTermId, logPosition, thisMember.id(), catchupEndpoint);
    }

    private void addCatchupLogDestination()
    {
        final String destination = ChannelUri.createDestinationUri(ctx.logChannel(), thisMember.catchupEndpoint());
        logSubscription.addDestination(destination);
        consensusModuleAgent.catchupLogDestination(destination);
    }

    private void addLiveLogDestination()
    {
        final String destination;
        if (ctx.isLogMdc())
        {
            destination = ChannelUri.createDestinationUri(ctx.logChannel(), thisMember.logEndpoint());
        }
        else
        {
            destination = ctx.logChannel();
        }
        logSubscription.addDestination(destination);
        consensusModuleAgent.liveLogDestination(destination);
    }

    private Subscription addFollowerSubscription()
    {
        final Aeron aeron = ctx.aeron();
        final ChannelUri logChannelUri = ChannelUri.parse(ctx.logChannel());
        final String channel = new ChannelUriStringBuilder()
            .media(UDP_MEDIA)
            .tags(aeron.nextCorrelationId() + "," + aeron.nextCorrelationId())
            .controlMode(MDC_CONTROL_MODE_MANUAL)
            .sessionId(logSessionId)
            .group(Boolean.TRUE)
            .rejoin(Boolean.FALSE)
            .socketRcvbufLength(logChannelUri)
            .receiverWindowLength(logChannelUri)
            .alias("log-cm")
            .build();

        return aeron.addSubscription(channel, ctx.logStreamId());
    }

    private void state(final ElectionState newState, final long nowNs, final String reason)
    {
        if (newState != state)
        {
            if (CANVASS == state)
            {
                isExtendedCanvass = false;
            }

            switch (newState)
            {
                case CANVASS:
                    resetMembers();
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;

                case CANDIDATE_BALLOT:
                    consensusModuleAgent.role(Cluster.Role.CANDIDATE);
                    break;

                case LEADER_LOG_REPLICATION:
                    consensusModuleAgent.role(Cluster.Role.LEADER);
                    logSessionId = consensusModuleAgent.addLogPublication(appendPosition);
                    break;

                case FOLLOWER_LOG_REPLICATION:
                case FOLLOWER_REPLAY:
                    consensusModuleAgent.role(Cluster.Role.FOLLOWER);
                    break;

                default:
                    break;
            }

            logStateChange(
                thisMember.id(),
                state,
                newState,
                null != leaderMember ? leaderMember.id() : NULL_VALUE,
                candidateTermId,
                leadershipTermId,
                logPosition,
                logLeadershipTermId,
                appendPosition,
                catchupJoinPosition,
                reason);

            state = newState;
            ctx.electionStateCounter().setRelease(newState.code());
            timeOfLastStateChangeNs = nowNs;
            timeOfLastUpdateNs = initialTimeOfLastUpdateNs;
            timeOfLastCommitPositionUpdateNs = initialTimeOfLastUpdateNs;
        }
    }

    private void stopCatchup()
    {
        consensusModuleAgent.stopAllCatchups();
        catchupJoinPosition = NULL_POSITION;
    }

    private void resetMembers()
    {
        ClusterMember.reset(clusterMembers);
        thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);
        leaderMember = null;
    }

    private void stopReplay()
    {
        if (null != logReplay)
        {
            logReplay.close();
            logReplay = null;
        }
        lastPublishedAppendPosition = 0;
    }

    private void stopLogReplication()
    {
        if (null != logReplication)
        {
            logReplication.close();
            logReplication = null;
        }
        replicationDeadlineNs = 0;
        lastPublishedCommitPosition = 0;
    }

    private void ensureRecordingLogCoherent(
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        ensureRecordingLogCoherent(
            ctx,
            consensusModuleAgent.logRecordingId(),
            initialLogLeadershipTermId,
            initialTermBaseLogPosition,
            leadershipTermId,
            termBaseLogPosition,
            logPosition,
            nowNs);
    }

    static void ensureRecordingLogCoherent(
        final ConsensusModule.Context ctx,
        final long recordingId,
        final long initialLogLeadershipTermId,
        final long initialTermBaseLogPosition,
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        if (NULL_VALUE == recordingId)
        {
            // This can happen during a log replication if the initial appendPosition != 0 and
            // nextTermLogPosition == appendPosition.
            return;
        }

        final long timestamp = nanosToTimestamp(ctx, nowNs);
        final RecordingLog recordingLog = ctx.recordingLog();

        recordingLog.ensureCoherent(
            recordingId,
            initialLogLeadershipTermId,
            initialTermBaseLogPosition,
            leadershipTermId,
            NULL_VALUE != termBaseLogPosition ? termBaseLogPosition : initialTermBaseLogPosition,
            logPosition,
            nowNs,
            timestamp,
            ctx.fileSyncLevel());
    }

    private void updateRecordingLog(final long nowNs)
    {
        ensureRecordingLogCoherent(leadershipTermId, logPosition, NULL_VALUE, nowNs);
        logLeadershipTermId = leadershipTermId;
    }

    private void updateRecordingLogForReplication(
        final long leadershipTermId,
        final long termBaseLogPosition,
        final long logPosition,
        final long nowNs)
    {
        ensureRecordingLogCoherent(leadershipTermId, termBaseLogPosition, logPosition, nowNs);
        logLeadershipTermId = leadershipTermId;
    }

    private void verifyLogJoinPosition(final String state, final long joinPosition)
    {
        if (joinPosition != logPosition)
        {
            final String inequality = joinPosition < logPosition ? " less " : " greater ";
            throw new ClusterEvent(
                state + " - joinPosition=" + joinPosition + inequality + "than logPosition=" + logPosition);
        }
    }

    private boolean hasUpdateIntervalExpired(final long nowNs, final long intervalNs)
    {
        return hasIntervalExpired(nowNs, timeOfLastUpdateNs, intervalNs);
    }

    private boolean hasIntervalExpired(
        final long nowNs, final long previousTimestampForIntervalNs, final long intervalNs)
    {
        return (nowNs - previousTimestampForIntervalNs) >= intervalNs;
    }

    private void logStateChange(
        final int memberId,
        final ElectionState oldState,
        final ElectionState newState,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
        /*
        System.out.println("Election: memberId=" + memberId + " " + oldState + " -> " + newState +
            " leaderId=" + leaderId +
            " candidateTermId=" + candidateTermId +
            " leadershipTermId=" + leadershipTermId +
            " logPosition=" + logPosition +
            " logLeadershipTermId=" + logLeadershipTermId +
            " appendPosition=" + appendPosition +
            " catchupPosition=" + catchupPosition +
            " notifiedCommitPosition=" + notifiedCommitPosition +
            " reason=" + reason);
         */
    }

    private void prepareForNewLeadership(final long nowNs)
    {
        final long lastAppendPosition = consensusModuleAgent.prepareForNewLeadership(logPosition, nowNs);
        if (NULL_POSITION != lastAppendPosition)
        {
            appendPosition = lastAppendPosition;
        }
    }

    private static long nowNs(final ConsensusModule.Context ctx)
    {
        return ctx.clusterClock().timeNanos();
    }

    private static long nanosToTimestamp(final ConsensusModule.Context ctx, final long timeNs)
    {
        return ctx.clusterClock().timeUnit().convert(timeNs, TimeUnit.NANOSECONDS);
    }

    private static long timestampToNanos(final ConsensusModule.Context ctx, final long timestamp)
    {
        return ctx.clusterClock().timeUnit().toNanos(timestamp);
    }

    public String toString()
    {
        return "Election{" +
            "clusterMembers=" + Arrays.toString(clusterMembers) +
            ", thisMember=" + thisMember +
            ", leaderMember=" + leaderMember +
            ", initialLogLeadershipTermId=" + initialLogLeadershipTermId +
            ", initialTimeOfLastUpdateNs=" + initialTimeOfLastUpdateNs +
            ", initialTermBaseLogPosition=" + initialTermBaseLogPosition +
            ", isNodeStartup=" + isNodeStartup +
            ", timeOfLastStateChangeNs=" + timeOfLastStateChangeNs +
            ", timeOfLastUpdateNs=" + timeOfLastUpdateNs +
            ", timeOfLastCommitPositionUpdateNs=" + timeOfLastCommitPositionUpdateNs +
            ", nominationDeadlineNs=" + nominationDeadlineNs +
            ", logPosition=" + logPosition +
            ", appendPosition=" + appendPosition +
            ", notifiedCommitPosition=" + notifiedCommitPosition +
            ", catchupJoinPosition=" + catchupJoinPosition +
            ", replicationLeadershipTermId=" + replicationLeadershipTermId +
            ", replicationStopPosition=" + replicationStopPosition +
            ", replicationDeadlineNs=" + replicationDeadlineNs +
            ", replicationTermBaseLogPosition=" + replicationTermBaseLogPosition +
            ", leaderRecordingId=" + leaderRecordingId +
            ", leadershipTermId=" + leadershipTermId +
            ", logLeadershipTermId=" + logLeadershipTermId +
            ", candidateTermId=" + candidateTermId +
            ", lastPublishedCommitPosition=" + lastPublishedCommitPosition +
            ", logSessionId=" + logSessionId +
            ", gracefulClosedLeaderId=" + gracefulClosedLeaderId +
            ", isFirstInit=" + isFirstInit +
            ", isLeaderStartup=" + isLeaderStartup +
            ", isExtendedCanvass=" + isExtendedCanvass +
            ", state=" + state +
            ", logSubscription=" + logSubscription +
            ", logReplay=" + logReplay +
            ", logReplication=" + logReplication +
            '}';
    }
}

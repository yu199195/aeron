# Election.doWork() 深度解析

本文档详细解析 Aeron Cluster 选举的核心方法 `election.doWork(clusterClock.timeNanos())`，包含完整的中文注释和流程图。

---

## 1. 调用入口

**位置**：`ConsensusModuleAgent.java:367`

```java
// ConsensusModuleAgent.doWork() 方法中
if (null != election)
{
    // 执行选举逻辑（每次循环都调用）
    // - clusterClock.timeNanos(): 集群确定性时间（纳秒）
    // - 返回值 workCount: 本次执行的工作量（用于空闲策略判断）
    workCount += election.doWork(clusterClock.timeNanos());
}
```

**何时创建 Election 对象？**
- 节点启动时无 Leader
- Leader 心跳超时（leaderHeartbeatTimeoutNs）
- 集群成员变更

---

## 2. doWork() 方法完整源码（带中文注释）

**位置**：`Election.java:370-449`

```java
/**
 * Election 的主工作循环：根据当前选举状态执行对应的逻辑。
 * <p>
 * 这是一个<b>状态机</b>，每次调用都会根据 {@code state} 执行对应的处理函数。
 * 状态机保证了选举流程的有序推进：
 * <ul>
 *   <li>INIT → CANVASS → NOMINATE → BALLOT → READY</li>
 *   <li>或者直接进入 FOLLOWER 状态</li>
 * </ul>
 * <p>
 * <b>执行频率</b>：每次 ConsensusModuleAgent.doWork() 都会调用（约 1000 次/秒）
 * <p>
 * <b>返回值</b>：本次执行的工作量（0 表示无事可做，>0 表示有工作完成）
 *
 * @param nowNs 当前集群时间（纳秒），用于超时判断
 * @return 本次执行的工作量
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
            // - 转换到 CANVASS 状态
            workCount += init(nowNs);
            break;

        // ==================== 阶段 2: 询问阶段（Canvass） ====================
        case CANVASS:
            // 询问阶段：向所有节点发送 RequestVote 请求，收集日志位置信息
            // 职责：
            // - 向所有成员发送 CanvassPosition 消息（询问日志位置）
            // - 收集响应，判断自己的日志是否是最新的
            // - 超时后转换到 NOMINATE 状态
            workCount += canvass(nowNs);
            break;

        // ==================== 阶段 3: 提名阶段（Nominate） ====================
        case NOMINATE:
            // 提名阶段：决定是否参选
            // 职责：
            // - 比较所有节点的日志位置
            // - 如果自己的日志最新，转换到 CANDIDATE_BALLOT（参选）
            // - 否则转换到 FOLLOWER_BALLOT（跟随）
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
            workCount += candidateBallot(nowNs);
            break;

        // ==================== 阶段 4b: 跟随者投票阶段 ====================
        case FOLLOWER_BALLOT:
            // 跟随者投票阶段：作为跟随者，等待投票结果
            // 职责：
            // - 等待接收 NewLeadershipTerm 消息（新 Leader 通知）
            // - 收到后转换到 FOLLOWER_LOG_REPLICATION
            // - 超时后重新开始选举（回到 CANVASS）
            workCount += followerBallot(nowNs);
            break;

        // ==================== 阶段 5a: Leader 日志复制 ====================
        case LEADER_LOG_REPLICATION:
            // Leader 日志复制阶段：启动日志 publication，等待 Followers 订阅
            // 职责：
            // - 创建日志 publication（供 Followers 订阅）
            // - 等待 Followers 连接并开始复制
            // - 转换到 LEADER_REPLAY
            workCount += leaderLogReplication(nowNs);
            break;

        // ==================== 阶段 6a: Leader 日志重放 ====================
        case LEADER_REPLAY:
            // Leader 日志重放阶段：重放未提交的日志
            // 职责：
            // - 从上次提交位置（commitPosition）重放到当前位置（logPosition）
            // - 确保 Leader 状态与日志一致
            // - 转换到 LEADER_INIT
            workCount += leaderReplay(nowNs);
            break;

        // ==================== 阶段 7a: Leader 初始化 ====================
        case LEADER_INIT:
            // Leader 初始化阶段：准备接受客户端请求
            // 职责：
            // - 创建 NewLeadershipTerm 事件，写入日志
            // - 通知所有 Followers 新的 leadership term
            // - 转换到 LEADER_READY
            workCount += leaderInit(nowNs);
            break;

        // ==================== 阶段 8a: Leader 就绪 ====================
        case LEADER_READY:
            // Leader 就绪阶段：等待所有 Followers 追赶到 leadershipTermId
            // 职责：
            // - 等待 quorum 的 Followers 追赶到 NewLeadershipTerm 位置
            // - 所有 Followers 就绪后，选举结束
            // - 返回到 ConsensusModuleAgent，切换到 ACTIVE 状态
            workCount += leaderReady(nowNs);
            break;

        // ==================== 阶段 5b: Follower 日志复制 ====================
        case FOLLOWER_LOG_REPLICATION:
            // Follower 日志复制阶段：订阅 Leader 的日志
            // 职责：
            // - 订阅 Leader 的日志 publication
            // - 等待订阅成功
            // - 转换到 FOLLOWER_REPLAY
            workCount += followerLogReplication(nowNs);
            break;

        // ==================== 阶段 6b: Follower 日志重放 ====================
        case FOLLOWER_REPLAY:
            // Follower 日志重放阶段：重放未提交的日志
            // 职责：
            // - 从上次提交位置重放到 Leader 通知的位置
            // - 确保 Follower 状态与日志一致
            // - 根据日志差距决定下一步：
            //   - 日志一致 → FOLLOWER_LOG_INIT
            //   - 日志落后 → FOLLOWER_CATCHUP_INIT（需要追赶）
            workCount += followerReplay(nowNs);
            break;

        // ==================== 阶段 7b: Follower 追赶初始化 ====================
        case FOLLOWER_CATCHUP_INIT:
            // Follower 追赶初始化阶段：准备从 Leader 同步落后的日志
            // 职责：
            // - 请求 Leader 提供 catchup 日志（从 replayPosition 开始）
            // - 等待 Leader 创建 catchup publication
            // - 转换到 FOLLOWER_CATCHUP_AWAIT
            workCount += followerCatchupInit(nowNs);
            break;

        // ==================== 阶段 8b: Follower 等待追赶 ====================
        case FOLLOWER_CATCHUP_AWAIT:
            // Follower 等待追赶阶段：等待 Leader 提供的 catchup publication 就绪
            // 职责：
            // - 等待 Leader 返回 CatchupPosition 消息（包含 recordingId）
            // - 订阅 catchup publication
            // - 转换到 FOLLOWER_CATCHUP
            workCount += followerCatchupAwait(nowNs);
            break;

        // ==================== 阶段 9b: Follower 追赶中 ====================
        case FOLLOWER_CATCHUP:
            // Follower 追赶阶段：从 Leader 同步落后的日志
            // 职责：
            // - 从 catchup publication 接收日志
            // - 写入本地 Archive
            // - 追赶完成后转换到 FOLLOWER_LOG_INIT
            workCount += followerCatchup(nowNs);
            break;

        // ==================== 阶段 10b: Follower 日志初始化 ====================
        case FOLLOWER_LOG_INIT:
            // Follower 日志初始化阶段：准备接收 Leader 的实时日志
            // 职责：
            // - 从 Archive 加载 catchup 的日志（如果有）
            // - 准备订阅 Leader 的实时日志
            // - 转换到 FOLLOWER_LOG_AWAIT
            workCount += followerLogInit(nowNs);
            break;

        // ==================== 阶段 11b: Follower 等待日志 ====================
        case FOLLOWER_LOG_AWAIT:
            // Follower 等待日志阶段：等待日志订阅就绪
            // 职责：
            // - 等待日志订阅连接到 Leader
            // - 转换到 FOLLOWER_READY
            workCount += followerLogAwait(nowNs);
            break;

        // ==================== 阶段 12b: Follower 就绪 ====================
        case FOLLOWER_READY:
            // Follower 就绪阶段：通知 Leader 自己已就绪
            // 职责：
            // - 向 Leader 发送 AppendPosition 消息（确认已就绪）
            // - 选举结束
            // - 返回到 ConsensusModuleAgent，切换到 ACTIVE 状态
            workCount += followerReady(nowNs);
            break;

        // ==================== 终止状态 ====================
        case CLOSED:
            // 选举已关闭：不执行任何操作
            // 当 Election 对象被关闭时进入此状态
            break;
    }

    // ==================== 返回工作量 ====================
    // workCount 用于 ConsensusModuleAgent 的 idle 策略：
    // - workCount == 0: 调用 idleStrategy.idle()（降低 CPU 占用）
    // - workCount > 0: 调用 idleStrategy.reset()（保持高效率）
    return workCount;
}
```

---

## 3. 完整的选举状态机流程图

### 3.1 高层视图：选举的三条路径

```
┌─────────────────────────────────────────────────────────────────┐
│                      Election 状态机总览                         │
└─────────────────────────────────────────────────────────────────┘

                        ┌───────────────┐
                        │     INIT      │  ← 选举开始
                        │  初始化状态    │
                        └───────┬───────┘
                                │
                                ↓
                        ┌───────────────┐
                        │   CANVASS     │  ← 询问阶段
                        │ 收集日志位置   │
                        └───────┬───────┘
                                │
                                ↓
                        ┌───────────────┐
                        │   NOMINATE    │  ← 提名阶段
                        │  决定是否参选  │
                        └───────┬───────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
                    ↓                       ↓
        ┌───────────────────┐   ┌───────────────────┐
        │ CANDIDATE_BALLOT  │   │ FOLLOWER_BALLOT   │
        │   候选人投票       │   │   跟随者等待       │
        └─────────┬─────────┘   └─────────┬─────────┘
                  │ 获得多数票              │ 收到 NewLeadershipTerm
                  │                         │
                  ↓                         ↓
        ┌──────────────────────┐   ┌──────────────────────┐
        │  Leader 路径          │   │  Follower 路径        │
        ├──────────────────────┤   ├──────────────────────┤
        │ LEADER_LOG_REPLICATION│  │ FOLLOWER_LOG_REPLICATION│
        │        ↓              │   │        ↓              │
        │ LEADER_REPLAY         │   │ FOLLOWER_REPLAY       │
        │        ↓              │   │        ↓              │
        │ LEADER_INIT           │   │ 日志一致？             │
        │        ↓              │   │   ├─ 是 → FOLLOWER_LOG_INIT │
        │ LEADER_READY          │   │   └─ 否 → FOLLOWER_CATCHUP_INIT │
        └──────────────────────┘   │                       │
                                    │ FOLLOWER_CATCHUP_AWAIT│
                                    │        ↓              │
                                    │ FOLLOWER_CATCHUP      │
                                    │        ↓              │
                                    │ FOLLOWER_LOG_INIT     │
                                    │        ↓              │
                                    │ FOLLOWER_LOG_AWAIT    │
                                    │        ↓              │
                                    │ FOLLOWER_READY        │
                                    └──────────────────────┘
```

### 3.2 详细流程：每个状态的职责和转换

```
┌────────────────────────────────────────────────────────────────────┐
│                     Election 状态机详细流程                         │
└────────────────────────────────────────────────────────────────────┘

════════════════════════════════════════════════════════════════════
阶段 1: 选举初始化
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ INIT - 初始化                                            │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 重置所有成员的投票状态                                │
    │ 2. 设置选举超时时间 (startupCanvassTimeoutNs)           │
    │ 3. 生成新的 candidateTermId                             │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> state = CANVASS (立即转换)                          │
    └─────────────────────────────────────────────────────────┘
                            │
                            ↓

════════════════════════════════════════════════════════════════════
阶段 2: 询问阶段（Canvass）
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ CANVASS - 询问                                           │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 向所有成员发送 CanvassPosition 消息：                 │
    │    "我的日志位置是 (logLeadershipTermId, logPosition)" │
    │                                                          │
    │ 2. 接收其他成员的 CanvassPosition 响应                  │
    │    - 记录每个成员的日志位置                              │
    │                                                          │
    │ 3. 等待超时 (startupCanvassTimeoutNs)                   │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> 超时后 → state = NOMINATE                           │
    └─────────────────────────────────────────────────────────┘
                            │
                            ↓

════════════════════════════════════════════════════════════════════
阶段 3: 提名阶段（Nominate）
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ NOMINATE - 提名                                          │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 比较所有成员的日志位置：                              │
    │    compareLog(logLeadershipTermId, logPosition)         │
    │                                                          │
    │ 2. 决定是否参选：                                        │
    │    - 如果自己的日志是最新的 → 参选                       │
    │    - 否则 → 跟随                                         │
    │                                                          │
    │ 转换条件：                                               │
    │ ├─> 日志最新 → state = CANDIDATE_BALLOT (参选)          │
    │ └─> 日志落后 → state = FOLLOWER_BALLOT (跟随)           │
    └─────────────────────────────────────────────────────────┘
                            │
            ┌───────────────┴───────────────┐
            │                               │
            ↓                               ↓

════════════════════════════════════════════════════════════════════
阶段 4a: 候选人路径
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ CANDIDATE_BALLOT - 候选人投票                            │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 向所有成员发送 RequestVote 消息：                     │
    │    "我是候选人，请投票给我"                              │
    │    - candidateTermId                                    │
    │    - candidateId                                        │
    │    - logLeadershipTermId                                │
    │    - logPosition                                        │
    │                                                          │
    │ 2. 接收其他成员的 Vote 响应                              │
    │    - 记录每个成员的投票（同意/拒绝）                      │
    │                                                          │
    │ 3. 判断是否获得多数票（quorum）：                        │
    │    quorum = (clusterMembers.length / 2) + 1            │
    │                                                          │
    │ 转换条件：                                               │
    │ ├─> 获得多数票 → state = LEADER_LOG_REPLICATION         │
    │ ├─> 超时未获得多数票 → state = CANVASS (重新选举)        │
    │ └─> 收到更高 term 的 NewLeadershipTerm → state = FOLLOWER_BALLOT │
    └─────────────────────────────────────────────────────────┘
            │ 获得多数票
            ↓

════════════════════════════════════════════════════════════════════
阶段 5a: Leader 日志复制
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ LEADER_LOG_REPLICATION - Leader 日志复制                 │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 创建日志 publication：                                │
    │    - channel: logChannel (UDP multicast)                │
    │    - streamId: logStreamId                              │
    │                                                          │
    │ 2. 开始 Archive 录制日志                                 │
    │    - archive.startRecording(...)                        │
    │                                                          │
    │ 3. 等待 Followers 订阅日志                               │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> 日志 publication 就绪 → state = LEADER_REPLAY        │
    └─────────────────────────────────────────────────────────┘
            │
            ↓

════════════════════════════════════════════════════════════════════
阶段 6a: Leader 日志重放
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ LEADER_REPLAY - Leader 日志重放                          │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 重放未提交的日志：                                    │
    │    - 从 commitPosition 开始                             │
    │    - 到 logPosition 结束                                │
    │                                                          │
    │ 2. 确保 Leader 状态与日志一致                            │
    │    - 调用 ClusteredService.onSessionMessage()           │
    │    - 恢复会话状态、定时器等                              │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> 重放完成 → state = LEADER_INIT                       │
    └─────────────────────────────────────────────────────────┘
            │
            ↓

════════════════════════════════════════════════════════════════════
阶段 7a: Leader 初始化
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ LEADER_INIT - Leader 初始化                              │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 创建 NewLeadershipTerm 事件：                         │
    │    - leadershipTermId (新的 term ID)                    │
    │    - leaderMemberId (本节点 ID)                         │
    │    - logPosition (当前日志位置)                          │
    │                                                          │
    │ 2. 写入日志（供 Followers 消费）                         │
    │                                                          │
    │ 3. 向所有成员发送 NewLeadershipTerm 消息                 │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> NewLeadershipTerm 写入完成 → state = LEADER_READY   │
    └─────────────────────────────────────────────────────────┘
            │
            ↓

════════════════════════════════════════════════════════════════════
阶段 8a: Leader 就绪
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ LEADER_READY - Leader 就绪                               │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 等待 quorum 的 Followers 追赶到 leadershipTermId 位置│
    │                                                          │
    │ 2. 持续发送心跳消息（CommitPosition）                    │
    │                                                          │
    │ 3. 检查所有 Followers 的状态                             │
    │                                                          │
    │ 完成条件：                                               │
    │ └─> quorum 的 Followers 已就绪 → 选举结束               │
    │     - election = null                                   │
    │     - ConsensusModuleAgent → state = ACTIVE             │
    └─────────────────────────────────────────────────────────┘


════════════════════════════════════════════════════════════════════
阶段 4b: 跟随者路径
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ FOLLOWER_BALLOT - 跟随者投票                             │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 等待接收 NewLeadershipTerm 消息（来自 Leader）        │
    │                                                          │
    │ 2. 如果收到更高 candidateTermId 的 RequestVote：         │
    │    - 投票给该候选人                                      │
    │    - 更新 candidateTermId                               │
    │                                                          │
    │ 转换条件：                                               │
    │ ├─> 收到 NewLeadershipTerm → state = FOLLOWER_LOG_REPLICATION │
    │ └─> 超时未收到 → state = CANVASS (重新选举)              │
    └─────────────────────────────────────────────────────────┘
            │ 收到 NewLeadershipTerm
            ↓

════════════════════════════════════════════════════════════════════
阶段 5b: Follower 日志复制
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ FOLLOWER_LOG_REPLICATION - Follower 日志复制             │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 订阅 Leader 的日志 publication：                      │
    │    - channel: Leader 的 logChannel                      │
    │    - streamId: logStreamId                              │
    │                                                          │
    │ 2. 等待订阅连接成功                                      │
    │                                                          │
    │ 转换条件：                                               │
    │ └─> 订阅成功 → state = FOLLOWER_REPLAY                   │
    └─────────────────────────────────────────────────────────┘
            │
            ↓

════════════════════════════════════════════════════════════════════
阶段 6b: Follower 日志重放
════════════════════════════════════════════════════════════════════

    ┌─────────────────────────────────────────────────────────┐
    │ FOLLOWER_REPLAY - Follower 日志重放                      │
    ├─────────────────────────────────────────────────────────┤
    │ 执行内容：                                               │
    │ 1. 从 Archive 重放日志：                                 │
    │    - 从 commitPosition 开始                             │
    │    - 到 Leader 通知的位置结束                            │
    │                                                          │
    │ 2. 判断日志差距：                                        │
    │    - 如果 logPosition == Leader 的 logPosition：         │
    │      → 日志一致，直接进入 FOLLOWER_LOG_INIT             │
    │    - 如果 logPosition < Leader 的 logPosition：          │
    │      → 日志落后，需要追赶，进入 FOLLOWER_CATCHUP_INIT   │
    │                                                          │
    │ 转换条件：                                               │
    │ ├─> 日志一致 → state = FOLLOWER_LOG_INIT                 │
    │ └─> 日志落后 → state = FOLLOWER_CATCHUP_INIT             │
    └─────────────────────────────────────────────────────────┘
            │
            ├─── 日志落后 ───┐
            │                │
            │                ↓
            │   ┌─────────────────────────────────────────────┐
            │   │ FOLLOWER_CATCHUP_INIT - 追赶初始化           │
            │   ├─────────────────────────────────────────────┤
            │   │ 执行内容：                                   │
            │   │ 1. 向 Leader 发送 CatchupRequest：          │
            │   │    - 请求从 replayPosition 开始的日志       │
            │   │                                              │
            │   │ 2. Leader 创建 catchup publication          │
            │   │                                              │
            │   │ 转换条件：                                   │
            │   │ └─> 请求发送成功 → FOLLOWER_CATCHUP_AWAIT   │
            │   └─────────────────────────────────────────────┘
            │                │
            │                ↓
            │   ┌─────────────────────────────────────────────┐
            │   │ FOLLOWER_CATCHUP_AWAIT - 等待追赶            │
            │   ├─────────────────────────────────────────────┤
            │   │ 执行内容：                                   │
            │   │ 1. 等待 Leader 返回 CatchupPosition 消息    │
            │   │                                              │
            │   │ 2. 订阅 catchup publication                 │
            │   │                                              │
            │   │ 转换条件：                                   │
            │   │ └─> 订阅成功 → FOLLOWER_CATCHUP             │
            │   └─────────────────────────────────────────────┘
            │                │
            │                ↓
            │   ┌─────────────────────────────────────────────┐
            │   │ FOLLOWER_CATCHUP - 追赶中                    │
            │   ├─────────────────────────────────────────────┤
            │   │ 执行内容：                                   │
            │   │ 1. 从 catchup publication 接收日志          │
            │   │                                              │
            │   │ 2. 写入本地 Archive                         │
            │   │                                              │
            │   │ 3. 重放日志到 replayPosition                │
            │   │                                              │
            │   │ 转换条件：                                   │
            │   │ └─> 追赶完成 → FOLLOWER_LOG_INIT            │
            │   └─────────────────────────────────────────────┘
            │                │
            │                ↓
            │   ┌─────────────────────────────────────────────┐
            ├───┤ FOLLOWER_LOG_INIT - 日志初始化               │
            │   ├─────────────────────────────────────────────┤
            │   │ 执行内容：                                   │
            │   │ 1. 从 Archive 加载最新的日志录制             │
            │   │                                              │
            │   │ 2. 准备订阅 Leader 的实时日志                │
            │   │                                              │
            │   │ 转换条件：                                   │
            │   │ └─> 初始化完成 → FOLLOWER_LOG_AWAIT         │
            │   └─────────────────────────────────────────────┘
            │                │
            │                ↓
            │   ┌─────────────────────────────────────────────┐
            │   │ FOLLOWER_LOG_AWAIT - 等待日志                │
            │   ├─────────────────────────────────────────────┤
            │   │ 执行内容：                                   │
            │   │ 1. 等待日志订阅连接到 Leader                 │
            │   │                                              │
            │   │ 2. 等待 Image 可用                          │
            │   │                                              │
            │   │ 转换条件：                                   │
            │   │ └─> Image 可用 → FOLLOWER_READY             │
            │   └─────────────────────────────────────────────┘
            │                │
            │                ↓
            └────────────────>
                        ┌─────────────────────────────────────┐
                        │ FOLLOWER_READY - Follower 就绪       │
                        ├─────────────────────────────────────┤
                        │ 执行内容：                           │
                        │ 1. 向 Leader 发送 AppendPosition：   │
                        │    - 确认已追赶到 leadershipTermId  │
                        │                                      │
                        │ 2. 选举结束：                        │
                        │    - election = null                │
                        │    - ConsensusModuleAgent → ACTIVE  │
                        └─────────────────────────────────────┘
```

---

## 4. 时间线示例：3节点集群选举过程

```
┌────────────────────────────────────────────────────────────────────┐
│                  3节点集群选举时间线（详细版）                      │
└────────────────────────────────────────────────────────────────────┘

假设：
- 节点 0、1、2 同时启动
- 节点 0 的日志最新：(termId=5, logPos=10000)
- 节点 1 的日志：(termId=5, logPos=9000)
- 节点 2 的日志：(termId=4, logPos=8000)

时间轴：
t0 ─────────────────────────────────────────────────────────────────→

════════════════════════════════════════════════════════════════════
t0: 所有节点启动，进入 INIT 状态
════════════════════════════════════════════════════════════════════

Node 0: state = INIT
    └─> 初始化选举状态
    └─> candidateTermId = 6 (旧值 5 + 1)
    └─> state = CANVASS

Node 1: state = INIT
    └─> candidateTermId = 6
    └─> state = CANVASS

Node 2: state = INIT
    └─> candidateTermId = 5
    └─> state = CANVASS

════════════════════════════════════════════════════════════════════
t1 (50ms): CANVASS 阶段 - 收集日志位置
════════════════════════════════════════════════════════════════════

Node 0: state = CANVASS
    ├─> 发送 CanvassPosition(termId=5, logPos=10000) → Node 1, Node 2
    ├─> 收到 CanvassPosition(termId=5, logPos=9000) from Node 1
    └─> 收到 CanvassPosition(termId=4, logPos=8000) from Node 2

Node 1: state = CANVASS
    ├─> 发送 CanvassPosition(termId=5, logPos=9000) → Node 0, Node 2
    ├─> 收到 CanvassPosition(termId=5, logPos=10000) from Node 0  ← 发现 Node 0 更新
    └─> 收到 CanvassPosition(termId=4, logPos=8000) from Node 2

Node 2: state = CANVASS
    ├─> 发送 CanvassPosition(termId=4, logPos=8000) → Node 0, Node 1
    ├─> 收到 CanvassPosition(termId=5, logPos=10000) from Node 0  ← 发现 Node 0 更新
    └─> 收到 CanvassPosition(termId=5, logPos=9000) from Node 1   ← 发现 Node 1 更新

════════════════════════════════════════════════════════════════════
t2 (100ms): NOMINATE 阶段 - 决定是否参选
════════════════════════════════════════════════════════════════════

Node 0: state = NOMINATE
    ├─> 比较日志位置：
    │   - 自己：(termId=5, logPos=10000)
    │   - Node 1：(termId=5, logPos=9000)
    │   - Node 2：(termId=4, logPos=8000)
    ├─> 判断：自己的日志最新 → 参选
    └─> state = CANDIDATE_BALLOT

Node 1: state = NOMINATE
    ├─> 比较日志位置：
    │   - Node 0：(termId=5, logPos=10000) ← 更新
    │   - 自己：(termId=5, logPos=9000)
    │   - Node 2：(termId=4, logPos=8000)
    ├─> 判断：Node 0 的日志更新 → 跟随
    └─> state = FOLLOWER_BALLOT

Node 2: state = NOMINATE
    ├─> 比较日志位置：
    │   - Node 0：(termId=5, logPos=10000) ← 更新
    │   - Node 1：(termId=5, logPos=9000) ← 更新
    │   - 自己：(termId=4, logPos=8000)
    ├─> 判断：Node 0 和 Node 1 都更新 → 跟随
    └─> state = FOLLOWER_BALLOT

════════════════════════════════════════════════════════════════════
t3 (150ms): BALLOT 阶段 - 投票
════════════════════════════════════════════════════════════════════

Node 0: state = CANDIDATE_BALLOT
    ├─> 发送 RequestVote(candidateTermId=6, candidateId=0) → Node 1, Node 2
    ├─> 收到 Vote(candidateTermId=6, granted=true) from Node 1   ← 同意票
    ├─> 收到 Vote(candidateTermId=6, granted=true) from Node 2   ← 同意票
    ├─> 票数统计：3 票 (包括自己) ≥ quorum (2 票)
    └─> state = LEADER_LOG_REPLICATION

Node 1: state = FOLLOWER_BALLOT
    ├─> 收到 RequestVote(candidateTermId=6, candidateId=0) from Node 0
    ├─> 判断：Node 0 的日志更新 → 同意投票
    ├─> 发送 Vote(candidateTermId=6, granted=true) → Node 0
    └─> 等待 NewLeadershipTerm 消息...

Node 2: state = FOLLOWER_BALLOT
    ├─> 收到 RequestVote(candidateTermId=6, candidateId=0) from Node 0
    ├─> 判断：Node 0 的日志更新 → 同意投票
    ├─> 发送 Vote(candidateTermId=6, granted=true) → Node 0
    └─> 等待 NewLeadershipTerm 消息...

════════════════════════════════════════════════════════════════════
t4 (200ms): Leader 日志复制准备
════════════════════════════════════════════════════════════════════

Node 0: state = LEADER_LOG_REPLICATION
    ├─> 创建日志 publication (UDP multicast)
    │   - channel: aeron:udp?endpoint=224.0.1.1:20002
    │   - streamId: 103
    ├─> 开始 Archive 录制：archive.startRecording(...)
    ├─> 等待 Followers 订阅...
    └─> state = LEADER_REPLAY

Node 1: state = FOLLOWER_BALLOT
    └─> 继续等待 NewLeadershipTerm...

Node 2: state = FOLLOWER_BALLOT
    └─> 继续等待 NewLeadershipTerm...

════════════════════════════════════════════════════════════════════
t5 (250ms): Leader 重放日志
════════════════════════════════════════════════════════════════════

Node 0: state = LEADER_REPLAY
    ├─> 从 Archive 重放未提交的日志：
    │   - 从 commitPosition=9500 开始
    │   - 到 logPosition=10000 结束
    ├─> 调用 ClusteredService.onSessionMessage() 恢复状态
    ├─> 重放完成
    └─> state = LEADER_INIT

Node 1, Node 2: 继续等待...

════════════════════════════════════════════════════════════════════
t6 (300ms): Leader 初始化 - 通知 Followers
════════════════════════════════════════════════════════════════════

Node 0: state = LEADER_INIT
    ├─> 创建 NewLeadershipTerm 事件：
    │   - leadershipTermId = 6
    │   - leaderMemberId = 0
    │   - logPosition = 10000
    ├─> 写入日志（供 Followers 消费）
    ├─> 发送 NewLeadershipTerm 消息 → Node 1, Node 2
    └─> state = LEADER_READY

Node 1: state = FOLLOWER_BALLOT
    ├─> 收到 NewLeadershipTerm(leadershipTermId=6, leaderId=0)
    ├─> 判断：Node 0 是新 Leader
    └─> state = FOLLOWER_LOG_REPLICATION

Node 2: state = FOLLOWER_BALLOT
    ├─> 收到 NewLeadershipTerm(leadershipTermId=6, leaderId=0)
    └─> state = FOLLOWER_LOG_REPLICATION

════════════════════════════════════════════════════════════════════
t7 (350ms): Followers 订阅日志
════════════════════════════════════════════════════════════════════

Node 0: state = LEADER_READY
    └─> 等待 Followers 追赶到 leadershipTermId 位置...

Node 1: state = FOLLOWER_LOG_REPLICATION
    ├─> 订阅 Leader 的日志 publication：
    │   - channel: aeron:udp?endpoint=224.0.1.1:20002
    │   - streamId: 103
    ├─> 订阅成功
    └─> state = FOLLOWER_REPLAY

Node 2: state = FOLLOWER_LOG_REPLICATION
    ├─> 订阅成功
    └─> state = FOLLOWER_REPLAY

════════════════════════════════════════════════════════════════════
t8 (400ms): Followers 重放日志
════════════════════════════════════════════════════════════════════

Node 1: state = FOLLOWER_REPLAY
    ├─> 从 Archive 重放日志：
    │   - 从 commitPosition=9000 开始
    │   - 到 logPosition=9000 结束（本地已有）
    ├─> 判断日志差距：
    │   - 自己：logPosition=9000
    │   - Leader：logPosition=10000
    │   - 差距：1000 bytes
    ├─> 日志落后 → 需要追赶
    └─> state = FOLLOWER_CATCHUP_INIT

Node 2: state = FOLLOWER_REPLAY
    ├─> 从 Archive 重放日志（termId=4 → termId=5）
    ├─> 判断日志差距：
    │   - 自己：logPosition=8000
    │   - Leader：logPosition=10000
    │   - 差距：2000 bytes
    ├─> 日志落后 → 需要追赶
    └─> state = FOLLOWER_CATCHUP_INIT

════════════════════════════════════════════════════════════════════
t9 (450ms): Followers 请求追赶
════════════════════════════════════════════════════════════════════

Node 1: state = FOLLOWER_CATCHUP_INIT
    ├─> 发送 CatchupRequest(fromPosition=9000) → Node 0
    └─> state = FOLLOWER_CATCHUP_AWAIT

Node 2: state = FOLLOWER_CATCHUP_INIT
    ├─> 发送 CatchupRequest(fromPosition=8000) → Node 0
    └─> state = FOLLOWER_CATCHUP_AWAIT

Node 0: state = LEADER_READY
    ├─> 收到 CatchupRequest from Node 1
    │   └─> 创建 catchup publication for Node 1
    ├─> 收到 CatchupRequest from Node 2
    │   └─> 创建 catchup publication for Node 2
    └─> 发送 CatchupPosition 消息 → Node 1, Node 2

════════════════════════════════════════════════════════════════════
t10 (500ms): Followers 追赶日志
════════════════════════════════════════════════════════════════════

Node 1: state = FOLLOWER_CATCHUP_AWAIT
    ├─> 收到 CatchupPosition(recordingId=456)
    ├─> 订阅 catchup publication
    └─> state = FOLLOWER_CATCHUP
        ├─> 从 catchup publication 接收日志：
        │   - 接收 1000 bytes 日志
        │   - 写入本地 Archive
        ├─> 追赶完成：logPosition=10000
        └─> state = FOLLOWER_LOG_INIT

Node 2: state = FOLLOWER_CATCHUP
    ├─> 接收 2000 bytes 日志
    ├─> 追赶完成：logPosition=10000
    └─> state = FOLLOWER_LOG_INIT

════════════════════════════════════════════════════════════════════
t11 (550ms): Followers 准备接收实时日志
════════════════════════════════════════════════════════════════════

Node 1: state = FOLLOWER_LOG_INIT
    ├─> 从 Archive 加载最新日志录制
    ├─> 准备订阅 Leader 的实时日志
    └─> state = FOLLOWER_LOG_AWAIT
        ├─> 等待 Image 可用
        └─> state = FOLLOWER_READY

Node 2: state = FOLLOWER_LOG_AWAIT
    └─> state = FOLLOWER_READY

════════════════════════════════════════════════════════════════════
t12 (600ms): Followers 就绪，选举结束
════════════════════════════════════════════════════════════════════

Node 1: state = FOLLOWER_READY
    ├─> 发送 AppendPosition(logPosition=10000, leadershipTermId=6) → Node 0
    └─> 选举结束：election = null

Node 2: state = FOLLOWER_READY
    ├─> 发送 AppendPosition(logPosition=10000, leadershipTermId=6) → Node 0
    └─> 选举结束：election = null

Node 0: state = LEADER_READY
    ├─> 收到 AppendPosition from Node 1 (已就绪)
    ├─> 收到 AppendPosition from Node 2 (已就绪)
    ├─> 判断：quorum (2/3) 的 Followers 已就绪
    └─> 选举结束：election = null

════════════════════════════════════════════════════════════════════
t13 (650ms): 所有节点进入 ACTIVE 状态
════════════════════════════════════════════════════════════════════

Node 0: ConsensusModuleAgent.state = ACTIVE, role = LEADER
    └─> 开始接收客户端请求

Node 1: ConsensusModuleAgent.state = ACTIVE, role = FOLLOWER
    └─> 开始消费日志

Node 2: ConsensusModuleAgent.state = ACTIVE, role = FOLLOWER
    └─> 开始消费日志

════════════════════════════════════════════════════════════════════
选举总耗时：~600ms
════════════════════════════════════════════════════════════════════
```

---

## 5. 关键决策点

### 5.1 谁应该成为 Leader？

**判断依据**：`compareLog(logLeadershipTermId, logPosition)`

```java
// Election.java
public static int compareLog(
    long leadershipTermIdA, long logPositionA,
    long leadershipTermIdB, long logPositionB)
{
    // 1. 先比较 leadershipTermId（term ID 越大越新）
    if (leadershipTermIdA != leadershipTermIdB)
    {
        return Long.compare(leadershipTermIdA, leadershipTermIdB);
    }

    // 2. term ID 相同，比较 logPosition（日志位置越大越新）
    return Long.compare(logPositionA, logPositionB);
}
```

**示例**：
```
节点 A: (termId=5, logPos=10000)
节点 B: (termId=5, logPos=9000)
节点 C: (termId=4, logPos=8000)

排序结果：A > B > C
→ 节点 A 的日志最新，应该成为 Leader
```

### 5.2 何时需要追赶日志？

**判断条件**：
```java
// FOLLOWER_REPLAY 状态中
if (logPosition < leaderLogPosition)
{
    // 日志落后，需要追赶
    state = FOLLOWER_CATCHUP_INIT;
}
else
{
    // 日志一致，直接进入日志初始化
    state = FOLLOWER_LOG_INIT;
}
```

### 5.3 何时选举结束？

**Leader 视角**：
```java
// LEADER_READY 状态中
int readyFollowers = 0;
for (ClusterMember member : clusterMembers)
{
    if (member.logPosition() >= leadershipTermPosition)
    {
        readyFollowers++;
    }
}

if (readyFollowers >= quorumThreshold)
{
    // quorum 的 Followers 已就绪，选举结束
    election = null;
    state = ACTIVE;
}
```

**Follower 视角**：
```java
// FOLLOWER_READY 状态中
// 发送 AppendPosition 确认已就绪后，立即结束选举
sendAppendPosition(leadershipTermPosition);
election = null;
state = ACTIVE;
```

---

## 6. 性能指标

### 6.1 典型选举时长

| 场景 | 时长 | 说明 |
|------|------|------|
| **无日志差距** | 200-300ms | 所有节点日志一致，无需追赶 |
| **小日志差距** | 300-500ms | 日志差距 < 10MB |
| **大日志差距** | 500ms - 5s | 日志差距 > 100MB，取决于网络带宽 |
| **网络分区恢复** | 5-10s | 节点重新加入集群 |

### 6.2 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| **startupCanvassTimeoutNs** | 5s | Canvass 阶段超时时间 |
| **electionTimeoutNs** | 1s | 选举超时时间 |
| **leaderHeartbeatTimeoutNs** | 10s | Leader 心跳超时（触发重新选举） |

---

## 7. 总结

### 7.1 核心要点

1. **election.doWork() 是状态机核心**：根据 18 种状态执行对应逻辑
2. **选举流程**：INIT → CANVASS → NOMINATE → BALLOT → READY
3. **Leader 路径**：CANDIDATE_BALLOT → LEADER_LOG_REPLICATION → LEADER_REPLAY → LEADER_INIT → LEADER_READY
4. **Follower 路径**：FOLLOWER_BALLOT → FOLLOWER_LOG_REPLICATION → FOLLOWER_REPLAY → (追赶) → FOLLOWER_READY
5. **选举结束条件**：quorum 的节点就绪，所有节点进入 ACTIVE 状态

### 7.2 设计理念

- **确定性**：所有节点按相同逻辑选出相同的 Leader
- **容错性**：支持网络分区、节点故障、日志落后等场景
- **高效性**：无日志差距时 200ms 内完成选举
- **安全性**：保证日志最新的节点成为 Leader

### 7.3 与 Raft 的关系

Aeron Cluster 的选举实现基于 Raft 论文，但有以下优化：
- **Canvass 阶段**：预先收集日志位置，避免多次投票失败
- **追赶机制**：Leader 主动推送日志给落后的 Followers
- **Zero-Copy**：使用 Aeron 的 zero-copy 机制，提高日志复制效率

---

## 参考资料

- **源码位置**：`Election.java:370-449`
- **调用位置**：`ConsensusModuleAgent.java:367`
- **相关文档**：
  - [Raft 论文](https://raft.github.io/raft.pdf)
  - [Aeron Cluster 文档](https://github.com/real-logic/aeron/wiki/Cluster-Tutorial)

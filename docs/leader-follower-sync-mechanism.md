# Leader 如何判断 Followers 已同步完成？源码完整分析

## 目录
1. [核心机制概述](#1-核心机制概述)
2. [Follower 发送 appendPosition](#2-follower-发送-appendposition)
3. [Leader 接收 appendPosition](#3-leader-接收-appendposition)
4. [Leader 计算 quorumPosition](#4-leader-计算-quorumposition)
5. [Leader 更新 commitPosition](#5-leader-更新-commitposition)
6. [完整流程图](#6-完整流程图)
7. [源码调用链](#7-源码调用链)

---

## 1. 核心机制概述

### 1.1 核心问题

**问题**：Leader 写入日志后，如何知道 Followers 已经同步完成？

**答案**：通过 **appendPosition 心跳机制** + **quorum 计算**

### 1.2 关键概念

| 概念 | 说明 | 作用 |
|------|------|------|
| **appendPosition** | Follower 已持久化的日志位置 | Follower 向 Leader 报告进度 |
| **quorumPosition** | 大多数节点已持久化的位置 | Leader 计算可提交的位置 |
| **commitPosition** | 已提交的日志位置（可安全消费） | Service 只消费到此位置的消息 |
| **quorum** | 大多数节点（N/2 + 1） | 3 节点 → 2，5 节点 → 3，7 节点 → 4 |

### 1.3 核心流程

```
1. Leader 写入日志
   └─> logPublisher.appendMessage() → UDP multicast

2. Follower 接收日志
   └─> logAdapter.poll() → Archive 录制到磁盘

3. Follower 发送 appendPosition（心跳）
   └─> consensusPublisher.appendPosition() → 告诉 Leader 已同步到哪里

4. Leader 接收 appendPosition
   └─> ConsensusAdapter.poll() → onAppendPosition() → 更新 Follower 位置

5. Leader 计算 quorumPosition
   └─> ClusterMember.quorumPosition() → 找到大多数节点的最小位置

6. Leader 更新 commitPosition
   └─> commitPosition.proposeMaxRelease(quorumPosition) → 告诉 Service 可以消费

7. Service 消费已提交的日志
   └─> logAdapter.poll(commitPosition.get()) → 只消费到 commitPosition
```

---

## 2. Follower 发送 appendPosition

### 2.1 Follower 何时发送 appendPosition？

Follower 在两种情况下发送 appendPosition：

| 触发条件 | 说明 | 源码位置 |
|---------|------|----------|
| **位置更新** | appendPosition 增加时 | `ConsensusModuleAgent.updateFollowerPosition()` |
| **心跳超时** | 超过心跳间隔（默认 200ms）未发送 | 同上 |

### 2.2 Follower 发送 appendPosition 的源码

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 2659-2684

/**
 * Follower 更新并发送 appendPosition 给 Leader。
 * <p>
 * 这个方法在 Follower 的 doWork() 循环中定期调用，负责：
 * <ul>
 *   <li>检查 appendPosition 是否更新（Archive 录制进度）</li>
 *   <li>如果位置更新或心跳超时，向 Leader 发送 appendPosition</li>
 * </ul>
 */
private int updateFollowerPosition(
    final ExclusivePublication publication,  // ← Leader 的 publication（单播）
    final long nowNs,
    final long leadershipTermId,
    final long appendPosition,  // ← 当前 Archive 录制位置
    final short flags)
{
    // 取 appendPosition 和 lastAppendPosition 的较大值
    final long position = max(appendPosition, lastAppendPosition);

    // 检查是否需要发送：
    // 1. 位置更新（position > lastAppendPosition）
    // 2. 心跳超时（nowNs - timeOfLastAppendPositionSendNs > leaderHeartbeatIntervalNs）
    if (position > lastAppendPosition ||
        nowNs >= (timeOfLastAppendPositionSendNs + leaderHeartbeatIntervalNs))
    {
        // 调用 ConsensusPublisher 发送 appendPosition
        if (consensusPublisher.appendPosition(
            publication,            // ← 发送给 Leader
            leadershipTermId,       // ← 当前 Term ID
            position,               // ← 已持久化的位置
            memberId,               // ← Follower ID
            flags))                 // ← 标志（如 CATCHUP）
        {
            if (position > lastAppendPosition)
            {
                lastAppendPosition = position;
                timeOfLastAppendPositionUpdateNs = nowNs;
            }
            timeOfLastAppendPositionSendNs = nowNs;  // ← 记录发送时间

            return 1;  // ← 返回工作量
        }
    }

    return 0;
}
```

### 2.3 Follower 何时调用 updateFollowerPosition()？

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 2425-2456

/**
 * Follower 的主工作循环（followerWork）。
 */
private int followerWork(final long nowNs)
{
    int workCount = 0;

    if (Cluster.Role.FOLLOWER == role)
    {
        if (ConsensusModule.State.ACTIVE == state || ConsensusModule.State.SUSPENDED == state)
        {
            if (NULL_VALUE != terminationPosition && logAdapter.position() >= terminationPosition)
            {
                // Follower 到达终止位置
                doTermination(logAdapter.position());
            }
            else
            {
                // 步骤 1：消费日志（从 Leader 接收的消息）
                final long limit = null != appendPosition ?
                    appendPosition.get() :  // ← Archive 录制进度
                    logRecordingStopPosition;

                final int count = logAdapter.poll(min(notifiedCommitPosition, limit));

                if (0 == count && logAdapter.isImageClosed())
                {
                    // 日志断开，触发选举
                    final boolean isEos = logAdapter.isLogEndOfStream();
                    enterElection(isEos, "log disconnected from leader: eos=" + isEos);
                    return 1;
                }

                // 步骤 2：更新本地 commitPosition
                commitPosition.proposeMaxRelease(logAdapter.position());

                workCount += ingressAdapter.poll();
                workCount += count;
            }
        }

        // 步骤 3：向 Leader 发送 appendPosition（关键！）
        workCount += updateFollowerPosition(nowNs);  // ← 调用发送方法
    }

    // 步骤 4：接收 Leader 的命令（如 commitPosition）
    workCount += consensusModuleAdapter.poll();
    workCount += pollStandbySnapshotReplication(nowNs);

    return workCount;
}

/**
 * 调用 updateFollowerPosition() 发送 appendPosition。
 */
private int updateFollowerPosition(final long nowNs)
{
    if (null == appendPosition)
    {
        return 0;
    }

    final long recordedPosition = appendPosition.get();  // ← 从 Archive 读取录制位置

    return updateFollowerPosition(
        leaderMember.publication(),  // ← Leader 的 publication
        nowNs,
        leadershipTermId,
        recordedPosition,
        APPEND_POSITION_FLAG_NONE);
}
```

### 2.4 appendPosition 从哪里来？

```java
// appendPosition 是一个 ReadableCounter，指向 Archive 的 RecordingPos counter

// 初始化 appendPosition（ConsensusModuleAgent.java）
private boolean findAppendPosition(final CountersReader counters)
{
    final int counterId = RecordingPos.findCounterIdByRecording(counters, logRecordingId);

    if (NULL_COUNTER_ID == counterId)
    {
        return false;
    }

    final long registrationId = counters.getCounterRegistrationId(counterId);
    if (0 == registrationId)
    {
        return false;
    }

    final long recordingId = RecordingPos.getRecordingId(counters, counterId);
    if (RecordingPos.NULL_RECORDING_ID == recordingId)
    {
        return false;
    }

    logRecordingId(recordingId);
    appendPosition = new ReadableCounter(counters, registrationId, counterId);
    // ↑ appendPosition 指向 Archive 的 RecordingPos counter
    // Archive 会实时更新这个 counter，表示已录制到磁盘的位置

    return true;
}
```

**关键点**：
- `appendPosition` 不是 Follower 自己设置的，而是 **Archive 自动更新的**
- Archive 每次将数据写入磁盘后，会更新 `RecordingPos` counter
- Follower 通过 `appendPosition.get()` 读取 Archive 的录制进度
- 然后将这个进度发送给 Leader

---

## 3. Leader 接收 appendPosition

### 3.1 Leader 如何接收 appendPosition？

Leader 通过 **ConsensusAdapter** 接收 Follower 发送的 appendPosition：

```java
// 文件：ConsensusAdapter.java
// 位置：lines 166-183

/**
 * ConsensusAdapter 从 consensusSubscription 接收消息。
 * <p>
 * consensusSubscription 订阅所有节点发送的控制消息：
 * - AppendPosition（Follower → Leader）
 * - CommitPosition（Leader → Follower）
 * - Vote（选举时的投票）
 * - 等等
 */
public int poll()
{
    return subscription.poll(fragmentHandler, fragmentLimit);
}

// fragmentHandler 处理接收到的消息
private void onFragment(DirectBuffer buffer, int offset, int length, Header header)
{
    messageHeaderDecoder.wrap(buffer, offset);

    final int schemaId = messageHeaderDecoder.schemaId();
    if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
    {
        throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID +
            ", actual=" + schemaId);
    }

    final int templateId = messageHeaderDecoder.templateId();
    switch (templateId)
    {
        // ... 其他消息类型

        case AppendPositionDecoder.TEMPLATE_ID:  // ← 接收 appendPosition
            appendPositionDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final short flagsDecodedValue = appendPositionDecoder.flags();
            final short flags = AppendPositionDecoder.flagsNullValue() == flagsDecodedValue ?
                ConsensusModuleAgent.APPEND_POSITION_FLAG_NONE : flagsDecodedValue;

            // 调用 ConsensusModuleAgent 处理 appendPosition
            consensusModuleAgent.onAppendPosition(
                appendPositionDecoder.leadershipTermId(),  // ← Term ID
                appendPositionDecoder.logPosition(),       // ← Follower 的位置
                appendPositionDecoder.followerMemberId(),  // ← Follower ID
                flags);                                    // ← 标志

            break;

        // ... 其他消息类型
    }
}
```

### 3.2 Leader 处理 appendPosition

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 1033-1053

/**
 * Leader 接收到 Follower 的 appendPosition。
 * <p>
 * 这个方法会更新 Follower 的 logPosition，用于后续计算 quorumPosition。
 */
void onAppendPosition(
    final long leadershipTermId,
    final long logPosition,       // ← Follower 已持久化的位置
    final int followerMemberId,   // ← Follower ID
    final short flags)
{
    logOnAppendPosition(memberId, leadershipTermId, logPosition, followerMemberId, flags);

    if (null != election)
    {
        // 选举期间，交给 Election 处理
        election.onAppendPosition(leadershipTermId, logPosition, followerMemberId, flags);
    }
    else if (leadershipTermId <= this.leadershipTermId && Cluster.Role.LEADER == role)
    {
        // Leader 状态，更新 Follower 位置
        final ClusterMember follower = clusterMemberByIdMap.get(followerMemberId);
        if (null != follower)
        {
            // 关键：更新 Follower 的 logPosition
            updateMemberLogPosition(follower, leadershipTermId, logPosition);

            // 检查 Follower 是否追上 Leader（用于 catchup 场景）
            trackCatchupCompletion(follower, leadershipTermId, flags);
        }
    }
}

/**
 * 更新 ClusterMember 的 logPosition。
 */
void updateMemberLogPosition(final ClusterMember member, final long leadershipTermId, final long logPosition)
{
    member
        .leadershipTermId(leadershipTermId)
        .logPosition(logPosition)  // ← 更新 Follower 的位置
        .timeOfLastAppendPositionNs(clusterClock.timeNanos());  // ← 记录接收时间
}
```

**关键点**：
- Leader 接收到 appendPosition 后，会更新 `ClusterMember.logPosition`
- 每个 Follower 都有一个 `ClusterMember` 对象，记录其 `logPosition`
- Leader 后续会使用这些 `logPosition` 计算 `quorumPosition`

---

## 4. Leader 计算 quorumPosition

### 4.1 什么是 quorumPosition？

**quorumPosition** = **大多数节点（quorum）已持久化的最小位置**

示例（3 节点集群）：
```
Leader:    logPosition = 1000
Follower1: logPosition = 950
Follower2: logPosition = 980

quorum = 3/2 + 1 = 2（需要 2 个节点）

排序：[1000, 980, 950]
quorumPosition = 980  ← 取第 2 大的位置（quorum threshold 位置）
```

### 4.2 quorumPosition 计算算法

```java
// 文件：ClusterMember.java
// 位置：lines 867-895

/**
 * 计算 quorum 位置：大多数节点已持久化的最小位置。
 * <p>
 * 算法：
 * 1. 收集所有活跃节点的 logPosition
 * 2. 按降序排序
 * 3. 取第 quorumThreshold 个位置
 *
 * @param members         集群成员数组
 * @param rankedPositions 临时数组（用于排序，避免 GC）
 * @param nowNs           当前时间
 * @param timeoutNs       超时时间（超时的节点不参与计算）
 * @return quorum 位置
 */
public static long quorumPosition(
    final ClusterMember[] members,
    final long[] rankedPositions,  // ← 大小 = quorumThreshold
    final long nowNs,
    final long timeoutNs)
{
    final int length = rankedPositions.length;  // ← quorumThreshold = N/2 + 1

    // 步骤 1：初始化 rankedPositions 为 0
    for (int i = 0; i < length; i++)
    {
        rankedPositions[i] = 0;
    }

    // 步骤 2：收集所有活跃节点的 logPosition
    for (final ClusterMember member : members)
    {
        if (member.isActive(nowNs, timeoutNs))  // ← 检查节点是否活跃
        {
            long newPosition = member.logPosition;  // ← 读取 Follower 的位置

            // 步骤 3：插入排序（降序）
            // rankedPositions 始终保持前 quorumThreshold 个最大值
            for (int i = 0; i < length; i++)
            {
                final long rankedPosition = rankedPositions[i];

                if (newPosition > rankedPosition)
                {
                    // 插入 newPosition，将 rankedPosition 向后移动
                    rankedPositions[i] = newPosition;
                    newPosition = rankedPosition;
                }
            }
        }
    }

    // 步骤 4：返回最后一个位置（第 quorumThreshold 大的位置）
    return rankedPositions[length - 1];
}

/**
 * 计算 quorum 阈值：大多数节点数量。
 *
 * @param memberCount 节点总数
 * @return quorum 阈值（N/2 + 1）
 */
public static int quorumThreshold(final int memberCount)
{
    return (memberCount >> 1) + 1;  // ← N/2 + 1
}
```

### 4.3 quorumPosition 计算示例

**示例 1：3 节点集群**
```
members = [Leader, Follower1, Follower2]
quorumThreshold = 3/2 + 1 = 2

Leader.logPosition    = 1000
Follower1.logPosition = 950
Follower2.logPosition = 980

步骤 1：初始化 rankedPositions = [0, 0]

步骤 2：处理 Leader（1000）
rankedPositions = [1000, 0]

步骤 3：处理 Follower1（950）
rankedPositions = [1000, 950]

步骤 4：处理 Follower2（980）
插入 980：[1000, 980]（950 被挤出）

步骤 5：返回 rankedPositions[1] = 980

结果：quorumPosition = 980
```

**示例 2：5 节点集群**
```
members = [Leader, F1, F2, F3, F4]
quorumThreshold = 5/2 + 1 = 3

Leader.logPosition = 1000
F1.logPosition     = 950
F2.logPosition     = 980
F3.logPosition     = 920
F4.logPosition     = 990

排序：[1000, 990, 980, 950, 920]
rankedPositions = [1000, 990, 980]

结果：quorumPosition = 980（第 3 大）
```

**示例 3：节点故障**
```
members = [Leader, F1, F2]
quorumThreshold = 3/2 + 1 = 2

Leader.logPosition = 1000
F1.logPosition     = 950
F2.logPosition     = NULL（故障，超时）

只有 Leader 和 F1 活跃：
rankedPositions = [1000, 950]

结果：quorumPosition = 950
```

---

## 5. Leader 更新 commitPosition

### 5.1 Leader 何时更新 commitPosition？

Leader 在 **leaderWork()** 主工作循环中定期更新 commitPosition：

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 2784-2837

/**
 * Leader 更新 commitPosition（关键方法）。
 * <p>
 * 执行流程：
 * 1. 读取 Leader 自己的 appendPosition（Archive 录制进度）
 * 2. 计算 quorumPosition（大多数节点的最小位置）
 * 3. 更新 commitPosition（取 min(leaderAppendPosition, quorumPosition)）
 * 4. 发送 commitPosition 给所有 Followers
 */
private int updateLeaderPosition(final long nowNs)
{
    if (null != appendPosition)
    {
        final long leaderAppendPosition = appendPosition.get();  // ← Leader 的 Archive 录制进度

        return updateLeaderPosition(
            nowNs,
            leaderAppendPosition,
            quorumPositionBoundedByLeaderLog(leaderAppendPosition, nowNs));  // ← 计算 quorumPosition
    }

    return 0;
}

/**
 * 计算 quorumPosition，但不超过 Leader 的 appendPosition。
 */
long quorumPositionBoundedByLeaderLog(final long leaderAppendPosition, final long nowNs)
{
    // 步骤 1：计算 quorumPosition
    final long quorumPosition =
        ClusterMember.quorumPosition(activeMembers, rankedPositions, nowNs, leaderHeartbeatTimeoutNs);
    // ↑ 调用 ClusterMember.quorumPosition() 计算大多数节点的位置

    // 步骤 2：限制 quorumPosition 不超过 leaderAppendPosition
    // 原因：
    // 1) 正常情况：quorumPosition <= leaderAppendPosition（Followers 跟随 Leader）
    // 2) 特殊情况：quorumPosition > leaderAppendPosition（Leader 的 Archive 较慢，Followers 先录制完成）
    return min(quorumPosition, leaderAppendPosition);
}

/**
 * 更新 Leader 的位置和 commitPosition。
 */
int updateLeaderPosition(final long nowNs, final long appendPosition, final long quorumPosition)
{
    // 步骤 1：更新 Leader 自己的 logPosition
    thisMember.logPosition(appendPosition).timeOfLastAppendPositionNs(nowNs);

    // 步骤 2：读取当前 commitPosition
    final long leaderCommitPosition = commitPosition.getPlain();

    // 步骤 3：检查是否需要更新 commitPosition
    // 条件 1：quorumPosition > leaderCommitPosition（大多数节点推进了）
    // 条件 2：心跳超时（定期发送 commitPosition 给 Followers）
    if (quorumPosition > leaderCommitPosition ||
        nowNs >= (timeOfLastLogUpdateNs + leaderHeartbeatIntervalNs))
    {
        // 检查 quorumPosition 是否回退（异常情况）
        if (quorumPosition < leaderCommitPosition && leaderCommitPosition > lastQuorumBacktrackCommitPosition)
        {
            lastQuorumBacktrackCommitPosition = leaderCommitPosition;
            ctx.countedErrorHandler().onError(new ClusterEvent("quorum position went backwards: " +
                "leaderCommitPosition=" + leaderCommitPosition + " quorumPosition=" + quorumPosition));
        }

        // 步骤 4：发送 commitPosition 给所有 Followers
        publishCommitPosition(quorumPosition, leadershipTermId);

        // 步骤 5：更新本地 commitPosition counter
        commitPosition.proposeMaxRelease(quorumPosition);  // ← 关键：更新 commitPosition
        timeOfLastLogUpdateNs = nowNs;

        // 步骤 6：清理未提交的条目（如 pending sessions）
        sweepUncommittedEntriesTo(quorumPosition);

        return 1;
    }

    return 0;
}
```

### 5.2 Leader 如何发送 commitPosition 给 Followers？

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 2839-2848

/**
 * Leader 发送 commitPosition 给所有 Followers。
 */
void publishCommitPosition(final long commitPosition, final long leadershipTermId)
{
    for (final ClusterMember member : activeMembers)
    {
        if (member != thisMember)  // ← 不发送给自己
        {
            // 调用 ConsensusPublisher 发送 commitPosition
            consensusPublisher.commitPosition(
                member.publication(),  // ← Follower 的 publication
                leadershipTermId,
                commitPosition,
                memberId);
        }
    }
}
```

### 5.3 Follower 如何接收 commitPosition？

```java
// 文件：ConsensusAdapter.java
// 位置：lines 185-196

case CommitPositionDecoder.TEMPLATE_ID:  // ← 接收 commitPosition
    commitPositionDecoder.wrap(
        buffer,
        offset + MessageHeaderDecoder.ENCODED_LENGTH,
        messageHeaderDecoder.blockLength(),
        messageHeaderDecoder.version());

    // 调用 ConsensusModuleAgent 处理 commitPosition
    consensusModuleAgent.onCommitPosition(
        commitPositionDecoder.leadershipTermId(),
        commitPositionDecoder.logPosition(),  // ← commitPosition
        commitPositionDecoder.leaderMemberId());

    break;
```

```java
// 文件：ConsensusModuleAgent.java
// 位置：lines 1000-1031

/**
 * Follower 接收到 Leader 的 commitPosition。
 */
void onCommitPosition(final long leadershipTermId, final long commitPosition, final int leaderId)
{
    if (Cluster.Role.FOLLOWER == role &&
        leadershipTermId == this.leadershipTermId &&
        leaderId == leaderMember.id())
    {
        // 更新 notifiedCommitPosition
        notifiedCommitPosition = max(notifiedCommitPosition, commitPosition);
        timeOfLastLogUpdateNs = nowNs;
    }
    else if (leadershipTermId > this.leadershipTermId)
    {
        // Term ID 更大，触发选举
        enterElection(false, "unexpected new leadership term event:" +
            " this.leadershipTermId=" + this.leadershipTermId +
            " newLeadershipTermId=" + leadershipTermId);
    }
}
```

---

## 6. 完整流程图

### 6.1 时间线流程

```
┌─────────────────────────────────────────────────────────────────────┐
│            Leader 判断 Followers 同步完成的完整流程                  │
└─────────────────────────────────────────────────────────────────────┘

Leader                          Follower1                    Follower2
  │                                  │                            │
  │ t0: 写入日志                      │                            │
  ├─> logPublisher.appendMessage()   │                            │
  │   └─> UDP multicast              │                            │
  │       └────────────────────────> ├─> t1: 接收日志              │
  │                                  │   └─> logAdapter.poll()    │
  │                                  │       └─> Archive 录制      │
  │                                  │           (磁盘写入)         │
  │                                  │                            ├─> t1: 接收日志
  │                                  │                            │   └─> Archive 录制
  │                                  │                            │
  │                                  │ t2: 发送 appendPosition      │
  │                                  ├─> consensusPublisher.      │
  │                                  │   appendPosition()         │
  │                                  │   └─> unicast to Leader    │
  │                                  │                            │ t2: 发送 appendPosition
  │                                  │                            ├─> unicast to Leader
  │ t3: 接收 appendPosition           │                            │
  ├─> ConsensusAdapter.poll()        │                            │
  │   └─> onAppendPosition(F1, 950)  │                            │
  │       └─> updateMemberLogPosition│                            │
  │           F1.logPosition = 950   │                            │
  │                                  │                            │
  │ t4: 接收 appendPosition           │                            │
  ├─> onAppendPosition(F2, 980)      │                            │
  │   └─> F2.logPosition = 980       │                            │
  │                                  │                            │
  │ t5: 计算 quorumPosition           │                            │
  ├─> updateLeaderPosition()         │                            │
  │   └─> quorumPositionBoundedByLeaderLog()                      │
  │       └─> ClusterMember.quorumPosition()                      │
  │           ├─ Leader: 1000        │                            │
  │           ├─ F1: 950             │                            │
  │           └─ F2: 980             │                            │
  │           排序：[1000, 980, 950] │                            │
  │           quorumPosition = 980   ← 第 2 大（quorum = 2）      │
  │                                  │                            │
  │ t6: 更新 commitPosition           │                            │
  ├─> commitPosition.proposeMaxRelease(980)                       │
  │   └─> commitPosition counter = 980                            │
  │                                  │                            │
  │ t7: 发送 commitPosition           │                            │
  ├─> publishCommitPosition(980)     │                            │
  │   ├─> F1.publication.offer()    ─────> t8: 接收 commitPosition │
  │   │                              │   └─> onCommitPosition(980)│
  │   │                              │       └─> notifiedCommitPosition = 980
  │   │                              │                            │
  │   └─> F2.publication.offer()    ─────────────────────────────────> t8: 接收 commitPosition
  │                                  │                            │   └─> notifiedCommitPosition = 980
  │                                  │                            │
  │ t9: Service 消费已提交的日志       │                            │
  ├─> Service logAdapter.poll(980)   │                            │
  │   └─> 消费 logPosition <= 980 的消息                           │
  │                                  │ t9: Service 消费            │
  │                                  ├─> logAdapter.poll(980)     │
  │                                  │                            │ t9: Service 消费
  │                                  │                            ├─> logAdapter.poll(980)
  │                                  │                            │


关键时间点：
────────────────────────────────────────────────────────────────────
t0 → t1: 网络延迟（< 1ms，UDP multicast）
t1 → t2: Archive 录制延迟（1-10ms，取决于磁盘）
t2 → t3: 网络延迟（< 1ms，unicast）
t3 → t5: Leader 收集所有 Follower 位置（< 1ms）
t5 → t6: 计算 quorumPosition（< 1μs，纯内存操作）
t6 → t7: 更新 commitPosition（< 1μs）
t7 → t8: 网络延迟（< 1ms）
t8 → t9: Service 消费日志（立即）

总延迟：约 2-20ms（主要取决于 Archive 录制速度）
```

### 6.2 节点间通信架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                      节点间通信架构                                   │
└─────────────────────────────────────────────────────────────────────┘

Leader
  ├─ logPublisher (ExclusivePublication)
  │  └─> UDP multicast (日志复制)
  │      ├─> channel: aeron:udp?endpoint=224.0.1.1:20002
  │      └─> streamId: 103
  │
  ├─ consensusPublisher (向 Followers 发送 commitPosition)
  │  ├─> F1.publication (unicast)
  │  │   └─> commitPosition(980, leadershipTermId, memberId)
  │  └─> F2.publication (unicast)
  │      └─> commitPosition(980, leadershipTermId, memberId)
  │
  └─ consensusAdapter (接收 Followers 的 appendPosition)
      └─> consensusSubscription
          ├─ 接收 F1: appendPosition(950, F1.id, flags)
          └─ 接收 F2: appendPosition(980, F2.id, flags)


Follower1
  ├─ logAdapter (订阅 Leader 的日志)
  │  └─> logSubscription
  │      └─> UDP multicast (接收日志消息)
  │
  ├─ consensusPublisher (向 Leader 发送 appendPosition)
  │  └─> Leader.publication (unicast)
  │      └─> appendPosition(950, F1.id, flags)
  │
  └─ consensusAdapter (接收 Leader 的 commitPosition)
      └─> consensusSubscription
          └─> 接收 Leader: commitPosition(980, leadershipTermId, leaderId)


Follower2
  ├─ logAdapter (订阅 Leader 的日志)
  │  └─> logSubscription
  │      └─> UDP multicast (接收日志消息)
  │
  ├─ consensusPublisher (向 Leader 发送 appendPosition)
  │  └─> Leader.publication (unicast)
  │      └─> appendPosition(980, F2.id, flags)
  │
  └─ consensusAdapter (接收 Leader 的 commitPosition)
      └─> consensusSubscription
          └─> 接收 Leader: commitPosition(980, leadershipTermId, leaderId)


通信类型：
─────────────────────────────────────────────────────────────────────
1. 日志复制（Leader → All）：
   - 协议：UDP multicast
   - 频率：每次写入日志
   - 作用：复制日志到所有节点

2. appendPosition（Follower → Leader）：
   - 协议：Unicast（单播）
   - 频率：位置更新 或 心跳超时（200ms）
   - 作用：向 Leader 报告录制进度

3. commitPosition（Leader → Followers）：
   - 协议：Unicast（单播）
   - 频率：quorumPosition 更新 或 心跳超时（200ms）
   - 作用：通知 Followers 可提交的位置
```

---

## 7. 源码调用链

### 7.1 Follower 发送 appendPosition 调用链

```
Follower ConsensusModuleAgent.doWork()
  └─> followerWork(nowNs)
      └─> updateFollowerPosition(nowNs)
          └─> updateFollowerPosition(publication, nowNs, leadershipTermId, recordedPosition, flags)
              ├─ 检查：position > lastAppendPosition || 心跳超时
              └─> consensusPublisher.appendPosition(publication, leadershipTermId, position, memberId, flags)
                  └─> publication.tryClaim(length, bufferClaim)
                      └─> appendPositionEncoder.wrap(...).leadershipTermId(...).logPosition(...).followerMemberId(...)
                          └─> bufferClaim.commit()  // ← 发送给 Leader
```

### 7.2 Leader 接收 appendPosition 调用链

```
Leader ConsensusModuleAgent.doWork()
  └─> consensusModuleAdapter.poll()
      └─> ConsensusAdapter.poll()
          └─> subscription.poll(fragmentHandler, fragmentLimit)
              └─> onFragment(buffer, offset, length, header)
                  └─> switch (templateId):
                      case AppendPositionDecoder.TEMPLATE_ID:
                          └─> consensusModuleAgent.onAppendPosition(leadershipTermId, logPosition, followerMemberId, flags)
                              └─> updateMemberLogPosition(follower, leadershipTermId, logPosition)
                                  └─> follower.logPosition(logPosition)  // ← 更新 Follower 位置
```

### 7.3 Leader 计算 quorumPosition 调用链

```
Leader ConsensusModuleAgent.doWork()
  └─> leaderWork(nowNs)
      └─> updateLeaderPosition(nowNs)
          ├─ final long leaderAppendPosition = appendPosition.get();  // ← Leader 的 Archive 录制进度
          └─> updateLeaderPosition(nowNs, leaderAppendPosition, quorumPositionBoundedByLeaderLog(...))
              └─> quorumPositionBoundedByLeaderLog(leaderAppendPosition, nowNs)
                  ├─> ClusterMember.quorumPosition(activeMembers, rankedPositions, nowNs, timeoutNs)
                  │   ├─ 收集所有活跃节点的 logPosition
                  │   ├─ 按降序排序（插入排序）
                  │   └─ 返回 rankedPositions[quorumThreshold - 1]  // ← 第 N/2+1 大的位置
                  └─> min(quorumPosition, leaderAppendPosition)  // ← 限制不超过 Leader
```

### 7.4 Leader 更新 commitPosition 调用链

```
Leader ConsensusModuleAgent.updateLeaderPosition(nowNs, appendPosition, quorumPosition)
  ├─ thisMember.logPosition(appendPosition)  // ← 更新 Leader 自己的位置
  ├─ final long leaderCommitPosition = commitPosition.getPlain();
  ├─ if (quorumPosition > leaderCommitPosition || 心跳超时):
  │   ├─> publishCommitPosition(quorumPosition, leadershipTermId)
  │   │   └─> for (member : activeMembers):
  │   │       └─> consensusPublisher.commitPosition(member.publication(), leadershipTermId, commitPosition, memberId)
  │   │           └─> publication.tryClaim(...) → commitPositionEncoder.wrap(...) → bufferClaim.commit()
  │   │
  │   ├─> commitPosition.proposeMaxRelease(quorumPosition)  // ← 关键：更新 commitPosition counter
  │   └─> sweepUncommittedEntriesTo(quorumPosition)  // ← 清理未提交的条目
  │
  └─ return 1;
```

### 7.5 Service 消费已提交日志调用链

```
ClusteredServiceAgent.doWork()
  └─> if (null != logAdapter.image()):
      └─> logAdapter.poll(commitPosition.get())  // ← 只消费到 commitPosition
          └─> image.poll(fragmentHandler, limit)
              └─> onFragment(buffer, offset, length, header)
                  └─> switch (templateId):
                      case SessionMessageHeaderDecoder.TEMPLATE_ID:
                          └─> onSessionMessage(logPosition, clusterSessionId, timestamp, buffer, offset, length, header)
                              └─> service.onSessionMessage(clientSession, timestamp, buffer, offset, length, header)
                                  └─> 业务逻辑处理消息
```

---

## 8. 常见问题 FAQ

### Q1: 为什么需要 appendPosition？直接用 logPosition 不行吗？

**答**：不行。原因：

| 位置类型 | 说明 | 位置 |
|---------|------|------|
| **logPosition** | Follower 从 UDP 接收到的位置 | 在内存中（logAdapter） |
| **appendPosition** | Follower Archive 录制到磁盘的位置 | 在磁盘上（Archive） |

**关键差异**：
- `logPosition` 表示接收到，但**未必已持久化**
- `appendPosition` 表示**已持久化到磁盘**，重启后可恢复

**为什么重要**：
- Leader 只能提交**已持久化**的数据（commitPosition 基于 appendPosition）
- 否则 Follower 重启后，数据丢失，导致不一致

### Q2: 如果 Follower 挂了，Leader 会等它吗？

**答**：不会！Leader 只需要 **quorum（大多数）节点**同步即可。

**示例**（5 节点集群）：
```
Leader:    appendPosition = 1000
Follower1: appendPosition = 950
Follower2: appendPosition = 980
Follower3: appendPosition = NULL（挂了）
Follower4: appendPosition = 990

quorum = 5/2 + 1 = 3（需要 3 个节点）

排序：[1000, 990, 980, 950]（F3 不参与）
quorumPosition = 980（第 3 大）

commitPosition = 980
```

**结论**：只要有 quorum 节点同步，Leader 就可以提交，不会被少数故障节点阻塞。

### Q3: appendPosition 发送频率是多少？

**答**：两种情况触发发送：

| 触发条件 | 频率 | 源码 |
|---------|------|------|
| **位置更新** | 每次 appendPosition 增加时 | `position > lastAppendPosition` |
| **心跳超时** | 默认 200ms | `nowNs - timeOfLastAppendPositionSendNs > leaderHeartbeatIntervalNs` |

**优化**：
- 位置频繁更新时（高吞吐量），每次更新都发送
- 位置不更新时（低吞吐量），每 200ms 发送心跳

### Q4: commitPosition 更新频率是多少？

**答**：与 appendPosition 类似：

| 触发条件 | 频率 | 源码 |
|---------|------|------|
| **quorumPosition 增加** | 每次 quorumPosition > commitPosition 时 | `quorumPosition > leaderCommitPosition` |
| **心跳超时** | 默认 200ms | `nowNs - timeOfLastLogUpdateNs > leaderHeartbeatIntervalNs` |

### Q5: 如果 Leader 的 Archive 很慢怎么办？

**答**：Leader 会限制 commitPosition 不超过自己的 appendPosition。

**源码**：
```java
long quorumPositionBoundedByLeaderLog(final long leaderAppendPosition, final long nowNs)
{
    final long quorumPosition =
        ClusterMember.quorumPosition(activeMembers, rankedPositions, nowNs, leaderHeartbeatTimeoutNs);

    // 限制不超过 Leader 的 appendPosition
    return min(quorumPosition, leaderAppendPosition);
}
```

**示例**：
```
Leader:    appendPosition = 900（Archive 慢）
Follower1: appendPosition = 950
Follower2: appendPosition = 980

quorumPosition = 950（Followers 较快）
但 commitPosition = min(950, 900) = 900  ← 限制不超过 Leader
```

**原因**：Leader 必须自己也持久化后，才能提交，否则 Leader 重启后数据丢失。

---

## 9. 总结

### 9.1 核心机制

| 步骤 | 说明 | 源码位置 |
|------|------|----------|
| **1. Follower 发送 appendPosition** | Follower 向 Leader 报告 Archive 录制进度 | `ConsensusModuleAgent.updateFollowerPosition()` |
| **2. Leader 接收 appendPosition** | Leader 更新 Follower 的 logPosition | `ConsensusModuleAgent.onAppendPosition()` |
| **3. Leader 计算 quorumPosition** | Leader 计算大多数节点的最小位置 | `ClusterMember.quorumPosition()` |
| **4. Leader 更新 commitPosition** | Leader 更新 commitPosition counter | `ConsensusModuleAgent.updateLeaderPosition()` |
| **5. Leader 发送 commitPosition** | Leader 通知 Followers 可提交的位置 | `ConsensusModuleAgent.publishCommitPosition()` |
| **6. Service 消费已提交日志** | Service 只消费到 commitPosition 的消息 | `ClusteredServiceAgent.doWork()` |

### 9.2 关键设计理念

| 设计理念 | 说明 | 效果 |
|---------|------|------|
| **Quorum 机制** | 只需大多数节点同步 | 容错性：N 个节点可容忍 N/2 个故障 |
| **心跳机制** | 定期发送 appendPosition 和 commitPosition | 即使无新消息，也能检测节点活跃性 |
| **异步复制** | Leader 不等待 Followers 同步完成 | 高吞吐量：Leader 不被慢 Follower 阻塞 |
| **安全提交** | commitPosition <= quorumPosition | 一致性：已提交的数据不会丢失 |

### 9.3 性能优化

| 优化方向 | 方法 | 效果 |
|---------|------|------|
| **减少网络开销** | appendPosition 只在位置更新时发送 | 高吞吐量时减少网络消息 |
| **避免 GC** | 使用 rankedPositions 临时数组 | 零 GC，高性能 |
| **批量处理** | Service 一次 poll 多个消息 | 提高消费吞吐量 |
| **零拷贝** | 使用 tryClaim 直接编码 | 避免内存拷贝 |

---

**完整的源码分析已完成！所有结论均基于对实际源码的严格追踪。**

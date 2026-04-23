# Aeron Cluster 快照 vs 归档：深度解析

本文档详细解答三个核心问题：
1. Leader 打快照会暂停接收消息吗？
2. 快照具体保存什么信息？
3. 快照和归档有什么区别？

---

## 1. Leader 打快照会暂停接收消息吗？

### 答案：**不会完全暂停，但会进入特殊状态**

### 1.1 快照触发流程

```java
// ConsensusModuleAgent.java:2477
private int leaderWork(final long nowNs)
{
    // ... 正常处理消息 ...

    // 快照触发条件检查
    if (ClusterControl.ToggleState.SNAPSHOT == ClusterControl.ToggleState.get(controlToggle))
    {
        final long timestamp = clusterClock.time();
        if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
        {
            offerPositionAndPreviousState(logPublisher.position(), state);
            // 状态切换：ACTIVE → SNAPSHOT
            state(ConsensusModule.State.SNAPSHOT, "ClusterControl.SNAPSHOT");
            totalSnapshotDurationTracker.onSnapshotBegin(nowNs);
        }
    }
}
```

**关键点**：
- **状态转换**：`ConsensusModule.State.ACTIVE` → `ConsensusModule.State.SNAPSHOT`
- **日志记录**：先将 `ClusterAction.SNAPSHOT` 写入日志，然后所有节点（包括 Followers）都会执行快照

### 1.2 SNAPSHOT 状态下的行为

```java
// ConsensusModuleAgent.java:2550
else if (ConsensusModule.State.SNAPSHOT == state)
{
    if (0 == serviceCount && logPublisher.position() <= commitPosition.getWeak())
    {
        final long timestamp = clusterClock.time();
        snapshotOnServiceAck(commitPosition.getWeak(), timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
    }
}
```

**在 SNAPSHOT 状态下**：

| 操作 | 是否允许 | 说明 |
|------|---------|------|
| **接收客户端消息** | ❌ **暂停** | Leader 不再从 ingress 读取新消息 |
| **处理已提交消息** | ✅ **继续** | 继续消费日志中已提交的消息，直到追上 `commitPosition` |
| **发送心跳** | ✅ **继续** | 继续向 Followers 发送 `CommitPosition`、`NewLeadershipTerm` |
| **日志复制** | ✅ **继续** | Followers 继续复制日志（快照前的消息） |

### 1.3 快照执行流程

```
┌────────────────────────────────────────────────────────────────┐
│                     快照执行时间线                              │
└────────────────────────────────────────────────────────────────┘

时间轴：
t0  ─────────────────────────────────────────────────────────────→
    │                │                  │                │
    ACTIVE           SNAPSHOT           Service 快照     ACTIVE
    (接收消息)        (暂停接收)        (等待所有节点)    (恢复接收)
    │                │                  │                │
    │                │                  │                │
    ▼                ▼                  ▼                ▼
logPos=1000      logPos=1500        等待 Service     恢复客户端
处理 msg #1000   写入 SNAPSHOT      ACK (1-5s)      处理 msg #1501
                 action
                 停止接收新消息     ConsensusModule   新客户端请求
                                   和 Service 快照   开始排队

时长：
- ACTIVE → SNAPSHOT：< 1ms（仅写日志）
- SNAPSHOT 等待 Service：1-5 秒（取决于业务状态大小）
- SNAPSHOT → ACTIVE：< 1ms（状态切换）
```

**详细步骤**：

1. **t0: 触发快照**
   ```java
   // Leader 写入 ClusterAction.SNAPSHOT 到日志
   appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT);
   state(ConsensusModule.State.SNAPSHOT, "ClusterControl.SNAPSHOT");
   ```

2. **t1: 停止接收新消息**
   - Leader 不再 poll `ingressAdapter`（客户端请求排队在网络缓冲区）
   - 继续处理日志中已提交的消息

3. **t2: 等待所有节点追赶**
   ```java
   // 等待 commitPosition 达到快照位置
   if (logPublisher.position() <= commitPosition.getWeak())
   {
       snapshotOnServiceAck(commitPosition.getWeak(), timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
   }
   ```

4. **t3: ConsensusModule 保存快照**
   ```java
   // ConsensusModuleAgent.java:3056
   private void takeSnapshot(final long timestamp, final long logPosition, final ServiceAck[] serviceAcks)
   {
       // 1. 创建快照 publication
       try (ExclusivePublication publication = aeron.addExclusivePublication(
           ctx.snapshotChannel(), ctx.snapshotStreamId()))
       {
           // 2. 开始 Archive 录制快照
           archive.startRecording(channel, ctx.snapshotStreamId(), LOCAL, true);

           // 3. 保存 ConsensusModule 状态（会话、定时器等）
           snapshotState(publication, logPosition, leadershipTermId);

           // 4. 等待录制完成
           awaitRecordingComplete(recordingId, publication.position(), counters, counterId);
       }
   }
   ```

5. **t4: 通知 Service 保存快照**
   - ConsensusModule 向所有 Service 发送 `ClusterAction.SNAPSHOT` 消息
   - Service 调用 `onTakeSnapshot(snapshotPublication)`

6. **t5: 等待 Service ACK**
   ```java
   // 等待所有 Service 返回 ACK（通常 1-5 秒，取决于业务状态大小）
   if (isSnapshotSetComplete(serviceAcks))
   {
       // 记录快照元数据到 RecordingLog
       recordingLog.appendSnapshot(recordingId, leadershipTermId, ...);
       // 恢复到 ACTIVE 状态
       state(ConsensusModule.State.ACTIVE, "snapshot complete");
   }
   ```

7. **t6: 恢复接收消息**
   - 状态切换回 `ACTIVE`
   - Leader 重新开始 poll `ingressAdapter`
   - 客户端请求开始被处理（之前排队的请求现在被消费）

### 1.4 对客户端的影响

**客户端体验**：

```java
// ClusterClient.java
while ((result = aeronCluster.offer(buffer, 0, bytes.length)) < 0 && attempts++ < 3)
{
    if (result == io.aeron.Publication.BACK_PRESSURED)
    {
        System.err.println("[Client] 背压，等待重试...");  // ← 快照期间可能看到此消息
        idleStrategy.idle();
    }
}
```

**现象**：
- 客户端发送消息时可能遇到 **背压（BACK_PRESSURED）**
- 消息不会丢失，只是在网络缓冲区排队
- 快照完成后（1-5 秒），消息立即被处理

**吞吐量影响**：

```
正常状态：100,000 msg/s
快照期间：0 msg/s (暂停 1-5 秒)
快照完成：恢复到 100,000 msg/s

平均影响：
- 快照间隔：10 分钟（600 秒）
- 快照时长：3 秒
- 可用性：99.5%
```

---

## 2. 快照具体保存什么信息？

### 答案：**ConsensusModule 状态 + Service 业务状态**

### 2.1 ConsensusModule 快照内容

```java
// ConsensusModuleAgent.java:3124
private void snapshotState(
    final ExclusivePublication publication, final long logPosition, final long leadershipTermId)
{
    final ConsensusModuleSnapshotTaker snapshotTaker = new ConsensusModuleSnapshotTaker(
        publication, idleStrategy, aeronClientInvoker);

    // 1. 快照标记（开始）
    snapshotTaker.markBegin(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());

    // 2. ConsensusModule 内部状态
    snapshotTaker.snapshotConsensusModuleState(
        sessionManager.nextCommittedSessionId(),  // 下一个会话 ID
        trackerOne.nextServiceSessionId(),        // Service 会话 ID
        trackerOne.logServiceSessionId(),         // 日志会话 ID
        trackerOne.size());                       // 待处理消息数量

    // 3. 所有客户端会话
    sessionManager.snapshotSessions(snapshotTaker);

    // 4. 所有定时器
    timerService.snapshot(snapshotTaker);

    // 5. 待处理的 Service 消息
    for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
    {
        snapshotTaker.snapshot(tracker, ctx.countedErrorHandler());
    }

    // 6. 快照标记（结束）
    snapshotTaker.markEnd(SNAPSHOT_TYPE_ID, logPosition, leadershipTermId, 0, clusterTimeUnit, ctx.appVersion());
}
```

**ConsensusModule 快照包含**：

| 类别 | 内容 | 示例 |
|------|------|------|
| **会话信息** | 所有客户端会话 | `{sessionId=12345, responseStreamId=..., lastCorrelationId=...}` |
| **定时器** | 所有未到期的定时器 | `{correlationId=999, deadline=1234567890}` |
| **元数据** | 下一个会话 ID、日志位置 | `nextSessionId=12346, logPosition=5000000` |
| **待处理消息** | 尚未被 Service 消费的消息 | `{serviceId=0, messageCount=5}` |

### 2.2 Service 快照内容

```java
// SimpleClusteredService.java:149
@Override
public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
{
    System.out.println("  [Service] 开始保存快照: globalCount=" + globalMessageCount +
        ", sessions=" + sessionMessageCounts.size());

    // 1. 保存全局计数
    responseBuffer.putLong(0, globalMessageCount);
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

    // 2. 保存会话数量
    responseBuffer.putInt(0, sessionMessageCounts.size());
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Integer.BYTES);

    // 3. 保存每个会话的计数
    sessionMessageCounts.forEach((sessionId, count) ->
    {
        responseBuffer.putLong(0, sessionId);
        responseBuffer.putLong(Long.BYTES, count);
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES * 2);
    });

    System.out.println("  [Service] 快照保存完成");
}
```

**Service 快照包含**：
- **业务状态**：所有确定性的业务数据
- **示例**：订单簿、账户余额、消息计数器、缓存数据等

### 2.3 快照存储格式

```
┌──────────────────────────────────────────────────────────┐
│               快照文件结构（Archive 录制）                │
└──────────────────────────────────────────────────────────┘

aeron-archive/
└─ recording-XXXX.dat           ← 快照数据文件
   ├─ [SnapshotMarker: BEGIN]   ← 快照开始标记
   │   └─ logPosition=5000000
   │   └─ leadershipTermId=10
   │   └─ timestamp=1234567890
   │
   ├─ [ConsensusModuleState]    ← ConsensusModule 状态
   │   └─ nextSessionId=12346
   │   └─ nextServiceSessionId=...
   │
   ├─ [ClusterSession]          ← 会话 1
   │   └─ sessionId=12345
   │   └─ responseStreamId=1001
   │   └─ lastCorrelationId=999
   │
   ├─ [ClusterSession]          ← 会话 2
   │   └─ sessionId=12346
   │   └─ ...
   │
   ├─ [Timer]                   ← 定时器 1
   │   └─ correlationId=999
   │   └─ deadline=1234567890
   │
   ├─ [Timer]                   ← 定时器 2
   │   └─ ...
   │
   ├─ [Service Snapshot: ID=0]  ← Service 快照
   │   └─ globalMessageCount=1000
   │   └─ sessionCount=2
   │   └─ session[12345]=500
   │   └─ session[12346]=500
   │
   └─ [SnapshotMarker: END]     ← 快照结束标记
       └─ logPosition=5000000
```

### 2.4 快照元数据（RecordingLog）

```java
// ConsensusModuleAgent.java:3087
recordingLog.appendSnapshot(
    recordingId,           // Archive 录制 ID
    leadershipTermId,      // Leadership term ID
    termBaseLogPosition,   // Term 基准位置
    logPosition,           // 快照对应的日志位置
    timestamp,             // 时间戳
    SERVICE_ID);           // 服务 ID
```

**RecordingLog 记录**（文本文件）：

```
# aeron-cluster/recording-log.dat
# 格式：recordingId, leadershipTermId, termBaseLogPosition, logPosition, timestamp, type, serviceId

# 日志条目
123, 10, 0, 5000000, 1234567890, 0, -1    ← Log entry (type=0)

# 快照条目
456, 10, 0, 5000000, 1234567890, 1, -1    ← Snapshot: ConsensusModule (serviceId=-1)
457, 10, 0, 5000000, 1234567890, 1, 0     ← Snapshot: Service 0
```

---

## 3. 快照和归档有什么区别？

### 答案：**快照是状态备份，归档是操作日志**

### 3.1 核心区别对比

| 维度 | **快照 (Snapshot)** | **归档 (Archive)** |
|------|---------------------|-------------------|
| **本质** | **状态备份**（State Checkpoint） | **操作日志**（Event Log） |
| **内容** | 某个时间点的完整业务状态 | 所有客户端消息和集群事件 |
| **大小** | 固定（取决于业务状态大小） | 持续增长（随消息累积） |
| **用途** | 快速恢复到某个时间点 | 重放所有历史操作 |
| **生成频率** | 周期性（如每 1GB 日志） | 实时录制 |
| **保存位置** | `aeron-archive/recording-XXXX.dat` | `aeron-archive/recording-YYYY.dat` |
| **录制方式** | ConsensusModule 主动触发 | Archive 被动录制 stream |
| **删除策略** | 保留最近 N 个快照 | 可删除已快照前的日志 |

### 3.2 详细对比

#### 3.2.1 快照（Snapshot）

**定义**：
> 快照是在某个 **日志位置 (logPosition)** 的完整业务状态备份。

**类比**：
- 就像数据库的 **全量备份**
- 或者游戏的 **存档点**

**示例**：

```
logPosition=0        logPosition=1000     logPosition=5000
    │                     │                     │
    ▼                     ▼                     ▼
初始状态              快照 #1                快照 #2
globalCount=0        globalCount=100        globalCount=500
sessions={}          sessions={12345=50}    sessions={12345=200, 12346=300}
```

**快照包含**：
- ✅ `globalMessageCount = 500`
- ✅ `sessionMessageCounts = {12345=200, 12346=300}`
- ✅ 所有客户端会话
- ✅ 所有未到期定时器
- ❌ **不包含历史消息内容**（只有最终状态）

**恢复流程**：

```java
// 1. 加载快照
loadSnapshot(snapshotRecordingId=457);
// 恢复后：globalMessageCount=500, sessions={12345=200, 12346=300}

// 2. 重放快照后的日志
replayLog(fromPosition=5000, toPosition=6000);
// 处理消息 #501 到 #600

// 3. 最终状态
// globalMessageCount=600, sessions={12345=300, 12346=300}
```

**优势**：
- ⚡ **快速恢复**：不需要重放所有历史日志
- 💾 **节省空间**：可以删除快照前的日志
- 🔧 **易于维护**：定期清理旧日志

#### 3.2.2 归档（Archive）

**定义**：
> 归档是 Archive 模块对所有 **日志消息流** 的实时录制。

**类比**：
- 就像数据库的 **binlog**（MySQL）
- 或者 **WAL**（Write-Ahead Log）

**示例**：

```
Archive 录制文件：recording-123.dat

偏移 0:     [SessionOpen: sessionId=12345]
偏移 1024:  [SessionMessage: "Hello", timestamp=1234567890]
偏移 2048:  [SessionMessage: "World", timestamp=1234567891]
偏移 3072:  [TimerEvent: correlationId=999]
偏移 4096:  [SessionClose: sessionId=12345, reason=CLIENT_ACTION]
偏移 5120:  [NewLeadershipTerm: termId=10, leaderId=0]
...
偏移 5MB:   最新消息
```

**归档包含**：
- ✅ 所有客户端消息（完整内容）
- ✅ 所有集群事件（选举、快照、关闭等）
- ✅ 时间戳、会话 ID、correlationId
- ✅ 可以完整重放整个历史

**恢复流程**：

```java
// 场景：节点启动时无快照，只有归档

// 1. 从头重放日志
replayLog(fromPosition=0, toPosition=6000);

// 2. 逐条处理
// - msg #1: globalMessageCount=1
// - msg #2: globalMessageCount=2
// - ...
// - msg #600: globalMessageCount=600

// 3. 最终状态
// globalMessageCount=600, sessions={12345=300, 12346=300}
```

**优势**：
- 📜 **完整历史**：保留所有操作记录
- 🔍 **可审计**：可以追溯任何时间点的操作
- 🔄 **可重放**：支持时间旅行调试

**劣势**：
- 🐌 **恢复慢**：需要重放所有历史消息
- 💾 **占用空间大**：日志持续增长

### 3.3 协同工作

**快照 + 归档 = 高效恢复**

```
┌────────────────────────────────────────────────────────────┐
│                  快照 + 归档协同工作                        │
└────────────────────────────────────────────────────────────┘

时间线：
t0 ─────────────────────────────────────────────────────────→
    │            │             │             │
    启动         快照 #1       快照 #2       崩溃
    │            │             │             │
    ▼            ▼             ▼             ▼
logPos=0     logPos=5000   logPos=10000  logPos=15000

归档（持续录制）：
├─ recording-123.dat (0 → 20000)   ← 包含所有历史消息
│   └─ 5GB

快照（周期性）：
├─ snapshot-456.dat (logPos=5000)  ← 快照 #1
│   └─ 50MB
└─ snapshot-789.dat (logPos=10000) ← 快照 #2
    └─ 50MB

恢复流程（从 logPos=15000 崩溃点恢复）：
1. 加载最近快照：snapshot-789.dat (logPos=10000)
   └─ 耗时：2 秒（50MB 读取 + 反序列化）
2. 重放日志：recording-123.dat (10000 → 15000)
   └─ 耗时：1 秒（5000 条消息）
3. 总耗时：3 秒

如果没有快照（只靠归档）：
1. 重放日志：recording-123.dat (0 → 15000)
   └─ 耗时：30 秒（15000 条消息）
```

**删除策略**：

```java
// RecordingLog.java
// 快照后可以安全删除旧日志

if (hasValidSnapshot(logPosition=10000))
{
    // 删除 logPosition < 10000 的日志
    archive.truncateRecording(recordingId, 10000);
    // 节省 3GB 空间
}
```

### 3.4 实际案例

**场景：交易系统**

```
初始状态 (logPos=0):
- 账户余额：{}

消息 #1-1000 (logPos=0 → 100000):
- 1000 笔交易，每笔修改余额

快照 #1 (logPos=100000):
- 账户余额：{A=5000, B=3000, C=2000}
- 快照大小：1 KB（3 个账户）
- 归档大小：10 MB（1000 条交易记录）

消息 #1001-2000 (logPos=100000 → 200000):
- 又 1000 笔交易

快照 #2 (logPos=200000):
- 账户余额：{A=8000, B=5000, C=4000}
- 快照大小：1 KB
- 归档大小：20 MB（累计 2000 条）

恢复策略：
- 加载快照 #2：1 KB（秒级恢复）
- 重放 logPos > 200000 的日志
- 无需重放 2000 笔历史交易
```

---

## 4. 最佳实践

### 4.1 快照频率配置

```java
// ConsensusModule.Context
consensusModuleContext
    .snapshotIntervalLength(1024 * 1024 * 1024)  // 1GB 日志后触发快照
    .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));
```

**建议**：
- **高频交易系统**：每 500MB 日志快照一次
- **普通业务系统**：每 1GB 日志快照一次
- **低频系统**：每 10 分钟快照一次

### 4.2 快照保留策略

```java
// RecordingLog.java
// 保留最近 3 个快照

final List<Snapshot> snapshots = recordingLog.getSnapshots();
if (snapshots.size() > 3)
{
    for (int i = 3; i < snapshots.size(); i++)
    {
        archive.deleteRecording(snapshots.get(i).recordingId);
    }
}
```

### 4.3 归档压缩策略

```java
// 快照后删除旧日志

final long snapshotPosition = latestSnapshot.logPosition;
for (final RecordingLog.Entry entry : recordingLog.entries())
{
    if (entry.type == RecordingLog.ENTRY_TYPE_LOG &&
        entry.logPosition < snapshotPosition)
    {
        archive.truncateRecording(entry.recordingId, snapshotPosition);
    }
}
```

---

## 5. 总结

### 快照 vs 归档

| 问题 | 答案 |
|------|------|
| **Leader 打快照会暂停吗？** | ✅ 是的，暂停接收新消息（1-5 秒），但不影响已有消息处理和心跳 |
| **快照保存什么？** | ✅ ConsensusModule 状态（会话、定时器）+ Service 业务状态 |
| **快照 vs 归档区别？** | ✅ 快照 = 状态备份，归档 = 操作日志 |

### 关键要点

1. **快照是优化手段**
   - 避免每次恢复都重放所有历史日志
   - 类似数据库的全量备份

2. **归档是完整记录**
   - 保留所有操作历史
   - 支持审计和重放

3. **协同使用最高效**
   - 快照：快速恢复到检查点
   - 归档：重放检查点后的增量日志
   - 删除策略：快照后可安全删除旧日志

4. **对客户端影响小**
   - 快照期间：背压 1-5 秒
   - 可用性：99.5%+
   - 消息不丢失

---

## 参考源码位置

- **快照触发**：`ConsensusModuleAgent.java:2477`
- **快照保存**：`ConsensusModuleAgent.java:3056`
- **快照状态**：`ConsensusModuleAgent.java:3124`
- **归档录制**：`ArchiveConductor.java:533`
- **RecordingLog**：`RecordingLog.java:46`

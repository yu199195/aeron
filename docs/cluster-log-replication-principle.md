# Aeron Cluster 日志复制原理深度解析

## 1. 为什么需要日志复制？

### 1.1 核心问题

在分布式集群中，我们需要解决以下问题：

| 问题 | 说明 | 日志复制如何解决 |
|------|------|------------------|
| **一致性** | 如何保证所有节点的状态一致？ | 所有节点按相同顺序执行相同的日志 |
| **容错性** | 如何在节点故障后恢复？ | 其他节点保存完整日志副本 |
| **持久性** | 如何在重启后恢复状态？ | 日志持久化到磁盘（AeronArchive） |
| **顺序性** | 如何保证消息顺序？ | 日志位置（logPosition）严格递增 |

### 1.2 状态机复制（State Machine Replication）

```
客户端请求 → Leader 写入日志 → Followers 复制日志 → 所有节点执行 → 返回结果

关键原理：
如果所有节点从相同的初始状态开始，
按照相同的顺序执行相同的操作（日志），
那么它们将到达相同的最终状态。
```

**示例**：
```
初始状态：counter = 0

日志 1：increment → counter = 1
日志 2：increment → counter = 2
日志 3：decrement → counter = 1

所有节点按照 日志1 → 日志2 → 日志3 的顺序执行，
最终 counter 都是 1（状态一致）
```

---

## 2. Aeron Cluster 日志复制架构

### 2.1 整体架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                    Aeron Cluster 日志复制架构                     │
└──────────────────────────────────────────────────────────────────┘

客户端                Leader 节点              Follower 节点 1        Follower 节点 2
   │                     │                          │                     │
   │  1. 发送请求        │                          │                     │
   ├──────────────────> │                          │                     │
   │  "increment"        │                          │                     │
   │                     │                          │                     │
   │                  ┌─────┐                       │                     │
   │                  │ CM  │ ConsensusModule       │                     │
   │                  └──┬──┘                       │                     │
   │                     │                          │                     │
   │                     │ 2. 写入日志              │                     │
   │                     │  (logPublication)        │                     │
   │                     ├──────────────────────────┼─────────────────────>
   │                     │  SessionMessage(         │                     │
   │                     │    clusterSessionId,     │                     │
   │                     │    timestamp,            │                     │
   │                     │    "increment"           │                     │
   │                     │  )                       │                     │
   │                     │                          │                     │
   │                  ┌──▼──┐                    ┌──▼──┐              ┌──▼──┐
   │                  │ Svc │ Service            │ Svc │              │ Svc │
   │                  └──┬──┘                    └──┬──┘              └──┬──┘
   │                     │                          │                     │
   │                     │ 3. 消费日志              │ 3. 消费日志          │ 3. 消费日志
   │                     │  (logSubscription)       │  (logSubscription)   │  (logSubscription)
   │                     ↓                          ↓                     ↓
   │              onSessionMessage()         onSessionMessage()     onSessionMessage()
   │              counter++                  counter++              counter++
   │                     │                          │                     │
   │                     │ 4. 确认消费位置          │                     │
   │                     │  (appendPosition)        │                     │
   │                     ├<─────────────────────────┤                     │
   │                     ├<─────────────────────────┼─────────────────────┤
   │                     │                          │                     │
   │                     │ 5. 更新 commitPosition    │                     │
   │                     │  (quorum 确认后提交)      │                     │
   │                     │                          │                     │
   │  6. 返回响应        │                          │                     │
   │ <──────────────────┤                          │                     │
   │  "OK"               │                          │                     │
```

### 2.2 关键组件

| 组件 | 职责 | 位置 |
|------|------|------|
| **ConsensusModule** | Leader: 写入日志<br>Follower: 监控 Leader | `ConsensusModule.java` |
| **logPublication** | Leader 发布日志（UDP multicast） | `ExclusivePublication` |
| **logSubscription** | Follower/Service 订阅日志 | `Subscription` |
| **ClusteredServiceAgent** | 消费日志，调用 ClusteredService 回调 | `ClusteredServiceAgent.java` |
| **AeronArchive** | 日志持久化到磁盘 | `AeronArchive` |
| **commitPosition** | 已提交的日志位置（quorum 确认） | `Counter` |

---

## 3. 日志复制详细流程

### 3.1 Leader 写入日志流程

```
┌────────────────────────────────────────────────────────────────┐
│          Leader 写入日志流程（ConsensusModule）                │
└────────────────────────────────────────────────────────────────┘

1. 客户端发送请求
   │
   ├─> Ingress: 接收客户端消息
   │   - IngressAdapter.poll()
   │   - onSessionMessage(clusterSessionId, buffer, ...)
   │
   ├─> 2. 追加到 Sequencer Buffer
   │   - sequencerAgent.offer(...)
   │   - 暂存在内存中（待写入日志）
   │
   ├─> 3. 写入日志（logPublication）
   │   - logPublisher.appendMessage(...)
   │   - 编码为 SessionMessage
   │   - publication.offer(buffer) → 通过 UDP multicast 发送
   │   - logPosition += messageLength
   │
   ├─> 4. Archive 录制日志
   │   - AeronArchive 自动录制 logPublication 的所有消息
   │   - 持久化到磁盘：recording-{recordingId}.dat
   │
   ├─> 5. 等待 Followers 确认
   │   - 接收 Followers 的 appendPosition 消息
   │   - follower1Position = X
   │   - follower2Position = Y
   │
   ├─> 6. 更新 commitPosition
   │   - 计算 quorum 位置：min(leader, follower1, follower2)
   │   - 如果 ≥ quorum，更新 commitPosition counter
   │   - Service 可以消费到 commitPosition 的消息
   │
   └─> 7. 返回响应给客户端
       - 通过 egressPublisher.sendEvent(...)
       - 客户端接收响应
```

### 3.2 Follower/Service 消费日志流程

```
┌────────────────────────────────────────────────────────────────┐
│       Follower/Service 消费日志流程（ClusteredServiceAgent）   │
└────────────────────────────────────────────────────────────────┘

1. 订阅日志
   │
   ├─> ConsensusModule 发送 JOIN_LOG 命令
   │   - onJoinLog(logChannel, logStreamId, ...)
   │
   ├─> Service 订阅日志
   │   - logSubscription = aeron.addSubscription(logChannel, logStreamId)
   │   - 等待 Image 可用
   │
   └─> logAdapter.image(image)

2. 消费日志（doWork() 循环）
   │
   ├─> logAdapter.poll(commitPosition.get())
   │   - 从 Image 读取消息（最多到 commitPosition）
   │   - 只消费已提交的消息（确保一致性）
   │
   ├─> 解析消息类型
   │   - SessionMessage → onSessionMessage()
   │   - TimerEvent → onTimerEvent()
   │   - SessionOpen → onSessionOpen()
   │   - SessionClose → onSessionClose()
   │   - ServiceAction.SNAPSHOT → onServiceAction()
   │   - NewLeadershipTerm → onNewLeadershipTermEvent()
   │
   ├─> 调用 ClusteredService 回调
   │   - service.onSessionMessage(clientSession, timestamp, buffer, ...)
   │   - 业务逻辑处理消息（如 counter++）
   │   - 更新 logPosition
   │
   └─> 发送 appendPosition 确认（Follower）
       - consensusModuleProxy.ack(logPosition, ...)
       - 通知 Leader 已消费到此位置

3. Archive 录制（Follower）
   │
   └─> AeronArchive 自动录制 logSubscription 的所有消息
       - 持久化到本地磁盘
       - 用于故障恢复
```

---

## 4. 关键概念详解

### 4.1 logPosition vs commitPosition

```
┌─────────────────────────────────────────────────────────────┐
│               logPosition vs commitPosition                 │
└─────────────────────────────────────────────────────────────┘

时间线：
t0 ─────────────────────────────────────────────────────────→

Leader:
  logPosition:        1000 ────> 1200 ────> 1400 ────> 1600
  commitPosition:     1000 ────> 1000 ────> 1200 ────> 1400
                               ↑ 等待 quorum        ↑ quorum 确认

Follower 1:
  logPosition:        1000 ────> 1200 ────> 1400 ────> 1600
  (appendPosition)

Follower 2:
  logPosition:        1000 ────> 1100 ────> 1200 ────> 1400
  (appendPosition)            ↑ 落后          ↑ 追上

commitPosition 计算：
t1: min(1200, 1200, 1100) = 1100 < quorum threshold → 不提交
t2: min(1400, 1400, 1200) = 1200 ≥ quorum threshold → 提交到 1200
t3: min(1600, 1600, 1400) = 1400 ≥ quorum threshold → 提交到 1400
```

**说明**：
- **logPosition**: Leader 已写入的日志位置
- **commitPosition**: quorum 节点已确认的日志位置
- **Service 只消费 commitPosition 以内的消息**（确保一致性）

### 4.2 日志消息类型

| 消息类型 | 说明 | 何时写入 | Service 如何处理 |
|---------|------|----------|------------------|
| **SessionMessage** | 客户端业务消息 | 客户端发送请求 | onSessionMessage() → 执行业务逻辑 |
| **TimerEvent** | 定时器事件 | 定时器触发 | onTimerEvent() → 执行定时任务 |
| **SessionOpen** | 会话打开 | 客户端连接 | onSessionOpen() → 创建会话 |
| **SessionClose** | 会话关闭 | 客户端断开 | onSessionClose() → 清理会话 |
| **ServiceAction** | 集群动作 | Leader 触发快照 | onServiceAction(SNAPSHOT) → 保存快照 |
| **NewLeadershipTerm** | 新 Leader 上任 | 选举完成 | onNewLeadershipTermEvent() → 更新 term |

### 4.3 日志持久化（AeronArchive）

```
┌─────────────────────────────────────────────────────────────┐
│                   日志持久化流程                             │
└─────────────────────────────────────────────────────────────┘

Leader:
  logPublication (UDP multicast)
         │
         ├─> 发送到网络 → Followers 接收
         │
         └─> AeronArchive 录制
             │
             └─> 持久化到磁盘
                 ├─ archive/
                 │   ├─ recording-123.dat  ← 日志数据文件
                 │   ├─ recording-123-0-1000.rec  ← 元数据文件
                 │   └─ catalog.dat  ← 目录文件

Follower:
  logSubscription (UDP multicast)
         │
         ├─> 从网络接收
         │
         └─> AeronArchive 录制
             │
             └─> 持久化到本地磁盘
                 ├─ archive/
                 │   ├─ recording-456.dat  ← 本地副本
                 │   ├─ recording-456-0-1000.rec
                 │   └─ catalog.dat

重启后：
  └─> 从 Archive 回放日志
      - archive.startReplay(recordingId, ...)
      - 恢复到 logPosition
```

---

## 5. 日志复制的关键保证

### 5.1 一致性保证

```
保证机制：
1. 顺序性：logPosition 严格递增
2. 原子性：quorum 确认后才提交
3. 持久性：Archive 持久化到磁盘
4. 确定性：相同输入 → 相同输出（deterministic time）
```

### 5.2 Quorum 机制

```
3 节点集群：quorum = 2（多数）

场景 1：所有节点正常
  Leader:     logPos = 1000  ✓
  Follower1:  logPos = 1000  ✓
  Follower2:  logPos = 1000  ✓
  → commitPosition = 1000（3/3 ≥ quorum）

场景 2：1 个 Follower 落后
  Leader:     logPos = 1000  ✓
  Follower1:  logPos = 1000  ✓
  Follower2:  logPos = 900   ✗
  → commitPosition = 900（2/3 ≥ quorum，但取最小值）

场景 3：1 个 Follower 故障
  Leader:     logPos = 1000  ✓
  Follower1:  logPos = 1000  ✓
  Follower2:  故障          ✗
  → commitPosition = 1000（2/3 ≥ quorum）

场景 4：2 个节点故障（失去 quorum）
  Leader:     logPos = 1000  ✓
  Follower1:  故障          ✗
  Follower2:  故障          ✗
  → commitPosition 停止更新（1/3 < quorum）
  → 集群进入只读状态
```

---

## 6. 源码位置索引

| 功能 | 源码位置 | 关键方法 |
|------|----------|----------|
| **Leader 写入日志** | `ConsensusModuleAgent.java` | `consensusWork()` → `logPublisher.appendMessage()` |
| **Service 消费日志** | `ClusteredServiceAgent.java` | `doWork()` → `logAdapter.poll()` |
| **日志发布** | `LogPublisher.java` | `appendMessage()` |
| **日志订阅** | `BoundedLogAdapter.java` | `poll()` |
| **commitPosition 更新** | `ConsensusModuleAgent.java` | `updateMemberPosition()` → `commitPosition.setOrdered()` |
| **Archive 录制** | `AeronArchive.java` | `startRecording()` |
| **Archive 回放** | `AeronArchive.java` | `startReplay()` |

---

## 7. 时序图：完整的日志复制流程

```
时间线：一条消息从客户端到所有节点的完整流程

t0 ────────────────────────────────────────────────────────────→

客户端          Leader CM         Leader Svc      Follower1 Svc    Follower2 Svc
  │                 │                  │                │                │
  │ 1. 发送请求      │                  │                │                │
  ├──────────────> │                  │                │                │
  │ "increment"     │                  │                │                │
  │                 │                  │                │                │
  │              ┌──▼──┐               │                │                │
  │              │ 接收 │               │                │                │
  │              │ 请求 │               │                │                │
  │              └──┬──┘               │                │                │
  │                 │                  │                │                │
  │                 │ 2. 写入日志       │                │                │
  │                 │  logPublication  │                │                │
  │                 ├──────────────────┼────────────────┼────────────────>
  │                 │  (UDP multicast) │                │                │
  │                 │                  │                │                │
  │                 │ 3a. Archive 录制  │                │                │
  │                 │  recording.dat   │                │                │
  │                 ↓                  │                │                │
  │              [持久化]               │                │                │
  │                 │                  │                │                │
  │                 │              3b. 消费日志       3b. 消费日志      3b. 消费日志
  │                 │                  ↓                ↓                ↓
  │                 │           onSessionMessage() onSessionMessage() onSessionMessage()
  │                 │           counter++         counter++         counter++
  │                 │           logPos=1000       logPos=1000       logPos=950
  │                 │                  │                │                │
  │                 │                  │ 4. 发送确认     │                │
  │                 │ <────────────────┤  appendPos     │                │
  │                 │ <────────────────┼────────────────┤                │
  │                 │ <────────────────┼────────────────┼────────────────┤
  │                 │                  │                │                │
  │                 │ 5. 更新 commit    │                │                │
  │                 │  min(1000,1000,950)=950          │                │
  │                 │  commitPos=950   │                │                │
  │                 │                  │                │                │
  │                 │ 6. Service 消费   │                │                │
  │                 │  到 950           │                │                │
  │                 │                  ↓                │                │
  │                 │           poll(commitPos=950)     │                │
  │                 │           处理消息                 │                │
  │                 │                  │                │                │
  │  7. 返回响应     │                  │                │                │
  │ <──────────────┤                  │                │                │
  │  "OK"           │                  │                │                │
  │                 │                  │                │                │
  │                 │ 8. Follower2 追赶 │                │                │
  │                 │                  │                │  logPos=1000   │
  │                 │ <────────────────┼────────────────┼────────────────┤
  │                 │                  │                │  appendPos=1000│
  │                 │                  │                │                │
  │                 │ 9. 更新 commit    │                │                │
  │                 │  min(1000,1000,1000)=1000        │                │
  │                 │  commitPos=1000  │                │                │

总耗时：约 5-10ms（取决于网络延迟和节点数量）
```

---

## 8. 常见问题

### Q1: 为什么 Service 只消费 commitPosition 以内的消息？

**答**：确保一致性。如果 Service 消费未提交的消息，当 Leader 故障时，新 Leader 可能没有这些消息，导致状态不一致。

### Q2: logPosition 和 commitPosition 的差距通常有多大？

**答**：通常 < 100 条消息（约 1-2ms 的延迟）。差距取决于：
- 网络延迟（UDP multicast）
- Follower 处理速度
- 节点数量（quorum 计算）

### Q3: 如果 Follower 落后太多怎么办？

**答**：ConsensusModule 会触发 **catchup 机制**：
1. Leader 检测到 Follower 落后（如 > 1000 条消息）
2. Leader 创建 catchup publication（点对点传输）
3. Follower 从 catchup publication 接收日志
4. 追赶完成后，回到正常的 multicast 订阅

### Q4: Archive 录制会影响性能吗？

**答**：影响很小（< 5% 延迟）：
- Archive 使用 **零拷贝**（zero-copy）机制
- 日志先发送到网络，Archive 异步录制
- 使用内存映射文件（mmap），顺序写入

### Q5: 日志会无限增长吗？

**答**：不会。集群有 **日志清理机制**：
1. 定期打快照（如每 5 分钟）
2. 快照位置之前的日志可以删除
3. 保留最近 N 个快照（如 3 个）
4. Archive 自动管理磁盘空间

---

## 9. 总结

### 9.1 日志复制的关键要点

| 要点 | 说明 |
|------|------|
| **为什么需要** | 实现状态机复制，保证一致性、容错性、持久性、顺序性 |
| **如何实现** | Leader 写入 → Followers 复制 → quorum 确认 → Service 消费 |
| **核心组件** | ConsensusModule、logPublication/Subscription、AeronArchive、commitPosition |
| **性能** | 延迟 < 10ms，吞吐量 > 1M msg/s（取决于网络和硬件） |
| **可靠性** | quorum 机制、Archive 持久化、catchup 机制 |

### 9.2 与 Raft 的关系

Aeron Cluster 的日志复制基于 Raft 论文，但有以下优化：

| 特性 | Raft | Aeron Cluster |
|------|------|---------------|
| **通信方式** | RPC（点对点） | UDP multicast（一对多） |
| **日志传输** | Leader → Follower 逐个发送 | Leader 一次广播，所有节点接收 |
| **性能** | 延迟随节点数增加 | 延迟不受节点数影响 |
| **持久化** | 应用层实现 | AeronArchive（零拷贝） |

---

## 10. 下一步学习

建议继续深入以下主题：

1. **LogPublisher.java** - Leader 如何写入日志
2. **BoundedLogAdapter.java** - Service 如何消费日志
3. **ConsensusModuleAgent.updateMemberPosition()** - commitPosition 如何更新
4. **Catchup 机制** - Follower 如何追赶日志
5. **快照机制** - 如何减少日志大小

---

**文档完成！接下来将添加相关源码的中文注释。**

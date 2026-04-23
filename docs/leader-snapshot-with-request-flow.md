# Leader 打快照时的完整请求流程（含日志复制与归档）

## 概述

本文档详细说明当 **Leader 节点正在打快照** 时，一个新的客户端请求如何被处理，包括：

1. 客户端请求接收
2. Leader ConsensusModule 处理
3. 日志写入与 Archive 归档
4. Follower 日志复制与归档
5. Service 消费与处理
6. 快照期间的特殊行为

---

## 完整流程图

```
┌────────────────────────────────────────────────────────────────────────────────┐
│          Leader 打快照时接收请求的完整流程（包含日志复制与归档）                │
└────────────────────────────────────────────────────────────────────────────────┘

时间轴：
t0: Leader 开始打快照
t1: 新请求到来
t2-t6: 请求处理流程


Client                Leader ConsensusModule       Leader Archive          Leader Service
  │                            │                         │                       │
  │                            │                         │                       │
  │                            │ t0: 开始快照             │                       │
  │                            ├──────────────────────────┼──────────────────────>
  │                            │ appendAction(SNAPSHOT)   │                       │
  │                            │                         │                       │
  │                            │ state = SNAPSHOT         │                       │
  │                            │ (暂停接收新请求)         │                       │
  │                            │                         │                       │
  │ t1: 发送请求                │                         │                       │
  ├─────────────────────────> │                         │                       │
  │ SessionMessage("order")   │                         │                       │
  │                            │                         │                       │
  │                            ▼                         │                       │
  │                      ┌──────────┐                   │                       │
  │                      │ 请求被缓存│ ← IngressAdapter │                       │
  │                      │   排队    │   背压(BACK_PRESSURED)                  │
  │                      └──────────┘                   │                       │
  │                            │                         │                       │
  │                            │ ... 快照进行中 ...       │                       │
  │                            │                         │                       │
  │                            │ <──────────────────────┼───────────────────────┤
  │                            │ Service ACK (recordingId)                      │
  │                            │                         │                       │
  │                            │ 快照完成                 │                       │
  │                            │ state = ACTIVE           │                       │
  │                            │ (恢复接收请求)           │                       │
  │                            │                         │                       │
  │                      ┌────▼─────┐                   │                       │
  │                      │ 处理排队  │                   │                       │
  │                      │   请求    │                   │                       │
  │                      └────┬─────┘                   │                       │
  │                            │                         │                       │
  │                            │ t2: 写入日志             │                       │
  │                            │ logPublisher.appendMessage()                   │
  │                            ├──────────────────────────────────────────────────>
  │                            │                         │                       │
  │                            │                         │                       │

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
       日志复制到 Followers + Archive 归档（t2-t5）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


Leader CM              Leader Archive           Follower1 CM          Follower1 Archive
    │                        │                         │                      │
    │ t2: appendMessage      │                         │                      │
    │  logPublication.offer()│                         │                      │
    ├───────────────────────>│                         │                      │
    │  (UDP multicast)       │                         │                      │
    │  SessionMessage(       │                         │                      │
    │    clusterSessionId,   │                         │                      │
    │    timestamp,          │                         │                      │
    │    "order"             │                         │                      │
    │  )                     │                         │                      │
    │                        │                         │                      │
    │                        │◄────────────────────────┤                      │
    │                        │  (spy 订阅同一 channel)   │                      │
    │                        │                         │                      │
    ├────────────────────────┼─────────────────────────┼─────────────────────>
    │                        │                         │  (UDP multicast)     │
    │                        │                         │                      │
    │                        │                         ▼                      │
    │                        │                   logSubscription             │
    │                        │                     接收消息                   │
    │                        │                         │                      │
    │                        │                         ├─────────────────────>│
    │                        │                         │  (spy 订阅)          │
    │                        │                         │                      │
    │                        ▼                         ▼                      ▼
    │                  ┌────────────┐           ┌────────────┐        ┌────────────┐
    │                  │ Archive 录制│           │ Archive 录制│        │ Archive 录制│
    │                  │            │           │            │        │            │
    │                  │ recording- │           │ recording- │        │ recording- │
    │                  │  123.dat   │           │  456.dat   │        │  456.dat   │
    │                  │            │           │            │        │            │
    │                  │ 持久化磁盘  │           │ 持久化磁盘  │        │ 持久化磁盘  │
    │                  └────────────┘           └────────────┘        └────────────┘
    │                        │                         │                      │
    │                   日志已持久化 ✓              日志已持久化 ✓          日志已持久化 ✓
    │                        │                         │                      │


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
       Service 消费日志（t4-t6）
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


Leader Service        Leader CM             Follower1 Service      Follower1 CM
      │                   │                         │                    │
      │                   │                         │                    │
      │ t4: 消费日志       │                         │                    │
      │  logAdapter.poll( │                         │                    │
      │    commitPosition)│                         │                    │
      ├<──────────────────┤                         │                    │
      │  (只消费已提交消息) │                         │                    │
      │                   │                         │                    │
      ▼                   │                         ▼                    │
onSessionMessage()        │                   onSessionMessage()        │
处理业务逻辑               │                   处理业务逻辑               │
logPosition = 1000        │                   logPosition = 1000        │
      │                   │                         │                    │
      │                   │                         │ t5: 发送 ACK       │
      │                   │ <───────────────────────┼────────────────────┤
      │                   │  appendPosition(1000)   │                    │
      │                   │                         │                    │
      │                   ▼                         │                    │
      │             更新 commitPosition              │                    │
      │             min(Leader, F1, F2)             │                    │
      │             = min(1000, 1000, 1000)         │                    │
      │             commitPosition = 1000           │                    │
      │                   │                         │                    │
      │                   │ t6: 回复客户端           │                    │
      │                   ├──────────────────────────────────────────────────>
      │                   │ egressPublisher.sendEvent()                 Client
      │                   │ "Order processed"       │                    │
      │                   │                         │                    │
```

---

## 关键阶段详解

### 阶段 1：快照开始（t0）

```
Leader ConsensusModule:
1. 检测到快照触发条件
   - logPosition - lastSnapshotPosition >= snapshotIntervalThreshold

2. 写入 ClusterAction.SNAPSHOT 到日志
   - logPublisher.appendClusterAction(SNAPSHOT, ...)

3. 状态切换：ACTIVE → SNAPSHOT
   - state = ConsensusModule.State.SNAPSHOT
   - 暂停接收新请求（不再 poll ingressAdapter）
   - 但继续处理已提交的日志
   - 继续发送心跳给 Followers

4. 通知 Service 执行快照
   - Service 消费到 ClusterAction.SNAPSHOT
   - 调用 service.onTakeSnapshot(snapshotPublication)
   - 保存业务状态到 Archive
```

### 阶段 2：请求到来与缓存（t1）

```
客户端:
1. 发送请求
   - client.offer(buffer) → Leader ingress

Leader ConsensusModule:
1. 请求到达 IngressAdapter
   - 但 state = SNAPSHOT，不处理新请求
   - 请求被缓存在网络缓冲区中

2. 客户端可能看到背压
   - Publication.offer() 返回 BACK_PRESSURED
   - 客户端 idle() 重试

关键点：
- 消息在网络层缓存（Aeron 的 Image 缓冲区）
- 不会丢失（缓冲区有足够空间）
- 等待快照完成后处理
```

### 阶段 3：快照完成，恢复接收（t2）

```
Leader Service:
1. 快照保存完成
   - archive.startRecording() → 持久化快照
   - service.onTakeSnapshot() 完成

2. 发送 ACK 给 ConsensusModule
   - consensusModuleProxy.ack(logPosition, recordingId)

Leader ConsensusModule:
1. 接收到 Service ACK
   - onServiceAck(recordingId)
   - 记录快照位置：lastSnapshotPosition = logPosition

2. 状态切换：SNAPSHOT → ACTIVE
   - state = ConsensusModule.State.ACTIVE
   - 恢复接收新请求（重新 poll ingressAdapter）

3. 处理排队的请求
   - ingressAdapter.poll() → 读取缓存的请求
   - 开始正常处理流程
```

### 阶段 4：日志写入与 Archive 归档（t3）

```
Leader ConsensusModule:
1. 写入日志
   - logPublisher.appendMessage(sessionMessage)
   - publication.offer(buffer) → UDP multicast

2. 日志内容：
   ┌────────────────────────────────────┐
   │ SessionMessage                     │
   ├────────────────────────────────────┤
   │ - clusterSessionId: 12345          │
   │ - timestamp: 1640000000000         │
   │ - correlationId: 999               │
   │ - messageLength: 100               │
   │ - payload: "order:100"             │
   └────────────────────────────────────┘

Leader Archive:
1. Archive 订阅了 logPublication（spy 模式）
   - archive.startRecording(logChannel, logStreamId, LOCAL)
   - 创建 aeron-spy: 订阅

2. Archive 接收到日志消息
   - RecordingSession.doWork()
   - 从 Image 读取消息
   - 写入磁盘：recording-123.dat

3. 持久化完成
   - 消息已安全存储在 Leader 磁盘 ✓

关键保证：
- 日志先持久化，再提交
- 即使 Service down，消息仍在 Archive 中
```

### 阶段 5：Follower 日志复制与归档（t3-t4）

```
Follower1 ConsensusModule:
1. 接收日志（UDP multicast）
   - logSubscription.poll() → 读取 SessionMessage
   - 消息通过网络到达 Follower1

2. Follower1 Service 消费日志
   - logAdapter.poll(commitPosition)
   - 只消费已提交的消息
   - onSessionMessage() → 处理业务逻辑
   - logPosition = 1000

3. 发送 ACK 给 Leader
   - consensusModuleProxy.ack(logPosition)
   - 通知 Leader：Follower1 已处理到 1000

Follower1 Archive:
1. Archive 订阅了 logSubscription（spy 模式）
   - archive.startRecording(logChannel, logStreamId, LOCAL)

2. Archive 接收到日志消息
   - RecordingSession.doWork()
   - 写入本地磁盘：recording-456.dat

3. 持久化完成
   - Follower1 的日志副本已安全存储 ✓

Follower2 同理：
- 接收日志、归档、发送 ACK
```

### 阶段 6：commitPosition 更新与客户端响应（t5-t6）

```
Leader ConsensusModule:
1. 接收所有 Followers 的 ACK
   - Follower1: appendPosition = 1000
   - Follower2: appendPosition = 1000

2. 计算 quorum 位置
   - commitPosition = min(Leader=1000, F1=1000, F2=1000)
   - commitPosition = 1000

3. 更新 commitPosition counter
   - commitPosition.setOrdered(1000)
   - Service 可以消费到 1000 的消息

Leader Service:
1. 继续消费已提交的日志
   - logAdapter.poll(commitPosition=1000)
   - 处理消息（如果还没处理）

2. 回复客户端
   - session.offer(responseBuffer)
   - egressPublisher.sendEvent()
   - 客户端接收响应："Order processed"
```

---

## Archive 归档的详细机制

### 1. Archive 如何订阅日志？

```java
// Leader ConsensusModule 启动时
String logChannel = "aeron:udp?endpoint=224.0.1.1:40456";
int logStreamId = 10;

// Leader Archive 启动录制
archive.startRecording(logChannel, logStreamId, SourceLocation.LOCAL);
```

**内部流程**：

```
AeronArchive.startRecording(channel, streamId, LOCAL)
    │
    └─> ArchiveProxy.startRecording()
        └─> 发送 StartRecordingRequest 到 Archive
            │
            └─> ArchiveConductor.startRecording()
                │
                ├─ 1. 构造 spy channel
                │   - strippedChannel = "aeron:udp?endpoint=224.0.1.1:40456"
                │   - spyChannel = "aeron-spy:aeron:udp?endpoint=224.0.1.1:40456"
                │
                ├─ 2. 创建 spy 订阅
                │   - aeron.addSubscription(spyChannel, streamId, imageHandler)
                │   - imageHandler: 当 Image 可用时 → startRecordingSession
                │
                └─ 3. 返回 OK
                    - subscriptionId = subscription.registrationId()
```

**spy 订阅的工作原理**：

```
aeron-spy: 前缀的订阅可以"监听"同一 channel 上的所有 Publication
而不影响原有的订阅者

正常订阅：
  Publication ──> Subscription (消费消息)

spy 订阅：
  Publication ──┬──> Subscription (正常消费)
                └──> spy Subscription (旁路监听，不影响正常流)
```

### 2. Archive 何时开始录制？

```
时间线：
t0: archive.startRecording() 调用
    └─> Archive 创建 spy 订阅
        └─> 等待 Image 可用

t1: ConsensusModule 创建 logPublication
    └─> Publication.offer(message)

t2: Image 可用事件触发
    └─> ArchiveConductor.startRecordingSession()
        ├─ catalog.addNewRecording() → recordingId
        ├─ RecordingPos.allocate() → 创建计数器
        └─ RecordingSession 开始录制

t3: 持续录制
    └─> RecordingSession.doWork()
        ├─ image.poll(fragmentHandler)
        ├─ 读取消息片段
        └─> 写入磁盘：recording-{recordingId}.dat
```

### 3. 录制的数据结构

```
Archive 目录结构：
archive/
├─ catalog.dat                    ← 录制目录（所有 recording 的元数据）
├─ recording-123.dat              ← Leader 的日志数据文件
├─ recording-123-0-10000.rec      ← 元数据（segment 信息）
├─ recording-456.dat              ← Follower1 的日志数据文件
└─ recording-456-0-10000.rec

recording-123.dat 内容：
┌────────────────────────────────────────────┐
│ [SessionMessage #1]                        │
│ - header (32 bytes)                        │
│ - payload (100 bytes)                      │
├────────────────────────────────────────────┤
│ [SessionMessage #2]                        │
│ - header (32 bytes)                        │
│ - payload (200 bytes)                      │
├────────────────────────────────────────────┤
│ ...                                        │
└────────────────────────────────────────────┘
```

### 4. Follower 的 Archive 如何工作？

```
Follower Archive 的订阅流程与 Leader 完全相同：

Follower ConsensusModule:
- logSubscription = aeron.addSubscription(logChannel, logStreamId)
  └─> 接收 Leader 的 UDP multicast 消息

Follower Archive:
- archive.startRecording(logChannel, logStreamId, LOCAL)
  └─> spySubscription = aeron.addSubscription("aeron-spy:" + logChannel, logStreamId)
      └─> 监听 logSubscription 的消息
          └─> 持久化到本地磁盘

结果：
- Follower1: recording-456.dat（与 Leader 的内容完全相同）
- Follower2: recording-789.dat（与 Leader 的内容完全相同）

所有节点都有完整的日志副本！
```

---

## 快照期间的特殊行为总结

### 1. 对客户端的影响

```
快照期间（约 1-5 秒）：
┌──────────────────────────────────────┐
│ 客户端体验                            │
├──────────────────────────────────────┤
│ ✓ 可以发送请求                        │
│ ⚠️ 请求会排队（背压）                 │
│ ✓ 快照完成后立即处理                  │
│ ✓ 不会丢失消息                        │
│ ⚠️ 响应延迟 +1-5 秒                   │
└──────────────────────────────────────┘
```

### 2. 对 Follower 的影响

```
快照期间：
┌──────────────────────────────────────┐
│ Follower 行为                         │
├──────────────────────────────────────┤
│ ✓ 继续接收日志                        │
│ ✓ 继续 Archive 归档                   │
│ ✓ 继续发送 ACK                        │
│ ✓ 继续处理已提交的消息                │
│ ⚠️ commitPosition 可能暂停更新        │
│   (Leader 不处理新请求)               │
└──────────────────────────────────────┘
```

### 3. Archive 的保证

```
关键保证：
┌──────────────────────────────────────┐
│ 1. 日志先持久化，再提交               │
│    - Archive 录制 → commitPosition    │
│                                      │
│ 2. 所有节点都有完整副本               │
│    - Leader Archive: recording-123   │
│    - Follower1 Archive: recording-456│
│    - Follower2 Archive: recording-789│
│                                      │
│ 3. 快照期间日志不停                   │
│    - Archive 持续录制                 │
│    - 不受快照影响                     │
│                                      │
│ 4. 即使 Service down，消息也不丢      │
│    - 消息在 Archive 中                │
│    - 重启后从 Archive 回放            │
└──────────────────────────────────────┘
```

---

## 完整时序图（精确版）

```
时间轴：完整的请求处理流程（包含快照）

t0 ────────────────────────────────────────────────────────────────────→

Client  Leader CM  Leader Svc  Leader Arc  Follower1 CM  Follower1 Arc
  │         │          │            │             │              │
  │         │ t0: 快照开始           │             │              │
  │         ├──────────┼────────────>            │              │
  │         │ SNAPSHOT │ onTakeSnapshot()        │              │
  │         │          │ 保存业务状态             │              │
  │         │          │ ... 1-5秒 ...           │              │
  │         │          │            │             │              │
  │ t1: 请求│          │            │             │              │
  ├────────>│          │            │             │              │
  │ "order" │          │            │             │              │
  │         │ 缓存请求  │            │             │              │
  │         │ (背压)    │            │             │              │
  │         │          │            │             │              │
  │         │          │◄───────────┤             │              │
  │         │          │ ACK        │             │              │
  │         │ 快照完成  │            │             │              │
  │         │ ACTIVE   │            │             │              │
  │         │          │            │             │              │
  │         │ t2: 处理请求           │             │              │
  │         │ appendMessage()        │             │              │
  │         ├────────────────────────┼─────────────┼──────────────>
  │         │ (UDP multicast)        │             │              │
  │         │                        │             │              │
  │         │          │           ┌─▼──┐       ┌─▼──┐        ┌─▼──┐
  │         │          │           │录制│       │录制│        │录制│
  │         │          │           │.dat│       │.dat│        │.dat│
  │         │          │           └─┬──┘       └─┬──┘        └─┬──┘
  │         │          │             ✓             ✓              ✓
  │         │          │        持久化完成      持久化完成    持久化完成
  │         │          │             │             │              │
  │         │          │ t3: 消费日志│             │              │
  │         │          ◄────────────┤             │              │
  │         │          │ logAdapter.poll()        │              │
  │         │          │ onSessionMessage()       │              │
  │         │          │ 处理业务    │             │              │
  │         │          │             │             │              │
  │         │          │             │    t4: ACK │              │
  │         │◄─────────┼─────────────┼─────────────┤              │
  │         │ appendPos│             │             │              │
  │         │          │             │             │              │
  │         │ t5: 更新 commit        │             │              │
  │         │ commitPos=1000         │             │              │
  │         │          │             │             │              │
  │         │          │ t6: Service │             │              │
  │         │          │ 继续消费     │             │              │
  │         │          │             │             │              │
  │  t7: 响应│          │             │             │              │
  │◄────────┤          │             │             │              │
  │ "OK"    │          │             │             │              │

总耗时：快照时间(1-5秒) + 正常处理(5-10ms) = 1-5秒
```

---

## 关键源码位置

| 功能 | 源码位置 | 关键方法 |
|------|----------|----------|
| **快照触发** | `ConsensusModuleAgent.java:2477` | `leaderWork()` → 检测快照条件 |
| **快照状态切换** | `ConsensusModuleAgent.java:2550` | `state(SNAPSHOT)` → 暂停接收 |
| **快照执行** | `ClusteredServiceAgent.java:1278` | `onTakeSnapshot()` |
| **日志写入** | `LogPublisher.java:180` | `appendMessage()` |
| **Archive 录制** | `ArchiveConductor.java:533` | `startRecording()` |
| **Follower 接收** | `BoundedLogAdapter.java:68` | `poll()` |
| **commitPosition 更新** | `ConsensusModuleAgent.java:2000` | `updateMemberPosition()` |

---

## 常见问题 FAQ

### Q1: 快照期间消息会丢失吗？

**答**：不会！消息有多重保障：

1. **网络缓冲区**：请求先缓存在 Aeron 的 Image 缓冲区
2. **Archive 持久化**：消息写入日志后立即被 Archive 录制到磁盘
3. **Follower 副本**：所有 Follower 也有完整的日志副本
4. **重启恢复**：即使 Service down，Archive 中的日志仍在

### Q2: Archive 录制会影响性能吗？

**答**：影响很小（< 5%）：

- Archive 使用 **spy 订阅**（旁路监听，不影响正常流）
- 使用 **零拷贝**（mmap）机制
- 异步写入磁盘（不阻塞主线程）

### Q3: 如果 Follower 的 Archive 磁盘满了怎么办？

**答**：Archive 有多重保护：

1. **磁盘空间检查**：`isLowStorageSpace()` 定期检查
2. **录制暂停**：磁盘不足时拒绝新录制请求
3. **日志清理**：快照后自动删除旧日志
4. **告警机制**：触发 `errorHandler` 通知运维

### Q4: Leader 打快照时，Follower 也打快照吗？

**答**：取决于配置：

- **默认**：只有 Leader 打快照（`standbySnapshotEnabled=false`）
- **启用 Standby**：所有节点都打快照（`standbySnapshotEnabled=true`）

### Q5: Archive 的日志和快照有什么区别？

**答**：

| 维度 | Archive 日志 | 快照 |
|------|-------------|------|
| **内容** | 所有消息的完整历史 | 某个时间点的状态 |
| **大小** | 持续增长 | 固定大小 |
| **用途** | 重放历史、审计 | 快速恢复 |
| **录制方式** | 实时录制（spy 订阅） | 周期性触发 |
| **删除策略** | 快照后可删除旧日志 | 保留最近 N 个快照 |

---

## 总结

### 核心要点

1. **快照不阻塞日志复制**
   - Leader 打快照时，日志复制继续进行
   - Archive 持续录制日志
   - Follower 正常接收和归档

2. **消息多重保障**
   - 网络缓冲区：临时缓存
   - Archive 日志：持久化存储
   - Follower 副本：冗余备份

3. **Archive 的关键作用**
   - 所有节点都有完整日志副本
   - 即使 Service down，消息仍在 Archive
   - 重启后从 Archive 回放恢复

4. **快照对性能的影响**
   - 客户端：延迟 +1-5 秒（快照期间请求排队）
   - Archive：几乎无影响（spy 订阅 + 零拷贝）
   - Follower：无影响（继续正常工作）

---

**文档完成！这是 Aeron Cluster 最核心的机制之一。**
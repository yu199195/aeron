
# Aeron Cluster 深度解析 - 第四次分享

> 前置知识：已完成前三次分享（整体架构 + MediaDriver + Archive）
> 时间预估：75～90 分钟

---

## 目录

1. [Aeron Cluster 是什么](#1-aeron-cluster-是什么)
2. [四层架构](#2-四层架构)
3. [核心组件详解](#3-核心组件详解)
4. [启动流程](#4-启动流程)
5. [选举机制（Raft Election）](#5-选举机制raft-election)
6. [日志复制原理](#6-日志复制原理)
7. [快照机制](#7-快照机制)
8. [崩溃恢复流程](#8-崩溃恢复流程)
9. [客户端双向通信](#9-客户端双向通信)
10. [磁盘目录结构与数据文件](#10-磁盘目录结构与数据文件)
11. [关键设计总结](#11-关键设计总结)

---

## 1. Aeron Cluster 是什么

**Aeron Cluster** 是基于 **Raft 共识算法** 实现的**高性能容错分布式服务框架**，提供：

- **强一致性**：基于多数派提交的日志序列化
- **自动故障转移**：Leader 失效时自动选举新 Leader
- **确定性状态机**：相同日志 → 相同状态（保证所有节点一致）
- **快照 + 增量日志**：快速恢复，不需要全量重放
- **微秒级延迟**：基于 Aeron 的低延迟特性

### 1.1 核心价值

| 对比项 | ZooKeeper | etcd | Aeron Cluster |
|--------|-----------|------|---------------|
| P50 延迟 | 毫秒 | 毫秒 | **微秒** |
| GC 停顿影响 | 有 | 有 | **无** |
| 内置消息传递 | 否 | 否 | **是（Aeron）** |
| 快照机制 | 有 | 有 | **有（与 Archive 集成）** |

### 1.2 典型应用

- 金融交易系统（订单簿、撮合引擎）
- 分布式锁服务
- 强一致配置管理
- 微秒级延迟的状态机服务

---

## 2. 四层架构

```
┌───────────────────────────────────────────────────────────────────┐
│                  应用层（Application Layer）                       │
│          ClusteredService 接口实现（用户业务状态机）                │
│  onSessionMessage() / onTimerEvent() / onTakeSnapshot()           │
└───────────────────────────────────────────────────────────────────┘
                              ↕ IPC（log channel）
┌──────────────────────────────────────────────────────────────────┐
│                  共识层（Consensus Layer）                         │
│  ConsensusModule（选举 + 日志序列化 + 客户端会话）                 │
│  Election / RecordingLog / ClusterMember                          │
│  ClusteredServiceContainer（日志消费 + 业务驱动）                  │
└──────────────────────────────────────────────────────────────────┘
                              ↕ Archive IPC
┌──────────────────────────────────────────────────────────────────┐
│                  持久化层（Persistence Layer）                      │
│          Archive（日志录制 + 快照存储 + Follower 同步回放）         │
└──────────────────────────────────────────────────────────────────┘
                              ↕ MediaDriver
┌──────────────────────────────────────────────────────────────────┐
│                  传输层（Transport Layer）                          │
│          MediaDriver（UDP 单播/多播，节点间通信）                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## 3. 核心组件详解

### 3.1 组件职责矩阵

| 组件 | 所在进程 | 核心职责 |
|------|---------|---------|
| **ClusteredMediaDriver** | 每个 Cluster 节点 | 聚合启动器：MediaDriver + Archive + ConsensusModule |
| **ConsensusModule** | 每个节点 | 选举、日志序列化、客户端会话、与 Service 通信 |
| **ConsensusModuleAgent** | ConsensusModule 线程 | 共识状态机主循环（Leader/Follower 角色切换） |
| **Election** | 内嵌在 ConsensusModuleAgent | Raft 选举逻辑（状态机：INIT→CANVASS→NOMINATE→BALLOT→READY） |
| **LogPublisher** | Leader 的 ConsensusModuleAgent | 向 Followers 广播日志条目 |
| **ClusteredServiceContainer** | 每个节点 | 消费日志，驱动用户 ClusteredService |
| **ClusteredServiceAgent** | ClusteredServiceContainer 线程 | 订阅 logChannel，回调 onSessionMessage 等 |
| **RecordingLog** | 每个节点磁盘 | 持久化日志元数据（term、snapshot 条目） |

### 3.2 节点内部通信关系

```
单个节点内部（3个主要进程角色）：

ConsensusModule                     Archive
      │                                │
      │ ① 录制日志                      │
      │ (logChannel → Archive 录制)    │
      ├──────────────────────────────▶ │
      │                                │
      │ ② 快照录制                      │
      ├──────────────────────────────▶ │
      │                                │
      │ ③ commitPosition 同步           │
      │ (Counter，共享内存)              │
      ├──────────────────────────────▶ │
      │                                │
ClusteredServiceContainer           Archive
      │                                │
      │ ④ 订阅日志回放                  │
      ◀─────────────────────────────── │
      │ (logChannel，Archive 录制中)    │
      │                                │
      │ ⑤ appendPosition 上报          │
      ├──────────────────────────────▶ ConsensusModule
```

---

## 4. 启动流程

### 4.1 ClusteredMediaDriver 启动链路

```java
ClusteredMediaDriver.launch(driverCtx, archiveCtx, consensusCtx)
    │
    ├─ ① MediaDriver.launch(driverCtx)
    │      启动三大 Agent（Conductor/Sender/Receiver）
    │
    ├─ ② Archive.launch(archiveCtx
    │         .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
    │         .aeronDirectoryName(...))
    │      Archive 与 MediaDriver 共享 CnC 目录
    │
    └─ ③ ConsensusModule.launch(consensusModuleCtx
              .aeronDirectoryName(...))
           └─ ConsensusModuleAgent 启动
                ├─ 加载 RecordingLog（历史 term 和 snapshot 记录）
                ├─ 检查是否需要从快照恢复
                └─ 启动选举（Election）或直接进入 FOLLOWER
```

### 4.2 ClusteredServiceContainer 启动

```
ClusteredServiceContainer.launch(ctx)
    └─ ClusteredServiceAgent 启动
         ├─ 连接 ConsensusModule（IPC）
         ├─ 连接 Archive（IPC）
         ├─ 如果有快照：从快照恢复业务状态（onLoadSnapshot）
         ├─ 订阅 logChannel（日志消费频道）
         └─ 进入主循环：logAdapter.poll(commitPosition) → onSessionMessage
```

### 4.3 启动后的稳态

```
Leader 节点稳态：

客户端 ──ingress──▶ ConsensusModule
                          │
                    LogPublisher.appendMessage() ──logChannel──▶ Archive 录制
                          │                                      │
                          │ UDP multicast/unicast                ▼
                          └──────────────────────▶ Follower 节点（复制日志）
                          │
                    ClusteredServiceContainer
                          │
                    logAdapter.poll(commitPosition)
                          │
                    ClusteredService.onSessionMessage()
                          │
                    结果通过 ClientSession.offer() 回传客户端
```

---

## 5. 选举机制（Raft Election）

### 5.1 选举触发条件

```
触发 Election 的场景：
  ① 节点首次启动（无已知 Leader）
  ② Leader 心跳超时（leaderHeartbeatTimeoutNs，默认 10 秒）
  ③ 集群成员变更
```

### 5.2 Election 状态机

```
状态流转（Election.doWork() 每次 ConsensusModuleAgent.doWork() 都调用）：

┌──────┐
│ INIT │  初始化，决定自己是否是唯一节点
└──┬───┘
   ↓
┌─────────┐
│ CANVASS │  "拉票"阶段：向所有节点发 CANVASS_POSITION 消息
└──┬──────┘  收集其他节点的日志位置，判断自己是否有资格当 Leader
   ↓
┌──────────┐
│ NOMINATE │  提名阶段：如果自己的日志最新，发送 NOMINATE 请求
└──┬───────┘  等待其他节点的 NOMINATE 回应（超时重试）
   ↓
┌────────┐
│ BALLOT │  投票阶段：向其他节点发 VOTE 请求，收集选票
└──┬─────┘  收到大多数选票 → LEADER
   ↓        收到更高 term 的候选人 → FOLLOWER_BALLOT
┌───────┐
│ READY │  选举完成，成为 Leader 或 Follower
└───────┘

特殊路径：
  收到其他节点的 NOMINATE（已有候选人）→ 进入 FOLLOWER_BALLOT 状态，等待领票
  收到 CANVASS 响应，发现自己日志不是最新 → 等待其他节点发起选举
```

### 5.3 日志最新度判断（关键安全保证）

```java
// 谁的日志最新？
// 1. Term ID 更大的节点日志更新
// 2. 同 Term 下，logPosition 更大（日志更长）的节点更新

boolean isLogNewerThan(ClusterMember candidate) {
    return logLeadershipTermId > candidate.logLeadershipTermId ||
           (logLeadershipTermId == candidate.logLeadershipTermId &&
            logPosition > candidate.logPosition);
}
```

> **Raft 核心保证**：只有日志最新的节点才能成为 Leader，防止已提交的日志丢失。

### 5.4 多数派检测

```
3 节点集群（quorum = 2）：
  ✓ Leader + 1 Follower = 2 = quorum → 可提交
  ✗ 只有 Leader = 1 < quorum → 无法提交（脑裂保护）

5 节点集群（quorum = 3）：
  ✓ Leader + 2 Followers = 3 = quorum → 可提交
  允许同时故障 2 个节点

选票计数（包含自己）：
  int voteCount = 1; // 自己给自己投票
  for (ClusterMember member : clusterMembers) {
      if (member.vote() != null && member.vote()) voteCount++;
  }
  if (voteCount >= quorumSize) 成为 Leader;
```

---

## 6. 日志复制原理

### 6.1 状态机复制（核心思想）

```
核心原理：
  相同初始状态 + 相同顺序执行相同操作 = 相同最终状态

日志条目 = 操作序列（严格有序，全局唯一 position）
  ┌─────┬─────┬─────┬─────┬─────┐
  │ L1  │ L2  │ L3  │ L4  │ L5  │  logPosition 单调递增
  └─────┴─────┴─────┴─────┴─────┘
    ↑ 所有节点按相同顺序消费这些日志 → 状态一致
```

### 6.2 日志复制完整流程

```
客户端                  Leader                  Followers (×2)
──────                  ──────                  ──────────────
发送请求
  ─────────────────▶
                    ConsensusModule 接收
                    state == ACTIVE?
                    ingressAdapter.poll()
                    │
                    LogPublisher.appendMessage()
                    │ 写入 logChannel
                    ├────── UDP multicast ──────▶  Follower.logAdapter.poll()
                    │                                   │ Archive 录制到磁盘
                    │                              appendPosition 上报
                    │ ◀─── appendPosition ─────────────┤
                    │
                    ClusteredServiceAgent.poll()
                    │ 消费 logChannel（本节点）
                    │
                    计算 quorumPosition：
                    取各节点 appendPosition 排序后的第 quorumIndex 个
                    │
                    commitPosition.proposeMaxRelease(quorumPosition)
                    │ 更新 commitPosition Counter
                    │
                    ClusteredService.onSessionMessage()
                    │ 业务处理
                    │
                    ClientSession.offer(response)
                    │ 通过 egressPublication 回传
  ◀────────────────
```

### 6.3 commitPosition 与 quorum

```
3 节点示例：
  Leader:    appendPosition = 1000
  Follower1: appendPosition = 1000
  Follower2: appendPosition = 800

  排序后: [800, 1000, 1000]
  quorumIndex = 3/2 = 1（向下取整）
  quorumPosition = sorted[1] = 1000

  ✓ 大多数节点（Leader + Follower1）已持久化到 1000
  → commitPosition = 1000
  → Service 可以消费到 position 1000 的所有日志
```

---

## 7. 快照机制

### 7.1 为什么需要快照

```
问题：日志无限增长 → 恢复时需要重放所有日志 → 恢复时间过长

解决：定期打快照
  快照 = 某个 logPosition 时的完整业务状态（序列化后写入 Archive）
  恢复时：加载最新快照 + 重放快照之后的增量日志
```

### 7.2 快照触发流程

```
触发方式：
  ① 外部命令：ClusterControl.SNAPSHOT 触发
  ② 自动触发（可配置）

ConsensusModuleAgent.checkClusterControlToggle():
  state == ACTIVE?
  → appendAction(ClusterAction.SNAPSHOT, timestamp)  ← 写入日志（重要！）
  → state = SNAPSHOT  ← 切换状态，停止接收新请求

状态切换的影响：
  ACTIVE → SNAPSHOT:
    ✗ ingressAdapter.poll() 不再调用（停止接收客户端请求）
    ✓ 继续处理已写入日志的消息（ClusteredService 继续消费）
    ✓ 触发 onTakeSnapshot() 回调
```

### 7.3 快照写入流程

```
ConsensusModuleAgent（Leader）:
  ① appendAction(SNAPSHOT, timestamp) 写入日志
  ② 通知 ClusteredServiceContainer 准备快照

ClusteredServiceAgent:
  ③ onTakeSnapshot(publication)
       │ ClusteredService.onTakeSnapshot(publication)
       │   ← 用户实现：把业务状态序列化写入 publication
       │   ← 底层：publication.offer() → Archive 录制
       └─ 写完通知 ConsensusModule 快照完成

ConsensusModuleAgent:
  ④ 收到快照完成信号
  ⑤ RecordingLog.appendSnapshot(recordingId, logPosition, ...)
       ← 在 RecordingLog 中记录快照位置
  ⑥ state = ACTIVE  ← 恢复接收客户端请求
```

### 7.4 RecordingLog 结构

```
RecordingLog（每个节点磁盘，recording.log 文件）：

条目类型：
┌────────────────────────────────────────────────────────────┐
│ TERM 条目：                                                 │
│   leadershipTermId | logPosition | timestamp | ...        │
│   标记一个 term 的开始（Leader 选举后写入）                  │
├────────────────────────────────────────────────────────────┤
│ SNAPSHOT 条目：                                             │
│   recordingId | logPosition | timestamp | serviceId | ...  │
│   标记某个 Archive recording 是快照，以及对应的 logPosition │
└────────────────────────────────────────────────────────────┘

用途：
  ① 节点启动时：找到最新的 SNAPSHOT 条目 → 决定从哪里恢复
  ② 日志截断：清理 snapshot 之前的日志条目（减少磁盘占用）
  ③ Follower 追赶：决定是需要重放日志还是先传 snapshot
```

---

## 8. 崩溃恢复流程

### 8.1 节点重启恢复

```
ClusteredServiceAgent 启动时：

① 读取 RecordingLog
   找到最新的 SNAPSHOT 条目 → snapshotRecordingId, snapshotLogPosition

② 从 Archive 回放快照
   archive.startReplay(snapshotRecordingId, 0, -1, replayChannel)
   ClusteredService.onLoadSnapshot(image)
   ← 用户实现：从 image 读取数据，恢复业务状态

③ 订阅 logChannel，从 snapshotLogPosition 之后开始消费
   ← 只重放快照之后的增量日志（通常很少）

④ 追赶到 commitPosition
   ← 追赶完成后，节点才能参与共识

恢复时间 = 加载快照时间 + 回放增量日志时间
（不需要重放所有历史日志）
```

### 8.2 详细恢复示例

```
场景：系统运行了很长时间

日志历史（从 position 0 开始）：
  pos 0 → pos 500000: 5000 条消息
                       ↑
                    position=500000 时打了快照

  pos 500000 → pos 700000: 2000 条增量消息
                             ↑
                          当前 commitPosition

节点崩溃后重启：
  ① 加载快照（position=500000 时的完整状态）
     耗时：取决于快照大小，通常毫秒级
  ② 从 500000 开始回放增量日志（2000 条）
     耗时：取决于增量大小，通常很快
  ③ 恢复完成，状态与其他节点一致

如果没有快照：
  ① 从 position=0 开始回放 7000 条消息
     耗时：可能很长（分钟级）
```

---

## 9. 客户端双向通信

### 9.1 消息流向

```
客户端 → Cluster（Ingress）：
  client → ingressPublication
         → ConsensusModule（处理所有节点请求）
         → 写入 logChannel

Cluster → 客户端（Egress）：
  Leader ClusteredService.onSessionMessage()
         → ClientSession.offer(responseBuffer)
         → egressPublication → 回传给客户端
```

### 9.2 双向通信示例

```java
// 服务端实现
public class SimpleClusteredService implements ClusteredService {

    @Override
    public void onSessionMessage(
        ClientSession session,     // 哪个客户端发来的
        long timestamp,
        DirectBuffer buffer,       // 消息内容
        int offset, int length,
        Header header)
    {
        // 处理请求
        String request = new String(buffer.byteArray(), offset, length);
        String response = "Echo: " + request;

        // 回复给客户端（通过 Egress）
        responseBuffer.putBytes(0, response.getBytes());
        while (session.offer(responseBuffer, 0, response.length()) < 0) {
            // 背压处理：重试
            Thread.yield();
        }
    }
}
```

### 9.3 Leader 广播 vs 定向回复

```
定向回复（只回复发出请求的那个客户端）：
  session.offer(buffer, offset, length)

广播给所有客户端（推送模式）：
  for (ClientSession session : cluster.clientSessions()) {
      if (session.isConnected()) {
          session.offer(eventBuffer, 0, eventLength);
      }
  }

注意：Follower 节点不处理请求，也不回复
       只有 Leader 的 ClusteredService 才会执行 onSessionMessage
```

---

## 10. 磁盘目录结构与数据文件

### 10.1 节点完整目录结构

每个 Cluster 节点有三个独立目录，分别由不同组件管理：

```
<baseDir>/
├── aeron/                          ← aeronDir（MediaDriver 目录）
│   ├── cnc.dat                     ← Command & Control mmap（Client ↔ Driver 通信）
│   └── publications/
│       └── <id>.logbuffer          ← ingress / egress / log Channel 的 LogBuffer
│
├── archive/                        ← archiveDir（Archive 目录）
│   ├── archive.catalog             ← 所有录制的 RecordingDescriptor 元数据
│   ├── archive.catalog.bak         ← Catalog 备份（原子更新保障）
│   ├── archive-mark.dat            ← Archive 版本标记 & 启动状态（崩溃检测）
│   │
│   ├── 0-0.rec                     ← Cluster 日志录制（logChannel 数据，recordingId=0）
│   ├── 0-16777216.rec              ← 同一日志录制的下一个 Segment（满 segmentLength 后滚动）
│   ├── 1-0.rec                     ← ConsensusModule 快照（recordingId=1，serviceId=-1）
│   └── 2-0.rec                     ← ClusteredService 快照（recordingId=2，serviceId=0）
│
└── cluster/                        ← clusterDir（Cluster 专属目录）
    ├── recording.log               ← RecordingLog：TERM + SNAPSHOT 条目元数据
    └── cluster-mark.dat            ← Cluster 版本标记 & 启动互斥状态
```

---

### 10.2 cluster/recording.log — 所有恢复决策的依据

**格式**：定长记录序列，SBE 编码，追加写入，不修改历史条目。

**TERM 条目**（`ENTRY_TYPE_TERM = 0`）— 每次新 Leader 当选后写入：

```
┌──────────────────────────┬──────────────────────────────────────────────────────┐
│ recordingId (long)       │ Archive 中该 term 日志流的录制 ID                      │
│ leadershipTermId (long)  │ term 编号（全局单调递增，对应 Raft 的 term）            │
│ termBaseLogPosition(long)│ 该 term 开始时的 logPosition（= 上一 term 结束位置）    │
│ logPosition (long)       │ 该 term 目前已确认的日志长度（节点运行时动态更新）       │
│ timestamp (long)         │ 该 term 开始时间戳（epoch ms）                         │
│ leaderMemberId (int)     │ 该 term 的 Leader 节点 memberId                       │
│ type = 0                 │ ENTRY_TYPE_TERM                                       │
└──────────────────────────┴──────────────────────────────────────────────────────┘
```

**SNAPSHOT 条目**（`ENTRY_TYPE_SNAPSHOT = 1`）— 每次快照写入一对（ConsensusModule + 每个用户 Service 各一条）：

```
┌──────────────────────────┬──────────────────────────────────────────────────────┐
│ recordingId (long)       │ Archive 中该快照录制的 ID（恢复时 startReplay 用）      │
│ leadershipTermId (long)  │ 打快照时所在的 term                                    │
│ termBaseLogPosition(long)│ 快照所在 term 的起始 logPosition                       │
│ logPosition (long)       │ 打快照时的精确 logPosition（恢复后从此处重放增量日志）   │
│ timestamp (long)         │ 快照时间戳（epoch ms）                                 │
│ serviceId (int)          │ -1 = ConsensusModule 自身；0 / 1 / 2… = 用户 Service  │
│ type = 1                 │ ENTRY_TYPE_SNAPSHOT                                   │
└──────────────────────────┴──────────────────────────────────────────────────────┘
```

**recording.log 内容示例**（两次快照、两次选举后）：

```
Index  Type      recordingId  leadershipTermId  logPosition  serviceId  说明
──────────────────────────────────────────────────────────────────────────────────
  0    TERM      0            0                 0            -          term=0 启动
  1    SNAPSHOT  1            0                 500000       -1         CM  快照 @500000
  2    SNAPSHOT  2            0                 500000        0         Svc 快照 @500000
  3    TERM      0            1                 500000        -         term=1（新 Leader）
  4    SNAPSHOT  3            1                 700000       -1         CM  快照 @700000
  5    SNAPSHOT  4            1                 700000        0         Svc 快照 @700000
                                                ↑
                               恢复时取最新：recordingId=3/4，从 logPosition=700000 继续回放
```

---

### 10.3 archive/archive.catalog — 录制元数据索引

Cluster 环境下 catalog 中常见的几类录制条目（RecordingDescriptor）：

```
recordingId=0   日志录制（贯穿集群整个生命周期）
  startPosition=0       stopPosition=-1（录制中，不停止）
  streamId=100          channel="aeron:ipc?term-length=..."
  sessionId=<自动>       sourceIdentity="<本机地址>"

recordingId=1   ConsensusModule 快照（第1次，serviceId=-1）
  startPosition=0       stopPosition=<快照字节数>
  streamId=106          channel="aeron:ipc?..."

recordingId=2   ClusteredService 快照（第1次，serviceId=0）
  startPosition=0       stopPosition=<快照字节数>
  streamId=107          channel="aeron:ipc?..."

recordingId=3   ConsensusModule 快照（第2次）
  ...
```

> `streamId` 编码了录制类型：日志流用 `LOG_STREAM_ID`（默认 100），快照流根据 `serviceId` 偏移计算（如 `SNAPSHOT_STREAM_ID + serviceId + 1`）。

完整 RecordingDescriptor 字段见 Archive 分享，Cluster 关注的核心字段：

| 字段 | Cluster 含义 |
|------|-------------|
| `recordingId` | RecordingLog 中 TERM / SNAPSHOT 条目引用的 ID |
| `stopPosition` | `-1` = 录制中（日志流）；正数 = 快照大小 |
| `startPosition` | 快照通常为 0；日志录制为该 term 的 termBaseLogPosition |
| `streamId` | 区分日志流 vs 哪个 serviceId 的快照 |

---

### 10.4 archive/*.rec — 数据文件内容

**日志录制文件（`0-0.rec`、`0-16777216.rec` …）**

```
内容：Aeron Data Frame 序列，每帧 payload 是 SBE 编码的 Cluster 协议消息

典型帧序列（ClusteredServiceAgent 按此顺序逐帧消费 → 确定性状态机）：

  Frame: SESSION_OPEN    { sessionId=1, responseStreamId, responseChannel, ... }
  Frame: TIMER_REGISTER  { correlationId=..., deadline=... }
  Frame: SESSION_MESSAGE { sessionId=1, timestamp=..., payload=<业务数据> }
  Frame: SESSION_MESSAGE { sessionId=2, timestamp=..., payload=<业务数据> }
  Frame: ACTION          { type=SNAPSHOT, logPosition=500000 }   ← 快照标记点
  Frame: SESSION_MESSAGE { sessionId=1, timestamp=..., payload=<增量数据> }
  Frame: SESSION_CLOSE   { sessionId=1, ... }

所有节点按相同顺序消费以上帧 → 回调 onSessionOpen / onSessionMessage /
onSessionClose → 相同的最终状态（确定性状态机的根本保证）
```

**快照录制文件（`1-0.rec`、`2-0.rec` …）**

```
ConsensusModule 快照（serviceId=-1，框架自动写入）：
  ┌──────────────────────────────────────────────────────┐
  │ activeSessionCount (int)                              │
  │ [ClusterSession × N]                                 │
  │   { sessionId, timeOfLastActivityMs, state, ... }    │
  │ pendingTimerCount (int)                               │
  │ [Timer × M]                                          │
  │   { correlationId, deadlineMs }                      │
  │ currentLeadershipTermId (long)                        │
  │ commitPosition (long)                                 │
  └──────────────────────────────────────────────────────┘

ClusteredService 快照（serviceId=0，用户实现 onTakeSnapshot 写入）：
  ┌──────────────────────────────────────────────────────┐
  │ 格式完全由业务代码决定，框架只负责存储原始字节         │
  │                                                      │
  │ 示例（订单簿服务）：                                  │
  │   orderCount (int)                                   │
  │   [Order × orderCount]                               │
  │     { orderId, side, price, qty, status, ... }       │
  └──────────────────────────────────────────────────────┘
```

---

### 10.5 cluster/cluster-mark.dat — 节点互斥与 Term 保护

```
主要字段：
┌──────────────────────────┬─────────────────────────────────────────────────────┐
│ candidateTermId (long)   │ 本节点参与过的最高 term（崩溃重启后不得使用更低的 term）│
│ memberId (int)           │ 本节点的 memberId                                    │
│ clusterId (int)          │ 集群 ID（防止不同集群的节点误用同一 clusterDir）        │
│ activityTimestamp (long) │ 最近一次刷新时间（定期更新，用于崩溃检测）              │
└──────────────────────────┴─────────────────────────────────────────────────────┘

启动时检查逻辑：
  old_ts = cluster-mark.dat.activityTimestamp
  if (now - old_ts < clusterMarkFileTimeout):
      → 上次进程仍在活跃 → 抛出异常，拒绝启动（防止同目录双进程 / 脑裂）
  else:
      → 上次进程已死 → 可以接管 clusterDir，覆写 activityTimestamp 继续启动

candidateTermId 的关键作用：
  节点在 BALLOT 阶段写盘 candidateTermId
  即使在写盘后、投票完成前崩溃，重启后也能读到已承诺的最高 term
  → Election 从 max(RecordingLog.maxTermId, candidateTermId) 起步
  → 不会以过期 term 参与投票 → 防止已提交日志被旧 Leader 覆盖
```

---

### 10.6 各文件与崩溃恢复的映射关系

```
节点重启时，文件读取顺序与决策逻辑：

① cluster/cluster-mark.dat
   └─ 验证进程唯一性（activityTimestamp 检查）
   └─ 获取 candidateTermId → 选举起点下限

② cluster/recording.log
   └─ 扫描所有条目，找：
      ├─ 最新 SNAPSHOT 条目组（最高 logPosition）
      │    snapshotRecordingId (serviceId=-1)   ← CM  快照
      │    snapshotRecordingId (serviceId= 0)   ← Svc 快照
      │    snapshotLogPosition                  ← 恢复后增量回放起点
      └─ 最新 TERM 条目
           logRecordingId                       ← 日志流录制 ID
           termBaseLogPosition                  ← 该 term 日志的起始偏移

③ archive/archive.catalog
   └─ 用 snapshotRecordingId 查找对应 .rec 文件路径和起止 position
   └─ 用 logRecordingId      查找日志录制的 segment 文件列表

④ archive/*.rec 文件（实际数据读取）
   ├─ startReplay(snapshotRecordingId, 0, -1)
   │    → ReplaySession 读盘 → onLoadSnapshot(image) → 恢复业务状态
   └─ startReplay(logRecordingId, snapshotLogPosition, -1)
        → ReplaySession 读盘 → onSessionMessage() 重放增量日志
        → 追赶到 commitPosition → 重新加入集群

恢复完成判断：
  logAdapter.poll() 追赶到 commitPosition
  → ClusteredServiceAgent 通知 ConsensusModule "我已就绪"
  → 节点重新参与共识（可参与投票，可接收新日志）
```

---

## 11. 关键设计总结

### 11.1 Raft 核心保证

```
Aeron Cluster 的 Raft 实现提供以下保证：

① 选举安全性：任意 term 最多一个 Leader
   ← 通过多数派投票，不可能两个节点同时获得多数票

② 日志匹配：如果两个日志在某 position 有相同的 term，
            则该 position 之前的所有日志相同
   ← logLeadershipTermId 标识每条日志属于哪个 term

③ Leader 完整性：如果一条日志被提交，
               所有后续 Leader 一定包含该日志
   ← 选举时只有日志最新的节点才能当选

④ 状态机安全：如果某 server 已将某 logPosition 的日志应用到状态机，
             则其他 server 在该位置应用的日志必须相同
```

### 11.2 与 Archive 的协作关系

```
Archive 在 Cluster 中的三个角色：

① 日志持久化存储：
   ConsensusModule → logChannel → Archive 录制
   停电重启后，日志不丢失

② 快照存储：
   ClusteredService → Archive 录制（快照数据）
   RecordingLog 记录 snapshotRecordingId + logPosition

③ Follower 日志同步：
   新加入的 Follower 需要追赶 Leader
   Leader Archive → 回放历史日志 → Follower
   Archive.startReplay(recordingId, fromPosition, ...)
```

### 11.3 确定性的重要性

```
ClusteredService.onSessionMessage 必须是确定性的：

✓ 相同输入 → 相同输出（不依赖随机数、当前时间、外部 IO）
✓ 时间通过 cluster.time() 获取（集群确定性时钟，所有节点相同）
✓ 随机需求通过 cluster.random() 获取（确定性随机数）
✗ 不能有线程竞争（单线程执行）
✗ 不能有外部 IO（如数据库查询）

为什么必须确定性？
  快照恢复时，从快照 + 日志重放，
  必须得到完全相同的状态，
  否则各节点状态会发散，快照恢复后状态不一致。
```

### 11.4 性能关键路径

```
关键路径延迟分析（3节点局域网集群）：

客户端 offer → Leader 接收：~5μs（UDP）
Leader LogPublisher → Follower 接收：~5μs（UDP multicast）
Follower Archive 录制（写盘）：~50～200μs（取决于磁盘）
appendPosition 上报 → Leader 收到：~5μs
quorum 计算 + commitPosition 更新：~1μs
ClusteredService.onSessionMessage：业务逻辑时间
Leader → 客户端 egress：~5μs

端到端 P50 ≈ 100～500μs（含一次磁盘写入的 quorum）

优化手段：
  ① 使用 SSD（降低写盘延迟）
  ② 节点绑核（降低调度延迟）
  ③ 使用 BusySpinIdleStrategy（降低 Agent 轮询延迟）
  ④ 调整 logChannel 为 UDP multicast（一次发送给所有 Follower）
```

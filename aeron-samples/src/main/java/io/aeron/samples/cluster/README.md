# Aeron Cluster 3节点集群 Demo

这是一个完整的 Aeron Cluster 示例，演示如何启动 3 节点集群并进行消息通信。

---

## 架构说明

### 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      Aeron Cluster 架构                          │
└─────────────────────────────────────────────────────────────────┘

          ┌──────────────┐
          │ ClusterClient│  ← 生产者（Producer）
          │  (发送消息)   │
          └──────┬───────┘
                 │ offer()
                 ↓
    ┌────────────────────────────────┐
    │    ClusterNode (Leader)        │
    │  ┌──────────────────────────┐  │
    │  │  ConsensusModule         │  │  ← Raft 协议处理
    │  │  - 接收客户端请求         │  │
    │  │  - 序列化到日志           │  │
    │  │  - 复制到 Followers       │  │
    │  └──────────────────────────┘  │
    └────────┬───────────────────────┘
             │ replicate
             ↓
    ┌────────────────────────────────┐
    │  ClusterNode (Follower 1 & 2)  │
    │  ┌──────────────────────────┐  │
    │  │  ConsensusModule         │  │
    │  │  - 复制日志               │  │
    │  │  - 等待提交通知           │  │
    │  └──────────────────────────┘  │
    └────────┬───────────────────────┘
             │ committed
             ↓
    ┌────────────────────────────────┐
    │     All 3 ClusterNodes         │
    │  ┌──────────────────────────┐  │
    │  │  SimpleClusteredService  │  │  ← 消费者（Consumer）
    │  │  - onSessionMessage()    │  │
    │  │  - 处理业务逻辑           │  │
    │  │  - 发送响应（Leader）     │  │
    │  └──────────────────────────┘  │
    └────────┬───────────────────────┘
             │ response (only Leader)
             ↓
          ┌──────────────┐
          │ ClusterClient│  ← 接收响应
          │  (收到 Echo)  │
          └──────────────┘
```

### 关键角色

| 组件 | 角色 | 职责 |
|------|------|------|
| **ClusterClient** | 生产者（Producer）| 发送消息到集群 |
| **ConsensusModule** | 协调者 | Raft 选举、日志复制、会话管理 |
| **SimpleClusteredService** | 消费者（Consumer）| 接收并处理消息（在所有节点上运行）|
| **Leader** | 服务提供者 | 接收客户端请求、发送响应 |
| **Followers** | 数据副本 | 复制日志、保持状态一致 |

### 消息流转

```
1. 客户端发送消息
   ClusterClient.sendMessage("Hello")
   ↓

2. Leader 接收并序列化
   ConsensusModule (Leader) → 写入日志 → recordingId=123
   ↓

3. 复制到 Followers
   Leader → Follower1: AppendPosition(logPosition=100)
   Leader → Follower2: AppendPosition(logPosition=100)
   ↓

4. 所有节点确认
   Follower1 → Leader: ACK(logPosition=100)
   Follower2 → Leader: ACK(logPosition=100)
   ↓

5. Leader 提交并广播
   Leader → All: CommitPosition(logPosition=100)
   ↓

6. 所有节点消费
   Node0: SimpleClusteredService.onSessionMessage("Hello") → globalCount=1
   Node1: SimpleClusteredService.onSessionMessage("Hello") → globalCount=1
   Node2: SimpleClusteredService.onSessionMessage("Hello") → globalCount=1
   ↓

7. Leader 发送响应
   Leader → ClusterClient: "[Echo #1] Hello (全局: 1, 本会话: 1)"
```

**关键点**：
- **生产者**：`ClusterClient` 发送消息
- **消费者**：`SimpleClusteredService.onSessionMessage()` 在所有 3 个节点上被调用
- **确定性**：所有节点以相同顺序处理相同消息，保证状态一致
- **响应路径**：只有 Leader 发送响应给客户端

---

## 文件说明

| 文件 | 说明 |
|------|------|
| `SimpleClusteredService.java` | **集群服务实现**（消费者）<br>- 接收消息：`onSessionMessage()`<br>- Echo 回显功能<br>- 全局消息计数器<br>- 快照保存/恢复 |
| `ClusterNode.java` | **节点启动器**<br>- 启动 MediaDriver、Archive、ConsensusModule<br>- 启动 ClusteredServiceContainer |
| `ClusterClient.java` | **集群客户端**（生产者）<br>- 连接到集群<br>- 发送消息<br>- 接收响应 |

---

## 使用步骤

### 1. 启动 3 个集群节点

在 **3 个不同的终端** 中分别执行：

```bash
# 终端 1：启动节点 0
cd /Users/yu.xiao/Documents/github/aeron
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterNode --args="0"

# 终端 2：启动节点 1
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterNode --args="1"

# 终端 3：启动节点 2
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterNode --args="2"
```

**预期输出**（节点 0 示例）：
```
========================================
  启动 Aeron Cluster Node 0
========================================
集群成员: 0,localhost:20000,...
工作目录: .../aeron-cluster-demo/node0

[Node 0] 所有组件已启动完成
[Node 0] 等待关闭信号（Ctrl+C）...
```

**选举过程**（大约 3-5 秒后）：
```
  [Service] ClusteredService 已启动
  [ConsensusModule] Election: CANVASS → NOMINATE → CANDIDATE_BALLOT
  [ConsensusModule] *** 本节点已成为 Leader ***
  [Service] 新的领导任期: termId=1, leaderId=0
```

### 2. 启动客户端发送消息

在 **第 4 个终端** 执行：

```bash
# 发送 5 条消息
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterClient --args="5"
```

**客户端输出**：
```
========================================
  Aeron Cluster Client Demo
========================================
Ingress 端点: localhost:20000,localhost:20010,localhost:20020
发送消息数量: 5

[Client] 启动 MediaDriver...
[Client] 连接到集群...
[Client] 已连接到集群，会话 ID: 12345

[Client] 发送消息: Hello from Client! Message #1
[Client] 收到响应: [Echo #1] Hello from Client! Message #1 (全局: 1, 本会话: 1)

[Client] 发送消息: Hello from Client! Message #2
[Client] 收到响应: [Echo #2] Hello from Client! Message #2 (全局: 2, 本会话: 2)

...

[Client] 所有消息已发送完成
[Client] 关闭连接...
```

**节点 0（Leader）输出**：
```
  [Service] 会话打开: sessionId=12345, timestamp=...
  [Service] 已回复: 欢迎连接到集群! sessionId=12345

  [Service] 收到消息 #1 (session #1): Hello from Client! Message #1
  [Service] 已回复: [Echo #1] Hello from Client! Message #1 (全局: 1, 本会话: 1)

  [Service] 收到消息 #2 (session #2): Hello from Client! Message #2
  [Service] 已回复: [Echo #2] Hello from Client! Message #2 (全局: 2, 本会话: 2)

  ...
```

**节点 1 & 2（Followers）输出**：
```
  [Service] 会话打开: sessionId=12345, timestamp=...
  [Service] 已回复: 欢迎连接到集群! sessionId=12345

  [Service] 收到消息 #1 (session #1): Hello from Client! Message #1
  [Service] 已回复: [Echo #1] ...  ← 注意：Follower 也会调用 sendResponse，但不会真正发送

  [Service] 收到消息 #2 (session #2): Hello from Client! Message #2
  ...
```

> **重要**：所有 3 个节点的 `SimpleClusteredService.onSessionMessage()` 都会被调用，保证状态一致性！

### 3. 验证状态一致性

在所有节点的输出中，你会看到相同的计数器：

```
Node 0: globalMessageCount=5, sessionMessageCounts={12345=5}
Node 1: globalMessageCount=5, sessionMessageCounts={12345=5}
Node 2: globalMessageCount=5, sessionMessageCounts={12345=5}
```

### 4. 测试故障转移

**步骤**：
1. 启动 3 个节点（节点 0 成为 Leader）
2. 启动客户端，发送 2 条消息
3. **关闭节点 0**（Ctrl+C）
4. 等待 3-5 秒，节点 1 或 2 会被选为新 Leader
5. 再次运行客户端，发送 3 条消息

**预期结果**：
- 新 Leader 的 `globalMessageCount` 从 3 开始（继承了之前的状态）
- 客户端自动连接到新 Leader
- 所有节点状态保持一致

---

## 集群配置

### 端口分配

| 节点 | Ingress | Consensus | Log | Replication | Archive |
|------|---------|-----------|-----|-------------|---------|
| Node 0 | 20000 | 20001 | 20002 | 20003 | 8010 |
| Node 1 | 20010 | 20011 | 20012 | 20013 | 8011 |
| Node 2 | 20020 | 20021 | 20022 | 20023 | 8012 |

**端口说明**：
- **Ingress**：客户端连接端口（客户端发送消息到此端口）
- **Consensus**：节点间通信端口（选举、心跳）
- **Log**：日志复制端口（Leader → Followers）
- **Replication**：日志追赶端口（Follower 从 Leader 追赶历史日志）
- **Archive**：Archive 控制端口（录制/回放）

### 工作目录结构

```
aeron-cluster-demo/
├─ node0/
│   ├─ aeron-media-driver/          # MediaDriver CnC 文件
│   │   ├─ cnc.dat
│   │   └─ ...
│   ├─ aeron-archive/               # Archive 录制文件
│   │   ├─ recording-0.dat
│   │   └─ recording-log.dat
│   └─ aeron-cluster/               # Cluster 元数据
│       ├─ cluster-mark.dat
│       └─ recording-log.dat
├─ node1/ (同上)
└─ node2/ (同上)
```

---

## 高级特性

### 1. 快照机制

SimpleClusteredService 演示了快照的保存与恢复：

```java
@Override
public void onTakeSnapshot(ExclusivePublication snapshotPublication)
{
    // 1. 保存全局计数
    responseBuffer.putLong(0, globalMessageCount);
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

    // 2. 保存会话数量
    responseBuffer.putInt(0, sessionMessageCounts.size());
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Integer.BYTES);

    // 3. 保存每个会话的计数
    sessionMessageCounts.forEach((sessionId, count) -> ...);
}
```

**触发快照**：
- 配置的日志长度达到阈值（默认 1MB）
- 节点重启前自动保存
- 管理员手动触发

**恢复快照**：
```java
@Override
public void onStart(Cluster cluster, Image snapshotImage)
{
    if (snapshotImage != null)
    {
        loadSnapshot(snapshotImage);  // 恢复 globalMessageCount 和 sessionMessageCounts
    }
}
```

### 2. 会话管理

```java
@Override
public void onSessionOpen(ClientSession session, long timestamp)
{
    // 初始化会话计数器
    sessionMessageCounts.put(session.id(), 0L);

    // 发送欢迎消息
    sendResponse(session, "欢迎连接到集群! sessionId=" + session.id());
}

@Override
public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason)
{
    // 清理会话计数器
    sessionMessageCounts.remove(session.id());
}
```

### 3. 确定性约束

**允许的操作**（在 `onSessionMessage` 中）：
- ✅ 修改业务状态（`globalMessageCount++`）
- ✅ 使用 `cluster.time()` 获取确定性时间
- ✅ 调用 `cluster.scheduleTimer()` 调度定时器

**禁止的操作**：
- ❌ 使用 `System.currentTimeMillis()`（非确定性）
- ❌ 使用 `new Random()`（未提供种子）
- ❌ 网络 IO（如调用外部 API）
- ❌ 在 `onStart`、`onTakeSnapshot` 中发送消息

---

## 常见问题

### Q1：为什么 Follower 也会调用 `onSessionMessage()`？

**A**：这是 Aeron Cluster 的核心设计——**确定性状态机复制（Deterministic State Machine Replication）**。

- 所有节点按 **相同顺序** 处理 **相同消息**
- 保证所有节点的 **业务状态完全一致**
- 即使 Leader 宕机，新 Leader 也能无缝接管（状态已同步）

### Q2：那响应消息会发送 3 次吗？

**A**：不会！虽然所有节点都调用 `sendResponse()`，但只有 **Leader 的响应会真正发送给客户端**。

- Leader：`session.offer()` 成功发送给客户端
- Followers：`session.offer()` 内部会检测到自己不是 Leader，不发送消息

### Q3：客户端如何知道谁是 Leader？

**A**：客户端连接时配置了所有节点的 ingress 端点：
```java
.ingressEndpoints("localhost:20000,localhost:20010,localhost:20020")
```

- 客户端尝试连接所有节点
- 非 Leader 节点会返回 **重定向响应**，告知 Leader 地址
- 客户端自动连接到 Leader

### Q4：如何扩展到生产环境？

**修改配置**：
```java
// ClusterNode.java
private static final String CLUSTER_MEMBERS =
    "0,node0.example.com:20000,node0.example.com:20001,...|" +
    "1,node1.example.com:20010,node1.example.com:20011,...|" +
    "2,node2.example.com:20020,node2.example.com:20021,...";

private static final String BASE_DIR = "/data/aeron-cluster";
```

**调整超时**：
```java
consensusModuleContext
    .leaderHeartbeatIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
    .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(10))
    .electionTimeoutNs(TimeUnit.SECONDS.toNanos(5));
```

---

## 监控与调试

### 查看集群状态

```bash
# 使用 aeron-stat 查看计数器
aeron-stat -f /path/to/node0/aeron-media-driver

# 关键计数器：
# - cluster-role: 0=Follower, 1=Candidate, 2=Leader
# - cluster-commit-pos: 已提交的日志位置
# - cluster-leadership-term-id: 当前 term ID
```

### 查看日志

```bash
# ConsensusModule 日志（如果配置了 SLF4J）
tail -f logs/node0-consensus.log

# Archive 录制文件
ls -lh aeron-cluster-demo/node0/aeron-archive/
```

### 调试模式

在 `ClusterNode.java` 中添加：
```java
System.setProperty("aeron.cluster.debug", "true");
System.setProperty("aeron.event.log", "all");
```

---

## 性能调优

### 1. 禁用 fsync（测试环境）

```java
archiveContext.fileSyncLevel(0);  // 0 = 禁用，1 = 正常，2 = 元数据+数据
```

### 2. 增大网络缓冲区

```java
mediaDriverContext
    .socketSndbufLength(2 * 1024 * 1024)  // 2MB 发送缓冲区
    .socketRcvbufLength(2 * 1024 * 1024); // 2MB 接收缓冲区
```

### 3. 使用专用线程模式

```java
// 当前：SHARED 模式（所有组件共享一个线程）
// 高性能：DEDICATED 模式（每个组件独立线程）

mediaDriverContext.threadingMode(ThreadingMode.DEDICATED);
archiveContext.threadingMode(ArchiveThreadingMode.DEDICATED);
```

---

## 总结

这个 Demo 展示了 Aeron Cluster 的核心概念：

1. **生产者-消费者模型**
   - 生产者：`ClusterClient` 发送消息
   - 消费者：`SimpleClusteredService` 在所有节点上处理消息

2. **确定性状态复制**
   - 所有节点按相同顺序处理相同消息
   - 保证业务状态完全一致

3. **高可用性**
   - 3 节点集群，容忍 1 节点故障
   - 自动选举新 Leader，无单点故障

4. **持久化与恢复**
   - Archive 录制所有日志
   - 快照机制快速恢复状态

如需进一步了解，请参考：
- [Aeron Cluster 官方文档](https://github.com/real-logic/aeron/wiki/Cluster-Tutorial)
- [源码注释](https://github.com/real-logic/aeron)

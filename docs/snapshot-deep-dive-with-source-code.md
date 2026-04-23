# 快照机制深度解析：为什么需要暂停？快照里有什么数据？

本文档用源码和具体例子解答两个核心问题：
1. **为什么打快照需要暂停接收请求？**（源码位置标注）
2. **从快照恢复为什么会快？快照里到底有没有数据？**

---

## 问题1：为什么打快照需要暂停接收请求？

### 答案：保证快照的一致性（快照必须对应一个精确的日志位置）

### 源码分析

#### 1.1 ACTIVE 状态下接收客户端请求

**位置**：`ConsensusModuleAgent.java:2398-2408`

```java
private int consensusWork(final long timestamp, final long nowNs)
{
    int workCount = 0;

    if (Cluster.Role.LEADER == role)
    {
        // ✅ 只有在 ACTIVE 状态下才处理客户端请求
        if (ConsensusModule.State.ACTIVE == state)  // ← 关键条件
        {
            workCount += timerService.poll(timestamp);      // 处理定时器
            for (final PendingServiceMessageTracker tracker : pendingServiceMessageTrackers)
            {
                workCount += tracker.poll();                // 处理 Service 消息
            }
            workCount += ingressAdapter.poll();  // ← 这里接收客户端请求！
        }

        workCount += updateLeaderPosition(nowNs);
    }
    // ...
}
```

**关键**：
- ✅ `if (ConsensusModule.State.ACTIVE == state)` - 只有 ACTIVE 状态才调用 `ingressAdapter.poll()`
- ✅ `ingressAdapter.poll()` - 从客户端 ingress channel 读取新消息

#### 1.2 切换到 SNAPSHOT 状态

**位置**：`ConsensusModuleAgent.java:2476-2486`

```java
private int checkClusterControlToggle(final long nowNs)
{
    if (ConsensusModule.State.ACTIVE == state)  // ← 只有 ACTIVE 才能触发快照
    {
        switch (ClusterControl.ToggleState.get(controlToggle))
        {
            case SNAPSHOT:
            {
                final long timestamp = clusterClock.time();
                if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
                {
                    offerPositionAndPreviousState(logPublisher.position(), state);
                    // ✅ 状态切换：ACTIVE → SNAPSHOT
                    state(ConsensusModule.State.SNAPSHOT, "ClusterControl.SNAPSHOT");
                    totalSnapshotDurationTracker.onSnapshotBegin(nowNs);
                }
                break;
            }
        }
    }
}
```

**关键**：
- ✅ `state(ConsensusModule.State.SNAPSHOT, ...)` - 状态从 ACTIVE 切换到 SNAPSHOT
- ✅ 此时 `state != ACTIVE`，所以 `ingressAdapter.poll()` 不再被调用
- ❌ **客户端请求停止被接收！**

#### 1.3 SNAPSHOT 状态下的行为

**位置**：`ConsensusModuleAgent.java:2550-2557`

```java
// 在 leaderWork() 中
else if (ConsensusModule.State.SNAPSHOT == state)  // ← 快照状态
{
    if (0 == serviceCount && logPublisher.position() <= commitPosition.getWeak())
    {
        final long timestamp = clusterClock.time();
        // 直接保存快照（无 Service 的情况）
        snapshotOnServiceAck(commitPosition.getWeak(), timestamp, ServiceAck.EMPTY_SERVICE_ACKS);
    }
    // ❌ 注意：这里没有调用 ingressAdapter.poll()
    // ❌ 客户端请求在网络缓冲区排队，不被处理
}
```

**关键**：
- ❌ SNAPSHOT 状态下**不调用** `ingressAdapter.poll()`
- ✅ 继续调用 `updateLeaderPosition(nowNs)` - 发送心跳
- ✅ 继续调用 `consensusModuleAdapter.poll()` - 处理集群间通信

---

### 为什么必须暂停？用例子说明

#### 场景：如果不暂停会发生什么？

假设：
- 快照开始时：`logPosition = 5000`
- 快照过程中：继续接收客户端请求

```
时间线：
t0: 开始快照，logPosition=5000
    │
    ├─ 快照线程：开始保存 ConsensusModule 状态
    │   └─ sessionManager.snapshotSessions(snapshotTaker);
    │       └─ 保存会话 #12345 (lastCorrelationId=100)
    │
t1: ❌ 客户端发送新消息 (correlationId=101) ← 如果不暂停
    │   └─ sessionManager 更新会话：lastCorrelationId=101
    │   └─ 写入日志：logPosition=5100
    │
t2: 快照线程：继续保存 Service 状态
    │   └─ Service.onTakeSnapshot()
    │       └─ 保存 globalMessageCount=500
    │
t3: 快照完成
    │   └─ 记录：快照对应 logPosition=5000 ← ❌ 但实际状态已经到 5100！
```

**问题**：
- 快照说对应 `logPosition=5000`
- 但快照中的会话状态是 `lastCorrelationId=101`（对应 `logPosition=5100`）
- **快照和日志位置不一致！**

#### 恢复时的灾难

```
恢复流程：
1. 加载快照（标记为 logPosition=5000）
   └─ 恢复会话：lastCorrelationId=101

2. 重放日志（从 logPosition=5000 开始）
   └─ 消息 correlationId=101 又被重放一次！← ❌ 重复处理

3. 结果：
   └─ globalMessageCount=501（应该是 500）
   └─ 业务逻辑错误！
```

#### 正确的流程（暂停后）

```
时间线：
t0: 开始快照，logPosition=5000
    │   └─ state(SNAPSHOT) ← ✅ 暂停接收
    │
t1: 客户端发送新消息 (correlationId=101)
    │   └─ ❌ ingressAdapter.poll() 不被调用
    │   └─ 消息在网络缓冲区排队等待
    │
t2: 快照线程：保存 ConsensusModule 状态
    │   └─ 保存会话：lastCorrelationId=100 ← ✅ 精确对应 logPosition=5000
    │
t3: 快照线程：保存 Service 状态
    │   └─ 保存 globalMessageCount=500 ← ✅ 精确对应 logPosition=5000
    │
t4: 快照完成
    │   └─ 记录：快照对应 logPosition=5000 ← ✅ 完全一致
    │   └─ state(ACTIVE) ← ✅ 恢复接收
    │
t5: 处理排队的消息
    │   └─ 消息 correlationId=101
    │   └─ 写入日志：logPosition=5100
```

**正确恢复**：
```
恢复流程：
1. 加载快照（logPosition=5000）
   └─ 恢复会话：lastCorrelationId=100
   └─ 恢复状态：globalMessageCount=500

2. 重放日志（从 logPosition=5000 开始）
   └─ 消息 correlationId=101（logPosition=5100）
   └─ globalMessageCount=501 ← ✅ 正确

3. 结果：完全一致！
```

---

### 源码标注：完整流程

```java
// 1. ACTIVE 状态：接收客户端请求
// ConsensusModuleAgent.java:2400-2408
if (Cluster.Role.LEADER == role)
{
    if (ConsensusModule.State.ACTIVE == state)  // ← ✅ 状态检查
    {
        workCount += ingressAdapter.poll();  // ← ✅ 接收客户端消息
    }
}

// 2. 触发快照：切换到 SNAPSHOT 状态
// ConsensusModuleAgent.java:2476-2486
case SNAPSHOT:
{
    if (appendAction(ClusterAction.SNAPSHOT, timestamp, CLUSTER_ACTION_FLAGS_DEFAULT))
    {
        state(ConsensusModule.State.SNAPSHOT, "ClusterControl.SNAPSHOT");  // ← ❌ 状态变为 SNAPSHOT
    }
    break;
}

// 3. SNAPSHOT 状态：不再接收新请求
// ConsensusModuleAgent.java:2400-2408（同上）
if (Cluster.Role.LEADER == role)
{
    if (ConsensusModule.State.ACTIVE == state)  // ← ❌ 条件不满足（state=SNAPSHOT）
    {
        workCount += ingressAdapter.poll();  // ← ❌ 不被调用
    }
}

// 4. 保存快照
// ConsensusModuleAgent.java:3056-3094
private void takeSnapshot(final long timestamp, final long logPosition, final ServiceAck[] serviceAcks)
{
    // 保存精确对应 logPosition 的状态
    snapshotState(publication, logPosition, leadershipTermId);
}

// 5. 快照完成：恢复 ACTIVE 状态
// ClusteredServiceAgent.java（Service 侧）
onServiceAck() {
    // ConsensusModule 收到所有 Service ACK 后
    state(ConsensusModule.State.ACTIVE, "snapshot complete");  // ← ✅ 恢复到 ACTIVE
}

// 6. 恢复接收客户端请求
if (ConsensusModule.State.ACTIVE == state)  // ← ✅ 条件满足
{
    workCount += ingressAdapter.poll();  // ← ✅ 重新开始接收
}
```

---

## 问题2：从快照恢复为什么会快？快照里到底有没有数据？

### 答案：快照里**有数据**！保存的是**业务状态**，不是消息内容

### 关键误解

你认为：
- ❌ "快照里没有数据"
- ❌ "恢复数据还是需要从归档全部重放"

实际上：
- ✅ **快照里有数据**：业务状态（账户余额、订单簿、消息计数器等）
- ✅ **归档里有消息内容**：历史操作记录（"转账 100 元"、"下单"、"Hello"）
- ✅ **恢复时**：加载快照得到最终状态，无需重放所有历史消息

---

### 具体例子：交易系统

#### 数据结构

```java
// 业务状态
class TradingService implements ClusteredService
{
    // ✅ 这就是要保存到快照的数据
    private final Map<String, Long> accountBalances = new HashMap<>();
    private long totalTradeCount = 0;
}
```

#### 运行流程

```
初始状态 (logPos=0):
- accountBalances = {}
- totalTradeCount = 0

────────────────────────────────────────────────────────────

消息 #1 (logPos=1000): "Alice deposit 100"
归档保存: 消息内容 "Alice deposit 100"
Service 处理:
  accountBalances.put("Alice", 100);
  totalTradeCount++;
状态变化:
  accountBalances = {Alice=100}
  totalTradeCount = 1

────────────────────────────────────────────────────────────

消息 #2 (logPos=2000): "Bob deposit 200"
归档保存: 消息内容 "Bob deposit 200"
Service 处理:
  accountBalances.put("Bob", 200);
  totalTradeCount++;
状态变化:
  accountBalances = {Alice=100, Bob=200}
  totalTradeCount = 2

────────────────────────────────────────────────────────────

消息 #3 (logPos=3000): "Alice transfer 50 to Bob"
归档保存: 消息内容 "Alice transfer 50 to Bob"
Service 处理:
  accountBalances.put("Alice", 50);
  accountBalances.put("Bob", 250);
  totalTradeCount++;
状态变化:
  accountBalances = {Alice=50, Bob=250}
  totalTradeCount = 3

────────────────────────────────────────────────────────────

... 997 条消息 ...

────────────────────────────────────────────────────────────

消息 #1000 (logPos=1000000): "Charlie deposit 500"
归档保存: 消息内容 "Charlie deposit 500"
Service 处理:
  accountBalances.put("Charlie", 500);
  totalTradeCount++;
最终状态:
  accountBalances = {Alice=5000, Bob=8000, Charlie=500, ...}
  totalTradeCount = 1000

────────────────────────────────────────────────────────────

触发快照 (logPos=1000000):
快照保存:
  ✅ accountBalances = {Alice=5000, Bob=8000, Charlie=500, ...}
  ✅ totalTradeCount = 1000

快照大小: 10 KB（100 个账户 × 100 字节）
归档大小: 100 MB（1000 条完整消息）
```

#### 快照文件内容

```
# snapshot-456.dat (10 KB)

[SnapshotMarker: BEGIN]
logPosition = 1000000
leadershipTermId = 5

[Service Snapshot]
totalTradeCount = 1000                   ← ✅ 有数据！
accountBalances.size = 100               ← ✅ 有数据！
accountBalances["Alice"] = 5000          ← ✅ 有数据！
accountBalances["Bob"] = 8000            ← ✅ 有数据！
accountBalances["Charlie"] = 500         ← ✅ 有数据！
... (共 100 个账户)

[SnapshotMarker: END]
```

#### 归档文件内容

```
# recording-123.dat (100 MB)

偏移 0:      [SessionMessage: "Alice deposit 100"]      ← ❌ 消息内容
偏移 1024:   [SessionMessage: "Bob deposit 200"]        ← ❌ 消息内容
偏移 2048:   [SessionMessage: "Alice transfer 50..."]   ← ❌ 消息内容
...
偏移 100MB:  [SessionMessage: "Charlie deposit 500"]    ← ❌ 消息内容
```

---

### 恢复对比：快照 vs 全量重放

#### 方案1：没有快照（全量重放归档）

```
恢复流程：
1. 从归档读取消息 #1
   └─ "Alice deposit 100"
   └─ onSessionMessage(buffer)
   └─ accountBalances.put("Alice", 100)
   └─ totalTradeCount = 1

2. 从归档读取消息 #2
   └─ "Bob deposit 200"
   └─ onSessionMessage(buffer)
   └─ accountBalances.put("Bob", 200)
   └─ totalTradeCount = 2

... 重复 1000 次 ...

3. 从归档读取消息 #1000
   └─ "Charlie deposit 500"
   └─ onSessionMessage(buffer)
   └─ accountBalances.put("Charlie", 500)
   └─ totalTradeCount = 1000

恢复时间：
- 读取 100 MB 归档
- 反序列化 1000 条消息
- 调用 1000 次 onSessionMessage()
- 总耗时：30 秒 ❌
```

#### 方案2：有快照（加载快照 + 增量重放）

```
恢复流程：
1. 加载快照 (logPos=1000000)
   └─ 读取 snapshot-456.dat (10 KB)
   └─ accountBalances = {Alice=5000, Bob=8000, Charlie=500, ...}  ← ✅ 直接得到最终状态！
   └─ totalTradeCount = 1000                                      ← ✅ 直接得到最终值！
   └─ 耗时：0.1 秒

2. 检查是否有快照后的新消息
   └─ 归档最新位置：logPos=1005000
   └─ 快照位置：logPos=1000000
   └─ 需要重放：logPos 1000000 → 1005000（5 条消息）

3. 重放增量日志
   └─ 消息 #1001: "Dave deposit 100"
   └─ 消息 #1002: "Eve deposit 200"
   └─ ...
   └─ 消息 #1005: "Frank deposit 50"
   └─ 耗时：0.1 秒

总耗时：0.2 秒 ✅

节省时间：30 秒 → 0.2 秒（快 150 倍！）
```

---

### 源码验证：快照里确实有数据

#### Service 保存快照

**位置**：`SimpleClusteredService.java:149-171`

```java
@Override
public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
{
    System.out.println("  [Service] 开始保存快照: globalCount=" + globalMessageCount +
        ", sessions=" + sessionMessageCounts.size());

    // ✅ 保存全局计数（业务数据！）
    responseBuffer.putLong(0, globalMessageCount);  // ← 这就是数据！
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES);

    // ✅ 保存会话数量（业务数据！）
    responseBuffer.putInt(0, sessionMessageCounts.size());  // ← 这就是数据！
    offerToSnapshot(snapshotPublication, responseBuffer, 0, Integer.BYTES);

    // ✅ 保存每个会话的计数（业务数据！）
    sessionMessageCounts.forEach((sessionId, count) ->
    {
        responseBuffer.putLong(0, sessionId);     // ← 数据：会话 ID
        responseBuffer.putLong(Long.BYTES, count); // ← 数据：消息计数
        offerToSnapshot(snapshotPublication, responseBuffer, 0, Long.BYTES * 2);
    });

    System.out.println("  [Service] 快照保存完成");
}
```

#### Service 加载快照

**位置**：`SimpleClusteredService.java:208-229`

```java
private void loadSnapshot(final Image snapshotImage)
{
    final MutableDirectBuffer buffer = new ExpandableArrayBuffer();

    // ✅ 恢复全局计数（直接读取，不需要重放消息！）
    pollSnapshot(snapshotImage, buffer, Long.BYTES);
    globalMessageCount = buffer.getLong(0);  // ← 直接得到 1000！

    // ✅ 恢复会话数量
    pollSnapshot(snapshotImage, buffer, Integer.BYTES);
    final int sessionCount = buffer.getInt(0);  // ← 直接得到 100！

    // ✅ 恢复每个会话的计数
    sessionMessageCounts.clear();
    for (int i = 0; i < sessionCount; i++)
    {
        pollSnapshot(snapshotImage, buffer, Long.BYTES * 2);
        final long sessionId = buffer.getLong(0);       // ← 直接得到会话 ID
        final long count = buffer.getLong(Long.BYTES);   // ← 直接得到计数值
        sessionMessageCounts.put(sessionId, count);      // ← 直接恢复状态
    }

    // ✅ 恢复完成：无需重放任何消息，状态已经恢复！
}
```

---

### 完整对比表

| 维度 | **快照** | **归档** |
|------|---------|---------|
| **内容** | 业务状态<br>`accountBalances={Alice=5000, Bob=8000}`<br>`totalTradeCount=1000` | 消息内容<br>`"Alice deposit 100"`<br>`"Bob deposit 200"`<br>... |
| **大小** | 固定（取决于状态大小）<br>10 KB | 持续增长<br>100 MB |
| **恢复方式** | 直接加载<br>`globalMessageCount = buffer.getLong(0)`<br>得到 1000 | 逐条重放<br>`onSessionMessage("Alice deposit 100")`<br>`onSessionMessage("Bob deposit 200")`<br>... 1000 次 |
| **恢复速度** | ⚡ 0.1 秒 | 🐌 30 秒 |
| **是否有数据** | ✅ 有！最终状态 | ✅ 有！历史操作 |

---

## 总结

### 问题1答案：为什么需要暂停？

**核心原因**：保证快照的一致性

**源码位置**：
```java
// ConsensusModuleAgent.java:2400-2408
if (ConsensusModule.State.ACTIVE == state)  // ← 只有 ACTIVE 才接收
{
    workCount += ingressAdapter.poll();  // ← 接收客户端请求
}

// ConsensusModuleAgent.java:2482
state(ConsensusModule.State.SNAPSHOT, "ClusterControl.SNAPSHOT");  // ← 切换到 SNAPSHOT

// 此时 state != ACTIVE，ingressAdapter.poll() 不再被调用
// 客户端请求在网络缓冲区排队，等待快照完成
```

**如果不暂停**：
- 快照说对应 `logPosition=5000`
- 但快照中的状态实际到了 `logPosition=5100`
- 恢复时会重复处理消息，导致数据错误

### 问题2答案：快照里有什么数据？

**核心误解**：快照里**有数据**，保存的是**业务状态**！

**快照内容**：
- ✅ `globalMessageCount = 1000`（最终状态）
- ✅ `accountBalances = {Alice=5000, Bob=8000}`（最终状态）

**归档内容**：
- ✅ `"Alice deposit 100"`（消息内容）
- ✅ `"Bob deposit 200"`（消息内容）

**恢复对比**：
- **有快照**：`globalMessageCount = buffer.getLong(0)` → 直接得到 1000（0.1 秒）
- **无快照**：重放 1000 条消息 → 逐步累加到 1000（30 秒）

**快 150 倍的原因**：
- 快照直接给你最终答案（1000）
- 归档需要你做 1000 次计算（1+1+1+...=1000）

---

## 类比

想象一下：

**归档** = 你的银行流水账
```
1月1日：存入 100 元
1月2日：取出 50 元
1月3日：存入 200 元
...
12月31日：取出 30 元
```

**快照** = 年底账户余额
```
余额：5000 元
```

**恢复余额**：
- **方案1（无快照）**：从 1 月 1 日开始，逐条计算 365 天的流水账 → 30 分钟
- **方案2（有快照）**：直接看年底余额 → 1 秒

快照保存的就是那个 **"5000 元"**，不是流水账的副本！

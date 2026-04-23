# 客户端连接方式对比

## 概述

有两种方式连接到 Aeron 集群并发送消息：

1. **使用 AeronCluster 客户端**（推荐，标准方式）
2. **直接使用 Aeron Publication**（灵活，高性能）

---

## 方案 1: AeronCluster 客户端（推荐）

### 实现类
- **ClusterMessageSender** - 发送者
- **InteractiveClusterClient** - 交互式客户端

### 连接方式

```java
AeronCluster aeronCluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressEndpoints("0=localhost:9002,1=localhost:9102,2=localhost:9202"));

// 发送消息
aeronCluster.offer(buffer, 0, length);

// 接收响应
aeronCluster.pollEgress();
```

### 架构图

```
┌─────────────┐
│ClusterMessage│ (机器 B)
│   Sender    │
└──────┬──────┘
       │ AeronCluster
       │ offer()
       │
       ▼ UDP 9002 (ingress)
┌─────────────────────────────┐
│   ClusterNodeWithConfig     │ (机器 A)
│                             │
│  ┌───────────────────────┐ │
│  │  ConsensusModule      │ │
│  │  (Raft 共识)          │ │
│  └──────────┬────────────┘ │
│             │               │
│             ▼               │
│  ┌───────────────────────┐ │
│  │ ClusteredService      │ │
│  │ onSessionMessage()    │ │
│  │  - 解码 Order/User    │ │
│  │  - 处理业务逻辑       │ │
│  │  - 发送响应           │ │
│  └───────────────────────┘ │
│             │               │
│             ▼               │
│  ┌───────────────────────┐ │
│  │  Archive              │ │
│  │  (自动录制)           │ │
│  └───────────────────────┘ │
└─────────────────────────────┘
       │
       │ egress response
       ▼
┌─────────────┐
│ClusterMessage│
│   Sender    │
│ pollEgress() │
└─────────────┘
```

### 特点

#### ✅ 优点

1. **经过 Raft 共识**
   - 所有消息都经过 leader 处理
   - 保证消息顺序和一致性
   - 自动故障转移

2. **ClusteredService 直接处理**
   - `onSessionMessage()` 会收到消息
   - 可以实现业务逻辑
   - 可以回复客户端

3. **Archive 自动录制**
   - 不需要手动配置
   - 集群内部流自动录制
   - 支持快照和恢复

4. **会话管理**
   - 自动维护客户端会话
   - 支持会话超时
   - 可以跟踪客户端状态

5. **Leader 切换**
   - 自动发现新 leader
   - 透明的故障转移
   - `onNewLeader()` 回调

#### ❌ 缺点

1. **不能使用自定义 streamId**
   - 集群内部管理 streamId
   - 无法区分不同类型的消息流
   - 需要在消息内部编码类型

2. **延迟较高**
   - 需要经过 Raft 共识
   - leader → followers 复制
   - 通常多几毫秒

3. **吞吐量受限**
   - 受集群共识速度限制
   - 单 leader 处理瓶颈

### 使用场景

适合以下场景：

- ✅ 需要强一致性保证
- ✅ 需要集群处理业务逻辑
- ✅ 需要响应客户端
- ✅ 需要会话管理
- ✅ 生产环境，高可用需求

### 启动示例

```bash
# 机器 A: 启动集群
java ClusterNodeWithConfig 0

# 机器 B: 启动发送者
java ClusterMessageSender "0=192.168.1.100:9002"

# 发送消息
> order 1001 AAPL 150.50 100 B

# 集群会输出（机器 A）:
# [Service] 收到 Order #1: Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}

# 客户端会收到响应（机器 B）:
# [Response] [Order Processed #1] orderId=1001, symbol=AAPL, price=150.50, qty=100, side=B (全局: 1, 本会话: 1)
```

---

## 方案 2: 直接使用 Aeron Publication

### 实现类
- **MessageSender** - 发送者
- **MessageReceiver** - 接收者

### 连接方式

```java
MediaDriver driver = MediaDriver.launch();
Aeron aeron = Aeron.connect();

// 发送消息
Publication pub = aeron.addPublication("aeron:udp?endpoint=192.168.1.100:9010", 100);
pub.offer(buffer, 0, length);

// 接收消息
Subscription sub = aeron.addSubscription("aeron:udp?endpoint=192.168.1.100:9010", 100);
sub.poll(handler, 10);
```

### 架构图

```
┌─────────────┐
│ MessageSender│ (机器 B)
│             │
│ Publication │
│ streamId 100│
└──────┬──────┘
       │ UDP 9010
       │
       ▼
┌─────────────────────────────┐
│   ClusterNodeWithConfig     │ (机器 A)
│                             │
│  ┌───────────────────────┐ │
│  │  Archive              │ │
│  │  (配置录制)           │ │
│  │  startRecording()     │ │
│  │  channel:             │ │
│  │   udp:9010           │ │
│  │  streamId: 100, 200   │ │
│  └───────────────────────┘ │
│                             │
│  注意：ClusteredService     │
│        不会收到这些消息      │
└─────────────────────────────┘
       │
       │ UDP 9001 (Archive control)
       ▼
┌─────────────┐
│MessageReceiver│ (机器 C)
│             │
│ list / replay│
└─────────────┘
```

### 特点

#### ✅ 优点

1. **支持自定义 streamId**
   - streamId 100: Order 消息
   - streamId 200: User 消息
   - 可以独立订阅不同类型

2. **低延迟**
   - 不经过 Raft 共识
   - 直接 UDP 传输
   - 纳秒级延迟

3. **高吞吐量**
   - 不受集群共识限制
   - 可以并发发送多个流
   - 适合高频交易场景

4. **灵活**
   - 可以点对点通信
   - 不依赖集群协议
   - 可以自定义编码格式

#### ❌ 缺点

1. **不经过 Raft 共识**
   - 没有一致性保证
   - 可能丢失消息（UDP）
   - 没有故障转移

2. **ClusteredService 不会收到**
   - 需要手动订阅
   - 无法直接处理业务逻辑
   - 不能回复客户端

3. **需要手动配置 Archive**
   - 使用 `ArchiveRecordingSetup` 配置
   - 集群重启后需要重新配置
   - 需要匹配 channel 和 streamId

4. **没有会话管理**
   - 无法跟踪客户端状态
   - 没有超时机制
   - 需要自己实现

### 使用场景

适合以下场景：

- ✅ 需要极低延迟
- ✅ 需要极高吞吐量
- ✅ 需要区分不同类型的消息流
- ✅ 只需要持久化（Archive），不需要处理
- ✅ 市场数据推送、日志收集等单向场景

### 启动示例

```bash
# 机器 A: 启动集群
java ClusterNodeWithConfig 0

# 任意机器: 配置 Archive 录制（一次性）
java ArchiveRecordingSetup 192.168.1.100 9001

# 机器 B: 启动发送者
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010"

# 发送消息
> order 1001 AAPL 150.50 100 B

# 集群不会有输出（不经过 ClusteredService）

# 机器 C: 回放消息
java MessageReceiver list 100
java MessageReceiver replay 0

# 输出:
# [Order] Position: 0, Stream ID: 999
#         Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
```

---

## 完整对比表

| 特性 | AeronCluster 客户端 | 直接 Aeron Publication |
|------|---------------------|------------------------|
| **实现类** | ClusterMessageSender | MessageSender |
| **连接端口** | 9002 (ingress) | 9010 (自定义) |
| **连接方式** | AeronCluster.connect() | Aeron.addPublication() |
| **经过 Raft 共识** | ✅ 是 | ❌ 否 |
| **一致性保证** | ✅ 强一致性 | ❌ 无保证 |
| **ClusteredService 处理** | ✅ onSessionMessage() | ❌ 不会收到 |
| **可以回复客户端** | ✅ 是 | ❌ 否 |
| **自定义 streamId** | ❌ 否 | ✅ 是 (100, 200) |
| **延迟** | 较高（几毫秒） | 极低（微秒级） |
| **吞吐量** | 受集群限制 | 极高 |
| **Archive 录制** | ✅ 自动 | ⚠️ 需配置 |
| **Leader 切换** | ✅ 自动 | ❌ 无 |
| **会话管理** | ✅ 是 | ❌ 否 |
| **消息顺序** | ✅ 保证 | ⚠️ 尽力而为 |
| **配置复杂度** | 简单 | 复杂 |
| **适用场景** | 业务消息、命令 | 市场数据、日志 |

---

## 如何选择？

### 选择 AeronCluster 客户端（方案 1）如果：

- ✅ 需要集群处理业务逻辑
- ✅ 需要响应客户端
- ✅ 需要强一致性保证
- ✅ 需要高可用（自动 failover）
- ✅ 生产环境

**示例**: 交易系统的订单提交、用户注册、配置更新

### 选择直接 Aeron Publication（方案 2）如果：

- ✅ 需要极低延迟
- ✅ 需要极高吞吐量
- ✅ 需要区分不同消息类型（多个 streamId）
- ✅ 只需要持久化，不需要处理
- ✅ 单向数据流

**示例**: 市场行情推送、日志收集、监控数据上报

---

## 混合使用

在实际场景中，可以**同时使用两种方案**：

```
┌─────────────────────┐
│ ClusterMessageSender│ ──┐
│ (业务命令)          │   │ AeronCluster
└─────────────────────┘   │ port 9002
                          │
                          ▼
               ┌────────────────────────┐
               │ ClusterNodeWithConfig  │
               │                        │
               │ ClusteredService       │
               │  - 处理业务命令        │
               │  - 回复客户端          │
               │                        │
               │ Archive                │
               │  - 录制所有消息        │
               └────────────────────────┘
                          ▲
                          │ UDP
                          │ port 9010
┌─────────────────────┐   │
│   MessageSender     │ ──┘
│ (市场数据、日志)    │
└─────────────────────┘
```

**示例场景**：交易系统

- **订单提交**: 使用 ClusterMessageSender（需要共识、回复）
- **市场行情**: 使用 MessageSender（高频、单向）
- **用户操作**: 使用 ClusterMessageSender（需要处理）
- **系统日志**: 使用 MessageSender（高吞吐）

---

## 总结

**你的问题问得很对！**

使用 `AeronCluster.connect()` 是连接集群的**正规方式**，适合需要集群处理的业务消息。

之前的 `MessageSender`/`MessageReceiver` 实际上是**绕过了集群协议**，直接使用底层 Aeron API，这种方式更灵活、性能更高，但失去了集群的很多特性。

**推荐方案**:
1. **业务消息**: 使用 `ClusterMessageSender`（新增的类）
2. **数据流**: 使用 `MessageSender`（原有的类）

两种方式各有优势，根据实际需求选择！

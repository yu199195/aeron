# 快速开始指南

## 推荐方案：使用 AeronCluster 客户端

这是连接 Aeron 集群的**标准方式**，消息会经过 Raft 共识，ClusteredService 会处理并回复。

---

## 3 步快速开始

### 步骤 1: 启动集群节点

```bash
# 终端 1: 启动 Node 0
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0

# 等待看到:
# [Node 0] ========== 所有组件已启动完成 ==========
```

### 步骤 2: 启动消息发送者

```bash
# 终端 2: 启动集群消息发送者
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterMessageSender

# 输出:
# ========================================
#   集群消息发送者
#   (使用 AeronCluster 客户端)
# ========================================
# Ingress Endpoints: 0=localhost:9002,1=localhost:9102,2=localhost:9202
#
# 特点:
#   - 消息经过 Raft 共识
#   - ClusteredService 会收到消息
#   - Archive 自动录制
#   - 自动 leader 切换
#
# [Sender] 启动 MediaDriver...
# [Sender] 连接到集群...
# [Sender] 已连接到集群
# [Sender] Session ID: 1
# [Sender] Leader Member ID: 0
#
# 可用命令:
#   order <orderId> <symbol> <price> <qty> <side>
#     - 发送 Order 消息到集群
#     - side: B=买入, S=卖出
#     - 示例: order 1001 AAPL 150.50 100 B
#
#   user <userId> <username> <email> <age> <balance>
#     - 发送 User 消息到集群
#     - 示例: user 2001 alice alice@example.com 25 1000.50
#
#   status - 显示集群状态
#   help   - 显示此帮助
#   exit   - 退出程序
#
# 开始输入命令:
# ========================================
#
# >
```

### 步骤 3: 发送消息并查看结果

#### 发送 Order 消息

```bash
> order 1001 AAPL 150.50 100 B

# 客户端输出:
[Sending Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
[Sent] Position: 0
[Response] [Order Processed #1] orderId=1001, symbol=AAPL, price=150.50, qty=100, side=B (全局: 1, 本会话: 1)

# 同时在集群节点（终端 1）会看到:
  [Service] 收到 Order #1 (session #1): Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
  [Service] 已回复: [Order Processed #1] orderId=1001, symbol=AAPL, price=150.50, qty=100, side=B (全局: 1, 本会话: 1)
```

#### 发送 User 消息

```bash
> user 2001 alice alice@example.com 25 1000.50

# 客户端输出:
[Sending User] User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
[Sent] Position: 128
[Response] [User Processed #2] userId=2001, username=alice, email=alice@example.com, age=25, balance=1000.50 (全局: 2, 本会话: 2)

# 集群节点输出:
  [Service] 收到 User #2 (session #2): User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
  [Service] 已回复: [User Processed #2] userId=2001, username=alice, email=alice@example.com, age=25, balance=1000.50 (全局: 2, 本会话: 2)
```

#### 查看集群状态

```bash
> status

# 输出:
集群状态:
  Session ID:      1
  Leader Member:   0
  Leadership Term: 0
  Is Closed:       false
```

---

## 消息流程

```
1. ClusterMessageSender
   │
   │ AeronCluster.offer()
   ▼
2. 集群 Ingress (port 9002)
   │
   │ 路由到 Leader
   ▼
3. ConsensusModule (Leader)
   │
   │ Raft 共识
   ▼
4. ClusteredService.onSessionMessage()
   │
   │ - 解码消息（Order/User）
   │ - 处理业务逻辑
   │ - 构造响应
   ▼
5. ClientSession.offer()
   │
   │ 发送响应
   ▼
6. ClusterMessageSender
   │
   │ AeronCluster.pollEgress()
   │ EgressListener.onMessage()
   ▼
7. 显示响应: [Response] ...
```

同时，Archive 会自动录制所有消息。

---

## 远程部署

### 连接到远程集群

```bash
# 假设集群节点在不同机器:
# Node 0: 192.168.1.100:9002
# Node 1: 192.168.1.101:9102
# Node 2: 192.168.1.102:9202

java ClusterMessageSender "0=192.168.1.100:9002,1=192.168.1.101:9102,2=192.168.1.102:9202"
```

### 防火墙配置

集群节点需要开放以下端口：

| 端口 | 协议 | 用途 |
|------|------|------|
| 9002 | UDP | Cluster ingress（客户端连接） |
| 9003 | UDP | Consensus（节点间共识） |
| 9004 | UDP | Log（日志复制） |
| 9005 | UDP | Catchup（数据同步） |

```bash
# 在集群节点上
sudo ufw allow 9002/udp
sudo ufw allow 9003/udp
sudo ufw allow 9004/udp
sudo ufw allow 9005/udp
```

---

## 高级功能

### 1. 发送纯文本消息（向后兼容）

```bash
> Hello, Cluster!

# 集群会当作纯文本处理（不是 Order/User）
  [Service] 收到消息 #3 (session #3): Hello, Cluster!
  [Service] 已回复: [Echo #3] Hello, Cluster! (全局: 3, 本会话: 3)
```

### 2. 处理 Leader 切换

如果 Leader 节点宕机，客户端会自动连接到新 Leader：

```bash
# 客户端输出:
[Session Event] code=CLOSED, detail=, leader=1
[New Leader] memberId=1, term=1

# 继续发送消息，自动路由到新 Leader
> order 1002 GOOGL 2800.75 50 S
[Sent] Position: 256
```

### 3. 会话超时

如果客户端长时间不活动，会话会超时：

```bash
[Session Event] code=ERROR, detail=session timeout, leader=0

# 需要重新连接
```

---

## 与直接 Aeron Publication 的对比

### AeronCluster 客户端（当前方案）

```java
AeronCluster cluster = AeronCluster.connect(...);
cluster.offer(buffer, 0, length);     // 发送到集群
cluster.pollEgress();                 // 接收响应
```

**特点**:
- ✅ 经过 Raft 共识
- ✅ ClusteredService 处理
- ✅ Archive 自动录制
- ✅ 可以回复客户端

### 直接 Aeron Publication（另一种方案）

```java
Publication pub = aeron.addPublication("aeron:udp?endpoint=...:9010", 100);
pub.offer(buffer, 0, length);         // 直接发送
```

**特点**:
- ✅ 低延迟
- ✅ 高吞吐量
- ✅ 支持自定义 streamId
- ❌ 不经过共识
- ❌ ClusteredService 不会收到
- ❌ 需要手动配置 Archive

详细对比见 [CLIENT_COMPARISON.md](CLIENT_COMPARISON.md)

---

## 故障排查

### 问题 1: 连接失败

```
[Error] Publication not connected
```

**检查**:
1. 集群节点是否启动？
2. Ingress endpoints 是否正确？
3. 防火墙是否开放端口 9002？

### 问题 2: 消息发送失败

```
[Error] 发送失败: -2
```

**原因**: Back pressure（背压）

**解决**: 减慢发送速度，或等待集群处理

### 问题 3: 没有收到响应

**检查**:
1. `pollEgress()` 是否调用？
2. EgressListener 是否正确实现？
3. ClusteredService 是否发送了响应？

---

## 完整示例

```bash
# 终端 1: 启动集群
java ClusterNodeWithConfig 0

# 终端 2: 启动客户端
java ClusterMessageSender

# 发送多条消息
> order 1001 AAPL 150.50 100 B
> order 1002 GOOGL 2800.75 50 S
> order 1003 TSLA 850.00 200 B
> user 2001 alice alice@example.com 25 1000.50
> user 2002 bob bob@example.com 30 2500.75
> status
> exit
```

---

## 下一步

1. 阅读 [CLIENT_COMPARISON.md](CLIENT_COMPARISON.md) 了解两种连接方式的对比
2. 阅读 [SimpleClusteredService.java](SimpleClusteredService.java:109) 了解如何处理消息
3. 阅读 [REMOTE_SETUP_GUIDE.md](REMOTE_SETUP_GUIDE.md) 了解远程部署

---

## 总结

**推荐使用 ClusterMessageSender（AeronCluster 客户端）**，这是连接 Aeron 集群的标准方式：

- ✅ 消息经过 Raft 共识
- ✅ ClusteredService 直接处理
- ✅ Archive 自动录制
- ✅ 支持回复客户端
- ✅ 自动 Leader 切换
- ✅ 会话管理

非常适合生产环境中的业务消息！

# 应用 A 发送消息，应用 B 订阅接收 - 快速入门

## 场景

- **应用 A**: 发送消息到集群（ClusterMessageSender）
- **应用 B**: 订阅并接收应用 A 发送的消息（ClusterSubscriber）
- **集群**: 运行 PubSubClusteredService，负责消息路由

---

## 完整流程

### 步骤 1: 启动集群（使用 PubSubClusteredService）

首先需要修改 `ClusterNodeWithConfig.java`，使用 PubSub 服务：

```java
// ClusterNodeWithConfig.java
final ClusterConfig clusterConfig = ClusterConfig.create(
    nodeId,
    HOSTNAMES,
    PORT_BASE,
    new PubSubClusteredService());  // 使用 PubSub 服务
```

然后启动集群节点：

```bash
# 终端 1: 启动 Node 0
cd /Users/yu.xiao/Documents/github/aeron
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterNodeWithConfig --args="0"
```

**预期输出**:
```
[ClusterNode] 集群节点配置
  节点 ID: 0
  主机名: localhost
  Ingress 端口: 9002
  Consensus 端口: 9003
  Log 端口: 9004
  Archive 端口: 9007

[PubSubService] 正在启动...
[PubSubService] 已启动

[提示] 客户端可以发送以下命令:
  SUBSCRIBE ORDER  - 订阅 Order 消息
  SUBSCRIBE USER   - 订阅 User 消息
  UNSUBSCRIBE ORDER
  UNSUBSCRIBE USER
```

### 步骤 2: 启动应用 B（订阅者 - ClusterSubscriber）

```bash
# 终端 2: 启动订阅者，订阅 Order 消息
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="ORDER"
```

**预期输出**:
```
========================================
  集群订阅者
  (使用 AeronCluster)
========================================
Ingress Endpoints: 0=localhost:9002,1=localhost:9102,2=localhost:9202
订阅类型: ORDER

优点:
  - 使用标准的 AeronCluster 协议
  - 无需硬编码地址
  - 自动 leader 切换
  - 适用于 K8s 部署

[Subscriber] 启动 MediaDriver...
[Subscriber] 连接到集群...
[Subscriber] Ingress Endpoints: 0=localhost:9002,1=localhost:9102,2=localhost:9202
[Subscriber] 已连接到集群
[Subscriber] Session ID: 1
[Subscriber] Leader Member ID: 0

[Subscriber] 发送订阅请求: ORDER
[Subscriber] 订阅请求已发送: ORDER
[Response] [OK] 已订阅 Order 消息
[Subscriber] 等待消息推送...
按 Ctrl+C 退出
========================================
```

**集群端输出**:
```
[PubSubService] 会话打开: sessionId=1
  [Response] 欢迎连接到 PubSub 集群! sessionId=1
使用命令: SUBSCRIBE ORDER 或 SUBSCRIBE USER
[PubSubService] 会话 1 订阅 Order 消息
  [Response] [OK] 已订阅 Order 消息
```

### 步骤 3: 启动应用 A（发送者 - ClusterMessageSender）

```bash
# 终端 3: 启动发送者
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterMessageSender
```

**预期输出**:
```
========================================
  集群消息发送器
========================================
Ingress Endpoints: 0=localhost:9002,1=localhost:9102,2=localhost:9202

[Sender] 启动 MediaDriver...
[Sender] 连接到集群...
[Sender] 已连接到集群
[Sender] Session ID: 2
[Sender] Leader Member ID: 0

========================================
命令:
  order <id> <symbol> <price> <qty> <side>
  user <id> <username> <email> <age> <balance>
  quit

示例:
  order 1001 AAPL 150.50 100 B
  user 2001 alice alice@example.com 25 1000.50
========================================

>
```

### 步骤 4: 应用 A 发送消息

在应用 A 的终端中输入：

```
> order 1001 AAPL 150.50 100 B
```

#### 应用 A（发送者）看到的输出:
```
> order 1001 AAPL 150.50 100 B
[Sending Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
[Sent] Position: 0
[Response] [Order Published #1] orderId=1001, symbol=AAPL, subscribers=1
```

#### 应用 B（订阅者）看到的输出:
```
[Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
```

#### 集群端看到的输出:
```
[PubSubService] 收到 Order #1: Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
  [Broadcast] Order 已推送给 1 个订阅者
  [Response] [Order Published #1] orderId=1001, symbol=AAPL, subscribers=1
```

---

## 完整的消息流

```
应用 A (ClusterMessageSender)
  │
  │ 1. 用户输入命令
  │    order 1001 AAPL 150.50 100 B
  │
  │ 2. 编码为二进制
  │    [1][1001][15050][100][B][timestamp][AAPL]
  │
  │ 3. 通过 AeronCluster 发送
  │    aeronCluster.offer(buffer)
  │    ingress streamId = 101
  │
  ▼
┌─────────────────────────────────────┐
│  Cluster (PubSubClusteredService)   │
│                                     │
│  4. 接收消息（经过 Raft 共识）      │
│     onSessionMessage(session=2, ...) │
│                                     │
│  5. 解码消息类型                    │
│     msgType = 1 (Order)             │
│                                     │
│  6. 查找订阅者                      │
│     getOrderSubscribers()           │
│     → [session 1]                   │
│                                     │
│  7. 推送给订阅者                    │
│     session1.offer(buffer)          │
│                                     │
│  8. 回复发送者                      │
│     session2.offer(response)        │
└─────────────────────────────────────┘
  │                    │
  │                    │ egress streamId = 102 (应用 A)
  │                    └──────────────────────────────┐
  │                                                   │
  │ egress streamId = 102 (应用 B)                   │
  └────────────────────────────────┐                 │
                                   ▼                 ▼
                     应用 B (ClusterSubscriber)   应用 A
                     [Order] Order{...}          [Response] Published
```

---

## 多个订阅者

你可以启动多个订阅者：

```bash
# 终端 4: 订阅者 B1（订阅 Order）
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="ORDER"

# 终端 5: 订阅者 B2（订阅 Order）
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="ORDER"

# 终端 6: 订阅者 C（订阅 User）
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="USER"
```

当应用 A 发送 1 条 Order 消息时：
- ✅ 订阅者 B1 收到
- ✅ 订阅者 B2 收到
- ❌ 订阅者 C **不会**收到（因为它只订阅了 User）

---

## 消息类型

### Order 消息

```bash
> order 1001 AAPL 150.50 100 B
```

**参数**:
- `1001`: 订单 ID
- `AAPL`: 股票代码
- `150.50`: 价格
- `100`: 数量
- `B`: 方向（B=买入, S=卖出）

### User 消息

```bash
> user 2001 alice alice@example.com 25 1000.50
```

**参数**:
- `2001`: 用户 ID
- `alice`: 用户名
- `alice@example.com`: 邮箱
- `25`: 年龄
- `1000.50`: 余额

---

## 订阅不同类型的消息

### 订阅 Order

```bash
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="ORDER"
```

### 订阅 User

```bash
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="USER"
```

### 订阅所有类型

```bash
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber --args="ORDER,USER"
```

---

## 远程部署

### 应用 A（发送者）在另一台机器

```bash
# 机器 1: 应用 A
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterMessageSender \
  --args="0=192.168.1.100:9002,1=192.168.1.101:9102,2=192.168.1.102:9202"
```

### 应用 B（订阅者）在另一台机器

```bash
# 机器 2: 应用 B
./gradlew :aeron-samples:run -PmainClass=io.aeron.samples.cluster.ClusterSubscriber \
  --args="0=192.168.1.100:9002,1=192.168.1.101:9102,2=192.168.1.102:9202 ORDER"
```

---

## K8s 部署

### 1. ConfigMap（集群地址）

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: cluster-config
data:
  ingressEndpoints: "0=aeron-cluster-0.aeron-cluster:9002,1=aeron-cluster-1.aeron-cluster:9102,2=aeron-cluster-2.aeron-cluster:9202"
```

### 2. 应用 A（发送者）Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-sender
spec:
  replicas: 1
  template:
    spec:
      containers:
      - name: sender
        image: your-registry/cluster-message-sender:latest
        env:
        - name: INGRESS_ENDPOINTS
          valueFrom:
            configMapKeyRef:
              name: cluster-config
              key: ingressEndpoints
        command:
        - java
        - -cp
        - aeron-all.jar
        - io.aeron.samples.cluster.ClusterMessageSender
        - $(INGRESS_ENDPOINTS)
```

### 3. 应用 B（订阅者）Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-subscriber
spec:
  replicas: 3  # 可以多个副本
  template:
    spec:
      containers:
      - name: subscriber
        image: your-registry/cluster-subscriber:latest
        env:
        - name: INGRESS_ENDPOINTS
          valueFrom:
            configMapKeyRef:
              name: cluster-config
              key: ingressEndpoints
        - name: SUBSCRIPTION_TYPE
          value: "ORDER"
        command:
        - java
        - -cp
        - aeron-all.jar
        - io.aeron.samples.cluster.ClusterSubscriber
        - $(INGRESS_ENDPOINTS)
        - $(SUBSCRIPTION_TYPE)
```

---

## 故障排查

### 问题 1: 订阅者连接失败

```
[Error] Publication not connected
```

**解决**:
1. 检查集群是否启动
2. 检查 ingress endpoints 是否正确
3. 检查防火墙/网络

### 问题 2: 订阅者没有收到消息

**检查**:
1. 订阅者是否发送了订阅请求？
   - 查看输出：`[Response] [OK] 已订阅 Order 消息`
2. 发送者是否发送了消息？
3. 集群是否输出了 `[Broadcast] 已推送给 N 个订阅者`？
4. 消息类型是否匹配？
   - 订阅 ORDER 但发送者发的是 USER

### 问题 3: 应用 A 也收到了自己发的消息？

**这是正常的！** 如果应用 A 也订阅了相同类型的消息，它也会收到。

如果不想收到，有两种方式：
1. 应用 A 不发送订阅请求
2. ClusteredService 判断 `if (session.id() != senderSessionId)` 跳过发送者

---

## 核心要点

### ✅ 应用 B 如何接收应用 A 的消息？

1. **应用 B 连接到集群**
   ```java
   AeronCluster.connect(new AeronCluster.Context()
       .ingressEndpoints("0=localhost:9002,..."))
   ```

2. **应用 B 发送订阅请求**
   ```java
   cluster.offer("SUBSCRIBE ORDER")
   ```

3. **ClusteredService 维护订阅者列表**
   ```java
   subscriptions.put(sessionId, [ORDER])
   ```

4. **应用 A 发送消息**
   ```java
   cluster.offer(orderBuffer)
   ```

5. **ClusteredService 推送给所有订阅者**
   ```java
   for (ClientSession subscriber : getOrderSubscribers())
       subscriber.offer(orderBuffer)  // 包括应用 B
   ```

6. **应用 B 通过 EgressListener 接收**
   ```java
   egressListener.onMessage(...)
   ```

### ✅ 关键机制

- **不是**通过 streamId 区分
- **不是**应用 B 直接监听应用 A 的消息
- **是**通过 ClusteredService 的 PubSub 机制
- **是**通过订阅命令和会话管理

---

## 总结

| 步骤 | 组件 | 命令 |
|------|------|------|
| 1 | 集群 | `ClusterNodeWithConfig 0` (使用 PubSubClusteredService) |
| 2 | 应用 B（订阅者）| `ClusterSubscriber ORDER` |
| 3 | 应用 A（发送者）| `ClusterMessageSender` |
| 4 | 发送消息 | `order 1001 AAPL 150.50 100 B` |
| 5 | 应用 B 收到 | `[Order] Order{...}` |

**这就是生产环境推荐的方式！** ✅

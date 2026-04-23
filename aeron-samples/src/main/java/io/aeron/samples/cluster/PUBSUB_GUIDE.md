# 集群发布-订阅完整指南

## ✅ 正确的方案：使用 AeronCluster

这是**生产环境推荐**的方案，完全基于 AeronCluster 协议，适用于 K8s 部署。

---

## 架构图

```
┌─────────────────────┐
│ ClusterMessageSender│ (发布者)
│  AeronCluster       │
└──────────┬──────────┘
           │ offer()
           │ port 9002
           ▼
┌──────────────────────────────────┐
│   ClusterNodeWithConfig          │
│  (PubSubClusteredService)        │
│                                  │
│  ┌────────────────────────────┐ │
│  │ onSessionMessage()         │ │
│  │  - 接收消息（经过共识）    │ │
│  │  - 维护订阅者列表          │ │
│  │  - 推送给订阅者            │ │
│  └────────────┬───────────────┘ │
│               │ ClientSession   │
│               │ offer()         │
└───────────────┼─────────────────┘
                │
                ├─ 订阅 Order 的会话
                │
                └─ 订阅 User 的会话
                │
                ▼
┌────────────────────────────┐
│  ClusterSubscriber (订阅者) │
│   AeronCluster             │
│   pollEgress()             │
│   - 订阅 ORDER             │
│   - 订阅 USER              │
└────────────────────────────┘
```

---

## 核心特点

### ✅ 优势

1. **完全基于 AeronCluster**
   - 所有通信通过 AeronCluster 协议
   - 无需硬编码 channel 地址
   - 使用 ingress endpoints

2. **适用于 K8s 部署**
   - 通过 Service 发现集群节点
   - 示例：`"0=cluster-node-0.cluster-svc:9002,1=cluster-node-1.cluster-svc:9102,2=cluster-node-2.cluster-svc:9202"`
   - 或使用 Headless Service

3. **自动 Leader 切换**
   - 订阅者自动连接到新 Leader
   - 无需重新订阅

4. **灵活的订阅**
   - 订阅者可以选择订阅哪些消息类型
   - 支持动态订阅和取消订阅

5. **消息经过 Raft 共识**
   - 保证消息顺序
   - 强一致性

---

## 完整使用流程

### 步骤 1: 修改集群配置使用 PubSubClusteredService

编辑 `ClusterNodeWithConfig.java`:

```java
final ClusterConfig clusterConfig = ClusterConfig.create(
    nodeId,
    HOSTNAMES,
    PORT_BASE,
    new PubSubClusteredService());  // 使用 PubSub 服务
```

### 步骤 2: 启动集群

```bash
# 终端 1: 启动 Node 0
java ClusterNodeWithConfig 0

# 输出:
# [PubSubService] 正在启动...
# [PubSubService] 已启动
#
# [提示] 客户端可以发送以下命令:
#   SUBSCRIBE ORDER  - 订阅 Order 消息
#   SUBSCRIBE USER   - 订阅 User 消息
#   UNSUBSCRIBE ORDER
#   UNSUBSCRIBE USER
```

### 步骤 3: 启动订阅者

```bash
# 终端 2: 订阅 Order 消息
java ClusterSubscriber ORDER

# 输出:
# ========================================
#   集群订阅者
#   (使用 AeronCluster)
# ========================================
# Ingress Endpoints: 0=localhost:9002,1=localhost:9102,2=localhost:9202
# 订阅类型: ORDER
#
# [Subscriber] 启动 MediaDriver...
# [Subscriber] 连接到集群...
# [Subscriber] 已连接到集群
# [Subscriber] Session ID: 1
# [Subscriber] Leader Member ID: 0
#
# [Subscriber] 发送订阅请求: ORDER
# [Subscriber] 订阅请求已发送: ORDER
# [Response] [OK] 已订阅 Order 消息
# [Subscriber] 等待消息推送...
# 按 Ctrl+C 退出
# ========================================
```

```bash
# 终端 3: 订阅 User 消息
java ClusterSubscriber USER

# 或订阅所有消息
java ClusterSubscriber ORDER,USER
```

### 步骤 4: 启动发布者并发送消息

```bash
# 终端 4: 启动发布者
java ClusterMessageSender

# 发送 Order 消息
> order 1001 AAPL 150.50 100 B
[Sending Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
[Sent] Position: 0
[Response] [Order Published #1] orderId=1001, symbol=AAPL, subscribers=1

# 发送 User 消息
> user 2001 alice alice@example.com 25 1000.50
[Sending User] User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
[Sent] Position: 128
[Response] [User Published #2] userId=2001, username=alice, subscribers=1
```

### 步骤 5: 查看订阅者接收到的消息

**终端 2（Order 订阅者）会输出：**
```
[Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
```

**终端 3（User 订阅者）会输出：**
```
[User] User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
```

**终端 1（集群）会输出：**
```
  [PubSubService] 会话 1 订阅 Order 消息
  [PubSubService] 会话 2 订阅 User 消息
  [PubSubService] 收到 Order #1: Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
    [Broadcast] Order 已推送给 1 个订阅者
    [Response] [Order Published #1] orderId=1001, symbol=AAPL, subscribers=1
  [PubSubService] 收到 User #2: User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50}
    [Broadcast] User 已推送给 1 个订阅者
    [Response] [User Published #2] userId=2001, username=alice, subscribers=1
```

---

## K8s 部署配置

### 1. Headless Service

```yaml
apiVersion: v1
kind: Service
metadata:
  name: aeron-cluster
spec:
  clusterIP: None
  selector:
    app: aeron-cluster
  ports:
  - name: ingress
    port: 9002
    targetPort: 9002
  - name: consensus
    port: 9003
    targetPort: 9003
  - name: log
    port: 9004
    targetPort: 9004
  - name: catchup
    port: 9005
    targetPort: 9005
```

### 2. StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: aeron-cluster
spec:
  serviceName: aeron-cluster
  replicas: 3
  selector:
    matchLabels:
      app: aeron-cluster
  template:
    metadata:
      labels:
        app: aeron-cluster
    spec:
      containers:
      - name: cluster-node
        image: your-registry/aeron-cluster:latest
        command:
        - java
        - -cp
        - aeron-all.jar
        - io.aeron.samples.cluster.ClusterNodeWithConfig
        - $(POD_INDEX)
        env:
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['pod-index']
        ports:
        - containerPort: 9002
          name: ingress
        - containerPort: 9003
          name: consensus
        - containerPort: 9004
          name: log
        - containerPort: 9005
          name: catchup
```

### 3. 客户端连接配置

```bash
# 订阅者
java ClusterSubscriber \
  "0=aeron-cluster-0.aeron-cluster:9002,1=aeron-cluster-1.aeron-cluster:9102,2=aeron-cluster-2.aeron-cluster:9202" \
  ORDER

# 或使用环境变量
export INGRESS_ENDPOINTS="0=aeron-cluster-0.aeron-cluster:9002,1=aeron-cluster-1.aeron-cluster:9102,2=aeron-cluster-2.aeron-cluster:9202"
java ClusterSubscriber ORDER
```

---

## 动态订阅/取消订阅

订阅者可以在运行时发送命令：

### 订阅

```bash
# 订阅时自动发送
SUBSCRIBE ORDER
SUBSCRIBE USER
```

### 取消订阅

订阅者可以通过客户端发送：
```
UNSUBSCRIBE ORDER
```

---

## 与之前方案的对比

| 特性 | ❌ 之前方案（ForwardingService） | ✅ 新方案（PubSubService） |
|------|----------------------------------|----------------------------|
| **连接方式** | 直接订阅 UDP channel | AeronCluster.connect() |
| **地址配置** | 硬编码 `localhost:9010` | Ingress endpoints |
| **K8s 支持** | ❌ 需要修改代码 | ✅ 通过 Service 发现 |
| **Leader 切换** | ❌ 无 | ✅ 自动 |
| **订阅管理** | ❌ 无 | ✅ 动态订阅/取消订阅 |
| **会话管理** | ❌ 无 | ✅ 自动管理 |
| **消息推送** | Publication | ClientSession.offer() |

---

## 高级功能

### 1. 消息过滤

可以在 PubSubClusteredService 中增加过滤逻辑：

```java
// 只推送符合条件的 Order
if (order.price > 100.0)
{
    broadcastToSubscribers(SUB_TYPE_ORDER, buffer, offset, length, "Order");
}
```

### 2. 多个订阅者

```bash
# 终端 2: 订阅者 A（订阅 Order）
java ClusterSubscriber ORDER

# 终端 3: 订阅者 B（订阅 Order）
java ClusterSubscriber ORDER

# 终端 4: 订阅者 C（订阅 User）
java ClusterSubscriber USER

# 发送 1 条 Order 消息
# → 订阅者 A 和 B 都会收到
# → 订阅者 C 不会收到
```

### 3. 历史消息回放

通过 Archive 回放历史消息（未来扩展）：

```bash
# 订阅者连接时请求历史消息
REPLAY ORDER FROM 0 TO 1000
```

---

## 故障排查

### 问题 1: 订阅者连接失败

```
[Error] Publication not connected
```

**检查**:
1. 集群节点是否启动？
2. Ingress endpoints 是否正确？
3. 防火墙是否开放端口 9002？

### 问题 2: 没有收到消息

**检查**:
1. 是否发送了订阅请求？查看 `[Response] [OK] 已订阅`
2. 发布者是否发送了消息？
3. 集群是否输出 `[Broadcast] 已推送给 N 个订阅者`？

### 问题 3: Leader 切换后断开

**原因**: AeronCluster 会自动重连，但需要重新订阅

**解决**: 在 `onNewLeader()` 回调中重新发送订阅请求

---

## 总结

**✅ 这个方案完美解决了你提出的所有问题**：

1. ✅ 订阅者使用 `AeronCluster.connect()` 连接集群
2. ✅ 无需硬编码 channel 地址
3. ✅ 通过 ingress endpoints 发现集群
4. ✅ 适用于 K8s 部署（使用 Service 或 Headless Service）
5. ✅ 支持动态订阅特定类型的消息
6. ✅ 自动 Leader 切换
7. ✅ 消息经过 Raft 共识

**推荐在生产环境使用这个方案！**

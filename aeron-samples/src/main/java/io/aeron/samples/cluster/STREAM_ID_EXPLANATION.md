# AeronCluster 的 ingressStreamId 和 egressStreamId 详解

## 简短回答

**不是！** `ingressStreamId` 和 `egressStreamId` **不能用来区分不同类型的业务消息**（如 Order、User）。

它们是 **AeronCluster 协议层的固定通信通道**，不是业务层的消息分类机制。

---

## 详细解释

### 1. 这两个 StreamId 的真正作用

```java
AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(101)   // 客户端 → 集群
    .egressStreamId(102));  // 集群 → 客户端
```

#### ingressStreamId (默认 101)
- **用途**: 客户端向集群发送消息的通道
- **方向**: Client → Leader
- **底层**: 客户端创建一个 Publication，向 Leader 的 ingressEndpoint 发送数据
- **协议**: 所有通过 `aeronCluster.offer()` 发送的消息都走这个 streamId

#### egressStreamId (默认 102)
- **用途**: 集群向客户端返回响应的通道
- **方向**: Leader → Client
- **底层**: Leader 创建一个 Publication，向客户端发送响应
- **协议**: 所有 `EgressListener.onMessage()` 接收的消息都来自这个 streamId

### 2. 架构示意图

```
┌─────────────────────────────────────────────────────────┐
│  AeronCluster Client                                    │
│                                                         │
│  ┌─────────────────────────┐                          │
│  │ Publication             │  发送消息                 │
│  │ streamId = 101 (ingress)├──────────────┐           │
│  └─────────────────────────┘              │           │
│                                            │           │
│  ┌─────────────────────────┐              │           │
│  │ Subscription            │◄─────────────┼───────┐   │
│  │ streamId = 102 (egress) │  接收响应    │       │   │
│  └─────────────────────────┘              │       │   │
└───────────────────────────────────────────┼───────┼───┘
                                            │       │
                                            ▼       │
                ┌────────────────────────────────┐ │
                │  Cluster Leader                │ │
                │                                │ │
                │  ┌──────────────────────────┐ │ │
                │  │ Subscription (ingress)   │ │ │
                │  │ streamId = 101           │◄┘ │
                │  └──────────────────────────┘   │
                │                                │ │
                │  ┌──────────────────────────┐ │ │
                │  │ Publication (egress)     │ │ │
                │  │ streamId = 102           ├─┘ │
                │  └──────────────────────────┘   │
                └────────────────────────────────┘
```

### 3. 关键特性

#### ✅ 每个 AeronCluster 连接只有一对 ingress/egress StreamId

```java
// 错误理解：创建多个 streamId 来区分消息类型
AeronCluster orderCluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(100));  // ❌ 这不是用来发 Order 的

AeronCluster userCluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(200));  // ❌ 这不是用来发 User 的
```

**问题**:
- 每个 `AeronCluster` 实例会创建一个独立的集群会话
- 需要维护多个 MediaDriver、多个连接
- 每个会话都有独立的 Raft 日志位置
- 浪费资源，且无法保证消息顺序

#### ✅ 所有业务消息都走同一个 ingress/egress

```java
// 正确做法：所有消息走同一个连接
AeronCluster cluster = AeronCluster.connect(...);

// 发送 Order（类型 1）
buffer.putByte(0, (byte) 1);  // 消息类型
// ... 编码 Order 数据
cluster.offer(buffer, 0, length);

// 发送 User（类型 2）
buffer.putByte(0, (byte) 2);  // 消息类型
// ... 编码 User 数据
cluster.offer(buffer, 0, length);

// ✅ 两种消息都通过同一个 ingressStreamId (101) 发送
```

### 4. 如何区分不同类型的消息？

既然不能用 streamId 区分，那么正确的方法是什么？

#### 方法 1: 消息内部类型字节（我们当前使用的）

```java
// 消息格式
// [类型字节][消息数据]

// Order 消息
buffer.putByte(0, (byte) 1);  // 类型 = 1
buffer.putLong(1, orderId);
// ...

// User 消息
buffer.putByte(0, (byte) 2);  // 类型 = 2
buffer.putLong(1, userId);
// ...
```

**ClusteredService 处理**:
```java
@Override
public void onSessionMessage(ClientSession session, DirectBuffer buffer, ...)
{
    final byte msgType = buffer.getByte(offset);

    if (msgType == 1)
    {
        handleOrder(buffer, offset, length);
    }
    else if (msgType == 2)
    {
        handleUser(buffer, offset, length);
    }
}
```

#### 方法 2: 使用 SBE 标准协议

使用 SBE (Simple Binary Encoding) 生成的 Codec：

```java
// 发送 Order
final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
final OrderEncoder orderEncoder = new OrderEncoder();

orderEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)
    .orderId(1001)
    .symbol("AAPL")
    .price(150.50);

cluster.offer(buffer, 0, orderEncoder.encodedLength());
```

SBE 会自动在消息头部添加模板 ID（类似于消息类型）。

#### 方法 3: 订阅机制（我们的 PubSubClusteredService）

```java
// 客户端发送订阅命令
cluster.offer("SUBSCRIBE ORDER");

// ClusteredService 维护订阅者列表
if (isSubscribed(session, ORDER_TYPE))
{
    session.offer(orderMessage);  // 只推送给订阅了 Order 的会话
}
```

### 5. 什么时候需要修改 ingressStreamId/egressStreamId？

#### 场景 1: 多个独立的集群客户端应用

如果同一台机器上运行多个不同的应用，都需要连接同一个集群：

```java
// 应用 A: 交易系统
AeronCluster tradingClient = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(101)   // 默认
    .egressStreamId(102));

// 应用 B: 监控系统（在同一台机器）
AeronCluster monitorClient = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(201)   // 避免冲突
    .egressStreamId(202));
```

**为什么？**
- 避免 streamId 冲突
- 每个应用有独立的会话

#### 场景 2: 端口冲突

如果默认的 streamId (101/102) 已经被其他 Aeron 应用占用：

```java
AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressStreamId(301)
    .egressStreamId(302));
```

### 6. 对比总结

| 特性 | ingressStreamId/egressStreamId | 消息类型字节 |
|------|-------------------------------|-------------|
| **作用层** | 协议层（Aeron transport） | 业务层（应用逻辑） |
| **数量** | 每个连接只有 1 对 | 可以有多种消息类型 |
| **用途** | 定义 Client ↔ Leader 的通信通道 | 区分 Order、User 等业务消息 |
| **修改场景** | 多个应用、端口冲突 | 新增业务消息类型 |
| **默认值** | ingress=101, egress=102 | 无默认（自定义） |
| **能否多值** | ❌ 一个连接只能一对 | ✅ 可以支持 255 种类型 |

### 7. 实际示例对比

#### ❌ 错误：试图用 streamId 区分消息类型

```java
// 错误方式
public class WrongApproach
{
    private AeronCluster orderCluster;   // 为 Order 创建连接
    private AeronCluster userCluster;    // 为 User 创建连接

    public void sendOrder(Order order)
    {
        orderCluster.offer(...);  // ❌ 浪费资源
    }

    public void sendUser(User user)
    {
        userCluster.offer(...);   // ❌ 浪费资源
    }
}
```

**问题**:
- 2 个 MediaDriver（或共享但需要 2 个 Aeron 实例）
- 2 个集群会话
- 2 倍的网络连接
- 2 倍的心跳开销
- Order 和 User 消息的顺序无法保证（因为是不同会话）

#### ✅ 正确：使用消息类型字节

```java
// 正确方式
public class CorrectApproach
{
    private AeronCluster cluster;  // 只需一个连接

    public void sendOrder(Order order)
    {
        buffer.putByte(0, MSG_TYPE_ORDER);  // 类型 = 1
        encodeOrder(buffer, 1, order);
        cluster.offer(buffer, 0, length);
    }

    public void sendUser(User user)
    {
        buffer.putByte(0, MSG_TYPE_USER);   // 类型 = 2
        encodeUser(buffer, 1, user);
        cluster.offer(buffer, 0, length);
    }
}
```

**优点**:
- 只需 1 个连接
- 1 个会话
- 消息顺序有保证（在同一个 Raft 日志中）
- 资源消耗最小

### 8. 完整的消息流

```
Client:
  buffer[0] = 1 (ORDER)              ┐
  buffer[1..N] = Order data          ├─ 通过 ingressStreamId=101
  cluster.offer(buffer)              ┘

    ↓ UDP 传输

Leader (ConsensusModule):
  接收 ingress streamId=101
  写入 Raft log
  复制到 Followers
  达成共识
  应用到 ClusteredService

ClusteredService:
  onSessionMessage()
    msgType = buffer[0]  // 读取类型字节
    if (msgType == 1)
      handleOrder()      // 处理 Order
    else if (msgType == 2)
      handleUser()       // 处理 User

    // 推送给订阅者
    session.offer(...)   ┐
                         ├─ 通过 egressStreamId=102
    ↓ UDP 传输           ┘

Client:
  EgressListener.onMessage()
    msgType = buffer[0]
    if (msgType == 1)
      printOrder()
    else if (msgType == 2)
      printUser()
```

---

## 总结

### ❌ 不能用 streamId 做什么

1. ❌ **不能**用不同的 ingressStreamId 发送不同类型的消息
2. ❌ **不能**用不同的 egressStreamId 接收不同类型的消息
3. ❌ **不能**在一个 AeronCluster 连接中使用多个 ingress/egress streamId

### ✅ 应该用 streamId 做什么

1. ✅ 区分不同的应用或客户端
2. ✅ 避免同一机器上多个进程的 streamId 冲突
3. ✅ 保持默认值（101/102），除非有特殊需求

### ✅ 应该用消息类型字节做什么

1. ✅ 区分不同的业务消息类型（Order、User、Trade 等）
2. ✅ 在 ClusteredService 中根据类型分发消息
3. ✅ 在订阅者中根据类型解码消息

### 最佳实践

```java
// ✅ 推荐：一个客户端，一个连接，消息类型用第一个字节区分
AeronCluster cluster = AeronCluster.connect(new AeronCluster.Context()
    .ingressEndpoints("0=localhost:9002,1=localhost:9102,2=localhost:9202"));
    // 使用默认 streamId (101/102)

// 发送不同类型的消息
sendOrder(cluster, order);   // buffer[0] = 1
sendUser(cluster, user);     // buffer[0] = 2

// ClusteredService 根据 buffer[0] 分发
// 订阅者根据 buffer[0] 解码
```

这就是我们在 `PubSubClusteredService` 和 `ClusterSubscriber` 中使用的方法！

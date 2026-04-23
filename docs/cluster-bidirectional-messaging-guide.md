# Aeron Cluster 双向消息传递完全指南

## 核心问题：集群可以主动发送消息吗？

### 答案：可以！集群不仅是接收者，更是业务服务提供者

Aeron Cluster 的 ClusteredService **既可以接收消息，也可以主动发送消息给客户端**。

---

## 1. 完整的消息流转模型

```
┌─────────────────────────────────────────────────────────────────┐
│                    完整的消息双向流动                            │
└─────────────────────────────────────────────────────────────────┘

客户端 A                          集群节点                       客户端 B
   │                                                                │
   │ 1. 发送订单请求                                                │
   ├──────────────────────>  Leader (ConsensusModule)              │
   │   "BUY AAPL 100 @ $150"       │                               │
   │                               │                               │
   │                               ↓ 2. 写入日志                    │
   │                        [日志] BUY Order                        │
   │                               │                               │
   │                               ↓ 3. 复制到 Followers            │
   │                        Follower1, Follower2                   │
   │                               │                               │
   │                               ↓ 4. 提交后所有节点消费           │
   │                    ┌──────────┴──────────┐                    │
   │                    ↓                     ↓                    │
   │           Node0: Service             Node1: Service           │
   │                    ↓                     ↓                    │
   │           onSessionMessage()     onSessionMessage()           │
   │           - 更新订单簿            - 更新订单簿                  │
   │           - 匹配卖单              - 匹配卖单                    │
   │           - 生成成交通知          - 生成成交通知                 │
   │                    │                     │                    │
   │                    ↓ 5. Leader 发送响应（只有 Leader！）        │
   │ <──────────────────┤                                          │
   │  "订单已接受 #12345"                                            │
   │                    │                                          │
   │                    ↓ 6. Leader 主动通知客户端 B（卖方）         │
   │                    ├────────────────────────────────────────> │
   │                    "您的卖单已成交 #67890"                      │
   │                                                                │
```

**关键点**：
- ✅ **可以接收消息**：客户端 A 发送订单请求
- ✅ **可以响应请求**：Leader 回复 "订单已接受"
- ✅ **可以主动通知**：Leader 通知客户端 B "卖单已成交"
- ✅ **可以实现复杂业务**：订单匹配、状态更新、事件通知

---

## 2. ClusteredService 的双重角色

### 2.1 作为**消费者**（接收消息）

```java
@Override
public void onSessionMessage(
    final ClientSession session,
    final long timestamp,
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final Header header)
{
    // ✅ 接收客户端发送的消息
    final String message = buffer.getStringWithoutLengthAscii(offset, length);
    System.out.println("收到消息: " + message);

    // 处理业务逻辑
    processBusinessLogic(message);
}
```

### 2.2 作为**生产者**（发送消息）

```java
@Override
public void onSessionMessage(
    final ClientSession session,
    final long timestamp,
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final Header header)
{
    // ✅ 主动发送响应给客户端
    final String response = "处理完成: " + processedResult;
    sendResponse(session, response);

    // ✅ 主动通知其他客户端
    notifyOtherClients(affectedSessions, "状态更新: " + event);
}

private void sendResponse(final ClientSession session, final String message)
{
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final int length = buffer.putStringWithoutLengthAscii(0, message);

    // 发送消息（只有 Leader 会真正发送）
    session.offer(buffer, 0, length);
}
```

---

## 3. 实际例子：订单匹配系统

### 3.1 完整代码实现

```java
/**
 * 交易订单匹配服务：演示双向消息传递
 */
public class TradingClusteredService implements ClusteredService
{
    // ==================== 业务状态 ====================

    // 买单簿：价格 -> 订单列表
    private final Map<Long, Queue<Order>> buyOrders = new TreeMap<>(Comparator.reverseOrder());

    // 卖单簿：价格 -> 订单列表
    private final Map<Long, Queue<Order>> sellOrders = new TreeMap<>();

    // 活跃会话：sessionId -> ClientSession
    private final Map<Long, ClientSession> activeSessions = new HashMap<>();

    // 订单 ID 生成器
    private long nextOrderId = 1;

    // ==================== 订单数据结构 ====================

    static class Order
    {
        final long orderId;
        final long sessionId;
        final String symbol;
        final long price;
        final long quantity;
        final boolean isBuy;

        Order(long orderId, long sessionId, String symbol, long price, long quantity, boolean isBuy)
        {
            this.orderId = orderId;
            this.sessionId = sessionId;
            this.symbol = symbol;
            this.price = price;
            this.quantity = quantity;
            this.isBuy = isBuy;
        }
    }

    // ==================== 生命周期回调 ====================

    @Override
    public void onSessionOpen(ClientSession session, long timestamp)
    {
        // 记录活跃会话（用于主动通知）
        activeSessions.put(session.id(), session);

        // 发送欢迎消息
        sendToSession(session, "欢迎连接到交易系统! sessionId=" + session.id());
    }

    @Override
    public void onSessionClose(ClientSession session, long timestamp, CloseReason closeReason)
    {
        // 移除会话
        activeSessions.remove(session.id());

        // 取消该会话的所有订单
        cancelAllOrders(session.id());
    }

    // ==================== 消息处理（接收 + 发送） ====================

    @Override
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        // ✅ 1. 接收客户端消息
        final String message = buffer.getStringWithoutLengthAscii(offset, length);
        System.out.println("[Service] 收到消息: " + message + " from session " + session.id());

        // ✅ 2. 解析命令
        final String[] parts = message.split(" ");
        final String command = parts[0];

        switch (command)
        {
            case "BUY":
            case "SELL":
                handleOrderCommand(session, parts, timestamp);
                break;

            case "CANCEL":
                handleCancelCommand(session, parts);
                break;

            case "QUERY":
                handleQueryCommand(session, parts);
                break;

            default:
                sendToSession(session, "错误: 未知命令 " + command);
        }
    }

    // ==================== 业务逻辑：订单处理 ====================

    private void handleOrderCommand(ClientSession session, String[] parts, long timestamp)
    {
        // 解析: BUY AAPL 100 150 (买入 AAPL 100股 @ $150)
        final boolean isBuy = "BUY".equals(parts[0]);
        final String symbol = parts[1];
        final long quantity = Long.parseLong(parts[2]);
        final long price = Long.parseLong(parts[3]);

        // 创建订单
        final Order order = new Order(nextOrderId++, session.id(), symbol, price, quantity, isBuy);

        // ✅ 3. 发送确认响应给下单客户端
        sendToSession(session, "订单已接受 #" + order.orderId + " (" +
            (isBuy ? "买入" : "卖出") + " " + symbol + " " + quantity + " @ $" + price + ")");

        // ✅ 4. 尝试匹配订单
        final List<Trade> trades = matchOrder(order);

        // ✅ 5. 主动通知成交双方
        for (final Trade trade : trades)
        {
            // 通知买方
            final ClientSession buyerSession = activeSessions.get(trade.buyerSessionId);
            if (null != buyerSession)
            {
                sendToSession(buyerSession,
                    "成交通知: 买入 " + trade.symbol + " " + trade.quantity + " @ $" + trade.price +
                    " (订单 #" + trade.buyOrderId + ")");
            }

            // 通知卖方
            final ClientSession sellerSession = activeSessions.get(trade.sellerSessionId);
            if (null != sellerSession)
            {
                sendToSession(sellerSession,
                    "成交通知: 卖出 " + trade.symbol + " " + trade.quantity + " @ $" + trade.price +
                    " (订单 #" + trade.sellOrderId + ")");
            }
        }

        // 如果订单未完全成交，添加到订单簿
        if (!order.isBuy && trades.isEmpty())
        {
            sellOrders.computeIfAbsent(order.price, k -> new LinkedList<>()).add(order);
        }
        else if (order.isBuy && trades.isEmpty())
        {
            buyOrders.computeIfAbsent(order.price, k -> new LinkedList<>()).add(order);
        }
    }

    // ==================== 订单匹配逻辑 ====================

    static class Trade
    {
        final String symbol;
        final long price;
        final long quantity;
        final long buyOrderId;
        final long sellOrderId;
        final long buyerSessionId;
        final long sellerSessionId;

        Trade(String symbol, long price, long quantity,
              long buyOrderId, long sellOrderId,
              long buyerSessionId, long sellerSessionId)
        {
            this.symbol = symbol;
            this.price = price;
            this.quantity = quantity;
            this.buyOrderId = buyOrderId;
            this.sellOrderId = sellOrderId;
            this.buyerSessionId = buyerSessionId;
            this.sellerSessionId = sellerSessionId;
        }
    }

    private List<Trade> matchOrder(Order newOrder)
    {
        final List<Trade> trades = new ArrayList<>();

        if (newOrder.isBuy)
        {
            // 买单：匹配价格 <= 买价的卖单
            for (final Map.Entry<Long, Queue<Order>> entry : sellOrders.entrySet())
            {
                if (entry.getKey() > newOrder.price) break;  // 卖价太高，停止匹配

                final Queue<Order> orders = entry.getValue();
                while (!orders.isEmpty())
                {
                    final Order sellOrder = orders.peek();

                    // 创建成交记录
                    final long tradeQuantity = Math.min(newOrder.quantity, sellOrder.quantity);
                    trades.add(new Trade(
                        newOrder.symbol,
                        sellOrder.price,  // 使用卖方价格
                        tradeQuantity,
                        newOrder.orderId,
                        sellOrder.orderId,
                        newOrder.sessionId,
                        sellOrder.sessionId));

                    // 如果卖单完全成交，移除
                    if (tradeQuantity == sellOrder.quantity)
                    {
                        orders.poll();
                    }

                    // 如果买单完全成交，停止匹配
                    if (tradeQuantity == newOrder.quantity)
                    {
                        return trades;
                    }
                }
            }
        }
        else
        {
            // 卖单：匹配价格 >= 卖价的买单
            for (final Map.Entry<Long, Queue<Order>> entry : buyOrders.entrySet())
            {
                if (entry.getKey() < newOrder.price) break;  // 买价太低，停止匹配

                final Queue<Order> orders = entry.getValue();
                while (!orders.isEmpty())
                {
                    final Order buyOrder = orders.peek();

                    final long tradeQuantity = Math.min(newOrder.quantity, buyOrder.quantity);
                    trades.add(new Trade(
                        newOrder.symbol,
                        buyOrder.price,  // 使用买方价格
                        tradeQuantity,
                        buyOrder.orderId,
                        newOrder.orderId,
                        buyOrder.sessionId,
                        newOrder.sessionId));

                    if (tradeQuantity == buyOrder.quantity)
                    {
                        orders.poll();
                    }

                    if (tradeQuantity == newOrder.quantity)
                    {
                        return trades;
                    }
                }
            }
        }

        return trades;
    }

    // ==================== 查询命令 ====================

    private void handleQueryCommand(ClientSession session, String[] parts)
    {
        // QUERY ORDERBOOK AAPL
        if (parts.length >= 3 && "ORDERBOOK".equals(parts[1]))
        {
            final String symbol = parts[2];
            final StringBuilder sb = new StringBuilder();

            sb.append("订单簿 ").append(symbol).append(":\n");
            sb.append("买单:\n");
            for (final Map.Entry<Long, Queue<Order>> entry : buyOrders.entrySet())
            {
                sb.append("  $").append(entry.getKey()).append(": ")
                  .append(entry.getValue().size()).append(" 笔\n");
            }

            sb.append("卖单:\n");
            for (final Map.Entry<Long, Queue<Order>> entry : sellOrders.entrySet())
            {
                sb.append("  $").append(entry.getKey()).append(": ")
                  .append(entry.getValue().size()).append(" 笔\n");
            }

            sendToSession(session, sb.toString());
        }
    }

    // ==================== 取消订单 ====================

    private void handleCancelCommand(ClientSession session, String[] parts)
    {
        // CANCEL 12345
        final long orderId = Long.parseLong(parts[1]);

        // 从订单簿中移除
        boolean found = false;

        for (final Queue<Order> orders : buyOrders.values())
        {
            if (orders.removeIf(o -> o.orderId == orderId && o.sessionId == session.id()))
            {
                found = true;
                break;
            }
        }

        if (!found)
        {
            for (final Queue<Order> orders : sellOrders.values())
            {
                if (orders.removeIf(o -> o.orderId == orderId && o.sessionId == session.id()))
                {
                    found = true;
                    break;
                }
            }
        }

        if (found)
        {
            sendToSession(session, "订单已取消 #" + orderId);
        }
        else
        {
            sendToSession(session, "订单未找到 #" + orderId);
        }
    }

    private void cancelAllOrders(long sessionId)
    {
        for (final Queue<Order> orders : buyOrders.values())
        {
            orders.removeIf(o -> o.sessionId == sessionId);
        }

        for (final Queue<Order> orders : sellOrders.values())
        {
            orders.removeIf(o -> o.sessionId == sessionId);
        }
    }

    // ==================== 发送消息辅助方法 ====================

    private void sendToSession(ClientSession session, String message)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = buffer.putStringWithoutLengthAscii(0, message);

        // ✅ 发送消息（只有 Leader 会真正发送）
        while (session.offer(buffer, 0, length) < 0)
        {
            // 背压处理：等待重试
            Thread.yield();
        }
    }

    // ==================== 快照保存/恢复 ====================

    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication)
    {
        // 保存订单簿状态...
    }

    @Override
    public void onStart(Cluster cluster, Image snapshotImage)
    {
        // 恢复订单簿状态...
    }

    @Override
    public void onRoleChange(Cluster.Role newRole)
    {
        System.out.println("[Service] 角色变更: " + newRole);
    }

    @Override
    public void onTerminate(Cluster cluster)
    {
        System.out.println("[Service] 服务终止");
    }

    @Override
    public void onNewLeadershipTermEvent(
        long leadershipTermId,
        long logPosition,
        long timestamp,
        long termBaseLogPosition,
        int leaderMemberId,
        int logSessionId,
        ClusterTimeUnit timeUnit,
        int appVersion)
    {
        System.out.println("[Service] 新领导任期: termId=" + leadershipTermId + ", leaderId=" + leaderMemberId);
    }
}
```

### 3.2 使用示例

**客户端 A（买方）**：
```bash
# 连接到集群
Client A: (连接)
← 收到: "欢迎连接到交易系统! sessionId=12345"

# 发送买单
Client A: "BUY AAPL 100 150"
← 收到: "订单已接受 #1 (买入 AAPL 100 @ $150)"

# 等待匹配...
← 收到: "成交通知: 买入 AAPL 100 @ $148 (订单 #1)"  # ← 主动通知！
```

**客户端 B（卖方）**：
```bash
# 连接到集群
Client B: (连接)
← 收到: "欢迎连接到交易系统! sessionId=67890"

# 发送卖单
Client B: "SELL AAPL 100 148"
← 收到: "订单已接受 #2 (卖出 AAPL 100 @ $148)"

# 立即匹配成功！
← 收到: "成交通知: 卖出 AAPL 100 @ $148 (订单 #2)"  # ← 主动通知！
```

---

## 4. 消息发送的关键点

### 4.1 所有节点都执行业务逻辑

```
┌─────────────────────────────────────────────────────────────────┐
│             所有 3 个节点都处理相同的消息                        │
└─────────────────────────────────────────────────────────────────┘

消息: "BUY AAPL 100 150"
    │
    ├──────────────────────┬──────────────────────┬──────────────────────┐
    ↓                      ↓                      ↓                      ↓
Node 0 (Leader)        Node 1 (Follower)     Node 2 (Follower)
    │                      │                      │
    ↓                      ↓                      ↓
onSessionMessage()    onSessionMessage()    onSessionMessage()
    │                      │                      │
    ↓                      ↓                      ↓
matchOrder()          matchOrder()          matchOrder()
    │                      │                      │
    ↓                      ↓                      ↓
buyOrders.add(...)    buyOrders.add(...)    buyOrders.add(...)
    │                      │                      │
    ↓                      ↓                      ↓
session.offer(...)    session.offer(...)    session.offer(...)
    │                      │                      │
    ✅ 发送成功              ❌ 检测到非 Leader      ❌ 检测到非 Leader
                           不发送                 不发送
```

**关键**：
- 所有节点都执行 `session.offer()`
- 但只有 **Leader 的响应真正发送给客户端**
- Followers 的 `session.offer()` 内部会检测到自己不是 Leader，直接返回成功但不发送

### 4.2 如何判断是否为 Leader？

```java
// 在 ClientSession.offer() 内部实现中：

public long offer(DirectBuffer buffer, int offset, int length)
{
    // 检查当前节点角色
    if (cluster.role() != Cluster.Role.LEADER)
    {
        // Follower 或 Candidate：不发送消息，直接返回成功
        return length;  // ← 假装发送成功
    }

    // Leader：真正发送消息
    return publication.offer(buffer, offset, length);
}
```

---

## 5. 实际应用场景

### 5.1 游戏服务器

```java
public class GameClusteredService implements ClusteredService
{
    private final Map<Long, Player> players = new HashMap<>();
    private final Map<Long, ClientSession> sessions = new HashMap<>();

    @Override
    public void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header)
    {
        final String command = parseCommand(buffer, offset, length);

        switch (command)
        {
            case "MOVE":
                // 1. 更新玩家位置
                updatePlayerPosition(session.id(), x, y);

                // 2. 广播给所有在线玩家
                broadcastToAllPlayers("玩家 " + session.id() + " 移动到 (" + x + ", " + y + ")");
                break;

            case "ATTACK":
                // 1. 处理攻击逻辑
                final long targetId = parseTargetId(buffer);
                final int damage = calculateDamage();

                // 2. 通知攻击者
                sendToSession(session, "你对玩家 " + targetId + " 造成 " + damage + " 伤害");

                // 3. 通知被攻击者
                final ClientSession targetSession = sessions.get(targetId);
                sendToSession(targetSession, "你被玩家 " + session.id() + " 攻击，损失 " + damage + " 生命值");

                // 4. 广播给其他玩家
                broadcastToOthers(session.id(), "玩家 " + session.id() + " 攻击了玩家 " + targetId);
                break;
        }
    }

    private void broadcastToAllPlayers(String message)
    {
        for (final ClientSession session : sessions.values())
        {
            sendToSession(session, message);
        }
    }
}
```

### 5.2 聊天服务器

```java
public class ChatClusteredService implements ClusteredService
{
    private final Map<String, Set<Long>> chatRooms = new HashMap<>();

    @Override
    public void onSessionMessage(...)
    {
        final String command = parseCommand(buffer);

        switch (command)
        {
            case "JOIN":
                final String roomName = parseRoomName(buffer);
                chatRooms.computeIfAbsent(roomName, k -> new HashSet<>()).add(session.id());

                // 通知房间内所有用户
                broadcastToRoom(roomName, "用户 " + session.id() + " 加入了房间");
                break;

            case "SEND":
                final String message = parseMessage(buffer);

                // 广播给房间内所有用户
                broadcastToRoom(currentRoom, session.id() + ": " + message);
                break;
        }
    }
}
```

### 5.3 实时通知系统

```java
public class NotificationClusteredService implements ClusteredService
{
    private final Map<Long, Set<Long>> subscriptions = new HashMap<>();  // userId -> sessionIds

    @Override
    public void onSessionMessage(...)
    {
        final String command = parseCommand(buffer);

        switch (command)
        {
            case "SUBSCRIBE":
                final long userId = parseUserId(buffer);
                subscriptions.computeIfAbsent(userId, k -> new HashSet<>()).add(session.id());
                sendToSession(session, "已订阅用户 " + userId + " 的通知");
                break;

            case "NOTIFY":
                final long targetUserId = parseUserId(buffer);
                final String notification = parseNotification(buffer);

                // 主动通知所有订阅者
                final Set<Long> subscribers = subscriptions.get(targetUserId);
                if (null != subscribers)
                {
                    for (final long sessionId : subscribers)
                    {
                        final ClientSession targetSession = activeSessions.get(sessionId);
                        sendToSession(targetSession, "通知: " + notification);
                    }
                }
                break;
        }
    }
}
```

---

## 6. 关键约束与最佳实践

### 6.1 确定性约束

**允许的操作**：
- ✅ 接收客户端消息
- ✅ 发送响应给客户端
- ✅ 主动通知其他客户端
- ✅ 使用 `cluster.time()` 获取确定性时间
- ✅ 修改业务状态（订单簿、玩家位置等）

**禁止的操作**：
- ❌ 使用 `System.currentTimeMillis()`（非确定性）
- ❌ 使用 `new Random()`（未提供种子）
- ❌ 调用外部 API（网络 IO）
- ❌ 在 `onStart`、`onTakeSnapshot` 中发送消息

### 6.2 消息发送时机

**正确**：
```java
@Override
public void onSessionMessage(...)
{
    // ✅ 在 onSessionMessage 中发送消息
    session.offer(buffer, 0, length);
}

@Override
public void onTimerEvent(...)
{
    // ✅ 在定时器回调中发送消息
    session.offer(buffer, 0, length);
}
```

**错误**：
```java
@Override
public void onStart(Cluster cluster, Image snapshotImage)
{
    // ❌ 不能在 onStart 中发送消息
    session.offer(buffer, 0, length);  // ← 错误！
}

@Override
public void onTakeSnapshot(ExclusivePublication snapshotPublication)
{
    // ❌ 不能在快照时发送消息
    session.offer(buffer, 0, length);  // ← 错误！
}
```

### 6.3 背压处理

```java
private void sendToSession(ClientSession session, String message)
{
    final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    final int length = buffer.putStringWithoutLengthAscii(0, message);

    // 正确的背压处理
    while (true)
    {
        final long result = session.offer(buffer, 0, length);

        if (result > 0)
        {
            break;  // 发送成功
        }
        else if (result == Publication.BACK_PRESSURED)
        {
            // 背压：等待重试
            Thread.yield();
        }
        else
        {
            // 其他错误（如会话关闭）
            System.err.println("发送失败: " + result);
            break;
        }
    }
}
```

---

## 7. 总结

### 问题：集群可以主动发送消息吗？

**答案**：✅ **可以！而且这是 Aeron Cluster 的核心能力！**

| 能力 | 支持情况 | 说明 |
|------|----------|------|
| **接收客户端消息** | ✅ 完全支持 | `onSessionMessage()` 接收 |
| **响应客户端请求** | ✅ 完全支持 | `session.offer()` 发送响应 |
| **主动通知其他客户端** | ✅ 完全支持 | 通过 `activeSessions` 主动发送 |
| **广播消息** | ✅ 完全支持 | 遍历所有会话发送 |
| **复杂业务逻辑** | ✅ 完全支持 | 订单匹配、游戏逻辑、聊天室等 |

### 核心设计理念

1. **ClusteredService 是业务服务提供者**，不仅仅是被动接收者
2. **所有节点运行相同的业务逻辑**，保证状态一致性
3. **只有 Leader 发送响应**，避免重复消息
4. **确定性约束**确保所有节点状态完全一致

### 典型应用场景

- ✅ 交易系统（订单匹配、成交通知）
- ✅ 游戏服务器（位置广播、战斗通知）
- ✅ 聊天服务器（消息转发、房间广播）
- ✅ 实时通知系统（事件推送、订阅通知）
- ✅ 协作编辑（文档变更广播）

Aeron Cluster 提供的是**高可用、强一致性的业务服务平台**，而不是简单的消息队列！

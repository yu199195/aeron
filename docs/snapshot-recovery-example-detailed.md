# Aeron Cluster 快照恢复详细示例

## 目录
1. [问题场景](#1-问题场景)
2. [完整代码示例](#2-完整代码示例)
3. [快照保存详解](#3-快照保存详解)
4. [快照恢复详解](#4-快照恢复详解)
5. [时间线完整流程](#5-时间线完整流程)
6. [快照的作用](#6-快照的作用)
7. [常见问题](#7-常见问题)

---

## 1. 问题场景

假设你有一个订单管理系统，业务状态包含：
- **订单集合**：`Map<Long, Order> orders`（100 万订单）
- **用户集合**：`Map<Long, User> users`（10 万用户）

**问题**：
1. 如何将这些集合保存到快照？
2. 重启后如何从快照恢复这些集合？
3. 快照到底起什么作用？

让我用一个完整的代码示例来说明。

---

## 2. 完整代码示例

### 2.1 业务数据结构

```java
// 订单类
class Order {
    long orderId;       // 订单 ID
    long userId;        // 用户 ID
    int amount;         // 订单金额
    OrderStatus status; // 订单状态

    enum OrderStatus {
        PENDING,   // 待支付
        PAID,      // 已支付
        SHIPPED,   // 已发货
        COMPLETED  // 已完成
    }
}

// 用户类
class User {
    long userId;        // 用户 ID
    String name;        // 用户名
    int balance;        // 账户余额
}
```

### 2.2 业务服务实现（完整代码）

```java
/**
 * 订单管理服务：演示如何保存和恢复集合数据
 */
class OrderService implements ClusteredService {

    // ==================== 业务状态（需要保存到快照的数据） ====================
    private final Map<Long, Order> orders = new HashMap<>();  // 订单集合
    private final Map<Long, User> users = new HashMap<>();    // 用户集合
    private long nextOrderId = 1;  // 下一个订单 ID

    // ==================== 集群接口引用 ====================
    private Cluster cluster;  // 集群接口（用于发送消息、调度定时器等）

    // ==================== 消息处理 ====================
    @Override
    public void onSessionMessage(
        ClientSession session,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header)
    {
        // 解析客户端消息
        String message = buffer.getStringWithoutLengthAscii(offset, length);
        String[] parts = message.split(":");
        String command = parts[0];

        if ("CREATE_ORDER".equals(command)) {
            // 创建订单：CREATE_ORDER:userId:amount
            long userId = Long.parseLong(parts[1]);
            int amount = Integer.parseInt(parts[2]);

            // 创建订单对象
            Order order = new Order();
            order.orderId = nextOrderId++;
            order.userId = userId;
            order.amount = amount;
            order.status = Order.OrderStatus.PENDING;

            // 保存订单到 Map
            orders.put(order.orderId, order);

            // 回复客户端
            String response = "ORDER_CREATED:" + order.orderId;
            session.offer(buffer, offset, response.length());

            System.out.println("创建订单：" + order.orderId + ", 用户：" + userId + ", 金额：" + amount);
        }
        else if ("PAY_ORDER".equals(command)) {
            // 支付订单：PAY_ORDER:orderId
            long orderId = Long.parseLong(parts[1]);
            Order order = orders.get(orderId);

            if (order != null) {
                order.status = Order.OrderStatus.PAID;
                System.out.println("订单已支付：" + orderId);
            }
        }
        // ... 其他命令
    }

    // ==================== 快照保存（关键！） ====================
    /**
     * 保存快照：将业务状态（orders、users）序列化到快照。
     * <p>
     * 这个方法会在以下情况被调用：
     * 1. Leader 检测到日志增长超过阈值（如 100 万条）
     * 2. 定时触发（如每 5 分钟）
     * 3. 手动触发
     */
    @Override
    public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
        System.out.println("========== 开始保存快照 ==========");
        System.out.println("当前订单数量：" + orders.size());
        System.out.println("当前用户数量：" + users.size());

        // ==================== 创建缓冲区 ====================
        // 使用 1MB 缓冲区（增量写入，避免阻塞）
        ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1_000_000);
        int offset = 0;

        // ==================== 1. 保存订单数量 ====================
        buffer.putInt(offset, orders.size());
        offset += 4;
        System.out.println("写入订单数量：" + orders.size());

        // ==================== 2. 保存每个订单 ====================
        int orderCount = 0;
        for (Map.Entry<Long, Order> entry : orders.entrySet()) {
            Order order = entry.getValue();

            // 序列化订单到 buffer
            buffer.putLong(offset, order.orderId);      // 8 bytes
            offset += 8;
            buffer.putLong(offset, order.userId);       // 8 bytes
            offset += 8;
            buffer.putInt(offset, order.amount);        // 4 bytes
            offset += 4;
            buffer.putInt(offset, order.status.ordinal());  // 4 bytes（枚举序号）
            offset += 4;

            orderCount++;

            // 每 1MB 写入一次（避免缓冲区溢出，也避免阻塞主线程）
            if (offset >= 900_000) {  // 留 100KB 余量
                snapshotPublication.offer(buffer, 0, offset);  // ← 非阻塞写入
                System.out.println("写入订单批次：" + orderCount + " 条，大小：" + offset + " bytes");
                offset = 0;  // 重置 offset
            }
        }

        // ==================== 3. 保存用户数量 ====================
        buffer.putInt(offset, users.size());
        offset += 4;
        System.out.println("写入用户数量：" + users.size());

        // ==================== 4. 保存每个用户 ====================
        int userCount = 0;
        for (Map.Entry<Long, User> entry : users.entrySet()) {
            User user = entry.getValue();

            // 序列化用户到 buffer
            buffer.putLong(offset, user.userId);        // 8 bytes
            offset += 8;
            buffer.putInt(offset, user.balance);        // 4 bytes
            offset += 4;

            // 保存用户名（变长字符串）
            int nameLength = user.name.length();
            buffer.putInt(offset, nameLength);          // 4 bytes（字符串长度）
            offset += 4;
            buffer.putStringWithoutLengthAscii(offset, user.name);  // nameLength bytes
            offset += nameLength;

            userCount++;

            // 每 1MB 写入一次
            if (offset >= 900_000) {
                snapshotPublication.offer(buffer, 0, offset);
                System.out.println("写入用户批次：" + userCount + " 条");
                offset = 0;
            }
        }

        // ==================== 5. 保存 nextOrderId ====================
        buffer.putLong(offset, nextOrderId);
        offset += 8;
        System.out.println("写入 nextOrderId：" + nextOrderId);

        // ==================== 6. 写入剩余数据 ====================
        if (offset > 0) {
            snapshotPublication.offer(buffer, 0, offset);
            System.out.println("写入剩余数据：" + offset + " bytes");
        }

        System.out.println("========== 快照保存完成 ==========");
        System.out.println("总订单数：" + orders.size());
        System.out.println("总用户数：" + users.size());
    }

    // ==================== 快照恢复（关键！） ====================
    /**
     * 启动时从快照恢复：反序列化快照数据，恢复业务状态。
     * <p>
     * 这个方法会在以下情况被调用：
     * 1. 节点重启时
     * 2. 新节点加入集群时
     *
     * @param cluster 集群接口
     * @param snapshotImage 快照 Image（null 表示首次启动，没有快照）
     */
    @Override
    public void onStart(Cluster cluster, Image snapshotImage) {
        this.cluster = cluster;

        if (snapshotImage == null) {
            // ==================== 首次启动，没有快照 ====================
            System.out.println("========== 首次启动，没有快照 ==========");
            System.out.println("从空状态开始");
            return;
        }

        // ==================== 从快照恢复 ====================
        System.out.println("========== 开始从快照恢复 ==========");

        // 创建快照读取处理器
        final FragmentHandler fragmentHandler = new FragmentHandler() {
            private final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer();
            private int totalBytesRead = 0;
            private int currentOffset = 0;
            private boolean ordersLoaded = false;
            private boolean usersLoaded = false;
            private int expectedOrderCount = 0;
            private int loadedOrderCount = 0;
            private int expectedUserCount = 0;
            private int loadedUserCount = 0;

            @Override
            public void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
                System.out.println("读取快照片段：offset=" + offset + ", length=" + length);

                // 将数据复制到临时缓冲区（因为可能跨多个片段）
                tempBuffer.putBytes(currentOffset, buffer, offset, length);
                currentOffset += length;
                totalBytesRead += length;
            }
        };

        // ==================== 循环读取快照数据 ====================
        while (true) {
            int fragments = snapshotImage.poll(fragmentHandler, 10);  // 最多读取 10 个片段

            if (fragments == 0 && snapshotImage.isClosed()) {
                // 快照读取完成
                break;
            }

            if (fragments == 0) {
                // 无数据，等待
                cluster.idle();
                continue;
            }
        }

        System.out.println("快照读取完成，总字节数：" + fragmentHandler.totalBytesRead);

        // ==================== 反序列化快照数据 ====================
        final ExpandableArrayBuffer tempBuffer = fragmentHandler.tempBuffer;
        int offset = 0;

        // 1. 读取订单数量
        int orderCount = tempBuffer.getInt(offset);
        offset += 4;
        System.out.println("快照中的订单数量：" + orderCount);

        // 2. 恢复每个订单
        orders.clear();  // 清空旧数据
        for (int i = 0; i < orderCount; i++) {
            Order order = new Order();
            order.orderId = tempBuffer.getLong(offset);
            offset += 8;
            order.userId = tempBuffer.getLong(offset);
            offset += 8;
            order.amount = tempBuffer.getInt(offset);
            offset += 4;
            order.status = Order.OrderStatus.values()[tempBuffer.getInt(offset)];
            offset += 4;

            orders.put(order.orderId, order);

            if (i < 5 || i >= orderCount - 5) {  // 打印前 5 个和后 5 个
                System.out.println("恢复订单：orderId=" + order.orderId +
                                 ", userId=" + order.userId +
                                 ", amount=" + order.amount +
                                 ", status=" + order.status);
            } else if (i == 5) {
                System.out.println("...");
            }
        }

        // 3. 读取用户数量
        int userCount = tempBuffer.getInt(offset);
        offset += 4;
        System.out.println("快照中的用户数量：" + userCount);

        // 4. 恢复每个用户
        users.clear();
        for (int i = 0; i < userCount; i++) {
            User user = new User();
            user.userId = tempBuffer.getLong(offset);
            offset += 8;
            user.balance = tempBuffer.getInt(offset);
            offset += 4;

            // 读取用户名（变长字符串）
            int nameLength = tempBuffer.getInt(offset);
            offset += 4;
            user.name = tempBuffer.getStringWithoutLengthAscii(offset, nameLength);
            offset += nameLength;

            users.put(user.userId, user);

            if (i < 5) {
                System.out.println("恢复用户：userId=" + user.userId +
                                 ", name=" + user.name +
                                 ", balance=" + user.balance);
            }
        }

        // 5. 恢复 nextOrderId
        nextOrderId = tempBuffer.getLong(offset);
        offset += 8;
        System.out.println("恢复 nextOrderId：" + nextOrderId);

        System.out.println("========== 快照恢复完成 ==========");
        System.out.println("恢复订单数：" + orders.size());
        System.out.println("恢复用户数：" + users.size());
        System.out.println("下一个订单 ID：" + nextOrderId);
    }

    // ... 其他回调方法
}
```

---

## 3. 快照保存详解

### 3.1 快照保存的数据结构

```
┌──────────────────────────────────────────────────────┐
│                 快照内容（详细结构）                  │
├──────────────────────────────────────────────────────┤
│  1. 订单数量（4 bytes）                              │
│     orderCount: 1000000                              │
├──────────────────────────────────────────────────────┤
│  2. 订单列表（每个订单 24 bytes）                    │
│     订单 1:                                          │
│       - orderId: 1 (8 bytes)                         │
│       - userId: 100 (8 bytes)                        │
│       - amount: 500 (4 bytes)                        │
│       - status: PAID (4 bytes, 枚举序号 = 1)         │
│                                                      │
│     订单 2:                                          │
│       - orderId: 2                                   │
│       - userId: 101                                  │
│       - amount: 300                                  │
│       - status: SHIPPED                              │
│                                                      │
│     ... (共 1000000 个订单)                          │
├──────────────────────────────────────────────────────┤
│  3. 用户数量（4 bytes）                              │
│     userCount: 100000                                │
├──────────────────────────────────────────────────────┤
│  4. 用户列表（每个用户 20+ bytes）                   │
│     用户 1:                                          │
│       - userId: 100 (8 bytes)                        │
│       - balance: 1000 (4 bytes)                      │
│       - nameLength: 5 (4 bytes)                      │
│       - name: "Alice" (5 bytes)                      │
│                                                      │
│     用户 2:                                          │
│       - userId: 101                                  │
│       - balance: 500                                 │
│       - nameLength: 3                                │
│       - name: "Bob"                                  │
│                                                      │
│     ... (共 100000 个用户)                           │
├──────────────────────────────────────────────────────┤
│  5. nextOrderId（8 bytes）                           │
│     nextOrderId: 1000001                             │
└──────────────────────────────────────────────────────┘

总大小：约 24MB
- 订单：1000000 * 24 bytes = 24MB
- 用户：100000 * 20 bytes = 2MB
- 其他：< 1MB
```

### 3.2 增量写入的重要性

```java
// ❌ 错误做法：一次性写入所有数据（可能阻塞主线程）
ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(24_000_000);  // 24MB
int offset = 0;

// 序列化所有订单到 buffer
for (Order order : orders.values()) {
    // 序列化...
    offset += 24;
}

// 一次性写入 24MB（可能需要多次重试，阻塞主线程）
snapshotPublication.offer(buffer, 0, offset);  // ← 可能阻塞！


// ✅ 正确做法：增量写入（每 1MB 写入一次）
ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(1_000_000);  // 1MB
int offset = 0;

for (Order order : orders.values()) {
    // 序列化订单
    buffer.putLong(offset, order.orderId);
    offset += 8;
    // ...
    offset += 24;

    // 每 1MB 写入一次
    if (offset >= 900_000) {
        snapshotPublication.offer(buffer, 0, offset);  // ← 立即返回，不阻塞
        offset = 0;  // 重置 offset
    }
}

// 写入剩余数据
if (offset > 0) {
    snapshotPublication.offer(buffer, 0, offset);
}
```

---

## 4. 快照恢复详解

### 4.1 恢复流程图

```
┌────────────────────────────────────────────────────────────┐
│              集群重启 → 从快照恢复流程                      │
└────────────────────────────────────────────────────────────┘

1. 节点重启
   └─> ClusteredServiceContainer.launch()
       └─> ClusteredServiceAgent.onStart()

2. 等待 Recovery State 计数器
   └─> awaitRecoveryCounter()
       └─> ConsensusModule 创建此计数器

3. 读取恢复信息
   ├─> logPosition = 5000000（快照对应的日志位置）
   ├─> snapshotRecordingId = 100（快照 Recording ID）
   └─> leadershipTermId = 5

4. 加载快照
   ├─> 连接 AeronArchive
   ├─> archive.startReplay(recordingId=100, ...)
   └─> 订阅快照回放

5. 读取快照数据
   └─> while (!snapshotImage.isClosed()) {
           snapshotImage.poll(fragmentHandler, 10);
           // fragmentHandler 读取快照片段
       }

6. 反序列化快照数据
   ├─> 读取订单数量：1000000
   ├─> 恢复每个订单到 orders Map
   ├─> 读取用户数量：100000
   ├─> 恢复每个用户到 users Map
   └─> 恢复 nextOrderId：1000001

7. 调用业务逻辑恢复
   └─> service.onStart(cluster, snapshotImage)
       └─> 业务逻辑从 snapshotImage 恢复状态

8. 发送就绪 ACK
   └─> consensusModuleProxy.ack(logPosition, ...)

9. 订阅日志（从 logPosition 开始）
   └─> ConsensusModule 发送 JOIN_LOG 命令
       └─> Service 订阅日志，从 logPosition = 5000000 开始

10. 回放日志（恢复快照之后的操作）
    └─> 回放 logPosition 5000001 → 5010000 的日志
        ├─> onSessionMessage(...) → 处理 CREATE_ORDER
        ├─> onSessionMessage(...) → 处理 PAY_ORDER
        └─> ...

11. 进入正常工作状态
    └─> 继续处理新消息
```

### 4.2 恢复后的状态

```
恢复完成后的业务状态：
┌─────────────────────────────────────────────┐
│  订单集合（从快照恢复）                      │
│  orders = {                                 │
│    1 → Order(id=1, userId=100, amount=500,  │
│              status=PAID)                   │
│    2 → Order(id=2, userId=101, amount=300,  │
│              status=SHIPPED)                │
│    ...                                      │
│    1000000 → Order(...)                     │
│  }                                          │
├─────────────────────────────────────────────┤
│  用户集合（从快照恢复）                      │
│  users = {                                  │
│    100 → User(id=100, name="Alice",         │
│              balance=1000)                  │
│    101 → User(id=101, name="Bob",           │
│              balance=500)                   │
│    ...                                      │
│    100100 → User(...)                       │
│  }                                          │
├─────────────────────────────────────────────┤
│  nextOrderId = 1000001（从快照恢复）        │
└─────────────────────────────────────────────┘

日志回放后（快照之后的 10000 条消息）：
┌─────────────────────────────────────────────┐
│  新增订单（从日志回放）                      │
│  orders.put(1000001, ...)  ← 日志 5000001   │
│  orders.put(1000002, ...)  ← 日志 5000002   │
│  ...                                        │
│  orders.put(1010000, ...)  ← 日志 5010000   │
├─────────────────────────────────────────────┤
│  最终状态：                                  │
│  orders.size() = 1010000（恢复 100 万 + 回放 1 万）│
│  users.size() = 100000                      │
│  nextOrderId = 1010001                      │
└─────────────────────────────────────────────┘
```

---

## 5. 时间线完整流程

### 5.1 从运行到重启的完整时间线

```
┌────────────────────────────────────────────────────────────┐
│          时间线：运行 → 打快照 → 重启 → 恢复                │
└────────────────────────────────────────────────────────────┘

================== 第一阶段：正常运行 ==================
t0 (00:00):
  系统启动（首次启动，没有快照）
  - orders.size() = 0
  - users.size() = 0
  - nextOrderId = 1
  - logPosition = 0

t1 (00:01):
  接收客户端请求（创建订单）
  - onSessionMessage("CREATE_ORDER:100:500")
  - orders.put(1, Order(...))  ← 订单 1
  - logPosition = 100

t2 (00:02):
  继续接收请求...
  - orders.put(2, Order(...))  ← 订单 2
  - orders.put(3, Order(...))  ← 订单 3
  - ...
  - logPosition = 1000

t3 (01:00):
  运行 1 小时后
  - orders.size() = 1000000（100 万订单）
  - users.size() = 100000（10 万用户）
  - nextOrderId = 1000001
  - logPosition = 5000000（500 万条日志）

================== 第二阶段：触发快照 ==================
t4 (01:01):
  Leader 检测到日志增长超过阈值
  - logPosition - lastSnapshotPosition = 5000000 - 0 = 5000000
  - 超过 snapshotIntervalThreshold (1000000)
  - 触发快照！

t5 (01:01):
  Leader 写入 ClusterAction.SNAPSHOT 到日志
  - logPublisher.appendClusterAction(SNAPSHOT, logPosition=5000000, ...)
  - logPosition = 5000001

t6 (01:01):
  Service 消费 ClusterAction.SNAPSHOT
  - logAdapter.poll() → onServiceAction(SNAPSHOT)
  - 调用 service.onTakeSnapshot(snapshotPublication)

t7 (01:01 - 01:02):
  Service 保存快照（耗时 1 分钟）
  - 序列化 100 万订单到快照
  - 序列化 10 万用户到快照
  - 序列化 nextOrderId 到快照
  - Archive 后台录制到磁盘
  - 快照文件：recording-100.dat (24MB)
  - 快照完成！recordingId = 100

t8 (01:02):
  Service 发送 ACK 给 ConsensusModule
  - consensusModuleProxy.ack(logPosition=5000000, recordingId=100)
  - ConsensusModule 记录快照位置
  - lastSnapshotPosition = 5000000

================== 第三阶段：继续运行（快照之后） ==================
t9 (01:03):
  继续接收请求（快照之后）
  - orders.put(1000001, Order(...))  ← 订单 1000001
  - logPosition = 5000002

t10 (01:10):
  继续运行...
  - orders.size() = 1010000（101 万订单）
  - logPosition = 5010000（501 万条日志）

================== 第四阶段：节点重启 ==================
t11 (01:11):
  节点崩溃或手动重启
  - 内存中的所有数据丢失！
  - orders = null
  - users = null
  - nextOrderId = null
  - 日志和快照仍然在磁盘上

t12 (01:12):
  节点重新启动
  - ClusteredServiceContainer.launch()
  - ClusteredServiceAgent.onStart()

================== 第五阶段：从快照恢复 ==================
t13 (01:12):
  等待 Recovery State 计数器
  - awaitRecoveryCounter()
  - 读取恢复信息：
    - logPosition = 5000000（快照对应的日志位置）
    - snapshotRecordingId = 100（快照 Recording ID）
    - leadershipTermId = 5

t14 (01:12):
  加载快照
  - archive.startReplay(recordingId=100)
  - 订阅快照回放

t15 (01:12 - 01:13):
  反序列化快照数据（耗时 1 分钟）
  - 读取订单数量：1000000
  - 恢复 100 万订单到 orders Map
  - 读取用户数量：100000
  - 恢复 10 万用户到 users Map
  - 恢复 nextOrderId = 1000001

  恢复后的状态：
  - orders.size() = 1000000  ← 从快照恢复
  - users.size() = 100000    ← 从快照恢复
  - nextOrderId = 1000001    ← 从快照恢复

t16 (01:13):
  发送就绪 ACK
  - consensusModuleProxy.ack(logPosition=5000000, ...)

t17 (01:13):
  订阅日志（从快照位置开始）
  - ConsensusModule 发送 JOIN_LOG 命令
  - Service 订阅日志，从 logPosition = 5000000 开始

================== 第六阶段：回放日志（恢复快照之后的操作） ==================
t18 (01:13 - 01:14):
  回放日志（从 logPosition 5000001 → 5010000）
  - logAdapter.poll() → 读取日志消息
  - onSessionMessage(...) → 处理 CREATE_ORDER（订单 1000001）
    - orders.put(1000001, Order(...))  ← 从日志恢复
  - onSessionMessage(...) → 处理 CREATE_ORDER（订单 1000002）
    - orders.put(1000002, Order(...))  ← 从日志恢复
  - ...
  - onSessionMessage(...) → 处理 CREATE_ORDER（订单 1010000）
    - orders.put(1010000, Order(...))  ← 从日志恢复

  回放完成后的状态：
  - orders.size() = 1010000  ← 快照 100 万 + 日志回放 1 万
  - users.size() = 100000
  - nextOrderId = 1010001
  - logPosition = 5010000

================== 第七阶段：恢复完成，进入正常工作状态 ==================
t19 (01:14):
  恢复完成！
  - 业务状态与重启前完全一致
  - orders.size() = 1010000（与 t10 时的状态一致）
  - 可以继续处理新请求

t20 (01:15):
  继续接收新请求
  - onSessionMessage("CREATE_ORDER:105:200")
  - orders.put(1010001, Order(...))  ← 新订单
  - logPosition = 5010001

总结：
- 快照保存：1 分钟（t7）
- 快照恢复：1 分钟（t15）
- 日志回放：1 分钟（t18）
- 总恢复时间：2 分钟

如果没有快照：
- 需要回放 501 万条日志（从 0 开始）
- 恢复时间：50 分钟（慢 25 倍！）
```

---

## 6. 快照的作用

### 6.1 快照的本质

**快照 = 某个时间点的完整状态备份**

```
快照保存的是：
┌─────────────────────────────────────┐
│  t7 时刻的完整业务状态：             │
│  - orders: 100 万订单               │
│  - users: 10 万用户                 │
│  - nextOrderId: 1000001             │
│  - 对应日志位置：logPosition = 5000000│
└─────────────────────────────────────┘

恢复时：
1. 先加载快照 → 恢复到 t7 时刻的状态
2. 再回放日志（5000001 → 5010000）→ 恢复到 t10 时刻的状态
3. 最终状态 = 快照状态 + 日志回放
```

### 6.2 快照的关键作用

| 作用 | 说明 | 效果 |
|------|------|------|
| **加速启动** | 不需要从头回放所有日志 | 50 分钟 → 2 分钟 |
| **减少磁盘占用** | 可以删除快照之前的旧日志 | 10GB → 150MB |
| **简化恢复** | 只需加载快照 + 回放少量日志 | 501 万条 → 1 万条 |
| **保证一致性** | 快照 + 日志回放 = 完整状态 | 100% 一致 |

### 6.3 快照 vs 数据库备份

| 对比项 | Aeron 快照 | 数据库备份 |
|-------|-----------|-----------|
| **触发方式** | 自动（日志增长）+ 手动 | 通常手动 |
| **数据格式** | 二进制序列化（高效） | SQL 转储或二进制 |
| **恢复方式** | 加载快照 + 回放日志 | 加载备份 |
| **一致性保证** | 强一致（快照 + 日志） | 取决于备份时机 |
| **性能影响** | 很小（异步、非阻塞） | 较大（可能锁表） |

---

## 7. 常见问题

### Q1: 快照保存的是内存数据还是磁盘数据？

**答**：保存的是**内存中的业务状态**。

```
内存中的数据：
- orders Map（100 万订单）
- users Map（10 万用户）
- nextOrderId

快照保存：
- 将内存中的 Map 序列化到快照文件
- 快照文件保存在磁盘（由 AeronArchive 管理）

恢复时：
- 从快照文件读取数据
- 反序列化到内存中的 Map
```

### Q2: 如果快照期间有新消息到达，会怎么样？

**答**：快照和消息处理是**并发进行**的，不会互相阻塞。

```
时间线：
t0: 开始快照
    ├─ 序列化 orders（后台进行）
    │
t1: 接收新消息（继续处理！）
    ├─ orders.put(1000001, ...)  ← 新订单
    │
t2: 继续快照
    ├─ 序列化 users
    │
t3: 接收新消息（继续处理！）
    ├─ orders.put(1000002, ...)  ← 新订单
    │
t4: 快照完成

快照内容：
- 包含 1000000 个订单（t0 时的状态）
- 不包含订单 1000001 和 1000002（快照之后的）

日志内容：
- 包含订单 1000001 和 1000002 的创建消息

恢复时：
- 加载快照（恢复 1000000 个订单）
- 回放日志（恢复订单 1000001 和 1000002）
- 最终状态：1000002 个订单 ✓
```

**关键**：快照对应一个**精确的 logPosition**（如 5000000），快照之后的消息（5000001+）会在日志回放时恢复。

### Q3: 如果快照损坏了怎么办？

**答**：使用**上一个快照**恢复。

```
快照历史：
- 快照 1：logPosition = 3000000（2 小时前）
- 快照 2：logPosition = 4000000（1 小时前）
- 快照 3：logPosition = 5000000（刚才）← 损坏

恢复策略：
1. 尝试加载快照 3 → 失败（损坏）
2. 回退到快照 2（logPosition = 4000000）→ 成功
3. 回放日志（4000001 → 5010000）→ 恢复到最新状态

总耗时：
- 加载快照 2：1 分钟
- 回放 101 万条日志：10 分钟
- 总计：11 分钟（比从头回放 501 万条快 5 倍）
```

**最佳实践**：保留最近 3-5 个快照（由 ConsensusModule 自动管理）。

### Q4: 集合数据很大，如何优化快照性能？

**答**：使用以下优化技术。

#### 优化 1：增量写入（已演示）

```java
// 每 1MB 写入一次，避免缓冲区溢出
if (offset >= 900_000) {
    snapshotPublication.offer(buffer, 0, offset);
    offset = 0;
}
```

#### 优化 2：使用高效序列化（SBE）

```java
// ❌ 不推荐：使用 Java 序列化（慢，占用空间大）
ObjectOutputStream oos = new ObjectOutputStream(baos);
oos.writeObject(orders);

// ✅ 推荐：使用 SBE 或自定义二进制序列化（快，占用空间小）
buffer.putLong(offset, order.orderId);
buffer.putLong(offset + 8, order.userId);
buffer.putInt(offset + 16, order.amount);
buffer.putInt(offset + 20, order.status.ordinal());
```

#### 优化 3：压缩快照数据

```java
// 使用 LZ4 或 Snappy 压缩
byte[] uncompressed = ...;  // 原始数据
byte[] compressed = LZ4Compressor.compress(uncompressed);  // 压缩
snapshotPublication.offer(compressed, 0, compressed.length);

// 恢复时解压
byte[] compressed = ...;  // 从快照读取
byte[] uncompressed = LZ4Compressor.decompress(compressed);  // 解压
```

#### 优化 4：只保存必要状态

```java
// ❌ 不推荐：保存所有状态（包括可计算的）
@Override
public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
    // 保存订单总数（可以从 orders.size() 计算）
    buffer.putInt(0, totalOrders);  // ← 冗余
    // 保存订单 Map
    // ...
}

// ✅ 推荐：只保存必要状态（不保存可计算的）
@Override
public void onTakeSnapshot(ExclusivePublication snapshotPublication) {
    // 只保存订单 Map（totalOrders 可以从 Map 计算）
    // ...
}
```

### Q5: 如何验证快照恢复的正确性？

**答**：使用以下验证方法。

```java
@Override
public void onStart(Cluster cluster, Image snapshotImage) {
    if (snapshotImage != null) {
        // 1. 记录恢复前的状态
        System.out.println("恢复前：orders.size() = 0");

        // 2. 从快照恢复
        // ... (反序列化逻辑)

        // 3. 验证恢复后的状态
        System.out.println("恢复后：orders.size() = " + orders.size());

        // 4. 检查数据完整性
        for (Order order : orders.values()) {
            if (order.orderId <= 0) {
                throw new IllegalStateException("无效的订单 ID: " + order.orderId);
            }
            if (order.amount < 0) {
                throw new IllegalStateException("无效的订单金额: " + order.amount);
            }
        }

        System.out.println("快照恢复验证通过！");
    }
}
```

---

## 8. 总结

### 8.1 快照保存和恢复的完整流程

```
保存：
内存状态（Map） → 序列化（二进制） → 写入快照 publication → Archive 录制 → 磁盘文件

恢复：
磁盘文件 → Archive 回放 → 读取快照 Image → 反序列化（二进制） → 内存状态（Map）
```

### 8.2 关键要点

1. **快照保存什么**：内存中的业务状态（如 Map、List、计数器等）
2. **如何保存**：序列化到二进制，写入快照 publication（非阻塞）
3. **如何恢复**：从快照 Image 读取，反序列化到内存
4. **快照的作用**：加速启动（50 分钟 → 2 分钟），减少磁盘占用（10GB → 150MB）
5. **快照 + 日志**：快照恢复基础状态，日志回放增量变化，最终状态 = 快照 + 日志

### 8.3 最佳实践

1. ✅ 使用增量写入（每 1MB 写入一次）
2. ✅ 使用高效序列化（SBE 或自定义二进制）
3. ✅ 只保存必要状态（不保存可计算的）
4. ✅ 验证恢复的正确性（检查数据完整性）
5. ✅ 保留多个快照（3-5 个），防止快照损坏

---

**完整的快照保存和恢复示例已完成！希望这个详细的代码示例能帮助您理解快照机制。**

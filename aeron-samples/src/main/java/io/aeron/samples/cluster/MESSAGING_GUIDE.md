# Aeron 消息收发工具使用指南

## 概述

已拆分为两个独立的工具：

1. **MessageSender** - 消息发送者
   - 发送 Order 消息（Stream ID: 100）
   - 发送 User 消息（Stream ID: 200）
   - 使用简化的 SBE 风格编码

2. **MessageReceiver** - 消息接收者和回放工具
   - 实时接收多个 streamId 的消息
   - 列出 Archive 录制（支持按 streamId 过滤）
   - 回放历史消息
   - 交互模式

---

## 前提条件

**远程部署模式**（支持不同机器部署）：

1. **启动集群节点**（机器 A）
2. **配置 Archive 录制**（一次性配置）
3. **启动 MessageSender 和 MessageReceiver**（机器 B、C）

```bash
# 步骤 1: 启动集群节点 0（机器 A）
java ClusterNodeWithConfig 0

# 步骤 2: 配置 Archive 自动录制（任意机器，一次性配置）
java ArchiveRecordingSetup localhost 9001
# 或远程: java ArchiveRecordingSetup 192.168.1.100 9001

# 步骤 3: 现在可以启动 MessageSender 和 MessageReceiver
```

**架构**:
- MessageSender/Receiver 创建独立的 MediaDriver
- 通过 UDP 网络连接到集群节点
- Archive 自动录制所有匹配的消息

---

## 1. MessageSender（发送者）

**重要**: MessageSender 连接到集群的 MediaDriver，默认连接到 Node 0 的目录 `/dev/shm/aeron-cluster-node0`。

### 启动方式

```bash
# 连接到 Node 0（默认）
java MessageSender

# 指定 channel（推荐使用 aeron:ipc 连接集群内部）
java MessageSender "aeron:ipc"

# 连接到其他节点，通过 -Daeron.dir 指定
java -Daeron.dir=/dev/shm/aeron-cluster-node1 MessageSender
```

### 交互命令

#### 发送 Order 消息

```bash
order <orderId> <symbol> <price> <quantity> <side>
```

**参数说明：**
- `orderId`: 订单 ID（long）
- `symbol`: 股票代码（String）
- `price`: 价格（double）
- `quantity`: 数量（int）
- `side`: 方向（B=买入, S=卖出）

**示例：**
```bash
> order 1001 AAPL 150.50 100 B
[Sending Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
[Sent] Position: 0 (Stream ID: 100)

> order 1002 GOOGL 2800.75 50 S
[Sending Order] Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}
[Sent] Position: 128 (Stream ID: 100)
```

#### 发送 User 消息

```bash
user <userId> <username> <email> <age> <balance>
```

**参数说明：**
- `userId`: 用户 ID（long）
- `username`: 用户名（String）
- `email`: 邮箱（String）
- `age`: 年龄（int）
- `balance`: 余额（double）

**示例：**
```bash
> user 2001 alice alice@example.com 25 1000.50
[Sending User] User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
[Sent] Position: 0 (Stream ID: 200)

> user 2002 bob bob@example.com 30 2500.75
[Sending User] User{id=2002, username=bob, email=bob@example.com, age=30, balance=2500.75, active=true}
[Sent] Position: 96 (Stream ID: 200)
```

#### 其他命令

- `help` - 显示帮助
- `exit` - 退出

---

## 2. MessageReceiver（接收者和回放工具）

**重要**: MessageReceiver 也连接到集群的 MediaDriver，默认连接到 Node 0。

### 模式 1：实时接收消息

接收来自多个 streamId 的实时消息。

```bash
# 接收 Order 和 User 消息（Stream ID: 100, 200）
java MessageReceiver receive aeron:ipc 100,200

# 只接收 Order 消息
java MessageReceiver receive aeron:ipc 100

# 只接收 User 消息
java MessageReceiver receive aeron:ipc 200

# 连接到其他节点
java -Daeron.dir=/dev/shm/aeron-cluster-node1 MessageReceiver receive aeron:ipc 100,200
```

**输出示例：**
```
========================================
  接收实时消息
========================================
Channel:    aeron:ipc
Stream IDs: 100, 200

[Receiver] 订阅 Stream ID 100...
[Receiver] 订阅 Stream ID 200...
[Receiver] 等待连接...
[Receiver] 已连接，等待消息...
按 Ctrl+C 退出
========================================

[Order] Position: 0, Stream ID: 100
        Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}

[User]  Position: 0, Stream ID: 200
        User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}

[Order] Position: 128, Stream ID: 100
        Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}
```

### 模式 2：列出录制

查看 Archive 中的历史录制。

```bash
# 列出所有录制
java MessageReceiver list

# 只列出 Order 录制（Stream ID: 100）
java MessageReceiver list 100

# 只列出 User 录制（Stream ID: 200）
java MessageReceiver list 200
```

**输出示例：**
```
========================================
  Archive 录制列表
========================================
过滤 Stream ID: 100

Recording ID: 5 (Order)
  Channel:    aeron:ipc
  Stream ID:  100
  Position:   0 - 5120 (5120 bytes)
  Start:      Wed Apr 01 16:00:00 CST 2026
  Stop:       [正在录制中]

Recording ID: 6 (Order)
  Channel:    aeron:ipc
  Stream ID:  100
  Position:   0 - 2048 (2048 bytes)
  Start:      Wed Apr 01 16:05:00 CST 2026
  Stop:       Wed Apr 01 16:06:00 CST 2026

========================================
共找到 2 个录制
========================================
```

### 模式 3：回放录制

回放 Archive 中的历史消息。

```bash
# 回放录制 ID=5
java MessageReceiver replay 5
```

**输出示例：**
```
========================================
  回放录制 5
========================================
[Replay] 回放会话 ID: 101
[Replay] 等待连接...
[Replay] 开始接收...

[Order] Position: 0, Stream ID: 999
        Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}

[Order] Position: 128, Stream ID: 999
        Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}

[Order] Position: 256, Stream ID: 999
        Order{id=1003, symbol=TSLA, price=850.00, qty=200, side=B}

========================================
回放完成，共 3 条消息
========================================
```

### 模式 4：交互模式

在交互模式下可以随时列出录制和回放。

```bash
# 启动交互模式
java MessageReceiver interactive aeron:ipc
```

**交互命令：**
```
> help
可用命令:
  list                 - 列出所有录制
  list <streamId>      - 列出指定 streamId 的录制
  replay <recordingId> - 回放指定录制
  help                 - 显示此帮助
  exit                 - 退出

> list 100
========================================
  Archive 录制列表
========================================
过滤 Stream ID: 100
...

> replay 5
========================================
  回放录制 5
========================================
...

> exit

[Receiver] 已退出
```

---

## 3. 完整使用流程

### 场景 1：发送和实时接收

**步骤 0 - 启动集群（终端 1）：**
```bash
java ClusterNodeWithConfig 0
# 等待看到 "所有组件已启动完成"
```

**步骤 1 - 启动接收者（终端 2）：**
```bash
java MessageReceiver receive aeron:ipc 100,200
```

**步骤 2 - 启动发送者（终端 3）：**
```bash
java MessageSender

> order 1001 AAPL 150.50 100 B
> order 1002 GOOGL 2800.75 50 S
> user 2001 alice alice@example.com 25 1000.50
> user 2002 bob bob@example.com 30 2500.75
> exit
```

**终端 1 会实时显示：**
```
[Order] Position: 0, Stream ID: 100
        Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}

[Order] Position: 128, Stream ID: 100
        Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}

[User]  Position: 0, Stream ID: 200
        User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}

[User]  Position: 96, Stream ID: 200
        User{id=2002, username=bob, email=bob@example.com, age=30, balance=2500.75, active=true}
```

### 场景 2：查看历史录制

```bash
# 查看所有 Order 录制
java MessageReceiver list 100

# 查看所有 User 录制
java MessageReceiver list 200
```

### 场景 3：回放历史消息

```bash
# 先查看录制 ID
java MessageReceiver list 100

# 输出：
# Recording ID: 5 (Order)
#   ...

# 回放录制 5
java MessageReceiver replay 5
```

---

## 4. StreamId 分配

| Stream ID | 消息类型 | 说明 |
|-----------|---------|------|
| 100 | Order | 订单消息 |
| 200 | User | 用户消息 |
| 999 | Replay | 回放专用（内部使用） |

---

## 5. Archive 连接配置

MessageReceiver 通过 UDP 连接到 Archive 进行回放操作（默认 Node 0: `localhost:9001`）：

```java
private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9001";
```

**连接其他节点的 Archive：**
- Node 0: `localhost:9001`
- Node 1: `localhost:9101`
- Node 2: `localhost:9201`

**MediaDriver 连接配置：**

MessageSender 和 MessageReceiver 通过共享内存（IPC）连接到集群的 MediaDriver：

```java
// 默认连接到 Node 0
final String aeronDir = "/dev/shm/aeron-cluster-node0";

// 通过系统属性指定
java -Daeron.dir=/dev/shm/aeron-cluster-node1 MessageSender
```

---

## 6. 编码格式

使用简化的 SBE 风格二进制编码：

### Order 消息格式

```
+-------+----------+---------+----------+------+-----------+--------+--------+
| Type  | OrderId  | Price   | Quantity | Side | Timestamp | SymLen | Symbol |
| (1B)  | (8B)     | (8B)    | (4B)     | (1B) | (8B)      | (4B)   | (var)  |
+-------+----------+---------+----------+------+-----------+--------+--------+
```

- Type: 消息类型 (1 = Order)
- OrderId: 订单 ID
- Price: 价格 × 100（避免浮点精度问题）
- Quantity: 数量
- Side: 方向 ('B' 或 'S')
- Timestamp: 时间戳
- SymLen: Symbol 长度
- Symbol: 股票代码（UTF-8）

### User 消息格式

```
+-------+--------+-----+---------+--------+----------+----------+--------+--------+
| Type  | UserId | Age | Balance | Active | UsernLen | Username | EmailLen | Email |
| (1B)  | (8B)   | (4B)| (8B)    | (1B)   | (4B)     | (var)    | (4B)     | (var) |
+-------+--------+-----+---------+--------+----------+----------+--------+--------+
```

- Type: 消息类型 (2 = User)
- UserId: 用户 ID
- Age: 年龄
- Balance: 余额 × 100
- Active: 活跃标志 (1=true, 0=false)
- UsernLen: Username 长度
- Username: 用户名（UTF-8）
- EmailLen: Email 长度
- Email: 邮箱（UTF-8）

---

## 7. 故障排查

### 问题 1：接收者或发送者连接超时

**原因**：集群节点未启动，或 MediaDriver 目录不匹配。

**解决**：
1. **首先确保集群节点已启动**: `java ClusterNodeWithConfig 0`
2. 检查 MediaDriver 目录是否正确（默认 `/dev/shm/aeron-cluster-node0`）
3. 如果连接其他节点，使用 `-Daeron.dir=/dev/shm/aeron-cluster-node<N>`
4. 使用 `ls /dev/shm/aeron-cluster-node*` 检查目录是否存在

### 问题 2：Archive 连接失败

**原因**：Archive 未运行，或端口配置错误。

**解决**：
1. 确保集群节点运行中
2. 检查 Archive 端口配置（9001 for Node 0）
3. 如果是本地连接，改为 `aeron:ipc`

### 问题 3：消息解码失败

**原因**：编码格式不匹配。

**解决**：
1. 确保发送者和接收者使用相同版本的代码
2. 检查消息类型字节（第一个字节）

---

## 8. 进阶用法

### 自定义消息类型

在 `MessageSender` 中添加新的消息类型：

```java
private static final byte MSG_TYPE_TRADE = 3;

// 添加编码方法
private int encodeTrade(final MutableDirectBuffer buffer, int offset, final Trade trade) {
    // ...
}

// 添加命令处理
else if (command.equals("trade")) {
    handleTradeCommand(parts);
}
```

在 `MessageReceiver` 中添加对应的解码：

```java
else if (msgType == MSG_TYPE_TRADE) {
    final Trade trade = decodeTrade(buffer, offset);
    // ...
}
```

### 使用真正的 SBE

如果需要高性能的序列化，可以使用真正的 SBE：

1. 定义 schema XML（已创建 `messaging-schema.xml`）
2. 使用 SBE 工具生成代码
3. 替换手动编解码为 SBE 生成的类

---

这个工具提供了完整的消息收发和回放能力，支持不同的消息类型和 streamId！

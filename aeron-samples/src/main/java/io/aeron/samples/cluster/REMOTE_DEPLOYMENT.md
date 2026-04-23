# 远程部署架构说明

## 架构概览（远程部署）

```
机器 A: 集群节点
┌───────────────────────────────────────────────────────┐
│            ClusterNodeWithConfig                      │
│                                                       │
│  ┌──────────────────────────────────────────────┐   │
│  │            MediaDriver                        │   │
│  │  - 监听 UDP 9010（用于外部消息）              │   │
│  └──────────────────────────────────────────────┘   │
│                                                       │
│  ┌──────────────────────────────────────────────┐   │
│  │            Archive                            │   │
│  │  - Control: UDP 9001                         │   │
│  │  - 录制所有 streamId                         │   │
│  └──────────────────────────────────────────────┘   │
│                                                       │
│  ┌──────────────────────────────────────────────┐   │
│  │    ConsensusModule + ClusteredService        │   │
│  │  - Ingress: UDP 9002                         │   │
│  └──────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────┘
                      ▲                  ▲
                      │ UDP 9010         │ UDP 9001
                      │ (消息)           │ (Archive)
                      │                  │
         ┌────────────┴──────┐    ┌─────┴────────┐
         │                   │    │              │
  机器 B: 消息发送者    机器 C: 消息接收者/回放
┌──────────────────┐  ┌───────────────────────────┐
│ MessageSender    │  │ MessageReceiver           │
│                  │  │                           │
│ 独立 MediaDriver │  │ 独立 MediaDriver          │
│                  │  │                           │
│ UDP Publication  │  │ - UDP Subscription        │
│ → 192.168.1.100: │  │   ← 192.168.1.100:9010   │
│   9010           │  │                           │
│ streamId 100,200 │  │ - Archive Client          │
│                  │  │   → 192.168.1.100:9001   │
└──────────────────┘  └───────────────────────────┘
```

## 问题：集群如何接收外部 streamId 的消息？

### 方案 1：通过 Aeron Archive 自动录制（推荐）

**原理**:
- MessageSender 通过 UDP 发送消息到集群节点的 MediaDriver
- 配置 Archive 自动录制匹配的 channel 和 streamId
- MessageReceiver 从 Archive 回放历史消息
- **不需要** ClusteredService 处理这些消息

**优点**:
- 简单，不需要修改 ClusteredService
- Archive 自动录制所有消息
- 支持任意数量的 streamId

**配置**:

```java
// ClusterNodeWithConfig.java
// 配置 Archive 自动录制所有匹配的流
clusterConfig.archiveContext()
    .controlChannel("aeron:udp?endpoint=localhost:9001")
    // 自动录制模式：录制所有传入的流
    .recordingEventsEnabled(true);
```

**限制**:
- 需要 MessageSender 和集群节点能够网络互通
- UDP 端口 9010 需要开放
- Archive 会录制所有匹配的流（包括 streamId 100, 200）

### 方案 2：通过桥接服务转发到集群（复杂）

**原理**:
- 创建一个桥接服务，订阅 streamId 100, 200
- 将消息转发到 AeronCluster 客户端
- ClusteredService 通过 `onSessionMessage` 接收
- Archive 录制集群内部的消息

**优点**:
- ClusteredService 可以处理消息
- 消息经过集群共识
- 可以回复处理结果

**缺点**:
- 需要额外的桥接服务
- 增加延迟（两跳）
- 架构更复杂

## 推荐架构（方案 1）

### 配置步骤

#### 1. ClusterNodeWithConfig - 集群节点配置

不需要修改，Archive 会自动录制所有传入的流。

#### 2. MessageSender - 消息发送者

```bash
# 发送到远程集群（机器 A: 192.168.1.100）
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010"

# 交互命令
> order 1001 AAPL 150.50 100 B
> user 2001 alice alice@example.com 25 1000.50
```

#### 3. MessageReceiver - 消息接收者

```bash
# 方式 1: 实时订阅（直接从 MediaDriver）
# 注意：需要集群配置允许外部订阅
java MessageReceiver receive "aeron:udp?endpoint=192.168.1.100:9010" 100,200

# 方式 2: 从 Archive 回放历史（推荐）
# 列出录制
java MessageReceiver list 100

# 回放录制
java MessageReceiver replay <recordingId>
```

### 网络配置要求

#### 集群节点（机器 A: 192.168.1.100）

需要开放以下 UDP 端口：

| 端口 | 用途 | 说明 |
|------|------|------|
| 9001 | Archive Control | MessageReceiver 连接 Archive |
| 9002 | Cluster Ingress | AeronCluster 客户端连接（InteractiveClusterClient） |
| 9010 | 外部消息 | MessageSender 发送消息（自定义端口） |

#### MessageSender（机器 B）

- 需要能访问集群节点的 UDP 9010
- 出站 UDP 端口（随机）

#### MessageReceiver（机器 C）

- 需要能访问集群节点的 UDP 9001（Archive）
- 如果要实时订阅，需要访问 UDP 9010
- 出站 UDP 端口（随机）

### 防火墙规则示例

```bash
# 集群节点（机器 A）
# 允许来自任意 IP 的 UDP 9001, 9002, 9010
sudo ufw allow 9001/udp
sudo ufw allow 9002/udp
sudo ufw allow 9010/udp

# 或者限制特定 IP
sudo ufw allow from 192.168.1.101 to any port 9010 proto udp  # MessageSender
sudo ufw allow from 192.168.1.102 to any port 9001 proto udp  # MessageReceiver
```

## 完整部署示例

### 步骤 1: 启动集群节点（机器 A: 192.168.1.100）

```bash
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0
```

**检查**:
```bash
# 检查端口监听
netstat -ulnp | grep java
# 应该看到 9001, 9002, 9003, 9004, 9005
```

### 步骤 2: 启动消息接收者（机器 C: 192.168.1.102）

```bash
# 方式 1: 列出 Archive 录制
java MessageReceiver list

# 方式 2: 交互模式
java MessageReceiver interactive
> list 100
> replay 0
```

### 步骤 3: 启动消息发送者（机器 B: 192.168.1.101）

```bash
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010"

# 发送消息
> order 1001 AAPL 150.50 100 B
> user 2001 alice alice@example.com 25 1000.50
```

### 步骤 4: 验证消息录制

在机器 C 上：

```bash
java MessageReceiver list 100
# 应该看到 streamId 100 的录制

java MessageReceiver replay <recordingId>
# 应该看到之前发送的 Order 消息
```

## 注意事项

### 1. Archive 自动录制配置

**重要**: 默认情况下，Archive 可能不会自动录制所有流。需要检查 Archive 的配置：

```java
// 在 ClusterNodeWithConfig 中添加：
clusterConfig.archiveContext()
    // 启用录制事件
    .recordingEventsEnabled(true)
    // 可选：指定录制目录
    .archiveDir(new File("/path/to/archive"));
```

### 2. UDP 端口 9010 是否存在？

**问题**: ClusterNodeWithConfig 默认配置可能不会监听 UDP 9010。

**解决方案**:

**方案 A**: 修改 SimpleClusteredService，添加订阅：

```java
// SimpleClusteredService.java
private Subscription orderSubscription;
private Subscription userSubscription;

@Override
public void onStart(final Cluster cluster, final Image snapshotImage) {
    final Aeron aeron = cluster.aeron();

    // 订阅外部消息
    orderSubscription = aeron.addSubscription(
        "aeron:udp?endpoint=localhost:9010", 100);
    userSubscription = aeron.addSubscription(
        "aeron:udp?endpoint=localhost:9010", 200);

    // 在 onTakeSnapshot 和其他地方处理这些消息
}
```

**方案 B**: 使用 Archive 的自动订阅功能（推荐）

修改 ClusterNodeWithConfig:

```java
// 配置 Archive 自动录制指定 channel 和 streamId
clusterConfig.archiveContext()
    .recordingEventsEnabled(true);

// 在启动后添加录制订阅
final AeronArchive archive = AeronArchive.connect(...);
archive.startRecording(
    "aeron:udp?endpoint=localhost:9010",  // channel
    100,                                   // streamId
    SourceLocation.REMOTE                  // 接收远程消息
);
archive.startRecording(
    "aeron:udp?endpoint=localhost:9010",
    200,
    SourceLocation.REMOTE
);
```

### 3. 多播 vs 单播

**单播** (默认):
- MessageSender 发送到特定 IP:PORT
- 只有订阅该 endpoint 的接收者能收到
- 需要接收者先订阅

**多播**:
- MessageSender 发送到多播地址（如 224.0.1.1:9010）
- 所有加入该多播组的接收者都能收到
- 不需要等待订阅者

```bash
# 多播示例
java MessageSender "aeron:udp?endpoint=224.0.1.1:9010|interface=192.168.1.100"
java MessageReceiver receive "aeron:udp?endpoint=224.0.1.1:9010|interface=192.168.1.102" 100,200
```

## 故障排查

### 问题 1: MessageSender 发送成功但 Archive 没有录制

**检查**:
1. Archive 是否启用录制？
2. 集群节点是否订阅了 streamId 100, 200？
3. UDP 端口 9010 是否开放？

**解决**:
- 添加录制订阅（见上面方案 B）
- 检查防火墙规则
- 使用 `netstat -ulnp | grep 9010` 检查端口

### 问题 2: MessageReceiver 无法连接到 Archive

**检查**:
```bash
# 测试 Archive 端口
telnet 192.168.1.100 9001
```

**解决**:
- 确保集群节点已启动
- 检查防火墙规则
- 修改 MessageReceiver.java 中的 ARCHIVE_CONTROL_CHANNEL

### 问题 3: UDP 消息丢失

**原因**: UDP 是不可靠协议，网络拥塞会导致丢包

**解决**:
- 增加 MTU: `aeron:udp?endpoint=...| mtu=8192`
- 使用可靠 UDP: `aeron:udp?endpoint=...|reliable=true`
- 检查网络质量

## 总结

远程部署架构的关键点：

1. **MessageSender 和 MessageReceiver 都启动独立的 MediaDriver**
2. **通过 UDP 网络连接到集群节点**
3. **Archive 负责录制消息**（需要配置自动录制）
4. **集群节点需要订阅 streamId 100, 200** 才能触发 Archive 录制
5. **网络端口需要正确配置和开放**

推荐的实现步骤：
1. 先实现方案 A（SimpleClusteredService 订阅外部消息）
2. 配置 Archive 自动录制
3. 测试 MessageSender → 集群 → Archive
4. 测试 MessageReceiver 回放

下一步：修改 SimpleClusteredService 添加外部消息订阅支持。

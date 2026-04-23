# Aeron 集群消息架构

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                    ClusterNodeWithConfig                     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │              MediaDriver                            │    │
│  │  aeron dir: /dev/shm/aeron-cluster-node0           │    │
│  │                                                     │    │
│  │  ┌──────────────┐  ┌──────────────┐               │    │
│  │  │ Publication  │  │ Subscription │               │    │
│  │  │ streamId 100 │  │ streamId 100 │               │    │
│  │  │ streamId 200 │  │ streamId 200 │               │    │
│  │  └──────────────┘  └──────────────┘               │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │                  Archive                            │    │
│  │  Control: UDP port 9001                            │    │
│  │  Records: streamId 100, 200                        │    │
│  └────────────────────────────────────────────────────┘    │
│                                                              │
│  ┌────────────────────────────────────────────────────┐    │
│  │          ConsensusModule + Service                  │    │
│  └────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                         ▲                ▲
                         │ IPC            │ UDP:9001
                         │ (shared mem)   │ (Archive)
                         │                │
        ┌────────────────┴───┐    ┌──────┴─────────┐
        │                    │    │                 │
   ┌────┴──────┐      ┌──────┴────┐         ┌──────┴────────┐
   │MessageSender│      │MessageReceiver│         │MessageReceiver│
   │             │      │  (real-time)  │         │   (replay)   │
   │Publishes to │      │Subscribes to  │         │Connects to   │
   │streamId 100,│      │streamId 100,  │         │Archive:9001  │
   │200          │      │200            │         │              │
   └─────────────┘      └───────────────┘         └──────────────┘
```

## 连接方式

### 1. ClusterNodeWithConfig（集群节点）

- **作用**: 启动完整的 Aeron Cluster 节点
- **组件**:
  - MediaDriver: 消息传输层
  - Archive: 消息持久化（录制和回放）
  - ConsensusModule: Raft 共识模块
  - ClusteredServiceContainer: 业务服务容器
- **MediaDriver 目录**: `/dev/shm/aeron-cluster-node<N>` （N = 节点 ID）
- **Archive 端口**: `9000 + nodeId × 100 + 1`

### 2. MessageSender（消息发送者）

- **连接方式**: 通过 **IPC（共享内存）** 连接到集群的 MediaDriver
- **默认目录**: `/dev/shm/aeron-cluster-node0`
- **功能**:
  - 创建 Publication 到 streamId 100（Order 消息）
  - 创建 Publication 到 streamId 200（User 消息）
  - 发送编码后的二进制消息

### 3. MessageReceiver（消息接收者）

- **连接方式 1（实时接收）**: 通过 **IPC** 连接到集群的 MediaDriver
  - 创建 Subscription 订阅 streamId 100 和 200
  - 接收并解码实时消息

- **连接方式 2（回放）**: 通过 **UDP** 连接到 Archive
  - 控制通道: `aeron:udp?endpoint=localhost:9001`
  - 列出历史录制
  - 回放历史消息

## 启动顺序

**必须按照以下顺序启动**：

```bash
# 步骤 1: 启动集群节点（终端 1）
java ClusterNodeWithConfig 0
# 等待看到 "所有组件已启动完成"

# 步骤 2: 启动接收者（终端 2，可选）
java MessageReceiver receive aeron:ipc 100,200

# 步骤 3: 启动发送者（终端 3）
java MessageSender

# 步骤 4: 发送消息
> order 1001 AAPL 150.50 100 B
> user 2001 alice alice@example.com 25 1000.50
```

## 关键配置

### ClusterNodeWithConfig

```java
// MediaDriver 目录（可预测路径）
final String aeronDir = "/dev/shm/aeron-cluster-node" + nodeId;
clusterConfig.mediaDriverContext()
    .aeronDirectoryName(aeronDir)
    .dirDeleteOnStart(true);
```

### MessageSender

```java
// 连接到集群的 MediaDriver（不创建自己的）
final String aeronDir = System.getProperty("aeron.dir", "/dev/shm/aeron-cluster-node0");
mediaDriver = null;  // 不启动自己的 MediaDriver
aeron = Aeron.connect(new Aeron.Context()
    .aeronDirectoryName(aeronDir));

// 创建 Publication
orderPublication = aeron.addPublication("aeron:ipc", ORDER_STREAM_ID);
userPublication = aeron.addPublication("aeron:ipc", USER_STREAM_ID);
```

### MessageReceiver

```java
// 1. 连接到集群的 MediaDriver（实时接收）
final String aeronDir = System.getProperty("aeron.dir", "/dev/shm/aeron-cluster-node0");
aeron = Aeron.connect(new Aeron.Context()
    .aeronDirectoryName(aeronDir));

// 2. 连接到 Archive（回放）
aeronArchive = AeronArchive.connect(new AeronArchive.Context()
    .aeron(aeron)
    .controlRequestChannel("aeron:udp?endpoint=localhost:9001")
    .controlResponseChannel("aeron:udp?endpoint=localhost:0"));
```

## 为什么这样设计？

### 1. IPC 连接 MediaDriver
- **优点**:
  - 零拷贝（zero-copy）通信
  - 低延迟（纳秒级）
  - 本地进程间共享内存
- **要求**:
  - 必须在同一台机器上
  - 需要访问 `/dev/shm/aeron-cluster-node<N>` 目录

### 2. UDP 连接 Archive
- **优点**:
  - 可以远程连接
  - Archive 提供专门的控制通道
- **用途**:
  - 列出历史录制
  - 回放历史消息

### 3. 分离的组件
- **MessageSender**: 专注于发送不同类型的消息
- **MessageReceiver**: 专注于接收和回放
- **集群节点**: 负责共识、持久化和服务处理

## 切换节点

如果要连接到其他集群节点（如 Node 1）：

```bash
# MessageSender 连接到 Node 1
java -Daeron.dir=/dev/shm/aeron-cluster-node1 MessageSender

# MessageReceiver 连接到 Node 1
java -Daeron.dir=/dev/shm/aeron-cluster-node1 MessageReceiver receive aeron:ipc 100,200

# MessageReceiver 回放 Node 1 的 Archive
# 修改 ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9101"
```

## 故障排查

### 错误: "无法连接到集群 MediaDriver"

**原因**: 集群节点未启动或 MediaDriver 目录不存在

**检查**:
```bash
# 检查 MediaDriver 目录是否存在
ls -la /dev/shm/aeron-cluster-node*

# 输出示例（集群运行中）:
# /dev/shm/aeron-cluster-node0/

# 检查集群进程
ps aux | grep ClusterNodeWithConfig
```

**解决**:
1. 先启动集群节点: `java ClusterNodeWithConfig 0`
2. 等待看到 "所有组件已启动完成"
3. 再启动 MessageSender 或 MessageReceiver

### 错误: "连接超时"

**原因**: Publication/Subscription 没有匹配的对端

**解决**:
1. 确保 channel 一致（都使用 `aeron:ipc`）
2. 确保 streamId 匹配
3. 检查是否在同一个 MediaDriver 实例中（同一个 aeron 目录）

### 错误: "Archive 连接失败"

**原因**: Archive 未运行或端口不对

**解决**:
1. 确保集群节点已启动（Archive 是集群的一部分）
2. 检查端口: Node 0 使用 9001, Node 1 使用 9101, Node 2 使用 9201
3. 使用 `netstat -tlnp | grep 900` 检查端口是否监听

## 总结

- **集群节点**: 提供 MediaDriver（IPC）和 Archive（UDP）
- **MessageSender**: 通过 IPC 发布消息到 streamId 100, 200
- **MessageReceiver**: 通过 IPC 订阅实时消息，通过 UDP 连接 Archive 回放历史
- **关键**: 必须先启动集群，再启动 Sender/Receiver

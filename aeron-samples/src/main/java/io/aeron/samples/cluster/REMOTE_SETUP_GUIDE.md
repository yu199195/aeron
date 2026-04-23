# 远程部署完整指南

## 快速开始（3 步部署）

### 环境假设

- **机器 A** (192.168.1.100): 集群节点
- **机器 B** (192.168.1.101): 消息发送者
- **机器 C** (192.168.1.102): 消息接收者

---

## 步骤 1: 启动集群节点（机器 A）

```bash
# 在机器 A 上
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0

# 等待看到:
# [Node 0] ========== 所有组件已启动完成 ==========
```

**验证**:
```bash
# 检查端口监听
netstat -ulnp | grep java

# 应该看到:
# 9001 - Archive control
# 9002 - Cluster ingress
# 9003 - Consensus
# 9004 - Log
# 9005 - Catchup
```

---

## 步骤 2: 配置 Archive 录制（任意机器）

```bash
# 从任意可以访问机器 A 的机器运行（只需运行一次）
java -cp aeron-all.jar io.aeron.samples.cluster.ArchiveRecordingSetup 192.168.1.100 9001

# 输出:
# [Setup] 连接到 Archive...
# [Setup] 已连接到 Archive
# [Setup] 配置 Order 消息录制 (Stream ID: 100)...
# [Setup] Order 录制订阅 ID: 0
# [Setup] 配置 User 消息录制 (Stream ID: 200)...
# [Setup] User 录制订阅 ID: 1
# 配置完成！
```

**这一步的作用**:
- 告诉 Archive 自动录制 UDP 9010 端口上的 streamId 100 和 200
- 配置后，Archive 会自动创建录制
- 重启集群后需要重新配置

---

## 步骤 3A: 启动消息发送者（机器 B）

```bash
# 在机器 B 上
java -cp aeron-all.jar io.aeron.samples.cluster.MessageSender \
  "aeron:udp?endpoint=192.168.1.100:9010"

# 启动后会显示:
# ========================================
#   消息发送者（远程模式）
# ========================================
# Channel: aeron:udp?endpoint=192.168.1.100:9010
# Order Stream ID: 100
# User Stream ID:  200
#
# 注意: 需要集群订阅这些 streamId 才能接收消息
#
# [Sender] 启动独立 MediaDriver...
# [Sender] 连接 Aeron...
# [Sender] 创建 Publications...
# [Sender] 准备就绪
#
# 可用命令:
#   order <orderId> <symbol> <price> <qty> <side>
#     - 发送 Order 消息到 Stream ID 100
#     - side: B=买入, S=卖出
#     - 示例: order 1001 AAPL 150.50 100 B
#
#   user <userId> <username> <email> <age> <balance>
#     - 发送 User 消息到 Stream ID 200
#     - 示例: user 2001 alice alice@example.com 25 1000.50
#
#   help  - 显示此帮助
#   exit  - 退出程序
#
# 开始输入命令:
# ========================================
#
# >
```

### 发送消息示例

```bash
> order 1001 AAPL 150.50 100 B
[Sending Order] Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
[Sent] Position: 0 (Stream ID: 100)

> order 1002 GOOGL 2800.75 50 S
[Sending Order] Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}
[Sent] Position: 128 (Stream ID: 100)

> user 2001 alice alice@example.com 25 1000.50
[Sending User] User{id=2001, username=alice, email=alice@example.com, age=25, balance=1000.50, active=true}
[Sent] Position: 0 (Stream ID: 200)

> exit
```

---

## 步骤 3B: 启动消息接收者（机器 C）

### 方式 1: 列出录制

```bash
# 在机器 C 上
java -cp aeron-all.jar io.aeron.samples.cluster.MessageReceiver list

# 输出:
# ========================================
#   Archive 录制列表
# ========================================
#
# Recording ID: 0 (Unknown)
#   Channel:    aeron:udp?endpoint=192.168.1.100:9010
#   Stream ID:  100
#   Position:   0 - 256 (256 bytes)
#   Start:      Thu Apr 02 10:00:00 CST 2026
#   Stop:       [正在录制中]
#
# Recording ID: 1 (Unknown)
#   Channel:    aeron:udp?endpoint=192.168.1.100:9010
#   Stream ID:  200
#   Position:   0 - 96 (96 bytes)
#   Start:      Thu Apr 02 10:00:05 CST 2026
#   Stop:       [正在录制中]
#
# ========================================
# 共找到 2 个录制
# ========================================
```

### 方式 2: 回放录制

```bash
# 回放 Order 消息（Recording ID: 0）
java -cp aeron-all.jar io.aeron.samples.cluster.MessageReceiver replay 0

# 输出:
# ========================================
#   回放录制 0
# ========================================
# [Replay] 回放会话 ID: 100
# [Replay] 等待连接...
# [Replay] 开始接收...
#
# [Order] Position: 0, Stream ID: 999
#         Order{id=1001, symbol=AAPL, price=150.50, qty=100, side=B}
#
# [Order] Position: 128, Stream ID: 999
#         Order{id=1002, symbol=GOOGL, price=2800.75, qty=50, side=S}
#
# ========================================
# 回放完成，共 2 条消息
# ========================================
```

### 方式 3: 交互模式

```bash
java -cp aeron-all.jar io.aeron.samples.cluster.MessageReceiver interactive

# 交互命令:
> list 100
> replay 0
> list 200
> replay 1
> exit
```

---

## 网络配置检查清单

### 机器 A（集群节点）需要开放的端口

| 端口 | 协议 | 用途 | 访问来源 |
|------|------|------|----------|
| 9001 | UDP | Archive control | MessageReceiver（机器 C） |
| 9002 | UDP | Cluster ingress | AeronCluster 客户端 |
| 9010 | UDP | 外部消息 | MessageSender（机器 B） |

### 防火墙配置（机器 A）

```bash
# Ubuntu/Debian
sudo ufw allow 9001/udp comment 'Archive control'
sudo ufw allow 9002/udp comment 'Cluster ingress'
sudo ufw allow 9010/udp comment 'External messages'

# CentOS/RHEL
sudo firewall-cmd --permanent --add-port=9001/udp
sudo firewall-cmd --permanent --add-port=9002/udp
sudo firewall-cmd --permanent --add-port=9010/udp
sudo firewall-cmd --reload

# 或者限制特定 IP
sudo ufw allow from 192.168.1.101 to any port 9010 proto udp
sudo ufw allow from 192.168.1.102 to any port 9001 proto udp
```

### 测试连接

```bash
# 从机器 B 测试到机器 A 的 9010 端口
# （UDP 不能用 telnet，可以用 nc）
echo "test" | nc -u 192.168.1.100 9010

# 检查机器 A 的端口监听
netstat -ulnp | grep 9010
```

---

## 故障排查

### 问题 1: Archive 录制配置失败

**错误**: `Connection refused` 或 `Timeout`

**检查**:
```bash
# 1. 集群是否启动？
ps aux | grep ClusterNodeWithConfig

# 2. Archive 端口是否监听？
netstat -ulnp | grep 9001

# 3. 防火墙是否开放？
sudo ufw status | grep 9001
```

**解决**:
- 确保集群已启动并看到 "所有组件已启动完成"
- 等待 10-20 秒让 Archive 完全启动
- 检查防火墙规则

### 问题 2: MessageSender 发送成功但 Archive 没有录制

**检查**:
```bash
# 1. 是否运行了 ArchiveRecordingSetup？
java MessageReceiver list
# 如果看到 "共找到 0 个录制"，说明没有配置

# 2. MessageSender 的 channel 是否匹配？
# MessageSender: aeron:udp?endpoint=192.168.1.100:9010
# ArchiveRecordingSetup: RECORDING_CHANNEL = "aeron:udp?endpoint=localhost:9010"
```

**解决**:
- 运行 `ArchiveRecordingSetup` 配置录制
- 确保 MessageSender 的 endpoint 和 ArchiveRecordingSetup 中的 RECORDING_CHANNEL 匹配
- 如果修改了 MessageSender 的 endpoint，需要修改 ArchiveRecordingSetup.java 中的 `RECORDING_CHANNEL` 常量

### 问题 3: MessageReceiver 列出录制为空

**检查**:
```bash
# 1. Archive 地址是否正确？
# MessageReceiver 中: ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9001"

# 2. 是否在正确的机器上运行？
# 如果在远程机器 C，需要修改为:
# ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=192.168.1.100:9001"
```

**解决**:
- 修改 `MessageReceiver.java`:

```java
private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=192.168.1.100:9001";
```

- 重新编译
- 或者创建配置文件/参数传递

### 问题 4: UDP 消息丢失

**现象**: 发送了 10 条消息，但回放只有 8 条

**原因**: UDP 是不可靠协议，网络拥塞或丢包导致

**解决**:
1. 增加 MTU 大小:
```bash
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010|mtu=8192"
```

2. 启用可靠 UDP（Aeron 的可靠传输）:
```bash
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010|reliable=true"
```

3. 检查网络质量:
```bash
# 测试丢包率
ping -c 100 192.168.1.100

# 测试带宽
iperf3 -c 192.168.1.100
```

---

## 性能调优

### 1. 减少延迟

```bash
# MessageSender 增加发送缓冲区
java -Daeron.socket.so_sndbuf=2097152 MessageSender ...

# MessageReceiver 增加接收缓冲区
java -Daeron.socket.so_rcvbuf=2097152 MessageReceiver ...
```

### 2. 增加吞吐量

```bash
# 使用更大的 MTU
java MessageSender "aeron:udp?endpoint=...|mtu=8192"

# 使用多线程 ThreadingMode
# 修改 MediaDriver.Context().threadingMode(ThreadingMode.DEDICATED)
```

### 3. 监控

```bash
# 启用 Aeron 统计
java -Daeron.dir=/dev/shm/aeron-sender \
     -Daeron.counters.free.to.reuse=false \
     MessageSender ...

# 查看统计
aeron-stat
```

---

## 生产部署建议

### 1. 高可用集群

部署 3 个节点形成 Raft 集群：

```bash
# 机器 A1: Node 0
java ClusterNodeWithConfig 0

# 机器 A2: Node 1
java ClusterNodeWithConfig 1

# 机器 A3: Node 2
java ClusterNodeWithConfig 2
```

### 2. 负载均衡

MessageSender 可以连接到任意节点：

```bash
# 连接到 Node 0
java MessageSender "aeron:udp?endpoint=192.168.1.100:9010"

# 连接到 Node 1
java MessageSender "aeron:udp?endpoint=192.168.1.101:9110"
```

### 3. 灾备

定期备份 Archive 存储目录：

```bash
# 集群节点上
rsync -av /path/to/aeron-cluster-node0-archive/ backup-server:/backup/
```

### 4. 监控告警

监控关键指标：
- Archive 录制状态
- 消息丢失率
- 录制延迟
- 磁盘空间

---

## 总结

**远程部署的关键点**:

1. ✅ MessageSender/Receiver 创建独立的 MediaDriver
2. ✅ 通过 UDP 连接到集群节点（不是 IPC）
3. ✅ 使用 ArchiveRecordingSetup 配置自动录制
4. ✅ Archive 自动录制匹配的 streamId
5. ✅ MessageReceiver 从 Archive 回放历史
6. ✅ 防火墙开放必需的 UDP 端口
7. ✅ Channel 配置必须匹配（endpoint 地址）

**下一步**:
- 测试基本功能
- 配置监控和告警
- 性能测试和调优
- 灾备方案

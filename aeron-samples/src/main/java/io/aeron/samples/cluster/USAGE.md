# Aeron Cluster 使用指南

本指南说明如何使用交互式客户端和回放工具。

## 1. 交互式集群客户端 (InteractiveClusterClient)

### 功能
- 从控制台输入消息并发送到集群
- 实时接收集群响应
- 支持命令和会话管理

### 启动方式

```bash
# IDEA 中运行
右键点击 InteractiveClusterClient.java → Run 'InteractiveClusterClient.main()'

# 命令行运行
java -cp aeron-all.jar io.aeron.samples.cluster.InteractiveClusterClient
```

### 使用示例

```
========================================
  Aeron Cluster 交互式客户端
========================================
Ingress 端点: 0=localhost:9002,1=localhost:9102,2=localhost:9202

[Client] 启动 MediaDriver...
[Client] 连接到集群...
[Client] 已连接到集群，会话 ID: 12345

========================================
  交互式模式 (输入命令或消息)
========================================
命令:
  exit / quit  - 退出客户端
  help         - 显示帮助
  session      - 显示会话信息
  其他输入     - 作为消息发送到集群

开始输入消息（按 Ctrl+D 或输入 'exit' 退出）:
========================================

> Hello Cluster!
[Client] 发送消息: Hello Cluster!
[Client] 收到响应: [Echo #1] Hello Cluster! (全局: 1, 本会话: 1)

> 测试中文消息
[Client] 发送消息: 测试中文消息
[Client] 收到响应: [Echo #2] 测试中文消息 (全局: 2, 本会话: 2)

> session
========================================
  会话信息
========================================
  会话 ID:      12345
  Leader ID:    0
  已关闭:       false
========================================

> exit
[Client] 退出中...

[Client] 交互式会话结束

[Client] 关闭连接...
```

### 可用命令

| 命令 | 说明 |
|-----|------|
| `exit` / `quit` | 退出客户端 |
| `help` | 显示帮助信息 |
| `session` | 显示当前会话信息（会话 ID、Leader ID 等） |
| 其他文本 | 直接发送到集群 |

---

## 2. 集群回放客户端 (ClusterReplayClient)

### 功能
- 连接到集群节点的 Archive
- 列出所有可用的历史录制
- 从指定位置回放历史消息

### 启动方式

#### 列出所有录制
```bash
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient list
```

#### 回放整个录制
```bash
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay <recordingId>
```

#### 从指定位置回放
```bash
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay <recordingId> <position> <length>
```

### 使用示例

#### 示例 1：列出所有录制

```bash
$ java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient list

========================================
  Aeron Cluster Replay Client
========================================
Archive 控制通道: aeron:udp?endpoint=localhost:9001

[Replay] 启动 MediaDriver...
[Replay] 启动 Aeron...
[Replay] 连接到 Archive...
[Replay] 已连接到 Archive

========================================
  可用的录制 (Recordings)
========================================

Recording ID: 0
  Channel:    aeron:ipc
  Stream ID:  10
  Start Pos:  0
  Stop Pos:   5120
  Length:     5120 bytes
  Start Time: Wed Apr 01 15:30:00 CST 2026
  Stop Time:  [正在录制中]

Recording ID: 1
  Channel:    aeron:udp?endpoint=localhost:9003
  Stream ID:  100
  Start Pos:  0
  Stop Pos:   10240
  Length:     10240 bytes
  Start Time: Wed Apr 01 15:31:00 CST 2026
  Stop Time:  Wed Apr 01 15:32:00 CST 2026

========================================
共找到 2 个录制
========================================
```

#### 示例 2：回放整个录制

```bash
$ java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay 0

========================================
  Aeron Cluster Replay Client
========================================
Archive 控制通道: aeron:udp?endpoint=localhost:9001

[Replay] 启动 MediaDriver...
[Replay] 启动 Aeron...
[Replay] 连接到 Archive...
[Replay] 已连接到 Archive

========================================
  回放录制
========================================
Recording ID: 0
Position:     0
Length:       全部

[Replay] 回放会话已启动，会话 ID: 101
[Replay] 等待回放流连接...
[Replay] 回放流已连接，开始接收数据...
========================================

[Replayed] Hello Cluster!
[Replayed] 测试消息
[Replayed] Another message

[Replay] 回放流已断开

========================================
回放完成，共接收 3 条消息
========================================

[Replay] 关闭连接...
```

#### 示例 3：从指定位置回放指定长度

```bash
# 从位置 1024 开始，回放 2048 字节
$ java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay 0 1024 2048
```

### 参数说明

| 参数 | 说明 | 必需 |
|-----|------|------|
| `list` | 列出所有录制 | - |
| `replay` | 回放命令 | ✓ |
| `recordingId` | 录制 ID（从 list 命令获取） | ✓ |
| `position` | 起始位置（字节偏移），默认 0 | - |
| `length` | 回放长度（字节），默认全部 | - |

### 连接其他节点

默认连接到 Node 0 的 Archive（端口 9001）。如果要连接其他节点：

```java
// 在 ClusterReplayClient.java 中修改
private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9101";  // Node 1
// 或
private static final String ARCHIVE_CONTROL_CHANNEL = "aeron:udp?endpoint=localhost:9201";  // Node 2
```

---

## 3. 完整使用流程

### 步骤 1：启动集群节点

在 3 个终端中分别启动 3 个节点：

```bash
# 终端 1 - Node 0
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0

# 终端 2 - Node 1
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 1

# 终端 3 - Node 2
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 2
```

等待集群选举完成（通常 Node 0 会成为 Leader）。

### 步骤 2：启动交互式客户端

```bash
# 终端 4
java -cp aeron-all.jar io.aeron.samples.cluster.InteractiveClusterClient
```

### 步骤 3：发送消息

在交互式客户端中输入消息：

```
> Hello World!
> 测试消息 1
> 测试消息 2
> exit
```

### 步骤 4：查看历史录制

```bash
# 终端 5
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient list
```

### 步骤 5：回放历史消息

```bash
java -cp aeron-all.jar io.aeron.samples.cluster.ClusterReplayClient replay 0
```

---

## 4. 常见问题

### Q1: 连接失败 "endpoint missing '=' separator"
**A:** 检查 `INGRESS_ENDPOINTS` 格式，必须是 `memberId=host:port`，例如：
```java
"0=localhost:9002,1=localhost:9102,2=localhost:9202"
```

### Q2: Archive 连接失败
**A:** 确保：
1. 集群节点正在运行
2. Archive 控制端口正确（默认 9001 for Node 0）
3. 没有防火墙阻止连接

### Q3: 回放没有数据
**A:** 可能原因：
1. Recording 还没有数据（刚启动的集群）
2. Recording ID 不存在
3. 先发送一些消息，确保有数据被录制

### Q4: 消息乱码
**A:** 回放客户端假设消息是 UTF-8 文本。如果是二进制数据，会显示字节数而不是内容。

---

## 5. 端口配置参考

使用 `ClusterNodeWithConfig` 时的端口分配（portBase=9000）：

| 节点 | Archive Control | Client Ingress | Consensus | Log | Catchup |
|-----|----------------|----------------|-----------|-----|---------|
| Node 0 | 9001 | 9002 | 9003 | 9004 | 9005 |
| Node 1 | 9101 | 9102 | 9103 | 9104 | 9105 |
| Node 2 | 9201 | 9202 | 9203 | 9204 | 9205 |

---

## 6. 进阶用法

### 自定义 Egress 处理

修改 `ClientEgressListener` 来自定义响应处理：

```java
@Override
public void onMessage(...)
{
    // 自定义解码逻辑
    // 例如：使用 SBE 解码、JSON 解析等
}
```

### 批量发送消息

```java
for (int i = 0; i < 1000; i++)
{
    client.sendMessage("Message " + i);
    // 处理背压和重试
}
```

### 回放特定时间范围

通过 Recording 的 `startTimestamp` 和 `stopTimestamp` 计算位置，然后回放指定时间范围的消息。

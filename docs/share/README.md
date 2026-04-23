# Aeron 技术分享系列

> 基于 `docs/` 目录下的源码分析文档整理，共 5 次分享

---

## 分享目录

| 序号 | 文件 | 主题 | 时间 | 前置要求 |
|------|------|------|------|---------|
| 第一次 | [00-aeron-overview.md](./00-aeron-overview.md) | 整体架构与核心技术 | 45～60 分钟 | 无 |
| 第二次 | [01-media-driver.md](./01-media-driver.md) | MediaDriver 深度 | 60～75 分钟 | 第一次 |
| 第三次 | [02-archive.md](./02-archive.md) | Archive 深度 | 60 分钟 | 第二次 |
| 第四次 | [03-cluster.md](./03-cluster.md) | Cluster 深度 | 75～90 分钟 | 第三次 |
| 第五次 | [04-agent-and-observability.md](./04-agent-and-observability.md) | Agent 与可观测性 | 45～60 分钟 | 第二次即可 |

---

## 各次分享核心内容

### 第一次：整体架构与核心技术

**核心问题**：Aeron 为什么快？

- Aeron 定位与竞品对比（Kafka / ZeroMQ / Aeron）
- 模块架构图（client / driver / archive / cluster / agent）
- **核心技术 1：mmap 共享内存**（CnC 文件布局、volatile 同步协议）
- **核心技术 2：ManyToOneRingBuffer**（无锁、负数技巧、CAS 竞争）
- **核心技术 3：LogBuffer / Term Buffer**（三分区轮转、帧格式）
- **核心技术 4：Flyweight 序列化**（零拷贝、零 GC）
- **核心技术 5：Agent 单线程模型**（IdleStrategy、三大 Agent）
- 一条消息的完整旅程（IPC 场景 vs UDP 场景）

**适合 PPT 的核心页**：
1. 竞品对比表格
2. 整体模块架构图
3. mmap 白板类比（"两人共用白板"）
4. 一条消息的完整旅程（动画展示）
5. 关键概念速查表

---

### 第二次：MediaDriver 深度

**核心问题**：一条消息从 offer 到网络对端 poll，每一步发生了什么？

- MediaDriver 三大 Agent（Conductor / Sender / Receiver）职责
- launchEmbedded 启动流程（CnC 初始化、Agent 创建）
- CnC 无锁连接（volatile 版本号同步）
- addSubscription 三阶段协议（命令 → 控制响应 → 数据面建立）
- **publication.offer 深度**（rawTail 编码、Term 轮转、负帧长技巧）
- **subscription.poll 深度**（Round-Robin、Release/Acquire 同步）
- **UDP 可靠传输协议**（SETUP/SM/NAK/RTT 帧）
- 流控闭环（端到端背压，全链路无锁）

**适合 PPT 的核心页**：
1. 三大 Agent 职责图
2. offer 写入五步流程
3. UDP 帧类型体系
4. NAK 重传时序图
5. 端到端背压闭环图

---

### 第三次：Archive 深度

**核心问题**：Aeron 如何以微秒级延迟持久化和回放消息？

- Archive 架构（控制面 vs 数据面）
- ArchivingMediaDriver 启动与嵌入式模式
- AeronArchive.connect 状态机（5 个状态）
- **startRecording 全链路**（Spy 订阅、RecordingSession、blockPoll）
- **磁盘数据结构**（Catalog / Segment 文件 / position 对齐）
- **replay 全链路**（双通道架构、ReplaySession 读盘）
- Archive 在 Cluster 中的三个角色

**适合 PPT 的核心页**：
1. 控制面 vs 数据面分工图
2. startRecording 端到端流程图
3. archiveDir 目录结构
4. Catalog Descriptor 字段表
5. replay 双通道架构图

---

### 第四次：Cluster 深度

**核心问题**：Aeron Cluster 如何在微秒级延迟下实现强一致？

- 四层架构（应用 / 共识 / 持久化 / 传输）
- 核心组件矩阵（ConsensusModule / Election / LogPublisher / ClusteredServiceAgent）
- 启动流程（ClusteredMediaDriver → ConsensusModule → ClusteredServiceContainer）
- **Raft 选举**（Election 状态机：INIT → CANVASS → NOMINATE → BALLOT → READY）
- **日志复制**（quorumPosition 计算、commitPosition 更新）
- **快照机制**（触发 → 暂停接收 → 写入 Archive → 恢复接收）
- **崩溃恢复**（快照 + 增量日志，O(1) 定位）
- 客户端双向通信（Ingress / Egress）

**适合 PPT 的核心页**：
1. 四层架构图
2. Election 状态机流转图
3. 日志复制时序图（3 节点）
4. quorumPosition 计算示例
5. 快照 + 恢复流程图

---

### 第五次：Agent 与可观测性

**核心问题**：如何在不影响微秒级延迟的前提下诊断 Aeron？

- Aeron 可观测性的难点（无 GC / 无锁 / 忙轮询）
- 三种手段（Agent / Counters / AeronStat）的适用场景
- **Java Agent + ByteBuddy 原理**（premain/agentmain、Retransformation、@Advice）
- 整体架构（业务线程 → Ring Buffer → Reader 线程 → 输出）
- 130+ 事件体系（Driver / Archive / Cluster 三类）
- 消息 ID 编码规则（typeId = componentType << 16 | eventCode）
- 启动方式（静态 / 动态 attach / DynamicLoggingAgent）
- 扩展机制（自定义 Reader 接入 Prometheus）
- **Counters 实时监控**（publication-limit / subscriber-pos / recording-pos）
- 生产排查实战案例（背压 / 连接失败 / 选举卡住）

**适合 PPT 的核心页**：
1. 传统日志 vs Aeron Agent 对比
2. ByteBuddy 字节码注入流程图
3. 整体架构图（业务线程 → Ring Buffer → Reader）
4. 事件体系树形图
5. 排查案例（背压问题定位步骤）

---

## 推荐阅读顺序

```
原始文档 → 分享文档对应关系：

cnc-file-and-connect-to-driver.md     → 00 第4节 + 01 第3节
many-to-one-ring-buffer.md            → 00 第5节
publication-offer-source-analysis.md  → 01 第5节
subscription-poll-source-analysis.md  → 01 第6节
aeron-udp-reliability-analysis.md     → 01 第7节
archiving-media-driver-and-aeron-archive-principle.md → 02 第3-4节
archive-start-recording-and-data-structures.md        → 02 第5-6节
archive-sender-receiver-replay-flow.md                → 02 第7节
aeron-cluster-architecture-and-source-analysis.md     → 03 第2-4节
election-dowork-deep-dive.md          → 03 第5节
cluster-log-replication-principle.md  → 03 第6节
leader-follower-sync-mechanism.md     → 03 第6节
snapshot-deep-dive-with-source-code.md → 03 第7节
snapshot-recovery-example-detailed.md  → 03 第8节
aeron-agent-technologies.md           → 04 第1-2节
aeron-agent-deep-dive.md             → 04 第3-8节
```

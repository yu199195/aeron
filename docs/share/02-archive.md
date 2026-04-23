# Aeron Archive 深度解析 - 第三次分享

> 前置知识：已完成前两次分享（整体架构 + MediaDriver）
> 时间预估：60 分钟

---

## 目录

1. [Archive 是什么 & 使用场景](#1-archive-是什么--使用场景)
2. [整体架构](#2-整体架构)
3. [启动流程：ArchivingMediaDriver](#3-启动流程archivingmediadriver)
4. [控制会话：AeronArchive.connect](#4-控制会话aeronarchiveconnect)
5. [录制流程：startRecording 全链路](#5-录制流程startrecording-全链路)
6. [磁盘数据结构](#6-磁盘数据结构)
7. [回放流程：replay 全链路](#7-回放流程replay-全链路)
8. [数据面：录制 vs 回放 的对比](#8-数据面录制-vs-回放-的对比)
9. [关键设计总结](#9-关键设计总结)

---

## 1. Archive 是什么 & 使用场景

**Aeron Archive** 是 Aeron 的**持久化存储层**，提供：

- **录制（Recording）**：将正在发布的消息流持久化到磁盘
- **回放（Replay）**：将历史消息以 Aeron Publication 方式重新发送给订阅者
- **目录管理（Catalog）**：元数据存储，支持查询、截断、删除

### 1.1 典型使用场景

| 场景 | 说明 |
|------|------|
| **行情存档** | 录制全量 tick 数据，支持历史回测 |
| **审计日志** | 不可篡改的交易记录 |
| **消费者追赶** | 新订阅者连接后可以从头回放历史 |
| **崩溃恢复** | 系统重启后从上次位置继续处理（Cluster 快照 + Archive 配合） |
| **数据分发** | 一次录制，多次回放给不同消费者 |

### 1.2 Archive vs Kafka 对比

| 维度 | Kafka | Aeron Archive |
|------|-------|---------------|
| P50 延迟 | 毫秒 | **微秒** |
| GC 影响 | 有 | **无** |
| 消息格式 | 字节流 | **Aeron 帧（带 position 信息）** |
| 流控 | Consumer group offset | **Position Counter（实时同步）** |
| 强一致集群 | ISR 机制 | **配合 Cluster（Raft）** |

---

## 2. 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                     ArchivingMediaDriver 进程                         │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                       MediaDriver                            │   │
│  │  DriverConductor │ Sender │ Receiver                         │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                        ↑↓ IPC (共享 CnC)                            │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                         Archive                              │   │
│  │                                                              │   │
│  │  ┌─────────────────┐   ┌──────────────────────────────────┐ │   │
│  │  │  ArchiveConductor│   │         Recorder                  │ │   │
│  │  │  控制面主循环     │   │  RecordingSession × N            │ │   │
│  │  │  管理 Session    │   │  blockPoll → RecordingWriter      │ │   │
│  │  └─────────────────┘   └──────────────────────────────────┘ │   │
│  │                                                              │   │
│  │  ┌─────────────────────────────────────────────────────────┐ │   │
│  │  │                      Replayer                           │ │   │
│  │  │  ReplaySession × N (读盘 → ExclusivePublication)        │ │   │
│  │  └─────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │           磁盘（archiveDir）                                  │   │
│  │  archive.catalog  │  <recordingId>-<segBase>.rec  × N        │   │
│  └──────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
```

### 2.1 控制面 vs 数据面

```
控制面（Control Plane）：
  Client ──SBE 编码──▶ ArchiveProxy ──▶ 控制 Publication (UDP/IPC)
  Archive ──SBE 编码──▶ ControlResponsePoller ──▶ Client
  职责：startRecording / stopRecording / listRecordings / replay 命令

数据面（Data Plane）：
  录制：Publication → Spy Subscription → RecordingSession → 磁盘
  回放：磁盘 → ReplaySession → ExclusivePublication → 网络 → 客户端 Subscription
```

---

## 3. 启动流程：ArchivingMediaDriver

### 3.1 启动链路

```java
ArchivingMediaDriver.launch(driverCtx, archiveCtx)
    │
    ├─ ① MediaDriver.launch(driverCtx)
    │      创建 CnC、启动三大 Agent（Conductor/Sender/Receiver）
    │
    ├─ ② 共享 errorCounter / errorHandler
    │      Archive 与 MediaDriver 共用同一 CnC 的计数器
    │
    └─ ③ Archive.launch(archiveCtx
              .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
              .aeronDirectoryName(driverCtx.aeronDirectoryName())
              ...)
           │
           ├─ ArchiveConductor 创建
           ├─ Recorder / Replayer 创建
           └─ 启动 Archive 线程（或共享 AgentInvoker）
```

### 3.2 嵌入式模式关键点

```
mediaDriverAgentInvoker 模式：
  Archive 与 MediaDriver 的 Agent 在同一线程中被轮询
  要求：archiveCtx.threadingMode(INVOKER)
  优势：少一个线程，减少上下文切换

独立线程模式（默认）：
  Archive 的 ArchiveConductor 在独立线程中运行
  通过 IPC 与 MediaDriver 通信
```

---

## 4. 控制会话：AeronArchive.connect

### 4.1 AsyncConnect 状态机

```
AeronArchive.connect(ctx)
  └─ asyncConnect(ctx)  →  poll() 循环直到连接完成

状态机流转：
┌─────────────────────┐
│ AWAIT_SUBSCRIPTION  │  等待 controlResponse Subscription 注册完成
└─────────┬───────────┘
          ↓
┌─────────────────────┐
│ ADD_PUBLICATION     │  asyncAddExclusivePublication(controlRequestChannel)
└─────────┬───────────┘  创建 ArchiveProxy
          ↓
┌─────────────────────┐
│ AWAIT_PUBLICATION   │  等待 Publication 就绪
└─────────┬───────────┘
          ↓
┌─────────────────────┐
│ SEND_CONNECT_REQUEST│  archiveProxy.connect(responseChannel, correlationId)
└─────────┬───────────┘  发送 SBE ConnectRequest
          ↓
┌─────────────────────┐
│ AWAIT_CONNECT_RESP  │  等待 Archive 返回 controlSessionId
└─────────┬───────────┘
          ↓
┌─────────────────────┐
│   DONE              │  返回 AeronArchive 实例 ✓
└─────────────────────┘
```

### 4.2 控制通道拓扑

```
Client 进程                         Archive 进程
──────────────                     ──────────────
AeronArchive
  archiveProxy
    controlPublication ──SBE──▶  controlSubscription (Archive 监听)
                                    ControlSessionAdapter 解码
                                    ControlSession 处理
  controlResponsePoller
    controlSubscription ◀──SBE──  clientProxy (回写响应)
```

---

## 5. 录制流程：startRecording 全链路

### 5.1 端到端链路

```
Client                          Archive ArchiveConductor
──────                          ────────────────────────

archiveProxy.startRecording(    ← SBE 编码: StartRecordingRequest
  channel, streamId,
  sourceLocation=LOCAL)
    │ publication.offer()
    ▼
[控制 Publication]
    │ (UDP/IPC)
    ▼
                                ControlSessionAdapter.onStartRecording()
                                    │
                                    ├─ 检查: maxConcurrentRecordings 上限
                                    ├─ 检查: 磁盘空间
                                    ├─ 去重: recordingSubscriptionByKeyMap
                                    │
                                    ├─ sourceLocation == LOCAL?
                                    │   → channel = "spy:" + strippedChannel
                                    │   (Spy 订阅：旁路监听同机 Publication，不影响原流)
                                    │
                                    ├─ aeron.addSubscription(channel, streamId,
                                    │       availableImageHandler)
                                    │
                                    └─ sendOkResponse(subscriptionId) ← 立即响应
                                       （此时 recordingId 还未分配！）

--- 异步：当 Publisher 连接后 Image 变为可用 ---

                                availableImageHandler 触发
                                    │ 推入 taskQueue
                                    ▼
                                startRecordingSession(image)
                                    ├─ catalog.addNewRecording(...) → recordingId
                                    ├─ RecordingPos.allocate(counter) ← position 追踪
                                    ├─ new RecordingSession(image, recordingWriter)
                                    ├─ controlSession.sendSignal(START, recordingId, ...)
                                    └─ recorder.addSession(session)

--- 数据面：RecordingSession.doWork() 循环 ---

RecordingSession.doWork():
  INIT → RECORDING:
    recordingWriter.init()    ← 打开/定位 segment 文件
  RECORDING:
    image.blockPoll(recordingWriter, blockLengthLimit)
        │ 每次最多读 min(termBufferLength, fileIoMaxLength) 字节
        ▼
    RecordingWriter.onBlock(termBuffer, offset, length)
        ├─ FileChannel.write(byteBuffer, fileOffset)   ← 写入磁盘
        ├─ 可选: force()（同步刷盘）
        ├─ segmentOffset += length
        └─ 达到 segmentLength → onFileRollOver()（新建下一个 .rec 文件）
    │
    └─ position.setRelease(recordingWriter.position())  ← 更新 RecordingPos Counter
```

### 5.2 Spy 订阅原理

```
普通订阅（REMOTE 场景）：
  addSubscription("aeron:udp?endpoint=...") → 创建 UDP Socket 接收网络数据

Spy 订阅（LOCAL 场景）：
  addSubscription("spy:aeron:udp?...") → 在 Driver 内部"窃听"同机 Publication

  Publisher → Term Buffer (mmap)
                   ├─▶ Sender → UDP → 远端订阅者
                   └─▶ Spy Subscription → RecordingSession → 磁盘
              （一份数据，两个消费者，零额外拷贝）
```

---

## 6. 磁盘数据结构

### 6.1 Archive 目录结构

```
archiveDir/
├── archive.catalog          ← 元数据：所有 recording 的 descriptor
├── archive.catalog.bak      ← 备份（原子更新时的保障）
├── mark.dat                 ← 版本标记文件
│
├── 0-0.rec                  ← recordingId=0, segmentBasePosition=0
├── 0-16777216.rec           ← recordingId=0, segmentBasePosition=16MB
├── 1-0.rec                  ← recordingId=1, segmentBasePosition=0
└── ...
```

### 6.2 Catalog 结构（archive.catalog）

每条 Recording 在 Catalog 中对应一个 Descriptor（SBE 编码）：

```
Recording Descriptor：
┌──────────────────┬─────────────────────────────────────────────────┐
│ recordingId      │ 全局唯一，自增 long                               │
│ startTimestamp   │ 录制开始时间（epoch ms）                          │
│ startPosition    │ 录制起始 position（与 image.joinPosition 对齐）   │
│ stopPosition     │ 录制停止 position（-1 表示进行中）                 │
│ initialTermId    │ 初始 term ID（用于 position 计算）                 │
│ segmentFileLength│ 每个 segment 文件的字节长度                        │
│ termBufferLength │ Term Buffer 大小                                  │
│ mtuLength        │ MTU                                              │
│ sessionId        │ Publication 会话 ID                              │
│ streamId         │ 流 ID                                            │
│ strippedChannel  │ 去掉变量参数后的 channel URI（用于索引/匹配）       │
│ originalChannel  │ 原始 channel URI                                 │
│ sourceIdentity   │ 数据源标识（IP:port 等）                          │
└──────────────────┴─────────────────────────────────────────────────┘
```

### 6.3 Segment 文件布局

```
<recordingId>-<segmentBasePosition>.rec：

内容：直接是 Aeron Data Frame 序列（与 Term Buffer 格式完全相同）
      │ Frame 1 Header (32B) │ Payload │ Frame 2 Header │ Payload │ ...

segmentBasePosition：该文件对应录制 position 的起始值
fileOffset = position - segmentBasePosition

回放时：
  ① 查 Catalog → 找到 recording
  ② 计算文件路径 = recordingId + "-" + segmentBasePosition(replayPosition) + ".rec"
  ③ 从 fileOffset 位置开始读取帧数据
  ④ 通过 ExclusivePublication.offer() 发出去
```

---

## 7. 回放流程：replay 全链路

### 7.1 双通道架构

```
Receiver 进程（回放客户端）      Archive 进程（Sender）
────────────────────────       ─────────────────────
AeronArchive
  │
  ├─ ① listRecordings()        ← 查询 Catalog，找到目标 recordingId
  │      ◀── RecordingDescriptor × N
  │
  └─ ② startReplay(             ─── SBE ReplayRequest ──▶
          recordingId,                Archive 收到请求
          position,                   │
          length,                     ├─ 查 Catalog，验证 recordingId
          replayChannel,              ├─ 计算起始 segment 文件
          replayStreamId)             ├─ new ReplaySession(...)
                                      └─ replayer.addSession(session)
  ◀── sessionId（回放流标识）

  ③ addSubscription(            ← 订阅回放数据流
       replayChannel,
       replayStreamId)
                                ReplaySession.doWork():
                                  ├─ 读取 segment 文件中的帧
                                  ├─ replayPublication.offer(frame)
                                  └─ 通过 Sender → UDP 发送
  ◀── 接收历史消息帧 ────────────── Receiver MediaDriver 接收，写入 LogBuffer
                                          │
  subscription.poll(handler)              ↓
  → 应用处理历史消息                    handler.onMessage(历史数据)
```

### 7.2 回放 vs 实时流

```
Receiver 进程同时订阅两个流：

实时流（正在录制中）：
  aeron:udp?endpoint=localhost:40456, streamId=10
  → Publisher 实时发送，正常 Aeron 流

回放流（历史数据）：
  aeron:udp?endpoint=localhost:40457, streamId=11
  → Archive ReplaySession 读盘后通过另一个 Publication 发送
  → replayChannel 中带有 session-id 参数（与 Publication 对齐，防止多个回放混淆）

两个流独立，不互相影响。
```

### 7.3 ReplaySession 读盘细节

```java
// ReplaySession.doWork()（简化）
switch (state) {
    case INIT:
        // 打开 segment 文件，定位到 startPosition 对应的 fileOffset
        openSegmentFile(recordingId, startPosition);
        state = REPLAY;
        break;

    case REPLAY:
        // 批量读取帧数据
        int bytesRead = fileChannel.read(replayBuffer, fileOffset);
        // 通过 Publication 发送（与普通 offer 完全相同）
        replayPublication.offerBlock(replayBuffer, 0, bytesRead);
        fileOffset += bytesRead;

        // 跨 segment 文件边界
        if (fileOffset >= segmentLength) {
            rollOverToNextSegment();
        }
        break;
}
```

---

## 8. 数据面：录制 vs 回放 的对比

```
录制（Recording）：
  Publisher 发布 ──▶ Term Buffer (mmap)
                         │
                  ┌──────▼──────┐
                  │ Spy 订阅    │  (或普通 UDP 订阅)
                  └──────┬──────┘
                         │ image.blockPoll()
                  ┌──────▼──────┐
                  │RecordingWriter│  FileChannel.write()
                  └──────┬──────┘
                         ▼
                    segment .rec 文件

回放（Replay）：
    segment .rec 文件
         │ fileChannel.read()
  ┌──────▼──────┐
  │ReplaySession│
  └──────┬──────┘
         │ publication.offerBlock()
  ┌──────▼──────┐
  │Term Buffer  │  (ExclusivePublication 写入)
  └──────┬──────┘
         │ Sender → UDP
         ▼
  Receiver Subscription.poll()
```

| 维度 | 录制 | 回放 |
|------|------|------|
| 数据来源 | Image（实时 Publication 或 Spy） | segment .rec 文件 |
| 数据消费方式 | `image.blockPoll()` | `fileChannel.read()` |
| 数据输出目标 | 磁盘 segment 文件 | `ExclusivePublication` → 网络 |
| position 追踪 | RecordingPos Counter | 内部 fileOffset |
| 信号通知 | RecordingSignal.START/STOP | 回放结束通知 |

---

## 9. 关键设计总结

### 9.1 控制面与数据面解耦

```
控制面：SBE over Aeron (UDP/IPC)
  延迟：毫秒级（可接受）
  职责：命令、状态、元数据查询

数据面：blockPoll（录制）/ offerBlock（回放）
  延迟：微秒级（关键路径）
  职责：高吞吐数据 IO
```

### 9.2 position 对齐机制

```
position = termId × termBufferLength + termOffset

Archive 的所有文件偏移都与 position 对齐：
  segmentFileBasePosition = position - (position % segmentLength)
  fileOffset = position - segmentFileBasePosition

这保证了：
  ① 断点续传：知道 startPosition 就能直接定位文件和偏移
  ② 与 Cluster 快照配合：快照 position = Archive 位置
  ③ 无需扫描：O(1) 时间找到任意历史帧
```

### 9.3 RecordingPos Counter

```
RecordingPos Counter 是 Archive 与外部系统的桥梁：

  Archive 写入端：
    position.setRelease(recordingWriter.position())
    ← 每次 blockPoll 后更新

  外部观察端：
    countersReader.getCounterValue(recordingId) → 当前录制进度
    Cluster ClusteredServiceAgent 监听此 Counter
    → 确认 Archive 已持久化到某个 position
    → 才触发快照 / 提交确认

  这是 Archive 与 Cluster 协作的关键连接点。
```

### 9.4 Archive 在 Cluster 中的角色

```
Cluster = ConsensusModule + ClusteredServiceContainer + Archive

Archive 负责：
  ① 录制 Cluster 日志（ConsensusModule 发布的所有命令序列）
  ② 录制快照（ClusteredService 的状态快照）
  ③ 向新加入的 Follower 回放日志（追赶进度）
  ④ 向恢复中的节点回放日志 + 快照（崩溃恢复）

没有 Archive，Cluster 无法：
  - 持久化共识日志
  - 做快照
  - 节点间同步
```

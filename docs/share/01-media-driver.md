# MediaDriver 深度解析 - 第二次分享

> 前置知识：已完成第一次分享（整体架构概览）
> 时间预估：60～75 分钟

---

## 目录

1. [MediaDriver 是什么](#1-mediadriver-是什么)
2. [启动流程：launchEmbedded 全链路](#2-启动流程launchembedded-全链路)
3. [CnC 文件与 Client 连接机制](#3-cnc-文件与-client-连接机制)
4. [addSubscription 全流程](#4-addsubscription-全流程)
5. [发送路径：数据结构全景](#5-发送路径数据结构全景)
6. [接收路径：数据结构全景](#6-接收路径数据结构全景)
7. [publication.offer 深度解析](#7-publicationoffer-深度解析)
8. [subscription.poll 深度解析](#8-subscriptionpoll-深度解析)
9. [UDP 可靠传输协议](#9-udp-可靠传输协议)
10. [流控闭环：端到端背压](#10-流控闭环端到端背压)
11. [关键设计总结](#11-关键设计总结)

---

## 1. MediaDriver 是什么

**MediaDriver** 是 Aeron 的"网络引擎"，负责：

- 管理所有 Publication/Subscription 的生命周期
- 分配和维护 LogBuffer（数据缓冲区）
- 通过 UDP 发送和接收数据帧
- 维护 Client 与 Driver 之间的通信通道（CnC mmap）

### 1.1 三大核心 Agent

```
MediaDriver 进程/嵌入式
┌─────────────────────────────────────────────────────────────────┐
│                      DriverConductor                            │
│  职责：控制面                                                    │
│  - 接收 Client 命令（add/remove pub/sub）                        │
│  - 分配 LogBuffer（mmap 文件）                                   │
│  - 创建 Counter（流控位置追踪）                                   │
│  - 协调 Sender 和 Receiver（通过 Proxy 发送命令）                │
│  - 维护 Publication/Subscription/Image 状态机                   │
├─────────────────────────────────────────────────────────────────┤
│                          Sender                                 │
│  职责：数据发送面                                                 │
│  - 扫描所有 NetworkPublication 的 Term Buffer                   │
│  - 将已完成帧通过 UDP DatagramChannel 发出                       │
│  - 处理 NAK 重传请求                                             │
│  - 发送 SETUP 帧建立连接                                         │
│  - 维护 senderLimit（发送窗口）                                   │
├─────────────────────────────────────────────────────────────────┤
│                         Receiver                                │
│  职责：数据接收面                                                 │
│  - NIO Selector.selectNow() 轮询 UDP Socket（非阻塞忙轮询）       │
│  - 解析帧类型（DATA/SETUP/SM/NAK/RTT）并分发                    │
│  - 将 DATA 帧写入 LogBuffer（PublicationImage.insertPacket）     │
│  - 发送 Status Message（SM）进行流控反馈                         │
│  - 检测丢包并发送 NAK                                            │
└─────────────────────────────────────────────────────────────────┘
```

### 1.2 三种运行模式

```java
// ThreadingMode 配置
DEDICATED     // 每个 Agent 独立线程（生产推荐）
SHARED        // 三个 Agent 共享一个线程（嵌入式轻量场景）
SHARED_NETWORK // Sender + Receiver 共享，Conductor 独立
INVOKER       // 调用者线程手动驱动（测试/嵌入集成）
```

---

## 2. 启动流程：launchEmbedded 全链路

### 2.1 完整流程

```
MediaDriver.launchEmbedded(ctx)
        │
        ├─ 生成唯一目录名（避免多实例冲突）
        │
        └─ launch(ctx)
               │
               └─ new MediaDriver(ctx)
                       │
                       ├─ ① concludeAeronDirectory()      创建 Aeron 目录
                       │
                       ├─ ② ensureDirectoryIsRecreated()  清理旧目录
                       │    ├─ 旧目录存在且旧 Driver 已死 → 删除重建
                       │    └─ 旧 Driver 还活着 → 抛出异常
                       │
                       ├─ ③ ctx.conclude()                核心初始化
                       │    ├─ 创建 CnC 文件（mmap）
                       │    ├─ 写入 Metadata（各 buffer 长度、PID、超时）
                       │    ├─ 创建 Counters（流控位置计数器）
                       │    ├─ 创建 ClientProxy（Driver→Client 响应代理）
                       │    ├─ 创建 ReceiverProxy/SenderProxy（Agent 间通信）
                       │    └─ putIntVolatile(版本号) ← "就绪信号"
                       │
                       ├─ ④ new DriverConductor(ctx)
                       │   new Receiver(ctx)
                       │   new Sender(ctx)
                       │
                       └─ ⑤ 根据 ThreadingMode 创建 Runner/Invoker 并启动线程
```

### 2.2 CnC 就绪信号（Volatile 同步）

```
Driver 端                              Client 端
─────────                              ─────────
fillMetaData(...)                      (轮询等待)
  写入所有 buffer 长度字段...
  写入 PID、超时...

putIntVolatile(版本号) ← store barrier → getIntVolatile(版本号) → 非零 ✓
                                         ⇒ 所有 Metadata 字段可见（happens-before）
                                         ⇒ Client 可以安全读取各 buffer 偏移量
```

> **关键**：版本号最后写入，利用 Java Memory Model 的 volatile happens-before 语义，无需任何锁。

---

## 3. CnC 文件与 Client 连接机制

### 3.1 Client 连接过程（connectToDriver）

```java
// 全部通过 mmap 共享内存，无 socket 连接
while (null == toDriverBuffer) {
    // ① 等待 cnc.dat 文件创建并 mmap
    cncMetaDataBuffer = awaitCncFileCreation(cncFile, clock, deadlineMs);

    // ② 校验文件完整性（各区段长度之和 = 文件大小）
    if (!isCncFileLengthSufficient(...)) { retry; }

    // ③ 在 mmap 内存上创建 ManyToOneRingBuffer
    ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
        CncFileDescriptor.createToDriverBuffer(cncByteBuffer, ...));

    // ④ 等待 Driver 第一次心跳（Driver 存活证明）
    while (0 == ringBuffer.consumerHeartbeatTime()) { wait; }

    // ⑤ 检查心跳时效（防止连接到已死 Driver 的残留 CnC）
    if (heartbeat < now - timeout) { retry; }

    toDriverBuffer = ringBuffer;  // ✅ 连接成功
}
```

### 3.2 CnC 内存布局

```
cnc.dat（mmap 共享内存）
┌──────────────────┬───────────────────────────────────────────┐
│   Metadata       │  版本号 | buffer 长度 | PID | 超时 ...    │  128B
├──────────────────┼───────────────────────────────────────────┤
│ to-driver Buffer │  ManyToOneRingBuffer (Client→Driver 命令) │  默认 1MB
├──────────────────┼───────────────────────────────────────────┤
│to-clients Buffer │  BroadcastReceiver  (Driver→Client 响应) │  默认 1MB
├──────────────────┼───────────────────────────────────────────┤
│Counters Metadata │  计数器标签（名称、类型描述）               │
├──────────────────┼───────────────────────────────────────────┤
│ Counters Values  │  pub-limit / sub-pos 等数值               │
├──────────────────┼───────────────────────────────────────────┤
│    Error Log     │  Driver 错误日志                          │
└──────────────────┴───────────────────────────────────────────┘
```

---

## 4. addSubscription 全流程

### 4.1 三阶段协议

```
应用线程                     CnC 共享内存               Driver Conductor      Receiver
────────                    ──────────────             ────────────────      ────────

① 命令发送（同步）
aeron.addSubscription()
  driverProxy.addSubscription()
  → tryClaim(CAS)
  → 填充 Flyweight
  → commit(volatile) ──────▶ [to-driver Buffer]
                                     │
② 控制面响应（同步等待）               │ 消费命令
awaitResponse(correlationId)         ├─ UdpChannel.parse(channel)
  轮询 to-clients Buffer             ├─ 创建 ReceiveChannelEndpoint
                                     ├─ DatagramChannel.bind(port) ← UDP 端口监听
                                     ├─ receiverProxy.addSubscription() ──▶ 注册 DataPacketDispatcher
                              ◀──── clientProxy.onSubscriptionReady()
                              [to-clients Buffer] ← 写入
return subscription ✓

③ 数据面建立（异步）
Publisher 连接后 Driver 发送 ON_AVAILABLE_IMAGE
ClientConductor 收到后：
  - logBuffers(logFileName) → mmap 打开 LogBuffer 文件
  - new Image(logBuffers, subscriberPositionCounter)
  - subscription.addImage(image)  ← volatile write
  ★ 此时 subscription.poll() 才能收到数据 ★
```

### 4.2 UDP Socket 创建链路

```
DriverConductor.onAddNetworkSubscription()
  └─ getOrCreateReceiveChannelEndpoint()
       └─ ReceiveChannelEndpoint.openChannel()
            └─ openDatagramChannel()
                 ├─ DatagramChannel.open(INET)           ← 创建 UDP Socket
                 ├─ channel.bind(localhost:40123)         ← 绑定端口，OS 开始监听
                 └─ channel.configureBlocking(false)      ← 非阻塞模式
  └─ receiverProxy.registerReceiveChannelEndpoint()
       └─ channel.register(selector, OP_READ)            ← 注册到 NIO Selector
```

> **端口复用**：同一 UDP 地址的多个 streamId 共享一个 DatagramChannel，由 DataPacketDispatcher 按 streamId 分发。

---

## 5. 发送路径：数据结构全景

### 5.1 发送路径总览

```
应用线程                    共享内存（mmap）                  Driver Sender 线程
──────────                  ────────────────                  ──────────────────
Publication                 LogBuffer 文件                    NetworkPublication
  positionLimit ◄────────── Counters Values Buffer             senderPosition
  logMetaDataBuffer ──────► Log Meta Data（4KB）               senderLimit
  termBuffers[3] ──────────► Term Buffer 0 / 1 / 2             TermScanner
                                                              SendChannelEndpoint
                                                              RetransmitHandler
```

发送路径上的关键对象：

| 对象 | 所在模块 | 职责 |
|------|---------|------|
| `ConcurrentPublication` / `ExclusivePublication` | Client 侧 | 对外暴露 `offer()` API，操作 Term Buffer |
| `LogBuffer`（mmap 文件） | 共享内存 | 3 个 Term Buffer + Log Meta Data，Publisher 写，Sender 读 |
| `NetworkPublication` | Driver Conductor 侧 | Driver 对 Publication 的视图；管理 senderLimit、重传队列 |
| `TermScanner` | Driver Sender 侧 | 扫描 Term Buffer 中已完成帧，计算可发字节数 |
| `SendChannelEndpoint` | Driver Sender 侧 | 封装 UDP DatagramChannel，执行实际发送 |
| `RetransmitHandler` | Driver Sender 侧 | 接收 NAK，去重后触发帧重传 |

---

### 5.2 LogBuffer 文件布局（mmap 核心）

每个 Publication 对应一个 **LogBuffer 文件**（位于 Aeron 目录下，如 `/dev/shm/aeron-<pid>/publications/<id>.logbuffer`）：

```
LogBuffer 文件（mmap 映射到进程地址空间）：

 文件起始                                                  文件末尾
 ┌────────────────┬────────────────┬────────────────┬──────────────┐
 │  Term Buffer 0 │  Term Buffer 1 │  Term Buffer 2 │ Log Meta Data│
 │  (termLength)  │  (termLength)  │  (termLength)  │   (4096B)    │
 └────────────────┴────────────────┴────────────────┴──────────────┘
  <───────────── 3 × termLength ──────────────────>
  总文件大小 = 3 × termLength + 4096

注：termLength 默认 16MB，总文件大小 ≈ 48MB + 4KB
    可通过 ctx.termBufferLength() 配置（必须是 2 的幂，范围 64KB～1GB）
```

每个 Term Buffer 内部是连续的帧序列：

```
Term Buffer（termLength 字节）：

offset=0                                              offset=termLength
 ┌──────────┬──────────┬──────────┬──────────┬────────────────────┐
 │ Frame #1 │ Frame #2 │ Frame #3 │  PADDING │     空闲区域        │
 │ [Hdr+Payload]       │  [Hdr+P] │ (末尾补齐)│  (tail 之后的位置) │
 └──────────┴──────────┴──────────┴──────────┴────────────────────┘
              ▲                               ▲
              │                               │
         Publisher 已写入                     tail（下次写入位置）
```

---

### 5.3 Log Meta Data 布局（4KB，控制信息核心）

Log Meta Data 紧跟在三个 Term Buffer 之后，存储所有控制信息：

```
Log Meta Data（4096 字节，关键字段）：

 字节偏移   字段名                         大小    说明
 ──────────────────────────────────────────────────────────────────────────
 +0         TERM_TAIL_COUNTER[0]           8B      rawTail for Term 0
                                                   高32位=termId，低32位=termOffset
 +8         TERM_TAIL_COUNTER[1]           8B      rawTail for Term 1
 +16        TERM_TAIL_COUNTER[2]           8B      rawTail for Term 2
 +24        ACTIVE_TERM_COUNT              4B      当前活跃 term 序号（volatile，CAS 更新）
 +28        (保留)                         4B
 +128       END_OF_STREAM_POSITION         8B      流结束 position（Publication 关闭时写入）
 +136       IS_CONNECTED                   4B      是否有活跃订阅者（Driver 设置，offer 检查）
 +140       ACTIVE_TRANSPORT_COUNT         4B      活跃传输数量
 +256       CORRELATION_ID                 8B      Publication 关联 ID（创建时分配）
 +264       INITIAL_TERM_ID                4B      初始 term ID（随机生成，防止不同流冲突）
 +268       DEFAULT_FRAME_HEADER_LENGTH    4B      默认帧头大小（固定 32B）
 +272       MTU_LENGTH                     4B      MTU（最大传输单元，默认 1408B）
 +276       TERM_LENGTH                    4B      单个 Term Buffer 大小
 +280       PAGE_SIZE                      4B      OS 页大小
 +320       DEFAULT_FRAME_HEADER           128B    预填充帧头模板（version/type/session/stream 等）
```

**关键原理**：
- `TERM_TAIL_COUNTER[i]` 是 `getAndAddLong` 的目标，多线程 offer 时原子推进
- `ACTIVE_TERM_COUNT` 通过 CAS 更新，触发 Term 轮转
- `IS_CONNECTED` 由 Driver 在有/无订阅者时切换，`offer` 据此决定返回 `NOT_CONNECTED` 还是继续写

---

### 5.4 Data Frame 帧格式（32 字节固定头）

Term Buffer 和网络传输使用**相同的帧格式**（零拷贝的基础）：

```
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 ┌───────────────────────────────────────────────────────────────┐
 │                       Frame Length                            │  offset  0
 │   写入时先置为负值（占位），完成后覆写正值（release语义发布）   │  (4B)
 ├───────────────────┬────────┬──────────────────────────────────┤
 │    Version (1B)   │ Flags  │            Type (2B)             │  offset  4
 │    0x00           │B|E|S|R │  DATA=0x0001 / PADDING=0x0002    │
 ├───────────────────────────────────────────────────────────────┤
 │                       Term Offset                             │  offset  8
 │              帧在当前 Term 内的起始字节偏移                     │  (4B)
 ├───────────────────────────────────────────────────────────────┤
 │                       Session ID                              │  offset 12
 │         Publication 的会话 ID（随机，区分同流多 Publisher）     │  (4B)
 ├───────────────────────────────────────────────────────────────┤
 │                       Stream ID                               │  offset 16
 │                 应用层流标识（如 10）                           │  (4B)
 ├───────────────────────────────────────────────────────────────┤
 │                       Term ID                                 │  offset 20
 │            Term 序号（单调递增，从 initialTermId 开始）         │  (4B)
 ├───────────────────────────────────────────────────────────────┤
 │                    Reserved Value                             │  offset 24
 │              用户自定义保留值（ReservedValueSupplier）         │  (8B)
 ├───────────────────────────────────────────────────────────────┤
 │                      Payload（消息体）                         │  offset 32
 │                  应用传入的原始字节                             │  (可变长)
 └───────────────────────────────────────────────────────────────┘
 │         对齐填充（Padding，凑足 FRAME_ALIGNMENT=32 字节边界）   │
 └───────────────────────────────────────────────────────────────┘

Flags 字段含义：
  B (bit 7, 0x80)  = BEGIN_FRAG_FLAG  ← 消息的第一个分片
  E (bit 6, 0x40)  = END_FRAG_FLAG   ← 消息的最后一个分片
  B+E = 0xC0                          ← 未分片的完整消息
  S (bit 5, 0x20)  = EOS              ← End of Stream 标记
  R (bit 4, 0x10)  = REVOKED          ← 事务已撤销
```

---

### 5.5 ConcurrentPublication 关键字段

```java
// ConcurrentPublication（Client 进程内的对象，不在 mmap 中）
class ConcurrentPublication {
    // ─── mmap 共享区域引用 ───────────────────────────────────────
    private final LogBuffers logBuffers;         // LogBuffer mmap 文件句柄
    private final UnsafeBuffer logMetaDataBuffer;// Log Meta Data 区域视图
    private final UnsafeBuffer[] termBuffers;    // termBuffers[3] = 三个 Term Buffer 视图

    // ─── Counter（也在 CnC mmap 中）────────────────────────────
    private final ReadablePosition positionLimit;// publication limit（Driver 维护，流控上限）

    // ─── 本地缓存（无需 volatile）───────────────────────────────
    private final int initialTermId;             // 从 logMetaDataBuffer 读取一次后缓存
    private final int positionBitsToShift;       // = log2(termLength)，用于 position 计算
    private final int maxPayloadLength;          // = MTU - HEADER_LENGTH，单帧最大 payload
    private final long maxPossiblePosition;      // = termLength × (Integer.MAX_VALUE + 1L)
    private final HeaderWriter headerWriter;     // 预构建帧头写入器（缓存 sessionId/streamId 等）
}
```

---

### 5.6 NetworkPublication（Driver 侧发送视图）

```java
// NetworkPublication（Driver 进程内，代表一个正在发送的流）
class NetworkPublication {
    // ─── mmap 共享区域引用（与 Client 侧 Publication 共享同一文件）──
    private final LogBuffers rawLog;
    private final UnsafeBuffer[] termBuffers;    // 同一块 mmap
    private final UnsafeBuffer metaDataBuffer;

    // ─── Sender 侧 Counter ─────────────────────────────────────
    private final Position senderPosition;       // Sender 已发送到的 position
    private final Position senderLimit;          // 流控上限（Sender 不超此位置）

    // ─── 重传管理 ────────────────────────────────────────────
    private final RetransmitHandler retransmitHandler; // 接收 NAK，触发重传
    private final FlowControl flowControl;       // 可插拔流控（UnicastFlowControl / ...）

    // ─── 状态追踪 ────────────────────────────────────────────
    private final SendChannelEndpoint channelEndpoint; // UDP 发送端点
    private long timeOfLastSendOrHeartbeatNs;    // 上次发送/心跳时间（超时发 SETUP）
}
```

**Sender 发送核心循环**：

```
Sender.doWork()
  └─ for each NetworkPublication pub:
       pub.send(nowNs)
         ├─ TermScanner.scanForAvailability(termBuffers[activeIndex], termOffset, scanLimit)
         │    扫描连续已完成帧（frameLengthVolatile > 0），累加 available 字节数
         │    遇到 0/负值 → 停止（帧未完成）
         │    遇到 PADDING → 记录并停止（Term 末尾）
         │
         ├─ sendBuffer.limit(termOffset + available).position(termOffset)
         │    切片 ByteBuffer（与 Term Buffer 共享底层内存，零拷贝）
         │
         ├─ channelEndpoint.send(sendBuffer)
         │    → DatagramChannel.write(sendBuffer) → sendto() 系统调用
         │
         └─ senderPosition.setRelease(senderPosition + bytesSent)
              更新已发送 position，反馈给流控计算
```

---

## 6. 接收路径：数据结构全景

### 6.1 接收路径总览

```
网络 UDP                    Driver Receiver 线程              Client 侧应用线程
──────────                  ────────────────────              ──────────────────
UDP DatagramChannel         ReceiveChannelEndpoint             Subscription
DatagramPacket              DataPacketDispatcher               Image[]
                            PublicationImage                   Image
                            ← 写入 LogBuffer（mmap）→         subscriberPosition
                                                              → handler.onFragment()
```

接收路径上的关键对象：

| 对象 | 所在模块 | 职责 |
|------|---------|------|
| `ReceiveChannelEndpoint` | Driver Receiver 侧 | 封装 UDP DatagramChannel，NIO Selector 轮询 |
| `DataPacketDispatcher` | Driver Receiver 侧 | 按 `streamId` + `sessionId` 分发收到的帧 |
| `PublicationImage` | Driver Receiver 侧 | Driver 对一条入站流的视图；写 LogBuffer，管理 NAK/SM |
| `LogBuffers`（mmap 文件） | 共享内存 | 接收端的 Term Buffer，Receiver 写，Image.poll() 读 |
| `Image` | Client 侧 | Client 对一条入站流的视图；`poll()` 从 Term Buffer 读帧 |
| `Subscription` | Client 侧 | 聚合多个 Image，`poll()` Round-Robin 轮询 |

---

### 6.2 ReceiveChannelEndpoint 与 DataPacketDispatcher

```
ReceiveChannelEndpoint（每个 UDP 地址对应一个实例）：

  字段                           说明
  ─────────────────────────────────────────────────────────────────
  DatagramChannel receiveChannel     非阻塞 UDP Socket（已 bind 到端口）
  NioSelectedKeySet / Selector       注册到 Receiver 的 NIO Selector
  DataPacketDispatcher dispatcher    帧分发器（按 stream/session 路由）
  InetSocketAddress bindAddress      监听地址（如 0.0.0.0:40123）

DataPacketDispatcher 内部路由表：

  Map<Integer streamId,
       Map<Integer sessionId, PublicationImage>> imageBySessionIdByStreamIdMap

  收到 DATA 帧时：
    ① 从帧头提取 streamId（offset 16）、sessionId（offset 12）
    ② imageBySessionIdByStreamIdMap.get(streamId).get(sessionId)
    ③ → publishImage.insertPacket(termId, termOffset, buffer, length)
       → 写入对应的 Term Buffer

  收到 SETUP 帧时：
    ① 检查是否已有 PublicationImage（防重复创建）
    ② 没有 → 通知 DriverConductor 创建新的 PublicationImage
    ③ 返回 SM 开放接收窗口
```

---

### 6.3 PublicationImage 关键字段（Driver 侧接收视图）

```java
// PublicationImage（Driver 进程内，每个 Publisher session 对应一个实例）
class PublicationImage {
    // ─── mmap LogBuffer（接收端独立文件）──────────────────────
    private final LogBuffers rawLog;
    private final UnsafeBuffer[] termBuffers;       // 3 个接收 Term Buffer

    // ─── 乱序/丢包追踪 ────────────────────────────────────────
    private long rebuildPosition;                    // 已连续收到的最高 position（无间隙）
    private final Position hwmPosition;              // high watermark = 收到最高 position（可有间隙）
    private final LossDetector lossDetector;         // 间隙检测器，触发 NAK

    // ─── 流控 ─────────────────────────────────────────────────
    private final CongestionControl congestionControl; // 窗口计算（Static/Cubic）
    private final Position[] subscriberPositions;    // 所有订阅者的消费 position

    // ─── SM 发送 ───────────────────────────────────────────────
    private long timeOfLastStatusMessageNs;          // 上次发 SM 的时间（定期发送）
    private int receiverWindowLength;                // SM 中携带的接收窗口大小
    private long nextSmPosition;                     // 下次发 SM 时的 position

    // ─── 流标识 ────────────────────────────────────────────────
    private final int sessionId;
    private final int streamId;
    private final int initialTermId;
    private final int termLengthMask;                // = termLength - 1（用于取模）
}
```

**insertPacket 写入流程**：

```
PublicationImage.insertPacket(termId, termOffset, buffer, length)
  │
  ├─ 计算 packetPosition = computePosition(termId, termOffset)
  │
  ├─ if (packetPosition == rebuildPosition)
  │    → 顺序到达，连续区间推进
  │  else
  │    → 乱序到达，只更新 hwmPosition（如果更高），等 NAK 触发重传
  │
  ├─ termBuffer = termBuffers[termIndex]
  │   termIndex = (termId - initialTermId) % 3
  │
  ├─ termBuffer.putBytes(termOffset + HEADER_LENGTH, buffer, offset, length)
  │   → 将 UDP payload 写入 Term Buffer 对应位置
  │
  └─ frameLengthOrdered(termBuffer, termOffset, frameLength)
     → release 语义写正帧长，对 Image.poll() 可见
```

---

### 6.4 LossDetector 与 NAK 机制

```
LossDetector（间隙检测器）：

  每次 Receiver.doWork() 调用：
    lossDetector.scan(termBuffer, rebuildPosition, hwmPosition, ...)
      │
      ├─ 从 rebuildPosition 开始扫描
      ├─ 遇到 frameLengthVolatile = 0 → 发现间隙（Gap）
      │
      ├─ 记录 gapPosition（间隙起点）
      │
      └─ 间隙持续超过 NAK_DELAY（默认 60μs）？
           是 → onLossDetected(termId, gapTermOffset, gapLength)
                → ReceiveChannelEndpoint.sendNak(NAKFrame)
                → UDP 发送到 Publisher

RetransmitHandler（Publisher Sender 侧，接收 NAK）：

  收到 NAK 帧 → checkRetransmit(termId, termOffset, length)
    ├─ 已在重传队列中？→ 重置定时器（抑制重复 NAK 风暴）
    └─ 新的 NAK → 加入重传队列 → 下次 Sender.doWork() 触发重传
```

---

### 6.5 Image 关键字段（Client 侧读取视图）

```java
// Image（Client 进程内，与 PublicationImage 对应，共享同一 LogBuffer mmap）
class Image {
    // ─── mmap LogBuffer（与 PublicationImage 共享同一文件）────
    private final LogBuffers logBuffers;
    private final UnsafeBuffer[] termBuffers;    // 3 个 Term Buffer（与 Driver 侧共享内存）

    // ─── 消费位置（在 CnC mmap 中）──────────────────────────────
    private final Position subscriberPosition;   // 当前消费到的 position
                                                 // → Driver 读取后更新 SM 窗口
    // ─── 读取辅助 ────────────────────────────────────────────
    private final Header header;                 // 可复用的帧头解析器（零分配）
    private final int termLengthMask;            // = termLength - 1

    // ─── 流标识 ────────────────────────────────────────────────
    private final int sessionId;
    private final int streamId;
    private final int initialTermId;
    private final int positionBitsToShift;       // = log2(termLength)

    // ─── 生命周期 ─────────────────────────────────────────────
    private volatile boolean isClosed;           // ClientConductor 关闭时设置
    private final String sourceIdentity;         // Publisher 端地址（IP:port）
    private final long correlationId;            // 与 Subscription 关联的 correlationId
}
```

---

### 6.6 LogBuffer 文件共享机制

**发送端和接收端的 LogBuffer 是完全独立的文件**（不共享），但 Driver 侧和 Client 侧在同一进程内共享：

```
UDP 场景（跨机/本机 UDP）：

Publisher 端（进程 A）:                Subscriber 端（进程 B）:
  /dev/shm/aeron-A/pub-xxx.logbuffer     /dev/shm/aeron-B/sub-yyy.logbuffer
  ┌──────────────────────────────┐       ┌──────────────────────────────┐
  │ Driver A + Client A 共享此文件│       │ Driver B + Client B 共享此文件│
  │ (mmap 同一文件的不同映射)      │       │ (mmap 同一文件的不同映射)      │
  └──────────────────────────────┘       └──────────────────────────────┘
        │                                       ▲
        └── UDP 网络传输 ──────────────────────►┘
            (Data/Setup/SM/NAK 帧)

IPC 场景（同机 IPC）：

Publisher 端（进程 A）:                Subscriber 端（进程 A 内部）:
  /dev/shm/aeron-A/pub-xxx.logbuffer     同一文件！
  ┌──────────────────────────────┐
  │     Publisher 写入 tail 端    │
  │     Subscriber 读取消费位置   │ ← 完全零拷贝，同一内存页
  └──────────────────────────────┘
```

**关键设计点**：
- UDP 场景：Receiver 将 UDP 包写入接收端 LogBuffer，Client 直接读取（一次网络拷贝，之后零拷贝）
- IPC 场景：Publisher 写入，Subscriber 直接读同一 mmap 文件，真正零拷贝

---

### 6.7 subscriberPosition 与流控反馈链路

```
Client 侧 Image.poll() 调用：
  ① 读取 subscriberPosition（CnC Counters Buffer 中的 long）
  ② 扫描帧，推进 offset
  ③ subscriberPosition.setRelease(newPosition)  ← release 写
      │
      │（Counter 在 CnC mmap 中，Driver 直接读取）
      ▼
Driver Receiver.doWork():
  ④ 读取 subscriberPositions[] → 计算 minSubscriberPosition
  ⑤ 定期发送 SM（Status Message）：
     SM.consumedPosition = minSubscriberPosition
     SM.receiverWindowLength = congestionControl.initialWindowLength()
      │
      │（UDP 发回 Publisher 端）
      ▼
Driver Sender.doWork()（Publisher 端）:
  ⑥ onStatusMessage(SM) → flowControl.onStatusMessage(SM)
  ⑦ senderLimit = SM.consumedPosition + SM.receiverWindowLength
      │
      │（写入 CnC Counters Buffer 的 positionLimit Counter）
      ▼
Client 侧 ConcurrentPublication.offer():
  ⑧ positionLimit.getVolatile() → 读到新的上限
  ⑨ position < limit → 继续写入（背压解除）
```

---

## 7. publication.offer 深度解析

### 7.1 核心路径（非阻塞写入 Term Buffer）

```
publication.offer(buffer, 0, length)
  │
  ├─ ① 读 positionLimit（Counter，由 Sender 根据接收端 SM 更新）
  │   读 activeTermCount（当前使用的 Term 索引）
  │   读 rawTail（当前 Term 的写入位置）
  │
  ├─ ② 背压检查
  │   position >= positionLimit → 返回 BACK_PRESSURED（非阻塞）
  │   publisher 未就绪 → 返回 NOT_CONNECTED
  │
  ├─ ③ 写入策略分支
  │   length <= maxPayloadLength → appendUnfragmentedMessage()
  │   length > maxPayloadLength  → appendFragmentedMessage()（自动分片）
  │
  └─ appendUnfragmentedMessage():
       │
       ├─ ④ getAndAddLong(rawTailIndex, alignedLength)
       │       ← 原子 fetch-and-add，无 CAS 重试，一次成功
       │       ← 多线程 offer 时各自拿到不重叠的区间
       │
       ├─ ⑤ HeaderWriter.write(termBuffer, termOffset, -frameLength, ...)
       │       ← 先写负帧长：告知 Sender 此帧尚未完成
       │
       ├─ ⑥ putBytes(termOffset + HEADER_LENGTH, srcBuffer, offset, length)
       │       ← 拷贝消息内容到 Term Buffer
       │
       └─ ⑦ frameLengthOrdered(termBuffer, termOffset, frameLength)
               ← putIntRelease（release 语义）写正帧长
               ← Sender 看到正帧长 = 此帧完成，可以发送
               ← 返回 newPosition
```

### 7.2 rawTail 编码（termId + termOffset 打包）

```
rawTail 是一个 long（8 字节）：
┌────────────────────┬────────────────────┐
│   termId (高32位)  │ termOffset (低32位) │
└────────────────────┴────────────────────┘

termOffset: 当前 Term 内的字节偏移（写到哪了）
termId: 第几个 Term（不回绕，全局递增）
activeTermIndex = termId % 3（选用哪个 Term Buffer）

从 rawTail 推导 position：
  position = (termId * termBufferLength) + termOffset
```

### 7.3 Term 轮转

```
三分区轮转（防止写入覆盖未消费数据）：

activeTermIndex 0 → 1 → 2 → 0 → 1 → ...

         termId=0        termId=1        termId=2
        ┌──────────┐    ┌──────────┐    ┌──────────┐
Term 0  │ 写满了    │    │          │    │          │
Term 1  │          │    │ 正在写入  │    │          │
Term 2  │          │    │          │    │ 被消费中  │
        └──────────┘    └──────────┘    └──────────┘

当 Term 1 写满时（tail 到达 termBufferLength）：
  - rawTail 高32位 termId 递增 → activeTermIndex 变为 2
  - 同时在尾部写 PADDING 帧补齐（让 Sender 可以连续扫描）
```

### 7.4 offer 返回值

| 返回值 | 含义 | 处理建议 |
|--------|------|---------|
| `> 0` | 成功，值为消息的新 position | 可选：记录用于流控检查 |
| `BACK_PRESSURED (-2)` | 接收端消费慢，发送窗口满 | 等待/重试 |
| `NOT_CONNECTED (-1)` | 无活跃订阅者 | 等待订阅者连接 |
| `ADMIN_ACTION (-3)` | Term 轮转等内部操作，无副作用 | 立即重试 |
| `CLOSED (-4)` | Publication 已关闭 | 停止使用 |

---

## 8. subscription.poll 深度解析

### 8.1 核心路径

```
subscription.poll(handler, fragmentLimit)
  │
  ├─ ① volatile read images[]
  │       ← 对 ClientConductor 添加 Image 的 volatile write 可见
  │
  ├─ ② Round-Robin 轮询（公平调度多个 Publisher）
  │   startingIndex = roundRobinIndex++
  │   for (i : images)
  │     fragmentsRead += images[i].poll(handler, remainingLimit)
  │
  └─ image.poll(handler, limit):
       │
       ├─ ③ 读 subscriberPosition（当前消费到哪里）
       │   termOffset = (int)position & (termBufferLength - 1)
       │   termIndex = activeTermIndex(position)
       │   termBuffer = logBuffers[termIndex]
       │
       ├─ ④ 帧扫描循环
       │   while fragmentsRead < limit:
       │     │
       │     ├─ frameLengthVolatile(termBuffer, termOffset)
       │     │       ← acquire 语义 volatile read
       │     │       ← 与 offer 的 frameLengthOrdered(release) 形成 happens-before
       │     │
       │     ├─ frameLength <= 0 → break（帧未完成，等待）
       │     │
       │     ├─ 跳过 PADDING 帧（Term 尾部填充）
       │     │
       │     └─ handler.onFragment(termBuffer, dataOffset, dataLength, header)
       │             ← 直接传递 mmap buffer 引用，零拷贝
       │             ← 应用层在回调中处理数据
       │
       └─ ⑤ 推进 subscriberPosition
               subscriberPosition.setRelease(position)
               ← 这个 Counter 被 Driver 读取，用于计算发送窗口
```

### 8.2 offer/poll 的生产者-消费者同步

```
offer（生产者）                            poll（消费者）
─────────────                             ─────────────
写帧头（负帧长）                          ↓
putBytes(payload)                         ↓
frameLengthOrdered(+frameLen)             ↓
← putIntRelease（store/release barrier）  ↓
                                          frameLengthVolatile(termBuffer, offset)
                                          ← getIntVolatile（load/acquire barrier）
                                          帧长 > 0 → 所有帧数据可见（happens-before）
```

---

## 9. UDP 可靠传输协议

### 9.1 帧类型体系

```
Aeron 协议帧类型：
┌──────────┬────────────────────────────────────────────────────┐
│  DATA    │ 数据帧，携带应用消息（1B payload~MTU）               │
│  PADDING │ 填充帧，Term 末尾对齐用                             │
│  SETUP   │ 建连帧，Publisher 发出，通知 Subscriber 流参数      │
│  SM      │ Status Message，Subscriber→Publisher 流控窗口通告  │
│  NAK     │ 否定确认，Subscriber 发现丢包时发送重传请求         │
│  RTT     │ RTT 测量帧，用于拥塞控制计算往返延迟               │
└──────────┴────────────────────────────────────────────────────┘

帧头公共部分（8 字节）：
┌─────────────────────────────────┬──────────────┬──────────────┐
│         Frame Length            │   Flags      │     Type     │
│            4B                   │    1B        │     1B+      │
└─────────────────────────────────┴──────────────┴──────────────┘
```

### 9.2 SETUP 握手（连接建立）

```
Publisher 进程                          Subscriber 进程
────────────                           ────────────────
Sender.doWork()
  │ 定期发送 SETUP 帧
  │ (sessionId, streamId, initialTermId, termBufferLength, mtu...)
  └─────── SETUP ──────────────────▶
                                        Receiver 接收到 SETUP
                                        createPublicationImage()
                                         ← mmap LogBuffer 文件
                                         ← 创建 PublicationImage
                                         ← 注册到 DataPacketDispatcher
                                        发送 SM（开放接收窗口）
  ◀─────── SM ──────────────────────
  收到 SM 后 Sender 开始发送 DATA 帧
  ──────── DATA ───────────────────▶
```

### 9.3 NAK 重传机制（丢包恢复）

```
正常路径（无 NAK 开销）：
Publisher → DATA(1) DATA(2) DATA(3) DATA(4) → Subscriber

丢包场景：
Publisher → DATA(1) DATA(2) ⊗丢失 DATA(4) → Subscriber
                                               检测到间隙: position 2 之后有 gap
                                               ← NAK(termId, termOffset=2, len)
Publisher 收到 NAK → 重传 DATA(3)
Publisher → DATA(3)(重传) → Subscriber ✓ 恢复
```

**NAK 触发机制（LossDetector）：**
```
Receiver.doWork()
  └─ checkForGap(image, nowNs)
       └─ termGapScanner.scan(termBuffer, rebuildPosition)
            ← 从 rebuildPosition 扫描，找第一个 frameLength=0 的位置
            ← gap 时长 > NAK_UNICAST_DELAY (默认 60μs)
            → NAK 发送

NAK 去重（RetransmitHandler）：
  ← 收到 NAK 后设置定时器
  ← 定时器内收到重复 NAK → 抑制（避免 NAK 风暴）
  ← 超时后发送重传
```

### 9.4 SM 流控（端到端背压）

```
Subscriber 端                                Publisher 端
─────────────                               ─────────────
Image.poll() 推进 subscriberPosition        Sender 读取 receiverWindowSize
                 │                          计算 senderLimit = subscriberPosition + windowSize
                 │
Receiver.doWork()                           DriverConductor 更新 publicationLimit
  发送 SM {                                  = min(senderLimit, flowControlLimit)
    consumedPosition = subscriberPosition        │
    receiverWindowSize = 256KB                   │
  }                                              ▼
  ──────── SM ──────────────────▶         publication.offer() 检查 positionLimit
                                           position >= limit → BACK_PRESSURED ← 背压生效
```

### 9.5 拥塞控制

```
可插拔接口：CongestionControl

默认实现：
┌─────────────────────────────────────────────────────┐
│  StaticWindowCongestionControl                      │
│  - 固定窗口大小（默认 256KB）                        │
│  - 简单高效，适合局域网                              │
└─────────────────────────────────────────────────────┘

高级实现（可选）：
┌─────────────────────────────────────────────────────┐
│  CubicCongestionControl（类 TCP CUBIC）              │
│  - 基于 RTT 动态调整窗口                             │
│  - 适合广域网/高延迟链路                             │
└─────────────────────────────────────────────────────┘
```

---

## 10. 流控闭环：端到端背压

```
完整流控闭环：

应用层 Subscriber                                     应用层 Publisher
┌──────────────────┐                                 ┌──────────────────┐
│ poll(handler)    │                                 │ offer(buffer)    │
│ 推进              │                                 │ 检查 positionLimit│
│ subscriberPosition│                                 └─────────┬────────┘
└────────┬─────────┘                                           │ BACK_PRESSURED
         │ Counter (CnC mmap)                                  │
         ▼                                                     │
┌──────────────────┐                                           │
│ Receiver Agent   │                                           │
│ 周期发送 SM      │                                           │
│ (consumedPos,    │                                           │
│  windowSize)     │                                           │
└────────┬─────────┘                                           │
         │ UDP SM 帧                                           │
         ▼                                                     │
┌──────────────────┐                                           │
│  Sender Agent   │                                           │
│  收到 SM 更新    │                                           │
│  senderPosition ─┼───── 更新 senderLimit ──────────────────▶│
└──────────────────┘      (通过 Counter mmap)                  │
                                                               │
┌──────────────────┐                                           │
│ DriverConductor  │                                           │
│ 计算 pubLimit    ├─── publicationLimit Counter ─────────────▶│
│ = min(sender,    │    (CnC mmap 直接读取)
│   flowControl)   │
└──────────────────┘

关键：整个流控链路通过 Counter（mmap 共享内存）实现，无任何锁。
```

---

## 11. 关键设计总结

### 11.1 零拷贝路径

```
offer 写入路径：
  应用 buffer → putBytes → Term Buffer (mmap)
               [唯一的一次拷贝，从应用 buffer 到 mmap]

send 发送路径：
  Term Buffer (mmap) → ByteBuffer.slice() → DatagramChannel.write()
               [零拷贝：slice 只是视图，不拷贝数据]

recv 接收路径：
  UDP recv → byteBuffer (临时) → PublicationImage.insertPacket()
           → copyBytes → Term Buffer (mmap)
               [一次拷贝：从内核 UDP buffer 到 mmap]

poll 读取路径：
  Term Buffer (mmap) → handler 回调（直接传 mmap buffer 引用）
               [零拷贝：应用直接访问 mmap 内存]
```

### 11.2 无锁并发保证

| 操作 | 机制 | 为什么无锁 |
|------|------|---------|
| 多线程 offer | `getAndAddLong`（原子 fetch-and-add） | 原子操作，无 CAS 重试 |
| offer/poll 同步 | Release/Acquire volatile | 帧长作为"发布信号" |
| Client/Driver 命令 | ManyToOneRingBuffer CAS | 无锁环形队列 |
| 流控 Counter | Agrona AtomicCounter | 原子 long 读写 |

### 11.3 延迟来源分析

```
延迟分解（局域网 UDP 场景，P50）：

应用 offer         ~10ns    CAS + 内存写入
Sender 轮询间隔    ~1μs     IdleStrategy 决定（BusySpin=0，BackOff=~1μs）
UDP 发送           ~500ns   系统调用 sendmsg
网络传输           ~1～5μs  硬件延迟
Receiver 轮询      ~1μs     selectNow 忙轮询
写入 LogBuffer     ~200ns   内存拷贝
应用 poll 读取     ~10ns    volatile read + 回调

总计 P50 ≈ 5～10μs（局域网），< 1μs（同机 IPC）
```

### 11.4 Busy-Polling 的代价与收益

```
Receiver 使用 selectNow()（非阻塞）而非 select()（阻塞）：

代价：  Receiver 线程 100% CPU 占用（专用核）
收益：  消除线程唤醒延迟（OS 调度延迟 ~10～50μs）
        UDP 包到达后立即被处理，不等待 OS 通知

这是 Aeron 实现微秒级延迟的关键之一：
  "用 CPU 换延迟"
```

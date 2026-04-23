# Aeron SBE 使用与高性能设计深度分析

## 一、SBE（Simple Binary Encoding）在 Aeron 中的使用

### 1.1 什么是 SBE

SBE 是 FIX 协议组织推出的一种高性能二进制编码标准，特点是：
- **零拷贝读写**：直接在 buffer 上读写字段，无需序列化/反序列化
- **无内存分配**：Encoder/Decoder 是 Flyweight 模式，wrap 到 buffer 上即可使用
- **Schema 驱动**：通过 XML schema 生成代码，保证编解码一致性
- **固定开销**：MessageHeader（8字节）+ 固定字段直接按偏移读写

### 1.2 SBE 在 Aeron 模块中的使用分布

```
┌─────────────────────────────────────────────────────────┐
│                    Aeron 项目架构                         │
├──────────────────┬──────────────────────────────────────┤
│     模块          │   二进制编码方式                       │
├──────────────────┼──────────────────────────────────────┤
│  aeron-client    │  Flyweight（手写零拷贝）               │
│  aeron-driver    │  Flyweight（手写零拷贝）               │
│  aeron-archive   │  SBE（Schema 生成 Codec）             │
│  aeron-cluster   │  SBE（Schema 生成 Codec）             │
└──────────────────┴──────────────────────────────────────┘
```

**关键发现：Aeron 核心传输层（client + driver）并不使用 SBE，而是使用手写的 Flyweight 模式！**

SBE 仅用在 **Archive（归档）** 和 **Cluster（集群）** 这两个上层模块中。

### 1.3 为什么核心传输层不用 SBE？

Aeron 的核心传输协议帧（DATA、SM、NAK、SETUP 等）结构极其简单固定，手写 Flyweight 比 SBE 生成的 Codec 更轻量：

```
DATA 帧头（32 字节）——手写 Flyweight 直接定义偏移常量：

  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        Frame Length                           |
 +---------------------------------------------------------------+
 |  Version    |B E S R Flags  |           Type (DATA=0x01)      |
 +---------------------------------------------------------------+
 |                          Term Offset                          |
 +---------------------------------------------------------------+
 |                          Session ID                           |
 +---------------------------------------------------------------+
 |                          Stream ID                            |
 +---------------------------------------------------------------+
 |                          Term ID                              |
 +---------------------------------------------------------------+
 |                       Reserved Value                          |
 |                                                               |
 +---------------------------------------------------------------+
 |                         Data Payload ...                      |
```

对比 SBE 和 Flyweight 的代码风格：

**Flyweight 方式（核心传输层使用）：**
```java
// DataHeaderFlyweight 继承 UnsafeBuffer，直接按偏移读写
public class DataHeaderFlyweight extends HeaderFlyweight {
    public static final int TERM_OFFSET_FIELD_OFFSET = 8;
    public static final int SESSION_ID_FIELD_OFFSET = 12;
    
    public int termOffset() {
        return getInt(TERM_OFFSET_FIELD_OFFSET, LITTLE_ENDIAN); // 一条指令
    }
}
```

**SBE 方式（Archive/Cluster 使用）：**
```java
// SBE 生成的 Codec 也是零拷贝，但有 MessageHeader + 更多抽象
RecordingDescriptorDecoder decoder = new RecordingDescriptorDecoder();
decoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
long recordingId = decoder.recordingId();
```

两者都是零拷贝，但 Flyweight 更极致——直接继承 `UnsafeBuffer`，一个方法调用就完成字段读取。

### 1.4 SBE Schema 文件位置

| Schema 文件 | 模块 | 用途 |
|-------------|------|------|
| `aeron-archive/src/main/resources/archive/aeron-archive-codecs.xml` | Archive | 录制/回放/控制协议 |
| `aeron-archive/src/main/resources/archive/aeron-archive-mark-codecs.xml` | Archive | Mark 文件元数据 |
| `aeron-cluster/src/main/resources/cluster/aeron-cluster-codecs.xml` | Cluster | 集群一致性协议 |
| `aeron-cluster/src/main/resources/cluster/aeron-cluster-mark-codecs.xml` | Cluster | 集群 Mark 文件 |
| `aeron-cluster/src/main/resources/cluster/aeron-cluster-node-state-codecs.xml` | Cluster | 节点状态 |

构建时由 Gradle 调用 `uk.co.real_logic.sbe.SbeTool`（版本 1.37.1）生成 Java Codec 类。

### 1.5 Flyweight 类完整列表

**协议层（`io.aeron.protocol`）—— UDP 传输帧：**

| Flyweight 类 | 帧类型 | 大小 |
|--------------|--------|------|
| `HeaderFlyweight` | 基类 | 8B（公共头） |
| `DataHeaderFlyweight` | DATA/PADDING | 32B |
| `SetupFlyweight` | SETUP | 40B |
| `StatusMessageFlyweight` | SM | 36B |
| `NakFlyweight` | NAK | 28B |
| `ErrorFlyweight` | ERROR | 可变 |
| `RttMeasurementFlyweight` | RTTM | 40B |
| `ResolutionEntryFlyweight` | RES | 可变 |
| `ResponseSetupFlyweight` | Response SETUP | - |

**命令层（`io.aeron.command`）—— Client ↔ Driver IPC：**

超过 25 个 Flyweight 类，用于 RingBuffer 中 Client 与 Driver 之间的命令编码，包括 `PublicationMessageFlyweight`、`SubscriptionMessageFlyweight`、`ImageBuffersReadyFlyweight` 等。

---

## 二、Aeron 高性能设计全面分析

Aeron 的极致性能来自**系统级的全栈优化**，不是单一技术点，而是 10+ 种高性能模式的协同。

### 2.1 零拷贝（Zero-Copy）—— 数据不经过任何中间缓冲区

这是 Aeron 性能的**最核心设计**。从应用写入到 UDP 发出，数据只存在一个地方——mmap 的 Term Buffer：

```
应用线程 offer()                    Sender 线程
     │                                    │
     ▼                                    ▼
 ┌──────────────────────────────────────────┐
 │      Term Buffer（mmap 内存映射文件）      │
 │                                          │
 │  ┌──────┬──────┬──────┬──────┬──────┐   │
 │  │Frame1│Frame2│Frame3│Frame4│Frame5│   │
 │  └──────┴──────┴──────┴──────┴──────┘   │
 │     ▲                    ▲              │
 │     │                    │              │
 │  offer() 直接写入     sendData() 直接读取  │
 │  (UnsafeBuffer.putBytes) (ByteBuffer.slice)│
 └──────────────────────────────────────────┘
                    │
                    ▼ DatagramChannel.write(sendBuffer)
              ┌───────────┐
              │  UDP 发送   │  sendBuffer 就是 mmap 的 slice
              │  零拷贝！   │  没有任何 memcpy
              └───────────┘
```

**关键代码路径：**

1. **应用写入**（`ConcurrentPublication.appendUnfragmentedMessage`）：
   ```java
   // 直接写入 mmap buffer，无中间缓冲区
   termBuffer.putBytes(frameOffset + HEADER_LENGTH, srcBuffer, srcOffset, length);
   ```

2. **网络发送**（`NetworkPublication.sendData`）：
   ```java
   // sendBuffer 是 mmap 的 ByteBuffer slice，直接传给内核
   sendBuffer.limit(termOffset + available).position(termOffset);
   sendDatagramChannel.write(sendBuffer);  // → syscall sendmsg
   ```

**从应用写入到 UDP 发出，数据没有经过任何 `memcpy`！**

### 2.2 内存映射文件（Memory-Mapped File）—— IPC 零拷贝共享

Aeron 的 LogBuffer 使用 mmap 实现应用进程与 Driver 进程之间的**零拷贝 IPC**：

```
应用进程                          Driver 进程
┌────────────┐               ┌────────────────┐
│ offer() 写入│               │ Sender 读取发送  │
│    ↓       │               │     ↓          │
│ UnsafeBuffer│               │ UnsafeBuffer    │
│    ↓       │               │     ↓          │
│ MappedByte │               │ MappedByte      │
│   Buffer   │               │   Buffer        │
└─────┬──────┘               └──────┬─────────┘
      │                             │
      └──────── 同一个文件 ──────────┘
           /dev/shm/aeron-xxx/publications/xxx.logbuffer
                    │
              ┌─────────────┐
              │ 物理内存页    │  操作系统页缓存
              │ (Page Cache) │  两个进程共享同一块物理内存
              └─────────────┘
```

**MappedRawLog 的文件布局：**
```
偏移 0:                    Term Buffer 0  (默认 16MB)
偏移 termLength:           Term Buffer 1  (默认 16MB)
偏移 2 × termLength:       Term Buffer 2  (默认 16MB)
偏移 3 × termLength:       Log Metadata   (含 rawTail、activeTermCount 等)
总大小 = 3 × 16MB + metadata ≈ 48MB
```

### 2.3 无锁并发（Lock-Free）—— 核心路径零锁

Aeron 在**整个数据路径上没有任何 mutex/synchronized**，全部使用原子操作：

#### 2.3.1 多生产者并发写入（ConcurrentPublication）

```java
// getAndAddLong 原子分配空间（一条 CPU 指令：lock xadd）
final long rawTail = metaDataBuffer.getAndAddLong(
    tailCounterOffset, alignedLength);
// 每个线程拿到不同的偏移量，可以并行写入不重叠的区域
```

#### 2.3.2 两阶段提交（帧可见性控制）

```java
// 阶段 1：写帧头时设置负长度（占位，对读者不可见）
termBuffer.putIntOrdered(frameOffset, -frameLength);

// 阶段 2：写完数据后设置正长度（release 语义，对读者可见）
frameLengthOrdered(termBuffer, frameOffset, frameLength);
```

#### 2.3.3 跨线程无锁通信（SeqLock 变体）

`PublicationImage` 在 Conductor 和 Receiver 线程间通信使用 VarHandle 实现 SeqLock：

```java
// Conductor 写入（生产者）
BEGIN_LOSS_CHANGE_VH.setRelease(this, changeNumber);
VarHandle.storeStoreFence();
lossTermId = termId;          // 写入丢包信息
lossTermOffset = termOffset;
lossLength = length;
END_LOSS_CHANGE_VH.setRelease(this, changeNumber);

// Receiver 读取（消费者）
final long changeNumber = END_LOSS_CHANGE_VH.getAcquire(this);
// ... 读取数据 ...
VarHandle.loadLoadFence();
if (changeNumber == BEGIN_LOSS_CHANGE_VH.getAcquire(this)) {
    // 数据一致，可以使用
}
```

### 2.4 缓存行填充（Cache Line Padding）—— 消除 False Sharing

现代 CPU 缓存以 64 字节为一个 cache line。当两个线程频繁读写**同一 cache line 中的不同字段**时，会导致 cache line 在 CPU 核心之间频繁弹跳（false sharing），严重影响性能。

Aeron 对所有跨线程访问的热点类都使用了 padding：

```java
// 128 字节 padding（左侧）
class SenderLhsPadding {
    byte p000, p001, ..., p063;  // 64 字节
}

// 热点字段
class SenderHotFields extends SenderLhsPadding {
    long controlPollDeadlineNs;
    int dutyCycleCounter;
    int roundRobinIndex;
}

// 128 字节 padding（右侧）
class SenderRhsPadding extends SenderHotFields {
    byte p064, p065, ..., p127;  // 64 字节
}

// Sender 继承自 RhsPadding，热点字段被前后 padding 包裹
public final class Sender extends SenderRhsPadding { ... }
```

**使用 padding 的类（10+个）：**

| 类 | 隔离的线程 |
|-----|-----------|
| `Sender` | Sender 线程的 duty cycle 计数器 |
| `NetworkPublication` | Conductor vs Sender 字段 |
| `PublicationImage` | Conductor vs Receiver 字段 |
| `SendChannelEndpoint` | 热点字段隔离 |
| `ReceiveChannelEndpoint` | 热点字段隔离 |
| `ImageConnection` | 时间戳字段 |
| `Subscription` | 订阅状态 |
| `AbstractMinMulticastFlowControl` | 流控状态 |
| `DutyCycleTracker` | 工作周期追踪 |
| `Destination`（MDC） | 目的地活性 |

### 2.5 专用线程 + Busy-Spin —— 最低延迟调度

```java
// LowLatencyMediaDriver.java —— 极致低延迟配置
final MediaDriver.Context ctx = new MediaDriver.Context()
    .threadingMode(ThreadingMode.DEDICATED)          // 3 个专用线程
    .conductorIdleStrategy(BusySpinIdleStrategy.INSTANCE)  // Conductor: busy-spin
    .receiverIdleStrategy(NoOpIdleStrategy.INSTANCE)       // Receiver: 完全不让出 CPU
    .senderIdleStrategy(NoOpIdleStrategy.INSTANCE);        // Sender: 完全不让出 CPU
```

**DEDICATED 模式的三个线程：**

| 线程 | 职责 | 建议空闲策略 |
|------|------|-------------|
| Conductor | 管理命令、流控更新、丢包检测 | BusySpinIdleStrategy |
| Sender | 从 LogBuffer 读取数据 → UDP 发送 | NoOpIdleStrategy |
| Receiver | UDP 接收 → 写入 LogBuffer | NoOpIdleStrategy |

`NoOpIdleStrategy` 的 idle() 方法是空实现——线程永远不 sleep，不 yield，不 park。在有足够 CPU 核心时，这消除了所有线程调度延迟。

### 2.6 预分配缓冲区 —— 零运行时内存分配

Aeron 在初始化时预分配所有控制帧 buffer，运行时**零 GC 压力**：

```java
// NetworkPublicationThreadLocals —— 预分配心跳、SETUP、RTT buffer
final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
    CACHE_LINE_LENGTH * 4,     // 256 字节
    CACHE_LINE_LENGTH);        // 64 字节对齐

heartbeatBuffer = byteBuffer.slice(...);  // 复用同一块内存
setupBuffer = byteBuffer.slice(...);
rttMeasurementBuffer = byteBuffer.slice(...);
```

```java
// ReceiveChannelEndpointThreadLocals —— 预分配 SM、NAK、RTT buffer
smBuffer = byteBuffer.slice(...);
nakBuffer = byteBuffer.slice(...);
rttMeasurementBuffer = byteBuffer.slice(...);
```

### 2.7 ManyToOneRingBuffer —— 高效 IPC 命令通道

Client 与 Driver 之间的命令通过 CnC 文件中的 `ManyToOneRingBuffer` 传递：

```
Client 进程                                    Driver 进程
┌─────────────┐                            ┌─────────────────┐
│DriverProxy  │   RingBuffer (mmap)        │ClientCommandAdapter│
│  addPub()   │ ─────────────────────────→ │  onMessage()      │
│  addSub()   │   toDriverCommands         │  dispatch()       │
│  removePub()│                            │                   │
└─────────────┘                            └─────────────────┘
                                                   │
                  RingBuffer (mmap)                 │
              ←──────────────────────────   toClientsBuffer
                  toClientCommands         （响应/错误/通知）
```

RingBuffer 特点：
- 基于 mmap 共享内存，跨进程零拷贝
- 多生产者单消费者（ManyToOne），使用 CAS 实现无锁
- 消息用 Flyweight 编码，无序列化开销

### 2.8 批处理发送 —— 小消息合并

`TermScanner.scanForAvailability()` 可以将多个小帧合并到一个 UDP 包中发送：

```
Term Buffer 中的帧：
┌──────┬──────┬──────┬──────┐
│64B帧 │64B帧 │128B帧│64B帧 │  四个帧共 320 字节
└──────┴──────┴──────┴──────┘
         │
    scanForAvailability(offset, mtuLength=1408)
         │
         ▼
   available = 320  →  一个 UDP 包发出全部 4 个帧

而不是 4 次 UDP 发送！
```

### 2.9 Connected UDP Socket —— 减少内核开销

```java
// SendChannelEndpoint.send()
sendDatagramChannel.write(buffer);  // connected 模式
// 而不是
sendDatagramChannel.send(buffer, address);  // unconnected 模式
```

connected 模式下，内核不需要每次查路由表和 ARP 表，减少 ~30% 的 syscall 开销。

### 2.10 单写者原则（Single Writer Principle）

Aeron 严格控制每个字段只被一个线程写入：

| 字段 | 唯一写入线程 | 读取线程 |
|------|-------------|----------|
| `rawTail` | 应用线程（via CAS） | Sender |
| `senderPosition` | Sender | Conductor |
| `senderLimit` | Sender（onStatusMessage） | 应用线程（offer） |
| `publisherLimit` | Conductor | 应用线程（offer） |
| `hwmPosition` | Receiver | Conductor |
| `rebuildPosition` | Conductor | Receiver |

这避免了写-写冲突，配合 `setRelease` / `getAcquire` 语义保证可见性。

---

## 三、性能对比总结

### Aeron vs 传统方案

| 技术点 | 传统 TCP/消息队列 | Aeron |
|--------|------------------|-------|
| 序列化 | JSON/Protobuf/对象序列化 | 零拷贝 Flyweight（1 条指令读字段） |
| 数据拷贝 | 应用→序列化缓冲→内核→网卡（3~4次） | 应用→mmap→内核→网卡（0 次 memcpy） |
| IPC | Socket/Pipe（内核参与） | mmap 共享内存（纯用户态） |
| 并发控制 | synchronized/Lock | 无锁 CAS + 原子操作 |
| 内存分配 | 每消息分配对象 | 预分配 + 零运行时分配 |
| 线程调度 | Thread.sleep/wait/notify | Busy-spin/NoOp 空闲策略 |
| 流控 | TCP 窗口（内核控制） | 应用层流控（Position + Limit） |
| 可靠性 | TCP ACK（累积确认） | NAK（否定确认，只重传丢失的） |
| False Sharing | 无防护 | 128~256 字节 Cache Line Padding |

### Aeron 典型延迟数据

| 场景 | 延迟 |
|------|------|
| IPC（同机进程间） | < 200 纳秒（p99） |
| 单播 UDP（数据中心内） | < 10 微秒（p99） |
| 吞吐量 | 数百万消息/秒（单流） |

---

## 四、一句话总结

> **Aeron 的极致性能不是来自某一项黑科技，而是在数据路径的每一个环节——编码、内存、拷贝、锁、线程调度、内核交互——都做到了最优选择，且这些优化彼此协同，形成了一个全栈零开销的消息传输系统。**

核心传输层使用手写 Flyweight（比 SBE 更轻量），SBE 仅用在 Archive/Cluster 等控制面协议上。Flyweight 和 SBE 本质思想相同——都是在 buffer 上按偏移直接读写的零拷贝编码，区别在于 Flyweight 是手写的极致简约版，SBE 是 Schema 驱动的通用版。

# Aeron UDP 传输协议与可靠性机制深度解析

## 目录

- [概述](#概述)
- [一、消息如何序列化发送](#一消息如何序列化发送)
  - [1.1 从 offer 到 UDP 数据报的完整路径](#11-从-offer-到-udp-数据报的完整路径)
  - [1.2 Data Frame — 消息的传输格式](#12-data-frame--消息的传输格式)
  - [1.3 一个 UDP 包 = 一帧还是多帧？](#13-一个-udp-包--一帧还是多帧)
  - [1.4 MTU 与分片策略](#14-mtu-与分片策略)
  - [1.5 零拷贝发送路径](#15-零拷贝发送路径)
- [二、Aeron 传输协议 — 所有帧类型](#二aeron-传输协议--所有帧类型)
  - [2.1 协议帧类型总览](#21-协议帧类型总览)
  - [2.2 公共帧头（8 字节）](#22-公共帧头8-字节)
  - [2.3 DATA 帧（数据帧）](#23-data-帧数据帧)
  - [2.4 SETUP 帧（流建立帧）](#24-setup-帧流建立帧)
  - [2.5 Status Message（SM，状态消息帧）](#25-status-messagesm状态消息帧)
  - [2.6 NAK 帧（否定确认帧）](#26-nak-帧否定确认帧)
  - [2.7 RTT Measurement 帧](#27-rtt-measurement-帧)
  - [2.8 PADDING 帧](#28-padding-帧)
- [三、UDP 的原生问题与 Aeron 的解决方案](#三udp-的原生问题与-aeron-的解决方案)
  - [3.1 问题总览：TCP vs UDP vs Aeron](#31-问题总览tcp-vs-udp-vs-aeron)
  - [3.2 丢包 — NAK-based 重传机制](#32-丢包--nak-based-重传机制)
  - [3.3 乱序 — 基于 Position 的有序交付](#33-乱序--基于-position-的有序交付)
  - [3.4 流控 — Position/Limit 端到端背压](#34-流控--positionlimit-端到端背压)
  - [3.5 拥塞控制 — 可插拔 CongestionControl](#35-拥塞控制--可插拔-congestioncontrol)
  - [3.6 连接管理 — SETUP/SM 握手](#36-连接管理--setupsm-握手)
  - [3.7 心跳与活性检测](#37-心跳与活性检测)
  - [3.8 消息边界保留](#38-消息边界保留)
- [四、端到端可靠传输全流程](#四端到端可靠传输全流程)
  - [4.1 正常数据传输流程](#41-正常数据传输流程)
  - [4.2 丢包检测与恢复流程](#42-丢包检测与恢复流程)
  - [4.3 流控反压流程](#43-流控反压流程)
- [五、Aeron vs TCP 设计哲学对比](#五aeron-vs-tcp-设计哲学对比)
- [流程总结](#流程总结)

---

## 概述

UDP 协议本身是**不可靠、无序、无流控、无连接**的。Aeron 选择 UDP 作为底层传输，是因为 UDP 提供了最大的灵活性——没有内核态的 TCP 拥塞控制、没有 head-of-line blocking、支持多播。但 Aeron 在用户态实现了一套完整的可靠传输协议，解决了 UDP 的所有原生缺陷。

**核心设计理念：**

| 设计选择 | 原因 |
|----------|------|
| **NAK-based（而非 ACK-based）** | 正常路径零控制消息开销；只在丢包时才产生 NAK |
| **接收端驱动流控** | 通过 Status Message 通告窗口，发送端不超过接收端的消费能力 |
| **用户态协议栈** | 绕过内核 TCP 栈，避免系统调用开销和内核锁竞争 |
| **共享内存 + mmap** | 应用线程直写 Term Buffer → Sender 直读 → UDP 发送，零拷贝 |
| **多播原生支持** | 一份数据，多个订阅者，NAK/SM 机制天然适配 |

---

## 一、消息如何序列化发送

### 1.1 从 offer 到 UDP 数据报的完整路径

```
┌──────────────┐
│  应用线程     │  publication.offer(buffer, 0, length)
│  (Client)    │
└──────┬───────┘
       │  ① 写入 Term Buffer（mmap 共享内存）
       │  帧格式：[32B Header][Payload]
       ▼
┌──────────────────────────────────────────────────────────────┐
│                     Term Buffer (mmap)                        │
│  ┌────────┬────────┬────────┬────────┬─────────────────────┐ │
│  │Frame 1 │Frame 2 │Frame 3 │Frame 4 │    ...空闲区域...    │ │
│  │32+data │32+data │32+data │32+data │                     │ │
│  └────────┴────────┴────────┴────────┴─────────────────────┘ │
└──────────────────────────────┬───────────────────────────────┘
                               │
       ② Sender 线程扫描已完成帧（frameLengthVolatile > 0）
                               │
┌──────────────────────────────▼───────────────────────────────┐
│  Sender.doSend(nowNs)                                         │
│    └─ NetworkPublication.send(nowNs)                          │
│         └─ sendData(nowNs, senderPosition, termOffset)       │
│              │                                                │
│              │  ③ TermScanner.scanForAvailability()           │
│              │     扫描 [termOffset, termOffset+mtu) 内的     │
│              │     连续已完成帧，累加 available 长度           │
│              │                                                │
│              │  ④ sendBuffer.position(termOffset)             │
│              │            .limit(termOffset + available)      │
│              │     切片 ByteBuffer（与 Term Buffer 同一内存）  │
│              │                                                │
│              └─ doSend(sendBuffer)                            │
│                   └─ SendChannelEndpoint.send(buffer)         │
│                        ├─ sendHook(buffer, address)           │
│                        └─ sendDatagramChannel.write(buffer)   │
│                             │                                 │
│                        ⑤ Java NIO DatagramChannel.write()    │
│                           → OS sendto() 系统调用              │
│                           → UDP 数据报发送到网络              │
└──────────────────────────────────────────────────────────────┘
```

**关键源码位置：**

| 步骤 | 类 | 方法 | 文件路径 |
|------|-----|------|----------|
| ② | Sender | doSend() | `aeron-driver/.../Sender.java:284` |
| ② | NetworkPublication | send() | `aeron-driver/.../NetworkPublication.java:451` |
| ③ | TermScanner | scanForAvailability() | `aeron-client/.../logbuffer/TermScanner.java:42` |
| ④ | NetworkPublication | sendData() | `aeron-driver/.../NetworkPublication.java:576` |
| ⑤ | SendChannelEndpoint | send() | `aeron-driver/.../media/SendChannelEndpoint.java:264` |
| ⑤ | UdpChannelTransport | sendHook() | `aeron-driver/.../media/UdpChannelTransport.java:298` |

### 1.2 Data Frame — 消息的传输格式

Aeron **不做额外的序列化**。消息在 Term Buffer 中以 **Data Frame** 格式存储，这个格式**同时也是网络传输格式**——Term Buffer 中的字节直接作为 UDP 负载发出去，零拷贝。

```
一个 Data Frame（也是 UDP 负载的一部分）：
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                         Frame Length                          |  0
 +-------+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 |Version|B|E|S|R|    Flags      |           Type (DATA=0x01)    |  4
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 |                         Term Offset                           |  8
 +---------------------------------------------------------------+
 |                         Session ID                            | 12
 +---------------------------------------------------------------+
 |                         Stream ID                             | 16
 +---------------------------------------------------------------+
 |                         Term ID                               | 20
 +---------------------------------------------------------------+
 |                         Reserved Value                        | 24
 |                                                               |
 +---------------------------------------------------------------+
 |                         Payload（应用消息原始字节）              | 32
 |                         ...                                   |
 +---------------------------------------------------------------+
```

**Aeron 不关心 Payload 的内容**。它就是一段 `byte[]`，Aeron 原封不动地搬运。序列化/反序列化完全由应用层负责（可用 SBE、Protobuf、JSON 等任何方式）。

### 1.3 一个 UDP 包 = 一帧还是多帧？

**一个 UDP 数据报可以包含多个 Data Frame（批处理发送）。**

`TermScanner.scanForAvailability()` 的扫描逻辑：

```java
// TermScanner.java:42-76（简化）
do {
    final int frameLength = frameLengthVolatile(termBuffer, offset + available);
    if (frameLength <= 0) break;              // 未完成帧，停止
    
    if (isPaddingFrame(termBuffer, offset)) { // PADDING 帧
        padding = alignedFrameLength;
        break;
    }
    
    available += alignedFrameLength;
    if (available > scanLimit) {              // 超过 MTU 限制
        available -= alignedFrameLength;       // 退回最后一帧
        break;
    }
} while (available < scanLimit);
```

**结果：**
- 多个小帧 → 打包进一个 UDP 包（不超过 MTU）
- 单个大帧（已被 offer 分片为 ≤ maxPayloadLength）→ 一帧一包
- 一个 UDP 包最多 MTU 字节

```
UDP 数据报内容示例（MTU=1408）：
┌──────────────────────────────────────────────────────────────┐
│ [Frame1: 32B hdr + 100B payload][Frame2: 32B hdr + 200B]... │
│              总长 ≤ 1408 字节 (MTU)                          │
└──────────────────────────────────────────────────────────────┘
```

### 1.4 MTU 与分片策略

**两层分片控制：**

| 层级 | 位置 | 机制 |
|------|------|------|
| **应用层分片** | `Publication.offer()` | 消息 > `maxPayloadLength`(MTU-32) 时，拆成多个 Frame |
| **网络层打包** | `NetworkPublication.sendData()` | 多个 Frame 打包进一个 UDP 包，不超过 `mtuLength` |

```
应用消息 (5000 bytes)                    网络传输
       │                                    │
       │ offer() 自动分片                    │
       ▼                                    ▼
  Frame 1 [32+1376B]  ──────────────► UDP 包 1 (1408B)
  Frame 2 [32+1376B]  ──────────────► UDP 包 2 (1408B)
  Frame 3 [32+1376B]  ──────────────► UDP 包 3 (1408B)
  Frame 4 [32+ 872B]  ──────────────► UDP 包 4 (904B)
```

**MTU 默认值与校验：**
- 默认 `MTU_LENGTH_DEFAULT = 1408`（以太网 MTU 1500 - IP头 20 - UDP头 8 = 1472，再留余量）
- 必须 > 32（帧头大小）
- 必须 ≤ 65504（UDP 最大负载）
- 必须是 32 的倍数（帧对齐）

### 1.5 零拷贝发送路径

```
Term Buffer (mmap 文件)
      ▲                         ▲
      │  UnsafeBuffer (写入)    │  ByteBuffer (发送)
      │                         │
 应用线程 offer()          Sender 线程 send()
      │                         │
      └───── 同一块物理内存 ─────┘
              ↓
         DatagramChannel.write(sendBuffer)
              ↓
         OS sendto() ── 直接从 mmap 页面 DMA 到网卡

整个路径：0 次用户态内存拷贝
（OS 层面可能有 1 次拷贝到 socket buffer，除非使用 sendfile/zero-copy NIC）
```

---

## 二、Aeron 传输协议 — 所有帧类型

### 2.1 协议帧类型总览

**源码位置：** `aeron-client/src/main/java/io/aeron/protocol/HeaderFlyweight.java`

| Type 值 | 常量 | 帧类型 | 方向 | 说明 |
|---------|------|--------|------|------|
| 0x00 | HDR_TYPE_PAD | PADDING | — | Term 末尾填充，标记空间不可用 |
| 0x01 | HDR_TYPE_DATA | DATA | Pub → Sub | 数据帧（含心跳帧，frameLength=0） |
| 0x02 | HDR_TYPE_NAK | NAK | Sub → Pub | 否定确认，请求重传丢失的数据 |
| 0x03 | HDR_TYPE_SM | Status Message | Sub → Pub | 状态消息，通告消费位置和窗口 |
| 0x04 | HDR_TYPE_ERR | ERROR | — | 错误帧 |
| 0x05 | HDR_TYPE_SETUP | SETUP | Pub → Sub | 流建立帧，通告流参数 |
| 0x06 | HDR_TYPE_RTTM | RTT Measurement | 双向 | 往返时延测量 |
| 0x07 | HDR_TYPE_RES | RESOLUTION | — | 名称解析 |

### 2.2 公共帧头（8 字节）

所有 Aeron 协议帧共享前 8 字节头部：

```
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                        Frame Length                           |  0
 +---------------------------------------------------------------+
 |  Version      |     Flags     |            Type               |  4
 +---------------------------------------------------------------+
 |                      Type-specific fields ...                 |  8+
```

### 2.3 DATA 帧（数据帧）

**Flyweight：** `DataHeaderFlyweight`，长度 = 32 字节 + Payload

```
 +0:  Frame Length (4B)       — 帧总长（含头 + 载荷）
 +4:  Version (1B)            — 协议版本
 +5:  Flags (1B)              — B=Begin(0x80), E=End(0x40), S=EOS(0x20), R=Revoked(0x10)
 +6:  Type (2B)               — 0x01 (DATA)
 +8:  Term Offset (4B)        — 帧在 term 内的偏移
+12:  Session ID (4B)         — Publication 会话标识
+16:  Stream ID (4B)          — 流标识
+20:  Term ID (4B)            — 当前 term 的 ID
+24:  Reserved Value (8B)     — 用户自定义值
+32:  Payload (变长)           — 应用消息
```

**心跳帧：** `Frame Length = 0`，Type = DATA。无数据时 Sender 周期性发送，用于活性检测。

### 2.4 SETUP 帧（流建立帧）

**Flyweight：** `SetupFlyweight`，固定长度 = 40 字节

```
 +0:  Frame Length (4B)       — 40
 +4:  Version (1B)
 +5:  Flags (1B)              — SEND_RESPONSE_SETUP(0x80), GROUP(0x40)
 +6:  Type (2B)               — 0x05 (SETUP)
 +8:  Term Offset (4B)        — 当前写入位置
+12:  Session ID (4B)
+16:  Stream ID (4B)
+20:  Initial Term ID (4B)    — 流的初始 term ID
+24:  Active Term ID (4B)     — 当前活跃 term ID
+28:  Term Length (4B)        — term 长度
+32:  MTU Length (4B)         — 协商 MTU
+36:  TTL (4B)                — 多播 TTL
```

**作用：** 发送端发送 SETUP 通知接收端流的参数（term 长度、MTU 等），接收端收到后创建 `PublicationImage` 并回复 Status Message。这是 Aeron 的"握手"过程。

### 2.5 Status Message（SM，状态消息帧）

**Flyweight：** `StatusMessageFlyweight`，长度 = 36 字节（+ 可选 8 字节 groupTag）

```
 +0:  Frame Length (4B)       — 36 或 44
 +4:  Version (1B)
 +5:  Flags (1B)              — SEND_SETUP(0x80), END_OF_STREAM(0x40)
 +6:  Type (2B)               — 0x03 (SM)
 +8:  Session ID (4B)
+12:  Stream ID (4B)
+16:  Consumption Term ID (4B)    — 接收端已消费到的 term ID
+20:  Consumption Term Offset (4B) — 接收端已消费到的 term 偏移
+24:  Receiver Window Length (4B)  — 接收端愿意接受的窗口大小
+28:  Receiver ID (8B)            — 接收端唯一标识（用于多播流控区分）
+36:  Group Tag (8B, 可选)        — 组标签
```

**这是 Aeron 可靠性的核心帧**。接收端周期性发送 SM 告诉发送端：
- **我消费到了哪里**（consumptionTermId + consumptionTermOffset）
- **我还能接受多少**（receiverWindowLength）

发送端据此计算 `senderLimit = consumptionPosition + receiverWindowLength`。

### 2.6 NAK 帧（否定确认帧）

**Flyweight：** `NakFlyweight`，固定长度 = 28 字节

```
 +0:  Frame Length (4B)       — 28
 +4:  Version (1B)
 +5:  Flags (1B)
 +6:  Type (2B)               — 0x02 (NAK)
 +8:  Session ID (4B)
+12:  Stream ID (4B)
+16:  Term ID (4B)            — 丢失数据所在的 term
+20:  Term Offset (4B)        — 丢失数据的起始偏移
+24:  Length (4B)              — 丢失数据的长度
```

**含义：** "我在 termId 的 [termOffset, termOffset+length) 位置没收到数据，请重传。"

### 2.7 RTT Measurement 帧

用于测量发送端到接收端的往返时延（Round Trip Time），供拥塞控制算法使用。

### 2.8 PADDING 帧

```
 +0:  Frame Length (4B)       — padding 总长
 +4:  Version (1B)
 +5:  Flags (1B)
 +6:  Type (2B)               — 0x00 (PAD)
 +8:  后续字段同 DATA 帧
```

**作用：** 当 term 末尾空间不足以放下一个完整帧时，用 PADDING 填充剩余空间，标记该区域不可用。Sender 和 Receiver 都会跳过 PADDING 帧。

---

## 三、UDP 的原生问题与 Aeron 的解决方案

### 3.1 问题总览：TCP vs UDP vs Aeron

| UDP 原生问题 | TCP 的解决方式 | Aeron 的解决方式 |
|-------------|---------------|-----------------|
| **丢包** | ACK + 超时重传 | **NAK-based 重传**（无 ACK 开销） |
| **乱序** | 序号 + 滑动窗口重排 | **Term Buffer + Position 有序交付** |
| **无流控** | 滑动窗口 | **positionLimit + Status Message** |
| **无拥塞控制** | AIMD/Cubic/BBR | **可插拔 CongestionControl**（Static/Cubic） |
| **无连接** | 三次握手 | **SETUP + SM 握手** |
| **无心跳** | Keep-alive | **周期性心跳帧（frameLength=0 的 DATA）** |
| **字节流无边界** | 应用层分帧 | **天然保留消息边界**（每个 Frame 独立） |
| **Head-of-line blocking** | 无解（TCP 固有） | **无此问题**（每个流独立、non-blocking） |
| **不支持多播** | 无（TCP 点对点） | **原生支持 UDP 多播** |

### 3.2 丢包 — NAK-based 重传机制

**核心设计：Aeron 使用 NAK（Negative Acknowledgement）而非 ACK。**

与 TCP 的 ACK-based 相比：
- **ACK-based**：每收到一个包都要发 ACK 回去 → 正常路径有大量控制消息
- **NAK-based**：只在**发现丢包**时才发 NAK → 正常路径**零控制消息开销**

#### 丢包检测（接收端）

```
DriverConductor（周期性调用）
  └─ PublicationImage.trackRebuild(nowNs)
       └─ LossDetector.scan(termBuffer, rebuildPosition, hwmPosition, ...)
            └─ TermGapScanner.scanForGap(termBuffer, termOffset, limitOffset)
```

**`TermGapScanner.scanForGap()` 检测逻辑：**

```java
// 从 rebuildPosition 扫描到 hwmPosition（高水位线）
for (offset = termOffset; offset < limitOffset; offset += alignedFrameLength) {
    frameLength = frameLengthVolatile(termBuffer, offset);
    if (frameLength <= 0) {
        // 这里有空洞！继续向前探测空洞的大小
        gapBeginOffset = offset;
        // 按 HEADER_LENGTH 步进，找到下一个有效帧
        ...
        handler.onGap(termId, gapBeginOffset, gapLength);
        return gapBeginOffset;  // 报告空洞
    }
}
```

**实际含义：** 接收端 Term Buffer 中的连续帧之间出现"空洞"——某个位置的 `frameLength ≤ 0` 表示该帧未到达。

```
Term Buffer（接收端）：
┌────────┬────────┬────────┬────────┬────────┬────────┐
│Frame 1 │Frame 2 │  空洞  │  空洞  │Frame 5 │Frame 6 │
│  ✓     │  ✓     │ len≤0  │ len≤0  │  ✓     │  ✓     │
└────────┴────────┴────┬───┴────────┴────────┴────────┘
                       │
              LossDetector 检测到这里有 gap
              生成 NAK: "termId=X, offset=128, length=128"
```

#### NAK 发送（接收端 Receiver 线程）

```
Receiver.doWork()
  └─ PublicationImage.processPendingLoss()
       │  检查 lossChangeNumber 是否变化
       └─ channelEndpoint.sendNakMessage(
              imageConnections, sessionId, streamId,
              termId, termOffset, length)
            └─ UDP 发送 NAK 帧到发送端
```

**NAK 防风暴机制（`FeedbackDelayGenerator`）：**
- 首次检测到空洞 → **不立即发 NAK**，而是设置一个随机延迟 deadline
- deadline 到期后仍有空洞 → 才真正发送 NAK
- 多播场景：延迟是随机的，避免多个接收端同时发 NAK 导致"NAK 风暴"

#### 重传处理（发送端）

```
ControlTransportPoller 收到 NAK 帧
  └─ SendChannelEndpoint.onNakMessage(msg, buffer, length, srcAddress)
       └─ NetworkPublication.onNak(termId, termOffset, length)
            └─ RetransmitHandler.onNak(termId, termOffset, length,
                   termBufferLength, mtuLength, flowControl, retransmitSender)
```

**`RetransmitHandler` 处理逻辑：**

```java
// RetransmitHandler.onNak()（简化）
1. 校验 NAK 参数合法性（offset 对齐、范围有效）
2. 限制重传长度：min(nakLength, flowControl.maxRetransmissionLength())
3. 检查并发重传限制：
   - 单播：最多 1 个并发重传
   - 多播：最多 maxRetransmits 个并发重传
4. 若 delay == 0 → 立即调用 retransmitSender.resend(termId, offset, length)
   若 delay > 0 → 进入 DELAYED 状态，超时后重传
```

**`NetworkPublication.resend()` — 实际重传：**

```java
// NetworkPublication.resend()（简化）
1. 计算 resendPosition，校验在 [bottomResendWindow, senderPosition) 范围内
2. 从 term buffer 扫描可用数据：
   scanOutcome = scanForAvailability(termBuffer, offset, min(mtuLength, remaining))
3. 通过 sendBuffer 切片发送：
   doSend(sendBuffer)  → channelEndpoint.send() → UDP 重传
```

**关键：重传的数据仍在 Term Buffer 中**——因为发送端的 Term Buffer 保留了已发送但可能未被确认的数据，直到接收端的消费位置推进过去。

#### 完整 NAK 重传时序

```
发送端 Sender                网络               接收端 Receiver
   │                          │                     │
   │  DATA Frame 1  ─────────►│──────────────────► │  ✓ 写入 Term Buffer
   │  DATA Frame 2  ─────────►│──────────────────► │  ✓ 写入 Term Buffer
   │  DATA Frame 3  ────X     │  (丢失！)          │
   │  DATA Frame 4  ─────────►│──────────────────► │  ✓ 写入 Term Buffer
   │  DATA Frame 5  ─────────►│──────────────────► │  ✓ 写入 Term Buffer
   │                          │                     │
   │                          │                     │  LossDetector 扫描：
   │                          │                     │  Frame 3 位置 len≤0 = 空洞
   │                          │                     │  设置延迟 deadline
   │                          │                     │
   │                          │                     │  deadline 到期
   │                          │    NAK(termId,      │
   │  ◄──────────────────────│◄── offset=128,     │  发送 NAK
   │                          │    length=64)       │
   │                          │                     │
   │  RetransmitHandler       │                     │
   │    → resend()            │                     │
   │  DATA Frame 3 (重传) ───►│──────────────────► │  ✓ 填补空洞
   │                          │                     │  rebuildPosition 推进
```

### 3.3 乱序 — 基于 Position 的有序交付

**UDP 包可能乱序到达，但 Aeron 保证应用层看到的消息是有序的。**

**机制：** 每个 Data Frame 携带 `termId + termOffset`，接收端根据这些信息将帧写入 Term Buffer 的**精确位置**。

```
接收端 Term Buffer：
Offset:   0      64     128    192    256    320
         ┌──────┬──────┬──────┬──────┬──────┬──────┐
收到顺序: │  #1  │  #2  │      │  #4  │      │  #6  │  ← 帧按 offset 精确放置
         └──────┴──────┴──────┴──────┴──────┴──────┘
                        ↑             ↑
                      空洞           空洞
```

- 应用层 `Subscription.poll()` 只从 `subscriberPosition` 开始连续读取
- 遇到空洞（`frameLengthVolatile ≤ 0`）时停止
- 空洞被重传填补后，`rebuildPosition` 推进，应用才能读到后续数据

**这意味着：** 乱序到达的包会被正确放到 Term Buffer 中，但应用层只能看到连续的数据——保证了顺序性。

### 3.4 流控 — Position/Limit 端到端背压

**Aeron 的流控是端到端的**，从接收端一直反压到发送端应用层。

```
                    ┌──────────────────────────────────────────────┐
                    │                 Position 链                   │
                    │                                              │
 positionLimit      │   senderLimit      senderPosition            │
(Client 可见)       │   (Sender 可见)    (Sender 维护)              │
     │              │        │                │                    │
     ▼              │        ▼                ▼                    │
 ────┤──────────────┤────────┤────────────────┤────────────────────┤──►
     │              │        │                │                    │   position
     │  应用写入窗口 │        │  网络发送窗口   │   已发送待确认      │
     │              │        │                │                    │
     │              │        │                │                    │
     │              │        │            receiverPosition         │
     │              │        │            + receiverWindow         │
     │              │        │            (通过 SM 通告)            │
                    └──────────────────────────────────────────────┘
```

**四个关键 Position：**

| Position | 维护方 | 含义 |
|----------|--------|------|
| **subscriberPosition** | 接收端 Client | 应用层已消费（poll 读过）的位置 |
| **receiverHwmPosition** | 接收端 Driver | 收到的最高水位线 |
| **senderPosition** | 发送端 Driver | Sender 已发送到网络的位置 |
| **publisherPosition** | 发送端 Client | 应用层已写入 Term Buffer 的位置（tail） |

**流控链路：**

```
接收端 subscriberPosition  ──Status Message──►  发送端 FlowControl
                                                    │
                                              senderLimit = consumptionPos + windowLength
                                                    │
                                              positionLimit（写入 CnC counter）
                                                    │
                                              应用线程 offer(): position < limit ?
                                                    │
                                              超过 → 返回 BACK_PRESSURED
```

### 3.5 拥塞控制 — 可插拔 CongestionControl

Aeron 的拥塞控制在**接收端**实现，通过控制 Status Message 中的 `receiverWindowLength` 来限制发送速率。

**CongestionControl 接口（`aeron-driver/.../CongestionControl.java`）：**

| 方法 | 说明 |
|------|------|
| `initialWindowLength()` | 初始窗口大小 |
| `maxWindowLength()` | 最大窗口大小 |
| `onTrackRebuild(...)` | 根据 rebuild 进度调整窗口 |
| `shouldMeasureRtt()` | 是否需要 RTT 测量 |
| `onRttMeasurement(...)` | 处理 RTT 测量结果 |

**内置实现：**

| 实现 | 说明 |
|------|------|
| **StaticWindowCongestionControl** | 固定窗口（默认），`min(initialWindow, termLength/2)` |
| **CubicCongestionControl** | 基于 TCP CUBIC 算法，动态调整窗口，支持 RTT 测量 |

**工作原理：**
1. CongestionControl 根据网络状况计算 `receiverWindowLength`
2. 此值放入 Status Message 发给发送端
3. 发送端 FlowControl 用 `consumptionPosition + receiverWindowLength` 计算 `senderLimit`
4. Sender 只在 `senderPosition < senderLimit` 时才发数据

### 3.6 连接管理 — SETUP/SM 握手

UDP 没有连接的概念，Aeron 通过 SETUP 帧和 Status Message 实现类似"连接建立"的效果。

```
发送端                            接收端
  │                                │
  │  ① SETUP 帧 ───────────────► │  收到 SETUP，了解流参数
  │  (sessionId, streamId,         │  (termLength, MTU, initialTermId...)
  │   initialTermId, termLength,   │
  │   MTU, activeTermId)           │  创建 PublicationImage
  │                                │
  │                                │  ② Status Message ◄────────
  │  ◄────────────────────────── │  (consumptionPos, windowLength)
  │                                │
  │  收到 SM → isConnected=true    │
  │  开始正常数据传输               │
  │                                │
  │  ③ DATA 帧 ────────────────► │  接收数据
  │  ...                           │
```

**与 TCP 三次握手的对比：**
- TCP：SYN → SYN-ACK → ACK（三次）
- Aeron：SETUP → SM（两次），更轻量

### 3.7 心跳与活性检测

**发送端心跳：** 当没有新数据时，Sender 线程周期性发送**心跳帧**（`Frame Length = 0` 的 DATA 帧）。

```java
// NetworkPublication.heartbeatMessageCheck()
if (nowNs > (timeOfLastDataOrHeartbeatNs + PUBLICATION_HEARTBEAT_TIMEOUT_NS)) {
    // 发送 frameLength=0 的 DATA 帧
    heartbeatBuffer.putInt(0, 0);  // frameLength = 0
    channelEndpoint.send(heartbeatBuffer);
}
```

**接收端超时：** 如果超过 `PUBLICATION_CONNECTION_TIMEOUT_NS` 未收到任何 DATA 或心跳帧，认为发送端断开：
- `isConnected` 设为 false
- 应用层 `offer()` 返回 `NOT_CONNECTED`

### 3.8 消息边界保留

**TCP 的痛点：** TCP 是字节流协议，不保留消息边界。应用需要自己做分帧（长度前缀、分隔符等）。

**Aeron 的优势：** 每个 `offer()` 调用对应一个或多个完整的 Data Frame，接收端 `poll()` 以"fragment"为单位回调，天然保留消息边界。

```
发送端:
  offer("Hello")  → Frame [32B header + 5B "Hello"]
  offer("World")  → Frame [32B header + 5B "World"]

接收端 poll() 回调:
  onFragment(buffer, offset, 5, header)  → "Hello"  （完整消息）
  onFragment(buffer, offset, 5, header)  → "World"  （完整消息）
```

大消息分片时，通过 BEGIN/END 标志自动重组：
- `FragmentAssembler` 会将多个分片重组为一条完整消息后回调

---

## 四、端到端可靠传输全流程

### 4.1 正常数据传输流程

```
发送端 App        发送端 Driver (Sender)         网络        接收端 Driver (Receiver)    接收端 App
    │                     │                       │                │                      │
    │ offer(msg)          │                       │                │                      │
    │──► Term Buffer 写入 │                       │                │                      │
    │                     │                       │                │                      │
    │                     │ scan Term Buffer      │                │                      │
    │                     │ 发现完成帧             │                │                      │
    │                     │                       │                │                      │
    │                     │ UDP DATA ────────────►│──────────────►│                      │
    │                     │                       │                │ 写入接收端 Term Buffer │
    │                     │                       │                │                      │
    │                     │                       │ UDP SM ◄──────│ 周期性发送 SM          │
    │                     │◄──────────────────────│                │ (consumptionPos,      │
    │                     │ 更新 senderLimit       │                │  windowLength)        │
    │                     │                       │                │                      │
    │                     │                       │                │                      │ poll()
    │                     │                       │                │──────────────────────►│
    │                     │                       │                │ 从 Term Buffer 读取    │
    │                     │                       │                │ 回调 onFragment()     │
```

### 4.2 丢包检测与恢复流程

```
发送端 Driver              网络            接收端 Driver
      │                     │                  │
      │ DATA Frame 3 ──X   │  丢失             │
      │ DATA Frame 4 ─────►│────────────────►│ 写入 offset=192
      │ DATA Frame 5 ─────►│────────────────►│ 写入 offset=256
      │                     │                  │
      │                     │                  │ Conductor.trackRebuild()
      │                     │                  │   → LossDetector.scan()
      │                     │                  │   → 发现 offset=128 空洞
      │                     │                  │   → 设置延迟 deadline
      │                     │                  │
      │                     │                  │ deadline 到期，空洞仍在
      │                     │                  │ processPendingLoss()
      │                     │                  │
      │       NAK ◄─────────│◄─────────────── │ sendNakMessage(termId=X,
      │  (termId=X,         │                  │   offset=128, len=64)
      │   offset=128,       │                  │
      │   len=64)           │                  │
      │                     │                  │
      │ RetransmitHandler   │                  │
      │   → resend()        │                  │
      │ DATA Frame 3 ──────►│────────────────►│ 填补空洞
      │  (重传)             │                  │ rebuildPosition 推进
      │                     │                  │ 应用可读取 Frame 3,4,5
```

### 4.3 流控反压流程

```
发送端 App      发送端 Driver           网络          接收端 Driver      接收端 App
    │               │                    │                │                  │
    │ offer()       │                    │                │                  │
    │──►写入快────►│──►发送快────────►│──►接收 ─────►│──► poll() 慢     │
    │               │                    │                │                  │
    │               │                    │                │                  │
    │               │                    │◄── SM ────────│ subscriberPos    │
    │               │◄───────────────────│    消费慢       │ 推进慢           │
    │               │                    │    窗口缩小     │                  │
    │               │                    │                │                  │
    │               │ senderLimit 降低   │                │                  │
    │               │ → 停止发送新数据   │                │                  │
    │               │ → positionLimit 降低│                │                  │
    │               │                    │                │                  │
    │ offer()       │                    │                │                  │
    │ position≥limit│                    │                │                  │
    │ ← BACK_PRESSURED                  │                │                  │
    │               │                    │                │                  │
    │ (等待...)     │                    │                │                  │
    │               │                    │                │                  │
    │               │                    │                │              poll()
    │               │                    │                │◄─────────────────│
    │               │                    │◄── SM ────────│ subscriberPos    │
    │               │◄───────────────────│    消费推进     │ 推进             │
    │               │ senderLimit 提高   │    窗口打开     │                  │
    │               │ positionLimit 提高 │                │                  │
    │               │                    │                │                  │
    │ offer()       │                    │                │                  │
    │ position<limit│                    │                │                  │
    │ ── 成功 ✓    │                    │                │                  │
```

---

## 五、Aeron vs TCP 设计哲学对比

| 维度 | TCP | Aeron (UDP) |
|------|-----|-------------|
| **确认机制** | ACK-based（每包确认） | NAK-based（仅丢包时确认） |
| **正常路径开销** | 每个数据包产生 ACK | 仅周期性 SM，无逐包确认 |
| **重传触发** | 超时 + 快速重传（3个冗余ACK） | NAK 显式请求 |
| **流控** | 内核态滑动窗口 | 用户态 Position/Limit + SM |
| **拥塞控制** | 内核态（CUBIC/BBR等） | 用户态可插拔（Static/Cubic） |
| **消息边界** | 字节流，无边界 | 帧级别，保留消息边界 |
| **多播** | 不支持 | 原生支持 |
| **Head-of-line blocking** | 有（单连接内所有数据排队） | 无（每个流独立） |
| **延迟** | 高（内核协议栈 + Nagle + 延迟ACK） | 低（用户态 + busy-wait） |
| **可调性** | 有限（内核参数） | 完全可配（代码级别） |

**为什么 Aeron 选择 NAK 而非 ACK？**

```
ACK-based (TCP):
  DATA→  DATA→  DATA→  DATA→  DATA→
  ←ACK   ←ACK   ←ACK   ←ACK   ←ACK    ← 每个包都要确认，5个数据包 = 5个ACK

NAK-based (Aeron):
  DATA→  DATA→  DATA→  DATA→  DATA→
                                 ←SM    ← 周期性 SM（而非逐包），只在丢包时发 NAK
```

**正常传输（无丢包）时：**
- TCP：N 个数据包 → N 个 ACK（或延迟ACK约 N/2）
- Aeron：N 个数据包 → 0 个 NAK + 周期性 SM（频率远低于数据率）

**这就是 Aeron 在高吞吐场景下延迟更低、CPU 开销更小的原因。**

---

## 流程总结

### Aeron 在 UDP 上构建的可靠传输协议栈

```
┌──────────────────────────────────────────────────────────┐
│                    应用层 (Application)                    │
│         offer() / poll() — 消息级别 API                   │
├──────────────────────────────────────────────────────────┤
│                 消息分片 / 重组层                          │
│  offer: 大消息 → 多个 Frame (BEGIN/END 标志)              │
│  poll:  FragmentAssembler 重组为完整消息                   │
├──────────────────────────────────────────────────────────┤
│                 流控层 (Flow Control)                      │
│  Position/Limit 机制 + Status Message 反馈                │
│  positionLimit → senderLimit → publisherLimit             │
├──────────────────────────────────────────────────────────┤
│               可靠性层 (Reliability)                       │
│  NAK-based 丢包检测 + RetransmitHandler 重传              │
│  LossDetector → NAK → resend()                           │
├──────────────────────────────────────────────────────────┤
│              拥塞控制层 (Congestion Control)               │
│  StaticWindow / Cubic — 控制 receiverWindowLength         │
├──────────────────────────────────────────────────────────┤
│               连接管理层 (Session)                         │
│  SETUP/SM 握手 + 心跳 + 超时断开                          │
├──────────────────────────────────────────────────────────┤
│                 帧化层 (Framing)                          │
│  Data Frame: 32B header + payload                        │
│  所有帧类型: DATA/NAK/SM/SETUP/RTTM/PADDING              │
├──────────────────────────────────────────────────────────┤
│                传输层 (Transport)                          │
│  UDP DatagramChannel — 非阻塞 NIO                        │
│  MTU 限制 (默认 1408B)                                   │
│  单播 / 多播                                              │
├──────────────────────────────────────────────────────────┤
│                 网络层 (Network)                           │
│  IP + UDP                                                │
└──────────────────────────────────────────────────────────┘
```

**一句话总结：Aeron 在 UDP 之上用 NAK-based 重传保证可靠性、用 Status Message 驱动流控、用可插拔拥塞控制适配网络、用 Term Buffer + Position 保证有序交付——在获得 UDP 的低延迟、多播、无 head-of-line blocking 优势的同时，提供了比肩甚至超越 TCP 的可靠性保障。**

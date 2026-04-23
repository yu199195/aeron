# `publication.offer(buffer, 0, length)` 源码深度解析

## 目录

- [概述](#概述)
- [整体架构图](#整体架构图)
- [完整调用时序](#完整调用时序)
- [第一层：Publication.offer() — 用户入口](#第一层publicationoffer--用户入口)
- [第二层：ConcurrentPublication.offer() — 流控检查与分发](#第二层concurrentpublicationoffer--流控检查与分发)
  - [读取 positionLimit 与当前 term 状态](#读取-positionlimit-与当前-term-状态)
  - [position 与 limit 比较 — 背压判断](#position-与-limit-比较--背压判断)
  - [消息路径选择 — 整帧 vs 分片](#消息路径选择--整帧-vs-分片)
- [第三层：appendUnfragmentedMessage — 单帧写入核心](#第三层appendunfragmentedmessage--单帧写入核心)
  - [CAS 原子推进 tail](#cas-原子推进-tail)
  - [HeaderWriter.write() — 写帧头](#headerwriterwrite--写帧头)
  - [拷贝消息体](#拷贝消息体)
  - [frameLengthOrdered() — 发布帧](#framelengthOrdered--发布帧)
- [第四层：appendFragmentedMessage — 大消息分片写入](#第四层appendfragmentedmessage--大消息分片写入)
- [第五层：handleEndOfLog — Term 末尾与轮转](#第五层handleendoflog--term-末尾与轮转)
- [第六层：Sender 线程 — 从 Term Buffer 到网络](#第六层sender-线程--从-term-buffer-到网络)
  - [sendData() — 扫描并发送](#senddata--扫描并发送)
  - [流控与 senderLimit](#流控与-senderlimit)
- [关键数据结构](#关键数据结构)
  - [Log Buffer 内存布局](#log-buffer-内存布局)
  - [Data Frame 帧格式](#data-frame-帧格式)
  - [rawTail 编码](#rawtail-编码)
  - [Position 计算](#position-计算)
- [offer 返回值语义](#offer-返回值语义)
- [流程总结](#流程总结)

---

## 概述

`publication.offer(buffer, 0, length)` 是 Aeron **数据面（Data Plane）** 的核心写入 API。它是**非阻塞**的，应用线程调用 `offer` 将消息直接写入共享内存中的 **Term Buffer**，无需经过 Driver 的命令通道。随后，Driver 的 **Sender 线程** 从 Term Buffer 中扫描已完成的帧，通过 UDP 发送到网络。

**核心设计思想：**
- **零拷贝路径**：应用线程直接写入 mmap 的 Term Buffer，Sender 线程直接从同一块内存发送 UDP 数据包
- **无锁并发**（ConcurrentPublication）：通过 `getAndAddLong`（原子 fetch-and-add）推进 tail，多个发送者线程可并发 offer
- **流控背压**：通过 `positionLimit`（由 Driver 根据接收端 Status Message 更新）实现端到端流控
- **三分区轮转**：3 个 Term Buffer 循环使用，保证一个在写、一个在发送、一个在清理

**关键路径：**

```
应用线程                           Driver Sender 线程
   │                                    │
   │  offer(buffer, 0, length)          │
   │         │                          │
   │   ┌─────▼──────────┐              │
   │   │ 读 positionLimit│              │
   │   │ 读 activeTermCount            │
   │   │ 读 rawTail      │              │
   │   └─────┬──────────┘              │
   │         │                          │
   │   ┌─────▼──────────┐              │
   │   │ position < limit? ──N──► 返回 BACK_PRESSURED / NOT_CONNECTED
   │   └─────┬──────────┘
   │         │Y
   │   ┌─────▼──────────┐
   │   │ getAndAddLong   │  原子推进 tail
   │   │ (CAS-free)      │
   │   └─────┬──────────┘
   │         │
   │   ┌─────▼──────────┐
   │   │ 写帧头(负长度)  │  HeaderWriter.write()
   │   │ 拷贝消息体      │  putBytes()
   │   │ 写帧长(正长度)  │  frameLengthOrdered() ←── 此刻帧对 Sender 可见
   │   └─────┬──────────┘
   │         │                    ┌─────▼──────────┐
   │   返回 newPosition          │ scanForAvailability│  扫描已完成帧
   │                              │ DatagramChannel.send│  UDP 发送
   │                              │ 更新 senderPosition │
   │                              └────────────────┘
```

---

## 完整调用时序

```
┌──────────┐       ┌──────────────────┐      ┌──────────────────┐     ┌────────────────┐
│ App线程   │       │ ConcurrentPub    │      │ Term Buffer      │     │ Sender 线程    │
│          │       │ (offer)          │      │ (共享内存mmap)     │     │ (Driver侧)    │
└────┬─────┘       └───────┬──────────┘      └───────┬──────────┘     └───────┬────────┘
     │  offer(buf,0,len)   │                         │                        │
     │────────────────────►│                         │                        │
     │                     │  1. 读 positionLimit    │                        │
     │                     │  2. 读 activeTermCount  │                        │
     │                     │  3. 读 rawTail          │                        │
     │                     │  4. 计算 position       │                        │
     │                     │                         │                        │
     │                     │  position < limit?      │                        │
     │                     │──────Y──────────────►   │                        │
     │                     │                         │                        │
     │                     │  5. getAndAddLong(tail)  │                        │
     │                     │─────────────────────────►│ tail 原子推进          │
     │                     │                         │                        │
     │                     │  6. write(header,-len)   │                        │
     │                     │─────────────────────────►│ 帧头占位（负长度）      │
     │                     │                         │                        │
     │                     │  7. putBytes(payload)    │                        │
     │                     │─────────────────────────►│ 消息体写入             │
     │                     │                         │                        │
     │                     │  8. frameLengthOrdered() │                        │
     │                     │─────────────────────────►│ 正长度发布 ─────────── │─► 帧可见
     │                     │                         │                        │
     │  return position    │                         │                        │
     │◄────────────────────│                         │  scanForAvailability   │
     │                     │                         │◄───────────────────────│
     │                     │                         │                        │
     │                     │                         │  frameLengthVolatile   │
     │                     │                         │  读到正长度 ──────────►│
     │                     │                         │                        │
     │                     │                         │  sendBuffer切片        │
     │                     │                         │─────────UDP send──────►│ Network
```

---

## 第一层：Publication.offer() — 用户入口

`Publication` 是抽象基类，`offer(buffer, offset, length)` 最终委托给带 `ReservedValueSupplier` 参数的抽象方法。

**源码位置：** `aeron-client/src/main/java/io/aeron/Publication.java`

```java
// 便捷方法：发送整个 buffer
public final long offer(final DirectBuffer buffer) {
    return offer(buffer, 0, buffer.capacity());
}

// 三参数版本：委托给四参数版本，reservedValueSupplier 传 null
public final long offer(final DirectBuffer buffer, final int offset, final int length) {
    return offer(buffer, offset, length, null);
}

// 抽象方法：由 ConcurrentPublication / ExclusivePublication 实现
public abstract long offer(
    DirectBuffer buffer, int offset, int length, ReservedValueSupplier reservedValueSupplier);
```

**两种 Publication 子类的区别：**

| 特性 | ConcurrentPublication | ExclusivePublication |
|------|----------------------|---------------------|
| 线程安全 | 是（多线程可并发 offer） | 否（仅单线程使用） |
| tail 推进方式 | `getAndAddLong`（原子 fetch-and-add） | 直接读写 tail（无竞争） |
| 创建方式 | `aeron.addPublication()` | `aeron.addExclusivePublication()` |
| 典型场景 | 多发送线程共享一个 Publication | 单线程独占，性能更优 |

本文以 `ConcurrentPublication`（默认的 `addPublication` 返回类型）为主线分析。

---

## 第二层：ConcurrentPublication.offer() — 流控检查与分发

**源码位置：** `aeron-client/src/main/java/io/aeron/ConcurrentPublication.java:93-139`

```java
public long offer(
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final ReservedValueSupplier reservedValueSupplier)
{
    long newPosition = CLOSED;
    if (!isClosed)
    {
        final long limit = positionLimit.getVolatile();           // ① 流控上限
        final int termCount = activeTermCount(logMetaDataBuffer);  // ② 当前 term 序号
        final int index = indexByTermCount(termCount);             // ③ term buffer 下标(0/1/2)
        final UnsafeBuffer termBuffer = termBuffers[index];
        final int tailCounterOffset = TERM_TAIL_COUNTERS_OFFSET + (index * SIZE_OF_LONG);
        final long rawTail = logMetaDataBuffer.getLongVolatile(tailCounterOffset);  // ④ 当前 tail
        final int termOffset = termOffset(rawTail, termBuffer.capacity());
        final int termId = termId(rawTail);

        if (termCount != (termId - initialTermId))
        {
            return ADMIN_ACTION;  // ⑤ term 正在轮转
        }

        final long position = computePosition(termId, termOffset, positionBitsToShift, initialTermId);
        if (position < limit)                                      // ⑥ 背压检查
        {
            if (length <= maxPayloadLength)
            {
                checkPositiveLength(length);
                newPosition = appendUnfragmentedMessage(            // ⑦ 整帧写入
                    termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
            }
            else
            {
                checkMaxMessageLength(length);
                newPosition = appendFragmentedMessage(              // ⑧ 分片写入
                    termBuffer, tailCounterOffset, buffer, offset, length, reservedValueSupplier);
            }
        }
        else
        {
            newPosition = backPressureStatus(position, length);    // ⑨ 背压/未连接
        }
    }

    return newPosition;
}
```

### 读取 positionLimit 与当前 term 状态

**① positionLimit**：这是一个 `ReadablePosition`，底层对应 CnC counters buffer 中的一个计数器。Driver 的 Conductor 线程会根据**接收端 Status Message 反馈的窗口**定期更新此值。`getVolatile()` 保证读到最新值。

**② activeTermCount**：从 logMetaDataBuffer 的 `LOG_ACTIVE_TERM_COUNT_OFFSET` 处 volatile 读取。表示当前活跃的 term 序号（从 0 开始单调递增）。

**③ indexByTermCount**：`termCount % 3`，将 term 序号映射到 3 个 term buffer 分区之一。

**④ rawTail**：64 位值，高 32 位是 termId，低 32 位是 termOffset（当前写入位置）。

**⑤ 一致性校验**：若 `termCount != (termId - initialTermId)`，说明 term 正在轮转中，返回 `ADMIN_ACTION` 让调用者重试。

### position 与 limit 比较 — 背压判断

**⑥ 核心流控**：`position = (termId - initialTermId) << positionBitsToShift + termOffset`

- `position < limit` → 有发送窗口，可以写入
- `position >= limit` → 进入 `backPressureStatus()`：
  - 若 `isConnected` → 返回 `BACK_PRESSURED`（有连接但窗口满）
  - 若未连接 → 返回 `NOT_CONNECTED`
  - 若超过 `maxPossiblePosition` → 返回 `MAX_POSITION_EXCEEDED`

```java
final long backPressureStatus(final long currentPosition, final int messageLength) {
    if ((currentPosition + align(messageLength + HEADER_LENGTH, FRAME_ALIGNMENT)) >= maxPossiblePosition) {
        return MAX_POSITION_EXCEEDED;
    }
    if (LogBufferDescriptor.isConnected(logMetaDataBuffer)) {
        return BACK_PRESSURED;
    }
    return NOT_CONNECTED;
}
```

### 消息路径选择 — 整帧 vs 分片

**⑦ 整帧（Unfragmented）**：消息长度 ≤ `maxPayloadLength`（= MTU - 32 字节帧头）。单个 Data Frame 即可容纳。

**⑧ 分片（Fragmented）**：消息超过单帧负载能力，需拆分为多个 Frame。每个 Frame 最多携带 `maxPayloadLength` 字节，通过 BEGIN/END 标志位标识分片关系。

---

## 第三层：appendUnfragmentedMessage — 单帧写入核心

**源码位置：** `ConcurrentPublication.java:348-385`

这是 offer 最常见的路径——消息够小，一帧搞定。

```java
private long appendUnfragmentedMessage(
    final UnsafeBuffer termBuffer,
    final int tailCounterOffset,
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final ReservedValueSupplier reservedValueSupplier)
{
    final int frameLength = length + HEADER_LENGTH;                 // ① 帧总长 = 消息 + 32字节头
    final int alignedLength = align(frameLength, FRAME_ALIGNMENT);  // ② 对齐到 32 字节
    final int termLength = termBuffer.capacity();

    // ③ 原子推进 tail
    final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, alignedLength);
    final int termId = termId(rawTail);
    final int termOffset = termOffset(rawTail, termLength);

    final int resultingOffset = termOffset + alignedLength;
    final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);

    if (resultingOffset > termLength)
    {
        return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);  // ④ term 末尾
    }
    else
    {
        // ⑤ 写帧头（负长度占位）
        headerWriter.write(termBuffer, termOffset, frameLength, termId);
        // ⑥ 拷贝消息体
        termBuffer.putBytes(termOffset + HEADER_LENGTH, buffer, offset, length);

        if (null != reservedValueSupplier) {
            final long reservedValue = reservedValueSupplier.get(termBuffer, termOffset, frameLength);
            termBuffer.putLong(termOffset + RESERVED_VALUE_OFFSET, reservedValue, LITTLE_ENDIAN);
        }

        // ⑦ 发布帧（正长度，release 语义）
        frameLengthOrdered(termBuffer, termOffset, frameLength);
    }

    return position;
}
```

### CAS 原子推进 tail

**③ `getAndAddLong`**：这是整个并发写入的关键。对于 `ConcurrentPublication`，多线程可以同时 offer。每个线程通过 `getAndAddLong(tailCounterOffset, alignedLength)` 原子地"预留"一段空间：

```
线程A: getAndAddLong → 得到 oldTail=100, tail 变为 164 (100+64)
线程B: getAndAddLong → 得到 oldTail=164, tail 变为 228 (164+64)
```

- 返回值 `rawTail` 是**更新前**的 tail（即本线程的写入起点）
- 每个线程拿到不同的 `termOffset`，互不冲突
- 这比 CAS 循环更高效：无重试、无 ABA 问题，一条 CPU 指令（`lock xadd`）完成

### HeaderWriter.write() — 写帧头

**⑤ 帧头写入**使用巧妙的**负长度占位**技术：

```java
// HeaderWriter.write() — 小端字节序版本
public void write(final UnsafeBuffer termBuffer, final int offset, final int length, final int termId) {
    // 第一个 long：高 32 位 = versionFlagsType，低 32 位 = 负长度（占位）
    termBuffer.putLongRelease(offset + FRAME_LENGTH_FIELD_OFFSET, 
        versionFlagsType | ((-length) & 0xFFFF_FFFFL));
    VarHandle.storeStoreFence();

    // 第二个 long：sessionId | termOffset
    termBuffer.putLong(offset + TERM_OFFSET_FIELD_OFFSET, sessionId | offset);
    // 第三个 long：termId | streamId
    termBuffer.putLong(offset + STREAM_ID_FIELD_OFFSET, (((long)termId) << 32) | streamId);
}
```

**为什么用负长度？**

这是 Aeron 的**两阶段提交协议**：
1. **阶段一**：`HeaderWriter.write()` 写入**负的帧长度**（`-frameLength`），表示"帧正在填充中"
2. 应用线程拷贝消息体到帧头后面
3. **阶段二**：`frameLengthOrdered()` 将帧长度覆写为**正值**，带 release 语义

Sender 线程使用 `frameLengthVolatile()` 读帧长度：
- 读到 **≤ 0** → 帧尚未完成，停止扫描
- 读到 **> 0** → 帧已完成，可以发送

这保证了即使多线程并发写入，Sender 也只会发送已完整写好的帧。

### 拷贝消息体

**⑥ `putBytes`**：将用户 buffer 中 `[offset, offset+length)` 的数据拷贝到 term buffer 的 `termOffset + HEADER_LENGTH` 处。这里是真正的内存拷贝操作。

### frameLengthOrdered() — 发布帧

**⑦ 发布帧**：

```java
public static void frameLengthOrdered(
    final UnsafeBuffer buffer, final int termOffset, final int frameLength) {
    int length = frameLength;
    if (ByteOrder.nativeOrder() != LITTLE_ENDIAN) {
        length = Integer.reverseBytes(frameLength);
    }
    buffer.putIntRelease(termOffset, length);  // release 语义，保证之前的写入对其他线程可见
}
```

`putIntRelease` 使用 `VarHandle.setRelease()` 语义：**保证之前所有的 store（帧头写入、消息体拷贝）在此 store 之前对其他线程可见**。这是 Sender 线程能安全读取完整帧的关键。

---

## 第四层：appendFragmentedMessage — 大消息分片写入

当消息长度超过 `maxPayloadLength`（= MTU - 32）时，需要拆分成多个 Frame。

**源码位置：** `ConcurrentPublication.java:387-450`

```java
private long appendFragmentedMessage(
    final UnsafeBuffer termBuffer,
    final int tailCounterOffset,
    final DirectBuffer buffer,
    final int offset,
    final int length,
    final ReservedValueSupplier reservedValueSupplier)
{
    // ① 计算分片后的总帧长度
    final int framedLength = computeFragmentedFrameLength(length, maxPayloadLength);
    final int termLength = termBuffer.capacity();

    // ② 一次性原子预留所有分片的空间
    final long rawTail = logMetaDataBuffer.getAndAddLong(tailCounterOffset, framedLength);
    final int termId = termId(rawTail);
    final int termOffset = termOffset(rawTail, termLength);

    final int resultingOffset = termOffset + framedLength;
    final long position = computePosition(termId, resultingOffset, positionBitsToShift, initialTermId);
    if (resultingOffset > termLength) {
        return handleEndOfLog(termBuffer, termLength, termId, termOffset, position);
    }
    else
    {
        int frameOffset = termOffset;
        byte flags = BEGIN_FRAG_FLAG;       // 第一个分片：B=1
        int remaining = length;

        do {
            final int bytesToWrite = Math.min(remaining, maxPayloadLength);
            final int frameLength = bytesToWrite + HEADER_LENGTH;
            final int alignedLength = align(frameLength, FRAME_ALIGNMENT);

            headerWriter.write(termBuffer, frameOffset, frameLength, termId);
            termBuffer.putBytes(
                frameOffset + HEADER_LENGTH, buffer,
                offset + (length - remaining), bytesToWrite);

            if (remaining <= maxPayloadLength) {
                flags |= END_FRAG_FLAG;     // 最后一个分片：E=1
            }
            frameFlags(termBuffer, frameOffset, flags);

            if (null != reservedValueSupplier) { ... }

            frameLengthOrdered(termBuffer, frameOffset, frameLength);  // 逐帧发布

            flags = 0;                       // 中间分片：B=0, E=0
            frameOffset += alignedLength;
            remaining -= bytesToWrite;
        } while (remaining > 0);
    }

    return position;
}
```

**分片策略：**

```
原始消息 (length = 5000, maxPayloadLength = 1376)
    │
    ├─ Frame 1: [Header 32B][Payload 1376B] flags=B   (BEGIN)
    ├─ Frame 2: [Header 32B][Payload 1376B] flags=0   (中间)
    ├─ Frame 3: [Header 32B][Payload 1376B] flags=0   (中间)
    └─ Frame 4: [Header 32B][Payload  872B] flags=E   (END)
```

**关键设计点：**
1. **原子预留**：所有分片的空间通过一次 `getAndAddLong` 原子预留，确保连续排列
2. **逐帧发布**：每个分片独立调用 `frameLengthOrdered()` 发布，Sender 可以逐帧扫描
3. **标志位**：`BEGIN_FRAG_FLAG (0x80)` 和 `END_FRAG_FLAG (0x40)` 标识分片边界，接收端据此重组

**`computeFragmentedFrameLength` 计算公式：**

```java
public static int computeFragmentedFrameLength(final int length, final int maxPayloadSize) {
    final int numMaxPayloads = length / maxPayloadSize;
    final int remainingPayload = length % maxPayloadSize;
    final int lastFrameLength =
        remainingPayload > 0 ? align(remainingPayload + HEADER_LENGTH, FRAME_ALIGNMENT) : 0;
    return (numMaxPayloads * (maxPayloadSize + HEADER_LENGTH)) + lastFrameLength;
}
```

---

## 第五层：handleEndOfLog — Term 末尾与轮转

当 `getAndAddLong` 推进的 tail 超过 term buffer 容量时，触发 term 轮转。

**源码位置：** `ConcurrentPublication.java:743-766`

```java
private long handleEndOfLog(
    final UnsafeBuffer termBuffer, final int termLength,
    final int termId, final int termOffset, final long position)
{
    // ① 填充 padding 帧（若当前 term 还有空间）
    if (termOffset < termLength)
    {
        final int paddingLength = termLength - termOffset;
        headerWriter.write(termBuffer, termOffset, paddingLength, termId);
        frameType(termBuffer, termOffset, PADDING_FRAME_TYPE);   // 标记为 PADDING 帧
        frameLengthOrdered(termBuffer, termOffset, paddingLength);
    }

    // ② 检查是否超过流的最大 position
    if (position >= maxPossiblePosition) {
        return MAX_POSITION_EXCEEDED;
    }

    // ③ 轮转到下一个 term
    rotateLog(logMetaDataBuffer, termId - initialTermId, termId);

    return ADMIN_ACTION;  // 告知调用者重试
}
```

**Term 轮转流程 (`rotateLog`)：**

```java
public static boolean rotateLog(final UnsafeBuffer metadataBuffer, final int termCount, final int termId) {
    final int nextTermId = termId + 1;
    final int nextTermCount = termCount + 1;
    final int nextIndex = indexByTermCount(nextTermCount);       // 下一个分区 (0→1→2→0)
    final int expectedTermId = nextTermId - PARTITION_COUNT;     // 该分区上一次的 termId

    long rawTail;
    do {
        rawTail = rawTailVolatile(metadataBuffer, nextIndex);
        if (expectedTermId != termId(rawTail)) {
            break;  // 已经被其他线程轮转过了
        }
    } while (!casRawTail(metadataBuffer, nextIndex, rawTail, packTail(nextTermId, 0)));

    return casActiveTermCount(metadataBuffer, termCount, nextTermCount);  // CAS 更新活跃 term
}
```

**轮转是并发安全的**：多个线程可能同时发现 term 满了，但通过 CAS 操作保证只有一个线程成功轮转。其余线程拿到 `ADMIN_ACTION` 后重试 offer，会看到新的 `activeTermCount`。

```
 Term 0 (termId=42)     Term 1 (termId=43)     Term 2 (termId=44)
┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐
│ ████████████████ │   │ ██████░░░░░░░░░░ │   │ ░░░░░░░░░░░░░░░░ │
│ 已发送完毕       │   │ 当前写入中       │   │ 等待清理/复用    │
│ 可被 Sender 扫描 │   │ activeTermCount  │   │                  │
└──────────────────┘   └──────────────────┘   └──────────────────┘
      ▲ Sender 追赶          ▲ 写入方向             ▲ 下次轮转目标
```

---

## 第六层：Sender 线程 — 从 Term Buffer 到网络

offer 完成后，消息已在 Term Buffer 中。接下来由 Driver 的 **Sender 线程** 将其发送到网络。

### sendData() — 扫描并发送

**核心流程（NetworkPublication.sendData）：**

```
1. 根据 senderPosition 计算当前 term 索引:
   activeIndex = indexByPosition(senderPosition, positionBitsToShift)

2. 用 TermScanner 扫描已完成的帧:
   scanOutcome = scanForAvailability(termBuffers[activeIndex], termOffset, scanLimit)
   available = available(scanOutcome)

3. 从 term buffer 切片发送 UDP:
   sendBuffer.limit(termOffset + available).position(termOffset)
   channelEndpoint.send(sendBuffer)  // → DatagramChannel.send()

4. 更新 senderPosition:
   senderPosition.setRelease(senderPosition + bytesSent)
```

**TermScanner.scanForAvailability**：从 `termOffset` 开始，逐帧读取 `frameLengthVolatile()`：
- 读到正值 → 帧已完成，累加 available 长度
- 读到 0 或负值 → 帧未完成或空位，停止扫描
- 读到 PADDING 帧 → 当前 term 结束，标记 padding

### 流控与 senderLimit

Sender 不会无限发送，受 `senderLimit` 约束：

```
availableWindow = senderLimit - senderPosition
scanLimit = min(availableWindow, mtuLength)
```

**senderLimit 更新路径：**

```
Receiver                     Network                Driver(Sender侧)
   │                           │                        │
   │  Status Message(SM)       │                        │
   │  ┌─consumptionPos──────┐  │                        │
   │  │ receiverWindowLength │  │                        │
   │  └─────────────────────┘  │                        │
   │──────────UDP──────────────►│                        │
   │                           │  onStatusMessage()     │
   │                           │───────────────────────►│
   │                           │                        │
   │                           │  flowControl.onSM()    │
   │                           │  senderLimit =         │
   │                           │   position + window    │
   │                           │◄───────────────────────│
```

---

## 关键数据结构

### Log Buffer 内存布局

一个 Publication 对应一个 log 文件（mmap 映射），结构如下：

```
Offset 0                                              文件末尾
├──────────────────┬──────────────────┬──────────────────┬─────────────┤
│     Term 0       │     Term 1       │     Term 2       │  Log Meta   │
│  (termLength)    │  (termLength)    │  (termLength)    │  Data(4KB)  │
├──────────────────┴──────────────────┴──────────────────┴─────────────┤
│              总长度 = 3 × termLength + LOG_META_DATA_LENGTH          │
└──────────────────────────────────────────────────────────────────────┘
```

**Log Meta Data 布局（4KB，关键字段）：**

```
Offset (相对 metaData 起始)
 +0    : Tail Counter 0  (8 bytes)  ← rawTail = termId << 32 | termOffset
 +8    : Tail Counter 1  (8 bytes)
 +16   : Tail Counter 2  (8 bytes)
 +24   : Active Term Count (4 bytes) ← volatile, term 轮转时 CAS 更新
 +128  : End Of Stream Position (8 bytes)
 +136  : Is Connected (4 bytes)     ← Driver 根据 SM 设置, offer 检查
 +256  : Correlation ID (8 bytes)
 +264  : Initial Term ID (4 bytes)
 +268  : Default Frame Header Length (4 bytes)
 +272  : MTU Length (4 bytes)
 +276  : Term Length (4 bytes)
 +280  : Page Size (4 bytes)
 +320  : Default Frame Header (128 bytes max)
```

### Data Frame 帧格式

每条消息在 Term Buffer 中以 **Data Frame** 格式存储，固定 32 字节帧头：

```
  0                   1                   2                   3
  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 |                         Frame Length                          |  offset  0
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 |    Version    |B|E|S|R|Flags  |            Type               |  offset  4
 +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
 |                         Term Offset                           |  offset  8
 +---------------------------------------------------------------+
 |                         Session ID                            |  offset 12
 +---------------------------------------------------------------+
 |                         Stream ID                             |  offset 16
 +---------------------------------------------------------------+
 |                         Term ID                               |  offset 20
 +---------------------------------------------------------------+
 |                         Reserved Value                        |  offset 24
 |                                                               |
 +---------------------------------------------------------------+
 |                         Payload (消息体)                      |  offset 32
 |                         ...                                   |
 +---------------------------------------------------------------+
```

**字段说明：**

| 字段 | 大小 | 说明 |
|------|------|------|
| Frame Length | 4B | 帧总长度（含头）。写入时先为**负值**，完成后覆写为**正值** |
| Version | 1B | 协议版本 |
| Flags | 1B | B=Begin(0x80), E=End(0x40), S=EOS(0x20), R=Revoked(0x10) |
| Type | 2B | 帧类型：DATA(0x01) 或 PADDING(0x02) |
| Term Offset | 4B | 帧在 term 内的起始偏移 |
| Session ID | 4B | Publication 的会话标识 |
| Stream ID | 4B | 流标识 |
| Term ID | 4B | 当前 term 的 ID（单调递增） |
| Reserved Value | 8B | 用户自定义保留值 |

**帧对齐：** 所有帧按 `FRAME_ALIGNMENT = 32` 字节对齐，不足部分补 0。

### rawTail 编码

```
 63                              32 31                               0
 ├───────────── termId ────────────┤├────────── termOffset ───────────┤
 │          (高 32 位)              ││          (低 32 位)              │
 └──────────────────────────────────┘└──────────────────────────────────┘
```

- `termId(rawTail)` = `(int)(rawTail >> 32)`
- `termOffset(rawTail, termLength)` = `min(rawTail & 0xFFFF_FFFF, termLength)`
- `packTail(termId, offset)` = `((long)termId << 32) | offset`

### Position 计算

Position 是**流级别的绝对字节位置**，单调递增，跨越所有 term：

```java
public static long computePosition(
    final int activeTermId, final int termOffset,
    final int positionBitsToShift, final int initialTermId)
{
    final long termCount = activeTermId - initialTermId;
    return (termCount << positionBitsToShift) + termOffset;
}
```

示例（termLength = 1MB = 2^20，positionBitsToShift = 20）：

```
Term 0 (termId=42): position = 0 × 2^20 + termOffset = 0 ~ 1,048,575
Term 1 (termId=43): position = 1 × 2^20 + termOffset = 1,048,576 ~ 2,097,151
Term 2 (termId=44): position = 2 × 2^20 + termOffset = 2,097,152 ~ 3,145,727
Term 0 (termId=45): position = 3 × 2^20 + termOffset = 3,145,728 ~ ...
```

---

## offer 返回值语义

| 返回值 | 常量名 | 含义 | 应对策略 |
|--------|--------|------|----------|
| `> 0` | — | 成功，值为新的流 position | 继续 |
| `-1` | `NOT_CONNECTED` | 无活跃订阅者 | 等待连接建立后重试 |
| `-2` | `BACK_PRESSURED` | 流控窗口已满 | 短暂 idle 后重试 |
| `-3` | `ADMIN_ACTION` | Term 轮转或其他管理操作 | 立即重试 |
| `-4` | `CLOSED` | Publication 已关闭 | 不可恢复 |
| `-5` | `MAX_POSITION_EXCEEDED` | 超过流最大位置（term×2^31） | 关闭后重建 Publication |

---

## 流程总结

### 一次成功的 offer 完整路径

```
1. 应用线程调用 publication.offer(buffer, 0, length)
      │
2. Publication.offer(buf, 0, len) → 委托给 ConcurrentPublication.offer()
      │
3. 读取 positionLimit（volatile）— 流控上限
   读取 activeTermCount（volatile）— 当前 term 序号
   读取 rawTail（volatile）— 当前写入位置
      │
4. 一致性校验：termCount == (termId - initialTermId)?
      │ 是
5. 计算 position，与 limit 比较
      │ position < limit
6. 判断消息长度 ≤ maxPayloadLength?
      │ 是（走 unfragmented 路径）
7. 计算帧长：frameLength = length + 32, alignedLength = align(frameLength, 32)
      │
8. getAndAddLong(tailCounterOffset, alignedLength) — 原子推进 tail，获得写入起点
      │
9. resultingOffset ≤ termLength?
      │ 是（term 有空间）
10. HeaderWriter.write() — 写帧头（负长度占位 + version/flags/type + session/stream/term）
      │
11. putBytes() — 拷贝消息体到 termOffset + 32
      │
12. frameLengthOrdered() — 将帧长覆写为正值（release 语义）
      │                        │
13. 返回 newPosition          ▼ 此刻帧对 Sender 线程可见
      │
      ▼                   ┌─────────────────────────────────────────┐
   应用线程继续            │  Sender 线程（Driver 进程/嵌入式线程）    │
                          │                                         │
                          │  14. TermScanner 扫描到正的 frameLength  │
                          │  15. 切片 sendBuffer，通过 UDP 发出      │
                          │  16. 更新 senderPosition                 │
                          │                                         │
                          │  17. Receiver 收到后发 Status Message     │
                          │  18. Driver 更新 positionLimit（流控窗口）│
                          └─────────────────────────────────────────┘
```

### 核心设计亮点

| 设计 | 说明 |
|------|------|
| **内存映射零拷贝** | Term Buffer 是 mmap 文件，应用线程直写，Sender 直读，无 IPC 拷贝 |
| **fetch-and-add 无锁** | `getAndAddLong` 一条原子指令推进 tail，无 CAS 重试循环 |
| **两阶段提交** | 负长度占位 → 写入内容 → 正长度发布，保证帧的原子可见性 |
| **三分区轮转** | 写 → 发送 → 清理，流水线式利用 term buffer |
| **端到端流控** | position/positionLimit 机制，基于 Status Message 反馈，防止覆盖未消费数据 |
| **帧对齐** | 32 字节对齐，利于 CPU cache line 和 SIMD 操作 |
| **分片透明** | 大消息自动分片，接收端自动重组，对应用透明 |

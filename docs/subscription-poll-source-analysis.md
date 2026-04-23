# `subscription.poll(handler, FRAGMENT_LIMIT)` 源码深度解析

## 目录

- [概述](#概述)
- [整体架构：发送与接收的对称设计](#整体架构发送与接收的对称设计)
- [完整调用链时序图](#完整调用链时序图)
- [第一层：DemoReceiver — 应用入口](#第一层demoreceiver--应用入口)
  - [接收循环与 Idle Strategy](#接收循环与-idle-strategy)
  - [FragmentHandler 回调](#fragmenthandler-回调)
- [第二层：Subscription.poll() — Image 轮询调度](#第二层subscriptionpoll--image-轮询调度)
  - [Round-Robin 公平调度](#round-robin-公平调度)
  - [配额分配机制](#配额分配机制)
- [第三层：Image.poll() — 核心读取逻辑](#第三层imagepoll--核心读取逻辑)
  - [Position 与 Term Buffer 选择](#position-与-term-buffer-选择)
  - [帧扫描循环](#帧扫描循环)
  - [frameLengthVolatile — 生产者-消费者同步点](#framelengthvolatile--生产者-消费者同步点)
  - [Padding 帧跳过](#padding-帧跳过)
  - [FragmentHandler 回调交付](#fragmenthandler-回调交付)
  - [subscriberPosition 推进与流控](#subscriberposition-推进与流控)
- [关键数据结构](#关键数据结构)
  - [Log Buffer 内存布局](#log-buffer-内存布局)
  - [Data Frame 帧格式](#data-frame-帧格式)
  - [Position 计算](#position-计算)
- [发送端 offer 与接收端 poll 的对称关系](#发送端-offer-与接收端-poll-的对称关系)
  - [offer 写入流程回顾](#offer-写入流程回顾)
  - [offer 与 poll 的生产者-消费者协议](#offer-与-poll-的生产者-消费者协议)
  - [端到端流控闭环](#端到端流控闭环)
- [完整端到端流程图](#完整端到端流程图)
- [关键设计总结](#关键设计总结)

---

## 概述

`subscription.poll(handler, FRAGMENT_LIMIT)` 是 Aeron **数据面（Data Plane）** 的核心读取 API。它是 **非阻塞** 的，应用线程调用 `poll` 从共享内存中的 **Term Buffer** 直接读取消息，无需经过 Driver 的命令通道。

与 `publication.offer()` 形成对称：

| 方面 | offer（发送） | poll（接收） |
|------|-------------|-------------|
| 操作对象 | Term Buffer 的写入 | Term Buffer 的读取 |
| 线程模型 | 应用线程直接写 | 应用线程直接读 |
| 同步机制 | `frameLengthOrdered`（release 写） | `frameLengthVolatile`（acquire 读） |
| 流控方向 | 受 `positionLimit` 约束 | 推进 `subscriberPosition` 反馈 |
| 位置跟踪 | 返回新 position | 推进 subscriberPosition |

**核心设计思想：**
- **零拷贝路径**：应用线程直接从 mmap 的 Term Buffer 中读取帧数据
- **拉取模型（Pull）**：应用自主控制读取节奏，而非被推送
- **背压传递**：`subscriberPosition` 推进越慢，Publisher 越容易被 `BACK_PRESSURED`
- **Round-Robin 公平**：多个 Publisher（多个 Image）间轮流读取，避免饥饿

---

## 整体架构：发送与接收的对称设计

```
 发送端应用线程                    共享内存（Term Buffer）                 接收端应用线程
      │                                                                    │
      │  publication.offer(buf,0,len)                                      │
      │         │                                                          │
      │   ┌─────▼──────────────┐                                          │
      │   │ 1. 读 positionLimit │                                          │
      │   │ 2. 原子推进 tail    │                                          │
      │   │ 3. 写帧头(负length) │                                          │
      │   │ 4. 拷贝 payload     │                                          │
      │   │ 5. frameLengthOrdered│──── release 语义 ────┐                  │
      │   │    (正 length)       │                       │                  │
      │   └────────────────────┘                       │                  │
      │                                                 ▼                  │
      │                              ┌──────────────────────────┐          │
      │                              │   Term Buffer (mmap)      │          │
      │                              │                            │          │
      │                              │  ┌─────────┬────────────┐ │          │
      │                              │  │Frame Hdr│  Payload   │ │          │
      │                              │  │(32 byte)│            │ │          │
      │                              │  └─────────┴────────────┘ │          │
      │                              └──────────────────────────┘          │
      │                                                 │                  │
      │                                                 │                  │
      │                                    ┌────────────┘                  │
      │                                    │  acquire 语义                  │
      │                                    ▼                               │
      │                              ┌──────────────────┐                  │
      │                              │frameLengthVolatile│──────►subscription.poll()
      │                              │  > 0 → 帧可读    │         │
      │                              │  ≤ 0 → 未完成    │   ┌─────▼────────────┐
      │                              └──────────────────┘   │ 1. 读subscriberPos │
      │                                                      │ 2. 选 termBuffer   │
      │                                                      │ 3. 扫描帧          │
      │                                                      │ 4. 回调 handler    │
      │                                                      │ 5. 推进 subPos     │
      │                                                      └──────────────────┘
      │                                                               │
      │◄─────────── subscriberPosition 反馈 ─────────────────────────┘
      │             (Driver 据此更新 positionLimit)
```

---

## 完整调用链时序图

```
DemoReceiver                 Subscription               Image                    Term Buffer
    │                            │                        │                          │
    │  poll(handler, 10)         │                        │                          │
    │───────────────────────────►│                        │                          │
    │                            │ images[] 快照           │                          │
    │                            │ roundRobinIndex++       │                          │
    │                            │                        │                          │
    │                            │ poll(handler, 10)      │                          │
    │                            │───────────────────────►│                          │
    │                            │                        │ subscriberPosition.get() │
    │                            │                        │ initialOffset = pos & mask│
    │                            │                        │ activeTermBuffer(pos)     │
    │                            │                        │                          │
    │                            │                        │  ┌─── 帧扫描循环 ───┐    │
    │                            │                        │  │                    │    │
    │                            │                        │  │ frameLengthVolatile│───►│
    │                            │                        │  │  > 0? ────────────│────│
    │                            │                        │  │                    │    │
    │                            │                        │  │ isPaddingFrame?    │───►│
    │                            │                        │  │  N → onFragment() │    │
    │  onFragment(buf,off,len,h) │                        │  │       │           │    │
    │◄───────────────────────────│────────────────────────│──│───────┘           │    │
    │  处理消息 + 打印            │                        │  │                    │    │
    │                            │                        │  │ offset += align()  │    │
    │                            │                        │  │                    │    │
    │                            │                        │  └────── 继续循环 ───┘    │
    │                            │                        │                          │
    │                            │                        │ subscriberPosition       │
    │                            │                        │   .setRelease(newPos)    │
    │                            │                        │                          │
    │                            │  return fragmentsRead  │                          │
    │                            │◄───────────────────────│                          │
    │  return totalFragments     │                        │                          │
    │◄───────────────────────────│                        │                          │
    │                            │                        │                          │
    │  idle.idle(fragmentsRead)  │                        │                          │
    │  (= 0 → sleep 1ms)        │                        │                          │
```

---

## 第一层：DemoReceiver — 应用入口

### 接收循环与 Idle Strategy

```java
// DemoReceiver.java
while (running.get())                                   // 主循环：直到 Ctrl+C
{
    final int fragmentsRead = subscription.poll(        // 最多读取 FRAGMENT_LIMIT(10) 个 fragment
        handler, FRAGMENT_LIMIT);
    idle.idle(fragmentsRead);                           // fragmentsRead > 0 不休眠，= 0 时 sleep 1ms
}
```

**设计要点：**
- `FRAGMENT_LIMIT = 10`：每次 poll 最多处理 10 个 fragment，防止单次 poll 耗时过长
- `SleepingIdleStrategy(1ms)`：无数据时 sleep 节省 CPU，适合 demo；低延迟场景用 `BusySpinIdleStrategy`
- `poll` 返回值直接驱动 idle strategy：有工作就继续，无工作就等待

### FragmentHandler 回调

```java
final FragmentHandler handler = (buffer, offset, length, header) ->
{
    final byte[] bytes = new byte[length];              // 分配 byte[] 接收 payload
    buffer.getBytes(offset, bytes);                     // 从 termBuffer 拷贝 payload
    final String message = new String(bytes, StandardCharsets.UTF_8);
    System.out.println("  [收到] " + message);
};
```

**参数说明：**

| 参数 | 类型 | 含义 |
|------|------|------|
| `buffer` | `DirectBuffer` | 当前 termBuffer 的视图，消息数据就在其中 |
| `offset` | `int` | payload 起始偏移 = `frameOffset + HEADER_LENGTH(32)` |
| `length` | `int` | payload 长度 = `frameLength - HEADER_LENGTH` |
| `header` | `Header` | 帧元数据：可获取 `sessionId`, `streamId`, `termId`, `position()` 等 |

---

## 第二层：Subscription.poll() — Image 轮询调度

**源码位置：** `aeron-client/src/main/java/io/aeron/Subscription.java`

```java
public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
{
    final Image[] images = this.images;                 // ① volatile 读取 Image[] 快照
    final int length = images.length;                   // Image 数量（每个 Image 对应一个 Publisher session）
    int fragmentsRead = 0;                              // 累计已读 fragment 数

    int startingIndex = roundRobinIndex++;              // ② 取上次轮询起始下标并自增
    if (startingIndex >= length)                        // 越界检查（Image 数组可能缩小）
    {
        roundRobinIndex = startingIndex = 0;            // 重置为 0
    }

    // ③ 从 startingIndex 向后遍历
    for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
    {
        fragmentsRead += images[i].poll(                // 委托 Image.poll 读取
            fragmentHandler, fragmentLimit - fragmentsRead);  // 剩余配额
    }

    // ④ 绕回：遍历 startingIndex 之前的 Image
    for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
    {
        fragmentsRead += images[i].poll(
            fragmentHandler, fragmentLimit - fragmentsRead);
    }

    return fragmentsRead;                               // 返回总消费数
}
```

### Round-Robin 公平调度

```
假设 3 个 Image（3 个 Publisher），fragmentLimit = 10

第 1 次 poll：startingIndex = 0
  Image[0] → 读到 4 个，剩余配额 6
  Image[1] → 读到 3 个，剩余配额 3
  Image[2] → 读到 3 个，配额用完
  roundRobinIndex 变为 1

第 2 次 poll：startingIndex = 1
  Image[1] → 先从上次的下一个开始
  Image[2] → ...
  Image[0] → 绕回
  roundRobinIndex 变为 2

→ 每次 poll 从不同 Image 开始，保证公平
```

### 配额分配机制

- `fragmentLimit - fragmentsRead`：每个 Image 能读取的上限 = 总配额 - 已读数量
- 若某个 Image 无数据（返回 0），不消耗配额，下一个 Image 能获得全部剩余配额
- 这是**弹性分配**：忙的 Image 可以用完配额，闲的 Image 不浪费

---

## 第三层：Image.poll() — 核心读取逻辑

**源码位置：** `aeron-client/src/main/java/io/aeron/Image.java`

这是整个 poll 流程的核心方法，直接从 Term Buffer 中读取帧并回调应用。

```java
public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
{
    if (isClosed)                                       // ① 快速检查：Image 已关闭则直接返回
    {
        return 0;
    }

    int fragmentsRead = 0;
    final long initialPosition = subscriberPosition.get();          // ② 读取当前消费位置
    final int initialOffset = (int)initialPosition & termLengthMask;// ③ 计算 term 内偏移
    int offset = initialOffset;                                     // 扫描游标
    final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);  // ④ 选择活跃 term buffer
    final int capacity = termBuffer.capacity();
    final Header header = this.header;
    header.buffer(termBuffer);                                      // 绑定 header 到当前 term

    try
    {
        while (fragmentsRead < fragmentLimit && offset < capacity && !isClosed)  // ⑤ 帧扫描循环
        {
            final int frameLength = frameLengthVolatile(termBuffer, offset);     // ⑥ volatile 读帧长度
            if (frameLength <= 0)                                   // ⑦ <= 0 → 到达写入前沿
            {
                break;
            }

            final int frameOffset = offset;
            offset += BitUtil.align(frameLength, FRAME_ALIGNMENT);  // ⑧ 推进到下一帧

            if (!isPaddingFrame(termBuffer, frameOffset))           // ⑨ 跳过 padding 帧
            {
                ++fragmentsRead;
                header.offset(frameOffset);
                fragmentHandler.onFragment(                         // ⑩ 回调应用 handler
                    termBuffer,
                    frameOffset + HEADER_LENGTH,                    // payload 起始
                    frameLength - HEADER_LENGTH,                    // payload 长度
                    header);
            }
        }
    }
    catch (final Exception ex)
    {
        errorHandler.onError(ex);                                   // ⑪ 异常处理
    }
    finally
    {
        final long newPosition = initialPosition + (offset - initialOffset);
        if (newPosition > initialPosition && !isClosed)
        {
            subscriberPosition.setRelease(newPosition);             // ⑫ 推进消费位置
        }
    }

    return fragmentsRead;
}
```

### Position 与 Term Buffer 选择

```java
final long initialPosition = subscriberPosition.get();              // 绝对字节位置，单调递增
final int initialOffset = (int)initialPosition & termLengthMask;    // position % termLength
final UnsafeBuffer termBuffer = activeTermBuffer(initialPosition);  // termBuffers[indexByPosition()]
```

**计算逻辑：**
```
termLength = 64KB（默认，2 的幂次）
termLengthMask = termLength - 1 = 0xFFFF

position = 131200（绝对字节位置）
initialOffset = 131200 & 0xFFFF = 131200 % 65536 = 128 → term 内偏移 128 字节处

termIndex = (position >>> positionBitsToShift) % 3 = (131200 >>> 16) % 3 = 2 % 3 = 2
→ 使用 termBuffers[2]
```

### 帧扫描循环

```
Term Buffer 布局（从 initialOffset 开始扫描）：

offset ─────────────────────────────────────────────────► capacity
   │                                                         │
   ▼                                                         ▼
   ┌──────────────┬──────────────┬──────────────┬───────────┐
   │  Frame #1    │  Frame #2    │  Frame #3    │  未写入    │
   │ len=64       │ len=96       │ len=0        │           │
   │ (data)       │ (data)       │ (未完成)      │           │
   └──────────────┴──────────────┴──────────────┴───────────┘
                                   ▲
                                   │
                            frameLengthVolatile <= 0
                            → break，退出循环
```

**循环终止条件（任一满足）：**
1. `fragmentsRead >= fragmentLimit`：配额用完
2. `offset >= capacity`：到达 term 末尾
3. `isClosed`：Image 被关闭
4. `frameLength <= 0`：到达写入前沿（Producer 尚未完成此帧）

### frameLengthVolatile — 生产者-消费者同步点

```java
// FrameDescriptor.java
public static int frameLengthVolatile(final UnsafeBuffer buffer, final int termOffset)
{
    int frameLength = buffer.getIntVolatile(termOffset);    // volatile 读帧头第一个 int
    if (ByteOrder.nativeOrder() != LITTLE_ENDIAN)
    {
        frameLength = Integer.reverseBytes(frameLength);    // 大端平台需翻转字节序
    }
    return frameLength;
}
```

**这是生产者-消费者协议的核心同步点：**

| 生产者（offer）| 消费者（poll） |
|---------------|---------------|
| `headerWriter.write()` 写入**负的** frameLength | `frameLengthVolatile()` 读到 **≤ 0** → 帧未完成，跳过 |
| 写帧头、拷贝 payload... | — |
| `frameLengthOrdered()` 将 frameLength 改为**正值**（release 语义） | `frameLengthVolatile()` 读到 **> 0** → 帧可读 |

**内存序保证：**
- 生产者：`frameLengthOrdered` = `putIntRelease` → store-release 语义
- 消费者：`frameLengthVolatile` = `getIntVolatile` → load-acquire 语义
- 形成 **release-acquire 对**：消费者读到正的 frameLength 时，帧头和 payload 的所有写入都已对消费者可见

### Padding 帧跳过

```java
if (!isPaddingFrame(termBuffer, frameOffset))   // 检查帧类型是否为 PADDING
```

**Padding 帧产生的原因：**
- 当 `offer` 写入时发现剩余 term 空间不够放下当前帧
- 生产者在 `handleEndOfLog` 中用 padding 帧填充 term 末尾的剩余空间
- 消费者遇到 padding 帧时只推进 offset，不计入 `fragmentsRead`，也不回调 handler

### FragmentHandler 回调交付

```java
fragmentHandler.onFragment(
    termBuffer,                          // buffer: 当前 term buffer（直接引用，零拷贝）
    frameOffset + HEADER_LENGTH,         // offset: 跳过 32 字节帧头
    frameLength - HEADER_LENGTH,         // length: 纯 payload 长度
    header);                             // header: 帧元数据
```

**注意：** 回调中的 `buffer` 是 term buffer 的直接引用，不是拷贝。如果需要在回调外保留数据，必须自行拷贝（如 demo 中的 `buffer.getBytes(offset, bytes)`）。

### subscriberPosition 推进与流控

```java
finally
{
    final long newPosition = initialPosition + (offset - initialOffset);
    if (newPosition > initialPosition && !isClosed)
    {
        subscriberPosition.setRelease(newPosition);     // release 语义推进位置
    }
}
```

**关键行为：**
1. `newPosition = initialPosition + 扫描推进的字节数`（包括 padding 帧的字节）
2. 即使 handler 抛出异常，`finally` 仍会推进 position（避免重复读取导致无限循环）
3. `setRelease`：store-release 语义，确保所有读取操作对 Driver 可见
4. Driver 读取此 position 后更新 `positionLimit` → Publisher 据此获得更多写入空间

---

## 关键数据结构

### Log Buffer 内存布局

```
┌─────────────────────────────────────────────┐
│              Log Buffer（mmap 文件）          │
├─────────────────────────────────────────────┤
│  Term Buffer 0  │  Term Buffer 1  │  Term 2 │  ← 3 个等大的 term partition
├─────────────────────────────────────────────┤
│              Meta Data Buffer                │  ← tail counters, positions, 状态
└─────────────────────────────────────────────┘

发送端写入：                         接收端读取：
  offer → termBuffers[index]          poll → activeTermBuffer(position)
  index = termCount % 3               index = (position >>> shift) % 3
```

### Data Frame 帧格式

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                         Frame Length                          |  ← frameLengthVolatile 读的就是这个
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
|  Version      |B|E| Flags     |             Type              |  ← B=BEGIN, E=END（分片标志）
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-------------------------------+
|                         Term Offset                           |
+---------------------------------------------------------------+
|                         Session ID                            |
+---------------------------------------------------------------+
|                         Stream ID                             |
+---------------------------------------------------------------+
|                          Term ID                              |
+---------------------------------------------------------------+
|                       Reserved Value                          |
|                                                               |
+---------------------------------------------------------------+
|                      Payload (消息体)                         |
|                          ...                                  |
+---------------------------------------------------------------+
|                    Padding (对齐到 32 字节)                    |
+---------------------------------------------------------------+

帧头固定 32 字节 (HEADER_LENGTH)
对齐单位 32 字节 (FRAME_ALIGNMENT)
```

**帧分片标志（Flags 字段）：**

| 标志 | 值 | 含义 |
|------|------|------|
| `BEGIN_FRAG_FLAG` | `0x80` | 消息的第一个 fragment |
| `END_FRAG_FLAG` | `0x40` | 消息的最后一个 fragment |
| `UNFRAGMENTED` | `0xC0` | 完整消息（BEGIN + END 都设置） |
| `0x00` | `0x00` | 消息的中间 fragment |

### Position 计算

```
position = (termId - initialTermId) << positionBitsToShift + termOffset
         = termCount * termLength + termOffset

示例（termLength = 64KB = 65536）：
  termId = 5, initialTermId = 0, termOffset = 1024
  positionBitsToShift = 16（因为 65536 = 2^16）
  position = (5 - 0) << 16 + 1024 = 5 * 65536 + 1024 = 328704
```

---

## 发送端 offer 与接收端 poll 的对称关系

### offer 写入流程回顾

```java
// ConcurrentPublication.offer() 简化流程
public long offer(DirectBuffer buffer, int offset, int length, ReservedValueSupplier rvs)
{
    long limit = positionLimit.getVolatile();            // ① 读流控上限（subscriberPosition 驱动）
    int termCount = activeTermCount(logMetaDataBuffer);  // ② 读当前活跃 term
    int index = indexByTermCount(termCount);              // ③ 选 term buffer
    long rawTail = logMetaDataBuffer.getLongVolatile(..); // ④ 读 tail
    long position = computePosition(termId, termOffset, ...);

    if (position < limit)                                // ⑤ 流控检查
    {
        if (length <= maxPayloadLength)                  // ⑥ 单帧 or 分片
        {
            // appendUnfragmentedMessage:
            rawTail = getAndAddLong(tailCounterOffset, alignedLength);  // ⑦ 原子推进 tail
            headerWriter.write(termBuffer, termOffset, frameLength, termId);  // ⑧ 写帧头（负 length）
            termBuffer.putBytes(termOffset + HEADER_LENGTH, buffer, offset, length);  // ⑨ 拷贝 payload
            frameLengthOrdered(termBuffer, termOffset, frameLength);  // ⑩ 发布帧（正 length，release）
        }
    }
    else
    {
        return backPressureStatus(position, length);     // BACK_PRESSURED / NOT_CONNECTED
    }
}
```

### offer 与 poll 的生产者-消费者协议

```
时间线 ──────────────────────────────────────────────────────────►

生产者线程（offer）:
  │
  │  getAndAddLong(tail, alignedLen)    ← 原子预留空间
  │         │
  │  headerWriter.write(... -length)    ← 写帧头（负 length = "帧未完成"）
  │         │
  │  putBytes(payload)                  ← 拷贝消息体
  │         │
  │  frameLengthOrdered(+length)        ← release 写正 length = "帧可读"
  │         │                               │
  ▼         ▼                               ▼ (release-acquire 同步)

消费者线程（poll）:
                                            │
                               frameLengthVolatile()  ← acquire 读
                                            │
                                      > 0? ──► 是：帧可读，处理
                                            │
                                      ≤ 0? ──► 否：帧未完成，退出循环
```

**协议保证：**
1. 消费者看到正的 `frameLength` 时，帧头和 payload 的所有写入都已对消费者可见（release-acquire）
2. 消费者看到 `frameLength ≤ 0` 时，要么帧未开始写入（0），要么正在写入中（负值）
3. padding 帧（`frameType = PADDING_FRAME_TYPE`）告诉消费者跳过 term 末尾空白区

### 端到端流控闭环

```
┌─── Publisher ────┐     ┌──── Term Buffer ────┐     ┌──── Subscriber ───┐
│                  │     │                      │     │                   │
│ offer()          │     │  ┌───┬───┬───┬───┐  │     │    poll()         │
│   │              │     │  │F1 │F2 │F3 │...│  │     │      │            │
│   │ position     │     │  └───┴───┴───┴───┘  │     │      │            │
│   │ < limit? ◄───│─────│── positionLimit ◄────│─────│── subscriberPos  │
│   │              │     │                      │     │      │            │
│   │ Y: 写入      │     │  tail ──────────►    │     │  ◄── 扫描 ──     │
│   │ N: BACK_     │     │                      │     │      │            │
│   │   PRESSURED  │     │                      │     │ setRelease(pos)  │
│   │              │     │                      │     │      │            │
└──────────────────┘     └──────────────────────┘     └──────────────────┘
                                    │
                                    ▼
                         ┌──── Driver ──────────┐
                         │                      │
                         │ Sender: 读 term,     │
                         │   发 UDP 数据包       │
                         │                      │
                         │ Receiver: 收 UDP,    │
                         │   写入接收端 term      │
                         │                      │
                         │ Flow Control:        │
                         │   读 subscriberPos   │
                         │   更新 positionLimit  │
                         └──────────────────────┘

流控闭环：
  subscriber 消费快 → subscriberPosition 推进快
  → Driver 更新 positionLimit 增大
  → publisher 有更多写入空间

  subscriber 消费慢 → subscriberPosition 推进慢
  → positionLimit 增长慢
  → publisher position >= limit → BACK_PRESSURED
```

---

## 完整端到端流程图

以 DemoSender 发送 `"Hello"` → DemoReceiver 接收并打印为例：

```
DemoSender                                                 DemoReceiver
    │                                                          │
    │  1. "Hello".getBytes(UTF-8) → 5 字节                     │
    │  2. buffer.putBytes(0, bytes)                             │
    │                                                          │
    │  3. publication.offer(buffer, 0, 5)                       │
    │     │                                                    │
    │     ├─ Publication.offer(buf, 0, 5)                      │
    │     │    → offer(buf, 0, 5, null)                        │
    │     │                                                    │
    │     ├─ ConcurrentPublication.offer()                     │
    │     │    ├─ positionLimit = 131072 (128KB)               │
    │     │    ├─ termCount = 0                                │
    │     │    ├─ termBuffer = termBuffers[0]                  │
    │     │    ├─ rawTail = 0x00000000_00000000                │
    │     │    ├─ position = 0                                 │
    │     │    ├─ 0 < 131072 → 流控通过                        │
    │     │    ├─ 5 ≤ maxPayloadLength → 单帧                  │
    │     │    │                                               │
    │     │    ├─ appendUnfragmentedMessage()                  │
    │     │    │    ├─ frameLength = 5 + 32 = 37               │
    │     │    │    ├─ alignedLength = align(37, 32) = 64      │
    │     │    │    ├─ FAA: tail 0 → 64                        │
    │     │    │    ├─ headerWriter.write(buf, 0, 37, termId0) │
    │     │    │    │    └─ frameLength = -37 (帧未完成)        │
    │     │    │    ├─ putBytes(32, payload, 0, 5)             │
    │     │    │    │    └─ "Hello" 写入 offset 32~36          │
    │     │    │    └─ frameLengthOrdered(buf, 0, 37)          │
    │     │    │         └─ frameLength = +37 (帧可读！)        │
    │     │    │                                               │
    │     │    └─ return position = 64                         │
    │     │                                                    │
    │  4. result = 64 > 0 → "✓ 已发送 (5 字节)"               │
    │                                                          │
    │  ═══════════ Driver Sender 线程 ══════════════           │
    │  5. 扫描 termBuffer[0] 发现新帧 → 封装 UDP → 发送         │
    │  ═══════════ Driver Receiver 线程 ═══════════            │
    │                                        6. 收到 UDP 包     │
    │                                        7. 写入接收端 term  │
    │                                           buffer          │
    │                                                          │
    │                                        8. subscription.poll(handler, 10)
    │                                           │
    │                                           ├─ Subscription.poll()
    │                                           │    ├─ images[0].poll(handler, 10)
    │                                           │    │
    │                                           │    ├─ Image.poll()
    │                                           │    │    ├─ subscriberPosition = 0
    │                                           │    │    ├─ initialOffset = 0
    │                                           │    │    ├─ termBuffer = activeTermBuffer(0)
    │                                           │    │    │
    │                                           │    │    ├─ 帧扫描循环:
    │                                           │    │    │    ├─ frameLengthVolatile(buf, 0) = 37
    │                                           │    │    │    ├─ 37 > 0 → 帧可读
    │                                           │    │    │    ├─ isPaddingFrame? → No
    │                                           │    │    │    ├─ fragmentsRead = 1
    │                                           │    │    │    ├─ handler.onFragment(buf, 32, 5, header)
    │                                           │    │    │    │    ├─ bytes = new byte[5]
    │                                           │    │    │    │    ├─ buf.getBytes(32, bytes) → "Hello"
    │                                           │    │    │    │    └─ println("[收到] Hello")
    │                                           │    │    │    │
    │                                           │    │    │    ├─ offset = 0 + 64 = 64
    │                                           │    │    │    └─ frameLengthVolatile(buf, 64) = 0
    │                                           │    │    │         → 无更多帧，break
    │                                           │    │    │
    │                                           │    │    └─ subscriberPosition.setRelease(64)
    │                                           │    │         → Driver 据此更新流控
    │                                           │    │
    │                                           │    └─ return 1
    │                                           │
    │                                        9. idle.idle(1)  → 不休眠，立即继续下一轮
```

---

## 关键设计总结

### 1. 零拷贝 + 共享内存

| 环节 | 拷贝次数 |
|------|---------|
| `offer`: 应用 buffer → Term Buffer | 1 次 `putBytes` |
| Driver Sender: Term Buffer → 网络 | 0 次（直接从 mmap 读） |
| Driver Receiver: 网络 → Term Buffer | 1 次（写入 mmap） |
| `poll`: Term Buffer → 应用 | 0 次（直接引用 term buffer）|

### 2. 无锁并发（Lock-Free）

- **写入端**（ConcurrentPublication）：`getAndAddLong` 原子推进 tail，多线程无锁并发写入
- **读取端**（Image）：单线程读取，无需同步（Image 不能跨线程共享）
- **生产者-消费者同步**：`frameLengthOrdered` / `frameLengthVolatile` = release-acquire 对

### 3. 端到端流控

```
                positionLimit
                    │
    position ──────┤──────────── subscriberPosition
    (写入位)       │             (消费位)
                    │
                   窗口 = positionLimit - position

position >= positionLimit → BACK_PRESSURED（写入端阻塞）
subscriberPosition 推进 → positionLimit 随之增大 → 写入端解除背压
```

### 4. 容错设计

| 场景 | 处理方式 |
|------|---------|
| Handler 抛异常 | `catch` 交给 errorHandler，`finally` 仍推进 position（避免卡死） |
| Image 被关闭 | 循环条件检查 `!isClosed`，`finally` 中检查后不推进 |
| Term 空间不足 | 生产者写 padding 帧，消费者遇到后跳过并推进 position |
| 大消息分片 | 生产者设置 BEGIN/END flags，消费者可用 `FragmentAssembler` 重组 |

### 5. 性能关键路径

`Image.poll()` 的热路径（每个 fragment）只涉及：
1. 一次 `volatile` 读（`frameLengthVolatile`）
2. 一次类型检查（`isPaddingFrame`）
3. 一次回调（`onFragment`）
4. 一次加法 + 位对齐（`offset += align(frameLength, 32)`）

最终 `finally` 中一次 `release` 写推进 position。整个过程**无锁、无系统调用、无分配**（除了应用回调中的操作）。

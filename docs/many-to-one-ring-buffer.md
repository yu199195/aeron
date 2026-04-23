# ManyToOneRingBuffer 深度源码解析

> 源码来自 Agrona 库 (`org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer`)，版本 2.4.0。
> 这是 Aeron 中 Client ↔ Driver 通信的**基础数据结构**，承载了 CnC 文件中的 toDriverBuffer。

## 目录

- [1. 是什么：一句话总结](#1-是什么一句话总结)
- [2. 为什么需要它](#2-为什么需要它)
- [3. 整体内存布局](#3-整体内存布局)
  - [3.1 Buffer 整体结构](#31-buffer-整体结构)
  - [3.2 消息记录（Record）结构](#32-消息记录record结构)
  - [3.3 Trailer 区域详细布局](#33-trailer-区域详细布局)
- [4. 核心原理：无锁并发写入](#4-核心原理无锁并发写入)
  - [4.1 关键变量](#41-关键变量)
  - [4.2 Head/Tail 位置的语义](#42-headtail-位置的语义)
  - [4.3 为什么 capacity 必须是 2 的幂](#43-为什么-capacity-必须是-2-的幂)
- [5. 写入流程（生产者端）](#5-写入流程生产者端)
  - [5.1 write() 一步完成写入](#51-write-一步完成写入)
  - [5.2 tryClaim() + commit() 两阶段写入](#52-tryclaim--commit-两阶段写入)
  - [5.3 claimCapacity() 核心：CAS 竞争空间](#53-claimcapacity-核心cas-竞争空间)
  - [5.4 消息长度的"负数技巧"](#54-消息长度的负数技巧)
  - [5.5 Buffer 尾部 Wrap-around 处理](#55-buffer-尾部-wrap-around-处理)
- [6. 读取流程（消费者端）](#6-读取流程消费者端)
  - [6.1 read() 方法解析](#61-read-方法解析)
  - [6.2 批量消费 + 零拷贝回调](#62-批量消费--零拷贝回调)
- [7. headCachePosition 优化](#7-headcacheposition-优化)
- [8. unblock() 死亡生产者恢复](#8-unblock-死亡生产者恢复)
- [9. 与 Java ConcurrentLinkedQueue 的对比](#9-与-java-concurrentlinkedqueue-的对比)
- [10. 在 Aeron 中的应用](#10-在-aeron-中的应用)

---

## 1. 是什么：一句话总结

**ManyToOneRingBuffer 是一个无锁、固定大小、基于共享内存的环形缓冲区，支持多个生产者线程并发写入，单个消费者线程顺序读取。**

```
Producer-1 ──┐
Producer-2 ──┤──CAS竞争写入──▶ [Ring Buffer (mmap)] ──单线程读取──▶ Consumer
Producer-N ──┘
```

## 2. 为什么需要它

在 Aeron 中，多个 Client 线程可能同时调用 `addPublication()`、`addSubscription()` 等方法，这些命令都需要写入 CnC 文件的 toDriverBuffer。Driver 的 Conductor 线程作为唯一消费者读取并处理这些命令。

传统方案（如加锁队列、ConcurrentLinkedQueue）的问题：

| 方案 | 问题 |
|------|------|
| synchronized / Lock | 线程阻塞，延迟不可预测 |
| ConcurrentLinkedQueue | GC 压力（每次 offer 都分配 Node 对象） |
| BlockingQueue | 阻塞 + 锁竞争 |

ManyToOneRingBuffer 的优势：
- **无锁**：写入用 CAS，读取无竞争（单消费者）
- **零 GC**：固定大小数组，无任何对象分配
- **零拷贝**：基于 mmap，生产者写入即对消费者可见
- **Cache 友好**：顺序内存访问，关键字段 cache line 隔离

---

## 3. 整体内存布局

### 3.1 Buffer 整体结构

```
地址低 ──────────────────────────────────────────────────── 地址高
┌─────────────────────────────────────────┬──────────────────┐
│             Data Area                   │     Trailer       │
│         (消息数据区)                     │   (元数据尾部)     │
│     容量 = capacity (2^N 字节)          │   768 字节        │
│                                         │                  │
│  ┌──────┐┌──────┐┌──────┐┌──────┐      │ tail position    │
│  │ msg1 ││ msg2 ││ msg3 ││ ...  │      │ headCache pos    │
│  └──────┘└──────┘└──────┘└──────┘      │ head position    │
│                                         │ correlation ctr  │
│  ← head 指向此处    tail 指向此处 →     │ consumer heartbt │
│    (消费者读取位置)  (生产者写入位置)    │                  │
└─────────────────────────────────────────┴──────────────────┘
│◄──────── capacity (2的幂) ────────────▶│◄── TRAILER_LENGTH ─▶│
│◄──────────────── buffer.capacity() ────────────────────────▶│
```

**总 buffer 大小 = capacity（2 的幂） + TRAILER_LENGTH（768 字节）**

### 3.2 消息记录（Record）结构

每条消息在 ring buffer 中存储为一个 Record：

```
Record 布局（RecordDescriptor）：

  偏移量   大小    字段       说明
  ──────  ─────  ──────     ──────────────────────────────
  +0      4 字节  Length     记录总长度（含 header），写入时先设为负值
  +4      4 字节  Type       消息类型 ID（如 ADD_SUBSCRIPTION = 0x04）
  +8      变长    Message    实际消息内容（Flyweight 序列化的命令数据）

  总长度对齐到 ALIGNMENT (8 字节)

示例：一条 24 字节消息的存储：
  ┌─────────┬─────────┬────────────────────────────────┐
  │ Len=32  │ Type=4  │  24 bytes message payload       │
  │ (4B)    │ (4B)    │  (24B)                          │
  └─────────┴─────────┴────────────────────────────────┘
  │◄──── header (8B) ────▶│◄──── payload (24B) ────────▶│
  │◄──────────── total: 32B (aligned to 8) ────────────▶│
```

**Length 字段的"正负号"语义（核心设计）：**

| Length 值 | 含义 |
|-----------|------|
| **负数** (-N) | 空间已被生产者 claim，但消息尚未写完（消费者必须等待） |
| **正数** (+N) | 消息已完整写入，消费者可以安全读取 |
| **0** | 空闲空间（未被使用） |

### 3.3 Trailer 区域详细布局

> 源码位置：`RingBufferDescriptor.java`

```
Trailer 偏移量（相对于 capacity 起始）：

  偏移      大小          字段名                    说明
  ──────   ──────       ──────────                ──────────────────
  +0       128 字节     (padding)                 隔离 data area 和 trailer
  +128     128 字节     tail position             生产者写入位置（CAS 原子更新）
  +256     128 字节     head cache position       head 的缓存副本（减少 volatile read）
  +384     128 字节     head position             消费者读取位置
  +512     128 字节     correlation counter       correlationId 原子递增计数器
  +640     128 字节     consumer heartbeat        消费者心跳时间戳
  ──────
  总计: 768 字节 (TRAILER_LENGTH = 6 × 128)
```

**为什么每个字段占 128 字节（2 个 cache line）？**

为了**消除 false sharing**。现代 CPU 的 cache line 通常是 64 字节。如果 `tailPosition` 和 `headPosition` 在同一 cache line 上，生产者更新 tail 时会导致消费者所在 core 的 cache line 失效，即使消费者只需要读 head。128 字节的间隔保证了它们必定在不同的 cache line 上。

```
Core 0 (Producer)          Core 1 (Consumer)
┌────────────┐             ┌────────────┐
│ L1 Cache   │             │ L1 Cache   │
│ ┌────────┐ │             │ ┌────────┐ │
│ │ tail   │ │  ← 独立     │ │ head   │ │  ← 独立
│ │cacheLine│ │  cache line │ │cacheLine│ │  cache line
│ └────────┘ │             │ └────────┘ │
└────────────┘             └────────────┘
        ↕ 无互相失效           ↕ 无互相失效
```

---

## 4. 核心原理：无锁并发写入

### 4.1 关键变量

```java
// Data 区域
private final int capacity;          // 数据区大小（2 的幂）
private final AtomicBuffer buffer;   // 底层 buffer（通常是 mmap 的 UnsafeBuffer）

// Trailer 中各字段在 buffer 中的绝对索引
private final int tailPositionIndex;         // = capacity + TAIL_POSITION_OFFSET
private final int headCachePositionIndex;    // = capacity + HEAD_CACHE_POSITION_OFFSET
private final int headPositionIndex;         // = capacity + HEAD_POSITION_OFFSET
private final int correlationIdCounterIndex; // = capacity + CORRELATION_COUNTER_OFFSET
private final int consumerHeartbeatIndex;    // = capacity + CONSUMER_HEARTBEAT_OFFSET
```

### 4.2 Head/Tail 位置的语义

```
位置变量的语义：

  head                        tail
    │                           │
    ▼                           ▼
    ┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
    │ ■ │ ■ │ ■ │ ■ │ ■ │   │   │   │   │   │
    └───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
    │◄── 已写入未消费 ──▶│◄── 空闲空间 ──▶│

  head: 消费者下一次读取的位置（只有消费者线程更新）
  tail: 生产者下一次写入的位置（多个生产者通过 CAS 竞争更新）

  可用空间 = capacity - (tail - head)
  已用空间 = tail - head

  注意：head 和 tail 是单调递增的 long，通过 & (capacity - 1) 取模得到 buffer 中的实际索引。
```

### 4.3 为什么 capacity 必须是 2 的幂

```java
// 取模运算可以用位与替代（极快）：
int index = (int)position & (capacity - 1);

// 等价于：
int index = (int)(position % capacity);

// 但位与只需 1 个 CPU 周期，取模需要 20+ 个周期
```

---

## 5. 写入流程（生产者端）

### 5.1 write() 一步完成写入

```java
public boolean write(int msgTypeId, DirectBuffer srcBuffer, int offset, int length)
{
    checkTypeId(msgTypeId);
    checkMsgLength(length);

    final AtomicBuffer buffer = this.buffer;
    final int recordLength = length + HEADER_LENGTH;    // 消息长度 + 8 字节 header

    // ① CAS 竞争空间（核心逻辑，详见 5.3）
    final int recordIndex = claimCapacity(buffer, recordLength);
    if (INSUFFICIENT_CAPACITY == recordIndex)
    {
        return false;  // buffer 满，写入失败（非阻塞）
    }

    // ② 先写入 -recordLength（负数），表示"空间已占用，消息未完成"
    //    这是消费者跳过未完成消息的关键信号
    buffer.putIntRelease(lengthOffset(recordIndex), -recordLength);
    VarHandle.releaseFence();  // 确保负长度对其他线程可见（store barrier）

    // ③ 拷贝消息内容到 buffer
    buffer.putBytes(encodedMsgOffset(recordIndex), srcBuffer, offset, length);

    // ④ 写入消息类型
    buffer.putInt(typeOffset(recordIndex), msgTypeId);

    // ⑤ 将 length 改为正数 → 消息完成，消费者可以读取了
    //    putIntRelease = volatile write，保证所有前序写入对消费者可见
    buffer.putIntRelease(lengthOffset(recordIndex), recordLength);

    return true;
}
```

**写入的 5 步状态变迁：**

```
步骤    Length 字段      含义
──────  ──────────      ──────────────────
claim   0 → -32        空间已被占用，消息未完成（消费者等待）
write   -32            正在写入消息内容...
commit  -32 → +32      消息完成，消费者可以读取
read    +32 → 0        消费者读取完毕，空间回收（setMemory 清零）
```

### 5.2 tryClaim() + commit() 两阶段写入

这是 Aeron 中 `DriverProxy.addSubscription()` 使用的模式：

```java
// 阶段一：预留空间
public int tryClaim(int msgTypeId, int length)
{
    final int recordLength = length + HEADER_LENGTH;
    final int recordIndex = claimCapacity(buffer, recordLength);  // CAS 竞争

    if (INSUFFICIENT_CAPACITY == recordIndex)
        return recordIndex;

    // 写入负长度（占位）和消息类型
    buffer.putIntRelease(lengthOffset(recordIndex), -recordLength);
    VarHandle.releaseFence();
    buffer.putInt(typeOffset(recordIndex), msgTypeId);

    // 返回消息内容的起始偏移量（跳过 header）
    // 调用者直接在此偏移量上用 Flyweight 写入数据（零拷贝）
    return encodedMsgOffset(recordIndex);
}

// 阶段二：提交消息
public void commit(int index)
{
    final int recordIndex = computeRecordIndex(index);  // index - HEADER_LENGTH
    final int recordLength = verifyClaimedSpaceNotReleased(buffer, recordIndex);

    // 将 length 从负变正 → 消费者可见
    buffer.putIntRelease(lengthOffset(recordIndex), -recordLength);
    // 注意：recordLength 已经是负数（从 buffer 中读出），-(-N) = +N
}
```

**tryClaim/commit 的优势：调用者直接在 ring buffer 的 mmap 内存上写入数据，无需先写到临时 buffer 再拷贝。**

### 5.3 claimCapacity() 核心：CAS 竞争空间

这是整个 ManyToOneRingBuffer **最核心的方法**：

```java
private int claimCapacity(AtomicBuffer buffer, int recordLength)
{
    // 对齐到 8 字节
    final int requiredCapacity = align(recordLength, ALIGNMENT);
    final int capacity = this.capacity;
    final int mask = capacity - 1;

    // 先读 headCache（不是 volatile read，减少跨核开销）
    long head = buffer.getLongVolatile(headCachePositionIndex);

    long tail;
    long newTail;
    int tailIndex;
    int padding;
    int writeIndex;

    do
    {
        // ① volatile read tail（当前写入位置）
        tail = buffer.getLongVolatile(tailPositionIndex);

        // ② 计算可用空间
        final int availableCapacity = capacity - (int)(tail - head);

        if (requiredCapacity > availableCapacity)
        {
            // headCache 可能过期了，读取真实的 head
            head = buffer.getLongVolatile(headPositionIndex);

            if (requiredCapacity > (capacity - (int)(tail - head)))
            {
                return INSUFFICIENT_CAPACITY;  // 真的满了
            }

            // 更新 headCache
            buffer.putLongRelease(headCachePositionIndex, head);
        }

        newTail = tail + requiredCapacity;
        padding = 0;
        tailIndex = (int)tail & mask;  // tail 在 buffer 中的实际位置
        writeIndex = tailIndex;

        // ③ 检查是否需要 wrap-around（消息跨越 buffer 尾部）
        final int toBufferEndLength = capacity - tailIndex;
        if (requiredCapacity > toBufferEndLength)
        {
            // 消息放不下尾部剩余空间 → 需要 wrap 到开头
            // 详见 5.5
            ...
            padding = toBufferEndLength;
            newTail += padding;
        }
    }
    // ④ CAS 更新 tail：如果失败说明有其他生产者先抢到了，重试
    while (!buffer.compareAndSetLong(tailPositionIndex, tail, newTail));

    // ⑤ 如果有 padding，写入 padding record
    if (0 != padding)
    {
        buffer.putIntRelease(lengthOffset(tailIndex), -padding);
        VarHandle.releaseFence();
        buffer.putInt(typeOffset(tailIndex), PADDING_MSG_TYPE_ID);
        buffer.putIntRelease(lengthOffset(tailIndex), padding);
    }

    return writeIndex;
}
```

**CAS 循环的关键：**

```
Thread-1:  read tail=100  →  CAS(100, 132) → 成功 ✓  →  写入消息到 [100, 132)
Thread-2:  read tail=100  →  CAS(100, 148) → 失败 ✗  →  重试
           read tail=132  →  CAS(132, 180) → 成功 ✓  →  写入消息到 [132, 180)
```

多个生产者通过 CAS 在 tail 上竞争，只有一个能成功推进 tail，失败的自动重试。这就是"无锁"的实现机制。

### 5.4 消息长度的"负数技巧"

这是 ManyToOneRingBuffer 最精妙的设计之一：

```
问题：多个生产者可能同时 claim 到相邻的空间。
      消费者从 head 开始顺序读取，如果中间某个生产者还没写完怎么办？

解决：Length 字段初始写为负数。
      消费者看到 length <= 0 就停止读取，等待生产者完成。

时间线：
  T1: Producer-A claim [100,132)  → length[100] = -32
  T2: Producer-B claim [132,180)  → length[132] = -48
  T3: Producer-B commit           → length[132] = +48   (B 先完成)
  T4: Consumer read from head=100 → length[100] = -32   → 停止！等待 A
  T5: Producer-A commit           → length[100] = +32   (A 完成)
  T6: Consumer read from head=100 → length[100] = +32   → 读取 A 的消息
                         head=132 → length[132] = +48   → 读取 B 的消息
```

**消费者保持严格的顺序读取，即使后面的消息先完成，也必须等前面的完成后才能读取。** 这保证了 FIFO 语义。

### 5.5 Buffer 尾部 Wrap-around 处理

当消息放不进 buffer 尾部的剩余空间时，需要"折返"到 buffer 开头：

```
情况：tail 在位置 900，capacity = 1024，消息需要 200 字节
      尾部只剩 1024 - 900 = 124 字节，放不下

处理：
  1. 尾部 124 字节写入一条 PADDING 记录（填充占位）
  2. 消息从 buffer 开头（offset 0）开始写入
  3. tail 推进 200 + 124 = 324（消息 + padding）

  Before:                                    After:
  ┌────┬─────────┬──────────────────────┐    ┌────┬─────────┬──────────┬────────┐
  │free│  data   │       free      |tail│    │msg │  data   │  free    │PADDING │
  └────┴─────────┴──────────────────┘───┘    └────┴─────────┴──────────┴────────┘
  0    100       500               900 1024  0  200         500        900     1024

  消费者读到 PADDING 记录时会跳过（continue），不回调 handler。
```

---

## 6. 读取流程（消费者端）

### 6.1 read() 方法解析

```java
public int read(MessageHandler handler, int messageCountLimit)
{
    int messagesRead = 0;

    final AtomicBuffer buffer = this.buffer;
    // 读取 head 位置（非 volatile，因为只有消费者线程会写）
    final long head = buffer.getLong(headPositionIndex);

    final int capacity = this.capacity;
    final int headIndex = (int)head & (capacity - 1);   // head 在 buffer 中的实际索引
    final int maxBlockLength = capacity - headIndex;     // 到 buffer 尾部的距离
    int bytesRead = 0;

    try
    {
        while ((bytesRead < maxBlockLength) && (messagesRead < messageCountLimit))
        {
            final int recordIndex = headIndex + bytesRead;

            // ★ volatile read length：
            //   > 0  → 消息完整，可以读取
            //   <= 0 → 生产者还没写完（或空闲），停止
            final int recordLength = buffer.getIntVolatile(lengthOffset(recordIndex));
            if (recordLength <= 0)
            {
                break;  // 遇到未完成的消息，停止读取
            }

            bytesRead += align(recordLength, ALIGNMENT);

            // 跳过 PADDING 记录
            final int messageTypeId = buffer.getInt(typeOffset(recordIndex));
            if (PADDING_MSG_TYPE_ID == messageTypeId)
            {
                continue;
            }

            // 回调 handler，直接传递 buffer 引用和偏移量（零拷贝）
            handler.onMessage(
                messageTypeId,
                buffer,
                recordIndex + HEADER_LENGTH,     // 消息内容起始位置
                recordLength - HEADER_LENGTH);   // 消息内容长度
            ++messagesRead;
        }
    }
    finally
    {
        if (bytesRead > 0)
        {
            // ① 清零已读区域（防止脏数据影响后续写入的 length 判断）
            buffer.setMemory(headIndex, bytesRead, (byte)0);

            // ② 推进 head（release 语义，对生产者可见）
            buffer.putLongRelease(headPositionIndex, head + bytesRead);
        }
    }

    return messagesRead;
}
```

### 6.2 批量消费 + 零拷贝回调

```
读取的关键设计：

1. 批量读取：一次 read() 调用可读取多条消息，最后统一推进 head
   → 减少 volatile write 次数（只写一次 headPosition）

2. 零拷贝回调：handler.onMessage() 直接接收 buffer 引用和偏移量
   → 无需拷贝消息到新的 byte[]，handler 直接在 mmap 内存上操作

3. 先清零再推进：setMemory(0) → putLongRelease(head)
   → 保证生产者看到新 head 时，对应空间已经被清零
   → 否则残留的旧 length 值可能被误读为有效消息

4. 消费者无需任何 CAS 或锁：
   → head 只有消费者线程写入，生产者只读取
   → 天然无竞争
```

---

## 7. headCachePosition 优化

这是一个减少跨核 cache line 访问的优化：

```java
// 在 claimCapacity() 中：
long head = buffer.getLongVolatile(headCachePositionIndex);  // 先读缓存

tail = buffer.getLongVolatile(tailPositionIndex);
int availableCapacity = capacity - (int)(tail - head);

if (requiredCapacity > availableCapacity)
{
    // 缓存的 head 可能落后了，读取真实值
    head = buffer.getLongVolatile(headPositionIndex);
    ...
    // 更新缓存
    buffer.putLongRelease(headCachePositionIndex, head);
}
```

**为什么不直接读 headPosition？**

```
headPosition 在消费者的 cache line 上，消费者每次 read() 都会更新它。
如果生产者每次 write 都去 volatile read headPosition，会导致：
  - 消费者 core 的 cache line 不断被"snoop"
  - 生产者 core 频繁 cache miss

优化策略：
  - headCachePosition 存储在生产者自己的 cache line 上（独立的 128 字节区间）
  - 生产者先读 headCache，大多数时候空间充足，不需要读真实 head
  - 只有空间不足时才读一次真实 headPosition 并更新缓存

效果：
  - 99% 的情况下生产者只访问自己的 cache line（tailPosition + headCachePosition）
  - 只有 1% 空间不足时才跨核读取 headPosition
  - 显著减少 cache coherency 开销
```

---

## 8. unblock() 死亡生产者恢复

如果一个生产者线程在 `tryClaim()` 之后、`commit()` 之前崩溃了怎么办？

这时 ring buffer 会被"卡住"：消费者在 head 位置看到 `length < 0`（负数，表示消息未完成），永远等不到正数。

```java
public boolean unblock()
{
    long headPosition = buffer.getLongVolatile(headPositionIndex);
    long tailPosition = buffer.getLongVolatile(tailPositionIndex);

    int consumerIndex = (int)(headPosition & mask);
    int length = buffer.getIntVolatile(consumerIndex);

    if (length < 0)
    {
        // 情况 1：head 位置有未完成的消息（length 为负）
        // 强制将其转为 PADDING 并设为正长度 → 消费者跳过它
        buffer.putInt(typeOffset(consumerIndex), PADDING_MSG_TYPE_ID);
        buffer.putIntRelease(lengthOffset(consumerIndex), -length);  // 负转正
        return true;
    }
    else if (0 == length)
    {
        // 情况 2：head 位置是空的（length 为 0），但 tail > head
        // 说明生产者 claim 了空间但连负长度都没写
        // 扫描找到下一个有数据的位置，中间空隙用 PADDING 填充
        ...
    }
}
```

**Aeron 的 DriverConductor 会周期性调用 `unblock()` 来处理死亡生产者的情况。**

---

## 9. 与 Java ConcurrentLinkedQueue 的对比

| 维度 | ManyToOneRingBuffer | ConcurrentLinkedQueue |
|------|--------------------|-----------------------|
| **内存分配** | 固定大小，零分配 | 每次 offer 分配 Node 对象 |
| **GC 影响** | 无 | 大量短生命周期对象，GC 压力大 |
| **缓存友好** | 顺序数组访问，极好 | 链表节点分散在堆中，cache miss 多 |
| **跨进程** | 支持（mmap 共享内存） | 不支持（JVM 堆内对象） |
| **背压机制** | write 返回 false | 无界队列，可能 OOM |
| **延迟** | 纳秒级（内存操作 + CAS） | 微秒级（对象分配 + CAS + GC） |
| **吞吐** | 极高（零拷贝批量读取） | 较高（但受 GC 影响） |
| **有界/无界** | 固定容量（有界） | 无界 |

---

## 10. 在 Aeron 中的应用

### 10.1 作为 CnC toDriverBuffer

```
位置：CnC 文件（cnc.dat）中 Metadata 之后的第一个区段

  CnC 文件:
  ┌──────────┬────────────────────────────┬────────────────────┐
  │ Metadata │    toDriverBuffer (mmap)   │   toClientsBuffer  │...
  │ 128B     │    ManyToOneRingBuffer     │   BroadcastBuffer  │
  └──────────┴────────────────────────────┴────────────────────┘

用途：
  Client-1 (addPublication)    ──┐
  Client-2 (addSubscription)   ──┤── CAS 写入 ──▶ [toDriverBuffer] ── 单线程读取 ──▶ DriverConductor
  Client-3 (removePublication) ──┘
```

### 10.2 消息类型

| msgTypeId | 常量名 | 说明 |
|-----------|--------|------|
| 0x01 | ADD_PUBLICATION | 添加发布 |
| 0x02 | REMOVE_PUBLICATION | 移除发布 |
| 0x04 | ADD_SUBSCRIPTION | 添加订阅 |
| 0x05 | REMOVE_SUBSCRIPTION | 移除订阅 |
| 0x08 | ADD_DESTINATION | 添加目的地 |
| 0x0D | ADD_COUNTER | 添加计数器 |
| ... | ... | ... |

### 10.3 correlationId 的生成

```java
// ManyToOneRingBuffer.nextCorrelationId()
public long nextCorrelationId()
{
    // 原子递增 trailer 中的 correlation counter
    return buffer.getAndAddLong(correlationIdCounterIndex, 1);
}
```

每个 Client 调用 `addSubscription()` 等方法时，都会通过 `nextCorrelationId()` 分配一个全局唯一的 ID。这个 ID 跟随命令写入 ring buffer，Driver 处理完后用同一个 ID 回写响应，Client 据此匹配"哪个命令的响应回来了"。

### 10.4 consumerHeartbeat 的作用

```java
// Driver Conductor 周期性调用：
ringBuffer.consumerHeartbeatTime(epochClock.time());

// Client 在 connectToDriver() 中检查：
while (0 == ringBuffer.consumerHeartbeatTime()) { ... }  // 等待首次心跳
if (ringBuffer.consumerHeartbeatTime() < (now - timeout)) { ... }  // 心跳过期
```

`consumerHeartbeatTime` 存储在 Trailer 的最后一个 cache line 区间中。Driver 的 Conductor 线程（消费者）周期性更新这个值，Client 据此判断 Driver 是否存活。

---

## 附录：完整数据流图

```
Client 线程                    ManyToOneRingBuffer (mmap)                 Driver Conductor 线程
──────────                    ─────────────────────────                  ─────────────────────

driverProxy                                                              
.addSubscription()                                                       
    │                                                                    
    ├─ nextCorrelationId()     ┌─ Trailer ─────────────┐                 
    │   getAndAddLong ────────▶│ correlation: 41 → 42   │                 
    │   return 42              │ tail: 500              │                 
    │                          │ headCache: 200         │                 
    ├─ tryClaim()              │ head: 300              │                 
    │   │                      │ heartbeat: 1709812345  │                 
    │   │ read tail=500        └───────────────────────┘                 
    │   │ CAS(500, 540) ✓                                                
    │   │                      ┌─ Data Area ───────────┐                 
    │   │ length[500] = -40    │ ...                    │                 
    │   │ type[500] = 0x04     │ [500] len=-40 type=4   │  ← 消费者到此停止
    │   │ return 508           │ [508] (msg payload)    │     length<0，等待
    │   │                      │ ...                    │                 
    │                          │                        │                 
    ├─ 填充 Flyweight          │ [508] channel=...      │                 
    │   at offset 508          │       streamId=10      │                 
    │                          │       clientId=42      │                 
    │                          │       correlationId=42 │                 
    │                          │                        │                 
    ├─ commit()                │                        │                 
    │   length[500] = +40      │ [500] len=+40 type=4   │  ← 消费者可以读了
    │                          │                        │    read():
    │                          │                        │    length=+40 > 0 ✓
    │                          │                        │    handler.onMessage(
    │                          │                        │      0x04, buf, 508, 32)
    │                          │                        │    head += 40
    │                          │                        │    清零 [500,540)
    │                          └────────────────────────┘                 
```

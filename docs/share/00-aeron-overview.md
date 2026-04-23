# Aeron 整体架构与核心技术 - 第一次分享

> 适合人群：对 Aeron 不熟悉，想快速建立整体认知的工程师
> 时间预估：45～60 分钟

---

## 目录

1. [Aeron 是什么 & 为什么选它](#1-aeron-是什么--为什么选它)
2. [整体模块架构](#2-整体模块架构)
3. [核心技术栈](#3-核心技术栈)
4. [为什么选 UDP & UDP 的挑战与解决方案](#4-为什么选-udp--udp-的挑战与解决方案)
5. [为什么这么快：性能原理全景](#5-为什么这么快性能原理全景)
6. [核心技术一：mmap 共享内存](#6-核心技术一mmap-共享内存)
7. [核心技术二：ManyToOneRingBuffer 无锁队列](#7-核心技术二manytooineringbuffer-无锁队列)
8. [核心技术三：LogBuffer & Term Buffer](#8-核心技术三logbuffer--term-buffer)
9. [核心技术四：Flyweight 零拷贝序列化](#9-核心技术四flyweight-零拷贝序列化)
10. [核心技术五：Agent 单线程模型](#10-核心技术五agent-单线程模型)
11. [一条消息的完整旅程](#11-一条消息的完整旅程)
12. [分模块简介（后续分享预告）](#12-分模块简介后续分享预告)

---

## 1. Aeron 是什么 & 为什么选它

### 1.1 定位

**Aeron** 是一个**高性能、低延迟的消息传输中间件**，专为金融交易、实时控制、高频计算等极端场景设计。

```
对标产品对比：
┌─────────────┬──────────┬────────────┬──────────────┐
│             │  Kafka   │ ZeroMQ     │  Aeron       │
├─────────────┼──────────┼────────────┼──────────────┤
│ P99 延迟    │ ms 级    │ 10μs 级    │ < 1μs 级     │
│ 跨机器      │  ✓       │  ✓         │  ✓           │
│ 同机 IPC    │  ✗       │  ✓         │  ✓           │
│ 强一致集群  │  ✓       │  ✗         │  ✓ (Cluster) │
│ 消息回放    │  ✓       │  ✗         │  ✓ (Archive) │
│ 无 GC 停顿  │  ✗       │  -         │  ✓           │
└─────────────┴──────────┴────────────┴──────────────┘
```

### 1.2 核心价值主张

| 特性 | 说明 |
|------|------|
| **微秒级延迟** | P50 < 1μs（同机 IPC），P50 < 10μs（局域网 UDP） |
| **零 GC** | 所有热路径零对象分配，不受 GC 停顿影响 |
| **零拷贝** | 应用层写入 → Sender 发送 → 接收写入 LogBuffer，全程无数据拷贝 |
| **无锁并发** | CAS + volatile 替代 mutex，无线程阻塞 |
| **可靠 UDP** | 用户态实现 NAK 重传、流控、拥塞控制 |
| **可扩展** | 支持 UDP 单播/多播、IPC、Archive 录制、Cluster 强一致 |

---

## 2. 整体模块架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        用户应用层                                    │
│     Publication.offer()          Subscription.poll()               │
└────────────────────────┬──────────────────┬─────────────────────────┘
                         │                  │
┌────────────────────────▼──────────────────▼─────────────────────────┐
│                     aeron-client (客户端 API)                        │
│  Aeron | Publication | Subscription | Image | ClientConductor       │
└────────────────────────────────────┬────────────────────────────────┘
                                     │ CnC mmap (共享内存)
┌────────────────────────────────────▼────────────────────────────────┐
│                     aeron-driver (媒体驱动)                          │
│  ┌───────────────┐  ┌────────────┐  ┌────────────────────────────┐  │
│  │DriverConductor│  │  Receiver  │  │         Sender             │  │
│  │ 管理 Pub/Sub  │  │ 接收 UDP   │  │  发送 UDP / IPC            │  │
│  │ 分配 LogBuffer│  │ 写入LogBuf │  │  读取 LogBuffer 发送       │  │
│  └───────────────┘  └────────────┘  └────────────────────────────┘  │
│             ↑                              ↑                         │
│     LogBuffer (mmap 文件)           LogBuffer (mmap 文件)            │
└─────────────────────────────────────────────────────────────────────┘
         ↓ 可选扩展                            ↓ 可选扩展
┌────────────────┐                   ┌──────────────────────────────┐
│ aeron-archive  │                   │       aeron-cluster          │
│ 录制/回放消息   │                   │   Raft 强一致分布式服务       │
└────────────────┘                   └──────────────────────────────┘
         ↓ 可观测性
┌────────────────┐
│  aeron-agent   │
│ Java Agent 诊断│
└────────────────┘
```

### 2.1 模块说明

| 模块 | 功能 | 典型场景 |
|------|------|---------|
| **aeron-client** | 提供 `Aeron`、`Publication`、`Subscription` API | 所有应用 |
| **aeron-driver** | MediaDriver，管理网络收发、LogBuffer | 核心必须 |
| **aeron-archive** | 录制订阅到磁盘，支持历史消息回放 | 行情存档、审计 |
| **aeron-cluster** | Raft 共识集群，强一致状态机 | 订单簿、撮合引擎 |
| **aeron-agent** | Java Agent，低侵入实时事件诊断 | 生产排查、调优 |

---

## 3. 核心技术栈

```
技术层次：
┌────────────────────────────────────────────────────────────────┐
│  应用协议层    │  SBE（Simple Binary Encoding）零拷贝序列化     │
├────────────────────────────────────────────────────────────────┤
│  一致性层      │  Raft 算法（aeron-cluster）                    │
├────────────────────────────────────────────────────────────────┤
│  持久化层      │  mmap 文件 + Catalog（aeron-archive）          │
├────────────────────────────────────────────────────────────────┤
│  并发模型层    │  Agent 单线程 + IdleStrategy                  │
├────────────────────────────────────────────────────────────────┤
│  IPC 通信层    │  ManyToOneRingBuffer（CnC mmap）               │
├────────────────────────────────────────────────────────────────┤
│  数据缓冲层    │  LogBuffer / Term Buffer（mmap 文件）           │
├────────────────────────────────────────────────────────────────┤
│  内存访问层    │  Agrona UnsafeBuffer（sun.misc.Unsafe）        │
├────────────────────────────────────────────────────────────────┤
│  传输层        │  UDP（单播/多播）/ IPC（本地）                 │
├────────────────────────────────────────────────────────────────┤
│  OS 层         │  mmap、epoll/kqueue、tmpfs（/dev/shm）          │
└────────────────────────────────────────────────────────────────┘
```

---

## 4. 为什么选 UDP & UDP 的挑战与解决方案

### 4.1 为什么不用 TCP

Aeron 选择 UDP 而非 TCP，是一个经过权衡的核心设计决策：

```
TCP 的隐患（为什么不用）：

问题 1：Head-of-Line Blocking（行头阻塞）
  TCP 流是严格有序的。一个包丢失后，后续所有包都被内核缓冲，
  等待重传完成才能交付应用。
  → 一次丢包 ⇒ 整条流停顿（即使后续包已到达）
  → Aeron 的 NAK-based 重传：只重传丢失的帧，其他帧不阻塞

问题 2：内核态拥塞控制不可控
  TCP 的拥塞控制（CUBIC/BBR）在内核态运行，应用无法干预。
  → 金融场景需要精确控制发送节奏，TCP 做不到
  → Aeron 用户态流控：通过 SM + positionLimit，应用可精细调优

问题 3：连接建立开销
  TCP 需要三次握手，有状态连接维护
  → Aeron SETUP/SM 握手更轻量，无状态化设计

问题 4：多播支持
  TCP 本质上是点对点的，无法原生多播
  → Aeron 原生支持 UDP 多播（一份数据，多个订阅者，NAK/SM 适配）

问题 5：Nagle 算法 / 延迟 ACK
  TCP 默认开启 Nagle 和延迟 ACK，会人为引入 ~40ms 延迟
  → 即使关闭 TCP_NODELAY，内核仍有额外处理开销
  → Aeron 没有这些限制
```

### 4.2 UDP 的原生问题

UDP 本身是"裸"的，不提供任何可靠性保证：

| UDP 问题 | 表现 | 后果 |
|----------|------|------|
| **丢包** | 路由器缓冲区溢出、网络抖动 | 消息丢失，应用收不到数据 |
| **乱序** | 多路径路由，包走不同路径 | 消息顺序错乱，状态机状态错误 |
| **无流控** | 发送端速度超过接收端 | 接收缓冲区溢出，大量丢包 |
| **无拥塞控制** | 突发流量填满链路 | 网络拥塞，延迟飙升 |
| **无连接状态** | 不知道对端是否在线 | 发送了数据却无人接收 |
| **重复包** | 网络设备重传 | 应用收到重复消息 |

### 4.3 Aeron 如何解决每一个 UDP 问题

#### 问题一：丢包 → NAK-based 重传

```
Aeron 的解决方案：否定确认（NAK），而非肯定确认（ACK）

正常路径（无开销）：
  Publisher ─── DATA(1) DATA(2) DATA(3) ──▶ Subscriber
  无任何控制消息，开销接近零

丢包场景：
  Publisher ─── DATA(1) DATA(2) ✗DATA(3) DATA(4) ──▶ Subscriber
                                                      │
                                           检测到间隙：position 2 之后有 gap
                                           等待 NAK_DELAY (默认 60μs)
                                           ◀─── NAK(termOffset=2, len=64) ───
  Publisher 收到 NAK → 重传 DATA(3)
  Publisher ─── DATA(3) 重传 ──────────────────────▶ ✓

为什么是 NAK 而不是 ACK？
  ACK：每收到一帧就要 ACK，正常路径有大量控制消息开销
  NAK：只在丢包时才发，正常路径零控制开销
  → 金融行情等场景：99.99% 的帧都是正常到达，NAK 开销极低

间隙检测原理（LossDetector）：
  接收端维护 rebuildPosition（已连续收到的最高 position）
  和 hwmPosition（收到的最高 position，可有间隙）
  rebuildPosition < hwmPosition → 存在间隙 → 触发 NAK
```

#### 问题二：乱序 → Position 有序交付

```
Aeron 的解决方案：基于 position 的有序缓冲

每个帧携带：termId + termOffset → 唯一确定 position（全局字节偏移）
  position = (termId - initialTermId) × termLength + termOffset

收到乱序帧时：
  ① 根据 termId + termOffset 计算 position
  ② 将帧写入 LogBuffer 的对应位置（不是追加，而是按 offset 写入）
  ③ frameLengthOrdered() 发布该帧（release 语义）
  ④ poll() 从 rebuildPosition 顺序扫描，遇到未到达的 gap 就停止

效果：
  即使乱序到达，也会被按正确顺序交付给应用
  LogBuffer 就是"接收缓冲区"，天然解决乱序问题
```

#### 问题三：无流控 → SM + positionLimit 端到端背压

```
Aeron 的解决方案：Status Message（SM）窗口机制

① 接收端定期发送 SM：
   SM { consumedPosition, receiverWindowLength }
   consumedPosition = 接收端已消费到的 position
   receiverWindowLength = 接收端还能接受多少字节（拥塞控制决定）

② 发送端更新 senderLimit：
   senderLimit = consumedPosition + receiverWindowLength

③ publication.offer() 检查 positionLimit：
   position >= positionLimit → 返回 BACK_PRESSURED（非阻塞，立即返回）
   应用层自行决定如何处理背压（等待、丢弃、报警）

④ 接收端消费加快 → SM 中 consumedPosition 增大
   → senderLimit 增大 → positionLimit 增大 → offer 可以继续写入

特点：
  - 全程无锁：positionLimit 是 Counter（CnC mmap 中的 long），volatile 读写
  - 非阻塞：offer 不等待，立即返回 BACK_PRESSURED，发送端自主决策
  - 精确：按字节粒度控制，而非包粒度
```

#### 问题四：无拥塞控制 → 可插拔 CongestionControl

```
Aeron 的解决方案：CongestionControl 接口，可插拔实现

interface CongestionControl {
    int initialWindowLength();                    // 初始接收窗口
    int maxWindowLength();                        // 最大接收窗口
    boolean shouldMeasureRtt(long nowNs);         // 是否发 RTT 测量帧
    int onTrackRebuildWork(...);                  // 更新重建进度
    int onRttMeasurement(long nowNs, long rttNs); // 收到 RTT 响应后更新窗口
}

内置实现：
  ① StaticWindowCongestionControl（默认）
     固定窗口大小（默认 256KB），简单高效，适合局域网
     不发 RTT 帧，零控制开销

  ② CubicCongestionControl（类 TCP CUBIC）
     基于 RTT 动态调整窗口
     丢包时减窗，低 RTT 时扩窗
     适合广域网、高延迟链路

RTT 测量帧（RTT Measurement）：
  Publisher 发 RTT_MEASUREMENT 帧携带时间戳
  Subscriber 立即回复，Publisher 测量往返延迟
  用于拥塞控制的窗口调整依据
```

#### 问题五：无连接状态 → SETUP 握手协议

```
Aeron 的解决方案：SETUP 帧建立连接感知

① Publisher Sender 定期发送 SETUP 帧：
   SETUP { sessionId, streamId, initialTermId, termBufferLength, MTU, ... }
   作用：通知 Subscriber "我存在，我的流参数如下"

② Subscriber Receiver 收到 SETUP 后：
   - 检查是否已有对应的 PublicationImage
   - 没有 → 创建 PublicationImage，分配 LogBuffer
   - 回复 SM 帧（开放接收窗口）
   - 通知 ClientConductor → 向应用交付 Image（availableImageHandler）

③ Publisher 收到 SM 后：
   - 开始发送 DATA 帧
   - LogBuffer 中 IS_CONNECTED 标志位置 1
   - offer() 不再返回 NOT_CONNECTED

心跳机制：
  连接建立后，即使没有数据，Publisher 也定期发 HEARTBEAT 帧（data = 0 的 DATA 帧）
  Subscriber 超时未收到 → Image 标记为 closed → 通知应用（unavailableImageHandler）
```

#### 问题六：重复包 → Position 去重

```
重复包的来源：
  ① NAK 重传时，如果之前的原始包也到达了
  ② 网络设备的重传机制

Aeron 的处理：
  收到帧时，计算 packetPosition = computePosition(termId, termOffset)
  if (packetPosition < rebuildPosition):
    → 已经消费过的 position，直接丢弃（写入 LogBuffer 相同位置，幂等）
  if (packetPosition == frameLength at that offset != 0):
    → 已经写入的 position（可能是重复包），也幂等（写入相同数据）

效果：
  重复包不会导致应用收到重复消息
  因为 poll() 是按 subscriberPosition 顺序推进的，不会回退
```

### 4.4 Aeron 协议帧体系总览

```
Aeron 在 UDP 上构建了完整的传输协议，共 6 种帧类型：

┌──────────┬───────────┬────────────────────────────────────────┐
│  帧类型  │  方向     │  解决的 UDP 问题                        │
├──────────┼───────────┼────────────────────────────────────────┤
│  DATA    │ Pub→Sub   │ 数据传输（内容 = Term Buffer 中的帧）    │
│  PADDING │ Pub→Sub   │ Term 末尾填充（保持顺序扫描连续性）      │
│  SETUP   │ Pub→Sub   │ 解决"无连接状态"，通知流参数           │
│  SM      │ Sub→Pub   │ 解决"无流控"，通告消费位置和窗口大小    │
│  NAK     │ Sub→Pub   │ 解决"丢包"，请求重传指定区间            │
│  RTT     │ Pub↔Sub   │ 解决"无拥塞控制"，测量往返延迟          │
└──────────┴───────────┴────────────────────────────────────────┘

正常路径：只有 DATA + SETUP + SM（SM 定期发送，很低频）
异常路径：额外有 NAK + RTT
```

---

## 5. 为什么这么快：性能原理全景

### 5.1 性能瓶颈在哪里（常见系统的问题）

```
传统消息系统的延迟来源：

① 内存分配（GC）
  每条消息创建新对象 → GC 触发 → Stop-the-World → 延迟飙升到毫秒级
  → Aeron 热路径零对象分配

② 数据拷贝
  应用 → serialize → byte[] → write() → 内核缓冲 → send → 内核缓冲 → read() → deserialize → 应用
  → 4-6 次内存拷贝，每次 ~100ns
  → Aeron：应用 → Term Buffer（1次）→ UDP 发送（0次，零拷贝）

③ 锁竞争
  synchronized / ReentrantLock → 线程挂起 → OS 调度 → 唤醒 → ~10μs
  → Aeron：CAS + volatile，无线程挂起

④ 系统调用
  每次 read/write → 用户态/内核态切换 → ~1μs
  → Aeron：IPC 路径完全无系统调用；UDP 路径每批次 1 次 sendmsg

⑤ 线程调度延迟
  线程被 OS 调度出 CPU → 等待重新分配 → ~10-50μs
  → Aeron：BusySpinIdleStrategy（Busy-Poll），永不主动让出 CPU
```

### 5.2 零 GC：热路径零对象分配

```
GC 如何破坏延迟：

正常运行：P50 = 1μs
GC 触发（Minor GC）：停顿 5-50ms  → P99.9 = 50ms
GC 触发（Full GC）：停顿 100ms-1s → 所有请求超时

Aeron 的零 GC 策略：
┌─────────────────────────────────────────────────────────────────┐
│ 热路径操作                  避免分配的手段                        │
├─────────────────────────────────────────────────────────────────┤
│ offer(buffer)               直接写入 mmap Term Buffer            │
│ 帧头写入                    Flyweight（wrap 到 mmap，不 new）     │
│ poll(handler)               回调传入 mmap buffer 引用（不拷贝）   │
│ Driver 命令写入             RingBuffer tryClaim（预分配内存）     │
│ 日志记录（aeron-agent）      Ring Buffer 二进制写（不 String）     │
│ 流控 Counter 更新           CnC mmap long 写（不 new Long）       │
└─────────────────────────────────────────────────────────────────┘

结果：热路径完全不触发 GC，延迟曲线极其平坦
      P50 ≈ P99 ≈ P99.9（无 GC 导致的长尾延迟）
```

### 5.3 零拷贝：从 offer 到 poll 的拷贝次数

```
IPC 场景（同机进程间）：

  发送端应用       LogBuffer (mmap)      接收端应用
      │                                      │
      │  putBytes()  ──────────────────────► │  buffer 引用（零拷贝）
      │  [1次拷贝]                           │  onFragment(mmap buffer)
                                             │  [0次拷贝]
  总拷贝次数：1（应用 buffer → mmap）


UDP 场景（跨机器）：

  发送端应用       发送端 mmap     网络       接收端 mmap     接收端应用
      │                │             │              │               │
      │ putBytes()     │             │              │               │
      │ ─────────────► │             │              │               │
      │  [1次拷贝]      │ sendmsg()  │              │               │
      │                │ ──────────► │ ─────────── ►│               │
      │                │  [0次拷贝]  │  网络传输     │ recv          │
      │                │             │              │ ─────────────►│
      │                │             │              │  [1次拷贝]    │
      │                │             │              │               │ onFragment(mmap)
      │                │             │              │               │ [0次拷贝]
  总拷贝次数：2（每端各1次，无额外中间拷贝）


对比传统 Socket 方案：
  应用 → serialize → byte[] → Socket.write() → 内核buf → send
  → 网络 → recv → 内核buf → read() → byte[] → deserialize → 应用
  总拷贝：4-6 次
```

### 5.4 无锁并发：CAS + volatile 替代 mutex

```
锁的代价（为什么要避免）：

  mutex.lock():
    ① CAS 尝试获取锁
    ② 失败 → futex 系统调用 → 线程挂起（~1μs 系统调用 + ~10-50μs 调度延迟）
    ③ 持有者释放 → 唤醒等待线程（又一次调度）
    ④ 总代价：~20-100μs（含调度延迟）

Aeron 的无锁方案：

┌───────────────────┬──────────────────────────────────────────┐
│ 操作               │ 无锁机制                                  │
├───────────────────┼──────────────────────────────────────────┤
│ 多线程 offer       │ getAndAddLong (lock xadd, ~10ns)         │
│                   │ 原子 fetch-and-add，无 CAS 重试            │
├───────────────────┼──────────────────────────────────────────┤
│ offer/poll 同步    │ frameLengthOrdered (store-release)        │
│                   │ frameLengthVolatile (load-acquire)        │
│                   │ 帧长作为"发布信号"，happens-before 保证    │
├───────────────────┼──────────────────────────────────────────┤
│ Client→Driver 命令 │ ManyToOneRingBuffer CAS tail             │
│                   │ 失败则自旋重试（无 futex）                 │
├───────────────────┼──────────────────────────────────────────┤
│ Term 轮转          │ casActiveTermCount（单次 CAS）            │
│                   │ 多线程竞争只有一个成功，其余返回 ADMIN_ACTION│
├───────────────────┼──────────────────────────────────────────┤
│ 流控 Counter 更新  │ AtomicCounter.setRelease（单次 volatile） │
└───────────────────┴──────────────────────────────────────────┘

关键：所有操作都是 wait-free 或 lock-free，不存在线程挂起
      即使有竞争也是 CPU 自旋（纳秒级），而非 OS 调度（微秒级）
```

### 5.5 CPU Cache 优化：False Sharing 消除

```
False Sharing 问题：
  CPU Cache Line = 64 字节
  如果两个变量在同一 Cache Line 上，不同线程修改各自变量
  → 每次修改都让另一个线程的 Cache Line 失效 → 性能急剧下降

Aeron 的解法：Padded 字段，128 字节隔离

示例（ManyToOneRingBuffer Trailer）：
  headCachePosition: [8B value][56B padding] = 64B → 独占 1 Cache Line
  tailPosition:      [8B value][56B padding] = 64B → 独占 1 Cache Line
  headPosition:      [8B value][56B padding] = 64B → 独占 1 Cache Line

效果：
  生产者读写 tailPosition，消费者读写 headPosition
  两个 Cache Line 永不共享 → 无 False Sharing
  → 多核并发性能接近单线程理论值

AtomicCounter（位于 CnC Counters Buffer）：
  每个 Counter 独占 128 字节（64 字节 value + 64 字节 padding）
  所有 Counter 的修改互不干扰
```

### 5.6 Busy-Poll：用 CPU 换延迟

```
传统方式（epoll 阻塞）：
  线程 sleep → 数据到达 → OS 唤醒线程 → 线程重新调度 → 处理数据
  延迟额外开销：OS 调度 = ~10μs ~ 50μs（取决于内核调度器）

Aeron 的方式（selectNow 忙轮询）：
  while (true) {
      int ready = selector.selectNow();  // 非阻塞，立即返回
      if (ready > 0) {
          processPackets();
      }
      // else: 继续循环，永不休眠（BusySpinIdleStrategy）
  }

代价与收益：
  代价：Receiver 线程 100% CPU 占用（需要专用核）
  收益：UDP 包到达后 < 1μs 即可被处理（无调度延迟）

生产环境建议：
  ① 给 Aeron 的 Sender/Receiver 线程绑定专用 CPU 核（taskset/affinity）
  ② 关闭这些核的 CPU 电源管理（C-State）和频率调节（turbo boost 开启）
  ③ 使用 NUMA-aware 内存分配（LogBuffer 分配在与线程同 NUMA 节点的内存）
  ④ 禁用 THP（Transparent Huge Pages），避免内存整理导致延迟抖动
```

### 5.7 性能数字汇总

```
同机 IPC 场景（同一个 JVM 进程，嵌入式 Driver）：
  P50 延迟：< 1μs
  P99 延迟：< 2μs
  吞吐量：> 50M msg/s（小消息）

局域网 UDP 场景（10GbE 网卡，专用核）：
  P50 延迟：5 ~ 10μs（含网络传输）
  P99 延迟：< 20μs
  吞吐量：> 10M msg/s

与竞品对比（相同硬件）：
  ┌─────────────┬──────────────┬──────────────┬──────────────┐
  │             │    Kafka      │   ZeroMQ     │    Aeron     │
  ├─────────────┼──────────────┼──────────────┼──────────────┤
  │ P50（IPC）  │    N/A        │    ~5μs      │   < 1μs      │
  │ P50（UDP）  │   ~5ms       │    ~10μs     │   ~5μs       │
  │ P99（UDP）  │   ~50ms      │    ~100μs    │   < 20μs     │
  │ GC 停顿影响 │    高         │    无        │    无        │
  └─────────────┴──────────────┴──────────────┴──────────────┘
```

### 5.8 性能设计原则总结

```
Aeron 快的根本原因：系统性地消除每一类延迟来源

延迟来源          Aeron 的解决方案                    效果
────────────────  ──────────────────────────────────  ────────────────
GC 停顿           零分配热路径 + Flyweight             无长尾延迟
数据拷贝          mmap 共享内存 + 零拷贝发送路径        节省 ~400ns/msg
内核态切换        ManyToOneRingBuffer（纯用户态 IPC）   节省 ~1μs/cmd
锁竞争            CAS + volatile（无 futex）            节省 ~20μs/contention
调度延迟          BusySpinIdleStrategy（忙轮询）        节省 ~10μs/wakeup
False Sharing     128 字节 Padding 隔离                 多核线性扩展
序列化开销        Flyweight + SBE（纳秒级编解码）        节省 ~1μs/msg
UDP 丢包恢复      NAK-based（正常路径零开销）            不影响正常延迟
TCP Head-of-Line  UDP + 独立流控（NAK 只重传丢失部分）   避免全流阻塞
```

---

## 6. 核心技术一：mmap 共享内存

### 6.1 Aeron 中的三类 mmap

| 用途 | 文件 | 说明 |
|------|------|------|
| **Client ↔ Driver IPC** | `cnc.dat` | 命令/响应通道、计数器 |
| **Publisher ↔ Subscriber 数据** | `<streamId>-<sessionId>.log` | LogBuffer，消息数据 |
| **Archive 录制** | `<recordingId>-<segment>.rec` | 录制的消息数据文件 |

### 6.2 为什么选 mmap

```
对比维度        Socket/TCP              CnC mmap
─────────      ──────────              ────────
延迟            微秒～毫秒               纳秒级（直接内存访问）
数据拷贝次数    ≥ 2 次（内核↔用户态）    0 次（共享同一物理页）
系统调用        每次 send/recv           不需要（普通内存读写）
线程模型        需要阻塞/epoll           无锁 CAS
```

### 6.3 CnC 文件内存布局

```
偏移量
0         ┌─────────────────────────────────┐
          │       Metadata（128 字节）        │  版本号、各区段长度、心跳超时
128       ├─────────────────────────────────┤
          │     to-driver Buffer            │  默认 1MB
          │   (ManyToOneRingBuffer)         │  Client → Driver 命令通道
          ├─────────────────────────────────┤
          │     to-clients Buffer           │  默认 1MB
          │   (BroadcastReceiver)           │  Driver → Client 响应通道
          ├─────────────────────────────────┤
          │   Counters Metadata Buffer      │  计数器标签（名称、类型）
          ├─────────────────────────────────┤
          │    Counters Values Buffer       │  计数器数值（pub-limit、sub-pos 等）
          ├─────────────────────────────────┤
          │          Error Log              │  Driver 错误日志
          └─────────────────────────────────┘
```

### 6.4 Client 连接 Driver 的无锁同步

```
Driver 端                                  Client 端
─────────                                  ─────────
mapNewFile("cnc.dat")
填写 Metadata 各字段...
putIntVolatile(版本号)  ←── store barrier ─── getIntVolatile(版本号) → 非零
                                               版本号可见 ⇒ 所有 Metadata 可见
                                               ⇒ 连接成功！
```

> **关键设计**：版本号最后写入（volatile），利用 Java Memory Model 的 happens-before 保证其他字段对 Client 可见，无需任何锁。

---

## 7. 核心技术二：ManyToOneRingBuffer 无锁队列

### 7.1 是什么

**ManyToOneRingBuffer** 是多生产者/单消费者的无锁环形缓冲区，承载 CnC 文件中的 `to-driver Buffer`：

```
Client-1 (addPublication) ──┐
Client-2 (addSubscription) ─┤─ CAS 竞争写入 ─▶ [Ring Buffer (mmap)] ─ 单线程读取 ─▶ DriverConductor
Client-3 (removePublication)┘
```

### 7.2 内存布局

```
地址低 ────────────────────────────────────── 地址高
┌────────────────────────────────┬────────────────────┐
│          Data Area             │      Trailer        │
│       消息记录区（2^N 字节）    │   768 字节          │
│  [len|type|payload][len|...]   │  tail pos (128B)    │
│                                │  headCache (128B)   │
│  ← head              tail →   │  head pos  (128B)   │
└────────────────────────────────┴────────────────────┘
```

> **128 字节隔离**：每个关键字段独占 2 个 cache line，消除 false sharing，生产者和消费者各自访问独立 cache line。

### 7.3 写入三步协议（负数技巧）

```
步骤        Length 字段值    含义
──────      ─────────────   ───────────────────────────────
claim       0 → -32         CAS 占位，消费者看到负数则等待
write       -32             生产者正在写入消息内容
commit      -32 → +32       volatile write，消费者可以读取
```

### 7.4 CAS 无锁竞争

```
Thread-1: read tail=100 → CAS(100, 132) → 成功 ✓ → 写入消息到 [100, 132)
Thread-2: read tail=100 → CAS(100, 148) → 失败 ✗ → 重试
          read tail=132 → CAS(132, 180) → 成功 ✓ → 写入消息到 [132, 180)
```

---

## 8. 核心技术三：LogBuffer & Term Buffer

### 8.1 LogBuffer 是什么

**LogBuffer** 是 Publisher 和 Subscriber 之间**共享的数据缓冲区**，以 mmap 文件形式存在。Publisher 写数据，Sender 读取发送，Receiver 写入接收到的数据，Subscriber 读取——**全部操作直接在 mmap 内存上，零拷贝**。

### 8.2 LogBuffer 内部结构

```
LogBuffer = 3 个 Term + Meta
┌──────────────────────┬──────────────────────┬──────────────────────┬──────────────┐
│      Term 0          │      Term 1          │      Term 2          │    Meta      │
│   (默认 16MB)        │   (默认 16MB)        │   (默认 16MB)        │ 计数器状态   │
└──────────────────────┴──────────────────────┴──────────────────────┴──────────────┘

轮转使用：当 Term 0 写满后切换到 Term 1，Term 1 写满后切换到 Term 2，
          Term 2 写满后 Term 0 已被消费，再次切换到 Term 0 → 循环复用
```

### 8.3 Term Buffer 中的数据帧

```
每条 offer() 在 Term Buffer 中存储为一个帧：

┌───────┬────────┬────────┬──────────────────────────────┐
│Version│  Flags │ Type   │         Frame Length         │  ← 4B
│ 0x00  │        │ DATA   │            ≤ MTU             │
├───────┴────────┴────────┴──────────────────────────────┤
│              Term Offset (4B)                          │  帧在 Term 中的起始偏移
├────────────────────────────────────────────────────────┤
│              Session ID  (4B)                          │  发布者会话 ID
├────────────────────────────────────────────────────────┤
│              Stream ID   (4B)                          │  流 ID
├────────────────────────────────────────────────────────┤
│              Term ID     (4B)                          │  Term 编号
├────────────────────────────────────────────────────────┤
│              Reserved    (8B)                          │  保留
├────────────────────────────────────────────────────────┤
│              Payload     (N B)                         │  应用数据
└────────────────────────────────────────────────────────┘
   32B Header（公共帧头）
```

---

## 9. 核心技术四：Flyweight 零拷贝序列化

### 9.1 什么是 Flyweight 模式

Aeron 不使用传统的对象序列化（如 JSON、Protobuf、Java Serialization），而是使用 **Flyweight 模式**：

```
传统方式：                           Flyweight 方式：
  对象 → 序列化 → byte[] → 拷贝     直接在 mmap 内存上按偏移量读写字段
  到 buffer → Driver 反序列化       无中间对象、无拷贝
  → 新对象

  3 次拷贝 + 2 次对象分配            0 次拷贝 + 0 次对象分配
```

### 9.2 使用示例

```java
// Flyweight 只是一个"视图"对象，wrap 到内存段后直接读写
subscriptionMessageFlyweight
    .wrap(buffer, index)          // 绑定到 mmap 内存的某段
    .streamId(streamId)           // 直接写到内存偏移 24
    .channel(channel)             // 直接写到内存偏移 32
    .clientId(clientId)           // 直接写到内存偏移 0
    .correlationId(correlationId); // 直接写到内存偏移 8

// 读取时同样 wrap 同一内存段，直接从内存偏移读取
int streamId = flyweight.streamId();  // 无对象分配
```

### 9.3 SBE（Simple Binary Encoding）

Cluster 和 Archive 内部通信使用 **SBE**，是 Flyweight 模式的进一步标准化：

| 对比项 | JSON | Protobuf | SBE |
|--------|------|---------|-----|
| 编解码延迟 | 微秒 | 亚微秒 | **纳秒** |
| GC 影响 | 高 | 中 | **零** |
| Schema 演进 | 灵活 | 好 | 有限（固定偏移） |
| 使用场景 | 通用 | 通用 | **极低延迟** |

---

## 10. 核心技术五：Agent 单线程模型

### 10.1 为什么用单线程模型

Aeron 的每个核心组件（DriverConductor、Sender、Receiver）都是一个 **Agent**，运行在**独立的单线程**上：

```
好处：
  ✓ 无锁 - 单线程内部不需要锁保护状态
  ✓ Cache 局部性 - 核心数据常驻 L1/L2 Cache
  ✓ 可预测延迟 - 无线程切换开销，无锁竞争
  ✓ CPU 亲和性 - 可绑核（taskset），避免 NUMA 跨核访问
```

### 10.2 Agent 接口与运行模式

```java
// Agent 接口极简
public interface Agent {
    int doWork() throws Exception;   // 核心工作方法，返回完成的工作量
    String roleName();               // Agent 名称
    default void onStart() {}        // 可选：启动前初始化
    default void onClose() {}        // 可选：关闭时清理
}

// AgentRunner：在独立线程中驱动 Agent
// AgentInvoker：由调用者线程手动驱动（嵌入式模式）
```

### 10.3 IdleStrategy（空闲策略）

当 `doWork()` 返回 0（无工作）时，Agent 使用 IdleStrategy 决定如何等待：

| 策略 | 延迟 | CPU | 适用场景 |
|------|------|-----|---------|
| **BusySpinIdleStrategy** | 最低（纳秒） | 100% | 极低延迟，专用核 |
| **YieldingIdleStrategy** | 低（微秒） | 高 | 低延迟，共享核 |
| **SleepingIdleStrategy** | 高（毫秒） | 低 | 开发/测试 |
| **BackoffIdleStrategy** | 自适应 | 中 | 生产默认 |

### 10.4 三大 Agent 的职责

```
MediaDriver 中的三大 Agent：

┌──────────────────────────────────────────────────────────────────┐
│                    DriverConductor                                │
│  - 从 CnC toDriverBuffer 读取 Client 命令（add/remove pub/sub）  │
│  - 管理 Publication/Subscription/Image 生命周期                  │
│  - 分配 LogBuffer、创建 Counter                                  │
│  - 协调 Sender 和 Receiver                                       │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                        Sender                                    │
│  - 扫描所有 NetworkPublication 的 LogBuffer                      │
│  - 将已写入的数据帧通过 UDP Socket 发送出去                       │
│  - 处理重传请求（NAK）                                            │
│  - 发送 SETUP 帧建立连接                                          │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                        Receiver                                  │
│  - NIO Selector 轮询所有 UDP Socket（selectNow 非阻塞）           │
│  - 解析帧类型（DATA/SETUP/SM/NAK）并分发                         │
│  - 将数据帧写入 LogBuffer（Subscriber 通过 mmap 直接读取）        │
│  - 发送 Status Message（SM）进行流控                              │
└──────────────────────────────────────────────────────────────────┘
```

---

## 11. 一条消息的完整旅程

### 11.1 同机 IPC 场景

```
应用线程 (Publisher)                      应用线程 (Subscriber)
────────────────────                      ─────────────────────
publication.offer(msg)                    subscription.poll(handler, limit)
  │                                          │
  │ ① CAS 在 Term Buffer 占位               │ ⑤ volatile read frameLength > 0
  │ ② 写入帧头（sessionId/streamId/termId） │ ⑥ 直接从 mmap 读取消息内容
  │ ③ 拷贝 payload 到 Term Buffer           │ ⑦ 回调 handler(buffer, offset, len)
  │ ④ volatile write frameLength（publish） │
  ▼                                          ▲
┌───────────────────────────────────────────────────────────────┐
│                  LogBuffer (mmap 文件)                         │
│        Publisher 写入   ────────────▶   Subscriber 读取        │
└───────────────────────────────────────────────────────────────┘
```

### 11.2 跨机器 UDP 场景

```
Publisher 进程                                     Subscriber 进程
──────────────                                    ─────────────────
publication.offer(msg)
  │
  ① 写入 Term Buffer（mmap）
  │
  ② Sender.doWork()
     读取 Term Buffer
     sendDatagramChannel.send(packet)  ──UDP──▶  Receiver.doWork()
                                                     │
                                                  ③ 从 NIO DatagramChannel 收包
                                                     │
                                                  ④ DataPacketDispatcher 按 streamId 分发
                                                     │
                                                  ⑤ PublicationImage.insertPacket()
                                                     写入 LogBuffer（mmap）
                                                     │
                                                  ⑥ subscription.poll()
                                                     直接从 LogBuffer mmap 读取
                                                     handler.onMessage(...)
```

### 11.3 关键延迟路径分析

| 步骤 | 实现 | 延迟量级 |
|------|------|---------|
| offer 写入 Term Buffer | CAS + memory write | ~10ns |
| Sender 读取并发送 | 内存读 + UDP sendmsg | ~1μs |
| 网络传输（局域网） | 硬件 | ~1～5μs |
| Receiver 收包 | selectNow + recv | ~1μs |
| poll 从 LogBuffer 读取 | volatile read + 回调 | ~10ns |

---

## 12. 分模块简介（后续分享预告）

### 第二次：MediaDriver 深度

| 主题 | 内容 |
|------|------|
| launchEmbedded 启动流程 | CnC 初始化、三大 Agent 创建 |
| publication.offer 完整路径 | CAS、帧头写入、背压 |
| subscription.poll 完整路径 | Image 机制、Round-Robin |
| UDP 可靠传输协议 | NAK 重传、SM 流控、SETUP 握手 |
| 拥塞控制 | StaticWindow / UnicastFlowControl |

### 第三次：Archive 深度

| 主题 | 内容 |
|------|------|
| Archive 架构 | ArchivingMediaDriver、AeronArchive |
| 录制流程 | Spy 订阅、RecordingSession、文件写入 |
| 回放流程 | ReplaySession、Catalog 查询 |
| 数据结构 | RecordingLog、Catalog、Segment 文件 |

### 第四次：Cluster 深度

| 主题 | 内容 |
|------|------|
| Raft 实现 | Election 选举、日志复制、多数派提交 |
| 快照机制 | 触发、序列化、恢复 |
| Leader/Follower 同步 | RecordingLog 同步、Position 对齐 |
| 双向通信 | Egress、ClientSession |

### 第五次：Agent（可观测性）

| 主题 | 内容 |
|------|------|
| Java Agent 架构 | premain/agentmain、Byte Buddy 织入 |
| 事件模型 | DriverEventCode、ArchiveEventCode、ClusterEventCode |
| 实时诊断 | 动态挂载、事件过滤、落日志 |

---

## 关键概念速查

| 术语 | 含义 |
|------|------|
| **CnC** | Command and Control，Client 与 Driver 通信的 mmap 文件 |
| **LogBuffer** | Publisher/Subscriber 共享的 mmap 数据缓冲区 |
| **Term Buffer** | LogBuffer 中的一个分区（通常 3 个 Term 轮转） |
| **Position** | 消息在整个发布流中的字节偏移（`termId × termLength + termOffset`） |
| **Image** | Subscriber 端代表一个 Publisher 数据源的对象，持有 LogBuffer 引用 |
| **Agent** | 单线程无限循环的工作单元，Aeron 的核心并发模型 |
| **Flyweight** | 直接在内存缓冲区上读写字段，零对象分配、零序列化 |
| **SBE** | Simple Binary Encoding，Aeron 内部协议的二进制编解码框架 |
| **NAK** | Negative Acknowledgment，丢包后接收端发送的重传请求 |
| **SM** | Status Message，接收端向发送端发送的流控窗口通告 |

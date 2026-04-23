# Aeron CnC 文件、connectToDriver 与 addSubscription 源码深度解析

## 目录

- [1. CnC 文件是什么](#1-cnc-文件是什么)
- [2. 为什么不用 Socket/TCP](#2-为什么不用-sockettcp)
- [3. CnC 文件的内存布局](#3-cnc-文件的内存布局)
  - [3.1 整体结构](#31-整体结构)
  - [3.2 Metadata 区段详细字段](#32-metadata-区段详细字段)
  - [3.3 各 Buffer 区段的偏移量计算](#33-各-buffer-区段的偏移量计算)
- [4. 谁创建 CnC 文件：Driver 端流程](#4-谁创建-cnc-文件driver-端流程)
- [5. Aeron.connect() 完整调用链](#5-aeronconnect-完整调用链)
  - [5.1 调用链总览](#51-调用链总览)
  - [5.2 connect() 入口](#52-connect-入口)
  - [5.3 Aeron 构造函数](#53-aeron-构造函数)
  - [5.4 ctx.conclude() 关键流程](#54-ctxconclude-关键流程)
- [6. connectToDriver() 核心源码逐行解析](#6-connecttodriver-核心源码逐行解析)
  - [6.1 方法概述](#61-方法概述)
  - [6.2 源码分步解析](#62-源码分步解析)
- [7. awaitCncFileCreation() 源码解析](#7-awaitcncfilecreation-源码解析)
- [8. conclude() 中 connectToDriver 之后的 Buffer 初始化](#8-conclude-中-connecttodriver-之后的-buffer-初始化)
- [9. 一图总结：完整连接时序](#9-一图总结完整连接时序)
- [10. 关键设计思想](#10-关键设计思想)
- [11. addSubscription() 完整流程解析](#11-addsubscription-完整流程解析)
  - [11.1 调用链总览](#111-调用链总览)
  - [11.2 第一步：DriverProxy 写入命令到共享内存](#112-第一步driverproxy-写入命令到共享内存)
  - [11.3 第二步：创建客户端 Subscription 对象](#113-第二步创建客户端-subscription-对象)
  - [11.4 第三步：awaitResponse 阻塞等待 Driver 响应](#114-第三步awaitresponse-阻塞等待-driver-响应)
  - [11.5 第四步：Driver 端处理 ADD_SUBSCRIPTION](#115-第四步driver-端处理-add_subscription)
  - [11.6 第五步：Image 的建立（数据通道打通）](#116-第五步image-的建立数据通道打通)
  - [11.7 第六步：subscription.poll() 拉取数据](#117-第六步subscriptionpoll-拉取数据)
- [12. addSubscription 完整时序图](#12-addsubscription-完整时序图)
- [13. addSubscription 中的关键数据结构](#13-addsubscription-中的关键数据结构)
- [14. Driver 端 onAddNetworkSubscription：UDP 端口监听全链路](#14-driver-端-onaddnetworksubscriptionudp-端口监听全链路)
  - [14.1 整体调用链](#141-整体调用链)
  - [14.2 UdpChannel.parse()：URI 解析为网络地址](#142-udpchannelparseuri-解析为网络地址)
  - [14.3 getOrCreateReceiveChannelEndpoint：创建 UDP Socket](#143-getorcreatereceivechannelendpoint创建-udp-socket)
  - [14.4 openDatagramChannel()：绑定 UDP 端口](#144-opendatagramchannel绑定-udp-端口)
  - [14.5 注册到 NIO Selector：开始监听](#145-注册到-nio-selector开始监听)
  - [14.6 Receiver 线程：收包与分发](#146-receiver-线程收包与分发)
  - [14.7 从 UDP 包到应用数据的完整路径](#147-从-udp-包到应用数据的完整路径)

---

## 1. CnC 文件是什么

**CnC = Command and Control**，即"命令与控制"文件。

它是 Aeron **客户端（Client）与媒体驱动（Media Driver）之间通信的唯一桥梁**。物理上是一个普通文件，默认路径：

```
<aeronDirectory>/cnc.dat
```

例如 `/dev/shm/aeron-user/cnc.dat`（Linux tmpfs）或 `/tmp/aeron-user/cnc.dat`。

**核心思想：** 客户端和 Driver 各自通过 `mmap`（内存映射文件）将 `cnc.dat` 映射到自己的进程地址空间。映射完成后，双方对这块内存的读写操作**直接在物理内存上进行**，无需经过内核的 `read()`/`write()` 系统调用，也无需任何网络栈。

```
┌──────────────────┐                              ┌──────────────────┐
│   Client 进程     │                              │   Driver 进程     │
│                  │                              │                  │
│  ┌────────────┐  │     ┌──────────────────┐     │  ┌────────────┐  │
│  │ UnsafeBuffer│──┼────▶│   cnc.dat (mmap) │◀────┼──│ UnsafeBuffer│  │
│  └────────────┘  │     │   物理内存页       │     │  └────────────┘  │
│                  │     └──────────────────┘     │                  │
└──────────────────┘                              └──────────────────┘
```

> **类比理解：** 如果把传统 IPC（如 Unix Socket）比作"写信"（需要信封、邮递、拆封），那 CnC mmap 就像两个人**共用一块白板**——一方写上去，另一方立刻就能看到，零拷贝、零系统调用。

---

## 2. 为什么不用 Socket/TCP

| 对比维度 | Socket/TCP | CnC mmap |
|---------|-----------|----------|
| 延迟 | 微秒～毫秒（经过内核网络栈） | **纳秒级**（直接内存访问） |
| 拷贝次数 | 至少 2 次（用户→内核→用户） | **0 次**（共享同一物理页） |
| 系统调用 | 每次 send/recv 都需要 | **不需要**（普通内存读写） |
| 线程模型 | 通常需要阻塞/epoll | **无锁 CAS**（无阻塞） |
| 适用场景 | 跨机器通信 | 同机 IPC |

Aeron 的设计哲学：**控制面（Client ↔ Driver）走 mmap，数据面（网络收发）走 UDP/IPC log buffer**。这样控制面的延迟影响被最小化。

---

## 3. CnC 文件的内存布局

### 3.1 整体结构

> 源码位置：`CncFileDescriptor.java`

```
偏移量(字节)
0          ┌─────────────────────────────────┐
           │          Metadata               │  128 字节（2 个 cache line，64*2）
128        ├─────────────────────────────────┤
           │      to-driver Buffer           │  默认 1MB + 128B (trailer)
           │   (ManyToOneRingBuffer)         │  客户端 → Driver 的命令通道
           ├─────────────────────────────────┤
           │      to-clients Buffer          │  默认 1MB + 128B (trailer)
           │    (BroadcastReceiver)          │  Driver → 客户端 的响应通道
           ├─────────────────────────────────┤
           │   Counters Metadata Buffer      │  计数器标签（名称、类型描述）
           ├─────────────────────────────────┤
           │    Counters Values Buffer       │  计数器数值（pub-limit、sub-pos 等）
           ├─────────────────────────────────┤
           │          Error Log              │  Driver 错误日志
           └─────────────────────────────────┘
```

**所有区段紧密相邻，共享同一块 mmap 内存。** 各区段的起始偏移量和长度存储在 Metadata 的头部字段中。

### 3.2 Metadata 区段详细字段

Metadata 固定占 **128 字节**（`META_DATA_LENGTH = CACHE_LINE_LENGTH * 2 = 128`），内部布局：

```
偏移量   大小      字段名                        说明
──────   ─────    ──────────────                ─────────────────────────────────
0        4 字节   CnC Version                   语义版本号（主版本号不兼容则拒绝连接）
4        4 字节   to-driver buffer length        客户端→Driver ring buffer 的字节长度
8        4 字节   to-clients buffer length       Driver→客户端 broadcast buffer 的字节长度
12       4 字节   counters metadata buf length   计数器元数据区段的字节长度
16       4 字节   counters values buf length     计数器值区段的字节长度
20       4 字节   error log buffer length        错误日志区段的字节长度
24       8 字节   client liveness timeout (ns)   客户端心跳超时（纳秒），Driver 以此判定客户端存活
32       8 字节   driver start timestamp (ms)    Driver 启动的 epoch 毫秒时间戳
40       8 字节   driver PID                     Driver 进程 ID
48       4 字节   file page size                 文件对齐页大小
52~127   -        保留/填充                       对齐到 128 字节（2 cache line）
```

> **为什么对齐到 cache line？** 避免 false sharing。Metadata 头部可能被多线程 volatile 读写（如版本号），cache line 对齐保证不同区段的数据不在同一 cache line 上。

### 3.3 各 Buffer 区段的偏移量计算

以下是 `CncFileDescriptor` 中各 `createXxxBuffer` 方法的偏移量计算逻辑：

```java
// to-driver buffer 起始于 metadata 之后
toDriverOffset  = META_DATA_LENGTH  // 128

// to-clients buffer 紧接 to-driver buffer 之后
toClientsOffset = META_DATA_LENGTH + toDriverBufferLength

// counters metadata 紧接 to-clients buffer 之后
countersMetaOffset = META_DATA_LENGTH + toDriverBufferLength + toClientsBufferLength

// counters values 紧接 counters metadata 之后
countersValuesOffset = countersMetaOffset + countersMetaDataBufferLength

// error log 紧接 counters values 之后
errorLogOffset = countersValuesOffset + countersValuesBufferLength
```

每个区段的长度都记录在 Metadata 头部，客户端连接时**先读 Metadata 头部，再据此切分出各个 UnsafeBuffer**。

---

## 4. 谁创建 CnC 文件：Driver 端流程

> 源码位置：`MediaDriver.Context.conclude()`

Driver 启动时在 `conclude()` 方法中创建并初始化 CnC 文件：

```java
// 1. 创建文件并 mmap
cncByteBuffer = mapNewFile(cncFile(), cncFileLength);

// 2. 切出 metadata 区段
cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

// 3. 写入所有 metadata 字段（各 buffer 长度、超时、PID 等）
CncFileDescriptor.fillMetaData(
    cncMetaDataBuffer,
    conductorBufferLength,    // to-driver buffer 长度
    toClientsBufferLength,    // to-clients buffer 长度
    countersMetadataBufferLength,
    counterValuesBufferLength,
    clientLivenessTimeoutNs,
    errorBufferLength,
    epochClock.time(),        // 启动时间戳
    SystemUtil.getPid(),      // Driver 进程 PID
    filePageSize);

// 4. 初始化 ring buffer 的第一个心跳
toDriverCommands.nextCorrelationId();
toDriverCommands.consumerHeartbeatTime(epochClock.time());

// 5. 写入版本号 —— 这是 "ready 信号"
//    使用 putIntVolatile 确保所有前序写入对客户端可见（memory barrier）
CncFileDescriptor.signalCncReady(cncMetaDataBuffer);

// 6. 强制刷盘（确保非 tmpfs 文件系统也能持久化）
cncByteBuffer.force();
```

**关键设计：版本号是最后写入的，且使用 `putIntVolatile`（带 store barrier）。** 客户端用 `getIntVolatile` 读取版本号，如果读到非零值，就能保证所有 metadata 字段都已经写入完毕。这是一个经典的 **"publish via volatile"** 无锁同步模式。

---

## 5. Aeron.connect() 完整调用链

### 5.1 调用链总览

```
DemoReceiver:
    Aeron aeron = Aeron.connect(ctx);

调用链展开：
Aeron.connect(ctx)                              // 静态工厂方法
  │
  ├─ new Aeron(ctx)                              // 构造函数
  │    │
  │    ├─ ctx.conclude()                          // Context 终结化
  │    │    │
  │    │    ├─ super.conclude()                   // CommonContext.conclude()
  │    │    │    └─ cncFile = new File(aeronDir, "cnc.dat")
  │    │    │
  │    │    ├─ ... 校验配置、设置默认值 ...
  │    │    │
  │    │    ├─ connectToDriver()                  // ★★★ 核心：mmap CnC 文件，建立共享内存通道
  │    │    │    │
  │    │    │    ├─ awaitCncFileCreation()         // 等待文件存在 + mmap + 版本校验
  │    │    │    ├─ isCncFileLengthSufficient()    // 校验文件完整性
  │    │    │    ├─ new ManyToOneRingBuffer()      // 在 mmap 内存上创建 ring buffer
  │    │    │    ├─ consumerHeartbeatTime 校验     // 确认 Driver 存活
  │    │    │    └─ toDriverBuffer = ringBuffer    // 保存引用 → 连接成功
  │    │    │
  │    │    ├─ 创建 toClientBuffer                // Driver→Client 响应通道
  │    │    ├─ 创建 counters buffers              // 共享计数器
  │    │    └─ 创建 DriverProxy(toDriverBuffer)   // 命令发送代理
  │    │
  │    ├─ commandBuffer = ctx.toDriverBuffer()
  │    └─ conductor = new ClientConductor(ctx)    // 客户端 Conductor
  │
  └─ AgentRunner.startOnThread(conductorRunner)   // 启动 Conductor 线程
```

### 5.2 connect() 入口

> 源码位置：`Aeron.java` 第 160 行

```java
public static Aeron connect(final Context ctx)
{
    try
    {
        // 步骤 A：构造 Aeron 实例
        // 内部调用 ctx.conclude() → connectToDriver()，完成 mmap 连接
        final Aeron aeron = new Aeron(ctx);

        // 步骤 B：启动 ClientConductor 线程
        // Conductor 在独立线程中持续执行：
        //   - 从 toClientBuffer 读取 Driver 的响应消息
        //   - 向 toDriverBuffer 发送心跳保活
        //   - 处理 Publication/Subscription 的异步确认与错误
        if (ctx.useConductorAgentInvoker())
        {
            aeron.conductorInvoker.start();     // 调用者线程模式
        }
        else
        {
            AgentRunner.startOnThread(aeron.conductorRunner, ctx.threadFactory());  // 独立线程模式
        }

        return aeron;
    }
    catch (final ConcurrentConcludeException ex) { throw ex; }
    catch (final Exception ex) { ctx.close(); throw ex; }
}
```

### 5.3 Aeron 构造函数

> 源码位置：`Aeron.java` 第 107 行

```java
Aeron(final Context ctx)
{
    // ① conclude() 是连接的核心入口
    //    完成后，ctx 中的 toDriverBuffer / toClientBuffer / counters 全部就绪
    ctx.conclude();

    this.ctx = ctx;
    clientId = ctx.clientId();

    // ② commandBuffer 就是 connectToDriver() 中创建的 ManyToOneRingBuffer
    //    指向 CnC 文件中 to-driver 区段的 mmap 内存
    commandBuffer = ctx.toDriverBuffer();

    // ③ ClientConductor 负责：轮询 toClientBuffer、维护 Pub/Sub 生命周期、发送心跳
    conductor = new ClientConductor(ctx, this);

    // 根据配置选择 Conductor 运行模式
    if (ctx.useConductorAgentInvoker())
    {
        conductorInvoker = new AgentInvoker(..., conductor);  // 由调用者线程驱动
    }
    else
    {
        conductorRunner = new AgentRunner(..., conductor);     // 独立线程驱动
    }
}
```

### 5.4 ctx.conclude() 关键流程

> 源码位置：`Aeron.Context.conclude()`（Aeron.java 第 1159 行）

`conclude()` 方法很长，这里只列出与连接相关的关键步骤：

```java
public Context conclude()
{
    super.conclude();  // CommonContext: cncFile = new File(aeronDir, "cnc.dat")

    // ... 校验各种配置 ...
    // ... 设置默认 clock、idleStrategy 等 ...

    // ★ 核心：连接到 Driver
    connectToDriver();

    // 连接成功后，基于同一块 mmap 内存创建各个通信 buffer：

    // 1. toDriverBuffer（通常 connectToDriver 已创建，这里是防御性检查）
    if (null == toDriverBuffer)
    {
        toDriverBuffer = new ManyToOneRingBuffer(
            CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
    }

    // 2. toClientBuffer：Driver → Client 的响应通道
    //    BroadcastReceiver = 一写多读，CopyBroadcastReceiver 将消息拷贝到本地防止覆盖
    if (null == toClientBuffer)
    {
        toClientBuffer = new CopyBroadcastReceiver(new BroadcastReceiver(
            CncFileDescriptor.createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer)), ...);
    }

    // 3. counters buffer：共享计数器，用于流控位置追踪、监控指标
    if (countersMetaDataBuffer() == null) { ... }
    if (countersValuesBuffer() == null)   { ... }

    // 4. DriverProxy：封装向 toDriverBuffer 写入命令的 API
    //    clientId 通过 ring buffer 的 correlation counter 原子分配，全局唯一
    if (null == driverProxy)
    {
        clientId = toDriverBuffer.nextCorrelationId();
        driverProxy = new DriverProxy(toDriverBuffer, clientId);
    }

    return this;
}
```

---

## 6. connectToDriver() 核心源码逐行解析

### 6.1 方法概述

`connectToDriver()` 是客户端与 Driver 建立连接的**核心方法**。它的本质不是"网络连接"，而是：

1. 找到 Driver 创建的 `cnc.dat` 文件
2. 用 `mmap` 映射到自己的进程地址空间
3. 验证 Driver 是活的（心跳检测）
4. 在映射的内存上创建 Ring Buffer 对象

### 6.2 源码分步解析

> 源码位置：`Aeron.java` `Context.connectToDriver()` 方法

```java
private void connectToDriver()
{
    final EpochClock clock = epochClock;
    // 计算超时截止时间 = 当前时间 + driverTimeoutMs（默认 10 秒）
    final long deadlineMs = clock.time() + driverTimeoutMs();
    // cncFile = <aeronDirectory>/cnc.dat
    final File cncFile = cncFile();

    // 循环直到成功拿到 toDriverBuffer
    while (null == toDriverBuffer)
    {
```

#### 第一步：等待 CnC 文件创建并 mmap

```java
        cncMetaDataBuffer = awaitCncFileCreation(cncFile, clock, deadlineMs);
        cncByteBuffer = cncMetaDataBuffer.byteBuffer();
```

`awaitCncFileCreation` 是阻塞等待+mmap 的核心（详见第 7 节），返回值 `cncMetaDataBuffer` 是一个 `UnsafeBuffer`，它**直接指向 mmap 内存中的 Metadata 区段**。`cncByteBuffer` 是底层的 `MappedByteBuffer`，覆盖整个 CnC 文件。

#### 第二步：校验文件完整性

```java
        if (!CncFileDescriptor.isCncFileLengthSufficient(cncMetaDataBuffer, cncByteBuffer.capacity()))
        {
            BufferUtil.free(cncByteBuffer);
            cncByteBuffer = null;
            cncMetaDataBuffer = null;
            sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
            continue;  // 文件不完整，释放后重试
        }
```

`isCncFileLengthSufficient` 读取 Metadata 中各区段的长度字段，求和后与文件实际大小比较。如果文件太小，说明 Driver 还在写入中。

#### 第三步：创建 ManyToOneRingBuffer

```java
        final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
            CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
```

`createToDriverBuffer` 在 `cncByteBuffer` 上切出 to-driver 区段（从 offset=128 开始，长度从 Metadata 中读取），包装为 `UnsafeBuffer`。然后用这个 buffer 构造 `ManyToOneRingBuffer`。

**ManyToOneRingBuffer 的特性：**
- **Many-To-One**：多个客户端线程可以并发写入（通过 CAS 无锁竞争），Driver 的 Conductor 线程作为唯一消费者读取
- 底层就是一段 mmap 内存 + trailer 区域（存储 head/tail position 和 heartbeat）
- 写入使用 `CAS`（Compare-And-Swap），读取使用 `volatile load`

#### 第四步：等待 Driver 心跳

```java
        while (0 == ringBuffer.consumerHeartbeatTime())
        {
            if (clock.time() > deadlineMs)
            {
                throw new DriverTimeoutException("no driver heartbeat detected");
            }
            sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
        }
```

`consumerHeartbeatTime()` 读取 ring buffer trailer 区域的心跳时间戳。这个值由 **Driver 的 Conductor 线程周期性写入**。如果为 0，说明 Driver 尚未开始消费命令。

#### 第五步：校验心跳时效

```java
        final long timeMs = clock.time();
        if (ringBuffer.consumerHeartbeatTime() < (timeMs - driverTimeoutMs()))
        {
            if (timeMs > deadlineMs)
            {
                throw new DriverTimeoutException("no driver heartbeat detected");
            }
            BufferUtil.free(cncByteBuffer);
            cncByteBuffer = null;
            cncMetaDataBuffer = null;
            sleep(100);
            continue;  // 旧 Driver 残留，释放后重试
        }
```

即使心跳值非零，也可能是**旧 Driver 留下的残留 CnC 文件**。如果最后一次心跳距今超过 `driverTimeoutMs`，说明 Driver 已死。释放映射后等待新 Driver 启动。

#### 第六步：连接成功

```java
        toDriverBuffer = ringBuffer;
    }
}
```

将 ring buffer 赋值给 `toDriverBuffer`，循环结束。至此，客户端通过 mmap 成功"连接"到了 Driver。

---

## 7. awaitCncFileCreation() 源码解析

> 源码位置：`CommonContext.java` 第 1300 行

这是 `connectToDriver()` 调用的第一个关键方法，负责等待 CnC 文件出现并完成 mmap 映射。

```java
static UnsafeBuffer awaitCncFileCreation(
    final File cncFile, final EpochClock clock, final long deadlineMs)
{
    while (true)
    {
        // ① 轮询等待文件存在且长度 >= 128 字节（Metadata 最小长度）
        while (!cncFile.exists() || cncFile.length() < CncFileDescriptor.META_DATA_LENGTH)
        {
            if (clock.time() > deadlineMs)
                throw new DriverTimeoutException("CnC file not created: " + cncFile.getAbsolutePath());
            sleep(IDLE_SLEEP_DEFAULT_MS);
        }

        try (FileChannel fileChannel = FileChannel.open(cncFile.toPath(), READ, WRITE))
        {
            final long fileSize = fileChannel.size();

            // ② 再次确认文件大小（防止 race condition）
            if (fileSize < CncFileDescriptor.META_DATA_LENGTH)
            {
                if (clock.time() > deadlineMs)
                    throw new DriverTimeoutException("CnC file is created but not populated");
                sleep(IDLE_SLEEP_DEFAULT_MS);
                continue;
            }

            // ③ ★ 核心：mmap 映射整个文件到进程内存
            //    fileChannel.map(READ_WRITE, 0, fileSize) 返回 MappedByteBuffer
            //    createMetaDataBuffer 在其上切出前 128 字节作为 Metadata 区段
            final UnsafeBuffer metaDataBuffer =
                CncFileDescriptor.createMetaDataBuffer(fileChannel.map(READ_WRITE, 0, fileSize));

            // ④ 等待版本号被写入（volatile read）
            //    Driver 在 signalCncReady() 中用 putIntVolatile 写入版本号
            //    这里用 getIntVolatile 读取，构成 happens-before 关系：
            //    读到非零版本号 → 保证 fillMetaData 的所有写入对本线程可见
            int cncVersion;
            while (0 == (cncVersion = metaDataBuffer.getIntVolatile(
                CncFileDescriptor.cncVersionOffset(0))))
            {
                if (clock.time() > deadlineMs)
                    throw new DriverTimeoutException("CnC file is created but not initialised");
                sleep(AWAITING_IDLE_SLEEP_MS);
            }

            // ⑤ 校验版本兼容性
            CncFileDescriptor.checkVersion(cncVersion);       // 主版本号必须一致
            if (minor(cncVersion) < minor(CNC_VERSION))       // 次版本号不能低于客户端
                throw new AeronException("driverVersion insufficient for clientVersion");

            return metaDataBuffer;  // 返回指向 mmap Metadata 区段的 UnsafeBuffer
        }
        catch (final NoSuchFileException | AccessDeniedException ignore)
        {
            // 文件可能刚好被 Driver 删除重建，重试
        }
    }
}
```

**核心要点：**
- 使用 `FileChannel.map(READ_WRITE, 0, fileSize)` 进行 mmap 映射
- 版本号通过 `volatile` 语义实现无锁的 publish-subscribe 同步
- 整个方法是幂等可重试的，任何异常都会释放资源后重新进入循环

---

## 8. conclude() 中 connectToDriver 之后的 Buffer 初始化

`connectToDriver()` 成功后，客户端拿到了 `cncByteBuffer`（整个 CnC 文件的 mmap）和 `toDriverBuffer`。接下来在同一块 mmap 内存上创建其余通信通道：

```
connectToDriver() 之后的初始化：

cncByteBuffer (MappedByteBuffer, 指向整个 cnc.dat 的 mmap 内存)
  │
  ├─ [0, 128)         → cncMetaDataBuffer        (已在 connectToDriver 中创建)
  │
  ├─ [128, ...)        → toDriverBuffer           (已在 connectToDriver 中创建)
  │                     ManyToOneRingBuffer
  │                     客户端写命令 → Driver 消费
  │
  ├─ [128+toDriverLen, ...)  → toClientBuffer
  │                     CopyBroadcastReceiver(BroadcastReceiver(...))
  │                     Driver 写响应 → 客户端读取
  │                     CopyBroadcastReceiver 拷贝到本地 scratch buffer 防止被覆盖
  │
  ├─ [... , ...)       → countersMetaDataBuffer   (计数器标签/名称)
  │
  ├─ [... , ...)       → countersValuesBuffer     (计数器数值)
  │                     流控位置(pub-limit, sub-pos)、系统监控指标等
  │                     全部通过 mmap 共享内存实现零拷贝读写
  │
  └─ driverProxy = new DriverProxy(toDriverBuffer, clientId)
                        封装写命令 API，后续 addPublication/addSubscription
                        都通过 driverProxy 将命令写入共享内存
```

---

## 9. 一图总结：完整连接时序

```
时间线 ──────────────────────────────────────────────────────────────────▶

Driver 进程                                 Client 进程
──────────                                  ────────────
                                            Aeron.connect(ctx)
                                              │
mapNewFile("cnc.dat")                         │
  创建文件 + mmap                              │
                                              │
fillMetaData(各 buffer 长度, PID, ...)          │  ctx.conclude()
  写入 metadata 字段                            │    ├─ connectToDriver()
                                              │    │    │
consumerHeartbeatTime(now)                    │    │    ├─ awaitCncFileCreation()
  写入第一次心跳                                │    │    │    轮询等待 cnc.dat 存在...
                                              │    │    │
signalCncReady()  ← putIntVolatile(版本号)     │    │    │    getIntVolatile(版本号) → 非零 ✓
  ──── volatile store barrier ────            │    │    │    ──── volatile load barrier ────
                                              │    │    │    → 保证所有 metadata 可见
                                              │    │    │
                                              │    │    ├─ isCncFileLengthSufficient ✓
                                              │    │    ├─ new ManyToOneRingBuffer (mmap 上)
                                              │    │    ├─ consumerHeartbeatTime > 0 ✓
Conductor 线程开始轮询消费                      │    │    ├─ heartbeat 在超时窗口内 ✓
toDriverBuffer.read(...)                      │    │    └─ toDriverBuffer = ringBuffer ✅
                                              │    │
                                              │    ├─ 创建 toClientBuffer (mmap 上)
                                              │    ├─ 创建 counters buffers (mmap 上)
                                              │    └─ 创建 DriverProxy
                                              │
                                              ├─ new ClientConductor
                                              └─ 启动 Conductor 线程
                                                    开始轮询 toClientBuffer
                                                    开始发送 keepalive 心跳

                 ★ 双方通过同一块 mmap 内存通信，连接完成 ★
```

---

## 10. 关键设计思想

### 10.1 零拷贝 IPC

整个 Client ↔ Driver 通信**没有任何 `read()`/`write()` 系统调用**。mmap 之后，Ring Buffer 的读写就是普通的**内存访问指令**（通过 `sun.misc.Unsafe`），操作系统透明地管理物理页映射。

### 10.2 无锁并发

- **toDriverBuffer（ManyToOneRingBuffer）**：多客户端线程用 CAS 竞争写入，Driver 的 Conductor 单线程消费——无锁
- **toClientBuffer（BroadcastReceiver）**：Driver 单线程写入，多客户端各自 copy 读取——无锁
- **Counters**：每个 counter 占独立的 cache line，原子 `long` 读写——无锁

### 10.3 Volatile 同步协议

Driver 端用 `putIntVolatile` 写入版本号作为"发布信号"，客户端用 `getIntVolatile` 读取。这利用了 Java Memory Model 的 **happens-before** 语义：volatile 写之前的所有普通写入，对 volatile 读之后的所有读取可见。因此客户端读到版本号时，所有 metadata 字段都已保证可见。

### 10.4 心跳存活检测

Driver 的 Conductor 线程会周期性更新 `consumerHeartbeatTime`。客户端通过检查这个时间戳来判断 Driver 是否存活，避免连接到已死的 Driver 残留的 CnC 文件。

### 10.5 优雅降级

整个连接过程是幂等可重试的循环：
- CnC 文件不存在？→ 等待
- 文件太小/未初始化？→ 释放 mmap，等待重试
- 心跳过期（旧 Driver）？→ 释放 mmap，等待新 Driver
- 超过 deadline？→ 抛出 `DriverTimeoutException`

---

## 11. addSubscription() 完整流程解析

当应用代码执行：

```java
Subscription subscription = aeron.addSubscription("aeron:udp?endpoint=localhost:40123", 10);
```

背后发生了一套复杂的**命令-响应-事件**三阶段协议，全部通过 CnC 共享内存完成。

### 11.1 调用链总览

```
应用线程                         ClientConductor              Driver Conductor          Receiver 线程
────────                        ────────────────             ────────────────          ────────────
aeron.addSubscription(ch, sid)
  │
  └─ conductor.addSubscription(ch, sid)
       │
       ├─ ① driverProxy.addSubscription(ch, sid)
       │     └─ toDriverBuffer.tryClaim(ADD_SUBSCRIPTION)    ──写入──▶  [CnC toDriverBuffer]
       │        填充 Flyweight → commit                                      │
       │                                                                      ▼
       ├─ ② new Subscription(...)                            从 RingBuffer 消费命令
       │     images = [] (空)                                      │
       │                                                           ├─ 解析 channel URI
       ├─ ③ resourceByRegIdMap.put(correlationId, sub)            ├─ 创建 ReceiveChannelEndpoint
       │                                                           ├─ receiverProxy.addSubscription()
       ├─ ④ awaitResponse(correlationId)                          │     └──命令──▶ 注册到 DataPacketDispatcher
       │     │                                                     │
       │     │  轮询 toClientBuffer                                ├─ clientProxy.onSubscriptionReady()
       │     │       ◀──读取──  [CnC toClientsBuffer] ◀──写入──    │     └─ 写入 ON_SUBSCRIPTION_READY
       │     │                                                     │
       │     └─ receivedCorrelationId == correlationId ✓           └─ linkMatchingImages()
       │         return                                                 如果已有匹配 Publisher：
       │                                                                clientProxy.onAvailableImage()
       └─ return subscription                                           └─ 写入 ON_AVAILABLE_IMAGE
                                                                              │
                                                                              ▼
       后续 ClientConductor 轮询到 ON_AVAILABLE_IMAGE:                   [CnC toClientsBuffer]
         onAvailableImage()
           ├─ mmap 打开 Publisher 的 LogBuffer 文件
           ├─ new Image(logBuffers, subscriberPosition)
           └─ subscription.addImage(image)  ← 此时 subscription 可以 poll 到数据了
```

### 11.2 第一步：DriverProxy 写入命令到共享内存

> 源码位置：`DriverProxy.addSubscription()`

```java
public long addSubscription(final String channel, final int streamId)
{
    // 原子分配 correlationId
    final long correlationId = toDriverCommandBuffer.nextCorrelationId();

    // 计算 Flyweight 消息总长度
    final int length = SubscriptionMessageFlyweight.computeLength(channel.length());

    // CAS 在 ring buffer 中预留空间
    final int index = toDriverCommandBuffer.tryClaim(ADD_SUBSCRIPTION, length);

    // 直接在 mmap 内存上序列化命令（零拷贝 Flyweight 模式）
    subscriptionMessageFlyweight
        .wrap(toDriverCommandBuffer.buffer(), index)
        .registrationCorrelationId(registrationId)
        .streamId(streamId)
        .channel(channel)
        .clientId(clientId)
        .correlationId(correlationId);

    // volatile write 提交，Driver 可见
    toDriverCommandBuffer.commit(index);

    return correlationId;
}
```

**Ring Buffer 三阶段写入协议：**

```
tryClaim (CAS)          填充 Flyweight            commit (volatile write)
───────────────         ──────────────            ─────────────────────
  预留空间                 写入字段                  设置 length → Driver 可消费
  ┌─────────┐           ┌─────────┐              ┌─────────┐
  │ PADDING │   ──▶     │ DATA    │    ──▶       │ READY   │
  └─────────┘           └─────────┘              └─────────┘
```

**SubscriptionMessageFlyweight 消息布局：**

```
偏移量   大小      字段
──────   ─────    ──────
0        8 字节   clientId          客户端 ID
8        8 字节   correlationId     命令关联 ID（用于匹配响应）
16       8 字节   registrationId    注册关联 ID（新订阅为 -1）
24       4 字节   streamId          流 ID
28       4 字节   channel.length    通道字符串长度
32       变长     channel           通道 URI 字符串 "aeron:udp?endpoint=..."
```

### 11.3 第二步：创建客户端 Subscription 对象

> 源码位置：`ClientConductor.addSubscription()`

```java
final Subscription subscription = new Subscription(
    this,                         // ClientConductor 引用
    channel,                      // "aeron:udp?endpoint=localhost:40123"
    streamId,                     // 10
    correlationId,                // 全局唯一 ID
    availableImageHandler,        // Publisher 连接时的回调
    unavailableImageHandler);     // Publisher 断开时的回调
```

**此时 `subscription.images` 是空数组。** Subscription 本身只是一个"注册凭证"，真正的数据通道（Image）要等到 Publisher 连接后才建立。

Subscription 的关键字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `registrationId` | `long` | 等于 `correlationId`，全局唯一标识 |
| `images` | `volatile Image[]` | 可用的数据流，初始为空数组 |
| `channel` | `String` | 通道 URI |
| `streamId` | `int` | 流 ID |
| `roundRobinIndex` | `int` | poll 时轮询 Image 的起始下标 |
| `availableImageHandler` | handler | Image 可用时的回调 |

### 11.4 第三步：awaitResponse 阻塞等待 Driver 响应

> 源码位置：`ClientConductor.awaitResponse()`

```java
private void awaitResponse(final long correlationId)
{
    final long deadlineNs = nanoClock.nanoTime() + driverTimeoutNs;

    do
    {
        // 从 toClientBuffer（CnC 共享内存 BroadcastReceiver）读取 Driver 的响应
        service(correlationId);
          └─ driverEventsAdapter.receive(correlationId)
               └─ receiver.receive(this)   // 从 mmap 中读取消息
                    └─ onMessage(msgTypeId, buffer, index, length)
                         // 按 msgTypeId 分发：
                         // ON_SUBSCRIPTION_READY → conductor.onNewSubscription()
                         // ON_ERROR             → 设置 driverException

        // 检查是否收到匹配的响应
        if (driverEventsAdapter.receivedCorrelationId() == correlationId)
        {
            return;  // ✅ 成功
        }
    }
    while (deadlineNs - nanoClock.nanoTime() > 0);

    throw new DriverTimeoutException(...);  // ❌ 超时
}
```

**关键理解：** `awaitResponse` 并不是 socket 阻塞等待，而是在一个忙等循环中不断**轮询 CnC 共享内存**。`toClientBuffer` 是 CnC 文件中 Driver→Client 方向的 BroadcastReceiver 区段，Driver 通过 `ClientProxy.transmit()` 将响应写入这块 mmap 内存，客户端在这里读取。

### 11.5 第四步：Driver 端处理 ADD_SUBSCRIPTION

> 源码位置：`DriverConductor.onAddNetworkSubscription()`

Driver 的 Conductor 线程从 toDriverBuffer（RingBuffer）消费到 ADD_SUBSCRIPTION 命令后：

```java
void onAddNetworkSubscription(String channel, int streamId, long registrationId, long clientId)
{
    // 1. 解析 channel URI → UdpChannel
    UdpChannel udpChannel = UdpChannel.parse(channel, nameResolver, false);

    // 2. 获取或创建 ReceiveChannelEndpoint
    //    同一 UDP 地址复用同一个 endpoint（共享 UDP socket）
    ReceiveChannelEndpoint channelEndpoint = getOrCreateReceiveChannelEndpoint(params, udpChannel);

    // 3. 创建 SubscriptionLink 并注册
    NetworkSubscriptionLink subscription = new NetworkSubscriptionLink(
        registrationId, channelEndpoint, streamId, channel, ...);
    subscriptionLinks.add(subscription);

    // 4. 通过 ReceiverProxy 通知 Receiver Agent（接收线程）
    //    在 Receiver 线程中将 subscription 注册到 DataPacketDispatcher
    receiverProxy.addSubscription(channelEndpoint, streamId);

    // 5. ★ 向 Client 回写 ON_SUBSCRIPTION_READY
    //    通过 ClientProxy.onSubscriptionReady() 写入 CnC toClientsBuffer
    clientProxy.onSubscriptionReady(registrationId, channelEndpoint.statusIndicatorCounter().id());

    // 6. 关联已有匹配的 Image（如果 Publisher 已在发送）
    linkMatchingImages(subscription);
}
```

**ClientProxy.onSubscriptionReady() 的底层：**

```java
void onSubscriptionReady(long correlationId, int channelStatusCounterId)
{
    subscriptionReady
        .correlationId(correlationId)
        .channelStatusCounterId(channelStatusCounterId);

    // 通过 BroadcastTransmitter 写入 CnC toClientsBuffer（mmap 共享内存）
    transmitter.transmit(ON_SUBSCRIPTION_READY, buffer, 0, SubscriptionReadyFlyweight.LENGTH);
}
```

### 11.6 第五步：Image 的建立（数据通道打通）

当 Publisher 连接到相同的 channel + streamId 时，Driver 会发送 `ON_AVAILABLE_IMAGE` 事件。ClientConductor 收到后在 `onAvailableImage()` 中：

> 源码位置：`ClientConductor.onAvailableImage()`

```java
void onAvailableImage(
    long correlationId, int sessionId, long subscriptionRegistrationId,
    int subscriberPositionId, String logFileName, String sourceIdentity)
{
    Subscription subscription = resourceByRegIdMap.get(subscriptionRegistrationId);

    // ★ 核心：mmap 打开 Publisher 的 LogBuffer 文件
    // LogBuffer 是 Publisher 写入数据的缓冲区，由 Driver 在处理 ADD_PUBLICATION 时创建
    // 现在 Subscriber 通过 mmap 映射同一文件，实现零拷贝数据传输
    Image image = new Image(
        subscription,
        sessionId,
        new UnsafeBufferPosition(counterValuesBuffer, subscriberPositionId),  // 订阅者位置计数器
        logBuffers(correlationId, logFileName, subscription.channel()),       // mmap LogBuffer
        ...);

    // volatile 写入 images 数组，对 poll 线程立即可见
    subscription.addImage(image);

    // 回调应用层
    handler.onAvailableImage(image);
}
```

**此时 Subscription 才真正具备接收数据的能力。** Image 代表了一条从 Publisher 到 Subscriber 的数据通道：

```
Publisher 进程                     Subscriber 进程
──────────────                    ──────────────
Publication.offer(msg)            subscription.poll(handler, limit)
  │                                 │
  ▼                                 ▼
写入 LogBuffer                    Image.poll()
  │                                 │
  ▼                                 ▼
┌─────────────┐                  ┌─────────────┐
│  LogBuffer   │ ◀── 同一 mmap ──▶│  LogBuffer   │
│  (mmap file) │     物理内存      │  (mmap file) │
└─────────────┘                  └─────────────┘
```

### 11.7 第六步：subscription.poll() 拉取数据

> 源码位置：`Subscription.poll()`

```java
public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
{
    final Image[] images = this.images;  // volatile read
    final int length = images.length;
    int fragmentsRead = 0;

    // Round-robin 轮询所有 Image
    int startingIndex = roundRobinIndex++;
    if (startingIndex >= length)
    {
        roundRobinIndex = startingIndex = 0;
    }

    for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
    {
        // 从 Image 的 LogBuffer（mmap 内存）中直接读取数据帧
        fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
    }

    // 从头部继续轮询（wrap around）
    for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
    {
        fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
    }

    return fragmentsRead;
}
```

**关键设计：**

- `images` 是 `volatile` 的，Conductor 线程添加/删除 Image 后，poll 线程立即可见
- Round-robin 保证多个 Image（多路 Publisher）之间公平消费
- `Image.poll()` 直接从 mmap 内存读取数据帧，**零系统调用、零拷贝**
- 每次 poll 后更新 `subscriberPosition` 计数器，Driver/Publisher 据此实现流控

---

## 12. addSubscription 完整时序图

```
时间线 ──────────────────────────────────────────────────────────────────▶

应用线程                    CnC 共享内存                Driver Conductor        Receiver 线程
────────                   ──────────                  ────────────────        ────────────

aeron.addSubscription()
  │
  ├─ driverProxy
  │  .addSubscription()
  │    tryClaim (CAS)
  │    填充 Flyweight
  │    commit ──────────▶ [toDriverBuffer]
  │                            │
  ├─ new Subscription          │ 消费 ADD_SUBSCRIPTION
  │  (images = [])             │         │
  │                            │    解析 channel URI
  ├─ resourceByRegIdMap        │    创建 ReceiveChannelEndpoint
  │  .put(correlationId)       │         │
  │                            │    receiverProxy ──────────────▶ 注册 subscription
  ├─ awaitResponse()           │    .addSubscription()            到 DataPacketDispatcher
  │    │                       │         │
  │    │ 轮询               ◀──── [toClientsBuffer] ◀─── clientProxy
  │    │ toClientBuffer        │         │                .onSubscriptionReady()
  │    │                       │         │
  │    └─ 匹配到 ✓             │    linkMatchingImages()
  │       return               │    (如果已有 Publisher)
  │                            │         │
  └─ return subscription       │    clientProxy ────────▶ [toClientsBuffer]
                               │    .onAvailableImage()
                               │         │
    ClientConductor            │         │
    后续轮询到               ◀──── [toClientsBuffer]
    ON_AVAILABLE_IMAGE         │
      │                        │
      ├─ logBuffers()          │
      │  mmap LogBuffer 文件   │
      │                        │
      ├─ new Image(            │
      │    logBuffers,         │
      │    subscriberPosition) │
      │                        │
      └─ subscription          │
         .addImage(image)      │         ★ 数据通道打通，可以 poll 了 ★
                               │
    subscription.poll()        │
      └─ image.poll()          │
           直接从 LogBuffer    │
           mmap 内存读取数据   │
```

---

## 13. addSubscription 中的关键数据结构

### 13.1 通信通道一览

| 通道 | 方向 | 底层实现 | 用途 |
|------|------|---------|------|
| toDriverBuffer | Client → Driver | ManyToOneRingBuffer (CnC mmap) | 发送 ADD_SUBSCRIPTION 等命令 |
| toClientsBuffer | Driver → Client | BroadcastReceiver (CnC mmap) | 接收 SUBSCRIPTION_READY / AVAILABLE_IMAGE 等响应 |
| LogBuffer | Publisher → Subscriber | 独立 mmap 文件 | 实际数据传输（通过 Image.poll()） |
| Counters | 双向 | CnC mmap | subscriberPosition 流控、channelStatus 状态 |

### 13.2 关键对象关系

```
Aeron
 └─ ClientConductor
      ├─ driverProxy (DriverProxy)
      │    └─ toDriverCommandBuffer ──────▶ CnC toDriverBuffer (mmap)
      │
      ├─ driverEventsAdapter (DriverEventsAdapter)
      │    └─ receiver (CopyBroadcastReceiver) ◀── CnC toClientsBuffer (mmap)
      │
      └─ resourceByRegIdMap
           └─ correlationId → Subscription
                                  │
                                  └─ images[] (volatile)
                                       └─ Image
                                            ├─ logBuffers ──▶ LogBuffer (mmap 文件)
                                            └─ subscriberPosition ──▶ CnC countersValuesBuffer
```

### 13.3 Flyweight 模式说明

Aeron 大量使用 **Flyweight 模式** 序列化/反序列化消息：

```
传统方式：                           Flyweight 方式：
  对象 → 序列化 → byte[] → 拷贝     直接在 mmap 内存上读写字段
  到 buffer → Driver 反序列化       无中间对象、无拷贝
  → 新对象

  3 次拷贝 + 2 次对象分配            0 次拷贝 + 0 次对象分配
```

Flyweight 只是一个"视图对象"，通过 `wrap(buffer, offset)` 绑定到一段内存，然后直接在该内存上按偏移量读写各字段。没有任何序列化/反序列化开销。

### 13.4 整体设计总结

```
addSubscription 的三个阶段：

┌──────────────────────────────────────────────────────────────────┐
│ 阶段一：命令发送（同步）                                          │
│   Client → [CnC toDriverBuffer] → Driver                       │
│   ADD_SUBSCRIPTION 命令通过 RingBuffer 写入共享内存               │
├──────────────────────────────────────────────────────────────────┤
│ 阶段二：控制面响应（同步等待）                                     │
│   Driver → [CnC toClientsBuffer] → Client                      │
│   ON_SUBSCRIPTION_READY 确认订阅已在 Driver 注册                  │
│   此时 Subscription 对象可用，但 images 为空                       │
├──────────────────────────────────────────────────────────────────┤
│ 阶段三：数据面建立（异步）                                        │
│   Publisher 连接后 Driver 发送 ON_AVAILABLE_IMAGE                 │
│   Client 的 onAvailableImage() mmap LogBuffer → 创建 Image       │
│   subscription.addImage(image) → 此时可以 poll 拉取数据           │
│   数据通过 LogBuffer mmap 文件零拷贝传输                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 14. Driver 端 onAddNetworkSubscription：UDP 端口监听全链路

当 Client 调用 `aeron.addSubscription("aeron:udp?endpoint=localhost:40123", 10)` 后，
Driver 端的 `DriverConductor.onAddNetworkSubscription()` 是**真正创建 UDP Socket 并监听端口**的地方。

### 14.1 整体调用链

```
DriverConductor.onAddNetworkSubscription(channel, streamId, registrationId, clientId)
  │
  ├─ ① UdpChannel.parse("aeron:udp?endpoint=localhost:40123")
  │     └─ 解析 URI → remoteData = InetSocketAddress("localhost", 40123)
  │
  ├─ ② getOrCreateReceiveChannelEndpoint(params, udpChannel, registrationId)
  │     │
  │     ├─ findExistingReceiveChannelEndpoint(udpChannel)
  │     │     → 查找是否已有同一 UDP 地址的 endpoint（复用同一 socket）
  │     │
  │     │  如果不存在，创建新的：
  │     ├─ new DataPacketDispatcher(...)
  │     │     → 按 streamId + sessionId 分发 UDP 数据包
  │     │
  │     ├─ new ReceiveChannelEndpoint(udpChannel, dispatcher, ...)
  │     │     → 继承自 UdpChannelTransport
  │     │     → 持有 bindAddress = udpChannel.remoteData() = localhost:40123
  │     │
  │     ├─ channelEndpoint.openChannel()                        ★ 创建 UDP Socket ★
  │     │     └─ openDatagramChannel(statusIndicator)
  │     │          ├─ DatagramChannel.open(INET)                ← Java NIO 创建 UDP channel
  │     │          ├─ channel.bind(localhost:40123)              ← ★ 绑定 UDP 端口 ★
  │     │          ├─ channel.setOption(SO_RCVBUF, ...)         ← 设置接收缓冲区
  │     │          └─ channel.configureBlocking(false)           ← 非阻塞模式
  │     │
  │     └─ receiverProxy.registerReceiveChannelEndpoint(endpoint)
  │           └─ Receiver 线程中：
  │                dataTransportPoller.registerForRead(endpoint)
  │                  └─ channel.register(selector, OP_READ)     ★ 注册到 NIO Selector ★
  │
  ├─ ③ addNetworkSubscriptionToReceiver(subscription)
  │     └─ receiverProxy.addSubscription(endpoint, streamId)
  │           → Receiver 线程中：向 DataPacketDispatcher 注册 streamId
  │
  ├─ ④ clientProxy.onSubscriptionReady(registrationId, ...)
  │     → 回写 ON_SUBSCRIPTION_READY 到 CnC toClientsBuffer
  │
  └─ ⑤ linkMatchingImages(subscription)
        → 关联已有匹配的 Publisher
```

### 14.2 UdpChannel.parse()：URI 解析为网络地址

```java
UdpChannel.parse("aeron:udp?endpoint=localhost:40123", nameResolver, false)
```

解析后的关键字段：

| 字段 | 值 | 说明 |
|------|-----|------|
| `remoteData` | `InetSocketAddress("localhost", 40123)` | 对于 Subscription 来说就是要 bind 的地址 |
| `localData` | `InetSocketAddress("0.0.0.0", 40123)` | 本地绑定地址 |
| `protocolFamily` | `INET` (IPv4) | 协议族 |
| `canonicalForm` | `"UDP-0.0.0.0:40123-localhost:40123"` | 用于 endpoint 复用查找的 key |

> **注意：** 在 Subscription 的语境下，`remoteData`（endpoint 参数指定的地址）同时用作
> `ReceiveChannelEndpoint` 构造函数的 `endPointAddress` 和 `bindAddress`，
> 即 `DatagramChannel.bind()` 的目标地址。

### 14.3 getOrCreateReceiveChannelEndpoint：创建 UDP Socket

这个方法的核心逻辑是**端口复用**：

```
情况 1：第一次订阅 localhost:40123
  → 创建新的 ReceiveChannelEndpoint
  → 打开 DatagramChannel + bind 端口
  → 注册到 Receiver 的 NIO Selector

情况 2：第二次订阅 localhost:40123（不同 streamId）
  → findExistingReceiveChannelEndpoint 找到已有 endpoint
  → 直接复用，不创建新 socket
  → 只需在 DataPacketDispatcher 中注册新的 streamId
```

**同一个 UDP 地址只有一个 DatagramChannel（一个 socket fd）**，多个 streamId 共享。收到 UDP 包后由 `DataPacketDispatcher` 根据包头中的 `streamId` 分发到不同的 `PublicationImage`。

### 14.4 openDatagramChannel()：绑定 UDP 端口

> 源码位置：`UdpChannelTransport.openDatagramChannel()`

```java
public void openDatagramChannel(final AtomicCounter statusIndicator)
{
    // ① 创建 Java NIO DatagramChannel（UDP Socket）
    sendDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
    receiveDatagramChannel = sendDatagramChannel;  // 收发共用同一个 channel

    if (udpChannel.isMulticast())
    {
        // 组播模式：bind 到通配地址 + join 组播组
        receiveDatagramChannel.setOption(SO_REUSEADDR, true);
        receiveDatagramChannel.bind(new InetSocketAddress(endPointAddress.getPort()));
        receiveDatagramChannel.join(endPointAddress.getAddress(), udpChannel.localInterface());
    }
    else
    {
        // ② ★ 单播模式：bind 到指定地址和端口 ★
        //    bindAddress = "localhost:40123"
        //    相当于执行了 socket.bind(new InetSocketAddress("localhost", 40123))
        //    操作系统内核此时开始在该端口上接收 UDP 数据报
        bindAddress = portManager.getManagedPort(udpChannel, bindAddress);
        sendDatagramChannel.bind(bindAddress);
    }

    // ③ 设置 Socket 选项
    if (0 != socketSndbufLength())
        sendDatagramChannel.setOption(SO_SNDBUF, socketSndbufLength());
    if (0 != socketRcvbufLength())
        receiveDatagramChannel.setOption(SO_RCVBUF, socketRcvbufLength());

    // ④ ★ 关键：配置为非阻塞模式 ★
    //    这样才能用 NIO Selector 进行多路复用轮询
    sendDatagramChannel.configureBlocking(false);
    receiveDatagramChannel.configureBlocking(false);
}
```

**执行完这个方法后，操作系统内核已经在 40123 端口上监听 UDP 数据报了。**

### 14.5 注册到 NIO Selector：开始监听

UDP Socket 创建并绑定后，还需要注册到 Receiver 线程的 NIO Selector，才能被轮询到：

```java
// DriverConductor 中：
receiverProxy.registerReceiveChannelEndpoint(channelEndpoint);

// → ReceiverProxy 将命令投递到 Receiver 线程的命令队列

// → Receiver 线程消费命令：
void onRegisterReceiveChannelEndpoint(ReceiveChannelEndpoint channelEndpoint)
{
    // 注册到 DataTransportPoller 的 NIO Selector
    dataTransportPoller.registerForRead(channelEndpoint, channelEndpoint, 0);
}

// → DataTransportPoller 中：
public void registerForRead(ReceiveChannelEndpoint endpoint, UdpChannelTransport transport, int idx)
{
    // ★ 核心：将 DatagramChannel 注册到 Selector，关注 OP_READ 事件 ★
    // 之后 selector.selectNow() 就能检测到该 channel 上有可读的 UDP 包
    selectionKey = transport.receiveDatagramChannel()
        .register(selector, SelectionKey.OP_READ, channelAndTransport);
}
```

### 14.6 Receiver 线程：收包与分发

Receiver 是一个独立的 Agent 线程，在 `doWork()` 循环中不断轮询：

```java
// Receiver.doWork()
public int doWork()
{
    // 1. 排空 Conductor 发来的命令（注册/注销 endpoint 等）
    commandQueue.drain(CommandProxy.RUN_TASK, COMMAND_DRAIN_LIMIT);

    // 2. ★ 从所有 UDP Socket 收包 ★
    int bytesReceived = dataTransportPoller.pollTransports();

    // 3. 处理各个 PublicationImage（检查连接状态等）
    ...
}

// DataTransportPoller.pollTransports()
public int pollTransports()
{
    // 少量 channel 时直接遍历（避免 Selector 开销）
    if (channelAndTransports.size() <= ITERATION_THRESHOLD)
    {
        for (ChannelAndTransport cat : channelAndTransports)
            poll(cat);  // 直接调用 receive
    }
    else
    {
        selector.selectNow(selectorPoller);  // NIO Selector 多路复用
    }
}

// poll() → receive()
private void receive(ChannelAndTransport channelAndTransport)
{
    // ★ 核心：从 UDP Socket 读取一个数据报 ★
    // receiveDatagramChannel.receive(byteBuffer) → 非阻塞，无数据返回 null
    InetSocketAddress srcAddress = channelAndTransport.transport.receive(byteBuffer);

    if (null != srcAddress)
    {
        // 收到 UDP 包，按帧类型分发
        int frameType = frameType(unsafeBuffer, 0);

        if (HDR_TYPE_DATA == frameType || HDR_TYPE_PAD == frameType)
        {
            // ★ 数据帧 → 交给 ReceiveChannelEndpoint.onDataPacket() ★
            channelEndpoint.onDataPacket(dataMessage, unsafeBuffer, length, srcAddress, transportIndex);
            // → dispatcher.onDataPacket()
            //     → 按 streamId + sessionId 找到 PublicationImage
            //     → image.insertPacket(termId, termOffset, buffer, length)
            //         → 写入 LogBuffer（mmap 文件）
            //         → Subscriber 端 image.poll() 可以读到了
        }
        else if (HDR_TYPE_SETUP == frameType)
        {
            // Setup 帧：Publisher 发来的建连请求
            channelEndpoint.onSetupMessage(...);
        }
    }
}
```

### 14.7 从 UDP 包到应用数据的完整路径

```
Publisher 进程                                        Subscriber 进程
──────────────                                       ──────────────

Publication.offer(msg)                               
  │                                                  
  ▼                                                  
写入 LogBuffer                                       
  │                                                  
  ▼                                                  
Sender 线程读取 LogBuffer                            
  │                                                  
  ▼                                                  
sendDatagramChannel.send(packet, remoteAddr)         
  │                                                  
  ▼ ─── UDP 网络传输 ───▶                            
                                                     Receiver 线程 (doWork 循环)
                                                       │
                                                       ▼
                                                     dataTransportPoller.pollTransports()
                                                       │
                                                       ▼
                                                     receiveDatagramChannel.receive(byteBuffer)
                                                       │  ← 从内核 UDP Socket 缓冲区读取
                                                       ▼
                                                     channelEndpoint.onDataPacket()
                                                       │
                                                       ▼
                                                     DataPacketDispatcher.onDataPacket()
                                                       │  ← 按 streamId+sessionId 分发
                                                       ▼
                                                     PublicationImage.insertPacket()
                                                       │  ← 写入 LogBuffer (mmap)
                                                       ▼
                                                     ┌─────────────┐
                                                     │  LogBuffer   │ ← mmap 共享文件
                                                     └──────┬──────┘
                                                            │
                                                            ▼
                                                     应用线程: subscription.poll()
                                                       → image.poll()
                                                         → 直接从 LogBuffer mmap 读取
                                                         → handler.onMessage(buffer, offset, len)
                                                         → "收到消息！"
```

**关键设计总结：**

| 层次 | 组件 | 职责 |
|------|------|------|
| **传输层** | `UdpChannelTransport` | 创建 `DatagramChannel`，bind UDP 端口 |
| **端点层** | `ReceiveChannelEndpoint` | 复用同一 UDP Socket，管理多个 stream |
| **分发层** | `DataPacketDispatcher` | 按 `streamId + sessionId` 分发 UDP 包 |
| **缓冲层** | `PublicationImage` | 将数据帧写入 LogBuffer（mmap 文件） |
| **应用层** | `Subscription.poll()` | 从 LogBuffer mmap 内存零拷贝读取 |

**Receiver 线程不使用传统的 `selector.select()`（会阻塞），而是使用 `selector.selectNow()`（非阻塞立即返回）或直接遍历所有 channel 调用 `receive()`。** 这种忙轮询（busy-polling）虽然消耗更多 CPU，但消除了线程唤醒延迟，是 Aeron 实现微秒级延迟的关键之一。

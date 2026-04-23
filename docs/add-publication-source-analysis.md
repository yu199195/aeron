# `aeron.addPublication(channel, streamId)` 源码深度解析

## 目录

- [概述](#概述)
- [整体架构图](#整体架构图)
- [完整调用时序](#完整调用时序)
- [第一层：Aeron.addPublication() — 用户入口](#第一层aeronaddpublication--用户入口)
- [第二层：ClientConductor.addPublication() — 客户端协调器](#第二层clientconductoraddpublication--客户端协调器)
- [第三层：DriverProxy.addPublication() — 写入命令到共享内存](#第三层driverproxyaddpublication--写入命令到共享内存)
- [第四层：Driver 端消费命令](#第四层driver-端消费命令)
  - [ClientCommandAdapter — 命令分发](#clientcommandadapter--命令分发)
  - [DriverConductor.onAddNetworkPublication() — 网络 Publication](#driverconductoronaddnetworkpublication--网络-publication)
  - [DriverConductor.onAddIpcPublication() — IPC Publication](#driverconductoronaddipcpublication--ipc-publication)
- [第五层：Driver 回写 PUBLICATION_READY 响应](#第五层driver-回写-publication_ready-响应)
- [第六层：Client 端接收响应并构造 Publication 对象](#第六层client-端接收响应并构造-publication-对象)
  - [awaitResponse() — 阻塞等待](#awaitresponse--阻塞等待)
  - [onNewPublication() — 构造 ConcurrentPublication](#onnewpublication--构造-concurrentpublication)
- [关键数据结构与通信机制](#关键数据结构与通信机制)
- [流程总结](#流程总结)

---

## 概述

`aeron.addPublication(channel, streamId)` 是 Aeron 客户端创建消息发布通道的核心 API。该调用会触发一次**跨进程的请求-响应交互**，通过 CnC（Command and Control）共享内存文件完成 Client 与 MediaDriver 之间的协调。

**核心要点：**
- Client 通过 **to-driver RingBuffer** 向 Driver 发送 `ADD_PUBLICATION` 命令
- Driver 处理命令后，通过 **to-clients BroadcastBuffer** 回写 `PUBLICATION_READY` 响应
- Client 阻塞等待响应，收到后 **mmap** Driver 创建的 LogBuffer 文件，构建 `ConcurrentPublication` 对象
- 后续的数据发送（`offer()`）直接写入 mmap 映射的 LogBuffer，**不再经过 Driver 中转**

---

## 整体架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                          Client 进程                                │
│                                                                      │
│  ┌──────────┐    ┌──────────────────┐    ┌──────────────┐           │
│  │  Aeron   │───>│ ClientConductor  │───>│  DriverProxy │           │
│  │ .addPub  │    │  .addPublication │    │ .addPub      │           │
│  └──────────┘    └────────┬─────────┘    └──────┬───────┘           │
│                           │ awaitResponse()      │                   │
│                           │                      ▼                   │
│                           │         ┌────────────────────────┐       │
│                           │         │  CnC 共享内存文件       │       │
│                           │         │ ┌────────────────────┐ │       │
│                           │         │ │ to-driver RingBuf  │ │       │
│                           │         │ │  (Client → Driver) │─┼───┐   │
│                           │         │ └────────────────────┘ │   │   │
│                           │         │ ┌────────────────────┐ │   │   │
│                    poll() │◄────────┤ │ to-clients Broad   │ │   │   │
│                           │         │ │  (Driver → Client) │ │   │   │
│                           │         │ └────────────────────┘ │   │   │
│                           ▼         └────────────────────────┘   │   │
│              ┌──────────────────────┐                             │   │
│              │ onNewPublication()   │                             │   │
│              │ mmap LogBuffer 文件   │                             │   │
│              │ 构造 ConcurrentPub   │                             │   │
│              └──────────────────────┘                             │   │
└──────────────────────────────────────────────────────────────────┼───┘
                                                                   │
                ┌──────────────────────────────────────────────────┼───┐
                │                    Driver 进程                    │   │
                │                                                  ▼   │
                │  ┌─────────────────────┐   ┌──────────────────────┐  │
                │  │ ClientCommandAdapter│◄──│ to-driver RingBuffer │  │
                │  │ 命令分发 switch      │   └──────────────────────┘  │
                │  └─────────┬───────────┘                             │
                │            │ ADD_PUBLICATION                          │
                │            ▼                                         │
                │  ┌──────────────────────────────┐                    │
                │  │    DriverConductor            │                    │
                │  │  onAddNetworkPublication()    │                    │
                │  │  或 onAddIpcPublication()     │                    │
                │  │                               │                    │
                │  │  1. 解析 channel URI           │                    │
                │  │  2. 获取/创建 Endpoint         │                    │
                │  │  3. 分配 LogBuffer 文件        │                    │
                │  │  4. 分配 Counters             │                    │
                │  │  5. 创建 NetworkPublication   │                    │
                │  └─────────┬─────────────────────┘                    │
                │            │                                         │
                │            ▼                                         │
                │  ┌──────────────────────────────┐                    │
                │  │ ClientProxy                   │                    │
                │  │ .onPublicationReady()         │                    │
                │  │  写入 to-clients BroadcastBuf │                    │
                │  └──────────────────────────────┘                    │
                └──────────────────────────────────────────────────────┘
```

---

## 完整调用时序

```
  用户代码                Aeron           ClientConductor        DriverProxy         CnC共享内存          ClientCommandAdapter    DriverConductor         ClientProxy
    │                      │                   │                    │                    │                       │                    │                     │
    │ addPublication()     │                   │                    │                    │                       │                    │                     │
    │─────────────────────>│                   │                    │                    │                       │                    │                     │
    │                      │  addPublication() │                    │                    │                       │                    │                     │
    │                      │──────────────────>│                    │                    │                       │                    │                     │
    │                      │                   │  lock()            │                    │                       │                    │                     │
    │                      │                   │  ensureActive()    │                    │                       │                    │                     │
    │                      │                   │                    │                    │                       │                    │                     │
    │                      │                   │  addPublication()  │                    │                       │                    │                     │
    │                      │                   │──────────────────>│                    │                       │                    │                     │
    │                      │                   │                    │ tryClaim()         │                       │                    │                     │
    │                      │                   │                    │───────────────────>│ [写入 ADD_PUBLICATION] │                    │                     │
    │                      │                   │                    │ commit()           │                       │                    │                     │
    │                      │                   │                    │───────────────────>│                       │                    │                     │
    │                      │                   │  <correlationId>   │                    │                       │                    │                     │
    │                      │                   │<──────────────────│                    │                       │                    │                     │
    │                      │                   │                    │                    │                       │                    │                     │
    │                      │                   │  awaitResponse()   │                    │                       │                    │                     │
    │                      │                   │  ┌─── loop ──┐    │                    │                       │                    │                     │
    │                      │                   │  │           │    │                    │  onMessage()           │                    │                     │
    │                      │                   │  │           │    │                    │──────────────────────>│                    │                     │
    │                      │                   │  │           │    │                    │                       │  addPublication()  │                     │
    │                      │                   │  │           │    │                    │                       │──────────────────>│                     │
    │                      │                   │  │           │    │                    │                       │                    │  1.解析URI           │
    │                      │                   │  │           │    │                    │                       │                    │  2.创建Endpoint      │
    │                      │                   │  │           │    │                    │                       │                    │  3.分配LogBuffer     │
    │                      │                   │  │           │    │                    │                       │                    │  4.创建Publication   │
    │                      │                   │  │           │    │                    │                       │                    │                     │
    │                      │                   │  │           │    │                    │                       │                    │ onPublicationReady() │
    │                      │                   │  │           │    │                    │                       │                    │────────────────────>│
    │                      │                   │  │           │    │                    │  [写入 PUB_READY]      │                    │                     │
    │                      │                   │  │           │    │                    │<─────────────────────────────────────────────────────────────────│
    │                      │                   │  │           │    │                    │                       │                    │                     │
    │                      │                   │  │ service() │    │                    │                       │                    │                     │
    │                      │                   │  │ 读取响应   │    │                    │                       │                    │                     │
    │                      │                   │  │ onNewPub()│    │                    │                       │                    │                     │
    │                      │                   │  │ mmap log  │    │                    │                       │                    │                     │
    │                      │                   │  └───────────┘    │                    │                       │                    │                     │
    │                      │                   │                    │                    │                       │                    │                     │
    │                      │  <ConcurrentPub>  │                    │                    │                       │                    │                     │
    │                      │<──────────────────│                    │                    │                       │                    │                     │
    │  <ConcurrentPub>     │                   │                    │                    │                       │                    │                     │
    │<─────────────────────│                   │                    │                    │                       │                    │                     │
```

---

## 第一层：Aeron.addPublication() — 用户入口

**文件：** `aeron-client/src/main/java/io/aeron/Aeron.java`

```java
/**
 * 添加一个用于向订阅者发布消息的 ConcurrentPublication（线程安全，多线程可共享）。
 *
 * @param channel  媒体层通道（如 "aeron:udp?endpoint=localhost:40123" 或 "aeron:ipc"）
 * @param streamId 通道内的流 ID，用于在同一通道上多路复用不同的数据流
 * @return 新的 ConcurrentPublication，可用于 offer() 发送消息
 */
public ConcurrentPublication addPublication(final String channel, final int streamId)
{
    // 直接委托给 ClientConductor 处理
    // conductor 是在 Aeron.connect() 阶段创建的，持有与 Driver 通信所需的所有资源
    return conductor.addPublication(channel, streamId);
}
```

**说明：** 这是一个简单的委托方法，真正的逻辑在 `ClientConductor` 中。`Aeron` 类本身是用户面向的门面（Facade），封装了底层复杂的交互细节。

---

## 第二层：ClientConductor.addPublication() — 客户端协调器

**文件：** `aeron-client/src/main/java/io/aeron/ClientConductor.java`

```java
/**
 * 同步添加 Publication：
 * 1. 向 Driver 发送 ADD_PUBLICATION 命令
 * 2. 阻塞等待 Driver 返回 PUBLICATION_READY
 * 3. 根据 Driver 返回的 logFileName 等信息构造 ConcurrentPublication
 */
ConcurrentPublication addPublication(final String channel, final int streamId)
{
    // ① 加锁：保证同一时刻只有一个线程执行 Client API 操作
    //   Aeron Client 所有公共 API 都需要串行化
    clientLock.lock();
    try
    {
        // ② 前置检查
        ensureActive();       // 确认 Client 未关闭
        ensureNotReentrant(); // 防止在回调中重入调用（如在 availableImageHandler 中再调 addPublication）

        // ③ 通过 DriverProxy 向 Driver 发送 ADD_PUBLICATION 命令
        //    返回的 registrationId 就是本次命令的 correlationId（唯一标识）
        final long registrationId = driverProxy.addPublication(channel, streamId);

        // ④ 暂存 channel 字符串，后续 onNewPublication() 回调时需要用到
        stashedChannelByRegistrationId.put(registrationId, channel);

        // ⑤ 阻塞等待 Driver 的响应
        //    内部循环 poll CnC to-clients BroadcastBuffer，直到收到匹配的 correlationId
        //    收到 PUBLICATION_READY 后会触发 onNewPublication() 回调，
        //    在那里构造 ConcurrentPublication 并存入 resourceByRegIdMap
        awaitResponse(registrationId);

        // ⑥ 从 map 中取出已构造好的 ConcurrentPublication 返回给用户
        return (ConcurrentPublication)resourceByRegIdMap.get(registrationId);
    }
    finally
    {
        clientLock.unlock();
    }
}
```

**关键点：**
- `clientLock` 是 `ReentrantLock`，保证 API 调用的线程安全
- `driverProxy.addPublication()` 并非 RPC 调用，而是**写入共享内存**
- `awaitResponse()` 是一个 busy-wait 循环，不断从共享内存中读取 Driver 的响应

---

## 第三层：DriverProxy.addPublication() — 写入命令到共享内存

**文件：** `aeron-client/src/main/java/io/aeron/DriverProxy.java`

```java
/**
 * 向 Driver 发送 ADD_PUBLICATION 命令。
 * 实际上是将命令编码后写入 CnC 文件中的 to-driver ManyToOneRingBuffer。
 *
 * @param channel  通道 URI 字符串
 * @param streamId 流 ID
 * @return correlationId（命令唯一标识，用于后续匹配响应）
 */
public long addPublication(final String channel, final int streamId)
{
    // ① 从 RingBuffer 获取一个全局递增的 correlationId
    //    这个 ID 存储在 RingBuffer 的 metadata 区域，通过 CAS 原子递增
    final long correlationId = toDriverCommandBuffer.nextCorrelationId();

    // ② 计算消息总长度：固定头部 + channel 字符串长度
    final int length = PublicationMessageFlyweight.computeLength(channel.length());

    // ③ 在 RingBuffer 中 claim 一块空间
    //    msgTypeId = ADD_PUBLICATION，Driver 端根据此类型分发处理
    //    返回值 index 是写入位置的偏移量；< 0 表示 RingBuffer 已满
    final int index = toDriverCommandBuffer.tryClaim(ADD_PUBLICATION, length);
    if (index < 0)
    {
        throw new AeronException("failed to write add publication command");
    }

    // ④ 通过 Flyweight 模式零拷贝编码消息内容
    //    直接在 RingBuffer 的 buffer 上写入字段，避免中间序列化
    publicationMessageFlyweight
        .wrap(toDriverCommandBuffer.buffer(), index)  // 包裹到 RingBuffer 的底层 buffer
        .streamId(streamId)                            // 写入 streamId
        .channel(channel)                              // 写入 channel URI 字符串
        .clientId(clientId)                            // 写入 clientId（标识哪个 Client）
        .correlationId(correlationId);                 // 写入 correlationId（命令标识）

    // ⑤ 提交：设置消息长度标志位，使 Driver 可见
    //    底层使用 LAZYPUT + StoreStore barrier 保证可见性
    toDriverCommandBuffer.commit(index);

    return correlationId;
}
```

### 消息布局

```
  RingBuffer 中的消息格式：
  ┌──────────────────────────────────────────────────────────┐
  │ Record Header (8 bytes)                                  │
  │ ┌──────────┬──────────┐                                  │
  │ │ length   │ msgTypeId│  ← ADD_PUBLICATION = 0x01        │
  │ └──────────┴──────────┘                                  │
  ├──────────────────────────────────────────────────────────┤
  │ PublicationMessageFlyweight                              │
  │ ┌──────────────┬─────────────┬───────────────────┐       │
  │ │ correlationId│  clientId   │    streamId        │       │
  │ │   (8 bytes)  │  (8 bytes)  │   (4 bytes)       │       │
  │ ├──────────────┴─────────────┴───────────────────┤       │
  │ │ channel string (length-prefixed UTF-8)          │       │
  │ │ "aeron:udp?endpoint=localhost:40123"            │       │
  │ └────────────────────────────────────────────────┘       │
  └──────────────────────────────────────────────────────────┘
```

---

## 第四层：Driver 端消费命令

### ClientCommandAdapter — 命令分发

**文件：** `aeron-driver/src/main/java/io/aeron/driver/ClientCommandAdapter.java`

Driver 的 Conductor 线程会周期性地从 to-driver RingBuffer 中消费消息：

```java
/**
 * 从 to-driver RingBuffer 读取消息后的分发入口。
 * 根据 msgTypeId 将消息路由到对应的 DriverConductor 方法。
 */
public ControlledMessageHandler.Action onMessage(
    final int msgTypeId, final MutableDirectBuffer buffer,
    final int index, final int length)
{
    // ... 省略前置检查 ...

    switch (msgTypeId)
    {
        case ADD_PUBLICATION:  // ← 我们关注的分支
        {
            publicationMsgFlyweight.wrap(buffer, index);
            publicationMsgFlyweight.validateLength(msgTypeId, length);

            correlationId = publicationMsgFlyweight.correlationId();
            addPublication(correlationId, false);  // isExclusive = false
            break;
        }

        case ADD_EXCLUSIVE_PUBLICATION:
        {
            // ... 类似，isExclusive = true
        }
        // ... 其他命令类型
    }
}

/**
 * 根据 channel 前缀判断走 IPC 还是网络路径：
 * - "aeron:ipc" 开头 → onAddIpcPublication()
 * - 其他（如 "aeron:udp?..."） → onAddNetworkPublication()
 */
private void addPublication(final long correlationId, final boolean isExclusive)
{
    final long clientId = publicationMsgFlyweight.clientId();
    final int streamId = publicationMsgFlyweight.streamId();
    final String channel = publicationMsgFlyweight.channel();

    if (channel.startsWith(IPC_CHANNEL))
    {
        conductor.onAddIpcPublication(channel, streamId, correlationId, clientId, isExclusive);
    }
    else
    {
        conductor.onAddNetworkPublication(channel, streamId, correlationId, clientId, isExclusive);
    }
}
```

---

### DriverConductor.onAddNetworkPublication() — 网络 Publication

**文件：** `aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java`

这是**网络类型 Publication**（如 UDP）的核心处理流程：

```java
/**
 * 处理 ADD_PUBLICATION 命令（网络类型）
 *
 * 当 Client 调用 aeron.addPublication("aeron:udp?endpoint=...", streamId) 时触发。
 */
void onAddNetworkPublication(
    final String channel, final int streamId,
    final long correlationId, final long clientId, final boolean isExclusive)
{
    executeAsyncClientTask(
        correlationId,
        // ============== 异步阶段 1：解析 Channel URI ==============
        // 可能涉及 DNS 解析，因此放在异步任务中避免阻塞 Conductor 线程
        () -> UdpChannel.parse(channel, nameResolver, false),

        // ============== 异步阶段 2：创建 Publication ==============
        (asyncResult) ->
        {
            final UdpChannel udpChannel = asyncResult.get();
            final ChannelUri channelUri = udpChannel.channelUri();

            // 1. 从 URI 中提取 Publication 参数
            //    （termLength, mtu, initialTermId, sessionId 等）
            final PublicationParams params =
                getPublicationParams(channelUri, ctx, this, streamId, udpChannel.canonicalForm());

            // 2. 参数校验
            validateExperimentalFeatures(ctx.enableExperimentalFeatures(), udpChannel);
            validateEndpointForPublication(udpChannel);
            validateControlForPublication(udpChannel);

            // 3. 获取或创建 SendChannelEndpoint
            //    相同 UDP 地址复用同一个 endpoint（底层同一个 DatagramChannel）
            //    一个 endpoint 可承载多个不同 streamId 的 Publication
            final SendChannelEndpoint channelEndpoint =
                getOrCreateSendChannelEndpoint(params, udpChannel, correlationId);

            // 4. 非 exclusive 模式：尝试复用已有 Publication
            //    如果已经存在相同 streamId + endpoint 的 Publication，直接复用
            //    这就是 ConcurrentPublication 支持多个 Client 共享同一个 Publication 的原理
            NetworkPublication publication = null;
            if (!isExclusive)
            {
                publication = findPublication(
                    networkPublications, streamId, channelEndpoint, params.responseCorrelationId);
            }

            boolean isNewPublication = false;
            if (null == publication)
            {
                // 5. 创建全新的 NetworkPublication
                //    内部会：
                //    - 检查 sessionId 冲突
                //    - 在磁盘上分配 LogBuffer 文件（默认 /dev/shm/aeron-xxx/publications/）
                //    - 分配 publisherPos、publisherLimit、senderPos、senderLimit 等 Counters
                //    - 创建 FlowControl 实例（单播 / 多播）
                //    - 创建 RetransmitHandler（处理 NAK 重传）
                //    - 注册到 Sender 线程（senderProxy.newNetworkPublication）
                checkForSessionClash(params.sessionId, streamId, udpChannel.canonicalForm(), channel);
                publication = newNetworkPublication(
                    correlationId, clientId, streamId, channel,
                    udpChannel, channelEndpoint, params, isExclusive);
                isNewPublication = true;
            }
            else
            {
                // 6. 复用已有 Publication：校验参数一致性
                //    termLength, mtu, sessionId 等必须匹配
                confirmMatch(channelUri, params, publication.rawLog(),
                    publication.sessionId(), publication.channel(),
                    publication.initialTermId(), publication.startingTermId(),
                    publication.startingTermOffset());
            }

            // 7. 记录 Client ↔ Publication 的关联关系（PublicationLink）
            //    用于 Client 断连或 close() 时清理
            publicationLinks.add(new PublicationLink(
                correlationId, getOrAddClient(clientId), publication));

            // 8. ★ 通过 ClientProxy 向 Client 回写 PUBLICATION_READY 事件 ★
            //    Client 收到后会用 logFileName 做 mmap 映射
            clientProxy.onPublicationReady(
                correlationId,
                publication.registrationId(),
                streamId,
                publication.sessionId(),
                publication.rawLog().fileName(),        // LogBuffer 文件路径
                publication.publisherLimitId(),          // publisherLimit 计数器 ID
                channelEndpoint.statusIndicatorCounterId(),
                isExclusive);

            // 9. 新 Publication 创建后，自动 link 匹配的 Spy Subscription
            //    Spy 可以在发送端本地订阅数据副本，用于监控或本地消费
            if (isNewPublication)
            {
                linkSpies(subscriptionLinks, publication);
            }
        });
}
```

### newNetworkPublication() — 创建 Publication 实例

```java
private NetworkPublication newNetworkPublication(...)
{
    // 1. 创建 FlowControl 实例
    //    单播 → unicastFlowControlSupplier（默认 MaxFlowControl）
    //    多播 → multicastFlowControlSupplier（默认 MaxMulticastFlowControl）
    final FlowControl flowControl = udpChannel.isMulticast() || udpChannel.isMultiDestination() ?
        ctx.multicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId) :
        ctx.unicastFlowControlSupplier().newInstance(udpChannel, streamId, registrationId);

    // 2. 分配 RawLog（LogBuffer 文件）
    //    在 aeronDir/publications/ 目录下创建内存映射文件
    //    文件包含 3 个 term buffer + 1 个 log metadata buffer
    final RawLog rawLog = newNetworkPublicationLog(...);

    // 3. 分配各种 Counter（存储在 CnC 的 counters 区域）
    UnsafeBufferPosition publisherPos = PublisherPos.allocate(...);    // 发布者位置
    UnsafeBufferPosition publisherLmt = PublisherLimit.allocate(...);  // 发布者限制（流控）
    UnsafeBufferPosition senderPos    = SenderPos.allocate(...);      // 发送者位置
    UnsafeBufferPosition senderLmt    = SenderLimit.allocate(...);    // 发送者限制
    AtomicCounter senderBpe           = SenderBpe.allocate(...);      // 背压事件计数
    AtomicCounter senderNaksReceived  = SenderNaksReceived.allocate(...); // NAK 计数

    // 4. 创建 RetransmitHandler（处理 NAK 触发的重传）
    final RetransmitHandler retransmitHandler = new RetransmitHandler(...);

    // 5. 构造 NetworkPublication 对象
    final NetworkPublication publication = new NetworkPublication(
        registrationId, ctx, params, channelEndpoint, rawLog,
        params.publicationWindowLength,
        publisherPos, publisherLmt, senderPos, senderLmt,
        senderBpe, senderNaksReceived,
        params.sessionId, streamId, params.initialTermId,
        flowControl, retransmitHandler, networkPublicationThreadLocals, isExclusive);

    // 6. 注册到各组件
    channelEndpoint.incRef();                      // endpoint 引用计数+1
    networkPublications.add(publication);           // 加入 publication 列表
    senderProxy.newNetworkPublication(publication); // 通知 Sender 线程有新 Publication

    return publication;
}
```

---

### DriverConductor.onAddIpcPublication() — IPC Publication

**文件：** `aeron-driver/src/main/java/io/aeron/driver/DriverConductor.java`

IPC（进程间通信）Publication 不走网络，发送端和接收端**通过共享同一个 LogBuffer 文件实现零拷贝通信**：

```java
/**
 * 处理 ADD_PUBLICATION 命令（IPC 类型）
 * 当 Client 调用 aeron.addPublication("aeron:ipc", streamId) 时触发。
 */
void onAddIpcPublication(
    final String channel, final int streamId,
    final long correlationId, final long clientId, final boolean isExclusive)
{
    IpcPublication publication = null;
    final ChannelUri channelUri = parseUri(channel);
    final PublicationParams params = getPublicationParams(channelUri, ctx, this, streamId, IPC_MEDIA);

    // 非 exclusive 模式：复用相同 streamId 的已有 IPC Publication
    if (!isExclusive)
    {
        publication = findSharedIpcPublication(ipcPublications, streamId, params.responseCorrelationId);
    }

    boolean isNewPublication = false;
    if (null == publication)
    {
        // 创建新的 IPC Publication（分配 LogBuffer、Counters 等）
        checkForSessionClash(params.sessionId, streamId, IPC_MEDIA, channel);
        publication = addIpcPublication(correlationId, clientId, streamId, channel, isExclusive, params);
        isNewPublication = true;
    }
    else
    {
        // 复用已有，校验参数一致性
        confirmMatch(channelUri, params, publication.rawLog(), publication.sessionId(),
            publication.channel(), publication.initialTermId(),
            publication.startingTermId(), publication.startingTermOffset());
    }

    publicationLinks.add(new PublicationLink(correlationId, getOrAddClient(clientId), publication));

    // 回写 PUBLICATION_READY（注意：IPC 没有 channelEndpoint，statusIndicatorId = NO_ID）
    clientProxy.onPublicationReady(
        correlationId, publication.registrationId(), streamId, publication.sessionId(),
        publication.rawLog().fileName(),
        publication.publisherLimitId(),
        ChannelEndpointStatus.NO_ID_ALLOCATED,  // IPC 无网络 endpoint
        isExclusive);

    // 新 IPC Publication 创建后，自动 link 所有匹配的 IPC Subscription
    if (isNewPublication)
    {
        linkIpcSubscriptions(publication);
    }
}
```

**IPC vs 网络 Publication 的区别：**

| 特性 | IPC Publication | Network Publication |
|------|----------------|-------------------|
| 传输介质 | 共享 LogBuffer 文件（mmap） | UDP DatagramChannel |
| Endpoint | 无 | SendChannelEndpoint |
| FlowControl | 无 | 有（单播/多播） |
| DNS 解析 | 不需要 | 可能需要（异步） |
| 延迟 | 纳秒级 | 微秒级 |
| 跨机器 | 不支持 | 支持 |

---

## 第五层：Driver 回写 PUBLICATION_READY 响应

**文件：** `aeron-driver/src/main/java/io/aeron/driver/ClientProxy.java`

```java
/**
 * 通过 CnC 的 to-clients BroadcastBuffer 向 Client 发送 PUBLICATION_READY 事件。
 * Client 收到后，用 logFileName 做 mmap 映射来构造 ConcurrentPublication。
 */
void onPublicationReady(
    final long correlationId,       // 命令的 correlationId，Client 用它匹配响应
    final long registrationId,      // Publication 的注册 ID（首次创建的 correlationId）
    final int streamId,             // 流 ID
    final int sessionId,            // 会话 ID
    final String logFileName,       // ★ LogBuffer 文件路径（Client 要 mmap 这个文件）
    final int positionCounterId,    // publisherLimit 计数器 ID（流控用）
    final int channelStatusCounterId, // 通道状态计数器 ID
    final boolean isExclusive)      // 是否独占
{
    // 通过 Flyweight 编码响应消息
    publicationReady
        .correlationId(correlationId)
        .registrationId(registrationId)
        .sessionId(sessionId)
        .streamId(streamId)
        .publicationLimitCounterId(positionCounterId)
        .channelStatusCounterId(channelStatusCounterId)
        .logFileName(logFileName);

    // 选择消息类型：ON_PUBLICATION_READY 或 ON_EXCLUSIVE_PUBLICATION_READY
    final int msgTypeId = isExclusive ? ON_EXCLUSIVE_PUBLICATION_READY : ON_PUBLICATION_READY;

    // 写入 to-clients BroadcastBuffer
    //   底层是 BroadcastTransmitter.transmit()
    //   使用 one-to-many broadcast 机制，所有连接的 Client 都可以收到
    transmit(msgTypeId, buffer, 0, publicationReady.length());
}
```

---

## 第六层：Client 端接收响应并构造 Publication 对象

### awaitResponse() — 阻塞等待

**文件：** `aeron-client/src/main/java/io/aeron/ClientConductor.java`

```java
/**
 * 阻塞等待 Driver 对指定 correlationId 命令的响应。
 *
 * 核心机制：在 do-while 循环中反复调用 service()，从 CnC 共享内存的
 * toClientBuffer（BroadcastReceiver）中读取 Driver 写入的响应消息。
 */
private void awaitResponse(final long correlationId)
{
    final long nowNs = nanoClock.nanoTime();
    final long deadlineNs = nowNs + driverTimeoutNs;  // 超时时间（默认 10 秒）
    checkTimeouts(nowNs);

    awaitingIdleStrategy.reset();
    do
    {
        // 嵌入式 Driver 模式：直接调用 Driver 的 doWork() 处理命令
        // 独立 Driver 模式：仅做 idle（因为 Driver 在另一个进程中运行）
        if (null == driverAgentInvoker)
        {
            awaitingIdleStrategy.idle();   // 短暂 sleep/yield
        }
        else
        {
            driverAgentInvoker.invoke();   // 嵌入式：驱动 Driver 执行
        }

        // ★ 核心：从 to-clients BroadcastBuffer 读取响应并分发
        //
        // 响应处理链路：
        //   service(correlationId)
        //     └─ driverEventsAdapter.receive(correlationId)
        //          └─ receiver.receive(handler)
        //               └─ onMessage(msgTypeId, buffer, index, length)
        //                    ├─ ON_PUBLICATION_READY  → conductor.onNewPublication()
        //                    ├─ ON_SUBSCRIPTION_READY → conductor.onNewSubscription()
        //                    ├─ ON_AVAILABLE_IMAGE    → conductor.onAvailableImage()
        //                    ├─ ON_ERROR              → 设置 driverException
        //                    └─ ...
        service(correlationId);

        // 检查是否已收到目标 correlationId 的响应
        if (driverEventsAdapter.receivedCorrelationId() == correlationId)
        {
            stashedChannelByRegistrationId.remove(correlationId);
            // 如果 Driver 返回的是错误响应，则抛出异常
            final RegistrationException ex = driverException;
            if (null != ex)
            {
                driverException = null;
                throw ex;
            }
            return;  // 成功返回
        }

        if (Thread.currentThread().isInterrupted())
        {
            terminateConductor();
            throw new AeronException("unexpected interrupt");
        }
    }
    while (deadlineNs - nanoClock.nanoTime() > 0);  // 超时检查

    throw new DriverTimeoutException(
        "no response from MediaDriver within " + SystemUtil.formatDuration(driverTimeoutNs));
}
```

### onNewPublication() — 构造 ConcurrentPublication

当 `service()` 中收到 `ON_PUBLICATION_READY` 消息时，会回调此方法：

```java
/**
 * Driver 端 PUBLICATION_READY 响应的客户端回调。
 * 根据 Driver 返回的信息构造 ConcurrentPublication 对象。
 */
void onNewPublication(
    final long correlationId,       // 命令 correlationId
    final long registrationId,      // Publication 注册 ID
    final int streamId,
    final int sessionId,
    final int publicationLimitId,   // publisherLimit 计数器 ID
    final int statusIndicatorId,    // 通道状态计数器 ID
    final String logFileName)       // LogBuffer 文件路径
{
    // ① 取出之前暂存的 channel 字符串
    final String stashedChannel = stashedChannelByRegistrationId.remove(correlationId);

    // ② 构造 ConcurrentPublication
    final ConcurrentPublication publication = new ConcurrentPublication(
        this,
        stashedChannel,                                       // channel URI
        streamId,
        sessionId,
        new UnsafeBufferPosition(counterValuesBuffer,         // publisherLimit：
            publicationLimitId),                              //   读取 CnC counters 中的流控位置
        statusIndicatorId,
        logBuffers(registrationId, logFileName, stashedChannel), // ★ mmap LogBuffer 文件
        registrationId,
        correlationId);

    // ③ 存入 resourceByRegIdMap，addPublication() 中的 awaitResponse() 返回后
    //    会从这个 map 中取出并返回给用户
    resourceByRegIdMap.put(correlationId, publication);
}
```

### logBuffers() — 内存映射 LogBuffer 文件

```java
/**
 * 打开并 mmap 映射 Driver 创建的 LogBuffer 文件。
 * 该文件包含 3 个 Term Buffer 和 1 个 Log Metadata Buffer。
 *
 * 后续 offer() 发送消息时，直接写入 mmap 映射的内存区域，
 * Driver/Sender 线程通过映射同一文件来读取数据，实现零拷贝。
 */
private LogBuffers logBuffers(
    final long registrationId, final String logFileName, final String channel)
{
    // 先查缓存：如果是复用已有 Publication，LogBuffers 可能已经映射过
    LogBuffers logBuffers = logBuffersByIdMap.get(registrationId);
    if (null == logBuffers)
    {
        // 首次映射：调用 logBuffersFactory.map() 打开文件并 mmap
        logBuffers = logBuffersFactory.map(logFileName);

        // 可选：预触摸（pre-touch）所有页面，避免后续首次写入时的 page fault
        if (ctx.preTouchMappedMemory())
        {
            logBuffers.preTouch();
        }

        logBuffersByIdMap.put(registrationId, logBuffers);
    }

    logBuffers.incRef();  // 引用计数+1
    return logBuffers;
}
```

---

## 关键数据结构与通信机制

### 1. CnC 文件布局

```
CnC (Command and Control) 文件结构：
┌─────────────────────────────────────────┐
│ CnC Metadata (version, pid, etc.)       │  固定大小
├─────────────────────────────────────────┤
│ to-driver RingBuffer                    │  Client → Driver 命令通道
│   （ManyToOneRingBuffer）               │  默认 1MB
├─────────────────────────────────────────┤
│ to-clients BroadcastBuffer              │  Driver → Client 响应通道
│   （BroadcastTransmitter/Receiver）     │  默认 1MB
├─────────────────────────────────────────┤
│ Counters Metadata Buffer                │  计数器元数据（名称、类型等）
├─────────────────────────────────────────┤
│ Counters Values Buffer                  │  计数器值（publisherLimit 等）
│   （各 Counter 的 long 值存储区域）      │
├─────────────────────────────────────────┤
│ Error Log Buffer                        │  错误日志
└─────────────────────────────────────────┘
```

### 2. LogBuffer 文件布局

```
LogBuffer 文件结构（每个 Publication 一个文件）：
┌─────────────────────────────────────────┐
│ Term Buffer 0                           │  默认 16MB
├─────────────────────────────────────────┤
│ Term Buffer 1                           │  默认 16MB
├─────────────────────────────────────────┤
│ Term Buffer 2                           │  默认 16MB
├─────────────────────────────────────────┤
│ Log Metadata Buffer                     │  包含 activeTermCount、
│  (tail positions, active term count,    │  tailPosition 等元数据
│   initial term id, term length, etc.)   │
└─────────────────────────────────────────┘

3 个 Term 轮转使用（active → dirty → clean），
Publisher 写入 active term，写满后切换到下一个。
```

### 3. 关键 Counter 说明

| Counter | 作用 | 谁写 | 谁读 |
|---------|------|------|------|
| **publisherPos** | 发布者已写入的位置 | Client（offer 时） | Driver Conductor |
| **publisherLimit** | 发布者可写入的最大位置 | Driver（流控计算） | Client（offer 时检查） |
| **senderPos** | Sender 已发送到网络的位置 | Sender 线程 | Driver Conductor |
| **senderLimit** | Sender 可发送的最大位置 | Driver Conductor | Sender 线程 |
| **senderBpe** | 背压事件计数 | Sender 线程 | 监控 |

---

## 流程总结

### 完整流程（一句话版本）

> Client 将 `ADD_PUBLICATION` 命令写入 CnC to-driver RingBuffer → Driver 消费命令，创建 LogBuffer 文件和各种 Counter → Driver 将 `PUBLICATION_READY`（含 logFileName）写入 to-clients BroadcastBuffer → Client 读取响应，mmap LogBuffer 文件，构造 `ConcurrentPublication` 返回。

### 详细步骤

```
1. [Client] Aeron.addPublication(channel, streamId)
     │
     ▼
2. [Client] ClientConductor.addPublication()
     │  加锁 → ensureActive → ensureNotReentrant
     │
     ▼
3. [Client] DriverProxy.addPublication()
     │  生成 correlationId
     │  在 to-driver RingBuffer 中 tryClaim 空间
     │  通过 Flyweight 编码 {correlationId, clientId, streamId, channel}
     │  commit 使 Driver 可见
     │
     ▼
4. [Client] awaitResponse(correlationId)
     │  进入 busy-wait 循环
     │
     ▼ （同时，Driver 端在自己的 Conductor 线程中消费 RingBuffer）
     │
5. [Driver] ClientCommandAdapter.onMessage()
     │  解码 ADD_PUBLICATION 命令
     │  根据 channel 前缀分发：
     │    ├─ "aeron:ipc" → onAddIpcPublication()
     │    └─ 其他        → onAddNetworkPublication()
     │
     ▼
6. [Driver] DriverConductor.onAddNetworkPublication()
     │  a. 异步解析 UdpChannel（可能涉及 DNS）
     │  b. 获取或创建 SendChannelEndpoint（复用 UDP Socket）
     │  c. 尝试复用已有 Publication（非 exclusive 模式）
     │  d. 若无可复用：
     │     - 分配 LogBuffer 文件（3 个 term + metadata）
     │     - 分配各种 Counter（publisherPos/Limit, senderPos/Limit 等）
     │     - 创建 FlowControl、RetransmitHandler
     │     - 构造 NetworkPublication 对象
     │     - 注册到 Sender 线程
     │  e. 记录 PublicationLink（Client ↔ Publication 关联）
     │
     ▼
7. [Driver] ClientProxy.onPublicationReady()
     │  将 {correlationId, registrationId, sessionId, streamId,
     │       logFileName, publisherLimitId, channelStatusId}
     │  写入 to-clients BroadcastBuffer
     │
     ▼
8. [Client] awaitResponse() 中 service() 读取到 PUBLICATION_READY
     │
     ▼
9. [Client] ClientConductor.onNewPublication()
     │  a. 用 logFileName 打开并 mmap LogBuffer 文件
     │  b. 用 publisherLimitId 包装为 UnsafeBufferPosition（读取 CnC counter）
     │  c. 构造 ConcurrentPublication 对象
     │  d. 存入 resourceByRegIdMap
     │
     ▼
10. [Client] awaitResponse() 检测到 correlationId 匹配，返回
     │
     ▼
11. [Client] addPublication() 从 resourceByRegIdMap 取出 ConcurrentPublication 返回给用户
     │
     ▼
12. [用户] 拿到 Publication，可以调用 offer() 发送消息了！
      （offer 直接写入 mmap 的 LogBuffer，不再经过 Driver 中转）
```

### 关键设计亮点

1. **零拷贝通信**：Client 和 Driver 通过 mmap 共享同一个 LogBuffer 文件，`offer()` 直接写入映射内存，没有数据拷贝和系统调用。

2. **命令-响应分离**：`addPublication()` 是控制面操作，走 CnC RingBuffer/BroadcastBuffer；`offer()` 是数据面操作，走 LogBuffer，两者完全解耦。

3. **Publication 复用**：非 exclusive 模式下，多个 Client 对同一 `{endpoint, streamId}` 调用 `addPublication()` 会共享同一个底层 Publication，避免资源浪费。

4. **Flyweight 模式**：所有消息编码/解码都使用 Flyweight 直接操作底层 buffer，避免对象分配和序列化开销。

5. **异步 DNS 解析**：网络 Publication 的 URI 解析放在 `executeAsyncClientTask` 中，避免 DNS 查询阻塞 Conductor 线程。

---

## 深入疑问一：LogBuffer 文件什么时候创建的？

### 创建时机

LogBuffer 文件在 **Driver 端处理 `ADD_PUBLICATION` 命令时创建**，具体时机是 `DriverConductor.newNetworkPublication()`（或 `addIpcPublication()`）方法执行过程中。

**不是** Client 端创建，也**不是** Aeron 启动时创建——而是每次 `addPublication()` 需要新建 Publication（非复用已有的）时按需创建。

### 完整创建链路

```
Client: aeron.addPublication("aeron:udp?endpoint=...", streamId)
  │
  ▼ （命令写入 to-driver RingBuffer，Driver Conductor 消费）
  
Driver: DriverConductor.onAddNetworkPublication()
  │
  │  找不到可复用的 Publication → 需要新建
  │
  ▼
Driver: DriverConductor.newNetworkPublication()
  │
  ▼
Driver: DriverConductor.newNetworkPublicationLog()
  │
  ▼
Driver: logFactory.newPublication(registrationId, termLength, isSparse)
  │     （logFactory 类型是 FileStoreLogFactory）
  ▼
Driver: FileStoreLogFactory.newInstance(publicationsDir, correlationId, termLength, useSparseFiles)
  │
  │  1. computeLogLength(termLength, filePageSize)  → 计算文件总大小
  │  2. checkStorage(logLength)                      → 检查磁盘空间
  │  3. streamLocation(rootDir, correlationId)       → 生成文件路径
  │
  ▼
Driver: new MappedRawLog(location, useSparseFiles, logLength, termLength, ...)
  │
  │  ★ 这里是文件实际创建和 mmap 映射的地方：
  │
  │  1. FileChannel.open(logFile, CREATE_NEW)     → 在磁盘上创建新文件
  │  2. logChannel.truncate(logLength)             → 设置文件大小
  │  3. logChannel.map(READ_WRITE, 0, logLength)   → mmap 映射到进程虚拟内存
  │  4. 切分为 3 个 termBuffer + logMetaDataBuffer
  │  5. [可选] preTouchPages()                     → 预触摸页面避免 page fault
  │
  ▼
返回 RawLog → Driver 继续初始化 Log Metadata → 构建 NetworkPublication
  │
  ▼
Driver: clientProxy.onPublicationReady(... rawLog.fileName() ...)
  │     （把 LogBuffer 文件路径通过 PUBLICATION_READY 发给 Client）
  ▼
Client: onNewPublication() → logBuffersFactory.map(logFileName)
        （Client 用同一个文件路径做 mmap 映射，与 Driver 共享物理内存）
```

### 文件路径与布局

```
文件路径：{aeronDir}/publications/{correlationId}.logbuffer

aeronDir 默认值：
  Linux:   /dev/shm/aeron-{user}   （tmpfs 内存文件系统，mmap 直接操作物理内存）
  macOS:   /tmp/aeron-{user}       （磁盘文件系统，但 mmap 仍避免 read/write 系统调用）

文件内部布局：
┌─────────────────────────┐  offset 0
│  Term Buffer 0          │  大小 = termLength（默认 16MB）
├─────────────────────────┤  offset termLength
│  Term Buffer 1          │  大小 = termLength
├─────────────────────────┤  offset 2 × termLength
│  Term Buffer 2          │  大小 = termLength
├─────────────────────────┤  offset 3 × termLength
│  Log Metadata Buffer    │  包含 activeTermCount、tailPosition、
│                         │  initialTermId、termLength、mtu 等元数据
└─────────────────────────┘

总大小 ≈ termLength × 3 + LOG_META_DATA_LENGTH（约 48MB+）
```

### 关键源码位置

| 步骤 | 类 | 方法 | 说明 |
|------|---|------|------|
| 入口 | `DriverConductor` | `newNetworkPublicationLog()` | 调用 logFactory 并初始化 metadata |
| 工厂 | `FileStoreLogFactory` | `newPublication()` → `newInstance()` | 计算大小、检查空间、生成路径 |
| 创建 | `MappedRawLog` | 构造函数 | FileChannel.open + truncate + mmap |
| Client映射 | `ClientConductor` | `logBuffers()` | logBuffersFactory.map(logFileName) |

---

## 深入疑问二：UDP 端口是怎么开启并监听的？

### 创建时机

UDP Socket 在 **Driver 端处理 `ADD_PUBLICATION` 命令时按需创建**，具体时机是 `DriverConductor.getOrCreateSendChannelEndpoint()` 方法中。

**关键点**：相同 UDP 地址的多个 Publication **共享同一个 Socket**（即同一个 `SendChannelEndpoint`），不会为每个 Publication 都创建新的 Socket。

### 完整创建链路

```
Client: aeron.addPublication("aeron:udp?endpoint=localhost:40123", streamId)
  │
  ▼ （命令写入 to-driver RingBuffer，Driver Conductor 消费）

Driver: DriverConductor.onAddNetworkPublication()
  │
  ▼
Driver: getOrCreateSendChannelEndpoint(params, udpChannel, correlationId)
  │
  │  findExistingSendChannelEndpoint(udpChannel)
  │    → 查找相同地址的已有 endpoint
  │    → 找到则直接复用，不创建新 Socket
  │    → 没找到则继续 ↓
  │
  ▼ （需要新建 endpoint 和 UDP Socket）
  │
  │  1. SendChannelStatus.allocate(...)              → 分配通道状态计数器
  │  2. ctx.sendChannelEndpointSupplier().newInstance()  → 创建 SendChannelEndpoint 对象
  │  3. ★ channelEndpoint.openChannel()              → 打开 UDP Socket
  │       │
  │       ▼
  │     SendChannelEndpoint.openChannel()
  │       │
  │       ▼
  │     UdpChannelTransport.openDatagramChannel(statusIndicator)
  │       │
  │       │  ┌─────── 单播模式 ───────┐  ┌─────── 多播模式 ────────────┐
  │       │  │ DatagramChannel.open() │  │ DatagramChannel.open()      │
  │       │  │ bind(bindAddress)      │  │ SO_REUSEADDR = true         │
  │       │  │   └→ 绑定本地端口      │  │ bind(multicastPort)         │
  │       │  │      （可能为临时端口） │  │ join(multicastGroup, iface) │
  │       │  │                        │  │ IP_MULTICAST_IF = iface     │
  │       │  └────────────────────────┘  └─────────────────────────────┘
  │       │  
  │       │  [可选] connect(connectAddress)   → 单播连接到远端
  │       │  SO_SNDBUF / SO_RCVBUF            → 设置缓冲区大小
  │       │  configureBlocking(false)          → 非阻塞模式
  │       │
  │       ▼
  │     更新 localSocketAddress 计数器（记录实际绑定的地址和端口）
  │
  │  4. senderProxy.registerSendChannelEndpoint(channelEndpoint)
  │       │
  │       ▼ （通过队列传递到 Sender 线程）
  │     Sender.onRegisterSendChannelEndpoint(channelEndpoint)
  │       │
  │       ▼
  │     controlTransportPoller.registerForRead(channelEndpoint)
  │       │
  │       ▼
  │     receiveDatagramChannel.register(selector, OP_READ, endpoint)
  │       │
  │       └→ ★ 将 UDP Socket 注册到 Sender 线程的 NIO Selector
  │            后续 Selector.select() 就能监听到来自接收端的控制消息
  │
  │  5. sendChannelEndpointByChannelMap.put(canonicalForm, channelEndpoint)
  │       └→ 缓存 endpoint，后续相同地址的 Publication 直接复用
  │
  ▼
返回 channelEndpoint → 继续创建 NetworkPublication
```

### Sender 线程如何使用这个 Socket

创建完成后，Sender 线程在 `doWork()` 循环中：

```
Sender.doWork()
  │
  ├── 1. 轮询所有 NetworkPublication：
  │      从 LogBuffer（mmap 内存）中读取 Client 写入的消息帧，
  │      通过 sendDatagramChannel.send(buffer, destination) 发送到网络
  │
  └── 2. controlTransportPoller.pollTransports()：
         通过 Selector.select() 监听 receiveDatagramChannel 上的读事件，
         接收来自远端接收者的控制消息：
           ├── Status Message (SM)：携带接收窗口信息，用于流控计算
           └── NAK：通知发送端某些帧丢失，触发重传
```

### 发送端 vs 接收端的 Socket

注意区分：`addPublication()` 创建的是**发送端**的 Socket（`SendChannelEndpoint`），而不是接收端的。

| | 发送端 (SendChannelEndpoint) | 接收端 (ReceiveChannelEndpoint) |
|---|---|---|
| 创建时机 | `addPublication()` | `addSubscription()` |
| 主要用途 | 发送数据帧到网络 | 接收数据帧 |
| 控制消息 | 接收 SM/NAK（流控+重传） | 发送 SM/NAK |
| 所在线程 | Sender 线程 | Receiver 线程 |

### 关键源码位置

| 步骤 | 类 | 方法 | 说明 |
|------|---|------|------|
| 入口 | `DriverConductor` | `getOrCreateSendChannelEndpoint()` | 查找或创建 endpoint |
| 创建endpoint | `SendChannelEndpointSupplier` | `newInstance()` | 构造 SendChannelEndpoint |
| 打开Socket | `SendChannelEndpoint` | `openChannel()` | 委托给父类 |
| UDP绑定 | `UdpChannelTransport` | `openDatagramChannel()` | DatagramChannel.open + bind |
| 注册Selector | `ControlTransportPoller` | `registerForRead()` | 注册到 NIO Selector |
| Sender线程 | `Sender` | `onRegisterSendChannelEndpoint()` | 触发 registerForRead |

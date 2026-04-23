# Archive startRecording 源码链路与归档数据结构

本文聚焦 `ArchiveProxy.startRecording(...)` 触发的端到端录制/归档链路，并整理 Archive 在**内存**与**磁盘**上的关键数据结构。

> 范围：Aeron Archive（`aeron-archive` 模块）  
> 重点代码：`ArchiveProxy` / `ControlSessionAdapter` / `ControlSession` / `ArchiveConductor` / `RecordingSession` / `RecordingWriter` / `Catalog`

---

## 1. 端到端链路总览（从 client 到落盘）

### 1.1 “控制面”与“数据面”的分工

- **控制面（control plane）**：client 发请求，archive 解码请求，创建/管理录制会话与资源，返回响应与信号。  
  - 典型对象：`ArchiveProxy`（编码/投递）、`ControlSessionAdapter`（解码分发）、`ControlSession`（鉴权/状态机/回包）、`ArchiveConductor`（控制面主循环、创建 session）。

- **数据面（data plane）**：真正消费 `Image` 中的流数据并写入归档文件。  
  - 典型对象：`RecordingSession`（驱动 blockPoll）、`RecordingWriter`（BlockHandler，负责写段文件、rollover、checksum/force）。

### 1.2 流程图（startRecording → 录制 → 归档落盘）

以下为 `text` 代码块（Unicode 框线），任意 Markdown 预览器均可读。

```text
Client: ArchiveProxy.startRecording()
    │ SBE StartRecordingRequest(2) + publication.offer
    ▼
Archive: control Subscription 收到消息
    ▼
ControlSessionAdapter（templateId 分支 → 解码 StartRecording）
    ▼
ControlSession.onStartRecording()（attemptToActivate，仅 ACTIVE 可继续）
    ▼
ArchiveConductor.startRecording()
    ├─ maxConcurrentRecordings 超限? ──是──► sendErrorResponse(MAX_RECORDINGS)
    ├─ 低磁盘? ──是──► sendErrorResponse(LOW_STORAGE...)
    ├─ Parse ChannelUri + makeKey
    ├─ recordingSubscriptionByKeyMap 已有 key? ──是──► sendErrorResponse(ACTIVE_SUBSCRIPTION)
    ├─ strippedChannel +（LOCAL UDP 则 spy 前缀）
    ├─ addSubscription(channel, streamId, AvailableImageHandler)
    │       ├─ sendOkResponse(correlationId, subscription.registrationId)  ← 客户端先收到 OK
    │       └─（并行）Image 可用时 AvailableImageHandler 触发
    │               ▼
    │           taskQueue → startRecordingSession(...)
    │               ▼
    │           catalog.addNewRecording → recordingId
    │               ▼
    │           RecordingPos.allocate（position = joinPosition）
    │               ▼
    │           new RecordingSession（含 RecordingWriter）
    │               ├─ controlSession.sendSignal(START, recordingId, ...)
    │               └─ recorder.addSession(session)
    │                       ▼
    │               RecordingSession.doWork() INIT→RECORDING
    │                       ▼
    │               image.blockPoll(recordingWriter)
    │                       ▼
    │               RecordingWriter.onBlock()（FileChannel.write、可选 force）
    │                       ├─ segment rollover → 新 *.rec
    │                       └─ position counter 更新（recordingWriter.position()）
```

---

## 2. `ArchiveProxy.startRecording`：它到底“做”了什么？

`ArchiveProxy.startRecording` 是**控制协议客户端**的一部分：把调用参数写入 SBE 编码器（`StartRecordingRequest(2)`），再通过控制面 `Publication` 投递给 archive。

要点：

- `controlSessionId`：标识本次连接/会话（来自 connect/openSession 流程）。
- `correlationId`：请求-响应/信号关联的 id（client 通常用它匹配回包）。
- `sourceLocation`：LOCAL/REMOTE 的语义会影响 archive 侧订阅 channel（例如 UDP LOCAL 会走 `spy:` 旁路订阅）。
- `autoStop`（v2 才有）：当录制完成/流结束时是否自动 stop（具体 stop 触发点在会话/停止链路中）。

> 重要：`ArchiveProxy.startRecording(...)` 返回的 `boolean` **只表示 offer 是否成功**（是否被 back pressure 拒绝）。  
> 录制是否真正开始，要看后续 archive 侧的 `OK response` / `RecordingSignal.START` 等响应与信号。

---

## 3. Archive 端：从“收到请求”到“创建 RecordingSession”

### 3.1 解码与分发：`ControlSessionAdapter`

- 通过 `templateId` 区分请求类型（v1/v2）。
- 解码出 `controlSessionId/correlationId/streamId/channel/sourceLocation/autoStop`。
- 找到 `ControlSession`，然后调用 `controlSession.onStartRecording(...)`。

### 3.2 会话门禁：`ControlSession`

`ControlSession` 会先 `attemptToActivate()`，只有在 `State.ACTIVE` 才会转发给 `ArchiveConductor`。  
这样保证：**未鉴权/未激活的控制连接无法发起录制**。

### 3.3 录制订阅：`ArchiveConductor.startRecording(...)`

`ArchiveConductor.startRecording` 的核心是“把数据面接入”：

1. **资源/安全检查**
   - `maxConcurrentRecordings` 上限。
   - 低磁盘空间检查（防止写盘中途失败导致大量录制异常）。

2. **构造订阅 key（去重）**
   - `key = makeKey(streamId, channelUri)`。
   - `recordingSubscriptionByKeyMap` 用于防止同一 stream/channel 组合重复录制。

3. **组装真正用于订阅的 channel**
   - `strippedChannel`：从 `ChannelUri` 中剥离可变/不参与匹配的参数，形成稳定的录制描述与 key。
   - 对 UDP 且 `sourceLocation == LOCAL`：使用 `spy:` 前缀在本机旁路订阅，录制本机 publication 的流。

4. **关键：AvailableImageHandler + taskQueue**
   - `aeron.addSubscription(...)` 时设置 `AvailableImageHandler`。
   - 当 `Image` 变为可用时：把 `startRecordingSession(...)` 推入 `taskQueue`，由 conductor 线程后续执行。

5. **立即响应**
   - archive 会先 `sendOkResponse` 返回 `subscription.registrationId()`（注意：此时还没 `recordingId`）。
   - `recordingId` 要等 `Image` 到来后 `startRecordingSession` 才能分配。

### 3.4 真正创建会话：`startRecordingSession(...)`

`startRecordingSession` 发生在 **Image available** 之后：

- 从 `Image` 读取元信息（`joinPosition/termBufferLength/mtu/initialTermId/sessionId/sourceIdentity`）。
- **Catalog**：`catalog.addNewRecording(...)` 写入 recording descriptor，生成 `recordingId`。
- **Position Counter**：`RecordingPos.allocate(...)` 创建 counter，初值 `startPosition = image.joinPosition()`。
- 创建 `RecordingSession`（内部创建 `RecordingWriter`）。
- 发送 `RecordingSignal.START`（包含 recordingId、subscriptionId、startPosition 等）。
- 把 `RecordingSession` 加入 `recorder`（工作器）并进入调度循环。

---

## 4. 数据面：RecordingSession / RecordingWriter 如何把数据写入归档

### 4.1 `RecordingSession`：状态机 + blockPoll 驱动

`RecordingSession` 作为 `Session`：

- 状态机：`INIT → RECORDING → INACTIVE → STOPPED`
- `doWork()`：
  - `INIT`：调用 `recordingWriter.init()`（打开/定位段文件），发 started event（若启用）。
  - `RECORDING`：调用 `image.blockPoll(recordingWriter, blockLengthLimit)`。
  - 录到数据则更新 position counter：`position.setRelease(recordingWriter.position())`。
  - 若 `image.isEndOfStream()` 或 `image.isClosed()` 则进入 `INACTIVE`，最终关闭 writer 并发 stopped event。

`blockLengthLimit`：

- `blockLengthLimit = min(image.termBufferLength(), ctx.fileIoMaxLength())`
- 用于限制单次 block IO 的最大长度，避免一次写盘过大影响延迟或触发系统限制。

### 4.2 `RecordingWriter`：BlockHandler，负责落盘与 segment rollover

`RecordingWriter` 实现 `BlockHandler`：

- `onBlock(...)`：对 termBuffer 的一个 block：
  - padding frame 特判（只写 header，不写 padding payload）。
  - 可选 checksum：把数据复制到 `checksumBuffer` 后计算并写入（实现上把 computed checksum 写到 frame 的 sessionId 字段位置）。
  - `FileChannel.write(byteBuffer, fileOffset)` 写入当前 segment 文件的对应偏移。
  - 按 `fileSyncLevel` 配置可 `force()`（`forceWrites/forceMetadata`）。
  - 更新 `segmentOffset += length`，达到 `segmentLength` 触发 `onFileRollOver()`。

- `init()`：
  - 计算 `segmentBasePosition/segmentOffset`：
    - `segmentBasePosition` 来自 `segmentFileBasePosition(...)`
    - `segmentOffset = joinPosition - segmentBasePosition`
  - 打开段文件并把文件长度扩到 `segmentLength`（`RandomAccessFile.setLength(segmentLength)`）。

- `onFileRollOver()`：
  - 关闭旧 `recordingFileChannel`
  - `segmentBasePosition += segmentLength`，新建下一段文件
  - 若新段文件已存在则抛异常（防止覆盖已有归档数据）

---

## 5. 磁盘归档布局（ArchiveDir 下有哪些文件）

### 5.1 Catalog（元数据）

- 文件：`archive.catalog`  
- 作用：保存每个 recording 的 descriptor（元数据），例如：
  - `recordingId`
  - `startPosition/stopPosition`
  - `initialTermId`
  - `segmentFileLength`
  - `termBufferLength`
  - `mtuLength`
  - `sessionId/streamId`
  - `strippedChannel/originalChannel`
  - `sourceIdentity`
  - 以及时间戳等信息

`startRecordingSession` 会调用 `catalog.addNewRecording(...)` 写入一条 descriptor，并返回 `recordingId`。

### 5.2 Segment 文件（数据）

每条 recording 对应一组段文件，通常以如下方式命名（概念上）：

- `<recordingId>-<segmentBasePosition>.rec`

其中：

- `recordingId`：来自 catalog。
- `segmentBasePosition`：段文件覆盖的起始 position（由 `segmentFileBasePosition(...)` 计算）。
- 扩容：每个段文件创建时会预设长度为 `segmentLength`，随后在对应偏移写入 block 数据。

### 5.3 Mark File（进程存活/状态）

- 文件：通常为 `archive-mark.dat`（位置可通过配置调整）
- 作用：向外暴露 Archive 的运行状态/元信息（例如 ready/terminated），并支持外部进程探测。

> 录制数据不写在 mark file 里；它更像“Archive 实例级别”的状态公告板。

### 5.4 图示：目录、命名与数据形态

#### 5.4.1 `archiveDir` 下典型文件布局

```text
┌──────────────────────────────────────────────────────────────────────────┐
│  archiveDir  （例如 Archive.Context.archiveDir）                         │
├──────────────────────────────────────────────────────────────────────────┤
│  archive-mark.dat          Archive 实例存活 / 版本等（不含录制 payload）   │
│  archive.catalog           二进制：CatalogHeader + 多条 RecordingDescriptor │
│  {id}-{position}.rec       段数据文件，例：7-0.rec、7-67108864.rec         │
│  …                                                                         │
└──────────────────────────────────────────────────────────────────────────┘

  archive.catalog ──────────「索引 / 元数据描述」──────────►  各 *.rec 段文件
```

等价目录树（示意）：

```text
archiveDir/
├── archive-mark.dat          # Archive 进程 mark
├── archive.catalog           # 所有 recording 的元数据（SBE descriptor）
├── 7-0.rec                   # recordingId=7 的第 1 个 segment（固定长度 segmentLength）
├── 7-67108864.rec            # 同一条 recording 的第 2 个 segment（示例数值）
└── 8-0.rec                   # 另一条 recording
```

#### 5.4.2 一条 `recordingId` 与多个 `.rec` 的关系

```text
                    archive.catalog 中的一条 RecordingDescriptor（概念）
                    ┌─────────────────────────────────────────┐
                    │ recordingId                             │
                    │ startPosition / stopPosition            │
                    │ segmentFileLength                       │
                    │ termBufferLength / mtuLength            │
                    │ streamId / channel / sessionId / …      │
                    └──────────────────┬──────────────────────┘
                                       │
                         描述「属于哪条流、position 范围、段多大」
                                       │
           ┌───────────────────────────┼───────────────────────────┐
           ▼                           ▼                           ▼
    ┌─────────────┐            ┌─────────────┐            ┌─────────────┐
    │   7-0.rec   │            │   7-L.rec   │            │  7-2L.rec   │
    │ [0 , L)     │            │ [L , 2L)    │            │  …          │
    └─────────────┘            └─────────────┘            └─────────────┘
         磁盘上的 segment 文件序列（L = segmentFileLength，示意）
```

文件名规则（与源码 `Archive.segmentFileName` 一致）：

```text
{recordingId}-{segmentBasePosition}.rec
```

- `segmentBasePosition`：该文件在 **log stream position** 上的起始位置（rollover 时 `segmentBasePosition += segmentLength`）。

#### 5.4.3 单个 `.rec` 文件内部（逻辑结构）

段文件**不是** JSON/表结构，而是 **Aeron term 中连续帧的原始字节拼接**（`RecordingWriter.onBlock` 按块写入）：

```text
单个 .rec 文件（预分配 segmentLength 字节）

|<────────────── segmentLength（固定文件容量） ──────────────>|

+-------------------------------------------------------------+
|  从文件内偏移 segmentOffset 起，顺序追加：                    |
|                                                             |
|  +----------+  +----------+  +----------+       +-----+     |
|  | Frame 0  |  | Frame 1  |  | Frame 2  |  ...  | ... |     |
|  | Header + |  | Header + |  | padding  |       |     |     |
|  | payload  |  | payload  |  | 可能仅头  |       |     |     |
|  +----------+  +----------+  +----------+       +-----+     |
|                                                             |
|  （各帧 FRAME_ALIGNMENT 对齐；内容同 term buffer 布局）       |
+-------------------------------------------------------------+
```

更紧凑的线性示意：

```text
|←——— segmentLength（固定文件容量） ———→|
[ 已写入：若干 Aeron 帧头+载荷连续拼接 … ][ 未写区域为稀疏/零或预分配空洞 ]
         ↑
    segmentOffset 随每次 onBlock 递增（逻辑 position = segmentBasePosition + segmentOffset）
```

帧格式由 **`io.aeron.logbuffer.FrameDescriptor`** / **`DataHeaderFlyweight`** 定义；**`Image.blockPoll` + `TermBlockScanner`** 保证每次写入的块边界落在完整 frame 上。

#### 5.4.4 `archive.catalog` 文件内部（逻辑结构）

与 **`Catalog.java`** 类注释一致：文件头部 + 多条「descriptor header + 变长 RecordingDescriptor」：

```text
archive.catalog（mmap，可按需 growCatalog 扩容）

+---------------+------------------------+------------------------+------------------+
| CatalogHeader | Entry 1                | Entry 2                | ...              |
| （文件头；     | RecordingDescriptor    | RecordingDescriptor    |                  |
|  首区对齐）    | Header + Descriptor    | Header + Descriptor    |                  |
|               | （变长）                | （变长）                |                  |
+---------------+------------------------+------------------------+------------------+
        │                    │                      │
        └────────────────────┴──────────────────────┴──► 字节流顺序增长，非 JSON
```

---

## 6. 归档的“内存数据结构”（Conductor 侧的核心表）

### 6.1 Subscription 去重与引用计数

- `recordingSubscriptionByKeyMap: Map<String, Subscription>`
  - key 通常由 `streamId + strippedChannel`（或 channelUri 相关字段）构成，用于避免重复对同一流录制。
- `subscriptionRefCountMap: Long2LongCounterMap`
  - 以 `subscription.registrationId()` 为键做引用计数：
  - 一个 subscription 可能对应多个 recording/extend 情况，引用计数归零时才可安全关闭。

### 6.2 Session 管理

- `recordingSessionByIdMap: Long2ObjectHashMap<RecordingSession>`
  - key 是 `recordingId`，value 是正在录制的会话。
- `replaySessionByIdMap / replicationSessionByIdMap / deleteSegmentsSessionByIdMap`
  - 对应 replay/replicate/delete 等其它控制能力（与本文 startRecording 主题类似，都是 conductor 统一管理）。

### 6.3 taskQueue（跨回调边界的“串行化”）

- `taskQueue: ArrayDeque<Runnable>`
  - `AvailableImageHandler` 发生在回调上下文中，它不直接做重活，而是把 `startRecordingSession(...)` 以 Runnable 形式丢进队列。
  - 这样可以保证：**所有控制面状态变更**在 conductor 线程中串行执行，减少并发复杂度。

---

## 7. “归档的数据结构”到底长什么样？

把它拆成两层更容易理解：

### 7.1 元数据（Catalog 里的 Recording Descriptor）

可以把 catalog 中每条记录理解为：

```text
RecordingDescriptor
  - recordingId
  - startPosition
  - stopPosition (录制结束后更新)
  - startTimestamp / stopTimestamp
  - initialTermId
  - segmentFileLength
  - termBufferLength
  - mtuLength
  - sessionId
  - streamId
  - strippedChannel
  - originalChannel
  - sourceIdentity
  - ...（版本/扩展字段）
```

它的用途：

- **定位**：通过 recordingId 找到对应段文件集合
- **解释**：知道该 recording 属于哪个 stream/channel，如何还原 position 到 term/offset
- **校验/扩展**：extend recording 时会校验新 image 是否匹配（initialTermId/termBufferLength/mtuLength/joinPosition 等）

### 7.2 数据（Segment Files）

每条 recording 的数据被切成多个 segment 文件：

- segment 文件按 `segmentLength` 固定大小滚动
- 写入数据来自 `Image` 的 termBuffer block（`blockPoll` 驱动）
- `recording position`（counter）持续更新，表示已落盘的最新 position

这层结构支持：

- 顺序回放（replay 从某个 position 开始）
- 断点续录/extend（在 stopPosition 处继续）
- 分段删除（delete segments session 等能力）

---

## 8. 常见疑问（结合源码语义）

### 8.1 为什么 startRecording 先返回 subscriptionId，而不是 recordingId？

因为 `recordingId` 的分配在 `startRecordingSession(...)`，它需要 `Image`（尤其是 `joinPosition/initialTermId/...`）来写入 catalog descriptor。  
而 `Image` 只有在订阅真正“接上流”之后才出现，因此响应被拆成两步：

- **OK response**：立刻返回 subscriptionId（表明已开始尝试订阅该流）
- **RecordingSignal.START**：image 可用后发送（带 recordingId 等完整信息）

### 8.2 真正写盘发生在哪里？

发生在数据面链路：

`RecordingSession.record()` → `image.blockPoll(recordingWriter, blockLengthLimit)` → `RecordingWriter.onBlock()` → `FileChannel.write(...)`

### 8.3 “归档”除了写段文件，还写了什么？

至少有两类输出：

- `archive.catalog`：descriptor 元数据（创建 recording、extend、停止时更新 stopPosition 等）
- 段文件 `*.rec`：实际数据

另外还有实例级别的 `archive-mark.dat` 用于进程状态声明。

---

## 9. 追加：stopRecording / autoStop 的收尾链路（停止、写 Catalog、发 STOP 信号、关闭订阅）

这一节回答两个最常见的追问：

- **调用 stopRecording 后，Archive 内部如何让录制停下来？**
- **autoStop 开启后，Archive 怎么决定要不要把 subscription 也一起关掉？**

### 9.1 stopRecording 有几种入口？

控制协议里常见的三种“停录”入口（语义略有差别）：

- **按 `(channel, streamId)` 停止**：`ArchiveConductor.stopRecording(...)`
  - 先从 `recordingSubscriptionByKeyMap` 移除对应 subscription
  - 再 `abortRecordingSessionAndCloseSubscription(subscription)`
  - 适合“我知道我录的是哪个 channel/stream”

- **按 subscriptionId 停止**：`ArchiveConductor.stopRecordingSubscription(...)`
  - 通过 subscriptionId 找到并移除 subscription
  - 再 abort+close
  - 适合 client 已拿到 startRecording 的 OK response（返回的就是 subscription.registrationId）

- **按 recordingId 停止**：`ArchiveConductor.stopRecordingByIdentity(...)`
  - 直接从 `recordingSessionByIdMap` 找 `RecordingSession` 并 abort
  - 尝试移除 subscription，并在引用计数归零时 close
  - 适合“我只拿到了 recordingId（例如信号里给的）”

> 不管入口是哪一种，本质动作都是：**让对应 RecordingSession 进入 aborted/inactive，然后在 closeRecordingSession(...) 里做收尾。**

### 9.2 “让它停下来”：abort 的实际效果

`RecordingSession.abort(reason)` 只做一件事：置 `isAborted=true` 并记录原因。  
随后 `RecordingSession.doWork()` 会把状态转为 `INACTIVE`，并关闭 writer：

- `INIT/RECORDING` → 发现 `isAborted` → `state(INACTIVE, reason)`
- `INACTIVE` 分支：`state(STOPPED)` + `recordingWriter.close()` +（可选）eventsProxy.stopped(...)

这意味着 stopRecording 并不强制“立刻杀死线程”，而是通过 session 的状态机在下一轮 `doWork()` 中**有序停机**。

### 9.3 收尾的中心点：`ArchiveConductor.closeRecordingSession(...)`

当 recorder 工作者发现 `RecordingSession` done（进入 STOPPED）后，会调用 conductor 的关闭逻辑。  
这里做了三件最关键的事：

1. **写 Catalog 的 stopPosition/stopTimestamp**
   - `catalog.recordingStopped(recordingId, position, epochClock.time())`
2. **发控制信号 `RecordingSignal.STOP`**
   - `controlSession.sendSignal(correlationId, recordingId, subscriptionId, position, RecordingSignal.STOP)`
3. **决定是否关闭 subscription（引用计数 + autoStop）**
   - `if (refCount <= 0 || session.isAutoStop()) closeAndRemoveRecordingSubscription(...)`

也就是说：**STOP 信号与 Catalog 更新是在 session 正式收尾时发出的**，而不是 stopRecording 请求一进来就立刻发。

### 9.4 引用计数 vs autoStop：什么时候 subscription 会被关？

Archive 维护 `subscriptionRefCountMap`（key 是 `subscription.registrationId()`）：

- 每当 image 到来并创建/extend recording session，会对对应 subscriptionId 做 `incrementAndGet(...)`。
- session 收尾时做 `decrementAndGet(...)`。

关闭 subscription 的条件（其中一个满足就可能触发关闭）：

- **引用计数归零**：没有任何 recording session 再依赖这个 subscription
- **autoStop 为 true**：即使引用计数还没归零，也会执行 `closeAndRemoveRecordingSubscription(...)`

`closeAndRemoveRecordingSubscription(...)` 会：

- 从 `subscriptionRefCountMap` 移除该 subscriptionId
- 遍历 `recordingSessionByIdMap`，对仍绑定该 subscription 的 session 执行 `abort(reason)`
- 从 `recordingSubscriptionByKeyMap` 移除 subscription（避免未来再按 key 找到）
- 最后 `CloseHelper.close(..., subscription)` 真正关闭 subscription

因此 autoStop 的直观语义是：

- “录完就把录制订阅也关掉”，把资源释放做得更激进、更自动化（减少用户忘记 stop 的风险）。

### 9.5 STOP 信号是怎么保证送达/可重试的？

`ControlSession.sendSignal(...)` 会优先尝试通过 `controlResponseProxy.sendSignal(...)` 立即发送。  
如果发送失败（例如 response publication 背压）或队列不为空，则把发送动作排入 `syncResponseQueue`，后续在 conductor 线程中重试。

这让 STOP/START/DELETE 等信号在控制面背压时不会直接丢失，而是进入“同步响应队列”延后发送。

---

## 10. 追加：extendRecording（续录）链路与“无缝续接”校验

extendRecording 可以理解为：**对一个已存在（并且已停止）的 recordingId，在 stopPosition 处继续录制**。  
它的难点在于“续接一致性”：新来的 live `Image` 必须与旧 recording 的参数匹配，并且 `image.joinPosition()` 必须等于旧 recording 的 `stopPosition`，否则无法保证数据连续。

### 10.1 控制面入口：ExtendRecordingRequest / ExtendRecordingRequest2

ExtendRecording 有 v1/v2 两个模板，v2 多了 `autoStop`：

- `ControlSessionAdapter` 解码后调用：
  - `controlSession.onExtendRecording(correlationId, recordingId, streamId, sourceLocation, autoStop, channel)`
- `ControlSession` 在 `State.ACTIVE` 后转发：
  - `conductor.extendRecording(...)`

> 注意：extendRecording 需要显式提供 `recordingId`，这是它与 startRecording 的关键差异。

### 10.2 `ArchiveConductor.extendRecording(...)` 做了哪些前置检查？

（按实现的先后顺序整理）

- **并发录制上限**：`recordingSessionByIdMap.size() >= maxConcurrentRecordings` 则拒绝
- **recordingId 存在性**：`catalog.hasRecording(recordingId)` 否则 `UNKNOWN_RECORDING`
- **streamId 必须一致**：入参 `streamId` 必须等于 catalog 中该 recording 的 `streamId`
- **不能续录“正在录制”的 recording**：`recordingSessionByIdMap.containsKey(recordingId)` 则 `ACTIVE_RECORDING`
- **不能与 deleteSegments 冲突**：若该 recording 正在 delete 且删到了 stopPosition（或之后），会拒绝续录
- **低磁盘空间保护**：同 startRecording
- **subscription 去重**：同样用 `makeKey(streamId, channelUri)` 防止同一流重复订阅
- **订阅 + image 到来机制**：依旧是 `AvailableImageHandler` → `taskQueue` → `extendRecordingSession(...)`

行为上它和 startRecording 很像：**先 addSubscription 并返回 subscriptionId**；真正创建 `RecordingSession` 要等 image 到来。

### 10.3 真正续录发生在 `extendRecordingSession(...)`

当 image available 后，`extendRecordingSession(...)` 会做“续接一致性”的关键校验，并完成元数据重置与会话创建：

1. **拒绝续录 active recording**（双保险）
2. **读取 recordingSummary**（包含 start/stopPosition、termBufferLength、mtu、initialTermId、segmentFileLength 等）
3. **校验 image 是否能无缝续接**（核心）：
   - `image.joinPosition() == recordingSummary.stopPosition`
   - `image.initialTermId() == recordingSummary.initialTermId`
   - `image.termBufferLength() == recordingSummary.termBufferLength`
   - `image.mtuLength() == recordingSummary.mtuLength`

其中第一条是“连续性”约束，其余是“编码/布局一致性”约束：这些参数不一致会导致 position→term/offset 解释不一致，或数据对齐/rollover 语义变化，因而禁止续录。

4. **分配新的 RecordingPos counter**（以 joinPosition 作为起点）
5. **创建新的 RecordingSession**（继续写同一 recordingId 的后续段文件）
6. **Catalog.extendRecording(...)**：把 descriptor 中的 stopPosition/stopTimestamp 重置为 NULL，并记录本次续录关联的 controlSessionId/correlationId/sessionId
7. **发 `RecordingSignal.EXTEND`**：告诉 client “续录已建立”

### 10.4 Catalog 在 extendRecording 时到底改了什么？

`Catalog.extendRecording(recordingId, controlSessionId, correlationId, sessionId)` 的核心语义是“重新打开 recording”：

- 把 `stopPosition` 置回 `NULL_POSITION`
- 把 `stopTimestamp` 置回 `NULL_TIMESTAMP`
- 写入本次续录的 `controlSessionId/correlationId/sessionId`（用于追踪/诊断）
- 更新 checksum 并 force 写入 catalog

这一步很关键：它把 recording 从“已停止/有 stopPosition”变回“正在进行/待写 stopPosition”，使得后续 stop 时 `recordingStopped(...)` 能再次写入新的 stopPosition。

### 10.5 续录与 autoStop 的关系

- extendRecording 的 `autoStop` 会一路传入 `RecordingSession`。
- session 收尾时仍复用 `closeRecordingSession(...)`：写 `recordingStopped(...)`、发 STOP 信号，并根据 `refCount <= 0 || isAutoStop` 决定是否关闭 subscription。

因此：**autoStop 不会改变“能否续录”的校验条件**，只影响“续录完成后是否自动释放订阅资源”。

---

## 11. 追加：truncate / purge / detach / deleteDetached / attach / migrate 段文件生命周期

这部分是“归档数据治理”能力，核心关注点不是“继续写入”，而是“如何调整已有 recording 的可见区间和文件集合”。

### 11.1 `truncateRecording(recordingId, position)`：把 stopPosition 截到更早位置

`ArchiveConductor.truncateRecording(...)` 的关键步骤：

1. 校验 recording 存在、truncate 参数合法、当前无并发 delete 操作
2. 先把 catalog 的 `stopPosition` 更新为目标 `position`
3. 根据 `position` 所在段与旧 stopPosition 的关系：
   - 需要时对“当前段”做尾部擦除（`eraseRemainingSegment(...)`）
   - 把 `position` 之后所有 segment 收集到删除列表
4. 启动 `deleteSegments(...)` 异步删除

语义上：truncate 改变的是“可回放终点”，并清理终点之后的物理文件。

### 11.2 `purgeRecording(recordingId)`：逻辑删除 + 清空数据段

`ArchiveConductor.purgeRecording(...)`：

- 先 `catalog.changeState(recordingId, DELETED)`（逻辑状态进入 DELETED）
- 收集该 recording 的所有 segment 文件
- 走 `deleteSegments(...)` 异步删除

语义上：purge 是“删除整个 recording 内容”，并把 descriptor 状态置为 deleted。

### 11.3 `detachSegments(recordingId, newStartPosition)`：只调整逻辑起点

`detachSegments(...)` 不直接删文件：

- 通过校验后，仅更新 `catalog.startPosition(recordingId, newStartPosition)`
- 返回 OK

语义上：把 recording 前部数据“从逻辑视图中脱离”，但物理文件还在磁盘（后续可 deleteDetached）。

### 11.4 `deleteDetachedSegments(recordingId)`：删除已脱离的前段

`deleteDetachedSegments(...)`：

- 扫描现有 segment 文件，找到最小在册段位置
- 计算小于当前 `startPosition` 的 detached 文件集合
- 调用 `deleteSegments(...)` 删除

这通常是 `detachSegments` 的后续清理动作。

### 11.5 `attachSegments(recordingId)`：把前面可用旧段重新接回

`attachSegments(...)` 会向前探测 `<recordingId>-<position>.rec`：

- 要求段文件存在且长度等于 `segmentLength`
- 打开文件校验首个有效 frame 的 termId/streamId
- 根据找到的 termOffset 更新更早的 `catalog.startPosition`

语义上：把此前“脱离”但仍存在的段重新纳入 recording 可见范围。

### 11.6 `migrateSegments(srcRecordingId, dstRecordingId)`：跨 recording 搬段

`migrateSegments(...)` 的约束比较严格：

- src/dst 都必须存在，且不能是 active recording
- stream 参数要匹配
- src 与 dst 必须在 position 上连续（contiguous）
- joinPosition 还要满足段对齐约束

满足后会执行搬段、更新 src/dst 的 start/stopPosition，并可能触发 DELETE 信号（取决于是否还有待删文件）。

---

## 12. 追加：DeleteSegmentsSession 的异步删除机制

`deleteSegments(...)` 实际通过 `DeleteSegmentsSession` 异步执行，每次 `doWork()` 删一个文件，避免长时间阻塞 conductor。

关键行为：

- `files` 队列为空即 session done
- 删除失败会尝试处理 `*.del` 后缀场景，并上报 error response
- `close()` 时从 conductor 移除 session，并发送 `RecordingSignal.DELETE`

这解释了为什么很多删除类 API 是“先 OK，再异步发 DELETE signal”：  
控制面先确认任务已受理，最终完成由 signal 表示。

---

## 13. 追加：查询位点语义（start / recording / stop / max）

`ArchiveConductor` 对同一 recording 提供多个 position 查询，含义不同：

- `getStartPosition(recordingId)`：catalog 中 descriptor 的 `startPosition`
- `getRecordingPosition(recordingId)`：
  - 若 recording 正在进行：返回 live `RecordingSession` 的 counter 值
  - 若已停止：返回 `NULL_POSITION`
- `getStopPosition(recordingId)`：catalog 的 `stopPosition`
- `getMaxRecordedPosition(recordingId)`：
  - 若 active：返回 live counter
  - 否则返回 catalog.stopPosition

实战建议：

- 想看“当前正在录到哪了”用 `getRecordingPosition`
- 想看“最终可回放上界”用 `getStopPosition`
- 想兼容 active/inactive 统一读取可用上界，用 `getMaxRecordedPosition`

---

## 14. 追加：信号与响应的语义速查

### 14.1 常见响应类型

- `sendOkResponse(correlationId[, relevantId])`
  - 请求被受理/执行成功；`relevantId` 视 API 语义可能是 `subscriptionId`、`replicationId` 等
- `sendErrorResponse(...)`
  - 参数校验失败、状态冲突、存储不足、未知 recording 等错误

### 14.2 常见 RecordingSignal（本文涉及）

- `START`：录制会话建立（image ready 后）
- `EXTEND`：续录会话建立（extend 成功）
- `STOP`：录制会话收尾完成（catalog 已写 stop 信息）
- `DELETE`：删除任务完成（delete session close 时发）

### 14.3 信号发送可靠性

`ControlSession.sendSignal(...)` 在 publication 背压时，会把发送动作排入 `syncResponseQueue`，后续重试；因此信号不是“一次 send 失败就丢”。

---

## 15. 追加：线程模型与性能计数器（录制路径）

### 15.1 线程与执行单元

- **ArchiveConductor**：控制面主循环（请求处理、会话管理、任务队列）
- **Recorder**：`SessionWorker<RecordingSession>`，驱动录制会话 `doWork()`
- **RecordingSession**：状态机 + `image.blockPoll(...)`
- **RecordingWriter**：磁盘写入执行者

在 `DEDICATED` 模式下，conductor/recorder/replayer 的并发隔离更明显；`SHARED/INVOKER` 则会共享或外部驱动。

### 15.2 关键 counters（录制侧）

`ArchiveConductor.Recorder` 会聚合并上报：

- `totalWriteBytes`
- `totalWriteTimeNs`
- `maxWriteTimeNs`

`RecordingWriter.onBlock(...)` 每次写盘都会回传：

- `bytesWritten(dataLength)`
- `writeTimeNs(writeTime)`

`Recorder.doWork()` 在有工作时把累计值写入 counters。  
这组指标对定位“磁盘慢”“写抖动大”“吞吐下降”很有用。

---

## 16. 追加：故障与排障路径（按优先级）

### 16.1 start/extend 请求已发但迟迟没有 START/EXTEND

优先检查：

1. control session 是否 ACTIVE（鉴权是否完成）
2. addSubscription 后是否真的出现 image（上游 publication 是否存在、channel/streamId/sessionId 是否匹配）
3. `recordingSubscriptionByKeyMap` 是否因重复 key 被拒绝（ACTIVE_SUBSCRIPTION）
4. 是否触发 low storage 拒绝

### 16.2 录制中断或 STOP 过早

优先检查：

- `RecordingSession.record()` 是否遇到 `image.isEndOfStream()` / `image.isClosed()`
- `RecordingWriter` 是否出现 IO 异常/空间不足（`StorageSpaceException`）
- 是否被 stopRecording/autoStop 触发 abort

### 16.3 extend 失败

优先看四个一致性条件：

- joinPosition vs stopPosition
- initialTermId
- termBufferLength
- mtuLength

只要有一个不一致，就不是“可无缝续接”的同一条 recording。

---

## 17. 结论：如何把 Archive 录制链路记成一个统一模型

可以把 Archive 录制/归档看成三层：

1. **控制层**：请求、鉴权、状态机、响应/信号
2. **会话层**：RecordingSession 生命周期（INIT/RECORDING/INACTIVE/STOPPED）
3. **存储层**：Catalog descriptor + segment files + delete/migrate 等治理操作

理解这三层后，start/extend/stop/truncate/purge/attach/migrate 看起来是不同 API，本质上都在操作同一个 recording 的“元数据状态 + 段文件集合 + 活跃会话”。


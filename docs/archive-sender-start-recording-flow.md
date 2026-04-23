# `ArchiveSender` 中 `startRecording(..., SourceLocation.LOCAL)` 调用链

本文说明示例 [`ArchiveSender.java`](../aeron-samples/src/main/java/io/aeron/samples/ArchiveSender.java) 里这一行：

```java
archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);
```

在 Aeron Archive 中的**请求路径**、**Archive 内部处理**，以及**何时真正开始落盘**。

---

## 总览

| 阶段 | 位置 | 作用 |
|------|------|------|
| 1. 发请求 | 客户端 `AeronArchive` + `ArchiveProxy` | 经控制 Publication 发送 `StartRecordingRequest`（SBE） |
| 2. 收响应 | `AeronArchive#pollForResponse` | 阻塞直到 OK，返回 **录制 Subscription 的 `registrationId`** |
| 3. 处理请求 | `ArchiveConductor` + `ControlSessionAdapter` | 解析消息，创建 **spy Subscription** |
| 4. 开始会话 | `startRecordingSession`（Image 可用后） |  catalog 记一条 recording、分配 `RecordingPos`、启动 `RecordingSession` |

要点：**`startRecording` 返回时，通常只表示「Archive 已挂上 spy 订阅」**；**`recordingId` 与 `RecordingPos` 计数器**要在 **同一 `channel`/`streamId` 上出现 `Publication` 且 Image join 之后**才会在 `startRecordingSession` 里创建。`ArchiveSender` 因此在 `addPublication` 之后用 `RecordingPos.findCounterIdBySession` 等待。

---

## 序列图（逻辑）

以下为 `text` 代码块，任意 Markdown 预览器均可阅读。

```text
ArchiveSender ── startRecording(ch, streamId, LOCAL) ──► AeronArchive
                              │ nextCorrelationId()
                              └──► ArchiveProxy.startRecording(...)
                                        │
                                        ▼
                              controlRequest Publication.offer(StartRecordingRequest)
                                        │
                                        ▼
                              MediaDriver（UDP 控制通道）→ Archive 内 Aeron 控制 Subscription

[循环 ArchiveConductor#doWork]
  ArchiveConductor ── poll() ──► ControlSessionAdapter
  ControlSessionAdapter ── onFragment(StartRecordingRequest) ──► ControlSession.onStartRecording(...)
  ControlSession ──► ArchiveConductor.startRecording(...)
  ArchiveConductor ── addSubscription("aeron-spy:"+stripped, streamId, handler) ──► Aeron
  ArchiveConductor ── sendOkResponse(correlationId, subscriptionId) ──► ControlSession

ControlSession ── ControlResponse OK(relevantId = subscription registrationId) ──► AeronArchive
AeronArchive ── pollForResponse ──► 返回 subscriptionId

[随后 Sender 侧 addPublication(同 channel/streamId)]
Archive 内 Aeron ── Image available ──► taskQueue ──► startRecordingSession
ArchiveConductor: catalog.addNewRecording, RecordingPos, RecordingSession, recorder.addSession
```

---

## 1. 客户端：`AeronArchive#startRecording`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/client/AeronArchive.java`

- 加锁、`ensureConnected()`。
- `lastCorrelationId = aeron.nextCorrelationId()`。
- `archiveProxy.startRecording(channel, streamId, sourceLocation, lastCorrelationId, controlSessionId)`：把请求写入 **control request** Publication。
- `pollForResponse(lastCorrelationId)`：在 **control response** Subscription 上读响应，直到 `correlationId` 匹配且 `ControlResponseCode.OK`，返回 `poller.relevantId()`。

对 `startRecording` 而言，`sendOkResponse` 传入的 `relevantId` 是 **Archive 为录制创建的 `Subscription.registrationId()`**（不是 `recordingId`）。

---

## 2. 编码与发送：`ArchiveProxy#startRecording`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/client/ArchiveProxy.java`

- 使用 `StartRecordingRequestEncoder` 填充：`controlSessionId`、`correlationId`、`streamId`、`sourceLocation`、`channel`。
- `offer(...)` 发到与 `Archive.Context.controlChannel` 对应的控制流（本示例为 `localhost:8010`）。

---

## 3. Archive 拉取控制消息：`ArchiveConductor#doWork`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/ArchiveConductor.java`

在 conductor 的 `doWork()` 中会调用：

```java
workCount += controlSessionAdapter.poll();
```

即周期性从控制 Subscription 拉取 fragment。

---

## 4. 解码与分发：`ControlSessionAdapter`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/ControlSessionAdapter.java`

- `poll()` 对 `controlSubscription` / `localControlSubscription` 做 `poll`。
- `onFragment` 中若模板为 `StartRecordingRequest`，则解码并调用对应 `ControlSession` 的 `onStartRecording(correlationId, streamId, sourceLocation, autoStop=false, channel)`。

---

## 5. 会话入口：`ControlSession#onStartRecording`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/ControlSession.java`

若会话已 `ACTIVE`，则调用：

`conductor.startRecording(correlationId, streamId, sourceLocation, autoStop, channel, this)`。

---

## 6. 核心逻辑：`ArchiveConductor#startRecording`

**文件**：`aeron-archive/src/main/java/io/aeron/archive/ArchiveConductor.java`

主要步骤：

1. **并发与磁盘**：检查 `maxConcurrentRecordings`、`isLowStorageSpace` 等。
2. **去重**：`makeKey(streamId, channelUri)`；若该 key 已有录制订阅，返回 `ACTIVE_SUBSCRIPTION` 错误。
3. **构造订阅 channel**：
   - `strippedChannel`：去掉 session 等后的 URI。
   - 若 `sourceLocation == SourceLocation.LOCAL` 且为 UDP：  
     `channel = SPY_PREFIX + strippedChannel`，其中 `SPY_PREFIX` 为 `aeron-spy:`（`CommonContext`）。
4. **`aeron.addSubscription(channel, streamId, AvailableImageHandler, null)`**  
   - `AvailableImageHandler` 在 **Image 可用** 时将 `startRecordingSession(...)` 投递到 `taskQueue`。
5. **`controlSession.sendOkResponse(correlationId, subscription.registrationId())`**  
   客户端 `pollForResponse` 据此返回。

此时 **尚未** 创建 `RecordingSession` 或 catalog 中的新 recording（除非已有 Image 立即就绪的边界情况）。

---

## 7. Image 出现后：`startRecordingSession`

**文件**：同上 `ArchiveConductor.java`（`private void startRecordingSession(...)`）

当 spy Subscription 与目标 Publication 匹配并出现 `Image` 时：

- 从 `Image` 读取 `sessionId`、`joinPosition`、`termBufferLength` 等。
- `catalog.addNewRecording(...)` 分配 **`recordingId`**。
- `RecordingPos.allocate(...)` 注册 **Archive 侧 counters**，供客户端按 `sessionId` / `archiveId` 查找。
- 构造 `RecordingSession`，`recorder.addSession(session)` 进入实际片段写入路径。
- `controlSession.sendSignal(..., RecordingSignal.START)`。

`ArchiveSender` 在 `addPublication` 之后通过 `RecordingPos.findCounterIdBySession` 等待该计数器，再 `getRecordingId` —— 与上述时机一致。

---

## 8. 与 `ArchiveSender` 其它代码的关系

| 代码 | 说明 |
|------|------|
| `driverCtx.spiesSimulateConnection(true)` | 让 spy 被视作已连接，避免无订阅时 `offer` 长期 `NOT_CONNECTED`。 |
| `aeron.addPublication(CHANNEL, STREAM_ID)` | 创建被 spy 的 Publication；驱动 `startRecordingSession`。 |
| `RecordingPos.findCounterIdBySession(...)` | 等待 `recordingId` / 录制位点计数器就绪。 |
| `archive.stopRecording(CHANNEL, STREAM_ID)` | 走 `StopRecordingRequest` 对称路径，移除录制订阅并结束会话。 |

---

## 9. 源码索引（便于跳转）

| 步骤 | 类#方法 |
|------|---------|
| API 入口 | `AeronArchive#startRecording(String, int, SourceLocation)` |
| 写控制消息 | `ArchiveProxy#startRecording` |
| Conductor 轮询 | `ArchiveConductor#doWork` → `ControlSessionAdapter#poll` |
| 解码 | `ControlSessionAdapter#onFragment`（`StartRecordingRequest` 分支） |
| 转发 | `ControlSession#onStartRecording` |
| 建 spy 订阅 | `ArchiveConductor#startRecording` |
| 建录制会话 | `ArchiveConductor#startRecordingSession` |

---

## 10. 返回值说明

`AeronArchive.startRecording(...)` 的返回值是 **`Subscription#registrationId()`**（Archive 内部为录制创建的订阅 ID），可用于 `stopRecording(long subscriptionId)`。本 Demo 只按 `channel` + `streamId` 调用 `stopRecording`，因此未保存该返回值；需要按 subscription 精确停止时可保留返回值。

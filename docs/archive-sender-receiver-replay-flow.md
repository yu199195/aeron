# ArchiveSender 录制与 ArchiveReceiver 回放流程解析

本文说明 `ArchiveSender` 完成本地归档后，`ArchiveReceiver` 发起 **replay** 时的端到端行为，并对照 Aeron Archive 源码中的关键类。

**图表说明**：下文流程图均为 `text` 代码块（Unicode 框线），**任意 Markdown 预览器**均可直接阅读，无需 Mermaid 插件。

**拓扑速览**

```text
控制面（Archive 协议，UDP）:
  Receiver:AeronArchive  ----UDP:8010---->  Sender:Archive
  Sender:Archive         ----UDP:动态端口---->  Receiver:AeronArchive

数据面（回放日志帧，UDP）:
  Sender:ReplaySession -> ExclusivePublication -> Sender:MediaDriver 发送
  ----UDP 目的 localhost:40457, streamId=11----
  Receiver:MediaDriver 接收 -> Subscription -> Image.poll -> FragmentHandler
```

---

## 1. 角色与网络拓扑

| 组件 | 进程 | 作用 |
|------|------|------|
| `ArchivingMediaDriver` | ArchiveSender | 同一 JVM 内 MediaDriver + Archive；录制写 `archiveDir`，控制面监听 `localhost:8010` |
| `Aeron` + `Publication` | ArchiveSender | 实时业务 UDP：`aeron:udp?endpoint=localhost:40456`，`streamId=10` |
| `MediaDriver` | ArchiveReceiver | 仅媒体层，无 Archive |
| `Subscription`（实时） | ArchiveReceiver | 订阅 `40456` + `streamId=10`，收实时消息 |
| `AeronArchive`（客户端） | ArchiveReceiver | 经 **UDP 控制通道** 连接 Sender 的 Archive，发 list / replay 命令 |
| 回放 UDP | Sender → Receiver | `REPLAY_CHANNEL`：`localhost:40457`，`streamId=11`（与实时流分离） |

录制路径：**Publication term buffer → spy Subscription → RecordingSession → 磁盘 segment**。  
回放路径：**磁盘 → ReplaySession → ExclusivePublication → UDP → Receiver Subscription**。

---

## 2. 双通道：控制 UDP 与 回放数据 UDP（核心）

Replay **不是**「客户端直接从磁盘读」；客户端只通过 **控制 UDP** 告诉 Archive「往哪发」；**历史帧**由 Archive 在 **Sender 进程** 里读盘后，经 **Sender 的 MediaDriver** 用 **另一条 UDP（数据面）** 发到 Receiver。

```text
┌─ Receiver 进程 (localhost) ─────────────────────────────────────────┐
│  AeronArchive 客户端 ──► Receiver MediaDriver ──► Subscription 回放 │
└───────────────────────────────┬───────────────────────────────────┘
                                │
     ① 控制 UDP :8010            │ ③ 数据 UDP :40457, streamId=11
     (connect / list / replay)   │ (回放日志帧)
              │                  │
              ▼                  ▼
┌─ Sender 进程 (localhost) ───────────────────────────────────────────┐
│  Archive 服务 ◄──② 响应 UDP 动态端口── 与客户端控制 Subscription 对接 │
│       │                                                             │
│       └──► ReplaySession 读盘 ◄──► archiveDir 磁盘                  │
│                 │                                                   │
│                 └──► ExclusivePublication 回放 ──► Sender MD ─────┘
└─────────────────────────────────────────────────────────────────────┘
```

要点：

| 步骤 | 协议与方向 | 内容 |
|------|------------|------|
| ①② | Receiver → Sender `:8010`；Archive → Receiver 动态端口 | SBE 控制消息：`connect`、`listRecordings`、`replay` 请求与 `OK`、descriptor 等 |
| ③ | Sender MediaDriver → Receiver MediaDriver `:40457` | Aeron 日志帧（与实时流相同的帧格式），**streamId=11**，URI 带 **session-id** 与 publication 对齐 |

---

## 3. Replay 数据面：Sender 如何经 UDP 发出、Receiver 如何收到

下面只画 **回放数据** 路径（不含 8010 控制）。Archive 在收到 `startReplay` 后，在 **本机 Sender MediaDriver** 上创建 **ExclusivePublication**，`ReplaySession` 循环 `offer`，由 **SendChannelEndpoint** 打成 UDP 发往你在请求里写的 `replayChannel`（Demo 为 `endpoint=localhost:40457`）。

```text
Sender JVM（Archive + MediaDriver）
┌─────────────────────────────────────────┐
│  ReplaySession                          │
│       │ offer 日志帧                     │
│       ▼                                 │
│  ExclusivePublication                   │
│       │                                 │
│       ▼                                 │
│  SendChannelEndpoint（UDP 发送）         │
└───────────────────┬─────────────────────┘
                    │
                    ▼
            ┌───────────────┐
            │ 本机 UDP       │
            │ dest :40457   │
            └───────┬───────┘
                    │
                    ▼
Receiver JVM（仅 MediaDriver）
┌─────────────────────────────────────────┐
│  ReceiveChannelEndpoint（UDP 接收）      │
│       │                                 │
│       ▼                                 │
│  Subscription（带 sessionId）            │
│       │                                 │
│       ▼                                 │
│  Image.poll                             │
│       │                                 │
│       ▼                                 │
│  FragmentHandler（打印等）               │
└─────────────────────────────────────────┘
```

对应代码侧（概念上）：

- **发出端**：`ReplaySession` 读 segment → `replayPublication.offer(buffer, ...)` → Driver 序列化 term 帧 → **UDP 发到 `replayChannel` 的 endpoint**。
- **接收端**：`aeron.addSubscription(replayChannelWithSessionId, REPLAY_STREAM_ID)` → Driver 收 UDP → 组 `Image` → `image.poll(handler)` 收到与实时相同的 fragment 回调。

---

## 4. 客户端如何通过 UDP「发起」回放（控制面时序）

`AeronArchive` 使用 **与实时 Aeron 相同的 UDP 传输**：`controlRequestPublication` 发往 Archive 的 `controlChannel`（8010），`controlResponseSubscription` 绑定本机 `localhost:0` 收响应。

```text
Receiver_AeronArchive          UDP→8010          Sender_Archive          UDP 动态端口
       │                            │                  │                      │
       │── 写入 Connect 请求 ──────►│── datagram ─────►│                      │
       │                            │                  │── ControlResponse OK ►│
       │◄── poll 解析 sessionId ────│◄─────────────────│◄─────────────────────│
       │                            │                  │                      │
       │── ListRecordingsForUri ───►│── UDP ──────────►│                      │
       │◄── RecordingDescriptor ────│◄─────────────────│◄─────────────────────│
       │    （得到 recordingId）     │                  │                      │
       │                            │                  │                      │
       │── Replay(40457, stream11) ─►│── UDP ──────────►│                      │
       │                            │                  │ 内部：ReplaySession + │
       │                            │                  │ ExclusivePublication  │
       │◄── OK + replaySessionId ───│◄─────────────────│◄─────────────────────│
       │    startReplay 返回         │                  │                      │
```

随后客户端在本机 **Receiver MediaDriver** 上 `addSubscription`（channel 拼上返回的 sessionId），等待 **数据面 UDP** 从 Sender 到来（见第 3 节）。

---

## 5. 录制阶段（简要）

```text
User          Publication(stream10)    Sender_MediaDriver      Archive           Disk
  │                  │                      │                  │                │
  │── offer msg ────►│── term buffer ─────►│                  │                │
  │                  │                      │── LOCAL spy ──►│                │
  │                  │                      │   读 buffer     │── RecordingWriter ►│
```

- `SourceLocation.LOCAL`：**不经过网卡复制**，spy 直接读共享 buffer。
- `stopRecording` 后 catalog 有 **stopPosition**，供 replay 定界。

---

## 6. 端到端：控制 + 回放数据（一条时序图）

```text
[控制面 UDP :8010]
Receiver_App ── connect / list / replay ──► Sender_Archive
Receiver_App ◄── recordingId, sessionId ── Sender_Archive

[Archive 在 Sender 内建回放 publication]
Sender_Archive ── 读 segment ──► archiveDir
Sender_Archive ── ExclusivePublication.offer ──► Sender_MediaDriver

[数据面 UDP :40457, streamId=11]
Sender_MediaDriver ── UDP frames ──► Receiver_MediaDriver
Receiver_MediaDriver ── Subscription poll ──► Receiver_App

[结束] Image isClosed 时回放结束
```

---

## 7. 生命周期总览

```text
Sender 进程                              Receiver 进程
─────────────────────────────────        ─────────────────────────────────
startRecording(LOCAL)                    Subscription :40456（实时）
    ↓                                        ↓
Publication 发 :40456  ──实时 UDP 40456──►  收实时流
    ↓                                        ↓
RecordingPos → recordingId                 是否发起 replay？
    ↓                                        ├─ 否 → 继续 poll 实时
stopRecording                              └─ 是 → AeronArchive 连 :8010
    ↓                                            ↓
Archive 监听 :8010  ◄──控制 8010 + 回放 40457──  list + startReplay
                                                 ↓
                                             Sub :40457（回放）
                                                 ↓
                                             poll 至 Image 关闭 → 回到实时 poll
```

---

## 8. 常见注意点

1. **必须先起 Sender** 且完成录制并 `stopRecording`，catalog 才有完整条目。
2. **实时 40456** 与 **回放 40457** 分离，避免 channel 冲突。
3. **sessionId**：unicast 下订阅 URI 必须带 `startReplay` 返回的 session（`ChannelUri.addSessionId`）。
4. Demo 中 `doReplay` **阻塞主线程**；若要并行实时接收，需单独线程。

---

## 9. 相关源码

- `aeron-samples/.../ArchiveSender.java`
- `aeron-samples/.../ArchiveReceiver.java`
- `aeron-archive/.../client/AeronArchive.java`（`startReplay`、`listRecordingsForUri`）
- `aeron-archive/.../ArchiveConductor.java`（`startReplay`、`newReplaySession`）
- `aeron-archive/.../ReplaySession.java`

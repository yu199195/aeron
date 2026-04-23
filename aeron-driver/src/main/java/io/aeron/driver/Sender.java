/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.SendChannelEndpoint;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;

import static io.aeron.driver.status.SystemCounterDescriptor.*;

/**
 * 【缓存行填充 - 左侧】防止 Sender 的热点字段与其他对象共享同一 cache line，避免 false sharing。
 * 这是高性能并发编程的经典模式：在热点字段前后各放 64 字节 padding。
 */
class SenderLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

/**
 * 【热点字段】Sender 在每次 doWork() 中频繁读写的字段，被 LhsPadding 和 RhsPadding 前后包裹。
 */
class SenderHotFields extends SenderLhsPadding
{
    long controlPollDeadlineNs;     // 下一次轮询控制消息（SM/NAK）的截止时间
    long reResolutionDeadlineNs;    // 下一次 DNS 重新解析的截止时间
    int dutyCycleCounter;           // 工作周期计数器，控制发送与控制轮询的比例
    int roundRobinIndex = 0;        // 轮询 Publication 数组的起始索引，保证公平性
}

/**
 * 【缓存行填充 - 右侧】与 LhsPadding 配合，确保热点字段独占 cache line。
 */
class SenderRhsPadding extends SenderHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * Agent that iterates over {@link NetworkPublication}s for sending them to {@link Receiver}s on behalf of registered
 * subscribers.
 *
 * 【源码解析】Sender —— Media Driver 的"发送引擎"，三大核心 Agent 之一。
 *
 * 职责：
 * 1. 遍历所有 NetworkPublication，从 LogBuffer 中读取数据帧并通过 UDP Socket 发送到网络
 * 2. 轮询控制通道（ControlTransportPoller）接收来自 Receiver 的 Status Message（SM）和 NAK
 *    - SM 包含接收端的 position，用于更新 publisherLimit（流控）
 *    - NAK 触发数据重传
 * 3. 管理 SendChannelEndpoint 的注册/注销
 * 4. 定期检查 DNS 重新解析
 *
 * 关键设计：
 * - 发送和控制轮询之间有 dutyCycleRatio 比例控制，避免控制消息处理不及时
 * - 使用 roundRobinIndex 轮询 Publication 数组，保证多个 Publication 间的公平性
 * - 通过 cache line padding 避免 false sharing，确保极致性能
 *
 * 线程模型：在 DEDICATED 模式下独占一个线程；Conductor 通过 SenderProxy 命令队列向 Sender 发送指令。
 * <p>
 * 【数据从哪来】每个 {@link NetworkPublication} 对应一块与客户端 {@code Publication}（含 Archive 回放的
 * {@code ExclusivePublication}）共享的 {@code RawLog} term；应用或 Archive {@code offer} 的帧即落在此 buffer，
 * 由本 Agent 的 {@link #doSend(long)} 轮询 {@link NetworkPublication#send(long)} 读出并走 UDP 发送路径。
 */
public final class Sender extends SenderRhsPadding implements Agent
{
    private NetworkPublication[] networkPublications = new NetworkPublication[0]; // 当前所有网络 Publication（无锁数组结构）

    private final long statusMessageReadTimeoutNs;   // 控制消息轮询超时的一半，用于设置 controlPollDeadlineNs
    private final long reResolutionCheckIntervalNs;  // DNS 重新解析检查间隔
    private final int dutyCycleRatio;                // 发送 vs 控制轮询的比例（每发送 N 次才轮询一次控制通道）
    private final ControlTransportPoller controlTransportPoller; // 控制通道轮询器：接收 SM / NAK / RTT 等控制帧
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue; // Conductor → Sender 的命令队列
    private final AtomicCounter totalBytesSent;      // 累计发送字节数计数器
    private final AtomicCounter resolutionChanges;   // DNS 解析变化次数
    private final AtomicCounter shortSends;          // 短发送计数（sendmsg 返回的字节数少于预期）
    private final NanoClock nanoClock;
    private final CachedNanoClock cachedNanoClock;   // 缓存时钟，减少系统调用
    private final DriverConductorProxy conductorProxy; // Sender → Conductor 通知代理
    private final DutyCycleTracker dutyCycleTracker;   // 工作周期耗时追踪

    /**
     * 【构造函数】从 MediaDriver.Context 提取依赖。
     * ControlTransportPoller 负责接收控制帧（SM/NAK），commandQueue 接收 Conductor 的指令。
     */
    Sender(final MediaDriver.Context ctx)
    {
        controlTransportPoller = ctx.controlTransportPoller();  // 控制通道轮询器
        commandQueue = ctx.senderCommandQueue();                // Conductor → Sender 命令队列
        totalBytesSent = ctx.systemCounters().get(BYTES_SENT);
        resolutionChanges = ctx.systemCounters().get(RESOLUTION_CHANGES);
        shortSends = ctx.systemCounters().get(SHORT_SENDS);
        nanoClock = ctx.nanoClock();
        cachedNanoClock = ctx.senderCachedNanoClock();
        statusMessageReadTimeoutNs = ctx.statusMessageTimeoutNs() >> 1;  // SM 超时的一半作为轮询间隔
        reResolutionCheckIntervalNs = ctx.reResolutionCheckIntervalNs();
        dutyCycleRatio = ctx.sendToStatusMessagePollRatio();     // 发送与控制轮询的比例
        conductorProxy = ctx.driverConductorProxy();
        dutyCycleTracker = ctx.senderDutyCycleTracker();
    }

    /**
     * {@inheritDoc}
     */
    public void onStart()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.update(nowNs);
        reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
    }

    /**
     * {@inheritDoc}
     */
    public void onClose()
    {
        controlTransportPoller.close();
    }

    /**
     * {@inheritDoc}
     */
    /**
     * 【核心工作循环】由 AgentRunner 反复调用，每次执行以下步骤：
     *
     * 1. 排空 Conductor 发来的命令（注册/注销 Publication、ChannelEndpoint 等）
     * 2. 执行 doSend() —— 遍历所有 NetworkPublication，从 LogBuffer 读取数据并通过 UDP 发送
     * 3. 条件性地轮询控制通道（接收 SM / NAK / RTT 等控制帧），触发条件：
     *    - 本轮没有发送任何数据（空闲时多处理控制消息）
     *    - dutyCycleCounter 达到比例阈值
     *    - 距上次轮询已超过 statusMessageReadTimeoutNs
     *    - 检测到 short send（需要尽快获取新的 SM 更新流控窗口）
     * 4. 定期 DNS 重新解析检查
     *
     * @return workCount > 0 表示有工作完成
     */
    public int doWork()
    {
        final long nowNs = nanoClock.nanoTime();
        cachedNanoClock.update(nowNs);
        dutyCycleTracker.measureAndUpdate(nowNs);

        // 步骤 1：排空 Conductor 发来的命令
        final int workCount = commandQueue.drain(CommandProxy.RUN_TASK, Configuration.COMMAND_DRAIN_LIMIT);

        // 步骤 2：实际发送数据
        final long shortSendsBefore = shortSends.get();
        final int bytesSent = doSend(nowNs);
        int bytesReceived = 0;

        // 步骤 3：条件性轮询控制通道（SM / NAK 等）
        if (0 == bytesSent ||                              // 没有数据要发 → 空闲时多处理控制消息
            ++dutyCycleCounter >= dutyCycleRatio ||         // 达到发送/控制轮询比例
            (controlPollDeadlineNs - nowNs < 0) ||         // 距上次轮询超时
            shortSendsBefore < shortSends.get())           // 检测到 short send
        {
            bytesReceived = controlTransportPoller.pollTransports(); // 从控制通道接收 SM/NAK

            dutyCycleCounter = 0;
            controlPollDeadlineNs = nowNs + statusMessageReadTimeoutNs;
        }

        // 步骤 4：定期 DNS 重新解析
        if (reResolutionCheckIntervalNs > 0 && (reResolutionDeadlineNs - nowNs) < 0)
        {
            reResolutionDeadlineNs = nowNs + reResolutionCheckIntervalNs;
            controlTransportPoller.checkForReResolutions(nowNs, conductorProxy);
        }

        return workCount + bytesSent + bytesReceived;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "sender";
    }

    /**
     * 【注册发送端 ChannelEndpoint】将该 endpoint 的控制通道 UDP Socket 注册到 ControlTransportPoller，
     * 使后续 pollTransports() 能接收来自 Receiver 的 SM / NAK 等控制帧。
     */
    void onRegisterSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        controlTransportPoller.registerForRead(channelEndpoint);
    }

    /**
     * 【关闭发送端 ChannelEndpoint】取消控制通道的读注册，并通知 Conductor。
     */
    void onCloseSendChannelEndpoint(final SendChannelEndpoint channelEndpoint)
    {
        controlTransportPoller.cancelRead(channelEndpoint);
        conductorProxy.sendChannelEndpointClosed(channelEndpoint);
    }

    /**
     * 【新增 NetworkPublication】将新的 Publication 加入 Sender 的轮询数组，
     * 并在 ChannelEndpoint 上注册发送（关联 LogBuffer 与 UDP Socket）。
     */
    void onNewNetworkPublication(final NetworkPublication publication)
    {
        networkPublications = ArrayUtil.add(networkPublications, publication);
        publication.channelEndpoint().registerForSend(publication);
    }

    /**
     * 【移除 NetworkPublication】从轮询数组中移除，取消发送注册，释放 Sender 端资源。
     */
    void onRemoveNetworkPublication(final NetworkPublication publication)
    {
        networkPublications = ArrayUtil.remove(networkPublications, publication);
        publication.channelEndpoint().unregisterForSend(publication);
        publication.senderRelease();
    }

    void onAddDestination(
        final SendChannelEndpoint channelEndpoint,
        final ChannelUri channelUri,
        final InetSocketAddress address,
        final long registrationId)
    {
        channelEndpoint.addDestination(channelUri, address, registrationId);
    }

    void onRemoveDestination(
        final SendChannelEndpoint channelEndpoint, final ChannelUri channelUri, final InetSocketAddress address)
    {
        channelEndpoint.removeDestination(channelUri, address);
    }

    void onRemoveDestination(final SendChannelEndpoint channelEndpoint, final long destinationRegistrationId)
    {
        channelEndpoint.removeDestination(destinationRegistrationId);
    }

    void onResolutionChange(
        final SendChannelEndpoint channelEndpoint, final String endpoint, final InetSocketAddress newAddress)
    {
        channelEndpoint.resolutionChange(endpoint, newAddress);
        resolutionChanges.getAndAddRelease(1);
    }

    /**
     * 【实际发送逻辑】遍历所有 NetworkPublication，从 LogBuffer 中读取待发送的帧并通过 UDP 发出。
     *
     * 关键设计 —— Round-Robin 公平轮询：
     * - roundRobinIndex 每次递增，作为遍历的起始索引
     * - 先从 startingIndex 遍历到末尾，再从 0 遍历到 startingIndex
     * - 这样保证每个 Publication 都能公平地得到发送机会，不会因为数组前面的 Publication 数据量大而饿死后面的
     *
     * publication.send(nowNs) 内部逻辑：
     * 1. 检查 senderPosition vs senderLimit（由 Receiver 的 SM 更新），确定可以发多少
     * 2. 从 LogBuffer 的 term 中读取数据帧
     * 3. 通过 ChannelEndpoint 的 DatagramChannel 发送到网络
     * 4. 更新 senderPosition
     */
    private int doSend(final long nowNs)
    {
        int bytesSent = 0;
        final NetworkPublication[] publications = this.networkPublications;
        final int length = publications.length;

        // round-robin: 每次从不同的位置开始遍历
        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        // 从 startingIndex 到末尾
        for (int i = startingIndex; i < length; i++)
        {
            bytesSent += publications[i].send(nowNs);
        }

        // 从 0 到 startingIndex（回绕）
        for (int i = 0; i < startingIndex; i++)
        {
            bytesSent += publications[i].send(nowNs);
        }

        totalBytesSent.getAndAddRelease(bytesSent);

        return bytesSent;
    }
}

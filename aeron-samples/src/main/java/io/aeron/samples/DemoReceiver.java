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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 后台常驻的接收端 Demo：订阅通道并持续打印收到的消息，直到手动终止（如 Ctrl+C）。
 * 使用 UTF-8 解码，正确显示中文等字符。使用嵌入式 Media Driver，无需单独启动驱动。
 * <p>
 * 使用方式：先启动本程序，再启动 {@link DemoSender}。按 Ctrl+C 关闭。
 * <p>
 * 这份 demo 体现了 Aeron "应用线程拉取（poll）"的接收模型：
 * <ul>
 *     <li>网络收包、重组、写入 log buffer：由 Media Driver 的 receiver 线程完成。</li>
 *     <li>应用读取：由你的线程调用 {@link Subscription#poll(FragmentHandler, int)} 主动拉取。</li>
 * </ul>
 * 这种设计把线程调度与 back pressure 的控制权更多交给应用，从而获得更可预测的延迟与吞吐。
 */
public class DemoReceiver
{
    private static final String CHANNEL = "aeron:udp?endpoint=172.23.39.208:9000";
    private static final int STREAM_ID = 10;
    private static final int FRAGMENT_LIMIT = 10;

    public static void main(final String[] args)
    {
        System.out.println("========================================");
        System.out.println("  DemoReceiver 接收端");
        System.out.println("  通道: " + CHANNEL + "  streamId=" + STREAM_ID);
        System.out.println("========================================");
        System.out.println("  等待消息中... 按 Ctrl+C 退出。");
        System.out.println("========================================");
        System.out.println();

        final AtomicBoolean running = new AtomicBoolean(true);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().terminationHook(barrier::signalAll)))
        {
            /*
             * 与发送端一致：通过 aeronDirectoryName 把 client attach 到同一个 driver 实例。
             * 这背后是 Aeron 的 IPC 机制：CnC 文件 + 若干 ring buffer/计数器的共享内存映射。
             */
            final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName());
            try (Aeron aeron = Aeron.connect(ctx);
                Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
            {
                /*
                 * addSubscription 在控制面创建订阅关系。
                 *
                 * 一个 Subscription 可以对应多个 Image（例如多路 publisher，或 MDC 多目的地）。
                 * poll 时 Aeron 会在内部轮询这些 Image，并以 fragment 回调形式交付数据。
                 */
                final FragmentHandler handler = (buffer, offset, length, header) ->
                {
                    /*
                     * fragment：Aeron 在 log buffer 里存的是"帧（frame）"，大消息会被拆分为多个 fragment。
                     * 这个 demo 每次回调处理一个 fragment，并把它完整拷贝为 byte[] 再解码打印。
                     *
                     * 想要"按原始消息重组"（例如超过 MTU 的大消息），通常会用 FragmentAssembler
                     * 把同一条 message 的多个 fragment 组装成一次回调，再由应用处理。
                     */
                    final byte[] bytes = new byte[length];              // 分配 byte[] 接收 payload（demo 简单实现，生产环境应复用 buffer）
                    buffer.getBytes(offset, bytes);                     // 从 termBuffer 中拷贝 payload 到 byte[]
                    final String message = new String(bytes, StandardCharsets.UTF_8);    // UTF-8 解码为字符串
                    System.out.println("  [收到] " + message);          // 打印收到的消息
                };

                /*
                 * Aeron 的接收通常写成 tight loop + idle strategy：
                 * - poll 返回读取到的 fragment 数
                 * - idle strategy 根据"是否有工作"选择 spin/yield/sleep/backoff
                 *
                 * 这里用 SleepingIdleStrategy(1ms) 是最省 CPU 的演示配置，但会引入额外延迟。
                 * 低延迟场景更常见 BusySpinIdleStrategy / BackoffIdleStrategy 等。
                 */
                final SleepingIdleStrategy idle = new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(1));

                while (running.get())                                   // 主循环：持续运行直到收到关闭信号
                {
                    /*
                     * subscription.poll(handler, FRAGMENT_LIMIT) 完整调用链：
                     *
                     * 1. Subscription.poll() — 入口
                     *    - 获取 Image[] 快照（每个 Image 对应一个 Publisher session）
                     *    - round-robin 轮询每个 Image，将剩余配额分配给各 Image
                     *
                     * 2. Image.poll() — 核心读取
                     *    - 读取 subscriberPosition → 计算 term 内偏移 → 选择活跃 termBuffer
                     *    - 循环扫描帧：frameLengthVolatile 读帧头长度
                     *      > 0 → 帧已完成可读 | <= 0 → 到达写入前沿，退出
                     *    - 跳过 padding 帧，对 data 帧回调 handler.onFragment()
                     *    - finally：推进 subscriberPosition（release 语义）→ Driver 据此更新流控窗口
                     *
                     * 3. FragmentHandler.onFragment() — 业务回调
                     *    - buffer: termBuffer 视图
                     *    - offset: 跳过 32 字节帧头后的 payload 起始
                     *    - length: payload 长度（= frameLength - HEADER_LENGTH）
                     *    - header: 帧元数据（sessionId, streamId, termId, position 等）
                     *
                     * 流控影响：处理得慢 → position 推进慢 → 发送端 BACK_PRESSURED
                     */
                    final int fragmentsRead = subscription.poll(handler, FRAGMENT_LIMIT);   // 最多读取 10 个 fragment
                    idle.idle(fragmentsRead);                           // fragmentsRead > 0 不休眠，= 0 时 sleep 1ms 节省 CPU
                }
            }
            System.out.println();
            System.out.println("DemoReceiver 已退出。");
        }
    }
}

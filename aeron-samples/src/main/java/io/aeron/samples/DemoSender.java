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
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 后台常驻的发送端 Demo：从控制台读取输入，输入一行发送一条消息，直到手动终止（如 Ctrl+C 或 Ctrl+D）。
 * 使用 UTF-8 编码，支持中文等字符。使用嵌入式 Media Driver，无需单独启动驱动。
 * <p>
 * 使用方式：先启动 {@link DemoReceiver}，再启动本程序。输入内容后按回车发送，按 Ctrl+C 或 Ctrl+D 退出。
 * <p>
 * 读这份 demo 时可以带着两条主线：
 * <ul>
 *     <li>控制面（Control Plane）：Aeron Client 通过命令/控制 ring buffer 与 Media Driver 协作，
 *     完成 Publication/Subscription 的创建、连接、心跳、Flow Control 等。</li>
 *     <li>数据面（Data Plane）：应用线程把消息写入 Publication 的 log buffer（term buffers），
 *     Driver 的 Sender 把它们切成帧发送到网络（UDP）；接收端 Driver 收包写入 log buffer，
 *     应用线程通过 Subscription.poll 以“fragment”为单位读取。</li>
 * </ul>
 */
public class DemoSender
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:40123";
    private static final int STREAM_ID = 10;
    private static final int MAX_MESSAGE_BYTES = 1024;

    public static void main(final String[] args) throws InterruptedException
    {
        System.out.println("========================================");
        System.out.println("  DemoSender 发送端");
        System.out.println("  通道: " + CHANNEL + "  streamId=" + STREAM_ID);
        System.out.println("========================================");
        System.out.println("  输入内容后按回车发送，支持中文。");
        System.out.println("  退出: Ctrl+D (Mac/Linux) 或 Ctrl+Z 回车 (Windows)");
        System.out.println("========================================");
        System.out.println();

        final AtomicBoolean running = new AtomicBoolean(true);
        /*
         * Aeron 的消息内容是“应用自管理”的字节序列。
         * 这里用 Agrona 的 UnsafeBuffer + 直接内存（Direct ByteBuffer）来减少一次 copy，并且对齐到 cache line。
         * 注意：这只是 demo 的做法；高性能场景通常会复用 buffer、避免每条消息都 new byte[]。
         */
        final UnsafeBuffer buffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(MAX_MESSAGE_BYTES, BitUtil.CACHE_LINE_LENGTH));

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier(() -> running.set(false));
            MediaDriver driver = MediaDriver.launchEmbedded(new MediaDriver.Context().terminationHook(barrier::signalAll)))
        {
            /*
             * Aeron 的“驱动”和“客户端”默认通过一个共享目录（aeron directory）发现彼此并共享内存映射文件。
             * - MediaDriver 启动时创建/管理该目录下的一组 CnC 文件、log buffer 文件等
             * - Aeron.connect(ctx) 会 attach 到同一个目录，从而完成控制面交互
             *
             * 这里我们启动的是嵌入式 driver，因此需要显式把 ctx 指到 driver 的 aeronDirectoryName。
             * （如果用独立 driver 进程运行，则 ctx 可以用默认目录，或通过环境/系统属性配置。）
             */
            final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName());
            try (Aeron aeron = Aeron.connect(ctx);
                Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
                Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name()))
            {
                /*
                 * addPublication 会在控制面向 driver 发命令创建 Publication。
                 * 真正开始“能发到网络上”，依赖：
                 * - 至少有一个 subscriber（或 MDC 的 receiver）完成连接/接收窗口通告
                 * - driver 为该 Publication 分配好 log buffer 并开始 sender duty cycle
                 *
                 * 下面的循环里我们用 offer 的返回值来处理“背压/未连接/管理动作”等常见状态。
                 */
                while (running.get() && scanner.hasNextLine())
                {
                    System.out.print("> ");
                    System.out.flush();
                    final String line = scanner.nextLine();
                    if (line == null)
                    {
                        break;
                    }
                    final byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
                    if (bytes.length > MAX_MESSAGE_BYTES)
                    {
                        System.out.println("  [跳过] 内容过长，最多 " + MAX_MESSAGE_BYTES + " 字节");
                        continue;
                    }
                    buffer.putBytes(0, bytes);
                    final int length = bytes.length;

                    long result;
                    /*
                     * Publication.offer 是非阻塞 API：应用线程尝试把一条消息写入 Publication 的 term buffer。
                     *
                     * 成功：返回 position（该 stream 的绝对位点，单调递增）。
                     * 失败：返回负值（常见原因见下方），意味着“这次没写进去”，通常需要 idle/backoff 后重试。
                     *
                     * offer 失败并不等价于网络发送失败：这里失败发生在“写入本地 log buffer”阶段，
                     * 原因往往是下游（订阅者）未追上导致的 back pressure，或尚未建立连接。
                     */
                    while ((result = publication.offer(buffer, 0, length)) < 0)
                    {
                        if (result == Publication.BACK_PRESSURED)
                        {
                            /*
                             * BACK_PRESSURED：发送窗口/接收窗口限制导致无法继续推进 position。
                             * 本质是 Aeron 的流控：订阅端（以及中间 driver）通过状态消息（SM）通告可接收的窗口，
                             * publication 必须保证不会覆盖仍可能被订阅者读取的 term buffer 区域。
                             */
                            Thread.sleep(1);
                        }
                        else if (result == Publication.NOT_CONNECTED)
                        {
                            /*
                             * NOT_CONNECTED：还没有可用的连接（例如接收端没启动、或还未完成握手/心跳）。
                             * demo 简化处理为更慢的重试；生产上通常会有超时、降级或告警策略。
                             */
                            Thread.sleep(100);
                        }
                        else
                        {
                            /*
                             * 其他负值还可能包括：
                             * - ADMIN_ACTION：driver 正在轮换 term、清理资源等管理动作
                             * - CLOSED / MAX_POSITION_EXCEEDED：生命周期/容量边界
                             * demo 不展开，直接跳出；需要时可以按 SimplePublisher 的方式细分打印。
                             */
                            break;
                        }
                    }

                    if (result > 0)
                    {
                        System.out.println("  ✓ 已发送 (" + length + " 字节)");
                    }
                }
            }
            System.out.println();
            System.out.println("DemoSender 已退出。");
        }
    }
}

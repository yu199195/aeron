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
package io.aeron.cluster;

import io.aeron.archive.Archive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.status.AtomicCounter;

import static org.agrona.SystemUtil.loadPropertiesFiles;

/**
 * 集群化 MediaDriver 聚合器：将 {@link MediaDriver}、{@link Archive} 和 {@link ConsensusModule} 聚合在同一进程内启动。
 * <p>
 * 这是 Aeron Cluster 的核心部署组件，实现了三个关键模块的协同工作：
 * <ul>
 *   <li><b>MediaDriver</b>：底层传输引擎，负责 UDP/IPC 可靠消息传输</li>
 *   <li><b>Archive</b>：持久化服务，负责日志录制、快照存储与回放</li>
 *   <li><b>ConsensusModule</b>：共识模块，负责 Raft 选举、日志复制、会话管理</li>
 * </ul>
 * <p>
 * <b>关键设计</b>：
 * <ul>
 *   <li>三个组件共享同一 CnC 目录，实现进程内 IPC 通信</li>
 *   <li>Archive 与 MediaDriver 共享 AgentInvoker，避免线程切换开销</li>
 *   <li>统一错误处理与计数器体系，便于监控</li>
 * </ul>
 * <p>
 * <b>典型使用</b>：
 * <pre>{@code
 * ClusteredMediaDriver clusteredMediaDriver = ClusteredMediaDriver.launch(
 *     new MediaDriver.Context().threadingMode(ThreadingMode.SHARED),
 *     new Archive.Context().archiveDir(new File("/data/archive")),
 *     new ConsensusModule.Context()
 *         .clusterMemberId(0)
 *         .clusterMembers("0,node0:9000,node0:9001,node0:9002|...")
 * );
 * }</pre>
 *
 * @see MediaDriver
 * @see Archive
 * @see ConsensusModule
 */
public class ClusteredMediaDriver implements AutoCloseable
{
    /** 底层传输引擎，处理 UDP/IPC 消息收发 */
    private final MediaDriver driver;

    /** 持久化服务，负责日志与快照的录制、存储、回放 */
    private final Archive archive;

    /** 共识模块，负责 Raft 选举、日志序列化、客户端会话管理 */
    private final ConsensusModule consensusModule;

    /**
     * 私有构造函数：创建 ClusteredMediaDriver 实例。
     * <p>
     * 注意：不直接使用此构造函数，应使用静态工厂方法 {@link #launch()}。
     *
     * @param driver          已启动的 MediaDriver 实例
     * @param archive         已启动的 Archive 实例
     * @param consensusModule 已启动的 ConsensusModule 实例
     */
    ClusteredMediaDriver(final MediaDriver driver, final Archive archive, final ConsensusModule consensusModule)
    {
        this.driver = driver;
        this.archive = archive;
        this.consensusModule = consensusModule;
    }

    /**
     * 主入口方法：启动集群 MediaDriver 并等待关闭信号。
     * <p>
     * 典型用法：
     * <pre>
     * java -cp aeron-all.jar io.aeron.cluster.ClusteredMediaDriver [properties-file1] [properties-file2]
     * </pre>
     * <p>
     * 启动流程：
     * <ol>
     *   <li>加载配置文件（通过系统属性或参数传入）</li>
     *   <li>使用默认 Context 启动三大组件</li>
     *   <li>注册关闭钩子（SIGTERM/SIGINT）</li>
     *   <li>阻塞等待关闭信号</li>
     *   <li>收到信号后优雅关闭所有组件</li>
     * </ol>
     *
     * @param args 命令行参数，指定配置文件路径列表（URL 或本地文件名）
     */
    @SuppressWarnings("try")
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
            ClusteredMediaDriver ignore = launch(
                new MediaDriver.Context().terminationHook(barrier::signalAll),
                new Archive.Context(),
                new ConsensusModule.Context().terminationHook(barrier::signalAll)))
        {
            barrier.await();
            System.out.println("Shutdown ClusteredMediaDriver...");
        }
    }

    /**
     * 使用默认配置启动集群 MediaDriver。
     * <p>
     * 等价于：
     * <pre>{@code
     * launch(new MediaDriver.Context(), new Archive.Context(), new ConsensusModule.Context());
     * }</pre>
     * <p>
     * 默认配置适用于开发和测试环境。生产环境应使用 {@link #launch(MediaDriver.Context, Archive.Context, ConsensusModule.Context)}
     * 并显式指定各项配置。
     *
     * @return 已启动的 {@link ClusteredMediaDriver} 实例
     * @see #launch(MediaDriver.Context, Archive.Context, ConsensusModule.Context)
     */
    public static ClusteredMediaDriver launch()
    {
        return launch(new MediaDriver.Context(), new Archive.Context(), new ConsensusModule.Context());
    }

    /**
     * 使用提供的配置上下文启动集群 MediaDriver。
     * <p>
     * 这是推荐的启动方式，允许完全控制三大组件的配置。
     * <p>
     * <b>启动顺序</b>：
     * <ol>
     *   <li><b>MediaDriver</b>：首先启动传输引擎，创建 CnC 文件、共享内存缓冲区</li>
     *   <li><b>Archive</b>：启动持久化服务，复用 MediaDriver 的 AgentInvoker 与 CnC 目录</li>
     *   <li><b>ConsensusModule</b>：启动共识模块，连接到 MediaDriver 和 Archive</li>
     * </ol>
     * <p>
     * <b>异常安全</b>：如果任一组件启动失败，已启动的组件会被自动关闭。
     * <p>
     * <b>配置示例</b>：
     * <pre>{@code
     * ClusteredMediaDriver.launch(
     *     // MediaDriver 配置
     *     new MediaDriver.Context()
     *         .threadingMode(ThreadingMode.SHARED)
     *         .dirDeleteOnStart(false),
     *
     *     // Archive 配置
     *     new Archive.Context()
     *         .archiveDir(new File("/data/aeron/archive"))
     *         .threadingMode(ArchiveThreadingMode.SHARED),
     *
     *     // ConsensusModule 配置
     *     new ConsensusModule.Context()
     *         .clusterMemberId(0)
     *         .clusterMembers("0,node0:9000,node0:9001,node0:9002,node0:8010|" +
     *                         "1,node1:9000,node1:9001,node1:9002,node1:8010|" +
     *                         "2,node2:9000,node2:9001,node2:9002,node2:8010")
     *         .ingressChannel("aeron:udp?endpoint=node0:9000")
     * );
     * }</pre>
     *
     * @param driverCtx          MediaDriver 配置上下文
     * @param archiveCtx         Archive 配置上下文
     * @param consensusModuleCtx ConsensusModule 配置上下文
     * @return 已启动的 {@link ClusteredMediaDriver} 实例
     * @throws io.aeron.exceptions.ConfigurationException 如果配置无效
     * @throws io.aeron.exceptions.RegistrationException 如果资源注册失败（如端口占用）
     */
    public static ClusteredMediaDriver launch(
        final MediaDriver.Context driverCtx,
        final Archive.Context archiveCtx,
        final ConsensusModule.Context consensusModuleCtx)
    {
        MediaDriver driver = null;
        Archive archive = null;
        ConsensusModule consensusModule = null;

        try
        {
            // 步骤 1：启动 MediaDriver（底层传输引擎）
            driver = MediaDriver.launch(driverCtx);

            // 准备 Archive 所需的错误计数器：优先使用 archiveCtx 提供的，否则从 MediaDriver 的 CnC 缓冲区分配
            final int errorCounterId = SystemCounterDescriptor.ERRORS.id();
            final AtomicCounter errorCounter = null != archiveCtx.errorCounter() ?
                archiveCtx.errorCounter() : new AtomicCounter(driverCtx.countersValuesBuffer(), errorCounterId);
            final ErrorHandler errorHandler = null != archiveCtx.errorHandler() ?
                archiveCtx.errorHandler() : driverCtx.errorHandler();

            // 启动 Archive：与同进程 MediaDriver 共享 AgentInvoker、目录及错误处理
            // - mediaDriverAgentInvoker: Archive 与 MediaDriver 在同一线程/调用链中执行，避免多线程与锁
            // - aeronDirectoryName: 使用与 MediaDriver 相同的 CnC/Log 目录，确保 IPC 通信
            // - errorHandler/errorCounter: 统一错误上报与计数
            archive = Archive.launch(archiveCtx
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .aeronDirectoryName(driver.aeronDirectoryName())
                .errorHandler(errorHandler)
                .errorCounter(errorCounter));

            consensusModule = ConsensusModule.launch(consensusModuleCtx
                .aeronDirectoryName(driverCtx.aeronDirectoryName()));

            return new ClusteredMediaDriver(driver, archive, consensusModule);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietCloseAll(consensusModule, archive, driver);
            throw ex;
        }
    }

    /**
     * 获取聚合器中的 {@link MediaDriver} 实例。
     * <p>
     * 可用于访问 MediaDriver 的统计信息、配置或进行高级操作。
     *
     * @return 已启动的 {@link MediaDriver} 实例
     */
    public MediaDriver mediaDriver()
    {
        return driver;
    }

    /**
     * 获取聚合器中的 {@link Archive} 实例。
     * <p>
     * 可用于访问 Archive 的录制信息、控制录制行为或查询存储状态。
     *
     * @return 已启动的 {@link Archive} 实例
     */
    public Archive archive()
    {
        return archive;
    }

    /**
     * 获取聚合器中的 {@link ConsensusModule} 实例。
     * <p>
     * 可用于访问集群状态、成员信息或进行管理操作。
     *
     * @return 已启动的 {@link ConsensusModule} 实例
     */
    public ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    /**
     * 关闭所有聚合的组件（ConsensusModule、Archive、MediaDriver）。
     * <p>
     * <b>关闭顺序</b>（与启动顺序相反）：
     * <ol>
     *   <li><b>ConsensusModule</b>：停止共识协议，关闭客户端会话</li>
     *   <li><b>Archive</b>：停止所有录制与回放，关闭录制文件</li>
     *   <li><b>MediaDriver</b>：关闭所有 Publication/Subscription，释放共享内存</li>
     * </ol>
     * <p>
     * 此方法是幂等的，可以安全地多次调用。
     * <p>
     * 实现了 {@link AutoCloseable}，支持 try-with-resources 语法。
     */
    public void close()
    {
        CloseHelper.closeAll(consensusModule, archive, driver);
    }
}

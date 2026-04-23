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
package io.aeron.samples.cluster;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

/**
 * 集群节点启动器：启动指定 ID 的集群节点（0、1 或 2）。
 * <p>
 * 本类演示如何启动一个完整的 Aeron Cluster 节点，包含三个核心组件：
 * <ul>
 *   <li><b>MediaDriver</b>：底层传输引擎</li>
 *   <li><b>Archive</b>：持久化服务</li>
 *   <li><b>ConsensusModule</b>：共识模块（Raft 协议）</li>
 *   <li><b>ClusteredServiceContainer</b>：业务服务容器</li>
 * </ul>
 * <p>
 * <b>集群拓扑</b>（3 节点，本地测试）：
 * <pre>
 * Node 0: ingress=20000, consensus=20001, log=20002, replication=20003, archive=8010
 * Node 1: ingress=20010, consensus=20011, log=20012, replication=20013, archive=8011
 * Node 2: ingress=20020, consensus=20021, log=20022, replication=20023, archive=8012
 * </pre>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 终端 1：启动节点 0
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNode 0
 *
 * # 终端 2：启动节点 1
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNode 1
 *
 * # 终端 3：启动节点 2
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNode 2
 * </pre>
 * <p>
 * <b>目录结构</b>：
 * <pre>
 * aeron-cluster-demo/
 *   ├─ node0/
 *   │   ├─ aeron-media-driver/      (MediaDriver CnC 文件)
 *   │   ├─ aeron-archive/           (Archive 录制文件)
 *   │   └─ aeron-cluster/           (ConsensusModule 元数据)
 *   ├─ node1/ ...
 *   └─ node2/ ...
 * </pre>
 */
public class ClusterNode
{
    /** 集群成员配置字符串（格式：memberId,clientFacingEndpoint,memberFacingEndpoint,logEndpoint,replicationEndpoint,archiveEndpoint） */
    private static final String CLUSTER_MEMBERS =
        "0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:8010|" +
        "1,localhost:20010,localhost:20011,localhost:20012,localhost:20013,localhost:8011|" +
        "2,localhost:20020,localhost:20021,localhost:20022,localhost:20023,localhost:8012";

    /** 基础工作目录 */
    private static final String BASE_DIR = System.getProperty("user.dir") + "/aeron-cluster-demo";
    
    public static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";

    /**
     * 主入口：启动指定节点。
     *
     * @param args 命令行参数，args[0] 为节点 ID（0、1 或 2）
     */
    public static void main(final String[] args)
    {
        if (args.length != 1)
        {
            System.err.println("用法: ClusterNode <nodeId>");
            System.err.println("  nodeId: 节点 ID，取值范围 [0, 1, 2]");
            System.err.println("\n示例:");
            System.err.println("  java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNode 0");
            System.exit(1);
        }

        final int nodeId = Integer.parseInt(args[0]);
        if (nodeId < 0 || nodeId > 2)
        {
            System.err.println("错误: nodeId 必须是 0、1 或 2，当前值为: " + nodeId);
            System.exit(1);
        }

        System.out.println("========================================");
        System.out.println("  启动 Aeron Cluster Node " + nodeId);
        System.out.println("========================================");
        System.out.println("集群成员: " + CLUSTER_MEMBERS);
        System.out.println("工作目录: " + BASE_DIR + "/node" + nodeId);
        System.out.println();

        // 创建并启动节点
        final ClusterNode node = new ClusterNode();
        node.startNode(nodeId);
    }

    /**
     * 启动指定节点，包括 ClusteredMediaDriver 和 ClusteredServiceContainer。
     *
     * @param nodeId 节点 ID（0、1 或 2）
     */
    private void startNode(final int nodeId)
    {
        final String baseDir = BASE_DIR + "/node" + nodeId;

        // 创建工作目录
        final File baseDirFile = new File(baseDir);
        if (!baseDirFile.exists())
        {
            baseDirFile.mkdirs();
        }

        // 统一的错误处理器（增强版，打印更多上下文信息）
        final ErrorHandler errorHandler = throwable -> {
            System.err.println("========================================");
            System.err.println("[Node " + nodeId + "] !!! 捕获到错误 !!!");
            System.err.println("========================================");
            throwable.printStackTrace(System.err);
            System.err.flush();  // 强制刷新错误流
        };

        // 配置 MediaDriver
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(baseDir + "/aeron-media-driver")
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(errorHandler)
            .dirDeleteOnStart(true);

        // 配置 Archive
        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(baseDir + "/aeron-media-driver")
            .archiveDir(new File(baseDir, "aeron-archive"))
            .controlChannel(udpChannel(nodeId, 8010))
            .replicationChannel(REPLICATION_CHANNEL)
            .threadingMode(ArchiveThreadingMode.SHARED)
            .errorHandler(errorHandler)
            .deleteArchiveOnStart(false);

        // 配置 AeronArchive（用于 ConsensusModule 和 ServiceContainer）
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveContext.controlChannel())
            .controlResponseChannel(archiveContext.localControlChannel());

        // 配置 ConsensusModule
        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .clusterMemberId(nodeId)
            .clusterMembers(CLUSTER_MEMBERS)
            .clusterDir(new File(baseDir, "aeron-cluster"))
            .aeronDirectoryName(baseDir + "/aeron-media-driver")
            .archiveContext(aeronArchiveContext.clone())
            .ingressChannel("aeron:udp")  // 配置客户端入口通道（必需）
            .errorHandler(errorHandler)
            .deleteDirOnStart(false);

        // 配置 ClusteredServiceContainer
        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(baseDir + "/aeron-media-driver")
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDir, "aeron-cluster"))
            .clusteredService(new SimpleClusteredService())
            .errorHandler(errorHandler);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        ClusteredMediaDriver clusteredMediaDriver = null;
        ClusteredServiceContainer container = null;

        try
        {
            System.out.println("[Node " + nodeId + "] 正在启动 ClusteredMediaDriver...");
            clusteredMediaDriver = ClusteredMediaDriver.launch(
                mediaDriverContext.terminationHook(barrier::signalAll),
                archiveContext,
                consensusModuleContext.terminationHook(barrier::signalAll));
            System.out.println("[Node " + nodeId + "] ClusteredMediaDriver 启动成功");

            System.out.println("[Node " + nodeId + "] 正在启动 ClusteredServiceContainer...");
            container = ClusteredServiceContainer.launch(serviceContainerContext);
            System.out.println("[Node " + nodeId + "] ClusteredServiceContainer 启动成功");

            System.out.println();
            System.out.println("[Node " + nodeId + "] ========== 所有组件已启动完成 ==========");
            System.out.println("[Node " + nodeId + "] 等待关闭信号（Ctrl+C）...");
            System.out.println();

            // 阻塞等待关闭信号（SIGTERM 或 SIGINT）
            barrier.await();

            System.out.println();
            System.out.println("[Node " + nodeId + "] 收到关闭信号，开始优雅关闭...");
        } catch (Exception e)
        {
            System.err.println();
            System.err.println("========================================");
            System.err.println("[Node " + nodeId + "] !!! 启动失败 !!!");
            System.err.println("========================================");
            e.printStackTrace(System.err);
            System.err.flush();
            throw new RuntimeException("Node " + nodeId + " 启动失败", e);
        }
        finally
        {
            if (null != container)
            {
                container.close();
            }
            if (null != clusteredMediaDriver)
            {
                clusteredMediaDriver.close();
            }
            barrier.signal();
        }

        System.out.println("[Node " + nodeId + "] 已完全关闭");
    }

    /**
     * 构造 UDP 通道 URI。
     *
     * @param nodeId 节点 ID
     * @param portBase 基础端口（每个节点在此基础上偏移）
     * @return UDP 通道字符串
     */
    private static String udpChannel(final int nodeId, final int portBase)
    {
        final int port = portBase + nodeId;
        return "aeron:udp?endpoint=localhost:" + port;
    }
}

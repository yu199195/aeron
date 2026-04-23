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

import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.util.Arrays;
import java.util.List;

/**
 * 使用 ClusterConfig 的集群节点启动器（推荐方式）。
 * <p>
 * 本类演示如何使用 {@link ClusterConfig} 辅助类来简化集群配置。
 * ClusterConfig 使用标准的端口偏移策略：
 * <ul>
 *   <li>每个节点占用 100 个端口 (PORTS_PER_NODE = 100)</li>
 *   <li>Archive control: basePort + nodeId * 100 + 1</li>
 *   <li>Client facing (ingress): basePort + nodeId * 100 + 2</li>
 *   <li>Member facing (consensus): basePort + nodeId * 100 + 3</li>
 *   <li>Log: basePort + nodeId * 100 + 4</li>
 *   <li>Transfer (catchup): basePort + nodeId * 100 + 5</li>
 * </ul>
 * <p>
 * <b>端口分配示例</b>（portBase=9000, 3 节点）：
 * <pre>
 * Node 0: archive=9001, ingress=9002, consensus=9003, log=9004, catchup=9005
 * Node 1: archive=9101, ingress=9102, consensus=9103, log=9104, catchup=9105
 * Node 2: archive=9201, ingress=9202, consensus=9203, log=9204, catchup=9205
 * </pre>
 * <p>
 * <b>使用方式</b>：
 * <pre>
 * # 终端 1：启动节点 0
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0
 *
 * # 终端 2：启动节点 1
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 1
 *
 * # 终端 3：启动节点 2
 * java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 2
 * </pre>
 */
public class ClusterNodeWithConfig
{
    /** 集群主机列表 */
    private static final List<String> HOSTNAMES = Arrays.asList("localhost", "localhost", "localhost");

    /** 基础端口 */
    private static final int PORT_BASE = 9000;

    /**
     * 主入口：启动指定节点。
     *
     * @param args 命令行参数，args[0] 为节点 ID（0、1 或 2）
     */
    public static void main(final String[] args)
    {
        if (args.length != 1)
        {
            System.err.println("用法: ClusterNodeWithConfig <nodeId>");
            System.err.println("  nodeId: 节点 ID，取值范围 [0, 1, 2]");
            System.err.println("\n示例:");
            System.err.println("  java -cp aeron-all.jar io.aeron.samples.cluster.ClusterNodeWithConfig 0");
            System.exit(1);
        }

        final int nodeId = Integer.parseInt(args[0]);
        if (nodeId < 0 || nodeId >= HOSTNAMES.size())
        {
            System.err.println("错误: nodeId 必须在 [0, " + (HOSTNAMES.size() - 1) + "] 范围内，当前值为: " + nodeId);
            System.exit(1);
        }

        System.out.println("========================================");
        System.out.println("  启动 Aeron Cluster Node " + nodeId);
        System.out.println("  使用 ClusterConfig (推荐方式)");
        System.out.println("========================================");

        // 打印集群成员配置（用于调试）
        final String clusterMembers = ClusterConfig.clusterMembers(HOSTNAMES, HOSTNAMES, PORT_BASE);
        System.out.println("集群成员配置:");
        System.out.println(clusterMembers);
        System.out.println();

        // 打印当前节点的端口分配
        printNodePorts(nodeId, PORT_BASE);
        System.out.println();

        // 创建并启动节点
        final ClusterNodeWithConfig node = new ClusterNodeWithConfig();
        node.startNode(nodeId);
    }

    /**
     * 打印节点的端口分配。
     */
    private static void printNodePorts(final int nodeId, final int portBase)
    {
        System.out.println("Node " + nodeId + " 端口分配:");
        System.out.println("  Archive Control: " +
            ClusterConfig.calculatePort(nodeId, portBase, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET));
        System.out.println("  Client Ingress:  " +
            ClusterConfig.calculatePort(nodeId, portBase, ClusterConfig.CLIENT_FACING_PORT_OFFSET));
        System.out.println("  Consensus:       " +
            ClusterConfig.calculatePort(nodeId, portBase, ClusterConfig.MEMBER_FACING_PORT_OFFSET));
        System.out.println("  Log:             " +
            ClusterConfig.calculatePort(nodeId, portBase, ClusterConfig.LOG_PORT_OFFSET));
        System.out.println("  Catchup:         " +
            ClusterConfig.calculatePort(nodeId, portBase, ClusterConfig.TRANSFER_PORT_OFFSET));
    }

    /**
     * 启动指定节点，使用 ClusterConfig 简化配置。
     *
     * @param nodeId 节点 ID
     */
    private void startNode(final int nodeId)
    {
        // 使用 ClusterConfig.create() 创建配置
        // 这会自动配置所有的 Context 对象
        final ClusterConfig clusterConfig = ClusterConfig.create(
            nodeId,
            HOSTNAMES,
            PORT_BASE,
            new SimpleClusteredService());  // 业务服务

        // 统一的错误处理器
        final ErrorHandler errorHandler = throwable -> {
            System.err.println("========================================");
            System.err.println("[Node " + nodeId + "] !!! 捕获到错误 !!!");
            System.err.println("========================================");
            throwable.printStackTrace(System.err);
            System.err.flush();
        };

        // 设置错误处理器
        clusterConfig.errorHandler(errorHandler);

        // 可选：修改默认配置
        // 设置 MediaDriver 目录为可预测的路径，方便外部客户端连接
        final String aeronDir = "/dev/shm/aeron-cluster-node" + nodeId;
        System.out.println("[Node " + nodeId + "] MediaDriver 目录: " + aeronDir);
        clusterConfig.mediaDriverContext()
            .aeronDirectoryName(aeronDir)
            .dirDeleteOnStart(true);
        clusterConfig.archiveContext().deleteArchiveOnStart(false);
        clusterConfig.consensusModuleContext().deleteDirOnStart(false);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        ClusteredMediaDriver clusteredMediaDriver = null;
        ClusteredServiceContainer container = null;

        try
        {
            System.out.println("[Node " + nodeId + "] 正在启动 ClusteredMediaDriver...");

            // 使用 ClusterConfig 提供的 Context 对象启动
            clusteredMediaDriver = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext().terminationHook(barrier::signalAll),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext().terminationHook(barrier::signalAll));

            System.out.println("[Node " + nodeId + "] ClusteredMediaDriver 启动成功");

            System.out.println("[Node " + nodeId + "] 正在启动 ClusteredServiceContainer...");
            container = ClusteredServiceContainer.launch(clusterConfig.clusteredServiceContext());
            System.out.println("[Node " + nodeId + "] ClusteredServiceContainer 启动成功");

            System.out.println();
            System.out.println("[Node " + nodeId + "] ========== 所有组件已启动完成 ==========");
            System.out.println("[Node " + nodeId + "] 等待关闭信号（Ctrl+C）...");
            System.out.println();

            // 阻塞等待关闭信号
            barrier.await();

            System.out.println();
            System.out.println("[Node " + nodeId + "] 收到关闭信号，开始优雅关闭...");
        }
        catch (Exception e)
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
}
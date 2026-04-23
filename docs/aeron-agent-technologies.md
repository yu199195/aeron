# Aeron Agent（`aeron-agent`）技术说明

本文档说明 **Aeron Agent** 子模块采用的技术栈、各技术的作用，以及模块在整体系统中的功能边界。

---

## 1. 模块定位与核心功能

**Aeron Agent** 是一个 **Java Agent（`java.lang.instrument`）**：在 JVM 启动时或运行时附加到目标进程后，通过 **字节码织入（bytecode weaving）** 在 Aeron **Media Driver**、**Archive**、**Cluster** 等组件的关键路径上插入探针，把结构化事件写入内存 **Ring Buffer**；再由后台 **Reader Agent** 异步消费 Ring Buffer，解码为人类可读文本，输出到 **标准输出** 或 **日志文件**。

典型用途：

- 低侵入地诊断 publication / subscription、网络帧、命令处理、Archive 控制会话、Cluster 共识等行为；
- 无需改业务代码即可打开/关闭细粒度事件日志（通过 JVM 系统属性或动态附加参数配置）。

---

## 2. 技术栈总览

| 技术 / 组件 | 作用 |
|-------------|------|
| **Java Instrumentation API** | Agent 入口（`premain` / `agentmain`）、类重转换（retransform）能力 |
| **Byte Buddy** | 声明式字节码增强：`AgentBuilder`、`Advice` 在目标方法前后注入日志调用 |
| **Gradle Shadow** | 打 **Fat JAR**，并把 Byte Buddy 包名 **relocate**，避免与业务 classpath 冲突 |
| **Agrona** | 无锁 `ManyToOneRingBuffer`、`Agent` / `AgentRunner`、直接内存 `UnsafeBuffer`、时钟与集合工具 |
| **`java.util.ServiceLoader`（SPI）** | 发现各子系统的 `ComponentLogger`，可扩展自定义日志组件 |
| **Aeron 多模块源码依赖** | Agent 织入目标类来自 `aeron-driver`、`aeron-archive`、`aeron-cluster` 等（编译期可见） |
| **`aeron-annotations`（含 `@Versioned`）** | 与 Aeron 版本元数据、文档生成等约定一致 |

---

## 3. Java Agent 与 Manifest

构建产物为 **Shadow JAR**，Manifest 中声明（见根目录 `build.gradle` 中 `project(':aeron-agent')`）：

- **`Premain-Class` / `Agent-Class`**：`io.aeron.agent.EventLogAgent`  
  - `premain`：随 JVM **`-javaagent:...`** 在 `main` 之前启动；  
  - `agentmain`：支持 **动态附加**（attach），并可传 `stop` 停止日志。
- **`Can-Redefine-Classes` / `Can-Retransform-Classes`**：`true`，允许对已加载类做 **retransform**，与 Byte Buddy 的 `RedefinitionStrategy.RETRANSFORMATION` 配合。

**`EventLogAgent`** 负责：

1. 用 `ServiceLoader` 加载所有 `ComponentLogger` 实现；
2. 链式调用各 logger 的 `addInstrumentation`，向 `AgentBuilder` 注册匹配规则与 Advice；
3. `installOn(instrumentation)` 安装 **可重置** 的 `ResettableClassFileTransformer`；
4. 启动守护线程运行 **Reader**（默认 `EventLogReaderAgent`），从全局 `EVENT_RING_BUFFER` 读消息并输出。

---

## 4. Byte Buddy：字节码织入

Agent 使用 **Byte Buddy**（`byte-buddy`、`byte-buddy-agent`）而非手写 ASM，主要模式：

- **`AgentBuilder.Default`**：配置不重写类文件格式（`disableClassFormatChanges`）、**retransform** 发现策略等；
- **`Advice` 类**（如 `DriverInterceptor`）：用 `@Advice.OnMethodEnter` / `@OnMethodExit` 在目标方法入口/出口调用静态方法，内部再调用各 `*EventLogger` 写入 Ring Buffer；
- **`ElementMatchers`**：按类名、方法名等匹配 Driver / Archive / Cluster 中的具体类型。

**Relocate**：Shadow 配置将 `net.bytebuddy` 重定位到 `io.aeron.shadow.net.bytebuddy`，降低与应用程序自带 Byte Buddy 的版本/类加载冲突风险。

---

## 5. Agrona：Ring Buffer 与异步 Reader

### 5.1 `ManyToOneRingBuffer`

- 定义在 **`EventConfiguration.EVENT_RING_BUFFER`**：多块生产者（织入后的各线程）**单消费者**（Reader 线程）；
- 底层为 **直接内存、cache line 对齐** 的 `UnsafeBuffer`，容量由 `aeron.event.buffer.length` 控制（默认 8 MiB + trailer）。

### 5.2 `Agent` + `AgentRunner`

- Reader 实现 **`org.agrona.concurrent.Agent`** 接口（`doWork` 循环）；
- 由 **`AgentRunner`** + **`SleepingMillisIdleStrategy`** 驱动，运行在名为 `event-log-reader` 的 **daemon 线程**上，避免阻塞应用主逻辑。

这样设计把 **高频探针写入**（尽量短、写内存）与 **格式化 I/O**（字符串拼接、写文件）解耦，降低对媒体路径的影响。

---

## 6. 事件模型：`EventCode` 与三类子系统

- **`EventCode`**：在某一 **`EventCodeType`**（Driver / Archive / Cluster）内唯一的 **数值 id**；
- 具体枚举：  
  - **`DriverEventCode`**：驱动层 publication、image、UDP 帧、拥塞、名称解析等；  
  - **`ArchiveEventCode`**：录制、重放、目录、会话等；  
  - **`ClusterEventCode`**：集群角色、选举、append、快照等相关事件。

每条消息在 buffer 中会带 **组件 type code** + **event id**，Reader 侧通过 **`ComponentLogger.decode`** 分派到对应的 **`Dissector`** 逻辑，拼进 `StringBuilder` 再输出。

---

## 7. SPI：`ComponentLogger`

`META-INF/services/io.aeron.agent.ComponentLogger` 注册：

- `io.aeron.agent.DriverComponentLogger`  
- `io.aeron.agent.ArchiveComponentLogger`  
- `io.aeron.agent.ClusterComponentLogger`  

每个实现负责：

- **`typeCode()`**：与消息头中的组件类型对应；  
- **`addInstrumentation(...)`**：根据配置启用的事件集合，向 `AgentBuilder` 注册 Advice；若未启用任何事件，应 **原样返回** 传入的 `AgentBuilder`（`EventLogAgent` 会据此 **提前返回、不安装 transformer**）；  
- **`decode(...)`**：Reader 侧反序列化；  
- **`reset()`**：停止日志时清理静态状态（如 `ENABLED_EVENTS`）。

扩展第三方组件日志时，可提供自己的 `ComponentLogger` 实现并注册到同一 SPI 文件（需注意与 Shadow JAR 的 `mergeServiceFiles` 行为）。

---

## 8. 编码与解码链路（Encoder / Logger / Dissector）

典型路径：

1. **Interceptor（Advice）** → 调用 **`DriverEventLogger` / `ArchiveEventLogger` / `ClusterEventLogger`**（或类似门面）；  
2. Logger 使用 **`CommonEventEncoder`、`*EventEncoder`** 把字段顺序写入 Ring Buffer 消息体；  
3. **`EventLogReaderAgent`** 作为 `MessageHandler` 读 ring，根据 type code 查找 `ComponentLogger`，调用 **`decode`**；  
4. 具体枚举的 **`DriverEventDissector` / `ArchiveEventDissector` / `ClusterEventDissector`** 将二进制字段解析为文本。

**`LogUtil`** 等工具类负责时间戳、十六进制 dump 等格式化细节，保证日志一致、可 grep。

---

## 9. 主要配置项（系统属性）

| 属性 | 含义 |
|------|------|
| `aeron.event.log` | Driver 事件：逗号分隔事件名/id，`all`，或 `admin`（排除高频帧类事件等） |
| `aeron.event.log.disable` | 从已启用集合中排除 |
| `aeron.event.archive.log` / `.disable` | Archive 事件开关 |
| `aeron.event.cluster.log` / `.disable` | Cluster 事件开关 |
| `aeron.event.log.filename` | 指定则追加写文件；否则 `System.out` |
| `aeron.event.log.reader.classname` | 自定义 Reader `Agent` 实现（需有无参或 `(String)` / `(String, List)` 构造） |
| `aeron.event.buffer.length` | Ring Buffer 数据区字节数 |

动态附加时，`EventLogAgent.agentmain` 可解析 `key=value|key2=value2` 形式的 **agentArgs**（见 `ConfigOption.parseAgentArgs`）；`stop` 会撤销 transformer 并关闭 Reader。

---

## 10. `DynamicLoggingAgent`

独立 **main** 程序：通过 **Byte Buddy Agent Attach API**（`ByteBuddyAgent.attach`）把 **Shadow JAR** 挂到 **指定 PID** 的已有 JVM 上，支持 `start`（可先加载 properties 文件再组装 agent 参数）与 `stop`。适用于生产环境不便加 `-javaagent` 启动参数时的 **事后诊断**。

---

## 11. 构建与测试相关

- **`sourcesJar`**：聚合 client/driver/archive/cluster 源码（含生成代码），便于发布与 IDE 关联；  
- **JUnit**：`aeron-agent/src/test` 覆盖 encoder、dissector、配置解析、与 Agent 集成的场景；  
- **`CollectingEventLogReaderAgent`**：测试或 JMX 场景下收集事件，而非写控制台/文件。

---

## 12. 小结

**Aeron Agent** 的本质是：**Instrumentation + Byte Buddy Advice** 在 Aeron 运行时插入探针，**Agrona Ring Buffer** 做生产者与消费者之间的 **有界、无锁缓冲**，**SPI** 划分 Driver / Archive / Cluster 三类日志能力，**可配置事件掩码** 控制开销。理解这条链路后，阅读 `EventLogAgent`、`DriverComponentLogger`、`EventLogReaderAgent` 即可快速定位某一事件从「触发」到「落盘/stdout」的完整路径。

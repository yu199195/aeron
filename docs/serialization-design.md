# 中间件序列化抽象设计

## 1. 设计目标

- **中间件**：提供统一的序列化接口，不关心具体格式（SBE/JSON/Protobuf 等）
- **业务方**：按需实现/选择序列化方式，对接自己的实体类
- **性能**：支持零拷贝、Buffer 复用（适配 Aeron、Netty 等）
- **多类型**：支持同一通道多种消息类型（需类型标识）

---

## 2. 核心接口设计

### 2.1 单类型序列化器 `MessageSerializer<T>`

业务为**单一消息类型**实现，最简单契约。

```
┌─────────────────────────────────────────────────────────────────┐
│  MessageSerializer<T>                                            │
├─────────────────────────────────────────────────────────────────┤
│  + encode(msg: T, buffer, offset): int     → 返回编码字节数       │
│  + decode(buffer, offset, length): T       → 反序列化             │
│  + maxEncodedLength(): int                 → 预分配 buffer 用     │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 带类型标识的编解码器 `MessageCodec<T>`

业务为**每种消息类型**实现一个 Codec，中间件通过 `typeId` 路由。

```
┌─────────────────────────────────────────────────────────────────┐
│  MessageCodec<T> extends MessageSerializer<T>                    │
├─────────────────────────────────────────────────────────────────┤
│  + typeId(): int                    → 消息类型 ID（如 SBE templateId）│
│  + messageType(): Class<T>           → 对应 Java 类型              │
│  + encode(msg: T, buffer, offset): int                           │
│  + decode(buffer, offset, length): T                              │
│  + maxEncodedLength(): int                                        │
└─────────────────────────────────────────────────────────────────┘
```

### 2.3 编解码器注册表 `MessageCodecRegistry`

中间件持有，业务启动时注册所有 Codec，收发时按 `typeId` 查找。

```
┌─────────────────────────────────────────────────────────────────┐
│  MessageCodecRegistry                                            │
├─────────────────────────────────────────────────────────────────┤
│  + register(codec: MessageCodec<?>): void                        │
│  + encode(msg: Object, buffer, offset): int     → 自动取 typeId   │
│  + decode(typeId, buffer, offset, length): Object                │
│  + getCodec(typeId): MessageCodec<?>                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 线格式（Wire Format）

中间件需要统一的「类型 + 载荷」布局，用于多类型场景：

```
┌──────────────────────────────────────────────────────────────────┐
│  Wire Format（每条消息的二进制布局）                                 │
├──────────────────────────────────────────────────────────────────┤
│  [0..3]   typeId    (int32, 4 字节)  消息类型标识                   │
│  [4..7]   length    (int32, 4 字节)  载荷长度（不含 header）          │
│  [8..n]   payload   (变长)           业务序列化后的字节              │
├──────────────────────────────────────────────────────────────────┤
│  Header 固定 8 字节，由中间件写入/解析；payload 由业务 Codec 负责。   │
└──────────────────────────────────────────────────────────────────┘
```

- **单类型通道**：可省略 typeId，只传 payload
- **多类型通道**：必须带 typeId，中间件负责解析 header、按 typeId 分发

---

## 4. 接口定义（Java）

### 4.1 文件结构

```
io.aeron.samples.middleware.serialization/
├── MessageClient.java           # 业务入口：send / subscribe（零感知）
├── MessageClientImpl.java      # 内部实现，封装 encode/decode/WireFormat
├── MessageHandler.java          # 接收回调：onMessage(Object)
├── MessageTransport.java        # 传输层抽象（对接 Aeron/Netty）
├── InMemoryTransport.java      # 内存传输（Demo/单机测试）
├── MessageSerializer.java      # 单类型序列化接口（Codec 实现）
├── MessageCodec.java            # 带 typeId 的编解码器接口
├── MessageCodecRegistry.java    # 编解码器注册表（内部使用）
├── MessageCodecRegistryImpl.java
├── WireFormat.java              # 线格式（内部使用）
├── SerializationDemo.java       # 使用示例
└── impl/
    ├── NewOrder.java             # 业务实体示例
    ├── SbeNewOrderCodec.java     # SBE 实现
    ├── JsonMessageCodec.java     # 通用 JSON 编解码器（接入 toJson/fromJson）
    └── JsonNewOrderCodec.java    # JSON 实现示例
```

### 4.2 业务方使用示例

```java
// 1. 为每种消息实现 Codec，选择序列化方式
MessageCodec<NewOrder> newOrderCodec = new SbeNewOrderCodec();   // SBE
// 或
MessageCodec<NewOrder> newOrderCodec = new JsonNewOrderCodec();   // JSON

// 2. 注册到中间件
MessageCodecRegistry registry = middleware.getCodecRegistry();
registry.register(newOrderCodec);

// 3. 发送（中间件内部调用 registry.encode）
middleware.send(new NewOrder(1001, 88888, 15000, 100, BUY, LIMIT, "AAPL"));

// 4. 接收（中间件解析 typeId，调用 registry.decode，回调业务）
middleware.subscribe(msg -> {
    if (msg instanceof NewOrder order) {
        // 处理订单
    }
});
```

### 4.3 接入 Jackson 示例

```java
ObjectMapper mapper = new ObjectMapper();

MessageCodec<NewOrder> jsonCodec = new JsonMessageCodec<>(
    1,
    NewOrder.class,
    mapper::writeValueAsString,
    s -> mapper.readValue(s, NewOrder.class),
    512);

registry.register(jsonCodec);
```

---

## 5. 实现方式

| 实现 | 适用场景 | 特点 |
|------|----------|------|
| **SbeMessageCodec** | 低延迟、固定结构 | 零分配、按偏移读写、需 Schema |
| **JsonMessageCodec** | 开发调试、灵活结构 | 易用、有 GC、反射/Jackson |
| **ProtobufCodec** | 跨语言、版本演进 | 折中性能、需 .proto |

---

## 6. 使用流程

### 业务方（零感知 API）

业务只需与 `MessageClient` 交互，**无需接触 buffer、offset、encode/decode、WireFormat**：

```java
// 1. 创建客户端（传输层由中间件注入）
MessageClient client = MessageClient.create(transport);

// 2. 注册 Codec（业务选择 SBE 或 JSON）
client.register(new SbeNewOrderCodec());

// 3. 订阅：回调中收到的是已解码的对象
client.subscribe(msg -> {
    if (msg instanceof NewOrder order) {
        // 直接使用业务对象
    }
});

// 4. 发送：直接传业务对象
client.send(new NewOrder(1001, 88888, 15000, 100, BUY, LIMIT, "AAPL"));
```

### 中间件内部（业务不可见）

`MessageClientImpl` 内部封装了编码、Wire 格式、解码：

```java
// 发送路径
void send(Object message) {
    MessageCodec<?> codec = registry.getByMessage(message);
    int payloadLen = registry.encode(message, buffer, WireFormat.HEADER_LENGTH);
    WireFormat.writeHeader(buffer, 0, codec.typeId(), payloadLen);
    transport.send(buffer, 0, WireFormat.HEADER_LENGTH + payloadLen);
}

// 接收路径（transport 回调）
void onReceive(DirectBuffer buffer, int offset, int length) {
    int typeId = WireFormat.readTypeId(buffer, offset);
    int payloadLen = WireFormat.readPayloadLength(buffer, offset);
    Object message = registry.decode(typeId, buffer, offset + 8, payloadLen);
    handler.onMessage(message);
}
```

---

## 7. 扩展点

- **Buffer 池**：中间件可提供 `BufferPool`，业务 Codec 可申请复用
- **Schema 版本**：typeId 可编码版本（如 `(templateId << 8) | version`）
- **压缩**：payload 层可插一层压缩，对业务透明

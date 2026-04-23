# SBE（Simple Binary Encoding）使用指南与 Demo

## 1. 什么是 SBE？

SBE 是一个 **Schema 驱动的二进制编码框架**，专为低延迟消息传递设计。

### 核心思想

```
传统序列化（如 JSON / Protobuf）:
  Java Object → 序列化 → byte[] → 反序列化 → Java Object
  ↑ 每次都创建对象、分配内存、拷贝数据

SBE 方式:
  buffer.putLong(offset, value)  ← 编码：直接写 buffer，无对象创建
  buffer.getLong(offset)          ← 解码：直接读 buffer，无对象创建
  ↑ Flyweight 模式：Codec 只是一层薄包装，数据始终在 buffer 中
```

### SBE vs 传统序列化 对比

| 特性 | JSON | Protobuf | SBE |
|------|------|----------|-----|
| 编码格式 | 文本 | 变长二进制 | 固定偏移二进制 |
| 解码方式 | 解析 + 反射 | 反序列化到对象 | 直接按偏移读 |
| GC 压力 | 高（大量 String） | 中（Builder 对象） | **零**（无对象分配） |
| 编解码延迟 | ~微秒级 | ~百纳秒 | **~纳秒级** |
| 消息大小 | 大 | 中 | **最小** |
| Schema 演进 | 灵活 | 灵活 | 有限但够用 |

---

## 2. 完整工作流程

```
步骤 1                    步骤 2                      步骤 3
定义 Schema          →   SbeTool 生成代码        →    在代码中使用
(trading-codecs.xml)     (Encoder/Decoder.java)       (wrap + read/write)
```

---

## 3. 步骤 1: 定义 SBE Schema

Schema 文件是 XML 格式，定义消息的字段、类型、枚举。

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<sbe:messageSchema xmlns:sbe="http://fixprotocol.io/2016/sbe"
                   package="io.aeron.samples.sbe.codecs"
                   id="200"
                   version="1"
                   description="Sample trading codecs."
                   byteOrder="littleEndian">
    <types>
        <!-- 每条消息前的 8 字节标准 Header -->
        <composite name="messageHeader">
            <type name="blockLength" primitiveType="uint16"/>  <!-- 消息体固定部分长度 -->
            <type name="templateId"  primitiveType="uint16"/>  <!-- 消息类型 ID -->
            <type name="schemaId"    primitiveType="uint16"/>  <!-- Schema ID -->
            <type name="version"     primitiveType="uint16"/>  <!-- Schema 版本 -->
        </composite>

        <!-- 变长字符串的编码方式 -->
        <composite name="varAsciiEncoding">
            <type name="length"  primitiveType="uint32" maxValue="1073741824"/>
            <type name="varData" primitiveType="uint8" length="0" characterEncoding="US-ASCII"/>
        </composite>

        <!-- 枚举 -->
        <enum name="Side" encodingType="uint8">
            <validValue name="BUY">0</validValue>
            <validValue name="SELL">1</validValue>
        </enum>
    </types>

    <!-- 消息定义: templateId=1 -->
    <sbe:message name="NewOrder" id="1" description="Submit a new order.">
        <!-- 固定字段：按定义顺序紧密排列 -->
        <field name="orderId"   id="1" type="int64"/>    <!-- 偏移 0,  8 字节 -->
        <field name="accountId" id="2" type="int64"/>    <!-- 偏移 8,  8 字节 -->
        <field name="price"     id="3" type="int64"/>    <!-- 偏移 16, 8 字节 -->
        <field name="quantity"  id="4" type="int32"/>    <!-- 偏移 24, 4 字节 -->
        <field name="side"      id="5" type="Side"/>     <!-- 偏移 28, 1 字节 -->
        <!-- blockLength = 29 字节 -->

        <!-- 变长字段（必须放在最后） -->
        <data name="symbol" id="6" type="varAsciiEncoding"/>
    </sbe:message>
</sbe:messageSchema>
```

### Schema 关键规则

- **固定字段**按定义顺序紧密排列（无 padding，除非你自己定义对齐）
- **变长字段**（`<data>`）必须放在消息定义的最后
- **枚举**映射为整数，编解码都是一个字节操作
- **messageHeader** 是每条消息的固定前缀，用于标识消息类型和版本

---

## 4. 步骤 2: 用 SbeTool 生成代码

### 方式一：命令行

```bash
# 下载 sbe-all jar（版本与 Aeron 项目一致: 1.37.1）
wget https://repo1.maven.org/maven2/uk/co/real-logic/sbe-all/1.37.1/sbe-all-1.37.1.jar

# 运行 SbeTool
java -Dsbe.output.dir=src/main/java \
     -Dsbe.target.language=Java \
     -jar sbe-all-1.37.1.jar \
     src/main/resources/sample/trading-codecs.xml
```

### 方式二：Gradle（参考 Aeron 的 build.gradle）

```groovy
configurations {
    codecGeneration
}

dependencies {
    codecGeneration 'uk.co.real-logic:sbe-tool:1.37.1'
}

tasks.register('generateCodecs', JavaExec) {
    mainClass.set('uk.co.real_logic.sbe.SbeTool')
    classpath = configurations.codecGeneration
    systemProperties(
        'sbe.output.dir': 'src/main/java',
        'sbe.target.language': 'Java',
        'sbe.validation.stop.on.error': 'true')
    args = ['src/main/resources/sample/trading-codecs.xml']
}
```

### 方式三：Maven

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <executions>
        <execution>
            <goals><goal>java</goal></goals>
        </execution>
    </executions>
    <configuration>
        <mainClass>uk.co.real_logic.sbe.SbeTool</mainClass>
        <systemProperties>
            <systemProperty>
                <key>sbe.output.dir</key>
                <value>${project.build.directory}/generated-sources/java</value>
            </systemProperty>
            <systemProperty>
                <key>sbe.target.language</key>
                <value>Java</value>
            </systemProperty>
        </systemProperties>
        <arguments>
            <argument>src/main/resources/sample/trading-codecs.xml</argument>
        </arguments>
    </configuration>
</plugin>
```

### 生成的代码结构

```
io/aeron/samples/sbe/codecs/
├── MessageHeaderEncoder.java     ← Header 编码器
├── MessageHeaderDecoder.java     ← Header 解码器
├── NewOrderEncoder.java          ← 消息编码器（由 <sbe:message name="NewOrder"> 生成）
├── NewOrderDecoder.java          ← 消息解码器
├── ExecutionReportEncoder.java   ← 另一个消息的编码器
├── ExecutionReportDecoder.java   ← 另一个消息的解码器
├── Side.java                     ← 枚举（由 <enum name="Side"> 生成）
├── OrderType.java                ← 枚举
└── MetaAttribute.java            ← 元属性
```

---

## 5. 步骤 3: 在 Aeron 中使用 SBE Codec

### 5.1 发送端完整代码

```java
import io.aeron.Publication;
import io.aeron.samples.sbe.codecs.*;
import org.agrona.concurrent.UnsafeBuffer;

public class SbeSender {

    // 关键: Encoder 和 buffer 都是预分配的，整个生命周期只创建一次
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final NewOrderEncoder orderEncoder = new NewOrderEncoder();
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    public void sendOrder(Publication publication, long orderId, String symbol) {
        // ① wrapAndApplyHeader: 将 Encoder 绑定到 buffer 上
        //    内部做了: headerEncoder.wrap(buffer, 0) + 设置 blockLength/templateId/schemaId/version
        //    orderEncoder 的写位置从 offset=8（header 之后）开始
        orderEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        // ② 链式调用设置字段（每个调用都是一次 buffer.putXxx）
        orderEncoder
            .orderId(orderId)         // buffer.putLong(8 + 0, orderId)
            .accountId(88888L)        // buffer.putLong(8 + 8, 88888)
            .price(15000L)            // buffer.putLong(8 + 16, 15000)  ← $150.00 用 cents 表示
            .quantity(100)            // buffer.putInt(8 + 24, 100)
            .side(Side.BUY)           // buffer.putByte(8 + 28, 0)
            .orderType(OrderType.LIMIT) // buffer.putByte(8 + 29, 1)
            .symbol(symbol);          // buffer.putInt(8+30, len) + buffer.putBytes(8+34, bytes)

        // ③ 计算总长度 = header(8) + 固定部分(blockLength) + 变长部分
        final int totalLength = MessageHeaderEncoder.ENCODED_LENGTH + orderEncoder.encodedLength();

        // ④ 通过 Aeron Publication 发送
        //    内部: 将 buffer[0..totalLength] 拷贝到 Term Buffer 的 mmap 区域
        //    从这里开始就是 Aeron 的零拷贝路径了（mmap → DatagramChannel.write）
        long result = publication.offer(buffer, 0, totalLength);
        while (result < 0) {
            // 背压处理: BACK_PRESSURED / ADMIN_ACTION 时重试
            Thread.yield();
            result = publication.offer(buffer, 0, totalLength);
        }
    }
}
```

### 5.2 接收端完整代码

```java
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.sbe.codecs.*;
import org.agrona.DirectBuffer;

public class SbeReceiver {

    // Decoder 也是预分配的，复用同一个实例
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final NewOrderDecoder orderDecoder = new NewOrderDecoder();
    private final ExecutionReportDecoder execDecoder = new ExecutionReportDecoder();

    // FragmentHandler: Aeron subscription.poll() 的回调
    private final FragmentHandler handler = (buffer, offset, length, header) -> {
        // ① 先解码 MessageHeader，拿到 templateId 判断消息类型
        headerDecoder.wrap(buffer, offset);

        // ② 根据 templateId 分发到对应的 Decoder
        switch (headerDecoder.templateId()) {
            case NewOrderDecoder.TEMPLATE_ID:     // templateId=1
                decodeNewOrder(buffer, offset);
                break;
            case ExecutionReportDecoder.TEMPLATE_ID: // templateId=2
                decodeExecutionReport(buffer, offset);
                break;
            default:
                System.out.println("Unknown templateId: " + headerDecoder.templateId());
        }
    };

    private void decodeNewOrder(DirectBuffer buffer, int offset) {
        // wrapAndApplyHeader: 将 Decoder 绑定到 buffer，跳过 header
        orderDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

        // 读取字段（每个调用都是一次 buffer.getXxx，零拷贝零分配）
        long orderId   = orderDecoder.orderId();     // buffer.getLong(8 + 0)
        long accountId = orderDecoder.accountId();    // buffer.getLong(8 + 8)
        long price     = orderDecoder.price();        // buffer.getLong(8 + 16)
        int quantity   = orderDecoder.quantity();     // buffer.getInt(8 + 24)
        Side side      = orderDecoder.side();         // buffer.getByte(8 + 28) → 枚举
        OrderType type = orderDecoder.orderType();    // buffer.getByte(8 + 29) → 枚举
        String symbol  = orderDecoder.symbol();       // 读长度前缀 + ASCII 字节

        System.out.printf("NewOrder: id=%d, %s %s %d@$%.2f%n",
            orderId, side, symbol, quantity, price / 100.0);
    }

    private void decodeExecutionReport(DirectBuffer buffer, int offset) {
        execDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);
        // ... 同理读取字段
    }

    public void receive(Subscription subscription) {
        // poll() 内部: 从 Term Buffer 读取 frame → 回调 handler
        subscription.poll(handler, 10);
    }
}
```

---

## 6. 生成代码的内部原理

SBE 生成的 `NewOrderEncoder` 大致长这样：

```java
// SbeTool 自动生成的代码（简化版，展示核心思想）
public final class NewOrderEncoder {
    public static final int TEMPLATE_ID = 1;
    public static final int BLOCK_LENGTH = 30;

    private UnsafeBuffer buffer;
    private int offset;     // header 之后的起始位置

    // wrap: 将 Encoder 绑定到 buffer 的某个位置
    public NewOrderEncoder wrap(UnsafeBuffer buffer, int offset) {
        this.buffer = buffer;
        this.offset = offset;
        return this;
    }

    // 每个字段的 setter 就是一个 putXxx 调用
    public NewOrderEncoder orderId(long value) {
        buffer.putLong(offset + 0, value, LITTLE_ENDIAN);
        return this;
    }

    public NewOrderEncoder accountId(long value) {
        buffer.putLong(offset + 8, value, LITTLE_ENDIAN);
        return this;
    }

    public NewOrderEncoder price(long value) {
        buffer.putLong(offset + 16, value, LITTLE_ENDIAN);
        return this;
    }

    public NewOrderEncoder quantity(int value) {
        buffer.putInt(offset + 24, value, LITTLE_ENDIAN);
        return this;
    }

    public NewOrderEncoder side(Side value) {
        buffer.putByte(offset + 28, (byte)value.value());
        return this;
    }

    // 变长字段: 写长度前缀 + 数据
    public NewOrderEncoder symbol(String value) {
        byte[] bytes = value.getBytes(US_ASCII);
        int varOffset = offset + BLOCK_LENGTH;
        buffer.putInt(varOffset, bytes.length, LITTLE_ENDIAN);
        buffer.putBytes(varOffset + 4, bytes);
        return this;
    }
}
```

**关键理解：整个过程没有创建任何 Java 对象**（除了 `symbol` 的 `getBytes`），
Encoder/Decoder 始终操作同一个预分配的 buffer。

---

## 7. 消息在 buffer 中的二进制布局

以 `NewOrder(orderId=1001, accountId=88888, price=15000, qty=100, side=BUY, symbol="AAPL")` 为例：

```
偏移(byte)  内容                              说明
──────────  ──────────────────────────────    ──────────────
[Header: 8 字节]
 0-1        1E 00                            blockLength=30
 2-3        01 00                            templateId=1 (NewOrder)
 4-5        C8 00                            schemaId=200
 6-7        01 00                            version=1

[固定字段: 30 字节]
 8-15       E9 03 00 00 00 00 00 00          orderId=1001 (int64 LE)
16-23       38 5B 01 00 00 00 00 00          accountId=88888 (int64 LE)
24-31       98 3A 00 00 00 00 00 00          price=15000 (int64 LE)
32-35       64 00 00 00                      quantity=100 (int32 LE)
36          00                               side=BUY (uint8)
37          01                               orderType=LIMIT (uint8)

[变长字段: 4+4=8 字节]
38-41       04 00 00 00                      symbol 长度=4 (uint32 LE)
42-45       41 41 50 4C                      "AAPL" (ASCII)

总计: 8 + 30 + 8 = 46 字节
```

对比 JSON: `{"orderId":1001,"accountId":88888,"price":15000,"quantity":100,"side":"BUY","orderType":"LIMIT","symbol":"AAPL"}`
= **约 110 字节**，且需要解析。

**SBE 只需 46 字节，解码只需按偏移读取。**

---

## 8. 与 Aeron 的集成要点

### 数据流

```
应用层:                Aeron 传输层:                   网络:
                       ┌──────────────────────┐
Encoder.wrap(buf)  →   │ publication.offer()   │  →  UDP DatagramChannel.write()
 ↓ 写字段              │   ↓ 拷贝到 TermBuffer │       ↓ 零拷贝发送
buf[0..len]            │   mmap 共享内存        │       ↓
                       │   Sender 线程扫描发送   │      网络
                       └──────────────────────┘
                       ┌──────────────────────┐
Decoder.wrap(buf)  ←   │ subscription.poll()   │  ←  UDP DatagramChannel.read()
 ↓ 读字段              │   ↓ 从 TermBuffer 读   │       ↑ 接收到 buffer
读取字段值             │   回调 FragmentHandler  │
                       └──────────────────────┘
```

### 最佳实践

1. **预分配所有 Encoder/Decoder 和 buffer**，生命周期内复用
2. **价格用整数（如 cents）** 而非浮点数——避免精度问题且 SBE 原生支持
3. **固定字段优先**，变长字段放最后——这是 SBE 的规则
4. **用 templateId 分发消息**——不需要额外的类型字段
5. **BufferClaim 模式更高效**——避免从应用 buffer 到 TermBuffer 的拷贝

### BufferClaim 模式（进阶）

```java
// 常规模式: 编码到临时 buffer → offer 拷贝到 TermBuffer
encoder.wrap(tempBuffer, 0);
encoder.orderId(1001L);
publication.offer(tempBuffer, 0, length);  // 这里有一次 memcpy

// BufferClaim 模式: 直接在 TermBuffer 上编码，省去一次拷贝
BufferClaim claim = new BufferClaim();
if (publication.tryClaim(length, claim) > 0) {
    // claim.buffer() 直接指向 TermBuffer 的 mmap 内存
    encoder.wrap(claim.buffer(), claim.offset() + HEADER_LENGTH);
    encoder.orderId(1001L);
    // ... 写其他字段 ...
    claim.commit();  // 提交（设置正长度，release 语义）
}
```

---

## 9. 独立运行 Demo

项目中包含一个可直接运行的 Demo 文件：
`aeron-samples/src/main/java/io/aeron/samples/sbe/SbeDemo.java`

该 Demo 用手写的简化 Codec 模拟了 SBE 生成代码的行为，无需先运行 SbeTool，
可以直接编译运行来理解整个数据流。

```bash
# 在 Aeron 项目根目录
./gradlew :aeron-samples:compileJava

# 运行 Demo
java -cp aeron-samples/build/classes/java/main:aeron-client/build/libs/*:aeron-driver/build/libs/*:... \
    io.aeron.samples.sbe.SbeDemo
```

---

## 10. 总结

| 步骤 | 做什么 | 产出 |
|------|--------|------|
| 1. 定义 Schema | 写 `.xml` 文件 | `trading-codecs.xml` |
| 2. 生成代码 | 运行 `SbeTool` | `*Encoder.java`, `*Decoder.java` |
| 3. 编码 | `encoder.wrap(buf).field(value)` | buffer 中的二进制数据 |
| 4. 发送 | `publication.offer(buf, 0, len)` | 数据进入 Aeron Term Buffer |
| 5. 接收 | `subscription.poll(handler, limit)` | 回调中拿到 buffer |
| 6. 解码 | `decoder.wrap(buf).field()` | 直接从 buffer 读取值 |

**整个过程的精髓：数据始终在 buffer 中，Encoder/Decoder 只是一层按偏移读写的薄壳。
没有序列化/反序列化，没有对象创建，没有 GC。这就是 SBE 在纳秒级延迟场景下的核心优势。**

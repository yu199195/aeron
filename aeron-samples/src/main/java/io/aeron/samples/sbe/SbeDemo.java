/*
 * 【SBE + Aeron 完整 Demo】
 *
 * 本文件演示如何在 Aeron 中使用 SBE（Simple Binary Encoding）传递结构化消息。
 *
 * ================================
 * 整体流程（3 步）：
 * ================================
 *
 * 步骤 1: 定义 Schema（trading-codecs.xml）
 *   → 定义消息结构（字段名、类型、枚举）
 *
 * 步骤 2: 用 SbeTool 生成 Java 代码
 *   → 生成 NewOrderEncoder/Decoder、ExecutionReportEncoder/Decoder 等
 *   → 生成的代码是 Flyweight 模式：直接在 buffer 上按偏移读写
 *
 * 步骤 3: 在 Aeron 中使用生成的 Codec
 *   → 发送端：Encoder.wrap(buffer) → 写字段 → publication.offer(buffer)
 *   → 接收端：subscription.poll() → Decoder.wrap(buffer) → 读字段
 *
 * ================================
 * 由于 SBE 生成代码需要构建工具支持，本 Demo 用两种方式演示：
 *   Part A: 模拟 SBE 生成代码的行为（可直接运行，理解原理）
 *   Part B: 真实 SBE 使用说明（需要先生成代码）
 * ================================
 */
package io.aeron.samples.sbe;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * 完整的 SBE + Aeron Demo。
 * <p>
 * 演示了"面向 buffer 编程"的核心思想：
 * 数据从头到尾都在 buffer 中，没有 Java 对象的创建和销毁。
 */
public class SbeDemo
{
    private static final String CHANNEL = "aeron:ipc";
    private static final int STREAM_ID = 100;

    // =========================================================================
    // Part A: 模拟 SBE 生成的 Codec（可直接运行）
    //
    // 真实 SBE 生成的代码本质上就是这样——在 buffer 上按偏移读写。
    // 这里手写一个简化版，让你理解 SBE Codec 的内部实现。
    // =========================================================================

    /**
     * 模拟 SBE MessageHeader（8 字节）
     * <pre>
     * 偏移 0: blockLength (uint16) - 消息体固定部分的长度
     * 偏移 2: templateId  (uint16) - 消息类型 ID
     * 偏移 4: schemaId    (uint16) - Schema ID
     * 偏移 6: version     (uint16) - Schema 版本
     * </pre>
     */
    static final int HEADER_SIZE = 8;

    /**
     * 模拟 SBE 生成的 NewOrderEncoder
     * <pre>
     * templateId = 1
     * 固定字段布局（紧跟在 8 字节 header 之后）：
     * 偏移 0: orderId   (int64)  8 字节
     * 偏移 8: accountId (int64)  8 字节
     * 偏移 16: price    (int64)  8 字节
     * 偏移 24: quantity (int32)  4 字节
     * 偏移 28: side     (uint8)  1 字节
     * 偏移 29: orderType(uint8)  1 字节
     * 固定部分总长 = 30 字节 (blockLength)
     *
     * 变长字段（紧跟在固定部分之后）：
     * symbol: 4 字节长度前缀 + ASCII 字节
     * </pre>
     */
    static class NewOrderCodec
    {
        static final int TEMPLATE_ID = 1;
        static final int SCHEMA_ID = 200;
        static final int VERSION = 1;
        static final int BLOCK_LENGTH = 30;

        static final int ORDER_ID_OFFSET = 0;
        static final int ACCOUNT_ID_OFFSET = 8;
        static final int PRICE_OFFSET = 16;
        static final int QUANTITY_OFFSET = 24;
        static final int SIDE_OFFSET = 28;
        static final int ORDER_TYPE_OFFSET = 29;

        static final byte SIDE_BUY = 0;
        static final byte SIDE_SELL = 1;
        static final byte ORDER_TYPE_MARKET = 0;
        static final byte ORDER_TYPE_LIMIT = 1;

        /**
         * 编码一条 NewOrder 消息到 buffer。
         * 这就是 SBE 生成的 Encoder 本质做的事情——一系列 putLong/putInt/putByte。
         *
         * @return 编码后的总字节数（header + body + 变长字段）
         */
        static int encode(
            final UnsafeBuffer buffer,
            final int offset,
            final long orderId,
            final long accountId,
            final long price,
            final int quantity,
            final byte side,
            final byte orderType,
            final String symbol)
        {
            // 写 MessageHeader（8 字节）
            buffer.putShort(offset + 0, (short)BLOCK_LENGTH, LITTLE_ENDIAN);  // blockLength
            buffer.putShort(offset + 2, (short)TEMPLATE_ID, LITTLE_ENDIAN);   // templateId
            buffer.putShort(offset + 4, (short)SCHEMA_ID, LITTLE_ENDIAN);     // schemaId
            buffer.putShort(offset + 6, (short)VERSION, LITTLE_ENDIAN);       // version

            // 写固定字段（紧跟 header）
            final int bodyOffset = offset + HEADER_SIZE;
            buffer.putLong(bodyOffset + ORDER_ID_OFFSET, orderId, LITTLE_ENDIAN);
            buffer.putLong(bodyOffset + ACCOUNT_ID_OFFSET, accountId, LITTLE_ENDIAN);
            buffer.putLong(bodyOffset + PRICE_OFFSET, price, LITTLE_ENDIAN);
            buffer.putInt(bodyOffset + QUANTITY_OFFSET, quantity, LITTLE_ENDIAN);
            buffer.putByte(bodyOffset + SIDE_OFFSET, side);
            buffer.putByte(bodyOffset + ORDER_TYPE_OFFSET, orderType);

            // 写变长字段 symbol（4 字节长度 + ASCII）
            final int varFieldOffset = bodyOffset + BLOCK_LENGTH;
            final byte[] symbolBytes = symbol.getBytes();
            buffer.putInt(varFieldOffset, symbolBytes.length, LITTLE_ENDIAN);     // 长度前缀
            buffer.putBytes(varFieldOffset + 4, symbolBytes);                     // 数据

            return HEADER_SIZE + BLOCK_LENGTH + 4 + symbolBytes.length;
        }

        /**
         * 从 buffer 解码 NewOrder 消息并打印。
         * 这就是 SBE 生成的 Decoder 本质做的事情——一系列 getLong/getInt/getByte。
         */
        static void decode(final DirectBuffer buffer, final int offset)
        {
            // 读 MessageHeader
            final int blockLength = buffer.getShort(offset + 0, LITTLE_ENDIAN) & 0xFFFF;
            final int templateId = buffer.getShort(offset + 2, LITTLE_ENDIAN) & 0xFFFF;

            if (templateId != TEMPLATE_ID)
            {
                System.out.println("  [错误] templateId 不匹配: " + templateId);
                return;
            }

            // 读固定字段
            final int bodyOffset = offset + HEADER_SIZE;
            final long orderId = buffer.getLong(bodyOffset + ORDER_ID_OFFSET, LITTLE_ENDIAN);
            final long accountId = buffer.getLong(bodyOffset + ACCOUNT_ID_OFFSET, LITTLE_ENDIAN);
            final long price = buffer.getLong(bodyOffset + PRICE_OFFSET, LITTLE_ENDIAN);
            final int quantity = buffer.getInt(bodyOffset + QUANTITY_OFFSET, LITTLE_ENDIAN);
            final byte side = buffer.getByte(bodyOffset + SIDE_OFFSET);
            final byte orderType = buffer.getByte(bodyOffset + ORDER_TYPE_OFFSET);

            // 读变长字段 symbol
            final int varFieldOffset = bodyOffset + blockLength;
            final int symbolLength = buffer.getInt(varFieldOffset, LITTLE_ENDIAN);
            final byte[] symbolBytes = new byte[symbolLength];
            buffer.getBytes(varFieldOffset + 4, symbolBytes);
            final String symbol = new String(symbolBytes);

            System.out.printf("  收到 NewOrder: orderId=%d, accountId=%d, price=$%.2f, " +
                    "qty=%d, side=%s, type=%s, symbol=%s%n",
                orderId, accountId, price / 100.0, quantity,
                side == SIDE_BUY ? "BUY" : "SELL",
                orderType == ORDER_TYPE_LIMIT ? "LIMIT" : "MARKET",
                symbol);
        }
    }

    // =========================================================================
    // Part B: 真实 SBE 使用方式（伪代码，展示 API 风格）
    //
    // 如果你已经用 SbeTool 从 trading-codecs.xml 生成了代码，
    // 使用方式如下：
    // =========================================================================

    /*
    === 发送端（使用 SBE 生成的 Encoder）===

    // SBE 生成的类，直接 import
    import io.aeron.samples.sbe.codecs.MessageHeaderEncoder;
    import io.aeron.samples.sbe.codecs.NewOrderEncoder;
    import io.aeron.samples.sbe.codecs.Side;
    import io.aeron.samples.sbe.codecs.OrderType;

    // 预分配（只在初始化时创建一次，后续复用）
    final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    final NewOrderEncoder orderEncoder = new NewOrderEncoder();
    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[256]);

    // 编码消息
    orderEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder)  // wrap 到 buffer 上
        .orderId(1001L)                // 本质: buffer.putLong(偏移, 1001L)
        .accountId(88888L)             // 本质: buffer.putLong(偏移, 88888L)
        .price(12345L)                 // 本质: buffer.putLong(偏移, 12345L)
        .quantity(100)                 // 本质: buffer.putInt(偏移, 100)
        .side(Side.BUY)               // 本质: buffer.putByte(偏移, 0)
        .orderType(OrderType.LIMIT)   // 本质: buffer.putByte(偏移, 1)
        .symbol("AAPL");              // 本质: buffer.putInt(偏移, 4) + buffer.putBytes(偏移, "AAPL")

    // 计算总长度
    final int totalLength = headerEncoder.encodedLength() + orderEncoder.encodedLength();

    // 通过 Aeron 发送
    publication.offer(buffer, 0, totalLength);

    === 接收端（使用 SBE 生成的 Decoder）===

    import io.aeron.samples.sbe.codecs.MessageHeaderDecoder;
    import io.aeron.samples.sbe.codecs.NewOrderDecoder;

    // 预分配（只在初始化时创建一次）
    final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    final NewOrderDecoder orderDecoder = new NewOrderDecoder();

    // 在 FragmentHandler 回调中解码
    void onFragment(DirectBuffer buffer, int offset, int length, Header header) {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (templateId == NewOrderDecoder.TEMPLATE_ID) {
            orderDecoder.wrapAndApplyHeader(buffer, offset, headerDecoder);

            long orderId  = orderDecoder.orderId();   // 本质: buffer.getLong(偏移)
            long accountId = orderDecoder.accountId();
            long price    = orderDecoder.price();
            int quantity  = orderDecoder.quantity();
            Side side     = orderDecoder.side();       // 本质: buffer.getByte(偏移) → 枚举
            String symbol = orderDecoder.symbol();     // 本质: 读长度前缀 + 读 ASCII 字节
        }
    }
    */

    // =========================================================================
    // Main: 使用 Part A 的手写 Codec 运行完整 Demo
    // =========================================================================

    public static void main(final String[] args) throws InterruptedException
    {
        System.out.println("=============================================");
        System.out.println("  SBE + Aeron Demo（模拟 SBE 生成的 Codec）");
        System.out.println("=============================================");
        System.out.println();

        final UnsafeBuffer sendBuffer = new UnsafeBuffer(
            BufferUtil.allocateDirectAligned(256, BitUtil.CACHE_LINE_LENGTH));

        try (MediaDriver driver = MediaDriver.launchEmbedded();
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication publication = aeron.addPublication(CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID))
        {
            final IdleStrategy idle = new SleepingMillisIdleStrategy(1);

            // 等待连接建立
            while (!publication.isConnected())
            {
                idle.idle();
            }

            System.out.println("[发送端] 连接已建立，开始发送订单...");
            System.out.println();

            // ---------- 发送 5 条订单 ----------
            final String[] symbols = {"AAPL", "GOOG", "TSLA", "MSFT", "AMZN"};
            for (int i = 0; i < 5; i++)
            {
                final int encodedLength = NewOrderCodec.encode(
                    sendBuffer, 0,
                    1001L + i,                                          // orderId
                    88888L,                                             // accountId
                    (long)(150_00 + i * 10_00),                        // price (cents)
                    100 + i * 10,                                       // quantity
                    i % 2 == 0 ? NewOrderCodec.SIDE_BUY : NewOrderCodec.SIDE_SELL,
                    NewOrderCodec.ORDER_TYPE_LIMIT,
                    symbols[i]);

                // offer: 将编码好的字节直接写入 Term Buffer（零拷贝）
                while (publication.offer(sendBuffer, 0, encodedLength) < 0)
                {
                    idle.idle();
                }

                System.out.printf("[发送端] 已发送订单 #%d: %s %s %d@$%.2f (%d 字节)%n",
                    1001 + i,
                    i % 2 == 0 ? "BUY" : "SELL",
                    symbols[i],
                    100 + i * 10,
                    (150_00 + i * 10_00) / 100.0,
                    encodedLength);
            }

            System.out.println();

            // ---------- 接收端：poll 读取消息 ----------
            System.out.println("[接收端] 开始接收...");
            final FragmentHandler handler = (buffer, offset, length, header) ->
            {
                // 读 MessageHeader 的 templateId 判断消息类型
                final int templateId = buffer.getShort(offset + 2, LITTLE_ENDIAN) & 0xFFFF;

                switch (templateId)
                {
                    case NewOrderCodec.TEMPLATE_ID:
                        NewOrderCodec.decode(buffer, offset);
                        break;
                    default:
                        System.out.println("  未知消息类型: templateId=" + templateId);
                        break;
                }
            };

            int received = 0;
            final long deadline = System.currentTimeMillis() + 3000;
            while (received < 5 && System.currentTimeMillis() < deadline)
            {
                received += subscription.poll(handler, 10);
                if (received < 5)
                {
                    idle.idle();
                }
            }

            System.out.println();
            System.out.printf("[完成] 共发送 5 条，接收 %d 条。%n", received);
        }
    }
}

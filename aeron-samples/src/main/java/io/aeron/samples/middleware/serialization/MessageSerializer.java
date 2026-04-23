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
package io.aeron.samples.middleware.serialization;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * 消息序列化接口（单类型）。
 * <p>
 * 业务为每种消息类型实现此接口，中间件通过该接口完成「对象 ↔ 字节」的转换，
 * 不关心具体实现是 SBE、JSON 还是 Protobuf。
 * <p>
 * 设计要点：
 * <ul>
 *   <li>基于 Buffer 而非 byte[]：支持零拷贝、复用，适配 Aeron/Netty 等</li>
 *   <li>encode 返回长度：调用方据此推进 writerIndex 或计算发送字节数</li>
 *   <li>maxEncodedLength：用于预分配 buffer，避免动态扩容</li>
 * </ul>
 *
 * @param <T> 消息类型（业务实体类）
 */
public interface MessageSerializer<T>
{
    /**
     * 将消息编码到 buffer。
     *
     * @param message 待序列化的消息
     * @param buffer  目标 buffer
     * @param offset  写入起始偏移
     * @return 编码后的字节数（从 offset 起）
     */
    int encode(T message, MutableDirectBuffer buffer, int offset);

    /**
     * 从 buffer 解码消息。
     *
     * @param buffer 源 buffer
     * @param offset 读取起始偏移
     * @param length 有效字节长度
     * @return 反序列化后的消息
     */
    T decode(DirectBuffer buffer, int offset, int length);

    /**
     * 该类型消息的最大编码长度。
     * <p>
     * 用于预分配 buffer、校验容量。若无法预估，可返回保守上限。
     *
     * @return 最大编码字节数
     */
    int maxEncodedLength();
}

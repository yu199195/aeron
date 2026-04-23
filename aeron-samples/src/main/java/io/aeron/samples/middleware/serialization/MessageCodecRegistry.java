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
 * 消息编解码器注册表。
 * <p>
 * 中间件持有，业务在启动时注册所有 {@link MessageCodec}，
 * 收发时通过 typeId 或消息对象查找对应 Codec 完成编解码。
 */
public interface MessageCodecRegistry
{
    /**
     * 注册一个消息编解码器。
     *
     * @param codec 编解码器
     * @throws IllegalArgumentException 若 typeId 或 messageType 已存在
     */
    void register(MessageCodec<?> codec);

    /**
     * 按 typeId 获取编解码器。
     *
     * @param typeId 消息类型 ID
     * @return 编解码器，不存在则 null
     */
    MessageCodec<?> getByTypeId(int typeId);

    /**
     * 按消息对象获取编解码器（通过 messageType 匹配）。
     *
     * @param message 消息对象
     * @return 编解码器，不存在则 null
     */
    MessageCodec<?> getByMessage(Object message);

    /**
     * 编码消息到 buffer。
     * <p>
     * 内部根据 message 的 Class 查找 Codec，再调用 encode。
     *
     * @param message 消息对象
     * @param buffer  目标 buffer
     * @param offset  写入起始偏移（payload 起始，不含 wire header）
     * @return 编码后的 payload 字节数
     * @throws IllegalArgumentException 若未找到对应 Codec
     */
    int encode(Object message, MutableDirectBuffer buffer, int offset);

    /**
     * 解码 buffer 为消息对象。
     *
     * @param typeId 消息类型 ID（从 wire header 解析）
     * @param buffer 源 buffer
     * @param offset payload 起始偏移
     * @param length payload 字节长度
     * @return 反序列化后的消息
     * @throws IllegalArgumentException 若未找到对应 Codec
     */
    Object decode(int typeId, DirectBuffer buffer, int offset, int length);
}

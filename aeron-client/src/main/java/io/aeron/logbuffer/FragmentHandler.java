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
package io.aeron.logbuffer;

import org.agrona.DirectBuffer;

/**
 * 从 log buffer 读取数据时的回调：每帧可能是整条消息，或需重组的一条消息的一个 fragment
 * （当消息长度超过 MTU 时会分片）。DemoReceiver 中传入的 lambda 即实现此接口。
 */
@FunctionalInterface
public interface FragmentHandler
{
    /**
     * 每从 log 中读到一个 fragment 时调用；payload 在 buffer 的 [offset, offset+length)。
     * 回调内禁止对 {@link io.aeron.Aeron} 客户端做重入调用（如 addPublication），否则行为未定义。
     *
     * @param buffer 包含数据的 buffer（通常为 term buffer 的视图）。
     * @param offset 数据起始偏移（已跳过 Data 帧头）。
     * @param length 数据字节长度。
     * @param header 本帧的元数据（sessionId、termId 等）。
     */
    void onFragment(DirectBuffer buffer, int offset, int length, Header header);
}

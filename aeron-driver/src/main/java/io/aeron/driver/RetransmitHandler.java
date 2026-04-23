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
package io.aeron.driver;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * 【源码解析】RetransmitHandler —— 发送端处理 NAK（否定确认）并执行重传的核心组件。
 * <p>
 * 当接收端检测到丢包（Term Buffer 中的空洞）时，会发送 NAK 帧到发送端。
 * 发送端的 ControlTransportPoller 收到 NAK 后，经 SendChannelEndpoint → NetworkPublication.onNak()
 * 最终调用本类的 onNak() 方法。
 * <p>
 * 调用链路：
 * ControlTransportPoller.receive() → SendChannelEndpoint.onNakMessage()
 *   → NetworkPublication.onNak() → RetransmitHandler.onNak()
 *   → RetransmitSender.resend() → NetworkPublication.resend() → UDP 重传
 * <p>
 * 并发重传限制：
 * - 单播模式：最多 1 个并发重传（新 NAK 若不重叠旧的则替换之）
 * - 多播模式：最多 maxRetransmits 个并发重传（防止多个接收端 NAK 导致的重传风暴）
 * <p>
 * 重传动作状态机：
 * INACTIVE → (收到 NAK) → DELAYED → (延迟到期) → resend() → LINGERING → (超时) → INACTIVE
 *                         若 delay=0 则直接 resend() → LINGERING
 * <p>
 * LINGERING 状态：重传已发出，但短时间内忽略同一范围的重复 NAK（避免重复重传）。
 */
public final class RetransmitHandler
{
    private final RetransmitAction[] retransmitActionPool; // 重传动作池（单播 1 个，多播 maxRetransmits 个）
    private final NanoClock nanoClock;
    private final FeedbackDelayGenerator delayGenerator;         // 重传延迟生成器（多播防风暴）
    private final FeedbackDelayGenerator lingerTimeoutGenerator; // 重传后的 linger 超时
    private final AtomicCounter invalidPackets;
    private final boolean hasGroupSemantics;                     // 是否多播/MDC 语义
    private final AtomicCounter retransmitOverflowCounter;       // 重传池溢出计数

    private int activeRetransmitCount = 0; // 当前活跃的重传动作数

    /**
     * Create a handler for the dealing with the reception of frame request a frame to be retransmitted.
     *
     * @param nanoClock                 used to determine time.
     * @param invalidPackets            for recording invalid packets.
     * @param delayGenerator            to use for delay determination.
     * @param lingerTimeoutGenerator    to use for linger timeout.
     * @param hasGroupSemantics         indicates multicast/MDC semantics.
     * @param maxRetransmits            max retransmits for when group semantics is enabled
     * @param retransmitOverflowCounter counter to track overflows.
     */
    public RetransmitHandler(
        final NanoClock nanoClock,
        final AtomicCounter invalidPackets,
        final FeedbackDelayGenerator delayGenerator,
        final FeedbackDelayGenerator lingerTimeoutGenerator,
        final boolean hasGroupSemantics,
        final int maxRetransmits,
        final AtomicCounter retransmitOverflowCounter)
    {
        this.nanoClock = nanoClock;
        this.invalidPackets = invalidPackets;
        this.delayGenerator = delayGenerator;
        this.lingerTimeoutGenerator = lingerTimeoutGenerator;
        this.hasGroupSemantics = hasGroupSemantics;
        this.retransmitOverflowCounter = retransmitOverflowCounter;

        final int actualMaxRetransmits = this.hasGroupSemantics ? maxRetransmits : 1;

        retransmitActionPool = new RetransmitAction[actualMaxRetransmits];
        for (int i = 0; i < actualMaxRetransmits; i++)
        {
            retransmitActionPool[i] = new RetransmitAction();
        }
    }

    /**
     * 【NAK 处理入口】收到接收端发来的 NAK 后调用，决定是否以及何时重传。
     * <p>
     * 处理流程：
     * 1. 校验 NAK 参数合法性（offset 对齐、范围有效）
     * 2. 通过 flowControl.maxRetransmissionLength() 限制重传长度（防止过大重传）
     * 3. scanForAvailableRetransmit()：在重传池中查找可用槽位
     *    - 若已有相同范围的重传在进行中 → 返回 null，忽略重复 NAK
     *    - 单播：新 NAK 不重叠旧的 → 直接替换
     *    - 多播：池满 → 溢出计数器递增，忽略
     * 4. 若 delay == 0 → 立即重传（单播通常如此）
     *    若 delay > 0 → 进入 DELAYED 状态，由 processTimeouts() 超时后重传（多播防风暴）
     *
     * @param termId           NAK 指定的 term ID（要重传的数据在哪个 term）
     * @param termOffset       NAK 指定的偏移（要重传的数据起始位置）
     * @param length           NAK 指定的缺失长度
     * @param termLength       term buffer 长度
     * @param mtuLength        MTU 长度
     * @param flowControl      流控策略（用于限制重传长度）
     * @param retransmitSender 重传执行器（通常是 NetworkPublication）
     */
    public void onNak(
        final int termId,
        final int termOffset,
        final int length,
        final int termLength,
        final int mtuLength,
        final FlowControl flowControl,
        final RetransmitSender retransmitSender)
    {
        if (!isInvalid(termOffset, termLength, length) && 0 != length)
        {
            // 通过流控策略限制单次重传长度，防止一个 NAK 触发过大的重传
            final int retransmitLength = flowControl.maxRetransmissionLength(termOffset, length, termLength, mtuLength);
            // 在池中查找可用的重传槽位（同时去重——忽略已在处理中的相同范围 NAK）
            final RetransmitAction action = scanForAvailableRetransmit(termId, termOffset, retransmitLength);
            if (null != action)
            {
                action.termId = termId;
                action.termOffset = termOffset;
                action.length = retransmitLength;

                final long delay = delayGenerator.generateDelayNs();
                if (0 == delay)
                {
                    // 单播通常无延迟，立即重传
                    retransmitSender.resend(termId, termOffset, action.length);
                    action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
                }
                else
                {
                    // 多播：延迟后重传（防止多个接收端 NAK 同一段数据导致的重传风暴）
                    action.delay(delay, nanoClock.nanoTime());
                }
            }
        }
    }

    /**
     * Called to indicate a retransmission is received that may obviate the need to send one ourselves.
     * <p>
     * NOTE: Currently only called from unit tests. Would be used for retransmitting from receivers for NAK suppression.
     *
     * @param termId     of the data.
     * @param termOffset of the data.
     */
    public void onRetransmitReceived(final int termId, final int termOffset)
    {
        final RetransmitAction action = scanForExistingRetransmit(termId, termOffset);

        if (null != action && RetransmitAction.State.DELAYED == action.state)
        {
            removeRetransmit(action);
        }
    }

    /**
     * 【超时处理】由 Sender 线程周期性调用，处理延迟中和 linger 中的重传动作。
     * <p>
     * - DELAYED 状态到期 → 执行 resend()，转入 LINGERING
     * - LINGERING 状态到期 → 释放槽位，转入 INACTIVE（可接受新的 NAK）
     *
     * @param nowNs            当前时间
     * @param retransmitSender 重传执行器
     */
    public void processTimeouts(final long nowNs, final RetransmitSender retransmitSender)
    {
        if (activeRetransmitCount > 0)
        {
            for (final RetransmitAction action : retransmitActionPool)
            {
                if (RetransmitAction.State.DELAYED == action.state && (action.expiryNs - nowNs < 0))
                {
                    // 延迟到期 → 执行重传
                    retransmitSender.resend(action.termId, action.termOffset, action.length);
                    action.linger(lingerTimeoutGenerator.generateDelayNs(), nanoClock.nanoTime());
                }
                else if (RetransmitAction.State.LINGERING == action.state && (action.expiryNs - nowNs < 0))
                {
                    // Linger 到期 → 释放槽位（此范围可接受新 NAK）
                    removeRetransmit(action);
                }
            }
        }
    }

    private boolean isInvalid(final int termOffset, final int termLength, final int length)
    {
        final boolean isInvalid = (termOffset > (termLength - DataHeaderFlyweight.HEADER_LENGTH)) || (termOffset < 0) ||
            (length < 0);

        if (isInvalid)
        {
            invalidPackets.increment();
        }

        return isInvalid;
    }

    private RetransmitAction scanForAvailableRetransmit(final int termId, final int termOffset, final int length)
    {
        if (0 == activeRetransmitCount)
        {
            return addRetransmit(retransmitActionPool[0]);
        }

        RetransmitAction availableAction = null;
        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case INACTIVE:
                    if (null == availableAction)
                    {
                        availableAction = action;
                    }
                    break;

                case DELAYED:
                case LINGERING:
                    if (action.termId == termId &&
                        action.termOffset <= termOffset && termOffset < action.termOffset + action.length)
                    {
                        return null;
                    }

                    if (!hasGroupSemantics)
                    {
                        // this is unicast, and the NAK does NOT overlap the previous one, so just reuse it
                        availableAction = action;
                    }
                    break;
            }
        }

        if (hasGroupSemantics)
        {
            if (null != availableAction)
            {
                return addRetransmit(availableAction);
            }

            retransmitOverflowCounter.increment();
        }

        return availableAction;
    }

    private RetransmitAction scanForExistingRetransmit(final int termId, final int termOffset)
    {
        if (0 == activeRetransmitCount)
        {
            return null;
        }

        for (final RetransmitAction action : retransmitActionPool)
        {
            switch (action.state)
            {
                case DELAYED:
                case LINGERING:
                    if (action.termId == termId && action.termOffset == termOffset)
                    {
                        return action;
                    }
                    break;

                default:
                    break;
            }
        }

        return null;
    }

    private RetransmitAction addRetransmit(final RetransmitAction retransmitAction)
    {
        ++activeRetransmitCount;
        return retransmitAction;
    }

    private void removeRetransmit(final RetransmitAction action)
    {
        --activeRetransmitCount;
        action.cancel();
    }

    /**
     * 重传动作状态机：
     * INACTIVE  → 收到 NAK → DELAYED(等待延迟)
     * DELAYED   → 延迟到期 → resend() → LINGERING(忽略重复NAK)
     * LINGERING → linger到期 → INACTIVE(释放槽位)
     */
    static final class RetransmitAction
    {
        @SuppressWarnings("JavadocVariable")
        enum State
        {
            DELAYED,    // 等待延迟到期后重传（多播防 NAK 风暴）
            LINGERING,  // 已重传，短时间内忽略同一范围的重复 NAK
            INACTIVE    // 空闲，可接受新 NAK
        }

        long expiryNs;
        int termId;
        int termOffset;
        int length;
        State state = State.INACTIVE;

        void delay(final long delayNs, final long nowNs)
        {
            state = State.DELAYED;
            expiryNs = nowNs + delayNs;
        }

        void linger(final long timeoutNs, final long nowNs)
        {
            state = State.LINGERING;
            expiryNs = nowNs + timeoutNs;
        }

        void cancel()
        {
            state = State.INACTIVE;
        }
    }
}

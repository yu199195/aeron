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

import io.aeron.Aeron;
import io.aeron.logbuffer.TermGapScanner;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.logbuffer.TermGapScanner.scanForGap;

/**
 * 【源码解析】LossDetector —— 接收端丢包检测与 NAK 触发的核心组件。
 * <p>
 * 职责：在接收端周期性扫描 Term Buffer，检测 UDP 丢包导致的"空洞"，
 * 并在延迟超时后通过 LossHandler（通常是 PublicationImage）触发 NAK 发送。
 * <p>
 * 调用链路：
 * DriverConductor.trackStreamPositions() → PublicationImage.trackRebuild()
 *   → LossDetector.scan() → TermGapScanner.scanForGap()
 *   → onGap() 记录空洞 → checkTimerExpiry() → lossHandler.onGapDetected() → 发送 NAK
 * <p>
 * 关键设计 —— 延迟 NAK 防风暴：
 * - 首次检测到空洞不立即发 NAK，而是通过 FeedbackDelayGenerator 设置一个随机延迟
 * - 延迟到期后空洞仍在 → 才真正发送 NAK
 * - 多播场景中，各接收端的延迟是随机的，避免多个接收端同时发 NAK 导致"NAK 风暴"
 * <p>
 * 状态追踪：
 * - scanned*: 最近一次 scan 发现的空洞位置
 * - active*:  当前正在追踪（等待超时）的空洞位置
 * - 若两次 scan 发现的空洞位置不同 → 旧空洞已被修复，激活新空洞
 */
public class LossDetector implements TermGapScanner.GapHandler
{
    private long deadlineNs = Aeron.NULL_VALUE;    // NAK 发送截止时间，到期才真正发 NAK

    private int scannedTermId;            // 最近一次扫描发现的空洞 termId
    private int scannedTermOffset = -1;   // 最近一次扫描发现的空洞偏移
    private int scannedLength;            // 最近一次扫描发现的空洞长度

    private int activeTermId;             // 当前正在追踪的空洞 termId
    private int activeTermOffset = -1;    // 当前正在追踪的空洞偏移
    private int activeLength;             // 当前正在追踪的空洞长度

    private final FeedbackDelayGenerator delayGenerator; // NAK 延迟生成器（防风暴）
    private final LossHandler lossHandler;               // 空洞处理器（通常是 PublicationImage）

    /**
     * 创建丢包检测器。
     *
     * @param delayGenerator NAK 延迟生成器，控制首次和重试的 NAK 发送时机
     * @param lossHandler    空洞处理器，onGapDetected() 触发 NAK 发送
     */
    public LossDetector(final FeedbackDelayGenerator delayGenerator, final LossHandler lossHandler)
    {
        this.delayGenerator = delayGenerator;
        this.lossHandler = lossHandler;
    }

    /**
     * 【核心扫描方法】在 Term Buffer 中从 rebuildPosition 到 hwmPosition 之间检测空洞。
     * <p>
     * rebuildPosition：接收端已连续重建（无空洞）到的位置
     * hwmPosition：    接收端收到的最高水位线（可能有空洞）
     * <p>
     * 流程：
     * 1. 调用 TermGapScanner.scanForGap() 扫描空洞，结果通过 onGap() 回调记录到 scanned* 字段
     * 2. 若发现新空洞（与 active* 不同）→ activateGap()：记录到 active*，设置延迟 deadline
     * 3. checkTimerExpiry()：若 deadline 已到期 → 调用 lossHandler.onGapDetected() 触发 NAK
     *
     * @param termBuffer          接收端 term buffer
     * @param rebuildPosition     已连续重建的位置（无空洞）
     * @param hwmPosition         收到的最高水位线
     * @param nowNs               当前时间
     * @param termLengthMask      termLength - 1，用于快速取模
     * @param positionBitsToShift position 位移量
     * @param initialTermId       流的初始 term ID
     * @return 打包结果：高 32 位 = rebuildOffset，低 1 位 = 是否发现新空洞
     */
    public long scan(
        final UnsafeBuffer termBuffer,
        final long rebuildPosition,
        final long hwmPosition,
        final long nowNs,
        final int termLengthMask,
        final int positionBitsToShift,
        final int initialTermId)
    {
        boolean lossFound = false;
        int rebuildOffset = (int)(rebuildPosition & termLengthMask);

        if (rebuildPosition < hwmPosition) // 只在 rebuild 落后于高水位线时扫描
        {
            final int rebuildTermCount = (int)(rebuildPosition >>> positionBitsToShift);
            final int hwmTermCount = (int)(hwmPosition >>> positionBitsToShift);

            final int rebuildTermId = initialTermId + rebuildTermCount;
            final int hwmTermOffset = (int)(hwmPosition & termLengthMask);
            // 若 rebuild 和 hwm 在同一个 term → 只扫到 hwmTermOffset；否则扫到 term 末尾
            final int limitOffset = rebuildTermCount == hwmTermCount ? hwmTermOffset : termLengthMask + 1;

            // 扫描空洞，结果通过 onGap() 回调记录到 scanned* 字段
            rebuildOffset = scanForGap(termBuffer, rebuildTermId, rebuildOffset, limitOffset, this);
            if (rebuildOffset < limitOffset) // 有空洞
            {
                // 检测是否为新空洞（与上次追踪的 active* 不同）
                if (scannedTermOffset != activeTermOffset ||
                    scannedTermId != activeTermId ||
                    scannedLength != activeLength)
                {
                    activateGap(nowNs); // 激活新空洞，设置延迟 deadline
                    lossFound = true;
                }

                checkTimerExpiry(nowNs); // 检查 deadline 是否到期，到期则发 NAK
            }
        }

        return pack(rebuildOffset, lossFound);
    }

    /**
     * 【GapHandler 回调】TermGapScanner 发现空洞时调用，记录空洞位置到 scanned* 字段。
     */
    public void onGap(final int termId, final int offset, final int length)
    {
        scannedTermId = termId;
        scannedTermOffset = offset;
        scannedLength = length;
    }

    /**
     * Pack the values for workCount and rebuildOffset into a long for returning on the stack.
     *
     * @param rebuildOffset value to be packed.
     * @param lossFound     value to be packed.
     * @return a long with rebuildOffset and lossFound packed into it.
     */
    public static long pack(final int rebuildOffset, final boolean lossFound)
    {
        return ((long)rebuildOffset << 32) | (lossFound ? 1 : 0);
    }

    /**
     * Has loss been found in the scan?
     *
     * @param scanOutcome into which the fragments read value has been packed.
     * @return if loss has been found or not.
     */
    public static boolean lossFound(final long scanOutcome)
    {
        return ((int)scanOutcome) != 0;
    }

    /**
     * The offset up to which the log has been rebuilt.
     *
     * @param scanOutcome into which the offset value has been packed.
     * @return the offset up to which the log has been rebuilt.
     */
    public static int rebuildOffset(final long scanOutcome)
    {
        return (int)(scanOutcome >>> 32);
    }

    /**
     * 【激活新空洞】将 scanned* 复制到 active*，并设置首次 NAK 的延迟截止时间。
     * 延迟由 FeedbackDelayGenerator 生成（多播场景下为随机值，防止 NAK 风暴）。
     */
    private void activateGap(final long nowNs)
    {
        activeTermId = scannedTermId;
        activeTermOffset = scannedTermOffset;
        activeLength = scannedLength;

        deadlineNs = nowNs + delayGenerator.generateDelayNs(); // 首次 NAK 延迟
    }

    /**
     * 【检查 NAK 超时】若 deadline 已到期，调用 lossHandler.onGapDetected() 触发 NAK 发送，
     * 并设置下一次重试延迟（若空洞仍未修复，下次 scan 时再次检查）。
     */
    private void checkTimerExpiry(final long nowNs)
    {
        if (deadlineNs - nowNs <= 0)
        {
            lossHandler.onGapDetected(activeTermId, activeTermOffset, activeLength); // → 发送 NAK
            deadlineNs = nowNs + delayGenerator.retryDelayNs(); // 设置重试延迟
        }
    }
}

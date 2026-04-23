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
package io.aeron.driver.media;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.PortUnreachableException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import static io.aeron.logbuffer.FrameDescriptor.frameVersion;
import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;

/**
 * 发送端 / 接收端 UDP 传输的基类：持有 {@link DatagramChannel}、bind/connect 地址与 socket 参数。
 * <p>
 * {@link SendChannelEndpoint} 在 {@link #openDatagramChannel} 中创建并绑定发送用 {@code DatagramChannel}；
 * 数据帧经子类 {@link SendChannelEndpoint#send(ByteBuffer)} 调用 {@code DatagramChannel.write/send} 进入内核 UDP。
 * 接收侧 {@link io.aeron.driver.media.ReceiveChannelEndpoint} 则对另一 {@code DatagramChannel} 做 {@code receive} / selector 读。
 */
public abstract class UdpChannelTransport implements AutoCloseable
{
    /**
     * Context for configuration.
     */
    protected final MediaDriver.Context context;

    /**
     * {@link ErrorHandler} for logging errors and progressing with throwing.
     */
    protected final ErrorHandler errorHandler;

    /**
     * Media configuration for the channel.
     */
    protected final UdpChannel udpChannel;

    /**
     * Channel to be used for sending frames from the perspective of the endpoint.
     */
    protected DatagramChannel sendDatagramChannel;

    /**
     * Channel to be used for receiving frames from the perspective of the endpoint.
     */
    protected DatagramChannel receiveDatagramChannel;

    /**
     * Address to connect to if appropriate for sending.
     */
    protected InetSocketAddress connectAddress;

    private InetSocketAddress bindAddress;
    private final InetSocketAddress endPointAddress;
    private final AtomicCounter invalidPackets;
    private final PortManager portManager;

    private int multicastTtl = 0;
    private final int socketSndbufLength;
    private final int socketRcvbufLength;

    /**
     * Construct transport for a given channel.
     *
     * @param udpChannel         configuration for the media.
     * @param endPointAddress    to which data will be sent.
     * @param bindAddress        for listening on.
     * @param connectAddress     for sending data to.
     * @param context            for configuration.
     * @param portManager        for port binding.
     * @param socketRcvbufLength set SO_RCVBUF for socket, 0 for OS default.
     * @param socketSndbufLength set SO_SNDBUF for socket, 0 for OS default.
     */
    protected UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final PortManager portManager,
        final MediaDriver.Context context,
        final int socketRcvbufLength,
        final int socketSndbufLength)
    {
        this.context = context;
        this.udpChannel = udpChannel;
        this.errorHandler = context.countedErrorHandler();
        this.portManager = portManager;
        this.endPointAddress = endPointAddress;
        this.bindAddress = bindAddress;
        this.connectAddress = connectAddress;
        this.invalidPackets = context.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);
        this.socketRcvbufLength = socketRcvbufLength;
        this.socketSndbufLength = socketSndbufLength;
    }

    /**
     * Construct transport for a given channel.
     *
     * @param udpChannel      configuration for the media.
     * @param endPointAddress to which data will be sent.
     * @param bindAddress     for listening on.
     * @param connectAddress  for sending data to.
     * @param portManager     for port binding.
     * @param context         for configuration.
     */
    protected UdpChannelTransport(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final PortManager portManager,
        final MediaDriver.Context context)
    {
        this(
            udpChannel,
            endPointAddress,
            bindAddress,
            connectAddress,
            portManager,
            context,
            udpChannel.socketRcvbufLengthOrDefault(context.socketRcvbufLength()),
            udpChannel.socketSndbufLengthOrDefault(context.socketSndbufLength()));
    }

    /**
     * Throw a {@link AeronException} with a message for a send error.
     *
     * @param bytesToSend expected to be sent to the network.
     * @param ex          experienced.
     * @param destination to which the send operation was addressed.
     * @see #onSendError(IOException, InetSocketAddress, ErrorHandler)
     * @deprecated {@link #onSendError(IOException, InetSocketAddress, ErrorHandler)} is used instead.
     */
    @Deprecated(forRemoval = true, since = "1.46.6")
    public static void sendError(final int bytesToSend, final IOException ex, final InetSocketAddress destination)
    {
        throw new AeronException(
            "failed to send " + bytesToSend + " byte packet to " + destination, ex, AeronException.Category.WARN);
    }

    /**
     * Report an {@link AeronEvent} with a message for a send error.
     *
     * @param ex           experienced.
     * @param destination  to which the send operation was addressed.
     * @param errorHandler to report error to.
     */
    public static void onSendError(
        final IOException ex, final InetSocketAddress destination, final ErrorHandler errorHandler)
    {
        errorHandler.onError(new AeronEvent(
            "failed to send datagram to " + destination + ", cause: " + ex, AeronException.Category.WARN));
    }

    /**
     * 【UDP Socket 打开的核心方法】
     * 创建 DatagramChannel 并 bind 到指定地址/端口，配置非阻塞模式。
     * <p>
     * 对于发送端（SendChannelEndpoint.openChannel() 调用此方法）：
     * <pre>
     *   单播模式：bind 到本地地址（端口可能由 PortManager 自动分配临时端口）
     *            → 后续 Sender 线程通过此 channel 发送数据帧到远端 endpoint
     *   多播模式：bind 到多播端口 + join 多播组
     *            → 通过 IP_MULTICAST_IF 指定出站网卡
     * </pre>
     * 创建完成后，此 DatagramChannel 会被注册到 Sender 线程的 NIO Selector 上：
     * <pre>
     *   senderProxy.registerSendChannelEndpoint(endpoint)
     *     → controlTransportPoller.registerForRead(endpoint)
     *       → receiveDatagramChannel.register(selector, OP_READ)
     * </pre>
     * 这样 Sender 线程就能通过 Selector.select() 监听来自接收端的控制消息（NAK/StatusMessage）。
     *
     * @param statusIndicator 通道状态计数器，出错时设为 ERRORED
     */
    public void openDatagramChannel(final AtomicCounter statusIndicator)
    {
        try
        {
            // ★ 创建 UDP DatagramChannel（IPv4 或 IPv6 取决于 channel URI 中的地址）
            sendDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
            // 默认收发共用同一个 channel（单播场景）
            receiveDatagramChannel = sendDatagramChannel;

            if (udpChannel.isMulticast())
            {
                // === 多播模式 ===
                if (null != connectAddress)
                {
                    // 有 connect 地址时，收发用不同的 channel
                    receiveDatagramChannel = DatagramChannel.open(udpChannel.protocolFamily());
                }

                // 允许多个进程/线程 bind 同一多播端口
                receiveDatagramChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
                // bind 到多播端口
                receiveDatagramChannel.bind(new InetSocketAddress(endPointAddress.getPort()));
                // 加入多播组，指定网卡接口
                receiveDatagramChannel.join(endPointAddress.getAddress(), udpChannel.localInterface());
                // 设置多播出站网卡
                sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_IF, udpChannel.localInterface());

                if (udpChannel.hasMulticastTtl())
                {
                    sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, udpChannel.multicastTtl());
                    multicastTtl = sendDatagramChannel.getOption(StandardSocketOptions.IP_MULTICAST_TTL);
                }
                else if (context.socketMulticastTtl() != 0)
                {
                    sendDatagramChannel.setOption(StandardSocketOptions.IP_MULTICAST_TTL, context.socketMulticastTtl());
                    multicastTtl = sendDatagramChannel.getOption(StandardSocketOptions.IP_MULTICAST_TTL);
                }
            }
            else
            {
                // === 单播模式 ===
                // PortManager 负责端口管理：如果 bindAddress 端口为 0，OS 会自动分配一个临时端口
                bindAddress = portManager.getManagedPort(udpChannel, bindAddress);
                // ★ bind：将 DatagramChannel 绑定到本地地址和端口，此时 UDP Socket 正式开启
                sendDatagramChannel.bind(bindAddress);
            }

            if (null != connectAddress)
            {
                // connect（仅对单播有意义）：将 socket 连接到远端地址，
                // 后续 send() 不需要再指定目标地址，且只接收来自该地址的数据
                sendDatagramChannel.connect(connectAddress);
            }

            // 设置 socket 缓冲区大小
            if (0 != socketSndbufLength())
            {
                sendDatagramChannel.setOption(SO_SNDBUF, socketSndbufLength());
            }

            if (0 != socketRcvbufLength())
            {
                receiveDatagramChannel.setOption(SO_RCVBUF, socketRcvbufLength());
            }

            // ★ 配置为非阻塞模式：Sender 线程通过 NIO Selector 进行事件驱动的 I/O
            sendDatagramChannel.configureBlocking(false);
            receiveDatagramChannel.configureBlocking(false);
        }
        catch (final IOException ex)
        {
            if (null != statusIndicator)
            {
                statusIndicator.setRelease(ChannelEndpointStatus.ERRORED);
            }

            CloseHelper.quietClose(sendDatagramChannel);
            if (receiveDatagramChannel != sendDatagramChannel)
            {
                CloseHelper.quietClose(receiveDatagramChannel);
            }

            sendDatagramChannel = null;
            receiveDatagramChannel = null;

            final String message = "channel error - " + ex.getMessage() +
                " (at " + ex.getStackTrace()[0].toString() + "): " + udpChannel.originalUriString();

            throw new AeronException(message, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, sendDatagramChannel);
        if (receiveDatagramChannel != sendDatagramChannel)
        {
            CloseHelper.close(errorHandler, receiveDatagramChannel);
        }
        portManager.freeManagedPort(bindAddress);
    }

    /**
     * Return underlying {@link UdpChannel}.
     *
     * @return underlying channel.
     */
    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * The {@link DatagramChannel} for this transport channel.
     *
     * @return {@link DatagramChannel} for this transport channel.
     */
    public DatagramChannel receiveDatagramChannel()
    {
        return receiveDatagramChannel;
    }

    /**
     * Get the multicast TTL value for sending datagrams on the channel.
     *
     * @return the multicast TTL value for sending datagrams on the channel.
     */
    public int multicastTtl()
    {
        return multicastTtl;
    }

    /**
     * Get the bind address and port in endpoint-style format (ip:port).
     * <p>
     * Must be called after the channel is opened.
     *
     * @return the bind address and port in endpoint-style format (ip:port).
     */
    public String bindAddressAndPort()
    {
        try
        {
            final InetSocketAddress localAddress = (InetSocketAddress)receiveDatagramChannel.getLocalAddress();
            if (null != localAddress)
            {
                return NetworkUtil.formatAddressAndPort(localAddress.getAddress(), localAddress.getPort());
            }
        }
        catch (final IOException ignore)
        {
        }

        return "";
    }

    /**
     * Is transport representing a multicast media?
     *
     * @return true if transport is multicast media, otherwise false.
     */
    public boolean isMulticast()
    {
        return udpChannel.isMulticast();
    }

    /**
     * Is the received frame valid. This method will do some basic checks on the header and can be
     * overridden in a subclass for further validation.
     *
     * @param buffer containing the frame.
     * @param length of the frame.
     * @return true if the frame is believed valid otherwise false.
     */
    public boolean isValidFrame(final UnsafeBuffer buffer, final int length)
    {
        if (length >= HeaderFlyweight.MIN_HEADER_LENGTH &&
            frameVersion(buffer, 0) == HeaderFlyweight.CURRENT_VERSION)
        {
            return true;
        }

        invalidPackets.increment();
        return false;
    }

    /**
     * Send packet hook that can be used for logging.
     *
     * @param buffer  containing the packet.
     * @param address to which the packet will be sent.
     */
    @SuppressWarnings("unused")
    public void sendHook(final ByteBuffer buffer, final InetSocketAddress address)
    {
    }

    /**
     * Receive packet hook that can be useful for logging.
     *
     * @param buffer  containing the packet.
     * @param length  length of the packet in bytes.
     * @param address from which the packet came.
     */
    @SuppressWarnings("unused")
    public void receiveHook(final UnsafeBuffer buffer, final int length, final InetSocketAddress address)
    {
    }

    /**
     * Useful hook for logging resend calls.
     *
     * @param sessionId  to resend
     * @param streamId   to resend
     * @param termId     to resend
     * @param termOffset to resend
     * @param length     to resend
     */
    @SuppressWarnings("unused")
    public void resendHook(
        final int sessionId, final int streamId, final int termId, final int termOffset, final int length)
    {
    }

    /**
     * Receive a datagram from the media layer.
     *
     * @param buffer into which the datagram will be received.
     * @return the source address of the datagram if one is available otherwise false.
     */
    public InetSocketAddress receive(final ByteBuffer buffer)
    {
        buffer.clear();

        InetSocketAddress address = null;
        try
        {
            if (receiveDatagramChannel.isOpen())
            {
                address = (InetSocketAddress)receiveDatagramChannel.receive(buffer);
            }
        }
        catch (final PortUnreachableException ignored)
        {
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return address;
    }

    /**
     * Endpoint has moved to a new address. Handle this.
     *
     * @param newAddress      to send data to.
     * @param statusIndicator for the channel
     */
    public void updateEndpoint(final InetSocketAddress newAddress, final AtomicCounter statusIndicator)
    {
        try
        {
            if (null != sendDatagramChannel)
            {
                sendDatagramChannel.disconnect();
                sendDatagramChannel.connect(newAddress);
                connectAddress = newAddress;

                if (null != statusIndicator)
                {
                    statusIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
                }
            }
        }
        catch (final Exception ex)
        {
            if (null != statusIndicator)
            {
                statusIndicator.setRelease(ChannelEndpointStatus.ERRORED);
            }

            final String message = "re-resolve endpoint channel error - " + ex.getMessage() +
                " (at " + ex.getStackTrace()[0].toString() + "): " + udpChannel.originalUriString();

            throw new AeronException(message, ex);
        }
    }

    /**
     * Get the configured OS send socket buffer length (SO_SNDBUF) for the endpoint's socket.
     *
     * @return OS socket send buffer length or 0 if using OS default.
     */
    public int socketSndbufLength()
    {
        return socketSndbufLength;
    }

    /**
     * Get the configured OS receive socket buffer length (SO_RCVBUF) for the endpoint's socket.
     *
     * @return OS socket receive buffer length or 0 if using OS default.
     */
    public int socketRcvbufLength()
    {
        return socketRcvbufLength;
    }
}

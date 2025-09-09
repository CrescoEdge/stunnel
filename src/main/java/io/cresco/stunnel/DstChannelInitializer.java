package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.ScheduledFuture;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DstChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final Map<String, String> tunnelConfig;
    private final String clientId;
    private final PerformanceMonitor performanceMonitor;

    public DstChannelInitializer(SocketController sc, PluginBuilder pb, Map<String, String> tc, String clientId, PerformanceMonitor pm) {
        this.socketController = sc;
        this.plugin = pb;
        this.tunnelConfig = tc;
        this.clientId = clientId;
        this.performanceMonitor = pm;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        String stunnelId = tunnelConfig.get("stunnel_id");
        ch.config().setAllowHalfClosure(true);
        ch.attr(SrcChannelInitializer.CLIENT_ID_KEY).set(clientId);
        ch.attr(SrcChannelInitializer.STUNNEL_ID_KEY).set(stunnelId);
        p.addLast(new DstSessionHandler(socketController, plugin, performanceMonitor));
    }
}

class DstSessionHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final PerformanceMonitor performanceMonitor;
    private final CLogger logger;
    private String clientId;
    private String stunnelId;
    private String jmsListenerId;
    private boolean exceptionHandled = false;

    private volatile boolean gracefulCloseInitiatedBySrc = false;
    private volatile boolean eosSeen = false;
    private volatile boolean outputShutdown = false;
    private long pendingWrites = 0;
    private static final long EOS_TIMEOUT_MS = 5000;
    private ScheduledFuture<?> eosTimeout;

    public DstSessionHandler(SocketController sc, PluginBuilder pb, PerformanceMonitor pm) {
        this.socketController = sc;
        this.plugin = pb;
        this.performanceMonitor = pm;
        this.logger = plugin.getLogger(getClass().getName(), CLogger.Level.Info);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientId = ctx.channel().attr(SrcChannelInitializer.CLIENT_ID_KEY).get();
        this.stunnelId = ctx.channel().attr(SrcChannelInitializer.STUNNEL_ID_KEY).get();
        socketController.addTargetChannel(clientId, ctx.channel());
        logger.info("DST Channel Active (to target): " + ctx.channel().remoteAddress() + ", ClientID: " + clientId + ", StunnelID: " + stunnelId);
        setupJmsListener(ctx);
    }

    private void setupJmsListener(ChannelHandlerContext ctx) {
        try {
            MessageListener ml = msg -> {
                if (ctx.channel().eventLoop().inEventLoop()) {
                    processJmsMessage(ctx, msg);
                } else {
                    ctx.channel().eventLoop().execute(() -> processJmsMessage(ctx, msg));
                }
            };
            String queryString = String.format("stunnel_id='%s' AND client_id='%s' AND direction='dst'", this.stunnelId, this.clientId);
            this.jmsListenerId = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL, ml, queryString);
        } catch (Exception e) {
            logger.error("Failed to setup JMS listener for ClientID: " + clientId + " on DST side. Closing connection.", e);
            notifySrcOfError(e);
            ctx.close();
        }
    }

    private void processJmsMessage(ChannelHandlerContext ctx, Message msg) {
        if (!ctx.channel().isActive()) return;

        try {
            if (msg instanceof BytesMessage) {
                BytesMessage m = (BytesMessage) msg;
                boolean eos = m.propertyExists("eos") && m.getBooleanProperty("eos");

                if (eos) {
                    logger.info("EOS marker received from SRC for ClientID: " + clientId);
                    eosSeen = true;
                    if (eosTimeout != null) { eosTimeout.cancel(false); eosTimeout = null; }
                    maybeHalfClose(ctx);
                    return;
                }

                m.reset();
                byte[] chunk = new byte[8192];
                int n;
                long total = 0L;
                while ((n = m.readBytes(chunk)) > 0) {
                    ByteBuf buf = ctx.alloc().buffer(n);
                    buf.writeBytes(chunk, 0, n);
                    ChannelPromise p = ctx.newPromise();
                    pendingWrites++;
                    ctx.write(buf, p);
                    p.addListener(f -> {
                        if (!f.isSuccess()) {
                            logger.warn("Write failed for ClientID: " + clientId, f.cause());
                        }
                        if (--pendingWrites == 0) {
                            maybeHalfClose(ctx);
                        }
                    });
                    total += n;
                }
                ctx.flush();
                if (total > 0) performanceMonitor.addBytes(total);

            } else if (msg instanceof MapMessage) {
                MapMessage statusMessage = (MapMessage) msg;
                if (statusMessage.itemExists("status")) {
                    int status = statusMessage.getInt("status");
                    if (status == 8) {
                        logger.info("Received graceful close (prepare) from SRC for ClientID: " + clientId + ". Waiting for EOS and drain.");
                        this.gracefulCloseInitiatedBySrc = true;
                        if (!eosSeen && eosTimeout == null) {
                            eosTimeout = ctx.executor().schedule(() -> {
                                if (!eosSeen) {
                                    logger.error("EOS not received within timeout for ClientID: " + clientId + ". Failing the tunnel.");
                                    notifySrcOfError(new Exception("EOS timeout on DST"));
                                    ctx.close();
                                }
                            }, EOS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing JMS message for ClientID: " + clientId + " on DST side.", e);
            ctx.close();
        }
    }

    private void maybeHalfClose(ChannelHandlerContext ctx) {
        if (outputShutdown || !eosSeen || pendingWrites != 0) return;

        outputShutdown = true;
        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE)
                .addListener(f -> {
                    if (f.isSuccess()) {
                        try {
                            ((SocketChannel) ctx.channel()).shutdownOutput().addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
                            logger.debug("shutdownOutput sent for ClientID: " + clientId);
                        } catch (Throwable t) {
                            logger.warn("shutdownOutput failed for ClientID: " + clientId, t);
                            ctx.close();
                        }
                    } else {
                        ctx.close();
                    }
                });
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        int bytesRead = in.readableBytes();
        if (bytesRead > 0) {
            performanceMonitor.addBytes(bytesRead);
            BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
            bytesMessage.setJMSPriority(0);
            bytesMessage.setStringProperty("stunnel_id", this.stunnelId);
            bytesMessage.setStringProperty("direction", "src");
            bytesMessage.setStringProperty("client_id", this.clientId);
            byte[] data = new byte[bytesRead];
            in.readBytes(data);
            bytesMessage.writeBytes(data);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("DST Channel Inactive (to target): " + ctx.channel().remoteAddress() + ", ClientID: " + clientId);
        if (eosTimeout != null) { eosTimeout.cancel(false); eosTimeout = null; }
        if (this.clientId != null) socketController.removeTargetChannel(this.clientId);

        if (this.jmsListenerId != null && !exceptionHandled && !gracefulCloseInitiatedBySrc) {
            sendEosToSrc();
            notifySrcOfGracefulClose();
        }
        removeJmsListener();
    }

    private void removeJmsListener() {
        if (this.jmsListenerId != null) {
            plugin.getAgentService().getDataPlaneService().removeMessageListener(this.jmsListenerId);
            this.jmsListenerId = null;
        }
    }

    private void sendEosToSrc() {
        try {
            BytesMessage eos = plugin.getAgentService().getDataPlaneService().createBytesMessage();
            eos.setJMSPriority(0);
            eos.setStringProperty("stunnel_id", stunnelId);
            eos.setStringProperty("direction", "src");
            eos.setStringProperty("client_id", clientId);
            eos.setBooleanProperty("eos", true);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, eos);
            logger.debug("EOS sent to SRC for ClientID: " + clientId);
        } catch (Exception e) {
            logger.error("Failed to send EOS to SRC for ClientID: " + clientId, e);
        }
    }

    private void notifySrcOfError(Throwable cause) {
        try {
            MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
            closeMessage.setStringProperty("stunnel_id", this.stunnelId);
            closeMessage.setStringProperty("direction", "src");
            closeMessage.setStringProperty("client_id", this.clientId);
            closeMessage.setInt("status", 9);
            closeMessage.setString("error", "DST Failure: " + (cause != null ? cause.toString() : "Unknown reason"));
            closeMessage.setJMSPriority(0);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);
        } catch (Exception e) {
            logger.error("Failed to send error notification to SRC for ClientID: " + clientId, e);
        }
    }

    private void notifySrcOfGracefulClose() {
        try {
            MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
            closeMessage.setStringProperty("stunnel_id", this.stunnelId);
            closeMessage.setStringProperty("direction", "src");
            closeMessage.setStringProperty("client_id", this.clientId);
            closeMessage.setInt("status", 8);
            closeMessage.setJMSPriority(0);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);
        } catch (Exception e) {
            logger.error("Failed to send graceful close notification to SRC for ClientID: " + clientId, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        this.exceptionHandled = true;
        if (cause instanceof ConnectException) {
            logger.error("DST Connection Exception Caught for ClientID: " + clientId + ", Reason: " + cause.getMessage());
        } else if (cause instanceof IOException) {
            logger.info("DST Channel for ClientID: " + clientId + " was closed by the target server. Treating as a normal disconnect.");
        } else {
            logger.error("DST Unhandled Exception Caught for ClientID: " + clientId, cause);
        }

        sendEosToSrc();
        notifySrcOfError(cause);
        ctx.close();
    }
}
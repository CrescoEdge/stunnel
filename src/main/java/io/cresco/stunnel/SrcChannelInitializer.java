package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SrcChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final Map<String, String> tunnelConfig;
    private final PerformanceMonitor performanceMonitor;

    public static final AttributeKey<String> CLIENT_ID_KEY = AttributeKey.valueOf("stunnelClientId");
    public static final AttributeKey<String> STUNNEL_ID_KEY = AttributeKey.valueOf("stunnelId");

    public SrcChannelInitializer(SocketController socketController, PluginBuilder plugin, Map<String, String> tunnelConfig, PerformanceMonitor performanceMonitor) {
        this.socketController = socketController;
        this.plugin = plugin;
        this.tunnelConfig = tunnelConfig;
        this.performanceMonitor = performanceMonitor;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        String clientId = UUID.randomUUID().toString();
        String stunnelId = tunnelConfig.get("stunnel_id");
        ch.config().setAllowHalfClosure(true);
        ch.attr(CLIENT_ID_KEY).set(clientId);
        ch.attr(STUNNEL_ID_KEY).set(stunnelId);
        p.addLast(new SrcSessionHandler(socketController, plugin, performanceMonitor));
    }
}

class SrcSessionHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final PerformanceMonitor performanceMonitor;
    private final CLogger logger;
    private String clientId;
    private String stunnelId;
    private String jmsListenerId;

    private volatile boolean gracefulCloseInitiatedByDst = false;
    private volatile boolean eosSeen = false;
    private volatile boolean outputShutdown = false;
    private long pendingWrites = 0;
    private static final long EOS_TIMEOUT_MS = 5000;
    private io.netty.util.concurrent.ScheduledFuture<?> eosTimeout;

    public SrcSessionHandler(SocketController sc, PluginBuilder pb, PerformanceMonitor pm) {
        this.socketController = sc;
        this.plugin = pb;
        this.performanceMonitor = pm;
        this.logger = plugin.getLogger(getClass().getName(), CLogger.Level.Info);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.clientId = ctx.channel().attr(SrcChannelInitializer.CLIENT_ID_KEY).get();
        this.stunnelId = ctx.channel().attr(SrcChannelInitializer.STUNNEL_ID_KEY).get();
        socketController.addClientChannel(clientId, ctx.channel());
        logger.info("SRC Channel Active: " + ctx.channel().remoteAddress() + ", ClientID: " + clientId + ", StunnelID: " + stunnelId);

        Map<String, String> currentTunnelConfig = socketController.getTunnelConfig(stunnelId);
        if (currentTunnelConfig == null) {
            logger.error("CRITICAL: Tunnel config not found for StunnelID: " + stunnelId + ". Closing channel.");
            ctx.close();
            return;
        }
        initiateDstSession(ctx, currentTunnelConfig);
    }

    private void initiateDstSession(ChannelHandlerContext ctx, Map<String, String> currentTunnelConfig) {
        MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG,
                currentTunnelConfig.get("dst_region"),
                currentTunnelConfig.get("dst_agent"),
                currentTunnelConfig.get("dst_plugin"));
        request.setParam("action", "configdstsession");
        Map<String, String> sessionConfig = new java.util.HashMap<>();
        sessionConfig.put("stunnel_id", stunnelId);
        sessionConfig.put("client_id", clientId);
        Gson gson = new Gson();
        request.setParam("action_session_config", gson.toJson(sessionConfig));
        MsgEvent response = plugin.sendRPC(request);

        if (response != null && "10".equals(response.getParam("status"))) {
            logger.info("DST session initiation request successful for ClientID: " + clientId);
            setupJmsListener(ctx);
        } else {
            logger.error("Failed to initiate DST session for ClientID: " + clientId + ". Closing SRC connection. Response: "
                    + (response != null ? response.getParams() : "null"));
            ctx.close();
        }
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
            String queryString = String.format("stunnel_id='%s' AND client_id='%s' AND direction='src'", this.stunnelId, this.clientId);
            this.jmsListenerId = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL, ml, queryString);
        } catch (Exception e) {
            logger.error("Failed to setup JMS listener for ClientID: " + clientId + ". Closing connection.", e);
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
                    logger.info("EOS marker received from DST for ClientID: " + clientId);
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
                        logger.info("Received graceful close (prepare) from DST for ClientID: " + clientId + ". Waiting for EOS and drain.");
                        this.gracefulCloseInitiatedByDst = true;
                        if (!eosSeen && eosTimeout == null) {
                            eosTimeout = ctx.executor().schedule(() -> {
                                if (!eosSeen) {
                                    logger.error("EOS not received within timeout for ClientID: " + clientId + ". Failing the tunnel.");
                                    sendFailureToDst("EOS timeout on SRC");
                                    ctx.close();
                                }
                            }, EOS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                        }
                    } else if (status == 9) {
                        String error = statusMessage.getString("error");
                        logger.error("Connection failed (status 9) from DST for ClientID: " + clientId + ". Reason: " + error + ". Closing SRC channel.");
                        ctx.close();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing JMS message for ClientID: " + clientId, e);
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
            bytesMessage.setStringProperty("direction", "dst");
            bytesMessage.setStringProperty("client_id", this.clientId);
            byte[] data = new byte[bytesRead];
            in.readBytes(data);
            bytesMessage.writeBytes(data);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("SRC Channel Inactive: " + ctx.channel().remoteAddress() + ", ClientID: " + clientId);
        if (eosTimeout != null) { eosTimeout.cancel(false); eosTimeout = null; }
        if (this.clientId != null) socketController.removeClientChannel(this.clientId);

        if (this.jmsListenerId != null) {
            if (!gracefulCloseInitiatedByDst) {
                notifyDstOfClose();
            }
            removeJmsListener();
        }
    }

    private void removeJmsListener() {
        if (this.jmsListenerId != null) {
            plugin.getAgentService().getDataPlaneService().removeMessageListener(this.jmsListenerId);
            this.jmsListenerId = null;
        }
    }

    private void notifyDstOfClose() {
        try {
            MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
            closeMessage.setStringProperty("stunnel_id", this.stunnelId);
            closeMessage.setStringProperty("direction", "dst");
            closeMessage.setStringProperty("client_id", this.clientId);
            closeMessage.setInt("status", 8);
            closeMessage.setJMSPriority(0);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);
        } catch (Exception e) {
            logger.error("Failed to send close notification to DST for ClientID: " + clientId, e);
        }
    }

    private void sendFailureToDst(String reason) {
        try {
            MapMessage fail = plugin.getAgentService().getDataPlaneService().createMapMessage();
            fail.setStringProperty("stunnel_id", this.stunnelId);
            fail.setStringProperty("direction", "dst");
            fail.setStringProperty("client_id", this.clientId);
            fail.setInt("status", 9);
            fail.setString("error", reason);
            fail.setJMSPriority(0);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, fail);
        } catch (Exception e) {
            logger.warn("Failed to send failure to DST for ClientID: " + clientId, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException) {
            logger.warn("SRC Exception Caught (likely client disconnect) for ClientID: " + clientId + ", Reason: " + cause.getMessage());
        } else {
            logger.error("SRC Unhandled Exception Caught for ClientID: " + clientId, cause);
        }
        ctx.close();
    }
}
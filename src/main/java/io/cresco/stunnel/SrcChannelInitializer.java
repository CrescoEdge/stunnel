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
    private TunnelDemux demux;

    private volatile boolean gracefulCloseInitiatedByDst = false;
    private volatile boolean eosSeen = false;
    private volatile boolean outputShutdown = false;
    private volatile boolean eosSentToDst = false;
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
        // Blocking the event loop on the init RPC froze every other client on this loop thread for
        // the RPC timeout and suppressed the tunnel's traffic-based proof-of-life. Hold reads until
        // the DST session is confirmed (preserving the no-forwarding-before-init ordering), and run
        // the RPC off-loop. The slot bound keeps a connect burst during a fabric stall from parking
        // an unbounded number of init threads; over-bound connects fail fast.
        ctx.channel().config().setAutoRead(false);
        if (!socketController.tryAcquireDstInitSlot()) {
            logger.error("DST session initiation rejected for ClientID: " + clientId + ": too many concurrent initiations. Closing SRC connection.");
            ctx.close();
            return;
        }
        try {
            socketController.getDstInitExecutor().execute(() -> initiateDstSession(ctx, currentTunnelConfig));
        } catch (Throwable t) {
            socketController.releaseDstInitSlot();
            logger.error("Failed to submit DST session initiation for ClientID: " + clientId + ". Closing SRC connection.", t);
            ctx.close();
        }
    }

    private void initiateDstSession(ChannelHandlerContext ctx, Map<String, String> currentTunnelConfig) {
        // Any throw on this off-loop thread would otherwise leave the channel frozen forever:
        // open, autoRead(false), no listener, never closed (exceptionCaught only covers pipeline
        // threads). Fail closed instead.
        try {
            // Register our return-path handler BEFORE the RPC: the DST only starts publishing
            // direction='src' traffic (e.g. a server-speaks-first banner) after it receives this
            // request, so registering first guarantees nothing from the target can be dropped.
            // The tunnel's consumer already exists — this is a map insert, not a JMS call.
            if (!setupDemuxHandler(ctx)) {
                ctx.close();
                return;
            }

            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG,
                    currentTunnelConfig.get("dst_region"),
                    currentTunnelConfig.get("dst_agent"),
                    currentTunnelConfig.get("dst_plugin"));
            if (request == null) {
                // PluginBuilder returns null when the agent service is transiently unavailable
                logger.error("Failed to build DST session request for ClientID: " + clientId + ". Closing SRC connection.");
                ctx.close();
                return;
            }
            request.setParam("action", "configdstsession");
            Map<String, String> sessionConfig = new java.util.HashMap<>();
            sessionConfig.put("stunnel_id", stunnelId);
            sessionConfig.put("client_id", clientId);
            Gson gson = new Gson();
            request.setParam("action_session_config", gson.toJson(sessionConfig));
            MsgEvent response = plugin.sendRPC(request, socketController.getDstInitTimeoutMs());

            ctx.channel().eventLoop().execute(() -> {
                if (!ctx.channel().isActive()) {
                    // Client went away while the RPC was in flight; channelInactive has already
                    // notified the DST side to release any session it may have opened.
                    logger.info("SRC channel closed during DST session initiation for ClientID: " + clientId);
                    return;
                }
                if (response != null && "10".equals(response.getParam("status"))) {
                    logger.info("DST session initiation request successful for ClientID: " + clientId);
                    ctx.channel().config().setAutoRead(true);
                } else {
                    logger.error("Failed to initiate DST session for ClientID: " + clientId + ". Closing SRC connection. Response: "
                            + (response != null ? response.getParams() : "null"));
                    ctx.close();
                }
            });
        } catch (Throwable t) {
            logger.error("DST session initiation failed for ClientID: " + clientId + ". Closing SRC connection.", t);
            ctx.close();
        } finally {
            socketController.releaseDstInitSlot();
        }
    }

    private boolean setupDemuxHandler(ChannelHandlerContext ctx) {
        try {
            demux = socketController.getSrcDemux(stunnelId);
            if (demux == null) {
                logger.error("No SRC demux for StunnelID: " + stunnelId + " (ClientID: " + clientId + ")");
                return false;
            }
            MessageListener ml = msg -> {
                if (ctx.channel().eventLoop().inEventLoop()) {
                    processJmsMessage(ctx, msg);
                } else {
                    ctx.channel().eventLoop().execute(() -> processJmsMessage(ctx, msg));
                }
            };
            demux.expect(clientId);
            if (!demux.register(clientId, ml)) {
                logger.error("SRC demux unusable for ClientID: " + clientId);
                return false;
            }
            return true;
        } catch (Exception e) {
            logger.error("Failed to register SRC demux handler for ClientID: " + clientId, e);
            return false;
        }
    }

    private void processJmsMessage(ChannelHandlerContext ctx, Message msg) {
        if (!ctx.channel().isActive()) return;

        try {
            if (msg instanceof BytesMessage) {
                BytesMessage m = (BytesMessage) msg;
                // HOP TRACE: brokers stamped the DST->SRC broker path onto cresco_hops as this arrived.
                if (m.propertyExists("cresco_hops")) performanceMonitor.setHops(m.getStringProperty("cresco_hops"));
                boolean eos = m.propertyExists("eos") && m.getBooleanProperty("eos");

                if (eos) {
                    logger.info("EOS marker received from DST for ClientID: " + clientId);
                    eosSeen = true;
                    if (eosTimeout != null) { eosTimeout.cancel(false); eosTimeout = null; }
                    maybeHalfClose(ctx);
                    return;
                }

                m.reset();
                // Write the whole message body as ONE buffer. Re-chunking into 8KB writes cost
                // 32x (alloc + copy + write + promise + listener) per 256KB message and was a
                // primary stunnel throughput sink. Read once, wrap (no second copy), single write.
                long bodyLen = m.getBodyLength();
                if (bodyLen > 0) {
                    byte[] data = new byte[(int) bodyLen];
                    int read = m.readBytes(data);
                    ByteBuf buf = Unpooled.wrappedBuffer(data, 0, read);
                    ChannelPromise p = ctx.newPromise();
                    pendingWrites++;
                    ctx.writeAndFlush(buf, p);
                    p.addListener(f -> {
                        if (!f.isSuccess()) {
                            logger.warn("Write failed for ClientID: " + clientId, f.cause());
                        }
                        if (--pendingWrites == 0) {
                            maybeHalfClose(ctx);
                        }
                    });
                    performanceMonitor.addBytes(read);
                }

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
                            ((SocketChannel) ctx.channel()).shutdownOutput().addListener(sf -> {
                                // both directions done -> full close (channelInactive cleans up)
                                if (!sf.isSuccess() || ((SocketChannel) ctx.channel()).isInputShutdown()) {
                                    ctx.close();
                                }
                            });
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
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof io.netty.channel.socket.ChannelInputShutdownEvent) {
            // allowHalfClosure(true) turns a client EOF into this event INSTEAD of channelInactive.
            // Ignoring it (the old behavior) meant a client-initiated close never propagated: no
            // EOS to the DST, target never released, session half-open until tunnel teardown.
            // Relay the half-close: EOS lets the DST FIN the target after drain; keep this channel
            // open for remaining DST->SRC data unless our write side is already shut too.
            logger.debug("Client input shutdown (EOF) for ClientID: " + clientId);
            sendEosToDst();
            if (outputShutdown) {
                ctx.close();
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
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
            bytesMessage.setStringProperty("cresco_trace", "1");   // brokers stamp cresco_hops as it transits
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

        // Notify the DST side even when the session never completed setup (init RPC failed or was
        // still in flight): the configdstsession CONFIG is persistent and can be delivered late, so
        // the DST may open (or hold) a target connection for this already-dead client otherwise.
        // EOS first (mirrors the DST's close path): without it the DST sat in its 5s EOS timeout
        // and tore the session down as an error on every client-initiated close.
        if (!gracefulCloseInitiatedByDst && this.clientId != null && this.stunnelId != null) {
            sendEosToDst();
            notifyDstOfClose();
        }
        if (demux != null && clientId != null) {
            demux.discard(clientId);
        }
    }

    private void sendEosToDst() {
        if (eosSentToDst) return;
        eosSentToDst = true;
        try {
            BytesMessage eos = plugin.getAgentService().getDataPlaneService().createBytesMessage();
            eos.setJMSPriority(0);
            eos.setStringProperty("stunnel_id", stunnelId);
            eos.setStringProperty("direction", "dst");
            eos.setStringProperty("client_id", clientId);
            eos.setBooleanProperty("eos", true);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, eos);
            logger.debug("EOS sent to DST for ClientID: " + clientId);
        } catch (Exception e) {
            logger.error("Failed to send EOS to DST for ClientID: " + clientId, e);
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
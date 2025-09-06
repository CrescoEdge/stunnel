package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.AttributeKey;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Initializes the ChannelPipeline for incoming connections accepted by the ServerBootstrap
 * on the source side of the tunnel.
 */
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
        ch.attr(CLIENT_ID_KEY).set(clientId);
        ch.attr(STUNNEL_ID_KEY).set(stunnelId);
        p.addLast(new SrcSessionHandler(socketController, plugin, performanceMonitor));
    }
}

/**
 * Handles data flow and lifecycle events for a single client connection on the source side.
 */
class SrcSessionHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final PerformanceMonitor performanceMonitor;
    private final CLogger logger;
    private String clientId;
    private String stunnelId;
    private String jmsListenerId;

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

        if (clientId == null || stunnelId == null) {
            logger.error("CRITICAL: ClientID or StunnelID missing from channel attributes. Cannot proceed. Closing channel.");
            ctx.close();
            return;
        }

        Map<String, String> currentTunnelConfig = socketController.getTunnelConfig(stunnelId);
        if (currentTunnelConfig == null) {
            logger.error("CRITICAL: Tunnel config not found for StunnelID: " + stunnelId + ". Closing channel.");
            ctx.close();
            return;
        }

        socketController.addClientChannel(clientId, ctx.channel());
        logger.info("SRC Channel Active: " + ctx.channel().remoteAddress() + ", ClientID: " + clientId + ", StunnelID: " + stunnelId);

        // MODIFIED: Perform a pre-flight health check before attempting to create a session
        logger.debug("Performing pre-flight health check for DST config on tunnel: " + stunnelId);
        MsgEvent healthCheckRequest = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC,
                currentTunnelConfig.get("dst_region"),
                currentTunnelConfig.get("dst_agent"),
                currentTunnelConfig.get("dst_plugin"));
        healthCheckRequest.setParam("action", "tunnelhealthcheck");
        healthCheckRequest.setParam("action_stunnel_id", stunnelId);

        MsgEvent healthCheckResponse = plugin.sendRPC(healthCheckRequest);

        if (healthCheckResponse != null && "10".equals(healthCheckResponse.getParam("status"))) {
            logger.debug("Pre-flight health check successful for tunnel: " + stunnelId + ". Proceeding with session setup.");
            initiateDstSession(ctx, currentTunnelConfig);
        } else {
            logger.error("Pre-flight health check FAILED for tunnel " + stunnelId + ". Destination is not ready or config is missing. Closing SRC channel.");
            ctx.close();
        }
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
            setupJmsListener(ctx, currentTunnelConfig);
        } else {
            logger.error("Failed to initiate DST session for ClientID: " + clientId + ". Closing SRC connection. Response: "
                    + (response != null ? response.getParams() : "null"));
            ctx.close();
        }
    }

    private void setupJmsListener(ChannelHandlerContext ctx, Map<String, String> currentTunnelConfig) {
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
            logger.debug("JMS listener started for ClientID: " + clientId + " (ListenerID: " + jmsListenerId + ")");
        } catch (Exception e) {
            logger.error("Failed to setup JMS listener for ClientID: " + clientId + ". Closing connection.", e);
            ctx.close();
        }
    }

    private void processJmsMessage(ChannelHandlerContext ctx, Message msg) {
        if (!ctx.channel().isActive()) {
            logger.debug("processJmsMessage: Channel inactive for ClientID: " + clientId + ". Ignoring JMS message.");
            return;
        }
        try {
            if (msg instanceof BytesMessage) {
                BytesMessage bytesMessage = (BytesMessage) msg;
                long bodyLength = bytesMessage.getBodyLength();
                if (bodyLength > 0) {
                    ByteBuf buffer = ctx.alloc().buffer((int) bodyLength);
                    try {
                        byte[] data = new byte[(int) bodyLength];
                        int bytesRead = bytesMessage.readBytes(data);
                        if (bytesRead != bodyLength) {
                            logger.warn("JMS BytesMessage read mismatch for ClientID: " + clientId + ". Expected " + bodyLength + ", got " + bytesRead);
                        }
                        buffer.writeBytes(data);
                        ctx.writeAndFlush(buffer);
                        performanceMonitor.addBytes(bodyLength);
                    } catch (Exception readEx) {
                        buffer.release();
                        throw readEx;
                    }
                }
            } else if (msg instanceof MapMessage) {
                MapMessage statusMessage = (MapMessage) msg;
                if (statusMessage.itemExists("status")) {
                    int status = statusMessage.getInt("status");
                    logger.debug("Received status message from DST for ClientID: " + clientId + ", Status: " + status);
                    if (status == 8) {
                        logger.info("Received graceful close notification (status 8) from DST for ClientID: " + clientId + ". Closing SRC channel.");
                        removeJmsListener();
                        ctx.close();
                    } else if (status == 9) {
                        String error = statusMessage.itemExists("error") ? statusMessage.getString("error") : "Unknown error";
                        logger.error("Received connection failed notification (status 9) from DST for ClientID: " + clientId + ". Reason: " + error + ". Closing SRC channel.");
                        ctx.close();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing JMS message for ClientID: " + clientId, e);
            ctx.close();
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
            byte[] data = new byte[bytesRead];
            in.readBytes(data);
            bytesMessage.writeBytes(data);
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("SRC Channel Inactive: " + ctx.channel().remoteAddress() + ", ClientID: " + clientId);
        if (this.clientId != null) {
            socketController.removeClientChannel(this.clientId);
        }

        if (this.jmsListenerId != null) {
            notifyDstOfClose();
            removeJmsListener();
        }
    }

    private void removeJmsListener() {
        if (this.jmsListenerId != null) {
            try {
                plugin.getAgentService().getDataPlaneService().removeMessageListener(this.jmsListenerId);
                logger.debug("JMS listener removed for ClientID: " + clientId + " (ListenerID: " + jmsListenerId + ")");
            } catch (Exception e) {
                logger.error("Error removing JMS listener for ClientID: " + clientId, e);
            }
            this.jmsListenerId = null;
        }
    }

    private void notifyDstOfClose() {
        try {
            Map<String, String> currentTunnelConfig = socketController.getTunnelConfig(stunnelId);
            if (currentTunnelConfig != null) {
                MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
                closeMessage.setStringProperty("stunnel_id", this.stunnelId);
                closeMessage.setStringProperty("direction", "dst");
                closeMessage.setStringProperty("client_id", this.clientId);
                closeMessage.setInt("status", 8);
                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);
                logger.debug("Sent graceful close notification (status 8) to DST for ClientID: " + clientId);
            } else {
                logger.warn("Cannot send close notification for ClientID " + clientId + ": Tunnel config not found.");
            }
        } catch (Exception e) {
            logger.error("Failed to send close notification to DST for ClientID: " + clientId, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException && cause.getMessage() != null &&
                (cause.getMessage().toLowerCase().contains("connection reset by peer") ||
                        cause.getMessage().toLowerCase().contains("broken pipe"))) {
            logger.warn("SRC Exception Caught (likely client disconnect) for ClientID: " + clientId + ", Reason: " + cause.getMessage());
        } else {
            logger.error("SRC Unhandled Exception Caught for ClientID: " + clientId, cause);
        }
        ctx.close();
    }
}
package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.io.IOException;
import java.util.Map;

/**
 * Initializes the ChannelPipeline for outgoing connections created by the Bootstrap
 * on the destination side of the tunnel (connecting to the target server).
 */
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
        ch.attr(SrcChannelInitializer.CLIENT_ID_KEY).set(clientId);
        ch.attr(SrcChannelInitializer.STUNNEL_ID_KEY).set(stunnelId);
        p.addLast(new DstSessionHandler(socketController, plugin, performanceMonitor));
    }
}

/**
 * Handles data flow and lifecycle for the connection to the target server on the destination side.
 */
class DstSessionHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final SocketController socketController;
    private final PluginBuilder plugin;
    private final PerformanceMonitor performanceMonitor;
    private final CLogger logger;
    private String clientId;
    private String stunnelId;
    private String jmsListenerId;

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

        if (clientId == null || stunnelId == null) {
            logger.error("CRITICAL: ClientID or StunnelID missing from channel attributes on DST side. Closing channel.");
            ctx.close();
            return;
        }

        Map<String, String> currentTunnelConfig = socketController.getTunnelConfig(stunnelId);
        if (currentTunnelConfig == null) {
            logger.error("CRITICAL: Tunnel config not found for StunnelID: " + stunnelId + " on DST side. Closing channel.");
            ctx.close();
            return;
        }

        socketController.addTargetChannel(clientId, ctx.channel());
        logger.info("DST Channel Active (to target): " + ctx.channel().remoteAddress() + ", ClientID: " + clientId + ", StunnelID: " + stunnelId);
        setupJmsListener(ctx, currentTunnelConfig);
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
            String queryString = String.format("stunnel_id='%s' AND client_id='%s' AND direction='dst'", this.stunnelId, this.clientId);
            this.jmsListenerId = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL, ml, queryString);
            logger.debug("JMS listener started for ClientID: " + clientId + " (ListenerID: " + jmsListenerId + ")");
        } catch (Exception e) {
            logger.error("Failed to setup JMS listener for ClientID: " + clientId + " on DST side. Closing connection.", e);
            ctx.close();
            sendDstSessionFailedStatus(currentTunnelConfig, clientId, e);
        }
    }

    private void sendDstSessionFailedStatus(Map<String,String> tunnelConfig, String clientId, Throwable cause) {
        try {
            MapMessage statusMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
            statusMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
            statusMessage.setStringProperty("direction", "src");
            statusMessage.setStringProperty("client_id", clientId);
            statusMessage.setInt("status", 9);
            statusMessage.setString("error", "Failed during DST session setup/JMS: " + (cause != null ? cause.getMessage() : "Unknown reason"));
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, statusMessage);
            logger.debug("Sent DST session setup failed status (9) to SRC for ClientID: " + clientId);
        } catch (Exception e) {
            logger.error("Failed to send DST session failed status to SRC for ClientID: " + clientId, e);
        }
    }

    private void processJmsMessage(ChannelHandlerContext ctx, Message msg) {
        if (!ctx.channel().isActive()) {
            logger.warn("processJmsMessage: DST Channel inactive for ClientID: " + clientId + ". Ignoring JMS message.");
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
                    } catch(Exception readEx) {
                        buffer.release();
                        throw readEx;
                    }
                }
            } else if (msg instanceof MapMessage) {
                MapMessage statusMessage = (MapMessage) msg;
                if (statusMessage.itemExists("status")) {
                    int status = statusMessage.getInt("status");
                    logger.debug("Received status message from SRC for ClientID: " + clientId + ", Status: " + status);
                    if (status == 8) {
                        logger.info("Received graceful close notification (status 8) from SRC for ClientID: " + clientId + ". Closing DST channel (to target).");
                        removeJmsListener();
                        ctx.close();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing JMS message for ClientID: " + clientId + " on DST side.", e);
            ctx.close();
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        int bytesRead = in.readableBytes();
        if (bytesRead > 0) {
            performanceMonitor.addBytes(bytesRead);
            BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
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
        if (this.clientId != null) {
            socketController.removeTargetChannel(this.clientId);
        }

        // Only send a notification if we are the ones initiating the close
        if (this.jmsListenerId != null) {
            notifySrcOfClose();
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

    private void notifySrcOfClose() {
        try {
            Map<String, String> currentTunnelConfig = socketController.getTunnelConfig(stunnelId);
            if (currentTunnelConfig != null) {
                MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
                closeMessage.setStringProperty("stunnel_id", this.stunnelId);
                closeMessage.setStringProperty("direction", "src");
                closeMessage.setStringProperty("client_id", this.clientId);
                closeMessage.setInt("status", 8);
                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);
                logger.debug("Sent graceful close notification (status 8) to SRC for ClientID: " + clientId);
            } else {
                logger.warn("Cannot send close notification for ClientID " + clientId + ": Tunnel config not found.");
            }
        } catch (Exception e) {
            logger.error("Failed to send close notification to SRC for ClientID: " + clientId, e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof IOException && cause.getMessage() != null &&
                (cause.getMessage().toLowerCase().contains("connection reset by peer") ||
                        cause.getMessage().toLowerCase().contains("broken pipe") ||
                        cause.getMessage().toLowerCase().contains("forcibly closed"))) {
            logger.warn("DST Exception Caught (likely target server disconnect) for ClientID: " + clientId + ", Reason: " + cause.getMessage());
        } else if (cause instanceof java.net.ConnectException) {
            logger.error("DST Connection Exception Caught for ClientID: " + clientId, ", Reason: " + cause.getMessage());
        } else {
            logger.error("DST Unhandled Exception Caught for ClientID: " + clientId, cause);
        }
        ctx.close();
    }
}
package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.UUID;

public class PluginExecutor implements Executor {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final Gson gson;


    private final SocketController socketController;

    public PluginExecutor(PluginBuilder pluginBuilder, SocketController socketController) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(PluginExecutor.class.getName(), CLogger.Level.Info);
        this.socketController = socketController;
        gson = new Gson();

    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {
        logger.debug("Processing Exec message : " + incoming.getParams());

        if(incoming.getParams().containsKey("action")) {
            switch (incoming.getParam("action")) {

                case "configsrctunnel":
                    return createSrcTunnel(incoming);
                case "configdsttunnel":
                    return createDstTunnel(incoming);
                case "configdstsession":
                    return createDstSession(incoming);
                case "srcportcheck":
                    return srcPortCheck(incoming);
                case "dstportcheck":
                    return dstPortCheck(incoming);
                case "closesrcclient":
                    return closeSrcClient(incoming);
                case "closedstclient":
                    return closeDstClient(incoming);

                default:
                    logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                    logger.error(incoming.getParams().toString());
                    break;

            }
        }
        return null;

    }
    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {

        logger.debug("Processing Exec message : " + incoming.getParams());

        if(incoming.getParams().containsKey("action")) {
            switch (incoming.getParam("action")) {

                case "tunnelhealthcheck":
                    return tunnelHealthCheck(incoming);

                default:
                    logger.error("Unknown configtype found {} for {}:", incoming.getParam("action"), incoming.getMsgType().toString());
                    logger.error(incoming.getParams().toString());
                    break;

            }
        }
        return null;

    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) { return null; }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) { return null; }

    private MsgEvent createSrcTunnel(MsgEvent incoming) {

        logger.debug("createSrcTunnel INCOMING: " + incoming.getParams());
        // set state

        try {

            if ((incoming.getParam("action_src_port") != null) && (incoming.getParam("action_dst_host") != null) &&
                    (incoming.getParam("action_dst_port") != null) && (incoming.getParam("action_dst_region") != null) &&
                    (incoming.getParam("action_dst_agent") != null) && (incoming.getParam("action_dst_plugin") != null)) {

                int srcPort = Integer.parseInt(incoming.getParam("action_src_port"));
                String dstHost = incoming.getParam("action_dst_host");
                int dstPort = Integer.parseInt(incoming.getParam("action_dst_port"));
                String dstRegion = incoming.getParam("action_dst_region");
                String dstAgent = incoming.getParam("action_dst_agent");
                String dstPlugin = incoming.getParam("action_dst_plugin");

                int bufferSize = 65536;
                if(incoming.getParam("action_buffer_size") != null) {
                    bufferSize = Integer.parseInt(incoming.getParam("action_buffer_size"));
                    logger.debug("custom buffer_size: " + bufferSize);
                }

                int watchDogTimeout = 5000;
                if(incoming.getParam("action_watchdog_timeout") != null) {
                    bufferSize = Integer.parseInt(incoming.getParam("action_watchdog_timeout"));
                    logger.debug("custom watchdog timeout: " + watchDogTimeout);
                }
                String sTunnelId = UUID.randomUUID().toString();
                if(incoming.getParam("action_stunnel_id") != null) {
                    sTunnelId = incoming.getParam("action_stunnel_id");
                    logger.debug("custom sTunnelId: " + sTunnelId);
                }

                if(isSrcPortFree(srcPort)) {

                    logger.error("(1): local port is free");
                    sTunnelId = socketController.createSrcTunnel(sTunnelId, srcPort, dstHost, dstPort, dstRegion, dstAgent, dstPlugin, bufferSize, watchDogTimeout);
                    if(sTunnelId != null) {
                        // set state
                        incoming.setParam("status", "10");
                        incoming.setParam("status_desc", "tunnel created");
                        incoming.setParam("stunnel_id", sTunnelId);
                    } else {
                        // set state
                        incoming.setParam("status", "9");
                        incoming.setParam("status_desc", "unable to create tunnel");
                    }
                } else {
                    //set state
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "requested src port already bound");
                }

            } else {
                // set state
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent createDstTunnel(MsgEvent incoming) {

        //logger.info("createDstTunnel INCOMING: " + incoming.getParams());
        try {

            if (incoming.getParam("action_tunnel_config") != null) {

                Map<String,String> tunnelConfig = gson.fromJson(incoming.getParam("action_tunnel_config"),socketController.mapType);
                String dstHost = tunnelConfig.get("dst_host");
                int dstPort = Integer.parseInt(tunnelConfig.get("dst_port"));

                //if(isDstPortListening(dstHost, dstPort)) {

                    tunnelConfig = socketController.createDstTunnel(tunnelConfig);

                    if (tunnelConfig != null) {
                        incoming.setParam("action_tunnel_config", gson.toJson(tunnelConfig));
                        incoming.setParam("status", "10");
                        incoming.setParam("status_desc", "dst tunnel config created");
                    } else {
                        incoming.setParam("status", "9");
                        incoming.setParam("status_desc", "unable to create dst tunnel config");
                    }
                /*
                } else {
                    incoming.setParam("status", "8");
                    incoming.setParam("status_desc", "isDstPortListening == false");
                }

                 */
            } else {
                incoming.setParam("status", "7");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "6");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent createDstSession(MsgEvent incoming) {

        //logger.info("createDstTunnel INCOMING: " + incoming.getParams());
        try {

            if ((incoming.getParam("action_tunnel_id") != null) && (incoming.getParam("action_client_id") != null)) {

                String tunnelId = incoming.getParam("action_tunnel_id");
                String clientId = incoming.getParam("action_client_id");

                boolean createdSession = socketController.createDstSession(tunnelId, clientId);

                if (createdSession) {
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "dst session created");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "unable to create dst session");
                }

            } else {
                incoming.setParam("status", "7");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "6");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent dstPortCheck(MsgEvent incoming) {

        //logger.info("dstPortCheck INCOMING: " + incoming.getParams());
        try {

            if ((incoming.getParam("action_dst_host") != null) && (incoming.getParam("action_dst_port") != null)) {

                String dstHost = incoming.getParam("action_dst_host");
                int dstPort = Integer.parseInt(incoming.getParam("action_dst_port"));

                if(isDstPortListening(dstHost, dstPort)) {

                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "dst tunnel config created");

                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "requested dst port already bound");
                }

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent srcPortCheck(MsgEvent incoming) {

        //logger.info("dstPortCheck INCOMING: " + incoming.getParams());
        try {

            if (incoming.getParam("action_src_port") != null) {

                int srcPort = Integer.parseInt(incoming.getParam("action_src_port"));
                if(isSrcPortFree(srcPort)) {

                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "src port is free");

                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "requested src port already bound");
                }

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent closeSrcClient(MsgEvent incoming) {

        try {

            if (incoming.getParam("action_client_id") != null) {

                boolean closeClient = socketController.tunnelListener.closeClient(incoming.getParam("action_client_id"));
                if(closeClient) {
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "dst tunnel config created");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "unable to create dst tunnel config");
                }

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private MsgEvent closeDstClient(MsgEvent incoming) {

        try {

            if (incoming.getParam("action_client_id") != null) {


                boolean closeClient = socketController.tunnelSender.closeClient(incoming.getParam("action_client_id"));
                if(closeClient) {
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "dst tunnel config created");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "unable to create dst tunnel config");
                }

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "missing required parameter(s)");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }

    private boolean isSrcPortFree(int port) {

        boolean isFree = false;

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            isFree = true;


        } catch (IOException ignored) {
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }
        return isFree;
    }

    private boolean isDstPortListening(String node, int port) {

        boolean isListening = false;

        int timeout = 2;

        Socket s = null;
        String reason = null ;
        try {
            s = new Socket();
            s.setReuseAddress(true);
            SocketAddress sa = new InetSocketAddress(node, port);
            s.connect(sa, timeout * 1000);
        } catch (IOException e) {
            if ( e.getMessage().equals("Connection refused")) {
                reason = "port " + port + " on " + node + " is closed.";
            }
            if ( e instanceof UnknownHostException ) {
                reason = "node " + node + " is unresolved.";
            }
            if ( e instanceof SocketTimeoutException ) {
                reason = "timeout while attempting to reach node " + node + " on port " + port;
            }
        } finally {
            if (s != null) {
                if ( s.isConnected()) {
                    isListening = true;
                    logger.debug("Port " + port + " on " + node + " is reachable!");
                } else {
                    logger.error("Port " + port + " on " + node + " is not reachable; reason: " + reason );
                }
                try {
                    s.close();
                } catch (IOException e) {
                }
            }
        }

        return isListening;
    }

    public MsgEvent tunnelHealthCheck(MsgEvent incoming) {

        try {

            //String stunnelId = incoming.getParam("action_stunnel_id");
            incoming.setParam("status", "10");
            incoming.setParam("status_desc", "tunnel health check ok.");

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return incoming;
    }

}
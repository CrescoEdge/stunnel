package io.cresco.stunnel;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ExecutorImpl implements Executor {

    private PluginBuilder plugin;
    private CLogger logger;
    private Gson gson;


    private SocketController socketController;

    public ExecutorImpl(PluginBuilder pluginBuilder, SocketController socketController) {
        this.plugin = pluginBuilder;
        logger = plugin.getLogger(ExecutorImpl.class.getName(), CLogger.Level.Info);
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
                int bufferSize = 8192;
                if(incoming.getParam("action_dst_agent") != null) {
                    bufferSize = Integer.parseInt(incoming.getParam("action_buffer_size"));
                    logger.error("custom buffer_size: " + bufferSize);
                }


                if(isSrcPortFree(srcPort)) {
                    logger.error("(1): local port is free");
                    String sTunnelId = socketController.createSrcTunnel(srcPort, dstHost, dstPort, dstRegion, dstAgent, dstPlugin, bufferSize);
                    if(sTunnelId != null) {
                        incoming.setParam("status", "10");
                        incoming.setParam("status_desc", "tunnel created");
                        incoming.setParam("stunnel_id", sTunnelId);
                    } else {
                        incoming.setParam("status", "9");
                        incoming.setParam("status_desc", "unable to create tunnel");
                    }
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

    private MsgEvent createDstTunnel(MsgEvent incoming) {

        //logger.info("createDstTunnel INCOMING: " + incoming.getParams());
        try {

            if (incoming.getParam("action_tunnel_config") != null) {

                Map<String,String> tunnelConfig = gson.fromJson(incoming.getParam("action_tunnel_config"),socketController.mapType);

                tunnelConfig = socketController.createDstTunnel(tunnelConfig);
                if(tunnelConfig != null) {
                    incoming.setParam("action_tunnel_config",gson.toJson(tunnelConfig));
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

    private MsgEvent closeSrcClient(MsgEvent incoming) {

        try {

            if (incoming.getParam("action_client_id") != null) {

                boolean closeClient = socketController.socketListener.closeClient(incoming.getParam("action_client_id"));
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


                boolean closeClient = socketController.socketSender.closeClient(incoming.getParam("action_client_id"));
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

        while (!isFree) {
            ServerSocket ss = null;
            DatagramSocket ds = null;
            try {
                ss = new ServerSocket(port);
                ss.setReuseAddress(true);
                ds = new DatagramSocket(port);
                ds.setReuseAddress(true);
                isFree = true;


            } catch (IOException e) {
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
            };
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

    public MsgEvent test(MsgEvent incoming) {

        try {

            String stunnelId = incoming.getParam("action_stunnel_id");
            logger.info("INCOMING TEST REQUEST: sTunnelId:" + stunnelId);

            TextMessage updateMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
            updateMessage.setText("SENDING TEST MESSAGE");
            updateMessage.setStringProperty("filerepo_name", stunnelId);
            updateMessage.setStringProperty("region_id", plugin.getRegion());
            updateMessage.setStringProperty("agent_id", plugin.getAgent());
            updateMessage.setStringProperty("plugin_id", plugin.getPluginID());
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, updateMessage);
            logger.info("OUTGOING TEST MESSAGE");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return incoming;
    }
    private MsgEvent listenSrc(MsgEvent incoming) {

        logger.info("listenSrc INCOMING: " + incoming);
        try {

            if ((incoming.getParam("action_stunnel_id") != null) && (incoming.getParam("action_stunnel_listen_port") != null)) {
                String sTunnelId = incoming.getParam("action_stunnel_id");
                int listenPort = Integer.parseInt(incoming.getParam("action_stunnel_listen_port"));
                //socketListener = new SocketListener(plugin, sTunnelId, listenPort);
                logger.info("listenSrc PRE-START");
                //new Thread(socketListener).start();
                logger.info("listenSrc POST-START");
                logger.info("listenSrc Started new listener");

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "no stunnel_id or stunnel_listen_port parameter");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "getFile error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }
    private MsgEvent listenDst(MsgEvent incoming) {

        logger.info("listenDst INCOMING: " + incoming);
        try {

            if ((incoming.getParam("action_stunnel_id") != null) && (incoming.getParam("action_stunnel_dst_port") != null) && (incoming.getParam("action_stunnel_dst_host") != null)) {
                String sTunnelId = incoming.getParam("action_stunnel_id");
                String sTunnelHost = incoming.getParam("action_stunnel_dst_host");
                int sTunnelDstPort = Integer.parseInt(incoming.getParam("action_stunnel_dst_port"));
                logger.info("listenDst PRE-START");
                //socketSender = new SocketSender(plugin, sTunnelId, sTunnelHost, sTunnelDstPort);
                //socketSender.listenDP();
                //socketSender.go();
                /*
                String sq = "t0-t0-t0-t0-t0";
                String queryString = "stunnel_name='" + sq + "'";
                getlist(queryString);
                */

                logger.info("listenDst POST-START");
                logger.info("listenDst Started new listener");

            } else {
                incoming.setParam("status", "8");
                incoming.setParam("status_desc", "no stunnel_id or stunnel_listen_port parameter");
            }
        } catch (Exception ex) {
            incoming.setParam("status", "7");
            incoming.setParam("status_desc", "getFile error " + ex.getMessage());
            ex.printStackTrace();
        }
        return incoming;
    }
    private void getlist(String queryString) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof BytesMessage) {
                        logger.info("WE GOT SOMETHING BYTES");
                        logger.info("querystring: " + queryString);
                        logger.info("stunnel: " + msg.getStringProperty("stunnel_name"));
                        Enumeration<?> prop_enum;
                        prop_enum = msg.getPropertyNames();
                        String prop;
                        while (prop_enum.hasMoreElements()) {
                            prop = (String) prop_enum.nextElement();
                            logger.info(prop);
                        }

                    }

                    if (msg instanceof StreamMessage) {
                        logger.info("WE GOT SOMETHING Stream");
                        logger.info("stunnel: " + msg.getStringProperty("stunnel_name"));
                    }

                    if (msg instanceof TextMessage) {
                        logger.info("WE GOT SOMETHING TEXT");
                        logger.info("stunnel: " + msg.getStringProperty("stunnel_name"));

                    }
                } catch(Exception ex) {

                    ex.printStackTrace();
                }
            }
        };

        //String queryString = "stunnel_name='" + sTunnelId + "' AND broadcast";
        //String queryString = "stunnel_name='" + "t0-t0-t0-t0-t0" + "'";

        String node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);
        logger.info("listenDP Listner: " + node_from_listner_id + " querystring:" + queryString);

    }

}
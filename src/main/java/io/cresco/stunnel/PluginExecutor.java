package io.cresco.stunnel;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.cresco.stunnel.state.SocketControllerSM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PluginExecutor implements Executor {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final Gson gson;
    private final SocketController socketController;

    public PluginExecutor(PluginBuilder pluginBuilder, SocketController socketController) {
        this.plugin = pluginBuilder;
        this.logger = plugin.getLogger(PluginExecutor.class.getName(), CLogger.Level.Info);
        this.socketController = socketController;
        this.gson = new Gson();
    }

    @Override
    public MsgEvent executeCONFIG(MsgEvent incoming) {
        logger.debug("Processing CONFIG message: Action = " + incoming.getParam("action"));

        if (incoming.getParams().containsKey("action")) {
            String action = incoming.getParam("action");
            try {
                switch (action) {
                    case "configsrctunnel":
                        return configSrcTunnel(incoming);
                    case "configdsttunnel":
                        return configDstTunnel(incoming);
                    case "configdstsession":
                        return configDstSession(incoming);
                    case "removesrctunnel":
                        return removeSrcTunnel(incoming);
                    case "removedsttunnel":
                        return removeDstTunnel(incoming);
                    default:
                        logger.error("Unknown/Unsupported CONFIG action: {}", action);
                        incoming.setParam("status", "99");
                        incoming.setParam("status_desc", "Unknown/Unsupported config action: " + action);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error processing CONFIG action '" + action + "': " + e.getMessage(), e);
                incoming.setParam("status", "500");
                incoming.setParam("status_desc", "Internal error processing action '" + action + "': " + e.getMessage());
            }
        } else {
            logger.error("CONFIG message received without 'action' parameter.");
            incoming.setParam("status", "400");
            incoming.setParam("status_desc", "Missing 'action' parameter in CONFIG message.");
        }
        return incoming;
    }

    @Override
    public MsgEvent executeEXEC(MsgEvent incoming) {
        logger.debug("Processing EXEC message: Action = " + incoming.getParam("action"));
        if (incoming.getParams().containsKey("action")) {
            String action = incoming.getParam("action");
            try {
                switch (action) {
                    case "listtunnels":
                        return listTunnels(incoming);
                    case "gettunnelstatus":
                        return getTunnelStatus(incoming);
                    case "gettunnelconfig":
                        return getTunnelConfig(incoming);
                    default:
                        logger.error("Unknown/Unsupported EXEC action: {}", action);
                        incoming.setParam("status", "99");
                        incoming.setParam("status_desc", "Unknown/Unsupported exec action: " + action);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error processing EXEC action '" + action + "': " + e.getMessage(), e);
                incoming.setParam("status", "500");
                incoming.setParam("status_desc", "Internal error processing action '" + action + "': " + e.getMessage());
            }
        } else {
            logger.error("EXEC message received without 'action' parameter.");
            incoming.setParam("status", "400");
            incoming.setParam("status_desc", "Missing 'action' parameter in EXEC message.");
        }
        return incoming;
    }

    private MsgEvent configSrcTunnel(MsgEvent incoming) {
        logger.info("Handling configsrctunnel request...");
        try {
            Map<String, String> tunnelConfig;
            if (incoming.getParam("action_tunnel_config") != null) {
                tunnelConfig = gson.fromJson(incoming.getParam("action_tunnel_config"), socketController.mapType);
            } else {
                logger.warn("configsrctunnel using individual parameters is deprecated. Use action_tunnel_config (JSON map).");
                tunnelConfig = new java.util.HashMap<>();
                tunnelConfig.put("stunnel_id", incoming.getParam("action_stunnel_id"));
                tunnelConfig.put("src_port", incoming.getParam("action_src_port"));
                tunnelConfig.put("dst_host", incoming.getParam("action_dst_host"));
                tunnelConfig.put("dst_port", incoming.getParam("action_dst_port"));
                tunnelConfig.put("dst_region", incoming.getParam("action_dst_region"));
                tunnelConfig.put("dst_agent", incoming.getParam("action_dst_agent"));
                tunnelConfig.put("dst_plugin", incoming.getParam("action_dst_plugin"));
                tunnelConfig.put("src_region", plugin.getRegion());
                tunnelConfig.put("src_agent", plugin.getAgent());
                tunnelConfig.put("src_plugin", plugin.getPluginID());
                if(incoming.getParam("action_buffer_size") != null) tunnelConfig.put("buffer_size", incoming.getParam("action_buffer_size"));
                if(incoming.getParam("action_watchdog_timeout") != null) tunnelConfig.put("watchdog_timeout", incoming.getParam("action_watchdog_timeout"));
                if(incoming.getParam("action_performance_report_rate") != null) tunnelConfig.put("performance_report_rate", incoming.getParam("action_performance_report_rate"));
            }

            // Delegate tunnel creation directly and exclusively to the SocketController
            String createdStunnelId = socketController.startSrcTunnel(tunnelConfig);

            if (createdStunnelId != null) {
                incoming.setParam("status", "10");
                incoming.setParam("status_desc", "SRC tunnel creation initiated successfully.");
                incoming.setParam("stunnel_id", createdStunnelId);
                incoming.setCompressedParam("stunnel_config", gson.toJson(tunnelConfig));
            } else {
                incoming.setParam("status", "9");
                incoming.setParam("status_desc", "Failed to create SRC tunnel (check logs for details).");
            }

        } catch (JsonSyntaxException e) {
            logger.error("Error parsing action_tunnel_config JSON for configsrctunnel", e);
            incoming.setParam("status", "400");
            incoming.setParam("status_desc", "Invalid JSON format in action_tunnel_config.");
        } catch (Exception e) {
            logger.error("Error during configsrctunnel processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent configDstTunnel(MsgEvent incoming) {
        logger.info("Handling configdsttunnel request...");
        try {
            if (incoming.getParam("action_tunnel_config") != null) {
                Map<String, String> tunnelConfig = gson.fromJson(incoming.getParam("action_tunnel_config"), socketController.mapType);
                Map<String, String> configured = socketController.createDstTunnel(tunnelConfig);

                if (configured != null) {
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "DST tunnel configured successfully.");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "Failed to configure DST tunnel (check logs).");
                }
            } else {
                logger.error("Missing 'action_tunnel_config' parameter for configdsttunnel.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_tunnel_config");
            }
        } catch (JsonSyntaxException e) {
            logger.error("Error parsing action_tunnel_config JSON for configdsttunnel", e);
            incoming.setParam("status", "400");
            incoming.setParam("status_desc", "Invalid JSON format in action_tunnel_config.");
        } catch (Exception e) {
            logger.error("Error during configdsttunnel processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent configDstSession(MsgEvent incoming) {
        logger.info("Handling configdstsession request...");
        try {
            if (incoming.getParam("action_session_config") != null) {
                Map<String, String> sessionConfig = gson.fromJson(incoming.getParam("action_session_config"), socketController.mapType);
                String stunnelId = sessionConfig.get("stunnel_id");
                String clientId = sessionConfig.get("client_id");

                if (stunnelId != null && clientId != null) {
                    boolean attemptStarted = socketController.createDstSession(stunnelId, clientId);

                    if (attemptStarted) {
                        incoming.setParam("status", "10");
                        incoming.setParam("status_desc", "DST session connection attempt initiated.");
                    } else {
                        incoming.setParam("status", "9");
                        incoming.setParam("status_desc", "Failed to initiate DST session connection attempt (e.g., config missing).");
                    }
                } else {
                    logger.error("Missing 'stunnel_id' or 'client_id' in action_session_config.");
                    incoming.setParam("status", "400");
                    incoming.setParam("status_desc", "Missing required parameters in action_session_config: stunnel_id or client_id");
                }
            } else {
                logger.error("Missing 'action_session_config' parameter for configdstsession.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_session_config");
            }
        } catch (JsonSyntaxException e) {
            logger.error("Error parsing action_session_config JSON for configdstsession", e);
            incoming.setParam("status", "400");
            incoming.setParam("status_desc", "Invalid JSON format in action_session_config.");
        } catch (Exception e) {
            logger.error("Error during configdstsession processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent removeSrcTunnel(MsgEvent incoming) {
        logger.info("Handling removesrctunnel request...");
        try {
            String stunnelId = incoming.getParam("action_stunnel_id");
            if (stunnelId != null) {
                socketController.removeSrcTunnel(stunnelId);
                incoming.setParam("status", "10");
                incoming.setParam("status_desc", "SRC tunnel removal initiated for " + stunnelId);
            } else {
                logger.error("Missing 'action_stunnel_id' for removesrctunnel.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_stunnel_id");
            }
        } catch (Exception e) {
            logger.error("Error during removesrctunnel processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent removeDstTunnel(MsgEvent incoming) {
        logger.info("Handling removedsttunnel request...");
        try {
            String stunnelId = incoming.getParam("action_stunnel_id");
            if (stunnelId != null) {
                socketController.removeDstTunnel(stunnelId);
                incoming.setParam("status", "10");
                incoming.setParam("status_desc", "DST tunnel removal initiated for " + stunnelId);
            } else {
                logger.error("Missing 'action_stunnel_id' for removedsttunnel.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_stunnel_id");
            }
        } catch (Exception e) {
            logger.error("Error during removedsttunnel processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent listTunnels(MsgEvent incoming) {
        logger.info("Handling listtunnels request...");
        try {
            Map<String, Map<String, String>> tunnels = socketController.getActiveTunnels();
            List<Map<String, String>> tunnelList = new ArrayList<>();

            for(String stunnelId : tunnels.keySet()) {
                Map<String, String> tunnelInfo = new HashMap<>();
                SocketControllerSM sm = socketController.getTunnelStateMachine(stunnelId);
                String status = (sm != null) ? sm.getState().name() : "UNKNOWN";
                tunnelInfo.put("stunnel_id", stunnelId);
                tunnelInfo.put("status", status);
                tunnelList.add(tunnelInfo);
            }

            incoming.setParam("tunnels", gson.toJson(tunnelList));
            incoming.setParam("status", "10");
            incoming.setParam("status_desc", "Successfully retrieved tunnel list.");

        } catch (Exception e) {
            logger.error("Error during listtunnels processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }


    private MsgEvent getTunnelStatus(MsgEvent incoming) {
        logger.info("Handling gettunnelstatus request...");
        try {
            String stunnelId = incoming.getParam("action_stunnel_id");
            if (stunnelId != null) {
                Map<String, String> tunnelConfig = socketController.getTunnelConfig(stunnelId);
                if (tunnelConfig != null) {
                    SocketControllerSM sm = socketController.getTunnelStateMachine(stunnelId);
                    String status = (sm != null) ? sm.getState().name() : "UNKNOWN";
                    incoming.setParam("stunnel_id", stunnelId);
                    incoming.setParam("tunnel_status", status);
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "Successfully retrieved tunnel status.");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "Tunnel config not found for " + stunnelId);
                }
            } else {
                logger.error("Missing 'action_stunnel_id' for gettunnelstatus.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_stunnel_id");
            }
        } catch (Exception e) {
            logger.error("Error during gettunnelstatus processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    private MsgEvent getTunnelConfig(MsgEvent incoming) {
        logger.info("Handling gettunnelconfig request...");
        try {
            String stunnelId = incoming.getParam("action_stunnel_id");
            if (stunnelId != null) {
                Map<String, String> tunnelConfig = socketController.getTunnelConfig(stunnelId);
                if (tunnelConfig != null) {
                    incoming.setParam("tunnel_config", gson.toJson(tunnelConfig));
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "Successfully retrieved tunnel configuration.");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "Tunnel config not found for " + stunnelId);
                }
            } else {
                logger.error("Missing 'action_stunnel_id' for gettunnelconfig.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_stunnel_id");
            }
        } catch (Exception e) {
            logger.error("Error during gettunnelconfig processing", e);
            incoming.setParam("status", "500");
            incoming.setParam("status_desc", "Internal error: " + e.getMessage());
        }
        return incoming;
    }

    @Override
    public MsgEvent executeDISCOVER(MsgEvent incoming) {
        logger.warn("Received unimplemented DISCOVER message.");
        return null;
    }
    @Override
    public MsgEvent executeERROR(MsgEvent incoming) {
        logger.error("Received ERROR message: " + incoming.getParams());
        return null;
    }
    @Override
    public MsgEvent executeINFO(MsgEvent incoming) {
        logger.info("Received INFO message: " + incoming.getParams());
        return null;
    }
    @Override
    public MsgEvent executeWATCHDOG(MsgEvent incoming) {
        logger.debug("Received WATCHDOG message.");
        return null;
    }
    @Override
    public MsgEvent executeKPI(MsgEvent incoming) {
        logger.debug("Received KPI message.");
        return null;
    }
}
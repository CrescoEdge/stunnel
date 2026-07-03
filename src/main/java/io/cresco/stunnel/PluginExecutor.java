package io.cresco.stunnel;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.cresco.library.capability.*;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CrescoCapabilities(namespace = "stunnel", target = "plugin",
        routingParams = {"region", "agent", "pluginid"},
        summary = "Secure TCP tunnel plugin: forwards a local TCP port across the Cresco fabric to a remote host:port via a src/dst tunnel pair, with health checks and live metrics.")
@CrescoActions({
    @CrescoAction(name = "configsrctunnel", type = "CONFIG",
        summary = "Create the source (listener) side of a TCP tunnel: opens a local port that forwards to a remote dst tunnel.",
        why = "First step to expose a remote TCP service locally. Pair with configdsttunnel on the remote agent.",
        params = @CrescoParam(name = "action_tunnel_config", required = true, compressed = true, type = "object",
                description = "compressed JSON: {stunnel_id, src_port, dst_host, dst_port, dst_region, dst_agent, dst_plugin, ...}"),
        returns = {
            @CrescoReturn(name = "status", description = "10 success / 9 fail"),
            @CrescoReturn(name = "stunnel_id", description = "the tunnel id"),
            @CrescoReturn(name = "stunnel_config", type = "object", compressed = true, description = "the resolved tunnel config")
        }),
    @CrescoAction(name = "configdsttunnel", type = "CONFIG",
        summary = "Configure the destination side of a TCP tunnel (the agent nearest the target host:port).",
        why = "Second step; the dst side actually connects to the target service when a session opens.",
        params = @CrescoParam(name = "action_tunnel_config", required = true, compressed = true, type = "object", description = "compressed JSON tunnel config"),
        returns = @CrescoReturn(name = "status", description = "10 success / 9 fail")),
    @CrescoAction(name = "configdstsession", type = "CONFIG",
        summary = "Open a destination-side session (client connection) for an existing dst tunnel.",
        why = "Internal per-connection setup; triggered when a client connects to the src listener.",
        params = @CrescoParam(name = "action_session_config", required = true, compressed = true, type = "object", description = "compressed JSON: {stunnel_id, client_id}"),
        returns = @CrescoReturn(name = "status", description = "10 success / 9 fail")),
    @CrescoAction(name = "removesrctunnel", type = "CONFIG",
        summary = "Tear down the source (listener) side of a tunnel.",
        why = "Use to stop exposing a tunnel locally.",
        params = @CrescoParam(name = "action_stunnel_id", required = true, description = "tunnel id"),
        returns = @CrescoReturn(name = "status", description = "10 success / 9 fail")),
    @CrescoAction(name = "removedsttunnel", type = "CONFIG",
        summary = "Tear down the destination side of a tunnel.",
        why = "Use to stop the remote end of a tunnel.",
        params = @CrescoParam(name = "action_stunnel_id", required = true, description = "tunnel id"),
        returns = @CrescoReturn(name = "status", description = "10 success / 9 fail")),
    @CrescoAction(name = "nettuning", type = "CONFIG",
        summary = "Apply fabric-wide network tuning (buffer/block sizes) to live tunnels.",
        why = "Pushed by the controller AutoTuner to adapt tunnel I/O sizing under load.",
        returns = @CrescoReturn(name = "status", description = "10 on success")),
    @CrescoAction(name = "tunnelhealthcheck",
        summary = "Check whether a tunnel config exists on this node.",
        why = "Use to verify a tunnel is configured before relying on it.",
        params = @CrescoParam(name = "action_stunnel_id", required = true, description = "tunnel id"),
        returns = @CrescoReturn(name = "status", description = "10 found / 9 not found")),
    @CrescoAction(name = "listtunnels",
        summary = "List all active tunnels on this node with their live status.",
        why = "Use to enumerate tunnels and their ACTIVE/RECOVERING/DOWN state.",
        returns = @CrescoReturn(name = "tunnels", type = "array", description = "JSON array of {stunnel_id, status}")),
    @CrescoAction(name = "gettunnelstatus",
        summary = "Get the live status of one tunnel (ACTIVE/RECOVERING/DOWN/UNKNOWN).",
        why = "Use to health-check a specific tunnel.",
        params = @CrescoParam(name = "action_stunnel_id", required = true, description = "tunnel id"),
        returns = {
            @CrescoReturn(name = "stunnel_id", description = "the tunnel id"),
            @CrescoReturn(name = "tunnel_status", description = "ACTIVE|RECOVERING|DOWN|UNKNOWN")
        }),
    @CrescoAction(name = "gettunnelconfig",
        summary = "Get the full configuration of one tunnel.",
        why = "Use to inspect a tunnel's src/dst wiring and tuning.",
        params = @CrescoParam(name = "action_stunnel_id", required = true, description = "tunnel id"),
        returns = @CrescoReturn(name = "tunnel_config", type = "object", description = "tunnel config JSON")),
    @CrescoAction(name = "getmetrics",
        summary = "Return live tunnel metrics (active tunnels/clients/targets) as MeasurementEngine gauges JSON.",
        why = "Standard cross-bundle metrics contract; folded into getmetricinventory.",
        returns = @CrescoReturn(name = "metrics", type = "object", description = "getAllMetrics() JSON")),
    @CrescoAction(name = "getcapabilities",
        summary = "Return this plugin's self-describing capability document (its message actions as LLM tool specs).",
        why = "Discovery: lets a client/LLM learn what this plugin can do and how to call it.",
        returns = @CrescoReturn(name = "capabilities", type = "object", description = "CapabilityDocument JSON"))
})
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
                    case "nettuning":
                        // fabric-wide buffer/block-size tuning pushed by the controller AutoTuner
                        socketController.applyNetTuning(incoming.getParams());
                        incoming.setParam("status", "10");
                        return incoming;
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
                    case "tunnelhealthcheck":
                        return tunnelHealthCheck(incoming);
                    case "listtunnels":
                        return listTunnels(incoming);
                    case "gettunnelstatus":
                        return getTunnelStatus(incoming);
                    case "gettunnelconfig":
                        return getTunnelConfig(incoming);
                    case "getmetrics":
                        // unified metrics inventory: return this plugin's live metrics as JSON
                        incoming.setParam("metrics", socketController.getMetricsJson());
                        incoming.setParam("status", "10");
                        return incoming;
                    case "getcapabilities":
                        return CapabilityResponder.respond(incoming, this);
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
                String status = socketController.getTunnelStatus(stunnelId);
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

    private MsgEvent tunnelHealthCheck(MsgEvent incoming) {
        logger.debug("Handling tunnelhealthcheck request...");
        try {
            String stunnelId = incoming.getParam("action_stunnel_id");
            if (stunnelId != null) {
                boolean isConfigured = socketController.getTunnelConfig(stunnelId) != null;
                if (isConfigured) {
                    incoming.setParam("status", "10");
                    incoming.setParam("status_desc", "Tunnel config found locally.");
                } else {
                    incoming.setParam("status", "9");
                    incoming.setParam("status_desc", "Tunnel config not found locally for " + stunnelId);
                }
            } else {
                logger.error("Missing 'action_stunnel_id' for tunnelhealthcheck.");
                incoming.setParam("status", "400");
                incoming.setParam("status_desc", "Missing required parameter: action_stunnel_id");
            }
        } catch (Exception e) {
            logger.error("Error during tunnelhealthcheck processing", e);
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
                    String status = socketController.getTunnelStatus(stunnelId);
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
package io.cresco.stunnel;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.CMetric;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.AdaptiveRecvByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import jakarta.jms.MapMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SocketController {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final Gson gson;
    public final Type mapType;

    // Netty specific components
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    // sized in the constructor: ONE thread here serialized every tunnel's health probe,
    // reconnect, and beacon behind each other, so one blocking probe to a slow peer stalled all
    private final ScheduledExecutorService scheduler;
    // DST-session-init RPCs are blocking; running them on the Netty event loop froze every other
    // client channel on that loop thread for the RPC timeout (and suppressed the health check's
    // traffic-based proof-of-life). SrcSessionHandler runs them here instead. The semaphore bounds
    // the parked threads a connect burst during a fabric stall can create; connects beyond the
    // bound fail fast (they would only time out anyway).
    private final ExecutorService dstInitExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "stunnel-dst-init");
        t.setDaemon(true);
        return t;
    });
    private final java.util.concurrent.Semaphore dstInitSlots;


    // Tunnel management (Thread-safe maps)
    private final Map<String, Map<String, String>> activeTunnelsConfig = new ConcurrentHashMap<>();
    private final Map<String, PerformanceMonitor> performanceMonitors = new ConcurrentHashMap<>(); // Key: stunnelId_[src|dst]
    private final Map<String, Channel> activeServerChannels = new ConcurrentHashMap<>(); // Map stunnel_id -> Server Channel (src)
    private final Map<String, Channel> activeClientChannels = new ConcurrentHashMap<>(); // Map client_id -> Client Channel (src)
    private final Map<String, Channel> activeTargetChannels = new ConcurrentHashMap<>(); // Map client_id -> Target Channel (dst)
    private final Map<String, ScheduledFuture<?>> activeHealthChecks = new ConcurrentHashMap<>();
    // ONE dataplane consumer per tunnel per direction (see TunnelDemux). Sessions register into
    // these by client_id — no per-session JMS consumer, so no ActiveMQSession.stop() storm and no
    // lock-across-JMS surface.
    private final Map<String, TunnelDemux> srcDemux = new ConcurrentHashMap<>(); // stunnel_id -> src demux
    private final Map<String, TunnelDemux> dstDemux = new ConcurrentHashMap<>(); // stunnel_id -> dst demux

    // NOTE: live tunnel status is derived from the real channel/config/health-check maps above
    // (see getTunnelStatus). The UMPLE-generated io.cresco.stunnel.state.SocketControllerSM models
    // the *intended* per-tunnel lifecycle (init/active/recovery/error/shutdown) but its guarded
    // transitions were never driven, so it only ever read pluginActive. Rather than ship a
    // half-wired FSM whose state silently diverges from reality, status is computed from ground
    // truth; SocketControllerSM.java is retained purely as the lifecycle reference model for a
    // future full FSM implementation (see docs — "wire the SM" is the effort-L alternative).

    // Live, dynamically-tunable I/O sizes (seeded from config). The controller's AutoTuner pushes a
    // 'nettuning' CONFIG message that updates these; NEW tunnels/sessions read the current value at
    // bootstrap, so buffer/block sizes track the fabric-wide tuning without a restart.
    private final java.util.concurrent.atomic.AtomicInteger socketBufferBytes = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger readChunkBytes = new java.util.concurrent.atomic.AtomicInteger();
    private final java.util.concurrent.atomic.AtomicInteger writeHighWaterBytes = new java.util.concurrent.atomic.AtomicInteger();

    // B-2 metrics unification: one plugin-wide MeasurementEngine exposing stunnel's live counters via
    // the standard getmetrics EXEC, so they fold into the controller's unified metric inventory.
    private final MeasurementEngine metricEngine;

    public SocketController(PluginBuilder plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.mapType = new TypeToken<Map<String, String>>() {}.getType();
        this.gson = new GsonBuilder().setPrettyPrinting().create();

        // seed the live tunables from static config (defaults preserve current behavior)
        this.socketBufferBytes.set(plugin.getConfig().getIntegerParam("stunnel_socket_buffer_bytes", 4 * 1024 * 1024));
        this.readChunkBytes.set(plugin.getConfig().getIntegerParam("stunnel_read_chunk_bytes", 256 * 1024));
        this.writeHighWaterBytes.set(plugin.getConfig().getIntegerParam("stunnel_write_high_water_bytes", 2 * 1024 * 1024));
        this.dstInitSlots = new java.util.concurrent.Semaphore(
                plugin.getConfig().getIntegerParam("stunnel_dst_init_max_concurrent", 64));
        this.scheduler = Executors.newScheduledThreadPool(
                plugin.getConfig().getIntegerParam("stunnel_scheduler_threads", 4), r -> {
                    Thread t = new Thread(r, "stunnel-scheduler");
                    t.setDaemon(true);
                    return t;
                });

        this.metricEngine = new MeasurementEngine(plugin);
        this.metricEngine.setGauge("stunnel.active.tunnels", "active SRC tunnel listeners", "stunnel", CMetric.MeasureClass.GAUGE_INT);
        this.metricEngine.setGauge("stunnel.active.clients", "active client channels", "stunnel", CMetric.MeasureClass.GAUGE_INT);
        this.metricEngine.setGauge("stunnel.active.targets", "active target channels", "stunnel", CMetric.MeasureClass.GAUGE_INT);

        // Initialize Netty Event Loop Groups
        this.bossGroup = new NioEventLoopGroup(1); // For accepting connections
        this.workerGroup = new NioEventLoopGroup(); // For handling I/O

        logger.info("SocketController initialized with Netty EventLoopGroups.");
        checkStartUpConfig(); // Check for persisted config on startup

        // EXISTENCE BEACON: push each configured tunnel's identity + status on the subscribable
        // stunnel_trace stream every few seconds, so a subscriber (dashboard) knows a tunnel EXISTS —
        // active or not — even with zero traffic. Traffic traces (hops + throughput) come separately
        // from the per-session PerformanceMonitor.
        long beaconMs = plugin.getConfig().getLongParam("stunnel_beacon_ms", 3000L);
        scheduler.scheduleAtFixedRate(this::publishTunnelBeacons, beaconMs, beaconMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    }

    /** Push one status beacon per SRC-configured tunnel on the subscribable stunnel_trace stream. */
    private void publishTunnelBeacons() {
        try {
            for (Map.Entry<String, Map<String, String>> e : activeTunnelsConfig.entrySet()) {
                Map<String, String> cfg = e.getValue();
                if (cfg == null || !isSrcConfig(cfg)) continue;   // beacon from the SRC side only
                String sid = e.getKey();
                boolean listening = activeServerChannels.containsKey(sid)
                        && activeServerChannels.get(sid) != null && activeServerChannels.get(sid).isActive();
                Map<String, String> b = new java.util.HashMap<>();
                b.put("stunnel_id", sid);
                b.put("type", "tunnel");
                b.put("status", listening ? "ACTIVE" : "INACTIVE");
                b.put("src_region", cfg.get("src_region")); b.put("src_agent", cfg.get("src_agent"));
                b.put("dst_region", cfg.get("dst_region")); b.put("dst_agent", cfg.get("dst_agent"));
                b.put("src_port", cfg.get("src_port"));
                b.put("dst_host", cfg.get("dst_host")); b.put("dst_port", cfg.get("dst_port"));
                b.put("clients", String.valueOf(activeClientChannels.size()));
                jakarta.jms.TextMessage tm = plugin.getAgentService().getDataPlaneService().createTextMessage();
                tm.setStringProperty("stunnel_id", sid);
                tm.setStringProperty("type", "tunnel");
                tm.setStringProperty("cresco_msg_type", "stunnel_trace");
                tm.setText(new com.google.gson.Gson().toJson(b));
                plugin.getAgentService().getDataPlaneService().sendMessage(
                        io.cresco.library.data.TopicType.GLOBAL, tm,
                        jakarta.jms.DeliveryMode.NON_PERSISTENT, 0, (int) (beaconMsTtl()));
            }
        } catch (Exception ex) {
            logger.debug("publishTunnelBeacons error: " + ex.getMessage());
        }
    }
    private long beaconMsTtl() { return plugin.getConfig().getLongParam("stunnel_beacon_ms", 3000L) * 4; }

    // --- Tunnel Configuration Persistence ---

    private Path getTunnelConfigPath(String stunnelId) {
        Path pluginDataDir = Paths.get(plugin.getPluginDataDirectory());
        try {
            if (!Files.exists(pluginDataDir)) {
                Files.createDirectories(pluginDataDir);
            }
        } catch (IOException e) {
            logger.error("Failed to create plugin data directory: " + pluginDataDir, e);
            return null;
        }
        return pluginDataDir.resolve(stunnelId + "_tunnel_config.json");
    }


    private Map<String, String> getSavedTunnelConfig(String stunnelId) {
        Map<String, String> savedTunnelConfig = null;
        Path configPath = getTunnelConfigPath(stunnelId);
        if (configPath == null) return null;

        if (Files.exists(configPath) && !Files.isDirectory(configPath)) {
            logger.debug("Loading tunnel config: " + configPath);
            try (BufferedReader reader = Files.newBufferedReader(configPath)) {
                savedTunnelConfig = gson.fromJson(reader, mapType);
            } catch (Exception e) {
                logger.error("Error loading saved tunnel config for " + stunnelId + " from " + configPath, e);
            }
        }
        return savedTunnelConfig;
    }

    private void saveTunnelConfig(Map<String, String> tunnelConfig) {
        String stunnelId = tunnelConfig.get("stunnel_id");
        if (stunnelId == null || stunnelId.trim().isEmpty()) {
            logger.error("Cannot save tunnel config: stunnel_id is missing or empty.");
            return;
        }
        Path configPath = getTunnelConfigPath(stunnelId);
        if (configPath == null) return;

        try {
            logger.info("Saving tunnel config: " + configPath);
            try (BufferedWriter writer = Files.newBufferedWriter(configPath)) {
                gson.toJson(tunnelConfig, writer);
            }
        } catch (Exception e) {
            logger.error("Error saving tunnel config for " + stunnelId + " to " + configPath, e);
        }
    }

    private void deleteTunnelConfig(String stunnelId) {
        Path configPath = getTunnelConfigPath(stunnelId);
        if (configPath == null) return;

        try {
            if(Files.deleteIfExists(configPath)) {
                logger.info("Deleted saved tunnel config: " + configPath);
            }
        } catch (IOException e) {
            logger.error("Error deleting saved tunnel config: " + configPath, e);
        }
    }

    private void checkStartUpConfig() {
        new Thread(() -> {
            logger.info("Checking startup config in directory: " + plugin.getPluginDataDirectory());
            Path pluginDataDir = Paths.get(plugin.getPluginDataDirectory());
            if (!Files.isDirectory(pluginDataDir)) {
                logger.info("Plugin data directory does not exist or is not a directory.");
                return;
            }

            try (Stream<Path> stream = Files.list(pluginDataDir)) {
                List<Path> configFiles = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.getFileName().toString().endsWith("_tunnel_config.json"))
                        .collect(Collectors.toList());

                if (configFiles.isEmpty()) {
                    logger.info("No startup tunnel config files found.");
                    return;
                }

                for (Path configFile : configFiles) {
                    String fileName = configFile.getFileName().toString();
                    String stunnelId = fileName.substring(0, fileName.indexOf("_tunnel_config.json"));
                    logger.info("Found potential startup config for stunnel_id: " + stunnelId);
                    Map<String, String> candidateConfig = getSavedTunnelConfig(stunnelId);

                    if (candidateConfig != null) {
                        if (validateTunnelConfig(candidateConfig)) {
                            logger.info("Valid startup config found for " + stunnelId + ". Attempting to recreate tunnel...");
                            if (isSrcConfig(candidateConfig)) {
                                scheduler.schedule(new ReconnectTask(stunnelId), 1, TimeUnit.SECONDS);
                            } else {
                                logger.warn("Startup config for " + stunnelId + " appears to be for DST side. Cannot auto-start.");
                            }
                        } else {
                            logger.warn("Found startup config for " + stunnelId + " but it's invalid. Deleting.");
                            deleteTunnelConfig(stunnelId);
                        }
                    }
                }
            } catch (IOException e) {
                logger.error("Error scanning startup config directory", e);
            } catch (Exception e) {
                logger.error("Unexpected error during startup config check", e);
            }
        }, "stunnel-startup-config-checker").start();
    }

    private boolean isSrcConfig(Map<String, String> config) {
        return config.containsKey("src_port") &&
                config.containsKey("dst_region") &&
                config.containsKey("dst_agent") &&
                config.containsKey("dst_plugin") &&
                plugin.getRegion().equals(config.get("src_region")) &&
                plugin.getAgent().equals(config.get("src_agent")) &&
                plugin.getPluginID().equals(config.get("src_plugin"));
    }


    private boolean validateTunnelConfig(Map<String, String> config) {
        if (config == null) return false;
        List<String> requiredKeys = Arrays.asList(
                "stunnel_id", "src_port", "dst_host", "dst_port",
                "dst_region", "dst_agent", "dst_plugin", "src_region",
                "src_agent", "src_plugin"
        );
        for (String key : requiredKeys) {
            if (!config.containsKey(key) || config.get(key) == null || config.get(key).trim().isEmpty()) {
                logger.error("Tunnel config validation failed: Missing or empty key '" + key + "'");
                return false;
            }
        }
        try {
            Integer.parseInt(config.get("src_port"));
            Integer.parseInt(config.get("dst_port"));
            if (config.containsKey("buffer_size")) Integer.parseInt(config.get("buffer_size"));
            if (config.containsKey("watchdog_timeout")) Integer.parseInt(config.get("watchdog_timeout"));
            if (config.containsKey("performance_report_rate")) Integer.parseInt(config.get("performance_report_rate"));
        } catch (NumberFormatException e) {
            logger.error("Tunnel config validation failed: Port or other numeric value is not a valid integer.", e);
            return false;
        }
        return true;
    }


    // --- Netty Tunnel Creation ---

    // Resolve the io.cresco.stunnel plugin id on an agent via the global controller's
    // registration state (reliable, no per-agent RPC). Returns null if not found.
    private String resolveStunnelPlugin(String dstRegion, String dstAgent) {
        try {
            MsgEvent req = plugin.getGlobalControllerMsgEvent(MsgEvent.Type.EXEC);
            req.setParam("action", "listplugins");
            req.setParam("action_region", dstRegion);
            req.setParam("action_agent", dstAgent);
            MsgEvent resp = plugin.sendRPC(req);
            if (resp == null) return null;
            String listStr = resp.getCompressedParam("pluginslist");
            if (listStr == null) return null;
            Map<?, ?> parsed = gson.fromJson(listStr, Map.class);
            Object plugins = parsed.get("plugins");
            if (plugins instanceof List) {
                for (Object o : (List<?>) plugins) {
                    if (o instanceof Map) {
                        Map<?, ?> pm = (Map<?, ?>) o;
                        if ("io.cresco.stunnel".equals(pm.get("pluginname"))) {
                            Object name = pm.get("name");
                            if (name != null) return name.toString();
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to resolve dst stunnel plugin for " + dstRegion + "/" + dstAgent + ": " + e.getMessage());
        }
        return null;
    }

    public String startSrcTunnel(Map<String, String> tunnelConfig) {
        // Auto-resolve the destination stunnel plugin from the global controller when omitted,
        // so callers only need dst_region/dst_agent (no client-side plugin-id discovery).
        String dstPluginArg = tunnelConfig.get("dst_plugin");
        if ((dstPluginArg == null || dstPluginArg.isEmpty())
                && tunnelConfig.get("dst_region") != null && tunnelConfig.get("dst_agent") != null) {
            String resolved = resolveStunnelPlugin(tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"));
            if (resolved != null) {
                tunnelConfig.put("dst_plugin", resolved);
                logger.info("Resolved dst stunnel plugin for " + tunnelConfig.get("dst_region") + "/"
                        + tunnelConfig.get("dst_agent") + " -> " + resolved);
            }
        }
        if (!validateTunnelConfig(tunnelConfig)) {
            logger.error("Cannot create src tunnel: Invalid configuration provided.");
            return null;
        }

        String stunnelId = tunnelConfig.get("stunnel_id");
        if (activeServerChannels.containsKey(stunnelId)) {
            logger.warn("Src tunnel with ID " + stunnelId + " already exists. Ignoring request.");
            return stunnelId;
        }

        // Save the config first so the reconnect task can find it
        saveTunnelConfig(tunnelConfig);

        // THIS IS THE SYNCHRONOUS, CORRECTED INITIAL CREATION
        logger.info("Attempting to create SRC tunnel: " + stunnelId);
        logger.debug("Sending CONFIG message to setup DST tunnel: " + stunnelId);
        MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
        request.setParam("action", "configdsttunnel");
        request.setParam("action_tunnel_config", gson.toJson(tunnelConfig));
        MsgEvent response = plugin.sendRPC(request);

        if (response != null && "10".equals(response.getParam("status"))) {
            logger.info("DST tunnel setup successful for " + stunnelId + ". Starting SRC listener.");
            if (startSrcTunnelNettyInternal(tunnelConfig)) {
                return stunnelId;
            } else {
                logger.error("Failed to start Netty SRC listener for " + stunnelId + " after DST setup.");
                // Schedule a reconnect because the config is valid but the listener failed
                scheduler.schedule(new ReconnectTask(stunnelId), 5, TimeUnit.SECONDS);
                return null;
            }
        } else {
            logger.error("Failed to setup DST tunnel for " + stunnelId + ". Aborting SRC setup. Scheduling reconnect. Response: " + (response != null ? response.getParams() : "null"));
            // Schedule a reconnect because the config is valid but the destination is not ready
            scheduler.schedule(new ReconnectTask(stunnelId), 5, TimeUnit.SECONDS);
            return null;
        }
    }

    private boolean startSrcTunnelNettyInternal(Map<String, String> tunnelConfig) {
        String stunnelId = tunnelConfig.get("stunnel_id");
        int srcPort = Integer.parseInt(tunnelConfig.get("src_port"));

        try {
            PerformanceMonitor pm = createPerformanceMonitor(tunnelConfig, "src");
            if (pm == null) {
                logger.error("Failed to create Performance Monitor for SRC tunnel " + stunnelId);
                return false;
            }

            int sockBuf = socketBufferBytes.get();
            int readMax = readChunkBytes.get();
            int writeHigh = writeHighWaterBytes.get();

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new SrcChannelInitializer(this, plugin, tunnelConfig, pm))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // Enlarge per-read size so each ingress read -> one large broker message (default
                    // adaptive allocator caps reads at 64KB -> the slow small-message regime), and grow
                    // the socket buffers. All configurable; defaults preserve behavior on slow edges.
                    .childOption(ChannelOption.SO_RCVBUF, sockBuf)
                    .childOption(ChannelOption.SO_SNDBUF, sockBuf)
                    .childOption(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(2048, 65536, readMax))
                    .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(writeHigh / 2, writeHigh));

            ChannelFuture f = b.bind(srcPort).sync();
            Channel serverChannel = f.channel();

            if (!serverChannel.isActive()) {
                throw new IOException("Server channel is not active after binding to port " + srcPort);
            }

            TunnelDemux sDemux = new TunnelDemux(plugin, stunnelId, "src");
            if (!openDemuxBounded(sDemux, stunnelId, "src")) {
                serverChannel.close();
                throw new IOException("SRC demux open failed for tunnel " + stunnelId);
            }
            TunnelDemux oldSrc = srcDemux.put(stunnelId, sDemux);
            if (oldSrc != null) oldSrc.close();

            activeServerChannels.put(stunnelId, serverChannel);
            activeTunnelsConfig.put(stunnelId, tunnelConfig);

            logger.info("Netty Server (Src) started successfully on port " + srcPort + " for tunnel " + stunnelId);

            startHealthCheck(stunnelId, tunnelConfig, serverChannel);

            serverChannel.closeFuture().addListener(future -> {
                logger.warn("Netty Server (Src) channel for tunnel " + stunnelId + " has closed.");
                cleanupSrcTunnelResources(stunnelId);

                // **FIX**: The decision to reconnect is now based on the existence of the config file,
                // ensuring the tunnel recovers unless explicitly removed.
                if (getSavedTunnelConfig(stunnelId) != null) {
                    // Prevent scheduling if the plugin itself is being shut down completely
                    if(!scheduler.isShutdown()) {
                        logger.info("Tunnel config exists. Scheduling persistent reconnection for tunnel " + stunnelId);
                        scheduler.schedule(new ReconnectTask(stunnelId), 5, TimeUnit.SECONDS);
                    } else {
                        logger.warn("Scheduler is shutdown. Cannot reconnect tunnel " + stunnelId);
                    }
                } else {
                    logger.info("Tunnel config has been removed. Will not reconnect SRC tunnel " + stunnelId);
                }
            });

            return true;

        } catch (Exception e) {
            logger.error("Failed to start or bind Netty server (Src) to port " + srcPort + " for tunnel " + stunnelId, e);
            cleanupSrcTunnelResources(stunnelId);
            return false;
        }
    }

    private class ReconnectTask implements Runnable {
        private final String stunnelId;

        ReconnectTask(String stunnelId) {
            this.stunnelId = stunnelId;
        }

        @Override
        public void run() {
            // **FIX**: The check for plugin.isActive() is removed to ensure reconnection is always attempted.
            // The task will only abort if it's shutting down or the tunnel is already active.
            if (scheduler.isShutdown() || activeServerChannels.containsKey(stunnelId)) {
                if(scheduler.isShutdown()) logger.warn("ReconnectTask: Scheduler is shutdown, aborting reconnect for " + stunnelId);
                if(activeServerChannels.containsKey(stunnelId)) logger.warn("ReconnectTask: Tunnel already active, aborting reconnect for " + stunnelId);
                return;
            }

            logger.info("ReconnectTask: Attempting to re-establish tunnel " + stunnelId);
            Map<String, String> tunnelConfig = getSavedTunnelConfig(stunnelId);
            if (tunnelConfig == null) {
                logger.error("ReconnectTask: Could not find saved config for stunnel_id " + stunnelId + ". Aborting reconnect permanently.");
                return; // Stop retrying if config is gone
            }

            // Step 1: RELENTLESSLY RE-CONFIGURE THE DESTINATION.
            logger.info("ReconnectTask: Sending configuration to destination for " + stunnelId);
            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
            request.setParam("action", "configdsttunnel");
            request.setParam("action_tunnel_config", gson.toJson(tunnelConfig));
            MsgEvent response = plugin.sendRPC(request);

            // Step 2: If destination is configured, start the source. If not, TRY AGAIN.
            if (response != null && "10".equals(response.getParam("status"))) {
                logger.info("ReconnectTask: Destination for " + stunnelId + " configured successfully. Starting source listener.");
                if (startSrcTunnelNettyInternal(tunnelConfig)) {
                    logger.info("ReconnectTask: Tunnel " + stunnelId + " successfully re-established.");
                } else {
                    logger.error("ReconnectTask: Failed to start source listener for " + stunnelId + ". Retrying in 10 seconds.");
                    scheduler.schedule(this, 10, TimeUnit.SECONDS);
                }
            } else {
                logger.warn("ReconnectTask: Failed to re-configure destination tunnel for " + stunnelId + ". Retrying in 10 seconds. Response: " + (response != null ? response.getParams() : "null"));
                scheduler.schedule(this, 10, TimeUnit.SECONDS);
            }
        }
    }

    public Map<String, String> createDstTunnel(Map<String, String> tunnelConfig) {
        if (!validateTunnelConfig(tunnelConfig)) {
            logger.error("Cannot create dst tunnel: Invalid configuration provided.");
            return null;
        }
        String stunnelId = tunnelConfig.get("stunnel_id");
        // Clean up any old resources for this tunnel ID before creating a new one.
        cleanupDstTunnelResources(stunnelId);

        activeTunnelsConfig.put(stunnelId, tunnelConfig);
        if (createPerformanceMonitor(tunnelConfig, "dst") == null) {
            logger.error("Failed to create Performance Monitor for DST tunnel " + stunnelId);
            activeTunnelsConfig.remove(stunnelId);
            return null;
        }

        // One consumer for the whole tunnel, opened once here instead of once per client session.
        TunnelDemux demux = new TunnelDemux(plugin, stunnelId, "dst");
        if (!openDemuxBounded(demux, stunnelId, "dst")) {
            performanceMonitors.remove(stunnelId + "_dst");
            activeTunnelsConfig.remove(stunnelId);
            return null;
        }
        dstDemux.put(stunnelId, demux);

        logger.info("DST tunnel configured successfully for ID: " + stunnelId);
        return tunnelConfig;
    }

    /**
     * Open a tunnel demux with a bounded wait. demux.open() makes the tunnel's single JMS call, and
     * it runs on the plugin message thread (configdsttunnel, retried by ReconnectTask every 10s) —
     * an unbounded block there would park a message thread per attempt, the same failure shape as
     * the per-session attach. Fail the tunnel instead; the caller reports it and the retry loop
     * tries again later.
     */
    private boolean openDemuxBounded(TunnelDemux demux, String stunnelId, String direction) {
        long timeoutMs = plugin.getConfig().getLongParam("stunnel_demux_open_timeout_ms", 15000L);
        Future<?> f = dstInitExecutor.submit(() -> {
            try {
                demux.open();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        try {
            f.get(timeoutMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (Exception e) {
            f.cancel(true);
            demux.close();
            logger.error("Failed to open " + direction + " demux for tunnel " + stunnelId
                    + " within " + timeoutMs + "ms (dataplane may be unavailable) - " + e);
            return false;
        }
    }

    public boolean createDstSession(String stunnelId, String clientId) {
        Map<String, String> tunnelConfig = activeTunnelsConfig.get(stunnelId);
        if (tunnelConfig == null) {
            logger.error("Cannot create DST session for client " + clientId + ": Tunnel config not found for stunnel_id " + stunnelId);
            return false;
        }
        if (activeTargetChannels.containsKey(clientId)) {
            logger.warn("DST session for client " + clientId + " already exists or is connecting. Ignoring request.");
            return true;
        }

        String dstHost = tunnelConfig.get("dst_host");
        int dstPort = Integer.parseInt(tunnelConfig.get("dst_port"));
        PerformanceMonitor pm = performanceMonitors.get(stunnelId + "_dst");
        if (pm == null) {
            logger.error("Cannot create DST session for client " + clientId + ": PerformanceMonitor not found for stunnel_id " + stunnelId);
            return false;
        }

        logger.info("Attempting to create DST session for ClientID: " + clientId + " connecting to " + dstHost + ":" + dstPort);

        // Subscribe for this session's payload BEFORE initiating the connect (and before our RPC
        // reply releases the SRC to forward): the SRC's first bytes must never race the listener
        // attach. The relay buffers until the target channel activates.
        TunnelDemux demux = dstDemux.get(stunnelId);
        if (demux == null) {
            logger.error("Cannot create DST session for client " + clientId + ": no demux for stunnel_id " + stunnelId);
            return false;
        }
        // Buffer this client's payload until its target channel activates (the first-bytes race).
        // This is a map insert — no JMS call, so it cannot block, stall the shared dataplane
        // session, or deadlock the way the old per-session listener attach did.
        demux.expect(clientId);

        int sockBuf = socketBufferBytes.get();
        int readMax = readChunkBytes.get();
        int writeHigh = writeHighWaterBytes.get();

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new DstChannelInitializer(this, plugin, tunnelConfig, clientId, pm, demux))
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
                // Match the SRC side: large reads -> large broker messages, enlarged socket buffers.
                .option(ChannelOption.SO_RCVBUF, sockBuf)
                .option(ChannelOption.SO_SNDBUF, sockBuf)
                .option(ChannelOption.RCVBUF_ALLOCATOR, new AdaptiveRecvByteBufAllocator(2048, 65536, readMax))
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(writeHigh / 2, writeHigh));

        connectWithRetry(b, dstHost, dstPort, tunnelConfig, clientId, 3);

        return true;
    }

    /** The tunnel's DST demux, or null when the tunnel is not configured here. */
    public TunnelDemux getDstDemux(String stunnelId) {
        return dstDemux.get(stunnelId);
    }

    /** The tunnel's SRC demux, or null when this node does not host the listener. */
    public TunnelDemux getSrcDemux(String stunnelId) {
        return srcDemux.get(stunnelId);
    }

    private void connectWithRetry(Bootstrap bootstrap, String host, int port, Map<String, String> tunnelConfig, String clientId, int retriesLeft) {
        if (retriesLeft <= 0) {
            logger.error("Netty Client (Dst) connection FAILED for ClientID: " + clientId + " to " + host + ":" + port + " after multiple retries.");
            TunnelDemux d = dstDemux.get(tunnelConfig.get("stunnel_id"));
            if (d != null) {
                d.discard(clientId);
            }
            sendDstSessionFailedStatus(tunnelConfig, clientId, new ConnectException("Connection timed out after retries"));
            return;
        }

        bootstrap.connect(host, port).addListener((ChannelFuture future) -> {
            if (future.isSuccess()) {
                logger.info("Netty Client (Dst) connection successful for ClientID: " + clientId + " to " + host + ":" + port);
            } else {
                logger.warn("Netty Client (Dst) connection attempt failed for ClientID: " + clientId + ". Retries left: " + (retriesLeft - 1), future.cause());
                scheduler.schedule(() -> connectWithRetry(bootstrap, host, port, tunnelConfig, clientId, retriesLeft - 1), 5, TimeUnit.SECONDS);
            }
        });
    }

    private void sendDstSessionFailedStatus(Map<String,String> tunnelConfig, String clientId, Throwable cause) {
        try {
            MapMessage statusMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
            statusMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
            statusMessage.setStringProperty("direction", "src");
            statusMessage.setStringProperty("client_id", clientId);
            statusMessage.setInt("status", 9);
            statusMessage.setString("error", "Failed to connect to target server: " + (cause != null ? cause.getMessage() : "Unknown reason"));
            plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, statusMessage);
            logger.debug("Sent DST session connection failed status (9) to SRC for ClientID: " + clientId);
        } catch (Exception e) {
            logger.error("Failed to send DST session failed status to SRC for ClientID: " + clientId, e);
        }
    }


    // --- Health Check Management ---

    private void startHealthCheck(String stunnelId, Map<String, String> tunnelConfig, Channel serverChannel) {
        AtomicInteger consecutiveFailures = new AtomicInteger(0);
        int failureThreshold = plugin.getConfig().getIntegerParam("stunnel_health_failure_threshold", 3);
        long healthCheckInterval = plugin.getConfig().getLongParam("stunnel_health_check_interval_sec", 5L);
        // Probe budget: a probe that outlives the check interval parks the shared scheduler thread,
        // so overdue probes fire back-to-back into a stalled fabric and a probe sent BEFORE a
        // control-plane recovery decides the tunnel's fate AFTER it (the 2026-08-15 drop cascade).
        // A short budget keeps probes sampling the current fabric state. The per-tunnel
        // watchdog_timeout config key (ms) overrides the plugin-wide default.
        long probeTimeoutMs = plugin.getConfig().getLongParam("stunnel_health_probe_timeout_ms", 10000L);
        if (tunnelConfig.containsKey("watchdog_timeout")) {
            try {
                probeTimeoutMs = Long.parseLong(tunnelConfig.get("watchdog_timeout"));
            } catch (NumberFormatException e) {
                logger.warn("Invalid watchdog_timeout '" + tunnelConfig.get("watchdog_timeout")
                        + "' for tunnel " + stunnelId + ", using default: " + probeTimeoutMs);
            }
        }
        final long probeTimeout = probeTimeoutMs;

        Runnable healthCheckTask = () -> {
            try {
                if (!serverChannel.isOpen()) {
                    stopHealthCheck(stunnelId);
                    return;
                }

                PerformanceMonitor pm = performanceMonitors.get(stunnelId + "_src");
                boolean recentlyActive = false;
                if (pm != null) {
                    long lastActivity = pm.getLastActivityTimeMs();
                    if (lastActivity > 0) { // Ensure we don't check against the initial '0' value
                        long idleTime = System.currentTimeMillis() - lastActivity;
                        // If active within the last 2 health check intervals, consider it healthy.
                        if (idleTime < (healthCheckInterval * 1000 * 2)) {
                            recentlyActive = true;
                        }
                    }
                }

                // If data is flowing, the tunnel is healthy. Skip the active probe.
                if (recentlyActive) {
                    logger.debug("Tunnel " + stunnelId + " is actively transferring data. Skipping active health check.");
                    consecutiveFailures.set(0); // Reset failures because we have proof of life
                    return; // End the task for this cycle
                }

                // If the tunnel is idle, proceed with the active network probe.
                logger.debug("Tunnel " + stunnelId + " is idle. Performing active health check.");
                MsgEvent response = sendTunnelHealthProbe(stunnelId, tunnelConfig, probeTimeout);

                if (response != null && "10".equals(response.getParam("status"))) {
                    consecutiveFailures.set(0);
                    logger.debug("Health check successful for tunnel: " + stunnelId);
                } else {
                    int failures = consecutiveFailures.incrementAndGet();
                    logger.warn("Health check failed for tunnel: " + stunnelId + ". Consecutive failures: " + failures);
                    if (failures >= failureThreshold) {
                        // A probe interrupted by cancel(true) surfaces as a null response (the RPC
                        // layer swallows the interrupt), so re-check liveness before spending
                        // another probe budget or closing on behalf of a cancelled check.
                        if (scheduler.isShutdown() || !serverChannel.isOpen() || !activeHealthChecks.containsKey(stunnelId)) {
                            logger.info("Health check for tunnel " + stunnelId + " cancelled at threshold; skipping closure.");
                            return;
                        }
                        // Teardown kills every live client session on the tunnel, so require one
                        // fresh probe to fail too: the counted failures may all be probes that were
                        // sent into a stall that has since cleared.
                        logger.warn("Health check failure threshold reached for tunnel: " + stunnelId + ". Sending verification probe before closure.");
                        MsgEvent verify = sendTunnelHealthProbe(stunnelId, tunnelConfig, probeTimeout);
                        if (verify != null && "10".equals(verify.getParam("status"))) {
                            consecutiveFailures.set(0);
                            logger.info("Verification probe succeeded for tunnel: " + stunnelId + ". Cancelling closure.");
                        } else if (!activeHealthChecks.containsKey(stunnelId)) {
                            logger.info("Health check for tunnel " + stunnelId + " cancelled during verification; skipping closure.");
                        } else {
                            logger.error("Health check failure threshold reached for tunnel: " + stunnelId + ". Forcing tunnel closure and rebuild.");
                            serverChannel.close(); // This will trigger the closeFuture listener to rebuild the tunnel
                            stopHealthCheck(stunnelId);
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Exception during health check for tunnel: " + stunnelId, e);
                int failures = consecutiveFailures.incrementAndGet();
                if (failures >= failureThreshold) {
                    logger.error("Health check exception threshold reached for tunnel: " + stunnelId + ". Forcing tunnel closure and rebuild.");
                    serverChannel.close();
                    stopHealthCheck(stunnelId);
                }
            }
        };

        // Fixed DELAY, not fixed rate: a slow probe must not queue an overdue probe that then fires
        // back-to-back — each cycle should observe the fabric as it is now.
        ScheduledFuture<?> healthCheckFuture = scheduler.scheduleWithFixedDelay(healthCheckTask, healthCheckInterval, healthCheckInterval, TimeUnit.SECONDS);
        activeHealthChecks.put(stunnelId, healthCheckFuture);
        logger.info("Health check scheduled for tunnel " + stunnelId + " every " + healthCheckInterval
                + " seconds (probe timeout " + probeTimeout + " ms, failure threshold " + failureThreshold + ").");
    }

    private MsgEvent sendTunnelHealthProbe(String stunnelId, Map<String, String> tunnelConfig, long timeoutMs) {
        MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
        request.setParam("action", "tunnelhealthcheck");
        request.setParam("action_stunnel_id", stunnelId);
        return plugin.sendRPC(request, timeoutMs);
    }

    private void stopHealthCheck(String stunnelId) {
        ScheduledFuture<?> healthCheckFuture = activeHealthChecks.remove(stunnelId);
        if (healthCheckFuture != null) {
            healthCheckFuture.cancel(true);
            logger.info("Health check stopped for tunnel: " + stunnelId);
        }
    }


    // --- Performance Monitor Management ---

    private PerformanceMonitor createPerformanceMonitor(Map<String, String> tunnelConfig, String direction) {
        String stunnelId = tunnelConfig.get("stunnel_id");
        String monitorKey = stunnelId + "_" + direction;

        if (performanceMonitors.containsKey(monitorKey)) {
            logger.warn("PerformanceMonitor for " + monitorKey + " already exists.");
            return performanceMonitors.get(monitorKey);
        }

        int reportingIntervalMs = 5000;
        if (tunnelConfig.containsKey("performance_report_rate")) {
            try {
                reportingIntervalMs = Integer.parseInt(tunnelConfig.get("performance_report_rate"));
            } catch (NumberFormatException e) {
                logger.warn("Invalid performance_report_rate '" + tunnelConfig.get("performance_report_rate") + "', using default: " + reportingIntervalMs);
            }
        }

        String metricName = "cresco.stunnel.bytes.per.second." + (direction.equals("src") ? "ingress" : "egress");

        try {
            PerformanceMonitor pm = new PerformanceMonitor(plugin, tunnelConfig, direction, metricName, reportingIntervalMs);
            performanceMonitors.put(monitorKey, pm);
            logger.info("PerformanceMonitor created for " + monitorKey);
            return pm;
        } catch (Exception e) {
            logger.error("Failed to create PerformanceMonitor for " + monitorKey, e);
            return null;
        }
    }

    // --- Channel Management (Called by Handlers) ---

    public void addClientChannel(String clientId, Channel channel) {
        activeClientChannels.put(clientId, channel);
        logger.debug("Added active client channel: " + clientId + " (" + channel.remoteAddress() + ")");
    }

    public void removeClientChannel(String clientId) {
        if (activeClientChannels.remove(clientId) != null) {
            logger.debug("Removed active client channel: " + clientId);
        }
    }

    public void addTargetChannel(String clientId, Channel channel) {
        activeTargetChannels.put(clientId, channel);
        logger.debug("Added active target channel: " + clientId + " (" + channel.remoteAddress() + ")");
    }

    public void removeTargetChannel(String clientId) {
        if (activeTargetChannels.remove(clientId) != null) {
            logger.debug("Removed active target channel: " + clientId);
        }
    }


    // --- Tunnel Removal / Shutdown ---

    public void removeSrcTunnel(String stunnelId) {
        logger.info("Removing SRC tunnel: " + stunnelId);

        // Capture the dst coordinates before the config is deleted so we can cascade the
        // teardown to the paired DST tunnel (otherwise it is orphaned on the remote agent).
        Map<String, String> removedConfig = activeTunnelsConfig.get(stunnelId);

        // This is the crucial step that prevents reconnection.
        deleteTunnelConfig(stunnelId);

        stopHealthCheck(stunnelId);

        Channel serverChannel = activeServerChannels.get(stunnelId);
        if (serverChannel != null && serverChannel.isOpen()) {
            serverChannel.close(); // This will trigger the closeFuture listener, which will see the deleted config and stop.
        } else {
            // If channel is already closed, we still need to clean up resources
            cleanupSrcTunnelResources(stunnelId);
        }

        // Cascade: tell the paired DST tunnel to tear down too, so it is not left orphaned.
        cascadeRemoveDst(stunnelId, removedConfig);
    }

    private void cascadeRemoveDst(String stunnelId, Map<String, String> cfg) {
        if (cfg == null) return;
        String dstRegion = cfg.get("dst_region");
        String dstAgent = cfg.get("dst_agent");
        String dstPlugin = cfg.get("dst_plugin");
        if (dstRegion == null || dstAgent == null || dstPlugin == null) return;
        try {
            MsgEvent req = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, dstRegion, dstAgent, dstPlugin);
            req.setParam("action", "removedsttunnel");
            req.setParam("action_stunnel_id", stunnelId);
            plugin.msgOut(req);
            logger.info("Cascade teardown: sent removedsttunnel for " + stunnelId + " to " + dstRegion + "/" + dstAgent);
        } catch (Exception e) {
            logger.error("Cascade removedsttunnel failed for " + stunnelId + ": " + e.getMessage());
        }
    }

    private void cleanupSrcTunnelResources(String stunnelId) {
        stopHealthCheck(stunnelId);
        activeServerChannels.remove(stunnelId);

        List<String> clientsToClose = new ArrayList<>();
        activeClientChannels.forEach((clientId, channel) -> {
            String channelStunnelId = channel.attr(SrcChannelInitializer.STUNNEL_ID_KEY).get();
            if (stunnelId.equals(channelStunnelId)) {
                clientsToClose.add(clientId);
            }
        });

        if (!clientsToClose.isEmpty()) {
            logger.debug("Closing " + clientsToClose.size() + " client channels for SRC tunnel " + stunnelId);
            clientsToClose.forEach(clientId -> {
                Channel clientChannel = activeClientChannels.remove(clientId);
                if(clientChannel != null && clientChannel.isOpen()) {
                    clientChannel.close();
                }
            });
        }

        TunnelDemux sd = srcDemux.remove(stunnelId);
        if (sd != null) sd.close();

        activeTunnelsConfig.remove(stunnelId);
        PerformanceMonitor pm = performanceMonitors.remove(stunnelId + "_src");
        if (pm != null) {
            pm.shutdown();
        }
        logger.info("SRC tunnel resource cleanup complete for: " + stunnelId);
    }


    public void removeDstTunnel(String stunnelId) {
        logger.info("Removing DST tunnel configuration: " + stunnelId);
        cleanupDstTunnelResources(stunnelId);
    }

    private void cleanupDstTunnelResources(String stunnelId) {
        List<String> targetsToClose = new ArrayList<>();
        activeTargetChannels.forEach((clientId, channel) -> {
            String channelStunnelId = channel.attr(SrcChannelInitializer.STUNNEL_ID_KEY).get();
            if (stunnelId.equals(channelStunnelId)) {
                targetsToClose.add(clientId);
            }
        });

        if (!targetsToClose.isEmpty()) {
            logger.debug("Closing " + targetsToClose.size() + " target channels for DST tunnel " + stunnelId);
            targetsToClose.forEach(clientId -> {
                Channel targetChannel = activeTargetChannels.remove(clientId);
                if(targetChannel != null && targetChannel.isOpen()) {
                    targetChannel.close();
                }
            });
        }

        TunnelDemux dd = dstDemux.remove(stunnelId);
        if (dd != null) dd.close();

        activeTunnelsConfig.remove(stunnelId);
        PerformanceMonitor pm = performanceMonitors.remove(stunnelId + "_dst");
        if (pm != null) {
            pm.shutdown();
        }
        logger.info("DST tunnel resource cleanup complete for: " + stunnelId);
    }


    public void shutdown() {
        logger.info("Shutting down SocketController and all Netty components...");
        // Gracefully shutdown the scheduler to stop new reconnection tasks
        scheduler.shutdown();
        dstInitExecutor.shutdownNow();

        // Close all active server channels, which will trigger their cleanup listeners
        activeServerChannels.values().forEach(Channel::close);

        try {
            // Give time for cleanup and tasks to finish
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        logger.debug("Shutting down Netty EventLoopGroups...");
        try {
            if (bossGroup != null) {
                bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            }
            if (workerGroup != null) {
                workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).syncUninterruptibly();
            }
        } catch (Exception e) {
            logger.error("Error during Netty EventLoopGroup shutdown", e);
        } finally {
            bossGroup = null;
            workerGroup = null;
            logger.debug("Netty EventLoopGroups shutdown.");
        }

        // Clear any remaining in-memory state
        activeServerChannels.clear();
        activeClientChannels.clear();
        activeTargetChannels.clear();
        activeTunnelsConfig.clear();
        performanceMonitors.clear();
        srcDemux.values().forEach(TunnelDemux::close);
        srcDemux.clear();
        dstDemux.values().forEach(TunnelDemux::close);
        dstDemux.clear();

        logger.info("SocketController shutdown complete.");
    }

    /**
     * Apply a fabric-wide net-tuning update (from the controller AutoTuner's 'nettuning' CONFIG msg).
     * Updates the live socket-buffer / read-block / write-watermark sizes; NEW tunnels and sessions
     * read these at bootstrap, so buffer/block sizes track the tuning without a plugin restart.
     */
    public void applyNetTuning(Map<String, String> tuning) {
        try {
            if (tuning.containsKey("net_socket_buffer_bytes")) socketBufferBytes.set(Integer.parseInt(tuning.get("net_socket_buffer_bytes")));
            if (tuning.containsKey("net_read_chunk_bytes")) readChunkBytes.set(Integer.parseInt(tuning.get("net_read_chunk_bytes")));
            if (tuning.containsKey("net_write_high_water_bytes")) writeHighWaterBytes.set(Integer.parseInt(tuning.get("net_write_high_water_bytes")));
            logger.info("applyNetTuning: sockBuf=" + socketBufferBytes.get() + " readChunk="
                    + readChunkBytes.get() + " writeHi=" + writeHighWaterBytes.get());
        } catch (Exception ex) {
            logger.warn("applyNetTuning failed: " + ex.getMessage());
        }
    }

    /** Current stunnel metrics as grouped JSON, for the controller's unified metric inventory (getmetrics). */
    public String getMetricsJson() {
        try {
            metricEngine.updateIntGauge("stunnel.active.tunnels", activeServerChannels.size());
            metricEngine.updateIntGauge("stunnel.active.clients", activeClientChannels.size());
            metricEngine.updateIntGauge("stunnel.active.targets", activeTargetChannels.size());
            return gson.toJson(metricEngine.getAllMetrics());
        } catch (Exception ex) {
            logger.error("getMetricsJson", ex);
            return "{}";
        }
    }

    public Map<String, Map<String,String>> getActiveTunnels() {
        return new HashMap<>(activeTunnelsConfig);
    }

    public Map<String, String> getTunnelConfig(String stunnelId) {
        Map<String, String> config = activeTunnelsConfig.get(stunnelId);
        return (config != null) ? Collections.unmodifiableMap(config) : null;
    }

    /** Executor for blocking DST-session-init RPCs, keeping them off the Netty event loops. */
    public ExecutorService getDstInitExecutor() {
        return dstInitExecutor;
    }

    /** Bound on concurrent DST-init RPCs; false = at capacity, caller should fail the connect fast. */
    public boolean tryAcquireDstInitSlot() {
        return dstInitSlots.tryAcquire();
    }

    public void releaseDstInitSlot() {
        dstInitSlots.release();
    }

    /** RPC budget for the per-client configdstsession call (SRC connect -> DST target connect). */
    public long getDstInitTimeoutMs() {
        return plugin.getConfig().getLongParam("stunnel_dst_init_timeout_ms", 10000L);
    }

    /**
     * Real tunnel status derived from live controller state (not the decorative SocketControllerSM,
     * which is never advanced). Ground truth comes from the actual tunnel maps:
     * <ul>
     *   <li>{@code UNKNOWN}  — no config for this id (never configured / already removed).</li>
     *   <li>{@code ACTIVE}   — SRC listener channel is open, or a DST responder is configured.</li>
     *   <li>{@code RECOVERING} — SRC config present, listener down, but a reconnect/health-check is
     *       still scheduled (the ReconnectTask relentlessly re-establishes it).</li>
     *   <li>{@code DOWN}     — SRC config present, listener down, nothing scheduled to recover it.</li>
     * </ul>
     * The channel state is authoritative: an open listener always reads ACTIVE. RECOVERING vs DOWN
     * is distinguished from real scheduler state so callers see a truthful lifecycle rather than a
     * single static value. (Full FSM lifecycle reporting is the effort-L SocketControllerSM path.)
     */
    public String getTunnelStatus(String stunnelId) {
        Map<String, String> config = activeTunnelsConfig.get(stunnelId);
        if (config == null) {
            return "UNKNOWN";
        }
        if (isSrcConfig(config)) {
            Channel ch = activeServerChannels.get(stunnelId);
            if (ch != null && ch.isActive()) {
                return "ACTIVE";
            }
            // Listener is down. If a reconnect is pending (health check scheduled, or a saved config
            // the ReconnectTask will act on), the tunnel is actively recovering rather than dead.
            boolean recovering = activeHealthChecks.containsKey(stunnelId)
                    || (!scheduler.isShutdown() && getSavedTunnelConfig(stunnelId) != null);
            return recovering ? "RECOVERING" : "DOWN";
        }
        return "ACTIVE";
    }
}
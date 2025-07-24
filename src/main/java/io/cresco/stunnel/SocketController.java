package io.cresco.stunnel;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.cresco.stunnel.state.SocketControllerSM;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
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
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);


    // Tunnel management (Thread-safe maps)
    private final Map<String, Map<String, String>> activeTunnelsConfig = new ConcurrentHashMap<>();
    private final Map<String, PerformanceMonitor> performanceMonitors = new ConcurrentHashMap<>(); // Key: stunnelId_[src|dst]
    private final Map<String, Channel> activeServerChannels = new ConcurrentHashMap<>(); // Map stunnel_id -> Server Channel (src)
    private final Map<String, Channel> activeClientChannels = new ConcurrentHashMap<>(); // Map client_id -> Client Channel (src)
    private final Map<String, Channel> activeTargetChannels = new ConcurrentHashMap<>(); // Map client_id -> Target Channel (dst)
    private final Map<String, ScheduledFuture<?>> activeHealthChecks = new ConcurrentHashMap<>();


    // State machine for overall tunnel state. For a multi-tunnel environment, this
    // should be evolved into a Map<String, SocketControllerSM> to track each tunnel's state.
    private final Map<String, SocketControllerSM> tunnelStateMachines = new ConcurrentHashMap<>();


    public SocketController(PluginBuilder plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.mapType = new TypeToken<Map<String, String>>() {}.getType();
        this.gson = new GsonBuilder().setPrettyPrinting().create();

        // Initialize Netty Event Loop Groups
        this.bossGroup = new NioEventLoopGroup(1); // For accepting connections
        this.workerGroup = new NioEventLoopGroup(); // For handling I/O

        logger.info("SocketController initialized with Netty EventLoopGroups.");
        checkStartUpConfig(); // Check for persisted config on startup
    }

    // --- State Management ---
    public void stateNotify(String stunnelId, String node) {
        if(logger != null) {
            logger.info("Tunnel [" + stunnelId + "] State Change: " + node);
        }
    }

    public SocketControllerSM getTunnelStateMachine(String stunnelId) {
        return tunnelStateMachines.computeIfAbsent(stunnelId, k -> new SocketControllerSM());
    }


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

    public String startSrcTunnel(Map<String, String> tunnelConfig) {
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

            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new SrcChannelInitializer(this, plugin, tunnelConfig, pm))
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture f = b.bind(srcPort).sync();
            Channel serverChannel = f.channel();

            if (!serverChannel.isActive()) {
                throw new IOException("Server channel is not active after binding to port " + srcPort);
            }

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

        logger.info("DST tunnel configured successfully for ID: " + stunnelId);
        return tunnelConfig;
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

        Bootstrap b = new Bootstrap();
        b.group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new DstChannelInitializer(this, plugin, tunnelConfig, clientId, pm))
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);

        connectWithRetry(b, dstHost, dstPort, tunnelConfig, clientId, 3);

        return true;
    }

    private void connectWithRetry(Bootstrap bootstrap, String host, int port, Map<String, String> tunnelConfig, String clientId, int retriesLeft) {
        if (retriesLeft <= 0) {
            logger.error("Netty Client (Dst) connection FAILED for ClientID: " + clientId + " to " + host + ":" + port + " after multiple retries.");
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
        int failureThreshold = 2;
        long healthCheckInterval = 5; // seconds

        Runnable healthCheckTask = () -> {
            try {
                if (!serverChannel.isOpen()) {
                    stopHealthCheck(stunnelId);
                    return;
                }

                logger.debug("Performing health check for tunnel: " + stunnelId);
                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
                request.setParam("action", "tunnelhealthcheck");
                request.setParam("action_stunnel_id", stunnelId);

                MsgEvent response = plugin.sendRPC(request);

                if (response != null && "10".equals(response.getParam("status"))) {
                    consecutiveFailures.set(0);
                    logger.debug("Health check successful for tunnel: " + stunnelId);
                } else {
                    int failures = consecutiveFailures.incrementAndGet();
                    logger.warn("Health check failed for tunnel: " + stunnelId + ". Consecutive failures: " + failures);
                    if (failures >= failureThreshold) {
                        logger.error("Health check failure threshold reached for tunnel: " + stunnelId + ". Forcing tunnel closure and rebuild.");
                        serverChannel.close(); // This will trigger the closeFuture listener to rebuild the tunnel
                        stopHealthCheck(stunnelId);
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

        ScheduledFuture<?> healthCheckFuture = scheduler.scheduleAtFixedRate(healthCheckTask, healthCheckInterval, healthCheckInterval, TimeUnit.SECONDS);
        activeHealthChecks.put(stunnelId, healthCheckFuture);
        logger.info("Health check scheduled for tunnel " + stunnelId + " every " + healthCheckInterval + " seconds.");
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

        activeTunnelsConfig.remove(stunnelId);
        PerformanceMonitor pm = performanceMonitors.remove(stunnelId + "_src");
        if (pm != null) {
            pm.shutdown();
        }
        tunnelStateMachines.remove(stunnelId);
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

        activeTunnelsConfig.remove(stunnelId);
        PerformanceMonitor pm = performanceMonitors.remove(stunnelId + "_dst");
        if (pm != null) {
            pm.shutdown();
        }
        tunnelStateMachines.remove(stunnelId);
        logger.info("DST tunnel resource cleanup complete for: " + stunnelId);
    }


    public void shutdown() {
        logger.info("Shutting down SocketController and all Netty components...");
        // Gracefully shutdown the scheduler to stop new reconnection tasks
        scheduler.shutdown();

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

        logger.info("SocketController shutdown complete.");
    }

    public Map<String, Map<String,String>> getActiveTunnels() {
        return new HashMap<>(activeTunnelsConfig);
    }

    public Map<String, String> getTunnelConfig(String stunnelId) {
        Map<String, String> config = activeTunnelsConfig.get(stunnelId);
        return (config != null) ? Collections.unmodifiableMap(config) : null;
    }
}
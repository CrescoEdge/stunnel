package io.cresco.stunnel;

import io.cresco.library.agent.AgentService;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.Executor;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.plugin.PluginService;
import io.cresco.library.utilities.CLogger;
import org.apache.felix.hc.api.HealthCheck;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Component(
        service = { PluginService.class },
        scope=ServiceScope.PROTOTYPE,
        configurationPolicy = ConfigurationPolicy.REQUIRE, // Requires configuration provided by framework
        reference=@Reference(name="io.cresco.library.agent.AgentService", service= AgentService.class)
)
public class Plugin implements PluginService {

    // OSGi Bundle context
    private BundleContext context;
    // Cresco PluginBuilder helper
    private PluginBuilder pluginBuilder;
    // Custom message executor
    private Executor executor;
    // Logger instance
    private CLogger logger;
    // SLF4J fallback for lifecycle paths where the CLogger is null (before start / after deactivate).
    private static final Logger slog = LoggerFactory.getLogger(Plugin.class);
    // Configuration map from OSGi
    private Map<String,Object> map;
    // The core controller, now using Netty
    private SocketController socketController;
    // Felix HealthCheck registration (central health); best-effort, unregistered on stop
    private ServiceRegistration<HealthCheck> healthReg;

    // FIX: Use an AtomicBoolean to safely track the plugin's active state.
    // This avoids NullPointerExceptions during shutdown race conditions.
    private final AtomicBoolean isActive = new AtomicBoolean(false);


    // Called when the OSGi component is activated
    @Activate
    void activate(BundleContext context, Map<String,Object> map) {
        this.context = context;
        this.map = map;
        // Initialization happens in isStarted() which is called by the agent framework
    }

    // Called when the OSGi component configuration is modified
    @Modified
    void modified(BundleContext context, Map<String,Object> map) {
        // Handle configuration updates if necessary
        // This might involve restarting tunnels based on new config
        if (logger != null) {
            logger.info("Configuration modified. Restarting/reconfiguring might be needed.");
        } else {
            slog.info("Plugin configuration modified but logger not yet initialized.");
        }
    }

    // Called when the OSGi component is deactivated
    @Deactivate
    void deactivate(BundleContext context, Map<String,Object> map) {
        // Call the null-safe isStopped() method to ensure cleanup
        isStopped();
        // Nullify references
        this.context = null;
        this.map = null;
        this.pluginBuilder = null;
        this.executor = null;
        this.socketController = null;
        this.logger = null;
        slog.info("sTunnel Plugin Deactivated.");
    }

    // Check if the plugin is currently active using the internal state flag
    @Override
    public boolean isActive() {
        return this.isActive.get();
    }

    // Set the plugin's active status using the internal state flag
    @Override
    public void setIsActive(boolean isActive) {
        this.isActive.set(isActive);
    }

    // Entry point for incoming messages from the agent
    @Override
    public boolean inMsg(MsgEvent incoming) {
        // Route incoming messages to the PluginBuilder, which dispatches to the Executor
        if (pluginBuilder != null) {
            pluginBuilder.msgIn(incoming);
            return true; // Indicate message was accepted
        }
        return false; // Plugin not ready
    }

    /**
     * Called by the Cresco agent framework to initialize the plugin.
     * Sets up the PluginBuilder, Logger, SocketController, and Executor.
     * Waits for the agent to be fully active before completing.
     */
    @Override
    public boolean isStarted() {
        try {
            if (pluginBuilder == null) {
                // Initialize PluginBuilder (provides agent services, logging, etc.)
                pluginBuilder = new PluginBuilder(this.getClass().getName(), context, map);
                this.logger = pluginBuilder.getLogger(Plugin.class.getName(), CLogger.Level.Info);
                logger.info("Initializing sTunnel Plugin...");

                // Initialize the Netty-based SocketController
                this.socketController = new SocketController(pluginBuilder);
                logger.info("SocketController initialized.");

                // Initialize the custom Executor, passing the SocketController
                this.executor = new PluginExecutor(pluginBuilder, socketController);
                pluginBuilder.setExecutor(executor); // Register executor with PluginBuilder
                logger.info("PluginExecutor initialized and set.");

                // Wait until the agent is fully initialized and active
                while (!pluginBuilder.getAgentService().getAgentState().isActive()) {
                    logger.info("Plugin " + pluginBuilder.getPluginID() + " waiting for Agent to become active...");
                    Thread.sleep(1000);
                }
                logger.info("Agent is active. Plugin startup complete.");

                // FIX: Set the internal state flag to true upon successful startup.
                this.isActive.set(true);
                // Also reflect active state on the PluginBuilder like every other Cresco plugin, so
                // pluginBuilder.isActive() is truthful (the framework + the central HealthCheck rely
                // on it). Previously stunnel tracked liveness ONLY via the private AtomicBoolean, so
                // pluginBuilder.isActive() stayed false and health checks read the plugin as down.
                pluginBuilder.setIsActive(true);

                // Register the central Felix HealthCheck (best-effort; must never break startup).
                registerHealthCheck();
            }
            return this.isActive.get(); // Return the state from our flag
        } catch (Exception ex) {
            // Log any errors during startup
            if (logger != null) {
                logger.error("sTunnel Plugin startup failed: " + ex.getMessage(), ex);
            } else {
                slog.error("sTunnel Plugin startup failed (logger not initialized)", ex);
            }
            // Ensure cleanup if startup fails partially
            isStopped();
            return false; // Indicate startup failure
        }
    }

    /** Register stunnel's Felix HealthCheck so CrescoHealthExecutor discovers it. Best-effort. */
    private void registerHealthCheck() {
        try {
            Dictionary<String, Object> props = new Hashtable<>();
            props.put(HealthCheck.NAME, "stunnel");
            props.put(HealthCheck.TAGS, new String[]{"local"});
            healthReg = context.registerService(HealthCheck.class,
                    new StunnelHealthCheck(pluginBuilder, socketController), props);
            logger.info("Registered stunnel HealthCheck (Felix HC)");
        } catch (Throwable t) {
            // health is best-effort: a missing hc.api bundle must never break stunnel
            if (logger != null) logger.warn("Could not register stunnel HealthCheck: " + t.getMessage());
        }
    }

    /**
     * FIX: This method is now null-safe and idempotent.
     * Called by the Cresco agent framework or during deactivation to stop the plugin.
     * It relies only on the internal state flag and avoids using services like the logger
     * that may have already been shut down.
     */
    @Override
    public boolean isStopped() {
        // This method must be safe to call even after deactivation.
        // Do not use the logger or other services here as they may be null.

        // Set internal state to inactive
        this.isActive.set(false);
        // Keep the PluginBuilder's active flag in sync (null-safe: it may already be torn down).
        if (pluginBuilder != null) {
            try { pluginBuilder.setIsActive(false); } catch (Exception ignore) { }
        }

        // Unregister the central HealthCheck (best-effort)
        try {
            if (healthReg != null) { healthReg.unregister(); healthReg = null; }
        } catch (Exception ignore) { }

        // Safely shut down the socket controller if it exists
        if (socketController != null) {
            socketController.shutdown();
        }

        // Unset the executor in the pluginBuilder if it exists
        if (pluginBuilder != null) {
            pluginBuilder.setExecutor(null);
        }

        // Return true to indicate the stop process has been completed.
        return true;
    }
}

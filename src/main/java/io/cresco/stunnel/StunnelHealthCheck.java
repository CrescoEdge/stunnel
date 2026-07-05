package io.cresco.stunnel;

import io.cresco.library.plugin.PluginBuilder;
import org.apache.felix.hc.api.HealthCheck;
import org.apache.felix.hc.api.Result;

/**
 * Central health for stunnel. Registered as an {@code org.apache.felix.hc.api.HealthCheck} OSGi
 * service (name "stunnel", tag "local") so the controller's CrescoHealthExecutor discovers and
 * schedules it alongside the built-in broker/db/disk/memory/plugins checks — the same Felix Health
 * Check system every other Cresco bundle uses. Follows the identical guard pattern as the other
 * plugin health checks: TEMPORARILY_UNAVAILABLE until the plugin is active and its controller exists.
 */
public class StunnelHealthCheck implements HealthCheck {

    private final PluginBuilder plugin;
    private final SocketController socketController;

    public StunnelHealthCheck(PluginBuilder plugin, SocketController socketController) {
        this.plugin = plugin;
        this.socketController = socketController;
    }

    @Override
    public Result execute() {
        try {
            if (plugin == null || !plugin.isActive() || socketController == null) {
                return new Result(Result.Status.TEMPORARILY_UNAVAILABLE, "stunnel not active");
            }
            int tunnels = socketController.getActiveTunnels().size();
            return new Result(Result.Status.OK, "stunnel OK: " + tunnels + " configured tunnel(s)");
        } catch (Exception ex) {
            return new Result(Result.Status.WARN, "stunnel health error: " + ex.getMessage());
        }
    }
}

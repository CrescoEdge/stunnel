package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.DistributionSummary;
import jakarta.jms.DeliveryMode;
import jakarta.jms.TextMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * A reusable class for monitoring and reporting byte transfer performance.
 * This class eliminates the duplicate code in TunnelListener and TunnelSender.
 * Designed to be fully OSGi-compatible with no static fields or finals.
 */
public class PerformanceMonitor {
    // Core dependencies - these aren't final for OSGi compatibility
    private PluginBuilder plugin;
    private CLogger logger;
    private Map<String, String> tunnelConfig;
    private String direction;
    private String metricName;
    private Gson gson;

    // Performance tracking objects
    private LongAdder bytesAdder;
    private AtomicLong lastTotalBytes;
    private AtomicLong lastReportTimeMs;

    // Constants converted to instance fields
    private double bytesToBits;
    private double bitsToMegabits;
    private double bytesToMegabytes;

    // Configuration
    private int bufferSize;

    // Metrics and scheduling
    private DistributionSummary bytesPerSecond;
    private ScheduledExecutorService scheduler;
    private volatile boolean isHealthy;

    // Debugging configuration
    private boolean debugMode;

    /**
     * Creates a new performance monitor
     *
     * @param plugin The plugin builder instance
     * @param tunnelConfig The tunnel configuration
     * @param direction The data direction ("src" or "dst")
     * @param metricName The metric name for Micrometer reporting
     * @param reportingIntervalMs The interval for reporting metrics in milliseconds
     */
    public PerformanceMonitor(PluginBuilder plugin, Map<String, String> tunnelConfig,
                              String direction, String metricName, int reportingIntervalMs) {
        // Initialize core dependencies
        this.plugin = plugin;
        this.logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.tunnelConfig = tunnelConfig;
        this.direction = direction;
        this.metricName = metricName;
        this.gson = new Gson();

        // Initialize measuring objects
        this.bytesAdder = new LongAdder();
        this.lastTotalBytes = new AtomicLong(0);
        this.lastReportTimeMs = new AtomicLong(System.currentTimeMillis());

        // Set up constants as instance variables
        this.bytesToBits = 8.0;
        // We're not using these anymore - keeping them to avoid null errors
        this.bitsToMegabits = 1.0 / 1_000_000.0;
        this.bytesToMegabytes = 1.0 / (1024.0 * 1024.0);

        // Configure debug mode from config
        this.debugMode = Boolean.parseBoolean(
                tunnelConfig.getOrDefault("debug_performance", "false"));

        // Get buffer size from config or use default
        int configBufferSize = 8192; // Default
        if(tunnelConfig.containsKey("buffer_size")) {
            try {
                configBufferSize = Integer.parseInt(tunnelConfig.get("buffer_size"));
            } catch (NumberFormatException e) {
                logger.warn("Invalid buffer_size in config, using default: " + e.getMessage());
            }
        }
        this.bufferSize = configBufferSize;

        // Set initial health status
        this.isHealthy = true;

        // Log configuration details
        logger.debug("PerformanceMonitor initialized: direction=" + direction +
                ", bufferSize=" + bufferSize +
                ", reportingInterval=" + reportingIntervalMs + "ms");

        // Initialize metrics
        initPerformanceMetrics();

        // Create named thread pool for scheduler - no final modifier for OSGi
        this.scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "PerfMonitor-" + direction + "-" + tunnelConfig.get("stunnel_id"));
            t.setDaemon(true);
            return t;
        });

        // Schedule reporting task
        scheduler.scheduleAtFixedRate(
                new PerformanceReporterTask(), 1, reportingIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Initialize the performance metrics with Micrometer
     */
    private void initPerformanceMetrics() {
        try {
            MeasurementEngine me = new MeasurementEngine(plugin);

            bytesPerSecond = DistributionSummary
                    .builder(metricName)
                    .baseUnit("bytes")
                    .description("Bytes transferred per second")
                    .register(me.getCrescoMeterRegistry());

        } catch (Exception ex) {
            logger.error("Failed to initialize PerformanceMetrics", ex);
        }
    }

    /**
     * Set the health status for reporting
     */
    public void setHealthy(boolean healthy) {
        this.isHealthy = healthy;
    }

    /**
     * Add bytes to the monitor with improved recording
     * This method ensures we count all bytes accurately.
     */
    public void addBytes(long byteCount) {
        if (byteCount <= 0) {
            return; // Ignore non-positive byte counts
        }

        // Add to the adder for thread-safe accumulation
        bytesAdder.add(byteCount);

        // Record in metrics registry
        bytesPerSecond.record(byteCount);

        if (debugMode && byteCount > bufferSize) {
            logger.debug("Large byte count added: " + byteCount + " bytes in " + direction);
        }
    }

    /**
     * Get the current total byte count
     */
    public long getTotalBytes() {
        return bytesAdder.sum();
    }

    /**
     * Shut down the performance monitor
     */
    public void shutdown() {
        try {
            // Shutdown the scheduler properly
            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            }

            logger.info("Performance monitor shutdown complete for " + direction);
        } catch (InterruptedException e) {
            if (scheduler != null) {
                scheduler.shutdownNow();
            }
            Thread.currentThread().interrupt();
            logger.error("Performance monitor shutdown interrupted for " + direction);
        }
    }

    /**
     * Internal task to report performance metrics
     * Improved accuracy by using more precise time measurement and calculation
     */
    private class PerformanceReporterTask implements Runnable {
        private long lastReportedBytes = 0;
        private long consecutiveZeroReports = 0;

        @Override
        public void run() {
            try {
                // Get precise current time
                long currentTimeMs = System.currentTimeMillis();

                // Get current byte count WITHOUT resetting
                long currentByteCount = bytesAdder.sum();

                // Calculate deltas - bytes transferred during this period
                long bytesDelta = currentByteCount - lastReportedBytes;

                // Calculate elapsed time in seconds (as a double for floating point division)
                double elapsedSeconds = (currentTimeMs - lastReportTimeMs.get()) / 1000.0;

                // Detect potential measurement issues
                if (bytesDelta == 0) {
                    consecutiveZeroReports++;
                    if (consecutiveZeroReports > 3 && currentByteCount > 0) {
                        logger.warn(direction + ": No bytes recorded for " +
                                consecutiveZeroReports + " reports, possible measurement issue");
                    }
                } else {
                    consecutiveZeroReports = 0;
                }

                // Calculate rates with proper unit conversion
                double bytesPerSec = (elapsedSeconds > 0) ? bytesDelta / elapsedSeconds : 0;
                double bitsPerSec = bytesPerSec * bytesToBits;
                double megabitsPerSec = bitsPerSec * bitsToMegabits;
                double megaBytesPerSec = bytesPerSec * bytesToMegabytes;

                // Enhanced logging with focus on bits per second (network standard)
                if (bytesDelta > 0 || debugMode) {
                    logger.debug(String.format(
                            "%s Performance: %d bits/sec, elapsed: %.3fs",
                            direction, (long)bitsPerSec, elapsedSeconds
                    ));

                    // Only log additional details if in debug mode
                    if (debugMode) {
                        logger.debug(String.format(
                                "%s Additional: delta=%d bytes, total=%d bytes",
                                direction, bytesDelta, currentByteCount
                        ));
                    }
                }

                // Update tracking variables for next interval
                lastReportedBytes = currentByteCount;
                lastReportTimeMs.set(currentTimeMs);

                // Create message with more detailed metrics
                TextMessage updatePerformanceMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
                updatePerformanceMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                updatePerformanceMessage.setStringProperty("direction", direction);
                updatePerformanceMessage.setStringProperty("type", "stats");

                // Create metrics map with only bits per second - standard for networking
                Map<String, String> performanceMetrics = new HashMap<>();
                performanceMetrics.put("stunnel_id", tunnelConfig.get("stunnel_id"));
                performanceMetrics.put("direction", direction);
                performanceMetrics.put("bytes_delta", String.valueOf(bytesDelta));

                // Only bits per second (standard network metric)
                performanceMetrics.put("bits_per_second", String.format("%.0f", bitsPerSec));

                // Additional metadata
                performanceMetrics.put("total_bytes", String.valueOf(currentByteCount));
                performanceMetrics.put("tid", String.valueOf(Thread.currentThread().getId()));
                performanceMetrics.put("is_healthy", String.valueOf(isHealthy));
                performanceMetrics.put("elapsed_time", String.format("%.3f", elapsedSeconds));
                performanceMetrics.put("buffer_size", String.valueOf(bufferSize));

                String performanceMetricsJson = gson.toJson(performanceMetrics);
                updatePerformanceMessage.setText(performanceMetricsJson);

                // Send the metrics with higher priority for accurate timing
                plugin.getAgentService().getDataPlaneService().sendMessage(
                        TopicType.GLOBAL,
                        updatePerformanceMessage,
                        DeliveryMode.NON_PERSISTENT,
                        2,  // Higher priority
                        0
                );
            } catch (Exception ex) {
                logger.error("Error in performance reporting: " + ex.getMessage(), ex);
            }
        }
    }
}
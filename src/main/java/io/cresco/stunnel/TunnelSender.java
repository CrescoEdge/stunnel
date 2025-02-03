package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.metrics.MeasurementEngine;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.micrometer.core.instrument.DistributionSummary;
import jakarta.jms.DeliveryMode;
import jakarta.jms.TextMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class TunnelSender {

    private final PluginBuilder plugin;
    private final CLogger logger;

    private final AtomicBoolean sessionSenderLock;
    private final Map<String, SessionSender> sessionSenders;

    public SocketController socketController;

    private final Map<String,String> tunnelConfig;

    private final Timer senderHealthWatcherTask;

    private boolean inHealthCheck = false;
    private boolean isHealthy = true;


    private DistributionSummary bytesPerSecond;
    private final Timer performanceReporterTask;

    //public AtomicLong bytes = new AtomicLong(0);
    public LongAdder bytes = new LongAdder();

    private long lastReportTS = 0;

    private Gson gson;

    public TunnelSender(PluginBuilder plugin, SocketController socketController, Map<String,String> tunnelConfig)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.socketController = socketController;
        this.tunnelConfig = tunnelConfig;

        gson = new Gson();

        sessionSenderLock = new AtomicBoolean();
        sessionSenders = Collections.synchronizedMap(new HashMap<>());

        //report speed
        initPerformanceMetrics();

        int watchDogTimeout = 5000;
        if(tunnelConfig.containsKey("watchdog_timeout")) {
            watchDogTimeout = Integer.parseInt(tunnelConfig.get("watchdog_timeout"));
        }
        senderHealthWatcherTask = new Timer();
        senderHealthWatcherTask.scheduleAtFixedRate(new SenderHealthWatcherTask(), 5000, watchDogTimeout);

        // report bps
        int performanceReportRate = 5000;
        if(tunnelConfig.containsKey("performance_report_rate")) {
            performanceReportRate = Integer.parseInt(tunnelConfig.get("performance_report_rate"));
        }

        performanceReporterTask = new Timer();
        performanceReporterTask.scheduleAtFixedRate(new PerformanceReporter(), 5000, performanceReportRate);

    }

    private void initPerformanceMetrics() {
        try {

            MeasurementEngine me = new MeasurementEngine(plugin);

            bytesPerSecond = DistributionSummary
                    .builder("bytes.per.second.sender")
                    .baseUnit("bytes")
                    .description("Bytes transferred per second")
                    .register(me.getCrescoMeterRegistry());


        } catch (Exception ex) {
            logger.error("failed to initialize PerformanceMetrics", ex);
        }
    }

    class PerformanceReporter extends TimerTask {

        public void run() {
            try {
                //calculate
                float bytesPS = bytes.sum() / ((float) (System.currentTimeMillis() - lastReportTS) / 1000);
                bytes.reset();
                lastReportTS = System.currentTimeMillis();

                // record locally
                bytesPerSecond.record(bytesPS);
                // send message
                TextMessage updatePerformanceMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
                updatePerformanceMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                updatePerformanceMessage.setStringProperty("direction", "dst");

                Map<String,String> performanceMetrics = new HashMap<>();
                performanceMetrics.put("stunnel_id", tunnelConfig.get("stunnel_id"));
                performanceMetrics.put("BPS", String.valueOf(bytesPS));
                performanceMetrics.put("MBPS", String.valueOf(bytesPerSecond.mean()));
                performanceMetrics.put("direction", "dst");
                performanceMetrics.put("tid", String.valueOf(Thread.currentThread().getId()));
                performanceMetrics.put("is_healthy", String.valueOf(isHealthy));
                String performanceMetricsJson = gson.toJson(performanceMetrics);
                updatePerformanceMessage.setText(performanceMetricsJson);

                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, updatePerformanceMessage, DeliveryMode.NON_PERSISTENT, 4, 0);

            } catch (Exception ex) {
                logger.error("failed to initialize PerformanceMetrics", ex);
            }

        }
    }


    public boolean createSession(String clientId) {
        boolean isStarted = false;
        try {
            setSessionSender(plugin, this, tunnelConfig, clientId);
            if(getSessionSender(clientId).start()) {
                while (getSessionSender(clientId).getStatus() == -1) {
                    Thread.sleep(100);
                }
                logger.debug("boolean start() STATUS: " + getSessionSender(clientId).getStatus());
                if (getSessionSender(clientId).getStatus() == 10) {
                    isStarted = true;
                }
                logger.debug("(8): [dst] ClientThread started: " + isStarted + " status: " + getSessionSender(clientId).getStatus());
            } else {
                logger.error("(8): [dst] ClientThread failed: " + isStarted);
            }
        } catch (Exception ex) {
            logger.error("(8): [dst] ClientThread error: " + ex.getMessage());
        }
        return isStarted;
    }

    public void close() {
        // remove all sessions
        closeSessions();

        // cancel performance monitor
        performanceReporterTask.cancel();

        // cancel checks
        senderHealthWatcherTask.cancel();

    }

    private void closeSessions() {

        synchronized (sessionSenderLock) {

            for (Map.Entry<String, SessionSender> entry : sessionSenders.entrySet()) {
                String clientId = entry.getKey();
                SessionSender sessionSender = entry.getValue();
                sessionSender.close();
                logger.info("Shutting down clientID: " + clientId);
            }
        }
    }


    public boolean closeClient(String clientId) {
        // set state

        boolean isClosed = false;
        try {
            SessionSender sessionSender = getSessionSender(clientId);
            if(sessionSender != null) {
                sessionSender.close();
            } else {
                logger.error("closeClient() client_id: " + clientId + " clientThread == null");
            }
            removeSessionSender(clientId);
            isClosed = true;

        } catch (Exception ex) {
            logger.error("closeClient clientId: " + clientId + " error!");
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            ex.printStackTrace(pw);
            logger.error(sw.toString());
        }

        return isClosed;
    }

    private SessionSender getSessionSender(String clientId) {

        SessionSender sessionSender;
        synchronized (sessionSenderLock) {
            sessionSender = sessionSenders.get(clientId);
        }
        return sessionSender;
    }

    private void removeSessionSender(String clientId) {

        synchronized (sessionSenderLock) {
            sessionSenders.remove(clientId);
        }

    }

    private void setSessionSender(PluginBuilder plugin, TunnelSender tunnelSender, Map<String,String> tunnelConfig, String clientId) {

        synchronized (sessionSenderLock) {
            sessionSenders.put(clientId, new SessionSender(plugin, tunnelSender, tunnelConfig, clientId));
        }

    }

    class SenderHealthWatcherTask extends TimerTask {
        public void run() {

            if(!inHealthCheck) {
                // set lock
                inHealthCheck = true;
                boolean isHealthy = false;
                try {

                    //send message to remote plugin and check if dst host/port is listening
                    MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, tunnelConfig.get("src_region"), tunnelConfig.get("src_agent"), tunnelConfig.get("src_plugin"));
                    request.setParam("action", "tunnelhealthcheck");
                    request.setParam("action_stunnel_id", tunnelConfig.get("stunnel_id"));
                    MsgEvent response = plugin.sendRPC(request);
                    if(response != null) {
                        if (response.getParam("status") != null) {
                            int status = Integer.parseInt(response.getParam("status"));
                            if (status == 10) {
                                isHealthy = true;
                            }

                        } else {
                            logger.error("SenderHealthWatcherTask: Error in config of dst tunnel: Missing status from response: " + response.getParams());
                        }
                    } else {
                        logger.error("SenderHealthWatcherTask: remote response is null");
                    }

                } catch (Exception ex) {
                    logger.error("SenderHealthWatcherTask Run {}", ex.getMessage());
                    ex.printStackTrace();
                }
                if (!isHealthy) {
                    logger.error("SenderHealthWatcherTask: Health check failed");
                    // for now clear clear the sessions and remove the tunnel config
                    socketController.removeDstTunnel();
                    isHealthy = false;

                } else {
                    logger.debug("SenderHealthWatcherTask: Health check ok");
                    isHealthy = true;
                }
                // release lock
                inHealthCheck = false;
            }
        }
    }


}





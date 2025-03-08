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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class TunnelListener implements Runnable  {

    private final PluginBuilder plugin;
    private final CLogger logger;

    private ServerSocket serverSocket;

    private final AtomicBoolean sessionListenerLock;
    private final Map<String, SessionListener> sessionListeners;

    private boolean isActive = false;

    private boolean isInit = false;

    private final Map<String,String> tunnelConfig;

    private final Timer listenerHealthWatcherTask;
    private boolean inHealthCheck = false;
    private boolean isHealthy = true;
    public SocketController socketController;

    private DistributionSummary bytesPerSecond;
    private final Timer performanceReporterTask;

    //public AtomicLong bytes = new AtomicLong(0);
    public LongAdder bytes = new LongAdder();

    private long lastReportTS = 0;

    private Gson gson;

    public TunnelListener(PluginBuilder plugin, SocketController socketController, Map<String,String> tunnelConfig)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.socketController = socketController;


        gson = new Gson();

        this.tunnelConfig = tunnelConfig;

        sessionListenerLock = new AtomicBoolean();
        sessionListeners = Collections.synchronizedMap(new HashMap<>());

        //report speed
        initPerformanceMetrics();

        int watchDogTimeout = 5000;
        if(tunnelConfig.containsKey("watchdog_timeout")) {
            watchDogTimeout = Integer.parseInt(tunnelConfig.get("watchdog_timeout"));
        }
        listenerHealthWatcherTask = new Timer();
        listenerHealthWatcherTask.scheduleAtFixedRate(new ListenerHealthWatcherTask(), 5000, watchDogTimeout);

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
                    .builder("bytes.per.second.listener")
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
                //logger.error("performance reporter: " + bytesPerSecond.mean() + " tunnel: " + tunnelConfig.get("stunnel_id"));
                float bytesPS = bytes.sum() / ((float) (System.currentTimeMillis() - lastReportTS) / 1000);
                bytes.reset();
                lastReportTS = System.currentTimeMillis();

                // record locally
                bytesPerSecond.record(bytesPS);
                // send message
                TextMessage updatePerformanceMessage = plugin.getAgentService().getDataPlaneService().createTextMessage();
                updatePerformanceMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                updatePerformanceMessage.setStringProperty("direction", "src");
                updatePerformanceMessage.setStringProperty("type", "stats");

                Map<String,String> performanceMetrics = new HashMap<>();
                performanceMetrics.put("stunnel_id", tunnelConfig.get("stunnel_id"));
                performanceMetrics.put("BPS", String.valueOf(bytesPS));
                performanceMetrics.put("MBPS", String.valueOf(bytesPerSecond.mean()));
                performanceMetrics.put("direction", "src");
                performanceMetrics.put("tid", String.valueOf(Thread.currentThread().getId()));
                performanceMetrics.put("is_healthy", String.valueOf(isHealthy));
                String performanceMetricsJson = gson.toJson(performanceMetrics);
                updatePerformanceMessage.setText(performanceMetricsJson);

                //plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, updatePerformanceMessage);
                plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, updatePerformanceMessage, DeliveryMode.NON_PERSISTENT, 4, 0);

                // set new time


            } catch (Exception ex) {
                logger.error("failed to initialize PerformanceMetrics", ex);
            }

        }
    }


    public boolean isActive() {
        return isActive;
    }
    public boolean isInit() {
        return isInit;
    }

    public void close() {

        // stop checks
        listenerHealthWatcherTask.cancel();

        // stop performance checks
        performanceReporterTask.cancel();

        // remove all sessions
        closeSessions();

        if(isActive) {
            isActive = false;
        }

        //close listener socket
        closeSocket();

    }

    public void run() {

        isActive = true;

        try {
            //logger.info("Plugin " + plugin.getPluginID() + "stunnel_id:" + sTunnelId + " listening on port " + srcPort);
            serverSocket = new ServerSocket(Integer.parseInt(tunnelConfig.get("src_port")));
            //socketController.setTunnelStatus(sTunnelId, SocketController.StatusType.ACTIVE);

            logger.debug("(4): port open and waiting for incoming request on port: " + tunnelConfig.get("dst_port"));
            //socketController.completeSocketListenerInit();
            isInit = true;

            while(isActive) {

                try {

                    Socket clientSocket = serverSocket.accept();

                    String clientId = UUID.randomUUID().toString();

                    //set thread, arrange comm with remote host
                    setClientThreads(this, clientId, clientSocket);

                    //start thread
                    getClientThread(clientId).start();


                }  catch (SocketException sx) {
                    if(!sx.getMessage().equals("Socket closed")) {
                        logger.error("Socket error: " + tunnelConfig.get("dst_port") + " error: " + sx.getMessage());
                    }


                } catch(Exception ex) {
                    ex.printStackTrace();
                    logger.error("problem when accepting: " + tunnelConfig.get("dst_port") + " error: " + ex.getMessage());
                }

            }
            // set state
            //socketController.shutdown();

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            synchronized (sessionListenerLock) {

                for (Map.Entry<String, SessionListener> entry : sessionListeners.entrySet()) {
                    String clientId = entry.getKey();
                    SessionListener sessionListener = entry.getValue();
                    sessionListener.close();
                    logger.info("Shutting down clientID: " + clientId);
                }
            }

            //close the main socket
            closeSocket();
            isInit = true;
        }
    }



    private void setClientThreads(TunnelListener tunnelListener, String clientId, Socket clientSocket) {

        synchronized (sessionListenerLock) {
            sessionListeners.put(clientId, new SessionListener(plugin, tunnelConfig, tunnelListener, clientSocket, clientId));
        }

    }

    public boolean closeClient(String clientId) {
        boolean isClosed = false;
        try {
            getClientThread(clientId).close();
            removeClientThread(clientId);
            isClosed = true;

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return isClosed;
    }

    private void closeSessions() {

        synchronized (sessionListenerLock) {

            for (Map.Entry<String, SessionListener> entry : sessionListeners.entrySet()) {
                String clientId = entry.getKey();
                SessionListener sessionListener = entry.getValue();
                sessionListener.close();
                logger.info("Shutting down clientID: " + clientId);
            }
        }

    }


    private SessionListener getClientThread(String clientId) {

        SessionListener sessionListener;
        synchronized (sessionListenerLock) {
            sessionListener = sessionListeners.get(clientId);
        }
        return sessionListener;
    }

    private void removeClientThread(String clientId) {

        synchronized (sessionListenerLock) {
            sessionListeners.remove(clientId);
        }

    }

    public void closeSocket() {
        logger.info("closeSocket(): Closing socket");
        if(serverSocket != null ) {
            if(!serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    class ListenerHealthWatcherTask extends TimerTask {
        public void run() {

            if(!inHealthCheck) {
                // set lock
                inHealthCheck = true;
                boolean isHealthy = false;
                try {

                    //send message to remote plugin and check if dst host/port is listening
                    MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.EXEC, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
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
                            logger.error("ListenerHealthWatcherTask: Error in config of dst tunnel: Missing status from response: " + response.getParams());
                        }
                    } else {
                        logger.error("ListenerHealthWatcherTask: remote response is null");
                    }

                } catch (Exception ex) {

                    logger.error("ListenerHealthWatcherTask Run {}", ex.getMessage());
                    ex.printStackTrace();
                }

                if (!isHealthy) {
                    logger.error("ListenerHealthWatcherTask: Health check failed");
                    // for now try and wipe it out
                    socketController.dstCommFailure();
                    isHealthy = false;
                } else {
                    logger.debug("ListenerHealthWatcherTask: Health check ok");
                    isHealthy = true;
                }
                // release lock
                inHealthCheck = false;
            }
        }
    }

}






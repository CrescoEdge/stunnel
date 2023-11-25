package io.cresco.stunnel;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.BytesMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;


public class SocketController  {

    private PluginBuilder plugin;
    private CLogger logger;

    private final AtomicBoolean tunnelConfigsLock;
    private Map<String, Map<String,String>> tunnelConfigs;

    private final AtomicBoolean tunnelStatusLock;
    private Map<String, StatusType> tunnelStatus;

    private final AtomicBoolean tunnelListenersLock;
    private Map<String, SocketListener> tunnelListners;

    public SocketListener socketListener;
    public SocketSender socketSender;
    private Gson gson;

    public Type mapType;

    public SocketController(PluginBuilder plugin)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        tunnelConfigsLock = new AtomicBoolean();
        tunnelConfigs = Collections.synchronizedMap(new HashMap<>());
        tunnelStatusLock = new AtomicBoolean();
        tunnelStatus = Collections.synchronizedMap(new HashMap<>());
        tunnelListenersLock = new AtomicBoolean();
        tunnelListners = Collections.synchronizedMap(new HashMap<>());
        gson = new Gson();
        mapType = new TypeToken<Map<String, String>>(){}.getType();
    }

    public boolean createSrcTunnel(int srcPort, String dstHost, int dstPort, String dstRegion, String dstAgent, String dstPlugin, int bufferSize) {
        boolean isCreated = false;

        try{
            //tunnel id,based on initial request
            String sTunnelId = UUID.randomUUID().toString();

            //create config
            Map<String,String> tunnelConfig = new HashMap<>();
            tunnelConfig.put("stunnel_id", sTunnelId);
            tunnelConfig.put("src_port", String.valueOf(srcPort));
            tunnelConfig.put("dst_host", dstHost);
            tunnelConfig.put("dst_port", String.valueOf(dstPort));
            tunnelConfig.put("dst_region", dstRegion);
            tunnelConfig.put("dst_agent", dstAgent);
            tunnelConfig.put("dst_plugin", dstPlugin);
            tunnelConfig.put("src_region", plugin.getRegion());
            tunnelConfig.put("src_agent", plugin.getAgent());
            tunnelConfig.put("src_plugin", plugin.getPluginID());
            tunnelConfig.put("buffer_size", String.valueOf(bufferSize));

            logger.error("(2): [REMOVED] send message to remote plugin and check if dst host/port is listening");
            //send message to remote plugin and check if dst host/port is listening
            /*
            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, dstRegion, dstAgent, dstPlugin);
            request.setParam("action", "dstportcheck");
            request.setParam("action_dst_host", dstHost);
            request.setParam("action_dst_port", String.valueOf(dstPort));
            MsgEvent response = plugin.sendRPC(request);

            int dstPortStatus = - 1;
            if(response.paramsContains("status")) {
                dstPortStatus = Integer.parseInt(response.getParam("status"));
            }

            //if dst host/port is listening = 10, move forward
            if(dstPortStatus == 10) {

                //now set status to init
                setTunnelStatus(sTunnelId,StatusType.INIT);

                //set the tunnel config
                setTunnelConfig(sTunnelId, tunnelConfig);

                logger.error("(3): remote port is listening, start SocketListener()");
                //create listener
                socketListener = new SocketListener(plugin, this, sTunnelId, srcPort);
                //start new listner thread
                //logger.error("STARTING THREAD FOR LISTENER");
                new Thread(socketListener).start();


            } else {
                //tear down things broke
                logger.error("Failed to create tunnel error: Remote dstTunnel config failed.");
            }

             */

            //now set status to init
            setTunnelStatus(sTunnelId,StatusType.INIT);

            //set the tunnel config
            setTunnelConfig(sTunnelId, tunnelConfig);

            logger.error("(3): remote port is listening, start SocketListener()");
            //create listener
            socketListener = new SocketListener(plugin, this, sTunnelId, srcPort, bufferSize);

            //start new listner thread
            new Thread(socketListener).start();

        } catch (Exception ex) {
            logger.error("Failed to create tunnel error: " + ex.getMessage());
            ex.printStackTrace();
        }

        return isCreated;
    }

    public void setTunnelStatus(String tunnelId, StatusType statusType) {

        synchronized (tunnelStatusLock) {
            tunnelStatus.put(tunnelId,statusType);
        }

    }
    public StatusType getTunnelStatus(String tunnelId) {

        StatusType statusType;

        synchronized (tunnelStatusLock) {
            statusType = tunnelStatus.get(tunnelId);
        }
        return statusType;
    }

    public void setTunnelConfig(String tunnelId, Map<String,String> tunnelConfig) {

        synchronized (tunnelConfigsLock) {
            tunnelConfigs.put(tunnelId, tunnelConfig);
        }

    }

    public Map<String,String> getTunnelConfig(String tunnelId) {

        Map<String,String> tunnelConfig;

        synchronized (tunnelConfigsLock) {
            tunnelConfig = tunnelConfigs.get(tunnelId);
        }
        return tunnelConfig;
    }

    public Map<String,String> createDstTunnel(Map<String,String> tunnelConfig) {

        try{

            socketSender = new SocketSender(plugin, this, tunnelConfig);
            if(!socketSender.start()) {
                tunnelConfig = null;
                logger.error("Unable to start socketSender.");
            }

        } catch (Exception ex) {
            logger.error("Unable to createDstTunnel: " + ex.getMessage());
            logger.error("tunnelConfig: " + tunnelConfig);
            tunnelConfig = null;
            ex.printStackTrace();
        }

        return tunnelConfig;
    }

    public void shutdown() {
        if(socketListener != null) {
            socketListener.close();
        }
        if(socketSender != null) {
            socketSender.close();
        }
    }
    private String getControlDBListener(String sTunnelId, String stype, String direction) {

        MessageListener ml = new MessageListener() {
            public void onMessage(Message msg) {
                try {

                    if (msg instanceof TextMessage) {
                        logger.error("CONTROLLER MESSAGE: " + ((TextMessage) msg).getText());
                    }

                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }
        };


        String queryString = "stunnel_name='" + sTunnelId + "' and stype='" + stype + "' and direction='" + direction + "'";
        return plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,queryString);

    }

    public enum StatusType {
        INIT, READY, ACTIVE, CLOSED
    }
}




package io.cresco.stunnel;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import io.cresco.stunnel.state.SocketControllerSM;
import com.google.gson.GsonBuilder;
import java.io.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SocketController extends SocketControllerSM {

    private final PluginBuilder plugin;
    private final CLogger logger;

    public TunnelListener tunnelListener;
    public TunnelSender tunnelSender;

    // Create a GsonBuilder and enable the pretty print
    private Gson gson;
    public Type mapType;

    private Map<String,String> tunnelConfig;


    public SocketController(PluginBuilder plugin) {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        mapType = new TypeToken<Map<String, String>>(){}.getType();
        gson = new GsonBuilder().setPrettyPrinting().create();
        // check local config for startup data
        checkStartUpConfig();

    }

    private Map<String,String> getSavedTunnelConfig() {
        Map<String,String> savedTunnelConfig = null;
        try {
            String configPath = plugin.getPluginDataDirectory() + "/tunnel_config.json";


            File f = new File(configPath);
            if(f.exists() && !f.isDirectory()) {
                logger.info("Loading tunnel config: " + configPath);
                try (BufferedReader reader = new BufferedReader(new FileReader(configPath))) {
                    StringBuilder content = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        content.append(line).append("\n");
                    }

                    Type type = new TypeToken<Map<String, String>>(){}.getType();
                    savedTunnelConfig = gson.fromJson(content.toString(), type);

                } catch (Exception e) {
                    logger.error("Error loading saved tunnel config", e.getMessage());
                }
            }

        } catch (Exception e) {
            logger.error("Error getting saved tunnel config", e.getMessage());
        }

        return savedTunnelConfig;
    }

    private void saveTunnelConfig(Map<String,String> tunnelConfig) {
        try {
            String configPath = plugin.getPluginDataDirectory() + "/tunnel_config.json";
            logger.info("Saving tunnel config: " + configPath);
            Files.createDirectories(Paths.get(plugin.getPluginDataDirectory()));
            String configJson = gson.toJson(tunnelConfig);
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(configPath))) {
                writer.write(configJson);
            }
        } catch (Exception e) {
            logger.error("Error saving tunnel config", e.getMessage());
        }
    }

    private void checkStartUpConfig() {

        new Thread(() -> {
            try {

                logger.info("Checking startup config...");

                Map<String,String> canidateConfig = getSavedTunnelConfig();

                if (canidateConfig != null) {

                    // check if config contains everything we need
                    List<String> requiredConfigKeys = new ArrayList<>();
                    requiredConfigKeys.add("stunnel_id");
                    requiredConfigKeys.add("src_port");
                    requiredConfigKeys.add("dst_host");
                    requiredConfigKeys.add("dst_port");
                    requiredConfigKeys.add("dst_region");
                    requiredConfigKeys.add("dst_agent");
                    requiredConfigKeys.add("dst_plugin");
                    requiredConfigKeys.add("src_region");
                    requiredConfigKeys.add("src_agent");
                    requiredConfigKeys.add("src_plugin");
                    requiredConfigKeys.add("buffer_size");
                    requiredConfigKeys.add("watchdog_timeout");

                    boolean goodConfig = true;

                    for (String key : requiredConfigKeys) {
                        if(!canidateConfig.containsKey(key)) {
                            goodConfig = false;
                        }
                    }

                    if (goodConfig) {
                        logger.info("Startup config ok...creating tunnel");
                        String sTunnelId = null;
                        while (sTunnelId == null) {
                            logger.info("Waiting for Tunnel ID...");
                            sTunnelId = createSrcTunnel(canidateConfig);
                        }
                    }
                }

            } catch(Exception v) {
                System.out.println(v);
            }
        }).start();

    }

    public boolean stateNotify(String node) {
        if(logger != null) {
            logger.info("SocketController stateNotify: " + node);
        }

        State state = State.valueOf(node);

        switch(state)
        {
            case pluginActive:
                // line 5 "model.ump"
                break;
            case initTunnelListener:
                // line 16 "model.ump"
                break;
            case activeTunnelListener:
                // line 24 "model.ump"
                break;
            case tunnelListenerRecovery:
                // line 32 "model.ump"
                // save config
                Map<String,String> savedTunnelConfig = new HashMap<>(this.tunnelConfig);
                // clear out src tunnel
                removeSrcTunnel();
                String sTunnelId = null;
                while(sTunnelId == null) {
                    sTunnelId = createSrcTunnel(savedTunnelConfig);
                }
                // notify that things have recovered
                recoveredTunnel();
                break;
            case errorTunnelListener:
                // line 39 "model.ump"
                break;
            case initTunnelSender:
                // line 48 "model.ump"
                break;
            case activeTunnelSender:
                // line 55 "model.ump"
                break;
            case errorTunnelSender:
                // line 62 "model.ump"
                break;
            case pluginShutdown:
                // line 68 "model.ump"
                break;
        }

        return true;
    }


    public String createSrcTunnel(int srcPort, String dstHost, int dstPort, String dstRegion, String dstAgent, String dstPlugin, int bufferSize, int watchDogTimeout) {
        // set state
        this.incomingSrcTunnelConfig();

        String sTunnelId = UUID.randomUUID().toString();

        try{

            if(tunnelListener == null) {

                //create config
                Map<String, String> tunnelConfig = new HashMap<>();
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
                tunnelConfig.put("watchdog_timeout", String.valueOf(watchDogTimeout));

                // create from map
                return createSrcTunnel(tunnelConfig);

            } else {
                sTunnelId = null;
                logger.error("tunnelListener exists, plugin is already configured!");
            }

        } catch (Exception ex) {
            sTunnelId = null;
            logger.error("Failed to create tunnel error: " + ex.getMessage());
            ex.printStackTrace();
        }

        if (sTunnelId != null) {
            this.completeTunnelListenerInit();
        } else {
            this.failedTunnelListenerInit();
        }

        return sTunnelId;
    }

    public String createSrcTunnel(Map<String, String> tunnelConfig) {

        String sTunnelId = null;

        try{

            if(tunnelListener == null) {

                sTunnelId = tunnelConfig.get("stunnel_id");

                logger.debug("(2): send message to remote to create dst tunnel");

                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
                request.setParam("action", "configdsttunnel");
                request.setParam("action_tunnel_config", gson.toJson(tunnelConfig));
                MsgEvent response = plugin.sendRPC(request);

                if (response.getParam("status") != null) {
                    int status = Integer.parseInt(response.getParam("status"));
                    if (status == 10) {
                        try {
                            logger.debug("(2): TunnelSender started, starting TunnelListener()");

                            //create listener
                            tunnelListener = new TunnelListener(plugin, this, tunnelConfig);

                            //start new listner thread
                            new Thread(tunnelListener).start();

                            while(!tunnelListener.isInit()) {
                                Thread.sleep(100);
                            }

                            if (!tunnelListener.isActive()) {
                                sTunnelId = null;
                                logger.error("(2): TunnelListener -> start failed");
                                tunnelListener.closeSocket();
                                tunnelListener.close();
                                tunnelListener = null;

                            }

                        } catch (Exception ex) {
                            sTunnelId = null;
                            logger.error("(2): TunnelListener -> create failed: " + ex.getMessage());
                        }

                    } else {
                        sTunnelId = null;
                        logger.error("(2): TunnelListener: Error in remote config of TunnelSender: " + response.getParams());
                    }
                } else {
                    sTunnelId = null;
                    logger.error("(2): TunnelSender: Error in remote response of TunnelSender config: Missing status from response: " + response.getParams());
                }

            } else {
                sTunnelId = null;
                logger.error("tunnelListener exists, plugin is already configured!");
            }

        } catch (Exception ex) {
            sTunnelId = null;
            logger.error("Failed to create tunnel error: " + ex.getMessage());
            ex.printStackTrace();
        }

        if (sTunnelId != null) {
            this.completeTunnelListenerInit();
            // set the tunnel config
            this.tunnelConfig = tunnelConfig;
            // save config to disk
            saveTunnelConfig(tunnelConfig);
        } else {
            this.failedTunnelListenerInit();
        }

        return sTunnelId;
    }

    private void removeSrcTunnel() {
        if(tunnelListener != null) {
            tunnelListener.close();
            tunnelListener = null;
        }
        tunnelConfig = null;
    }

    public Map<String,String> createDstTunnel(Map<String,String> tunnelConfig) {
        // set state
        this.incomingDstTunnelConfig();

        this.tunnelConfig = tunnelConfig;

        //String sTunnelId = tunnelConfig.get("stunnel_id");

        //now set status to init
        //setTunnelStatus(sTunnelId, StatusType.INIT);

        //set the tunnel config
        //setTunnelConfig(sTunnelId, tunnelConfig);


        try{
            if(tunnelSender == null) {
                //logger.error("I JUST CREATED A NEW TUNNEL SENDER!!!!");
                tunnelSender = new TunnelSender(plugin, this, tunnelConfig);
            } else {
                logger.error("tunnelSender is already set, this plugin is already configured!");
            }

        } catch (Exception ex) {
            // set state
            // this.failedSocketSenderInit();
            logger.error("Unable to createDstTunnel: " + ex.getMessage());
            logger.error("tunnelConfig: " + tunnelConfig);
            tunnelConfig = null;
            ex.printStackTrace();
        }

        if (tunnelConfig != null) {
            this.completeTunnelSenderInit();
        } else {
            this.failedTunnelSenderInit();
        }
        return tunnelConfig;
    }

    public void removeDstTunnel() {
        if(tunnelSender != null) {
            tunnelSender.close();
            tunnelSender = null;
        }

        tunnelConfig = null;
    }

    public boolean createDstSession(String tunnelId, String clientId) {

        boolean isStarted = false;

        try{
            logger.debug("Trying to create new dst session: tunnelId=" + tunnelId + ", clientId=" + clientId);
            if(tunnelSender != null) {
                isStarted = tunnelSender.createSession(clientId);
            } else {
                logger.error("tunnelSender is null, is it configured?");
            }

        } catch (Exception ex) {
            // set state
            // this.failedSocketSenderInit();
            logger.error("Unable to createDstSession: " + ex.getMessage());
            ex.printStackTrace();
        }

        return isStarted;
    }

    public void shutdown() {
        removeDstTunnel();
        removeSrcTunnel();
    }
}


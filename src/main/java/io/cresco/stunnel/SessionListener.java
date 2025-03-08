package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.BytesMessage;
import jakarta.jms.MapMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import io.cresco.stunnel.state.SessionListenerSM;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

class SessionListener extends SessionListenerSM {

    private final PluginBuilder plugin;
    private final CLogger logger;

    private final Socket mClientSocket;


    private ListenerForwardThread clientForward;

    private final String clientId;

    public TunnelListener tunnelListener;

    private final Gson gson;

    private final Map<String,String> tunnelConfig;

    public AtomicBoolean outForwardingActive = new AtomicBoolean(false);
    public AtomicBoolean inForwardingActive = new AtomicBoolean(false);


    public SessionListener(PluginBuilder plugin, Map<String,String> tunnelConfig, TunnelListener tunnelListener, Socket aClientSocket, String clientId) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.tunnelConfig = tunnelConfig;

        this.tunnelListener = tunnelListener;
        mClientSocket = aClientSocket;
        this.clientId = clientId;
        gson = new Gson();
        logger.debug("(5): started SessionListener with client_id" + clientId);

    }

    public boolean stateNotify(String node) {

        if(logger != null) {
            logger.error("SessionListener stateNotify: " + node + " " + Thread.currentThread().getName());
        }

        State state = State.valueOf(node);

        // entry actions and do activities
        switch(state)
        {
            case initSessionListener:
                // line 6 "model.ump"
                break;
            case initForwardThread:
                // line 14 "model.ump"
                break;
            case activeForwardThread:
                // line 22 "model.ump"
                break;
            case waitCloseListener:
                // line 31 "model.ump"
                try {
                    mClientSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case closeSessionListener:
                // line 30 "model.ump"
                // gracefully shutdown the session listener from tunnel listener
                inForwardingActive.set(false);
                outForwardingActive.set(false);
                tunnelListener.closeClient(clientId);
                break;
        }
        return true;
    }

    public void close() {

        try {

            if(clientForward != null) {
                clientForward.closeListener();
            }

            if(mClientSocket != null) {
                if(!mClientSocket.isClosed()) {
                    mClientSocket.close();
                }
            }


        } catch (Exception ex) {
            logger.error("ClientThread close() error: " + ex.getMessage());
        }



    }
    /**
     * Establishes connection to the destination server and
     * starts bidirectional forwarding ot data between the
     * client and the server.
     */
    public boolean start() {

        boolean isStarted = false;

        InputStream clientIn;
        OutputStream clientOut;

        try {

            //logger.info("Plugin " + plugin.getPluginID() + " creating client socket streams.");

            // Turn on keep-alive for both the sockets
            mClientSocket.setKeepAlive(true);

            // Obtain client & server input & output streams
            clientIn = mClientSocket.getInputStream();
            clientOut = mClientSocket.getOutputStream();

            //will start listening now
            clientForward = new ListenerForwardThread(this, clientIn, clientOut, clientId);
            // set state
            this.createForwardThread();
            //send message to remote to bring up port and list

            logger.debug("(7): SessionListener sending message to remote host to get ready for incoming data");

            //send message to remote plugin and check if dst host/port is listening
            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
            request.setParam("action", "configdstsession");
            request.setParam("action_tunnel_id", tunnelConfig.get("stunnel_id"));
            request.setParam("action_client_id", clientId);
            MsgEvent response = plugin.sendRPC(request);

            if(response.getParam("status") != null) {
                int status = Integer.parseInt(response.getParam("status"));
                if(status == 10) {
                    try {
                        clientForward.start();
                        // set state
                        this.startedFowardThread();
                        logger.debug("(12): SessionListener -> clientForward started");
                        isStarted = true;
                    } catch (Exception ex) {
                        this.failedStartFowardThread();
                        logger.error("(12): SessionListener -> clientForward failed: " + ex.getMessage());
                    }

                } else {
                    logger.error("SessionListener: Error in config of dst tunnel: " + response.getParams());
                }
            } else {
                logger.error("SessionListener: Error in config of dst tunnel: Missing status from response: " + response.getParams());
            }

        } catch (Exception ex) {
            // set state
            failedCreateForwardThread();
            ex.printStackTrace();
        }

        if(!isStarted) {
            // clear session
            tunnelListener.closeClient(clientId);
        }

        return isStarted;
    }

    class ListenerForwardThread extends Thread {

        private final InputStream mInputStream;
        private final OutputStream mOutputStream;
        private final SessionListener mParent;
        private String node_from_listener_id;
        private final String clientId;

        /**
         * Creates a new traffic redirection thread specifying
         * its parent, input stream and output stream.
         */

        public ListenerForwardThread(SessionListener aParent, InputStream aInputStream, OutputStream aOutputStream, String clientId) {
            mParent = aParent;
            mInputStream = aInputStream;
            mOutputStream = aOutputStream;
            this.clientId = clientId;

            inForwardingActive.set(true);

            //messages in
            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {

                        if (msg instanceof BytesMessage) {
                            byte[] buffer = new byte[(int)((BytesMessage) msg).getBodyLength()];
                            int bytesRead = ((BytesMessage) msg).readBytes(buffer);
                            //if(mClientSocket.isConnected()) {
                            if(inForwardingActive.get()) {
                                mOutputStream.write(buffer, 0, bytesRead);
                                mOutputStream.flush();

                                // record transfer metrics
                                tunnelListener.bytes.add(bytesRead);

                            }

                        } else if (msg instanceof MapMessage) {
                            MapMessage statusMessage = (MapMessage) msg;
                            int remoteStatus = statusMessage.getInt("status");

                            // ack close
                            if(remoteStatus == 8) {
                                logger.debug("(13) notified by dst port closed by external");
                                dstClose();
                            }
                        }

                    } catch (SocketException se) {
                        // stop trying to write to socket
                        inForwardingActive.set(false);
                        logger.error("SocketException: " + se.getMessage());
                        //if(!(mParent.getState() == SessionListenerSM.State.waitCloseListener)) {
                        // this will occur normally, when src notifies dst to close
                        //logger.error("forwardingActive: " + outForwardingActive.get());
                        //logger.error("SocketException: " + se.getMessage());
                        //logger.error("IS CLOSED: " + mParent.mClientSocket.isClosed());
                        //logger.error("STATE: " + mParent.getState());
                        //}

                    } catch(Exception ex) {
                        logger.error("ListenerForwardThread onMessage() error: " + ex.getMessage());
                        ex.printStackTrace();
                    }
                }
            };

            if(node_from_listener_id != null) {
                logger.error("WHY IS LIST NO NULL? l_id:" + node_from_listener_id);
            }

            String queryString = "stunnel_id='" + tunnelConfig.get("stunnel_id") + "' and client_id='" + clientId + "' and direction='src'";
            node_from_listener_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);

            logger.debug("(6): SessionListener-> ForwardThread listner_id:" + node_from_listener_id + " started");

        }

        public void closeListener () {

            if(node_from_listener_id != null) {
                logger.debug("(14) ForwardThread close() removing node_from_listner_id: " + node_from_listener_id);
                plugin.getAgentService().getDataPlaneService().removeMessageListener(node_from_listener_id);
                node_from_listener_id = null;
            }

        }

        /*
        private void connectionBroken() {

            mParent.close();
            //close remote
            //Map<String,String> tunnelConfig = socketController.getTunnelConfig(sTunnelId);
            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
            request.setParam("action", "closedstclient");
            request.setParam("action_client_id", clientId);
            MsgEvent response = plugin.sendRPC(request);
            if(response.getParam("status") != null) {
                int status = Integer.parseInt(response.getParam("status"));
                if(status == 10) {
                    logger.info("(15) Dst port confirmed closed.");
                } else {
                    logger.error("Error in closing dst port: " + response.getParams());
                }
            } else {
                logger.error("Missing status from response: " + response.getParams());
            }
            close();
        }
         */

        /**
         * Runs the thread. Continuously reads the input stream and
         * writes the read data to the output stream. If reading or
         * writing fail, exits the thread and notifies the parent
         * about the failure.
         */
        public void run() {

            outForwardingActive.set(true);

            byte[] buffer = new byte[Integer.parseInt(tunnelConfig.get("buffer_size"))];

            try {
                while (outForwardingActive.get()) {

                    int bytesRead = mInputStream.read(buffer);
                    tunnelListener.bytes.add(bytesRead);

                    if(bytesRead > 0) {
                        //logger.error("bytesRead: " + bytesRead);
                        BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                        bytesMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                        bytesMessage.setStringProperty("direction", "dst");
                        bytesMessage.setStringProperty("client_id", clientId);
                        bytesMessage.writeBytes(buffer, 0, bytesRead);
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);
                        //logger.error(String.valueOf(isSent));

                        //logger.debug("Plugin " + plugin.getPluginID() + " writing " + buffer.length + " bytes to stunnel_name:" + sTunnelId);
                    } else if (bytesRead == -1) {

                        // (1) socket is closed, stop listening for incoming messages
                        closeListener();

                        // (2) notify remote side that it is time to shut things down.
                        logger.debug("(13) src port closed by external, close gracefuly");
                        MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
                        closeMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                        closeMessage.setStringProperty("direction", "dst");
                        closeMessage.setStringProperty("client_id", clientId);
                        //status message 8 == close gracefully
                        closeMessage.setInt("status", 8);
                        //logger.error("status: " + closeMessage.getInt("status"));
                        //closeMessage.setText("SENDING TEST MESSAGE");
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);

                        // (3) shutdown session Listener
                        srcClose();
                    }

                }
            } catch (SocketException se) {
                outForwardingActive.set(false);
                /*
                if(!(mParent.getState() == SessionListenerSM.State.waitCloseListener)) {
                    // this will occur normally, when src notifies dst to close
                    logger.error("forwardingActive: " + outForwardingActive.get());
                    logger.error("SocketException: " + se.getMessage());
                    logger.error("IS CLOSED: " + mParent.mClientSocket.isClosed());
                    logger.error("STATE: " + mParent.getState());
                }

                 */

            } catch (IOException e) {
                logger.error("ForwardThread onMessage() error: " + e.getMessage());
                // Read/write failed --> connection is broken
                logger.error("run() mParent.mServerSocket isClosed: " + mParent.mClientSocket.isClosed() + " isBound: " +
                        mParent.mClientSocket.isBound() + " isConnected: " + mParent.mClientSocket.isConnected() +
                        " isinputshut: " + mParent.mClientSocket.isInputShutdown() + " isoutputshut: "
                        + mParent.mClientSocket.isOutputShutdown());
                //set state
                //mParent.connectionBroken();
                //connectionBroken();
            } catch (Exception ex) {
                logger.error("SOME EXCEPTION: " + ex.getMessage());
                //mParent.connectionBroken();
                //connectionBroken();
            } finally {
                srcClose();
            }

        }
    }


}


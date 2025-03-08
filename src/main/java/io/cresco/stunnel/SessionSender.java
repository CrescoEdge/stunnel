package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.*;
import io.cresco.stunnel.state.SessionSenderSM;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class SessionSender extends SessionSenderSM {

    private final PluginBuilder plugin;
    private final CLogger logger;

    private final AtomicInteger status = new AtomicInteger(-1);

    private Socket mServerSocket;

    private final Map<String,String> tunnelConfig;

    private SenderForwardThread clientForward;

    private final String clientId;

    private TunnelSender tunnelSender;

    //public AtomicBoolean incomingForwardingActive = new AtomicBoolean(false);
    public AtomicBoolean outForwardingActive = new AtomicBoolean(false);
    public AtomicBoolean inForwardingActive = new AtomicBoolean(false);

    public SessionSender(PluginBuilder plugin, TunnelSender tunnelSender, Map<String,String> tunnelConfig, String clientId) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.tunnelSender = tunnelSender;
        this.tunnelConfig = tunnelConfig;
        this.clientId = clientId;
    }

    /**
     * Establishes connection to the destination server and
     * starts bidirectional forwarding ot data between the
     * client and the server.
     */

    public boolean stateNotify(String node) {

        if(logger != null) {
            logger.error("SessionSender stateNotify: " + node + " " + Thread.currentThread().getName());
        }

        SessionSenderSM.State state = SessionSenderSM.State.valueOf(node);

        switch(state)
        {
            case initSessionSender:
                // line 6 "model.ump"
                break;
            case initForwardThread:
                // line 14 "model.ump"
                break;
            case activeForwardThread:
                // line 22 "model.ump"
                break;
            case waitCloseSender:
                // line 31 "model.ump"
                // stop forwarding
                try {
                    mServerSocket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                break;
            case closeSessionSender:
                // line 30 "model.ump"
                outForwardingActive.set(false);
                inForwardingActive.set(false);
                tunnelSender.closeClient(clientId);
                break;
        }

        return true;
    }

    public Socket getServerSocket() {
        return mServerSocket;
    }

    public boolean start() {

        boolean isStarted = false;

        InputStream serverIn;
        OutputStream serverOut;

        try {
            logger.debug("(9): [dst] connecting to dst port");
            // Connect to the destination server
            mServerSocket = new Socket(tunnelConfig.get("dst_host"), Integer.parseInt(tunnelConfig.get("dst_port")));

            // Turn on keep-alive for both the sockets
            mServerSocket.setKeepAlive(true);

            // Obtain client & server input & output streams
            serverIn = mServerSocket.getInputStream();
            serverOut = mServerSocket.getOutputStream();

            clientForward = new SenderForwardThread(this, serverIn, serverOut);
            //set state
            this.createForwardThread();

            clientForward.start();

            //set state
            this.startedFowardThread();

            logger.debug("(11): [dst] ForwardThread started. status: " + status.get());

            isStarted = true;


            setStatus(10);

        } catch (Exception ex) {
            setStatus(9);
            logger.error("Can not connect to " + tunnelConfig.get("dst_host") + " " + Integer.parseInt(tunnelConfig.get("dst_port")) + " status:" + status.get());
            logger.error("Exception: " + ex.getMessage());
            // set state
            this.failedCreateForwardThread();
        }
        return isStarted;
    }


    public int getStatus() {
        return status.get();
    }

    public void setStatus(int inStatus) {
        status.set(inStatus);
    }

    public void close() {

        try {

            if(clientForward != null) {
                logger.debug("CALLING CLOSE ON clientForward");
                clientForward.closeListener();
            } else {
                logger.error("CALLING CLOSE ON NULL clientForward");
            }

            if(mServerSocket != null) {
                logger.debug("CALLING CLOSE ON mServerSocket");
                if(!mServerSocket.isClosed()) {
                    mServerSocket.close();
                }
            } else {
                logger.error("CALLING CLOSE ON NULL mServerSocket");
            }

        } catch (Exception ex) {
            logger.error("ClientThread close() error: " + ex.getMessage());
        }

        //99 is closed
        setStatus(99);

    }

    class SenderForwardThread extends Thread {

        private final InputStream mInputStream;
        private final OutputStream mOutputStream;
        private final SessionSender mParent;
        private String node_from_listener_id;

        /**
         * Creates a new traffic redirection thread specifying
         * its parent, input stream and output stream.
         */

        public SenderForwardThread(SessionSender aParent, InputStream aInputStream, OutputStream aOutputStream) throws JMSException {
            logger.debug("Plugin " + plugin.getPluginID() + " creating forwarding thread.");
            mParent = aParent;
            mInputStream = aInputStream;
            mOutputStream = aOutputStream;

            inForwardingActive.set(true);

            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {

                        if (msg instanceof BytesMessage) {
                            byte[] buffer = new byte[(int)((BytesMessage) msg).getBodyLength()];
                            int bytesRead = ((BytesMessage) msg).readBytes(buffer);
                            //if(mServerSocket.isConnected()) {
                            if(inForwardingActive.get()) {
                                mOutputStream.write(buffer, 0, bytesRead);
                                mOutputStream.flush();

                                // record transfer metrics
                                tunnelSender.performanceMonitor.addBytes(bytesRead);
                            }

                        } else if (msg instanceof MapMessage) {
                            MapMessage statusMessage = (MapMessage) msg;
                            int remoteStatus = statusMessage.getInt("status");

                            // ack close
                            if(remoteStatus == 8) {
                                logger.debug("(13) [dst] notified by src port closed by external, close gracefuly");
                                srcClose();
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
                            //logger.error("IS CLOSED: " + mParent.mServerSocket.isClosed());
                            //logger.error("STATE: " + mParent.getState());
                        //}

                    } catch(Exception ex) {
                        logger.error("mParent.mServerSocket isClosed: " + mParent.mServerSocket.isClosed() + " isBound: " +
                                mParent.mServerSocket.isBound() + " isConnected: " + mParent.mServerSocket.isConnected() +
                                " isinputshut: " + mParent.mServerSocket.isInputShutdown() + " isoutputshut: "
                                + mParent.mServerSocket.isOutputShutdown());
                        ex.printStackTrace();
                    }
                }
            };

            String queryString = "stunnel_id='" + tunnelConfig.get("stunnel_id") + "' and client_id='" + clientId + "' and direction='dst'";
            node_from_listener_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);
            logger.debug("(10): [dst] listner: " + node_from_listener_id + " started");
        }

        public void closeListener() {

            if(node_from_listener_id != null) {
                logger.debug("(14) [dst] ForwardThread close() removing node_from_listner_id: " + node_from_listener_id);
                plugin.getAgentService().getDataPlaneService().removeMessageListener(node_from_listener_id);
                node_from_listener_id = null;
            }

        }
        /*
        private void connectionBroken() {

            // set state
            mParent.connectionBroken();

            logger.error("(15) [dst] connectionBroken");
            try {
                mParent.close();
                //close remote
                logger.error("sTunnelId: " + tunnelConfig.get("stunnel_id"));

                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
                request.setParam("action", "closedstclient");
                request.setParam("action_client_id", clientId);
                MsgEvent response = plugin.sendRPC(request);
                if (response.getParam("status") != null) {
                    int status = Integer.parseInt(response.getParam("status"));
                    if (status == 10) {
                        logger.info("(16) [dst] Src port confirmed closed.");
                    } else {
                        logger.error("Error in closing src port: " + response.getParams());
                    }
                } else {
                    logger.error("sendRPC Missing status from response: " + response.getParams());
                }
            } catch (Exception ex) {
                logger.error("connectionBroken: error!");
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                logger.error(sw.toString());
            }

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
                    tunnelSender.performanceMonitor.addBytes(bytesRead);

                    if(bytesRead > 0) {
                        // normal incoming data
                        BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                        bytesMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                        bytesMessage.setStringProperty("direction", "src");
                        bytesMessage.setStringProperty("client_id", clientId);
                        bytesMessage.writeBytes(buffer, 0, bytesRead);
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);

                    } else if (bytesRead == -1) {

                        // (1) socket is closed, stop listening for incoming messages
                        closeListener();

                        // (2) notify remote side that it is time to shut things down.
                        logger.debug("(13) [dst] dst closed before, notify src");
                        MapMessage closeMessage = plugin.getAgentService().getDataPlaneService().createMapMessage();
                        closeMessage.setStringProperty("stunnel_id", tunnelConfig.get("stunnel_id"));
                        closeMessage.setStringProperty("direction", "src");
                        closeMessage.setStringProperty("client_id", clientId);
                        //status message 8 == close gracefully
                        closeMessage.setInt("status", 8);
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, closeMessage);

                        // (3) shutdown session Listener
                        dstClose();
                    }
                }

            } catch (SocketException se) {
                outForwardingActive.set(false);
                /*
                if(!(mParent.getState() == SessionSenderSM.State.waitCloseSender)) {
                    // this will occur normally, when src notifies dst to close
                    logger.error("forwardingActive: " + outForwardingActive.get());
                    logger.error("SocketException: " + se.getMessage());
                    logger.error("IS CLOSED: " + mParent.mServerSocket.isClosed());
                    logger.error("STATE: " + mParent.getState());
                }

                 */

            } catch (IOException e) {
                logger.error("IO EXCEPTION: " + e.getMessage());
                e.printStackTrace();
                // Read/write failed --> connection is broken
                logger.error("run() mParent.mServerSocket isClosed: " + mParent.mServerSocket.isClosed() + " isBound: " +
                        mParent.mServerSocket.isBound() + " isConnected: " + mParent.mServerSocket.isConnected() +
                        " isinputshut: " + mParent.mServerSocket.isInputShutdown() + " isoutputshut: "
                        + mParent.mServerSocket.isOutputShutdown());
                logger.error("IOException error: " + e.getMessage());
                //connectionBroken();
            } catch (Exception ex) {
                logger.error("run: " + ex.getMessage());
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                logger.error(sw.toString());
                //connectionBroken();
            } finally {
                dstClose();
            }

            // Notify parent thread that the connection is broken
            //mParent.connectionBroken();
        }
    }


}


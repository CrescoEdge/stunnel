package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class SocketSender  {

    private PluginBuilder plugin;
    CLogger logger;

    private String sTunnelId;
    private String clientId;

    private String remoteHost;
    private int remotePort;

    private final AtomicBoolean clientThreadsLock;
    private Map<String, ClientThread> clientThreads;


    public SocketController socketController;

    public int bufferSize = 8192;

    public SocketSender(PluginBuilder plugin, SocketController socketController, Map<String,String> tunnelConfig)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.socketController = socketController;
        this.sTunnelId = tunnelConfig.get("stunnel_id");
        this.clientId = tunnelConfig.get("client_id");
        this.remoteHost = tunnelConfig.get("dst_host");
        this.remotePort = Integer.parseInt(tunnelConfig.get("dst_port"));
        clientThreadsLock = new AtomicBoolean();
        clientThreads = Collections.synchronizedMap(new HashMap<>());

        if(tunnelConfig.containsKey("buffer_size")) {
            bufferSize = Integer.parseInt(tunnelConfig.get("buffer_size"));
            logger.error("custom buffer_size: " + bufferSize);
        }

    }

    public boolean start() {
        boolean isStarted = false;
        try {
            setClientThreads(clientId, remoteHost, remotePort);
            getClientThread(clientId).start();
            while (getClientThread(clientId).getStatus() == -1) {
                Thread.sleep(100);
            }
            logger.debug("boolean start() STATUS: " + getClientThread(clientId).getStatus());
            if (getClientThread(clientId).getStatus() == 10) {
                isStarted = true;
            }
            logger.error("(8): [dst] ClientThread started: " + isStarted + " status: " + getClientThread(clientId).getStatus());
        } catch (Exception ex) {
            logger.error("(8): [dst] ClientThread error: " + ex.getMessage());
        }
        return isStarted;
    }

    public void close() {

        synchronized (clientThreadsLock) {

            for (Map.Entry<String, ClientThread> entry : clientThreads.entrySet()) {
                String clientId = entry.getKey();
                ClientThread clientThread = entry.getValue();
                clientThread.close();
                logger.info("Shutting down clientID: " + clientId);
            }
        }

    }

    public boolean closeClient(String clientId) {
        boolean isClosed = false;
        try {
            ClientThread clientThread = getClientThread(clientId);
            if(clientThread != null) {
                clientThread.close();
            } else {
                logger.error("closeClient() client_id: " +clientId + " clientThread == null");
            }
            removeClientThread(clientId);
            isClosed = true;

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return isClosed;
    }

    private ClientThread getClientThread(String clientId) {

        ClientThread clientThread;
        synchronized (clientThreadsLock) {
            clientThread = clientThreads.get(clientId);
        }
        return clientThread;
    }
    private void removeClientThread(String clientId) {

        synchronized (clientThreadsLock) {
            clientThreads.remove(clientId);
        }

    }

    private void setClientThreads(String clientId, String remoteHost, int remotePort) {

        synchronized (clientThreadsLock) {
            clientThreads.put(clientId, new ClientThread(remoteHost, remotePort));
        }

    }

    class ClientThread extends Thread {
        //private int BUFFER_SIZE;

        private AtomicInteger status = new AtomicInteger(-1);

        private Socket mServerSocket;

        private String remoteHost;
        private int remotePort;

        private ForwardThread clientForward;

        public ClientThread(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
            //BUFFER_SIZE = plugin.getConfig().getIntegerParam("stunnel__buffer_size",8192);
        }

        /**
         * Establishes connection to the destination server and
         * starts bidirectional forwarding ot data between the
         * client and the server.
         */

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
                    clientForward.close();
                } else {
                    logger.error("CALLING CLOSE ON NULL clientForward");
                }

                if(mServerSocket != null) {
                    if(!mServerSocket.isClosed()) {
                        mServerSocket.close();
                    }
                }

            } catch (Exception ex) {
                logger.error("ClientThread close() error: " + ex.getMessage());
            }

            //99 is closed
            setStatus(99);

        }


        public void run() {
            InputStream serverIn;
            OutputStream serverOut;

            try {
                logger.error("(9): [dst] connecting to dst port");
                // Connect to the destination server
                mServerSocket = new Socket(remoteHost, remotePort);

                // Turn on keep-alive for both the sockets
                mServerSocket.setKeepAlive(true);


                // Obtain client & server input & output streams
                serverIn = mServerSocket.getInputStream();
                serverOut = mServerSocket.getOutputStream();


                clientForward = new ForwardThread(this, serverIn, serverOut);
                clientForward.start();

                setStatus(10);
                logger.error("(11): [dst] ForwardThread started. status: " + status.get());


            } catch (Exception ex) {

                setStatus(9);
                logger.error("Can not connect to " + remoteHost + " " + remotePort + " status:" + status.get());
                logger.error("Exception: " + ex.getMessage());
                //System.err.println("Can not connect to " + remoteHost + " " + remotePort);
                //TCPForwardServer.DESTINATION_HOST + ":" +
                //TCPForwardServer.DESTINATION_PORT);
                //connectionBroken();
                //return;
                close();
            }


        }
    }

    class ForwardThread extends Thread {
        //private final int BUFFER_SIZE = 8192;
        //private final int BUFFER_SIZE = 8192;

        InputStream mInputStream;
        OutputStream mOutputStream;
        ClientThread mParent;

        boolean forwardingActive = false;

        String node_from_listner_id;
        //BytesMessage bytesMessage;
        /**
         * Creates a new traffic redirection thread specifying
         * its parent, input stream and output stream.
         */
        public ForwardThread(ClientThread aParent, InputStream aInputStream, OutputStream aOutputStream) throws JMSException {
            logger.debug("Plugin " + plugin.getPluginID() + " creating forwarding thread.");
            mParent = aParent;
            mInputStream = aInputStream;
            mOutputStream = aOutputStream;

            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {

                        //byte[] buffer = new byte[bufferSize];

                        if (msg instanceof BytesMessage) {
                            byte[] buffer = new byte[(int)((BytesMessage) msg).getBodyLength()];
                            int bytesRead = ((BytesMessage) msg).readBytes(buffer);
                            mOutputStream.write(buffer, 0, bytesRead);
                            mOutputStream.flush();
                            //logger.error("bufferSize length: " + buffer.length + " bytesRead: " + bytesRead + " body length:" + ((BytesMessage) msg).getBodyLength());
                        }

                    } catch(Exception ex) {
                        logger.error("mParent.mServerSocket isClosed: " + mParent.mServerSocket.isClosed() + " isBound: " +
                                mParent.mServerSocket.isBound() + " isConnected: " + mParent.mServerSocket.isConnected() +
                                " isinputshut: " + mParent.mServerSocket.isInputShutdown() + " isoutputshut: "
                                + mParent.mServerSocket.isOutputShutdown());
                        ex.printStackTrace();
                    }
                }
            };

            String queryString = "stunnel_id='" + sTunnelId + "' and client_id='" + clientId + "' and direction='dst'";
            node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,queryString);
            logger.error("(10): [dst] listner: " + node_from_listner_id + " started");
        }

        public void close () {


            if(node_from_listner_id != null) {
                logger.error("(14) [dst] ForwardThread close() removing node_from_listner_id: " + node_from_listner_id);
                plugin.getAgentService().getDataPlaneService().removeMessageListener(node_from_listner_id);
                node_from_listner_id = null;
            }

            forwardingActive = false;
        }

        private void connectionBroken() {

            mParent.close();
            //close remote
            Map<String,String> tunnelConfig = socketController.getTunnelConfig(sTunnelId);
            MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
            request.setParam("action", "closesrcclient");
            request.setParam("action_client_id", clientId);
            MsgEvent response = plugin.sendRPC(request);
            if(response.getParam("status") != null) {
                int status = Integer.parseInt(response.getParam("status"));
                if(status == 10) {
                    logger.info("(15) [dst] Src port confirmed closed.");
                } else {
                    logger.error("Error in closing src port: " + response.getParams());
                }
            } else {
                logger.error("Missing status from response: " + response.getParams());
            }

        }
        /**
         * Runs the thread. Continuously reads the input stream and
         * writes the read data to the output stream. If reading or
         * writing fail, exits the thread and notifies the parent
         * about the failure.
         */
        public void run() {

            forwardingActive = true;

            byte[] buffer = new byte[bufferSize];
            try {
                while (forwardingActive) {
                    int bytesRead = mInputStream.read(buffer);
                    if (bytesRead == -1) {
                        logger.error("(13) [dst] dst port closed by external");
                        connectionBroken();
                    }

                    if(bytesRead > 0) {
                        BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                        bytesMessage.setStringProperty("stunnel_id", sTunnelId);
                        bytesMessage.setStringProperty("direction", "src");
                        bytesMessage.setStringProperty("client_id", clientId);
                        bytesMessage.writeBytes(buffer, 0, bytesRead);
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT, bytesMessage);
                        //logger.error(String.valueOf(isSent));
                        //logger.debug("Plugin " + plugin.getPluginID() + " writing " + buffer.length + " bytes to stunnel_name:" + sTunnelId);
                    }
                    //mOutputStream.write(buffer, 0, bytesRead);
                    //mOutputStream.flush();
                }
            } catch (IOException e) {
                // Read/write failed --> connection is broken
                logger.error("run() mParent.mServerSocket isClosed: " + mParent.mServerSocket.isClosed() + " isBound: " +
                        mParent.mServerSocket.isBound() + " isConnected: " + mParent.mServerSocket.isConnected() +
                        " isinputshut: " + mParent.mServerSocket.isInputShutdown() + " isoutputshut: "
                        + mParent.mServerSocket.isOutputShutdown());
                logger.error("IOException error: " + e.getMessage());
                connectionBroken();
            } catch (Exception ex) {
                logger.error("SOME EXCEPTION: " + ex.getMessage());
                connectionBroken();
            }

            // Notify parent thread that the connection is broken
            //mParent.connectionBroken();
        }
    }




}





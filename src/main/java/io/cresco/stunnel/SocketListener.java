package io.cresco.stunnel;

import com.google.gson.Gson;
import io.cresco.library.data.TopicType;
import io.cresco.library.messaging.MsgEvent;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import jakarta.jms.*;

import java.io.*;
import java.net.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class SocketListener implements Runnable  {

    private PluginBuilder plugin;
    CLogger logger;

    private String sTunnelId;

    private int srcPort;

    private ServerSocket serverSocket;

    private final AtomicBoolean clientThreadsLock;
    private Map<String, ClientThread> clientThreads;

    private boolean isActive = false;

    private final AtomicBoolean clientStatusLock;
    private Map<String, SocketController.StatusType> clientStatus;

    public int bufferSize;
    private SocketController socketController;

    public SocketListener(PluginBuilder plugin, SocketController socketController, String sTunnelId, int srcPort, int bufferSize)  {
        this.plugin = plugin;
        this.socketController = socketController;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.sTunnelId = sTunnelId;
        this.srcPort = srcPort;
        this.bufferSize = bufferSize;
        clientThreadsLock = new AtomicBoolean();
        clientThreads = Collections.synchronizedMap(new HashMap<>());
        clientStatusLock = new AtomicBoolean();
        clientStatus = Collections.synchronizedMap(new HashMap<>());
    }



    public void close() {

        if(isActive) {
            isActive = false;
        }
        socketController.setTunnelStatus(sTunnelId, SocketController.StatusType.CLOSED);

        //close listener socket
        closeSocket();

    }
    public void run() {

        isActive = true;

        try {
            //logger.info("Plugin " + plugin.getPluginID() + "stunnel_id:" + sTunnelId + " listening on port " + srcPort);
            serverSocket = new ServerSocket(srcPort);
            socketController.setTunnelStatus(sTunnelId, SocketController.StatusType.ACTIVE);

            logger.error("(4): port open and waiting for incoming request on port: " + srcPort);

            while(isActive) {

                try {

                    Socket clientSocket = serverSocket.accept();

                    String clientId = UUID.randomUUID().toString();
                    //logger.error("SOCKET CONNECTED!");

                    //set thread, arrange comm with remote host
                    setClientThreads(this, clientId, clientSocket);
                    //start thread
                    new Thread(getClientThread(clientId)).start();



                }  catch (java.net.SocketException sx) {
                    if(!sx.getMessage().equals("Socket closed")) {
                        logger.error("Socket error: " + srcPort + " error: " + sx.getMessage());
                    }


                } catch(Exception ex) {
                    ex.printStackTrace();
                    logger.error("problem when accepting: " + srcPort + " error: " + ex.getMessage());
                }

            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            synchronized (clientThreadsLock) {

                for (Map.Entry<String, ClientThread> entry : clientThreads.entrySet()) {
                    String clientId = entry.getKey();
                    ClientThread clientThread = entry.getValue();
                    clientThread.close();
                    logger.info("Shutting down clientID: " + clientId);
                }
            }

            //close the main socket
            closeSocket();
        }
    }

    public void setClientStatus(String clientId, SocketController.StatusType statusType) {

        synchronized (clientStatusLock) {
            clientStatus.put(clientId,statusType);
        }

    }
    public SocketController.StatusType getclientStatus(String clientId) {

        SocketController.StatusType statusType;

        synchronized (clientStatusLock) {
            statusType = clientStatus.get(clientId);
        }
        return statusType;
    }

    private void setClientThreads(SocketListener socketListener, String clientId, Socket clientSocket) {

        synchronized (clientThreadsLock) {
            clientThreads.put(clientId, new ClientThread(socketListener, clientSocket,clientId));
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

    class ClientThread extends Thread {

        private Socket mClientSocket;
        private boolean mForwardingActive = false;

        private ForwardThread clientForward;

        private String clientId;

        public  SocketListener socketListener;

        private Gson gson;

        public ClientThread(SocketListener socketListener, Socket aClientSocket, String clientId) {
            this.socketListener = socketListener;
            mClientSocket = aClientSocket;
            this.clientId = clientId;
            gson = new Gson();
            logger.error("(5): started Client Thread with client_id" + clientId);
        }

        public void close() {

            try {

                if(clientForward != null) {
                    clientForward.close();
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
        public void run() {
            InputStream clientIn;
            OutputStream clientOut;

            try {

                //logger.info("Plugin " + plugin.getPluginID() + " creating client socket streams.");

                // Turn on keep-alive for both the sockets
                mClientSocket.setKeepAlive(true);

                // Obtain client & server input & output streams
                clientIn = mClientSocket.getInputStream();
                clientOut = mClientSocket.getOutputStream();

                // Start forwarding data between server and client
                mForwardingActive = true;
                //will start listening now
                clientForward = new ForwardThread(this, clientIn, clientOut, clientId);
                //send message to remote to bring up port and list

                logger.error("(7): sending message to remote host to get ready for incoming data");

                Map<String,String> tunnelConfig = socketController.getTunnelConfig(sTunnelId);
                //set client_id
                tunnelConfig.put("client_id",clientId);
                //send message to remote plugin and check if dst host/port is listening
                MsgEvent request = plugin.getGlobalPluginMsgEvent(MsgEvent.Type.CONFIG, tunnelConfig.get("dst_region"), tunnelConfig.get("dst_agent"), tunnelConfig.get("dst_plugin"));
                request.setParam("action", "configdsttunnel");
                request.setParam("action_tunnel_config", gson.toJson(tunnelConfig));
                MsgEvent response = plugin.sendRPC(request);

                if(response.getParam("status") != null) {
                    int status = Integer.parseInt(response.getParam("status"));
                    if(status == 10) {
                        clientForward.start();
                        logger.error("(12): clientForward started");
                    } else {
                        logger.error("Error in config of dst tunnel: " + response.getParams());
                        close();
                    }
                } else {
                    logger.error("Error in config of dst tunnel: Missing status from response: " + response.getParams());
                }

            } catch (Exception ex) {
                ex.printStackTrace();
                return;
            }

        }

    }

    class ForwardThread extends Thread {
        //private final int BUFFER_SIZE;

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
        String clientId;
        public ForwardThread(ClientThread aParent, InputStream aInputStream, OutputStream aOutputStream, String clientId) {
            //logger.info("Plugin " + plugin.getPluginID() + " creating forwarding thread.");
            mParent = aParent;
            mInputStream = aInputStream;
            mOutputStream = aOutputStream;
            this.clientId = clientId;
            //BUFFER_SIZE = plugin.getConfig().getIntegerParam("stunnel__buffer_size",8192);


            //messages in
            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {

                        if (msg instanceof BytesMessage) {
                            byte[] buffer = new byte[(int)((BytesMessage) msg).getBodyLength()];
                            int bytesRead = ((BytesMessage) msg).readBytes(buffer);
                            mOutputStream.write(buffer, 0, bytesRead);
                            mOutputStream.flush();
                            //logger.error("bufferSize length: " + buffer.length + " bytesRead: " + bytesRead + " body length:" + ((BytesMessage) msg).getBodyLength());
                        }

                    } catch(Exception ex) {

                        ex.printStackTrace();
                    }
                }
            };

            if(node_from_listner_id != null) {
                logger.error("WHY IS LIST NO NULL? l_id:" + node_from_listner_id );
            }

            String queryString = "stunnel_id='" + sTunnelId + "' and client_id='" + clientId + "' and direction='src'";
            node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL,ml,queryString);

            logger.error("(6): src listner:" + node_from_listner_id + " started");

            forwardingActive = true;

        }

        public void close () {

            if(node_from_listner_id != null) {
                logger.error("(14) ForwardThread close() removing node_from_listner_id: " + node_from_listner_id);
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

        }

        /**
         * Runs the thread. Continuously reads the input stream and
         * writes the read data to the output stream. If reading or
         * writing fail, exits the thread and notifies the parent
         * about the failure.
         */
        public void run() {
            byte[] buffer = new byte[bufferSize];
            try {
                while (forwardingActive) {

                    int bytesRead = mInputStream.read(buffer);
                    if (bytesRead == -1) {
                        logger.error("(13) src port closed by external");
                        connectionBroken();

                    }

                    if(bytesRead > 0) {
                        //logger.error("bytesRead: " + bytesRead);
                        BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                        bytesMessage.setStringProperty("stunnel_id", sTunnelId);
                        bytesMessage.setStringProperty("direction", "dst");
                        bytesMessage.setStringProperty("client_id", clientId);
                        bytesMessage.writeBytes(buffer, 0, bytesRead);
                        plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.GLOBAL, bytesMessage);
                        //logger.error(String.valueOf(isSent));

                        //logger.debug("Plugin " + plugin.getPluginID() + " writing " + buffer.length + " bytes to stunnel_name:" + sTunnelId);
                    }

                }
            } catch (IOException e) {
                // Read/write failed --> connection is broken
                logger.error("run() mParent.mServerSocket isClosed: " + mParent.mClientSocket.isClosed() + " isBound: " +
                        mParent.mClientSocket.isBound() + " isConnected: " + mParent.mClientSocket.isConnected() +
                        " isinputshut: " + mParent.mClientSocket.isInputShutdown() + " isoutputshut: "
                        + mParent.mClientSocket.isOutputShutdown());
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






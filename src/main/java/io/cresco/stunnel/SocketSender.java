package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class SocketSender  {

    private PluginBuilder plugin;
    CLogger logger;

    private String sTunnelId;
    private String clientId;

    private String remoteHost;
    private int remotePort;

    public SocketController socketController;

    public SocketSender(PluginBuilder plugin, SocketController socketController, Map<String,String> tunnelConfig)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.socketController = socketController;
        this.sTunnelId = tunnelConfig.get("stunnel_id");
        this.clientId = tunnelConfig.get("client_id");
        this.remoteHost = tunnelConfig.get("dst_host");
        this.remotePort = Integer.parseInt(tunnelConfig.get("dst_port"));
    }

    public void go() {

        ClientThread clientThread = new ClientThread(remoteHost, remotePort);
        clientThread.start();

    }

    class ClientThread extends Thread {
        private final int BUFFER_SIZE = 8192;

        private Socket mServerSocket;
        private boolean mForwardingActive = false;

        private String remoteHost;
        private int remotePort;

        public ClientThread(String remoteHost, int remotePort) {
            this.remoteHost = remoteHost;
            this.remotePort = remotePort;
        }

        /**
         * Establishes connection to the destination server and
         * starts bidirectional forwarding ot data between the
         * client and the server.
         */
        public void run() {
            InputStream serverIn;
            OutputStream serverOut;

            try {
                // Connect to the destination server
                mServerSocket = new Socket(remoteHost, remotePort);

                // Turn on keep-alive for both the sockets
                mServerSocket.setKeepAlive(true);


                // Obtain client & server input & output streams
                serverIn = mServerSocket.getInputStream();
                serverOut = mServerSocket.getOutputStream();

                ForwardThread clientForward = new ForwardThread(this, serverIn, serverOut);
                clientForward.start();


            } catch (IOException | JMSException ioe) {
                System.err.println("Can not connect to " + remoteHost + " " + remotePort);
                        //TCPForwardServer.DESTINATION_HOST + ":" +
                        //TCPForwardServer.DESTINATION_PORT);
                //connectionBroken();
                return;
            }


        }
    }

    class ForwardThread extends Thread {
        //private final int BUFFER_SIZE = 8192;
        private final int BUFFER_SIZE = 8192;

        InputStream mInputStream;
        OutputStream mOutputStream;
        ClientThread mParent;

        String node_from_listner_id;
        //BytesMessage bytesMessage;
        /**
         * Creates a new traffic redirection thread specifying
         * its parent, input stream and output stream.
         */
        public ForwardThread(ClientThread aParent, InputStream aInputStream, OutputStream aOutputStream) throws JMSException {
            logger.info("Plugin " + plugin.getPluginID() + " creating forwarding thread.");
            mParent = aParent;
            mInputStream = aInputStream;
            mOutputStream = aOutputStream;
            //bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
            //bytesMessage.setStringProperty("stunnel_name",sTunnelId);

            //messages in
            MessageListener ml = new MessageListener() {
                public void onMessage(Message msg) {
                    try {

                        byte[] buffer = new byte[BUFFER_SIZE];

                        if (msg instanceof BytesMessage) {
                            int bytesRead = ((BytesMessage) msg).readBytes(buffer);
                            logger.info("Message In: " + new String(buffer));
                            mOutputStream.write(buffer, 0, bytesRead);
                            mOutputStream.flush();
                        }

                    } catch(Exception ex) {

                        ex.printStackTrace();
                    }
                }
            };

            String queryString = "stunnel_id='" + sTunnelId + "' and client_id='" + clientId + "' and direction='dst'";
            node_from_listner_id = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.AGENT,ml,queryString);
        }

        /**
         * Runs the thread. Continuously reads the input stream and
         * writes the read data to the output stream. If reading or
         * writing fail, exits the thread and notifies the parent
         * about the failure.
         */
        public void run() {
            byte[] buffer = new byte[BUFFER_SIZE];
            try {
                while (true) {
                    int bytesRead = mInputStream.read(buffer);
                    if (bytesRead == -1)
                        break; // End of stream is reached --> exit

                    BytesMessage bytesMessage = plugin.getAgentService().getDataPlaneService().createBytesMessage();
                    bytesMessage.setStringProperty("stunnel_id",sTunnelId);
                    bytesMessage.setStringProperty("direction","src");
                    bytesMessage.setStringProperty("client_id",clientId);

                    bytesMessage.writeBytes(buffer);
                    plugin.getAgentService().getDataPlaneService().sendMessage(TopicType.AGENT,bytesMessage);

                    logger.info("Plugin " + plugin.getPluginID() + " writing " + buffer.length + " bytes to stunnel_name:" + sTunnelId);
                    //mOutputStream.write(buffer, 0, bytesRead);
                    //mOutputStream.flush();
                }
            } catch (IOException e) {
                // Read/write failed --> connection is broken
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }

            // Notify parent thread that the connection is broken
            //mParent.connectionBroken();
        }
    }




}




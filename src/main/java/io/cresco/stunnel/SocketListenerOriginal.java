package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

//https://www.nakov.com/books/inetjava/source-code-html/Chapter-1-Sockets/1.4-TCP-Sockets/TCPForwardServer.java.html

public class SocketListenerOriginal implements Runnable  {

    private PluginBuilder plugin;
    CLogger logger;

    private String sTunnelId;

    private int SOURCE_PORT;

    public SocketListenerOriginal(PluginBuilder plugin, String sTunnelId, int SOURCE_PORT)  {
        this.plugin = plugin;
        logger = plugin.getLogger(this.getClass().getName(), CLogger.Level.Info);
        this.sTunnelId = sTunnelId;
        this.SOURCE_PORT = SOURCE_PORT;
    }

    public void run() {

        try {
            logger.info("Plugin " + plugin.getPluginID() + "stunnel_id:" + sTunnelId + " listening on port " + SOURCE_PORT);

            ServerSocket serverSocket = new ServerSocket(SOURCE_PORT);

            while(plugin.isActive()) {
                try {

                    Socket clientSocket = serverSocket.accept();
                    ClientThread clientThread = new ClientThread(clientSocket);
                    clientThread.start();

                } catch(Exception ex) {
                    ex.printStackTrace();
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    class ClientThread extends Thread {

        private Socket mClientSocket;
        private boolean mForwardingActive = false;

        public ClientThread(Socket aClientSocket) {
            mClientSocket = aClientSocket;
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

                logger.info("Plugin " + plugin.getPluginID() + " creating client socket streams.");

                // Turn on keep-alive for both the sockets
                mClientSocket.setKeepAlive(true);

                // Obtain client & server input & output streams
                clientIn = mClientSocket.getInputStream();
                clientOut = mClientSocket.getOutputStream();

                // Start forwarding data between server and client
                mForwardingActive = true;
                ForwardThread clientForward = new ForwardThread(this, clientIn, clientOut);
                clientForward.start();


            } catch (IOException | JMSException ioe) {
                ioe.printStackTrace();
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
                            logger.info("WE GOT SOMETHING BYTES L");
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


            String queryString = "stunnel_name='" + sTunnelId + "' and direction='outgoing'";
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
                    bytesMessage.setStringProperty("stunnel_name",sTunnelId);
                    bytesMessage.setStringProperty("direction","incoming");
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

    /**
     * ClientThread is responsible for starting forwarding between
     * the client and the server. It keeps track of the client and
     * servers sockets that are both closed on input/output error
     * durinf the forwarding. The forwarding is bidirectional and
     * is performed by two ForwardThread instances.
     */




}




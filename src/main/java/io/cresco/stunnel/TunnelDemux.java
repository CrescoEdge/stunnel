package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ONE dataplane consumer per tunnel per direction, demultiplexed to per-client handlers in Java.
 *
 * <p>Previously stunnel created a JMS consumer per CLIENT CONNECTION, selecting on client_id.
 * That was structurally hostile: {@code ActiveMQMessageConsumer.setMessageListener} calls
 * {@code ActiveMQSession.stop()}, which stops EVERY consumer on the shared dataplane session and
 * needs each one's dispatch mutex — so every new tunnel session briefly stalled the whole node's
 * dataplane (wsapi, CEP, every other tunnel), and any lock held across that call deadlocked the
 * agent permanently (2026-08-19 outage). Registering a session is now a map insert: no JMS call,
 * no session stop/start, no listener add/remove churn, and no lock is ever held across JMS.
 *
 * <p>Ordering is preserved per client. A consumer's JMS dispatch is serial, so the only reordering
 * risk is the buffered→live handoff for the first-bytes race; {@code register} drains under the
 * same lock that {@code dispatch} uses to decide buffer-vs-deliver, and delivery happens OUTSIDE
 * that lock. Messages for a client that was never announced via {@link #expect} are dropped, so a
 * late or stray delivery cannot grow the buffer map without bound.
 */
public class TunnelDemux {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final String stunnelId;
    private final String direction;
    private final int maxBufferedPerClient;

    private final Map<String, MessageListener> handlers = new ConcurrentHashMap<>();
    private final Map<String, List<Message>> pending = new ConcurrentHashMap<>();
    private final Map<String, Boolean> overflowed = new ConcurrentHashMap<>();
    private final Object lock = new Object();   // guards the buffer/handler handoff ONLY

    private volatile String jmsListenerId;
    private volatile boolean closed;

    public TunnelDemux(PluginBuilder plugin, String stunnelId, String direction) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(getClass().getName(), CLogger.Level.Info);
        this.stunnelId = stunnelId;
        this.direction = direction;
        this.maxBufferedPerClient = plugin.getConfig().getIntegerParam("stunnel_demux_buffer_max", 256);
    }

    /** Subscribe the tunnel's single consumer. Called once per tunnel, never per session. */
    public void open() throws Exception {
        String selector = String.format("stunnel_id='%s' AND direction='%s'", stunnelId, direction);
        MessageListener ml = this::dispatch;
        // No lock held across this JMS call, by construction.
        String id = plugin.getAgentService().getDataPlaneService()
                .addMessageListener(TopicType.GLOBAL, ml, selector);
        if (id == null) {
            throw new IllegalStateException("null listener id for tunnel " + stunnelId + " (" + direction + ")");
        }
        jmsListenerId = id;
        logger.info("Tunnel demux open: stunnel_id=" + stunnelId + " direction=" + direction);
    }

    private void dispatch(Message msg) {
        if (closed) return;
        String clientId;
        try {
            clientId = msg.getStringProperty("client_id");
        } catch (Exception ex) {
            logger.warn("demux: message without readable client_id on " + stunnelId + ": " + ex.getMessage());
            return;
        }
        if (clientId == null) return;

        MessageListener handler;
        synchronized (lock) {
            handler = handlers.get(clientId);
            if (handler == null) {
                List<Message> buf = pending.get(clientId);
                if (buf == null) {
                    // never announced (or already finished): stray/late delivery, drop it
                    logger.debug("demux: dropping message for unknown client " + clientId + " on " + stunnelId);
                    return;
                }
                if (buf.size() >= maxBufferedPerClient) {
                    if (overflowed.putIfAbsent(clientId, Boolean.TRUE) == null) {
                        buf.clear();
                        logger.error("demux: pre-registration buffer exceeded " + maxBufferedPerClient
                                + " messages for client " + clientId + " on " + stunnelId + " - failing session");
                    }
                    return;
                }
                buf.add(msg);
                return;
            }
        }
        // Deliver outside the lock: handlers hand off to a Netty event loop, and holding a lock
        // across that work is exactly what deadlocked the old per-session relay.
        handler.onMessage(msg);
    }

    /** Announce a client whose session is being set up, so its early messages are buffered. */
    public void expect(String clientId) {
        synchronized (lock) {
            pending.putIfAbsent(clientId, new ArrayList<>());
            overflowed.remove(clientId);
        }
    }

    /**
     * Attach the session's handler, draining anything buffered since {@link #expect}.
     * @return false if the client overflowed its buffer or the demux is closed — fail the session.
     */
    public boolean register(String clientId, MessageListener handler) {
        List<Message> drain;
        synchronized (lock) {
            if (closed || overflowed.containsKey(clientId)) {
                return false;
            }
            drain = pending.remove(clientId);
            handlers.put(clientId, handler);
        }
        if (drain != null) {
            for (Message m : drain) {
                handler.onMessage(m);
            }
        }
        return true;
    }

    /** Drop a client: its handler, any buffer, and its overflow flag. No JMS involved. */
    public void discard(String clientId) {
        synchronized (lock) {
            handlers.remove(clientId);
            pending.remove(clientId);
            overflowed.remove(clientId);
        }
    }

    public int activeClients() {
        return handlers.size();
    }

    /** Remove the tunnel's consumer. Idempotent; the JMS call happens outside the lock. */
    public void close() {
        String id;
        synchronized (lock) {
            if (closed) return;
            closed = true;
            handlers.clear();
            pending.clear();
            overflowed.clear();
            id = jmsListenerId;
            jmsListenerId = null;
        }
        if (id != null) {
            try {
                plugin.getAgentService().getDataPlaneService().removeMessageListener(id);
                logger.info("Tunnel demux closed: stunnel_id=" + stunnelId + " direction=" + direction);
            } catch (Exception ex) {
                logger.warn("demux close failed for " + stunnelId + ": " + ex.getMessage());
            }
        }
    }
}

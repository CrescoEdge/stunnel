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
 *
 * <p><b>Pre-registration budget.</b> A client's messages are held from {@link #expect} until its
 * {@link #register} within {@code stunnel_demux_buffer_max} messages (default 256) and
 * {@code stunnel_demux_buffer_bytes} (default 24 MiB). The budget is what this node advertises to a
 * protocol-2 SRC ({@code fc_prereg_msgs}, {@code fc_prereg_bytes}), which then paces from its first byte
 * and cannot exceed it: early data is backpressured end to end instead of dropped. Before, the sender was
 * unpaced until the first ack and a fast client outran a slow target connect (256 x 4 KiB reads is ~1 MB)
 * and the session was failed. Exceeding the budget (only an old, unpaced SRC can) still fails the session,
 * loudly: an ERROR with the counts, and {@link #overflows()}.
 */
public class TunnelDemux {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final String stunnelId;
    private final String direction;
    private final int maxBufferedPerClient;     // per-client pre-registration budget, messages (0 = no count cap)
    private final long maxBufferedBytes;        // per-client pre-registration budget, payload bytes

    private final Map<String, MessageListener> handlers = new ConcurrentHashMap<>();
    private final Map<String, List<Message>> pending = new ConcurrentHashMap<>();
    private final Map<String, Boolean> overflowed = new ConcurrentHashMap<>();
    private final Map<String, long[]> pendingCost = new ConcurrentHashMap<>();   // guarded by lock
    private final java.util.concurrent.atomic.AtomicLong overflows = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.Set<String> draining = new java.util.HashSet<>();   // guarded by lock
    private final Object lock = new Object();   // guards the buffer/handler handoff ONLY

    private volatile String jmsListenerId;
    private volatile boolean closed;

    public static final long DEFAULT_BUFFER_BYTES = 24L * 1024 * 1024;

    public TunnelDemux(PluginBuilder plugin, String stunnelId, String direction) {
        this(plugin, plugin.getLogger(TunnelDemux.class.getName(), CLogger.Level.Info), stunnelId, direction,
                plugin.getConfig().getLongParam("stunnel_demux_buffer_bytes", DEFAULT_BUFFER_BYTES),
                plugin.getConfig().getIntegerParam("stunnel_demux_buffer_max", 256));
    }

    /** Buffering core without a dataplane (open/close need the plugin; tests pass null). */
    TunnelDemux(PluginBuilder plugin, CLogger logger, String stunnelId, String direction, long maxBufferedBytes, int maxBufferedPerClient) {
        this.plugin = plugin;
        this.logger = logger;
        this.stunnelId = stunnelId;
        this.direction = direction;
        this.maxBufferedBytes = maxBufferedBytes > 0 ? maxBufferedBytes : DEFAULT_BUFFER_BYTES;
        this.maxBufferedPerClient = Math.max(0, maxBufferedPerClient);
    }

    /** The per-client pre-registration budget in payload bytes (advertised to protocol-2 SRCs). */
    public long bufferBudgetBytes() { return maxBufferedBytes; }

    /** The per-client pre-registration budget in messages (advertised to protocol-2 SRCs). */
    public long bufferBudgetMsgs() { return maxBufferedPerClient > 0 ? maxBufferedPerClient : 1L << 20; }

    /** Sessions failed because their pre-registration buffer went over budget. */
    public long overflows() { return overflows.get(); }

    /** A message's payload bytes (the SRC stamps dp_bytes; else the body length; 0 for control messages). */
    static long payload(Message m) {
        long payload = 0;
        try {
            if (m.propertyExists("dp_bytes")) payload = m.getIntProperty("dp_bytes");
            else if (m instanceof jakarta.jms.BytesMessage) payload = ((jakarta.jms.BytesMessage) m).getBodyLength();
        } catch (Exception ignore) { }
        return Math.max(0, payload);
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
        boolean lateOpen;
        synchronized (lock) {
            if (closed) {
                // close() already ran (the bounded open timed out and gave up on us): nothing will
                // ever remove the consumer we just created, so remove it here or it leaks
                lateOpen = true;
            } else {
                jmsListenerId = id;
                lateOpen = false;
            }
        }
        if (lateOpen) {
            try {
                plugin.getAgentService().getDataPlaneService().removeMessageListener(id);
            } catch (Exception ex) {
                logger.warn("demux late-open consumer removal failed for " + stunnelId + ": " + ex.getMessage());
            }
            throw new IllegalStateException("demux closed during open for tunnel " + stunnelId + " (" + direction + ")");
        }
        logger.info("Tunnel demux open: stunnel_id=" + stunnelId + " direction=" + direction);
    }

    void dispatch(Message msg) {
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
            // While a drain is in progress the handler is registered but the buffered backlog has
            // not been delivered yet, so live messages MUST keep queueing behind it or the stream
            // reorders — the exact corruption the pre-registration buffer exists to prevent.
            if (handler == null || draining.contains(clientId)) {
                List<Message> buf = pending.get(clientId);
                if (buf == null) {
                    // never announced (or already finished): stray/late delivery, drop it
                    logger.debug("demux: dropping message for unknown client " + clientId + " on " + stunnelId);
                    return;
                }
                if (overflowed.containsKey(clientId)) return;   // already failed; register() reports it
                long[] used = pendingCost.computeIfAbsent(clientId, k -> new long[1]);
                long c = payload(msg);
                if (used[0] + c > maxBufferedBytes || (maxBufferedPerClient > 0 && buf.size() >= maxBufferedPerClient)) {
                    overflowed.put(clientId, Boolean.TRUE);
                    overflows.incrementAndGet();
                    logger.error("demux: pre-registration buffer over budget for client " + clientId + " on " + stunnelId
                            + " (" + direction + "): " + buf.size() + " messages / " + used[0] + " of " + maxBufferedBytes
                            + " bytes held (budget " + maxBufferedPerClient + " messages / " + maxBufferedBytes + " bytes), next message "
                            + c + " bytes - failing session (the sender did not pace to this node's fc_prereg budget; an old SRC?)");
                    buf.clear();
                    used[0] = 0;
                    return;
                }
                used[0] += c;
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
            pendingCost.put(clientId, new long[1]);
            overflowed.remove(clientId);
        }
    }

    /**
     * Attach the session's handler, draining anything buffered since {@link #expect}.
     * @return false if the client overflowed its buffer or the demux is closed — fail the session.
     */
    public boolean register(String clientId, MessageListener handler) {
        synchronized (lock) {
            if (closed || overflowed.containsKey(clientId)) {
                return false;
            }
            pending.putIfAbsent(clientId, new ArrayList<>());
            handlers.put(clientId, handler);
            draining.add(clientId);   // dispatch keeps buffering until the backlog is delivered
        }
        // Drain in passes: anything that arrives mid-drain lands in the buffer (dispatch sees
        // draining=true) and is picked up by the next pass, so per-client order is exact. Delivery
        // happens OUTSIDE the lock, so no lock is ever held across handler work.
        try {
            while (true) {
                List<Message> batch;
                synchronized (lock) {
                    List<Message> buf = pending.get(clientId);
                    if (buf == null || buf.isEmpty()) {
                        pending.remove(clientId);
                        pendingCost.remove(clientId);
                        draining.remove(clientId);
                        return true;
                    }
                    batch = new ArrayList<>(buf);
                    buf.clear();
                    long[] used = pendingCost.get(clientId);
                    if (used != null) used[0] = 0;
                }
                for (Message m : batch) {
                    handler.onMessage(m);
                }
            }
        } catch (RuntimeException ex) {
            synchronized (lock) {
                draining.remove(clientId);
            }
            throw ex;
        }
    }

    /** Drop a client: its handler, any buffer, and its overflow flag. No JMS involved. */
    public void discard(String clientId) {
        synchronized (lock) {
            handlers.remove(clientId);
            pending.remove(clientId);
            pendingCost.remove(clientId);
            overflowed.remove(clientId);
            draining.remove(clientId);
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
            pendingCost.clear();
            overflowed.clear();
            draining.clear();
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

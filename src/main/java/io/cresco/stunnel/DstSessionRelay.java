package io.cresco.stunnel;

import io.cresco.library.data.TopicType;
import io.cresco.library.plugin.PluginBuilder;
import io.cresco.library.utilities.CLogger;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-client DST-side dataplane subscription that is attached BEFORE the target connect is
 * initiated, buffering payload until the target channel is active.
 *
 * The configdstsession RPC reply ("status 10") is sent when the target connect is merely
 * initiated; the SRC side starts forwarding the moment that reply lands. Without this relay the
 * dataplane listener only existed once the target channel became active, so the first messages of
 * a session raced the listener attach and were silently dropped (JMS topics do not retain).
 * Buffered and live messages are delivered under one lock, preserving arrival order across the
 * buffered->direct handoff.
 */
public class DstSessionRelay {

    private final PluginBuilder plugin;
    private final CLogger logger;
    private final Object lock = new Object();
    private final List<Message> buffered = new ArrayList<>();
    private MessageListener target;   // guarded by lock; null until the session handler activates
    private String jmsListenerId;     // guarded by lock; null after close
    private boolean closed;           // guarded by lock

    public DstSessionRelay(PluginBuilder plugin) {
        this.plugin = plugin;
        this.logger = plugin.getLogger(getClass().getName(), CLogger.Level.Info);
    }

    /** Subscribe for this session's payload. Call before initiating the target connect. */
    public void attach(String stunnelId, String clientId) throws Exception {
        MessageListener ml = msg -> {
            synchronized (lock) {
                if (closed) {
                    return;
                }
                if (target == null) {
                    buffered.add(msg);
                } else {
                    target.onMessage(msg);
                }
            }
        };
        String queryString = String.format("stunnel_id='%s' AND client_id='%s' AND direction='dst'", stunnelId, clientId);
        synchronized (lock) {
            this.jmsListenerId = plugin.getAgentService().getDataPlaneService().addMessageListener(TopicType.GLOBAL, ml, queryString);
        }
    }

    /** Drain buffered messages into the real listener and hand it all future deliveries. */
    public void activate(MessageListener listener) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            for (Message m : buffered) {
                listener.onMessage(m);
            }
            buffered.clear();
            target = listener;
        }
    }

    /** Remove the subscription. Idempotent. */
    public void close() {
        synchronized (lock) {
            closed = true;
            buffered.clear();
            target = null;
            if (jmsListenerId != null) {
                try {
                    plugin.getAgentService().getDataPlaneService().removeMessageListener(jmsListenerId);
                } catch (Exception e) {
                    logger.warn("Failed to remove DST session listener: " + e.getMessage());
                }
                jmsListenerId = null;
            }
        }
    }
}

package io.cresco.stunnel;

/**
 * src-&gt;dst credit flow control state for one SRC session. Confined to the session's event loop.
 *
 * <p>The DST acks what reached its target socket (MapMessage status 7, {@code fc_bytes}); the SRC stops
 * reading its client once the unacked total reaches the window and resumes at half.
 *
 * <p><b>Before the first ack.</b> The DST's target connection opens asynchronously, and until it does the
 * DST's {@link TunnelDemux} holds this session's messages. Pacing used to arm only on the first ack (so
 * a non-acking DST behaved as before), which left that pre-registration window unpaced: a fast client
 * outran a slow target connect, the DST's buffer overflowed and the session was failed (the speed
 * suite's 4 KiB stunnel cell under host load). Protocol 2 closes it: the SRC asks for {@code fc=2}
 * in configdstsession, a protocol-2 DST answers with its pre-registration budget
 * ({@code fc_prereg_bytes}), and the SRC paces from the first byte so it can never send more than that
 * budget before the DST has registered. Both sides then count every data message as its payload plus
 * {@link #MSG_OVERHEAD} ("cost"), so the budget bounds the buffered message count as well as the bytes.
 * An old DST answers without {@code fc_prereg_bytes}; the SRC then keeps the old behaviour.
 */
final class SrcFlowControl {

    /** Protocol version the SRC requests (session config {@code fc}) and a DST that supports it honours. */
    static final String PROTOCOL = "2";
    /** Per-message cost on top of the payload, protocol 2 (both sides). Roughly a JMS message's heap overhead. */
    static final int MSG_OVERHEAD = 1024;
    /** The DST acks every this many delivered cost units; a pre-ack limit below 2x this could stall. */
    static final long ACK_EVERY = 1024L * 1024;
    static final long MIN_PREACK = 2 * ACK_EVERY;

    private long outstanding;
    private boolean acked;       // first ack seen: the DST speaks the ack protocol
    private boolean paused;
    private boolean costMode;    // protocol 2 negotiated
    private long preAckLimit;    // 0 = no pacing before the first ack (old DST)

    /**
     * Apply the DST's answer to the session request.
     * @param dstPreregBudget the DST's pre-registration budget ({@code fc_prereg_bytes}), or &lt;= 0 when the DST did not send one
     * @param maxMessage the largest single message this SRC sends (its read chunk), in payload bytes
     * @param window the flow-control window
     * @return the pre-ack limit in cost units, 0 when pacing before the first ack is off
     */
    long negotiate(long dstPreregBudget, long maxMessage, long window) {
        if (dstPreregBudget <= 0) {
            costMode = false;
            preAckLimit = 0;
            return 0;
        }
        costMode = true;
        // the limit is checked after a message is counted, so one more message can land on top of it
        long limit = dstPreregBudget - (maxMessage + MSG_OVERHEAD);
        if (window > 0) limit = Math.min(limit, window);
        preAckLimit = limit >= MIN_PREACK ? limit : 0;
        return preAckLimit;
    }

    boolean costMode() { return costMode; }

    long cost(long payload) { return costMode ? payload + MSG_OVERHEAD : payload; }

    /** A data message went out. @return true when reading must pause now. */
    boolean onSent(long payload, long window) {
        outstanding += cost(payload);
        long limit = acked ? window : preAckLimit;
        if (!paused && limit > 0 && outstanding >= limit) {
            paused = true;
            return true;
        }
        return false;
    }

    /** The DST acked {@code ackCost} units. @return true when reading must resume now. */
    boolean onAck(long ackCost, long window) {
        acked = true;
        outstanding -= ackCost;
        if (outstanding < 0) outstanding = 0;
        if (paused && (window <= 0 || outstanding <= window / 2)) {
            paused = false;
            return true;
        }
        return false;
    }

    long outstanding() { return outstanding; }
    boolean paused() { return paused; }
}

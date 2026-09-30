package io.cresco.stunnel;

/**
 * src-&gt;dst credit flow control state for one SRC session. Confined to the session's event loop.
 *
 * <p>The DST acks what reached its target socket (MapMessage status 7: {@code fc_bytes}, and for
 * protocol 2 also {@code fc_msgs}); the SRC stops reading its client once the unacked total reaches the
 * window and resumes at half.
 *
 * <p><b>Before the first ack.</b> The DST's target connection opens asynchronously, and until it does the
 * DST's {@link TunnelDemux} holds this session's messages (256 of them). Pacing used to arm only on the
 * first ack (so a non-acking DST behaved as before), which left that pre-registration window unpaced: a
 * fast client outran a slow target connect, the DST's buffer overflowed and the session was failed. Protocol
 * 2 closes it: the SRC asks for {@code fc=2} in configdstsession, a protocol-2 DST answers with its
 * pre-registration budget ({@code fc_prereg_bytes}, {@code fc_prereg_msgs}), and the SRC paces from the
 * first byte so it never sends more than that budget before the DST has registered.
 *
 * <p><b>Messages, not only bytes.</b> A tunnel's DST consumer is a topic subscriber, and the broker discards
 * a slow topic subscriber's messages past its pending-message limit (prefetch x 2.5, ~350 in all at the
 * default prefetch 100). A 16 MiB byte window of 4 KiB reads is ~4,000 messages, far past it, so under host
 * load the broker dropped tunnel data and the stream was silently truncated. Protocol 2 also windows
 * messages ({@code stunnel_fc_window_msgs}, default 128 in flight per session) and the DST acks them.
 * An old DST answers without {@code fc_prereg_*}; the SRC then keeps the old behaviour.
 */
final class SrcFlowControl {

    /** Protocol version the SRC requests (session config {@code fc}) and a DST that supports it honours. */
    static final String PROTOCOL = "2";
    /** The DST acks every this many delivered bytes ... */
    static final long ACK_EVERY_BYTES = 1024L * 1024;
    /** ... or (protocol 2) every this many delivered messages, whichever comes first. */
    static final int ACK_EVERY_MSGS = 16;
    /** Smallest limits that can always be acked against: resume happens at half, acks come every ACK_EVERY_*. */
    static final long MIN_BYTES = 2 * ACK_EVERY_BYTES;
    static final int MIN_MSGS = 2 * ACK_EVERY_MSGS;
    static final int DEFAULT_WINDOW_MSGS = 128;

    private long outBytes;
    private long outMsgs;
    private boolean acked;       // first ack seen: the DST speaks the ack protocol
    private boolean paused;
    private boolean proto2;      // protocol 2 negotiated
    private long preAckBytes;    // 0 = no pacing before the first ack (old DST)
    private long preAckMsgs;

    /**
     * Apply the DST's answer to the session request.
     * @param dstBudgetBytes the DST's pre-registration byte budget ({@code fc_prereg_bytes}), &lt;= 0 when absent (old DST)
     * @param dstBudgetMsgs the DST's pre-registration message budget ({@code fc_prereg_msgs}), &lt;= 0 when absent
     * @param maxMessage the largest single message this SRC sends (its read chunk)
     * @return true when the session paces from its first byte
     */
    boolean negotiate(long dstBudgetBytes, long dstBudgetMsgs, long maxMessage, long windowBytes, int windowMsgs) {
        proto2 = dstBudgetBytes > 0 && dstBudgetMsgs > 0;
        if (!proto2) {
            preAckBytes = 0;
            preAckMsgs = 0;
            return false;
        }
        // limits are checked after a message is counted, so one more message can land on top of them
        long b = dstBudgetBytes - maxMessage;
        if (windowBytes > 0) b = Math.min(b, windowBytes);
        long m = dstBudgetMsgs - 1;
        if (windowMsgs > 0) m = Math.min(m, windowMsgs);
        if (b >= MIN_BYTES && m >= MIN_MSGS) {
            preAckBytes = b;
            preAckMsgs = m;
        } else {
            preAckBytes = 0;   // too small to ack against without stalling: fall back to pacing on the first ack
            preAckMsgs = 0;
        }
        return preAckBytes > 0;
    }

    boolean proto2() { return proto2; }

    private long byteLimit(long windowBytes) { return acked ? Math.max(0, windowBytes) : preAckBytes; }

    private long msgLimit(int windowMsgs) {
        if (!proto2) return 0;
        return acked ? (windowMsgs > 0 ? Math.max(MIN_MSGS, windowMsgs) : 0) : preAckMsgs;
    }

    /** A data message went out. @return true when reading must pause now. */
    boolean onSent(long payload, long windowBytes, int windowMsgs) {
        outBytes += payload;
        outMsgs++;
        if (paused) return false;
        long bl = byteLimit(windowBytes), ml = msgLimit(windowMsgs);
        if ((bl > 0 && outBytes >= bl) || (ml > 0 && outMsgs >= ml)) {
            paused = true;
            return true;
        }
        return false;
    }

    /** The DST acked {@code ackBytes} bytes and (protocol 2) {@code ackMsgs} messages. @return true when reading must resume now. */
    boolean onAck(long ackBytes, long ackMsgs, long windowBytes, int windowMsgs) {
        acked = true;
        outBytes = Math.max(0, outBytes - ackBytes);
        outMsgs = Math.max(0, outMsgs - Math.max(0, ackMsgs));
        if (!paused) return false;
        long ml = msgLimit(windowMsgs);
        boolean bytesOk = windowBytes <= 0 || outBytes <= windowBytes / 2;
        boolean msgsOk = ml <= 0 || outMsgs <= ml / 2;
        if (bytesOk && msgsOk) {
            paused = false;
            return true;
        }
        return false;
    }

    long outstandingBytes() { return outBytes; }
    long outstandingMsgs() { return outMsgs; }
    boolean paused() { return paused; }

    /**
     * Per-direction stream sequence check (protocol 2 stamps {@code fc_seq} on every data message). The
     * dataplane is a best-effort topic: a gap means the broker dropped tunnel data, and a stream with a hole
     * must fail loudly instead of being served truncated.
     */
    static final class Seq {
        private long next = 0;
        /** @return -1 when {@code seq} is the next one, else the number of messages missing (or duplicated, as a negative gap below -1). */
        long check(long seq) {
            if (seq == next) {
                next++;
                return -1;
            }
            long gap = seq - next;
            next = seq + 1;
            return gap > 0 ? gap : gap - 2;   // gap - 2 keeps a duplicate/reorder distinct from "ok" (-1)
        }
        long expected() { return next; }
    }
}

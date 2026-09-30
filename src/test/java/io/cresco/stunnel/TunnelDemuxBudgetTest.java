package io.cresco.stunnel;

import io.cresco.library.utilities.CLogger;
import jakarta.jms.BytesMessage;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The DST's pre-registration buffer (TunnelDemux) and the SRC's pacing (SrcFlowControl), without JMS or
 * Netty: messages are proxies carrying client_id and dp_bytes, the "wire" is a direct call.
 */
class TunnelDemuxBudgetTest {

    private static final String CLIENT = "c-1";

    static final class Log implements java.lang.reflect.InvocationHandler {
        final List<String> errors = new ArrayList<>();
        @Override public Object invoke(Object p, java.lang.reflect.Method m, Object[] a) {
            if (m.getName().equals("error") && a != null && a.length > 0) errors.add(String.valueOf(a[0]));
            if (m.getReturnType() == boolean.class) return false;
            return null;
        }
    }

    private static Log log;

    private static TunnelDemux demux(long budget, int maxMsgs) {
        log = new Log();
        CLogger l = (CLogger) Proxy.newProxyInstance(CLogger.class.getClassLoader(), new Class<?>[]{CLogger.class}, log);
        return new TunnelDemux(null, l, "t-1", "dst", budget, maxMsgs);
    }

    /** A data message the way the SRC sends it: client_id, dp_bytes, sequence number for ordering checks. */
    private static Message data(String client, int payload, int seq) {
        return (Message) Proxy.newProxyInstance(BytesMessage.class.getClassLoader(), new Class<?>[]{BytesMessage.class}, (p, m, a) -> {
            switch (m.getName()) {
                case "getStringProperty": return "client_id".equals(a[0]) ? client : null;
                case "propertyExists": return "dp_bytes".equals(a[0]) || "seq".equals(a[0]);
                case "getIntProperty": return "dp_bytes".equals(a[0]) ? payload : seq;
                case "getBodyLength": return (long) payload;
                case "hashCode": return System.identityHashCode(p);
                case "equals": return p == a[0];
                default: return null;
            }
        });
    }

    private static int seq(Message m) {
        try { return m.getIntProperty("seq"); } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void anUnpacedSenderOverflowsTheBufferBeforeTheTargetConnects() {
        // The defect: the sender was unpaced until the first ack, which comes only after the DST's target
        // connects. 4 KiB writes before a slow connect (the speed suite's 4 KiB stunnel cell under load):
        // 300 messages, ~1.2 MB, past the 256-message buffer -> the session was failed.
        TunnelDemux d = demux(TunnelDemux.DEFAULT_BUFFER_BYTES, 256);
        d.expect(CLIENT);
        for (int i = 0; i < 300; i++) d.dispatch(data(CLIENT, 4096, i));
        assertFalse(d.register(CLIENT, m -> { }), "256-message buffer must have failed the session");
        assertEquals(1, d.overflows());
        assertEquals(1, log.errors.size(), "loud, once");
        assertTrue(log.errors.get(0).contains("over budget"), log.errors.toString());
    }

    @Test
    void theByteBudgetBindsLargeMessagesToo() {
        TunnelDemux d = demux(4L * 1024 * 1024, 256);
        d.expect(CLIENT);
        for (int i = 0; i < 5; i++) d.dispatch(data(CLIENT, 1024 * 1024, i));   // 5 MiB > 4 MiB
        assertEquals(1, d.overflows());
        assertFalse(d.register(CLIENT, m -> { }), "an over-budget session must be failed, never served truncated");
    }

    @Test
    void bufferedMessagesAreDeliveredInOrderThenLive() {
        TunnelDemux d = demux(TunnelDemux.DEFAULT_BUFFER_BYTES, 256);
        d.expect(CLIENT);
        for (int i = 0; i < 200; i++) d.dispatch(data(CLIENT, 4096, i));
        List<Integer> got = new ArrayList<>();
        assertTrue(d.register(CLIENT, m -> got.add(seq(m))));
        d.dispatch(data(CLIENT, 4096, 200));
        assertEquals(201, got.size());
        for (int i = 0; i <= 200; i++) assertEquals(i, got.get(i));
        assertEquals(0, d.overflows());
    }

    /**
     * End to end: a protocol-2 SRC paced to the DST's advertised budget never overflows it, however long the
     * target connect takes, keeps at most the message window in flight afterwards (the broker's slow-subscriber
     * discard limit is ~350 per consumer), and never stalls.
     */
    @Test
    void aProtocol2SenderNeverOverflowsNeverExceedsTheMessageWindowAndNeverStalls() {
        long window = 16L * 1024 * 1024, readMax = 1024 * 1024;
        int windowMsgs = SrcFlowControl.DEFAULT_WINDOW_MSGS;
        for (int chunk : new int[]{1, 4096, 65536, 1024 * 1024}) {
            TunnelDemux d = demux(TunnelDemux.DEFAULT_BUFFER_BYTES, 256);
            SrcFlowControl fc = new SrcFlowControl();
            assertTrue(fc.negotiate(d.bufferBudgetBytes(), d.bufferBudgetMsgs(), readMax, window, windowMsgs));
            d.expect(CLIENT);
            // phase 1: the target connect is slow; the sender writes until its flow control pauses it
            int sent = 0;
            while (!fc.paused() && sent < 10_000_000) {
                d.dispatch(data(CLIENT, chunk, sent));
                fc.onSent(chunk, window, windowMsgs);
                sent++;
            }
            assertTrue(fc.paused(), "chunk " + chunk + ": sender never paused");
            assertEquals(0, d.overflows(), "chunk " + chunk + ": buffer overflowed: " + log.errors);
            // phase 2: the target connects; the DST writes and acks (bytes + messages, every ACK_EVERY_*),
            // the sender resumes and streams on. Model the wire as a FIFO the DST drains one message at a time.
            java.util.ArrayDeque<Integer> wire = new java.util.ArrayDeque<>();
            long[] ub = {0}, um = {0};
            List<long[]> acks = new ArrayList<>();
            MessageListener dst = m -> {
                ub[0] += TunnelDemux.payload(m);
                um[0]++;
                if (ub[0] >= SrcFlowControl.ACK_EVERY_BYTES || um[0] >= SrcFlowControl.ACK_EVERY_MSGS) {
                    acks.add(new long[]{ub[0], um[0]});
                    ub[0] = 0; um[0] = 0;
                }
            };
            assertTrue(d.register(CLIENT, dst));
            long maxInFlight = 0;
            int steps = 0;
            while (steps++ < 200_000) {
                for (long[] a : acks) fc.onAck(a[0], a[1], window, windowMsgs);
                acks.clear();
                if (!fc.paused()) {
                    d.dispatch(data(CLIENT, chunk, sent));      // registered: goes straight to the DST
                    fc.onSent(chunk, window, windowMsgs);
                    sent++;
                    maxInFlight = Math.max(maxInFlight, fc.outstandingMsgs());
                } else if (acks.isEmpty() && ub[0] == 0 && um[0] == 0) {
                    break;
                } else if (acks.isEmpty()) {
                    fail("chunk " + chunk + ": stalled with " + fc.outstandingMsgs() + " msgs / " + fc.outstandingBytes() + " B unacked");
                }
                if (sent > 2000 && !fc.paused()) break;
            }
            assertTrue(maxInFlight <= windowMsgs, "chunk " + chunk + ": " + maxInFlight + " messages in flight");
            assertTrue(sent > 2000, "chunk " + chunk + ": only " + sent + " messages went through");
        }
    }

    @Test
    void anOldDstKeepsTheOldBehaviour() {
        SrcFlowControl fc = new SrcFlowControl();
        assertFalse(fc.negotiate(0, 0, 1024 * 1024, 16L * 1024 * 1024, 128));
        assertFalse(fc.proto2());
        long window = 16L * 1024 * 1024;
        for (int i = 0; i < 1000; i++) assertFalse(fc.onSent(4096, window, 128), "no pacing before the first ack, no message window");
        assertFalse(fc.onAck(1024 * 1024, 0, window, 128));
        for (int i = 0; i < 4000; i++) fc.onSent(4096, window, 128);
        assertTrue(fc.paused(), "byte window after the first ack");
        assertTrue(fc.onAck(fc.outstandingBytes(), 0, window, 128));
    }

    @Test
    void aBudgetTooSmallToAckAgainstIsNotUsedBeforeTheFirstAck() {
        SrcFlowControl fc = new SrcFlowControl();
        assertFalse(fc.negotiate(24L << 20, 16, 1024 * 1024, 16L << 20, 128));      // 15 msgs < 2 x ACK_EVERY_MSGS
        assertFalse(fc.negotiate(2L << 20, 256, 1024 * 1024, 16L << 20, 128));     // 1 MiB < 2 x ACK_EVERY_BYTES
    }

    @Test
    void aStreamGapIsDetected() {
        SrcFlowControl.Seq s = new SrcFlowControl.Seq();
        for (int i = 0; i < 10; i++) assertEquals(-1, s.check(i));
        assertEquals(3, s.check(13));      // 10, 11, 12 dropped by the dataplane
        assertEquals(-1, s.check(14));
        assertTrue(s.check(14) < -1);      // duplicate
    }
}

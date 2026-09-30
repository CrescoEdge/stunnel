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
    void theOldCountCapFailedAFastSenderBeforeTheTargetConnected() {
        // The pre-fix configuration: 256 messages, sender unpaced until the first ack. 4 KiB writes before
        // the target connects (the speed suite's 4 KiB stunnel cell under load) = 300 messages, ~1.2 MB.
        TunnelDemux d = demux(Long.MAX_VALUE, 256);
        d.expect(CLIENT);
        for (int i = 0; i < 300; i++) d.dispatch(data(CLIENT, 4096, i));
        assertFalse(d.register(CLIENT, m -> { }), "256-message cap must have failed the session");
        assertEquals(1, d.overflows());
        assertTrue(log.errors.get(0).contains("over budget"), log.errors.toString());
    }

    @Test
    void theByteBudgetHoldsThatBurstAndDeliversItInOrder() {
        TunnelDemux d = demux(TunnelDemux.DEFAULT_BUFFER_BYTES, 0);
        d.expect(CLIENT);
        for (int i = 0; i < 300; i++) d.dispatch(data(CLIENT, 4096, i));
        List<Integer> got = new ArrayList<>();
        assertTrue(d.register(CLIENT, m -> got.add(seq(m))));
        assertEquals(300, got.size());
        for (int i = 0; i < 300; i++) assertEquals(i, got.get(i));
        assertEquals(0, d.overflows());
        // live after registration
        d.dispatch(data(CLIENT, 4096, 300));
        assertEquals(301, got.size());
    }

    @Test
    void anUnpacedSenderPastTheBudgetFailsLoudlyNotSilently() {
        TunnelDemux d = demux(4L * 1024 * 1024, 0);
        d.expect(CLIENT);
        for (int i = 0; i < 5; i++) d.dispatch(data(CLIENT, 1024 * 1024, i));   // 5 MiB + overhead > 4 MiB
        assertEquals(1, d.overflows());
        assertEquals(1, log.errors.size());
        assertTrue(log.errors.get(0).contains("budget"), log.errors.toString());
        assertFalse(d.register(CLIENT, m -> { }), "an over-budget session must be failed, never served truncated");
    }

    /**
     * End to end: a protocol-2 SRC paced to the DST's advertised budget never overflows it, however long the
     * target connect takes, and resumes once the DST registers and acks.
     */
    @Test
    void aProtocol2SenderPacedToTheAdvertisedBudgetNeverOverflows() {
        for (int chunk : new int[]{1, 4096, 65536, 1024 * 1024}) {
            TunnelDemux d = demux(TunnelDemux.DEFAULT_BUFFER_BYTES, 0);
            SrcFlowControl fc = new SrcFlowControl();
            long window = 16L * 1024 * 1024, readMax = 1024 * 1024;
            long preAck = fc.negotiate(d.bufferBudgetBytes(), readMax, window);
            assertTrue(preAck >= SrcFlowControl.MIN_PREACK && preAck <= d.bufferBudgetBytes() - readMax, "preAck " + preAck);
            d.expect(CLIENT);
            // the target connect is "slow": the sender writes until its flow control pauses it
            int sent = 0;
            boolean paused = false;
            while (!paused && sent < 50_000_000) {
                d.dispatch(data(CLIENT, chunk, sent));
                paused = fc.onSent(chunk, window);
                sent++;
            }
            assertTrue(paused, "chunk " + chunk + ": sender never paused");
            assertEquals(0, d.overflows(), "chunk " + chunk + ": buffer overflowed: " + log.errors);
            // target connects: the demux drains, the DST acks in cost units every ACK_EVERY
            long[] unacked = {0};
            List<Long> acks = new ArrayList<>();
            MessageListener dst = m -> {
                unacked[0] += TunnelDemux.cost(m);
                if (unacked[0] >= SrcFlowControl.ACK_EVERY) { acks.add(unacked[0]); unacked[0] = 0; }
            };
            assertTrue(d.register(CLIENT, dst));
            boolean resumed = false;
            for (long a : acks) resumed |= fc.onAck(a, window);
            assertTrue(resumed, "chunk " + chunk + ": sender never resumed (outstanding " + fc.outstanding() + ")");
            assertTrue(fc.outstanding() < SrcFlowControl.ACK_EVERY, "chunk " + chunk + ": outstanding " + fc.outstanding());
        }
    }

    @Test
    void anOldDstKeepsTheOldBehaviour() {
        SrcFlowControl fc = new SrcFlowControl();
        assertEquals(0, fc.negotiate(0, 1024 * 1024, 16L * 1024 * 1024));
        assertFalse(fc.costMode());
        long window = 16L * 1024 * 1024;
        for (int i = 0; i < 100; i++) assertFalse(fc.onSent(1024 * 1024, window), "no pacing before the first ack");
        assertFalse(fc.onAck(1024 * 1024, window));                // first ack arms the window; still over it
        assertTrue(fc.onSent(1, window));                          // now paced
        assertTrue(fc.onAck(fc.outstanding(), window));            // drained: resume
    }

    @Test
    void aBudgetTooSmallToAckAgainstIsNotUsed() {
        SrcFlowControl fc = new SrcFlowControl();
        assertEquals(0, fc.negotiate(2L * 1024 * 1024, 1024 * 1024, 16L * 1024 * 1024));   // would stall below 2 x ACK_EVERY
    }
}

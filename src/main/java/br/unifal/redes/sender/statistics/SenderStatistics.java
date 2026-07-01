package br.unifal.redes.sender.statistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates protocol-level event counters for a single transmission session.
 *
 * <p>Consumers register discrete events via the {@code record*()} methods.
 * No output is produced here; formatting and printing are the responsibility
 * of higher-level components that call {@link #snapshot()}.
 *
 * <p>All counters are backed by {@link AtomicLong} so that concurrent
 * sender threads (e.g. the packet-sending loop and the ACK-listening loop)
 * may safely register events without external synchronization.
 *
 * <p>Usage pattern expected by the FSM:
 * <pre>{@code
 *   statistics.recordPacketSent();
 *   statistics.recordBytesSent(payload.length);
 *   if (timedOut) {
 *       statistics.recordTimeout();
 *       statistics.recordRetransmission();
 *   } else {
 *       statistics.recordAckReceived();
 *   }
 * }</pre>
 */
public final class SenderStatistics {

    private final AtomicLong totalPacketsSent    = new AtomicLong(0);
    private final AtomicLong totalRetransmitted  = new AtomicLong(0);
    private final AtomicLong totalAcksReceived   = new AtomicLong(0);
    private final AtomicLong totalTimeouts       = new AtomicLong(0);
    private final AtomicLong totalBytesSent      = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Event registration
    // -------------------------------------------------------------------------

    /**
     * Registers that a data packet was handed off to the socket for
     * transmission. Must be called once for every DATA datagram sent,
     * including retransmissions — use {@link #recordRetransmission()} in
     * addition to this method to distinguish the two.
     */
    public void recordPacketSent() {
        totalPacketsSent.incrementAndGet();
    }

    /**
     * Registers that a previously sent packet was retransmitted, e.g. as a
     * result of a timeout in a Go-Back-N retransmission burst.
     *
     * <p>Should be called together with {@link #recordPacketSent()}, since a
     * retransmission is still a packet sent on the wire.
     */
    public void recordRetransmission() {
        totalRetransmitted.incrementAndGet();
    }

    /**
     * Registers that an ACK datagram was received from the receiver,
     * regardless of whether it advanced the send window (duplicate ACKs
     * still count, since this method only tracks arrival, not effect).
     */
    public void recordAckReceived() {
        totalAcksReceived.incrementAndGet();
    }

    /**
     * Registers that the retransmission timer expired without a
     * corresponding ACK arriving in time.
     */
    public void recordTimeout() {
        totalTimeouts.incrementAndGet();
    }

    /**
     * Registers {@code byteCount} payload bytes as having been sent on the wire.
     *
     * <p>Should be called together with {@link #recordPacketSent()} for every
     * transmission, including retransmissions, so that {@code totalBytesSent}
     * reflects actual network usage rather than unique file bytes.
     *
     * @param byteCount the number of payload bytes sent; must be &gt;= 0
     * @throws IllegalArgumentException if {@code byteCount} is negative
     */
    public void recordBytesSent(long byteCount) {
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount must be >= 0, got: " + byteCount);
        }
        totalBytesSent.addAndGet(byteCount);
    }

    // -------------------------------------------------------------------------
    // Snapshot
    // -------------------------------------------------------------------------

    /**
     * Returns an immutable point-in-time snapshot of all counters.
     *
     * <p>The snapshot captures the current values atomically with respect to
     * each individual counter, but not across all counters simultaneously.
     * For the final report — called after the session is closed — this is
     * sufficient.
     *
     * @return a new {@link Snapshot} with current counter values
     */
    public Snapshot snapshot() {
        return new Snapshot(
                totalPacketsSent.get(),
                totalRetransmitted.get(),
                totalAcksReceived.get(),
                totalTimeouts.get(),
                totalBytesSent.get()
        );
    }

    // -------------------------------------------------------------------------
    // Snapshot record
    // -------------------------------------------------------------------------

    /**
     * Immutable, point-in-time view of the statistics at a given moment.
     *
     * <p>Consumers (report printers, unit tests) work against this type so they
     * are never affected by concurrent updates to the live counters.
     */
    public static final class Snapshot {

        private final long totalPacketsSent;
        private final long totalRetransmitted;
        private final long totalAcksReceived;
        private final long totalTimeouts;
        private final long totalBytesSent;

        private Snapshot(long totalPacketsSent, long totalRetransmitted,
                         long totalAcksReceived, long totalTimeouts,
                         long totalBytesSent) {
            this.totalPacketsSent   = totalPacketsSent;
            this.totalRetransmitted = totalRetransmitted;
            this.totalAcksReceived  = totalAcksReceived;
            this.totalTimeouts      = totalTimeouts;
            this.totalBytesSent     = totalBytesSent;
        }

        /** @return total DATA datagrams sent on the wire, including retransmissions */
        public long getTotalPacketsSent() {
            return totalPacketsSent;
        }

        /** @return packets sent again after a timeout (subset of {@link #getTotalPacketsSent()}) */
        public long getTotalRetransmitted() {
            return totalRetransmitted;
        }

        /** @return ACK datagrams received from the receiver */
        public long getTotalAcksReceived() {
            return totalAcksReceived;
        }

        /** @return number of times the retransmission timer expired */
        public long getTotalTimeouts() {
            return totalTimeouts;
        }

        /** @return total payload bytes sent on the wire, including retransmissions */
        public long getTotalBytesSent() {
            return totalBytesSent;
        }

        /**
         * Computes the retransmission rate as a value in {@code [0.0, 1.0]}.
         *
         * <p>Returns {@code 0.0} if no packets have been sent yet, to avoid
         * division by zero.
         *
         * @return {@code totalRetransmitted / totalPacketsSent}, or {@code 0.0}
         *         if {@code totalPacketsSent == 0}
         */
        public double retransmissionRate() {
            if (totalPacketsSent == 0) return 0.0;
            return (double) totalRetransmitted / totalPacketsSent;
        }

        @Override
        public String toString() {
            return "SenderStatistics.Snapshot{"
                    + "packetsSent=" + totalPacketsSent
                    + ", retransmitted=" + totalRetransmitted
                    + ", acksReceived=" + totalAcksReceived
                    + ", timeouts=" + totalTimeouts
                    + ", bytesSent=" + totalBytesSent
                    + ", retransmissionRate=" + String.format("%.2f%%", retransmissionRate() * 100)
                    + '}';
        }
    }
}
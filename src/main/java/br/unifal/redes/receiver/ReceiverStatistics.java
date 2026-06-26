package br.unifal.redes.receiver;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Accumulates protocol-level event counters for a single reception session.
 *
 * <p>Consumers register discrete events via the {@code record*()} methods.
 * No output is produced here; formatting and printing are the responsibility
 * of higher-level components that call {@link #snapshot()}.
 *
 * <p>All counters are backed by {@link AtomicLong} so that concurrent
 * sender/receiver threads may safely register events without external
 * synchronization.
 *
 * <p>Usage pattern expected by the FSM:
 * <pre>{@code
 *   statistics.recordPacketReceived();
 *   if (accepted) {
 *       statistics.recordPacketAccepted();
 *       statistics.recordAckSent();
 *   } else {
 *       statistics.recordPacketDiscarded();
 *   }
 * }</pre>
 */
public final class ReceiverStatistics {

    private final AtomicLong totalReceived  = new AtomicLong(0);
    private final AtomicLong totalAccepted  = new AtomicLong(0);
    private final AtomicLong totalDiscarded = new AtomicLong(0);
    private final AtomicLong totalAcksSent  = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Event registration
    // -------------------------------------------------------------------------

    /**
     * Registers that a data packet arrived at the socket (before any filtering).
     * Must be called for every incoming DATA datagram, regardless of outcome.
     */
    public void recordPacketReceived() {
        totalReceived.incrementAndGet();
    }

    /**
     * Registers that a packet was accepted by the GBN FSM (correct order,
     * not simulated-lost) and its payload was written to disk.
     *
     * <p>Should always be called together with {@link #recordPacketReceived()}.
     */
    public void recordPacketAccepted() {
        totalAccepted.incrementAndGet();
    }

    /**
     * Registers that a packet was discarded — either because it arrived out of
     * order (GBN policy) or because it was chosen for simulated loss.
     *
     * <p>Should always be called together with {@link #recordPacketReceived()}.
     */
    public void recordPacketDiscarded() {
        totalDiscarded.incrementAndGet();
    }

    /**
     * Registers that an ACK datagram was sent back to the sender.
     * Called once per accepted packet (not per retransmitted packet on the
     * sender side).
     */
    public void recordAckSent() {
        totalAcksSent.incrementAndGet();
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
                totalReceived.get(),
                totalAccepted.get(),
                totalDiscarded.get(),
                totalAcksSent.get()
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

        private final long totalReceived;
        private final long totalAccepted;
        private final long totalDiscarded;
        private final long totalAcksSent;

        private Snapshot(long totalReceived, long totalAccepted,
                         long totalDiscarded, long totalAcksSent) {
            this.totalReceived  = totalReceived;
            this.totalAccepted  = totalAccepted;
            this.totalDiscarded = totalDiscarded;
            this.totalAcksSent  = totalAcksSent;
        }

        /** @return total DATA datagrams that arrived at the socket */
        public long getTotalReceived() {
            return totalReceived;
        }

        /** @return packets accepted by the FSM and written to disk */
        public long getTotalAccepted() {
            return totalAccepted;
        }

        /**
         * @return packets discarded — out-of-order (GBN) plus simulated losses.
         *         The assignment specifies that only in-order packets are
         *         subject to loss simulation, so the split can be tracked by the
         *         FSM via separate calls if needed in a future iteration.
         */
        public long getTotalDiscarded() {
            return totalDiscarded;
        }

        /** @return ACK datagrams sent back to the sender */
        public long getTotalAcksSent() {
            return totalAcksSent;
        }

        /**
         * Computes the effective loss rate as a value in {@code [0.0, 1.0]}.
         *
         * <p>Returns {@code 0.0} if no packets have been received yet, to avoid
         * division by zero.
         *
         * @return {@code totalDiscarded / totalReceived}, or {@code 0.0} if
         *         {@code totalReceived == 0}
         */
        public double effectiveLossRate() {
            if (totalReceived == 0) return 0.0;
            return (double) totalDiscarded / totalReceived;
        }

        @Override
        public String toString() {
            return "ReceiverStatistics.Snapshot{"
                    + "received=" + totalReceived
                    + ", accepted=" + totalAccepted
                    + ", discarded=" + totalDiscarded
                    + ", acksSent=" + totalAcksSent
                    + ", effectiveLossRate=" + String.format("%.2f%%", effectiveLossRate() * 100)
                    + '}';
        }
    }
}
package br.unifal.redes.receiver.statistics;

import java.util.concurrent.atomic.AtomicLong;

public final class ReceiverStatistics {

    private final AtomicLong totalReceived  = new AtomicLong(0);
    private final AtomicLong totalAccepted  = new AtomicLong(0);
    private final AtomicLong totalDiscarded = new AtomicLong(0);
    private final AtomicLong totalAcksSent  = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Registro de eventos
    // -------------------------------------------------------------------------

    public void recordPacketReceived() {
        totalReceived.incrementAndGet();
    }

    public void recordPacketAccepted() {
        totalAccepted.incrementAndGet();
    }

    public void recordPacketDiscarded() {
        totalDiscarded.incrementAndGet();
    }

    public void recordAckSent() {
        totalAcksSent.incrementAndGet();
    }

    // -------------------------------------------------------------------------
    // Instantâneo
    // -------------------------------------------------------------------------

    public Snapshot snapshot() {
        return new Snapshot(
                totalReceived.get(),
                totalAccepted.get(),
                totalDiscarded.get(),
                totalAcksSent.get()
        );
    }

    // -------------------------------------------------------------------------
    // Registro de instantâneo
    // -------------------------------------------------------------------------

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

        public long getTotalReceived() {
            return totalReceived;
        }

        public long getTotalAccepted() {
            return totalAccepted;
        }

        public long getTotalDiscarded() {
            return totalDiscarded;
        }

        public long getTotalAcksSent() {
            return totalAcksSent;
        }

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
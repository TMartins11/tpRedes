package br.unifal.redes.sender.statistics;

import java.util.concurrent.atomic.AtomicLong;

public final class SenderStatistics {

    private final AtomicLong totalPacketsSent    = new AtomicLong(0);
    private final AtomicLong totalRetransmitted  = new AtomicLong(0);
    private final AtomicLong totalAcksReceived   = new AtomicLong(0);
    private final AtomicLong totalTimeouts       = new AtomicLong(0);
    private final AtomicLong totalBytesSent      = new AtomicLong(0);

    // -------------------------------------------------------------------------
    // Registro de eventos
    // -------------------------------------------------------------------------

    public void recordPacketSent() {
        totalPacketsSent.incrementAndGet();
    }

    public void recordRetransmission() {
        totalRetransmitted.incrementAndGet();
    }

    public void recordAckReceived() {
        totalAcksReceived.incrementAndGet();
    }

    public void recordTimeout() {
        totalTimeouts.incrementAndGet();
    }

    public void recordBytesSent(long byteCount) {
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount deve ser >= 0, recebido: " + byteCount);
        }
        totalBytesSent.addAndGet(byteCount);
    }

    // -------------------------------------------------------------------------
    // Instantâneo
    // -------------------------------------------------------------------------

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
    // Registro de instantâneo
    // -------------------------------------------------------------------------

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

        public long getTotalPacketsSent() {
            return totalPacketsSent;
        }

        public long getTotalRetransmitted() {
            return totalRetransmitted;
        }

        public long getTotalAcksReceived() {
            return totalAcksReceived;
        }

        public long getTotalTimeouts() {
            return totalTimeouts;
        }

        public long getTotalBytesSent() {
            return totalBytesSent;
        }

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
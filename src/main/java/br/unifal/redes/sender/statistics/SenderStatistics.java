package br.unifal.redes.sender.statistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Acumula contadores de eventos em nível de protocolo para uma única sessão de transmissão.
 *
 * <p>Os consumidores registram eventos discretos através dos métodos {@code record*()}.
 * Nenhuma saída é produzida aqui; formatação e impressão são responsabilidade
 * de componentes de nível superior que chamam {@link #snapshot()}.
 *
 * <p>Todos os contadores são suportados por {@link AtomicLong} para que threads
 * concorrentes do transmissor (por exemplo, o loop de envio de pacotes e o loop de
 * escuta de ACKs) possam registrar eventos com segurança sem sincronização externa.
 *
 * <p>Padrão de uso esperado pela FSM:
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
    // Registro de eventos
    // -------------------------------------------------------------------------

    /**
     * Registra que um pacote de dados foi entregue ao socket para
     * transmissão. Deve ser chamado uma vez para cada datagrama DATA enviado,
     * incluindo retransmissões — use {@link #recordRetransmission()} em
     * adição a este método para distinguir os dois.
     */
    public void recordPacketSent() {
        totalPacketsSent.incrementAndGet();
    }

    /**
     * Registra que um pacote enviado anteriormente foi retransmitido, por exemplo, como
     * resultado de um timeout em um burst de retransmissão Go-Back-N.
     *
     * <p>Deve ser chamado junto com {@link #recordPacketSent()}, pois uma
     * retransmissão ainda é um pacote enviado na rede.
     */
    public void recordRetransmission() {
        totalRetransmitted.incrementAndGet();
    }

    /**
     * Registra que um datagrama ACK foi recebido do receptor,
     * independentemente de ter avançado a janela de envio (ACKs duplicados
     * ainda contam, pois este método rastreia apenas a chegada, não o efeito).
     */
    public void recordAckReceived() {
        totalAcksReceived.incrementAndGet();
    }

    /**
     * Registra que o temporizador de retransmissão expirou sem que um
     * ACK correspondente chegasse a tempo.
     */
    public void recordTimeout() {
        totalTimeouts.incrementAndGet();
    }

    /**
     * Registra {@code byteCount} bytes de payload como tendo sido enviados na rede.
     *
     * <p>Deve ser chamado junto com {@link #recordPacketSent()} para cada
     * transmissão, incluindo retransmissões, para que {@code totalBytesSent}
     * reflita o uso real da rede em vez de bytes únicos do arquivo.
     *
     * @param byteCount o número de bytes de payload enviados; deve ser &gt;= 0
     * @throws IllegalArgumentException se {@code byteCount} for negativo
     */
    public void recordBytesSent(long byteCount) {
        if (byteCount < 0) {
            throw new IllegalArgumentException("byteCount deve ser >= 0, recebido: " + byteCount);
        }
        totalBytesSent.addAndGet(byteCount);
    }

    // -------------------------------------------------------------------------
    // Instantâneo
    // -------------------------------------------------------------------------

    /**
     * Retorna um instantâneo imutável de todos os contadores em um ponto específico no tempo.
     *
     * <p>O instantâneo captura os valores atuais atomicamente em relação a
     * cada contador individual, mas não em todos os contadores simultaneamente.
     * Para o relatório final — chamado após a sessão ser fechada — isso é
     * suficiente.
     *
     * @return um novo {@link Snapshot} com os valores atuais dos contadores
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
    // Registro de instantâneo
    // -------------------------------------------------------------------------

    /**
     * Visão imutável e pontual das estatísticas em um determinado momento.
     *
     * <p>Os consumidores (impressores de relatórios, testes unitários) trabalham
     * com este tipo para que nunca sejam afetados por atualizações concorrentes
     * nos contadores ao vivo.
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

        /** @return total de datagramas DATA enviados na rede, incluindo retransmissões */
        public long getTotalPacketsSent() {
            return totalPacketsSent;
        }

        /** @return pacotes reenviados após um timeout (subconjunto de {@link #getTotalPacketsSent()}) */
        public long getTotalRetransmitted() {
            return totalRetransmitted;
        }

        /** @return datagramas ACK recebidos do receptor */
        public long getTotalAcksReceived() {
            return totalAcksReceived;
        }

        /** @return número de vezes que o temporizador de retransmissão expirou */
        public long getTotalTimeouts() {
            return totalTimeouts;
        }

        /** @return total de bytes de payload enviados na rede, incluindo retransmissões */
        public long getTotalBytesSent() {
            return totalBytesSent;
        }

        /**
         * Calcula a taxa de retransmissão como um valor em {@code [0.0, 1.0]}.
         *
         * <p>Retorna {@code 0.0} se nenhum pacote tiver sido enviado ainda, para evitar
         * divisão por zero.
         *
         * @return {@code totalRetransmitted / totalPacketsSent}, ou {@code 0.0}
         *         se {@code totalPacketsSent == 0}
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
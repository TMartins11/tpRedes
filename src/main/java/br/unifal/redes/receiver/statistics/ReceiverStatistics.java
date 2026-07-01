package br.unifal.redes.receiver.statistics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Acumula contadores de eventos em nível de protocolo para uma única sessão de recepção.
 *
 * <p>Os consumidores registram eventos discretos através dos métodos {@code record*()}.
 * Nenhuma saída é produzida aqui; formatação e impressão são responsabilidade
 * de componentes de nível superior que chamam {@link #snapshot()}.
 *
 * <p>Todos os contadores são suportados por {@link AtomicLong} para que threads
 * concorrentes de envio/recepção possam registrar eventos com segurança sem
 * sincronização externa.
 *
 * <p>Padrão de uso esperado pela FSM:
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
    // Registro de eventos
    // -------------------------------------------------------------------------

    /**
     * Registra que um pacote de dados chegou ao socket (antes de qualquer filtragem).
     * Deve ser chamado para cada datagrama DATA recebido, independentemente do resultado.
     */
    public void recordPacketReceived() {
        totalReceived.incrementAndGet();
    }

    /**
     * Registra que um pacote foi aceito pela FSM GBN (ordem correta,
     * não foi simulado como perdido) e seu payload foi escrito em disco.
     *
     * <p>Deve ser sempre chamado junto com {@link #recordPacketReceived()}.
     */
    public void recordPacketAccepted() {
        totalAccepted.incrementAndGet();
    }

    /**
     * Registra que um pacote foi descartado — seja porque chegou fora de
     * ordem (política GBN) ou porque foi escolhido para perda simulada.
     *
     * <p>Deve ser sempre chamado junto com {@link #recordPacketReceived()}.
     */
    public void recordPacketDiscarded() {
        totalDiscarded.incrementAndGet();
    }

    /**
     * Registra que um datagrama ACK foi enviado de volta ao transmissor.
     * Chamado uma vez por pacote aceito (não por pacote retransmitido no
     * lado do transmissor).
     */
    public void recordAckSent() {
        totalAcksSent.incrementAndGet();
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
                totalReceived.get(),
                totalAccepted.get(),
                totalDiscarded.get(),
                totalAcksSent.get()
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

        /** @return total de datagramas DATA que chegaram ao socket */
        public long getTotalReceived() {
            return totalReceived;
        }

        /** @return pacotes aceitos pela FSM e escritos em disco */
        public long getTotalAccepted() {
            return totalAccepted;
        }

        /**
         * @return pacotes descartados — fora de ordem (GBN) mais perdas simuladas.
         *         A especificação determina que apenas pacotes em ordem estão
         *         sujeitos à simulação de perda, então a divisão pode ser rastreada
         *         pela FSM através de chamadas separadas, se necessário em uma iteração futura.
         */
        public long getTotalDiscarded() {
            return totalDiscarded;
        }

        /** @return datagramas ACK enviados de volta ao transmissor */
        public long getTotalAcksSent() {
            return totalAcksSent;
        }

        /**
         * Calcula a taxa de perda efetiva como um valor em {@code [0.0, 1.0]}.
         *
         * <p>Retorna {@code 0.0} se nenhum pacote tiver sido recebido ainda, para evitar
         * divisão por zero.
         *
         * @return {@code totalDiscarded / totalReceived}, ou {@code 0.0} se
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
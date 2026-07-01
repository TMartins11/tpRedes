package br.unifal.redes.sender.network;

import java.util.concurrent.TimeUnit;

/**
 * Rastreia o estado do timeout de retransmissão do transmissor Go-Back-N.
 *
 * <p>Esta classe é um componente puro de suporte ao protocolo. Ela armazena uma duração
 * de timeout configurada e o instante em que um temporizador foi armado pela última vez,
 * e expõe cálculos — tempo decorrido, tempo restante, expiração — que a
 * FSM do transmissor consulta periodicamente para decidir quando um burst de
 * retransmissão é devido.
 *
 * <p>Esta classe não realiza retransmissão real, E/S de rede, E/S de arquivo
 * e não agenda nenhum tipo de trabalho em segundo plano. Não há
 * {@link Thread}, {@code Timer} ou {@code ScheduledExecutorService}
 * envolvidos — espera-se que a FSM chame {@link #hasExpired()} periodicamente
 * (por exemplo, a partir de seu próprio loop de polling) e reaja de acordo.
 *
 * <p>Os cálculos de tempo decorrido usam {@link System#nanoTime()} em vez de
 * {@link System#currentTimeMillis()}, pois {@code nanoTime} é monotônico
 * e não é afetado por ajustes de relógio de parede, tornando-o a escolha correta
 * para medir durações.
 *
 * <p>Segurança de thread: esta classe não é sincronizada. Ela é destinada a ser
 * de propriedade e conduzida por uma única thread da FSM do transmissor; chamadores que
 * precisam de acesso concorrente devem coordenar externamente.
 */
public final class TimeoutManager {

    private final long timeoutNanos;
    private final long timeoutMillis;

    private boolean running;
    private long startNanos;

    /**
     * Cria um novo gerenciador de timeout com a duração de timeout fornecida.
     *
     * @param timeoutMillis a duração do timeout, em milissegundos; deve ser &gt; 0
     * @throws IllegalArgumentException se {@code timeoutMillis} não for positivo
     */
    public TimeoutManager(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis deve ser > 0, recebido: " + timeoutMillis);
        }
        this.timeoutMillis = timeoutMillis;
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.running = false;
    }

    // -------------------------------------------------------------------------
    // Mutadores voltados para a FSM
    // -------------------------------------------------------------------------

    /**
     * Arma o temporizador, capturando o instante atual como seu ponto de partida.
     *
     * <p>Use este método para o primeiro armamento de um timeout novo. Se um temporizador
     * já estiver em execução e simplesmente deve ser reiniciado, use {@link #restart()}
     * em vez disso.
     *
     * @throws IllegalStateException se o temporizador já estiver em execução
     */
    public void start() {
        if (running) {
            throw new IllegalStateException(
                    "O temporizador já está em execução; chame restart() para reiniciá-lo ou cancel() primeiro");
        }
        arm();
    }

    /**
     * Re-arma o temporizador, redefinindo seu ponto de partida para o instante atual,
     * independentemente de já estar em execução.
     *
     * <p>Esta é a operação típica executada quando um ACK parcial chega
     * enquanto pacotes ainda estão em trânsito: o relógio do timeout reinicia a partir de agora.
     */
    public void restart() {
        arm();
    }

    /**
     * Desarma o temporizador. Após esta chamada, {@link #isRunning()} retorna
     * {@code false} e {@link #hasExpired()} retorna {@code false} até que o
     * temporizador seja armado novamente via {@link #start()} ou {@link #restart()}.
     *
     * <p>Idempotente — chamar {@code cancel()} em um temporizador já parado
     * não tem efeito.
     */
    public void cancel() {
        running = false;
    }

    // -------------------------------------------------------------------------
    // Consultas
    // -------------------------------------------------------------------------

    /**
     * @return {@code true} se o temporizador estiver atualmente armado (iniciado ou
     *         reiniciado, e ainda não cancelado)
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Indica se a duração de timeout configurada decorreu desde que o
     * temporizador foi armado pela última vez.
     *
     * @return {@code true} se o temporizador estiver em execução e seu tempo decorrido
     *         tiver atingido ou excedido o timeout configurado; {@code false} se
     *         o temporizador não estiver em execução ou ainda não tiver expirado
     */
    public boolean hasExpired() {
        return running && elapsedNanosSinceArmed() >= timeoutNanos;
    }

    /**
     * @return o tempo decorrido, em milissegundos, desde que o temporizador foi
     *         armado pela última vez, ou {@code 0} se o temporizador não estiver
     *         atualmente em execução
     */
    public long elapsedMillis() {
        if (!running) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanosSinceArmed());
    }

    /**
     * @return o tempo restante, em milissegundos, antes que o temporizador expire,
     *         limitado a {@code 0} se já tiver expirado, ou {@code 0} se
     *         o temporizador não estiver atualmente em execução
     */
    public long remainingMillis() {
        if (!running) {
            return 0L;
        }
        long remainingNanos = timeoutNanos - elapsedNanosSinceArmed();
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, remainingNanos));
    }

    /** @return a duração de timeout configurada, em milissegundos */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    // -------------------------------------------------------------------------
    // Métodos auxiliares
    // -------------------------------------------------------------------------

    private void arm() {
        this.startNanos = System.nanoTime();
        this.running = true;
    }

    private long elapsedNanosSinceArmed() {
        return System.nanoTime() - startNanos;
    }

    @Override
    public String toString() {
        return "TimeoutManager{"
                + "running=" + running
                + ", timeoutMillis=" + timeoutMillis
                + ", elapsedMillis=" + elapsedMillis()
                + ", remainingMillis=" + remainingMillis()
                + '}';
    }
}
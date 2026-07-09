package br.unifal.redes.receiver.session;

import br.unifal.redes.common.SessionParameters;

import java.time.Instant;
import java.util.Objects;

public final class ReceiverSession {

    /** Estado de protocolo desta sessão. */
    public enum State {
        /** Handshake recebido; pronto para aceitar pacotes de dados. */
        RECEIVING,
        /** FIN recebido ou transferência concluída; nenhum dado adicional é esperado. */
        CLOSED
    }

    private final SessionParameters parameters;
    private final String destinationPath;
    private final Instant startedAt;

    /**
     * Próximo número de sequência que o receptor aceitará.
     * Inicializado como 0 conforme a FSM GBN (expectedseqnum = 0).
     */
    private int expectedSequenceNumber;

    /**
     * Número de sequência do último ACK enviado.
     * Inicializado como -1 para indicar que nenhum ACK foi enviado ainda.
     */
    private int lastAcknowledgedSequenceNumber;

    private State state;
    private Instant finishedAt;

    // -------------------------------------------------------------------------
    // Construção
    // -------------------------------------------------------------------------

    private ReceiverSession(SessionParameters parameters, String destinationPath) {
        this.parameters = parameters;
        this.destinationPath = destinationPath;
        this.expectedSequenceNumber = 0;
        this.lastAcknowledgedSequenceNumber = -1;
        this.state = State.RECEIVING;
        this.startedAt = Instant.now();
    }

    /**
     * Cria e abre uma nova sessão de recepção.
     *
     * @param parameters     parâmetros da sessão negociados durante o handshake; não deve ser {@code null}
     * @param destinationPath caminho absoluto onde o arquivo recebido será escrito; não deve estar em branco
     * @return uma nova {@link ReceiverSession} no estado {@link State#RECEIVING}
     * @throws NullPointerException     se {@code parameters} for {@code null}
     * @throws IllegalArgumentException se {@code destinationPath} estiver em branco
     */
    public static ReceiverSession open(SessionParameters parameters, String destinationPath) {
        Objects.requireNonNull(parameters, "parameters não deve ser nulo");
        if (destinationPath == null || destinationPath.isBlank()) {
            throw new IllegalArgumentException("destinationPath não deve estar em branco");
        }
        return new ReceiverSession(parameters, destinationPath);
    }

    // -------------------------------------------------------------------------
    // Mutadores voltados para a FSM
    // -------------------------------------------------------------------------


    public void advanceExpectedSequenceNumber() {
        requireState(State.RECEIVING);
        expectedSequenceNumber++;
    }

    /**
     * Registra o número de sequência do ACK enviado mais recentemente.
     *
     * @param seqnum o número de sequência que foi reconhecido (acknowledged)
     */
    public void recordAcknowledgement(int seqnum) {
        requireState(State.RECEIVING);
        validateSequenceNumber(seqnum);
        this.lastAcknowledgedSequenceNumber = seqnum;
    }

    /**
     * Transiciona a sessão para {@link State#CLOSED} e registra o timestamp de finalização.
     *
     * @throws IllegalStateException se a sessão já estiver fechada
     */
    public void close() {
        requireState(State.RECEIVING);
        this.state = State.CLOSED;
        this.finishedAt = Instant.now();
    }

    // -------------------------------------------------------------------------
    // Acessores
    // -------------------------------------------------------------------------

    /** @return os parâmetros da sessão recebidos durante o handshake */
    public SessionParameters getParameters() {
        return parameters;
    }

    /** @return caminho absoluto de destino para o arquivo sendo recebido */
    public String getDestinationPath() {
        return destinationPath;
    }

    /** @return o próximo número de sequência que a FSM GBN aceitará */
    public int getExpectedSequenceNumber() {
        return expectedSequenceNumber;
    }

    /**
     * @return o número de sequência do último ACK enviado, ou {@code -1} se nenhum
     *         ACK tiver sido enviado ainda
     */
    public int getLastAcknowledgedSequenceNumber() {
        return lastAcknowledgedSequenceNumber;
    }

    /** @return o estado atual desta sessão */
    public State getState() {
        return state;
    }

    /** @return o instante em que esta sessão foi aberta */
    public Instant getStartedAt() {
        return startedAt;
    }

    /**
     * @return o instante em que esta sessão foi fechada, ou {@code null} se ainda estiver aberta
     */
    public Instant getFinishedAt() {
        return finishedAt;
    }

    /**
     * @return {@code true} se a sessão ainda estiver aceitando pacotes
     */
    public boolean isReceiving() {
        return state == State.RECEIVING;
    }

    // -------------------------------------------------------------------------
    // Métodos auxiliares
    // -------------------------------------------------------------------------



    private void requireState(State expected) {
        if (state != expected) {
            throw new IllegalStateException(
                    "Operação requer o estado " + expected + " mas o estado atual é " + state);
        }
    }

    private void validateSequenceNumber(int seqnum) {
        if (seqnum < 0) {
            throw new IllegalArgumentException(
                    "O número de sequência deve ser não negativo: " + seqnum);
        }
    }

    @Override
    public String toString() {
        return "ReceiverSession{"
                + "state=" + state
                + ", expectedSeqNum=" + expectedSequenceNumber
                + ", lastAck=" + lastAcknowledgedSequenceNumber
                + ", destination='" + destinationPath + '\''
                + '}';
    }
}
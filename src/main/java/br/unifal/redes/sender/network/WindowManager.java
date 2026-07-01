package br.unifal.redes.sender.network;

/**
 * Maintains the state of the Go-Back-N sender's sliding window.
 *
 * <p>This class is a pure protocol-state component. It tracks the base
 * sequence number, the next sequence number to be assigned, and the
 * configured window size, and exposes the operations the sender's FSM needs
 * to decide when it may send, when it must wait, and how to slide the
 * window forward as cumulative ACKs arrive.
 *
 * <p>This class performs no network I/O, no file I/O, no timing, and no
 * retransmission logic. It does not know what a packet looks like on the
 * wire — it only reasons about sequence numbers as integers.
 *
 * <p>Thread-safety: this class is not synchronized. It is intended to be
 * owned and driven by a single sender FSM thread; callers that need
 * concurrent access must coordinate externally.
 */
public final class WindowManager {

    private final int windowSize;

    /**
     * Sequence number of the oldest packet sent but not yet acknowledged.
     * Initialized to 0 per GBN convention (base = 0).
     */
    private int base;

    /**
     * Sequence number that will be assigned to the next packet sent.
     * Initialized to 0 per GBN convention (nextseqnum = 0).
     */
    private int next;

    /**
     * Creates a new window manager with the given window size, starting the
     * base and next sequence numbers at {@code 0}.
     *
     * @param windowSize the maximum number of unacknowledged packets allowed
     *                   in flight at once; must be &gt; 0
     * @throws IllegalArgumentException if {@code windowSize} is not positive
     */
    public WindowManager(int windowSize) {
        this(windowSize, 0);
    }

    /**
     * Cria um novo gerenciador de janela com o tamanho de janela informado,
     * iniciando os números de sequência base e próximo em
     * {@code initialSequenceNumber} em vez do tradicional {@code 0}.
     *
     * <p>Esta sobrecarga existe para permitir que o protocolo reserve
     * deliberadamente certos números de sequência fora do espaço de
     * numeração dos segmentos DATA — por exemplo, reservando a sequência
     * {@code 0} exclusivamente para o pacote de HANDSHAKE e seu ACK, e
     * iniciando a numeração dos segmentos DATA em {@code 1}. Esta classe
     * continua não sabendo nada sobre HANDSHAKE ou qualquer outro tipo de
     * pacote — apenas aceita, de forma genérica, qual deve ser o ponto de
     * partida da numeração.
     *
     * @param windowSize            o número máximo de pacotes não confirmados
     *                              permitidos em trânsito simultaneamente; deve ser &gt; 0
     * @param initialSequenceNumber o número de sequência a partir do qual a
     *                              janela deve começar a contar; deve ser &gt;= 0
     * @throws IllegalArgumentException se {@code windowSize} não for positivo,
     *                                   ou se {@code initialSequenceNumber} for negativo
     */
    public WindowManager(int windowSize, int initialSequenceNumber) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be > 0, got: " + windowSize);
        }
        if (initialSequenceNumber < 0) {
            throw new IllegalArgumentException(
                    "initialSequenceNumber must be >= 0, got: " + initialSequenceNumber);
        }
        this.windowSize = windowSize;
        this.base = initialSequenceNumber;
        this.next = initialSequenceNumber;
    }

    // -------------------------------------------------------------------------
    // Window queries
    // -------------------------------------------------------------------------

    /**
     * Indicates whether the window currently has room to send another packet.
     *
     * @return {@code true} if {@link #getInFlightCount()} is strictly less
     *         than the configured window size
     */
    public boolean canSend() {
        return getInFlightCount() < windowSize;
    }

    /**
     * Indicates whether the window is at full capacity.
     *
     * @return {@code true} if no more packets may be sent without first
     *         sliding the window forward; equivalent to {@code !canSend()}
     */
    public boolean isFull() {
        return !canSend();
    }

    /**
     * @return the number of packets currently in flight (sent but not yet
     *         acknowledged), i.e. {@code next - base}
     */
    public int getInFlightCount() {
        return next - base;
    }

    // -------------------------------------------------------------------------
    // FSM-facing mutators
    // -------------------------------------------------------------------------

    /**
     * Records that a packet was logically sent, consuming and returning the
     * current next-sequence-number, then advancing it by one.
     *
     * <p>Callers should check {@link #canSend()} before invoking this method;
     * it does not block or wait, it only enforces the invariant defensively.
     *
     * @return the sequence number assigned to the packet that was just sent
     * @throws IllegalStateException if the window is already full
     */
    public int packetSent() {
        if (isFull()) {
            throw new IllegalStateException(
                    "Cannot send: window is full (inFlight=" + getInFlightCount()
                            + ", windowSize=" + windowSize + ")");
        }
        int assignedSequenceNumber = next;
        next++;
        return assignedSequenceNumber;
    }

    /**
     * Slides the window forward in response to a cumulative ACK.
     *
     * <p>{@code ackSequenceNumber} is interpreted using standard GBN
     * cumulative-ACK semantics: it confirms that every packet with a sequence
     * number up to and including {@code ackSequenceNumber} has been
     * delivered. The new base becomes {@code ackSequenceNumber + 1}.
     *
     * <p>Stale or duplicate ACKs — where {@code ackSequenceNumber + 1} does
     * not move the base strictly forward — are silently ignored, since
     * receiving duplicate ACKs is normal, expected behavior in Go-Back-N and
     * should not be treated as an error.
     *
     * @param ackSequenceNumber the highest sequence number being
     *                          cumulatively acknowledged; must be &gt;= 0
     * @throws IllegalArgumentException if {@code ackSequenceNumber} is
     *                                   negative, or if it acknowledges a
     *                                   sequence number that was never sent
     *                                   (i.e. {@code ackSequenceNumber >= next})
     */
    public void processAck(int ackSequenceNumber) {
        if (ackSequenceNumber < 0) {
            throw new IllegalArgumentException(
                    "ackSequenceNumber must be >= 0, got: " + ackSequenceNumber);
        }
        if (ackSequenceNumber >= next) {
            throw new IllegalArgumentException(
                    "ackSequenceNumber (" + ackSequenceNumber
                            + ") acknowledges a sequence number never sent (next=" + next + ")");
        }

        int newBase = ackSequenceNumber + 1;
        if (newBase <= base) {
            return; // stale or duplicate ACK — normal in GBN, no-op
        }
        base = newBase;
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** @return the current base sequence number (oldest unacknowledged packet) */
    public int getBaseSequenceNumber() {
        return base;
    }

    /** @return the sequence number that will be assigned to the next packet sent */
    public int getNextSequenceNumber() {
        return next;
    }

    /** @return the configured maximum number of unacknowledged packets in flight */
    public int getWindowSize() {
        return windowSize;
    }

    @Override
    public String toString() {
        return "WindowManager{"
                + "base=" + base
                + ", next=" + next
                + ", windowSize=" + windowSize
                + ", inFlight=" + getInFlightCount()
                + '}';
    }
}
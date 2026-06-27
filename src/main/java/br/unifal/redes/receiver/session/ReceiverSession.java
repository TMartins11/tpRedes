package br.unifal.redes.receiver.session;

import br.unifal.redes.common.SessionParameters;

import java.time.Instant;
import java.util.Objects;

/**
 * Represents the mutable state of a single Go-Back-N reception session.
 *
 * <p>This class is the single source of truth for protocol-level state during a
 * transfer. It does not perform any network I/O or file I/O. Its sole job is to
 * hold and expose the values that the FSM will read and update as packets arrive.
 *
 * <p>Instances are created once per transfer, via {@link #open(SessionParameters)},
 * and closed exactly once via {@link #close()}.
 *
 * <p>Thread-safety: individual field accessors are not synchronized. The FSM that
 * drives this session is responsible for coordinating concurrent access when
 * applicable.
 */
public final class ReceiverSession {

    /** Protocol state of this session. */
    public enum State {
        /** Handshake received; ready to accept data packets. */
        RECEIVING,
        /** FIN received or transfer completed; no more data expected. */
        CLOSED
    }

    private final SessionParameters parameters;
    private final String destinationPath;
    private final Instant startedAt;

    /**
     * Next sequence number the receiver will accept.
     * Initialized to 0 per GBN FSM (expectedseqnum = 0).
     */
    private int expectedSequenceNumber;

    /**
     * Sequence number of the last ACK sent.
     * Initialized to -1 to indicate no ACK has been sent yet.
     */
    private int lastAcknowledgedSequenceNumber;

    private State state;
    private Instant finishedAt;

    // -------------------------------------------------------------------------
    // Construction
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
     * Creates and opens a new reception session.
     *
     * @param parameters     session parameters negotiated during handshake; must not be {@code null}
     * @param destinationPath absolute path where the received file will be written; must not be blank
     * @return a new {@link ReceiverSession} in {@link State#RECEIVING}
     * @throws NullPointerException     if {@code parameters} is {@code null}
     * @throws IllegalArgumentException if {@code destinationPath} is blank
     */
    public static ReceiverSession open(SessionParameters parameters, String destinationPath) {
        Objects.requireNonNull(parameters, "parameters must not be null");
        if (destinationPath == null || destinationPath.isBlank()) {
            throw new IllegalArgumentException("destinationPath must not be blank");
        }
        return new ReceiverSession(parameters, destinationPath);
    }

    // -------------------------------------------------------------------------
    // FSM-facing mutators
    // -------------------------------------------------------------------------


    public void advanceExpectedSequenceNumber() {
        requireState(State.RECEIVING);
        expectedSequenceNumber++;
    }

    /**
     * Records the sequence number of the most recently sent ACK.
     *
     * @param seqnum the sequence number that was acknowledged
     */
    public void recordAcknowledgement(int seqnum) {
        requireState(State.RECEIVING);
        validateSequenceNumber(seqnum);
        this.lastAcknowledgedSequenceNumber = seqnum;
    }

    /**
     * Transitions the session to {@link State#CLOSED} and records the finish timestamp.
     *
     * @throws IllegalStateException if the session is already closed
     */
    public void close() {
        requireState(State.RECEIVING);
        this.state = State.CLOSED;
        this.finishedAt = Instant.now();
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** @return the session parameters received during handshake */
    public SessionParameters getParameters() {
        return parameters;
    }

    /** @return absolute destination path for the file being received */
    public String getDestinationPath() {
        return destinationPath;
    }

    /** @return the next sequence number the GBN FSM will accept */
    public int getExpectedSequenceNumber() {
        return expectedSequenceNumber;
    }

    /**
     * @return the sequence number of the last ACK sent, or {@code -1} if no
     *         ACK has been sent yet
     */
    public int getLastAcknowledgedSequenceNumber() {
        return lastAcknowledgedSequenceNumber;
    }

    /** @return the current state of this session */
    public State getState() {
        return state;
    }

    /** @return the instant this session was opened */
    public Instant getStartedAt() {
        return startedAt;
    }

    /**
     * @return the instant this session was closed, or {@code null} if still open
     */
    public Instant getFinishedAt() {
        return finishedAt;
    }

    /**
     * @return {@code true} if the session is still accepting packets
     */
    public boolean isReceiving() {
        return state == State.RECEIVING;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------



    private void requireState(State expected) {
        if (state != expected) {
            throw new IllegalStateException(
                    "Operation requires state " + expected + " but current state is " + state);
        }
    }

    private void validateSequenceNumber(int seqnum) {
        if (seqnum < 0) {
            throw new IllegalArgumentException(
                    "Sequence number must be non-negative: " + seqnum);
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
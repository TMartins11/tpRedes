package br.unifal.redes.sender.network;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Buffers packets that have been sent but not yet acknowledged.
 *
 * <p>This class is a pure state-management component. It associates each
 * outstanding packet with the sequence number it was sent under, and exposes
 * the operations the sender's Go-Back-N retransmission logic needs: looking
 * up a single buffered packet, dropping packets confirmed by a cumulative
 * ACK, and listing every packet that would need to be resent in a
 * retransmission burst.
 *
 * <p>This class performs no network I/O, no file I/O, and schedules no
 * background work. It does not know how to serialize, send, or interpret a
 * packet — {@code T} is treated as an opaque payload; only the sequence
 * number is meaningful to this class.
 *
 * <p>Packets are kept ordered by sequence number internally, so
 * {@link #getOutstandingPackets()} always returns them in ascending sequence
 * order, matching the order a Go-Back-N retransmission burst must resend
 * them in.
 *
 * <p>Thread-safety: this class is not synchronized. It is intended to be
 * owned and driven by a single sender FSM thread; callers that need
 * concurrent access must coordinate externally.
 *
 * @param <T> the type of packet payload being buffered; this class does not
 *            inspect or depend on its structure
 */
public final class PacketBuffer<T> {

    private final NavigableMap<Integer, T> buffer = new TreeMap<>();

    // -------------------------------------------------------------------------
    // FSM-facing mutators
    // -------------------------------------------------------------------------

    /**
     * Buffers {@code packet} as the outstanding packet for {@code seqNum}.
     *
     * @param seqNum the sequence number the packet was sent under; must be &gt;= 0
     * @param packet the packet payload to buffer; must not be {@code null}
     * @throws IllegalArgumentException if {@code seqNum} is negative
     * @throws NullPointerException     if {@code packet} is {@code null}
     * @throws IllegalStateException    if a packet is already buffered for
     *                                   {@code seqNum} — the caller must
     *                                   remove or replace it explicitly
     *                                   rather than silently overwriting it
     */
    public void add(int seqNum, T packet) {
        if (seqNum < 0) {
            throw new IllegalArgumentException("seqNum must be >= 0, got: " + seqNum);
        }
        Objects.requireNonNull(packet, "packet must not be null");
        if (buffer.containsKey(seqNum)) {
            throw new IllegalStateException("A packet is already buffered for seqNum " + seqNum);
        }
        buffer.put(seqNum, packet);
    }

    /**
     * Removes every buffered packet with a sequence number less than or
     * equal to {@code ackSeqNum}, reflecting a cumulative ACK.
     *
     * <p>This mirrors standard Go-Back-N cumulative-ACK semantics: an ACK for
     * {@code ackSeqNum} confirms delivery of every packet up to and
     * including it, so all of them can be dropped from the buffer.
     *
     * <p>If no buffered packet qualifies — e.g. a stale or duplicate ACK —
     * this method is a no-op rather than an error, since that is normal,
     * expected behavior in Go-Back-N.
     *
     * @param ackSeqNum the highest sequence number being cumulatively
     *                  acknowledged; must be &gt;= 0
     * @throws IllegalArgumentException if {@code ackSeqNum} is negative
     */
    public void removeUpTo(int ackSeqNum) {
        if (ackSeqNum < 0) {
            throw new IllegalArgumentException("ackSeqNum must be >= 0, got: " + ackSeqNum);
        }
        buffer.headMap(ackSeqNum, true).clear();
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /**
     * Returns the packet buffered for {@code seqNum}.
     *
     * @param seqNum the sequence number to look up; must be &gt;= 0
     * @return the packet buffered for {@code seqNum}
     * @throws IllegalArgumentException if {@code seqNum} is negative
     * @throws NoSuchElementException   if no packet is buffered for {@code seqNum}
     */
    public T get(int seqNum) {
        if (seqNum < 0) {
            throw new IllegalArgumentException("seqNum must be >= 0, got: " + seqNum);
        }
        T packet = buffer.get(seqNum);
        if (packet == null) {
            throw new NoSuchElementException("No packet buffered for seqNum " + seqNum);
        }
        return packet;
    }

    /**
     * Returns every currently buffered packet, paired with the sequence
     * number it was sent under, ordered ascending by sequence number.
     *
     * <p>This is the list a Go-Back-N retransmission burst would resend, in
     * the order it must resend them.
     *
     * @return an immutable, ascending-order list of (sequence number, packet) pairs
     */
    public List<Map.Entry<Integer, T>> getOutstandingPackets() {
        return buffer.entrySet().stream()
                .map(entry -> (Map.Entry<Integer, T>)
                        new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue()))
                .toList();
    }

    /** @return {@code true} if no packets are currently buffered */
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /** @return the number of packets currently buffered */
    public int size() {
        return buffer.size();
    }

    @Override
    public String toString() {
        return "PacketBuffer{"
                + "size=" + buffer.size()
                + ", seqNums=" + buffer.keySet()
                + '}';
    }
}
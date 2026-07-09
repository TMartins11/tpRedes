package br.unifal.redes.sender.network;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;

public final class PacketBuffer<T> {

    private final NavigableMap<Integer, T> buffer = new TreeMap<>();

    // -------------------------------------------------------------------------
    // Mutadores voltados para a FSM
    // -------------------------------------------------------------------------


    public void add(int seqNum, T packet) {
        if (seqNum < 0) {
            throw new IllegalArgumentException("seqNum deve ser >= 0, recebido: " + seqNum);
        }
        Objects.requireNonNull(packet, "packet não deve ser nulo");
        if (buffer.containsKey(seqNum)) {
            throw new IllegalStateException("Já existe um pacote em buffer para o seqNum " + seqNum);
        }
        buffer.put(seqNum, packet);
    }

    public void removeUpTo(int ackSeqNum) {
        if (ackSeqNum < 0) {
            throw new IllegalArgumentException("ackSeqNum deve ser >= 0, recebido: " + ackSeqNum);
        }
        buffer.headMap(ackSeqNum, true).clear();
    }

    // -------------------------------------------------------------------------
    // Consultas
    // -------------------------------------------------------------------------

    /**
     * Retorna o pacote armazenado em buffer para {@code seqNum}.
     *
     * @param seqNum o número de sequência a ser consultado; deve ser &gt;= 0
     * @return o pacote armazenado em buffer para {@code seqNum}
     * @throws IllegalArgumentException se {@code seqNum} for negativo
     * @throws NoSuchElementException   se nenhum pacote estiver em buffer para {@code seqNum}
     */
    public T get(int seqNum) {
        if (seqNum < 0) {
            throw new IllegalArgumentException("seqNum deve ser >= 0, recebido: " + seqNum);
        }
        T packet = buffer.get(seqNum);
        if (packet == null) {
            throw new NoSuchElementException("Nenhum pacote em buffer para o seqNum " + seqNum);
        }
        return packet;
    }

    /**
     * Retorna todos os pacotes atualmente em buffer, pareados com o número
     * de sequência sob o qual foram enviados, ordenados de forma crescente por número de sequência.
     *
     * <p>Esta é a lista que um burst de retransmissão Go-Back-N reenviaria, na
     * ordem em que deve reenviá-los.
     *
     * @return uma lista imutável e em ordem crescente de pares (número de sequência, pacote)
     */
    public List<Map.Entry<Integer, T>> getOutstandingPackets() {
        return buffer.entrySet().stream()
                .map(entry -> (Map.Entry<Integer, T>)
                        new AbstractMap.SimpleImmutableEntry<>(entry.getKey(), entry.getValue()))
                .toList();
    }

    /** @return {@code true} se nenhum pacote estiver atualmente em buffer */
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    /** @return o número de pacotes atualmente em buffer */
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
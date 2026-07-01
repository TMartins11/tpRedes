package br.unifal.redes.sender.network;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Armazena em buffer pacotes que foram enviados mas ainda não foram reconhecidos (acknowledged).
 *
 * <p>Esta classe é um componente puro de gerenciamento de estado. Ela associa cada
 * pacote pendente ao número de sequência sob o qual foi enviado e expõe
 * as operações que a lógica de retransmissão Go-Back-N do transmissor necessita:
 * buscar um único pacote em buffer, remover pacotes confirmados por um ACK
 * cumulativo e listar cada pacote que precisaria ser reenviado em um
 * burst de retransmissão.
 *
 * <p>Esta classe não realiza E/S de rede, E/S de arquivo e não agenda nenhum
 * trabalho em segundo plano. Ela não sabe como serializar, enviar ou interpretar um
 * pacote — {@code T} é tratado como um payload opaco; apenas o número
 * de sequência é significativo para esta classe.
 *
 * <p>Os pacotes são mantidos ordenados internamente por número de sequência, portanto
 * {@link #getOutstandingPackets()} sempre os retorna em ordem crescente de sequência,
 * correspondendo à ordem em que um burst de retransmissão Go-Back-N deve reenviá-los.
 *
 * <p>Segurança de thread: esta classe não é sincronizada. Ela é destinada a ser
 * de propriedade e conduzida por uma única thread da FSM do transmissor; chamadores que
 * precisam de acesso concorrente devem coordenar externamente.
 *
 * @param <T> o tipo de payload do pacote sendo armazenado em buffer; esta classe não
 *            inspeciona ou depende de sua estrutura
 */
public final class PacketBuffer<T> {

    private final NavigableMap<Integer, T> buffer = new TreeMap<>();

    // -------------------------------------------------------------------------
    // Mutadores voltados para a FSM
    // -------------------------------------------------------------------------

    /**
     * Armazena em buffer {@code packet} como o pacote pendente para {@code seqNum}.
     *
     * @param seqNum o número de sequência sob o qual o pacote foi enviado; deve ser &gt;= 0
     * @param packet o payload do pacote a ser armazenado em buffer; não deve ser {@code null}
     * @throws IllegalArgumentException se {@code seqNum} for negativo
     * @throws NullPointerException     se {@code packet} for {@code null}
     * @throws IllegalStateException    se um pacote já estiver em buffer para
     *                                   {@code seqNum} — o chamador deve
     *                                   removê-lo ou substituí-lo explicitamente
     *                                   em vez de sobrescrevê-lo silenciosamente
     */
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

    /**
     * Remove todos os pacotes em buffer com um número de sequência menor ou
     * igual a {@code ackSeqNum}, refletindo um ACK cumulativo.
     *
     * <p>Isso espelha a semântica padrão de ACK cumulativo do Go-Back-N: um ACK para
     * {@code ackSeqNum} confirma a entrega de todos os pacotes até e
     * incluindo ele, portanto todos podem ser removidos do buffer.
     *
     * <p>Se nenhum pacote em buffer se qualificar — por exemplo, um ACK desatualizado ou duplicado —
     * este método não tem efeito em vez de gerar erro, pois esse é um comportamento
     * normal e esperado no Go-Back-N.
     *
     * @param ackSeqNum o maior número de sequência sendo reconhecido
     *                  cumulativamente; deve ser &gt;= 0
     * @throws IllegalArgumentException se {@code ackSeqNum} for negativo
     */
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
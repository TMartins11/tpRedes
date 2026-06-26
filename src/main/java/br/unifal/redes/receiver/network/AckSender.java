package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Envia datagramas de confirmação (ACK) pelo {@link DatagramSocket} fornecido.
 *
 * <p>Responsabilidade única: dado um número de sequência a confirmar e o
 * endereço do remetente, construir o {@link Packet} de ACK, serializá-lo e
 * despachá-lo via UDP.
 *
 * <p>Esta classe <strong>não conhece</strong>:
 * <ul>
 *   <li>a FSM ou qualquer regra do protocolo Go-Back-N;</li>
 *   <li>sessão, estatísticas ou escrita de arquivo;</li>
 *   <li>quando ou por que um ACK deve ser enviado — essa decisão é da FSM.</li>
 * </ul>
 *
 * <p>O {@link DatagramSocket} é injetado no construtor e <em>não é de
 * propriedade</em> desta classe — o chamador é responsável por abri-lo e
 * fechá-lo.
 *
 * <p><strong>Thread-safety:</strong> {@link DatagramSocket#send} é sincronizado
 * internamente pelo JDK. Ainda assim, recomenda-se que apenas uma thread
 * chame {@link #sendAck} por vez para evitar entrelaçamento de ACKs.
 */
public final class AckSender {

    private final DatagramSocket socket;

    // -------------------------------------------------------------------------
    // Construção
    // -------------------------------------------------------------------------

    /**
     * Cria um {@code AckSender} que envia ACKs pelo socket informado.
     *
     * @param socket socket UDP já aberto; não pode ser {@code null}
     * @throws NullPointerException se {@code socket} for {@code null}
     */
    public AckSender(DatagramSocket socket) {
        this.socket = Objects.requireNonNull(socket, "socket não pode ser null");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    /**
     * Constrói um ACK cumulativo para {@code ackNum}, serializa-o e o envia
     * ao endereço e porta indicados.
     *
     * <p>O ACK é criado via {@link Packet#createAck(int)}, que zera os campos
     * {@code seqNum} e {@code data} — conforme a FSM do receptor GBN, que
     * confirma apenas o último pacote recebido em ordem.
     *
     * @param ackNum          número de sequência a ser confirmado cumulativamente; deve ser ≥ 0
     * @param enderecoDestino endereço IP do emissor para o qual o ACK será enviado;
     *                        não pode ser {@code null}
     * @param portaDestino    porta UDP do emissor; deve estar em [1, 65535]
     * @throws NullPointerException     se {@code enderecoDestino} for {@code null}
     * @throws IllegalArgumentException se {@code ackNum} for negativo ou
     *                                  {@code portaDestino} estiver fora do intervalo válido
     * @throws IOException              se ocorrer falha de rede ao enviar o datagrama
     */
    public void sendAck(int ackNum, InetAddress enderecoDestino, int portaDestino)
            throws IOException {

        Objects.requireNonNull(enderecoDestino, "enderecoDestino não pode ser null");

        if (ackNum < 0) {
            throw new IllegalArgumentException(
                    "ackNum deve ser >= 0, recebido: " + ackNum);
        }
        if (portaDestino < 1 || portaDestino > 65535) {
            throw new IllegalArgumentException(
                    "portaDestino deve estar em [1, 65535], recebido: " + portaDestino);
        }

        Packet ack = Packet.createAck(ackNum);
        byte[] bytes = PacketSerializer.serialize(ack);

        DatagramPacket datagram = new DatagramPacket(
                bytes,
                bytes.length,
                enderecoDestino,
                portaDestino
        );

        socket.send(datagram);
    }
}
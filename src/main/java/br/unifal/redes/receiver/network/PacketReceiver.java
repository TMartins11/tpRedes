package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Objects;

/**
 * Aguarda datagramas UDP em um {@link DatagramSocket} e os converte em
 * objetos {@link IncomingPacket} prontos para consumo pela FSM.
 *
 * <p>Responsabilidade única: receber um datagrama bruto, delegar a
 * desserialização ao {@link PacketSerializer} e retornar o resultado
 * encapsulado com as informações de endereçamento do remetente.
 *
 * <p>Esta classe <strong>não conhece</strong>:
 * <ul>
 *   <li>a FSM ou qualquer regra do protocolo Go-Back-N;</li>
 *   <li>sessão, estatísticas ou escrita de arquivo;</li>
 *   <li>ACKs ou lógica de retransmissão.</li>
 * </ul>
 *
 * <p>O {@link DatagramSocket} é injetado no construtor e <em>não é de
 * propriedade</em> desta classe — o chamador é responsável por abri-lo e
 * fechá-lo. Isso facilita testes sem abrir sockets reais.
 *
 * <p><strong>Thread-safety:</strong> {@link DatagramSocket#receive} bloqueia
 * a thread chamante. Não compartilhe uma instância desta classe entre múltiplas
 * threads.
 */
public final class PacketReceiver {

    private final DatagramSocket socket;

    // -------------------------------------------------------------------------
    // Construção
    // -------------------------------------------------------------------------

    /**
     * Cria um {@code PacketReceiver} que lê do socket informado.
     *
     * @param socket socket UDP já vinculado a uma porta; não pode ser {@code null}
     * @throws NullPointerException se {@code socket} for {@code null}
     */
    public PacketReceiver(DatagramSocket socket) {
        this.socket = Objects.requireNonNull(socket, "socket não pode ser null");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    /**
     * Bloqueia até que um datagrama UDP chegue, desserializa-o e retorna um
     * {@link IncomingPacket} contendo o {@link Packet} decodificado e o
     * endereço/porta do remetente.
     *
     * <p>Um novo buffer é alocado a cada chamada para evitar que dados de
     * um datagrama anterior contaminem o atual.
     *
     * @return o próximo {@link IncomingPacket} recebido pela rede
     * @throws IOException se o socket estiver fechado, ocorrer erro de rede ou
     *                     os bytes recebidos não formarem um {@link Packet} válido
     */
    public IncomingPacket receber() throws IOException {
        byte[] buffer = new byte[Packet.MAX_DATAGRAM_SIZE];
        DatagramPacket datagrama = new DatagramPacket(buffer, buffer.length);

        socket.receive(datagrama);

        /*
         * Passa apenas os bytes efetivamente recebidos (datagrama.getLength()),
         * não o buffer inteiro. PacketSerializer.deserialize exige o comprimento
         * real — bytes excedentes com valor zero causariam falha na validação
         * do cabeçalho.
         */
        Packet packet = PacketSerializer.deserialize(
                datagrama.getData(),
                datagrama.getLength()
        );

        return new IncomingPacket(packet, datagrama.getAddress(), datagrama.getPort());
    }
}
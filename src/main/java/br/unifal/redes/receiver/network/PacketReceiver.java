package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Objects;

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
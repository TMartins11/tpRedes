package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Objects;

public final class AckSender {

    private final DatagramSocket socket;

    // -------------------------------------------------------------------------
    // Construção
    // -------------------------------------------------------------------------

    public AckSender(DatagramSocket socket) {
        this.socket = Objects.requireNonNull(socket, "socket não pode ser null");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

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
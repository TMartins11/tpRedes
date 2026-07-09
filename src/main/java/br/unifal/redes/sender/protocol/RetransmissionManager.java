package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.sender.network.PacketBuffer;
import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.sender.network.TimeoutManager;
import br.unifal.redes.sender.statistics.SenderStatistics;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class RetransmissionManager {

    /** Construtor privado — classe utilitária, não instanciável. */
    private RetransmissionManager() {
        throw new AssertionError("RetransmissionManager não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    public static void retransmitOutstandingPackets(SenderSocketService socketService,
                                                    PacketBuffer<Packet> packetBuffer,
                                                    TimeoutManager timeoutManager,
                                                    SenderStatistics statistics,
                                                    InetAddress receiverAddress,
                                                    int receiverPort) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        Objects.requireNonNull(packetBuffer, "packetBuffer não pode ser nulo");
        Objects.requireNonNull(timeoutManager, "timeoutManager não pode ser nulo");
        Objects.requireNonNull(statistics, "statistics não pode ser nulo");
        Objects.requireNonNull(receiverAddress, "receiverAddress não pode ser nulo");
        validateReceiverPort(receiverPort);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de retransmitir pacotes; chame open() primeiro"
            );
        }

        if (packetBuffer.isEmpty()) {
            return;
        }

        List<Map.Entry<Integer, Packet>> outstandingPackets = packetBuffer.getOutstandingPackets();

        for (Map.Entry<Integer, Packet> entry : outstandingPackets) {
            Packet packet = entry.getValue();

            byte[] datagramBytes = PacketCodec.encode(packet);

            socketService.send(datagramBytes, receiverAddress, receiverPort);

            statistics.recordPacketSent();
            statistics.recordRetransmission();
            statistics.recordBytesSent(packet.getDataLength());
        }

        timeoutManager.restart();
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    private static void validateReceiverPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("receiverPort must be in [1, 65535], got: " + port);
        }
    }
}
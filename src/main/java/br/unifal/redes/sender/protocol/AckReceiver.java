package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.sender.network.SenderSocketService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.Objects;


public final class AckReceiver {

    /** Construtor privado — classe utilitária, não instanciável. */
    private AckReceiver() {
        throw new AssertionError("AckReceiver não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    public static Packet receiveAck(SenderSocketService socketService,
                                    int maxDatagramSize) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        validateMaxDatagramSize(maxDatagramSize);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de receber um ACK; chame open() primeiro"
            );
        }

        // 1. Recebe um único datagrama UDP, bloqueando até que ele chegue
        //    (ou até o timeout do socket, se configurado).
        DatagramPacket received = socketService.receive(maxDatagramSize);

        // 2. Decodifica os bytes recebidos de volta em um Packet, delegando
        //    toda a lógica de formato binário a PacketCodec — nenhuma
        //    lógica de desserialização é duplicada aqui.
        Packet packet = PacketCodec.decode(received.getData(), received.getLength());

        // 3. Garante que o pacote recebido é realmente um ACK. Qualquer
        //    outro tipo (DATA, HANDSHAKE, FIN) indica que o Receptor enviou
        //    algo inesperado neste ponto do protocolo, e isso é tratado
        //    como um erro de estado, não como um caso a ser silenciosamente
        //    ignorado ou reinterpretado.
        if (!packet.isAck()) {
            throw new IllegalStateException(
                    "Pacote recebido não é um ACK: " + packet
            );
        }

        return packet;
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Valida que {@code maxDatagramSize} é um tamanho de buffer válido,
     * falhando rapidamente antes de qualquer chamada bloqueante ao socket.
     *
     * @param maxDatagramSize o tamanho a ser validado
     * @throws IllegalArgumentException se {@code maxDatagramSize} não for positivo
     */
    private static void validateMaxDatagramSize(int maxDatagramSize) {
        if (maxDatagramSize <= 0) {
            throw new IllegalArgumentException(
                    "maxDatagramSize must be > 0, got: " + maxDatagramSize
            );
        }
    }
}
package br.unifal.redes.sender.protocol;

import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

public final class DataSender {

    /** Construtor privado — classe utilitária, não instanciável. */
    private DataSender() {
        throw new AssertionError("DataSender não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    public static void sendData(SenderSocketService socketService,
                                int sequenceNumber,
                                byte[] payload,
                                InetAddress receiverAddress,
                                int receiverPort) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        Objects.requireNonNull(payload, "payload não pode ser nulo");
        Objects.requireNonNull(receiverAddress, "receiverAddress não pode ser nulo");
        validateSequenceNumber(sequenceNumber);
        validatePayloadLength(payload);
        validateReceiverPort(receiverPort);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de enviar um pacote DATA; chame open() primeiro"
            );
        }

        // 1. Monta o pacote DATA usando a fábrica já existente em Packet —
        //    nenhum formato novo é inventado, e nenhuma lógica de protocolo
        //    além do envio em si é implementada aqui.
        Packet dataPacket = Packet.createData(sequenceNumber, payload, payload.length);

        // 2. Serializa o pacote inteiro (cabeçalho + payload) para bytes de
        //    rede, reaproveitando a infraestrutura existente de PacketCodec.
        //    A lógica de serialização não é duplicada nesta classe.
        byte[] datagramBytes = PacketCodec.encode(dataPacket);

        // 3. Envia o datagrama uma única vez. Nenhum loop, nenhum retry,
        //    nenhuma espera por ACK.
        socketService.send(datagramBytes, receiverAddress, receiverPort);
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Valida que {@code sequenceNumber} é um número de sequência válido,
     * falhando rapidamente antes de montar ou serializar qualquer pacote.
     *
     * @param sequenceNumber o número de sequência a ser validado
     * @throws IllegalArgumentException se {@code sequenceNumber} for negativo
     */
    private static void validateSequenceNumber(int sequenceNumber) {
        if (sequenceNumber < 0) {
            throw new IllegalArgumentException(
                    "sequenceNumber must be >= 0, got: " + sequenceNumber
            );
        }
    }

    /**
     * Valida que {@code payload} não excede o tamanho máximo de payload
     * permitido pelo protocolo, falhando rapidamente com uma mensagem
     * específica antes de delegar a {@link Packet#createData(int, byte[], int)}
     * (que já realiza essa mesma validação internamente, mas com uma
     * mensagem genérica de "tamanho do payload").
     *
     * @param payload o payload a ser validado
     * @throws IllegalArgumentException se {@code payload.length} exceder
     *                                   {@link Packet#MAX_PAYLOAD_SIZE}
     */
    private static void validatePayloadLength(byte[] payload) {
        if (payload.length > Packet.MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "payload excede o tamanho máximo permitido ("
                            + Packet.MAX_PAYLOAD_SIZE + "): " + payload.length
            );
        }
    }

    /**
     * Valida que {@code port} está no intervalo de portas UDP de destino
     * válidas, falhando rapidamente antes de montar ou serializar qualquer
     * pacote.
     *
     * @param port a porta a ser validada
     * @throws IllegalArgumentException se {@code port} estiver fora de {@code [1, 65535]}
     */
    private static void validateReceiverPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("receiverPort must be in [1, 65535], got: " + port);
        }
    }
}
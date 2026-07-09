package br.unifal.redes.sender.protocol;

import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.common.SessionParameters;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public final class HandshakeSender {

    /**
     * Comprimento, em bytes, do payload serializado de {@link SessionParameters}
     * antes do campo de tamanho variável {@code destPath}:
     * 8 (fileSize) + 4 (windowSize) + 8 (lossProb) + 1 (destPathLen) = 21 bytes.
     */
    private static final int PARAMETERS_FIXED_HEADER_SIZE = 21;

    /** Construtor privado — classe utilitária, não instanciável. */
    private HandshakeSender() {
        throw new AssertionError("HandshakeSender não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    public static void sendHandshake(SenderSocketService socketService,
                                     SessionParameters parameters,
                                     InetAddress receiverAddress,
                                     int receiverPort) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        Objects.requireNonNull(parameters, "parameters não pode ser nulo");
        Objects.requireNonNull(receiverAddress, "receiverAddress não pode ser nulo");
        validateReceiverPort(receiverPort);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de enviar o HANDSHAKE; chame open() primeiro"
            );
        }

        // 1. Serializa os parâmetros da sessão no formato de payload já
        //    documentado por SessionParameters.
        byte[] serializedParameters = serializeSessionParameters(parameters);

        // 2. Encapsula o payload em um Packet do tipo HANDSHAKE, usando a
        //    fábrica já existente em Packet — nenhum formato novo é inventado.
        Packet handshakePacket = Packet.createHandshake(serializedParameters);

        // 3. Serializa o Packet inteiro (cabeçalho + payload) para bytes de
        //    rede, reaproveitando a infraestrutura existente de PacketCodec.
        byte[] datagramBytes = PacketCodec.encode(handshakePacket);

        // 4. Envia o datagrama uma única vez. Nenhum loop, nenhum retry.
        socketService.send(datagramBytes, receiverAddress, receiverPort);
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    private static byte[] serializeSessionParameters(SessionParameters parameters) {
        byte[] destPathBytes = parameters.getDestPath().getBytes(StandardCharsets.UTF_8);

        // Validação defensiva: mesmo que o construtor de SessionParameters já
        // garanta essa invariante, validamos novamente aqui para que este
        // método nunca produza um payload inconsistente com o campo
        // destPathLen de 1 byte.
        if (destPathBytes.length > SessionParameters.MAX_PATH_LENGTH) {
            throw new IllegalArgumentException(
                    "destPath excede " + SessionParameters.MAX_PATH_LENGTH
                            + " bytes UTF-8: " + destPathBytes.length
            );
        }

        int totalSize = PARAMETERS_FIXED_HEADER_SIZE + destPathBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN);

        buffer.putLong(parameters.getFileSize());
        buffer.putInt(parameters.getWindowSize());
        buffer.putDouble(parameters.getLossProb());
        buffer.put((byte) destPathBytes.length);
        buffer.put(destPathBytes);

        return buffer.array();
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
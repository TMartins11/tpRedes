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

/**
 * Responsável por montar e enviar o pacote HANDSHAKE inicial do Emissor no
 * protocolo Go-Back-N.
 *
 * <p>Esta classe tem uma única responsabilidade: serializar
 * {@link SessionParameters} no formato de payload já documentado pela
 * própria classe {@code SessionParameters}, encapsular esses bytes em um
 * {@link Packet} do tipo {@code HANDSHAKE} (via {@link Packet#createHandshake(byte[])}),
 * serializar esse pacote para bytes de rede (via {@link PacketCodec#encode(Packet)})
 * e enviá-lo através de um {@link SenderSocketService} já aberto.
 *
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não implementa a FSM do Emissor.</li>
 *   <li>Não implementa retransmissão nem qualquer tipo de retry — o
 *       HANDSHAKE é enviado exatamente uma vez por chamada.</li>
 *   <li>Não envia pacotes de dados (DATA) nem processa ACKs.</li>
 *   <li>Não cria threads nem realiza laços de repetição.</li>
 *   <li>Não conhece {@code WindowManager}, {@code TimeoutManager} ou
 *       {@code PacketBuffer} — nenhuma lógica de janela deslizante ou de
 *       timeout de retransmissão pertence a esta classe.</li>
 *   <li>Não armazena estado de sessão: não guarda referência ao
 *       {@code SenderSocketService}, ao {@code SessionParameters} nem a
 *       qualquer outro dado entre chamadas. Cada chamada a
 *       {@link #sendHandshake} é completamente independente.</li>
 * </ul>
 *
 * <p>Como é apenas uma coleção de métodos estáticos, esta classe não pode
 * ser instanciada — assim como {@link PacketCodec}.
 */
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

    /**
     * Monta o pacote HANDSHAKE a partir de {@code parameters} e o envia, uma
     * única vez, para {@code receiverAddress}:{@code receiverPort} através de
     * {@code socketService}.
     *
     * <p>Esta operação é síncrona e bloqueante (herdando esse comportamento
     * de {@link SenderSocketService#send}), não realiza nenhuma tentativa
     * adicional em caso de falha, e não aguarda nenhuma resposta do
     * Receptor — aguardar e processar a confirmação do HANDSHAKE é
     * responsabilidade de outra parte do Emissor (a FSM), fora do escopo
     * desta classe.
     *
     * @param socketService   o serviço de socket UDP, já aberto, a ser usado
     *                        para o envio; não pode ser {@code null}
     * @param parameters      os parâmetros da sessão a serem enviados no
     *                        payload do HANDSHAKE; não pode ser {@code null}
     * @param receiverAddress o endereço IP do Receptor; não pode ser {@code null}
     * @param receiverPort    a porta UDP do Receptor; deve estar no intervalo {@code [1, 65535]}
     * @throws NullPointerException     se {@code socketService}, {@code parameters}
     *                                   ou {@code receiverAddress} forem {@code null}
     * @throws IllegalArgumentException se {@code receiverPort} estiver fora do
     *                                   intervalo válido
     * @throws IllegalStateException    se {@code socketService} não estiver aberto
     * @throws IOException              se ocorrer um erro de E/S durante o envio
     */
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

    /**
     * Serializa {@code parameters} no formato de payload fixo + variável já
     * documentado pela própria classe {@link SessionParameters}:
     * <pre>
     *   Offset  Tamanho  Campo
     *   0       8 bytes  fileSize      (long)
     *   8       4 bytes  windowSize    (int)
     *   12      8 bytes  lossProb      (double)
     *   20      1 byte   destPathLen   (byte — comprimento do path)
     *   21      N bytes  destPath      (UTF-8, N ≤ 255)
     * </pre>
     *
     * <p>Este método não inventa um formato novo — apenas implementa,
     * usando {@link ByteBuffer}, o layout binário que {@link SessionParameters}
     * já descreve em seu próprio Javadoc, já que nenhuma classe de
     * serialização dedicada a {@code SessionParameters} foi fornecida ao
     * projeto até o momento.
     *
     * @param parameters os parâmetros a serem serializados; não pode ser {@code null}
     * @return um novo {@code byte[]} com o payload serializado, de tamanho
     *         {@code 21 + N}, onde {@code N} é o comprimento em bytes UTF-8 de {@code destPath}
     * @throws IllegalArgumentException se o caminho de destino exceder
     *                                   {@link SessionParameters#MAX_PATH_LENGTH}
     *                                   bytes UTF-8 (verificação defensiva — em
     *                                   condições normais, o construtor de
     *                                   {@code SessionParameters} já garante
     *                                   essa invariante)
     */
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
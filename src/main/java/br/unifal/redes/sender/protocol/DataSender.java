package br.unifal.redes.sender.protocol;

import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

/**
 * Responsável por montar e enviar um único pacote DATA do Emissor no
 * protocolo Go-Back-N.
 *
 * <p>Esta classe tem uma única responsabilidade — o fluxo:
 * <pre>
 *   payload + número de sequência
 *               ↓
 *         pacote DATA       (via {@link Packet#createData(int, byte[], int)})
 *               ↓
 *         PacketCodec       (via {@link PacketCodec#encode(Packet)})
 *               ↓
 *   SenderSocketService.send(...)
 * </pre>
 *
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não gerencia janela deslizante e não conhece {@code WindowManager}.</li>
 *   <li>Não conhece {@code TimeoutManager} — não inicia, reinicia ou cancela
 *       nenhum temporizador de retransmissão.</li>
 *   <li>Não conhece {@code PacketBuffer} — não guarda o pacote enviado para
 *       uma eventual retransmissão futura.</li>
 *   <li>Não processa ACKs nem recebe nenhum tipo de pacote — esta classe é
 *       de mão única (somente envio).</li>
 *   <li>Não realiza retransmissão nem qualquer tipo de retry — o pacote DATA
 *       é enviado exatamente uma vez por chamada.</li>
 *   <li>Não atualiza nenhuma estatística (essa responsabilidade pertence a
 *       {@code SenderStatistics}, que deve ser atualizada pelo chamador,
 *       não por esta classe).</li>
 *   <li>Não realiza leitura de arquivos — recebe o payload já pronto,
 *       previamente lido por {@code FileChunkReader}.</li>
 *   <li>Não cria threads nem realiza laços de repetição.</li>
 *   <li>Não implementa a FSM do Emissor nem controla o fluxo do protocolo —
 *       decidir quando enviar, quando reenviar, e qual sequência usar é
 *       responsabilidade exclusiva de componentes de nível superior.</li>
 *   <li>Não armazena estado de sessão: não guarda referência a nenhum dos
 *       parâmetros recebidos entre chamadas. Cada chamada a
 *       {@link #sendData} é completamente independente, seguindo o mesmo
 *       padrão arquitetural já estabelecido em {@code HandshakeSender}.</li>
 * </ul>
 *
 * <p>Como é apenas uma coleção de métodos estáticos, esta classe não pode
 * ser instanciada — assim como {@link PacketCodec} e {@code HandshakeSender}.
 */
public final class DataSender {

    /** Construtor privado — classe utilitária, não instanciável. */
    private DataSender() {
        throw new AssertionError("DataSender não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    /**
     * Monta um pacote DATA com {@code sequenceNumber} e {@code payload}, e o
     * envia, uma única vez, para {@code receiverAddress}:{@code receiverPort}
     * através de {@code socketService}.
     *
     * <p>Esta operação é síncrona e bloqueante (herdando esse comportamento
     * de {@link SenderSocketService#send}), não realiza nenhuma tentativa
     * adicional em caso de falha, e não aguarda nenhum ACK do Receptor —
     * aguardar e processar a confirmação deste DATA, bem como decidir se e
     * quando retransmiti-lo, é responsabilidade de outras camadas do
     * Emissor (a FSM, em conjunto com {@code WindowManager},
     * {@code TimeoutManager} e {@code PacketBuffer}), fora do escopo desta
     * classe.
     *
     * <p>O array {@code payload} é considerado, em sua totalidade, como os
     * bytes válidos do segmento — ou seja, {@code payload.length} é usado
     * diretamente como o parâmetro {@code length} de
     * {@link Packet#createData(int, byte[], int)}. Isso é consistente com a
     * forma como {@code FileChunkReader} já produz seus blocos (cada
     * {@code byte[]} retornado já vem recortado para o tamanho exato do
     * bloco, sem espaço sobrando no final do array).
     *
     * @param socketService   o serviço de socket UDP, já aberto, a ser usado
     *                        para o envio; não pode ser {@code null}
     * @param sequenceNumber  o número de sequência do segmento DATA; deve
     *                        ser {@code >= 0}
     * @param payload         os bytes do segmento de dados a serem enviados;
     *                        não pode ser {@code null}; seu tamanho não pode
     *                        exceder {@link Packet#MAX_PAYLOAD_SIZE}
     * @param receiverAddress o endereço IP do Receptor; não pode ser {@code null}
     * @param receiverPort    a porta UDP do Receptor; deve estar no intervalo {@code [1, 65535]}
     * @throws NullPointerException     se {@code socketService}, {@code payload}
     *                                   ou {@code receiverAddress} forem {@code null}
     * @throws IllegalArgumentException se {@code sequenceNumber} for negativo;
     *                                   se {@code payload} exceder
     *                                   {@link Packet#MAX_PAYLOAD_SIZE}; ou se
     *                                   {@code receiverPort} estiver fora do
     *                                   intervalo válido
     * @throws IllegalStateException    se {@code socketService} não estiver aberto
     * @throws IOException              se ocorrer um erro de E/S durante o envio
     */
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
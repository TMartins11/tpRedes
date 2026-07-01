package br.unifal.redes.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Classe utilitária responsável por converter entre {@link Packet} e sua
 * representação binária ({@code byte[]}), conforme o formato de cabeçalho
 * definido no protocolo Go-Back-N do projeto.
 *
 * <p>Formato do cabeçalho serializado (11 bytes fixos, big-endian):
 * <pre>
 *   Offset  Tamanho  Campo
 *   0       1 byte   type       (PacketType.code)
 *   1       4 bytes  seqNum     (int)
 *   5       4 bytes  ackNum     (int)
 *   9       2 bytes  dataLength (short)
 *   11      N bytes  payload    (0 ≤ N ≤ Packet.MAX_PAYLOAD_SIZE)
 * </pre>
 *
 * <p>Esta classe é puramente utilitária: não realiza E/S de rede, não
 * realiza E/S de arquivo, não implementa nenhuma lógica de Go-Back-N
 * (janela deslizante, retransmissão, timeout) e não mantém estado entre
 * chamadas. Cada chamada a {@link #encode(Packet)} ou {@link #decode(byte[])}
 * é independente e sem efeitos colaterais.
 *
 * <p>A classe não modifica {@link Packet} nem {@link PacketType}; toda a
 * reconstrução de um {@link Packet} na desserialização é feita exclusivamente
 * através dos métodos de fábrica públicos já existentes em {@link Packet}
 * ({@code createData}, {@code createAck}, {@code createHandshake},
 * {@code createFin}).
 *
 * <p>Como é apenas uma coleção de métodos estáticos, esta classe não pode
 * ser instanciada.
 */
public final class PacketCodec {

    /** Construtor privado — classe utilitária, não instanciável. */
    private PacketCodec() {
        throw new AssertionError("PacketCodec não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Serialização: Packet -> byte[]
    // -------------------------------------------------------------------------

    /**
     * Serializa {@code packet} em sua representação binária, conforme o
     * formato de cabeçalho do protocolo.
     *
     * @param packet o pacote a ser serializado; não pode ser {@code null}
     * @return um novo {@code byte[]} contendo o cabeçalho seguido do payload,
     *         com tamanho total igual a {@code Packet.HEADER_SIZE + dataLength}
     * @throws NullPointerException     se {@code packet} for {@code null}
     * @throws IllegalArgumentException se o payload do pacote exceder
     *                                   {@code Packet.MAX_PAYLOAD_SIZE}
     *                                   (verificação defensiva — em condições
     *                                   normais, {@link Packet} já garante
     *                                   essa invariante internamente)
     */
    public static byte[] encode(Packet packet) {
        Objects.requireNonNull(packet, "packet não pode ser nulo");

        // getData() já retorna uma cópia defensiva (ver Packet.getData()),
        // então é seguro escrevê-la diretamente no buffer.
        byte[] payload = packet.getData();
        int dataLength = packet.getDataLength();

        // Validação defensiva: mesmo que Packet já garanta essa invariante
        // em seus métodos de fábrica, validamos novamente aqui para que o
        // codec nunca produza um datagrama inconsistente.
        if (dataLength > Packet.MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "Payload do pacote excede o tamanho máximo permitido ("
                            + Packet.MAX_PAYLOAD_SIZE + "): " + dataLength
            );
        }

        ByteBuffer buffer = ByteBuffer
                .allocate(Packet.HEADER_SIZE + dataLength)
                .order(ByteOrder.BIG_ENDIAN);

        buffer.put(packet.getType().getCode());
        buffer.putInt(packet.getSeqNum());
        buffer.putInt(packet.getAckNum());
        buffer.putShort((short) dataLength);
        if (dataLength > 0) {
            buffer.put(payload);
        }

        return buffer.array();
    }

    // -------------------------------------------------------------------------
    // Desserialização: byte[] -> Packet
    // -------------------------------------------------------------------------

    /**
     * Desserializa um datagrama completo (todo o array é considerado válido)
     * de volta em um {@link Packet}.
     *
     * <p>Equivalente a {@code decode(datagram, datagram.length)}. Use a
     * sobrecarga {@link #decode(byte[], int)} quando o array recebido for
     * maior que os dados efetivamente recebidos — por exemplo, o buffer de
     * um {@code DatagramPacket} no qual apenas {@code getLength()} bytes são
     * válidos.
     *
     * @param datagram os bytes recebidos; não pode ser {@code null}
     * @return o {@link Packet} reconstruído a partir dos bytes
     * @throws NullPointerException     se {@code datagram} for {@code null}
     * @throws IllegalArgumentException se o datagrama for inválido (ver
     *                                   {@link #decode(byte[], int)} para a
     *                                   lista completa de validações)
     */
    public static Packet decode(byte[] datagram) {
        Objects.requireNonNull(datagram, "datagram não pode ser nulo");
        return decode(datagram, datagram.length);
    }

    /**
     * Desserializa os primeiros {@code length} bytes de {@code datagram} de
     * volta em um {@link Packet}.
     *
     * <p>Esta sobrecarga existe especificamente para o caso de uso de UDP:
     * o buffer de um {@code DatagramPacket} normalmente é maior que os bytes
     * efetivamente recebidos, e apenas {@code DatagramPacket.getLength()}
     * bytes (não {@code datagram.length}) devem ser considerados.
     *
     * @param datagram array contendo os bytes recebidos; não pode ser {@code null}
     * @param length   quantidade de bytes válidos em {@code datagram},
     *                 a partir do offset 0; deve estar em {@code [0, datagram.length]}
     * @return o {@link Packet} reconstruído a partir dos bytes
     * @throws NullPointerException     se {@code datagram} for {@code null}
     * @throws IllegalArgumentException se {@code length} estiver fora do
     *                                   intervalo válido; se {@code length}
     *                                   for menor que {@code Packet.HEADER_SIZE}
     *                                   (datagrama menor que o cabeçalho mínimo);
     *                                   se o código de tipo lido for desconhecido
     *                                   (ver {@link PacketType#fromCode(byte)});
     *                                   se o {@code dataLength} declarado no
     *                                   cabeçalho exceder
     *                                   {@code Packet.MAX_PAYLOAD_SIZE}; ou se o
     *                                   {@code dataLength} declarado for
     *                                   inconsistente com {@code length}
     *                                   (isto é, {@code HEADER_SIZE + dataLength != length})
     */
    public static Packet decode(byte[] datagram, int length) {
        Objects.requireNonNull(datagram, "datagram não pode ser nulo");

        if (length < 0 || length > datagram.length) {
            throw new IllegalArgumentException(
                    "length inválido: " + length + " (tamanho do array: " + datagram.length + ")"
            );
        }

        // Validação obrigatória: datagrama menor que o cabeçalho mínimo.
        if (length < Packet.HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "Datagrama menor que o cabeçalho mínimo (" + Packet.HEADER_SIZE
                            + " bytes): " + length + " bytes recebidos"
            );
        }

        ByteBuffer buffer = ByteBuffer
                .wrap(datagram, 0, length)
                .order(ByteOrder.BIG_ENDIAN);

        byte typeCode = buffer.get();
        // Validação obrigatória: PacketType inválido.
        // PacketType.fromCode já lança IllegalArgumentException com uma
        // mensagem descritiva caso o código seja desconhecido — não há
        // necessidade de duplicar essa verificação aqui.
        PacketType type = PacketType.fromCode(typeCode);

        int seqNum = buffer.getInt();
        int ackNum = buffer.getInt();

        // Lemos a short com Short.toUnsignedInt para tratar corretamente o
        // caso de um datagrama corrompido em que o campo dataLength carregue
        // um padrão de bits que, interpretado como short com sinal, seria
        // negativo. Convertendo para o intervalo [0, 65535], a validação de
        // limite superior abaixo captura esse caso de forma previsível, em
        // vez de propagar um valor negativo para o restante do método.
        int dataLength = Short.toUnsignedInt(buffer.getShort());

        // Validação obrigatória: payload maior que MAX_PAYLOAD_SIZE.
        if (dataLength > Packet.MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "dataLength declarado no cabeçalho excede o payload máximo permitido ("
                            + Packet.MAX_PAYLOAD_SIZE + "): " + dataLength
            );
        }

        // Validação obrigatória: dataLength inconsistente com o tamanho recebido.
        int expectedTotalLength = Packet.HEADER_SIZE + dataLength;
        if (expectedTotalLength != length) {
            throw new IllegalArgumentException(
                    "dataLength declarado (" + dataLength
                            + ") é inconsistente com o tamanho do datagrama recebido ("
                            + length + " bytes; esperado " + expectedTotalLength + " bytes)"
            );
        }

        byte[] payload = new byte[dataLength];
        if (dataLength > 0) {
            buffer.get(payload);
        }

        return buildPacket(type, seqNum, ackNum, payload);
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Reconstrói um {@link Packet} a partir dos campos já lidos do
     * cabeçalho, delegando ao método de fábrica apropriado conforme o
     * {@link PacketType}.
     *
     * <p>Cada método de fábrica de {@link Packet} usa apenas os campos
     * relevantes para aquele tipo de pacote (por exemplo,
     * {@code createData} ignora {@code ackNum} e {@code createAck} ignora
     * {@code seqNum}), exatamente como {@link Packet} já faz ao criar esses
     * pacotes originalmente — por isso o round-trip serialização ↔
     * desserialização é fiel para pacotes originalmente criados através das
     * fábricas públicas de {@link Packet}.
     *
     * @param type    o tipo do pacote, já validado
     * @param seqNum  o número de sequência lido do cabeçalho
     * @param ackNum  o número de confirmação lido do cabeçalho
     * @param payload o payload lido do datagrama (pode ter tamanho zero)
     * @return o {@link Packet} reconstruído
     */
    private static Packet buildPacket(PacketType type, int seqNum, int ackNum, byte[] payload) {
        return switch (type) {
            case DATA -> Packet.createData(seqNum, payload, payload.length);
            case ACK -> Packet.createAck(ackNum);
            case HANDSHAKE -> Packet.createHandshake(payload);
            case FIN -> Packet.createFin(seqNum);
        };
    }
}
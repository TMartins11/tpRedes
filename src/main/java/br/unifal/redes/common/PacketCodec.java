package br.unifal.redes.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

public final class PacketCodec {

    private PacketCodec() {
        throw new AssertionError("PacketCodec não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Serialização: Packet -> byte[]
    // -------------------------------------------------------------------------

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

    public static Packet decode(byte[] datagram) {
        Objects.requireNonNull(datagram, "datagram não pode ser nulo");
        return decode(datagram, datagram.length);
    }

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

    private static Packet buildPacket(PacketType type, int seqNum, int ackNum, byte[] payload) {
        return switch (type) {
            case DATA -> Packet.createData(seqNum, payload, payload.length);
            case ACK -> Packet.createAck(ackNum);
            case HANDSHAKE -> Packet.createHandshake(payload);
            case FIN -> Packet.createFin(seqNum);
        };
    }
}
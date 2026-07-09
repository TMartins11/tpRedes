package br.unifal.redes.common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Responsável por converter {@link Packet} ↔ {@code byte[]} usando
 * {@link ByteBuffer} com ordem big-endian.
 *
 * <h2>Formato do datagrama</h2>
 * <pre>
 *   Byte 0      : type (1 byte)
 *   Bytes 1-4   : seqNum (int, 4 bytes, big-endian)
 *   Bytes 5-8   : ackNum (int, 4 bytes, big-endian)
 *   Bytes 9-10  : dataLength (short, 2 bytes, big-endian)
 *   Bytes 11+   : data (dataLength bytes)
 * </pre>
 */
public final class PacketSerializer {

    /** Esta classe é utilitária — sem instâncias. */
    private PacketSerializer() {}

    // -------------------------------------------------------------------------
    // Serialização: Packet → byte[]
    // -------------------------------------------------------------------------

    public static byte[] serialize(Packet packet) {
        int dataLength = packet.getDataLength();

        if (dataLength > Packet.MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "Payload excede o tamanho máximo: " + dataLength
            );
        }

        ByteBuffer buffer = ByteBuffer
                .allocate(Packet.HEADER_SIZE + dataLength)
                .order(ByteOrder.BIG_ENDIAN);

        buffer.put(packet.getType().getCode());   // 1 byte  — tipo
        buffer.putInt(packet.getSeqNum());         // 4 bytes — seqNum
        buffer.putInt(packet.getAckNum());         // 4 bytes — ackNum
        buffer.putShort((short) dataLength);       // 2 bytes — dataLength

        if (dataLength > 0) {
            buffer.put(packet.getData());          // N bytes — payload
        }

        return buffer.array();
    }

    public static Packet deserialize(byte[] raw, int length) {
        if (length < Packet.HEADER_SIZE) {
            throw new IllegalArgumentException(
                    "Datagrama muito curto para conter um cabeçalho válido: "
                            + length + " bytes"
            );
        }

        ByteBuffer buffer = ByteBuffer
                .wrap(raw, 0, length)
                .order(ByteOrder.BIG_ENDIAN);

        // Lê cabeçalho na mesma ordem em que foi escrito
        PacketType type       = PacketType.fromCode(buffer.get());
        int        seqNum     = buffer.getInt();
        int        ackNum     = buffer.getInt();
        short      dataLength = buffer.getShort();

        if (dataLength < 0 || dataLength > Packet.MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "dataLength inválido no cabeçalho: " + dataLength
            );
        }

        int expectedTotal = Packet.HEADER_SIZE + dataLength;
        if (length < expectedTotal) {
            throw new IllegalArgumentException(
                    "Datagrama incompleto: esperado " + expectedTotal
                            + " bytes, recebido " + length
            );
        }

        byte[] data = new byte[dataLength];
        if (dataLength > 0) {
            buffer.get(data);
        }

        // Reconstrói o Packet usando os factory methods corretos
        return switch (type) {
            case DATA      -> Packet.createData(seqNum, data, dataLength);
            case ACK       -> Packet.createAck(ackNum);
            case HANDSHAKE -> Packet.createHandshake(data);
            case FIN       -> Packet.createFin(seqNum);
        };
    }

    // -------------------------------------------------------------------------
    // Serialização dos parâmetros de sessão (payload do HANDSHAKE)
    // -------------------------------------------------------------------------

    public static byte[] serializeSessionParameters(SessionParameters params) {
        byte[] pathBytes = params.getDestPath()
                .getBytes(StandardCharsets.UTF_8);

        int totalSize = 8 + 4 + 8 + 1 + pathBytes.length; // 21 + pathLen

        ByteBuffer buffer = ByteBuffer
                .allocate(totalSize)
                .order(ByteOrder.BIG_ENDIAN);

        buffer.putLong(params.getFileSize());
        buffer.putInt(params.getWindowSize());
        buffer.putDouble(params.getLossProb());
        buffer.put((byte) pathBytes.length);
        buffer.put(pathBytes);

        return buffer.array();
    }

    public static SessionParameters deserializeSessionParameters(byte[] payload) {
        if (payload.length < 21) {
            throw new IllegalArgumentException(
                    "Payload de HANDSHAKE muito curto: " + payload.length
            );
        }

        ByteBuffer buffer = ByteBuffer
                .wrap(payload)
                .order(ByteOrder.BIG_ENDIAN);

        long   fileSize   = buffer.getLong();
        int    windowSize = buffer.getInt();
        double lossProb   = buffer.getDouble();
        int    pathLen    = Byte.toUnsignedInt(buffer.get());

        if (payload.length < 21 + pathLen) {
            throw new IllegalArgumentException(
                    "Payload de HANDSHAKE truncado ao ler destPath"
            );
        }

        byte[] pathBytes = new byte[pathLen];
        buffer.get(pathBytes);
        String destPath = new String(pathBytes, StandardCharsets.UTF_8);

        return new SessionParameters(fileSize, windowSize, lossProb, destPath);
    }
}
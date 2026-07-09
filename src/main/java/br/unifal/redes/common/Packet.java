package br.unifal.redes.common;

import java.util.Arrays;

/**
 * Representa um segmento do protocolo Go-Back-N.
 * <p>Formato do cabeçalho serializado (11 bytes fixos):
 * <pre>
 *   Offset  Tamanho  Campo
 *   0       1 byte   type      (PacketType.code)
 *   1       4 bytes  seqNum    (int, big-endian)
 *   5       4 bytes  ackNum    (int, big-endian)
 *   9       2 bytes  dataLength (short, big-endian)
 *   11      N bytes  data      (0 ≤ N ≤ MAX_PAYLOAD_SIZE)
 * </pre>
 */
public final class Packet {

    public static final int MAX_PAYLOAD_SIZE = 1024;

    public static final int HEADER_SIZE = 11;

    public static final int MAX_DATAGRAM_SIZE = HEADER_SIZE + MAX_PAYLOAD_SIZE;

    private final PacketType type;
    private final int seqNum;
    private final int ackNum;
    private final byte[] data;   // cópia defensiva — nunca expõe referência interna

    // -------------------------------------------------------------------------
    // Construtores privados — criação via factory methods estáticos
    // -------------------------------------------------------------------------

    private Packet(PacketType type, int seqNum, int ackNum, byte[] data) {
        this.type   = type;
        this.seqNum = seqNum;
        this.ackNum = ackNum;
        // Cópia defensiva: garante imutabilidade mesmo se o chamador
        // modificar o array original após a criação.
        this.data   = (data != null && data.length > 0)
                ? Arrays.copyOf(data, data.length)
                : new byte[0];
    }

    // -------------------------------------------------------------------------
    // Factory methods — nomes expressivos que documentam a intenção
    // -------------------------------------------------------------------------

    public static Packet createData(int seqNum, byte[] data, int length) {
        if (length < 0 || length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "Tamanho do payload inválido: " + length
            );
        }
        return new Packet(PacketType.DATA, seqNum, 0, Arrays.copyOf(data, length));
    }

    public static Packet createAck(int ackNum) {
        return new Packet(PacketType.ACK, 0, ackNum, new byte[0]);
    }

    public static Packet createHandshake(byte[] data) {
        return new Packet(PacketType.HANDSHAKE, 0, 0, data);
    }

    public static Packet createFin(int seqNum) {
        return new Packet(PacketType.FIN, seqNum, 0, new byte[0]);
    }

    // -------------------------------------------------------------------------
    // Getters — sem setters (imutabilidade)
    // -------------------------------------------------------------------------

    public PacketType getType() {
        return type;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public int getAckNum() {
        return ackNum;
    }

    public byte[] getData() {
        return Arrays.copyOf(data, data.length);
    }

    public int getDataLength() {
        return data.length;
    }

    // -------------------------------------------------------------------------
    // Métodos utilitários
    // -------------------------------------------------------------------------

    public boolean isData()      { return type == PacketType.DATA; }
    public boolean isAck()       { return type == PacketType.ACK; }
    public boolean isHandshake() { return type == PacketType.HANDSHAKE; }
    public boolean isFin()       { return type == PacketType.FIN; }

    @Override
    public String toString() {
        return String.format(
                "Packet{type=%s, seqNum=%d, ackNum=%d, dataLength=%d}",
                type, seqNum, ackNum, data.length
        );
    }
}
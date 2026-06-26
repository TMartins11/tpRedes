package br.unifal.redes.common;

import java.util.Arrays;

/**
 * Teste manual (sem JUnit) para validar serialização/deserialização
 * antes de integrar com UDP.
 *
 * Execute com:
 *   javac *.java && java br.unifal.redes.common.SerializationTest
 */
public class SerializationTest {

    public static void main(String[] args) {
        testDataPacket();
        testAckPacket();
        testFinPacket();
        testHandshakeWithSessionParameters();
        testInvalidPacket();
        System.out.println("\n✅  Todos os testes passaram.");
    }

    private static void testDataPacket() {
        byte[] payload = "Hello GBN".getBytes();
        Packet original = Packet.createData(42, payload, payload.length);

        byte[] serialized   = PacketSerializer.serialize(original);
        Packet deserialized = PacketSerializer.deserialize(serialized, serialized.length);

        assert deserialized.isData()                          : "Tipo incorreto";
        assert deserialized.getSeqNum() == 42                 : "SeqNum incorreto";
        assert deserialized.getDataLength() == payload.length : "DataLength incorreto";
        assert Arrays.equals(deserialized.getData(), payload) : "Payload incorreto";

        System.out.println("[OK] DATA packet: " + deserialized);
    }

    private static void testAckPacket() {
        Packet original     = Packet.createAck(17);
        byte[] serialized   = PacketSerializer.serialize(original);
        Packet deserialized = PacketSerializer.deserialize(serialized, serialized.length);

        assert deserialized.isAck()            : "Tipo incorreto";
        assert deserialized.getAckNum() == 17  : "AckNum incorreto";
        assert deserialized.getDataLength() == 0 : "DataLength deve ser 0 em ACK";

        System.out.println("[OK] ACK packet: " + deserialized);
    }

    private static void testFinPacket() {
        Packet original     = Packet.createFin(99);
        byte[] serialized   = PacketSerializer.serialize(original);
        Packet deserialized = PacketSerializer.deserialize(serialized, serialized.length);

        assert deserialized.isFin()            : "Tipo incorreto";
        assert deserialized.getSeqNum() == 99  : "SeqNum incorreto";

        System.out.println("[OK] FIN packet: " + deserialized);
    }

    private static void testHandshakeWithSessionParameters() {
        SessionParameters original = new SessionParameters(
                1_048_576L, 8, 0.10, "/tmp/foto_recebida.jpg"
        );

        byte[] payload = PacketSerializer.serializeSessionParameters(original);
        Packet handshake = Packet.createHandshake(payload);

        // Simula o ciclo completo: serializa o Packet, depois deserializa
        byte[] datagram     = PacketSerializer.serialize(handshake);
        Packet received     = PacketSerializer.deserialize(datagram, datagram.length);
        SessionParameters p = PacketSerializer.deserializeSessionParameters(received.getData());

        assert p.getFileSize()   == 1_048_576L         : "FileSize incorreto";
        assert p.getWindowSize() == 8                  : "WindowSize incorreto";
        assert Math.abs(p.getLossProb() - 0.10) < 1e-9 : "LossProb incorreto";
        assert p.getDestPath().equals("/tmp/foto_recebida.jpg") : "DestPath incorreto";

        System.out.println("[OK] HANDSHAKE + SessionParameters: " + p);
    }

    private static void testInvalidPacket() {
        try {
            PacketSerializer.deserialize(new byte[3], 3);
            throw new AssertionError("Deveria ter lançado exceção para datagrama curto");
        } catch (IllegalArgumentException e) {
            System.out.println("[OK] Rejeição de datagrama inválido: " + e.getMessage());
        }
    }
}
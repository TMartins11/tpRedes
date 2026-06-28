package br.unifal.redes.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("PacketSerializer")
class PacketSerializerTest {

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * Serializa e deserializa um Packet, retornando o resultado.
     * Usado para testar round-trips sem repetição.
     */
    private static Packet roundTrip(Packet original) {
        byte[] bytes = PacketSerializer.serialize(original);
        return PacketSerializer.deserialize(bytes, bytes.length);
    }

    /**
     * Monta manualmente um cabeçalho GBN como array de bytes, permitindo
     * injetar valores inválidos que os factory methods não aceitariam.
     */
    private static byte[] montarCabecalho(byte tipo, int seqNum, int ackNum,
                                          short dataLength, byte[] payload) {
        int total = Packet.HEADER_SIZE + (payload != null ? payload.length : 0);
        ByteBuffer buf = ByteBuffer.allocate(total).order(ByteOrder.BIG_ENDIAN);
        buf.put(tipo);
        buf.putInt(seqNum);
        buf.putInt(ackNum);
        buf.putShort(dataLength);
        if (payload != null && payload.length > 0) {
            buf.put(payload);
        }
        return buf.array();
    }

    // =========================================================================
    // Round-trips por tipo de pacote
    // =========================================================================

    @Nested
    @DisplayName("Round-trip por tipo")
    class RoundTrip {

        @Test
        @DisplayName("DATA — seqNum, payload e dataLength preservados")
        void data() {
            byte[] payload = "conteudo de teste".getBytes();
            Packet original = Packet.createData(42, payload, payload.length);

            Packet resultado = roundTrip(original);

            assertTrue(resultado.isData());
            assertEquals(42, resultado.getSeqNum());
            assertEquals(0, resultado.getAckNum());
            assertEquals(payload.length, resultado.getDataLength());
            assertArrayEquals(payload, resultado.getData());
        }

        @Test
        @DisplayName("ACK — ackNum preservado; seqNum e data são zero/vazio")
        void ack() {
            Packet original = Packet.createAck(17);

            Packet resultado = roundTrip(original);

            assertTrue(resultado.isAck());
            assertEquals(17, resultado.getAckNum());
            assertEquals(0, resultado.getSeqNum());
            assertEquals(0, resultado.getDataLength());
        }

        @Test
        @DisplayName("HANDSHAKE — payload preservado byte a byte")
        void handshake() {
            byte[] dados = {0x01, 0x02, 0x03, 0x04};
            Packet original = Packet.createHandshake(dados);

            Packet resultado = roundTrip(original);

            assertTrue(resultado.isHandshake());
            assertArrayEquals(dados, resultado.getData());
        }

        @Test
        @DisplayName("FIN — seqNum preservado; sem payload")
        void fin() {
            Packet original = Packet.createFin(99);

            Packet resultado = roundTrip(original);

            assertTrue(resultado.isFin());
            assertEquals(99, resultado.getSeqNum());
            assertEquals(0, resultado.getDataLength());
        }
    }

    // =========================================================================
    // Casos extremos de payload
    // =========================================================================

    @Nested
    @DisplayName("Casos extremos de payload")
    class CasosExtremos {

        @Test
        @DisplayName("payload vazio (DATA com length == 0) — round-trip correto")
        void payloadVazio() {
            Packet original = Packet.createData(0, new byte[0], 0);
            Packet resultado = roundTrip(original);

            assertEquals(0, resultado.getDataLength());
            assertArrayEquals(new byte[0], resultado.getData());
        }

        @Test
        @DisplayName("payload máximo (1024 bytes) — round-trip correto")
        void payloadMaximo() {
            byte[] payload = new byte[Packet.MAX_PAYLOAD_SIZE];
            Arrays.fill(payload, (byte) 0xAB);
            Packet original = Packet.createData(1, payload, payload.length);

            Packet resultado = roundTrip(original);

            assertEquals(Packet.MAX_PAYLOAD_SIZE, resultado.getDataLength());
            assertArrayEquals(payload, resultado.getData());
        }

        @Test
        @DisplayName("array de serialização tem tamanho correto: HEADER_SIZE + dataLength")
        void tamanhoSerializado() {
            byte[] payload = new byte[100];
            byte[] bytes = PacketSerializer.serialize(Packet.createData(0, payload, 100));

            assertEquals(Packet.HEADER_SIZE + 100, bytes.length);
        }

        @Test
        @DisplayName("ACK serializado tem exatamente HEADER_SIZE bytes")
        void ackTemTamanhoHeader() {
            byte[] bytes = PacketSerializer.serialize(Packet.createAck(0));
            assertEquals(Packet.HEADER_SIZE, bytes.length);
        }
    }

    // =========================================================================
    // deserialize — entradas inválidas
    // =========================================================================

    @Nested
    @DisplayName("deserialize() — entradas inválidas")
    class DeserializeInvalido {

        @Test
        @DisplayName("lança IllegalArgumentException para datagrama menor que HEADER_SIZE")
        void datagramaMenorQueHeader() {
            byte[] curto = new byte[Packet.HEADER_SIZE - 1];
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(curto, curto.length));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para array vazio")
        void arrayVazio() {
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(new byte[0], 0));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para tipo de pacote desconhecido")
        void tipoDesconhecido() {
            // byte 0x7F não é um PacketType válido
            byte[] raw = montarCabecalho((byte) 0x7F, 0, 0, (short) 0, null);
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(raw, raw.length));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para dataLength maior que MAX_PAYLOAD_SIZE")
        void dataLengthAcimaDoMaximo() {
            // Constrói cabeçalho com dataLength = MAX_PAYLOAD_SIZE + 1
            short dataLengthInvalido = (short) (Packet.MAX_PAYLOAD_SIZE + 1);
            byte[] raw = montarCabecalho((byte) 0, 0, 0, dataLengthInvalido, null);
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(raw, raw.length));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para datagrama truncado (length < HEADER + dataLength)")
        void datagramaTruncado() {
            // Cabeçalho declara 10 bytes de payload mas o array tem só o header
            byte[] raw = montarCabecalho((byte) 0, 0, 0, (short) 10, null);
            // Passa length igual ao tamanho do cabeçalho, mas dataLength diz que há 10 bytes a mais
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(raw, raw.length));
        }

        @Test
        @DisplayName("deserialize com length menor que o array real usa apenas os primeiros N bytes")
        void lengthMenorQueArray() {
            // Serializa um DATA com 5 bytes de payload
            byte[] payload = {1, 2, 3, 4, 5};
            byte[] full = PacketSerializer.serialize(Packet.createData(0, payload, payload.length));

            // Passa apenas HEADER_SIZE como length — datagrama sem payload
            // O header declara dataLength=5, mas length diz que há só 11 bytes → truncado
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserialize(full, Packet.HEADER_SIZE));
        }
    }

    // =========================================================================
    // serializeSessionParameters / deserializeSessionParameters
    // =========================================================================

    @Nested
    @DisplayName("SessionParameters — serialização e desserialização")
    class SessionParametersSerde {

        private final SessionParameters PARAMS = new SessionParameters(
                1_048_576L, 8, 0.10, "/tmp/arquivo.jpg"
        );

        @Test
        @DisplayName("round-trip preserva todos os campos")
        void roundTrip() {
            byte[] bytes = PacketSerializer.serializeSessionParameters(PARAMS);
            SessionParameters resultado = PacketSerializer.deserializeSessionParameters(bytes);

            assertEquals(PARAMS.getFileSize(),   resultado.getFileSize());
            assertEquals(PARAMS.getWindowSize(), resultado.getWindowSize());
            assertEquals(PARAMS.getDestPath(),   resultado.getDestPath());
            assertEquals(PARAMS.getLossProb(),   resultado.getLossProb(), 1e-9);
        }

        @Test
        @DisplayName("round-trip via HANDSHAKE completo (serialize → Packet → serialize → deserialize)")
        void roundTripViaHandshake() {
            byte[] payload = PacketSerializer.serializeSessionParameters(PARAMS);
            Packet handshake = Packet.createHandshake(payload);

            // Ciclo completo: embala no datagrama e desembala
            byte[] datagrama = PacketSerializer.serialize(handshake);
            Packet recebido  = PacketSerializer.deserialize(datagrama, datagrama.length);

            SessionParameters resultado =
                    PacketSerializer.deserializeSessionParameters(recebido.getData());

            assertEquals(PARAMS.getFileSize(),   resultado.getFileSize());
            assertEquals(PARAMS.getWindowSize(), resultado.getWindowSize());
            assertEquals(PARAMS.getDestPath(),   resultado.getDestPath());
            assertEquals(PARAMS.getLossProb(),   resultado.getLossProb(), 1e-9);
        }

        @Test
        @DisplayName("lossProb 0.0 é preservado exatamente")
        void lossProbZero() {
            SessionParameters params = new SessionParameters(0L, 1, 0.0, "/a");
            byte[] bytes = PacketSerializer.serializeSessionParameters(params);
            SessionParameters resultado = PacketSerializer.deserializeSessionParameters(bytes);

            assertEquals(0.0, resultado.getLossProb(), 0.0);
        }

        @Test
        @DisplayName("fileSize negativo (arquivo grande > Long.MAX_VALUE / 2) é preservado")
        void fileSizeGrande() {
            SessionParameters params = new SessionParameters(Long.MAX_VALUE, 1, 0.0, "/b");
            byte[] bytes = PacketSerializer.serializeSessionParameters(params);
            SessionParameters resultado = PacketSerializer.deserializeSessionParameters(bytes);

            assertEquals(Long.MAX_VALUE, resultado.getFileSize());
        }

        @Test
        @DisplayName("destPath com caracteres UTF-8 multibyte é preservado")
        void destPathUTF8() {
            // "ção" tem 5 bytes em UTF-8 (c=1, ã=2, o=1, \0=0 - na verdade ção = 5 bytes)
            SessionParameters params = new SessionParameters(0L, 1, 0.0, "/tmp/recepção");
            byte[] bytes = PacketSerializer.serializeSessionParameters(params);
            SessionParameters resultado = PacketSerializer.deserializeSessionParameters(bytes);

            assertEquals("/tmp/recepção", resultado.getDestPath());
        }

        @Test
        @DisplayName("lança IllegalArgumentException para payload menor que 21 bytes")
        void payloadCurto() {
            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserializeSessionParameters(new byte[20]));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para payload truncado no destPath")
        void payloadTruncadoNoPath() {
            // Constrói payload com pathLen=50 mas sem os bytes do path
            ByteBuffer buf = ByteBuffer.allocate(22).order(ByteOrder.BIG_ENDIAN);
            buf.putLong(0L);    // fileSize
            buf.putInt(1);      // windowSize
            buf.putDouble(0.0); // lossProb
            buf.put((byte) 50); // pathLen = 50, mas não há bytes a seguir
            byte[] truncado = buf.array();

            assertThrows(IllegalArgumentException.class,
                    () -> PacketSerializer.deserializeSessionParameters(truncado));
        }
    }

    // =========================================================================
    // Integridade do layout binário (big-endian, offsets)
    // =========================================================================

    @Nested
    @DisplayName("Layout binário")
    class LayoutBinario {

        @Test
        @DisplayName("byte 0 do datagrama contém o código do tipo")
        void byte0EhTipo() {
            byte[] bytes = PacketSerializer.serialize(Packet.createAck(0));
            assertEquals(PacketType.ACK.getCode(), bytes[0]);
        }

        @Test
        @DisplayName("bytes 1-4 contêm seqNum em big-endian")
        void bytes1a4SaoSeqNum() {
            Packet p = Packet.createData(0x01020304, new byte[0], 0);
            byte[] bytes = PacketSerializer.serialize(p);

            assertEquals(0x01, bytes[1] & 0xFF);
            assertEquals(0x02, bytes[2] & 0xFF);
            assertEquals(0x03, bytes[3] & 0xFF);
            assertEquals(0x04, bytes[4] & 0xFF);
        }

        @Test
        @DisplayName("bytes 5-8 contêm ackNum em big-endian")
        void bytes5a8SaoAckNum() {
            Packet p = Packet.createAck(0x0A0B0C0D);
            byte[] bytes = PacketSerializer.serialize(p);

            assertEquals(0x0A, bytes[5] & 0xFF);
            assertEquals(0x0B, bytes[6] & 0xFF);
            assertEquals(0x0C, bytes[7] & 0xFF);
            assertEquals(0x0D, bytes[8] & 0xFF);
        }

        @Test
        @DisplayName("bytes 9-10 contêm dataLength como short big-endian")
        void bytes9e10SaoDataLength() {
            byte[] payload = new byte[300];
            byte[] bytes = PacketSerializer.serialize(Packet.createData(0, payload, 300));

            int dataLength = ((bytes[9] & 0xFF) << 8) | (bytes[10] & 0xFF);
            assertEquals(300, dataLength);
        }

        @Test
        @DisplayName("payload começa no byte 11")
        void payloadComecaNoOffset11() {
            byte[] payload = {0x42};
            byte[] bytes = PacketSerializer.serialize(Packet.createData(0, payload, 1));

            assertEquals(0x42, bytes[11] & 0xFF);
        }
    }
}
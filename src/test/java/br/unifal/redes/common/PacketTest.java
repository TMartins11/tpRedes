package br.unifal.redes.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Packet")
class PacketTest {

    // =========================================================================
    // Constantes
    // =========================================================================

    @Nested
    @DisplayName("Constantes públicas")
    class Constantes {

        @Test
        @DisplayName("MAX_PAYLOAD_SIZE é 1024")
        void maxPayloadSize() {
            assertEquals(1024, Packet.MAX_PAYLOAD_SIZE);
        }

        @Test
        @DisplayName("HEADER_SIZE é 11")
        void headerSize() {
            assertEquals(11, Packet.HEADER_SIZE);
        }

        @Test
        @DisplayName("MAX_DATAGRAM_SIZE é HEADER_SIZE + MAX_PAYLOAD_SIZE")
        void maxDatagramSize() {
            assertEquals(Packet.HEADER_SIZE + Packet.MAX_PAYLOAD_SIZE,
                    Packet.MAX_DATAGRAM_SIZE);
        }
    }

    // =========================================================================
    // createData
    // =========================================================================

    @Nested
    @DisplayName("createData()")
    class CreateData {

        @Test
        @DisplayName("cria pacote do tipo DATA com seqNum e payload corretos")
        void camposBasicos() {
            byte[] payload = {1, 2, 3, 4, 5};
            Packet p = Packet.createData(7, payload, payload.length);

            assertEquals(PacketType.DATA, p.getType());
            assertEquals(7, p.getSeqNum());
            assertEquals(0, p.getAckNum());   // ackNum deve ser 0 em DATA
            assertEquals(5, p.getDataLength());
            assertArrayEquals(payload, p.getData());
        }

        @Test
        @DisplayName("respeita o parâmetro length — usa apenas os primeiros N bytes")
        void respeitaLength() {
            byte[] payload = {10, 20, 30, 40, 50};
            Packet p = Packet.createData(0, payload, 3);

            assertEquals(3, p.getDataLength());
            assertArrayEquals(new byte[]{10, 20, 30}, p.getData());
        }

        @Test
        @DisplayName("aceita payload vazio (length == 0)")
        void payloadVazio() {
            Packet p = Packet.createData(0, new byte[0], 0);
            assertEquals(0, p.getDataLength());
            assertArrayEquals(new byte[0], p.getData());
        }

        @Test
        @DisplayName("aceita payload de tamanho máximo (1024 bytes)")
        void payloadMaximo() {
            byte[] payload = new byte[Packet.MAX_PAYLOAD_SIZE];
            Arrays.fill(payload, (byte) 0xFF);

            Packet p = Packet.createData(99, payload, payload.length);

            assertEquals(Packet.MAX_PAYLOAD_SIZE, p.getDataLength());
            assertArrayEquals(payload, p.getData());
        }

        @Test
        @DisplayName("lança IllegalArgumentException para length negativo")
        void lengthNegativo() {
            assertThrows(IllegalArgumentException.class,
                    () -> Packet.createData(0, new byte[4], -1));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para length > MAX_PAYLOAD_SIZE")
        void lengthAcimaDoMaximo() {
            byte[] grande = new byte[Packet.MAX_PAYLOAD_SIZE + 1];
            assertThrows(IllegalArgumentException.class,
                    () -> Packet.createData(0, grande, grande.length));
        }
    }

    // =========================================================================
    // createAck
    // =========================================================================

    @Nested
    @DisplayName("createAck()")
    class CreateAck {

        @Test
        @DisplayName("cria pacote do tipo ACK com ackNum correto")
        void camposBasicos() {
            Packet p = Packet.createAck(42);

            assertEquals(PacketType.ACK, p.getType());
            assertEquals(0, p.getSeqNum());   // seqNum deve ser 0 em ACK
            assertEquals(42, p.getAckNum());
            assertEquals(0, p.getDataLength());
            assertArrayEquals(new byte[0], p.getData());
        }

        @Test
        @DisplayName("aceita ackNum zero")
        void ackNumZero() {
            Packet p = Packet.createAck(0);
            assertEquals(0, p.getAckNum());
        }

        @Test
        @DisplayName("aceita ackNum com valor grande")
        void ackNumGrande() {
            Packet p = Packet.createAck(Integer.MAX_VALUE);
            assertEquals(Integer.MAX_VALUE, p.getAckNum());
        }
    }

    // =========================================================================
    // createHandshake
    // =========================================================================

    @Nested
    @DisplayName("createHandshake()")
    class CreateHandshake {

        @Test
        @DisplayName("cria pacote do tipo HANDSHAKE com payload correto")
        void camposBasicos() {
            byte[] dados = {0x01, 0x02, 0x03};
            Packet p = Packet.createHandshake(dados);

            assertEquals(PacketType.HANDSHAKE, p.getType());
            assertEquals(0, p.getSeqNum());
            assertEquals(0, p.getAckNum());
            assertArrayEquals(dados, p.getData());
        }

        @Test
        @DisplayName("aceita payload null — trata como array vazio")
        void payloadNull() {
            Packet p = Packet.createHandshake(null);
            assertEquals(0, p.getDataLength());
        }

        @Test
        @DisplayName("aceita payload vazio")
        void payloadVazio() {
            Packet p = Packet.createHandshake(new byte[0]);
            assertEquals(0, p.getDataLength());
        }
    }

    // =========================================================================
    // createFin
    // =========================================================================

    @Nested
    @DisplayName("createFin()")
    class CreateFin {

        @Test
        @DisplayName("cria pacote do tipo FIN com seqNum correto")
        void camposBasicos() {
            Packet p = Packet.createFin(15);

            assertEquals(PacketType.FIN, p.getType());
            assertEquals(15, p.getSeqNum());
            assertEquals(0, p.getAckNum());
            assertEquals(0, p.getDataLength());
        }

        @Test
        @DisplayName("aceita seqNum zero")
        void seqNumZero() {
            Packet p = Packet.createFin(0);
            assertEquals(0, p.getSeqNum());
        }
    }

    // =========================================================================
    // Métodos de predicado (isData / isAck / isHandshake / isFin)
    // =========================================================================

    @Nested
    @DisplayName("Predicados de tipo")
    class Predicados {

        @Test
        @DisplayName("isData() retorna true apenas para DATA")
        void isData() {
            assertTrue(Packet.createData(0, new byte[1], 1).isData());
            assertFalse(Packet.createAck(0).isData());
            assertFalse(Packet.createHandshake(new byte[0]).isData());
            assertFalse(Packet.createFin(0).isData());
        }

        @Test
        @DisplayName("isAck() retorna true apenas para ACK")
        void isAck() {
            assertTrue(Packet.createAck(0).isAck());
            assertFalse(Packet.createData(0, new byte[1], 1).isAck());
            assertFalse(Packet.createHandshake(new byte[0]).isAck());
            assertFalse(Packet.createFin(0).isAck());
        }

        @Test
        @DisplayName("isHandshake() retorna true apenas para HANDSHAKE")
        void isHandshake() {
            assertTrue(Packet.createHandshake(new byte[1]).isHandshake());
            assertFalse(Packet.createData(0, new byte[1], 1).isHandshake());
            assertFalse(Packet.createAck(0).isHandshake());
            assertFalse(Packet.createFin(0).isHandshake());
        }

        @Test
        @DisplayName("isFin() retorna true apenas para FIN")
        void isFin() {
            assertTrue(Packet.createFin(0).isFin());
            assertFalse(Packet.createData(0, new byte[1], 1).isFin());
            assertFalse(Packet.createAck(0).isFin());
            assertFalse(Packet.createHandshake(new byte[0]).isFin());
        }
    }

    // =========================================================================
    // Imutabilidade — cópia defensiva
    // =========================================================================

    @Nested
    @DisplayName("Imutabilidade (cópia defensiva)")
    class Imutabilidade {

        @Test
        @DisplayName("modificar array original após createData não altera o Packet")
        void arrayOriginalNaoAfetaPacket() {
            byte[] payload = {1, 2, 3};
            Packet p = Packet.createData(0, payload, payload.length);

            payload[0] = 99;  // mutação externa

            assertEquals(1, p.getData()[0], "Packet não deve refletir mutação do array original");
        }

        @Test
        @DisplayName("modificar array retornado por getData() não altera o Packet")
        void getDataRetornaCopia() {
            Packet p = Packet.createData(0, new byte[]{10, 20, 30}, 3);

            byte[] copia1 = p.getData();
            copia1[0] = 99;  // mutação da cópia retornada

            byte[] copia2 = p.getData();
            assertEquals(10, copia2[0], "getData() deve retornar cópia independente a cada chamada");
        }

        @Test
        @DisplayName("duas chamadas a getData() retornam arrays distintos com mesmo conteúdo")
        void getDataRetornaNovoArrayCadaVez() {
            Packet p = Packet.createData(0, new byte[]{5, 6, 7}, 3);

            byte[] a = p.getData();
            byte[] b = p.getData();

            assertNotSame(a, b, "getData() não deve retornar a mesma referência interna");
            assertArrayEquals(a, b);
        }
    }

    // =========================================================================
    // toString
    // =========================================================================

    @Nested
    @DisplayName("toString()")
    class ToStringTest {

        @Test
        @DisplayName("contém tipo, seqNum, ackNum e dataLength")
        void formatoCorreto() {
            Packet p = Packet.createData(3, new byte[]{1, 2}, 2);
            String s = p.toString();

            assertTrue(s.contains("DATA"),       "deve conter o tipo");
            assertTrue(s.contains("seqNum=3"),   "deve conter seqNum");
            assertTrue(s.contains("ackNum=0"),   "deve conter ackNum");
            assertTrue(s.contains("dataLength=2"), "deve conter dataLength");
        }

        @Test
        @DisplayName("toString() de ACK contém ackNum correto")
        void toStringAck() {
            String s = Packet.createAck(17).toString();
            assertTrue(s.contains("ACK"));
            assertTrue(s.contains("ackNum=17"));
        }
    }
}
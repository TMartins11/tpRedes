package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Camada de rede: IncomingPacket, PacketReceiver, AckSender")
class NetworkTest {

    private static final InetAddress LOCALHOST;

    static {
        try {
            LOCALHOST = InetAddress.getByName("127.0.0.1");
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // =========================================================================
    // IncomingPacket
    // =========================================================================

    @Nested
    @DisplayName("IncomingPacket")
    class IncomingPacketTest {

        private final Packet pacoteData = Packet.createData(7, new byte[]{1, 2, 3}, 3);

        @Test
        @DisplayName("armazena packet, endereço e porta corretamente")
        void armazenaCampos() {
            IncomingPacket entrada = new IncomingPacket(pacoteData, LOCALHOST, 5000);

            assertSame(pacoteData, entrada.getPacket());
            assertEquals(LOCALHOST, entrada.getEnderecoRemetente());
            assertEquals(5000, entrada.getPortaRemetente());
        }

        @Test
        @DisplayName("lança NullPointerException para packet null")
        void packetNull() {
            assertThrows(NullPointerException.class,
                    () -> new IncomingPacket(null, LOCALHOST, 5000));
        }

        @Test
        @DisplayName("lança NullPointerException para endereço null")
        void enderecoNull() {
            assertThrows(NullPointerException.class,
                    () -> new IncomingPacket(pacoteData, null, 5000));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para porta 0")
        void portaZero() {
            assertThrows(IllegalArgumentException.class,
                    () -> new IncomingPacket(pacoteData, LOCALHOST, 0));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para porta 65536")
        void porta65536() {
            assertThrows(IllegalArgumentException.class,
                    () -> new IncomingPacket(pacoteData, LOCALHOST, 65536));
        }

        @Test
        @DisplayName("aceita porta 1 (limite inferior válido)")
        void portaMinima() {
            assertDoesNotThrow(() -> new IncomingPacket(pacoteData, LOCALHOST, 1));
        }

        @Test
        @DisplayName("aceita porta 65535 (limite superior válido)")
        void portaMaxima() {
            assertDoesNotThrow(() -> new IncomingPacket(pacoteData, LOCALHOST, 65535));
        }

        @Test
        @DisplayName("toString() contém tipo, seqNum e endereço do remetente")
        void toStringContemCampos() {
            String s = new IncomingPacket(pacoteData, LOCALHOST, 5000).toString();
            assertTrue(s.contains("DATA") || s.contains("tipo="));
            assertTrue(s.contains("127.0.0.1"));
            assertTrue(s.contains("5000"));
        }
    }

    // =========================================================================
    // PacketReceiver — testes com sockets UDP reais em localhost
    // =========================================================================

    @Nested
    @DisplayName("PacketReceiver")
    class PacketReceiverTest {

        @Test
        @DisplayName("recebe e desserializa um pacote DATA corretamente")
        void recebeData() throws IOException {
            try (DatagramSocket receptor = new DatagramSocket(0);
                 DatagramSocket emissor = new DatagramSocket()) {

                receptor.setSoTimeout(2000);
                int porta = receptor.getLocalPort();

                // Envia DATA pelo emissor
                byte[] payload = {10, 20, 30};
                Packet esperado = Packet.createData(42, payload, payload.length);
                byte[] bytes = PacketSerializer.serialize(esperado);
                emissor.send(new DatagramPacket(bytes, bytes.length, LOCALHOST, porta));

                // Recebe via PacketReceiver
                PacketReceiver pr = new PacketReceiver(receptor);
                IncomingPacket entrada = pr.receber();

                assertTrue(entrada.getPacket().isData());
                assertEquals(42, entrada.getPacket().getSeqNum());
                assertEquals(3, entrada.getPacket().getDataLength());
                assertArrayEquals(payload, entrada.getPacket().getData());
            }
        }

        @Test
        @DisplayName("recebe e desserializa um pacote HANDSHAKE corretamente")
        void recebeHandshake() throws IOException {
            try (DatagramSocket receptor = new DatagramSocket(0);
                 DatagramSocket emissor = new DatagramSocket()) {

                receptor.setSoTimeout(2000);
                int porta = receptor.getLocalPort();

                Packet hs = Packet.createHandshake(new byte[]{1, 2});
                byte[] bytes = PacketSerializer.serialize(hs);
                emissor.send(new DatagramPacket(bytes, bytes.length, LOCALHOST, porta));

                PacketReceiver pr = new PacketReceiver(receptor);
                IncomingPacket entrada = pr.receber();

                assertTrue(entrada.getPacket().isHandshake());
                assertEquals(LOCALHOST, entrada.getEnderecoRemetente());
            }
        }

        @Test
        @DisplayName("lança NullPointerException se socket for null")
        void socketNull() {
            assertThrows(NullPointerException.class,
                    () -> new PacketReceiver(null));
        }

        @Test
        @DisplayName("preenche endereço e porta do remetente no IncomingPacket")
        void preencheEnderecoRemetente() throws IOException {
            try (DatagramSocket receptor = new DatagramSocket(0);
                 DatagramSocket emissor = new DatagramSocket()) {

                receptor.setSoTimeout(2000);
                int portaReceptor = receptor.getLocalPort();

                byte[] bytes = PacketSerializer.serialize(Packet.createFin(0));
                emissor.send(new DatagramPacket(bytes, bytes.length, LOCALHOST, portaReceptor));

                IncomingPacket entrada = new PacketReceiver(receptor).receber();

                assertEquals(LOCALHOST, entrada.getEnderecoRemetente());
                assertEquals(emissor.getLocalPort(), entrada.getPortaRemetente());
            }
        }
    }

    // =========================================================================
    // AckSender — testes com sockets UDP reais em localhost
    // =========================================================================

    @Nested
    @DisplayName("AckSender")
    class AckSenderTest {

        @Test
        @DisplayName("envia ACK que pode ser recebido e desserializado no outro lado")
        void enviaAckRecebivel() throws IOException {
            try (DatagramSocket remetente = new DatagramSocket();
                 DatagramSocket destinatario = new DatagramSocket(0)) {

                destinatario.setSoTimeout(2000);
                int portaDestinatario = destinatario.getLocalPort();

                AckSender sender = new AckSender(remetente);
                sender.sendAck(17, LOCALHOST, portaDestinatario);

                // Recebe o datagrama no lado oposto
                byte[] buffer = new byte[Packet.MAX_DATAGRAM_SIZE];
                DatagramPacket datagram = new DatagramPacket(buffer, buffer.length);
                destinatario.receive(datagram);

                Packet ack = PacketSerializer.deserialize(datagram.getData(), datagram.getLength());
                assertTrue(ack.isAck());
                assertEquals(17, ack.getAckNum());
            }
        }

        @Test
        @DisplayName("lança NullPointerException se socket for null")
        void socketNull() {
            assertThrows(NullPointerException.class,
                    () -> new AckSender(null));
        }

        @Test
        @DisplayName("lança NullPointerException se endereçoDestino for null")
        void enderecoNull() throws IOException {
            try (DatagramSocket socket = new DatagramSocket()) {
                AckSender sender = new AckSender(socket);
                assertThrows(NullPointerException.class,
                        () -> sender.sendAck(0, null, 5000));
            }
        }

        @Test
        @DisplayName("lança IllegalArgumentException para ackNum negativo")
        void ackNumNegativo() throws IOException {
            try (DatagramSocket socket = new DatagramSocket()) {
                AckSender sender = new AckSender(socket);
                assertThrows(IllegalArgumentException.class,
                        () -> sender.sendAck(-1, LOCALHOST, 5000));
            }
        }

        @Test
        @DisplayName("lança IllegalArgumentException para porta inválida")
        void portaInvalida() throws IOException {
            try (DatagramSocket socket = new DatagramSocket()) {
                AckSender sender = new AckSender(socket);
                assertThrows(IllegalArgumentException.class,
                        () -> sender.sendAck(0, LOCALHOST, 0));
            }
        }
    }
}
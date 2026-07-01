package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.sender.network.SenderSocketService;

public class AckReceiverTest {

    public static void main(String[] args) throws Exception {

        SenderSocketService socket = new SenderSocketService();

        socket.open(5000);

        System.out.println("Aguardando ACK na porta 5000...");

        Packet ack =
                AckReceiver.receiveAck(
                        socket,
                        Packet.MAX_DATAGRAM_SIZE
                );

        System.out.println("ACK recebido!");
        System.out.println("AckNum = " + ack.getAckNum());

        socket.close();
    }
}
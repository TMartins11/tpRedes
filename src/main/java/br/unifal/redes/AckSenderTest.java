package br.unifal.redes;

import br.unifal.redes.receiver.network.AckSender;

import java.net.DatagramSocket;
import java.net.InetAddress;

public class AckSenderTest {

    public static void main(String[] args) throws Exception {

        DatagramSocket socket = new DatagramSocket();

        AckSender sender =
                new AckSender(socket);

        System.out.println("Enviando ACK 7");

        sender.sendAck(
                7,
                InetAddress.getByName("127.0.0.1"),
                5000
        );

        System.out.println("ACK enviado");

        socket.close();
    }
}
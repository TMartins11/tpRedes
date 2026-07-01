package br.unifal.redes.receiver;

import br.unifal.redes.sender.network.SenderSocketService;

import java.net.DatagramPacket;

public class ReceiverTest {

    public static void main(String[] args) throws Exception {

        System.out.println("Abrindo socket 5000");

        SenderSocketService socket =
                new SenderSocketService();

        socket.open(5000);

        System.out.println("Esperando pacote...");

        DatagramPacket packet =
                socket.receive(1024);

        System.out.println("Pacote recebido!");

        String msg =
                new String(packet.getData(), 0, packet.getLength());

        System.out.println(msg);

        socket.close();
    }
}
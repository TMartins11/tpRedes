package br.unifal.redes;

import br.unifal.redes.sender.network.SenderSocketService;

import java.net.InetAddress;

public class SenderTest {

    public static void main(String[] args) throws Exception {

        System.out.println("Abrindo socket");

        SenderSocketService socket =
                new SenderSocketService();

        socket.open();

        System.out.println("Enviando...");

        socket.send(
                "Teste UDP".getBytes(),
                InetAddress.getByName("127.0.0.1"),
                5000
        );

        System.out.println("Enviado!");

        socket.close();
    }
}
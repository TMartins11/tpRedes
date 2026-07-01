package br.unifal.redes;

import br.unifal.redes.common.SessionParameters;
import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.sender.protocol.HandshakeSender;

import java.net.InetAddress;

public class HandshakeSenderTest {

    public static void main(String[] args) throws Exception {

        SenderSocketService socket =
                new SenderSocketService();

        socket.open();

        SessionParameters params =
                new SessionParameters(
                        1000,
                        4,
                        0.1,
                        "arquivo.txt"
                );

        HandshakeSender.sendHandshake(
                socket,
                params,
                InetAddress.getByName("127.0.0.1"),
                5000
        );

        socket.close();
    }
}
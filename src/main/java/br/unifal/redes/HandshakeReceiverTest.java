package br.unifal.redes;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;

import java.net.DatagramPacket;
import java.net.DatagramSocket;

public class HandshakeReceiverTest {

    public static void main(String[] args) throws Exception {

        DatagramSocket socket =
                new DatagramSocket(5000);

        byte[] buffer = new byte[2048];

        DatagramPacket dp =
                new DatagramPacket(buffer, buffer.length);

        socket.receive(dp);

        Packet packet =
                PacketCodec.decode(
                        dp.getData(),
                        dp.getLength()
                );

        System.out.println(packet);

        socket.close();
    }
}
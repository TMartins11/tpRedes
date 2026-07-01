package br.unifal.redes;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;

public class TestPacketCodec {

    public static void main(String[] args) {

        Packet original =
                Packet.createData(1, "Hello".getBytes(), 5);

        byte[] bytes =
                PacketCodec.encode(original);

        Packet decoded =
                PacketCodec.decode(bytes);

        System.out.println(original);
        System.out.println(decoded);
    }
}
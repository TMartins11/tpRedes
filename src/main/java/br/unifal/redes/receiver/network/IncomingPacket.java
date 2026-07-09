package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;

import java.net.InetAddress;
import java.util.Objects;

public final class IncomingPacket {

    private final Packet packet;
    private final InetAddress enderecoRemetente;
    private final int portaRemetente;

    public IncomingPacket(Packet packet, InetAddress enderecoRemetente, int portaRemetente) {
        this.packet = Objects.requireNonNull(packet, "packet não pode ser null");
        this.enderecoRemetente = Objects.requireNonNull(enderecoRemetente, "enderecoRemetente não pode ser null");
        if (portaRemetente < 1 || portaRemetente > 65535) {
            throw new IllegalArgumentException(
                    "portaRemetente deve estar em [1, 65535], recebido: " + portaRemetente);
        }
        this.portaRemetente = portaRemetente;
    }

    /** @return o {@link Packet} extraído do datagrama UDP */
    public Packet getPacket() {
        return packet;
    }

    /** @return o {@link InetAddress} de origem do datagrama */
    public InetAddress getEnderecoRemetente() {
        return enderecoRemetente;
    }

    /** @return a porta UDP de origem do remetente */
    public int getPortaRemetente() {
        return portaRemetente;
    }

    @Override
    public String toString() {
        return "IncomingPacket{"
                + "tipo=" + packet.getType()
                + ", seqNum=" + packet.getSeqNum()
                + ", remetente=" + enderecoRemetente.getHostAddress() + ":" + portaRemetente
                + '}';
    }
}
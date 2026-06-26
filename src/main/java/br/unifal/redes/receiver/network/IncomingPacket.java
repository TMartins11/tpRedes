package br.unifal.redes.receiver.network;

import br.unifal.redes.common.Packet;

import java.net.InetAddress;
import java.util.Objects;

/**
 * Objeto de valor imutável produzido pelo {@link PacketReceiver} para cada
 * datagrama desserializado com sucesso.
 *
 * <p>Agrupa o {@link Packet} decodificado com as informações de endereçamento
 * do remetente, permitindo que a FSM envie ACKs ao destino correto sem precisar
 * inspecionar o datagrama bruto novamente.
 *
 * <p>Esta classe não possui comportamento próprio — é um portador de dados puro
 * entre a camada de rede ({@link PacketReceiver}) e a FSM.
 */
public final class IncomingPacket {

    private final Packet packet;
    private final InetAddress enderecoRemetente;
    private final int portaRemetente;

    /**
     * @param packet             o pacote decodificado; não pode ser {@code null}
     * @param enderecoRemetente  endereço IP do remetente; não pode ser {@code null}
     * @param portaRemetente     porta UDP do remetente; deve estar em [1, 65535]
     * @throws NullPointerException     se {@code packet} ou {@code enderecoRemetente} for {@code null}
     * @throws IllegalArgumentException se {@code portaRemetente} estiver fora do intervalo válido
     */
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
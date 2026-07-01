package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.sender.network.SenderSocketService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.util.Objects;

/**
 * Responsável por receber e decodificar um único pacote ACK do Receptor no
 * protocolo Go-Back-N.
 *
 * <p>Esta classe tem uma única responsabilidade — o fluxo:
 * <pre>
 *   SenderSocketService.receive(...)
 *               ↓
 *            byte[]
 *               ↓
 *      PacketCodec.decode(...)
 *               ↓
 *         Packet ACK
 *               ↓
 *           retorno
 * </pre>
 *
 * <p>Em outras palavras, esta classe é apenas uma <strong>ponte</strong>
 * entre o socket UDP e o {@link Packet} decodificado: recebe os bytes brutos
 * de um datagrama através de {@link SenderSocketService}, delega toda a
 * desserialização a {@link PacketCodec} (sem duplicar nenhuma lógica de
 * formato binário), confirma que o {@link Packet} resultante é de fato um
 * ACK, e o devolve ao chamador.
 *
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não implementa a FSM do Emissor.</li>
 *   <li>Não implementa lógica de janela deslizante e não conhece
 *       {@code WindowManager}.</li>
 *   <li>Não processa o ACK cumulativamente — não decide quais sequências
 *       foram confirmadas, nem avança nenhuma janela. Apenas devolve o
 *       {@link Packet} para que outra camada faça isso.</li>
 *   <li>Não conhece {@code TimeoutManager} — não reinicia, cancela ou
 *       consulta nenhum temporizador de retransmissão.</li>
 *   <li>Não conhece {@code PacketBuffer} — não remove nenhum pacote
 *       pendente de confirmação.</li>
 *   <li>Não conhece {@code SenderSession} — não atualiza nenhum estado de
 *       sessão.</li>
 *   <li>Não atualiza nenhuma estatística (essa responsabilidade pertence a
 *       {@code SenderStatistics}, que deve ser atualizada pelo chamador,
 *       não por esta classe).</li>
 *   <li>Não realiza retransmissões.</li>
 *   <li>Não cria threads e não executa nenhum laço de repetição — cada
 *       chamada a {@link #receiveAck} bloqueia para receber exatamente um
 *       datagrama e retorna.</li>
 *   <li>Não armazena estado entre chamadas: não guarda referência a nenhum
 *       dos parâmetros recebidos. Cada chamada a {@link #receiveAck} é
 *       completamente independente, seguindo o mesmo padrão arquitetural já
 *       estabelecido em {@code HandshakeSender} e {@code DataSender}.</li>
 * </ul>
 *
 * <p>Como é apenas uma coleção de métodos estáticos, esta classe não pode
 * ser instanciada — assim como {@link PacketCodec}, {@code HandshakeSender}
 * e {@code DataSender}.
 */
public final class AckReceiver {

    /** Construtor privado — classe utilitária, não instanciável. */
    private AckReceiver() {
        throw new AssertionError("AckReceiver não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    /**
     * Bloqueia até receber um datagrama através de {@code socketService},
     * decodifica-o com {@link PacketCodec}, e retorna o {@link Packet}
     * resultante, desde que ele seja de fato um pacote do tipo ACK.
     *
     * <p>Esta operação é síncrona e bloqueante (herdando esse comportamento
     * de {@link SenderSocketService#receive(int)}), recebe exatamente um
     * datagrama por chamada, e não realiza nenhuma nova tentativa em caso de
     * timeout ou erro de E/S — decidir o que fazer com um timeout (por
     * exemplo, disparar uma retransmissão) é responsabilidade de outras
     * camadas do Emissor (a FSM, em conjunto com {@code TimeoutManager} e
     * {@code PacketBuffer}), fora do escopo desta classe.
     *
     * <p>O {@code maxDatagramSize} determina o tamanho do buffer interno
     * alocado para receber o datagrama; em geral, o chamador deve usar
     * {@link Packet#MAX_DATAGRAM_SIZE}, que já contempla o maior datagrama
     * possível segundo o protocolo. Um valor menor que o datagrama
     * efetivamente enviado pelo Receptor resultaria em truncamento pelo
     * próprio {@link java.net.DatagramSocket}, não detectável por esta
     * classe — por isso recomenda-se sempre usar a constante mencionada.
     *
     * @param socketService    o serviço de socket UDP, já aberto, do qual o
     *                         ACK será recebido; não pode ser {@code null}
     * @param maxDatagramSize  o tamanho, em bytes, do buffer de recepção a
     *                         ser alocado; deve ser {@code > 0}
     * @return o {@link Packet} do tipo ACK recebido e decodificado
     * @throws NullPointerException     se {@code socketService} for {@code null}
     * @throws IllegalArgumentException se {@code maxDatagramSize} não for
     *                                   positivo, ou se o datagrama recebido
     *                                   não puder ser decodificado por
     *                                   {@link PacketCodec} (datagrama
     *                                   corrompido ou inválido — ver
     *                                   {@link PacketCodec#decode(byte[], int)})
     * @throws IllegalStateException    se {@code socketService} não estiver
     *                                   aberto, ou se o pacote recebido e
     *                                   decodificado não for do tipo ACK
     * @throws IOException              se ocorrer um erro de E/S durante o
     *                                   recebimento, incluindo
     *                                   {@link java.net.SocketTimeoutException}
     *                                   caso um timeout de socket tenha sido
     *                                   configurado e expire
     */
    public static Packet receiveAck(SenderSocketService socketService,
                                    int maxDatagramSize) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        validateMaxDatagramSize(maxDatagramSize);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de receber um ACK; chame open() primeiro"
            );
        }

        // 1. Recebe um único datagrama UDP, bloqueando até que ele chegue
        //    (ou até o timeout do socket, se configurado).
        DatagramPacket received = socketService.receive(maxDatagramSize);

        // 2. Decodifica os bytes recebidos de volta em um Packet, delegando
        //    toda a lógica de formato binário a PacketCodec — nenhuma
        //    lógica de desserialização é duplicada aqui.
        Packet packet = PacketCodec.decode(received.getData(), received.getLength());

        // 3. Garante que o pacote recebido é realmente um ACK. Qualquer
        //    outro tipo (DATA, HANDSHAKE, FIN) indica que o Receptor enviou
        //    algo inesperado neste ponto do protocolo, e isso é tratado
        //    como um erro de estado, não como um caso a ser silenciosamente
        //    ignorado ou reinterpretado.
        if (!packet.isAck()) {
            throw new IllegalStateException(
                    "Pacote recebido não é um ACK: " + packet
            );
        }

        return packet;
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Valida que {@code maxDatagramSize} é um tamanho de buffer válido,
     * falhando rapidamente antes de qualquer chamada bloqueante ao socket.
     *
     * @param maxDatagramSize o tamanho a ser validado
     * @throws IllegalArgumentException se {@code maxDatagramSize} não for positivo
     */
    private static void validateMaxDatagramSize(int maxDatagramSize) {
        if (maxDatagramSize <= 0) {
            throw new IllegalArgumentException(
                    "maxDatagramSize must be > 0, got: " + maxDatagramSize
            );
        }
    }
}
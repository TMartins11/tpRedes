package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.sender.network.PacketBuffer;
import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.sender.network.TimeoutManager;
import br.unifal.redes.sender.statistics.SenderStatistics;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Responsável por executar uma rajada de retransmissão Go-Back-N quando o
 * temporizador de retransmissão expira.
 *
 * <p>Esta classe tem uma única responsabilidade — o fluxo:
 * <pre>
 *   PacketBuffer.getOutstandingPackets()
 *               ↓
 *   para cada pacote, em ordem crescente de sequência:
 *               ↓
 *         PacketCodec.encode(...)
 *               ↓
 *   SenderSocketService.send(...)
 *               ↓
 *   SenderStatistics.recordPacketSent() / recordRetransmission() / recordBytesSent(...)
 *               ↓
 *   (ao final, para todos os pacotes)  TimeoutManager.restart()
 * </pre>
 *
 * <p>Esta classe representa exatamente o comportamento clássico do
 * Go-Back-N na ocorrência de um timeout: <strong>todos</strong> os pacotes
 * ainda não confirmados são reenviados, na mesma ordem de seus números de
 * sequência — nunca em ordem arbitrária — e o temporizador é reiniciado uma
 * única vez, após a rajada completa, não uma vez por pacote.
 *
 * <p>Importante: esta classe não decide <em>quando</em> uma retransmissão é
 * necessária. Ela não consulta {@link TimeoutManager#hasExpired()} nem
 * qualquer outra condição — apenas executa a rajada de reenvio quando
 * chamada, confiando que o chamador (a FSM do Emissor) já determinou que o
 * timeout expirou e que a retransmissão é apropriada neste momento.
 *
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não implementa a FSM do Emissor — não decide quando retransmitir,
 *       apenas executa a retransmissão quando instruída.</li>
 *   <li>Não realiza leitura de arquivos.</li>
 *   <li>Não recebe ACKs nem processa qualquer pacote recebido — esta classe
 *       é de mão única (somente reenvio).</li>
 *   <li>Não controla janela deslizante e não conhece {@code WindowManager}
 *       — apenas itera sobre o que {@link PacketBuffer} já expõe como
 *       pendente, sem decidir o que entra ou sai da janela.</li>
 *   <li>Não decide quando enviar pacotes <strong>novos</strong> — trata
 *       exclusivamente de pacotes já enviados anteriormente e ainda não
 *       confirmados.</li>
 *   <li>Não cria threads e não executa nenhum laço de repetição infinito —
 *       o único laço existente é finito, percorrendo exatamente os pacotes
 *       pendentes no momento da chamada, uma única vez.</li>
 *   <li>Não armazena estado entre chamadas: não guarda referência a nenhum
 *       dos parâmetros recebidos. Cada chamada a
 *       {@link #retransmitOutstandingPackets} é completamente independente,
 *       seguindo o mesmo padrão arquitetural já estabelecido em
 *       {@code HandshakeSender}, {@code DataSender} e {@code AckReceiver}.</li>
 * </ul>
 *
 * <p>Como é apenas uma coleção de métodos estáticos, esta classe não pode
 * ser instanciada — assim como {@link PacketCodec}, {@code HandshakeSender},
 * {@code DataSender} e {@code AckReceiver}.
 */
public final class RetransmissionManager {

    /** Construtor privado — classe utilitária, não instanciável. */
    private RetransmissionManager() {
        throw new AssertionError("RetransmissionManager não deve ser instanciada");
    }

    // -------------------------------------------------------------------------
    // Operação principal
    // -------------------------------------------------------------------------

    /**
     * Reenvia, em ordem crescente de número de sequência, todos os pacotes
     * atualmente pendentes em {@code packetBuffer}, atualiza
     * {@code statistics} para cada pacote reenviado, e reinicia
     * {@code timeoutManager} uma única vez ao final da rajada.
     *
     * <p>Se {@code packetBuffer} estiver vazio, este método não lança
     * exceção e não reinicia o temporizador — apenas retorna imediatamente,
     * já que não há nada a retransmitir e reiniciar o timer nessa situação
     * não corresponde a nenhuma retransmissão de fato ocorrida.
     *
     * <p>A ordem de reenvio é a mesma já garantida por
     * {@link PacketBuffer#getOutstandingPackets()} (ascendente por número de
     * sequência); esta classe não reordena nada por conta própria, apenas
     * confia nessa garantia já documentada.
     *
     * <p>Esta operação é síncrona e bloqueante (herdando esse comportamento
     * de {@link SenderSocketService#send}) e não realiza nenhuma nova
     * tentativa em caso de falha de envio de um pacote individual — uma
     * falha de E/S interrompe a rajada e é propagada ao chamador.
     *
     * @param socketService   o serviço de socket UDP, já aberto, a ser usado
     *                        para o reenvio; não pode ser {@code null}
     * @param packetBuffer    o buffer de pacotes pendentes de confirmação,
     *                        de onde os pacotes a retransmitir serão lidos;
     *                        não pode ser {@code null}
     * @param timeoutManager  o temporizador de retransmissão a ser
     *                        reiniciado ao final da rajada; não pode ser {@code null}
     * @param statistics      o coletor de estatísticas a ser atualizado para
     *                        cada pacote reenviado; não pode ser {@code null}
     * @param receiverAddress o endereço IP do Receptor; não pode ser {@code null}
     * @param receiverPort    a porta UDP do Receptor; deve estar no intervalo {@code [1, 65535]}
     * @throws NullPointerException     se {@code socketService}, {@code packetBuffer},
     *                                   {@code timeoutManager}, {@code statistics}
     *                                   ou {@code receiverAddress} forem {@code null}
     * @throws IllegalArgumentException se {@code receiverPort} estiver fora do
     *                                   intervalo válido
     * @throws IllegalStateException    se {@code socketService} não estiver aberto
     * @throws IOException              se ocorrer um erro de E/S durante o
     *                                   reenvio de algum pacote
     */
    public static void retransmitOutstandingPackets(SenderSocketService socketService,
                                                    PacketBuffer<Packet> packetBuffer,
                                                    TimeoutManager timeoutManager,
                                                    SenderStatistics statistics,
                                                    InetAddress receiverAddress,
                                                    int receiverPort) throws IOException {
        Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        Objects.requireNonNull(packetBuffer, "packetBuffer não pode ser nulo");
        Objects.requireNonNull(timeoutManager, "timeoutManager não pode ser nulo");
        Objects.requireNonNull(statistics, "statistics não pode ser nulo");
        Objects.requireNonNull(receiverAddress, "receiverAddress não pode ser nulo");
        validateReceiverPort(receiverPort);

        if (!socketService.isOpen()) {
            throw new IllegalStateException(
                    "socketService deve estar aberto antes de retransmitir pacotes; chame open() primeiro"
            );
        }

        // Comportamento especial para buffer vazio: nenhuma exceção, apenas
        // retorno imediato. Não há pacotes a reenviar, e reiniciar o timer
        // aqui não corresponderia a nenhuma retransmissão real.
        if (packetBuffer.isEmpty()) {
            return;
        }

        // 1. Consulta o PacketBuffer para obter todos os pacotes pendentes,
        //    já em ordem crescente de número de sequência — esta classe não
        //    reordena nada por conta própria.
        List<Map.Entry<Integer, Packet>> outstandingPackets = packetBuffer.getOutstandingPackets();

        // 2. Reenvia cada pacote pendente, um a um, na ordem recebida.
        for (Map.Entry<Integer, Packet> entry : outstandingPackets) {
            Packet packet = entry.getValue();

            // 2a. Serializa o pacote inteiro (cabeçalho + payload) para bytes
            //     de rede, reaproveitando a infraestrutura existente de
            //     PacketCodec — nenhuma lógica de serialização é duplicada aqui.
            byte[] datagramBytes = PacketCodec.encode(packet);

            // 2b. Reenvia o datagrama pelo socket já aberto.
            socketService.send(datagramBytes, receiverAddress, receiverPort);

            // 2c. Atualiza as estatísticas: um pacote enviado e, especificamente,
            //     uma retransmissão, além dos bytes efetivamente colocados na rede.
            statistics.recordPacketSent();
            statistics.recordRetransmission();
            statistics.recordBytesSent(packet.getDataLength());
        }

        // 3. Reinicia o temporizador de retransmissão uma única vez, após o
        //    término da rajada completa — não uma vez por pacote reenviado.
        timeoutManager.restart();
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Valida que {@code port} está no intervalo de portas UDP de destino
     * válidas, falhando rapidamente antes de consultar o buffer ou enviar
     * qualquer pacote.
     *
     * @param port a porta a ser validada
     * @throws IllegalArgumentException se {@code port} estiver fora de {@code [1, 65535]}
     */
    private static void validateReceiverPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("receiverPort must be in [1, 65535], got: " + port);
        }
    }
}
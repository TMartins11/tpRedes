package br.unifal.redes.sender.session;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.SessionParameters;
import br.unifal.redes.sender.network.PacketBuffer;
import br.unifal.redes.sender.network.TimeoutManager;
import br.unifal.redes.sender.network.WindowManager;
import br.unifal.redes.sender.statistics.SenderStatistics;

import java.net.InetAddress;
import java.util.List;
import java.util.Objects;

/**
 * Representa o estado completo de uma única transmissão realizada pelo Emissor
 * no protocolo Go-Back-N.
 *
 * <p>Esta classe é um <strong>contêiner de estado</strong>. Ela agrega e
 * disponibiliza todos os objetos compartilhados entre os componentes da camada
 * de protocolo do Emissor ({@code HandshakeSender}, {@code DataSender},
 * {@code RetransmissionManager}, etc.), de modo que a FSM do Emissor possa
 * receber uma única instância de {@code SenderSession} e, a partir dela,
 * acessar tudo o que precisa para conduzir a transmissão.
 *
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não implementa a FSM do Emissor nem qualquer lógica de controle de fluxo.</li>
 *   <li>Não envia, recebe nem retransmite pacotes.</li>
 *   <li>Não lê arquivos do disco.</li>
 *   <li>Não cria threads nem realiza laços de repetição.</li>
 *   <li>Não implementa lógica de rede de nenhum tipo.</li>
 * </ul>
 *
 * <p>Imutabilidade: todos os campos são {@code final}. A lista de chunks é
 * copiada defensivamente no construtor via {@link List#copyOf(java.util.Collection)},
 * tornando-a imutável e independente do array original fornecido pelo chamador.
 * Os demais campos são referências a objetos mutáveis (p.ex. {@link WindowManager}),
 * porém a própria referência armazenada não muda — a mutabilidade interna desses
 * objetos é intencional e gerenciada exclusivamente pela FSM.
 *
 * <p>Thread-safety: esta classe não é sincronizada. A segurança de acesso
 * concorrente aos objetos internos (quando aplicável) é responsabilidade dos
 * próprios componentes ou da FSM que os utiliza.
 */
public final class SenderSession {

    /** Parâmetros negociados no handshake: tamanho do arquivo, janela, prob. de perda e destino. */
    private final SessionParameters sessionParameters;

    /** Gerenciador da janela deslizante Go-Back-N: controla base, next e in-flight. */
    private final WindowManager windowManager;

    /** Gerenciador do temporizador de retransmissão: detecta expiração sem criar threads. */
    private final TimeoutManager timeoutManager;

    /** Buffer de pacotes enviados e ainda não confirmados, indexados por número de sequência. */
    private final PacketBuffer<Packet> packetBuffer;

    /** Coletor de estatísticas da sessão: pacotes enviados, retransmissões, ACKs, timeouts. */
    private final SenderStatistics statistics;

    /** Endereço IP do Receptor para o qual os datagramas devem ser enviados. */
    private final InetAddress receiverAddress;

    /** Porta UDP do Receptor; intervalo válido: {@code [1, 65535]}. */
    private final int receiverPort;

    /**
     * Chunks do arquivo fonte produzidos por {@code FileChunkReader} e que
     * serão transmitidos pela FSM na ordem em que aparecem nesta lista.
     * A lista é imutável e independente do argumento original do construtor.
     */
    private final List<byte[]> fileChunks;

    // -------------------------------------------------------------------------
    // Construtor
    // -------------------------------------------------------------------------

    /**
     * Cria uma nova sessão de transmissão completamente validada.
     *
     * <p>Todos os parâmetros são obrigatórios. Nenhum campo pode ser
     * {@code null}. A porta do Receptor deve estar no intervalo {@code [1, 65535]}.
     * A lista de chunks é copiada defensivamente para garantir imutabilidade.
     *
     * @param sessionParameters parâmetros da sessão negociados no handshake;
     *                          não pode ser {@code null}
     * @param windowManager     gerenciador da janela deslizante; não pode ser {@code null}
     * @param timeoutManager    gerenciador do temporizador de retransmissão;
     *                          não pode ser {@code null}
     * @param packetBuffer      buffer de pacotes pendentes de confirmação;
     *                          não pode ser {@code null}
     * @param statistics        coletor de estatísticas da sessão; não pode ser {@code null}
     * @param receiverAddress   endereço IP do Receptor; não pode ser {@code null}
     * @param receiverPort      porta UDP do Receptor; deve estar em {@code [1, 65535]}
     * @param fileChunks        chunks do arquivo a transmitir, na ordem de envio;
     *                          não pode ser {@code null}
     * @throws NullPointerException     se qualquer parâmetro de referência for {@code null}
     * @throws IllegalArgumentException se {@code receiverPort} estiver fora de {@code [1, 65535]}
     */
    public SenderSession(SessionParameters sessionParameters,
                         WindowManager windowManager,
                         TimeoutManager timeoutManager,
                         PacketBuffer<Packet> packetBuffer,
                         SenderStatistics statistics,
                         InetAddress receiverAddress,
                         int receiverPort,
                         List<byte[]> fileChunks) {

        // Validação de todos os parâmetros de referência
        Objects.requireNonNull(sessionParameters, "sessionParameters não pode ser nulo");
        Objects.requireNonNull(windowManager,     "windowManager não pode ser nulo");
        Objects.requireNonNull(timeoutManager,    "timeoutManager não pode ser nulo");
        Objects.requireNonNull(packetBuffer,      "packetBuffer não pode ser nulo");
        Objects.requireNonNull(statistics,        "statistics não pode ser nulo");
        Objects.requireNonNull(receiverAddress,   "receiverAddress não pode ser nulo");
        Objects.requireNonNull(fileChunks,        "fileChunks não pode ser nulo");

        // Validação da porta UDP do Receptor
        if (receiverPort < 1 || receiverPort > 65535) {
            throw new IllegalArgumentException(
                    "receiverPort deve estar em [1, 65535], recebido: " + receiverPort
            );
        }

        this.sessionParameters = sessionParameters;
        this.windowManager     = windowManager;
        this.timeoutManager    = timeoutManager;
        this.packetBuffer      = packetBuffer;
        this.statistics        = statistics;
        this.receiverAddress   = receiverAddress;
        this.receiverPort      = receiverPort;

        // Cópia defensiva: garante que a lista seja imutável e independente
        // do argumento original, mesmo que o chamador modifique a lista
        // fornecida após a criação da sessão.
        this.fileChunks = List.copyOf(fileChunks);
    }

    // -------------------------------------------------------------------------
    // Getters
    // -------------------------------------------------------------------------

    /**
     * Retorna os parâmetros da sessão negociados no handshake.
     *
     * @return os {@link SessionParameters} desta sessão; nunca {@code null}
     */
    public SessionParameters getSessionParameters() {
        return sessionParameters;
    }

    /**
     * Retorna o gerenciador da janela deslizante Go-Back-N.
     *
     * <p>A FSM utiliza este objeto para decidir quando pode enviar novos
     * pacotes, avançar a janela ao receber ACKs cumulativos e consultar
     * o estado atual de {@code base} e {@code next}.
     *
     * @return o {@link WindowManager} desta sessão; nunca {@code null}
     */
    public WindowManager getWindowManager() {
        return windowManager;
    }

    /**
     * Retorna o gerenciador do temporizador de retransmissão.
     *
     * <p>A FSM utiliza este objeto para armar, reiniciar e cancelar o
     * temporizador, e para consultar se ele expirou — sem criar threads.
     *
     * @return o {@link TimeoutManager} desta sessão; nunca {@code null}
     */
    public TimeoutManager getTimeoutManager() {
        return timeoutManager;
    }

    /**
     * Retorna o buffer de pacotes enviados ainda não confirmados.
     *
     * <p>A FSM adiciona pacotes a este buffer ao enviá-los e remove ao
     * receber ACKs cumulativos. O {@code RetransmissionManager} o consulta
     * para obter os pacotes a reenviar na rajada de retransmissão.
     *
     * @return o {@link PacketBuffer} desta sessão; nunca {@code null}
     */
    public PacketBuffer<Packet> getPacketBuffer() {
        return packetBuffer;
    }

    /**
     * Retorna o coletor de estatísticas da sessão.
     *
     * <p>A FSM e os componentes de protocolo registram eventos (pacotes
     * enviados, retransmissões, ACKs, timeouts) neste objeto ao longo da
     * transmissão.
     *
     * @return as {@link SenderStatistics} desta sessão; nunca {@code null}
     */
    public SenderStatistics getStatistics() {
        return statistics;
    }

    /**
     * Retorna o endereço IP do Receptor para o qual os datagramas devem
     * ser enviados.
     *
     * @return o {@link InetAddress} do Receptor; nunca {@code null}
     */
    public InetAddress getReceiverAddress() {
        return receiverAddress;
    }

    /**
     * Retorna a porta UDP do Receptor.
     *
     * @return a porta do Receptor, no intervalo {@code [1, 65535]}
     */
    public int getReceiverPort() {
        return receiverPort;
    }

    /**
     * Retorna a lista imutável de chunks do arquivo a transmitir.
     *
     * <p>Os chunks estão na mesma ordem em que foram produzidos pelo
     * {@code FileChunkReader}, que corresponde à ordem de transmissão
     * esperada pela FSM. A lista não pode ser modificada.
     *
     * @return lista imutável de chunks; nunca {@code null}
     */
    public List<byte[]> getFileChunks() {
        return fileChunks;
    }

    // -------------------------------------------------------------------------
    // Métodos utilitários
    // -------------------------------------------------------------------------

    /**
     * Retorna o número total de chunks do arquivo a transmitir.
     *
     * <p>Equivale a {@code getFileChunks().size()} e corresponde ao número
     * de pacotes DATA que a FSM precisará enviar para transmitir o arquivo
     * completo (excluindo handshake e FIN).
     *
     * @return o número de chunks; {@code 0} se o arquivo estiver vazio
     */
    public int getTotalChunks() {
        return fileChunks.size();
    }

    // -------------------------------------------------------------------------
    // Object
    // -------------------------------------------------------------------------

    /**
     * Retorna uma representação textual da sessão, útil para depuração.
     *
     * <p>Não imprime o conteúdo dos chunks para evitar poluição de log.
     * Exibe apenas os metadados relevantes da transmissão.
     *
     * @return string com quantidade de chunks, endereço e porta do Receptor
     *         e tamanho da janela
     */
    @Override
    public String toString() {
        return "SenderSession{"
                + "totalChunks=" + getTotalChunks()
                + ", receiverAddress=" + receiverAddress.getHostAddress()
                + ", receiverPort=" + receiverPort
                + ", windowSize=" + sessionParameters.getWindowSize()
                + '}';
    }
}
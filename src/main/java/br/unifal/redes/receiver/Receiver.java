package br.unifal.redes.receiver;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;
import br.unifal.redes.common.SessionParameters;
import br.unifal.redes.receiver.io.FileWriterService;
import br.unifal.redes.receiver.network.AckSender;
import br.unifal.redes.receiver.network.IncomingPacket;
import br.unifal.redes.receiver.network.PacketReceiver;
import br.unifal.redes.receiver.session.ReceiverSession;
import br.unifal.redes.receiver.statistics.ReceiverStatistics;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.Random;

/**
 * Ponto de entrada do módulo receptor do protocolo Go-Back-N.
 *
 * <p>Esta classe orquestra os componentes do receptor ao longo do ciclo de
 * vida de uma transferência. Ela não implementa lógica de protocolo —
 * delega cada responsabilidade à classe especializada correspondente:
 *
 * <ul>
 *   <li>{@link PacketReceiver} — recepção de datagramas UDP;</li>
 *   <li>{@link AckSender}     — envio de confirmações;</li>
 *   <li>{@link ReceiverSession}    — estado do protocolo GBN;</li>
 *   <li>{@link FileWriterService}  — escrita do arquivo em disco;</li>
 *   <li>{@link ReceiverStatistics} — contabilidade de eventos.</li>
 * </ul>
 *
 * <p><strong>Ciclo de vida esperado (completo, nas próximas etapas):</strong>
 * <ol>
 *   <li>{@link #iniciar()} — abre o socket e aguarda o HANDSHAKE.</li>
 *   <li>Loop FSM (etapa futura) — processa pacotes DATA e envia ACKs.</li>
 *   <li>{@link #encerrar()} — fecha arquivo, socket e exibe estatísticas.</li>
 * </ol>
 *
 * <p>{@link #executar()} contém o loop principal que despacha pacotes por tipo.
 *
 * <p><strong>Thread-safety:</strong> esta classe não é thread-safe. Espera-se
 * que seja usada a partir de uma única thread principal.
 */
public final class Receiver {

    /** Porta UDP padrão em que o receptor aguarda conexões. */
    public static final int PORTA_PADRAO = 5000;

    /**
     * Gerador de números aleatórios utilizado pela simulação de perda.
     * Instância única por {@code Receiver} — evita overhead de criação
     * repetida e garante distribuição uniforme ao longo da sessão.
     */
    private static final Random RANDOM = new Random();

    private final int porta;

    // Componentes inicializados em iniciar()
    private DatagramSocket socket;
    private PacketReceiver packetReceiver;
    private AckSender ackSender;
    private ReceiverSession sessao;
    private FileWriterService fileWriter;
    private ReceiverStatistics estatisticas;

    // Endereço/porta do emissor, extraídos do primeiro datagrama recebido
    // (necessários para enviar ACKs na etapa seguinte)
    private java.net.InetAddress enderecoEmissor;
    private int portaEmissor;

    // -------------------------------------------------------------------------
    // Construção
    // -------------------------------------------------------------------------

    /**
     * Cria um receptor que ouvirá na {@link #PORTA_PADRAO}.
     */
    public Receiver() {
        this(PORTA_PADRAO);
    }

    /**
     * Cria um receptor que ouvirá na porta informada.
     *
     * @param porta porta UDP local; deve estar em [1, 65535]
     * @throws IllegalArgumentException se a porta estiver fora do intervalo válido
     */
    public Receiver(int porta) {
        if (porta < 1 || porta > 65535) {
            throw new IllegalArgumentException(
                    "Porta inválida: " + porta + ". Deve estar em [1, 65535].");
        }
        this.porta = porta;
    }

    // -------------------------------------------------------------------------
    // Inicialização da sessão
    // -------------------------------------------------------------------------

    /**
     * Abre o socket UDP, instancia os componentes de rede e aguarda o pacote
     * de HANDSHAKE do emissor.
     *
     * <p>Ao retornar com sucesso, todos os componentes internos estão prontos:
     * {@link ReceiverSession}, {@link FileWriterService} (arquivo aberto),
     * {@link ReceiverStatistics}, {@link PacketReceiver} e {@link AckSender}.
     *
     * <p>Se o primeiro pacote recebido não for um HANDSHAKE válido, o socket
     * é fechado e uma {@link IOException} é lançada — o receptor não entra
     * em estado inconsistente.
     *
     * @throws IOException se ocorrer falha ao abrir o socket, ao receber o
     *                     datagrama inicial, ao desserializar os parâmetros da
     *                     sessão ou ao abrir o arquivo de destino
     */
    public void iniciar() throws IOException {
        abrirSocket();
        instanciarComponentesDeRede();
        processarHandshake();
    }

    // -------------------------------------------------------------------------
    // Loop principal
    // -------------------------------------------------------------------------

    /**
     * Executa o loop principal de recepção, bloqueando até que a sessão seja
     * encerrada por um pacote FIN ou por falha de rede.
     *
     * <p>Deve ser chamado após {@link #iniciar()}. A cada iteração:
     * <ol>
     *   <li>Aguarda o próximo datagrama via {@link PacketReceiver#receber()};</li>
     *   <li>Despacha o pacote para o handler correspondente ao seu tipo.</li>
     * </ol>
     *
     * <p>O loop termina naturalmente quando {@link ReceiverSession#isReceiving()}
     * retorna {@code false} — o que ocorre após {@link #tratarFin()} fechar a sessão.
     *
     * @throws IOException se ocorrer falha irrecuperável de rede durante a recepção
     * @throws IllegalStateException se chamado antes de {@link #iniciar()}
     */
    public void executar() throws IOException {
        if (sessao == null) {
            throw new IllegalStateException(
                    "executar() chamado antes de iniciar(). Chame iniciar() primeiro.");
        }

        System.out.println("[Receiver] Loop principal iniciado. Aguardando pacotes DATA...");

        while (sessao.isReceiving()) {
            IncomingPacket entrada = packetReceiver.receber();
            Packet pacote = entrada.getPacket();

            switch (pacote.getType()) {
                case HANDSHAKE -> tratarHandshakeDuplicado(pacote);
                case DATA      -> tratarData(pacote);
                case FIN       -> tratarFin(pacote);
                case ACK       -> tratarAckInesperado(pacote);
            }
        }

        System.out.println("[Receiver] Loop principal encerrado.");
    }

    // -------------------------------------------------------------------------
    // Handlers de pacotes
    // -------------------------------------------------------------------------

    /**
     * Trata um HANDSHAKE recebido fora do momento esperado.
     *
     * <p>Durante uma sessão ativa, um HANDSHAKE duplicado indica retransmissão
     * do emissor (que não recebeu confirmação do handshake original). O pacote
     * é descartado silenciosamente — a sessão já está estabelecida.
     *
     * @param pacote o pacote HANDSHAKE recebido
     */
    private void tratarHandshakeDuplicado(Packet pacote) {
        System.out.printf("[Receiver] HANDSHAKE duplicado ignorado (seqNum=%d).%n",
                pacote.getSeqNum());
    }

    /**
     * Implementa a FSM do receptor Go-Back-N para pacotes DATA.
     *
     * <p>Fluxo conforme Kurose &amp; Ross, Figura 3.21 (receptor GBN):
     *
     * <pre>
     * pacote recebido
     *   └─ contabiliza como recebido
     *        ├─ seqNum == expectedSeqNum ?
     *        │    ├─ NÃO → descarta + reenvia último ACK (política GBN)
     *        │    └─ SIM → simular perda?
     *        │                ├─ SIM → descarta silenciosamente (sem ACK)
     *        │                └─ NÃO → grava payload
     *        │                          avança expectedSeqNum
     *        │                          envia ACK(seqNum)
     *        │                          registra aceitação
     * </pre>
     *
     * <p><strong>Simulação de perda:</strong> aplica-se apenas a pacotes em ordem,
     * conforme o enunciado. Pacotes fora de ordem são descartados pela política
     * GBN e não entram na contagem de perda simulada.
     *
     * <p><strong>Reenvio de ACK fora de ordem:</strong> quando {@code lastAcknowledgedSeqNum}
     * é {@code -1} (nenhum ACK enviado ainda), não há ACK anterior válido para reenviar.
     * Nesse caso o pacote fora de ordem é silenciosamente descartado — o emissor
     * aguardará o timeout e retransmitirá a partir do pacote 0.
     *
     * @param pacote o pacote DATA recebido
     */
    private void tratarData(Packet pacote) {
        estatisticas.recordPacketReceived();

        int seqNumRecebido = pacote.getSeqNum();
        int seqNumEsperado = sessao.getExpectedSequenceNumber();

        if (seqNumRecebido != seqNumEsperado) {
            tratarDataForaDeOrdem(seqNumRecebido, seqNumEsperado);
            return;
        }

        if (simularPerda()) {
            tratarDataPerdidoPorSimulacao(seqNumRecebido);
            return;
        }

        tratarDataAceito(pacote, seqNumRecebido);
    }

    /**
     * Trata um pacote DATA cujo número de sequência não é o esperado.
     *
     * <p>Conforme a FSM do receptor GBN: descarta o pacote e reenvia o último
     * ACK enviado, informando ao emissor qual foi o último pacote aceito.
     * O emissor, ao receber o ACK duplicado ou ao sofrer timeout, retransmitirá
     * a partir do pacote não confirmado mais antigo.
     *
     * <p>Se nenhum ACK foi enviado ainda ({@code lastAck == -1}), não há ACK
     * válido para reenviar — o pacote é descartado silenciosamente.
     *
     * @param seqNumRecebido número de sequência do pacote descartado
     * @param seqNumEsperado número de sequência que o receptor aguardava
     */
    private void tratarDataForaDeOrdem(int seqNumRecebido, int seqNumEsperado) {
        estatisticas.recordPacketDiscarded();

        int ultimoAck = sessao.getLastAcknowledgedSequenceNumber();

        if (ultimoAck == -1) {
            // Nenhum ACK enviado ainda: não há confirmação válida para reenviar.
            // O emissor sofrerá timeout e retransmitirá desde o início.
            System.out.printf(
                    "[Receiver] DATA fora de ordem descartado (recebido=%d, esperado=%d). "
                            + "Sem ACK anterior para reenviar.%n",
                    seqNumRecebido, seqNumEsperado);
            return;
        }

        System.out.printf(
                "[Receiver] DATA fora de ordem descartado (recebido=%d, esperado=%d). "
                        + "Reenviando ACK(%d).%n",
                seqNumRecebido, seqNumEsperado, ultimoAck);

        enviarAck(ultimoAck);
    }

    /**
     * Trata um pacote DATA em ordem que foi escolhido para descarte pela
     * simulação de perda.
     *
     * <p>Nenhum ACK é enviado — do ponto de vista do emissor, o pacote nunca
     * chegou. O emissor aguardará o timeout e retransmitirá.
     *
     * @param seqNum número de sequência do pacote descartado por simulação
     */
    private void tratarDataPerdidoPorSimulacao(int seqNum) {
        estatisticas.recordPacketDiscarded();
        System.out.printf(
                "[Receiver] DATA(%d) descartado por simulação de perda.%n", seqNum);
    }

    /**
     * Trata um pacote DATA em ordem que passou pela simulação de perda.
     *
     * <p>Grava o payload em disco, avança o estado da sessão, envia ACK
     * cumulativo e contabiliza a aceitação.
     *
     * @param pacote       o pacote DATA aceito
     * @param seqNum       número de sequência confirmado (igual a {@code pacote.getSeqNum()})
     */
    private void tratarDataAceito(Packet pacote, int seqNum) {
        gravarPayload(pacote);

        sessao.advanceExpectedSequenceNumber();
        sessao.recordAcknowledgement(seqNum);

        enviarAck(seqNum);

        estatisticas.recordPacketAccepted();

        System.out.printf("[Receiver] DATA(%d) aceito e gravado. ACK(%d) enviado.%n",
                seqNum, seqNum);
    }

    /**
     * Grava o payload do pacote no arquivo de destino.
     *
     * <p>Erros de escrita são fatais para a integridade do arquivo — a exceção
     * é encapsulada em {@link RuntimeException} para interromper o loop principal
     * e acionar o bloco {@code finally} do chamador.
     *
     * @param pacote o pacote cujo payload será gravado
     */
    private void gravarPayload(Packet pacote) {
        try {
            fileWriter.write(pacote.getData(), 0, pacote.getDataLength());
        } catch (IOException e) {
            throw new RuntimeException(
                    "Falha irrecuperável ao gravar payload do pacote "
                            + pacote.getSeqNum() + " em disco: " + e.getMessage(), e);
        }
    }

    /**
     * Envia um ACK cumulativo ao emissor e contabiliza o envio nas estatísticas.
     *
     * <p>Erros de envio são registrados mas não interrompem o loop — um ACK
     * perdido é tolerado pelo protocolo, pois o emissor retransmitirá por timeout.
     *
     * @param ackNum número de sequência a confirmar
     */
    private void enviarAck(int ackNum) {
        try {
            ackSender.sendAck(ackNum, enderecoEmissor, portaEmissor);
            estatisticas.recordAckSent();
        } catch (IOException e) {
            System.err.printf(
                    "[Receiver] Aviso: falha ao enviar ACK(%d) — %s. "
                            + "O emissor retransmitirá por timeout.%n",
                    ackNum, e.getMessage());
        }
    }

    /**
     * Determina se um pacote em ordem deve ser descartado por simulação de perda.
     *
     * <p>Gera um valor aleatório {@code r ∈ [0,1)} e retorna {@code true} se
     * {@code r < lossProb}, conforme especificado no enunciado (Seção 4).
     * A simulação age apenas sobre pacotes em ordem — pacotes fora de ordem
     * já são descartados pela política GBN antes de chegar aqui.
     *
     * @return {@code true} se o pacote deve ser descartado por simulação
     */
    private boolean simularPerda() {
        double lossProb = sessao.getParameters().getLossProb();
        if (lossProb == 0.0) {
            return false; // atalho: evita geração de número aleatório desnecessária
        }
        return RANDOM.nextDouble() < lossProb;
    }

    /**
     * Trata o pacote de encerramento FIN enviado pelo emissor.
     *
     * <p>Fecha o {@link FileWriterService} para garantir que todos os bytes
     * em buffer sejam gravados em disco antes de encerrar a sessão.
     * Em seguida, fecha a {@link ReceiverSession}, o que interrompe o loop
     * principal na próxima verificação de {@link ReceiverSession#isReceiving()}.
     *
     * <p>Erros ao fechar o arquivo são registrados mas não interrompem o
     * encerramento da sessão — a sessão é fechada independentemente.
     *
     * @param pacote o pacote FIN recebido
     */
    private void tratarFin(Packet pacote) {
        System.out.printf("[Receiver] FIN recebido (seqNum=%d). Encerrando sessão...%n",
                pacote.getSeqNum());

        fecharFileWriter();
        sessao.close();

        System.out.println("[Receiver] Sessão encerrada com sucesso.");
    }

    /**
     * Trata um pacote ACK recebido pelo receptor — o que não deveria ocorrer
     * neste protocolo, pois ACKs fluem apenas do receptor para o emissor.
     *
     * <p>O pacote é descartado com aviso — pode indicar configuração incorreta
     * de portas ou datagrama entregue ao socket errado.
     *
     * @param pacote o pacote ACK recebido inesperadamente
     */
    private void tratarAckInesperado(Packet pacote) {
        System.out.printf("[Receiver] Aviso: ACK inesperado descartado (ackNum=%d).%n",
                pacote.getAckNum());
    }

    // -------------------------------------------------------------------------
    // Etapas internas de inicialização
    // -------------------------------------------------------------------------

    /**
     * Abre o {@link DatagramSocket} na porta configurada.
     */
    private void abrirSocket() throws IOException {
        socket = new DatagramSocket(porta);
        System.out.printf("[Receiver] Aguardando conexão na porta UDP %d...%n", porta);
    }

    /**
     * Instancia {@link PacketReceiver} e {@link AckSender} compartilhando
     * o mesmo socket — conforme o protocolo, receptor e emissor de ACKs
     * operam na mesma porta local.
     */
    private void instanciarComponentesDeRede() {
        packetReceiver = new PacketReceiver(socket);
        ackSender = new AckSender(socket);
    }

    /**
     * Bloqueia até receber o primeiro datagrama, valida que é um HANDSHAKE,
     * desserializa os {@link SessionParameters} e inicializa a sessão.
     *
     * @throws IOException se a recepção falhar, o pacote não for HANDSHAKE
     *                     ou o arquivo de destino não puder ser aberto
     */
    private void processarHandshake() throws IOException {
        IncomingPacket entrada = packetReceiver.receber();
        Packet pacote = entrada.getPacket();

        validarHandshake(pacote);

        // Preserva o endereço do emissor para uso nos ACKs (etapa seguinte)
        enderecoEmissor = entrada.getEnderecoRemetente();
        portaEmissor = entrada.getPortaRemetente();

        SessionParameters params =
                PacketSerializer.deserializeSessionParameters(pacote.getData());

        System.out.printf("[Receiver] HANDSHAKE recebido de %s:%d — %s%n",
                enderecoEmissor.getHostAddress(), portaEmissor, params);

        inicializarSessao(params);
    }

    /**
     * Garante que o pacote recebido é um HANDSHAKE. Caso contrário, fecha
     * o socket e lança exceção para evitar estado inválido.
     *
     * @param pacote pacote recebido
     * @throws IOException se o tipo não for HANDSHAKE
     */
    private void validarHandshake(Packet pacote) throws IOException {
        if (!pacote.isHandshake()) {
            fecharSocket();
            throw new IOException(
                    "Protocolo violado: esperado HANDSHAKE como primeiro pacote, "
                            + "recebido " + pacote.getType());
        }
    }

    /**
     * Com os parâmetros validados, cria {@link ReceiverSession},
     * abre o {@link FileWriterService} e instancia {@link ReceiverStatistics}.
     *
     * <p>A ordem importa: se {@code fileWriter.open()} falhar, a sessão ainda
     * não foi confirmada — o emissor receberá timeout e poderá retransmitir
     * o HANDSHAKE numa implementação futura.
     *
     * @param params parâmetros desserializados do HANDSHAKE
     * @throws IOException se o arquivo de destino não puder ser criado ou aberto
     */
    private void inicializarSessao(SessionParameters params) throws IOException {
        fileWriter = new FileWriterService();
        fileWriter.open(params.getDestPath());

        sessao = ReceiverSession.open(params, params.getDestPath());
        estatisticas = new ReceiverStatistics();

        System.out.printf("[Receiver] Sessão iniciada. Arquivo de destino: %s%n",
                params.getDestPath());
    }

    // -------------------------------------------------------------------------
    // Encerramento
    // -------------------------------------------------------------------------

    /**
     * Encerra a sessão de forma ordenada: fecha o arquivo em disco, fecha o
     * socket e fecha a {@link ReceiverSession}.
     *
     * <p>Este método é idempotente — chamadas adicionais não têm efeito.
     * Deve ser invocado em um bloco {@code finally} para garantir a liberação
     * de recursos mesmo em caso de erro.
     *
     * <p><strong>Nota:</strong> a exibição das estatísticas finais será
     * adicionada neste método em etapa futura.
     */
    public void encerrar() {
        fecharFileWriter();
        fecharSocket();
        if (sessao != null) {
            sessao.close();
        }
    }

    // -------------------------------------------------------------------------
    // Accessors (para uso pela FSM nas próximas etapas)
    // -------------------------------------------------------------------------

    /**
     * @return a sessão ativa; {@code null} se {@link #iniciar()} ainda não foi chamado
     */
    public ReceiverSession getSessao() {
        return sessao;
    }

    /**
     * @return as estatísticas da sessão atual; {@code null} se não inicializado
     */
    public ReceiverStatistics getEstatisticas() {
        return estatisticas;
    }

    /**
     * @return o componente de recepção de pacotes; {@code null} se não inicializado
     */
    public PacketReceiver getPacketReceiver() {
        return packetReceiver;
    }

    /**
     * @return o componente de envio de ACKs; {@code null} se não inicializado
     */
    public AckSender getAckSender() {
        return ackSender;
    }

    /**
     * @return o serviço de escrita do arquivo; {@code null} se não inicializado
     */
    public FileWriterService getFileWriter() {
        return fileWriter;
    }

    /**
     * @return o endereço IP do emissor, extraído do HANDSHAKE;
     *         {@code null} se {@link #iniciar()} ainda não foi chamado
     */
    public java.net.InetAddress getEnderecoEmissor() {
        return enderecoEmissor;
    }

    /**
     * @return a porta UDP do emissor, extraída do HANDSHAKE;
     *         {@code 0} se {@link #iniciar()} ainda não foi chamado
     */
    public int getPortaEmissor() {
        return portaEmissor;
    }

    // -------------------------------------------------------------------------
    // Helpers de fechamento
    // -------------------------------------------------------------------------

    private void fecharFileWriter() {
        if (fileWriter != null && fileWriter.isOpen()) {
            try {
                fileWriter.close();
            } catch (IOException e) {
                System.err.println("[Receiver] Aviso: erro ao fechar arquivo — " + e.getMessage());
            }
        }
    }

    private void fecharSocket() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
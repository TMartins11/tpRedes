package br.unifal.redes.receiver.fsm;

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
import java.net.InetAddress;
import java.util.Objects;
import java.util.Random;

/**
 * Máquina de estados finita (FSM) do Receptor no protocolo Go-Back-N.
 *
 * <p>Esta classe é o único componente do lado receptor que conhece o fluxo
 * completo do protocolo. Ela orquestra todos os demais componentes
 * especializados — {@link PacketReceiver}, {@link AckSender},
 * {@link FileWriterService}, {@link ReceiverSession} e
 * {@link ReceiverStatistics} — exatamente como o {@code SenderFSM}
 * orquestra os componentes do lado emissor.
 *
 * <h2>Fluxo geral da recepção</h2>
 * <ol>
 *   <li><strong>Fase 1 — HANDSHAKE:</strong> aguarda o pacote HANDSHAKE do
 *       Emissor via {@link PacketReceiver#receber()}, desserializa os
 *       {@link SessionParameters} do payload via
 *       {@link PacketSerializer#deserializeSessionParameters(byte[])}, abre
 *       o arquivo de destino via {@link FileWriterService#open(String)}, cria
 *       a {@link ReceiverSession} e responde com ACK(0) ao Emissor.
 *       Em seguida, chama {@link ReceiverSession#advanceExpectedSequenceNumber()}
 *       <strong>uma única vez</strong>, movendo {@code expectedSeqNum} de
 *       {@code 0} para {@code 1}. Isso preserva o invariante do protocolo:
 *       {@code seqNum = 0} é reservado exclusivamente ao HANDSHAKE e ao seu
 *       ACK; nenhum segmento DATA jamais usa esse valor.</li>
 *   <li><strong>Fase 2 — DATA (loop principal):</strong> enquanto a sessão
 *       estiver ativa ({@link ReceiverSession#isReceiving()}), cada
 *       {@link IncomingPacket} recebido é despachado para o handler
 *       correspondente ao tipo do pacote. O loop termina quando
 *       {@link ReceiverSession#close()} é chamado pelo handler do FIN.</li>
 *   <li><strong>Handler DATA — política GBN:</strong>
 *       <ul>
 *         <li>Pacote em ordem, não simulado como perdido → grava payload em
 *             disco, avança {@code expectedSeqNum}, envia ACK cumulativo,
 *             contabiliza aceitação nas estatísticas.</li>
 *         <li>Pacote em ordem, simulado como perdido → descartado sem ACK;
 *             contabiliza descarte nas estatísticas.</li>
 *         <li>Pacote fora de ordem → descartado; reenvia o último ACK
 *             confirmado (se houver); contabiliza descarte nas estatísticas.</li>
 *       </ul></li>
 *   <li><strong>Handler FIN:</strong> fecha o {@link FileWriterService}
 *       (garantindo flush dos buffers em disco), envia ACK(finSeqNum) ao
 *       Emissor e encerra a {@link ReceiverSession}.</li>
 *   <li><strong>Pacotes inesperados</strong> (ACK, HANDSHAKE duplicado):
 *       descartados silenciosamente sem alterar o estado da sessão.</li>
 * </ol>
 *
 * <h2>Estatísticas</h2>
 * <p>{@link ReceiverStatistics} é passado como parâmetro de {@link #executar}
 * e retornado como {@link ReceiverStatistics.Snapshot} ao final, permitindo
 * que o bootstrap ({@code Receiver}) exiba o relatório final sem precisar
 * conhecer os detalhes internos da FSM.
 *
 * <h2>Simulação de perda de pacotes</h2>
 * <p>Conforme Seção 4 do enunciado, a simulação atua <em>somente</em> sobre
 * pacotes DATA recebidos em ordem. Para cada pacote em ordem, um valor
 * {@code r ∈ [0,1)} é sorteado; se {@code r < lossProb}, o pacote é
 * descartado sem envio de ACK, como se tivesse sido perdido na rede. Pacotes
 * fora de ordem são descartados pela política GBN antes de chegar na
 * verificação de perda e não são contabilizados como perdas simuladas.
 *
 * <h2>Responsabilidade única</h2>
 * <p>Esta classe explicitamente <strong>não</strong> faz:
 * <ul>
 *   <li>Não abre nem fecha o {@link java.net.DatagramSocket} — responsabilidade
 *       do {@code Receiver} (bootstrap).</li>
 *   <li>Não serializa nem desserializa pacotes diretamente — delega a
 *       {@link PacketReceiver} e {@link AckSender}.</li>
 *   <li>Não cria threads, timers nem executores agendados — todo o controle
 *       de fluxo ocorre no thread chamador de {@link #executar}.</li>
 *   <li>Não implementa lógica de janela deslizante — o Receptor GBN é
 *       puramente sequencial; {@code expectedSeqNum} avança um a um.</li>
 * </ul>
 *
 * <p>Thread-safety: esta classe não é sincronizada. Cada instância deve ser
 * utilizada por um único thread.
 */
public final class GbnReceiverFsm {

    /**
     * Gerador de números aleatórios para simulação de perda de pacotes.
     * Instância única por {@code GbnReceiverFsm} — garante distribuição
     * uniforme ao longo de toda a sessão e evita o overhead de criação
     * repetida a cada chamada.
     */
    private final Random random = new Random();

    // -------------------------------------------------------------------------
    // Ponto de entrada principal
    // -------------------------------------------------------------------------

    /**
     * Executa o ciclo completo de recepção Go-Back-N: HANDSHAKE, loop de
     * recepção de dados e encerramento por FIN.
     *
     * <p>Este método bloqueia até que a sessão seja encerrada por um pacote
     * FIN recebido do Emissor, ou até que ocorra um erro irrecuperável de
     * rede ou de disco.
     *
     * <p><strong>Pré-condições:</strong>
     * <ul>
     *   <li>{@code packetReceiver} deve estar associado a um
     *       {@link java.net.DatagramSocket} já aberto e vinculado à porta
     *       local configurada.</li>
     *   <li>{@code ackSender} deve estar associado ao mesmo socket.</li>
     *   <li>{@code fileWriter} deve estar criado mas ainda não aberto —
     *       esta FSM chama {@link FileWriterService#open(String)} internamente
     *       após receber o HANDSHAKE.</li>
     *   <li>{@code estatisticas} deve estar criado e zerado — esta FSM
     *       acumula os eventos e o chamador usa o snapshot retornado para
     *       exibir o relatório final.</li>
     * </ul>
     *
     * @param packetReceiver componente de recepção de datagramas UDP;
     *                       não pode ser {@code null}
     * @param ackSender      componente de envio de confirmações ACK;
     *                       não pode ser {@code null}
     * @param fileWriter     serviço de escrita do arquivo recebido,
     *                       ainda não aberto; não pode ser {@code null}
     * @param estatisticas   coletor de estatísticas da sessão;
     *                       não pode ser {@code null}
     * @return retrato imutável das estatísticas acumuladas durante a sessão
     * @throws NullPointerException se qualquer parâmetro for {@code null}
     * @throws IOException          se ocorrer falha irrecuperável de rede
     *                               durante o HANDSHAKE ou a recepção de
     *                               pacotes, ou falha ao abrir/escrever/
     *                               fechar o arquivo de destino
     */
    public ReceiverStatistics.Snapshot executar(PacketReceiver packetReceiver,
                                                AckSender ackSender,
                                                FileWriterService fileWriter,
                                                ReceiverStatistics estatisticas) throws IOException {
        Objects.requireNonNull(packetReceiver, "packetReceiver não pode ser nulo");
        Objects.requireNonNull(ackSender,      "ackSender não pode ser nulo");
        Objects.requireNonNull(fileWriter,     "fileWriter não pode ser nulo");
        Objects.requireNonNull(estatisticas,   "estatisticas não pode ser nulo");

        // Fase 1 — HANDSHAKE.
        // Bloqueia até o HANDSHAKE chegar, inicializa todos os componentes,
        // responde com ACK(0) e avança expectedSeqNum para 1.
        ReceiverSession sessao = processarHandshake(packetReceiver, ackSender, fileWriter);

        // Fase 2 — Loop principal de DATA + FIN.
        // Retorna apenas quando sessao.isReceiving() se tornar false,
        // o que ocorre após o handler do FIN chamar sessao.close().
        executarLoopPrincipal(sessao, packetReceiver, ackSender, fileWriter, estatisticas);

        // Retorna o snapshot final das estatísticas para que o bootstrap
        // (Receiver.java) possa exibir o relatório sem conhecer os internos
        // da FSM.
        return estatisticas.snapshot();
    }

    // -------------------------------------------------------------------------
    // Fase 1 — HANDSHAKE
    // -------------------------------------------------------------------------

    /**
     * Aguarda e processa o pacote HANDSHAKE inicial do Emissor.
     *
     * <p>Sequência de operações:
     * <ol>
     *   <li>Bloqueia em {@link PacketReceiver#receber()} até o primeiro
     *       datagrama chegar. O Receptor aguarda passivamente, sem timeout.</li>
     *   <li>Valida que o tipo é {@code HANDSHAKE}; qualquer outro tipo
     *       viola o protocolo e aborta com {@link IOException}.</li>
     *   <li>Desserializa os {@link SessionParameters} do payload via
     *       {@link PacketSerializer#deserializeSessionParameters(byte[])}.</li>
     *   <li>Abre o arquivo de destino via
     *       {@link FileWriterService#open(String)} <em>antes</em> de criar
     *       a sessão e de enviar o ACK. Se a abertura falhar, nenhuma sessão
     *       é criada e o Emissor sofrerá timeout — sem estado inconsistente
     *       no Receptor.</li>
     *   <li>Cria a {@link ReceiverSession} com os parâmetros negociados.
     *       Neste ponto: {@code expectedSeqNum = 0},
     *       {@code lastAck = -1}, estado {@code RECEIVING}.</li>
     *   <li>Envia ACK(0) ao Emissor — confirmação explícita do HANDSHAKE.
     *       Sem este ACK, o {@code SenderFSM} permaneceria bloqueado
     *       aguardando a confirmação e nunca iniciaria o envio de DATA.</li>
     *   <li>Avança {@code expectedSeqNum} de {@code 0} para {@code 1} via
     *       {@link ReceiverSession#advanceExpectedSequenceNumber()}. Isso
     *       reserva estruturalmente {@code seqNum = 0} ao HANDSHAKE: o
     *       primeiro DATA que o Receptor aceitará terá {@code seqNum = 1},
     *       espelhando o {@code FIRST_DATA_SEQUENCE_NUMBER = 1} do
     *       {@code SenderFSM}. Toda a lógica de protocolo permanece
     *       encapsulada nesta FSM; {@link ReceiverSession} continua
     *       genérica, sem conhecer este detalhe.</li>
     * </ol>
     *
     * @param packetReceiver componente de recepção
     * @param ackSender      componente de envio de ACKs
     * @param fileWriter     serviço de escrita — será aberto aqui
     * @return a {@link ReceiverSession} inicializada com
     *         {@code expectedSeqNum = 1}, pronta para receber DATA
     * @throws IOException se a recepção falhar, o pacote não for HANDSHAKE,
     *                     a desserialização falhar, o arquivo não puder ser
     *                     criado ou o ACK não puder ser enviado
     */
    private ReceiverSession processarHandshake(PacketReceiver packetReceiver,
                                               AckSender ackSender,
                                               FileWriterService fileWriter) throws IOException {

        System.out.println("[FSM] 1 - Entrou em processarHandshake()");

        IncomingPacket entrada = packetReceiver.receber();

        System.out.println("[FSM] 2 - Datagrama recebido");

        Packet pacote = entrada.getPacket();

        System.out.println("[FSM] 3 - Tipo do pacote: " + pacote.getType());

        if (!pacote.isHandshake()) {
            throw new IOException(
                    "Esperava HANDSHAKE, recebeu " + pacote.getType()
            );
        }

        System.out.println("[FSM] 4 - HANDSHAKE validado");

        InetAddress enderecoEmissor = entrada.getEnderecoRemetente();
        int portaEmissor = entrada.getPortaRemetente();

        System.out.println("[FSM] 5 - Remetente: "
                + enderecoEmissor.getHostAddress()
                + ":" + portaEmissor);

        SessionParameters params =
                PacketSerializer.deserializeSessionParameters(pacote.getData());

        System.out.println("[FSM] 6 - SessionParameters desserializados");
        System.out.println("[FSM]     destino = " + params.getDestPath());
        System.out.println("[FSM]     janela = " + params.getWindowSize());
        System.out.println("[FSM]     perda   = " + params.getLossProb());

        fileWriter.open(params.getDestPath());

        System.out.println("[FSM] 7 - Arquivo aberto");

        ReceiverSession sessao =
                ReceiverSession.open(params, params.getDestPath());

        System.out.println("[FSM] 8 - Sessao criada");

        ackSender.sendAck(0, enderecoEmissor, portaEmissor);

        System.out.println("[FSM] 9 - ACK(0) enviado");

        sessao.advanceExpectedSequenceNumber();

        System.out.println("[FSM] 10 - expectedSequenceNumber = "
                + sessao.getExpectedSequenceNumber());

        return sessao;
    }

    // -------------------------------------------------------------------------
    // Fase 2 — Loop principal
    // -------------------------------------------------------------------------

    /**
     * Executa o loop principal de recepção Go-Back-N.
     *
     * <p>A cada iteração, aguarda um datagrama via
     * {@link PacketReceiver#receber()} e despacha o pacote para o handler
     * correspondente ao seu tipo. O loop termina quando
     * {@link ReceiverSession#isReceiving()} retorna {@code false}, o que
     * ocorre imediatamente após o handler do FIN chamar
     * {@link ReceiverSession#close()}.
     *
     * <p>O endereço do Emissor é atualizado a cada iteração a partir do
     * {@link IncomingPacket} recebido. Em condições normais, o Emissor usa
     * sempre o mesmo endereço e porta; a atualização contínua é defensiva
     * para cobrir eventuais cenários de NAT ou reuso de porta.
     *
     * @param sessao         sessão de recepção ativa, com
     *                       {@code expectedSeqNum = 1}
     * @param packetReceiver componente de recepção de datagramas
     * @param ackSender      componente de envio de ACKs
     * @param fileWriter     serviço de escrita em disco (já aberto)
     * @param estatisticas   coletor de estatísticas da sessão
     * @throws IOException se ocorrer erro irrecuperável de rede ou de disco
     */
    private void executarLoopPrincipal(ReceiverSession sessao,
                                       PacketReceiver packetReceiver,
                                       AckSender ackSender,
                                       FileWriterService fileWriter,
                                       ReceiverStatistics estatisticas) throws IOException {
        // Endereço do Emissor — atualizado a cada IncomingPacket recebido.
        InetAddress enderecoEmissor = null;
        int portaEmissor = 0;

        while (sessao.isReceiving()) {
            IncomingPacket entrada = packetReceiver.receber();
            Packet pacote = entrada.getPacket();

            // Atualiza o endereço do Emissor a cada datagrama recebido.
            enderecoEmissor = entrada.getEnderecoRemetente();
            portaEmissor    = entrada.getPortaRemetente();

            // Despacha para o handler correspondente ao tipo do pacote.
            switch (pacote.getType()) {
                case DATA ->
                        tratarData(pacote, sessao, ackSender, fileWriter,
                                estatisticas, enderecoEmissor, portaEmissor);
                case FIN ->
                        tratarFin(pacote, sessao, ackSender, fileWriter,
                                enderecoEmissor, portaEmissor);
                case HANDSHAKE ->
                        tratarHandshakeDuplicado(pacote);
                case ACK ->
                        tratarAckInesperado(pacote);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Handlers de pacotes
    // -------------------------------------------------------------------------

    /**
     * Implementa a política GBN do Receptor para pacotes DATA
     * (Kurose &amp; Ross, 8ª ed., Figura 3.21).
     *
     * <p>Fluxo de decisão:
     * <pre>
     * DATA recebido
     *   ├─ contabiliza como recebido (recordPacketReceived)
     *   ├─ seqNum == expectedSeqNum ?
     *   │    ├─ NÃO  → fora de ordem
     *   │    │          descarta + reenvia último ACK (se houver)
     *   │    │          contabiliza descarte (recordPacketDiscarded)
     *   │    └─ SIM  → em ordem; simular perda?
     *   │                 ├─ SIM  → descarta silenciosamente (sem ACK)
     *   │                 │          contabiliza descarte (recordPacketDiscarded)
     *   │                 └─ NÃO  → grava payload em disco
     *   │                            avança expectedSeqNum
     *   │                            registra ACK na sessão
     *   │                            envia ACK(seqNum)
     *   │                            contabiliza aceitacao (recordPacketAccepted)
     *   │                            contabiliza ACK enviado (recordAckSent)
     * </pre>
     *
     * <p>A simulação de perda aplica-se <strong>somente</strong> a pacotes
     * em ordem, conforme Seção 4 do enunciado. Pacotes fora de ordem são
     * descartados pela política GBN antes de chegar na verificação de perda.
     *
     * @param pacote          pacote DATA recebido
     * @param sessao          estado atual da sessão de recepção
     * @param ackSender       componente de envio de ACKs
     * @param fileWriter      serviço de escrita em disco
     * @param estatisticas    coletor de estatísticas
     * @param enderecoEmissor endereço IP do Emissor (do IncomingPacket)
     * @param portaEmissor    porta UDP do Emissor (do IncomingPacket)
     * @throws IOException se a gravação em disco falhar (erro irrecuperável)
     */
    private void tratarData(Packet pacote,
                            ReceiverSession sessao,
                            AckSender ackSender,
                            FileWriterService fileWriter,
                            ReceiverStatistics estatisticas,
                            InetAddress enderecoEmissor,
                            int portaEmissor) throws IOException {

        // Contabiliza o recebimento antes de qualquer decisão de descarte.
        estatisticas.recordPacketReceived();

        int seqNumRecebido = pacote.getSeqNum();
        int seqNumEsperado = sessao.getExpectedSequenceNumber();

        // --- Verificação GBN: número de sequência correto? ---
        if (seqNumRecebido != seqNumEsperado) {
            estatisticas.recordPacketDiscarded();
            tratarDataForaDeOrdem(seqNumRecebido, seqNumEsperado,
                    sessao, ackSender,
                    enderecoEmissor, portaEmissor);
            return;
        }

        // --- Pacote em ordem: simular perda? ---
        if (simularPerda(sessao.getParameters().getLossProb())) {
            // Descarte silencioso por simulação: do ponto de vista do Emissor,
            // o pacote nunca chegou. Nenhum ACK é enviado; o Emissor
            // retransmitirá ao expirar o timeout de retransmissão.
            estatisticas.recordPacketDiscarded();
            return;
        }

        // --- Pacote em ordem, não simulado como perdido: aceitar ---
        tratarDataAceito(pacote, seqNumRecebido,
                sessao, ackSender, fileWriter,
                estatisticas, enderecoEmissor, portaEmissor);
    }

    /**
     * Trata um pacote DATA cujo {@code seqNum} não é o esperado.
     *
     * <p>Política GBN: descarta o pacote e reenvia o último ACK cumulativo
     * confirmado, informando ao Emissor qual foi o último segmento aceito.
     * O Emissor, ao sofrer timeout, retransmitirá a partir do pacote não
     * confirmado mais antigo (base da janela).
     *
     * <p>Se nenhum ACK foi enviado ainda
     * ({@code lastAcknowledgedSeqNum == -1}), não há ACK anterior válido
     * para reenviar. O pacote é descartado silenciosamente — o Emissor
     * sofrerá timeout e retransmitirá desde o primeiro DATA (seqNum=1).
     *
     * <p>Erros de envio do ACK de reenvio são tolerados: um ACK perdido não
     * interrompe a transmissão — o Emissor retransmitirá por timeout.
     *
     * @param seqNumRecebido  número de sequência do pacote descartado
     * @param seqNumEsperado  número de sequência que o receptor aguardava
     * @param sessao          estado da sessão
     * @param ackSender       componente de envio de ACKs
     * @param enderecoEmissor endereço IP do Emissor
     * @param portaEmissor    porta UDP do Emissor
     */
    private static void tratarDataForaDeOrdem(int seqNumRecebido,
                                              int seqNumEsperado,
                                              ReceiverSession sessao,
                                              AckSender ackSender,
                                              InetAddress enderecoEmissor,
                                              int portaEmissor) {
        int ultimoAck = sessao.getLastAcknowledgedSequenceNumber();

        if (ultimoAck == -1) {
            // Nenhum ACK enviado ainda: não há confirmação válida para
            // reenviar. O Emissor sofrerá timeout e retransmitirá desde
            // seqNum=1 (primeiro DATA).
            return;
        }

        // Reenvia o último ACK cumulativo. Falhas de envio são toleradas —
        // o Emissor retransmitirá por timeout sem nenhum dano ao estado.
        enviarAckComTolerancia(ackSender, ultimoAck, enderecoEmissor, portaEmissor);
    }

    /**
     * Processa um pacote DATA em ordem que passou pela verificação de
     * simulação de perda e será efetivamente aceito.
     *
     * <p>Sequência de operações (ordem é importante):
     * <ol>
     *   <li>Grava o payload em disco via
     *       {@link FileWriterService#write(byte[], int, int)} —
     *       <strong>antes</strong> de enviar o ACK. Se a escrita falhar,
     *       o ACK não é emitido e o Emissor retransmitirá.</li>
     *   <li>Avança {@code expectedSeqNum} na sessão.</li>
     *   <li>Registra {@code seqNum} como último ACK confirmado na sessão,
     *       para uso em reenvios futuros de pacotes fora de ordem.</li>
     *   <li>Envia ACK(seqNum) ao Emissor. Falhas de envio são toleradas.</li>
     *   <li>Contabiliza aceitação e ACK enviado nas estatísticas.</li>
     * </ol>
     *
     * @param pacote          pacote DATA aceito
     * @param seqNum          número de sequência do pacote
     * @param sessao          estado da sessão
     * @param ackSender       componente de envio de ACKs
     * @param fileWriter      serviço de escrita em disco
     * @param estatisticas    coletor de estatísticas
     * @param enderecoEmissor endereço IP do Emissor
     * @param portaEmissor    porta UDP do Emissor
     * @throws IOException se a gravação em disco falhar
     */
    private static void tratarDataAceito(Packet pacote,
                                         int seqNum,
                                         ReceiverSession sessao,
                                         AckSender ackSender,
                                         FileWriterService fileWriter,
                                         ReceiverStatistics estatisticas,
                                         InetAddress enderecoEmissor,
                                         int portaEmissor) throws IOException {
        // 1. Grava o payload ANTES de enviar o ACK.
        //    Packet.getData() retorna uma cópia defensiva — seguro passar
        //    diretamente ao FileWriterService, que grava [0, dataLength).
        fileWriter.write(pacote.getData(), 0, pacote.getDataLength());

        // 2. Avança expectedSeqNum: próximo pacote aceito deve ter seqNum+1.
        sessao.advanceExpectedSequenceNumber();

        // 3. Registra seqNum como último ACK confirmado.
        //    tratarDataForaDeOrdem() usará este valor para reenviar o ACK
        //    em chegadas futuras de pacotes fora de ordem.
        sessao.recordAcknowledgement(seqNum);

        // 4. Envia ACK cumulativo. Falhas são toleradas: o Emissor
        //    retransmitirá por timeout e o próximo ACK bem-sucedido
        //    cobrirá este e todos os anteriores (ACK cumulativo).
        enviarAckComTolerancia(ackSender, seqNum, enderecoEmissor, portaEmissor);

        // 5. Contabiliza aceitação e ACK enviado. Feito após enviarAck
        //    para refletir apenas envios bem-sucedidos (enviarAckComTolerancia
        //    suprime exceções, mas o pacote foi aceito e gravado de qualquer
        //    forma — contabilizamos ambos independentemente do ACK de rede).
        estatisticas.recordPacketAccepted();
        estatisticas.recordAckSent();
    }

    /**
     * Trata o pacote de encerramento FIN enviado pelo Emissor.
     *
     * <p>Sequência de operações:
     * <ol>
     *   <li>Fecha o {@link FileWriterService}, garantindo que o
     *       {@link java.io.BufferedOutputStream} interno seja esvaziado
     *       ({@code flush}) e o file handle seja liberado antes de qualquer
     *       outra operação de encerramento.</li>
     *   <li>Envia ACK(finSeqNum) ao Emissor — confirmação do FIN. O
     *       {@code SenderFSM} verifica {@code ack.getAckNum() == finSeq}
     *       para registrar a confirmação; se o ACK for perdido, o FIN não
     *       é retransmitido ({@code SenderFSM} usa melhor esforço para o
     *       FIN).</li>
     *   <li>Fecha a {@link ReceiverSession} via {@link ReceiverSession#close()},
     *       fazendo {@link ReceiverSession#isReceiving()} retornar
     *       {@code false} e encerrando o loop principal.</li>
     * </ol>
     *
     * <p>Erros ao fechar o arquivo são registrados no {@code stderr} mas não
     * impedem o encerramento da sessão — a sessão é fechada
     * independentemente para liberar recursos e garantir estado consistente.
     *
     * @param pacote          pacote FIN recebido
     * @param sessao          sessão de recepção ativa
     * @param ackSender       componente de envio de ACKs
     * @param fileWriter      serviço de escrita em disco (será fechado aqui)
     * @param enderecoEmissor endereço IP do Emissor
     * @param portaEmissor    porta UDP do Emissor
     */
    private static void tratarFin(Packet pacote,
                                  ReceiverSession sessao,
                                  AckSender ackSender,
                                  FileWriterService fileWriter,
                                  InetAddress enderecoEmissor,
                                  int portaEmissor) {
        // 1. Fecha o arquivo: flush dos buffers internos + liberação do handle.
        //    Erros são registrados mas não impedem o encerramento da sessão.
        try {
            if (fileWriter.isOpen()) {
                fileWriter.close();
            }
        } catch (IOException e) {
            System.err.printf(
                    "[GbnReceiverFsm] Aviso: erro ao fechar arquivo após FIN "
                            + "(seqNum=%d): %s%n",
                    pacote.getSeqNum(), e.getMessage()
            );
        }

        // 2. Envia ACK(finSeqNum) ao Emissor.
        //    O SenderFSM verifica ack.getAckNum() == finSeq para contabilizar
        //    a confirmação. Falhas de envio são toleradas — o SenderFSM
        //    não retransmite o FIN (melhor esforço).
        enviarAckComTolerancia(ackSender, pacote.getSeqNum(),
                enderecoEmissor, portaEmissor);

        // 3. Encerra a sessão: isReceiving() passa a retornar false,
        //    encerrando o loop principal após o retorno deste método.
        sessao.close();
    }

    /**
     * Trata um pacote HANDSHAKE recebido durante a fase de dados.
     *
     * <p>Um HANDSHAKE duplicado nesta fase indica que o ACK(0) enviado pelo
     * Receptor não chegou ao Emissor (perdido na rede) e o Emissor
     * retransmitiu o HANDSHAKE. O pacote é descartado silenciosamente: a
     * sessão já está estabelecida e processar um novo HANDSHAKE corromperia
     * o estado em andamento.
     *
     * @param pacote pacote HANDSHAKE recebido durante a fase de dados
     */
    private static void tratarHandshakeDuplicado(Packet pacote) {
        // Descarte silencioso: sessão já estabelecida e ativa.
    }

    /**
     * Trata um pacote ACK recebido pelo Receptor.
     *
     * <p>No protocolo GBN, ACKs fluem exclusivamente do Receptor para o
     * Emissor — nunca na direção inversa. Receber um ACK no Receptor
     * indica configuração incorreta de portas ou datagrama entregue ao
     * socket errado. O pacote é descartado silenciosamente.
     *
     * @param pacote pacote ACK recebido inesperadamente
     */
    private static void tratarAckInesperado(Packet pacote) {
        // Descarte silencioso: ACKs não fazem parte do fluxo de recepção.
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    /**
     * Envia um ACK cumulativo ao Emissor com tolerância a falhas de rede.
     *
     * <p>Erros de envio são registrados no {@code stderr} mas não abortam
     * a transmissão. Um ACK perdido é tolerado pelo protocolo GBN: o
     * Emissor retransmitirá por timeout, e o próximo ACK bem-sucedido
     * confirmará cumulativamente todos os pacotes anteriores.
     *
     * @param ackSender       componente de envio de ACKs
     * @param ackNum          número de sequência a confirmar cumulativamente
     * @param enderecoEmissor endereço IP do Emissor
     * @param portaEmissor    porta UDP do Emissor
     */
    private static void enviarAckComTolerancia(AckSender ackSender,
                                               int ackNum,
                                               InetAddress enderecoEmissor,
                                               int portaEmissor) {
        try {
            ackSender.sendAck(ackNum, enderecoEmissor, portaEmissor);
        } catch (IOException e) {
            System.err.printf(
                    "[GbnReceiverFsm] Aviso: falha ao enviar ACK(%d) para %s:%d -- %s. "
                            + "O Emissor retransmitirá por timeout.%n",
                    ackNum, enderecoEmissor.getHostAddress(), portaEmissor, e.getMessage()
            );
        }
    }

    /**
     * Determina se um pacote DATA em ordem deve ser descartado por
     * simulação de perda.
     *
     * <p>Sorteia um valor {@code r ∈ [0,1)} e retorna {@code true} se
     * {@code r < lossProb}, conforme Seção 4 do enunciado. O atalho para
     * {@code lossProb == 0.0} evita a geração desnecessária de um número
     * aleatório no caso mais comum (execução sem simulação de perda).
     *
     * <p>Esta verificação é aplicada <strong>somente</strong> a pacotes em
     * ordem. Pacotes fora de ordem são descartados pela política GBN antes
     * de chegar aqui e não devem ser contabilizados como perdas simuladas.
     *
     * @param lossProb probabilidade de perda configurada, em {@code [0.0, 1.0)}
     * @return {@code true} se o pacote deve ser descartado por simulação
     */
    private boolean simularPerda(double lossProb) {
        if (lossProb == 0.0) {
            return false; // atalho: sem simulação de perda configurada
        }
        return random.nextDouble() < lossProb;
    }
}
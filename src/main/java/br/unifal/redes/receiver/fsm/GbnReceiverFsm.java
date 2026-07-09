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

public final class GbnReceiverFsm {

    private final Random random = new Random();

    // -------------------------------------------------------------------------
    // Ponto de entrada principal
    // -------------------------------------------------------------------------

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

    private static void tratarHandshakeDuplicado(Packet pacote) {
        // Descarte silencioso: sessão já estabelecida e ativa.
    }


    private static void tratarAckInesperado(Packet pacote) {
        // Descarte silencioso: ACKs não fazem parte do fluxo de recepção.
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

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

    private boolean simularPerda(double lossProb) {
        if (lossProb == 0.0) {
            return false; // atalho: sem simulação de perda configurada
        }
        return random.nextDouble() < lossProb;
    }
}
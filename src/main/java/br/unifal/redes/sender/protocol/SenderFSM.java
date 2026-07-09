package br.unifal.redes.sender.protocol;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketCodec;
import br.unifal.redes.common.SessionParameters;
import br.unifal.redes.sender.network.PacketBuffer;
import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.sender.network.TimeoutManager;
import br.unifal.redes.sender.network.WindowManager;
import br.unifal.redes.sender.protocol.AckReceiver;
import br.unifal.redes.sender.protocol.DataSender;
import br.unifal.redes.sender.protocol.HandshakeSender;
import br.unifal.redes.sender.protocol.RetransmissionManager;
import br.unifal.redes.sender.statistics.SenderStatistics;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.Objects;

public final class SenderFSM {

    /**
     * Número de sequência reservado exclusivamente para o HANDSHAKE e seu
     * ACK. Nenhum segmento DATA jamais usa este valor.
     */
    private static final int HANDSHAKE_SEQUENCE_NUMBER = 0;

    /**
     * Número de sequência do primeiro segmento DATA, imediatamente após o
     * HANDSHAKE confirmado.
     */
    private static final int FIRST_DATA_SEQUENCE_NUMBER = HANDSHAKE_SEQUENCE_NUMBER + 1;

    /** Número máximo de tentativas de HANDSHAKE antes de desistir. */
    private static final int MAX_HANDSHAKE_ATTEMPTS = 5;

    private final SenderSocketService socketService;
    private final SessionParameters sessionParameters;
    private final InetAddress receiverAddress;
    private final int receiverPort;
    private final List<byte[]> fileChunks;
    private final int timeoutMillis;

    /**
     * @param socketService     serviço de socket UDP, já aberto; não pode ser {@code null}
     * @param sessionParameters parâmetros da sessão a negociar no HANDSHAKE; não pode ser {@code null}
     * @param receiverAddress   endereço IP do Receptor; não pode ser {@code null}
     * @param receiverPort      porta UDP do Receptor; deve estar em {@code [1, 65535]}
     * @param fileChunks        blocos do arquivo já lidos por {@code FileChunkReader}, em ordem; não pode ser {@code null}
     * @param timeoutMillis     duração do timeout de retransmissão, em milissegundos; deve ser {@code > 0}
     */
    public SenderFSM(SenderSocketService socketService,
                     SessionParameters sessionParameters,
                     InetAddress receiverAddress,
                     int receiverPort,
                     List<byte[]> fileChunks,
                     int timeoutMillis) {
        this.socketService = Objects.requireNonNull(socketService, "socketService não pode ser nulo");
        this.sessionParameters = Objects.requireNonNull(sessionParameters, "sessionParameters não pode ser nulo");
        this.receiverAddress = Objects.requireNonNull(receiverAddress, "receiverAddress não pode ser nulo");
        if (receiverPort < 1 || receiverPort > 65535) {
            throw new IllegalArgumentException("receiverPort must be in [1, 65535], got: " + receiverPort);
        }
        this.receiverPort = receiverPort;
        this.fileChunks = List.copyOf(Objects.requireNonNull(fileChunks, "fileChunks não pode ser nulo"));
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0, got: " + timeoutMillis);
        }
        this.timeoutMillis = timeoutMillis;
    }

    // -------------------------------------------------------------------------
    // Orquestração geral
    // -------------------------------------------------------------------------

    /**
     * Executa a transmissão completa: HANDSHAKE, envio de todos os blocos
     * DATA com retransmissão Go-Back-N, e finalização com FIN.
     *
     * @return um retrato final das estatísticas da transmissão
     * @throws IOException se o HANDSHAKE não puder ser confirmado, ou se
     *                      ocorrer um erro de E/S irrecuperável durante a transmissão
     */
    public SenderStatistics.Snapshot run() throws IOException {
        // Fase 1 — HANDSHAKE. Só retorna depois de confirmado.
        WindowManager windowManager = performHandshake();

        // A partir daqui, e somente a partir daqui, a janela de DATA existe.
        PacketBuffer<Packet> packetBuffer = new PacketBuffer<>();
        TimeoutManager timeoutManager = new TimeoutManager(timeoutMillis);
        SenderStatistics statistics = new SenderStatistics();

        // Fase 2 — DATA.
        transmitFileChunks(windowManager, packetBuffer, timeoutManager, statistics);

        // Fase 3 — encerramento.
        sendFin(windowManager, statistics);

        return statistics.snapshot();
    }

    // -------------------------------------------------------------------------
    // Fase 1: HANDSHAKE
    // -------------------------------------------------------------------------

    /**
     * Envia o HANDSHAKE e bloqueia aguardando sua confirmação, retransmitindo
     * o HANDSHAKE em caso de timeout, até um número máximo de tentativas.
     *
     * <p>Somente após uma confirmação explícita (um ACK com
     * {@code ackNum == HANDSHAKE_SEQUENCE_NUMBER}) é que esta FSM cria o
     * {@link WindowManager} da fase de DATA — e já o cria com
     * {@code initialSequenceNumber = FIRST_DATA_SEQUENCE_NUMBER}, nunca 0.
     * Isso satisfaz, ao mesmo tempo: (a) o Emissor aguarda o ACK do
     * HANDSHAKE antes de iniciar a transmissão de DATA, e (b) os números de
     * sequência de DATA começam em 1.
     *
     * @return o {@link WindowManager} da fase de DATA, já inicializado corretamente
     * @throws IOException se o HANDSHAKE não for confirmado após
     *                      {@value #MAX_HANDSHAKE_ATTEMPTS} tentativas, ou se
     *                      um ACK de conteúdo inesperado for recebido durante
     *                      esta fase
     */
    private WindowManager performHandshake() throws IOException {
        socketService.setSoTimeoutMillis(timeoutMillis);

        for (int attempt = 1; attempt <= MAX_HANDSHAKE_ATTEMPTS; attempt++) {
            HandshakeSender.sendHandshake(socketService, sessionParameters, receiverAddress, receiverPort);

            try {
                Packet ack = AckReceiver.receiveAck(socketService, Packet.MAX_DATAGRAM_SIZE);

                if (ack.getAckNum() != HANDSHAKE_SEQUENCE_NUMBER) {
                    // Não é um "hack baseado em tipo de pacote" — é uma
                    // verificação de valor numérico contra a única
                    // sequência reservada ao HANDSHAKE. Qualquer outro
                    // valor, nesta fase, é uma violação de protocolo.
                    throw new IllegalStateException(
                            "ACK inesperado durante o HANDSHAKE (esperava ackNum="
                                    + HANDSHAKE_SEQUENCE_NUMBER + "): " + ack);
                }

                return new WindowManager(sessionParameters.getWindowSize(), FIRST_DATA_SEQUENCE_NUMBER);

            } catch (SocketTimeoutException semRespostaAinda) {
                // Sem ACK a tempo: tenta novamente, reenviando o HANDSHAKE.
            }
        }

        throw new IOException(
                "HANDSHAKE não confirmado após " + MAX_HANDSHAKE_ATTEMPTS + " tentativas");
    }

    // -------------------------------------------------------------------------
    // Fase 2: DATA
    // -------------------------------------------------------------------------

    /**
     * Envia todos os blocos de {@code fileChunks}, respeitando a janela
     * deslizante, processando ACKs recebidos e disparando retransmissões
     * quando o timeout expira, até que todos os blocos tenham sido enviados
     * e confirmados.
     */
    private void transmitFileChunks(WindowManager windowManager,
                                    PacketBuffer<Packet> packetBuffer,
                                    TimeoutManager timeoutManager,
                                    SenderStatistics statistics) throws IOException {
        // Timeout de socket curto, usado apenas como intervalo de polling
        // por ACKs — não deve ser confundido com timeoutMillis, que é o
        // timeout de retransmissão do protocolo, gerido por TimeoutManager.
        socketService.setSoTimeoutMillis(Math.max(1, timeoutMillis / 10));

        int nextChunkIndex = 0;

        while (nextChunkIndex < fileChunks.size() || !packetBuffer.isEmpty()) {

            // 1. Envia o quanto a janela permitir, enquanto houver blocos restantes.
            while (nextChunkIndex < fileChunks.size() && windowManager.canSend()) {
                byte[] chunk = fileChunks.get(nextChunkIndex);
                int seq = windowManager.packetSent();

                Packet dataPacket = Packet.createData(seq, chunk, chunk.length);
                packetBuffer.add(seq, dataPacket);

                DataSender.sendData(socketService, seq, chunk, receiverAddress, receiverPort);
                statistics.recordPacketSent();
                statistics.recordBytesSent(chunk.length);

                if (!timeoutManager.isRunning()) {
                    timeoutManager.start();
                }
                nextChunkIndex++;
            }

            // 2. Tenta receber um ACK dentro do intervalo de polling.
            try {
                Packet ack = AckReceiver.receiveAck(socketService, Packet.MAX_DATAGRAM_SIZE);
                statistics.recordAckReceived();

                int ackNum = ack.getAckNum();

                // Um ACK(0) tardio do HANDSHAKE chegando aqui (caso raro de
                // duplicação de rede) é tratado sem nenhuma verificação de
                // tipo: windowManager.processAck(0) calcula newBase=1, que
                // nunca é > base (pois base já começou em 1), então é
                // ignorado como ACK obsoleto pela lógica genérica já
                // existente em WindowManager. Da mesma forma,
                // packetBuffer.removeUpTo(0) nunca remove nada, pois nenhum
                // DATA jamais usou seqNum <= 0.
                windowManager.processAck(ackNum);
                packetBuffer.removeUpTo(ackNum);

                if (packetBuffer.isEmpty()) {
                    timeoutManager.cancel();
                } else {
                    timeoutManager.restart();
                }
            } catch (SocketTimeoutException semAckNesteCiclo) {
                // Nenhum ACK chegou neste ciclo de polling; segue para a
                // checagem de expiração do timeout abaixo.
            }

            // 3. Verifica se o timeout de retransmissão expirou.
            if (timeoutManager.hasExpired()) {
                statistics.recordTimeout();
                RetransmissionManager.retransmitOutstandingPackets(
                        socketService, packetBuffer, timeoutManager, statistics,
                        receiverAddress, receiverPort);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Fase 3: encerramento
    // -------------------------------------------------------------------------

    /**
     * Envia o pacote FIN, usando o próximo número de sequência disponível
     * (continuando a mesma numeração de DATA, nunca reaproveitando a
     * sequência 0 do HANDSHAKE), e aguarda sua confirmação em melhor esforço.
     */
    private void sendFin(WindowManager windowManager, SenderStatistics statistics) throws IOException {
        int finSeq = windowManager.getNextSequenceNumber();
        Packet finPacket = Packet.createFin(finSeq);
        byte[] datagram = PacketCodec.encode(finPacket);

        socketService.send(datagram, receiverAddress, receiverPort);
        statistics.recordPacketSent();

        try {
            Packet ack = AckReceiver.receiveAck(socketService, Packet.MAX_DATAGRAM_SIZE);
            if (ack.getAckNum() == finSeq) {
                statistics.recordAckReceived();
            }
        } catch (SocketTimeoutException semConfirmacaoDoFin) {
            // Melhor esforço: esta proposta não retransmite o FIN.
        }
    }
}
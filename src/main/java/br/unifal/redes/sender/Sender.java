package br.unifal.redes.sender;

import br.unifal.redes.common.SessionParameters;
import br.unifal.redes.sender.protocol.SenderFSM;
import br.unifal.redes.sender.io.FileChunkReader;
import br.unifal.redes.sender.network.SenderSocketService;
import br.unifal.redes.sender.statistics.SenderStatistics;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class Sender {

    // -------------------------------------------------------------------------
    // Constantes de configuração
    // -------------------------------------------------------------------------

    private static final int EXPECTED_ARG_COUNT = 4;

    private static final int CHUNK_SIZE = 1024;

    private static final int RECEIVER_PORT = 5000;

    private static final int RETRANSMISSION_TIMEOUT_MILLIS = 500;

    private Sender() {}

    // -------------------------------------------------------------------------
    // Ponto de entrada
    // -------------------------------------------------------------------------

    public static void main(String[] args) {

        // 1. Valida o número de argumentos — qualquer desvio exibe o uso correto.
        if (args.length != EXPECTED_ARG_COUNT) {
            exibirUsoESair();
        }

        // 2. Parse e validação de cada argumento.
        Path   arquivoOrigem   = parsearArquivoOrigem(args[0]);
        String destinoCompleto = args[1];          // formato: IP:path
        int    windowSize      = parsearWindowSize(args[2]);
        double lossProb        = parsearLossProb(args[3]);

        // 3. Decompõe o argumento composto "IP:path" nos seus dois componentes.
        //    Usa split com limite 2 para preservar ":" dentro do path de destino
        //    (ex: /tmp/arquivo:com:dois:pontos — improvável mas defensivo).
        String[] partes = destinoCompleto.split(":", 2);
        if (partes.length != 2 || partes[0].isBlank() || partes[1].isBlank()) {
            System.err.printf(
                    "[Emissor] Formato inválido para destino: '%s'%n"
                            + "  Use: IP:path — ex: 192.168.0.10:/tmp/saida.txt%n",
                    destinoCompleto
            );
            System.exit(1);
        }

        InetAddress enderecoReceptor = parsearEndereco(partes[0]);
        String      pathDestino      = partes[1];

        // 4. Exibe o resumo do que vai ser enviado antes de qualquer I/O de rede.
        System.out.printf("%n[Emissor] Arquivo de origem : %s%n", arquivoOrigem.toAbsolutePath());
        System.out.printf("[Emissor] Destino           : %s:%d -> %s%n",                enderecoReceptor.getHostAddress(), RECEIVER_PORT, pathDestino);
        System.out.printf("[Emissor] Tamanho da janela : %d%n", windowSize);
        System.out.printf("[Emissor] Prob. de perda    : %.2f%%%n", lossProb * 100);
        System.out.printf("[Emissor] Timeout           : %d ms%n%n", RETRANSMISSION_TIMEOUT_MILLIS);

        // 5. Executa a transmissão dentro de um try-with-resources que garante
        //    o fechamento do socket mesmo em caso de exceção.
        try (SenderSocketService socketService = new SenderSocketService()) {

            // 5a. Lê o arquivo inteiro em chunks de CHUNK_SIZE bytes.
            //     FileChunkReader já valida existência, tipo e permissão de leitura.
            FileChunkReader leitor = new FileChunkReader(CHUNK_SIZE);
            List<byte[]> chunks = leitor.readChunks(arquivoOrigem);

            if (chunks.isEmpty()) {
                System.err.println("[Emissor] Arquivo de origem está vazio. Nada a transmitir.");
                System.exit(1);
            }

            long tamanhoArquivo = Files.size(arquivoOrigem);

            System.out.printf("[Emissor] Arquivo lido: %,d bytes em %,d bloco(s) de %d bytes%n%n",
                    tamanhoArquivo, chunks.size(), CHUNK_SIZE);

            // 5b. Cria os parâmetros da sessão que serão enviados no HANDSHAKE.
            SessionParameters parametrosSessao =
                    new SessionParameters(tamanhoArquivo, windowSize, lossProb, pathDestino);

            // 5c. Abre o socket UDP em porta efêmera (escolhida pelo SO).
            socketService.open();

            System.out.printf("[Emissor] Socket aberto na porta local %d.%n",
                    socketService.getLocalPort());
            System.out.printf("[Emissor] Aguardando Receptor em %s:%d ...%n%n",
                    enderecoReceptor.getHostAddress(), RECEIVER_PORT);

            // 5d. Cria a FSM e inicia a transmissão.
            //     Nota sobre R6 (progresso em tempo real): SenderFSM.run() é
            //     bloqueante e não expõe acesso à SenderStatistics durante a
            //     execução. O progresso só pode ser exibido após run() retornar.
            //     Para progresso em tempo real, seria necessário adicionar um
            //     callback ou acesso à estatística ao SenderFSM — o que está
            //     fora do escopo desta classe.
            long inicioMs = System.currentTimeMillis();

            SenderFSM fsm = new SenderFSM(
                    socketService,
                    parametrosSessao,
                    enderecoReceptor,
                    RECEIVER_PORT,
                    chunks,
                    RETRANSMISSION_TIMEOUT_MILLIS
            );

            SenderStatistics.Snapshot snapshot = fsm.run();

            long duracaoMs = System.currentTimeMillis() - inicioMs;

            // 5e. Exibe as estatísticas finais.
            exibirEstatisticas(snapshot, tamanhoArquivo, duracaoMs);

        } catch (IOException e) {
            System.err.printf("%n[Emissor] Erro fatal durante a transmissão: %s%n", e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Exibição de estatísticas
    // -------------------------------------------------------------------------

    private static void exibirEstatisticas(SenderStatistics.Snapshot snapshot,
                                           long tamanhoArquivo,
                                           long duracaoMs) {
        // Throughput em KB/s: bytes únicos do arquivo ÷ tempo total.
        // Usamos tamanhoArquivo (não totalBytesSent) para refletir a taxa
        // de transferência efetiva do ponto de vista da aplicação — quanto
        // do arquivo original foi entregue por segundo.
        double duracaoSegundos = duracaoMs / 1000.0;
        double throughputKbps  = duracaoSegundos > 0
                ? (tamanhoArquivo / 1024.0) / duracaoSegundos
                : 0.0;

        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║              ESTATÍSTICAS DA TRANSMISSÃO                    ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  Pacotes enviados (total):   %-28d  ║%n",
                snapshot.getTotalPacketsSent());
        System.out.printf( "║  Pacotes retransmitidos:     %-28d  ║%n",
                snapshot.getTotalRetransmitted());
        System.out.printf( "║  ACKs recebidos:             %-28d  ║%n",
                snapshot.getTotalAcksReceived());
        System.out.printf( "║  Timeouts de retransmissão:  %-28d  ║%n",
                snapshot.getTotalTimeouts());
        System.out.printf( "║  Bytes enviados (na rede):   %-28s  ║%n",
                String.format("%,d", snapshot.getTotalBytesSent()));
        System.out.printf( "║  Taxa de retransmissão:      %-28s  ║%n",
                String.format("%.2f%%", snapshot.retransmissionRate() * 100));
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  Duração total:              %-28s  ║%n",
                formatarDuracao(duracaoMs));
        System.out.printf( "║  Throughput efetivo:         %-28s  ║%n",
                String.format("%.2f KB/s", throughputKbps));
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
        System.out.println("[Emissor] Transmissão concluída com sucesso.");
    }

    // -------------------------------------------------------------------------
    // Helpers de parse e validação
    // -------------------------------------------------------------------------

    private static Path parsearArquivoOrigem(String raw) {
        Path p = Path.of(raw);
        if (!Files.exists(p)) {
            System.err.printf("[Emissor] Arquivo não encontrado: '%s'%n", raw);
            System.exit(1);
        }
        if (!Files.isRegularFile(p)) {
            System.err.printf("[Emissor] O caminho não é um arquivo regular: '%s'%n", raw);
            System.exit(1);
        }
        if (!Files.isReadable(p)) {
            System.err.printf("[Emissor] Sem permissão de leitura no arquivo: '%s'%n", raw);
            System.exit(1);
        }
        return p;
    }

    private static InetAddress parsearEndereco(String raw) {
        try {
            return InetAddress.getByName(raw);
        } catch (UnknownHostException e) {
            System.err.printf(
                    "[Emissor] Endereço inválido ou não resolvível: '%s'%n"
                            + "  Verifique o IP ou hostname.%n", raw);
            System.exit(1);
            return null; // nunca alcançado
        }
    }

    private static int parsearWindowSize(String raw) {
        try {
            int value = Integer.parseInt(raw);
            if (value <= 0) {
                System.err.printf(
                        "[Emissor] O tamanho da janela deve ser > 0, recebido: %d%n", value);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[Emissor] Tamanho da janela inválido (deve ser inteiro): '%s'%n", raw);
            System.exit(1);
            return -1; // nunca alcançado
        }
    }

    private static double parsearLossProb(String raw) {
        try {
            // Aceita tanto ponto quanto vírgula como separador decimal,
            // tornando o programa robusto a diferentes locales do sistema.
            double value = Double.parseDouble(raw.replace(',', '.'));
            if (value < 0.0 || value >= 1.0) {
                System.err.printf(
                        "[Emissor] Probabilidade de perda deve estar em [0.0, 1.0), "
                                + "recebido: %.4f%n", value);
                System.exit(1);
            }
            return value;
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[Emissor] Probabilidade de perda inválida (deve ser número real): '%s'%n",
                    raw);
            System.exit(1);
            return -1.0; // nunca alcançado
        }
    }

    // -------------------------------------------------------------------------
    // Utilitários de formatação
    // -------------------------------------------------------------------------

    private static String formatarDuracao(long ms) {
        if (ms >= 60_000) {
            long minutos  = ms / 60_000;
            long segundos = (ms % 60_000) / 1000;
            return String.format("%dm %02ds", minutos, segundos);
        }
        return String.format("%.3fs", ms / 1000.0);
    }

    // -------------------------------------------------------------------------
    // Exibição de uso
    // -------------------------------------------------------------------------

    private static void exibirUsoESair() {
        System.err.println();
        System.err.println("╔═══════════════════════════════════════════════════════════════════╗");
        System.err.println("║                    EMISSOR  Go-Back-N                            ║");
        System.err.println("╠═══════════════════════════════════════════════════════════════════╣");
        System.err.println("║                                                                   ║");
        System.err.println("║  Uso:                                                             ║");
        System.err.println("║    java Emissor <arquivo> <IP:path> <janela> <perda>              ║");
        System.err.println("║                                                                   ║");
        System.err.println("║  Exemplo:                                                         ║");
        System.err.println("║    java Emissor /home/foto.jpg 192.168.0.10:/tmp/foto.jpg 8 0.10 ║");
        System.err.println("║                                                                   ║");
        System.err.println("║  Argumentos:                                                      ║");
        System.err.println("║    <arquivo>  Caminho do arquivo a enviar                         ║");
        System.err.println("║    <IP:path>  Endereço do Receptor e caminho de destino           ║");
        System.err.println("║    <janela>   Tamanho da janela GBN (inteiro > 0)                ║");
        System.err.println("║    <perda>    Probabilidade de perda simulada [0.0, 1.0)          ║");
        System.err.println("║                                                                   ║");
        System.err.println("║  Notas:                                                           ║");
        System.err.println("║    - O Receptor deve estar rodando na porta 5000 (fixa)           ║");
        System.err.println("║    - Timeout de retransmissão: 500 ms                            ║");
        System.err.println("║    - Inicie sempre o Receptor antes do Emissor                    ║");
        System.err.println("║                                                                   ║");
        System.err.println("╚═══════════════════════════════════════════════════════════════════╝");
        System.err.println();
        System.exit(1);
    }
}
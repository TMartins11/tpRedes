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

/**
 * Ponto de entrada do módulo Emissor Go-Back-N.
 *
 * <p>Conforme especificação do trabalho (Seção 3.3), a linha de comando é:
 * <pre>
 *   java Emissor &lt;arquivo_origem&gt; &lt;IP_destino&gt;:&lt;path_destino&gt; &lt;tamanho_janela&gt; &lt;prob_perda&gt;
 *
 *   Exemplo:
 *   java Emissor /home/alice/foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10
 * </pre>
 *
 * <p>Esta classe é exclusivamente bootstrap: valida os 4 argumentos obrigatórios,
 * lê o arquivo de origem em blocos de {@value #CHUNK_SIZE} bytes, abre o socket
 * UDP e delega toda a lógica Go-Back-N para {@link SenderFSM}.
 *
 * <p><strong>Sobre progresso em tempo real (R6):</strong> {@link SenderFSM#run()}
 * é um método bloqueante que só devolve controle ao terminar a transmissão
 * completa. Como a FSM não expõe callback ou acesso à estatística durante a
 * execução, o progresso em tempo real não pode ser implementado nesta camada
 * sem modificar a assinatura da FSM. O que é exibido aqui são as estatísticas
 * finais consolidadas após {@code run()} retornar.
 *
 * <p><strong>Porta do Receptor:</strong> fixa em {@value #RECEIVER_PORT},
 * conforme Seção 3.2 do enunciado — não é configurável via linha de comando.
 *
 * <p><strong>Timeout de retransmissão:</strong> fixo em
 * {@value #RETRANSMISSION_TIMEOUT_MILLIS} ms, valor razoável para testes em LAN.
 */
public final class Sender {

    // -------------------------------------------------------------------------
    // Constantes de configuração
    // -------------------------------------------------------------------------

    /** Número exato de argumentos de linha de comando exigidos pelo enunciado. */
    private static final int EXPECTED_ARG_COUNT = 4;

    /**
     * Tamanho do payload de cada segmento DATA em bytes.
     * Deve ser igual a {@code Packet.MAX_PAYLOAD_SIZE} (1024), conforme Seção 3.4.
     */
    private static final int CHUNK_SIZE = 1024;

    /**
     * Porta UDP fixa do Receptor, conforme Seção 3.2 do enunciado.
     * Não é configurável via linha de comando.
     */
    private static final int RECEIVER_PORT = 5000;

    /**
     * Timeout de retransmissão em milissegundos.
     * Valor de 3 segundos é adequado para LAN e conservador o suficiente
     * para cobrir eventuais picos de latência durante os testes.
     */
    private static final int RETRANSMISSION_TIMEOUT_MILLIS = 500;

    /** Classe utilitária — não instanciável. */
    private Sender() {}

    // -------------------------------------------------------------------------
    // Ponto de entrada
    // -------------------------------------------------------------------------

    /**
     * Método principal do Emissor.
     *
     * <p>Fluxo de execução:
     * <ol>
     *   <li>Valida que exatamente 4 argumentos foram fornecidos.</li>
     *   <li>Faz o parse e a validação de cada argumento.</li>
     *   <li>Lê o arquivo de origem em chunks de {@value #CHUNK_SIZE} bytes.</li>
     *   <li>Cria os {@link SessionParameters} da sessão.</li>
     *   <li>Abre o socket UDP em porta efêmera.</li>
     *   <li>Cria e executa a {@link SenderFSM}.</li>
     *   <li>Exibe as estatísticas finais.</li>
     * </ol>
     *
     * @param args argumentos de linha de comando:
     *             {@code args[0]} = caminho do arquivo de origem,
     *             {@code args[1]} = {@code IP:path} do destino,
     *             {@code args[2]} = tamanho da janela (N &gt; 0),
     *             {@code args[3]} = probabilidade de perda [0.0, 1.0)
     */
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

    /**
     * Exibe o relatório final de estatísticas da transmissão.
     *
     * <p>O throughput é calculado aqui, a partir de {@code totalBytesSent}
     * (bytes de payload efetivamente colocados na rede, incluindo retransmissões)
     * e do tempo de parede medido no {@code main()} — já que {@link SenderStatistics}
     * não armazena timestamps nem calcula throughput internamente.
     *
     * @param snapshot      retrato final das estatísticas produzido por
     *                       {@link SenderFSM#run()}
     * @param tamanhoArquivo tamanho do arquivo original em bytes (bytes únicos,
     *                       sem contar retransmissões), usado para mostrar
     *                       o throughput efetivo do ponto de vista da aplicação
     * @param duracaoMs      tempo total decorrido desde a abertura do socket
     *                       até o retorno de {@code run()}, em milissegundos
     */
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

    /**
     * Valida e retorna o {@link Path} do arquivo de origem.
     *
     * <p>Encapsula todas as verificações de existência, tipo e permissão de
     * leitura numa mensagem de erro clara antes de qualquer outra operação.
     *
     * @param raw o argumento bruto da linha de comando
     * @return o {@link Path} validado e pronto para uso
     */
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

    /**
     * Faz o parse do endereço IP/hostname do Receptor.
     *
     * @param raw o endereço IP ou hostname bruto
     * @return o {@link InetAddress} resolvido
     */
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

    /**
     * Faz o parse e valida o tamanho da janela.
     *
     * @param raw o argumento bruto da linha de comando
     * @return o windowSize validado ({@code > 0})
     */
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

    /**
     * Faz o parse e valida a probabilidade de perda.
     *
     * @param raw o argumento bruto da linha de comando
     * @return a probabilidade validada no intervalo {@code [0.0, 1.0)}
     */
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

    /**
     * Formata uma duração em milissegundos na forma {@code Xm Ys} ou {@code Y.ZZZs}.
     *
     * @param ms duração em milissegundos
     * @return string legível com a duração
     */
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

    /**
     * Exibe as instruções de uso e encerra o processo com código 1.
     */
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
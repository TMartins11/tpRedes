package br.unifal.redes.receiver;

import br.unifal.redes.receiver.fsm.GbnReceiverFsm;
import br.unifal.redes.receiver.io.FileWriterService;
import br.unifal.redes.receiver.network.AckSender;
import br.unifal.redes.receiver.network.PacketReceiver;
import br.unifal.redes.receiver.statistics.ReceiverStatistics;

import java.io.IOException;
import java.net.DatagramSocket;

/**
 * Ponto de entrada do módulo Receptor.
 *
 * <p>Responsabilidades desta classe:
 * <ul>
 *   <li>Validar o argumento de linha de comando.</li>
 *   <li>Abrir o {@link DatagramSocket} na porta configurada.</li>
 *   <li>Instanciar {@link PacketReceiver}, {@link AckSender},
 *       {@link FileWriterService} e {@link ReceiverStatistics}.</li>
 *   <li>Instanciar e executar a {@link GbnReceiverFsm}, que concentra
 *       toda a lógica do protocolo Go-Back-N.</li>
 *   <li>Exibir as estatísticas finais retornadas pela FSM.</li>
 * </ul>
 *
 * <p>Não há nenhuma lógica de protocolo nesta classe — apenas bootstrap.
 *
 * <h2>Uso</h2>
 * <pre>
 *   java Receiver &lt;porta&gt;
 *
 *   Exemplo:
 *   java Receiver 5000
 * </pre>
 */
public final class Receiver {

    private static final int EXPECTED_ARG_COUNT = 1;
    private static final int MIN_PORT           = 1024;
    private static final int MAX_PORT           = 65535;

    private Receiver() {}

    public static void main(String[] args) {
        if (args.length != EXPECTED_ARG_COUNT) {
            printUsageAndExit();
        }

        int port = parsePort(args[0]);

        System.out.printf("[Receptor] Iniciando na porta %d.%n", port);

        try (DatagramSocket socket = new DatagramSocket(port)) {

            PacketReceiver    packetReceiver = new PacketReceiver(socket);
            AckSender         ackSender      = new AckSender(socket);
            FileWriterService fileWriter     = new FileWriterService();
            ReceiverStatistics estatisticas  = new ReceiverStatistics();

            System.out.println("[Receptor] Infraestrutura pronta. Aguardando conexao do Emissor.");

            // Executa o protocolo Go-Back-N completo.
            // A FSM bloqueia até receber o FIN ou até ocorrer erro fatal,
            // e retorna o snapshot das estatísticas acumuladas durante a sessão.
            GbnReceiverFsm fsm = new GbnReceiverFsm();
            ReceiverStatistics.Snapshot snapshot =
                    fsm.executar(packetReceiver, ackSender, fileWriter, estatisticas);

            System.out.println("[Receptor] Transferencia concluida.");

            // Exibe o relatório final de estatísticas exigido pelo enunciado
            // (Seção 3.2): total de pacotes recebidos, descartados e taxa de perda.
            exibirEstatisticas(snapshot);

        } catch (IOException e) {
            System.err.printf(
                    "[Receptor] Erro fatal: %s%n",
                    e.getMessage()
            );
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Exibição de estatísticas
    // -------------------------------------------------------------------------

    /**
     * Exibe o relatório final de estatísticas da recepção, conforme exigido
     * pela Seção 3.2 do enunciado:
     * <ul>
     *   <li>Total de pacotes DATA recebidos (antes de qualquer filtragem).</li>
     *   <li>Total de pacotes descartados (fora de ordem + simulados como perdidos).</li>
     *   <li>Total de pacotes aceitos e gravados em disco.</li>
     *   <li>Total de ACKs enviados de volta ao Emissor.</li>
     *   <li>Taxa de perda efetiva (descartados / recebidos).</li>
     * </ul>
     *
     * @param snapshot retrato imutável das estatísticas retornado pela FSM
     */
    private static void exibirEstatisticas(ReceiverStatistics.Snapshot snapshot) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════╗");
        System.out.println("║              ESTATISTICAS DA RECEPCAO                      ║");
        System.out.println("╠══════════════════════════════════════════════════════════════╣");
        System.out.printf( "║  Pacotes recebidos (total):  %-28d  ║%n",
                snapshot.getTotalReceived());
        System.out.printf( "║  Pacotes aceitos:            %-28d  ║%n",
                snapshot.getTotalAccepted());
        System.out.printf( "║  Pacotes descartados:        %-28d  ║%n",
                snapshot.getTotalDiscarded());
        System.out.printf( "║  ACKs enviados:              %-28d  ║%n",
                snapshot.getTotalAcksSent());
        System.out.printf( "║  Taxa de perda efetiva:      %-28s  ║%n",
                String.format("%.2f%%", snapshot.effectiveLossRate() * 100));
        System.out.println("╚══════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    // -------------------------------------------------------------------------
    // Helpers de validação
    // -------------------------------------------------------------------------

    private static int parsePort(String raw) {
        int port;
        try {
            port = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            System.err.printf("[Receptor] Porta nao e um inteiro valido: '%s'%n", raw);
            System.exit(1);
            return -1;
        }

        if (port < MIN_PORT || port > MAX_PORT) {
            System.err.printf(
                    "[Receptor] Porta fora do intervalo permitido [%d, %d]: %d%n",
                    MIN_PORT, MAX_PORT, port
            );
            System.exit(1);
        }

        return port;
    }

    private static void printUsageAndExit() {
        System.err.println("Uso:     java Receiver <porta>");
        System.err.println("Exemplo: java Receiver 5000");
        System.exit(1);
    }
}
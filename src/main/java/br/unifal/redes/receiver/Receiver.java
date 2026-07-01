package br.unifal.redes.receiver;

import br.unifal.redes.receiver.fsm.GbnReceiverFsm;
import br.unifal.redes.receiver.io.FileWriterService;
import br.unifal.redes.receiver.network.AckSender;
import br.unifal.redes.receiver.network.PacketReceiver;

import java.io.IOException;
import java.net.DatagramSocket;

/**
 * Ponto de entrada do módulo Receptor.
 *
 * <p>Responsabilidades desta classe:
 * <ul>
 *   <li>Validar o argumento de linha de comando.</li>
 *   <li>Abrir o {@link DatagramSocket} na porta configurada.</li>
 *   <li>Instanciar {@link PacketReceiver}, {@link AckSender} e
 *       {@link FileWriterService}.</li>
 *   <li>Instanciar e executar a {@link GbnReceiverFsm}, que concentra
 *       toda a lógica do protocolo Go-Back-N.</li>
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

            PacketReceiver   packetReceiver = new PacketReceiver(socket);
            AckSender        ackSender      = new AckSender(socket);
            FileWriterService fileWriter    = new FileWriterService();

            System.out.println("[Receptor] Infraestrutura pronta. Aguardando conexão do Emissor.");

            // >>> INTEGRAÇÃO P2: executa o protocolo Go-Back-N completo.
            // A FSM bloqueia até receber o FIN ou até ocorrer erro fatal.
            // O socket permanece aberto durante toda a execução graças ao
            // try-with-resources acima — nenhuma alteração necessária aqui.
            GbnReceiverFsm fsm = new GbnReceiverFsm();
            fsm.executar(packetReceiver, ackSender, fileWriter);

            System.out.println("[Receptor] Transferência concluída. Encerrando.");

        } catch (IOException e) {
            System.err.printf(
                    "[Receptor] Erro fatal ao abrir socket na porta %d: %s%n",
                    port, e.getMessage()
            );
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers de validação
    // -------------------------------------------------------------------------

    private static int parsePort(String raw) {
        int port;
        try {
            port = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            System.err.printf("[Receptor] Porta não é um inteiro válido: '%s'%n", raw);
            System.exit(1);
            return -1; // nunca alcançado — satisfaz o compilador
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
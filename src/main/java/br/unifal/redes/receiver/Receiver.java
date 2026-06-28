package br.unifal.redes.receiver;

import br.unifal.redes.receiver.network.AckSender;
import br.unifal.redes.receiver.network.PacketReceiver;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.concurrent.CountDownLatch;

/**
 * Ponto de entrada do módulo Receptor.
 *
 * <p>Responsabilidades desta classe:
 * <ul>
 *   <li>Validar o argumento de linha de comando.</li>
 *   <li>Abrir o {@link DatagramSocket} na porta configurada.</li>
 *   <li>Instanciar {@link PacketReceiver} e {@link AckSender}.</li>
 *   <li>Deixar a infraestrutura pronta para que a FSM (P2) seja integrada
 *       sem reescrever esta classe.</li>
 * </ul>
 *
 * <p>Não há nenhuma lógica de protocolo aqui. Toda a FSM do Go-Back-N
 * será implementada no P2 e orquestrada a partir deste ponto de entrada.
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

            PacketReceiver packetReceiver = new PacketReceiver(socket);
            AckSender      ackSender      = new AckSender(socket);

            System.out.println("[Receptor] Infraestrutura pronta. Aguardando conexão do Emissor.");

            /*
             * >>> PONTO DE INTEGRAÇÃO DO P2 <<<
             *
             * Substituir o await() abaixo por:
             *
             *   GbnReceiverFsm fsm = new GbnReceiverFsm(packetReceiver, ackSender);
             *   fsm.run();
             *
             * O socket permanece aberto durante toda a execução do try-with-resources,
             * portanto a FSM terá acesso ao canal UDP pelo tempo que precisar.
             * Esta classe não precisará ser modificada.
             */
            new CountDownLatch(1).await();

        } catch (IOException e) {
            System.err.printf(
                    "[Receptor] Erro fatal ao abrir socket na porta %d: %s%n",
                    port, e.getMessage()
            );
            System.exit(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("[Receptor] Processo interrompido. Encerrando.");
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
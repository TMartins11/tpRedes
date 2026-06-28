package br.unifal.redes.sender;

import br.unifal.redes.common.Packet;
import br.unifal.redes.common.PacketSerializer;
import br.unifal.redes.common.SessionParameters;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Ponto de entrada do módulo Emissor.
 *
 * <p>No P1, o Emissor realiza tudo o que é possível fazer antes da FSM:
 * <ol>
 *   <li>Valida e interpreta os quatro argumentos de linha de comando.</li>
 *   <li>Valida que o arquivo de origem existe e é legível.</li>
 *   <li>Resolve o {@link InetAddress} do Receptor.</li>
 *   <li>Constrói {@link SessionParameters} com os parâmetros da sessão.</li>
 *   <li>Serializa os parâmetros e envia o pacote {@code HANDSHAKE}.</li>
 * </ol>
 *
 * <p>Enviar o HANDSHAKE <em>não</em> é lógica de FSM — é um único
 * {@code socket.send()}, exatamente como qualquer outro datagrama.
 * Adiá-lo para o P2 significaria reescrever esta classe sem motivo.
 *
 * <p>FSM, janela deslizante, timeout e retransmissão pertencem ao P2
 * e serão adicionados sem alterar esta classe.
 *
 * <h2>Uso</h2>
 * <pre>
 *   java Emissor &lt;arquivo_origem&gt; &lt;IP_destino&gt;:&lt;path_destino&gt; &lt;tamanho_janela&gt; &lt;prob_perda&gt;
 *
 *   Exemplo:
 *   java Emissor /home/alice/foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10
 * </pre>
 *
 * <h2>Contrato com o Receptor</h2>
 * O primeiro datagrama enviado ao Receptor DEVE ser do tipo {@code HANDSHAKE}.
 * O Receptor bloqueia aguardando esse pacote antes de aceitar qualquer {@code DATA}.
 * Essa invariante é estabelecida aqui e deve ser preservada no P2.
 */
public final class Sender {

    private static final int EXPECTED_ARG_COUNT = 4;

    /**
     * Porta padrão utilizada pelo Receptor neste projeto.
     *
     * <p>O enunciado do P1 não prevê a porta como argumento de linha de comando,
     * portanto ela é fixada aqui como constante. No P2, caso seja necessário
     * parametrizá-la, basta adicionar um quinto argumento opcional e substituir
     * esta constante pela variável correspondente — sem alterar nenhuma outra
     * lógica desta classe.
     */
    private static final int DEFAULT_RECEIVER_PORT = 5000;

    private Sender() {}

    public static void main(String[] args) {
        if (args.length != EXPECTED_ARG_COUNT) {
            printUsageAndExit();
        }

        File        sourceFile       = resolveSourceFile(args[0]);
        String[]    ipAndPath        = parseIpAndPath(args[1]);
        InetAddress receiverAddress  = resolveAddress(ipAndPath[0]);
        String      destPath         = ipAndPath[1];
        int         windowSize       = parseWindowSize(args[2]);
        double      lossProb         = parseLossProb(args[3]);

        SessionParameters params = new SessionParameters(
                sourceFile.length(),
                windowSize,
                lossProb,
                destPath
        );

        System.out.println("[Emissor] Sessão configurada: " + params);

        sendHandshake(params, receiverAddress);

        System.out.println("[Emissor] HANDSHAKE enviado com sucesso.");
        System.out.println("[Emissor] Infraestrutura pronta. Aguardando implementação da FSM (P2).");

        /*
         * >>> PONTO DE INTEGRAÇÃO DO P2 <<<
         *
         * No P2, sendHandshake() receberá o socket aberto no main e a FSM
         * será instanciada logo após o envio do HANDSHAKE:
         *
         *   DatagramSocket socket = new DatagramSocket();          // abrir aqui
         *   sendHandshake(params, receiverAddress, socket);        // reutilizar socket
         *   GbnSenderFsm fsm = new GbnSenderFsm(
         *       sourceFile, params, receiverAddress, DEFAULT_RECEIVER_PORT, socket);
         *   fsm.run();
         *   socket.close();
         *
         * A lógica de validação e construção de SessionParameters
         * permanece intacta — esta classe não precisará ser modificada.
         */
    }

    // -------------------------------------------------------------------------
    // Envio do HANDSHAKE
    // -------------------------------------------------------------------------

    /**
     * Serializa os parâmetros da sessão, cria o pacote HANDSHAKE e o envia.
     *
     * <p>No P1, o socket é criado localmente e fechado após o envio.
     * No P2, este método será refatorado para receber o {@link DatagramSocket}
     * já aberto no {@code main} — o mesmo socket que a FSM usará para enviar
     * os pacotes DATA. Isso evita abrir dois sockets e garante que o Receptor
     * associe todos os datagramas à mesma porta efêmera do Emissor.
     */
    private static void sendHandshake(SessionParameters params, InetAddress address) {
        byte[]  payload   = PacketSerializer.serializeSessionParameters(params);
        Packet  handshake = Packet.createHandshake(payload);
        byte[]  data      = PacketSerializer.serialize(handshake);

        try (DatagramSocket socket = new DatagramSocket()) {
            DatagramPacket datagram = new DatagramPacket(
                    data, data.length, address, DEFAULT_RECEIVER_PORT
            );
            socket.send(datagram);
        } catch (IOException e) {
            System.err.println("[Emissor] Falha ao enviar HANDSHAKE: " + e.getMessage());
            System.exit(1);
        }
    }

    // -------------------------------------------------------------------------
    // Parsing e validação dos argumentos
    // -------------------------------------------------------------------------

    private static File resolveSourceFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            System.err.printf("[Emissor] Arquivo não encontrado: '%s'%n", path);
            System.exit(1);
        }
        if (!file.isFile()) {
            System.err.printf("[Emissor] O caminho não é um arquivo regular: '%s'%n", path);
            System.exit(1);
        }
        if (!file.canRead()) {
            System.err.printf("[Emissor] Sem permissão de leitura: '%s'%n", path);
            System.exit(1);
        }
        return file;
    }

    /**
     * Interpreta o argumento no formato {@code <IP>:<path>}.
     *
     * <p>Utiliza {@code indexOf} em vez de {@code split} para tratar
     * corretamente paths que contenham {@code ':'} (ex.: caminhos Windows).
     * O separador é sempre o <em>primeiro</em> {@code ':'} encontrado.
     *
     * @return array {@code [ip, destPath]}
     */
    private static String[] parseIpAndPath(String arg) {
        int sep = arg.indexOf(':');
        if (sep < 1 || sep == arg.length() - 1) {
            System.err.printf(
                    "[Emissor] Formato inválido — esperado '<IP>:<path>', recebido: '%s'%n", arg
            );
            System.exit(1);
        }
        return new String[] { arg.substring(0, sep), arg.substring(sep + 1) };
    }

    private static InetAddress resolveAddress(String ip) {
        try {
            return InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            System.err.printf("[Emissor] Endereço inválido ou inacessível: '%s'%n", ip);
            System.exit(1);
            return null; // nunca alcançado
        }
    }

    private static int parseWindowSize(String raw) {
        try {
            int n = Integer.parseInt(raw);
            if (n <= 0) {
                System.err.printf(
                        "[Emissor] Tamanho da janela deve ser > 0, recebido: %d%n", n
                );
                System.exit(1);
            }
            return n;
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[Emissor] Tamanho da janela não é um inteiro válido: '%s'%n", raw
            );
            System.exit(1);
            return -1; // nunca alcançado
        }
    }

    /**
     * Converte a string de probabilidade para {@code double}.
     *
     * <p>Aceita tanto {@code "0.10"} quanto {@code "0,10"} para compatibilidade
     * com o locale pt-BR, evitando falhas silenciosas no dia da apresentação.
     */
    private static double parseLossProb(String raw) {
        try {
            double p = Double.parseDouble(raw.replace(',', '.'));
            if (p < 0.0 || p >= 1.0) {
                System.err.printf(
                        "[Emissor] prob_perda deve estar em [0.0, 1.0), recebido: %.4f%n", p
                );
                System.exit(1);
            }
            return p;
        } catch (NumberFormatException e) {
            System.err.printf(
                    "[Emissor] prob_perda não é um número válido: '%s'%n", raw
            );
            System.exit(1);
            return -1.0; // nunca alcançado
        }
    }

    private static void printUsageAndExit() {
        System.err.println(
                "Uso:     java Emissor <arquivo_origem> <IP_destino>:<path_destino> " +
                        "<tamanho_janela> <prob_perda>"
        );
        System.err.println(
                "Exemplo: java Emissor /home/alice/foto.jpg " +
                        "192.168.0.10:/tmp/foto_recebida.jpg 8 0.10"
        );
        System.exit(1);
    }
}
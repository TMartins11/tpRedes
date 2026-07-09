package br.unifal.redes.sender.network;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Objects;

public final class SenderSocketService implements AutoCloseable {

    private DatagramSocket socket;
    private boolean opened;

    // -------------------------------------------------------------------------
    // Ciclo de vida
    // -------------------------------------------------------------------------

    /**
     * Abre o socket UDP, vinculando-o a uma porta local efêmera escolhida
     * pelo sistema operacional.
     *
     * @throws IllegalStateException se este serviço já estiver aberto
     * @throws SocketException       se o socket não puder ser criado
     */
    public void open() throws SocketException {
        if (opened) {
            throw new IllegalStateException("SenderSocketService is already open; call close() first");
        }
        this.socket = new DatagramSocket();
        this.opened = true;
    }

    /**
     * Abre o socket UDP, vinculando-o explicitamente a {@code localPort}.
     *
     * @param localPort a porta local a ser usada; deve estar no intervalo
     *                  {@code [0, 65535]} ({@code 0} delega a escolha ao
     *                  sistema operacional)
     * @throws IllegalArgumentException se {@code localPort} estiver fora do intervalo válido
     * @throws IllegalStateException    se este serviço já estiver aberto
     * @throws SocketException          se o socket não puder ser criado ou vinculado
     */
    public void open(int localPort) throws SocketException {
        validatePort(localPort, "localPort");
        if (opened) {
            throw new IllegalStateException("SenderSocketService is already open; call close() first");
        }
        this.socket = new DatagramSocket(localPort);
        this.opened = true;
    }

    /**
     * Fecha o socket UDP, liberando a porta associada.
     *
     * <p>Idempotente — chamar {@code close()} em um serviço já fechado (ou
     * nunca aberto) não tem efeito, para que possa ser usado com segurança
     * em blocos try-with-resources mesmo após uma falha em {@link #open()}.
     */
    @Override
    public void close() {
        if (!opened || socket == null) {
            return; // idempotente — seguro mesmo se nunca foi aberto
        }
        socket.close();
        opened = false;
    }

    // -------------------------------------------------------------------------
    // Envio
    // -------------------------------------------------------------------------

    /**
     * Envia {@code packet} pelo socket UDP.
     *
     * @param packet o datagrama a ser enviado; não pode ser {@code null}
     * @throws NullPointerException  se {@code packet} for {@code null}
     * @throws IllegalStateException se o serviço não estiver aberto
     * @throws IOException           se ocorrer um erro de E/S durante o envio
     */
    public void send(DatagramPacket packet) throws IOException {
        requireOpen();
        Objects.requireNonNull(packet, "packet must not be null");
        socket.send(packet);
    }

    /**
     * Monta e envia um datagrama contendo {@code payload} para
     * {@code address}:{@code port}.
     *
     * <p>Método de conveniência equivalente a construir manualmente um
     * {@link DatagramPacket} e chamar {@link #send(DatagramPacket)}.
     *
     * @param payload os bytes a serem enviados; não pode ser {@code null}
     * @param address o endereço de destino; não pode ser {@code null}
     * @param port    a porta de destino; deve estar no intervalo {@code [1, 65535]}
     * @throws NullPointerException     se {@code payload} ou {@code address} forem {@code null}
     * @throws IllegalArgumentException se {@code port} estiver fora do intervalo válido
     * @throws IllegalStateException    se o serviço não estiver aberto
     * @throws IOException              se ocorrer um erro de E/S durante o envio
     */
    public void send(byte[] payload, InetAddress address, int port) throws IOException {
        requireOpen();
        Objects.requireNonNull(payload, "payload must not be null");
        Objects.requireNonNull(address, "address must not be null");
        validateDestinationPort(port);
        DatagramPacket packet = new DatagramPacket(payload, payload.length, address, port);
        socket.send(packet);
    }

    // -------------------------------------------------------------------------
    // Recebimento
    // -------------------------------------------------------------------------

    /**
     * Bloqueia até que um datagrama seja recebido e preenche {@code packet}
     * com seus dados, endereço e porta de origem.
     *
     * @param packet o datagrama (com seu buffer já alocado) a ser preenchido;
     *               não pode ser {@code null}
     * @throws NullPointerException  se {@code packet} for {@code null}
     * @throws IllegalStateException se o serviço não estiver aberto
     * @throws IOException           se ocorrer um erro de E/S durante o recebimento,
     *                                incluindo {@link java.net.SocketTimeoutException}
     *                                caso um timeout de socket tenha sido configurado
     *                                via {@link #setSoTimeoutMillis(int)} e expire
     */
    public void receive(DatagramPacket packet) throws IOException {
        requireOpen();
        Objects.requireNonNull(packet, "packet must not be null");
        socket.receive(packet);
    }

    /**
     * Bloqueia até que um datagrama seja recebido, alocando internamente um
     * buffer de {@code bufferSize} bytes para recebê-lo.
     *
     * <p>Método de conveniência equivalente a alocar um {@code byte[]}, criar
     * um {@link DatagramPacket} a partir dele, e chamar
     * {@link #receive(DatagramPacket)}.
     *
     * @param bufferSize o tamanho, em bytes, do buffer a ser alocado; deve ser &gt; 0
     * @return o {@link DatagramPacket} recebido, com dados, endereço e porta
     *         de origem preenchidos
     * @throws IllegalArgumentException se {@code bufferSize} não for positivo
     * @throws IllegalStateException    se o serviço não estiver aberto
     * @throws IOException              se ocorrer um erro de E/S durante o recebimento
     */
    public DatagramPacket receive(int bufferSize) throws IOException {
        requireOpen();
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("bufferSize must be > 0, got: " + bufferSize);
        }
        byte[] buffer = new byte[bufferSize];
        DatagramPacket packet = new DatagramPacket(buffer, bufferSize);
        socket.receive(packet);
        return packet;
    }

    // -------------------------------------------------------------------------
    // Configuração
    // -------------------------------------------------------------------------

    /**
     * Configura o timeout de leitura do socket subjacente — o tempo máximo
     * que {@link #receive} pode bloquear antes de lançar
     * {@link java.net.SocketTimeoutException}.
     *
     * <p>Importante: este é apenas o timeout nativo do {@link DatagramSocket}
     * (equivalente a {@code SO_TIMEOUT}), usado para que {@link #receive} não
     * bloqueie indefinidamente. Não tem relação com o timeout de
     * retransmissão do protocolo Go-Back-N, que é responsabilidade exclusiva
     * de {@code TimeoutManager}.
     *
     * @param timeoutMillis o timeout, em milissegundos; {@code 0} desativa o
     *                      timeout (bloqueio indefinido); deve ser &gt;= 0
     * @throws IllegalArgumentException se {@code timeoutMillis} for negativo
     * @throws IllegalStateException    se o serviço não estiver aberto
     * @throws SocketException          se o timeout não puder ser configurado
     */
    public void setSoTimeoutMillis(int timeoutMillis) throws SocketException {
        requireOpen();
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeoutMillis must be >= 0, got: " + timeoutMillis);
        }
        socket.setSoTimeout(timeoutMillis);
    }

    // -------------------------------------------------------------------------
    // Acessores
    // -------------------------------------------------------------------------

    /** @return {@code true} se o socket estiver atualmente aberto */
    public boolean isOpen() {
        return opened;
    }

    /**
     * @return a porta local à qual o socket está vinculado
     * @throws IllegalStateException se o serviço não estiver aberto
     */
    public int getLocalPort() {
        requireOpen();
        return socket.getLocalPort();
    }

    /**
     * @return o endereço local ao qual o socket está vinculado
     * @throws IllegalStateException se o serviço não estiver aberto
     */
    public InetAddress getLocalAddress() {
        requireOpen();
        return socket.getLocalAddress();
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    private void requireOpen() {
        if (!opened || socket == null) {
            throw new IllegalStateException("SenderSocketService is not open; call open() first");
        }
    }

    private static void validatePort(int port, String fieldName) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(fieldName + " must be in [0, 65535], got: " + port);
        }
    }

    private static void validateDestinationPort(int port) {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("port must be in [1, 65535], got: " + port);
        }
    }

    @Override
    public String toString() {
        return "SenderSocketService{"
                + "opened=" + opened
                + (opened ? ", localPort=" + socket.getLocalPort() : "")
                + '}';
    }
}
package br.unifal.redes.sender.network;


public final class WindowManager {

    private final int windowSize;

    /**
     * Número de sequência do pacote mais antigo enviado, mas ainda não reconhecido.
     * Inicializado como 0 conforme a convenção GBN (base = 0).
     */
    private int base;

    /**
     * Número de sequência que será atribuído ao próximo pacote enviado.
     * Inicializado como 0 conforme a convenção GBN (nextseqnum = 0).
     */
    private int next;

    /**
     * Cria um novo gerenciador de janela com o tamanho de janela informado,
     * iniciando os números de sequência base e próximo em {@code 0}.
     *
     * @param windowSize o número máximo de pacotes não confirmados permitidos
     *                   em trânsito simultaneamente; deve ser &gt; 0
     * @throws IllegalArgumentException se {@code windowSize} não for positivo
     */
    public WindowManager(int windowSize) {
        this(windowSize, 0);
    }

    public WindowManager(int windowSize, int initialSequenceNumber) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize deve ser > 0, recebido: " + windowSize);
        }
        if (initialSequenceNumber < 0) {
            throw new IllegalArgumentException(
                    "initialSequenceNumber deve ser >= 0, recebido: " + initialSequenceNumber);
        }
        this.windowSize = windowSize;
        this.base = initialSequenceNumber;
        this.next = initialSequenceNumber;
    }

    // -------------------------------------------------------------------------
    // Consultas da janela
    // -------------------------------------------------------------------------

    /**
     * Indica se a janela atualmente tem espaço para enviar outro pacote.
     *
     * @return {@code true} se {@link #getInFlightCount()} for estritamente menor
     *         que o tamanho de janela configurado
     */
    public boolean canSend() {
        return getInFlightCount() < windowSize;
    }

    /**
     * Indica se a janela está em capacidade total.
     *
     * @return {@code true} se nenhum pacote adicional puder ser enviado sem primeiro
     *         deslizar a janela para frente; equivalente a {@code !canSend()}
     */
    public boolean isFull() {
        return !canSend();
    }

    /**
     * @return o número de pacotes atualmente em trânsito (enviados mas ainda não
     *         reconhecidos), ou seja, {@code next - base}
     */
    public int getInFlightCount() {
        return next - base;
    }

    // -------------------------------------------------------------------------
    // Mutadores voltados para a FSM
    // -------------------------------------------------------------------------

    /**
     * Registra que um pacote foi logicamente enviado, consumindo e retornando o
     * número de sequência atual, e então avançando-o em uma unidade.
     *
     * <p>Os chamadores devem verificar {@link #canSend()} antes de invocar este método;
     * ele não bloqueia nem espera, apenas aplica o invariante de forma defensiva.
     *
     * @return o número de sequência atribuído ao pacote que acabou de ser enviado
     * @throws IllegalStateException se a janela já estiver cheia
     */
    public int packetSent() {
        if (isFull()) {
            throw new IllegalStateException(
                    "Não é possível enviar: janela cheia (emTrânsito=" + getInFlightCount()
                            + ", tamanhoJanela=" + windowSize + ")");
        }
        int assignedSequenceNumber = next;
        next++;
        return assignedSequenceNumber;
    }

    public void processAck(int ackSequenceNumber) {
        if (ackSequenceNumber < 0) {
            throw new IllegalArgumentException(
                    "ackSequenceNumber deve ser >= 0, recebido: " + ackSequenceNumber);
        }
        if (ackSequenceNumber >= next) {
            throw new IllegalArgumentException(
                    "ackSequenceNumber (" + ackSequenceNumber
                            + ") reconhece um número de sequência nunca enviado (next=" + next + ")");
        }

        int newBase = ackSequenceNumber + 1;
        if (newBase <= base) {
            return; // ACK desatualizado ou duplicado — normal em GBN, sem efeito
        }
        base = newBase;
    }

    // -------------------------------------------------------------------------
    // Acessores
    // -------------------------------------------------------------------------

    /** @return o número de sequência base atual (pacote não reconhecido mais antigo) */
    public int getBaseSequenceNumber() {
        return base;
    }

    /** @return o número de sequência que será atribuído ao próximo pacote enviado */
    public int getNextSequenceNumber() {
        return next;
    }

    /** @return o número máximo configurado de pacotes não reconhecidos em trânsito */
    public int getWindowSize() {
        return windowSize;
    }

    @Override
    public String toString() {
        return "WindowManager{"
                + "base=" + base
                + ", next=" + next
                + ", windowSize=" + windowSize
                + ", inFlight=" + getInFlightCount()
                + '}';
    }
}
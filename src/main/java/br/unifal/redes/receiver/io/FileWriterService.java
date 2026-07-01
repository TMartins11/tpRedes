package br.unifal.redes.receiver.io;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Realiza a escrita sequencial e apenas-adição de bytes brutos em um único arquivo de destino.
 *
 * <p>Este serviço não conhece nada sobre o protocolo GBN, pacotes ou números
 * de sequência. Seu contrato é simples:
 * <ol>
 *   <li>Chame {@link #open(String)} uma vez para criar ou truncar o arquivo de destino.</li>
 *   <li>Chame {@link #write(byte[], int, int)} para cada bloco de payload, em ordem.</li>
 *   <li>Chame {@link #close()} exatamente uma vez ao final da transferência.</li>
 * </ol>
 *
 * <p>A garantia de ordenação é responsabilidade do chamador — na prática, a
 * FSM do GBN garante que {@code write()} seja chamado apenas para bytes de
 * payload aceitos e em ordem.
 *
 * <p>Esta classe não é thread-safe. A FSM deve garantir que escrita e fechamento
 * sejam realizados por uma única thread (ou sob sincronização externa).
 */
public final class FileWriterService implements AutoCloseable {

    private static final int BUFFER_SIZE = 8 * 1024; // buffer de escrita do SO: 8 KiB

    private BufferedOutputStream outputStream;
    private Path destinationPath;
    private boolean opened;

    // -------------------------------------------------------------------------
    // Ciclo de vida
    // -------------------------------------------------------------------------

    /**
     * Abre o arquivo de destino para escrita, criando-o (e os diretórios pai
     * ausentes) se necessário, ou truncando-o caso já exista.
     *
     * @param absolutePath caminho absoluto para o arquivo de destino; não pode ser em branco
     * @throws IllegalArgumentException se {@code absolutePath} estiver em branco
     * @throws IllegalStateException    se este serviço já estiver aberto
     * @throws IOException              se o arquivo não puder ser criado ou aberto
     */
    public void open(String absolutePath) throws IOException {
        if (absolutePath == null || absolutePath.isBlank()) {
            throw new IllegalArgumentException("absolutePath must not be blank");
        }
        if (opened) {
            throw new IllegalStateException("FileWriterService is already open; call close() first");
        }

        destinationPath = Paths.get(absolutePath);
        createParentDirectoriesIfAbsent(destinationPath);

        outputStream = new BufferedOutputStream(
                new FileOutputStream(destinationPath.toFile(), /* append= */ false),
                BUFFER_SIZE
        );
        opened = true;
    }

    /**
     * Escreve {@code length} bytes de {@code payload} a partir de {@code offset}
     * no arquivo de destino.
     *
     * <p>Os bytes são escritos na ordem exata em que chegam; o chamador é
     * responsável por garantir que sejam entregues em sequência.
     *
     * @param payload o array de bytes de origem; não pode ser {@code null}
     * @param offset  o deslocamento inicial em {@code payload}; deve ser ≥ 0
     * @param length  o número de bytes a serem escritos; deve ser &gt; 0
     * @throws NullPointerException      se {@code payload} for {@code null}
     * @throws IllegalArgumentException  se {@code offset} ou {@code length} forem inválidos
     * @throws IllegalStateException     se o serviço ainda não tiver sido aberto
     * @throws IOException               se ocorrer um erro de E/S durante a escrita
     */
    public void write(byte[] payload, int offset, int length) throws IOException {
        Objects.requireNonNull(payload, "payload must not be null");
        requireOpen();

        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got: " + length);
        }
        if (offset + length > payload.length) {
            throw new IllegalArgumentException(
                    "offset + length (" + (offset + length) + ") exceeds payload length (" + payload.length + ")");
        }

        outputStream.write(payload, offset, length);
    }

    /**
     * Descarrega os dados em buffer e fecha o arquivo de destino.
     *
     * <p>Após esta chamada, o serviço passa para o estado fechado e não pode
     * ser reutilizado. Implementa {@link AutoCloseable} para que este serviço
     * possa ser usado em blocos try-with-resources.
     *
     * @throws IOException se ocorrer um erro de E/S durante o flush ou o fechamento
     */
    @Override
    public void close() throws IOException {
        if (!opened || outputStream == null) {
            return; // idempotente — seguro chamar mesmo se nunca foi aberto
        }
        try {
            outputStream.flush();
        } finally {
            outputStream.close();
            opened = false;
        }
    }

    // -------------------------------------------------------------------------
    // Acessores
    // -------------------------------------------------------------------------

    /**
     * @return {@code true} se este serviço possuir atualmente um handle de arquivo aberto
     */
    public boolean isOpen() {
        return opened;
    }

    /**
     * @return o {@link Path} para o qual este serviço está escrevendo, ou {@code null} se
     *         {@link #open(String)} nunca tiver sido chamado
     */
    public Path getDestinationPath() {
        return destinationPath;
    }

    // -------------------------------------------------------------------------
    // Auxiliares
    // -------------------------------------------------------------------------

    private void requireOpen() {
        if (!opened) {
            throw new IllegalStateException(
                    "FileWriterService is not open; call open(String) before writing");
        }
    }

    private static void createParentDirectoriesIfAbsent(Path path) throws IOException {
        Path parent = path.getParent();
        if (parent != null && !Files.exists(parent)) {
            Files.createDirectories(parent);
        }
    }
}
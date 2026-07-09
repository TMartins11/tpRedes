package br.unifal.redes.sender.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class FileChunkReader {

    private final int chunkSize;

    /**
     * Cria um novo leitor que divide arquivos em pedaços de {@code chunkSize} bytes.
     *
     * @param chunkSize o tamanho máximo, em bytes, de cada pedaço; deve ser &gt; 0
     * @throws IllegalArgumentException se {@code chunkSize} não for positivo
     */
    public FileChunkReader(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize deve ser > 0, recebido: " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    // -------------------------------------------------------------------------
    // Leitura
    // -------------------------------------------------------------------------

    public List<byte[]> readChunks(Path sourceFile) throws IOException {
        validateSourceFile(sourceFile);

        List<byte[]> chunks = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];

        try (InputStream in = Files.newInputStream(sourceFile)) {
            while (true) {
                int bytesRead = readFully(in, buffer);
                if (bytesRead == 0) {
                    break; // fim do arquivo, nada mais para ler
                }
                chunks.add(bytesRead == chunkSize ? buffer.clone() : Arrays.copyOf(buffer, bytesRead));
                if (bytesRead < chunkSize) {
                    break; // leitura curta significa que o fim do arquivo foi alcançado
                }
            }
        }

        return List.copyOf(chunks);
    }

    // -------------------------------------------------------------------------
    // Acessores
    // -------------------------------------------------------------------------

    /** @return o tamanho do pedaço configurado, em bytes */
    public int getChunkSize() {
        return chunkSize;
    }

    // -------------------------------------------------------------------------
    // Métodos auxiliares
    // -------------------------------------------------------------------------

    private static void validateSourceFile(Path sourceFile) {
        Objects.requireNonNull(sourceFile, "sourceFile não deve ser nulo");
        if (!Files.exists(sourceFile)) {
            throw new IllegalArgumentException("sourceFile não existe: " + sourceFile);
        }
        if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("sourceFile não é um arquivo regular: " + sourceFile);
        }
        if (!Files.isReadable(sourceFile)) {
            throw new IllegalArgumentException("sourceFile não é legível: " + sourceFile);
        }
    }

    /**
     * Lê de {@code in} até que {@code buffer} esteja completamente preenchido ou o
     * final do fluxo seja alcançado, o que ocorrer primeiro.
     *
     * <p>Isso é necessário porque uma única chamada {@link InputStream#read(byte[])}
     * não garante preencher o buffer mesmo quando mais bytes permanecem
     * disponíveis, portanto os limites dos pedaços devem ser montados de forma defensiva.
     *
     * @param in     o fluxo do qual ler
     * @param buffer o buffer a ser preenchido
     * @return o número de bytes efetivamente lidos em {@code buffer} (pode ser
     *         menor que {@code buffer.length} apenas no final do arquivo)
     */
    private static int readFully(InputStream in, byte[] buffer) throws IOException {
        int totalRead = 0;
        while (totalRead < buffer.length) {
            int read = in.read(buffer, totalRead, buffer.length - totalRead);
            if (read == -1) {
                break;
            }
            totalRead += read;
        }
        return totalRead;
    }
}
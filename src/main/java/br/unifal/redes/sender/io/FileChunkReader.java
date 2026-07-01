package br.unifal.redes.sender.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Lê um arquivo fonte do disco e divide seu conteúdo em pedaços (chunks) de bytes
 * de tamanho fixo e ordenados.
 *
 * <p>Esta classe não sabe nada sobre o protocolo GBN, pacotes, números
 * de sequência, soquetes ou qualquer forma de E/S de rede. Seu contrato é simples:
 * <pre>
 *   Arquivo -&gt; Bytes -&gt; Pedaços (Chunks)
 * </pre>
 *
 * <p>Os pedaços são retornados na ordem exata em que aparecem no arquivo fonte.
 * Cada pedaço tem um comprimento igual ao tamanho de pedaço configurado, exceto
 * possivelmente o último, que pode ser mais curto se o tamanho do arquivo não for
 * um múltiplo exato do tamanho do pedaço. Interpretar, numerar ou transmitir
 * esses pedaços é responsabilidade de componentes de nível superior (por exemplo, a
 * FSM do Transmissor).
 *
 * <p>Esta classe não possui estado, além do tamanho imutável do pedaço, e pode ser
 * reutilizada para ler múltiplos arquivos sequencialmente. Não é thread-safe para
 * leituras concorrentes da mesma instância, embora leituras concorrentes de
 * diferentes arquivos a partir de threads separadas sejam seguras, pois nenhum
 * estado mutável compartilhado está envolvido.
 */
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

    /**
     * Lê {@code sourceFile} por completo e o divide em pedaços ordenados de no
     * máximo {@link #getChunkSize()} bytes cada.
     *
     * <p>A lista retornada preserva a ordem original dos bytes do arquivo: o
     * primeiro pedaço contém os primeiros {@code chunkSize} bytes, o segundo pedaço
     * os próximos {@code chunkSize} bytes, e assim por diante. Apenas o pedaço final
     * pode ser mais curto que {@code chunkSize}, se o tamanho do arquivo não for um
     * múltiplo exato dele. Se o arquivo estiver vazio, uma lista vazia é retornada.
     *
     * @param sourceFile caminho para o arquivo a ser lido; não deve ser {@code null}
     * @return uma lista imutável e ordenada de pedaços de bytes
     * @throws NullPointerException     se {@code sourceFile} for {@code null}
     * @throws IllegalArgumentException se {@code sourceFile} não existir,
     *                                   não for um arquivo regular ou não for legível
     * @throws IOException              se ocorrer um erro de E/S durante a leitura
     */
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
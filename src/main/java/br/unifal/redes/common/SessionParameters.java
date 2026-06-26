package br.unifal.redes.common;

/**
 * Parâmetros da sessão GBN, transmitidos no pacote HANDSHAKE.
 *
 * <p>O Emissor popula este objeto com os argumentos de linha de comando
 * e o serializa no payload do HANDSHAKE. O Receptor o deserializa para
 * configurar a sessão antes de começar a receber dados.
 *
 * <p>Campos serializados (tamanho fixo = 21 bytes):
 * <pre>
 *   Offset  Tamanho  Campo
 *   0       8 bytes  fileSize      (long)
 *   8       4 bytes  windowSize    (int)
 *   12      8 bytes  lossProb      (double)
 *   20      1 byte   destPathLen   (byte — comprimento do path)
 *   21      N bytes  destPath      (UTF-8, N ≤ 255)
 * </pre>
 */
public final class SessionParameters {

    /** Comprimento máximo do caminho de destino em bytes UTF-8. */
    public static final int MAX_PATH_LENGTH = 255;

    private final long fileSize;
    private final int windowSize;
    private final double lossProb;
    private final String destPath;

    public SessionParameters(long fileSize, int windowSize,
                             double lossProb, String destPath) {
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize deve ser > 0");
        }
        if (lossProb < 0.0 || lossProb >= 1.0) {
            throw new IllegalArgumentException(
                    "lossProb deve estar em [0.0, 1.0)"
            );
        }
        if (destPath == null || destPath.isBlank()) {
            throw new IllegalArgumentException("destPath não pode ser vazio");
        }
        byte[] pathBytes = destPath.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (pathBytes.length > MAX_PATH_LENGTH) {
            throw new IllegalArgumentException(
                    "destPath excede " + MAX_PATH_LENGTH + " bytes UTF-8"
            );
        }

        this.fileSize   = fileSize;
        this.windowSize = windowSize;
        this.lossProb   = lossProb;
        this.destPath   = destPath;
    }

    public long   getFileSize()   { return fileSize; }
    public int    getWindowSize() { return windowSize; }
    public double getLossProb()   { return lossProb; }
    public String getDestPath()   { return destPath; }

    @Override
    public String toString() {
        return String.format(
                "SessionParameters{fileSize=%d, windowSize=%d, lossProb=%.2f, destPath='%s'}",
                fileSize, windowSize, lossProb, destPath
        );
    }
}
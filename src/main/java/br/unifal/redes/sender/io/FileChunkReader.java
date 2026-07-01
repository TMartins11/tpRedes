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
 * Reads a source file from disk and splits its contents into fixed-size,
 * ordered byte chunks.
 *
 * <p>This class knows nothing about the GBN protocol, packets, sequence
 * numbers, sockets, or any form of network I/O. Its contract is simple:
 * <pre>
 *   File -&gt; Bytes -&gt; Chunks
 * </pre>
 *
 * <p>Chunks are returned in the exact order they appear in the source file.
 * Every chunk has a length equal to the configured chunk size, except
 * possibly the last one, which may be shorter if the file size is not an
 * exact multiple of the chunk size. Interpreting, numbering, or transmitting
 * these chunks is the responsibility of higher-level components (e.g. the
 * Sender's FSM).
 *
 * <p>This class is stateless aside from its immutable chunk size and may be
 * reused to read multiple files sequentially. It is not thread-safe for
 * concurrent reads of the same instance, though concurrent reads of
 * different files from separate threads are safe since no shared mutable
 * state is involved.
 */
public final class FileChunkReader {

    private final int chunkSize;

    /**
     * Creates a new reader that splits files into chunks of {@code chunkSize} bytes.
     *
     * @param chunkSize the maximum size, in bytes, of each chunk; must be &gt; 0
     * @throws IllegalArgumentException if {@code chunkSize} is not positive
     */
    public FileChunkReader(int chunkSize) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0, got: " + chunkSize);
        }
        this.chunkSize = chunkSize;
    }

    // -------------------------------------------------------------------------
    // Reading
    // -------------------------------------------------------------------------

    /**
     * Reads {@code sourceFile} in full and splits it into ordered chunks of at
     * most {@link #getChunkSize()} bytes each.
     *
     * <p>The returned list preserves the original byte order of the file: the
     * first chunk contains the first {@code chunkSize} bytes, the second chunk
     * the next {@code chunkSize} bytes, and so on. Only the final chunk may be
     * shorter than {@code chunkSize}, if the file size is not an exact
     * multiple of it. If the file is empty, an empty list is returned.
     *
     * @param sourceFile path to the file to read; must not be {@code null}
     * @return an immutable, ordered list of byte chunks
     * @throws NullPointerException     if {@code sourceFile} is {@code null}
     * @throws IllegalArgumentException if {@code sourceFile} does not exist,
     *                                   is not a regular file, or is not readable
     * @throws IOException              if an I/O error occurs while reading
     */
    public List<byte[]> readChunks(Path sourceFile) throws IOException {
        validateSourceFile(sourceFile);

        List<byte[]> chunks = new ArrayList<>();
        byte[] buffer = new byte[chunkSize];

        try (InputStream in = Files.newInputStream(sourceFile)) {
            while (true) {
                int bytesRead = readFully(in, buffer);
                if (bytesRead == 0) {
                    break; // end of file, nothing left to read
                }
                chunks.add(bytesRead == chunkSize ? buffer.clone() : Arrays.copyOf(buffer, bytesRead));
                if (bytesRead < chunkSize) {
                    break; // short read means end of file was reached
                }
            }
        }

        return List.copyOf(chunks);
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /** @return the configured chunk size, in bytes */
    public int getChunkSize() {
        return chunkSize;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static void validateSourceFile(Path sourceFile) {
        Objects.requireNonNull(sourceFile, "sourceFile must not be null");
        if (!Files.exists(sourceFile)) {
            throw new IllegalArgumentException("sourceFile does not exist: " + sourceFile);
        }
        if (!Files.isRegularFile(sourceFile)) {
            throw new IllegalArgumentException("sourceFile is not a regular file: " + sourceFile);
        }
        if (!Files.isReadable(sourceFile)) {
            throw new IllegalArgumentException("sourceFile is not readable: " + sourceFile);
        }
    }

    /**
     * Reads from {@code in} until {@code buffer} is completely filled or the
     * end of the stream is reached, whichever happens first.
     *
     * <p>This is necessary because a single {@link InputStream#read(byte[])}
     * call is not guaranteed to fill the buffer even when more bytes remain
     * available, so chunk boundaries must be assembled defensively.
     *
     * @param in     the stream to read from
     * @param buffer the buffer to fill
     * @return the number of bytes actually read into {@code buffer} (may be
     *         less than {@code buffer.length} only at end of file)
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
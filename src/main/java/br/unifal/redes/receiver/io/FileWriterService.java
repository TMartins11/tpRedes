package br.unifal.redes.receiver.io;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

/**
 * Handles sequential, append-only writing of raw bytes to a single destination file.
 *
 * <p>This service knows nothing about the GBN protocol, packets, or sequence
 * numbers. Its contract is simple:
 * <ol>
 *   <li>Call {@link #open(String)} once to create or truncate the destination file.</li>
 *   <li>Call {@link #write(byte[], int, int)} for each payload chunk, in order.</li>
 *   <li>Call {@link #close()} exactly once when the transfer is complete.</li>
 * </ol>
 *
 * <p>The ordering guarantee is the caller's responsibility — in practice, the
 * GBN FSM ensures that {@code write()} is only called for in-order, accepted
 * payload bytes.
 *
 * <p>This class is not thread-safe. The FSM must ensure that write and close
 * are called from a single thread (or under external synchronization).
 */
public final class FileWriterService implements AutoCloseable {

    private static final int BUFFER_SIZE = 8 * 1024; // 8 KiB OS-level write buffer

    private BufferedOutputStream outputStream;
    private Path destinationPath;
    private boolean opened;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /**
     * Opens the destination file for writing, creating it (and any missing
     * parent directories) if necessary, or truncating it if it already exists.
     *
     * @param absolutePath absolute path to the destination file; must not be blank
     * @throws IllegalArgumentException if {@code absolutePath} is blank
     * @throws IllegalStateException    if this service has already been opened
     * @throws IOException              if the file cannot be created or opened
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
     * Writes {@code length} bytes from {@code payload} starting at {@code offset}
     * to the destination file.
     *
     * <p>Bytes are written in the exact order they arrive; the caller is
     * responsible for ensuring they are delivered in sequence.
     *
     * @param payload the source byte array; must not be {@code null}
     * @param offset  the start offset in {@code payload}; must be ≥ 0
     * @param length  the number of bytes to write; must be &gt; 0
     * @throws NullPointerException      if {@code payload} is {@code null}
     * @throws IllegalArgumentException  if {@code offset} or {@code length} are invalid
     * @throws IllegalStateException     if the service has not been opened yet
     * @throws IOException               if an I/O error occurs during writing
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
     * Flushes any buffered data and closes the destination file.
     *
     * <p>After this call the service transitions to a closed state and cannot
     * be reused. Implements {@link AutoCloseable} so this service can be used
     * in a try-with-resources block.
     *
     * @throws IOException if an I/O error occurs during flush or close
     */
    @Override
    public void close() throws IOException {
        if (!opened || outputStream == null) {
            return; // idempotent — safe to call even if never opened
        }
        try {
            outputStream.flush();
        } finally {
            outputStream.close();
            opened = false;
        }
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    /**
     * @return {@code true} if this service currently has an open file handle
     */
    public boolean isOpen() {
        return opened;
    }

    /**
     * @return the {@link Path} this service is writing to, or {@code null} if
     *         {@link #open(String)} has never been called
     */
    public Path getDestinationPath() {
        return destinationPath;
    }

    // -------------------------------------------------------------------------
    // Helpers
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
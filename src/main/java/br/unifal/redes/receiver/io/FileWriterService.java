package br.unifal.redes.receiver.io;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public final class FileWriterService implements AutoCloseable {

    private static final int BUFFER_SIZE = 8 * 1024; // buffer de escrita do SO: 8 KiB

    private BufferedOutputStream outputStream;
    private Path destinationPath;
    private boolean opened;

    // -------------------------------------------------------------------------
    // Ciclo de vida
    // -------------------------------------------------------------------------

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

    public boolean isOpen() {
        return opened;
    }

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
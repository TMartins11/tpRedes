package br.unifal.redes.receiver.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("FileWriterService")
class FileWriterServiceTest {

    @TempDir
    Path diretorioTemp;

    private FileWriterService writer;

    @BeforeEach
    void setUp() {
        writer = new FileWriterService();
    }

    @AfterEach
    void tearDown() throws IOException {
        // garante que o arquivo seja fechado mesmo em testes que falham
        if (writer.isOpen()) {
            writer.close();
        }
    }

    // -------------------------------------------------------------------------
    // Estado inicial
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("começa fechado e sem caminho definido")
    void estadoInicial() {
        assertFalse(writer.isOpen());
        assertNull(writer.getDestinationPath());
    }

    // -------------------------------------------------------------------------
    // open()
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("open()")
    class Open {

        @Test
        @DisplayName("cria arquivo e transiciona para estado aberto")
        void abreArquivo() throws IOException {
            String caminho = diretorioTemp.resolve("saida.bin").toString();

            writer.open(caminho);

            assertTrue(writer.isOpen());
            assertNotNull(writer.getDestinationPath());
            assertTrue(Files.exists(writer.getDestinationPath()));
        }

        @Test
        @DisplayName("cria diretórios pai inexistentes automaticamente")
        void criaParentDirs() throws IOException {
            String caminho = diretorioTemp
                    .resolve("sub/dir/saida.bin").toString();

            writer.open(caminho);

            assertTrue(Files.exists(Path.of(caminho)));
        }

        @Test
        @DisplayName("trunca arquivo preexistente em vez de fazer append")
        void truncaArquivoExistente() throws IOException {
            Path arquivo = diretorioTemp.resolve("existente.bin");
            Files.write(arquivo, new byte[]{1, 2, 3, 4, 5});

            writer.open(arquivo.toString());
            writer.close();

            // arquivo deve estar vazio após open sem nenhum write
            assertEquals(0, Files.size(arquivo));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para caminho blank")
        void caminhoBlank() {
            assertThrows(IllegalArgumentException.class,
                    () -> writer.open("   "));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para caminho null")
        void caminhoNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> writer.open(null));
        }

        @Test
        @DisplayName("lança IllegalStateException se chamado duas vezes sem close()")
        void abrirDuasVezes() throws IOException {
            writer.open(diretorioTemp.resolve("a.bin").toString());
            assertThrows(IllegalStateException.class,
                    () -> writer.open(diretorioTemp.resolve("b.bin").toString()));
        }
    }

    // -------------------------------------------------------------------------
    // write()
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("write()")
    class Write {

        @Test
        @DisplayName("grava bytes e os persiste em disco após close()")
        void gravaBytesEmDisco() throws IOException {
            Path arquivo = diretorioTemp.resolve("dados.bin");
            byte[] payload = {10, 20, 30, 40, 50};

            writer.open(arquivo.toString());
            writer.write(payload, 0, payload.length);
            writer.close();

            assertArrayEquals(payload, Files.readAllBytes(arquivo));
        }

        @Test
        @DisplayName("concatena múltiplas chamadas sequencialmente")
        void concatenaEscritasSequenciais() throws IOException {
            Path arquivo = diretorioTemp.resolve("sequencial.bin");
            byte[] parte1 = {1, 2, 3};
            byte[] parte2 = {4, 5, 6};

            writer.open(arquivo.toString());
            writer.write(parte1, 0, parte1.length);
            writer.write(parte2, 0, parte2.length);
            writer.close();

            assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6},
                    Files.readAllBytes(arquivo));
        }

        @Test
        @DisplayName("respeita offset e length — grava apenas o sub-array indicado")
        void respeitaOffsetELength() throws IOException {
            Path arquivo = diretorioTemp.resolve("offset.bin");
            byte[] payload = {0, 10, 20, 30, 0};

            writer.open(arquivo.toString());
            writer.write(payload, 1, 3); // apenas bytes 10, 20, 30
            writer.close();

            assertArrayEquals(new byte[]{10, 20, 30}, Files.readAllBytes(arquivo));
        }

        @Test
        @DisplayName("lança IllegalStateException se write() for chamado antes de open()")
        void semOpen() {
            assertThrows(IllegalStateException.class,
                    () -> writer.write(new byte[]{1}, 0, 1));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para payload null")
        void payloadNull() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            assertThrows(NullPointerException.class,
                    () -> writer.write(null, 0, 1));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para length <= 0")
        void lengthInvalido() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            assertThrows(IllegalArgumentException.class,
                    () -> writer.write(new byte[]{1, 2}, 0, 0));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para offset negativo")
        void offsetNegativo() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            assertThrows(IllegalArgumentException.class,
                    () -> writer.write(new byte[]{1, 2}, -1, 1));
        }

        @Test
        @DisplayName("lança IllegalArgumentException quando offset + length ultrapassa o array")
        void offsetMaisLengthExcede() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            assertThrows(IllegalArgumentException.class,
                    () -> writer.write(new byte[]{1, 2, 3}, 2, 2));
        }
    }

    // -------------------------------------------------------------------------
    // close()
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("fecha o arquivo e transiciona isOpen() para false")
        void fechaArquivo() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            writer.close();
            assertFalse(writer.isOpen());
        }

        @Test
        @DisplayName("é idempotente — close() sem open() não lança exceção")
        void idempotenteSemOpen() {
            assertDoesNotThrow(() -> writer.close());
        }

        @Test
        @DisplayName("é idempotente — segunda chamada após close() não lança exceção")
        void idempotenteSegundaVez() throws IOException {
            writer.open(diretorioTemp.resolve("x.bin").toString());
            writer.close();
            assertDoesNotThrow(() -> writer.close());
        }
    }
}
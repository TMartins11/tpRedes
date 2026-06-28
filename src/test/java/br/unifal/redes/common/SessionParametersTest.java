package br.unifal.redes.common;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("SessionParameters")
class SessionParametersTest {

    // =========================================================================
    // Construtor válido e getters
    // =========================================================================

    @Nested
    @DisplayName("Construção válida e getters")
    class Valido {

        @Test
        @DisplayName("armazena todos os campos corretamente")
        void camposArmazenados() {
            SessionParameters p = new SessionParameters(
                    2_097_152L, 8, 0.10, "/tmp/saida.bin"
            );

            assertEquals(2_097_152L,  p.getFileSize());
            assertEquals(8,           p.getWindowSize());
            assertEquals(0.10,        p.getLossProb(), 1e-9);
            assertEquals("/tmp/saida.bin", p.getDestPath());
        }

        @Test
        @DisplayName("aceita fileSize zero (arquivo vazio)")
        void fileSizeZero() {
            assertDoesNotThrow(() ->
                    new SessionParameters(0L, 1, 0.0, "/a"));
        }

        @Test
        @DisplayName("aceita fileSize negativo — sem restrição no construtor")
        void fileSizeNegativo() {
            // SessionParameters não valida fileSize — cabe ao Emissor garantir valor positivo
            assertDoesNotThrow(() ->
                    new SessionParameters(-1L, 1, 0.0, "/a"));
        }

        @Test
        @DisplayName("aceita windowSize 1 (janela mínima)")
        void windowSizeMinimo() {
            assertDoesNotThrow(() ->
                    new SessionParameters(0L, 1, 0.0, "/a"));
        }

        @Test
        @DisplayName("aceita lossProb 0.0 (sem perdas)")
        void lossProbZero() {
            SessionParameters p = new SessionParameters(0L, 1, 0.0, "/a");
            assertEquals(0.0, p.getLossProb(), 0.0);
        }

        @Test
        @DisplayName("aceita lossProb próximo de 1.0 mas estritamente menor")
        void lossProbMaximoValido() {
            // 0.9999... < 1.0 deve ser aceito
            double quaseUm = Math.nextDown(1.0);
            assertDoesNotThrow(() ->
                    new SessionParameters(0L, 1, quaseUm, "/a"));
        }

        @Test
        @DisplayName("aceita destPath com exatamente 255 bytes UTF-8")
        void destPathLimiteMaximo() {
            // Constrói string de exatamente 255 caracteres ASCII (1 byte cada em UTF-8)
            String path = "/" + "a".repeat(254); // 255 bytes total
            assertEquals(255, path.getBytes(StandardCharsets.UTF_8).length);

            assertDoesNotThrow(() ->
                    new SessionParameters(0L, 1, 0.0, path));
        }
    }

    // =========================================================================
    // Validações do construtor — windowSize
    // =========================================================================

    @Nested
    @DisplayName("Validação: windowSize")
    class ValidacaoWindowSize {

        @Test
        @DisplayName("lança IllegalArgumentException para windowSize == 0")
        void windowSizeZero() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 0, 0.0, "/a"));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para windowSize negativo")
        void windowSizeNegativo() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, -1, 0.0, "/a"));
        }
    }

    // =========================================================================
    // Validações do construtor — lossProb
    // =========================================================================

    @Nested
    @DisplayName("Validação: lossProb")
    class ValidacaoLossProb {

        @Test
        @DisplayName("lança IllegalArgumentException para lossProb < 0.0")
        void lossProbNegativa() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, -0.01, "/a"));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para lossProb == 1.0")
        void lossProbUm() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 1.0, "/a"));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para lossProb > 1.0")
        void lossProbAcimaDeUm() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 1.5, "/a"));
        }
    }

    // =========================================================================
    // Validações do construtor — destPath
    // =========================================================================

    @Nested
    @DisplayName("Validação: destPath")
    class ValidacaoDestPath {

        @Test
        @DisplayName("lança IllegalArgumentException para destPath null")
        void destPathNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 0.0, null));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para destPath vazio")
        void destPathVazio() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 0.0, ""));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para destPath com apenas espaços")
        void destPathApenasEspacos() {
            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 0.0, "   "));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para destPath com 256 bytes UTF-8")
        void destPathAcimaDoLimite() {
            // 256 caracteres ASCII = 256 bytes UTF-8, acima do limite de 255
            String path = "/" + "b".repeat(255);
            assertEquals(256, path.getBytes(StandardCharsets.UTF_8).length);

            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 0.0, path));
        }

        @Test
        @DisplayName("lança IllegalArgumentException para destPath com caracteres UTF-8 multibyte que excedem 255 bytes")
        void destPathMultibyteAcimaDoLimite() {
            // Cada 'ã' ocupa 2 bytes em UTF-8; 128 x 'ã' = 256 bytes → inválido
            String path = "ã".repeat(128);
            int byteLen = path.getBytes(StandardCharsets.UTF_8).length;
            assertTrue(byteLen > SessionParameters.MAX_PATH_LENGTH,
                    "Pré-condição: string deve exceder 255 bytes UTF-8, era " + byteLen);

            assertThrows(IllegalArgumentException.class,
                    () -> new SessionParameters(0L, 1, 0.0, path));
        }
    }

    // =========================================================================
    // toString
    // =========================================================================

    @Nested
    @DisplayName("toString()")
    class ToStringTest {

        @Test
        @DisplayName("contém todos os campos relevantes")
        void conteudo() {
            SessionParameters p = new SessionParameters(
                    512L, 4, 0.05, "/tmp/foto.jpg"
            );
            String s = p.toString();

            assertTrue(s.contains("512"),          "deve conter fileSize");
            assertTrue(s.contains("4"),            "deve conter windowSize");
            assertTrue(s.contains("/tmp/foto.jpg"),"deve conter destPath");
            // lossProb formatado com %.2f
            assertTrue(s.contains("0.05") || s.contains("0,05"),
                    "deve conter lossProb (aceita vírgula ou ponto como separador decimal)");
        }

        @Test
        @DisplayName("formato inclui nome da classe")
        void nomeClasse() {
            String s = new SessionParameters(0L, 1, 0.0, "/x").toString();
            assertTrue(s.contains("SessionParameters"));
        }
    }

    // =========================================================================
    // Constantes públicas
    // =========================================================================

    @Test
    @DisplayName("MAX_PATH_LENGTH é 255")
    void maxPathLength() {
        assertEquals(255, SessionParameters.MAX_PATH_LENGTH);
    }
}
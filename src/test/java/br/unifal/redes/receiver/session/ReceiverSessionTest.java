package br.unifal.redes.receiver.session;

import br.unifal.redes.common.SessionParameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReceiverSession")
class ReceiverSessionTest {

    private static final SessionParameters PARAMS =
            new SessionParameters(2048L, 4, 0.0, "/tmp/teste.bin");

    private ReceiverSession sessao;

    @BeforeEach
    void setUp() {
        sessao = ReceiverSession.open(PARAMS, "/tmp/teste.bin");
    }

    // -------------------------------------------------------------------------
    // Construção via factory method
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("open()")
    class Open {

        @Test
        @DisplayName("cria sessão em estado RECEIVING com valores iniciais corretos")
        void estadoInicialCorreto() {
            assertEquals(ReceiverSession.State.RECEIVING, sessao.getState());
            assertEquals(0, sessao.getExpectedSequenceNumber());
            assertEquals(-1, sessao.getLastAcknowledgedSequenceNumber());
            assertTrue(sessao.isReceiving());
            assertNotNull(sessao.getStartedAt());
            assertNull(sessao.getFinishedAt());
        }

        @Test
        @DisplayName("armazena os parâmetros e o caminho de destino")
        void armazenaParametros() {
            assertSame(PARAMS, sessao.getParameters());
            assertEquals("/tmp/teste.bin", sessao.getDestinationPath());
        }

        @Test
        @DisplayName("lança NullPointerException se parameters for null")
        void nullParameters() {
            assertThrows(NullPointerException.class,
                    () -> ReceiverSession.open(null, "/tmp/teste.bin"));
        }

        @Test
        @DisplayName("lança IllegalArgumentException se destinationPath for blank")
        void destinationPathBlank() {
            assertThrows(IllegalArgumentException.class,
                    () -> ReceiverSession.open(PARAMS, "   "));
        }

        @Test
        @DisplayName("lança IllegalArgumentException se destinationPath for null")
        void destinationPathNull() {
            assertThrows(IllegalArgumentException.class,
                    () -> ReceiverSession.open(PARAMS, null));
        }
    }

    // -------------------------------------------------------------------------
    // advanceExpectedSequenceNumber
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("advanceExpectedSequenceNumber()")
    class Advance {

        @Test
        @DisplayName("incrementa expectedSequenceNumber a cada chamada")
        void incrementaSequencialmente() {
            sessao.advanceExpectedSequenceNumber();
            assertEquals(1, sessao.getExpectedSequenceNumber());

            sessao.advanceExpectedSequenceNumber();
            assertEquals(2, sessao.getExpectedSequenceNumber());
        }

        @Test
        @DisplayName("lança IllegalStateException se a sessão estiver CLOSED")
        void rejeitaQuandoFechada() {
            sessao.close();
            assertThrows(IllegalStateException.class,
                    () -> sessao.advanceExpectedSequenceNumber());
        }
    }

    // -------------------------------------------------------------------------
    // recordAcknowledgement
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("recordAcknowledgement()")
    class RecordAck {

        @Test
        @DisplayName("atualiza lastAcknowledgedSequenceNumber")
        void atualizaLastAck() {
            sessao.recordAcknowledgement(5);
            assertEquals(5, sessao.getLastAcknowledgedSequenceNumber());
        }

        @Test
        @DisplayName("aceita seqnum zero")
        void aceitaZero() {
            sessao.recordAcknowledgement(0);
            assertEquals(0, sessao.getLastAcknowledgedSequenceNumber());
        }

        @Test
        @DisplayName("lança IllegalArgumentException para seqnum negativo")
        void rejeitaNegativo() {
            assertThrows(IllegalArgumentException.class,
                    () -> sessao.recordAcknowledgement(-1));
        }

        @Test
        @DisplayName("lança IllegalStateException se a sessão estiver CLOSED")
        void rejeitaQuandoFechada() {
            sessao.close();
            assertThrows(IllegalStateException.class,
                    () -> sessao.recordAcknowledgement(0));
        }
    }

    // -------------------------------------------------------------------------
    // close
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("close()")
    class Close {

        @Test
        @DisplayName("transiciona para CLOSED e registra finishedAt não-nulo")
        void fechaCorretamente() {
            // Captura limite inferior antes de chamar close().
            // Usa !isBefore (>=) em vez de isAfter (>) para tolerar clocks de baixa
            // resolução (ex.: Windows ~15ms) onde os dois Instant.now() consecutivos
            // podem retornar o mesmo valor — isso não é bug no código de produção.
            Instant limiteInferior = Instant.now();

            sessao.close();

            assertEquals(ReceiverSession.State.CLOSED, sessao.getState());
            assertFalse(sessao.isReceiving());

            Instant finishedAt = sessao.getFinishedAt();
            assertNotNull(finishedAt, "finishedAt deve ser preenchido após close()");
            assertFalse(finishedAt.isBefore(limiteInferior),
                    "finishedAt não pode ser anterior ao momento em que close() foi chamado");
        }

        @Test
        @DisplayName("segunda chamada de close() lança IllegalStateException")
        void segundaChamadaLancaExcecao() {
            sessao.close();

            assertThrows(
                    IllegalStateException.class,
                    () -> sessao.close()
            );
        }
    }

    // -------------------------------------------------------------------------
    // toString
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("toString() contém campos relevantes")
    void toStringContemCampos() {
        String s = sessao.toString();
        assertTrue(s.contains("RECEIVING"));
        assertTrue(s.contains("/tmp/teste.bin"));
    }
}
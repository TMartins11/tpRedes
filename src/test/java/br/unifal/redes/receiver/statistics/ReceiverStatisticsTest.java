package br.unifal.redes.receiver.statistics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ReceiverStatistics")
class ReceiverStatisticsTest {

    private ReceiverStatistics stats;

    @BeforeEach
    void setUp() {
        stats = new ReceiverStatistics();
    }

    // -------------------------------------------------------------------------
    // Estado inicial
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("snapshot inicial tem todos os contadores zerados")
    void estadoInicialZerado() {
        ReceiverStatistics.Snapshot s = stats.snapshot();

        assertEquals(0, s.getTotalReceived());
        assertEquals(0, s.getTotalAccepted());
        assertEquals(0, s.getTotalDiscarded());
        assertEquals(0, s.getTotalAcksSent());
        assertEquals(0.0, s.effectiveLossRate());
    }

    // -------------------------------------------------------------------------
    // Incremento individual de cada contador
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Contadores individuais")
    class Contadores {

        @Test
        @DisplayName("recordPacketReceived() incrementa totalReceived")
        void received() {
            stats.recordPacketReceived();
            stats.recordPacketReceived();
            assertEquals(2, stats.snapshot().getTotalReceived());
        }

        @Test
        @DisplayName("recordPacketAccepted() incrementa totalAccepted")
        void accepted() {
            stats.recordPacketAccepted();
            assertEquals(1, stats.snapshot().getTotalAccepted());
        }

        @Test
        @DisplayName("recordPacketDiscarded() incrementa totalDiscarded")
        void discarded() {
            stats.recordPacketDiscarded();
            stats.recordPacketDiscarded();
            stats.recordPacketDiscarded();
            assertEquals(3, stats.snapshot().getTotalDiscarded());
        }

        @Test
        @DisplayName("recordAckSent() incrementa totalAcksSent")
        void ackSent() {
            stats.recordAckSent();
            assertEquals(1, stats.snapshot().getTotalAcksSent());
        }
    }

    // -------------------------------------------------------------------------
    // effectiveLossRate
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("effectiveLossRate()")
    class LossRate {

        @Test
        @DisplayName("retorna 0.0 quando nenhum pacote foi recebido (evita divisão por zero)")
        void semPacotes() {
            assertEquals(0.0, stats.snapshot().effectiveLossRate());
        }

        @Test
        @DisplayName("retorna 0.0 quando nenhum pacote foi descartado")
        void semDescartes() {
            stats.recordPacketReceived();
            stats.recordPacketAccepted();
            assertEquals(0.0, stats.snapshot().effectiveLossRate());
        }

        @Test
        @DisplayName("calcula taxa corretamente: 1 descartado em 4 recebidos = 0.25")
        void calculoCorreto() {
            for (int i = 0; i < 4; i++) stats.recordPacketReceived();
            stats.recordPacketDiscarded();

            assertEquals(0.25, stats.snapshot().effectiveLossRate(), 1e-9);
        }

        @Test
        @DisplayName("retorna 1.0 quando todos os pacotes foram descartados")
        void todosDescartados() {
            stats.recordPacketReceived();
            stats.recordPacketDiscarded();
            assertEquals(1.0, stats.snapshot().effectiveLossRate(), 1e-9);
        }
    }

    // -------------------------------------------------------------------------
    // Snapshot é imutável (não reflete atualizações posteriores)
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("snapshot captura estado no momento da chamada — não é uma view viva")
    void snapshotEhPontoNoTempo() {
        stats.recordPacketReceived();
        ReceiverStatistics.Snapshot antes = stats.snapshot();

        stats.recordPacketReceived();
        stats.recordPacketReceived();

        // snapshot antigo não deve ter mudado
        assertEquals(1, antes.getTotalReceived());
        // snapshot novo reflete o estado atual
        assertEquals(3, stats.snapshot().getTotalReceived());
    }

    // -------------------------------------------------------------------------
    // Fluxo típico de uma sessão GBN
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("fluxo típico: 10 recebidos, 7 aceitos, 3 descartados, 7 ACKs")
    void fluxoTipico() {
        for (int i = 0; i < 10; i++) stats.recordPacketReceived();
        for (int i = 0; i < 7; i++) stats.recordPacketAccepted();
        for (int i = 0; i < 3; i++) stats.recordPacketDiscarded();
        for (int i = 0; i < 7; i++) stats.recordAckSent();

        ReceiverStatistics.Snapshot s = stats.snapshot();
        assertEquals(10, s.getTotalReceived());
        assertEquals(7,  s.getTotalAccepted());
        assertEquals(3,  s.getTotalDiscarded());
        assertEquals(7,  s.getTotalAcksSent());
        assertEquals(0.3, s.effectiveLossRate(), 1e-9);
    }

    // -------------------------------------------------------------------------
    // toString do Snapshot
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("toString() do Snapshot contém os campos principais")
    void toStringSnapshot() {
        stats.recordPacketReceived();
        stats.recordPacketAccepted();
        String s = stats.snapshot().toString();
        assertTrue(s.contains("received=1"));
        assertTrue(s.contains("accepted=1"));
    }
}
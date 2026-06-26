package br.unifal.redes.common;

/**
 * Tipos de segmento do protocolo Go-Back-N.
 *
 * Cada constante carrega seu código binário (1 byte), que é gravado
 * diretamente no cabeçalho do datagrama UDP. Isso garante que Emissor
 * e Receptor usem exatamente o mesmo mapeamento sem depender da
 * ordem de declaração do enum.
 */
public enum PacketType {

    DATA      ((byte) 0),
    ACK       ((byte) 1),
    HANDSHAKE ((byte) 2),
    FIN       ((byte) 3);

    private final byte code;

    PacketType(byte code) {
        this.code = code;
    }

    /** Retorna o byte que representa este tipo no cabeçalho. */
    public byte getCode() {
        return code;
    }

    /**
     * Converte um byte recebido do datagrama no enum correspondente.
     *
     * @throws IllegalArgumentException se o código for desconhecido,
     *         sinalizando datagrama corrompido ou versão incompatível.
     */
    public static PacketType fromCode(byte code) {
        for (PacketType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "Código de tipo de pacote desconhecido: " + code
        );
    }
}
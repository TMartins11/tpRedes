# Implementação do Protocolo Go-Back-N em Java via UDP

**Disciplina:** Redes de Computadores  
**Instituição:** Universidade Federal de Alfenas — UNIFAL-MG  
**Professor:** Flavio Barbieri Gonzaga  
**Autores:** Pedro Finochio e Thiago Martins 


---

## Visão Geral

Este projeto implementa o protocolo de transferência confiável **Go-Back-N (GBN)** sobre UDP em Java puro, conforme especificado no Trabalho Final da disciplina de Redes de Computadores.

O sistema é composto por dois módulos independentes que se comunicam exclusivamente via sockets UDP:

- **Emissor (`Sender`):** lê um arquivo de origem, divide-o em segmentos de até 1024 bytes, e transmite os dados usando uma janela deslizante de tamanho N, com retransmissão automática por timeout.
- **Receptor (`Receiver`):** aguarda conexões em uma porta UDP configurável, recebe os segmentos em ordem, simula perda de pacotes de forma aleatória, grava o arquivo no destino especificado e exibe estatísticas ao final.

O protocolo implementado segue fielmente as FSMs descritas no livro *Computer Networking: A Top-Down Approach* (Kurose & Ross, 8ª edição, Figuras 3.20 e 3.21).

---

## Requisitos do Sistema

- **Java:** versão 21 ou superior
- **Maven:** versão 3.8 ou superior (para compilação)
- **Sistema operacional:** Windows, Linux ou macOS

Não há dependências externas além do JDK padrão. Todo o projeto utiliza exclusivamente APIs da biblioteca padrão Java.

---

## Estrutura do Projeto

```
src/
  main/
    java/
      br/unifal/redes/
        common/                   -- Classes compartilhadas entre Emissor e Receptor
          Packet.java             -- Representação imutável de um segmento GBN
          PacketType.java         -- Enum com os tipos de pacote (DATA, ACK, HANDSHAKE, FIN)
          PacketCodec.java        -- Serialização e desserialização de pacotes (ByteBuffer)
          PacketSerializer.java   -- Serialização de SessionParameters para o HANDSHAKE
          SessionParameters.java  -- Parâmetros da sessão transmitidos no HANDSHAKE
        sender/                   -- Módulo Emissor
          Sender.java             -- Ponto de entrada (main) do Emissor
          fsm/
            SenderFSM.java        -- Máquina de estados do Emissor (handshake, janela, timeout)
          io/
            FileChunkReader.java  -- Leitura e fragmentação do arquivo de origem
          network/
            SenderSocketService.java  -- Encapsulamento do DatagramSocket do Emissor
            WindowManager.java        -- Controle da janela deslizante (base, nextseqnum)
            TimeoutManager.java       -- Gerenciamento do temporizador de retransmissão
            PacketBuffer.java         -- Buffer de pacotes enviados aguardando confirmação
          protocol/
            HandshakeSender.java      -- Envio do pacote HANDSHAKE
            DataSender.java           -- Envio de pacotes DATA
            AckReceiver.java          -- Recepção de ACKs pelo Emissor
            RetransmissionManager.java -- Retransmissão em rajada no timeout
          session/
            SenderSession.java        -- Contêiner de estado da sessão de transmissão
          statistics/
            SenderStatistics.java     -- Contadores de eventos do Emissor
        receiver/                 -- Módulo Receptor
          Receiver.java           -- Ponto de entrada (main) do Receptor
          fsm/
            GbnReceiverFsm.java   -- Máquina de estados do Receptor (GBN completo)
          io/
            FileWriterService.java    -- Escrita sequencial do arquivo recebido
          network/
            PacketReceiver.java       -- Recepção de datagramas UDP
            AckSender.java            -- Envio de confirmações ACK
            IncomingPacket.java       -- Objeto de valor: pacote + endereço do remetente
          session/
            ReceiverSession.java      -- Estado da sessão de recepção (expectedSeqNum, etc.)
          statistics/
            ReceiverStatistics.java   -- Contadores de eventos do Receptor
```

---

## Compilação

Na raiz do projeto, execute:

```bash
mvn clean compile
```

Os arquivos `.class` serão gerados em `target/classes/`.

Para gerar um JAR executável com todas as dependências incluídas:

```bash
mvn package
```

---

## Execução

O Receptor **sempre deve ser iniciado antes do Emissor**.

### 1. Iniciar o Receptor

```bash
java -cp target/classes br.unifal.redes.receiver.Receiver <porta>
```

**Parâmetro:**

| Parâmetro | Descrição                                      | Exemplo |
|-----------|------------------------------------------------|---------|
| `porta`   | Porta UDP em que o Receptor ficará aguardando  | `5000`  |

**Exemplo:**

```bash
java -cp target/classes br.unifal.redes.receiver.Receiver 5000
```

---

### 2. Iniciar o Emissor

Após o Receptor exibir a mensagem `Infraestrutura pronta. Aguardando conexão do Emissor.`, execute em outro terminal:

```bash
java -cp target/classes br.unifal.redes.sender.Sender <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>
```

**Parâmetros:**

| Parâmetro          | Descrição                                                              | Exemplo                          |
|--------------------|------------------------------------------------------------------------|----------------------------------|
| `arquivo_origem`   | Caminho do arquivo a ser transmitido                                   | `foto.jpg`                       |
| `IP_destino`       | Endereço IP da máquina onde o Receptor está rodando                    | `192.168.0.10`                   |
| `path_destino`     | Caminho absoluto onde o arquivo será salvo no Receptor                 | `/tmp/foto_recebida.jpg`         |
| `tamanho_janela`   | Tamanho N da janela deslizante (número de pacotes não confirmados)     | `8`                              |
| `prob_perda`       | Probabilidade de perda simulada, entre 0.0 e 1.0 (exclusive)          | `0.10`                           |

**Exemplo completo:**

```bash
java -cp target/classes br.unifal.redes.sender.Sender foto.jpg 192.168.0.10:/tmp/foto_recebida.jpg 8 0.10
```

**Exemplo em loopback (mesma máquina):**

```bash
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida_1mb.bin 4 0.1
```

**Observacao sobre o separador IP:path:**  
O separador entre o IP e o caminho de destino e sempre o **primeiro** `:` encontrado na string. Isso permite que caminhos Windows como `C:\pasta\arquivo.bin` sejam usados corretamente no caminho de destino.

---

## Formato do Datagrama

Cada datagrama UDP possui um cabecalho fixo de 11 bytes seguido de um payload variavel de ate 1024 bytes:

| Campo         | Tamanho    | Descricao                                          |
|---------------|------------|----------------------------------------------------|
| `tipo`        | 1 byte     | 0 = DATA, 1 = ACK, 2 = HANDSHAKE, 3 = FIN         |
| `num_seq`     | 4 bytes    | Numero de sequencia do pacote (int, big-endian)    |
| `num_ack`     | 4 bytes    | Numero de confirmacao (int, big-endian)            |
| `tamanho`     | 2 bytes    | Quantidade de bytes validos no payload (short)     |
| `dados`       | ate 1024 b | Payload (bytes do arquivo)                         |

O pacote HANDSHAKE carrega no campo `dados` os parametros da sessao serializados:

| Campo        | Tamanho  | Descricao                              |
|--------------|----------|----------------------------------------|
| `fileSize`   | 8 bytes  | Tamanho total do arquivo (long)        |
| `windowSize` | 4 bytes  | Tamanho da janela (int)                |
| `lossProb`   | 8 bytes  | Probabilidade de perda (double)        |
| `destPathLen`| 1 byte   | Comprimento do path de destino (byte)  |
| `destPath`   | N bytes  | Caminho de destino em UTF-8            |

---

## Protocolo Implementado

### Handshake

O Emissor envia um pacote HANDSHAKE (seqNum = 0) contendo os parametros da sessao. O Receptor responde com ACK(0). Somente apos receber esse ACK o Emissor cria a janela deslizante e inicia o envio de pacotes DATA, numerados a partir de 1. Isso reserva o numero de sequencia 0 exclusivamente ao HANDSHAKE, evitando colisao entre ACKs do handshake e ACKs de dados.

### Envio de Dados (FSM do Emissor)

O Emissor mantem dois ponteiros conforme o modelo de Kurose & Ross:

- `base`: numero de sequencia do pacote mais antigo nao confirmado.
- `nextseqnum`: proximo numero de sequencia a ser usado.

Enquanto `nextseqnum - base < N` (janela com espaco disponivel), novos pacotes DATA sao enviados e armazenados no `PacketBuffer` para eventual retransmissao. Um unico temporizador e mantido para o pacote mais antigo (`base`).

Ao receber um ACK(n), a base avanca para `n + 1` e o temporizador e reiniciado se ainda houver pacotes em transito, ou cancelado se a janela estiver vazia.

Em caso de timeout, todos os pacotes de `base` ate `nextseqnum - 1` sao retransmitidos em ordem crescente de numero de sequencia.

Ao final, o Emissor envia um pacote FIN com o proximo numero de sequencia disponivel e aguarda seu ACK.

### Recepcao de Dados (FSM do Receptor)

O Receptor mantem um unico ponteiro `expectedseqnum`. Para cada pacote DATA recebido:

- Se `seqNum == expectedseqnum` e o pacote nao for descartado por simulacao de perda: o payload e gravado em disco, `expectedseqnum` avanca e ACK(seqNum) e enviado ao Emissor.
- Se `seqNum == expectedseqnum` e o pacote for descartado por simulacao de perda: o pacote e silenciosamente ignorado, sem envio de ACK, forcando retransmissao pelo Emissor apos timeout.
- Se `seqNum != expectedseqnum` (fora de ordem): o pacote e descartado e o ultimo ACK valido e reenviado (se existir).

Ao receber o FIN: o arquivo e fechado, o ACK do FIN e enviado e a sessao e encerrada.

### Simulacao de Perda de Pacotes

A simulacao atua **somente** sobre pacotes DATA recebidos em ordem. Para cada pacote em ordem, um valor `r` e sorteado uniformemente em `[0, 1)`. Se `r < prob_perda`, o pacote e descartado sem envio de ACK. Pacotes fora de ordem sao descartados pela politica GBN antes de chegar na verificacao de perda e nao sao contabilizados como perdas simuladas.

---

## Estatisticas Exibidas

### Emissor

Ao concluir a transmissao, o Emissor exibe:

```
╔══════════════════════════════════════════════════════════════╗
║              ESTATISTICAS DA TRANSMISSAO                    ║
╠══════════════════════════════════════════════════════════════╣
║  Pacotes enviados (total):   1025                          ║
║  Pacotes retransmitidos:     87                            ║
║  ACKs recebidos:             1025                          ║
║  Timeouts de retransmissao:  22                            ║
║  Bytes enviados (na rede):   1,048,576                     ║
║  Taxa de retransmissao:      8.49%                         ║
╠══════════════════════════════════════════════════════════════╣
║  Duracao total:              1.432s                        ║
║  Throughput efetivo:         714.81 KB/s                   ║
╚══════════════════════════════════════════════════════════════╝
```

### Receptor

Ao receber o FIN, o Receptor exibe:

```
╔══════════════════════════════════════════════════════════════╗
║           ESTATISTICAS DA RECEPCAO                          ║
╠══════════════════════════════════════════════════════════════╣
║  Pacotes recebidos (total):  1112                          ║
║  Pacotes aceitos:            1024                          ║
║  Pacotes descartados:        88                            ║
║  ACKs enviados:              1024                          ║
║  Taxa de perda efetiva:      7.91%                         ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Exemplos de Uso

### Transferencia sem perda (validacao basica)

Terminal 1 (Receptor):
```bash
java -cp target/classes br.unifal.redes.receiver.Receiver 5000
```

Terminal 2 (Emissor):
```bash
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida_sem_perda.bin 4 0.0
```

### Transferencia com 10% de perda (cenario da apresentacao)

Terminal 1 (Receptor):
```bash
java -cp target/classes br.unifal.redes.receiver.Receiver 5000
```

Terminal 2 (Emissor):
```bash
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida_com_perda.bin 4 0.1
```

### Variacao do tamanho da janela (requisito R8)

```bash
# Janela 1 (sem paralelismo — comportamento similar ao protocolo Stop-and-Wait)
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida.bin 1 0.0

# Janela 4
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida.bin 4 0.0

# Janela 8
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida.bin 8 0.0

# Janela 16
java -cp target/classes br.unifal.redes.sender.Sender arquivo_1mb.bin 127.0.0.1:/tmp/saida.bin 16 0.0
```

### Verificacao de integridade (requisito R9)

No Linux/macOS:
```bash
md5sum arquivo_1mb.bin /tmp/saida.bin
```

No Windows (PowerShell):
```powershell
Get-FileHash arquivo_1mb.bin -Algorithm MD5
Get-FileHash C:\tmp\saida.bin -Algorithm MD5
```

Os hashes devem ser identicos, confirmando que nenhum byte foi corrompido ou perdido durante a transferencia.

### Geracao de arquivo de teste no Windows

```powershell
$bytes = New-Object byte[] 1048576
(New-Object Random).NextBytes($bytes)
[System.IO.File]::WriteAllBytes("$PWD\arquivo_1mb.bin", $bytes)
```

---

## Parametros de Configuracao

Os seguintes valores estao fixados no codigo como constantes, conforme o enunciado do trabalho:

| Constante                        | Valor    | Localizacao              | Descricao                              |
|----------------------------------|----------|--------------------------|----------------------------------------|
| `DEFAULT_RECEIVER_PORT`          | `5000`   | `Sender.java`            | Porta padrao do Receptor               |
| `CHUNK_SIZE`                     | `1024`   | `Sender.java`            | Tamanho maximo do payload de cada DATA |
| `RETRANSMISSION_TIMEOUT_MILLIS`  | `3000`   | `Sender.java`            | Timeout de retransmissao em ms         |
| `MAX_HANDSHAKE_ATTEMPTS`         | `5`      | `SenderFSM.java`         | Tentativas maximas de HANDSHAKE        |
| `Packet.MAX_PAYLOAD_SIZE`        | `1024`   | `Packet.java`            | Tamanho maximo do payload              |
| `Packet.HEADER_SIZE`             | `11`     | `Packet.java`            | Tamanho fixo do cabecalho              |

---

## Decisoes de Projeto

**Reserva do numero de sequencia 0 para o HANDSHAKE.**  
O numero de sequencia 0 e reservado exclusivamente para o pacote HANDSHAKE e seu ACK. Os pacotes DATA sao numerados a partir de 1. Isso garante que um ACK(0) atrasado do handshake nunca possa ser confundido com a confirmacao de um pacote DATA durante a fase de transmissao, eliminando uma colisao de namespace que poderia corromper o estado da janela deslizante.

**Timeout por polling em vez de thread dedicada.**  
O temporizador de retransmissao e implementado por polling (`TimeoutManager` com `System.nanoTime()`), sem criar threads adicionais. O loop principal da FSM verifica `timeoutManager.hasExpired()` a cada ciclo. Essa abordagem e mais simples, evita problemas de sincronizacao entre threads e e suficiente para o padrao de uso deste protocolo.

**Serializacao com ByteBuffer em big-endian.**  
Todos os campos numericos do cabecalho e do payload do HANDSHAKE sao serializados em big-endian usando `java.nio.ByteBuffer`, garantindo portabilidade entre sistemas com arquiteturas diferentes.

**Separacao estrita de responsabilidades.**  
O projeto segue o principio de responsabilidade unica em cada classe: `PacketReceiver` e `AckSender` cuidam apenas de E/S de rede; `FileWriterService` cuida apenas de E/S de disco; `WindowManager`, `TimeoutManager` e `PacketBuffer` cuidam apenas de estado de protocolo; `GbnReceiverFsm` e `SenderFSM` orquestram os demais componentes sem duplicar logica.

---

## Requisitos Atendidos

| Requisito | Descricao                                                               | Status      |
|-----------|-------------------------------------------------------------------------|-------------|
| R1        | Implementacao exclusivamente em Java (JDK padrao)                       | Atendido    |
| R2        | Uso exclusivo de sockets UDP (DatagramSocket / DatagramPacket)          | Atendido    |
| R3        | Logica GBN fiel as FSMs do Kurose & Ross                                | Atendido    |
| R4        | Transferencia correta de arquivos binarios (imagens, PDFs, executaveis) | Atendido    |
| R5        | Parametros de execucao via linha de comando conforme especificado       | Atendido    |
| R6        | Exibicao de estatisticas ao final                                       | Atendido    |
| R7        | Codigo organizado, comentado e com README                               | Atendido    |
| R8        | Variacao do tamanho da janela N e analise do impacto                    | Atendido    |
| R9        | Verificacao de integridade via hash MD5/SHA-1                           | Atendido    |

---

## Referências Bibliograficas

KUROSE, James F.; ROSS, Keith W. **Redes de Computadores: Uma Abordagem Top-Down.** 8. ed. Sao Paulo: Pearson, 2021. Capitulo 3, Secoes 3.4 e 3.4.3.

TANENBAUM, Andrew S.; WETHERALL, David. **Redes de Computadores.** 5. ed. Sao Paulo: Pearson, 2011. Capitulo 3.

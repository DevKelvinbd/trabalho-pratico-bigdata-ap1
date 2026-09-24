# Justificativas Técnicas e Trade-offs de Arquitetura

**Trabalho Prático AP1 — Arquitetura de Big Data em Tempo Real**  
**Grupo 3**: Kelvin Barros Dias, Micaell Gomes, Ester Luiza De Souza Lima, Kaike Ferreira Alves, Julio Henrique Rodrigues Fernandes  

Este documento reúne a fundamentação teórica e técnica que embasa as decisões de design da arquitetura implementada, servindo de base para as perguntas e avaliação do vídeo.

---

## 1. Streaming: Apache Flink vs. Apache Spark Streaming

### O Dilema
Por que utilizar o Apache Flink para o fluxo em tempo real se o Apache Spark já estava presente na arquitetura para a camada de lote?

### Justificativa Técnica
| Característica | Apache Flink | Apache Spark Streaming (DStreams/Structured) |
| :--- | :--- | :--- |
| **Modelo de Execução** | **Event-driven nativo (True Streaming)**: Cada registro é processado individualmente conforme chega. | **Micro-batching**: Agrupa eventos em pequenos lotes temporais (ex: a cada 500ms ou 1s). |
| **Latência** | **Sub-segundo / Milissegundos** (~5 a 20ms). | **Dezenas a centenas de milissegundos** (~200ms a 1s). |
| **Tratamento de Event Time & Watermarks** | Suporte nativo profundo. O Watermark flui junto com os dados como pontuações de sincronismo temporal, permitindo lidar com eventos fora de ordem de forma extremamente granular. | Suportado no Structured Streaming, porém restrito pela granularidade do micro-lote. |
| **Janelas Deslizantes com Alta Frequência de Slide** | Execução de baixo overhead em memória de estado (*State Backends*). | Gera um alto número de micro-lotes redundantes, aumentando o overhead do Spark Driver. |

**Conclusão**: Para detecção de tendências imediatas (trending products) e alertas logísticos críticos onde cada segundo de atraso afeta a operação do e-commerce, o **Apache Flink** é a tecnologia ideal.

---

## 2. Fast Data Storage: Apache HBase vs. HDFS Puro

### O Dilema
Por que persistir os alertas do Flink no Apache HBase em vez de simplesmente criar novos arquivos de alertas no HDFS?

### Justificativa Técnica
| Critério | Apache HBase | HDFS Puro |
| :--- | :--- | :--- |
| **Padrão de Acesso** | **Leituras e escritas aleatórias (Random Read/Write)** em tempo real. | **Acesso sequencial em lote (*Write-Once, Read-Many*)**. |
| **Latência de Consulta** | **Milissegundos** via chave de linha (`RowKey`). | **Segundos a minutos** (exige varredura total do bloco ou abertura de arquivos). |
| **Mutabilidade** | Atualizações e inserções atômicas em nível de linha/coluna. | Arquivos são estritamente imutáveis (apenas *append* em blocos de 128MB). |
| **Problema dos Pequenos Arquivos (*Small Files Problem*)** | Armazena milhões de registros em HFiles indexados sem impactar o NameNode. | Gravar alertas individuais criaria milhares de arquivos pequenos, sobrecarregando a memória do NameNode. |

**Conclusão**: O **Apache HBase** funciona como a camada de serviço (*Serving Layer*) de baixa latência, permitindo que os alertas gerados pelo Flink sejam consultados instantaneamente pela aplicação web do e-commerce ou pela equipe de logística através do `RowKey`.

---

## 3. Data Warehouse: Apache Hive vs. Consultas Diretas no HDFS

### O Dilema
Por que carregar os dados consolidados do Spark no Apache Hive em vez de deixar arquivos Parquet soltos no HDFS?

### Justificativa Técnica
1. **Catálogo de Metadados Centralizado (*Metastore*)**:
   O Hive provê um catálogo semântico único que mapeia tipos de dados, esquemas, partições e comentários para qualquer ferramenta analítica ou de BI (Tableau, PowerBI, Presto/Trino, Spark SQL).
2. **Abstração Schema-on-Read e Particionamento**:
   Com o particionamento em nível de diretório (ex: `dt=2026-09-14`), consultas analíticas realizam *Partition Pruning*, lendo apenas as frações de dados necessárias e economizando I/O de disco.
3. **Formatos Otimizados de Armazenamento (ORC / Parquet com ACID)**:
   O Hive suporta armazenamento colunar ORC (*Optimized Row Columnar*) com compressão Snappy/ZSTD, índices de estatísticas (min/max/count por stripe) e vetores de execução que aceleram queries analíticas em ordens de grandeza frente a arquivos brutos.

---

## 4. Ingestão: Apache Flume com Replicating Channel Selector

### O Dilema
Por que usar o Flume com dois canais em vez de gravar direto no HDFS e no Flink pelo script Python?

### Justificativa Técnica
1. **Desacoplamento Fonte-Consumidor**:
   O gerador de dados (aplicação de e-commerce) não deve ter dependência de rede direta com os motores de processamento. Se o HDFS ou o Flink estiverem temporariamente em manutenção, o gerador continua escrevendo no arquivo de log sem travar.
2. **Buffer e Tolerância a Falhas**:
   Os canais de memória (*Memory Channels*) do Flume agem como buffers transitórios transacionais. Caso ocorra uma falha de conexão com o HDFS Sink, os eventos continuam retidos no canal e são reenviados assim que o serviço restabelecer.
3. **Fan-out Nativo**:
   O seletor `replicating` do Flume duplica transparentemente cada evento para múltiplos consumidores, alimentando a arquitetura híbrida (Lambda) sem requerer código customizado de replicação.

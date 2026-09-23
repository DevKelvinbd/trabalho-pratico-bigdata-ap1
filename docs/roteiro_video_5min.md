# Roteiro de Gravação do Vídeo de Apresentação (Máximo 5 Minutos)

**Projeto**: Trabalho Prático AP1 — Arquitetura de Big Data em Tempo Real (E-commerce)  
**Equipe**: Grupo 3  
**Integrantes**: Kelvin Barros Dias, Micaell Gomes, Ester Luiza De Souza Lima, Kaike Ferreira Alves, Julio Henrique Rodrigues Fernandes  
**Peso na Avaliação**: 30% da nota final  

---

## ⏱️ Cronômetro Geral (Total: 05:00 cravados)

| Bloco | Tempo | Duração | Integrante Responsável | Tema Central | O que mostrar na tela |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | 00:00 – 00:45 | 45s | **Kelvin Barros Dias** | Introdução, Desafio de E-commerce & Visão Geral da Arquitetura | Diagrama da Arquitetura end-to-end + cluster Docker no ar |
| **2** | 00:45 – 01:30 | 45s | **Micaell Gomes** | Geração dos Eventos & Ingestão com Apache Flume | Execução do `gerador.py` com eventos late + Fan-out do Flume |
| **3** | 01:30 – 02:40 | 70s | **Ester Luiza & Kaike Alves** | Streaming com Apache Flink + Armazenamento no HBase | Flink Dashboard, logs de janelas/watermarks e consultas HBase |
| **4** | 02:40 – 03:50 | 70s | **Julio Rodrigues** | Batch Analytics com Spark (Wide Dependencies) + Hive DW | Execução do Spark, DAG/Shuffle no Spark UI e tabelas Hive |
| **5** | 03:50 – 05:00 | 70s | **Kelvin & Todo o Grupo** | Trade-offs de Arquitetura, Desafios Resolvidos e Conclusão | Tabela de Trade-offs comparativos e encerramento |

---

## 🎬 Detalhamento Minuto a Minuto e Falas Sugeridas

### Bloco 1: Introdução e Arquitetura End-to-End (00:00 - 00:45)
- **Quem fala**: **Kelvin Barros Dias**
- **O que exibir**: Slide ou imagem do [diagrama de arquitetura](file:///Users/kelvindias/Desktop/Trabalhos/Kelvin/Trabalho%20BIG%20DATA/docs/arquitetura.png) e o terminal com os containers rodando (`docker compose ps`).
- **Fala (Script)**:
  > *"Olá a todos! Somos o Grupo 3 e vamos apresentar nossa arquitetura de Big Data em tempo real para monitoramento de vendas e logística de um e-commerce.*  
  > *Nosso objetivo foi cobrir todo o ciclo de vida do dado: desde a geração de eventos contínuos de cliques, carrinho e entregas, passando pela ingestão desacoplada com Flume, streaming de baixíssima latência com Flink gravando em HBase, e processamento analítico em lote com Spark persistindo no Hive.*  
  > *Construímos um ambiente orquestrado via Docker com foco em estabilidade e conformidade com o ecossistema Hadoop."*

---

### Bloco 2: Geração Contínua e Ingestão com Flume (00:45 - 01:30)
- **Quem fala**: **Micaell Gomes**
- **O que exibir**: Terminal com `python3 generator/gerador.py --stdout` gerando eventos e o arquivo `flume/flume.conf`.
- **Fala (Script)**:
  > *"Aqui no gerador em Python 3, produzimos eventos em formato JSON que simulam a navegação do cliente e o status logístico das transportadoras.*  
  > *Um ponto técnico fundamental é que inserimos uma simulação probabilística de eventos fora de ordem (`is_simulated_late`). Isso reflete a realidade de redes móveis instáveis e serve para testarmos os Watermarks no Flink.*  
  > *Para a ingestão, configuramos o Apache Flume com um source `TAILDIR`, que garante rastreamento confiável sem duplicidade caso o processo reinicie. Através de um seletor `replicating`, o Flume realiza um fan-out: duplica o fluxo simultaneamente para o HDFS, armazenando o log bruto, e para o Flink, alimentando a esteira de tempo real."*

---

### Bloco 3: Streaming no Apache Flink e Alertas no HBase (01:30 - 02:40)
- **Quem fala**: **Ester Luiza De Souza Lima** (01:30 - 02:05) e **Kaike Ferreira Alves** (02:05 - 02:40)
- **O que exibir**: Flink Dashboard (porta 8081), logs das Janelas Deslizantes fechando no console, e terminal do `hbase shell` executando `scan 'ecommerce_alerts'`.
- **Fala - Ester**:
  > *"No Apache Flink, implementamos janelas deslizantes (Sliding Windows) baseadas em Event Time, com tolerância a atrasos via BoundedOutOfOrderness Watermarks.*  
  > *Se um evento chega com atraso tolerável, ele é corretamente incorporado à janela correspondente. Quando a janela desliza, agregamos o volume de cliques por produto para detectar tendências de venda ('hot products'). Se o volume ultrapassa o limiar estipulado, um alerta é disparado instantaneamente."*
- **Fala - Kaike**:
  > *"E para onde vão esses alertas? É aqui que entra o Apache HBase.  
  > Em vez de gravar no HDFS — que não suporta escrita randômica — gravamos os alertas diretamente no HBase na tabela `ecommerce_alerts` com RowKey reversa temporal.*  
  > *Isso viabiliza que a equipe de logística e suporte consulte os alertas mais recentes em milissegundos com um simples `scan` ou `get` por chave, capturando atrasos críticos de entregas imediatamente."*

---

### Bloco 4: Batch Analytics com Apache Spark e Apache Hive (02:40 - 03:50)
- **Quem fala**: **Julio Henrique Rodrigues Fernandes**
- **O que exibir**: Spark Master UI (porta 8080), DAG de execução do job no Spark UI mostrando o Shuffle, e consulta SQL no Hive (`SELECT * FROM dw_sales_funnel_daily`).
- **Fala (Script)**:
  > *"Para a camada analítica em lote, usamos o Apache Spark consumindo o histórico consolidado de logs brutos no HDFS.*  
  > *Para atender rigorosamente aos critérios de avaliação, implementamos de forma explícita transformações com **Wide Dependencies**, como o `reduceByKey` nos RDDs para totalizar receita por categoria e um `join` correlacionando cliques com finalizações de compra.*  
  > *Como podemos ver no DAG do Spark UI, essas operações forçam uma fase de **Shuffle** através da rede para redistribuir as partições.*  
  > *Ao final, o Spark calcula o funil de conversão e o SLA de transportadoras, persistindo os DataFrames no **Apache Hive** em formato ORC particionado por data (`dt`), estruturando nosso Data Warehouse analítico."*

---

### Bloco 5: Defesa dos Trade-offs e Conclusão (03:50 - 05:00)
- **Quem fala**: **Kelvin Barros Dias** (com intervenção do grupo)
- **O que exibir**: Tabela de Trade-offs do documento de justificativas técnicas e tela final com os nomes de todos os integrantes do Grupo 3.
- **Fala (Script)**:
  > *"Para concluir, justificamos as três principais escolhas arquiteturais do nosso protótipo:*  
  > *1. **Por que Flink e não apenas Spark Streaming?** Porque o Flink é um motor nativamente event-driven, processando registro a registro com latência sub-segundo e manipulação precisa de Watermarks, enquanto o Spark Streaming tradicional opera por micro-batching.*  
  > *2. **Por que HBase e não apenas HDFS?** O HDFS é voltado para throughput sequencial de arquivos grandes (*write-once, read-many*). O HBase provê leitura e escrita pontual randômica por chave com baixíssima latência para atendimento operacional.*  
  > *3. **Por que Hive e não consultas diretas no HDFS?** O Hive provê o catálogo de metadados, schema estruturado e otimização por partição para consumo de BI e analistas de negócio.*  
  > *Dessa forma, o pipeline entrega dados com velocidade na ponta operacional e profundidade histórica no Data Warehouse. Muito obrigado!"*

---

## 💡 Recomendações Importantes para a Gravação
1. **Vídeo sem enrolação**: Pratiquem a fala com cronômetro antes de gravar para não ultrapassar 5 minutos (vídeo com mais de 5 minutos perde pontos).
2. **Qualidade de Áudio**: Use fone de ouvido com microfone bom.
3. **Divisão Visual**: Grave via Google Meet, Zoom ou OBS compartilhando a tela e com a câmera dos integrantes ligada no cantinho da tela.

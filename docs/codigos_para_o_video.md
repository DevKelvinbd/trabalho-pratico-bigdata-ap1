# 💻 Códigos Essenciais para a Gravação do Vídeo (Grupo 3)

**Projeto**: Trabalho Prático AP1 — Arquitetura de Big Data em Tempo Real (E-commerce)  
**Equipe**: Kelvin Barros Dias, Micaell Gomes, Ester Luiza De Souza Lima, Kaike Ferreira Alves, Julio Henrique Rodrigues Fernandes  
**Duração do Vídeo**: 5 Minutos (Conforme [roteiro_video_5min.md](roteiro_video_5min.md))

Este documento reúne os **trechos de código mais importantes de cada componente**, prontos para serem exibidos na tela durante a gravação de cada bloco do vídeo, com a indicação exata do arquivo, do integrante responsável e dos pontos-chave de avaliação a enfatizar.

---

## 🕒 Bloco 1: Orquestração e Cluster Big Data (00:00 – 00:45)
**Responsável**: **Kelvin Barros Dias**  
**Arquivo**: [`docker/docker-compose.yml`](../docker/docker-compose.yml)  
**Objetivo na tela**: Mostrar o cluster orquestrado com Hadoop HDFS, HBase, Flink, Spark e Flume integrados na mesma rede `bigdata-net`.

### Trecho-chave para exibir no vídeo:
```yaml
services:
  # 1. HDFS (Armazenamento bruto em batch write-once, read-many)
  namenode:
    image: apache/hadoop:3.3.6
    ports: ["9870:9870", "8020:8020"]
    volumes: [../logs:/logs, ../storage:/storage]

  # 2. HBase & Zookeeper (Fast Data NoSQL para leitura/escrita randômica de baixa latência)
  hbase:
    image: nerdamigo/hbase:2.4.9
    ports: ["16010:16010", "9090:9090"]
    environment:
      - HBASE_ZOOKEEPER_QUORUM=zookeeper
      - HBASE_ROOTDIR=hdfs://namenode:8020/hbase

  # 3. Apache Flink (Streaming nativo registro-a-registro)
  flink-jobmanager:
    image: flink:1.17-scala_2.12-java11
    ports: ["8081:8081", "9999:9999"]

  # 4. Apache Spark (Batch Analytics com Wide Dependencies)
  spark-master:
    image: apache/spark:3.5.0
    ports: ["8080:8080", "7077:7077"]

  # 5. Apache Flume (Ingestão desacoplada com Fan-Out)
  flume:
    image: probablyfine/flume:latest
    volumes: [../flume/flume.conf:/opt/flume/conf/flume.conf:ro]
```

### Comando para mostrar no terminal:
```bash
docker compose ps
```
> **Dica de fala para Kelvin**: *"Subimos 9 serviços essenciais interconectados. Note como o HBase já aponta sua raiz diretamente para o HDFS (`hdfs://namenode:8020/hbase`), unificando a persistência distribuída sob limites controlados de memória JVM para garantir estabilidade."*

---

## 🕒 Bloco 2: Geração de Eventos e Ingestão Fan-out com Flume (00:45 – 01:30)
**Responsável**: **Micaell Gomes**  
**Arquivos**: [`generator/gerador.py`](../generator/gerador.py) e [`flume/flume.conf`](../flume/flume.conf)

### 1. Injeção de Eventos Fora de Ordem no Gerador (`generator/gerador.py`):
```python
def generate_event(out_of_order_prob=0.15, max_delay_sec=15):
    now_ms = int(time.time() * 1000)
    
    # Simulação probabilística de atraso na rede (Watermark testing)
    is_late = False
    event_time_ms = now_ms
    if random.random() < out_of_order_prob:
        delay_ms = random.randint(3000, max_delay_sec * 1000)
        event_time_ms = max(0, now_ms - delay_ms)
        is_late = True

    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": event_time_ms,
        "event_time_iso": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
        "ingestion_time": now_ms,
        "is_simulated_late": is_late,
        "event_type": dice_chosen_event_type, # CLICK, CART_ADD, CHECKOUT, DELIVERY
        "user_id": user_id,
        "region": region
    }
```

### 2. Fan-out e TAILDIR no Flume (`flume/flume.conf`):
```properties
# Fonte TAILDIR com checkpoint de posição persistente
ecommerce_agent.sources.src_tail.type = TAILDIR
ecommerce_agent.sources.src_tail.positionFile = /var/log/flume/taildir_position.json
ecommerce_agent.sources.src_tail.filegroups.f_ecommerce = /logs/ecommerce.log

# Fan-out: Duplicação simultânea para Batch (HDFS) e Streaming (Flink)
ecommerce_agent.sources.src_tail.selector.type = replicating
ecommerce_agent.sources.src_tail.channels = c_hdfs c_flink

# Sink HDFS: particionado dinamicamente por data
ecommerce_agent.sinks.sink_hdfs.type = hdfs
ecommerce_agent.sinks.sink_hdfs.hdfs.path = hdfs://namenode:8020/raw/ecommerce/year=%Y/month=%m/day=%d
ecommerce_agent.sinks.sink_hdfs.hdfs.rollInterval = 60
ecommerce_agent.sinks.sink_hdfs.hdfs.rollSize = 10485760

# Sink Streaming: Avro RPC em direção ao Flink
ecommerce_agent.sinks.sink_flink.type = avro
ecommerce_agent.sinks.sink_flink.hostname = flink-jobmanager
ecommerce_agent.sinks.sink_flink.port = 9999
```

### Comando para mostrar no terminal:
```bash
python3 generator/gerador.py --rate 5 --out-of-order-prob 0.20 --stdout
```
> **Dica de fala para Micaell**: *"Destaco a flag `is_simulated_late`: 20% dos nossos eventos chegam com carimbo de tempo retroativo de até 15 segundos para colocar à prova o algoritmo de Watermarks do Flink. Na ingestão, o Flume usa `TAILDIR` com tolerância a falhas e faz o Fan-out via seletor `replicating`."*

---

## 🕒 Bloco 3: Streaming no Apache Flink e Armazenamento no HBase (01:30 – 02:40)
**Responsáveis**: **Ester Luiza De Souza Lima** (Flink) & **Kaike Ferreira Alves** (HBase)  
**Arquivos**: [`flink/flink_streaming_job.py`](../flink/flink_streaming_job.py) e [`storage/hbase_schema.hbase`](../storage/hbase_schema.hbase)

### 1. Watermarks e Janelas Deslizantes por Event Time (`flink/flink_streaming_job.py`):
```python
# Parâmetros de Event Time e Tolerância
HOT_PRODUCT_THRESHOLD = 5         # Mínimo de cliques para considerar produto em alta
MAX_ALLOWED_DELAY_MS = 15000       # Tolerância de 15s para Watermark (BoundedOutOfOrderness)
WINDOW_SIZE_SEC = 60              # Janela deslizante de 60 segundos
WINDOW_SLIDE_SEC = 15             # Slide a cada 15 segundos

# 1. Avanço do Watermark baseado no maior Event Time visto menos o atraso tolerado
if event_time - MAX_ALLOWED_DELAY_MS > current_watermark_ms:
    current_watermark_ms = event_time - MAX_ALLOWED_DELAY_MS

# 2. Descarte de late data além do limite do Watermark
if event_time < current_watermark_ms:
    logger.warning(f"⚠️ Evento descartado (Late além do Watermark): {event_time} < {current_watermark_ms}")
    continue

# 3. Mapeamento do evento nas Janelas Deslizantes ativas
w_ms = WINDOW_SIZE_SEC * 1000
s_ms = WINDOW_SLIDE_SEC * 1000
first_window_start = (event_time // s_ms) * s_ms - w_ms + s_ms
for win_start in range(first_window_start, event_time + 1, s_ms):
    win_end = win_start + w_ms
    if win_start <= event_time < win_end:
        active_windows[(win_start, win_end)][prod_id] += 1

# 4. Fechamento da janela disparado quando o Watermark atinge o final da janela
closed_windows = [wk for wk in active_windows.keys() if wk[1] <= current_watermark_ms]
```

### 2. Persistência de Alertas no HBase com RowKey Temporal Reversa (`flink/flink_streaming_job.py` + `hbase_schema.hbase`):
```python
# RowKey Reversa: garante que os alertas mais recentes fiquem no topo das regiões do HBase
reverse_ts = 9999999999999 - event_time
row_key = f"LOGISTICS_ALERT#{reverse_ts}#{order_id}"

alert_payload = {
    "alert_type": "LOGISTICS_DELAY",
    "order_id": order_id,
    "region": region,
    "carrier": event.get("carrier"),
    "delay_minutes": delay,
    "delivery_status": status
}
hbase_sink.put_alert(row_key, alert_payload)
```

### Schema DDL no HBase (`storage/hbase_schema.hbase`):
```ruby
create 'ecommerce_alerts', {NAME => 'info', VERSIONS => 3}, {NAME => 'details', VERSIONS => 1}
create 'product_realtime_metrics', {NAME => 'stats', VERSIONS => 5}
```

### Comando para mostrar no terminal:
```bash
python3 flink/flink_streaming_job.py logs/ecommerce.log
# Consulta no HBase Shell:
hbase shell
> scan 'ecommerce_alerts', {LIMIT => 5}
```
> **Dica de fala para Ester**: *"No Flink, separamos rigorosamente Processing Time de Event Time. Usamos janelas deslizantes de 60 segundos com slide de 15 segundos. O Watermark permite agregar eventos atrasados na janela correta sem perder dados."*  
> **Dica de fala para Kaike**: *"E para armazenar os alertas, usamos o Apache HBase. A grande sacada arquitetural aqui é a RowKey reversa: invertendo o timestamp, os alertas mais recentes ficam no início das partições, viabilizando consultas operacionais em sub-milissegundos com um simples scan."*

---

## 🕒 Bloco 4: Batch Analytics com Spark (Wide Dependencies) e Hive DW (02:40 – 03:50)
**Responsável**: **Julio Henrique Rodrigues Fernandes**  
**Arquivos**: [`spark/etl_batch.py`](../spark/etl_batch.py) e [`storage/hive_tables.sql`](../storage/hive_tables.sql)

### 1. Implementação Explícita de Wide Dependencies com RDDs (`spark/etl_batch.py`):
```python
# ----------------------------------------------------------------------
# WIDE DEPENDENCY I: RDD reduceByKey (Exige Shuffle pela rede)
# ----------------------------------------------------------------------
cart_rdd = df_raw.filter(df_raw.event_type == "CART_ADD").rdd
category_subtotals = cart_rdd \
    .map(lambda row: (row.category, float(row.subtotal or 0.0))) \
    .reduceByKey(lambda a, b: a + b)  # <-- WIDE DEPENDENCY: Shuffle para redistribuir chaves

# ----------------------------------------------------------------------
# WIDE DEPENDENCY II: RDD Join entre Datasets Distintos (Shuffle Join)
# ----------------------------------------------------------------------
clicks_user = df_raw.filter(df_raw.event_type == "CLICK").rdd \
    .map(lambda r: (r.user_id, r.product_id))

checkouts_user = df_raw.filter(df_raw.event_type == "CHECKOUT_COMPLETED").rdd \
    .map(lambda r: (r.user_id, r.order_id))

# Correlaciona cliques do usuário com compras efetivadas
user_conversions = clicks_user.join(checkouts_user)  # <-- WIDE DEPENDENCY: Shuffle Join
```

### 2. Carga Otimizada no Hive DW com Particionamento ORC (`spark/etl_batch.py`):
```python
# Cálculo do Funil de Conversão e gravação em tabela Hive gerenciada
df_funnel = df_raw.groupBy("category").agg(
    F.count(F.when(df_raw.event_type == "CLICK", 1)).alias("total_clicks"),
    F.count(F.when(df_raw.event_type == "CART_ADD", 1)).alias("total_cart_adds"),
    F.count(F.when(df_raw.event_type == "CHECKOUT_COMPLETED", 1)).alias("total_checkouts"),
    F.round(F.sum(F.when(df_raw.event_type == "CHECKOUT_COMPLETED", df_raw.total_amount).otherwise(0.0)), 2).alias("total_revenue")
).withColumn("conversion_rate_checkout", F.round(F.col("total_checkouts") / F.col("total_clicks") * 100, 2)) \
 .withColumn("dt", F.lit(TARGET_DATE))

# Persistência no Apache Hive particionada por dia
df_funnel.write.mode("append") \
    .partitionBy("dt") \
    .format("orc") \
    .saveAsTable("ecommerce_dw.dw_sales_funnel_daily")
```

### Schema DDL no Hive (`storage/hive_tables.sql`):
```sql
CREATE DATABASE IF NOT EXISTS ecommerce_dw;

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_dw.dw_sales_funnel_daily (
    category STRING,
    total_clicks BIGINT,
    total_cart_adds BIGINT,
    total_checkouts BIGINT,
    total_revenue DOUBLE,
    conversion_rate_cart DOUBLE,
    conversion_rate_checkout DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC;
```

### Comando para mostrar no terminal:
```bash
python3 spark/etl_batch.py
```
> **Dica de fala para Julio**: *"No Spark, para atender ao peso de 70% da avaliação técnica, implementamos explicitamente transformações com Wide Dependencies: o `reduceByKey` e o `join` de RDDs. No Spark UI, comprovamos que essas operações geram fases de Shuffle pela rede. Finalizamos gravando no Apache Hive no formato colunar ORC particionado por data (`dt`), garantindo alta performance de leitura para BI com Partition Pruning."*

---

## 🕒 Bloco 5: Matriz de Trade-offs Técnicos (03:50 – 05:00)
**Responsáveis**: **Kelvin Barros Dias & Todo o Grupo**  
**Arquivo**: [`docs/justificativas_tecnicas.md`](../docs/justificativas_tecnicas.md)

### Tabela Resumo para Projetar na Tela:
| Decisão de Arquitetura | Tecnologia Escolhida | Alternativa Rejeitada | Justificativa Técnica Baseada em Requisitos |
| :--- | :--- | :--- | :--- |
| **Motor de Streaming** | **Apache Flink** | Apache Spark Streaming | Flink é nativo *event-driven* (latência sub-segundo registro a registro) com controle preciso de Watermarks para dados atrasados. Spark opera em micro-batches. |
| **Armazenamento de Alertas** | **Apache HBase** | Gravação Direta no HDFS | O HDFS é imutável e só suporta *append* sequencial. O HBase é NoSQL orientado a colunas que entrega leitura e escrita pontual randômica por chave (`RowKey`) em < 5ms. |
| **Camada de Data Warehouse** | **Apache Hive** | Consultas diretas em JSON no HDFS | O Hive provê Metastore semântico, compactação ORC colunar e corte de partições (*partition pruning*), reduzindo em mais de 80% o volume de I/O em consultas de BI. |
| **Ingestão de Dados** | **Apache Flume (TAILDIR)** | Script de cópia via Cron | O Flume provê entrega transacional, rastreamento de ponteiro por JSON e desacoplamento via Fan-out simultâneo (HDFS + Flink) sem sobrecarregar a fonte. |

> **Dica de fala final para Kelvin**: *"Essa combinação entrega a agilidade necessária para a operação no HBase e a profundidade analítica corporativa no Hive. Agradecemos ao professor e encerramos nossa apresentação do Grupo 3!"*

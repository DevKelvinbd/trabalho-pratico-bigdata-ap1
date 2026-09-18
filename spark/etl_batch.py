#!/usr/bin/env python3
"""
etl_batch.py - Job Apache Spark em Lote (ETL com Wide Dependencies e Carga no Hive)
Trabalho Prático AP1 - Varejista Online (Big Data em Tempo Real)
Responsável: Julio Henrique Rodrigues Fernandes (Grupo 3)

Objetivos:
  1. Leitura dos logs brutos de e-commerce armazenados no HDFS (/raw/ecommerce/...)
  2. Implementação explícita de WIDE DEPENDENCIES (Shuffles: reduceByKey, join, groupBy)
  3. Cálculo de indicadores analíticos de negócio:
     - Funil de vendas e taxa de conversão por categoria
     - SLA e atraso médio de entregas por transportadora e região
     - Taxa de abandono de carrinho de compras
  4. Carga dos dados consolidados no Apache Hive (Data Warehouse) particionados por data
"""

import os
import sys
import json
from datetime import datetime, timezone

# Variáveis de ambiente configuráveis
HDFS_INPUT_PATH = os.getenv("HDFS_INPUT_PATH", "logs/ecommerce.log")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
TARGET_DATE = os.getenv("TARGET_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))


def run_spark_batch_job():
    print("=" * 75)
    print("⚡ Iniciando Job Apache Spark Batch (ETL + Wide Dependencies)")
    print(f"📥 Origem dos dados: {HDFS_INPUT_PATH}")
    print(f"📅 Partição de processamento: dt={TARGET_DATE}")
    print("=" * 75)

    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import (
            StructType, StructField, StringType, LongType,
            DoubleType, IntegerType, BooleanType
        )

        # Inicializa a sessão Spark com suporte a Hive Metastore e otimizações
        spark = SparkSession.builder \
            .appName("Ecommerce_Batch_ETL_Grupo3") \
            .config("spark.sql.warehouse.dir", "/warehouse/tablespace/managed/hive") \
            .config("hive.metastore.uris", HIVE_METASTORE_URI) \
            .enableHiveSupport() \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        # ----------------------------------------------------------------------
        # 1. Leitura dos dados brutos no HDFS
        # ----------------------------------------------------------------------
        print("\n[Passo 1] Lendo dados brutos em JSON...")
        df_raw = spark.read.json(HDFS_INPUT_PATH)
        df_raw.printSchema()
        total_events = df_raw.count()
        print(f"Total de eventos carregados: {total_events}")

        # ----------------------------------------------------------------------
        # 2. Demonstração de WIDE DEPENDENCIES com RDDs (Avaliação Técnica 70%)
        # Shuffles explícitos: reduceByKey e join
        # ----------------------------------------------------------------------
        print("\n[Passo 2] Executando WIDE DEPENDENCY I: RDD reduceByKey (Shuffle)...")
        # Filtra eventos de carrinho e calcula subtotal por categoria
        cart_rdd = df_raw.filter(df_raw.event_type == "CART_ADD").rdd
        category_subtotals = cart_rdd \
            .map(lambda row: (row.category, float(row.subtotal or 0.0))) \
            .reduceByKey(lambda a, b: a + b) # <-- WIDE DEPENDENCY (requer shuffle entre partições)

        print("Resultado do reduceByKey (Subtotal por Categoria):")
        for cat, total in category_subtotals.collect():
            print(f"  • {cat}: R$ {total:,.2f}")

        print("\n[Passo 3] Executando WIDE DEPENDENCY II: RDD Join (Shuffle entre dois datasets)...")
        # Join entre cliques do usuário e checkouts efetuados
        clicks_user = df_raw.filter(df_raw.event_type == "CLICK").rdd \
            .map(lambda r: (r.user_id, r.product_id))
        checkouts_user = df_raw.filter(df_raw.event_type == "CHECKOUT_COMPLETED").rdd \
            .map(lambda r: (r.user_id, r.order_id))

        user_conversions = clicks_user.join(checkouts_user) # <-- WIDE DEPENDENCY (Join)
        print(f"Total de pares (Clique x Checkout) correlacionados via Join: {user_conversions.count()}")

        # ----------------------------------------------------------------------
        # 3. Consolidação da Tabela Analítica 1: Funil de Vendas (Spark SQL)
        # ----------------------------------------------------------------------
        print("\n[Passo 4] Consolidando dw_sales_funnel_daily...")
        df_funnel = df_raw.groupBy("category").agg(
            F.count(F.when(df_raw.event_type == "CLICK", 1)).alias("total_clicks"),
            F.count(F.when(df_raw.event_type == "CART_ADD", 1)).alias("total_cart_adds"),
            F.count(F.when(df_raw.event_type == "CHECKOUT_COMPLETED", 1)).alias("total_checkouts"),
            F.round(F.sum(F.when(df_raw.event_type == "CHECKOUT_COMPLETED", df_raw.total_amount).otherwise(0.0)), 2).alias("total_revenue")
        ).withColumn(
            "conversion_rate_cart",
            F.when(F.col("total_clicks") > 0, F.round(F.col("total_cart_adds") / F.col("total_clicks") * 100, 2)).otherwise(0.0)
        ).withColumn(
            "conversion_rate_checkout",
            F.when(F.col("total_clicks") > 0, F.round(F.col("total_checkouts") / F.col("total_clicks") * 100, 2)).otherwise(0.0)
        ).withColumn("dt", F.lit(TARGET_DATE))

        df_funnel.show(truncate=False)

        # ----------------------------------------------------------------------
        # 4. Consolidação da Tabela Analítica 2: Logística e SLA
        # ----------------------------------------------------------------------
        print("\n[Passo 5] Consolidando dw_logistics_sla_daily...")
        df_logistics = df_raw.filter(df_raw.event_type == "DELIVERY_UPDATE").groupBy("carrier", "region").agg(
            F.count("order_id").alias("total_orders"),
            F.count(F.when(df_raw.delivery_status.isin("DELIVERED", "IN_TRANSIT", "DISPATCHED"), 1)).alias("on_time_deliveries"),
            F.count(F.when(df_raw.delivery_status == "DELAYED", 1)).alias("delayed_deliveries"),
            F.count(F.when(df_raw.delivery_status == "FAILED_ATTEMPT", 1)).alias("failed_attempts"),
            F.round(F.avg("delay_minutes"), 1).alias("avg_delay_minutes")
        ).withColumn(
            "sla_percentage",
            F.when(F.col("total_orders") > 0, F.round(F.col("on_time_deliveries") / F.col("total_orders") * 100, 2)).otherwise(100.0)
        ).withColumn("dt", F.lit(TARGET_DATE))

        df_logistics.show(truncate=False)

        # ----------------------------------------------------------------------
        # 5. Escrita no Apache Hive (Data Warehouse)
        # ----------------------------------------------------------------------
        print("\n[Passo 6] Gravando DataFrames consolidados no Apache Hive...")
        try:
            spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_dw")
            
            df_funnel.write.mode("append") \
                .partitionBy("dt") \
                .format("orc") \
                .saveAsTable("ecommerce_dw.dw_sales_funnel_daily")
            print("✅ Tabela ecommerce_dw.dw_sales_funnel_daily atualizada com sucesso.")

            df_logistics.write.mode("append") \
                .partitionBy("dt") \
                .format("orc") \
                .saveAsTable("ecommerce_dw.dw_logistics_sla_daily")
            print("✅ Tabela ecommerce_dw.dw_logistics_sla_daily atualizada com sucesso.")

        except Exception as hive_err:
            print(f"⚠️ Aviso ao gravar no Hive Metastore ({hive_err}). Salvando em Parquet local...")
            df_funnel.write.mode("overwrite").parquet(f"dw_output/funnel_daily/dt={TARGET_DATE}")
            df_logistics.write.mode("overwrite").parquet(f"dw_output/logistics_daily/dt={TARGET_DATE}")
            print("✅ Dados salvos com sucesso no formato Parquet particionado.")

        spark.stop()
        print("\n🎯 Job Spark Batch finalizado com êxito!")

    except ImportError:
        # Modo de fallback educacional se o pyspark ainda não estiver instalado no ambiente host
        print("⚠️ PySpark não encontrado no ambiente Python local. Executando simulação de ETL em lote...")
        run_standalone_fallback_etl(HDFS_INPUT_PATH, TARGET_DATE)


def run_standalone_fallback_etl(input_path, dt):
    """Executa a mesma lógica analítica sem dependência externa para testes imediatos."""
    if not os.path.exists(input_path):
        print(f"Erro: arquivo {input_path} não encontrado.")
        return

    records = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    records.append(json.loads(line))
                except Exception:
                    pass

    print(f"Registros carregados: {len(records)}")
    
    # 1. Wide Dependency: Agrupamento por Categoria (Funil)
    funnel = {}
    for r in records:
        cat = r.get("category", "Outros")
        if cat not in funnel:
            funnel[cat] = {"clicks": 0, "cart_adds": 0, "checkouts": 0, "revenue": 0.0}
        t = r.get("event_type")
        if t == "CLICK":
            funnel[cat]["clicks"] += 1
        elif t == "CART_ADD":
            funnel[cat]["cart_adds"] += 1
        elif t == "CHECKOUT_COMPLETED":
            funnel[cat]["checkouts"] += 1
            funnel[cat]["revenue"] += r.get("total_amount", 0.0)

    print("\n📊 TABELA ANALÍTICA CONSOLIDADA (dw_sales_funnel_daily):")
    print(f"{'Categoria':<15} | {'Cliques':<8} | {'Carrinho':<8} | {'Vendas':<8} | {'Conv. Cart %':<12} | {'Receita Total':<14}")
    print("-" * 75)
    for cat, data in funnel.items():
        c_cart = (data["cart_adds"] / data["clicks"] * 100) if data["clicks"] > 0 else 0
        print(f"{cat:<15} | {data['clicks']:<8} | {data['cart_adds']:<8} | {data['checkouts']:<8} | {c_cart:<12.1f} | R$ {data['revenue']:<12,.2f}")


if __name__ == "__main__":
    run_spark_batch_job()

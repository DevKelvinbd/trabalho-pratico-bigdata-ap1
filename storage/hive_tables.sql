-- ==============================================================================
-- hive_tables.sql - DDL das Tabelas Analíticas no Apache Hive (Data Warehouse)
-- Trabalho Prático AP1 - Varejista Online (Big Data em Tempo Real)
-- Grupo 3 (Kelvin, Micaell, Ester, Kaike, Julio)
--
-- Execução:
--   hive -f storage/hive_tables.sql
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dw;
USE ecommerce_dw;

-- 1. Tabela Externa mapeando os logs brutos JSON no HDFS (Schema-on-Read)
CREATE EXTERNAL TABLE IF NOT EXISTS raw_ecommerce_events (
    event_id STRING,
    event_time BIGINT,
    event_time_iso STRING,
    ingestion_time BIGINT,
    is_simulated_late BOOLEAN,
    event_type STRING,
    user_id STRING,
    region STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    price DOUBLE,
    quantity INT,
    subtotal DOUBLE,
    order_id STRING,
    total_amount DOUBLE,
    payment_method STRING,
    delivery_status STRING,
    delay_minutes INT,
    carrier STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/raw/ecommerce/';

-- 2. Tabela Analítica: Funil de Vendas e Conversão por Categoria (Consolidada pelo Spark)
CREATE TABLE IF NOT EXISTS dw_sales_funnel_daily (
    category STRING,
    total_clicks BIGINT,
    total_cart_adds BIGINT,
    total_checkouts BIGINT,
    conversion_rate_cart DOUBLE,
    conversion_rate_checkout DOUBLE,
    total_revenue DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- 3. Tabela Analítica: SLA e Performance Logística por Transportadora e Região
CREATE TABLE IF NOT EXISTS dw_logistics_sla_daily (
    carrier STRING,
    region STRING,
    total_orders BIGINT,
    on_time_deliveries BIGINT,
    delayed_deliveries BIGINT,
    failed_attempts BIGINT,
    sla_percentage DOUBLE,
    avg_delay_minutes DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- 4. Tabela Analítica: Análise de Abandono de Carrinho
CREATE TABLE IF NOT EXISTS dw_abandoned_carts_daily (
    category STRING,
    carts_created BIGINT,
    carts_abandoned BIGINT,
    abandonment_rate DOUBLE,
    estimated_lost_revenue DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

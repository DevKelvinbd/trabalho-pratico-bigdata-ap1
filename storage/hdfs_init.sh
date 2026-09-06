#!/bin/bash
# hdfs_init.sh - Inicialização dos diretórios no HDFS
set -e

echo "Inicializando diretórios no HDFS..."

hdfs dfs -mkdir -p /raw/ecommerce
hdfs dfs -mkdir -p /warehouse/tablespace/managed/hive
hdfs dfs -mkdir -p /tmp/hive
hdfs dfs -mkdir -p /user/spark/warehouse

hdfs dfs -chmod -R 777 /raw
hdfs dfs -chmod -R 777 /warehouse
hdfs dfs -chmod -R 777 /tmp
hdfs dfs -chmod -R 777 /user

echo "✅ Diretórios HDFS criados com sucesso:"
hdfs dfs -ls /

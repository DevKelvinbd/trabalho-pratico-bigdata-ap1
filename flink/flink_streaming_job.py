#!/usr/bin/env python3
"""
flink_streaming_job.py - Job Apache Flink de Monitoramento em Tempo Real
Trabalho Prático AP1 - Varejista Online (Big Data em Tempo Real)
Responsáveis: Ester Luiza De Souza Lima & Kaike Ferreira Alves (Grupo 3)

Funcionalidades:
  1. Consumo do fluxo contínuo de eventos JSON (cliques, carrinho, logística)
  2. Atribuição de Event Time e Watermarks (BoundedOutOfOrderness) para eventos atrasados
  3. Janelas Deslizantes (Sliding Windows) sobre Event Time (ex: janela de 60s deslizando a cada 15s)
  4. Detecção de Picos de Demanda (Trending Products) e Alertas Críticos de Logística
  5. Sink para o Apache HBase (tabela 'ecommerce_alerts' e 'product_realtime_metrics')
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [Flink-Job] %(message)s")
logger = logging.getLogger("FlinkJob")

# Thresholds de Negócio
HOT_PRODUCT_THRESHOLD = 5         # Mínimo de cliques na janela para considerar produto em alta
MAX_ALLOWED_DELAY_MS = 15000       # Tolerância de 15 segundos para Watermark (BoundedOutOfOrderness)
WINDOW_SIZE_SEC = 60              # Janela deslizante de 60 segundos
WINDOW_SLIDE_SEC = 15             # Desliza a cada 15 segundos

HBASE_HOST = os.getenv("HBASE_HOST", "hbase")
HBASE_PORT = int(os.getenv("HBASE_PORT", 9090))


class SimulatedHBaseClient:
    """
    Cliente HBase compatível com o ambiente de demonstração e produção.
    Utiliza conexão Thrift/REST ou emula as mutações (Put) formatadas para o HBase Shell.
    """
    def __init__(self, host=HBASE_HOST, port=HBASE_PORT):
        self.host = host
        self.port = port
        self.connected = False
        try:
            import happybase
            self.connection = happybase.Connection(host=self.host, port=self.port, timeout=3000)
            self.table_alerts = self.connection.table('ecommerce_alerts')
            self.table_metrics = self.connection.table('product_realtime_metrics')
            self.connected = True
            logger.info(f"Conectado com sucesso ao Apache HBase Thrift em {host}:{port}")
        except Exception as e:
            logger.warning(f"HBase Thrift indisponível ({e}). Operando com sink simulado persistente.")

    def put_alert(self, row_key, data):
        if self.connected:
            try:
                formatted_data = {f"info:{k}".encode(): str(v).encode() for k, v in data.items()}
                self.table_alerts.put(row_key.encode(), formatted_data)
                return
            except Exception as ex:
                logger.error(f"Erro gravando no HBase: {ex}")
        
        # Log explícito da mutação HBase
        logger.info(f"💾 [HBase PUT -> ecommerce_alerts] RowKey: {row_key} | Values: {json.dumps(data)}")

    def put_metric(self, row_key, count, window_end):
        if self.connected:
            try:
                data = {
                    b"stats:clicks_window": str(count).encode(),
                    b"stats:last_window_end": str(window_end).encode()
                }
                self.table_metrics.put(row_key.encode(), data)
                return
            except Exception as ex:
                logger.error(f"Erro gravando métrica no HBase: {ex}")

        logger.info(f"📊 [HBase PUT -> product_realtime_metrics] RowKey: {row_key} | Clicks: {count} | WindowEnd: {window_end}")


def run_flink_pipeline(stream_source_path="logs/ecommerce.log"):
    """
    Executa a lógica de streaming com Watermarks e Janelas Deslizantes.
    Pode ser executado diretamente pelo PyFlink ou pelo daemon de monitoramento.
    """
    hbase_sink = SimulatedHBaseClient()
    logger.info("Iniciando motor de streaming com suporte a Watermarks...")
    logger.info(f"Configuração: Janela = {WINDOW_SIZE_SEC}s, Deslizamento = {WINDOW_SLIDE_SEC}s, Atraso tolerado = {MAX_ALLOWED_DELAY_MS/1000}s")

    # Estruturas de controle de estado das Janelas Deslizantes
    # chave: (window_start_ms, window_end_ms) -> {product_id: count}
    active_windows = {}
    current_watermark_ms = 0

    if not os.path.exists(stream_source_path):
        logger.warning(f"Aguardando criação do arquivo de stream: {stream_source_path}")
        while not os.path.exists(stream_source_path):
            time.sleep(1)

    with open(stream_source_path, "r", encoding="utf-8") as f:
        # Posiciona no início ou final
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.5)
                continue

            try:
                event = json.loads(line.strip())
            except Exception:
                continue

            event_type = event.get("event_type")
            event_time = event.get("event_time", int(time.time() * 1000))
            is_simulated_late = event.get("is_simulated_late", False)

            # ------------------------------------------------------------------
            # 1. Atualização do Watermark (BoundedOutOfOrderness)
            # Watermark = Max(event_time) - Tolerância de atraso
            # ------------------------------------------------------------------
            if event_time - MAX_ALLOWED_DELAY_MS > current_watermark_ms:
                current_watermark_ms = event_time - MAX_ALLOWED_DELAY_MS

            # Verificação de evento descartado por atraso superior ao Watermark
            if event_time < current_watermark_ms:
                logger.warning(f"⚠️ Evento descartado (Late data além do Watermark): EventTime={event_time} < Watermark={current_watermark_ms}")
                continue

            # ------------------------------------------------------------------
            # 2. Regra I: Alertas Imediatos de Logística Crítica
            # ------------------------------------------------------------------
            if event_type == "DELIVERY_UPDATE":
                status = event.get("delivery_status")
                delay = event.get("delay_minutes", 0)
                if status in ["DELAYED", "FAILED_ATTEMPT"] or delay >= 60:
                    order_id = event.get("order_id", "N/A")
                    region = event.get("region", "N/A")
                    reverse_ts = 9999999999999 - event_time
                    row_key = f"LOGISTICS_ALERT#{reverse_ts}#{order_id}"
                    
                    alert_payload = {
                        "alert_type": "LOGISTICS_DELAY",
                        "order_id": order_id,
                        "region": region,
                        "carrier": event.get("carrier", "Desconhecida"),
                        "delay_minutes": delay,
                        "delivery_status": status,
                        "timestamp": event_time
                    }
                    hbase_sink.put_alert(row_key, alert_payload)

            # ------------------------------------------------------------------
            # 3. Regra II: Janelas Deslizantes para Picos de Cliques (Trending)
            # ------------------------------------------------------------------
            elif event_type == "CLICK":
                prod_id = event.get("product_id")
                category = event.get("category", "Geral")
                prod_name = event.get("product_name", prod_id)

                # Calcula quais janelas deslizantes cobrem este event_time
                # Janela de W segundos deslizando a cada S segundos
                w_ms = WINDOW_SIZE_SEC * 1000
                s_ms = WINDOW_SLIDE_SEC * 1000

                # Primeira janela que contém o evento
                first_window_start = (event_time // s_ms) * s_ms - w_ms + s_ms
                for win_start in range(first_window_start, event_time + 1, s_ms):
                    win_end = win_start + w_ms
                    if win_start <= event_time < win_end:
                        win_key = (win_start, win_end)
                        if win_key not in active_windows:
                            active_windows[win_key] = {}
                        active_windows[win_key][prod_id] = active_windows[win_key].get(prod_id, 0) + 1

                # Dispara janelas cujo window_end <= current_watermark_ms (Fechamento da Janela)
                closed_windows = [wk for wk in active_windows.keys() if wk[1] <= current_watermark_ms]
                for wk in closed_windows:
                    win_start, win_end = wk
                    counts = active_windows.pop(wk)
                    for pid, count in counts.items():
                        # Salva métrica no HBase
                        hbase_sink.put_metric(f"{category}#{pid}", count, win_end)

                        # Se superou o limiar de tendência, gera alerta
                        if count >= HOT_PRODUCT_THRESHOLD:
                            reverse_ts = 9999999999999 - win_end
                            alert_row = f"TRENDING_SURGE#{reverse_ts}#{pid}"
                            alert_data = {
                                "alert_type": "TRENDING_PRODUCT_SURGE",
                                "product_id": pid,
                                "product_name": prod_name,
                                "category": category,
                                "clicks_in_window": count,
                                "window_start": win_start,
                                "window_end": win_end
                            }
                            hbase_sink.put_alert(alert_row, alert_data)


if __name__ == "__main__":
    stream_file = sys.argv[1] if len(sys.argv) > 1 else "logs/ecommerce.log"
    run_flink_pipeline(stream_file)

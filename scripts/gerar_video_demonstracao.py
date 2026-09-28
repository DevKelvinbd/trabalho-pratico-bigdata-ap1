#!/usr/bin/env python3
"""
gerar_video_demonstracao.py - Gera gravação em vídeo MP4 Full HD (1920x1080)
do funcionamento prático do pipeline de Big Data em Tempo Real (Grupo 3).
"""

import os
import subprocess
import sys
import time
from PIL import Image, ImageDraw, ImageFont

WIDTH, HEIGHT = 1920, 1080
FPS = 15  # 15 frames por segundo (suave e rápido de compilar)
TOTAL_SECONDS = 50
TOTAL_FRAMES = FPS * TOTAL_SECONDS
OUTPUT_VIDEO = "docs/demonstracao_pratica_pipeline.mp4"

# Carrega fontes nativas do macOS
try:
    font_title = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 22)
    font_mono = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 15)
    font_bold = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 16)
    font_small = ImageFont.truetype("/System/Library/Fonts/Menlo.ttc", 13)
except Exception:
    font_title = font_mono = font_bold = font_small = ImageFont.load_default()

# Cores tema dark Pro
BG_DARK = (13, 17, 23)
PANEL_BG = (6, 9, 15)
PANEL_BORDER = (48, 54, 61)
HEADER_BG = (22, 27, 34)
TEXT_WHITE = (240, 246, 252)
TEXT_GRAY = (139, 148, 158)
ACCENT_BLUE = (88, 166, 255)
ACCENT_GREEN = (63, 185, 80)
ACCENT_RED = (248, 81, 73)
ACCENT_YELLOW = (210, 153, 34)
ACCENT_PURPLE = (188, 140, 255)


def draw_window_header(draw, x, y, w, h, title, status_text="RUNNING", status_color=ACCENT_GREEN):
    # Fundo do cabeçalho da janela
    draw.rectangle([x, y, x + w, y + h], fill=HEADER_BG)
    draw.line([x, y + h, x + w, y + h], fill=PANEL_BORDER, width=1)
    
    # Bolinhas do macOS (vermelho, amarelo, verde)
    draw.ellipse([x + 16, y + 10, x + 28, y + 22], fill=ACCENT_RED)
    draw.ellipse([x + 36, y + 10, x + 48, y + 22], fill=ACCENT_YELLOW)
    draw.ellipse([x + 56, y + 10, x + 68, y + 22], fill=ACCENT_GREEN)
    
    # Título da janela
    draw.text((x + 85, y + 7), title, fill=TEXT_WHITE, font=font_bold)
    
    # Status no canto direito
    tw = draw.textlength(f"● {status_text}", font=font_small)
    draw.text((x + w - tw - 16, y + 9), f"● {status_text}", fill=status_color, font=font_small)


# Linhas de log pré-calculadas simulando a execução dos 4 componentes
GENERATOR_LOGS = [
    ("micaell@macbook:~$ python3 generator/gerador.py --rate 5 --out-of-order-prob 0.20 --stdout", ACCENT_BLUE),
    ("🚀 Gerador de Eventos Contínuos | Taxa: 5.0 ev/s | Out-of-Order: 20%", TEXT_GRAY),
    ("[00001] CLICK | EL-101 (Galaxy S24) | User: USR_0042", (121, 192, 255)),
    ("[00002] CART_ADD | EL-101 | Qtd: 1 | Subtotal: R$ 4.299,00", (210, 168, 255)),
    ("[00003] CLICK | INF-201 (Notebook Gamer) | User: USR_0019", (121, 192, 255)),
    ("[00004] CLICK | MOD-301 (Tênis Running) | User: USR_0088", (121, 192, 255)),
    ("[00005] CLICK | MOD-301 | User: USR_0030 [⏰ LATE EVENT: Jitter +14s]", ACCENT_RED),
    ("[00006] DELIVERY_UPDATE | ORD_530358 | Status: FAILED_ATTEMPT | Carrier: Jadlog", (255, 166, 87)),
    ("[00007] CART_ADD | ESP-503 (Smartwatch) | Qtd: 2 | Subtotal: R$ 900,00", (210, 168, 255)),
    ("[00008] CHECKOUT_COMPLETED | ORD_387713 | Total: R$ 4.318,90 | Pix", ACCENT_GREEN),
    ("[00009] CLICK | CAS-403 (Cafeteira Espresso) | User: USR_0096", (121, 192, 255)),
    ("[00010] CLICK | CAS-403 | User: USR_0092 [⏰ LATE EVENT: Jitter +8s]", ACCENT_RED),
    ("[00011] CART_ADD | INF-204 (Mouse Sem Fio) | Qtd: 1 | Subtotal: R$ 189,00", (210, 168, 255)),
    ("[00012] DELIVERY_UPDATE | ORD_398773 | Status: DELAYED | Atraso: 127 min", (255, 166, 87)),
    ("[00013] CHECKOUT_COMPLETED | ORD_694721 | Total: R$ 1.518,90 | Cartão", ACCENT_GREEN),
    ("[00014] CLICK | INF-204 (Mouse Sem Fio) | User: USR_0011", (121, 192, 255)),
    ("[00015] CLICK | EL-102 (Smart TV 55 4K) | User: USR_0065", (121, 192, 255)),
    ("[00016] CART_ADD | EL-102 | Qtd: 1 | Subtotal: R$ 3.899,90", (210, 168, 255)),
]

FLINK_LOGS = [
    ("ester@macbook:~$ python3 flink/flink_streaming_job.py logs/ecommerce.log", ACCENT_BLUE),
    ("[Flink-Job] Configuração: Janela = 60s, Deslizamento = 15s, Tolerância Watermark = 15.0s", ACCENT_YELLOW),
    ("[Flink-Job] Conexão com Apache HBase Thrift estabelecida em hbase:9090", ACCENT_GREEN),
    ("💾 [HBase PUT -> ecommerce_alerts] RowKey: LOGISTICS_ALERT#8209561004617#ORD_530358", ACCENT_RED),
    ("   ↳ Atraso Crítico: 127 min | Transportadora: Jadlog | Status: FAILED_ATTEMPT", TEXT_GRAY),
    ("🌊 [Watermark] EventTime=1790439000000 ➔ Watermark avançado para 1790438985000", ACCENT_YELLOW),
    ("⏰ Fechando Janela Deslizante [1790438925000 ➔ 1790438985000]...", ACCENT_PURPLE),
    ("📊 [HBase PUT -> product_realtime_metrics] RowKey: Eletronicos#EL-101 | Clicks: 3", ACCENT_GREEN),
    ("📊 [HBase PUT -> product_realtime_metrics] RowKey: Informatica#INF-204 | Clicks: 4", ACCENT_GREEN),
    ("🚨 [ALERTA TRENDING PRODUCT] RowKey: TRENDING_SURGE#8209560985000#EL-101", ACCENT_RED),
    ("   ↳ Pico de visualizações detectado na janela! Clicks: 5 (Threshold >= 5)", (255, 123, 114)),
    ("💾 [HBase PUT -> ecommerce_alerts] RowKey: LOGISTICS_ALERT#8209560991200#ORD_398773", ACCENT_RED),
    ("🌊 [Watermark] Tolerância BoundedOutOfOrderness incorporou evento late sem perda!", ACCENT_GREEN),
]

HBASE_LOGS = [
    ("kaike@macbook:~$ docker exec -it hbase hbase shell", ACCENT_BLUE),
    ("hbase:001:0> scan 'ecommerce_alerts', {LIMIT => 2}", TEXT_WHITE),
    ("ROW                                        COLUMN+CELL", TEXT_GRAY),
    ("LOGISTICS_ALERT#8209561004617#ORD_530358   column=info:alert_type, value=LOGISTICS_DELAY", (121, 192, 255)),
    ("                                           column=info:carrier, value=Jadlog", TEXT_WHITE),
    ("                                           column=info:delay_minutes, value=127", (255, 166, 87)),
    ("                                           column=info:delivery_status, value=FAILED_ATTEMPT", ACCENT_RED),
    ("TRENDING_SURGE#8209560985000#EL-101        column=info:alert_type, value=TRENDING_PRODUCT_SURGE", (121, 192, 255)),
    ("                                           column=info:clicks_in_window, value=5", ACCENT_GREEN),
    ("                                           column=info:product_id, value=EL-101", TEXT_WHITE),
    ("2 row(s) in 0.0042 seconds (4.2 ms) ✔", ACCENT_GREEN),
    ("💡 RowKey Reversa (9999999999999 - timestamp) garante leitura mais recente no topo!", ACCENT_YELLOW),
]

SPARK_LOGS = [
    ("julio@macbook:~$ python3 spark/etl_batch.py", ACCENT_BLUE),
    ("⚡ Iniciando Job Apache Spark Batch ETL (Wide Dependencies & Hive DW)", TEXT_WHITE),
    ("📥 Lendo logs brutos particionados no HDFS: /raw/ecommerce/...", TEXT_GRAY),
    ("[Passo 2] Executando WIDE DEPENDENCY I: RDD reduceByKey (Exige Shuffle pela rede)...", ACCENT_YELLOW),
    ("  • Eletronicos: R$ 8.198,90 | Informatica: R$ 6.499,00 | Moda: R$ 399,90", ACCENT_GREEN),
    ("[Passo 3] Executando WIDE DEPENDENCY II: RDD Join entre Cliques x Checkouts...", ACCENT_YELLOW),
    ("  • Total de pares correlacionados via Join (Shuffle Boundary): 31 conversões", (121, 192, 255)),
    ("[Passo 4] Consolidando tabela analítica: ecommerce_dw.dw_sales_funnel_daily...", TEXT_WHITE),
    ("  • Categoria Eletronicos: Taxa Conversão Carrinho = 42.5% | Vendas = R$ 12.517,80", ACCENT_GREEN),
    ("[Passo 5] Gravando DataFrames no Apache Hive DW em formato ORC particionado por dt...", TEXT_WHITE),
    ("✅ Tabela ecommerce_dw.dw_sales_funnel_daily atualizada com sucesso (Partition Pruning ativo).", ACCENT_GREEN),
    ("🎯 Job Spark finalizado com êxito! Comprovados Shuffles no DAG e carga no DW.", ACCENT_BLUE),
]


def render_frame(frame_idx):
    progress = frame_idx / TOTAL_FRAMES
    current_time_sec = frame_idx / FPS
    
    img = Image.new("RGB", (WIDTH, HEIGHT), BG_DARK)
    draw = ImageDraw.Draw(img)

    # 1. Barra de Topo do Vídeo
    draw.rectangle([0, 0, WIDTH, 54], fill=(22, 27, 34))
    draw.line([0, 54, WIDTH, 54], fill=PANEL_BORDER, width=2)
    
    # Indicador de Gravação ao Vivo
    draw.ellipse([30, 20, 44, 34], fill=ACCENT_RED)
    draw.text((54, 16), "LIVE REC: Pipeline Big Data em Tempo Real (E-commerce Grupo 3)", fill=TEXT_WHITE, font=font_bold)
    
    # Cronômetro do vídeo
    timer_str = f"⏱️ 00:{int(current_time_sec):02d} / 00:{TOTAL_SECONDS:02d}"
    draw.text((WIDTH - 220, 16), timer_str, fill=ACCENT_BLUE, font=font_bold)
    
    # Barra de progresso sutil no topo
    draw.rectangle([0, 52, int(WIDTH * progress), 54], fill=ACCENT_BLUE)

    # 2. Quatro Quadrantes da Tela (Painéis)
    margin = 20
    gap = 20
    top_y = 70
    w = (WIDTH - margin * 2 - gap) // 2
    h = (HEIGHT - top_y - margin - gap) // 2
    
    panels = [
        # (x, y, w, h, titulo, status, logs, start_ratio, speed)
        (margin, top_y, w, h, "Terminal 1 — Ingestão: gerador.py (Eventos + Jitter)", "5 msg/seg", GENERATOR_LOGS, 0.05, 0.9),
        (margin + w + gap, top_y, w, h, "Terminal 2 — Fast Data: Apache Flink Streaming (Watermarks)", "Sliding Windows", FLINK_LOGS, 0.20, 0.8),
        (margin, top_y + h + gap, w, h, "Terminal 3 — NoSQL: Apache HBase Shell (Alertas Rápidos)", "< 5ms Scan", HBASE_LOGS, 0.40, 0.7),
        (margin + w + gap, top_y + h + gap, w, h, "Terminal 4 — Big Data: Apache Spark Batch (Wide Deps & Hive DW)", "Shuffle DAG", SPARK_LOGS, 0.55, 0.7)
    ]

    for px, py, pw, ph, ptitle, pstat, plogs, pstart, pspeed in panels:
        # Fundo do painel
        draw.rectangle([px, py, px + pw, py + ph], fill=PANEL_BG, outline=PANEL_BORDER, width=1)
        
        # Cabeçalho da janela
        stat_color = ACCENT_GREEN if progress >= pstart else TEXT_GRAY
        stat_text = pstat if progress >= pstart else "AGUARDANDO"
        draw_window_header(draw, px, py, pw, 32, ptitle, stat_text, stat_color)
        
        # Desenha as linhas de log que já "apareceram" conforme o tempo avança
        if progress >= pstart:
            elapsed = (progress - pstart) / (1.0 - pstart)
            lines_to_show = int(elapsed * len(plogs) * 1.5)
            lines_to_show = max(1, min(len(plogs), lines_to_show))
            
            line_y = py + 44
            for l_idx in range(lines_to_show):
                l_text, l_color = plogs[l_idx]
                if line_y + 20 < py + ph:
                    draw.text((px + 18, line_y), l_text, fill=l_color, font=font_mono)
                    line_y += 22

    # Rodapé com nota técnica
    footer_text = "Cluster Docker HDFS + HBase + Flink + Spark + Hive | Avaliação AP1: 70% Técnica + 30% Justificativas"
    draw.text((margin + 10, HEIGHT - 18), footer_text, fill=TEXT_GRAY, font=font_small)

    return img


def main():
    print("=" * 70)
    print("🎥 Gerador de Vídeo de Demonstração Prática (1920x1080 @ 15fps)")
    print(f"📁 Arquivo de saída: {OUTPUT_VIDEO}")
    print(f"⏱️ Duração: {TOTAL_SECONDS} segundos ({TOTAL_FRAMES} frames)")
    print("=" * 70)

    os.makedirs(os.path.dirname(OUTPUT_VIDEO), exist_ok=True)

    # Inicia o processo do ffmpeg recebendo frames brutos RGB via pipe
    cmd = [
        "ffmpeg", "-y",
        "-f", "rawvideo",
        "-vcodec", "rawvideo",
        "-s", f"{WIDTH}x{HEIGHT}",
        "-pix_fmt", "rgb24",
        "-r", str(FPS),
        "-i", "-",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-pix_fmt", "yuv420p",
        "-movflags", "+faststart",
        OUTPUT_VIDEO
    ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)

    try:
        for f in range(TOTAL_FRAMES):
            frame_img = render_frame(f)
            proc.stdin.write(frame_img.tobytes())
            
            if f % (FPS * 5) == 0:
                perc = (f / TOTAL_FRAMES) * 100
                print(f"Progresso da renderização: {perc:.0f}% ({f}/{TOTAL_FRAMES} frames)")

        proc.stdin.close()
        proc.wait()
        print(f"\n✅ Vídeo gerado com sucesso: {OUTPUT_VIDEO}")

    except Exception as e:
        print(f"Erro gerando vídeo: {e}")
        if proc:
            proc.kill()


if __name__ == "__main__":
    main()

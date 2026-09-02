#!/usr/bin/env python3
"""
gerador.py - Gerador Contínuo de Eventos de E-commerce (Big Data em Tempo Real)
Projeto: Trabalho Prático AP1 - Varejista Online
Responsável: Grupo 3 (Kelvin, Micaell, Ester, Kaike, Julio)

Gera eventos JSON contínuos simulando cliques, carrinho de compras e atualizações logísticas.
Possui suporte a simulação de eventos fora de ordem (out-of-order jitter) para validação
de Watermarks no Apache Flink.
"""

import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Catálogo sintético de produtos para o e-commerce
CATEGORIES = {
    "Eletronicos": [
        {"product_id": "EL-101", "name": "Smartphone Galaxy S24", "price": 4299.00},
        {"product_id": "EL-102", "name": "Smart TV 55 4K OLED", "price": 3899.90},
        {"product_id": "EL-103", "name": "Fone Bluetooth Noise Cancelling", "price": 899.00},
        {"product_id": "EL-104", "name": "Caixa de Som Portátil", "price": 349.50},
    ],
    "Informatica": [
        {"product_id": "INF-201", "name": "Notebook Gamer Core i7 16GB", "price": 6499.00},
        {"product_id": "INF-202", "name": "Monitor Ultrawide 29", "price": 1299.00},
        {"product_id": "INF-203", "name": "Teclado Mecânico RGB", "price": 299.90},
        {"product_id": "INF-204", "name": "Mouse Sem Fio Ergonômico", "price": 189.00},
    ],
    "Moda": [
        {"product_id": "MOD-301", "name": "Tênis Esportivo Running", "price": 399.90},
        {"product_id": "MOD-302", "name": "Camiseta Algodão Egípcio", "price": 89.90},
        {"product_id": "MOD-303", "name": "Jaqueta Corta Vento", "price": 249.00},
    ],
    "Casa": [
        {"product_id": "CAS-401", "name": "Fritadeira Sem Óleo Air Fryer", "price": 379.00},
        {"product_id": "CAS-402", "name": "Robô Aspirador Inteligente", "price": 1499.00},
        {"product_id": "CAS-403", "name": "Cafeteira Espresso Automática", "price": 750.00},
    ],
    "Esportes": [
        {"product_id": "ESP-501", "name": "Bicicleta Mountain Bike Aro 29", "price": 1899.00},
        {"product_id": "ESP-502", "name": "Kit Halteres Ajustáveis 20kg", "price": 320.00},
        {"product_id": "ESP-503", "name": "Smartwatch Monitor Cardíaco", "price": 450.00},
    ],
}

REGIONS = ["Sudeste", "Sul", "Nordeste", "Centro-Oeste", "Norte"]
USERS = [f"USR_{i:04d}" for i in range(1, 101)]

# Histórico em memória de pedidos para atualização logística
ACTIVE_ORDERS = []


def get_random_product():
    cat = random.choice(list(CATEGORIES.keys()))
    prod = random.choice(CATEGORIES[cat])
    return cat, prod


def generate_event(out_of_order_prob=0.15, max_delay_sec=15):
    """Gera um evento JSON com event_time potencialmente fora de ordem."""
    now_ms = int(time.time() * 1000)
    
    # Simulação de atraso (Watermark testing)
    is_late = False
    event_time_ms = now_ms
    if random.random() < out_of_order_prob:
        delay_ms = random.randint(3000, max_delay_sec * 1000)
        event_time_ms = max(0, now_ms - delay_ms)
        is_late = True

    event_id = str(uuid.uuid4())
    user_id = random.choice(USERS)
    region = random.choice(REGIONS)

    # Distribuição de probabilidades de eventos
    # 55% cliques, 25% carrinho, 10% compra, 10% atualização logística
    dice = random.random()

    if dice < 0.55:
        # 1. Clique / Visualização de Produto
        category, product = get_random_product()
        event = {
            "event_id": event_id,
            "event_time": event_time_ms,
            "event_time_iso": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "ingestion_time": now_ms,
            "is_simulated_late": is_late,
            "event_type": "CLICK",
            "user_id": user_id,
            "region": region,
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": category,
            "price": product["price"]
        }

    elif dice < 0.80:
        # 2. Operações de Carrinho
        cart_action = random.choices(["CART_ADD", "CART_REMOVE"], weights=[0.8, 0.2])[0]
        category, product = get_random_product()
        quantity = random.randint(1, 3)
        event = {
            "event_id": event_id,
            "event_time": event_time_ms,
            "event_time_iso": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "ingestion_time": now_ms,
            "is_simulated_late": is_late,
            "event_type": cart_action,
            "user_id": user_id,
            "region": region,
            "cart_id": f"CRT_{user_id}",
            "product_id": product["product_id"],
            "category": category,
            "quantity": quantity,
            "item_price": product["price"],
            "subtotal": round(product["price"] * quantity, 2)
        }

    elif dice < 0.90:
        # 3. Finalização de Compra (Checkout)
        category, product = get_random_product()
        order_id = f"ORD_{random.randint(100000, 999999)}"
        quantity = random.randint(1, 2)
        total_amount = round(product["price"] * quantity + random.choice([0.0, 19.90, 39.90]), 2)
        
        # Salva o pedido na fila de logística
        ACTIVE_ORDERS.append({
            "order_id": order_id,
            "user_id": user_id,
            "region": region,
            "status": "DISPATCHED",
            "step": 0
        })
        # Mantém lista limitada
        if len(ACTIVE_ORDERS) > 200:
            ACTIVE_ORDERS.pop(0)

        event = {
            "event_id": event_id,
            "event_time": event_time_ms,
            "event_time_iso": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "ingestion_time": now_ms,
            "is_simulated_late": is_late,
            "event_type": "CHECKOUT_COMPLETED",
            "order_id": order_id,
            "user_id": user_id,
            "region": region,
            "items_count": quantity,
            "total_amount": total_amount,
            "payment_method": random.choice(["PIX", "CREDIT_CARD", "BOLETO"])
        }

    else:
        # 4. Status de Entrega e Logística
        if ACTIVE_ORDERS:
            order_track = random.choice(ACTIVE_ORDERS)
            order_id = order_track["order_id"]
            user_id = order_track["user_id"]
            region = order_track["region"]
        else:
            order_id = f"ORD_{random.randint(100000, 999999)}"

        status_progression = ["DISPATCHED", "IN_TRANSIT", "DELAYED", "OUT_FOR_DELIVERY", "DELIVERED", "FAILED_ATTEMPT"]
        status = random.choices(
            status_progression,
            weights=[0.15, 0.35, 0.20, 0.15, 0.10, 0.05]
        )[0]

        delay_minutes = random.randint(30, 240) if status in ["DELAYED", "FAILED_ATTEMPT"] else 0

        event = {
            "event_id": event_id,
            "event_time": event_time_ms,
            "event_time_iso": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat(),
            "ingestion_time": now_ms,
            "is_simulated_late": is_late,
            "event_type": "DELIVERY_UPDATE",
            "order_id": order_id,
            "user_id": user_id,
            "region": region,
            "delivery_status": status,
            "delay_minutes": delay_minutes,
            "carrier": random.choice(["Correios", "Loggi", "Total Express", "Jadlog"])
        }

    return event


def main():
    parser = argparse.ArgumentParser(description="Gerador Contínuo de Eventos de E-commerce (Big Data AP1)")
    parser.add_argument("--rate", type=float, default=5.0, help="Eventos gerados por segundo (padrão: 5)")
    parser.add_argument("--output", type=str, default="logs/ecommerce.log", help="Caminho do arquivo de log")
    parser.add_argument("--out-of-order-prob", type=float, default=0.15, help="Probabilidade de evento atrasado (0.0 a 1.0)")
    parser.add_argument("--max-delay-sec", type=int, default=20, help="Atraso máximo em segundos para eventos late")
    parser.add_argument("--limit", type=int, default=0, help="Limite total de eventos (0 = infinito)")
    parser.add_argument("--stdout", action="store_true", help="Também imprimir no stdout")

    args = parser.parse_args()

    output_path = Path(args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("🚀 Gerador de Eventos de E-commerce (Big Data em Tempo Real)")
    print(f"📁 Arquivo de saída: {output_path}")
    print(f"⚡ Taxa: {args.rate} eventos/segundo")
    print(f"⏳ Probabilidade fora de ordem: {args.out_of_order_prob * 100:.1f}% (atraso até {args.max_delay_sec}s)")
    print("=" * 70)

    sleep_interval = 1.0 / max(0.1, args.rate)
    count = 0

    with open(output_path, "a", encoding="utf-8") as log_file:
        try:
            while True:
                event = generate_event(
                    out_of_order_prob=args.out_of_order_prob,
                    max_delay_sec=args.max_delay_sec
                )
                line = json.dumps(event, ensure_ascii=False)
                log_file.write(line + "\n")
                log_file.flush()

                count += 1
                if args.stdout or count % 10 == 0:
                    late_tag = " [⏰ LATE EVENT]" if event["is_simulated_late"] else ""
                    print(f"[{count:05d}] {event['event_type']} | {event.get('product_id') or event.get('order_id') or ''} | User: {event['user_id']}{late_tag}")

                if args.limit > 0 and count >= args.limit:
                    print(f"✅ Limite de {args.limit} eventos atingido.")
                    break

                time.sleep(sleep_interval)

        except KeyboardInterrupt:
            print(f"\n🛑 Gerador interrompido pelo usuário. Total de eventos: {count}")


if __name__ == "__main__":
    main()

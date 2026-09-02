#!/bin/bash
# run_generator.sh - Inicia o gerador de eventos de e-commerce
set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$DIR")"

RATE=${1:-5}
OUT_OF_ORDER_PROB=${2:-0.15}
LOG_OUTPUT="$ROOT_DIR/logs/ecommerce.log"

echo "Iniciando gerador de eventos..."
echo "Taxa: $RATE eventos/s | Probabilidade Late: $OUT_OF_ORDER_PROB | Saída: $LOG_OUTPUT"

python3 "$DIR/gerador.py" \
  --rate "$RATE" \
  --out-of-order-prob "$OUT_OF_ORDER_PROB" \
  --output "$LOG_OUTPUT" \
  --stdout

#!/usr/bin/env bash
#
# rquest-bar.sh → exibe contador de updates para i3blocks e lemonbar
# Lê /tmp/rquest_updates.json gerado pelo update_notifier.py
#
# Uso:
#   Para i3blocks: adicionar no config → command=/usr/local/bin/rquest-bar.sh i3blocks
#   Para lemonbar: pipe do script → rquest-bar.sh lemonbar | lemonbar -p
#

JSON_FILE="/tmp/rquest_updates.json"

# Se arquivo não existir, força check inicial
if [ ! -f "$JSON_FILE" ]; then
    rquest notify check >/dev/null 2>&1 || true
fi

# Extrair valores com jq
if command -v jq >/dev/null; then
    TOTAL=$(jq -r '.updates_total // 0' "$JSON_FILE" 2>/dev/null)
    SECURITY=$(jq -r '.security_updates // 0' "$JSON_FILE" 2>/dev/null)
else
    TOTAL=0
    SECURITY=0
fi

ICON="⟳"
WARN="⚠"

MODE="$1"

case "$MODE" in
    i3blocks)
        # Linha principal
        if [ "$SECURITY" -gt 0 ]; then
            echo "$ICON $TOTAL ($SECURITY $WARN)"
            echo "$ICON $TOTAL ($SECURITY $WARN)"
            echo "#FF0000"
        else
            echo "$ICON $TOTAL"
            echo "$ICON $TOTAL"
            echo "#00FF00"
        fi
        ;;
    lemonbar)
        if [ "$SECURITY" -gt 0 ]; then
            echo "$ICON Updates: $TOTAL | Security: $SECURITY $WARN"
        else
            echo "$ICON Updates: $TOTAL"
        fi
        ;;
    *)
        echo "Uso: $0 [i3blocks|lemonbar]"
        exit 1
        ;;
esac

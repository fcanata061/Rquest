#!/usr/bin/env bash
set -euo pipefail

RQUEST_DIR="/usr/lib/rquest1.0"
BIN_DIR="/usr/bin"
CONFIG_DIR="/etc/rquest"
LOG_DIR="/var/log/rquest"
CACHE_DIR="/var/cache/rquest"
DB_DIR="/var/lib/rquest/db"
BUILDS_DIR="/var/lib/rquest/builds"
CONFIG_FILE="$CONFIG_DIR/config.yaml"

# Cores
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
RESET="\e[0m"

echo -e "${GREEN}[RQUEST] Instalador iniciado...${RESET}"

# 1. Criar diretórios principais
echo -e "${YELLOW}→ Criando diretórios do Rquest...${RESET}"
sudo mkdir -p "$RQUEST_DIR/modules" "$CONFIG_DIR" "$LOG_DIR" "$CACHE_DIR" "$DB_DIR" "$BUILDS_DIR"

# 2. Definir permissões corretas
echo -e "${YELLOW}→ Ajustando permissões...${RESET}"
sudo chown -R root:root "$RQUEST_DIR" "$CONFIG_DIR"
sudo chmod -R 755 "$RQUEST_DIR"
sudo chmod -R 755 "$LOG_DIR" "$CACHE_DIR" "$DB_DIR" "$BUILDS_DIR"
sudo chmod 700 "$DB_DIR"

# 3. Copiar módulos
echo -e "${YELLOW}→ Instalando módulos em $RQUEST_DIR...${RESET}"
sudo cp -r ./rquest1.0/modules/* "$RQUEST_DIR/modules/"

# 4. Copiar CLI wrapper
echo -e "${YELLOW}→ Instalando wrapper em $BIN_DIR/rquest...${RESET}"
sudo cp ./rquest1.0/rquest "$BIN_DIR/"
sudo chmod +x "$BIN_DIR/rquest"

# 5. Instalar config.yaml
if [[ -f "$CONFIG_FILE" ]]; then
    echo -e "${YELLOW}→ Backup do config.yaml existente...${RESET}"
    sudo mv "$CONFIG_FILE" "$CONFIG_FILE.bak"
fi

echo -e "${YELLOW}→ Instalando config.yaml em $CONFIG_FILE...${RESET}"
sudo cp ./rquest1.0/config.yaml "$CONFIG_FILE"
sudo chmod 644 "$CONFIG_FILE"

# 6. Dependências obrigatórias e opcionais do sistema
OBRIGATORIAS=(git patch fakeroot make gcc g++ tar wget curl)
OPCIONAIS=(llvm clang bwrap rsync aria2 btrfs zfs notify-send)

echo -e "${YELLOW}→ Checando dependências de sistema obrigatórias...${RESET}"
FALTANDO_OBR=()
for dep in "${OBRIGATORIAS[@]}"; do
    if ! command -v "$dep" &>/dev/null; then
        FALTANDO_OBR+=("$dep")
    fi
done

echo -e "${YELLOW}→ Checando dependências de sistema opcionais...${RESET}"
FALTANDO_OPT=()
for dep in "${OPCIONAIS[@]}"; do
    if ! command -v "$dep" &>/dev/null; then
        FALTANDO_OPT+=("$dep")
    fi
done

# 7. Dependências Python
PY_REQ=("pyyaml" "colorama" "rich" "sqlalchemy" "requests" "git" "gitpython")
PY_OPT=("psutil" "scikit-learn" "watchdog" "python-btrfs" "pyzfs")

echo -e "${YELLOW}→ Checando dependências Python obrigatórias...${RESET}"
FALTANDO_PY_OBR=()
for dep in "${PY_REQ[@]}"; do
    if ! python3 -c "import $dep" &>/dev/null; then
        FALTANDO_PY_OBR+=("$dep")
    fi
done

echo -e "${YELLOW}→ Checando dependências Python opcionais...${RESET}"
FALTANDO_PY_OPT=()
for dep in "${PY_OPT[@]}"; do
    if ! python3 -c "import $dep" &>/dev/null; then
        FALTANDO_PY_OPT+=("$dep")
    fi
done

# 8. Relatório final
echo -e "\n${GREEN}=== RELATÓRIO DE DEPENDÊNCIAS ===${RESET}"

if [[ ${#FALTANDO_OBR[@]} -eq 0 ]]; then
    echo -e "${GREEN}✔ Todas as dependências obrigatórias de sistema estão instaladas!${RESET}"
else
    echo -e "${RED}✘ Faltam dependências obrigatórias de sistema:${RESET} ${FALTANDO_OBR[*]}"
    echo -e "${RED}→ O Rquest NÃO funcionará corretamente até instalá-las.${RESET}"
fi

if [[ ${#FALTANDO_OPT[@]} -eq 0 ]]; then
    echo -e "${GREEN}✔ Todas as dependências opcionais de sistema estão instaladas!${RESET}"
else
    echo -e "${YELLOW}⚠ Faltam dependências opcionais:${RESET} ${FALTANDO_OPT[*]}"
    echo -e "${YELLOW}→ O Rquest funcionará sem elas, mas com funcionalidades limitadas.${RESET}"
fi

if [[ ${#FALTANDO_PY_OBR[@]} -eq 0 ]]; then
    echo -e "${GREEN}✔ Todas as libs Python obrigatórias estão instaladas!${RESET}"
else
    echo -e "${RED}✘ Faltam libs Python obrigatórias:${RESET} ${FALTANDO_PY_OBR[*]}"
    echo -e "${RED}→ Instale com: pip install ${FALTANDO_PY_OBR[*]}${RESET}"
fi

if [[ ${#FALTANDO_PY_OPT[@]} -eq 0 ]]; then
    echo -e "${GREEN}✔ Todas as libs Python opcionais estão instaladas!${RESET}"
else
    echo -e "${YELLOW}⚠ Faltam libs Python opcionais:${RESET} ${FALTANDO_PY_OPT[*]}"
    echo -e "${YELLOW}→ Instale com: pip install ${FALTANDO_PY_OPT[*]}${RESET}"
fi

echo -e "\n${GREEN}[RQUEST] Instalação finalizada.${RESET}"

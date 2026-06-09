#!/usr/bin/env bash
# =============================================================================
# setup.sh — Instalação automatizada do Data Lake on-premises
# Stack: MinIO + Iceberg REST + Spark + Livy + Trino + Cloudflare Tunnel
#
# Compatível com: Ubuntu 22.04 / 24.04 LTS (recomendado para servidor)
# Execução: sudo bash setup.sh
# =============================================================================
set -euo pipefail
IFS=$'\n\t'

# --- Cores -------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERRO]${NC}  $*" >&2; }
step()    { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════${NC}"; \
            echo -e "${BOLD}${CYAN}  $*${NC}"; \
            echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/.env"
CF_CONFIG_DIR="${SCRIPT_DIR}/config/cloudflare"
CF_CREDS_DIR="${SCRIPT_DIR}/config/cloudflare/credentials"

# =============================================================================
# BANNER
# =============================================================================
print_banner() {
  echo -e "${BOLD}${CYAN}"
  cat << 'EOF'
  ____        _        _        _
 |  _ \  __ _| |_ __ _| |      __ _| | _____
 | | | |/ _` | __/ _` | |     / _` | |/ / _ \
 | |_| | (_| | || (_| | |____| (_| |   <  __/
 |____/ \__,_|\__\__,_|______|\__,_|_|\_\___|

  On-Premises Lakehouse — Setup Automatizado
  MinIO · Iceberg · Spark · Livy · Trino · Cloudflare Tunnel
EOF
  echo -e "${NC}"
}

# =============================================================================
# PRÉ-REQUISITOS
# =============================================================================
check_root() {
  if [[ $EUID -ne 0 ]]; then
    error "Este script deve ser executado como root. Use: sudo bash setup.sh"
    exit 1
  fi
}

check_os() {
  step "Verificando sistema operacional"
  if [[ -f /etc/os-release ]]; then
    . /etc/os-release
    info "SO detectado: ${PRETTY_NAME}"
    if [[ "$ID" != "ubuntu" && "$ID" != "debian" ]]; then
      warn "SO não testado ($ID). O script foi desenvolvido para Ubuntu/Debian."
      read -r -p "Continuar mesmo assim? [s/N] " confirm
      [[ "$confirm" != "s" && "$confirm" != "S" ]] && exit 1
    fi
  else
    warn "Não foi possível identificar o SO. Continuando..."
  fi
}

# =============================================================================
# INSTALAÇÃO DE DEPENDÊNCIAS
# =============================================================================
install_docker() {
  step "Verificando/Instalando Docker"

  if command -v docker &>/dev/null; then
    DOCKER_VER=$(docker --version | awk '{print $3}' | tr -d ',')
    success "Docker já instalado: ${DOCKER_VER}"
  else
    info "Instalando Docker Engine..."
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg lsb-release

    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
      | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
      | tee /etc/apt/sources.list.d/docker.list > /dev/null

    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io \
      docker-buildx-plugin docker-compose-plugin

    systemctl enable --now docker
    success "Docker instalado com sucesso."
  fi

  # Verifica Docker Compose v2
  if docker compose version &>/dev/null; then
    success "Docker Compose v2 disponível."
  else
    error "Docker Compose plugin não encontrado. Instale manualmente."
    exit 1
  fi
}

install_cloudflared() {
  step "Verificando/Instalando cloudflared"

  if command -v cloudflared &>/dev/null; then
    CF_VER=$(cloudflared --version 2>&1 | awk '{print $3}')
    success "cloudflared já instalado: ${CF_VER}"
    return
  fi

  info "Instalando cloudflared..."
  ARCH=$(dpkg --print-architecture)  # amd64 ou arm64
  curl -fsSL "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH}.deb" \
    -o /tmp/cloudflared.deb
  dpkg -i /tmp/cloudflared.deb
  rm -f /tmp/cloudflared.deb
  success "cloudflared instalado: $(cloudflared --version 2>&1 | awk '{print $3}')"
}

install_extras() {
  step "Instalando utilitários auxiliares"
  apt-get install -y -qq curl jq openssl net-tools
  success "Utilitários instalados."
}

# =============================================================================
# COLETA DE PARÂMETROS (interativo)
# =============================================================================
collect_params() {
  step "Configuração do ambiente"

  echo ""
  echo -e "${BOLD}Preencha as informações abaixo. Campos em branco usam o padrão entre [colchetes].${NC}"
  echo ""

  # Domínio base
  read -r -p "  Domínio base (ex: bomgado.com.br): " INPUT_DOMAIN
  DOMAIN="${INPUT_DOMAIN:-bomgado.com.br}"

  # Subdomínios — padrão pré-definido, mas editável
  echo ""
  info "Subdomínios que serão criados (Enter para aceitar padrão):"
  read -r -p "  MinIO Console   [minio.${DOMAIN}]: "    INPUT && SUBDOMAIN_MINIO_CONSOLE="${INPUT:-minio.${DOMAIN}}"
  read -r -p "  MinIO S3 API    [s3.${DOMAIN}]: "       INPUT && SUBDOMAIN_MINIO_S3="${INPUT:-s3.${DOMAIN}}"
  read -r -p "  Trino           [trino.${DOMAIN}]: "    INPUT && SUBDOMAIN_TRINO="${INPUT:-trino.${DOMAIN}}"
  read -r -p "  Apache Livy     [livy.${DOMAIN}]: "     INPUT && SUBDOMAIN_LIVY="${INPUT:-livy.${DOMAIN}}"
  read -r -p "  Iceberg Catalog [iceberg.${DOMAIN}]: "  INPUT && SUBDOMAIN_ICEBERG="${INPUT:-iceberg.${DOMAIN}}"
  read -r -p "  Spark UI        [spark.${DOMAIN}]: "    INPUT && SUBDOMAIN_SPARK="${INPUT:-spark.${DOMAIN}}"

  # Credenciais MinIO
  echo ""
  info "Credenciais do MinIO:"
  read -r -p "  Usuário MinIO [minioadmin]: " INPUT && MINIO_USER="${INPUT:-minioadmin}"
  # Usa read -s para não ecoar senha na tela
  read -r -s -p "  Senha MinIO (Enter = gerar automaticamente): " INPUT
  echo ""
  if [[ -z "$INPUT" ]]; then
    MINIO_PASS=$(openssl rand -base64 24 | tr -d '/+=')
    info "Senha MinIO gerada automaticamente."
  else
    MINIO_PASS="$INPUT"
  fi

  # Cloudflare Tunnel Token
  echo ""
  echo -e "${YELLOW}  ─── Cloudflare Tunnel ───${NC}"
  echo "  1. Acesse: https://one.dash.cloudflare.com/"
  echo "  2. Vá em: Networks > Tunnels > Create a tunnel"
  echo "  3. Escolha 'Cloudflared', dê um nome (ex: datalake-bomgado)"
  echo "  4. Copie o token exibido na tela de instalação"
  echo ""
  # NOTA: não usamos read -s aqui porque o token é colado, não digitado
  # e o usuário precisa confirmar visualmente. Em produção, prefira variável de ambiente.
  read -r -p "  Cole o Cloudflare Tunnel Token: " CF_TUNNEL_TOKEN
  if [[ -z "$CF_TUNNEL_TOKEN" ]]; then
    warn "Token não fornecido. O cloudflared não será iniciado automaticamente."
    warn "Você pode adicioná-lo depois no arquivo .env (CLOUDFLARE_TUNNEL_TOKEN=...)"
    CF_TUNNEL_TOKEN="SEU_TOKEN_AQUI"
  fi

  # IP público (auto-detectado)
  echo ""
  DETECTED_IP=$(curl -s --max-time 5 https://api.ipify.org 2>/dev/null || echo "")
  if [[ -n "$DETECTED_IP" ]]; then
    read -r -p "  IP público detectado [${DETECTED_IP}]: " INPUT
    HOST_IP="${INPUT:-$DETECTED_IP}"
  else
    read -r -p "  IP público deste servidor: " HOST_IP
  fi
}

# =============================================================================
# GERAÇÃO DO .env
# =============================================================================
generate_env() {
  step "Gerando arquivo .env"

  cat > "${ENV_FILE}" << EOF
# =============================================================================
# .env — gerado automaticamente por setup.sh em $(date '+%Y-%m-%d %H:%M:%S')
# NÃO commite este arquivo. Ele contém credenciais de produção.
# =============================================================================

# --- Domínio e IP ---
DOMAIN=${DOMAIN}
HOST_IP=${HOST_IP}

# --- Subdomínios expostos pelo Cloudflare Tunnel ---
SUBDOMAIN_MINIO_CONSOLE=${SUBDOMAIN_MINIO_CONSOLE}
SUBDOMAIN_MINIO_S3=${SUBDOMAIN_MINIO_S3}
SUBDOMAIN_TRINO=${SUBDOMAIN_TRINO}
SUBDOMAIN_LIVY=${SUBDOMAIN_LIVY}
SUBDOMAIN_ICEBERG=${SUBDOMAIN_ICEBERG}
SUBDOMAIN_SPARK=${SUBDOMAIN_SPARK}

# --- MinIO ---
MINIO_ROOT_USER=${MINIO_USER}
MINIO_ROOT_PASSWORD=${MINIO_PASS}

# --- Cloudflare Tunnel ---
# Obtenha em: https://one.dash.cloudflare.com/ > Networks > Tunnels
CLOUDFLARE_TUNNEL_TOKEN=${CF_TUNNEL_TOKEN}
EOF

  chmod 600 "${ENV_FILE}"
  success ".env criado em ${ENV_FILE}"
}

# =============================================================================
# GERAÇÃO DA CONFIG DO CLOUDFLARE TUNNEL
# =============================================================================
generate_cloudflare_config() {
  step "Gerando configuração do Cloudflare Tunnel"

  mkdir -p "${CF_CONFIG_DIR}" "${CF_CREDS_DIR}"

  # A config.yml é usada apenas quando rodando com arquivo de credenciais
  # (fluxo alternativo ao token). Com token, o cloudflared ignora este arquivo,
  # mas mantemos para referência e para o modo de desenvolvimento local.
  cat > "${CF_CONFIG_DIR}/config.yml" << EOF
# =============================================================================
# Cloudflare Tunnel — Configuração de Ingress
# Gerado automaticamente por setup.sh
#
# MODO TOKEN (padrão deste setup):
#   O cloudflared usa a variável CLOUDFLARE_TUNNEL_TOKEN e as rotas
#   configuradas no dashboard Cloudflare (https://one.dash.cloudflare.com/).
#   Este arquivo serve apenas como referência das rotas esperadas.
#
# As rotas devem ser configuradas no dashboard em:
#   Networks > Tunnels > <seu tunnel> > Public Hostname
# =============================================================================

# Regras de ingress — espelhe exatamente estas no dashboard Cloudflare:
#
# Hostname                     Service
# ${SUBDOMAIN_MINIO_CONSOLE}   http://minio:9001       (MinIO Console Web)
# ${SUBDOMAIN_MINIO_S3}        http://minio:9000       (MinIO S3 API)
# ${SUBDOMAIN_TRINO}           http://trino:8080       (Trino Query Engine)
# ${SUBDOMAIN_LIVY}            http://livy:8998        (Apache Livy REST API)
# ${SUBDOMAIN_ICEBERG}         http://iceberg-rest:8181 (Iceberg REST Catalog)
# ${SUBDOMAIN_SPARK}           http://spark-master:8080 (Spark Master UI)
#
# Para usar com arquivo de credenciais (alternativa ao token):
#   cloudflared tunnel login
#   cloudflared tunnel create datalake-bomgado
#   cloudflared tunnel route dns datalake-bomgado ${SUBDOMAIN_LIVY}
#   ... (repetir para cada hostname)
EOF

  success "config/cloudflare/config.yml gerado."
}

# =============================================================================
# CONFIGURAÇÃO DE DNS VIA API CLOUDFLARE (opcional)
# =============================================================================
setup_cloudflare_dns_api() {
  step "Configuração automática de DNS no Cloudflare (opcional)"

  echo ""
  echo "  Para criar os registros DNS automaticamente, forneça:"
  read -r -p "  Cloudflare API Token (com permissão Zone:DNS:Edit) [pular]: " CF_API_TOKEN
  read -r -p "  Cloudflare Zone ID (encontrado no dashboard do domínio) [pular]: " CF_ZONE_ID
  read -r -p "  ID do Tunnel (visível no dashboard após criação) [pular]: " CF_TUNNEL_ID

  if [[ -z "$CF_API_TOKEN" || -z "$CF_ZONE_ID" || -z "$CF_TUNNEL_ID" ]]; then
    warn "DNS automático pulado. Crie manualmente os CNAMEs no Cloudflare:"
    echo ""
    for SUBDOMAIN in "$SUBDOMAIN_MINIO_CONSOLE" "$SUBDOMAIN_MINIO_S3" \
                     "$SUBDOMAIN_TRINO" "$SUBDOMAIN_LIVY" \
                     "$SUBDOMAIN_ICEBERG" "$SUBDOMAIN_SPARK"; do
      echo "    CNAME  ${SUBDOMAIN}  →  ${CF_TUNNEL_ID}.cfargotunnel.com"
    done
    echo ""
    return 0
  fi

  info "Criando registros CNAME no Cloudflare..."
  for SUBDOMAIN in "$SUBDOMAIN_MINIO_CONSOLE" "$SUBDOMAIN_MINIO_S3" \
                   "$SUBDOMAIN_TRINO" "$SUBDOMAIN_LIVY" \
                   "$SUBDOMAIN_ICEBERG" "$SUBDOMAIN_SPARK"; do
    HTTP_CODE=$(curl -s -o /tmp/cf_dns_resp.json -w "%{http_code}" \
      -X POST "https://api.cloudflare.com/client/v4/zones/${CF_ZONE_ID}/dns_records" \
      -H "Authorization: Bearer ${CF_API_TOKEN}" \
      -H "Content-Type: application/json" \
      --data "{
        \"type\": \"CNAME\",
        \"name\": \"${SUBDOMAIN}\",
        \"content\": \"${CF_TUNNEL_ID}.cfargotunnel.com\",
        \"ttl\": 1,
        \"proxied\": true
      }")

    if [[ "$HTTP_CODE" == "200" ]]; then
      success "CNAME criado: ${SUBDOMAIN}"
    else
      RESP=$(cat /tmp/cf_dns_resp.json)
      # Verifica se o erro é "already exists" (código 81057)
      if echo "$RESP" | grep -q '"code":81057'; then
        warn "CNAME já existe (ignorado): ${SUBDOMAIN}"
      else
        error "Falha ao criar CNAME ${SUBDOMAIN}: ${RESP}"
      fi
    fi
  done
}

# =============================================================================
# CRIAR ESTRUTURA DE DIRETÓRIOS
# =============================================================================
create_directories() {
  step "Criando estrutura de diretórios"

  mkdir -p \
    "${SCRIPT_DIR}/airflow/dags" \
    "${SCRIPT_DIR}/airflow/logs" \
    "${SCRIPT_DIR}/airflow/plugins" \
    "${SCRIPT_DIR}/config/spark/conf" \
    "${SCRIPT_DIR}/config/trino/etc/catalog" \
    "${SCRIPT_DIR}/config/cloudflare" \
    "${SCRIPT_DIR}/config/livy" \
    "${SCRIPT_DIR}/scripts" \
    "${SCRIPT_DIR}/jars"

  # Permissões necessárias para o Airflow (uid 50000 dentro do container)
  # Não aplicável aqui pois Airflow é externo — mantemos por consistência
  chmod -R 755 "${SCRIPT_DIR}/scripts" 2>/dev/null || true

  success "Diretórios criados."
}

# =============================================================================
# INICIAR O STACK
# =============================================================================
start_stack() {
  step "Baixando imagens Docker (pode demorar na primeira vez)"
  cd "${SCRIPT_DIR}"
  docker compose pull --quiet
  success "Imagens baixadas."

  step "Iniciando o stack"
  docker compose up -d --remove-orphans
  success "Stack iniciado."
}

# =============================================================================
# HEALTH CHECK
# =============================================================================
wait_for_service() {
  local NAME="$1"
  local URL="$2"
  local MAX_ATTEMPTS="${3:-30}"
  local INTERVAL="${4:-10}"

  info "Aguardando ${NAME}..."
  for i in $(seq 1 "$MAX_ATTEMPTS"); do
    if curl -sf --max-time 5 "$URL" > /dev/null 2>&1; then
      success "${NAME} está saudável! (${i}/${MAX_ATTEMPTS})"
      return 0
    fi
    echo -n "."
    sleep "$INTERVAL"
  done
  warn "${NAME} não respondeu após $((MAX_ATTEMPTS * INTERVAL))s. Verifique os logs."
  return 1
}

health_checks() {
  step "Verificando saúde dos serviços"
  echo ""

  wait_for_service "MinIO S3"          "http://localhost:9000/minio/health/live"  30 10
  wait_for_service "Iceberg REST"      "http://localhost:8181/v1/config"           20 10
  wait_for_service "Trino"             "http://localhost:8080/v1/info"             30 10
  wait_for_service "Spark Master"      "http://localhost:8090"                     20 10
  wait_for_service "Apache Livy"       "http://localhost:8998/sessions"            30 10
  wait_for_service "Cloudflare Tunnel" "http://localhost:8181/v1/config"           10  5 || true
}

# =============================================================================
# RESUMO FINAL
# =============================================================================
print_summary() {
  step "Setup concluído!"
  echo ""
  echo -e "${BOLD}Serviços locais:${NC}"
  echo -e "  MinIO Console    → http://localhost:9001  (${MINIO_USER} / <senha definida>)"
  echo -e "  MinIO S3 API     → http://localhost:9000"
  echo -e "  Trino            → http://localhost:8080"
  echo -e "  Spark Master UI  → http://localhost:8090"
  echo -e "  Apache Livy      → http://localhost:8998"
  echo -e "  Iceberg REST     → http://localhost:8181"
  echo ""
  echo -e "${BOLD}Serviços via Cloudflare Tunnel (após configurar DNS):${NC}"
  echo -e "  MinIO Console    → https://${SUBDOMAIN_MINIO_CONSOLE}"
  echo -e "  MinIO S3 API     → https://${SUBDOMAIN_MINIO_S3}"
  echo -e "  Trino            → https://${SUBDOMAIN_TRINO}"
  echo -e "  Apache Livy      → https://${SUBDOMAIN_LIVY}  ← usado pelo Airflow remoto"
  echo -e "  Iceberg Catalog  → https://${SUBDOMAIN_ICEBERG}"
  echo -e "  Spark UI         → https://${SUBDOMAIN_SPARK}"
  echo ""
  echo -e "${BOLD}Próximos passos no Airflow (${CYAN}airflow.bomgado.com.br${NC}${BOLD}):${NC}"
  echo -e "  Admin > Connections:"
  echo -e "    livy_default  → HTTP | https://${SUBDOMAIN_LIVY}  | port=443"
  echo -e "    hop_server    → HTTP | https://hop.bomgado.com.br | port=443"
  echo -e "    minio_health  → HTTP | https://${SUBDOMAIN_MINIO_S3}"
  echo -e "  Admin > Variables:"
  echo -e "    TRINO_HOST    → ${SUBDOMAIN_TRINO}"
  echo ""
  echo -e "${BOLD}Comandos úteis:${NC}"
  echo -e "  docker compose logs -f cloudflared     # logs do tunnel"
  echo -e "  docker compose logs -f livy            # logs do Livy"
  echo -e "  docker compose ps                      # status de todos os serviços"
  echo -e "  docker compose down                    # parar tudo"
  echo ""
}

# =============================================================================
# MAIN
# =============================================================================
main() {
  print_banner
  check_root
  check_os
  install_extras
  install_docker
  install_cloudflared
  collect_params
  create_directories
  generate_env
  generate_cloudflare_config
  setup_cloudflare_dns_api
  start_stack
  health_checks
  print_summary
}

main "$@"

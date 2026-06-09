#!/usr/bin/env bash
# =============================================================================
# upload-scripts-to-minio.sh
# Faz upload dos scripts PySpark para o MinIO (bucket datalake/scripts/)
# para que o Apache Livy possa referenciá-los via s3a://
#
# Execução: bash scripts/upload-scripts-to-minio.sh
# Pré-requisito: MinIO rodando e mc (minio client) disponível.
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../.env"

# Carrega variáveis do .env
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' "$ENV_FILE" | grep -v '^$' | xargs)
fi

MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="http://localhost:9000"

echo "[INFO] Configurando MinIO Client (mc)..."

# Usa o mc dentro do container minio-init para evitar instalar localmente
docker run --rm --network datalake_datalake-net \
  -v "${SCRIPT_DIR}:/scripts:ro" \
  minio/mc:latest \
  /bin/sh -c "
    mc alias set local ${MINIO_ENDPOINT} ${MINIO_USER} ${MINIO_PASS} &&
    mc mb --ignore-existing local/datalake &&
    mc cp /scripts/pyspark_iceberg_example.py local/datalake/scripts/pyspark_iceberg_example.py &&
    echo '[OK] Script enviado: s3a://datalake/scripts/pyspark_iceberg_example.py'
  "

echo "[OK] Upload concluído. O Apache Livy pode agora referenciar:"
echo "     s3a://datalake/scripts/pyspark_iceberg_example.py"

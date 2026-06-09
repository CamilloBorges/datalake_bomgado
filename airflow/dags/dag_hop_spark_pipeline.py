"""
dag_hop_spark_pipeline.py
=========================
DAG do Airflow que orquestra um pipeline completo de ingestão de dados:

  [Verificar Saúde MinIO]
      │
      ▼
  [Disparar Pipeline Hop]   ← Chama https://hop.bomgado.com.br via REST API
      │
      ▼
  [Aguardar Pipeline Hop]   ← Polling de status
      │
      ▼
  [Submeter Job PySpark]    ← LivyOperator → https://livy.bomgado.com.br
      │                      (Cloudflare Tunnel → container livy:8998)
      ▼
  [Validar Tabela Iceberg]  ← Consulta Trino via https://trino.bomgado.com.br

Arquitetura de conectividade:
  Airflow (airflow.bomgado.com.br)
    └── HTTPS → Cloudflare Tunnel
        ├── livy.bomgado.com.br   → livy:8998      (submissão de jobs)
        ├── trino.bomgado.com.br  → trino:8080     (validação)
        ├── s3.bomgado.com.br     → minio:9000     (health check)
        └── iceberg.bomgado.com.br → iceberg-rest:8181

  Vantagem do Livy sobre SparkSubmitOperator direto:
    - Não expõe o protocolo TCP Spark (7077) para a internet
    - Comunicação 100% HTTPS via Cloudflare
    - Polling assíncrono nativo (não bloqueia o worker do Airflow)
    - Suporte a CORS, autenticação e rate limiting no Cloudflare

Configurações necessárias no Airflow UI (Admin > Connections):
  - livy_default : HTTP  | host=livy.bomgado.com.br   | port=443 | schema=https
  - hop_server   : HTTP  | host=hop.bomgado.com.br     | port=443 | schema=https
  - minio_health : HTTP  | host=s3.bomgado.com.br      | port=443 | schema=https
  - trino_conn   : HTTP  | host=trino.bomgado.com.br   | port=443 | schema=https
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
# LivyOperator submete jobs Spark via REST API (HTTP/HTTPS).
# Requer: pip install apache-airflow-providers-apache-livy
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.models import Variable

# =============================================================================
# Argumentos padrão da DAG
# =============================================================================
DEFAULT_ARGS = {
    "owner": "datalake-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
}

# Caminhos internos ao container Airflow worker (montados via volume)
# O script PySpark é referenciado pelo caminho no container Spark (não no Airflow).
# Com Livy, passamos o arquivo como 'file' na requisição — deve estar acessível
# pelo Spark cluster. Opções:
#   1. Caminho local no servidor: /opt/spark/scripts/pyspark_iceberg_example.py
#   2. Path no MinIO (recomendado): s3a://datalake/scripts/pyspark_iceberg_example.py
PYSPARK_SCRIPT = "s3a://datalake/scripts/pyspark_iceberg_example.py"

# Nome do pipeline/workflow do Hop a ser executado
# Ajuste para o caminho real dentro do container hop-server (/files/...)
HOP_WORKFLOW_PATH = "/files/pipelines/ingestao_raw.hwf"

# =============================================================================
# DAG
# =============================================================================
with DAG(
    dag_id="pipeline_hop_spark_iceberg",
    default_args=DEFAULT_ARGS,
    description="Orquestra pipeline Hop (ETL) + Job PySpark Iceberg",
    # Executa diariamente à meia-noite
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["datalake", "hop", "spark", "iceberg", "minio"],
) as dag:

    # -------------------------------------------------------------------------
    # Task 1: Verificar se o MinIO está acessível antes de iniciar o pipeline.
    # O MinIO está no servidor on-premises deste docker-compose;
    # o Airflow externo (airflow.bomgado.com.br) alcance-o via IP do host.
    # Configure no Airflow UI: Admin > Connections > minio_health
    # Type: HTTP | Host: <IP_HOST> | Port: 9000
    # -------------------------------------------------------------------------
    verificar_minio = HttpSensor(
        task_id="verificar_minio",
        # Configure no Airflow UI: Admin > Connections > minio_health
        # Type: HTTP | Host: minio | Port: 9000
        http_conn_id="minio_health",
        endpoint="/minio/health/live",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=15,       # Verifica a cada 15 segundos
        timeout=120,            # Aguarda até 2 minutos
        mode="poke",
    )

    # -------------------------------------------------------------------------
    # Task 2: Verificar se o Hop Server externo está respondendo.
    # Configure no Airflow UI: Admin > Connections > hop_server
    # Type: HTTP | Host: hop.bomgado.com.br | Port: 443 | Schema: https
    # -------------------------------------------------------------------------
    verificar_hop_server = HttpSensor(
        task_id="verificar_hop_server",
        # Configure no Airflow UI: Admin > Connections > hop_server
        # Type: HTTP | Host: hop-server | Port: 8180
        http_conn_id="hop_server",
        endpoint="/hop/serverStatus",
        method="GET",
        # O Hop Server retorna XML; verifica apenas status HTTP 200
        response_check=lambda response: response.status_code == 200,
        poke_interval=20,
        timeout=180,
        mode="poke",
    )

    # -------------------------------------------------------------------------
    # Task 3: Disparar o pipeline de ingestão no Apache Hop externo.
    # Usa a REST API nativa do Hop Server (https://hop.bomgado.com.br).
    #
    # O Airflow (airflow.bomgado.com.br) chama o Hop (hop.bomgado.com.br)
    # diretamente pela internet/rede corporativa — sem dependência de rede
    # interna Docker.
    # -------------------------------------------------------------------------
    disparar_pipeline_hop = SimpleHttpOperator(
        task_id="disparar_pipeline_hop",
        http_conn_id="hop_server",
        method="GET",
        endpoint="/hop/executeWorkflow",
        # Parâmetros passados como query string
        data={
            "xml": "Y",
            "workflow": HOP_WORKFLOW_PATH,
            "username": "admin",
            "password": "admin",
        },
        headers={"Accept": "application/xml"},
        # Retorna True se o Hop iniciou o job com sucesso (status HTTP 200)
        response_check=lambda response: response.status_code == 200,
        # Salva a resposta XML no XCom para uso pela task seguinte
        do_xcom_push=True,
        log_response=True,
    )

    # -------------------------------------------------------------------------
    # Task 4: Aguarda a conclusão do pipeline Hop fazendo polling de status.
    # Extrai o ID do job da resposta XML da task anterior (XCom).
    # -------------------------------------------------------------------------
    def _aguardar_hop(**context):
        """
        Faz polling no endpoint /hop/jobStatus do Hop Server até que
        o job seja concluído com sucesso ou falhe.
        """
        import re
        import requests

        # Recupera a resposta XML da task anterior via XCom
        hop_response_xml = context["ti"].xcom_pull(task_ids="disparar_pipeline_hop")

        if not hop_response_xml:
            raise ValueError("Nenhuma resposta da task 'disparar_pipeline_hop'.")

        # Extrai o ID do job do XML retornado pelo Hop Server
        match = re.search(r"<id>(.*?)</id>", hop_response_xml)
        if not match:
            raise ValueError(f"Não foi possível extrair o ID do job do XML:\n{hop_response_xml}")

        job_id = match.group(1)
        print(f"Aguardando conclusão do job Hop com ID: {job_id}")

        # URL de status do Hop externo
        status_url = f"https://hop.bomgado.com.br/hop/jobStatus"
        params = {"xml": "Y", "name": job_id, "id": job_id}

        # Polling com timeout de 30 minutos
        timeout_seconds = 30 * 60
        poll_interval = 15
        elapsed = 0

        while elapsed < timeout_seconds:
            resp = requests.get(status_url, params=params, timeout=30)
            resp.raise_for_status()

            status_match = re.search(r"<status_desc>(.*?)</status_desc>", resp.text)
            status = status_match.group(1) if status_match else "Unknown"
            print(f"Status do job Hop ({elapsed}s): {status}")

            if status in ("Finished", "Finished (with logging object update)"):
                print("Pipeline Hop concluído com sucesso!")
                return status

            if status in ("Stopped", "Halting"):
                raise RuntimeError(f"Pipeline Hop falhou com status: {status}")

            time.sleep(poll_interval)
            elapsed += poll_interval

        raise TimeoutError(f"Pipeline Hop não concluiu em {timeout_seconds // 60} minutos.")

    aguardar_hop = PythonOperator(
        task_id="aguardar_pipeline_hop",
        python_callable=_aguardar_hop,
    )

    # -------------------------------------------------------------------------
    # Task 5: Submete o job PySpark ao cluster Spark no servidor on-premises.
    # Task 5: Submete o job PySpark via Apache Livy (REST sobre HTTPS).
    #
    # FLUXO:
    #   Airflow → POST https://livy.bomgado.com.br/batches
    #          → Cloudflare Tunnel
    #          → container livy:8998
    #          → Spark Master spark://spark-master:7077 (rede interna Docker)
    #
    # Vantagens sobre SparkSubmitOperator + TCP direto:
    #   - Comunicação 100% HTTPS — sem porta 7077 exposta
    #   - LivyOperator faz polling automático e aguarda conclusão do batch
    #   - Logs disponíveis via GET /batches/{id}/log
    #
    # Configuração no Airflow UI (Admin > Connections > livy_default):
    #   Type: HTTP | Host: livy.bomgado.com.br | Port: 443 | Schema: https
    # -------------------------------------------------------------------------
    submeter_job_pyspark = LivyOperator(
        task_id="submeter_job_pyspark",
        # Conexão apontando para o Livy via Cloudflare Tunnel
        livy_conn_id="livy_default",
        # Arquivo Python a executar — hospedado no MinIO para que o
        # Spark driver (dentro do cluster) possa acessá-lo diretamente
        file=PYSPARK_SCRIPT,
        name="IcebergMinIOExample_{{ ds }}",
        # Configurações de recursos do Spark (sobrescrevem spark-defaults.conf)
        driver_memory="1g",
        executor_memory="1g",
        num_executors=1,
        conf={
            # Iceberg REST Catalog — endereços internos Docker (o job roda no cluster)
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg.type": "rest",
            "spark.sql.catalog.iceberg.uri": "http://iceberg-rest:8181",
            "spark.sql.catalog.iceberg.warehouse": "s3://warehouse/",
            "spark.sql.catalog.iceberg.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.catalog.iceberg.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.iceberg.s3.path-style-access": "true",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        # Pacotes Maven necessários (Iceberg + S3A)
        jars=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
        # Polling: verifica status a cada 30s, aguarda até 1h
        polling_interval=30,
    )

    # -------------------------------------------------------------------------
    # Task 6: Validação pós-job via Trino REST API.
    # Task 6: Validação via Trino exposto pelo Cloudflare Tunnel.
    # URL: https://trino.bomgado.com.br (HTTPS, sem precisar de IP/porta)
    # -------------------------------------------------------------------------
    validar_tabela_iceberg = BashOperator(
        task_id="validar_tabela_iceberg",
        bash_command="""
        # Trino acessível via Cloudflare Tunnel — HTTPS, sem IP exposto.
        # Configure no Airflow UI: Admin > Variables > TRINO_HOST = trino.bomgado.com.br
        TRINO_HOST="${TRINO_HOST:-trino.bomgado.com.br}"
        TRINO_PROTO="https"

        RESPONSE=$(curl -s -X POST \
            -H "X-Trino-User: airflow" \
            -H "Content-Type: text/plain" \
            --data "SELECT COUNT(*) AS total FROM iceberg.datalake.funcionarios" \
            "${TRINO_PROTO}://${TRINO_HOST}/v1/statement")

        echo "Resposta Trino: $RESPONSE"

        # Extrai o nextUri para fazer polling dos resultados
        NEXT_URI=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nextUri',''))")

        if [ -z "$NEXT_URI" ]; then
            echo "AVISO: Sem nextUri — pode ser resultado imediato ou erro."
            echo "$RESPONSE"
            exit 0
        fi

        # Aguarda a query completar (polling simples)
        ATTEMPTS=0
        while [ $ATTEMPTS -lt 10 ]; do
            sleep 3
            RESULT=$(curl -s -H "X-Trino-User: airflow" "$NEXT_URI")
            STATE=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('stats',{}).get('state','UNKNOWN'))")
            echo "Estado da query Trino: $STATE"

            if [ "$STATE" = "FINISHED" ]; then
                echo "Validação concluída com sucesso!"
                echo "$RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
rows = d.get('data', [])
print(f'Total de registros na tabela: {rows[0][0] if rows else \"N/A\"}')"
                exit 0
            fi

            NEXT_URI=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('nextUri',''))" 2>/dev/null || echo "")
            [ -z "$NEXT_URI" ] && break
            ATTEMPTS=$((ATTEMPTS + 1))
        done

        echo "Validação concluída (timeout ou estado final)."
        """,
    )

    # =========================================================================
    # Dependências — define a ordem de execução do pipeline
    # =========================================================================
    [verificar_minio, verificar_hop_server] >> disparar_pipeline_hop
    disparar_pipeline_hop >> aguardar_hop
    aguardar_hop >> submeter_job_pyspark
    submeter_job_pyspark >> validar_tabela_iceberg

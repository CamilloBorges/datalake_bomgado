# Data Lake On-Premises — MinIO + Iceberg + Airflow + Hop + Spark + Trino

## Estrutura do Projeto

```
.
├── docker-compose.yml              # Orquestra todos os serviços
├── .env                            # Credenciais (não commitar em produção)
├── .gitignore
├── config/
│   ├── spark/conf/
│   │   └── spark-defaults.conf     # Iceberg REST Catalog + S3A/MinIO
│   └── trino/etc/
│       ├── config.properties
│       ├── jvm.config
│       ├── node.properties
│       └── catalog/
│           └── iceberg.properties  # Catálogo Iceberg no Trino
├── scripts/
│   └── pyspark_iceberg_example.py  # Job PySpark de exemplo
├── airflow/
│   ├── dags/
│   │   └── dag_hop_spark_pipeline.py  # DAG orquestradora
│   ├── logs/
│   └── plugins/
└── jars/                           # JARs extras do Spark (opcional)
```

## Portas dos Serviços

| Serviço           | URL                        | Credenciais         |
|-------------------|----------------------------|---------------------|
| MinIO Console     | http://localhost:9001       | minioadmin/minioadmin |
| MinIO S3 API      | http://localhost:9000       | —                   |
| Airflow Webserver | http://localhost:8085       | airflow/airflow     |
| Trino             | http://localhost:8080       | qualquer usuário    |
| Spark Master UI   | http://localhost:8090       | —                   |
| Spark Worker UI   | http://localhost:8091       | —                   |
| Hop Server        | http://localhost:8180       | admin/admin         |
| Iceberg REST      | http://localhost:8181       | —                   |

## Início Rápido

```bash
# 1. Suba o ambiente completo
docker compose up -d

# 2. Acompanhe os logs da inicialização
docker compose logs -f airflow-init minio-init

# 3. Submeta o job PySpark de exemplo manualmente
docker exec spark-master \
  /opt/bitnami/spark/bin/spark-submit \
  /opt/spark/scripts/pyspark_iceberg_example.py

# 4. Consulte a tabela via Trino CLI
docker exec -it trino trino --catalog iceberg --schema datalake
trino> SELECT * FROM funcionarios;
```

## Conexões necessárias no Airflow UI

Acesse **Admin > Connections** e crie:

| Conn Id        | Type  | Host         | Port |
|----------------|-------|--------------|------|
| `spark_default`| Spark | spark-master | 7077 |
| `hop_server`   | HTTP  | hop-server   | 8180 |
| `minio_health` | HTTP  | minio        | 9000 |

datalake_bomgado
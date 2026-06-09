"""
pyspark_iceberg_example.py
==========================
Script PySpark de exemplo que demonstra:
  1. Criação de um namespace (database) no Iceberg REST Catalog
  2. Escrita de um DataFrame fictício como tabela Iceberg no MinIO
  3. Leitura e validação dos dados gravados
  4. Operações DML avançadas suportadas pelo Iceberg (UPDATE / DELETE)
  5. Time Travel — consulta a versões anteriores da tabela

Pré-requisitos (já configurados via spark-defaults.conf):
  - Iceberg REST Catalog em http://iceberg-rest:8181
  - MinIO S3 em http://minio:9000 (bucket 'warehouse' e 'datalake' criados)
  - JARs: iceberg-spark-runtime, hadoop-aws, aws-java-sdk-bundle

Execução local (dentro do container spark-master):
  spark-submit /opt/spark/scripts/pyspark_iceberg_example.py

Execução via spark-submit externo (a partir do host):
  spark-submit --master spark://localhost:7077 \
               /opt/spark/scripts/pyspark_iceberg_example.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.functions import col, current_date
from datetime import date

# =============================================================================
# 1. SparkSession
# As configurações de catálogo e S3A são lidas do spark-defaults.conf.
# Aqui sobrescrevemos apenas o nome da aplicação.
# =============================================================================
spark = (
    SparkSession.builder
    .appName("IcebergMinIOExample")
    # Se executar fora do cluster (ex: modo local), defina o master aqui:
    # .master("spark://spark-master:7077")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("Spark Version   :", spark.version)
print("Catálogos ativos:", spark.catalog.listCatalogs() if hasattr(spark.catalog, 'listCatalogs') else "N/A")
print("=" * 60)

# =============================================================================
# 2. Namespace (equivalente a um "database" / "schema")
# Criado dentro do catálogo 'iceberg', que grava os metadados no MinIO
# no path: s3://warehouse/<namespace>/
# =============================================================================
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.datalake")
print("Namespace 'iceberg.datalake' verificado/criado.")

# =============================================================================
# 3. Dados fictícios — tabela de funcionários
# =============================================================================
schema = StructType([
    StructField("id",           IntegerType(), nullable=False),
    StructField("nome",         StringType(),  nullable=False),
    StructField("departamento", StringType(),  nullable=True),
    StructField("cargo",        StringType(),  nullable=True),
    StructField("salario",      DoubleType(),  nullable=True),
    StructField("data_admissao", StringType(), nullable=True),  # ISO 8601
])

registros = [
    (1,  "Ana Silva",       "Engenharia de Dados", "Engenheira Sênior",    8_500.00, "2022-03-15"),
    (2,  "Bruno Costa",     "Arquitetura",         "Arquiteto de Dados",   9_200.00, "2021-07-01"),
    (3,  "Carla Mendes",    "Produto",             "Product Manager",      7_800.00, "2023-01-20"),
    (4,  "Diego Rocha",     "Engenharia de Dados", "Engenheiro Pleno",     6_900.00, "2020-11-10"),
    (5,  "Evelyn Santos",   "Engenharia de Dados", "Engenheira de ML",    10_500.00, "2019-05-05"),
    (6,  "Felipe Nunes",    "DevOps",              "SRE",                  8_100.00, "2021-09-14"),
    (7,  "Gabriela Lima",   "Arquitetura",         "Arquiteta de Soluções",9_800.00, "2018-03-22"),
    (8,  "Henrique Matos",  "Produto",             "UX Researcher",        6_500.00, "2024-02-01"),
]

df_funcionarios = spark.createDataFrame(registros, schema)

print("\nDataFrame criado:")
df_funcionarios.show(truncate=False)

# =============================================================================
# 4. Escrita no formato Iceberg
# Catálogo : iceberg
# Namespace : datalake
# Tabela    : funcionarios
# Path final: s3://warehouse/datalake/funcionarios/
#
# writeTo().createOrReplace() é equivalente a CREATE OR REPLACE TABLE AS SELECT.
# O Iceberg grava os dados em Parquet e os metadados (manifests, snapshots)
# como arquivos JSON/Avro dentro da pasta 'metadata/' da tabela.
# =============================================================================
(
    df_funcionarios
    .writeTo("iceberg.datalake.funcionarios")
    .using("iceberg")
    .tableProperty("write.format.default", "parquet")
    # Compactação: habilita ZSTD para melhor compressão
    .tableProperty("write.parquet.compression-codec", "zstd")
    # Particionamento por departamento (omitido aqui para simplicidade;
    # descomente abaixo para habilitar)
    # .partitionedBy("departamento")
    .createOrReplace()
)

print("\nTabela 'iceberg.datalake.funcionarios' gravada com sucesso no MinIO!")

# =============================================================================
# 5. Leitura de volta para validação
# =============================================================================
df_lido = spark.table("iceberg.datalake.funcionarios")
print("\nDados lidos do MinIO via Iceberg:")
df_lido.show(truncate=False)
print(f"Total de registros: {df_lido.count()}")

# =============================================================================
# 6. DML Avançado — UPDATE e DELETE (suportados nativamente pelo Iceberg)
# Tabelas Parquet normais NÃO suportam UPDATE/DELETE; Iceberg sim.
# =============================================================================
print("\nAplicando UPDATE: aumento salarial para Engenharia de Dados...")
spark.sql("""
    UPDATE iceberg.datalake.funcionarios
    SET    salario = salario * 1.10
    WHERE  departamento = 'Engenharia de Dados'
""")

print("Aplicando DELETE: removendo cargos de Produto...")
spark.sql("""
    DELETE FROM iceberg.datalake.funcionarios
    WHERE  departamento = 'Produto'
""")

print("\nEstado atual após DML:")
spark.table("iceberg.datalake.funcionarios").show(truncate=False)

# =============================================================================
# 7. Time Travel — consulta o snapshot original (antes dos DMLs)
# O Iceberg mantém todos os snapshots; versão 0 = estado inicial.
# =============================================================================
print("\nHistórico de snapshots (Time Travel):")
spark.sql("SELECT * FROM iceberg.datalake.funcionarios.history").show(truncate=False)

print("\nDados do snapshot inicial (antes do UPDATE/DELETE):")
spark.sql("""
    SELECT * FROM iceberg.datalake.funcionarios VERSION AS OF 1
""").show(truncate=False)

# =============================================================================
# 8. Metadados da tabela
# =============================================================================
print("\nArquivos de dados físicos da tabela:")
spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM iceberg.datalake.funcionarios.files") \
     .show(truncate=False)

spark.stop()
print("\nJob PySpark finalizado com sucesso.")

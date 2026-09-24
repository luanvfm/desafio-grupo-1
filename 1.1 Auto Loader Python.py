# Databricks notebook source
# DBTITLE 1,Configuração — catálogo, paths e imports
# ============================================================
# AUTO LOADER — Ingestão Incremental para Camada Bronze
# ============================================================
# Implementação futura para cenários com APIs ou ingestão contínua.
# Baseado no padrão do notebook AutoLoader-Bronze.
# Usa cloudFiles (Auto Loader) com trigger(availableNow=True).
#
# ESCOPO B (hands-on): cada tabela Bronze deve conter data/hora de
# ingestão (updated) E arquivo de origem (arquivo_origem).
#
# PREVENÇÃO "sem arquivos novos":
#   - try/except verifica se a tabela já existe (evita reprocessar)
#   - trigger(availableNow=True): processa apenas arquivos disponíveis e para
#   - Se nenhum arquivo novo chegou, o Auto Loader completa sem erro
#
# MIGRAÇÃO: as tabelas bronze atuais são tabelas regulares (batch).
# Para ativar o Auto Loader, remova-as primeiro executando:
#   spark.sql("DROP TABLE IF EXISTS desafio_grupo1.bronze.bronze_clientes")
#   spark.sql("DROP TABLE IF EXISTS desafio_grupo1.bronze.bronze_vendas")
#   spark.sql("DROP TABLE IF EXISTS desafio_grupo1.bronze.bronze_suporte")
# ============================================================

from pyspark.sql.functions import col, current_timestamp, to_date

catalog = "desafio_grupo1"
source_path = f"/Volumes/{catalog}/landing/files/"

# Paths para schema e checkpoint de cada tabela
schema_paths = {
    "clientes": f"/Volumes/{catalog}/landing/metadata/clientes/schema",
    "vendas": f"/Volumes/{catalog}/landing/metadata/vendas/schema",
    "suporte": f"/Volumes/{catalog}/landing/metadata/suporte/schema",
}

checkpoint_paths = {
    "clientes": f"/Volumes/{catalog}/landing/metadata/clientes/checkpoint",
    "vendas": f"/Volumes/{catalog}/landing/metadata/vendas/checkpoint",
    "suporte": f"/Volumes/{catalog}/landing/metadata/suporte/checkpoint",
}

# COMMAND ----------

# DBTITLE 1,Auto Loader — bronze_clientes
# ============================================================
# 1. AUTO LOADER — bronze_clientes
# ============================================================
# Verifica se a tabela já existe para evitar re-processar dados
try:
    existing_count = spark.table(f"{catalog}.bronze.bronze_clientes").count()
    print(f"Tabela {catalog}.bronze.bronze_clientes já existe com {existing_count} linhas.")
    print("Pulando o Auto Loader — dados já foram carregados.")
except:
    print("Tabela não encontrada. Iniciando Auto Loader para bronze_clientes...")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.pathGlobFilter", "clientes*.csv")
        .option("cloudFiles.schemaLocation", schema_paths["clientes"])
        .option("header", "true")
        .load(source_path)
    )

    # Transformação: tipagem + arquivo_origem + updated
    transformed = df.select(
        col("id_cliente").cast("int").alias("id_cliente"),
        col("nome"),
        col("sexo"),
        to_date(col("data_nascimento")).alias("data_nascimento"),
        col("cidade"),
        col("estado"),
        to_date(col("data_cadastro")).alias("data_cadastro"),
        col("_metadata.file_name").alias("arquivo_origem"),
        current_timestamp().alias("updated"),
    )

    query = (
        transformed.writeStream
        .option("checkpointLocation", checkpoint_paths["clientes"])
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.bronze_clientes")
    )

    query.awaitTermination()
    print("Auto Loader bronze_clientes concluído com sucesso!")

# COMMAND ----------

# DBTITLE 1,Auto Loader — bronze_vendas
# ============================================================
# 2. AUTO LOADER — bronze_vendas
# ============================================================
try:
    existing_count = spark.table(f"{catalog}.bronze.bronze_vendas").count()
    print(f"Tabela {catalog}.bronze.bronze_vendas já existe com {existing_count} linhas.")
    print("Pulando o Auto Loader — dados já foram carregados.")
except:
    print("Tabela não encontrada. Iniciando Auto Loader para bronze_vendas...")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.pathGlobFilter", "vendas*.csv")
        .option("cloudFiles.schemaLocation", schema_paths["vendas"])
        .option("header", "true")
        .load(source_path)
    )

    transformed = df.select(
        col("id_venda").cast("int").alias("id_venda"),
        col("id_cliente").cast("int").alias("id_cliente"),
        to_date(col("data_venda")).alias("data_venda"),
        col("produto"),
        col("quantidade").cast("int").alias("quantidade"),
        col("valor_total").cast("decimal(10,2)").alias("valor_total"),
        col("_metadata.file_name").alias("arquivo_origem"),
        current_timestamp().alias("updated"),
    )

    query = (
        transformed.writeStream
        .option("checkpointLocation", checkpoint_paths["vendas"])
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.bronze_vendas")
    )

    query.awaitTermination()
    print("Auto Loader bronze_vendas concluído com sucesso!")

# COMMAND ----------

# DBTITLE 1,Auto Loader — bronze_suporte
# ============================================================
# 3. AUTO LOADER — bronze_suporte
# ============================================================
try:
    existing_count = spark.table(f"{catalog}.bronze.bronze_suporte").count()
    print(f"Tabela {catalog}.bronze.bronze_suporte já existe com {existing_count} linhas.")
    print("Pulando o Auto Loader — dados já foram carregados.")
except:
    print("Tabela não encontrada. Iniciando Auto Loader para bronze_suporte...")

    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.pathGlobFilter", "suporte*.csv")
        .option("cloudFiles.schemaLocation", schema_paths["suporte"])
        .option("header", "true")
        .load(source_path)
    )

    transformed = df.select(
        col("id_interacao").cast("int").alias("id_interacao"),
        col("id_cliente").cast("int").alias("id_cliente"),
        col("canal"),
        col("tipo_problema"),
        col("tempo_resolucao").cast("int").alias("tempo_resolucao"),
        col("satisfacao_cliente").cast("int").alias("satisfacao_cliente"),
        col("_metadata.file_name").alias("arquivo_origem"),
        current_timestamp().alias("updated"),
    )

    query = (
        transformed.writeStream
        .option("checkpointLocation", checkpoint_paths["suporte"])
        .trigger(availableNow=True)
        .toTable(f"{catalog}.bronze.bronze_suporte")
    )

    query.awaitTermination()
    print("Auto Loader bronze_suporte concluído com sucesso!")

# COMMAND ----------

# DBTITLE 1,Verificação
# ============================================================
# VERIFICAÇÃO
# ============================================================
for tabela in ["bronze_clientes", "bronze_vendas", "bronze_suporte"]:
    total = spark.table(f"{catalog}.bronze.{tabela}").count()
    print(f"{tabela}: {total} registros")

print("\nAmostra bronze_clientes (arquivo_origem + updated):")
spark.table(f"{catalog}.bronze.bronze_clientes").select(
    "id_cliente", "nome", "arquivo_origem", "updated"
).show(5)
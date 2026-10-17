# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Capa Bronze: Ingesta Cruda del Catálogo Horus
# MAGIC Lee el archivo `propiedades_horus_completo.csv` desde un Volume de Unity Catalog y lo persiste en formato **Delta Lake** con columnas de linaje y auditoría.

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Configuración de Esquema y Volumen en Unity Catalog
# (En Databricks Free Edition el catálogo por defecto suele ser 'workspace' o 'main')
CATALOG_NAME = "workspace"
SCHEMA_NAME = "horus_lakehouse"
VOLUME_NAME = "landing_zone"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")

ruta_csv_volumen = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/propiedades_horus_completo.csv"
tabla_bronze = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_propiedades_raw"

print(f"✅ Volumen listo. Subí tu archivo CSV a: {ruta_csv_volumen}")

# COMMAND ----------

# 2. Lectura del CSV con soporte para textos multilínea (ficha_descripcion_completa)
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .option("multiLine", "true")
    .option("escape", '"')
    .option("quote", '"')
    .option("inferSchema", "false") # En Bronze todo entra como STRING para no perder datos
    .load(ruta_csv_volumen)
)

# 3. Agregado de metadatos de auditoría e ingesta en Delta Lake
df_bronze = (
    df_raw
    .withColumn("_ingestion_timestamp", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_bronze)
)

print(f"🎉 Tabla Bronze creada con éxito: {tabla_bronze} | Total filas: {df_bronze.count()}")
display(spark.table(tabla_bronze).limit(10))
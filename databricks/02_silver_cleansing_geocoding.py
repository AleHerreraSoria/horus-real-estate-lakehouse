# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Capa Silver: Limpieza, Tipado, Reglas de Calidad y Enriquecimiento Geográfico
# MAGIC Transforma la tabla Bronze en un modelo dimensional limpio (`silver_propiedades_limpias`), normalizando monedas, superficies, barrios y servicios.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

CATALOG_NAME = "workspace"
SCHEMA_NAME = "horus_lakehouse"
tabla_bronze = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_propiedades_raw"
tabla_silver = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_propiedades_limpias"

df_bronze = spark.table(tabla_bronze)

# COMMAND ----------

# 1. Función auxiliar para limpiar números con formato argentino (ej. "78.500.000" o "161.60")
def limpiar_superficie_col(col_name):
    return (
        F.regexp_extract(
            F.regexp_replace(F.coalesce(F.col(col_name), F.lit("")), ",", "."),
            r"(\d+(?:\.\d+)?)", 1
        ).cast(DoubleType())
    )

# 2. Transformación integral con PySpark
df_silver = (
    df_bronze
    # Deduplicación garantizada por id_unico quedándose con la extracción más reciente
    .dropDuplicates(["id_unico"])
    
    # --- A. NORMALIZACIÓN DE MONEDA Y PRECIO ---
    .withColumn(
        "moneda",
        F.when(F.upper(F.col("card_precio")).rlike("U\\$S|USD|DOLAR"), "USD")
         .when(F.col("card_precio").contains("$"), "ARS")
         .otherwise("A_CONSULTAR")
    )
    .withColumn(
        "precio_valor",
        F.regexp_replace(
            F.regexp_extract(F.col("card_precio"), r"([\d\.]+)", 1),
            r"\.", ""
        ).cast(DoubleType())
    )
    
    # --- B. TIPOLOGÍA Y UBICACIÓN ESTANDARIZADA ---
    .withColumn(
        "tipo_propiedad",
        F.initcap(F.trim(F.regexp_replace(F.col("card_titulo"), r"(?i)\s+en\s+(venta|alquiler).*", "")))
    )
    .withColumn(
        "localidad",
        F.initcap(F.trim(F.coalesce(F.col("detalle_localidad"), F.lit("La Rioja"))))
    )
    .withColumn(
        "barrio",
        F.upper(F.trim(
            F.coalesce(
                F.col("detalle_barrio"),
                F.regexp_replace(F.col("card_ubicacion"), r"(?i)^la\s+rioja\s*", "")
            )
        ))
    )
    .withColumn("direccion_limpia", F.initcap(F.trim(F.col("detalle_direccion"))))
    .withColumn(
        "query_geocodificacion",
        F.when(
            F.col("direccion_limpia").isNotNull() & (F.col("direccion_limpia") != ""),
            F.concat_ws(", ", F.col("direccion_limpia"), F.col("localidad"), F.lit("La Rioja, Argentina"))
        ).otherwise(None)
    )
    
    # --- C. MÉTRICAS DE AMBIENTES (PRIORIZANDO DETALLE DE FICHA SOBRE TARJETA) ---
    .withColumn(
        "dormitorios",
        F.coalesce(
            F.regexp_extract(F.col("detalle_dormitorios"), r"(\d+)", 1).cast(IntegerType()),
            F.regexp_extract(F.col("card_dormitorios"), r"(\d+)", 1).cast(IntegerType()),
            F.lit(0)
        )
    )
    .withColumn(
        "banos",
        F.coalesce(
            F.regexp_extract(F.col("detalle_banos"), r"(\d+)", 1).cast(IntegerType()),
            F.regexp_extract(F.col("card_banos"), r"(\d+)", 1).cast(IntegerType()),
            F.lit(0)
        )
    )
    .withColumn(
        "cocheras",
        F.coalesce(
            F.regexp_extract(F.col("detalle_cocheras"), r"(\d+)", 1).cast(IntegerType()),
            F.lit(0)
        )
    )
    
    # --- D. SUPERFICIES EN M2 (TOTAL, CUBIERTA, SEMI-CUBIERTA, TERRENO) ---
    .withColumn("sup_total_m2", F.coalesce(limpiar_superficie_col("medida_sup_total"), limpiar_superficie_col("card_superficie")))
    .withColumn("sup_cubierta_m2", limpiar_superficie_col("medida_sup_cubierta"))
    .withColumn("sup_semicubierta_m2", limpiar_superficie_col("medida_sup_semi_cubierta"))
    .withColumn("sup_terreno_m2", limpiar_superficie_col("medida_sup_terreno"))
    
    # --- E. CÁLCULO DE VALOR DEL M2 ---
    .withColumn(
        "precio_m2_total",
        F.when((F.col("precio_valor") > 0) & (F.col("sup_total_m2") > 0), 
               F.round(F.col("precio_valor") / F.col("sup_total_m2"), 2))
         .otherwise(None)
    )
    .withColumn(
        "precio_m2_cubierto",
        F.when((F.col("precio_valor") > 0) & (F.col("sup_cubierta_m2") > 0), 
               F.round(F.col("precio_valor") / F.col("sup_cubierta_m2"), 2))
         .otherwise(None)
    )
    
    # --- F. EXTRACCIÓN DE SERVICIOS CLAVE (PARA FILTROS Y RAG) ---
    .withColumn("texto_busqueda_servicios", F.lower(F.concat_ws(" ", F.col("ficha_iconos_caracteristicas"), F.col("ficha_descripcion_completa"))))
    .withColumn("tiene_gas_natural", F.col("texto_busqueda_servicios").contains("gas natural"))
    .withColumn("tiene_cloacas", F.col("texto_busqueda_servicios").rlike("cloaca"))
    .withColumn("tiene_agua_corriente", F.col("texto_busqueda_servicios").contains("agua"))
    .withColumn("tiene_pavimento", F.col("texto_busqueda_servicios").rlike("pavimento|asfalto"))
    .withColumn("_silver_processed_at", F.current_timestamp())
)

# Seleccionamos las columnas principales ordenadas + el resto de atributos
columnas_clave = [
    "id_unico", "codigo_ficha", "operacion_busqueda", "tipo_propiedad",
    "moneda", "precio_valor", "precio_m2_total", "precio_m2_cubierto",
    "localidad", "barrio", "direccion_limpia", "query_geocodificacion",
    "dormitorios", "banos", "cocheras",
    "sup_total_m2", "sup_cubierta_m2", "sup_semicubierta_m2", "sup_terreno_m2",
    "tiene_gas_natural", "tiene_cloacas", "tiene_agua_corriente", "tiene_pavimento",
    "ficha_iconos_caracteristicas", "ficha_descripcion_completa",
    "card_imagen_url", "url_ficha", "_extracted_at", "_silver_processed_at"
]

df_silver_final = df_silver.select(*columnas_clave)

(
    df_silver_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(tabla_silver)
)

print(f"✅ Tabla Silver creada con éxito: {tabla_silver} | Total filas limpias: {df_silver_final.count()}")
display(spark.table(tabla_silver).limit(15))
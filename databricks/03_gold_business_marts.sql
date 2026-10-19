-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥇 Capa Gold: Data Marts de Negocio para Inmobiliaria Horus
-- MAGIC Construcción de las tablas analíticas para conectar a Dashboards, BI o microservicios web.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA horus_lakehouse;

-- COMMAND ----------

-- 1. TASADOR INTELIGENTE Y MONITOR DE PRECIO DE M2 / BARRIO
CREATE OR REPLACE TABLE gold_tasador_barrios AS
SELECT 
    operacion_busqueda,
    tipo_propiedad,
    barrio,
    moneda,
    COUNT(id_unico) AS cantidad_propiedades,
    ROUND(AVG(precio_valor), 0) AS precio_promedio,
    ROUND(PERCENTILE_APPROX(precio_valor, 0.5), 0) AS precio_mediana,
    ROUND(MIN(precio_valor), 0) AS precio_minimo,
    ROUND(MAX(precio_valor), 0) AS precio_maximo,
    ROUND(AVG(precio_m2_total), 2) AS valor_m2_total_promedio,
    ROUND(AVG(precio_m2_cubierto), 2) AS valor_m2_cubierto_promedio,
    ROUND(AVG(sup_total_m2), 1) AS superficie_promedio_m2
FROM silver_propiedades_limpias
WHERE moneda != 'A_CONSULTAR' 
  AND precio_valor > 0
  AND barrio IS NOT NULL AND barrio != ''
GROUP BY operacion_busqueda, tipo_propiedad, barrio, moneda
ORDER BY cantidad_propiedades DESC, precio_promedio DESC;

SELECT * FROM gold_tasador_barrios LIMIT 20;

-- COMMAND ----------

-- 2. AUDITORÍA DE CALIDAD DE LA INFORMACIÓN DE CARTERA PUBLICADA EN LA WEB (ALERTA PARA ADMINISTRACIÓN XINTEL)
CREATE OR REPLACE TABLE gold_auditoria_calidad_crm AS
SELECT 
    codigo_ficha,
    operacion_busqueda,
    tipo_propiedad,
    barrio,
    direccion_limpia,
    precio_valor,
    sup_total_m2,
    sup_terreno_m2,
    url_ficha,
    CONCAT_WS(' | ',
        CASE WHEN precio_valor IS NULL OR precio_valor <= 0 THEN 'Falta Precio' END,
        CASE WHEN direccion_limpia IS NULL OR direccion_limpia = '' THEN 'Falta Dirección Exacta' END,
        CASE WHEN barrio IS NULL OR barrio = '' THEN 'Falta Barrio' END,
        CASE WHEN (sup_total_m2 IS NULL OR sup_total_m2 <= 0) AND (sup_terreno_m2 IS NULL OR sup_terreno_m2 <= 0) THEN 'Sin Superficie (m2)' END,
        CASE WHEN ficha_descripcion_completa IS NULL OR LENGTH(ficha_descripcion_completa) < 30 THEN 'Descripción Muy Corta/Vacía' END
    ) AS alertas_calidad
FROM silver_propiedades_limpias
WHERE (precio_valor IS NULL OR precio_valor <= 0)
   OR (direccion_limpia IS NULL OR direccion_limpia = '')
   OR (barrio IS NULL OR barrio = '')
   OR ((sup_total_m2 IS NULL OR sup_total_m2 <= 0) AND (sup_terreno_m2 IS NULL OR sup_terreno_m2 <= 0))
   OR (ficha_descripcion_completa IS NULL OR LENGTH(ficha_descripcion_completa) < 30);

SELECT * FROM gold_auditoria_calidad_crm;

-- COMMAND ----------

-- 3. MOTOR DE RECOMENDACIÓN DE PROPIEDADES SIMILARES (TOP 3 ALTERNATIVAS POR FICHA)
CREATE OR REPLACE TABLE gold_recomendador_similares AS
WITH pares_similares AS (
    SELECT 
        a.codigo_ficha AS ficha_consultada,
        a.tipo_propiedad AS tipo_consultado,
        a.barrio AS barrio_consultado,
        a.precio_valor AS precio_consultado,
        b.codigo_ficha AS ficha_sugerida,
        b.barrio AS barrio_sugerido,
        b.direccion_limpia AS direccion_sugerida,
        b.precio_valor AS precio_sugerido,
        b.dormitorios AS dormitorios_sugeridos,
        b.sup_total_m2 AS sup_sugerida_m2,
        b.url_ficha AS url_sugerida,
        -- Score de cercanía comercial: prioriza mismo barrio y menor diferencia porcentual de precio
        (CASE WHEN a.barrio = b.barrio THEN 0 ELSE 15 END) + 
        ABS(a.precio_valor - b.precio_valor) / a.precio_valor * 100 +
        ABS(a.dormitorios - b.dormitorios) * 5 AS distancia_score,
        ROW_NUMBER() OVER (
            PARTITION BY a.codigo_ficha 
            ORDER BY 
                (CASE WHEN a.barrio = b.barrio THEN 0 ELSE 1 END),
                ABS(a.precio_valor - b.precio_valor) ASC
        ) AS ranking_sugerencia
    FROM silver_propiedades_limpias a
    JOIN silver_propiedades_limpias b
      ON a.operacion_busqueda = b.operacion_busqueda
     AND a.tipo_propiedad = b.tipo_propiedad
     AND a.moneda = b.moneda
     AND a.codigo_ficha != b.codigo_ficha
    WHERE a.precio_valor > 0 
      AND b.precio_valor BETWEEN a.precio_valor * 0.70 AND a.precio_valor * 1.30
)
SELECT 
    ficha_consultada,
    ranking_sugerencia,
    ficha_sugerida,
    barrio_sugerido,
    direccion_sugerida,
    precio_sugerido,
    dormitorios_sugeridos,
    sup_sugerida_m2,
    ROUND(distancia_score, 1) AS indice_similitud,
    url_sugerida
FROM pares_similares
WHERE ranking_sugerencia <= 3
ORDER BY ficha_consultada, ranking_sugerencia;

SELECT * FROM gold_recomendador_similares LIMIT 30;

-- COMMAND ----------

-- DBTITLE 1,Mapa de Propiedades
-- MAGIC %md
-- MAGIC ## 🗺️ Mapa Interactivo: Propiedades en Venta en La Rioja Capital
-- MAGIC Geocodificación de direcciones (OpenStreetMap Nominatim) + visualización con Folium.
-- MAGIC Cada pin muestra tipo, precio, barrio y enlace a la ficha original.

-- COMMAND ----------

-- DBTITLE 1,Geocodificación de direcciones
-- MAGIC %python
-- MAGIC import requests
-- MAGIC import time
-- MAGIC from pyspark.sql import functions as F
-- MAGIC
-- MAGIC # --- 1. Cargar propiedades en venta en La Rioja Capital ---
-- MAGIC df_silver = spark.table("workspace.horus_lakehouse.silver_propiedades_limpias")
-- MAGIC props = (
-- MAGIC     df_silver.filter(
-- MAGIC         (F.col("operacion_busqueda") == "Compra")
-- MAGIC         & (F.col("localidad") == "La Rioja")
-- MAGIC         & F.col("query_geocodificacion").isNotNull()
-- MAGIC     )
-- MAGIC     .select(
-- MAGIC         "id_unico", "tipo_propiedad", "direccion_limpia", "barrio",
-- MAGIC         "query_geocodificacion", "precio_valor", "moneda", "url_ficha",
-- MAGIC         "sup_total_m2", "dormitorios",
-- MAGIC     )
-- MAGIC     .toPandas()
-- MAGIC )
-- MAGIC
-- MAGIC unique_addresses = props["query_geocodificacion"].unique()
-- MAGIC print(f"Geocodificando {len(unique_addresses)} direcciones únicas...")
-- MAGIC
-- MAGIC # --- 2. Geocodificar via OpenStreetMap Nominatim (gratis, 1 req/seg) ---
-- MAGIC NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
-- MAGIC HEADERS = {"User-Agent": "HorusRealEstateLakehouse/1.0"}
-- MAGIC
-- MAGIC geocode_cache = {}
-- MAGIC for i, addr in enumerate(unique_addresses):
-- MAGIC     try:
-- MAGIC         resp = requests.get(
-- MAGIC             NOMINATIM_URL,
-- MAGIC             params={"q": addr, "format": "json", "limit": 1, "countrycodes": "ar"},
-- MAGIC             headers=HEADERS,
-- MAGIC             timeout=10,
-- MAGIC         )
-- MAGIC         if resp.ok and resp.json():
-- MAGIC             data = resp.json()[0]
-- MAGIC             geocode_cache[addr] = (float(data["lat"]), float(data["lon"]))
-- MAGIC     except Exception:
-- MAGIC         pass
-- MAGIC     if (i + 1) % 50 == 0:
-- MAGIC         print(f"  Progreso: {i + 1}/{len(unique_addresses)} ({len(geocode_cache)} exitosos)")
-- MAGIC     time.sleep(1)  # rate limit Nominatim
-- MAGIC
-- MAGIC print(f"✅ Geocodificación completa: {len(geocode_cache)}/{len(unique_addresses)} direcciones encontradas")
-- MAGIC
-- MAGIC # --- 3. Mergear coordenadas al dataset completo ---
-- MAGIC props["lat"] = props["query_geocodificacion"].map(lambda x: geocode_cache.get(x, (None, None))[0])
-- MAGIC props["lon"] = props["query_geocodificacion"].map(lambda x: geocode_cache.get(x, (None, None))[1])
-- MAGIC props_geocoded = props.dropna(subset=["lat", "lon"])
-- MAGIC print(f"Propiedades con coordenadas: {len(props_geocoded)}/{len(props)}")

-- COMMAND ----------

-- DBTITLE 1,Instalar folium
-- MAGIC %python
-- MAGIC %pip install folium --quiet

-- COMMAND ----------

-- DBTITLE 1,Renderizado del mapa interactivo
-- MAGIC %python
-- MAGIC import folium
-- MAGIC from folium.plugins import MarkerCluster
-- MAGIC
-- MAGIC # --- Crear mapa centrado en La Rioja Capital ---
-- MAGIC m = folium.Map(location=[-29.4117, -66.8507], zoom_start=13, tiles="OpenStreetMap")
-- MAGIC marker_cluster = MarkerCluster().add_to(m)
-- MAGIC
-- MAGIC for _, row in props_geocoded.iterrows():
-- MAGIC     precio_fmt = f"{row['precio_valor']:,.0f}"
-- MAGIC     popup_html = f"""
-- MAGIC     <div style="font-family: Arial; min-width: 200px;">
-- MAGIC         <b style="color: #d4af37;">{row['tipo_propiedad']}</b><br>
-- MAGIC         📍 Barrio: {row['barrio']}<br>
-- MAGIC         💰 Precio: {row['moneda']} {precio_fmt}<br>
-- MAGIC         🏠 Dormitorios: {row['dormitorios']}<br>
-- MAGIC         📐 Superficie: {row['sup_total_m2']} m²<br>
-- MAGIC         <a href="{row['url_ficha']}" target="_blank" style="color: #0066cc;">Ver ficha completa →</a>
-- MAGIC     </div>
-- MAGIC     """
-- MAGIC     folium.Marker(
-- MAGIC         location=[row["lat"], row["lon"]],
-- MAGIC         popup=folium.Popup(popup_html, max_width=300),
-- MAGIC         tooltip=f"{row['tipo_propiedad']} - {row['barrio']}",
-- MAGIC     ).add_to(marker_cluster)
-- MAGIC
-- MAGIC print(f"✅ Mapa generado con {len(props_geocoded)} propiedades geocodificadas")
-- MAGIC displayHTML(m._repr_html_())
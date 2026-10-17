-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥇 Capa Gold: Data Marts de Negocio para Inmobiliaria Horus
-- MAGIC Construye las tablas analíticas listas para conectar a Dashboards, BI o microservicios web.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA horus_lakehouse;

-- COMMAND ----------

-- 1. DATA MART: TASADOR INTELIGENTE Y MONITOR DE M2 POR BARRIO
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

-- 2. DATA MART: AUDITORÍA DE CALIDAD DE CARTERA (ALERTA PARA ADMINISTRACIÓN XINTEL)
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

-- 3. DATA MART: MOTOR DE RECOMENDACIÓN DE PROPIEDADES SIMILARES (TOP 3 ALTERNATIVAS POR FICHA)
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
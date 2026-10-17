import pandas as pd
from playwright.sync_api import sync_playwright
from config.settings import (
    BASE_URL, OPERACIONES, LANDING_URLS_PATH, RAW_EXTRACTED_PATH,
    HEADLESS_MODE, LIMIT_PAGES_TEST
)
from src.utils import (
    logger, aplicar_jitter, obtener_ids_procesados, guardar_fila_incremental
)
from src.extractor import (
    detectar_total_paginas, extraer_tarjetas_grilla, extraer_ficha_completa
)

def ejecutar_fase_1_tarjetas(page, contador_peticiones: int) -> int:
    """Recolecta todas las tarjetas (Foto 1) de Compra y Alquiler."""
    ids_landing = obtener_ids_procesados(LANDING_URLS_PATH, "id_unico")
    logger.info(f"=== FASE 1: RECOLECCIÓN DE TARJETAS (FOTO 1) | Preexistentes: {len(ids_landing)} ===")

    for op in OPERACIONES:
        cod_ope = op["codigo"]
        nom_ope = op["nombre"]
        url_busqueda = f"{BASE_URL}/propiedades?ope={cod_ope}&tipo=All&loc=&b1=All"
        
        logger.info(f"🚀 Iniciando operación: {nom_ope.upper()} -> {url_busqueda}")
        page.goto(url_busqueda, wait_until="networkidle", timeout=45000)
        page.wait_for_selector("#ct_blog_grid", timeout=15000)
        
        total_paginas = detectar_total_paginas(page)
        paginas_objetivo = min(total_paginas, LIMIT_PAGES_TEST) if LIMIT_PAGES_TEST else total_paginas
        
        logger.info(f"[{nom_ope}] Páginas a procesar: {paginas_objetivo} de {total_paginas}")

        for idx in range(paginas_objetivo):
            num_pag = idx + 1
            if idx > 0:
                logger.info(f"[{nom_ope}] Disparo JS traerPagina({idx}) -> Página {num_pag}/{paginas_objetivo}")
                page.evaluate(f"traerPagina({idx})")
                page.wait_for_load_state("networkidle")
                contador_peticiones += 1
                aplicar_jitter(contador_peticiones)
            else:
                logger.info(f"[{nom_ope}] Escaneando Página 1/{paginas_objetivo}...")

            html_grilla = page.inner_html("#ct_blog_grid")
            tarjetas = extraer_tarjetas_grilla(html_grilla, cod_ope, nom_ope, num_pag)
            
            nuevas = 0
            for card in tarjetas:
                if card["id_unico"] not in ids_landing:
                    guardar_fila_incremental(LANDING_URLS_PATH, card)
                    ids_landing.add(card["id_unico"])
                    nuevas += 1
                    
            logger.info(f"   -> Página {num_pag}: {nuevas} tarjetas nuevas guardadas en Landing.")

    return contador_peticiones

def ejecutar_fase_2_fichas_internas(page, contador_peticiones: int):
    """Entra al botón 'Ver más' (Foto 2) de cada tarjeta y une ambos conjuntos de datos."""
    if not LANDING_URLS_PATH.exists():
        logger.error("No se encontró el catálogo de tarjetas. Ejecutá la Fase 1 primero.")
        return

    df_landing = pd.read_csv(LANDING_URLS_PATH, dtype=str).fillna("")
    ids_extraidos = obtener_ids_procesados(RAW_EXTRACTED_PATH, "id_unico")
    
    pendientes = df_landing[~df_landing["id_unico"].isin(ids_extraidos)]
    total_pend = len(pendientes)
    
    logger.info(f"=== FASE 2: EXTRACCIÓN PROFUNDA 'VER MÁS' (FOTO 2) | Pendientes: {total_pend} ===")

    for idx, (_, fila_card) in enumerate(pendientes.iterrows(), start=1):
        id_unico = fila_card["id_unico"]
        url_ficha = fila_card["url_ficha"]
        operacion = fila_card["operacion_busqueda"]
        
        logger.info(f"🔍 [{idx}/{total_pend}] ({operacion}) Extrayendo interior de {fila_card['codigo_ficha']}...")
        
        try:
            datos_internos = extraer_ficha_completa(page, url_ficha)
            
            # Unimos todas las columnas de la Tarjeta (Foto 1) + Ficha Interna (Foto 2)
            registro_completo = fila_card.to_dict()
            registro_completo.update(datos_internos)
            
            guardar_fila_incremental(RAW_EXTRACTED_PATH, registro_completo)
            ids_extraidos.add(id_unico)
            
        except Exception as e:
            logger.error(f"❌ Error en ficha {id_unico} ({url_ficha}): {e}")
            
        contador_peticiones += 1
        aplicar_jitter(contador_peticiones)

def main():
    logger.info("==========================================================")
    logger.info("   PIPELINE ETL HORUS: TARJETAS (FOTO 1) + FICHAS (FOTO 2)")
    logger.info("==========================================================")
    contador = 0
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768}
        )
        page = context.new_page()
        
        try:
            contador = ejecutar_fase_1_tarjetas(page, contador)
            ejecutar_fase_2_fichas_internas(page, contador)
        finally:
            browser.close()
            
    logger.info(f"🎉 Proceso finalizado. CSV unificado disponible en: {RAW_EXTRACTED_PATH}")

if __name__ == "__main__":
    main()
import time
import random
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime
from config.settings import (
    LOG_PATH, MIN_DELAY_SEC, MAX_DELAY_SEC,
    BATCH_SIZE_COFFEE, COFFEE_MIN_SEC, COFFEE_MAX_SEC
)

def configurar_logger() -> logging.Logger:
    logger = logging.getLogger("Horus_Lakehouse_ETL")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        formato = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s", 
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
        fh.setFormatter(formato)
        logger.addHandler(fh)
        
        ch = logging.StreamHandler()
        ch.setFormatter(formato)
        logger.addHandler(ch)
        
    return logger

logger = configurar_logger()

def aplicar_jitter(contador_peticiones: int):
    """Ralentizador aleatorio entre peticiones para simular navegación humana."""
    espera = random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC)
    logger.info(f"⏳ [Anti-Bloqueo] Esperando {espera:.2f}s...")
    time.sleep(espera)
    
    if contador_peticiones > 0 and contador_peticiones % BATCH_SIZE_COFFEE == 0:
        descanso = random.uniform(COFFEE_MIN_SEC, COFFEE_MAX_SEC)
        logger.info(f"☕ [Batch {contador_peticiones}] Pausa preventiva larga de {descanso:.1f}s...")
        time.sleep(descanso)

def obtener_ids_procesados(ruta_csv: Path, col_id: str = "id_unico") -> set:
    """Lee el CSV existente para evitar reprocesar propiedades ya descargadas."""
    if ruta_csv.exists():
        try:
            df = pd.read_csv(ruta_csv, dtype=str)
            if col_id in df.columns:
                return set(df[col_id].dropna().unique())
        except Exception as e:
            logger.warning(f"No se pudo leer checkpoint {ruta_csv}: {e}")
    return set()

def guardar_fila_incremental(ruta_csv: Path, registro: dict):
    """Guarda un registro en tiempo real asegurando alineación de columnas dinámicas."""
    registro["_extracted_at"] = datetime.now().isoformat(timespec="seconds")
    df_nuevo = pd.DataFrame([registro])
    
    if not ruta_csv.exists():
        df_nuevo.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
    else:
        df_actual = pd.read_csv(ruta_csv, dtype=str)
        df_combinado = pd.concat([df_actual, df_nuevo], ignore_index=True)
        df_combinado.to_csv(ruta_csv, index=False, encoding="utf-8-sig")
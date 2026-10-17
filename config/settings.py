import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Rutas base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent
LANDING_URLS_PATH = BASE_DIR / "data" / "landing_urls" / "catalogo_tarjetas_horus.csv"
RAW_EXTRACTED_PATH = BASE_DIR / "data" / "raw_extracted" / "propiedades_horus_completo.csv"
LOG_PATH = BASE_DIR / "logs" / "pipeline.log"

# Asegurar que los directorios existan al iniciar
LANDING_URLS_PATH.parent.mkdir(parents=True, exist_ok=True)
RAW_EXTRACTED_PATH.parent.mkdir(parents=True, exist_ok=True)
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

# Configuración del Sitio Web
BASE_URL = "https://inmobiliariahorus.com.ar"

# Orden secuencial estricto: 1° Compra ('V'), 2° Alquiler ('A')
OPERACIONES = [
    {"codigo": "V", "nombre": "Compra"},
    {"codigo": "A", "nombre": "Alquiler"}
]

# Parámetros de ejecución y Anti-Bloqueo (Jitter)
HEADLESS_MODE = os.getenv("HEADLESS_MODE", "true").lower() == "true"
MIN_DELAY_SEC = float(os.getenv("MIN_DELAY_SEC", 2.5))
MAX_DELAY_SEC = float(os.getenv("MAX_DELAY_SEC", 5.8))
BATCH_SIZE_COFFEE = int(os.getenv("BATCH_SIZE_COFFEE", 25))
COFFEE_MIN_SEC = 15.0
COFFEE_MAX_SEC = 28.0

# Límite de páginas para Smoke Test (Poner en 0 o None en producción para descargar las 44+ páginas)
_limit_env = os.getenv("LIMIT_PAGES_TEST", "2")
LIMIT_PAGES_TEST = int(_limit_env) if _limit_env and _limit_env.isdigit() and int(_limit_env) > 0 else None
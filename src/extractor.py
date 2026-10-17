import re
import time
import unicodedata
from bs4 import BeautifulSoup, Tag
from urllib.parse import urljoin
from playwright.sync_api import Page
from config.settings import BASE_URL
from src.utils import logger

def limpiar_nombre_columna(prefijo: str, texto: str) -> str:
    """
    Convierte cualquier etiqueta HTML (ej. 'Cant. Naves', 'Categoría', 'F.O.T.')
    en un nombre de columna snake_case 100% compatible con Spark y Delta Lake.
    """
    # Quitar tildes y diéresis (Categoría -> Categoria, Antigüedad -> Antiguedad)
    sin_tildes = "".join(
        c for c in unicodedata.normalize("NFD", texto)
        if unicodedata.category(c) != "Mn"
    )
    # Reemplazar cualquier caracter no alfanumérico (puntos, guiones, espacios) por '_'
    limpio = re.sub(r"[^a-zA-Z0-9]+", "_", sin_tildes.lower()).strip("_")
    return f"{prefijo}_{limpio}"

def detectar_total_paginas(page: Page) -> int:
    """Detecta el número máximo en la barra de paginación."""
    botones = page.locator("#ct_blog_grid .ct-grid-pagination .page-numbers").all()
    if botones:
        ultimo_txt = botones[-1].inner_text().strip()
        if ultimo_txt.isdigit():
            return int(ultimo_texto) if (ultimo_texto := ultimo_txt) else 1
    return 1

def _subir_hasta_contenedor_tarjeta(a_tag: Tag) -> Tag:
    """Sube desde el botón 'Ver más' hasta el contenedor padre de toda la tarjeta individual."""
    actual = a_tag
    for _ in range(6):
        if actual.parent and isinstance(actual.parent, Tag):
            botones_en_padre = actual.parent.find_all("a", string=re.compile(r"Ver\s*m[aá]s", re.I))
            if len(botones_en_padre) > 1:
                break
            actual = actual.parent
    return actual

def extraer_tarjetas_grilla(html_grilla: str, cod_ope: str, nom_ope: str, num_pagina: int) -> list[dict]:
    """Extrae toda la información visible en cada tarjeta de la Foto 1 junto con el enlace 'Ver más'."""
    soup = BeautifulSoup(html_grilla, "html.parser")
    tarjetas_extraidas = []
    
    botones_ver_mas = soup.find_all("a", string=re.compile(r"Ver\s*m[aá]s", re.I))
    
    for boton in botones_ver_mas:
        href = boton.get("href", "").strip()
        if not href or href == "#":
            continue
            
        url_ficha = urljoin(BASE_URL, href)
        codigo_ficha = url_ficha.rstrip("/").split("-")[-1]
        id_unico = f"{cod_ope}_{codigo_ficha}"
        
        card = _subir_hasta_contenedor_tarjeta(boton)
        textos_card = [t.strip() for t in card.stripped_strings if t.strip() and not re.match(r"Ver\s*m[aá]s", t.strip(), re.I)]
        texto_unificado = " | ".join(textos_card)
        
        img_tag = card.find("img")
        imagen_url = urljoin(BASE_URL, img_tag.get("src")) if img_tag and img_tag.get("src") else ""
        
        heading = card.find(["h2", "h3", "h4", "h5"])
        titulo_card = heading.get_text(strip=True) if heading else (textos_card[0] if textos_card else "")
        
        precio_match = re.search(r"(?:U\$S|USD|\$)\s*[\d\.,]+|\bConsultar\b", texto_unificado, re.I)
        precio_card = precio_match.group(0).strip() if precio_match else ""
        
        m2_match = re.search(r"([\d\.,]+\s*m(?:2|²))", texto_unificado, re.I)
        banos_match = re.search(r"(\d+\s*bañ[oa]s?)", texto_unificado, re.I)
        dorm_match = re.search(r"(\d+\s*(?:d\b|dorm\w*))", texto_unificado, re.I)
        
        ubicacion_card = ""
        for i, txt in enumerate(textos_card):
            if "la rioja" in txt.lower() or "chilecito" in txt.lower():
                siguiente = textos_card[i + 1] if i + 1 < len(textos_card) else ""
                ubicacion_card = f"{txt} {siguiente}".strip()
                break
                
        tarjetas_extraidas.append({
            "id_unico": id_unico,
            "codigo_ficha": codigo_ficha,
            "operacion_busqueda": nom_ope,
            "pagina_origen": num_pagina,
            "card_titulo": titulo_card,
            "card_ubicacion": ubicacion_card,
            "card_precio": precio_card,
            "card_superficie": m2_match.group(1) if m2_match else "",
            "card_banos": banos_match.group(1) if banos_match else "",
            "card_dormitorios": dorm_match.group(1) if dorm_match else "",
            "card_resumen_completo": texto_unificado,
            "card_imagen_url": imagen_url,
            "url_ficha": url_ficha
        })
        
    return tarjetas_extraidas

def extraer_ficha_completa(page: Page, url_ficha: str) -> dict:
    """Extrae todos los datos internos de la ficha ('Ver más') con nombres compatibles con Delta Lake."""
    page.goto(url_ficha, wait_until="domcontentloaded", timeout=45000)
    page.wait_for_selector("div.infofic.infoficgen", timeout=15000)
    time.sleep(1.2)
    
    soup = BeautifulSoup(page.content(), "html.parser")
    datos_ficha = {}
    
    # A. Bloque de Descripción Larga
    div_desc = soup.select_one("div.infofic.infodesc")
    if div_desc:
        lineas_desc = [linea.strip() for linea in div_desc.stripped_strings if linea.strip()]
        datos_ficha["ficha_descripcion_completa"] = "\n".join(lineas_desc)
    else:
        datos_ficha["ficha_descripcion_completa"] = ""

    # B. Recuadro "Detalles de la propiedad" (div.infofic.infoficgen)
    div_detalles = soup.select_one("div.infofic.infoficgen")
    if div_detalles:
        for li in div_detalles.select("ul > li"):
            estilo = li.get("style", "").replace(" ", "").lower()
            if "display:none" in estilo:
                continue
            strong = li.find("strong")
            span = li.find("span")
            if strong and span:
                clave = strong.get_text(strip=True).rstrip(":")
                valor = span.get_text(strip=True)
                # Solo guardamos si la propiedad realmente tiene un valor cargado en ese campo
                if clave and valor:
                    nombre_col = limpiar_nombre_columna("detalle", clave)
                    datos_ficha[nombre_col] = valor

    # C. Sección "Medidas" y "Características"
    div_carac_general = soup.select_one("#fichacarac") or soup
    for div_bloque in div_carac_general.select("div.infofic"):
        clases = div_bloque.get("class", [])
        if "infodesc" in clases or "infoficgen" in clases:
            continue
        if "display:none" in div_bloque.get("style", "").replace(" ", "").lower():
            continue
            
        titulo_tag = div_bloque.find(["h3", "h4"])
        titulo_bloque = titulo_tag.get_text(strip=True).lower() if titulo_tag else "extra"
        
        if "medida" in titulo_bloque:
            for li in div_bloque.select("ul > li"):
                if "display:none" in li.get("style", "").replace(" ", "").lower():
                    continue
                strong = li.find("strong")
                span = li.find("span")
                if strong and span:
                    c_med = strong.get_text(strip=True).rstrip(":")
                    v_med = span.get_text(strip=True)
                    if c_med and v_med:
                        nombre_col_med = limpiar_nombre_columna("medida", c_med)
                        datos_ficha[nombre_col_med] = v_med
        elif "caracter" in titulo_bloque:
            items_carac = [
                li.get_text(" ", strip=True) 
                for li in div_bloque.select("ul > li") 
                if "display:none" not in li.get("style", "").replace(" ", "").lower()
                and li.get_text(strip=True)
            ]
            if items_carac:
                datos_ficha["ficha_iconos_caracteristicas"] = " | ".join(items_carac)

    return datos_ficha
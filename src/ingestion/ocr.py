# Importamos librerías necesarias
import cv2
import pytesseract
import re
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Union, Set

# Configuración OCR 
pytesseract.pytesseract.tesseract_cmd = (r"C:\Program Files\Tesseract-OCR\tesseract.exe")

# Utilidades generales
def normalizar_para_comparar(valor: Union[str, datetime, float]) -> str:
    """
    Normaliza fechas y textos a un formato uniforme para evitar
    duplicados falsos.

    Args:
        valor: Fecha u hora proveniente de Excel u OCR.

    Returns:
        Fecha formateada como 'DD/MM/AAAA HH:MM' o string vacío.
    """
    if pd.isna(valor) or valor == "":
        return ""

    if isinstance(valor, datetime):
        return valor.strftime("%d/%m/%Y %H:%M")

    texto = str(valor).strip()
    formatos = [
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M",
    ]

    for fmt in formatos:
        try:
            return datetime.strptime(texto, fmt).strftime("%d/%m/%Y %H:%M")
        except ValueError:
            continue

    return texto


def es_separador(texto: str) -> bool:
    """
    Determina si una línea OCR corresponde a metadata o basura
    y no a un nombre de local.

    Args:
        texto: Línea OCR.

    Returns:
        bool: True si es separador, False si es nombre válido.
    """
    t = texto.lower().strip()

    if re.match(r"^\d{7,}$", t):
        return True
    if any(
        kw in t
        for kw in [
            "pedido agrupado",
            "completado",
            "cancelado",
            "ver detalles",
            "horas conectado",
            "promedio",
            "ars",
        ]
    ):
        return True
    if "semana" in t and re.search(r"\d{2}", t):
        return True
    if re.match(r"^\w{3}, \d+", t):
        return True

    return False

# --------------------------------------------------
# OCR principal
# --------------------------------------------------
def procesar_imagen_ocr(ruta_imagen: str) -> List[Dict[str, str]]:
    """
    Procesa una imagen y extrae pedidos usando OCR.

    Args:
        ruta_imagen: Ruta al archivo de imagen.

    Returns:
        Pedidos con:
            - Hora_Aceptacion
            - Hora_Entrega
            - Nombre_Local
    """
    img = cv2.imread(ruta_imagen)
    if img is None:
        return []

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(
        gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
    )

    texto = pytesseract.image_to_string(thresh, lang="spa")
    lineas = [l.strip() for l in texto.split("\n") if l.strip()]

    meses = {
        "ene": "01", "feb": "02", "mar": "03", "abr": "04",
        "may": "05", "jun": "06", "jul": "07", "ago": "08",
        "sep": "09", "oct": "10", "nov": "11", "dic": "12",
    }

    regex_fecha = r"(\w{3}),\s*(\d{1,2})\s*de\s*(\w{3})"
    regex_anio = r"20\d{2}"
    regex_horas = r"(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})"

    fecha_base, anio_base = None, None

    for linea in lineas:
        if not fecha_base:
            m = re.search(regex_fecha, linea, re.IGNORECASE)
            if m:
                fecha_base = (
                    f"{m.group(2).zfill(2)}/"
                    f"{meses.get(m.group(3).lower()[:3], '01')}"
                )
        if not anio_base:
            m = re.search(regex_anio, linea)
            if m:
                anio_base = m.group(0)

    if not anio_base:
        anio_base = str(datetime.now().year)

    pedidos = []

    if not fecha_base:
        return pedidos

    fecha_base_dt = datetime.strptime(
        f"{fecha_base}/{anio_base}", "%d/%m/%Y"
    )

    for i, linea in enumerate(lineas):
        m = re.search(regex_horas, linea)
        if not m:
            continue

        h_ini = datetime.strptime(m.group(1), "%H:%M").time()
        h_fin = datetime.strptime(m.group(2), "%H:%M").time()

        dt_acep = datetime.combine(fecha_base_dt.date(), h_ini)
        dt_entr = datetime.combine(fecha_base_dt.date(), h_fin)

        if dt_entr < dt_acep:
            dt_entr += timedelta(days=1)

        nombre = "Desconocido"
        if i > 0:
            nombre = lineas[i - 1]
            if i > 1 and not es_separador(lineas[i - 2]):
                nombre = f"{lineas[i - 2]} {nombre}"

        nombre = re.sub(r"^[\W_]+", "", nombre)
        nombre = re.sub(r"[^\w\s\(\)]+$", "", nombre).strip()

        pedidos.append(
            {
                "Hora_Aceptacion": dt_acep.strftime("%d/%m/%Y %H:%M"),
                "Hora_Entrega": dt_entr.strftime("%d/%m/%Y %H:%M"),
                "Nombre_Local": nombre,
            }
        )

    return pedidos

# Deduplicación
def obtener_firmas_existentes(df_pedidos: pd.DataFrame) -> Set[str]:
    """
    Genera firmas Hora_Aceptacion_Hora_Entrega desde pedidos históricos.
    """
    firmas = set()

    for _, r in df_pedidos.iterrows():
        ha = normalizar_para_comparar(r.get("Hora_Aceptacion"))
        he = normalizar_para_comparar(r.get("Hora_Entrega"))
        if ha and he:
            firmas.add(f"{ha}_{he}")

    return firmas


def pedido_ya_existe(hora_aceptacion: str, hora_entrega: str, firmas_existentes: Set[str]) -> bool:
    """
    Verifica si un pedido ya existe según su firma temporal.
    """
    return f"{hora_aceptacion}_{hora_entrega}" in firmas_existentes


# Enriquecimiento desde histórico
def obtener_diccionario_locales(df_historico: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Construye una memoria de locales previamente registrados.
    """
    conocimiento = {}

    if df_historico.empty:
        return conocimiento

    columnas = ["Nombre_Local", "Tipo_Negocio", "Cadena", "CP_Local"]
    df_util = df_historico.dropna(subset=["Nombre_Local"])

    for local in df_util["Nombre_Local"].unique():
        fila = df_util[df_util["Nombre_Local"] == local].iloc[-1]
        conocimiento[local] = {
            "Tipo_Negocio": fila.get("Tipo_Negocio", ""),
            "Cadena": fila.get("Cadena", ""),
            "CP_Local": fila.get("CP_Local", ""),
        }

    return conocimiento


def completar_datos_local_desde_historico(nombre_local: str, df_historico: pd.DataFrame) -> Dict[str, Any]:
    """
    Autocompleta datos del local si existe en el histórico.

    Args:
        nombre_local: nombre del negocio.
        df_historico: dataframe con registros guardados hasta ese momento
    """
    dicc = obtener_diccionario_locales(df_historico)
    return dicc.get(
        nombre_local,
        {"Tipo_Negocio": "", "Cadena": "", "CP_Local": ""},
    )

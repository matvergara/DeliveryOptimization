"""
1. Leer excel RAW
2. Limpiar y normalizar datos
3. Aplicar validaciones minimas
4. Devolver datasets clean
"""

import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Paths
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_STAGE = ROOT_DIR / "data" / "stage"

ARCHIVO_EXCEL = DATA_RAW / "datos_pedidos.xlsx"

# --------------------------------------------------
# Funciones
# --------------------------------------------------
def normalizar_clima(valor):
    if pd.isna(valor):
        return None
    return str(valor).strip().capitalize()


def normalizar_evento(valor):
    if pd.isna(valor):
        return None
    v = str(valor).strip()
    return None if v.upper() in ("NA", "") else v


def normalizar_cp(valor):
    try:
        cp = int(valor)
        return cp if 1000 <= cp <= 9999 else None
    except Exception:
        return None


# --------------------------------------------------
# STAGING TURNOS
# --------------------------------------------------
def stage_turnos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ID_Turno"] = df["ID_Turno"].astype(int)
    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    df["Hora_Inicio"] = pd.to_datetime(df["Hora_Inicio"])
    df["Hora_Fin"] = pd.to_datetime(df["Hora_Fin"])

    df["Grupo_Semanal"] = df["Grupo_Semanal"].astype(int)

    columnas_ganancia = [
        "Ganancia_Pedido",
        "Ganancia_Km",
        "Ganancia_Publi",
        "Ganancia_Bonos",
        "Ganancia_Grupo",
        "Ganancia_Propinas_Total",
    ]

    for col in columnas_ganancia:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    df["Clima"] = df["Clima"].apply(normalizar_clima)
    df["Evento_Especial"] = df["Evento_Especial"].apply(normalizar_evento)

    # Validación temporal
    df = df[df["Hora_Fin"] > df["Hora_Inicio"]]

    return df.reset_index(drop=True)


# --------------------------------------------------
# STAGING PEDIDOS
# --------------------------------------------------
def stage_pedidos(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ID_Pedido"] = df["ID_Pedido"].astype(int)
    df["ID_Turno"] = pd.to_numeric(df["ID_Turno"], errors="coerce")

    df["Hora_Aceptacion"] = pd.to_datetime(df["Hora_Aceptacion"])
    df["Hora_Entrega"] = pd.to_datetime(df["Hora_Entrega"])

    df = df[df["Hora_Entrega"] >= df["Hora_Aceptacion"]]

    df["Nombre_Local"] = df["Nombre_Local"].astype(str).str.strip()
    df["Tipo_Negocio"] = df["Tipo_Negocio"].astype(str).str.strip().str.title()

    df["Cadena"] = (
        df["Cadena"]
        .astype(str)
        .str.strip()
        .str.lower()
        .map({"si": True, "no": False})
    )

    df["CP_Local"] = df["CP_Local"].apply(normalizar_cp)
    df["CP_Cliente"] = df["CP_Cliente"].apply(normalizar_cp)

    df["Propina_Pedido"] = pd.to_numeric(
        df["Propina_Pedido"], errors="coerce"
    ).fillna(0.0)

    # Derivada permitida
    df["tiempo_entrega_min"] = (
        (df["Hora_Entrega"] - df["Hora_Aceptacion"])
        .dt.total_seconds() / 60
    )

    df = df[~df["ID_Turno"].isna()]

    return df.reset_index(drop=True)


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main() -> None:
    DATA_STAGE.mkdir(parents=True, exist_ok=True)

    df_turnos_raw = pd.read_excel(ARCHIVO_EXCEL, sheet_name="Turnos")
    df_pedidos_raw = pd.read_excel(ARCHIVO_EXCEL, sheet_name="Pedidos")

    df_turnos_clean = stage_turnos(df_turnos_raw)
    df_pedidos_clean = stage_pedidos(df_pedidos_raw)

    df_turnos_clean.to_csv(DATA_STAGE / "turnos_clean.csv", index=False)
    df_pedidos_clean.to_csv(DATA_STAGE / "pedidos_clean.csv", index=False)


if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Paths
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_STAGE = ROOT_DIR / "data" / "stage"
DATA_MARTS = ROOT_DIR / "data" / "marts"

# --------------------------------------------------
# Helpers
# --------------------------------------------------
def generar_surrogate_key(df: pd.DataFrame, nombre_id: str) -> pd.DataFrame:
    """
    Agrega una columna de ID surrogate incremental.
    """
    df = df.copy().reset_index(drop=True)
    df[nombre_id] = range(1, len(df) + 1)
    return df


# --------------------------------------------------
# DIM TIEMPO
# --------------------------------------------------
def build_dim_tiempo(df_turnos: pd.DataFrame, df_pedidos: pd.DataFrame) -> pd.DataFrame:
    """
    Dimensión tiempo a nivel fecha calendario.
    """
    fechas_turnos = pd.to_datetime(df_turnos["Fecha"]).dt.date
    fechas_pedidos = pd.to_datetime(df_pedidos["Hora_Aceptacion"]).dt.date

    fechas = pd.Series(
        pd.concat([fechas_turnos, fechas_pedidos]).unique()
    )

    fechas = pd.to_datetime(fechas).sort_values()

    df = pd.DataFrame({"fecha": fechas})

    df["anio"] = df["fecha"].dt.year
    df["mes"] = df["fecha"].dt.month
    df["mes_nombre"] = df["fecha"].dt.month_name()
    df["dia"] = df["fecha"].dt.day
    df["dia_semana_num"] = df["fecha"].dt.weekday + 1
    df["dia_semana_nombre"] = df["fecha"].dt.day_name()
    df["semana_anio"] = df["fecha"].dt.isocalendar().week.astype(int)
    df["es_fin_de_semana"] = df["dia_semana_num"].isin([6, 7])

    df = generar_surrogate_key(df, "id_tiempo")

    return df[
        [
            "id_tiempo",
            "fecha",
            "anio",
            "mes",
            "mes_nombre",
            "dia",
            "dia_semana_num",
            "dia_semana_nombre",
            "semana_anio",
            "es_fin_de_semana",
        ]
    ]


# --------------------------------------------------
# DIM PROVEEDOR
# --------------------------------------------------
def build_dim_proveedor(df_pedidos: pd.DataFrame) -> pd.DataFrame:
    """
    Dimensión proveedor (locales).
    """
    df = (
        df_pedidos[
            ["Nombre_Local", "Tipo_Negocio", "Cadena"]
        ]
        .drop_duplicates()
        .sort_values("Nombre_Local")
        .reset_index(drop=True)
    )

    df = df.rename(
        columns={
            "Nombre_Local": "nombre_local",
            "Tipo_Negocio": "tipo_negocio",
            "Cadena": "es_cadena",
        }
    )

    df = generar_surrogate_key(df, "id_proveedor")

    return df[
        ["id_proveedor", "nombre_local", "tipo_negocio", "es_cadena"]
    ]


# --------------------------------------------------
# DIM ZONA
# --------------------------------------------------
def build_dim_zona(df_pedidos: pd.DataFrame) -> pd.DataFrame:
    """
    Dimensión zona basada en código postal.
    """
    cps = pd.concat(
        [df_pedidos["CP_Local"], df_pedidos["CP_Cliente"]]
    )

    df = (
        cps.dropna()
        .astype(int)
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
        .to_frame(name="codigo_postal")
    )

    # Lookup manual
    lookup_path = (
        Path(__file__).resolve().parents[2]
        / "data"
        / "lookups"
        / "zonas_cp.csv"
    )

    if lookup_path.exists():
        df_lookup = pd.read_csv(lookup_path)
        df = df.merge(
            df_lookup,
            on="codigo_postal",
            how="left"
        )
    else:
        df["nombre_zona"] = None
        df["ciudad"] = "Buenos Aires"

    df = generar_surrogate_key(df, "id_zona")

    return df[
        ["id_zona", "codigo_postal", "nombre_zona", "ciudad"]
    ]


# --------------------------------------------------
# DIM CLIMA
# --------------------------------------------------
def build_dim_clima(df_turnos: pd.DataFrame) -> pd.DataFrame:
    """
    Dimensión clima.
    """
    df = (
        df_turnos[["Clima"]]
        .dropna()
        .drop_duplicates()
        .sort_values("Clima")
        .reset_index(drop=True)
        .rename(columns={"Clima": "clima"})
    )

    df = generar_surrogate_key(df, "id_clima")

    return df[["id_clima", "clima"]]



# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main() -> None:
    DATA_MARTS.mkdir(parents=True, exist_ok=True)

    df_turnos = pd.read_csv(
        DATA_STAGE / "turnos_clean.csv",
        parse_dates=["Hora_Inicio", "Hora_Fin"]
    )

    df_pedidos = pd.read_csv(
        DATA_STAGE / "pedidos_clean.csv",
        parse_dates=["Hora_Aceptacion", "Hora_Entrega"]
    )

    dim_tiempo = build_dim_tiempo(df_turnos, df_pedidos)
    dim_proveedor = build_dim_proveedor(df_pedidos)
    dim_zona = build_dim_zona(df_pedidos)
    dim_clima = build_dim_clima(df_turnos)

    dim_tiempo.to_csv(DATA_MARTS / "dim_tiempo.csv", index=False)
    dim_proveedor.to_csv(DATA_MARTS / "dim_proveedor.csv", index=False)
    dim_zona.to_csv(DATA_MARTS / "dim_zona.csv", index=False)
    dim_clima.to_csv(DATA_MARTS / "dim_clima.csv", index=False)


if __name__ == "__main__":
    main()

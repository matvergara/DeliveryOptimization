import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Paths
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_STAGE = ROOT_DIR / "data" / "stage"
DATA_MARTS = ROOT_DIR / "data" / "marts"

# --------------------------------------------------
# Funciones
# --------------------------------------------------
def cargar_dim(path: Path, key: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df.set_index(key)


# --------------------------------------------------
# FACT TURNOS
# --------------------------------------------------
def build_fact_turnos(df_turnos: pd.DataFrame, dim_tiempo: pd.DataFrame, dim_clima: pd.DataFrame) -> pd.DataFrame:
    """
    Construye fact_turnos.
    """
    df = df_turnos.copy()

    df["Fecha"] = pd.to_datetime(df["Fecha"]).dt.date
    dim_tiempo["fecha"] =pd.to_datetime(dim_tiempo["fecha"]).dt.date

    # Métricas
    df["duracion_turno_horas"] = (
        (df["Hora_Fin"] - df["Hora_Inicio"])
        .dt.total_seconds() / 3600
    )

    df["ingreso_total"] = (
        df["Ganancia_Pedido"]
        + df["Ganancia_Km"]
        + df["Ganancia_Publi"]
        + df["Ganancia_Bonos"]
        + df["Ganancia_Grupo"]
        + df["Ganancia_Propinas_Total"]
    )


    # Join tiempo
    df = df.merge(
        dim_tiempo,
        left_on="Fecha",
        right_on="fecha",
        how="left"
    )

    # Join clima
    df = df.merge(
        dim_clima,
        left_on="Clima",
        right_on="clima",
        how="left"
    )

    return df[
        [
            "ID_Turno",
            "id_tiempo",
            "id_clima",
            "Grupo_Semanal",
            "Evento_Especial",
            "Km_Totales",
            "duracion_turno_horas",
            "ingreso_total"
        ]
    ].rename(
        columns={
            "ID_Turno": "id_turno",
            "Grupo_Semanal": "grupo_semanal",
            "Evento_Especial": "evento_especial",
        }
    )


# --------------------------------------------------
# FACT PEDIDOS
# --------------------------------------------------
def build_fact_pedidos(df_pedidos: pd.DataFrame, dim_tiempo: pd.DataFrame, dim_proveedor: pd.DataFrame, dim_zona: pd.DataFrame) -> pd.DataFrame:
    """
    Construye fact_pedidos.
    """
    df = df_pedidos.copy()

    # Join tiempo
    df["fecha"] = df["Hora_Aceptacion"].dt.date
    dim_tiempo["fecha"] =pd.to_datetime(dim_tiempo["fecha"]).dt.date
    
    df = df.merge(
        dim_tiempo,
        on="fecha",
        how="left"
    )

    # Join proveedor
    df = df.merge(
        dim_proveedor,
        left_on="Nombre_Local",
        right_on="nombre_local",
        how="left"
    )

    # Zona local
    df = df.merge(
        dim_zona,
        left_on="CP_Local",
        right_on="codigo_postal",
        how="left"
    ).rename(columns={"id_zona": "id_zona_local"})

    # Zona cliente
    df = df.merge(
        dim_zona,
        left_on="CP_Cliente",
        right_on="codigo_postal",
        how="left"
    ).rename(columns={"id_zona": "id_zona_cliente"})

    return df[
        [
            "ID_Pedido",
            "ID_Turno",
            "id_tiempo",
            "id_proveedor",
            "id_zona_local",
            "id_zona_cliente",
            "tiempo_entrega_min",
            "Propina_Pedido",
        ]
    ].rename(
        columns={
            "ID_Pedido": "id_pedido",
            "ID_Turno": "id_turno",
            "Propina_Pedido": "propina_pedido",
        }
    )


# --------------------------------------------------
# MAIN
# --------------------------------------------------
def main() -> None:
    DATA_MARTS.mkdir(parents=True, exist_ok=True)

    # STAGING
    
    df_turnos = pd.read_csv(
        DATA_STAGE / "turnos_clean.csv",
        parse_dates=["Hora_Inicio", "Hora_Fin"]
    )
    df_pedidos = pd.read_csv(
        DATA_STAGE / "pedidos_clean.csv",
        parse_dates=["Hora_Aceptacion", "Hora_Entrega"]
    )

    # DIMENSIONES
    dim_tiempo = pd.read_csv(
        DATA_MARTS / "dim_tiempo.csv",
        parse_dates=["fecha"]
    )
    dim_clima = cargar_dim(DATA_MARTS / "dim_clima.csv", "id_clima").reset_index()
    dim_proveedor = cargar_dim(DATA_MARTS / "dim_proveedor.csv", "id_proveedor").reset_index()
    dim_zona = cargar_dim(DATA_MARTS / "dim_zona.csv", "id_zona").reset_index()

    fact_turnos = build_fact_turnos(
        df_turnos,
        dim_tiempo,
        dim_clima
    )

    fact_pedidos = build_fact_pedidos(
        df_pedidos,
        dim_tiempo,
        dim_proveedor,
        dim_zona
    )

    fact_turnos.to_csv(DATA_MARTS / "fact_turnos.csv", index=False)
    fact_pedidos.to_csv(DATA_MARTS / "fact_pedidos.csv", index=False)


if __name__ == "__main__":
    main()

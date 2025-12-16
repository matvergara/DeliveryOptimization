import sys
from pathlib import Path
import streamlit as st
import pandas as pd
import tempfile
from datetime import datetime, date, time, timedelta

# --------------------------------------------------
# Asegurar root del proyecto en sys.path
# --------------------------------------------------
ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

# --------------------------------------------------
# Imports del proyecto
# --------------------------------------------------
from src.ingestion.utils_excel import (
    leer_hoja_raw,
    obtener_siguiente_id,
    insertar_registro_raw
)

from src.ingestion.ocr import (
    procesar_imagen_ocr,
    obtener_firmas_existentes,
    pedido_ya_existe,
    completar_datos_local_desde_historico
)

# --------------------------------------------------
# Configuración general
# --------------------------------------------------
ARCHIVO_EXCEL = "data/raw/datos_pedidos.xlsx"
HOJA_TURNOS = "Turnos"
HOJA_PEDIDOS = "Pedidos"

# --------------------------------------------------
# Funciones
# --------------------------------------------------

def resolver_id_turno_por_hora(hora_aceptacion: datetime, df_turnos: pd.DataFrame):
    """
    Devuelve el ID_Turno cuyo rango horario contiene la hora de aceptación.
    """
    mask = (
        (df_turnos["Hora_Inicio"] <= hora_aceptacion) &
        (df_turnos["Hora_Fin"] >= hora_aceptacion)
    )

    turnos_validos = df_turnos[mask]

    if turnos_validos.empty:
        return None

    if len(turnos_validos) > 1:
        return "AMBIGUO"

    return turnos_validos.iloc[0]["ID_Turno"]


st.set_page_config(
    page_title="Ingesta de Pedidos",
    layout="wide"
)

st.title("Ingesta de datos operativos")

tab_turnos, tab_pedidos = st.tabs(["Carga de turnos", "Carga de pedidos"])

# ==================================================
# TAB 1 - TURNOS
# ==================================================
with tab_turnos:
    st.subheader("Carga de turno")

    # Datos de tiempo
    col1, col2, col3 = st.columns(3)
    with col1:
        fecha_turno = st.date_input("Fecha", value=date.today())
    with col2:
        hora_inicio = st.time_input("Hora inicio", value=time(20, 0), step=60)
    with col3:
        hora_fin = st.time_input("Hora fin", value=time(0, 0), step=60)

    # Datos adicionales
    st.write("---")
    ganancia_pedidos = st.number_input("Ganancia por pedidos", min_value=0.0)
    ganancia_km = st.number_input("Ganancia por km", min_value=0.0)
    ganancia_publicidad = st.number_input("Ganancia por publicidad", min_value=0.0)
    ganancia_bonos = st.number_input("Ganancia por bonos", min_value=0.0)
    ganancia_grupo = st.number_input("Ganancia por grupo", min_value=0.0)
    ganancia_propinas = st.number_input("Ganancia por propinas", min_value=0.0)

    st.write("---")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        grupo = st.selectbox("Grupo semanal", list(range(1, 8)))
    with col2:
        clima = st.selectbox("Clima", ["Normal", "Frío", "Calor", "Lluvia"])
    with col3:
        evento = st.text_input("Evento especial (opcional)", "")
    with col4:    
        km_totales = st.number_input("Kilómetros totales", min_value=0.0, step=0.1)

    if st.button("Guardar turno"):
        if hora_inicio == hora_fin:
            st.error("La hora de inicio y fin no pueden ser iguales")
        else:
            df_turnos = leer_hoja_raw(ARCHIVO_EXCEL, HOJA_TURNOS)
            nuevo_id = obtener_siguiente_id(df_turnos, "ID_Turno")

            dt_inicio = datetime.combine(fecha_turno, hora_inicio)
            dt_fin = datetime.combine(fecha_turno, hora_fin)
            if hora_fin < hora_inicio:
                dt_fin += timedelta(days=1)

            registro_turno = {
                "ID_Turno": nuevo_id,
                "Fecha": fecha_turno,
                "Hora_Inicio": dt_inicio,
                "Hora_Fin": dt_fin,
                "Grupo_Semanal": grupo,
                "Km_Totales": km_totales,
                "Ganancia_Pedido": ganancia_pedidos,
                "Ganancia_Km": ganancia_km,
                "Ganancia_Publi": ganancia_publicidad,
                "Ganancia_Bonos": ganancia_bonos,
                "Ganancia_Grupo": ganancia_grupo,
                "Ganancia_Propinas_Total": ganancia_propinas,
                "Clima": clima,
                "Evento_Especial": evento or None
            }

            insertar_registro_raw(
                ARCHIVO_EXCEL,
                HOJA_TURNOS,
                registro_turno
            )

            st.success("Turno guardado correctamente")

# ==================================================
# TAB 2 - PEDIDOS
# ==================================================
with tab_pedidos:
    st.subheader("Carga de pedido")

    df_pedidos = leer_hoja_raw(ARCHIVO_EXCEL, HOJA_PEDIDOS)
    df_turnos = leer_hoja_raw(ARCHIVO_EXCEL, HOJA_TURNOS)

    firmas_existentes = obtener_firmas_existentes(df_pedidos)

    st.markdown("### Opcional: cargar desde imagen (OCR)")

    pedido_ocr = None
    archivo_imagen = st.file_uploader(
        "Subir captura de pantalla",
        type=["png", "jpg", "jpeg"]
    )

    if archivo_imagen:
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(archivo_imagen.read())
            pedidos_detectados = procesar_imagen_ocr(tmp.name)

        if pedidos_detectados:
            st.session_state["pedidos_ocr"] = pedidos_detectados
            st.success(f"Pedidos detectados: {len(pedidos_detectados)}")
        else:
            st.warning("No se detectaron pedidos en la imagen")

    if "pedidos_ocr" in st.session_state:
        st.markdown("### Preview pedidos detectados")
        df_preview = pd.DataFrame(st.session_state["pedidos_ocr"])
        st.dataframe(df_preview, use_container_width=True)

        idx = st.selectbox(
            "Seleccionar pedido a cargar",
            options=list(range(len(st.session_state["pedidos_ocr"]))),
            format_func=lambda i: f"Pedido {i + 1}"
        )

        pedido_ocr = st.session_state["pedidos_ocr"][idx]

    st.markdown("### Datos del pedido")

    col1, col2 = st.columns(2)

    with col1:
        hora_aceptacion = st.datetime_input(
            "Hora de aceptación",
            value=datetime.strptime(
                pedido_ocr["Hora_Aceptacion"], "%d/%m/%Y %H:%M"
            ) if pedido_ocr else None
        , step=60)

    with col2:
        hora_entrega = st.datetime_input(
            "Hora de entrega",
            value=datetime.strptime(
                pedido_ocr["Hora_Entrega"], "%d/%m/%Y %H:%M"
            ) if pedido_ocr else None
        )

    st.write("---")
    col1, col2, col3 = st.columns(3)

    with col1:
        nombre_local = st.text_input(
            "Nombre del local",
            value=pedido_ocr["Nombre_Local"] if pedido_ocr else ""
        )

    enriquecido = completar_datos_local_desde_historico(
        nombre_local,
        df_pedidos
    )

    with col2:
        tipo_negocio = st.text_input(
            "Tipo de negocio",
            value=enriquecido.get("Tipo_Negocio", "")
        )

    with col3:
        cadena = st.selectbox(
            "Es cadena",
            ["Si", "No"],
            index=0 if enriquecido.get("Cadena") == "Si" else 1
        )

    st.write("---")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        cp_local = st.number_input(
            "Código postal del local",
            min_value=0,
            max_value=9999,
            value=int(enriquecido.get("CP_Local") or 0)
        )

    with col2:
        cp_cliente = st.number_input(
            "Código postal del cliente",
            min_value=0,
            max_value=9999
        )

    with col3:
        propina = st.number_input(
            "Propina",
            min_value=0.0
        )

    if st.button("Guardar pedido"):
        if hora_aceptacion is None or hora_entrega is None:
            st.error("Faltan horas de aceptación o entrega")
        else:
            ha = hora_aceptacion.strftime("%d/%m/%Y %H:%M")
            he = hora_entrega.strftime("%d/%m/%Y %H:%M")

            if pedido_ya_existe(ha, he, firmas_existentes):
                st.error("El pedido ya existe en el registro")
            else:
                nuevo_id = obtener_siguiente_id(df_pedidos, "ID_Pedido")

                id_turno = resolver_id_turno_por_hora(hora_aceptacion, df_turnos)

                if id_turno is None:
                    st.error("No se encontró un turno que contenga la hora del pedido. Cargá el turno primero.")
                    st.stop()

                if id_turno == "AMBIGUO":
                    st.error("La hora del pedido coincide con más de un turno. Revisá los horarios.")
                    st.stop()

                registro_pedido = {
                    "ID_Pedido": nuevo_id,
                    "ID_Turno": id_turno,
                    "Hora_Aceptacion": ha,
                    "Hora_Entrega": he,
                    "Nombre_Local": nombre_local,
                    "Tipo_Negocio": tipo_negocio,
                    "Cadena": cadena,
                    "CP_Local": cp_local,
                    "CP_Cliente": cp_cliente,
                    "Propina_Pedido": propina
                }

                insertar_registro_raw(
                    ARCHIVO_EXCEL,
                    HOJA_PEDIDOS,
                    registro_pedido
                )

                st.success("Pedido guardado correctamente")



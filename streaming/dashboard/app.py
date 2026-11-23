"""Dashboard OPERATIVO en tiempo real (Streamlit).

Lee las predicciones que escribe el consumidor de Kafka en
`data/streaming/predicciones.jsonl` (una línea JSON por predicción).
NO consulta MySQL: eso lo hace el reporte estático (notebooks/).

La decisión que apoya este tablero: a qué colegios y municipios priorizar
apoyo/refuerzo. Por eso lo principal es operativo, no analítico:
  a) Lista de PRIORIDAD: colegios con mayor % de estudiantes en riesgo.
  b) ALERTA visible cuando un colegio/municipio supera un umbral (ajustable).
  c) Texto de ACCIÓN SUGERIDA.
  d) Apoyo analítico: contador de mensajes, % en riesgo total y por estrato.

Técnica (según la guía del curso):
  - @st.fragment(run_every="3s") refresca solo las partes en vivo, sin recargar
    toda la página.
  - @st.cache_data(ttl=60) solo en lo que casi no cambia (catálogo de municipios
    para el filtro). En lo que se actualiza en vivo NO se usa caché.

Ejecutar:
    streamlit run streaming/dashboard/app.py
    # o con Docker:  docker compose --profile streaming up dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Permite ejecutar tanto "streamlit run streaming/dashboard/app.py" como dentro del contenedor.
sys.path.insert(0, str(Path(__file__).resolve().parent))
import datos  # noqa: E402

st.set_page_config(page_title="Saber 11 · Riesgo en tiempo real", page_icon="📡", layout="wide")


# ---- Lo que casi no cambia: catálogo de municipios para el filtro (con caché) ----
@st.cache_data(ttl=60)
def catalogo_municipios() -> list[str]:
    df = datos.cargar_predicciones()
    if df.empty:
        return []
    return sorted(df["municipio"].dropna().unique().tolist())


# ------------------------------- Controles -------------------------------
st.title("📡 Riesgo escolar en tiempo real · Saber 11 Valle del Cauca")
st.caption(
    "Predicciones del modelo que llegan por Kafka y escribe el consumidor. "
    "Tablero **operativo**: sirve para decidir a qué colegios y municipios priorizar apoyo."
)

with st.sidebar:
    st.header("Controles")
    umbral_pct = st.slider(
        "Umbral de alerta por colegio/municipio (% en riesgo)",
        min_value=0, max_value=100, value=50, step=5,
        help="Un colegio o municipio entra en ALERTA si su % de estudiantes en riesgo supera este valor.",
    )
    umbral_prob = st.slider(
        "Umbral de prob. de riesgo individual",
        min_value=0.0, max_value=1.0, value=0.7, step=0.05,
        help="Marca los estudiantes con probabilidad de riesgo por encima de este valor.",
    )
    min_casos = st.slider(
        "Mínimo de casos para la lista de prioridad",
        min_value=1, max_value=50, value=5, step=1,
        help="Evita que un colegio con 1 o 2 estudiantes aparezca de primero por azar.",
    )
    municipios = catalogo_municipios()
    filtro_muni = st.multiselect("Filtrar municipios (opcional)", municipios, default=[])
    st.divider()
    st.caption(f"Fuente: `{datos.ruta_en_uso()}`")
    st.caption("Se actualiza cada 3 s mientras el consumidor escribe predicciones.")


# --------------------------- Parte en vivo (sin caché) ---------------------------
@st.fragment(run_every="3s")
def panel_en_vivo():
    df = datos.cargar_predicciones()  # sin caché: es lo que cambia en vivo
    if filtro_muni:
        df = df[df["municipio"].isin(filtro_muni)]

    res = datos.resumen_general(df)
    prio_col = datos.prioridad_colegios(df, min_casos=min_casos)
    prio_muni = datos.prioridad_municipios(df, min_casos=min_casos)
    alerta_col = datos.en_alerta(prio_col, umbral_pct)
    alerta_muni = datos.en_alerta(prio_muni, umbral_pct)

    if res["mensajes"] == 0:
        st.info("Aún no llegan predicciones. Inicia el consumidor (o ejecuta el DAG) y el tablero se irá llenando solo.")
        return

    # ---- KPIs (apoyo analítico) ----
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Mensajes procesados", f"{res['mensajes']:,}")
    k2.metric("% en riesgo", f"{res['pct_riesgo']:.1f}%", f"{res['en_riesgo']:,} estudiantes")
    k3.metric("Prob. media de riesgo", f"{res['prob_media']:.2f}")
    k4.metric("Colegios en alerta", f"{len(alerta_col)}", f"de {res['colegios']}")
    k5.metric("Municipios en alerta", f"{len(alerta_muni)}", f"de {res['municipios']}")

    # ---- ALERTA + acción sugerida ----
    n_prob_alta = int((df["prob_riesgo"] > umbral_prob).sum())
    if not alerta_col.empty or not alerta_muni.empty:
        st.error(
            f"🚨 **Alerta.** {len(alerta_col)} colegio(s) y {len(alerta_muni)} municipio(s) "
            f"superan el {umbral_pct:.0f}% de estudiantes en riesgo. "
            f"{n_prob_alta} estudiante(s) con probabilidad individual > {umbral_prob:.2f}."
        )
    else:
        st.success(f"✅ Sin colegios ni municipios por encima del {umbral_pct:.0f}% en riesgo por ahora.")
    st.info("👉 **Acción sugerida:** " + datos.accion_sugerida(prio_col, umbral_pct))

    # ---- Lo principal: prioridad ----
    col_izq, col_der = st.columns([3, 2])
    with col_izq:
        st.subheader("🏫 Prioridad de colegios")
        st.caption("Ordenados por mayor % de estudiantes en riesgo (con el mínimo de casos del control).")
        st.dataframe(
            _estilo_prioridad(prio_col, umbral_pct, ["Código", "Colegio", "Municipio"]),
            use_container_width=True, hide_index=True,
        )
    with col_der:
        st.subheader("📍 Prioridad de municipios")
        st.dataframe(
            _estilo_prioridad(prio_muni, umbral_pct, ["Municipio"]),
            use_container_width=True, hide_index=True,
        )

    # ---- Apoyo analítico ----
    st.divider()
    a_izq, a_der = st.columns(2)
    with a_izq:
        st.subheader("📊 % en riesgo por estrato")
        estrato = datos.riesgo_por_estrato(df)
        if not estrato.empty:
            st.bar_chart(estrato.set_index("estrato")["pct_riesgo"], height=260)
    with a_der:
        st.subheader("🕒 Últimas predicciones")
        ult = df.sort_values("ts", ascending=False, na_position="last").head(12)
        st.dataframe(
            ult[["ts", "nombre_colegio", "municipio", "estrato", "prob_riesgo", "pred_riesgo"]]
            .rename(columns={"ts": "Hora", "nombre_colegio": "Colegio", "municipio": "Municipio",
                             "estrato": "Estrato", "prob_riesgo": "Prob.", "pred_riesgo": "Riesgo"}),
            use_container_width=True, hide_index=True,
        )

    ts = res["ultimo_ts"]
    st.caption(f"Última predicción recibida: {ts:%Y-%m-%d %H:%M:%S UTC}" if ts is not None else "")


def _estilo_prioridad(tabla: pd.DataFrame, umbral_pct: float, cols_texto: list[str]):
    """Resalta en rojo las filas que superan el umbral de alerta."""
    cols = cols_texto + ["casos", "en_riesgo", "pct_riesgo", "prob_media"]
    vista = tabla[cols].rename(columns={
        "casos": "Casos", "en_riesgo": "En riesgo", "pct_riesgo": "% riesgo", "prob_media": "Prob. media",
    })

    def _resaltar(fila):
        color = "background-color: rgba(220,60,60,0.18)" if fila["% riesgo"] > umbral_pct else ""
        return [color] * len(fila)

    return vista.style.apply(_resaltar, axis=1).format({"% riesgo": "{:.1f}", "Prob. media": "{:.3f}"})


panel_en_vivo()

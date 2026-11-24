"""Dashboard de predicciones de riesgo en tiempo real.

Lee lo que escribe el consumidor de Kafka en `data/streaming/predicciones.jsonl`,
una línea JSON por predicción. No consulta MySQL.

Secciones:
  1. Resumen general
  2. Dónde apoyar primero, la parte operativa
  3. Predicciones en tiempo real
  4. Registro de mensajes
  5. Interpretación de resultados
  6. Monitoreo del streaming

Decisiones técnicas, justificadas en la sección 11 del README:
  - @st.fragment con run_every de 3 s refresca la parte viva sin recargar la página.
  - @st.cache_data con ttl de 60 s solo para las métricas del modelo, que casi no cambian.
  - st.session_state guarda los colegios atendidos y los que entraron en alerta.
  - Sin colores de fondo fijos, para que se vea bien en modo claro y en modo oscuro.
  - Paleta azul y naranja, apta para daltonismo, y cada estado lleva su palabra.

Ejecutar:
    streamlit run streaming/dashboard/app.py
    docker compose --profile streaming up dashboard
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
import datos  # noqa: E402

RUTA_METRICAS = datos.RAIZ / "models" / "metricas.json"
FILAS_VISIBLES = 10

# Paleta apta para daltonismo. Azul y naranja se distinguen aunque no se vea el rojo y el verde.
COLOR_RIESGO = "#D55E00"
COLOR_SIN_RIESGO = "#0072B2"
COLOR_NEUTRO = "#0072B2"
ESCALA_CATEGORIAS = alt.Scale(domain=["En riesgo", "Sin riesgo"], range=[COLOR_RIESGO, COLOR_SIN_RIESGO])
ESCALA_TRAZO = alt.Scale(domain=["En riesgo", "Sin riesgo"], range=[[1, 0], [6, 4]])

st.set_page_config(page_title="Predicciones de riesgo en Saber 11", layout="wide")


def coma(x: float, dec: int = 1) -> str:
    """Formato con coma decimal, como se escribe en español."""
    return f"{x:,.{dec}f}".replace(",", "X").replace(".", ",").replace("X", ".")


def miles(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")


def plural(n: int, singular: str, plural_: str) -> str:
    return f"{n} {singular if n == 1 else plural_}"


def hace(seg) -> str:
    if seg is None:
        return "sin dato"
    seg = max(0, int(seg))
    if seg < 60:
        return f"hace {seg} s"
    if seg < 3600:
        return f"hace {seg // 60} min"
    return f"hace {seg // 3600} h"


@st.cache_data(ttl=60)
def metricas_modelo(ruta: str) -> dict:
    """Métricas guardadas al entrenar. Casi no cambian, por eso van en caché."""
    p = Path(ruta)
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}


# ------------------------------------------------------------------ estado de la sesión
for clave, valor in (("atendidos", []), ("alertas_previas", None), ("nuevas_alertas", [])):
    if clave not in st.session_state:
        st.session_state[clave] = valor


# ------------------------------------------------------------------ encabezado y ajustes
st.title("Predicciones de riesgo en Saber 11 · Valle del Cauca")
st.markdown(
    "Cada estudiante que llega por Kafka se evalúa con un modelo que estima si su puntaje quedaría entre "
    "el 25 % más bajo de su semestre. Este tablero muestra qué predice el modelo, cómo se comportan esas "
    "predicciones y dónde conviene dirigir tutorías y refuerzo."
)

with st.expander("Cómo leer este tablero"):
    st.markdown(
        """
- **En riesgo** significa que el modelo estima una probabilidad alta de que el estudiante quede entre el 25 % con menor puntaje de su semestre. **Sin riesgo** es lo contrario.
- **Alerta** aparece cuando un colegio o municipio llega al % de estudiantes en riesgo que se elige en la barra lateral. **Vigilar** marca los que están sobre el promedio sin llegar a la alerta.
- El % en riesgo lo calcula el modelo y sale más alto que el real. Sirve para ordenar prioridades, no para medir cuántos estudiantes sacarán un puntaje bajo.
- Los datos llegan por Kafka y se actualizan solos cada 3 segundos. La sección de monitoreo, al final, confirma que el flujo está activo. El flujo es una simulación con estudiantes del conjunto de prueba.
- Esta herramienta orienta el apoyo a colegios y municipios. No sirve para tomar decisiones sobre un estudiante en particular.
        """
    )

with st.sidebar:
    st.header("Ajustes")
    st.caption("Cambia estos valores y el tablero se recalcula.")
    umbral_pct = st.slider(
        "Alertar desde este % de estudiantes en riesgo", 0, 100, 50, 5,
        help="Un colegio o municipio entra en alerta cuando su % de estudiantes en riesgo supera este valor.",
    )
    min_casos = st.slider(
        "Mínimo de estudiantes para evaluar un colegio", 1, 50, 5, 1,
        help="Evita que un colegio con uno o dos estudiantes aparezca de primero por azar.",
    )
    umbral_prob = st.slider(
        "Estudiantes de alta probabilidad desde", 0.0, 1.0, 0.70, 0.05,
        help="Cuenta los estudiantes cuya probabilidad de riesgo es igual o mayor a este valor.",
    )


# ------------------------------------------------------------------ gráficos
def dibujar(funcion, *args) -> None:
    """Dibuja un gráfico. Si uno falla, el resto del tablero sigue funcionando."""
    try:
        st.altair_chart(funcion(*args), use_container_width=True)
    except Exception as error:  # noqa: BLE001
        st.caption(f"No se pudo dibujar este gráfico: {error}")


def graf_distribucion(dist: pd.DataFrame):
    dist = dist.assign(etiqueta=[f"{miles(r.n)}  ·  {coma(r.pct)} %" for r in dist.itertuples()])
    barras = alt.Chart(dist).mark_bar(size=28).encode(
        y=alt.Y("categoria:N", title=None, sort=["En riesgo", "Sin riesgo"]),
        x=alt.X("n:Q", title="Estudiantes"),
        color=alt.Color("categoria:N", scale=ESCALA_CATEGORIAS, legend=None),
        tooltip=[alt.Tooltip("categoria:N", title="Categoría"), alt.Tooltip("n:Q", title="Estudiantes"),
                 alt.Tooltip("pct:Q", title="% del total")],
    )
    texto = alt.Chart(dist).mark_text(align="left", dx=6, fontSize=14, color="#7f7f7f").encode(
        y=alt.Y("categoria:N", sort=["En riesgo", "Sin riesgo"]), x="n:Q",
        text=alt.Text("etiqueta:N"),
    )
    return (barras + texto).properties(height=150)


def graf_acumulado(acum: pd.DataFrame):
    return alt.Chart(acum).mark_line(strokeWidth=2.5).encode(
        x=alt.X("n:Q", title="Estudiantes evaluados"),
        y=alt.Y("acum:Q", title="Estudiantes acumulados"),
        color=alt.Color("categoria:N", scale=ESCALA_CATEGORIAS, legend=alt.Legend(orient="top", title=None)),
        strokeDash=alt.StrokeDash("categoria:N", scale=ESCALA_TRAZO, legend=None),
        tooltip=[alt.Tooltip("categoria:N", title="Categoría"), alt.Tooltip("n:Q", title="Evaluados"),
                 alt.Tooltip("acum:Q", title="Acumulado")],
    ).properties(height=280)


def graf_evolucion(ev: pd.DataFrame, esperado):
    linea = alt.Chart(ev).mark_line(strokeWidth=2.5, color=COLOR_RIESGO).encode(
        x=alt.X("n:Q", title="Estudiantes evaluados"),
        y=alt.Y("pct:Q", title="% en riesgo acumulado", scale=alt.Scale(zero=False)),
        tooltip=[alt.Tooltip("n:Q", title="Evaluados"), alt.Tooltip("pct:Q", title="% en riesgo")],
    )
    capas = linea
    if esperado is not None:
        ref = pd.DataFrame({"pct": [esperado]})
        regla = alt.Chart(ref).mark_rule(strokeDash=[6, 4], strokeWidth=1.8, color=COLOR_SIN_RIESGO).encode(y="pct:Q")
        capas = linea + regla
    return capas.properties(height=280)


def graf_probabilidades(df: pd.DataFrame, umbral):
    d = df[["prob_riesgo", "pred_riesgo"]].dropna().assign(
        categoria=lambda x: x["pred_riesgo"].map(datos.CATEGORIAS))
    hist = alt.Chart(d).mark_bar().encode(
        x=alt.X("prob_riesgo:Q", bin=alt.Bin(step=0.05, extent=[0, 1]), title="Probabilidad de riesgo estimada"),
        y=alt.Y("count():Q", title="Estudiantes"),
        color=alt.Color("categoria:N", scale=ESCALA_CATEGORIAS, legend=alt.Legend(orient="top", title=None)),
        tooltip=[alt.Tooltip("count():Q", title="Estudiantes")],
    )
    capas = hist
    if umbral is not None:
        ref = pd.DataFrame({"u": [umbral]})
        regla = alt.Chart(ref).mark_rule(strokeDash=[6, 4], strokeWidth=1.8, color="#808080").encode(x="u:Q")
        capas = hist + regla
    return capas.properties(height=280)


def graf_estratos(e: pd.DataFrame):
    e = e.assign(estrato=e["estrato"].astype(str).replace({"Sin Estrato": "Sin estrato"}))
    orden = [x for x in ["Estrato 1", "Estrato 2", "Estrato 3", "Estrato 4", "Estrato 5", "Estrato 6",
                         "Sin estrato", "No informa"] if x in set(e["estrato"])]
    orden += sorted(set(e["estrato"]) - set(orden))
    base = alt.Chart(e).encode(y=alt.Y("estrato:N", sort=orden, title=None))
    barras = base.mark_bar(color=COLOR_NEUTRO, size=18).encode(
        x=alt.X("pct_riesgo:Q", title="% en riesgo"),
        tooltip=[alt.Tooltip("estrato:N", title="Estrato"), alt.Tooltip("pct_riesgo:Q", title="% en riesgo"),
                 alt.Tooltip("casos:Q", title="Estudiantes")])
    texto = base.mark_text(align="left", dx=5, fontSize=12, color="#7f7f7f").encode(
        x="pct_riesgo:Q", text=alt.Text("pct_riesgo:Q", format=".0f"))
    return (barras + texto).properties(height=280)


def graf_matriz(mc: dict):
    filas = [("Real: en riesgo", "Predicho: en riesgo", mc["tp"]), ("Real: en riesgo", "Predicho: sin riesgo", mc["fn"]),
             ("Real: sin riesgo", "Predicho: en riesgo", mc["fp"]), ("Real: sin riesgo", "Predicho: sin riesgo", mc["tn"])]
    d = pd.DataFrame(filas, columns=["real", "predicho", "n"])
    corte = float(d["n"].max()) * 0.55
    base = alt.Chart(d).encode(
        x=alt.X("predicho:N", title=None, sort=["Predicho: en riesgo", "Predicho: sin riesgo"],
                axis=alt.Axis(labelAngle=0)),
        y=alt.Y("real:N", title=None, sort=["Real: en riesgo", "Real: sin riesgo"]))
    celdas = base.mark_rect().encode(color=alt.Color("n:Q", scale=alt.Scale(range=["#d6e9f8", "#0072B2"]), legend=None))
    textos = base.mark_text(fontSize=16, fontWeight="bold").encode(
        text=alt.Text("n:Q", format=","),
        color=alt.condition(alt.datum.n > corte, alt.value("white"), alt.value("black")))
    return (celdas + textos).properties(height=200)


def graf_ritmo(r: pd.DataFrame):
    r = pd.DataFrame({"hora": r["hora"].dt.strftime("%H:%M:%S"), "mensajes": r["mensajes"]})
    orden = r["hora"].tolist()
    return alt.Chart(r).mark_bar(color=COLOR_NEUTRO).encode(
        x=alt.X("hora:O", sort=orden, title="Hora de Colombia", axis=alt.Axis(labelAngle=-45, labelOverlap="greedy")),
        y=alt.Y("mensajes:Q", title="Mensajes cada 10 s"),
        tooltip=[alt.Tooltip("hora:O", title="Hora"), alt.Tooltip("mensajes:Q", title="Mensajes")],
    ).properties(height=200)


# ------------------------------------------------------------------ prioridad
def tabla_prioridad(tabla: pd.DataFrame, nombre_col: str, con_municipio: bool) -> None:
    columnas = ["Estado", nombre_col] + (["Municipio"] if con_municipio else []) + ["casos", "en_riesgo", "pct_riesgo"]
    vista = tabla[columnas].copy()
    vista.insert(0, "#", range(1, len(vista) + 1))
    st.dataframe(
        vista, hide_index=True, use_container_width=True,
        column_config={
            "#": st.column_config.NumberColumn("N.º", width="small"),
            "Estado": st.column_config.TextColumn("Estado", width="small"),
            "casos": st.column_config.NumberColumn("Estudiantes"),
            "en_riesgo": st.column_config.NumberColumn("En riesgo"),
            "pct_riesgo": st.column_config.ProgressColumn("% en riesgo", min_value=0, max_value=100, format="%.0f %%"),
        },
    )


def bloque_prioridad(titulo: str, tabla: pd.DataFrame, nombre_col: str, con_municipio: bool) -> None:
    st.markdown(f"**{titulo}**")
    if tabla.empty:
        st.info("Todavía no hay suficientes estudiantes para evaluar.")
        return
    tabla_prioridad(tabla.head(FILAS_VISIBLES), nombre_col, con_municipio)
    if len(tabla) > FILAS_VISIBLES:
        with st.expander(f"Ver la lista completa, {len(tabla)} en total"):
            tabla_prioridad(tabla, nombre_col, con_municipio)


def lista_accion(titulo: str, tabla: pd.DataFrame, nombre_col: str, umbral: float, n: int = 3) -> None:
    st.markdown(f"**{titulo}**")
    en_alerta = datos.en_alerta(tabla, umbral)
    if en_alerta.empty:
        st.write("Ninguno supera el umbral por ahora.")
        return
    for i, fila in enumerate(en_alerta.head(n).itertuples(index=False), 1):
        st.write(f"{i}. **{getattr(fila, nombre_col)}** con {coma(fila.pct_riesgo, 0)} % en riesgo, "
                 f"{int(fila.en_riesgo)} de {int(fila.casos)} estudiantes")
    if len(en_alerta) > n:
        st.caption(f"Y {len(en_alerta) - n} más en alerta.")


def seccion(numero: int, titulo: str, ayuda: str = "") -> None:
    st.divider()
    st.header(f"{numero}. {titulo}")
    if ayuda:
        st.caption(ayuda)


# ------------------------------------------------------------------ parte en vivo
@st.fragment(run_every="3s")
def panel_en_vivo() -> None:
    df_total = datos.cargar_predicciones()  # sin caché: es lo que cambia en vivo
    if df_total.empty:
        st.info("Aún no llegan predicciones. Levanta el consumidor y publica eventos. "
                "En cuanto llegue la primera, este tablero se llena solo.")
        return

    en_vivo = datos.RUTA_PREDICCIONES.exists()
    estado = datos.estado_streaming(df_total, en_vivo)
    seg = datos.segundos_desde_ultima(df_total)
    met = metricas_modelo(str(RUTA_METRICAS))
    umbral_modelo = met.get("umbral_decision")
    esperado = datos.pct_esperado_modelo(met)

    # Filtro de municipio. Vive aquí dentro para que sus opciones se actualicen con los datos.
    municipios = sorted(df_total["municipio"].dropna().unique().tolist())
    previos = [m for m in st.session_state.get("filtro_muni", []) if m not in municipios]
    filtro = st.multiselect("Ver solo estos municipios, opcional", municipios + previos,
                            key="filtro_muni", placeholder="Todos los municipios",
                            help="Aplica a las secciones 1 a 5. El monitoreo siempre usa todos los mensajes.")
    df = df_total[df_total["municipio"].isin(filtro)] if filtro else df_total
    if df.empty:
        st.info("No hay predicciones de los municipios elegidos todavía.")
        return

    res = datos.resumen_general(df)
    st.caption(f"Estado del streaming: **{estado}**. Última predicción {hace(seg)}.")

    # ============================================================ 1. Resumen general
    seccion(1, "Resumen general")
    dist = datos.distribucion_categorias(df)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Predicciones generadas", miles(res["mensajes"]))
    k2.metric("En riesgo", miles(dist.loc[0, "n"]), f"{coma(dist.loc[0, 'pct'])} % del total", delta_color="off")
    k3.metric("Sin riesgo", miles(dist.loc[1, "n"]), f"{coma(dist.loc[1, 'pct'])} % del total", delta_color="off")
    ts = res["ultimo_ts"]
    k4.metric("Última predicción", f"{ts.tz_convert(datos.ZONA_COLOMBIA):%H:%M:%S}" if ts is not None else "sin dato",
              hace(seg), delta_color="off")
    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown("**Distribución de las categorías predichas**")
        dibujar(graf_distribucion, dist)
    with c2:
        st.markdown("**Qué significa cada categoría**")
        st.markdown(
            "- **En riesgo:** el modelo estima que el puntaje global del estudiante quedaría entre el 25 % más bajo de su semestre.\n"
            "- **Sin riesgo:** el modelo no espera un puntaje tan bajo.\n"
            f"- Cuenta como en riesgo toda probabilidad desde {coma(umbral_modelo, 2) if umbral_modelo else 'el umbral del modelo'}."
        )

    # ============================================================ 2. Dónde apoyar primero
    seccion(2, "Dónde apoyar primero",
            "Parte operativa. Ordena colegios y municipios para decidir hacia dónde dirigir tutorías y refuerzo.")

    etiquetas = {str(r.cod_colegio): f"{r.nombre_colegio}, {r.municipio}"
                 for r in df.drop_duplicates("cod_colegio").itertuples()}
    base_col = datos.prioridad_colegios(df, min_casos=min_casos)
    opciones = base_col["Código"].astype(str).head(30).tolist() if not base_col.empty else []
    opciones += [c for c in st.session_state["atendidos"] if c not in opciones]
    st.multiselect("Marcar colegios como atendidos para sacarlos de la lista", opciones,
                   key="atendidos", format_func=lambda c: etiquetas.get(c, c),
                   placeholder="Ninguno por ahora")

    df_vigente = datos.excluir_atendidos(df, st.session_state["atendidos"])
    prio_col = datos.prioridad_colegios(df_vigente, min_casos=min_casos)
    prio_muni = datos.prioridad_municipios(df, min_casos=min_casos)
    alerta_col = datos.en_alerta(prio_col, umbral_pct)
    alerta_muni = datos.en_alerta(prio_muni, umbral_pct)

    actuales = set(alerta_col["Código"].astype(str)) if not alerta_col.empty else set()
    previas = st.session_state["alertas_previas"]
    # Si cambió un ajuste, el filtro o los atendidos, el conjunto de alertas cambia por la selección y no
    # porque haya llegado algo nuevo. En ese caso solo se actualiza la referencia.
    firma = (umbral_pct, min_casos, tuple(sorted(filtro)), tuple(sorted(st.session_state["atendidos"])))
    if st.session_state.get("firma_alertas") != firma:
        previas = None
        st.session_state["firma_alertas"] = firma
    if previas is not None:
        hora = datetime.now(datos.ZONA_COLOMBIA).strftime("%H:%M:%S")
        nombres = {str(r.Código): r.Colegio for r in alerta_col.itertuples()} if not alerta_col.empty else {}
        for cod in sorted(actuales - previas):
            st.session_state["nuevas_alertas"].append((hora, nombres.get(cod, cod)))
    st.session_state["alertas_previas"] = actuales

    n_alta = int((df["prob_riesgo"] >= umbral_prob).sum())
    if not alerta_col.empty or not alerta_muni.empty:
        st.error(
            f"**Alerta.** Hay {plural(len(alerta_col), 'colegio', 'colegios')} y "
            f"{plural(len(alerta_muni), 'municipio', 'municipios')} con más de {umbral_pct:.0f} de cada 100 "
            f"estudiantes en riesgo. Además, {plural(n_alta, 'estudiante tiene', 'estudiantes tienen')} "
            f"una probabilidad de riesgo de {coma(umbral_prob, 2)} o más."
        )
    else:
        st.success(f"Ningún colegio ni municipio supera el {umbral_pct:.0f} % en riesgo por ahora.")

    with st.container(border=True):
        st.markdown("**Apoyar primero en**")
        a1, a2 = st.columns(2)
        with a1:
            lista_accion("Colegios", prio_col, "Colegio", umbral_pct)
        with a2:
            lista_accion("Municipios", prio_muni, "Municipio", umbral_pct)
    if st.session_state["nuevas_alertas"]:
        recientes = st.session_state["nuevas_alertas"][-3:][::-1]
        st.caption("Entraron en alerta desde que abriste el tablero: " +
                   "; ".join(f"{n} a las {h}" for h, n in recientes) + ".")

    promedio = res["pct_riesgo"]
    tab_col, tab_muni = st.tabs(["Prioridad de colegios", "Prioridad de municipios"])
    with tab_col:
        bloque_prioridad("Colegios de mayor a menor % en riesgo", datos.con_estado(prio_col, umbral_pct, promedio),
                         "Colegio", True)
    with tab_muni:
        bloque_prioridad("Municipios de mayor a menor % en riesgo", datos.con_estado(prio_muni, umbral_pct, promedio),
                         "Municipio", False)
    st.caption("Estado: Alerta supera el umbral elegido, Vigilar supera el promedio general y Normal es el resto. "
               "Los estudiantes sin colegio no entran en la lista de colegios.")

    # ============================================================ 3. Predicciones en tiempo real
    seccion(3, "Predicciones en tiempo real",
            "Cómo van llegando las predicciones y cómo se distribuyen. Los gráficos se actualizan solos.")
    g1, g2 = st.columns(2)
    with g1:
        st.markdown("**Estudiantes acumulados por categoría**")
        acum = datos.acumulado_por_categoria(df)
        if acum.empty:
            st.caption("Se mostrará cuando lleguen más predicciones.")
        else:
            dibujar(graf_acumulado, acum)
    with g2:
        st.markdown("**Evolución del % en riesgo**")
        ev = datos.evolucion_df(df)
        if ev.empty:
            st.caption("Se mostrará cuando lleguen más predicciones.")
        else:
            dibujar(graf_evolucion, ev, esperado)
            st.caption("El % se estabiliza a medida que llegan más estudiantes. La línea punteada azul marca lo que el modelo "
                       + (f"produjo con estudiantes que no vio al entrenar, {coma(esperado)} %." if esperado is not None
                          else "produjo en su prueba."))
    g3, g4 = st.columns(2)
    with g3:
        st.markdown("**Distribución de la probabilidad de riesgo**")
        dibujar(graf_probabilidades, df, umbral_modelo)
        st.caption("Cada estudiante recibe una probabilidad. A la derecha de la línea punteada gris, que es el umbral del modelo "
                   + (f"de {coma(umbral_modelo, 2)}, " if umbral_modelo else "") + "se clasifica en riesgo.")
    with g4:
        st.markdown("**% en riesgo por estrato**")
        estrato = datos.riesgo_por_estrato(df)
        if not estrato.empty:
            dibujar(graf_estratos, estrato)

    # ============================================================ 4. Registro de mensajes
    seccion(4, "Registro de mensajes",
            "Los últimos mensajes que el consumidor recibió y procesó.")
    ult = df.sort_values("ts", ascending=False, na_position="last").head(15).copy()
    registro = pd.DataFrame({
        "ID del evento": ult["event_id"].astype(int),
        "Fecha y hora": datos.hora_colombia(ult["ts"]).dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Colegio": ult["nombre_colegio"],
        "Municipio": ult["municipio"],
        "Estrato": ult["estrato"].replace({"Sin Estrato": "Sin estrato"}),
        "Semestre": ult["semestre"],
        "Probabilidad": ult["prob_riesgo"],
        "Predicción": ult["pred_riesgo"].map(datos.CATEGORIAS),
        "Estado": "Procesado",
    })
    st.dataframe(registro, hide_index=True, use_container_width=True,
                 column_config={"Probabilidad": st.column_config.NumberColumn(format="%.2f"),
                                "Colegio": st.column_config.TextColumn(width="medium"),
                                "ID del evento": st.column_config.NumberColumn(width="small", format="%d"),
                                "Semestre": st.column_config.NumberColumn(width="small"),
                                "Estado": st.column_config.TextColumn(width="small")})
    st.caption("Fecha y hora en horario de Colombia. Todo mensaje que aparece aquí ya pasó por el modelo.")

    # ============================================================ 5. Interpretación
    seccion(5, "Interpretación de resultados")
    puntos = []
    pct_ahora = res["pct_riesgo"]
    if esperado is not None:
        diferencia = pct_ahora - esperado
        if abs(diferencia) <= 3:
            lectura = "es coherente con lo que el modelo produjo en su prueba."
        elif diferencia > 0:
            lectura = "es más alto de lo que el modelo produjo en su prueba, vale la pena revisar si llegan colegios de mayor riesgo."
        else:
            lectura = "es más bajo de lo que el modelo produjo en su prueba."
        puntos.append(f"El modelo marcó **{coma(pct_ahora)} %** de los estudiantes como en riesgo. En su prueba, con estudiantes "
                      f"que no vio al entrenar, marcó **{coma(esperado)} %**, así que el valor actual {lectura}")
    else:
        puntos.append(f"El modelo marcó **{coma(pct_ahora)} %** de los estudiantes como en riesgo.")
    puntos.append("Por definición, el 25 % de los estudiantes está en riesgo. El modelo marca más a propósito, para no dejar "
                  "pasar casos. Por eso el % en riesgo sirve para comparar colegios y municipios entre sí y no como cifra real.")
    le = datos.lectura_estratos(df)
    if le:
        frase = (f"El % en riesgo es mayor en **{le[0]}**, con {coma(le[1])} %, y menor en **{le[2]}**, con {coma(le[3])} %.")
        if le[0][-1].isdigit() and le[2][-1].isdigit() and int(le[0][-1]) < int(le[2][-1]):
            frase += " Los estratos más bajos concentran más riesgo, como se espera en esta prueba."
        puntos.append(frase)
    if not prio_muni.empty:
        top = prio_muni.head(3)["Municipio"].tolist()
        puntos.append("Los municipios con mayor % en riesgo hasta ahora son " + ", ".join(top) + ".")
    puntos.append(f"Hay {plural(len(alerta_col), 'colegio', 'colegios')} sobre el umbral de {umbral_pct:.0f} %. "
                  "Con pocos estudiantes por colegio, el % puede cambiar rápido, por eso conviene leerlo junto con el número de estudiantes.")
    st.markdown("\n".join(f"- {p}" for p in puntos))

    if met:
        t = met.get("metricas_test", {})
        azar = met.get("linea_base", {}).get("prevalencia_test_(PR-AUC_del_azar)", 0)
        st.markdown("**Qué tan bien acierta el modelo**")
        st.caption("Resultados en el conjunto de prueba, con estudiantes que el modelo no vio al entrenar. "
                   "Los mensajes en vivo no traen el puntaje real, por eso no se puede medir el acierto en directo.")
        m1, m2, m3 = st.columns(3)
        m1.metric("Detecta", f"{coma(100 * t.get('recall', 0), 0)} de cada 100",
                  "estudiantes que de verdad quedaron en riesgo", delta_color="off")
        m2.metric("Acierta", f"{coma(100 * t.get('precision', 0), 0)} de cada 100",
                  "de los que marca en riesgo", delta_color="off")
        m3.metric("PR-AUC", coma(t.get("pr_auc", 0), 3), f"{coma(azar, 3)} si adivinara al azar", delta_color="off")
        mc = t.get("matriz_confusion")
        if mc:
            st.markdown("**Matriz de confusión en la prueba**")
            dibujar(graf_matriz, mc)
            reales = mc["tp"] + mc["fn"]
            st.markdown(
                f"- De {miles(reales)} estudiantes que de verdad quedaron en riesgo, el modelo detectó {miles(mc['tp'])} "
                f"y se le escaparon {miles(mc['fn'])}.\n"
                f"- Marcó en riesgo a {miles(mc['fp'])} estudiantes que no lo estaban. Es el costo de no dejar pasar casos."
            )

    # ============================================================ 6. Monitoreo del streaming
    seccion(6, "Monitoreo del streaming",
            "Confirma que el flujo de Kafka está activo. Usa todos los mensajes, sin importar el filtro.")
    retraso = datos.retraso_mediano_seg(df_total)
    ritmo = datos.mensajes_ultimo_minuto(df_total)
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Estado", estado)
    s2.metric("Mensajes procesados", miles(len(df_total)))
    s3.metric("Último mensaje", hace(seg), f"{ritmo} en el último minuto", delta_color="off")
    s4.metric("Retraso mediano", f"{coma(retraso, 2)} s" if retraso is not None else "sin dato",
              "del envío a la predicción", delta_color="off")
    ritmo_df = datos.mensajes_por_ventana(df_total)
    if not ritmo_df.empty:
        dibujar(graf_ritmo, ritmo_df)
    version = str(df_total["model_version"].dropna().iloc[-1]) if df_total["model_version"].notna().any() else "sin dato"
    origen = "predicciones del consumidor de Kafka" if en_vivo else "datos de ejemplo del repositorio"
    st.caption(f"Flujo: Data Warehouse, productor, tópico saber11.estudiantes, consumidor con el modelo, "
               f"archivo de predicciones y este tablero. Origen actual: {origen}. Versión del modelo: {version}. "
               f"El estado es Activo cuando llegó un mensaje en los últimos {datos.LIMITE_ACTIVO_SEG} segundos.")
    st.caption("El flujo es una simulación. El productor reenvía estudiantes del conjunto de prueba, que el modelo no vio "
               "al entrenar, como si llegaran en tiempo real. No hay estudiantes nuevos llegando de un sistema externo.")


panel_en_vivo()

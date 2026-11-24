"""Lógica de datos del dashboard operativo (sin Streamlit, para poder probarla).

Lee las predicciones que escribe el consumidor de Kafka
(`data/streaming/predicciones.jsonl`, una línea JSON por predicción) y las
convierte en las tablas que necesita el tablero: prioridad de colegios y
municipios, alertas y apoyo analítico.

El consumidor (streaming/consumidor.py) escribe estos campos por línea:
    event_id, model_id, model_version, prediction (0/1), prediction_label,
    probability_riesgo, event_ts, pred_ts, semestre, estu_mcpio_reside,
    fami_estratovivienda, cole_cod_icfes, cole_nombre_establecimiento

Aquí se normalizan a nombres cortos y estables para el tablero. Se aceptan
también los nombres "ideales" de la consigna (municipio, prob_riesgo, ...) por
si el formato del mensaje cambia, para que el dashboard no se rompa.
"""
from __future__ import annotations

import json
import os
from datetime import timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[2]

# Ruta real (la que escribe el consumidor). Si no existe, se usa el ejemplo del repo,
# así el tablero nunca sale vacío en una demostración o mientras corre el pipeline.
RUTA_PREDICCIONES = Path(
    os.getenv("RUTA_PREDICCIONES", RAIZ / "data" / "streaming" / "predicciones.jsonl")
)
RUTA_EJEMPLO = RAIZ / "streaming" / "ejemplo_predicciones.jsonl"

# nombre canónico -> posibles nombres en el JSON (consumidor real primero, consigna después)
_ALIAS = {
    "event_id": ["event_id"],
    "ts": ["pred_ts", "ts", "event_ts"],
    "municipio": ["estu_mcpio_reside", "municipio"],
    "cod_colegio": ["cole_cod_icfes", "cod_colegio"],
    "nombre_colegio": ["cole_nombre_establecimiento", "nombre_colegio"],
    "estrato": ["fami_estratovivienda", "estrato"],
    "semestre": ["semestre"],
    "prob_riesgo": ["probability_riesgo", "prob_riesgo"],
    "pred_riesgo": ["prediction", "pred_riesgo"],
    "model_version": ["model_version"],
    "evento_ts": ["event_ts"],
}

COLUMNAS = list(_ALIAS)

# Código que el productor envía cuando el estudiante no tiene colegio. No es una
# institución a la que se pueda dar apoyo, por eso no entra en la lista de colegios.
CODIGO_SIN_COLEGIO = "-1"


def ruta_en_uso() -> Path:
    """Devuelve la ruta que se va a leer (real si existe, si no el ejemplo)."""
    return RUTA_PREDICCIONES if RUTA_PREDICCIONES.exists() else RUTA_EJEMPLO


def _primer_valor(registro: dict, nombres: list[str]):
    for n in nombres:
        if n in registro and registro[n] is not None:
            return registro[n]
    return None


def _normalizar(registro: dict) -> dict:
    return {canon: _primer_valor(registro, nombres) for canon, nombres in _ALIAS.items()}


def cargar_predicciones(ruta: str | os.PathLike | None = None) -> pd.DataFrame:
    """Lee el .jsonl (una línea por predicción) y devuelve un DataFrame normalizado.

    Tolera líneas corruptas o una última línea a medio escribir (el consumidor
    escribe en vivo): esas líneas se saltan en vez de romper el tablero.
    """
    ruta = Path(ruta) if ruta is not None else ruta_en_uso()
    if not ruta.exists():
        return pd.DataFrame(columns=COLUMNAS)

    filas: list[dict] = []
    with open(ruta, encoding="utf-8") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            try:
                filas.append(_normalizar(json.loads(linea)))
            except json.JSONDecodeError:
                continue  # línea incompleta o malformada: se ignora

    df = pd.DataFrame(filas, columns=COLUMNAS)
    if df.empty:
        return df

    # Tipos: numéricos donde corresponde; evento único por si se releyó un repetido.
    df["prob_riesgo"] = pd.to_numeric(df["prob_riesgo"], errors="coerce")
    df["pred_riesgo"] = pd.to_numeric(df["pred_riesgo"], errors="coerce").fillna(0).astype(int)
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df["evento_ts"] = pd.to_datetime(df["evento_ts"], errors="coerce", utc=True)
    for col in ("municipio", "nombre_colegio", "estrato", "cod_colegio"):
        df[col] = df[col].astype("string").fillna("No informa")
    df = df.drop_duplicates(subset="event_id", keep="last").reset_index(drop=True)
    return df


def resumen_general(df: pd.DataFrame) -> dict:
    """KPIs globales para las tarjetas de arriba."""
    n = len(df)
    en_riesgo = int(df["pred_riesgo"].sum()) if n else 0
    return {
        "mensajes": n,
        "en_riesgo": en_riesgo,
        "pct_riesgo": round(100 * en_riesgo / n, 1) if n else 0.0,
        "prob_media": round(float(df["prob_riesgo"].mean()), 3) if n else 0.0,
        "colegios": int(df.loc[df["cod_colegio"] != CODIGO_SIN_COLEGIO, "cod_colegio"].nunique()) if n else 0,
        "municipios": int(df["municipio"].nunique()) if n else 0,
        "ultimo_ts": df["ts"].max() if n and df["ts"].notna().any() else None,
    }


def _prioridad(df: pd.DataFrame, clave: list[str], etiqueta: dict) -> pd.DataFrame:
    """Agrupa por colegio o municipio y calcula casos, en riesgo, % y prob. media."""
    if df.empty:
        return pd.DataFrame()
    g = (
        df.groupby(clave, dropna=False)
        .agg(casos=("pred_riesgo", "size"),
             en_riesgo=("pred_riesgo", "sum"),
             prob_media=("prob_riesgo", "mean"))
        .reset_index()
    )
    g["pct_riesgo"] = (100 * g["en_riesgo"] / g["casos"]).round(1)
    g["prob_media"] = g["prob_media"].round(3)
    g = g.rename(columns=etiqueta)
    return g.sort_values(["pct_riesgo", "casos"], ascending=[False, False]).reset_index(drop=True)


def prioridad_colegios(df: pd.DataFrame, min_casos: int = 1) -> pd.DataFrame:
    """Colegios ordenados por mayor % en riesgo. Filtra los que tienen pocos casos
    y deja por fuera a los estudiantes sin colegio."""
    df = df[df["cod_colegio"] != CODIGO_SIN_COLEGIO]
    tabla = _prioridad(
        df, ["cod_colegio", "nombre_colegio", "municipio"],
        {"cod_colegio": "Código", "nombre_colegio": "Colegio", "municipio": "Municipio"},
    )
    if tabla.empty:
        return tabla
    return tabla[tabla["casos"] >= min_casos].reset_index(drop=True)


def prioridad_municipios(df: pd.DataFrame, min_casos: int = 1) -> pd.DataFrame:
    """Municipios ordenados por mayor % en riesgo."""
    tabla = _prioridad(df, ["municipio"], {"municipio": "Municipio"})
    if tabla.empty:
        return tabla
    return tabla[tabla["casos"] >= min_casos].reset_index(drop=True)


def en_alerta(prioridad: pd.DataFrame, umbral_pct: float) -> pd.DataFrame:
    """Filas de una tabla de prioridad cuyo % en riesgo supera el umbral."""
    if prioridad.empty:
        return prioridad
    return prioridad[prioridad["pct_riesgo"] > umbral_pct].reset_index(drop=True)


def riesgo_por_estrato(df: pd.DataFrame) -> pd.DataFrame:
    """Apoyo analítico: % en riesgo por estrato de la vivienda."""
    if df.empty:
        return pd.DataFrame()
    g = (
        df.groupby("estrato", dropna=False)
        .agg(casos=("pred_riesgo", "size"), en_riesgo=("pred_riesgo", "sum"))
        .reset_index()
    )
    g["pct_riesgo"] = (100 * g["en_riesgo"] / g["casos"]).round(1)
    return g.sort_values("estrato").reset_index(drop=True)


def accion_sugerida(prioridad_col: pd.DataFrame, umbral_pct: float, top: int = 5) -> str:
    """Texto corto operativo: a qué colegios priorizar apoyo."""
    if prioridad_col.empty:
        return "Aún no llegan predicciones. El tablero se actualiza solo cuando el consumidor empieza a escribir."
    alerta = en_alerta(prioridad_col, umbral_pct)
    if alerta.empty:
        return (f"Ningún colegio supera el umbral de {umbral_pct:.0f}% en riesgo. "
                "Mantener el seguimiento; sin acción prioritaria por ahora.")
    nombres = alerta["Colegio"].head(top).tolist()
    lista = "; ".join(nombres)
    return (f"Priorizar tutorías y refuerzo en estos {len(nombres)} colegios "
            f"(los de mayor % en riesgo, por encima de {umbral_pct:.0f}%): {lista}.")


# ------------------------- Ayudas para la vista del tablero -------------------------

ORDEN_ESTRATO = ["Estrato 1", "Estrato 2", "Estrato 3", "Estrato 4", "Estrato 5", "Estrato 6",
                 "Sin Estrato", "No informa"]
ZONA_COLOMBIA = timezone(timedelta(hours=-5))


def excluir_atendidos(df: pd.DataFrame, atendidos) -> pd.DataFrame:
    """Quita los colegios que el usuario ya marcó como atendidos."""
    if df.empty or not atendidos:
        return df
    return df[~df["cod_colegio"].isin([str(a) for a in atendidos])]


def con_estado(prioridad: pd.DataFrame, umbral_pct: float, promedio_pct: float) -> pd.DataFrame:
    """Agrega una columna Estado escrita con palabras, para no depender del color.
    Alerta supera el umbral, Vigilar supera el promedio general y Normal es el resto."""
    t = prioridad.copy()
    if t.empty:
        t["Estado"] = []
        return t

    def _estado(p: float) -> str:
        if p > umbral_pct:
            return "Alerta"
        return "Vigilar" if p > promedio_pct else "Normal"

    t["Estado"] = t["pct_riesgo"].apply(_estado)
    return t


def hora_colombia(serie: pd.Series) -> pd.Series:
    """Pasa una columna de fechas en UTC a hora de Colombia, que no tiene horario de verano."""
    return serie.dt.tz_convert(ZONA_COLOMBIA)


def _indices_muestra(n: int, max_puntos: int) -> list[int]:
    """Índices repartidos de forma pareja para no dibujar miles de puntos."""
    if n <= max_puntos:
        return list(range(n))
    return sorted({round(k * (n - 1) / (max_puntos - 1)) for k in range(max_puntos)})


def evolucion_df(df: pd.DataFrame, max_puntos: int = 100) -> pd.DataFrame:
    """% en riesgo acumulado según llegan las predicciones. Se omiten los primeros puntos
    porque con pocos casos el porcentaje salta demasiado."""
    n = len(df)
    if n < 10:
        return pd.DataFrame(columns=["n", "pct"])
    d = df.sort_values("ts", na_position="first")
    pct = d["pred_riesgo"].to_numpy().cumsum() / (np.arange(n) + 1) * 100
    inicio = max(5, n // 10) if n > 40 else 0
    idx = [i + inicio for i in _indices_muestra(n - inicio, max_puntos)]
    return pd.DataFrame({"n": [i + 1 for i in idx], "pct": [round(float(pct[i]), 1) for i in idx]})


def evolucion_pct(df: pd.DataFrame, max_puntos: int = 100) -> list[float]:
    """Lo mismo que evolucion_df pero solo con los valores."""
    return evolucion_df(df, max_puntos)["pct"].tolist()


CATEGORIAS = {1: "En riesgo", 0: "Sin riesgo"}


def distribucion_categorias(df: pd.DataFrame) -> pd.DataFrame:
    """Cuántos estudiantes cayeron en cada categoría predicha."""
    n = len(df)
    filas = []
    for cod, nombre in CATEGORIAS.items():
        c = int((df["pred_riesgo"] == cod).sum()) if n else 0
        filas.append({"categoria": nombre, "n": c, "pct": round(100 * c / n, 1) if n else 0.0})
    return pd.DataFrame(filas)


def acumulado_por_categoria(df: pd.DataFrame, max_puntos: int = 80) -> pd.DataFrame:
    """Estudiantes acumulados en cada categoría según llegan las predicciones."""
    n = len(df)
    if n < 2:
        return pd.DataFrame(columns=["n", "categoria", "acum"])
    d = df.sort_values("ts", na_position="first")
    riesgo = d["pred_riesgo"].to_numpy().cumsum()
    filas = []
    for i in _indices_muestra(n, max_puntos):
        filas.append({"n": i + 1, "categoria": CATEGORIAS[1], "acum": int(riesgo[i])})
        filas.append({"n": i + 1, "categoria": CATEGORIAS[0], "acum": int(i + 1 - riesgo[i])})
    return pd.DataFrame(filas)


def pct_esperado_modelo(metricas: dict) -> float | None:
    """% de estudiantes que el modelo marcó en riesgo en la prueba, con datos que no vio al entrenar.
    Sirve de referencia: no es el 25 % real porque el modelo marca de más a propósito."""
    mc = metricas.get("metricas_test", {}).get("matriz_confusion") if metricas else None
    if not mc:
        return None
    total = sum(mc.values())
    return round(100 * (mc["tp"] + mc["fp"]) / total, 1) if total else None


def lectura_estratos(df: pd.DataFrame, min_casos: int = 30):
    """Estrato con mayor y menor % en riesgo, entre los que tienen suficientes estudiantes.
    Devuelve None si no hay al menos dos estratos para comparar."""
    e = riesgo_por_estrato(df)
    if e.empty:
        return None
    e = e[(e["casos"] >= min_casos) & (e["estrato"].astype(str).str.startswith("Estrato"))]
    if len(e) < 2:
        return None
    mayor, menor = e.loc[e["pct_riesgo"].idxmax()], e.loc[e["pct_riesgo"].idxmin()]
    return str(mayor["estrato"]), float(mayor["pct_riesgo"]), str(menor["estrato"]), float(menor["pct_riesgo"])


# ------------------------- Monitoreo del streaming -------------------------

LIMITE_ACTIVO_SEG = 30


def segundos_desde_ultima(df: pd.DataFrame, ahora=None):
    """Segundos desde la última predicción recibida, o None si no hay fecha."""
    if df.empty or not df["ts"].notna().any():
        return None
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz="UTC")
    return float((ahora - df["ts"].max()).total_seconds())


def estado_streaming(df: pd.DataFrame, en_vivo: bool, ahora=None) -> str:
    """Activo si llegó un mensaje hace menos de LIMITE_ACTIVO_SEG segundos.
    El tablero no habla con Kafka, así que el estado se deduce de lo que el consumidor escribe."""
    if df.empty:
        return "Sin datos"
    if not en_vivo:
        return "Datos de ejemplo"
    seg = segundos_desde_ultima(df, ahora)
    return "Activo" if seg is not None and seg <= LIMITE_ACTIVO_SEG else "Sin actividad"


def mensajes_ultimo_minuto(df: pd.DataFrame, ahora=None) -> int:
    if df.empty or not df["ts"].notna().any():
        return 0
    ahora = ahora if ahora is not None else pd.Timestamp.now(tz="UTC")
    return int((df["ts"] >= ahora - pd.Timedelta(seconds=60)).sum())


def retraso_mediano_seg(df: pd.DataFrame):
    """Mediana de segundos entre que el productor envía el evento y el consumidor termina la predicción."""
    if df.empty or "evento_ts" not in df:
        return None
    d = (df["ts"] - df["evento_ts"]).dt.total_seconds().dropna()
    return float(d.median()) if len(d) else None


def mensajes_por_ventana(df: pd.DataFrame, ventana_seg: int = 10, ventanas: int = 30) -> pd.DataFrame:
    """Mensajes recibidos en cada ventana de tiempo, para ver el ritmo de llegada."""
    t = df["ts"].dropna() if not df.empty else pd.Series(dtype="datetime64[ns, UTC]")
    if t.empty:
        return pd.DataFrame(columns=["hora", "mensajes"])
    c = t.dt.floor(f"{ventana_seg}s").value_counts().sort_index()
    rango = pd.date_range(end=c.index.max(), periods=ventanas, freq=f"{ventana_seg}s")
    c = c.reindex(rango, fill_value=0)
    return pd.DataFrame({"hora": c.index.tz_convert(ZONA_COLOMBIA), "mensajes": c.to_numpy()})

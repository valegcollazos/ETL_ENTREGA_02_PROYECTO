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
from pathlib import Path

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
}

COLUMNAS = list(_ALIAS)


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
        "colegios": int(df["cod_colegio"].nunique()) if n else 0,
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
    """Colegios ordenados por mayor % en riesgo. Filtra los que tienen pocos casos."""
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

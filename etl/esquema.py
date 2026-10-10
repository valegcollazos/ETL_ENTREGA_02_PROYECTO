"""
Esquema de staging v1: estructura común de las tres fuentes.

Describe el archivo staging_*.parquet que produce cada rama (API 2020, CSV 2019,
DB 2025). La versión legible está en el README (sección 4.1); si se cambia
el esquema, se actualizan los dos y se sube ESQUEMA_VERSION.
"""
from __future__ import annotations

import pandas as pd

ESQUEMA_VERSION = "v1"

# --- Centinelas y textos fijos ----------------------------------------------
COD_ICFES_SIN_COLEGIO = -1
NSE_SIN_DATO = -1
TEXTO_NO_INFORMA = "No informa"
TEXTO_NO_APLICA = "No aplica"
NOMBRE_SIN_COLEGIO = "Sin colegio (inscripción individual)"

# --- Las 25 columnas, en el orden exacto del esquema -------------------------
# tipo: "texto" -> pandas "string" | "entero" -> "Int64" | "decimal" -> "float64"
COLUMNAS_ESQUEMA: dict[str, str] = {
    # Identificación y tiempo
    "estu_consecutivo": "texto",
    "periodo": "texto",
    "anio": "entero",
    "semestre": "entero",
    # Ubicación
    "estu_depto_reside": "texto",
    "estu_mcpio_reside": "texto",
    # Colegio
    "cole_codigo_icfes": "entero",
    "cole_nombre_establecimiento": "texto",
    "cole_naturaleza": "texto",
    "cole_calendario": "texto",
    "cole_area_ubicacion": "texto",
    # Socioeconómicas
    "fami_estratovivienda": "texto",
    "fami_educacionpadre": "texto",
    "fami_educacionmadre": "texto",
    "fami_tieneinternet": "texto",
    "fami_tienecomputador": "texto",
    "fami_personashogar": "texto",
    "estu_nse_individual": "entero",
    "estu_inse_individual": "decimal",
    # Resultados
    "punt_global": "entero",
    "punt_matematicas": "entero",
    "punt_lectura_critica": "entero",
    "punt_c_naturales": "entero",
    "punt_sociales_ciudadanas": "entero",
    "punt_ingles": "entero",
}

DTYPE_PANDAS = {"texto": "string", "entero": "Int64", "decimal": "float64"}

COLUMNAS = list(COLUMNAS_ESQUEMA)                                   # las 25
COLUMNAS_CALCULADAS = ["anio", "semestre"]                           # las calcula la limpieza
COLUMNAS_ENTRADA = [c for c in COLUMNAS if c not in COLUMNAS_CALCULADAS]  # las 23 que entrega cada rama

COLUMNAS_PUNTAJE = [
    "punt_global", "punt_matematicas", "punt_lectura_critica",
    "punt_c_naturales", "punt_sociales_ciudadanas", "punt_ingles",
]
COLUMNAS_COLEGIO_TEXTO = [
    "cole_nombre_establecimiento", "cole_naturaleza",
    "cole_calendario", "cole_area_ubicacion",
]
COLUMNAS_TEXTO = [c for c, t in COLUMNAS_ESQUEMA.items() if t == "texto"]
# Texto que se repara y se rellena con "No informa" (las llaves no: se validan aparte)
COLUMNAS_TEXTO_DESCRIPTIVO = [c for c in COLUMNAS_TEXTO if c not in ("estu_consecutivo", "periodo")]

# Nombres de archivo fijos (sección 7 de la guía)
ARCHIVOS = {
    "raw_api": "raw_api.parquet",
    "raw_csv": "raw_csv.parquet",
    "raw_db": "raw_db.parquet",
    "staging_api": "staging_api.parquet",
    "staging_csv": "staging_csv.parquet",
    "staging_db": "staging_db.parquet",
    "staging_merged": "staging_merged.parquet",
}


class EsquemaError(ValueError):
    """Un DataFrame no cumple el esquema de staging."""


def validar_esquema(df: pd.DataFrame, nombre: str = "staging") -> None:
    """Lanza EsquemaError si df no tiene exactamente las 25 columnas, en orden,
    con los tipos del esquema, o si las llaves tienen nulos/duplicados.
    `nombre` aparece en el mensaje para saber qué archivo falló."""
    problemas: list[str] = []

    faltantes = [c for c in COLUMNAS if c not in df.columns]
    extras = [c for c in df.columns if c not in COLUMNAS_ESQUEMA]
    if faltantes:
        problemas.append(f"faltan columnas: {faltantes}")
    if extras:
        problemas.append(f"sobran columnas: {extras}")
    if not faltantes and not extras and list(df.columns) != COLUMNAS:
        problemas.append("las columnas no están en el orden del esquema")

    for col, tipo in COLUMNAS_ESQUEMA.items():
        if col not in df.columns:
            continue
        esperado = DTYPE_PANDAS[tipo]
        if str(df[col].dtype) != esperado:
            problemas.append(f"'{col}' es {df[col].dtype}, el esquema pide {esperado}")

    if "estu_consecutivo" in df.columns:
        n_nulos = int(df["estu_consecutivo"].isna().sum())
        n_dup = int(df["estu_consecutivo"].duplicated().sum())
        if n_nulos:
            problemas.append(f"estu_consecutivo tiene {n_nulos} nulos")
        if n_dup:
            problemas.append(f"estu_consecutivo tiene {n_dup} duplicados")
    if "periodo" in df.columns and df["periodo"].isna().any():
        problemas.append(f"periodo tiene {int(df['periodo'].isna().sum())} nulos")

    if problemas:
        raise EsquemaError(
            f"[{nombre}] no cumple el esquema {ESQUEMA_VERSION}:\n  - " + "\n  - ".join(problemas)
        )

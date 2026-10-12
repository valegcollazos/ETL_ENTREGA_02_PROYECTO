from __future__ import annotations

import argparse
import logging

from etl.esquema import ARCHIVOS, COLUMNAS_ENTRADA, EsquemaError
from etl.limpieza import limpiar
from etl.staging import escribir_parquet, leer_parquet, ruta_staging

log = logging.getLogger(__name__)

# el conjunto 2020-1 no trae estas dos, así que pueden faltar
COLUMNAS_PUEDEN_FALTAR = ("estu_nse_individual", "estu_inse_individual")

# ojo: el conjunto 2020-2 de la API publica el periodo como 20204
CORRECCION_PERIODO = {"20204": "20202"}


def transform_api(staging_dir: str | None = None, **kwargs) -> str:
    carpeta = ruta_staging(staging_dir)
    df = leer_parquet(carpeta / ARCHIVOS["raw_api"])
    log.info("raw_api: %s filas, %d columnas.", f"{len(df):,}", df.shape[1])

    # ojo: si falta NSE/INSE se crean en nulo, limpiar() las rellena después
    for col in COLUMNAS_PUEDEN_FALTAR:
        if col not in df.columns:
            df[col] = None
        else:
            log.info("'%s': %d filas sin dato.", col, int(df[col].isna().sum()))

    faltantes = [c for c in COLUMNAS_ENTRADA if c not in df.columns]
    if faltantes:
        raise EsquemaError(f"La API no trae columnas del esquema: {faltantes}")

    # ojo: la API trae muchas columnas de más, solo se dejan las 23 del esquema
    log.info("Columnas de la API que se descartan: %d", df.shape[1] - len(COLUMNAS_ENTRADA))
    df = df[COLUMNAS_ENTRADA]

    # ojo: aquí se corrige el periodo, antes de limpiar(), para que anio y semestre salgan bien
    df = df.copy()
    corregidas = int(df["periodo"].isin(CORRECCION_PERIODO).sum())
    df["periodo"] = df["periodo"].replace(CORRECCION_PERIODO)
    log.info("Periodos corregidos %s: %d filas.", CORRECCION_PERIODO, corregidas)

    limpio = limpiar(df, fuente="api_2020")
    
    salida = carpeta / ARCHIVOS["staging_api"]
    escribir_parquet(limpio, salida)
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", default=None)
    print(transform_api(ap.parse_args().staging))

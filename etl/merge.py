"""merge (Valeria): apila los tres staging en staging_merged.parquet.

Antes de apilar verifica que cada archivo cumpla el esquema (25 columnas,
orden, tipos, llaves). Si uno no cumple, falla diciendo cuál y por qué.

Probar fuera de Airflow:
    python -m etl.merge --staging data/staging
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd

from etl.esquema import ARCHIVOS, EsquemaError, validar_esquema
from etl.staging import escribir_parquet, leer_parquet, ruta_staging

log = logging.getLogger(__name__)

FUENTES = ("staging_csv", "staging_api", "staging_db")  # 2019, 2020, 2025


def merge(staging_dir: str | None = None, fuentes=FUENTES) -> str:
    carpeta = ruta_staging(staging_dir)
    partes, errores = [], []

    for clave in fuentes:
        ruta = carpeta / ARCHIVOS[clave]
        try:
            df = leer_parquet(ruta)
            validar_esquema(df, nombre=ARCHIVOS[clave])
        except (FileNotFoundError, EsquemaError) as e:
            errores.append(str(e))
            continue
        log.info("%s OK: %s filas, años %s", ARCHIVOS[clave], f"{len(df):,}",
                 sorted(df["anio"].dropna().unique().tolist()))
        partes.append(df)

    if errores:  # se reportan TODOS los archivos malos de una vez
        raise EsquemaError("merge detenido:\n" + "\n".join(errores))

    merged = pd.concat(partes, ignore_index=True)
    validar_esquema(merged, nombre=ARCHIVOS["staging_merged"])  # incluye duplicados entre fuentes

    log.info("Filas por año en el merge:\n%s", merged["anio"].value_counts().sort_index().to_string())
    salida = escribir_parquet(merged, carpeta / ARCHIVOS["staging_merged"])
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    p.add_argument("--solo", nargs="*", default=None,
                   help="probar con menos fuentes, ej.: --solo staging_db")
    a = p.parse_args()
    print(merge(a.staging, tuple(a.solo) if a.solo else FUENTES))

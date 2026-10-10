"""Rama DB (Valeria) — transform_db.

raw_db.parquet -> limpieza compartida -> staging_db.parquet (se sobreescribe).
La tabla raw de la entrega 1 ya trae las 23 columnas con los nombres del
esquema, así que aquí no hay que renombrar ni crear columnas.

Probar fuera de Airflow:
    python -m etl.db.transform_db --staging data/staging
"""
from __future__ import annotations

import argparse
import logging

from etl.esquema import ARCHIVOS
from etl.limpieza import limpiar
from etl.staging import escribir_parquet, leer_parquet, ruta_staging

log = logging.getLogger(__name__)


def transform_db(staging_dir: str | None = None) -> str:
    carpeta = ruta_staging(staging_dir)
    df_raw = leer_parquet(carpeta / ARCHIVOS["raw_db"])
    df = limpiar(df_raw, fuente="db_2025")
    salida = escribir_parquet(df, carpeta / ARCHIVOS["staging_db"])
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    print(transform_db(p.parse_args().staging))

"""Rama DB (Valeria) — read_db.

Lee la tabla raw de la entrega 1 (raw_saber11), SOLO Valle del Cauca y SOLO el
año de la rama (2025), filtrando dentro de la consulta para no traer todo el país.
Salida: raw_db.parquet en la carpeta de staging (se sobreescribe).

Probar fuera de Airflow:
    python -m etl.db.read_db --staging data/staging
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd
from sqlalchemy import bindparam, text

from etl.config import get_engine_raw
from etl.esquema import ARCHIVOS, COLUMNAS_ENTRADA
from etl.staging import escribir_parquet, ruta_staging

log = logging.getLogger(__name__)

RAW_TABLE = "raw_saber11"
ANIO_RAMA = 2025
# En la base de la entrega 1 aparece como VALLE; se dejan ambas por si acaso.
DEPTOS_VALLE = ("VALLE", "VALLE DEL CAUCA")


def listar_valores_valle(engine, tabla: str = RAW_TABLE) -> pd.DataFrame:
    """Valores únicos de estu_depto_reside que contienen VALLE (para la ficha de la fuente)."""
    q = text(f"""
        SELECT estu_depto_reside, COUNT(*) AS filas
        FROM {tabla}
        WHERE UPPER(TRIM(estu_depto_reside)) LIKE '%VALLE%'
        GROUP BY estu_depto_reside
    """)
    with engine.connect() as conn:
        return pd.read_sql(q, conn)


def read_db(staging_dir: str | None = None, anio: int = ANIO_RAMA,
            deptos=DEPTOS_VALLE, engine=None) -> str:
    """Extrae Valle + anio desde la tabla raw y escribe raw_db.parquet. Devuelve la ruta."""
    engine = engine or get_engine_raw()
    salida = ruta_staging(staging_dir) / ARCHIVOS["raw_db"]

    valores = listar_valores_valle(engine)
    log.info("Valores de estu_depto_reside que contienen VALLE:\n%s", valores.to_string(index=False))

    columnas = ", ".join(COLUMNAS_ENTRADA)
    q = text(f"""
        SELECT {columnas}
        FROM {RAW_TABLE}
        WHERE UPPER(TRIM(estu_depto_reside)) IN :deptos
          AND CAST(periodo AS CHAR) LIKE :anio
    """).bindparams(bindparam("deptos", expanding=True))

    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"deptos": [d.upper() for d in deptos],
                                          "anio": f"{anio}%"})

    if df.empty:
        raise RuntimeError(
            f"read_db no trajo filas para {deptos} y año {anio}. Revisa el filtro y la tabla {RAW_TABLE}."
        )
    log.info("read_db: %s filas de Valle %d. Periodos: %s", f"{len(df):,}", anio,
             sorted(df["periodo"].astype(str).unique().tolist()))

    # Todo como texto en el raw: los tipos los pone la limpieza compartida.
    df = df.astype("string")
    escribir_parquet(df, salida)
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    p.add_argument("--anio", type=int, default=ANIO_RAMA)
    a = p.parse_args()
    print(read_db(a.staging, a.anio))

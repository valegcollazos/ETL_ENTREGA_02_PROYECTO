from __future__ import annotations

import logging
import os
from pathlib import Path

import pandas as pd

from etl.esquema import COLUMNAS, COLUMNAS_ESQUEMA, DTYPE_PANDAS

log = logging.getLogger(__name__)


def ruta_staging(carpeta: str | os.PathLike | None = None) -> Path:

    carpeta = Path(carpeta or os.getenv("STAGING_DIR", "data/staging"))
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta


def escribir_parquet(df: pd.DataFrame, ruta: str | os.PathLike) -> Path:

    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_suffix(ruta.suffix + ".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, ruta)
    log.info("Escrito %s (%s filas, %d columnas).", ruta, f"{len(df):,}", df.shape[1])
    return ruta


def leer_parquet(ruta: str | os.PathLike) -> pd.DataFrame:

    ruta = Path(ruta)
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo de staging: {ruta}")
    df = pd.read_parquet(ruta)
    if list(df.columns) == COLUMNAS:
        df = df.astype({c: DTYPE_PANDAS[t] for c, t in COLUMNAS_ESQUEMA.items()})
    return df

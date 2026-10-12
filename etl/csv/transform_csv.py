from __future__ import annotations

import argparse
import logging

from etl.csv.read_csv import ANIO_RAMA, DEPTOS_VALLE
from etl.esquema import ARCHIVOS, COLUMNAS_ENTRADA, EsquemaError
from etl.limpieza import limpiar
from etl.staging import escribir_parquet, leer_parquet, ruta_staging

log = logging.getLogger(__name__)


def transform_csv(staging_dir: str | None = None, anio: int = ANIO_RAMA, **kwargs) -> str:
    carpeta = ruta_staging(staging_dir)
    df = leer_parquet(carpeta / ARCHIVOS["raw_csv"])
    df.columns = [str(c).strip().lower() for c in df.columns]

    faltantes = [c for c in COLUMNAS_ENTRADA if c not in df.columns]
    for col in faltantes:
        df[col] = None
    if faltantes:
        log.warning("Columnas que no existen en el CSV y se crearon en nulo: %s", faltantes)

    df = limpiar(df[COLUMNAS_ENTRADA], fuente="csv_2019")

    # Control de la rama: solo Valle y solo el año de la rama (lo demás lo valida limpiar()).
    otros_anios = sorted(set(df["anio"].dropna().tolist()) - {anio})
    otros_deptos = sorted(set(df["estu_depto_reside"].str.upper().str.strip()) - set(DEPTOS_VALLE))
    if otros_anios or otros_deptos:
        raise EsquemaError(f"[csv_2019] quedaron filas fuera del filtro: años {otros_anios}, "
                           f"departamentos {otros_deptos}. Revisar read_csv.")

    salida = escribir_parquet(df, carpeta / ARCHIVOS["staging_csv"])
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    print(transform_csv(p.parse_args().staging))

"""transform_csv — responsable: Carlos.

Entrada: raw_csv.parquet
Salida:  staging_csv.parquet
Último paso: etl.limpieza.limpiar(df, fuente='csv_2019') + etl.staging.escribir_parquet.
ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    transform_csv(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def transform_csv(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("transform_csv todavía no está implementada (Carlos).")

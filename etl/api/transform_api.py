"""transform_api — responsable: Lauren.

Entrada: raw_api.parquet
Salida:  staging_api.parquet
Último paso: etl.limpieza.limpiar(df, fuente='api_2020') + etl.staging.escribir_parquet.
ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    transform_api(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def transform_api(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("transform_api todavía no está implementada (Lauren).")

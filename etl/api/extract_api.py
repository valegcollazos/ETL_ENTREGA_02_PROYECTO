"""extract_api — responsable: Lauren.

Entrada: API de datos.gov.co (Saber 11 2020, solo Valle)
Salida:  raw_api.parquet
Variables de entorno sugeridas: SOCRATA_APP_TOKEN (opcional).
ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    extract_api(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def extract_api(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("extract_api todavía no está implementada (Lauren).")

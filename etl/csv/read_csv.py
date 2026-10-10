"""read_csv — responsable: Carlos.

Entrada: CSV 2019 (ruta en la variable de entorno CSV_2019_PATH o parámetro ruta_csv)
Salida:  raw_csv.parquet

ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    read_csv(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def read_csv(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("read_csv todavía no está implementada (Carlos).")

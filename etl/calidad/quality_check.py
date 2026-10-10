"""quality_check — responsable: Lauren.

Entrada: staging_merged.parquet
Salida:  aprueba (retorna) o lanza excepción; guarda el resultado de GX
Suite de Great Expectations + Checkpoint (guía, sección 6).
ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    quality_check(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def quality_check(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("quality_check todavía no está implementada (Lauren).")

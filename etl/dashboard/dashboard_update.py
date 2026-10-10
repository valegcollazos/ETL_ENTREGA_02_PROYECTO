"""dashboard_update — responsable: Carlos.

Entrada: Data Warehouse (DW_DB_NAME)
Salida:  tableros en dashboards/

ESQUELETO publicado por Valeria (Hito 1). Reemplazar el cuerpo; NO cambiar el
nombre ni los parámetros, porque el DAG la llama así:
    dashboard_update(staging_dir=...)  -> devuelve la ruta (str) del archivo escrito
Reglas: sobreescribir el archivo, lanzar excepción si algo falla, rutas por
parámetro o variable de entorno (nada escrito a mano), ver guía sección 8.
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def dashboard_update(staging_dir: str | None = None, **kwargs) -> str:
    raise NotImplementedError("dashboard_update todavía no está implementada (Carlos).")

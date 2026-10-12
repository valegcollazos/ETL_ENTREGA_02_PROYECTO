"""cleanup_staging (Valeria): borra los parquet temporales después de una carga exitosa.

Solo corre si load terminó bien (regla por defecto de Airflow: all_success).
Si quality_check falla, load y cleanup no corren y los archivos quedan para revisar.
Solo borra los nombres fijos del esquema: nunca borra otra cosa de la carpeta.
Excepción: staging_merged.parquet se conserva para el análisis exploratorio (notebook).
"""
from __future__ import annotations

import argparse
import logging

from etl.esquema import ARCHIVOS
from etl.staging import ruta_staging

log = logging.getLogger(__name__)


def cleanup_staging(staging_dir: str | None = None) -> list[str]:
    carpeta = ruta_staging(staging_dir)
    borrados = []
    for nombre in ARCHIVOS.values():

        if nombre == "staging_merged.parquet":
            continue
        for ruta in (carpeta / nombre, carpeta / f"{nombre}.tmp"):
            if ruta.exists():
                ruta.unlink()
                borrados.append(ruta.name)
    log.info("cleanup_staging: %d archivos borrados: %s", len(borrados), borrados)
    return borrados


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    print(cleanup_staging(p.parse_args().staging))

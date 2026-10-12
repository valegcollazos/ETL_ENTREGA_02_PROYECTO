from __future__ import annotations

import argparse
import logging
import os

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from etl.esquema import ARCHIVOS
from etl.staging import escribir_parquet, ruta_staging

log = logging.getLogger(__name__)

URL_BASE = "https://www.datos.gov.co/resource/{dataset}.json"
# 2020-1 (periodo 20201) y 2020-2 (periodo 20204)
DATASETS = ("a8xr-en99", "rnvb-vnyh")
DEPTO_VALLE = "VALLE"
TAMANO_PAGINA = 10_000
TIMEOUT_SEG = 120


def _sesion() -> requests.Session:
    sesion = requests.Session()
    # ojo: si la API se pone lenta o nos limita, reintenta sola
    reintentos = Retry(total=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504))
    sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
    token = os.getenv("SOCRATA_APP_TOKEN")
    if token:
        sesion.headers["X-App-Token"] = token
    else:
        log.info("SOCRATA_APP_TOKEN no está definido: se consulta sin token.")
    return sesion


def _descargar_dataset(sesion: requests.Session, dataset: str) -> pd.DataFrame:
    url = URL_BASE.format(dataset=dataset)
    filas: list[dict] = []
    offset = 0
    while True:
        params = {
            # ojo: el filtro de Valle se hace aquí, para no bajar todo el país
            "$where": f"upper(estu_depto_reside) = '{DEPTO_VALLE}'",
            # ojo: sin orden fijo las páginas se pueden repetir o saltar filas
            "$order": "estu_consecutivo",
            "$limit": TAMANO_PAGINA,
            "$offset": offset,
        }
        resp = sesion.get(url, params=params, timeout=TIMEOUT_SEG)
        resp.raise_for_status()
        pagina = resp.json()
        filas.extend(pagina)
        log.info("[%s] offset %d: %d filas (acumulado %d).", dataset, offset, len(pagina), len(filas))
        # ojo: una página incompleta significa que ya es la última
        if len(pagina) < TAMANO_PAGINA:
            break
        offset += TAMANO_PAGINA

    if not filas:
        raise RuntimeError(f"[{dataset}] la API no devolvió filas de {DEPTO_VALLE}.")
    # ojo: todo queda como texto, los tipos se arreglan en transform_api
    return pd.DataFrame(filas).astype("string")


def extract_api(staging_dir: str | None = None, **kwargs) -> str:
    salida = ruta_staging(staging_dir) / ARCHIVOS["raw_api"]

    with _sesion() as sesion:
        partes = [_descargar_dataset(sesion, d) for d in DATASETS]

    # ojo: las columnas que un conjunto no trae (NSE/INSE en 2020-1) quedan vacías
    df = pd.concat(partes, ignore_index=True)
    log.info("Periodos descargados: %s", df["periodo"].value_counts().to_dict())
    log.info("Valores de estu_depto_reside: %s", df["estu_depto_reside"].unique().tolist())

    escribir_parquet(df, salida)
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", default=None)
    print(extract_api(ap.parse_args().staging))

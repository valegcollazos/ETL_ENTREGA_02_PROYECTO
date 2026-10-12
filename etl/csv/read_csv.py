from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import pandas as pd

from etl.esquema import ARCHIVOS, COLUMNAS_ENTRADA
from etl.staging import escribir_parquet, ruta_staging

try:  
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger(__name__)

ANIO_RAMA = 2019
DEPTOS_VALLE = ("VALLE", "VALLE DEL CAUCA")
CHUNKSIZE = 100_000
SEPARADORES = (",", ";", "|", "\t", "¬")
ENCODINGS = ("utf-8-sig", "latin-1")
VALORES_NULOS = ["", "NA", "N/A", "null", "NULL"]  


def _rutas_csv(ruta_csv) -> list[Path]:
    if ruta_csv is None:
        valor = os.getenv("CSV_2019_PATH")
        if not valor:
            raise RuntimeError("Falta la ruta del CSV 2019: parámetro ruta_csv o variable CSV_2019_PATH en el .env.")
        ruta_csv = [r.strip() for r in valor.split(",") if r.strip()]
    if isinstance(ruta_csv, (str, os.PathLike)):
        ruta_csv = [ruta_csv]
    rutas = [Path(r) for r in ruta_csv]
    faltan = [str(r) for r in rutas if not r.is_file()]
    if not rutas or faltan:
        raise FileNotFoundError(f"No existe el CSV de origen: {faltan or 'ninguna ruta'}")
    return rutas


def _detectar_separador(ruta: Path) -> str:
    with open(ruta, "rb") as f:
        encabezado = f.readline().decode("latin-1")
    sep = max(SEPARADORES, key=encabezado.count)
    if encabezado.count(sep) == 0:
        raise ValueError(f"No se pudo detectar el separador de {ruta.name} (primera línea: {encabezado[:80]!r}).")
    return sep


def _normalizar(serie: pd.Series) -> pd.Series:
    return serie.fillna("").str.upper().str.strip().str.replace(r"\s+", " ", regex=True)


def _leer_archivo(ruta: Path, anio: int, deptos: tuple[str, ...], chunksize: int) -> tuple[pd.DataFrame, dict]:
    sep = _detectar_separador(ruta)
    necesarias = set(COLUMNAS_ENTRADA)

    for encoding in ENCODINGS:
        try:
            filas_leidas, partes = 0, []
            valores_valle, periodos = {}, set()
            lector = pd.read_csv(
                ruta, sep=sep, encoding=encoding, dtype=str, chunksize=chunksize,
                keep_default_na=True, na_values=VALORES_NULOS,
                usecols=lambda c: str(c).strip().lower() in necesarias,
            )
            for bloque in lector:
                bloque.columns = [str(c).strip().lower() for c in bloque.columns]
                if filas_leidas == 0:
                    faltan_filtro = [c for c in ("estu_consecutivo", "periodo", "estu_depto_reside")
                                     if c not in bloque.columns]
                    if faltan_filtro:
                        raise KeyError(f"{ruta.name} no tiene las columnas {faltan_filtro}. "
                                       "Revisen el encabezado del archivo.")
                filas_leidas += len(bloque)
                depto = _normalizar(bloque["estu_depto_reside"])
                periodo = bloque["periodo"].fillna("").str.strip().str.replace(r"\.0+$", "", regex=True)
                for v, n in depto[depto.str.contains("VALLE")].value_counts().items():
                    valores_valle[v] = valores_valle.get(v, 0) + int(n)
                periodos.update(p for p in periodo.unique() if p)
                mascara = depto.isin(deptos) & periodo.str.startswith(str(anio))
                if mascara.any():
                    partes.append(bloque.loc[mascara])
            break
        except UnicodeDecodeError as e:
            log.warning("%s no es %s (%s); se reintenta con otro encoding.", ruta.name, encoding, e)
    else:  # pragma: no cover - latin-1 decodifica cualquier byte
        raise ValueError(f"No se pudo decodificar {ruta}")

    df = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()
    info = {"archivo": ruta.name, "encoding": encoding, "separador": sep, "filas_leidas": filas_leidas,
            "filas_valle_anio": len(df), "valores_con_VALLE": valores_valle, "periodos_en_archivo": sorted(periodos)}
    log.info("Leído %s", info)
    return df, info


def read_csv(staging_dir: str | None = None, ruta_csv=None, anio: int = ANIO_RAMA,
             deptos=DEPTOS_VALLE, chunksize: int = CHUNKSIZE, **kwargs) -> str:
    """Lee el/los CSV, filtra Valle + anio y escribe raw_csv.parquet. Devuelve la ruta."""
    rutas = _rutas_csv(ruta_csv)
    salida = ruta_staging(staging_dir) / ARCHIVOS["raw_csv"]
    deptos = tuple(d.upper() for d in deptos)

    partes, infos = [], []
    for ruta in rutas:
        df_archivo, info = _leer_archivo(ruta, anio, deptos, chunksize)
        infos.append(info)
        if not df_archivo.empty:
            partes.append(df_archivo)

    valores_valle = sorted({v for i in infos for v in i["valores_con_VALLE"]})
    log.info("Valores de estu_depto_reside que contienen VALLE: %s", valores_valle)
    no_incluidos = [v for v in valores_valle if v not in deptos]
    if no_incluidos:
        log.warning("Valores con VALLE que NO se incluyeron: %s. Avisar al grupo.", no_incluidos)

    if not partes:
        raise RuntimeError(
            f"read_csv no trajo filas para {deptos} y año {anio}. Periodos en los archivos: "
            f"{sorted({p for i in infos for p in i['periodos_en_archivo']})}; valores con VALLE: {valores_valle}."
        )
    df = pd.concat(partes, ignore_index=True)
    df = df[[c for c in COLUMNAS_ENTRADA if c in df.columns]]

    faltantes = [c for c in COLUMNAS_ENTRADA if c not in df.columns]
    if faltantes:
        log.warning("Columnas del esquema que no existen en el CSV (transform_csv las crea en nulo): %s", faltantes)
    log.info("read_csv: %s filas leídas, %s de Valle %d. Filas por periodo: %s",
             f"{sum(i['filas_leidas'] for i in infos):,}", f"{len(df):,}", anio,
             df["periodo"].value_counts().sort_index().to_dict())

   
    df = df.astype("string")
    escribir_parquet(df, salida)
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    p.add_argument("--csv", nargs="+", default=None, help="ruta(s) del CSV; por defecto CSV_2019_PATH")
    p.add_argument("--anio", type=int, default=ANIO_RAMA)
    a = p.parse_args()
    print(read_csv(a.staging, ruta_csv=a.csv, anio=a.anio))

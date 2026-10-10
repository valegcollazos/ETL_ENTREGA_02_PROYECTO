"""load (Valeria): staging_merged.parquet -> Data Warehouse de la entrega 2.

Adaptado de transformacion.py (entrega 1): misma estrella, mismas dimensiones,
pero ahora lee del staging ya limpio y validado por GX en vez de la tabla raw.
Aquí (y solo aquí) se renombran punt_c_naturales -> punt_naturales y
cole_codigo_icfes -> cole_cod_icfes, como define el esquema.

Idempotente: INSERT IGNORE sobre llaves únicas, así que reejecutar no duplica.

Probar fuera de Airflow (con el DW creado con sql/esquema_dw.sql):
    python -m etl.carga.load --staging data/staging
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.mysql import insert as mysql_insert

from etl.config import get_engine_dw
from etl.esquema import ARCHIVOS, validar_esquema
from etl.staging import leer_parquet, ruta_staging

log = logging.getLogger(__name__)

COLS_SOCIOECO = [
    "fami_estratovivienda", "fami_educacionpadre", "fami_educacionmadre",
    "fami_tieneinternet", "fami_tienecomputador", "fami_personashogar",
    "estu_nse_individual",
]
COLS_UBICACION = ["estu_depto_reside", "estu_mcpio_reside"]
ETAPA_PANDEMIA = {2019: "Antes", 2020: "Durante", 2025: "Después"}


# --- Construcción de dimensiones (igual que en la entrega 1) -----------------
def construir_dim_colegio(df: pd.DataFrame) -> pd.DataFrame:
    dim = (
        df[["cole_codigo_icfes", "cole_nombre_establecimiento", "cole_naturaleza",
            "cole_calendario", "cole_area_ubicacion", "periodo"]]
        .sort_values("periodo", kind="stable")
        .drop_duplicates(subset=["cole_codigo_icfes"], keep="last")  # atributos más recientes
        .drop(columns="periodo")
    )
    return dim.rename(columns={"cole_codigo_icfes": "cole_cod_icfes"})


def construir_dim_ubicacion(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLS_UBICACION].drop_duplicates()


def construir_dim_socioeconomica(df: pd.DataFrame) -> pd.DataFrame:
    return df[COLS_SOCIOECO].drop_duplicates()


def construir_dim_tiempo(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["periodo", "anio", "semestre"]].drop_duplicates(subset=["periodo"]).copy()
    dim["etapa_pandemia"] = dim["anio"].map(ETAPA_PANDEMIA).fillna("Sin clasificar")
    return dim


# --- Carga idempotente --------------------------------------------------------
def insert_ignore(pd_table, conn, keys, data_iter):
    data = [dict(zip(keys, row)) for row in data_iter]
    if data:
        conn.execute(mysql_insert(pd_table.table).values(data).prefix_with("IGNORE"))


def _a_python(df: pd.DataFrame) -> pd.DataFrame:
    """Int64/string de pandas -> objetos Python con None, para que pymysql los entienda."""
    return df.astype(object).where(df.notna(), None)


def cargar_dimension(engine, df_dim: pd.DataFrame, tabla: str) -> pd.DataFrame:
    if len(df_dim):
        _a_python(df_dim).to_sql(tabla, con=engine, if_exists="append", index=False,
                                 method=insert_ignore, chunksize=5000)
    log.info("Dimensión '%s': %d combinaciones evaluadas.", tabla, len(df_dim))
    return pd.read_sql(text(f"SELECT * FROM {tabla}"), con=engine)


# --- Hechos ---------------------------------------------------------------------
def construir_fact(df, dim_colegio, dim_ubicacion, dim_socioeco, dim_tiempo) -> pd.DataFrame:
    # Las llaves leídas de MySQL llegan como object/int64: se alinean con los tipos del staging.
    colegio = dim_colegio[["cole_cod_icfes", "id_colegio"]].astype({"cole_cod_icfes": "Int64"})
    ubicacion = dim_ubicacion[COLS_UBICACION + ["id_ubicacion"]].astype({c: "string" for c in COLS_UBICACION})
    socioeco = dim_socioeco[COLS_SOCIOECO + ["id_socioeconomico"]].astype(
        {**{c: "string" for c in COLS_SOCIOECO[:-1]}, "estu_nse_individual": "Int64"})
    tiempo = dim_tiempo[["periodo", "id_tiempo"]].astype({"periodo": "string"})

    fact = (
        df.merge(colegio, left_on="cole_codigo_icfes", right_on="cole_cod_icfes", how="left")
          .merge(ubicacion, on=COLS_UBICACION, how="left")
          .merge(socioeco, on=COLS_SOCIOECO, how="left")
          .merge(tiempo, on="periodo", how="left")
          .rename(columns={"punt_c_naturales": "punt_naturales",
                           "estu_consecutivo": "bk_estu_consecutivo"})
    )
    if len(fact) != len(df):
        raise RuntimeError("El cruce con dimensiones multiplicó filas: revisar llaves únicas del DW.")

    llaves_fk = ["id_colegio", "id_socioeconomico", "id_tiempo", "id_ubicacion"]
    sin_fk = fact[llaves_fk].isna().any(axis=1)
    if sin_fk.any():
        # En la entrega 1 esto era un warning; ahora es error: no se pierden filas en silencio.
        raise RuntimeError(
            f"{int(sin_fk.sum())} filas no encontraron su llave en alguna dimensión. "
            f"Columnas sin llave: {fact.loc[sin_fk, llaves_fk].isna().sum().to_dict()}. "
            "Causa típica: texto más largo que el VARCHAR o collation que iguala tildes/mayúsculas."
        )

    columnas_fact = [
        "punt_global", "punt_matematicas", "punt_lectura_critica", "punt_naturales",
        "punt_sociales_ciudadanas", "punt_ingles", "estu_inse_individual",
        "id_colegio", "id_socioeconomico", "id_tiempo", "id_ubicacion",
        "bk_estu_consecutivo",
    ]
    return fact[columnas_fact]


def cargar_fact(engine, fact: pd.DataFrame) -> None:
    if len(fact):
        _a_python(fact).to_sql("fact_resultadossaber11", con=engine, if_exists="append",
                               index=False, method=insert_ignore, chunksize=5000)
    log.info("Tabla de hechos: %s filas procesadas.", f"{len(fact):,}")


def load(staging_dir: str | None = None, engine=None) -> int:
    engine = engine or get_engine_dw()
    df = leer_parquet(ruta_staging(staging_dir) / ARCHIVOS["staging_merged"])
    validar_esquema(df, nombre=ARCHIVOS["staging_merged"])

    dim_colegio = cargar_dimension(engine, construir_dim_colegio(df), "dim_colegio")
    dim_ubicacion = cargar_dimension(engine, construir_dim_ubicacion(df), "dim_ubicacion")
    dim_socioeco = cargar_dimension(engine, construir_dim_socioeconomica(df), "dim_socioeconomica")
    dim_tiempo = cargar_dimension(engine, construir_dim_tiempo(df), "dim_tiempo")

    fact = construir_fact(df, dim_colegio, dim_ubicacion, dim_socioeco, dim_tiempo)
    cargar_fact(engine, fact)

    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM fact_resultadossaber11")).scalar()
        por_anio = conn.execute(text("""
            SELECT t.anio, COUNT(*) FROM fact_resultadossaber11 f
            JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo GROUP BY t.anio ORDER BY t.anio
        """)).fetchall()
    log.info("Carga finalizada. Filas en fact: %s | por año: %s", f"{total:,}", dict(por_anio))
    return int(total)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--staging", default=None)
    print(load(p.parse_args().staging))

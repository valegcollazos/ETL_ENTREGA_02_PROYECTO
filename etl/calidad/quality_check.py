"""quality_check — responsable: Lauren.

Entrada: staging_merged.parquet
Salida:  resultado_gx.json; si la suite no pasa, lanza excepción y load no corre.
Suite de Great Expectations + Checkpoint 

Probar fuera de Airflow:
    python -m etl.calidad.quality_check --staging data/staging
"""
from __future__ import annotations

import argparse
import json
import logging

import great_expectations as gx
import great_expectations.expectations as gxe

from etl.esquema import ARCHIVOS, COLUMNAS, COLUMNAS_PUNTAJE
from etl.staging import leer_parquet, ruta_staging

log = logging.getLogger(__name__)

ARCHIVO_RESULTADO = "resultado_gx.json"

# ojo: los años de cada fuente y cuántas filas mínimo esperamos del staging de cada uno
   MIN_FILAS_POR_ANIO = {2019: 1_000, 2020: 40_000, 2025: 45_000}

# ojo: el periodo solo puede terminar en 1 o 2 (el 20204 de la API se corrige en transform_api)
REGEX_PERIODO = r"^(?:2019|2020|2025)[12]$"
REGEX_VALLE = r"(?i)^valle"


class CalidadError(RuntimeError):
    """La suite de Great Expectationss no pasó."""


def _crear_suite(anios: dict[int, int]) -> list:
    reglas = []

    # ojo: las 25 columnas, en orden
    reglas.append(gxe.ExpectTableColumnsToMatchOrderedList(column_list=COLUMNAS))

    # llaves: nunca nulas y sin duplicados
    for col in ("estu_consecutivo", "periodo"):
        reglas.append(gxe.ExpectColumnValuesToNotBeNull(column=col))
    reglas.append(gxe.ExpectColumnValuesToBeUnique(column="estu_consecutivo"))

    # tiempo: solo los periodos y años esperados
    reglas.append(gxe.ExpectColumnValuesToMatchRegex(column="periodo", regex=REGEX_PERIODO))
    reglas.append(gxe.ExpectColumnValuesToBeInSet(column="anio", value_set=list(anios)))
    reglas.append(gxe.ExpectColumnDistinctValuesToContainSet(column="anio", value_set=list(anios)))
    reglas.append(gxe.ExpectColumnValuesToBeInSet(column="semestre", value_set=[1, 2]))

    # ojo: solo Valle
    reglas.append(gxe.ExpectColumnValuesToMatchRegex(column="estu_depto_reside", regex=REGEX_VALLE))

    # mínimo de filas por año
    for anio, minimo in anios.items():
        reglas.append(gxe.ExpectTableRowCountToBeBetween(
            min_value=minimo, row_condition=f"anio=={anio}", condition_parser="pandas"))

    # puntajes: global 0 a 500, el resto 0 a 100 (los nulos no cuentan)
    for col in COLUMNAS_PUNTAJE:
        tope = 500 if col == "punt_global" else 100
        reglas.append(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=0, max_value=tope))

    # categorías: NSE entre -1 y 4, -1 significa sin dato
    reglas.append(gxe.ExpectColumnValuesToBeInSet(column="estu_nse_individual", value_set=[-1, 1, 2, 3, 4]))
    reglas.append(gxe.ExpectColumnValuesToBeBetween(column="cole_codigo_icfes", min_value=-1))
    for col in ("fami_tieneinternet", "fami_tienecomputador"):
        reglas.append(gxe.ExpectColumnValuesToBeInSet(column=col, value_set=["Si", "No", "No informa"]))

    return reglas


def _correr_suite(df, anios: dict[int, int]):
    # ojo: contexto en memoria, no escribe carpetas dentro del contenedor
    contexto = gx.get_context(mode="ephemeral")
    fuente = contexto.data_sources.add_pandas("staging")
    activo = fuente.add_dataframe_asset("staging_merged")
    lote = activo.add_batch_definition_whole_dataframe("lote_completo")

    suite = contexto.suites.add(gx.ExpectationSuite(name="suite_saber11_valle"))
    for regla in _crear_suite(anios):
        suite.add_expectation(regla)

    validacion = contexto.validation_definitions.add(
        gx.ValidationDefinition(name="validacion_saber11", data=lote, suite=suite))
    checkpoint = contexto.checkpoints.add(
        gx.Checkpoint(name="checkpoint_saber11", validation_definitions=[validacion]))
    return checkpoint.run(batch_parameters={"dataframe": df})


def _reglas_fallidas(resultado) -> list[str]:
    fallidas = []
    for validacion in resultado.run_results.values():
        for r in validacion.results:
            if not r.success:
                cfg = r.expectation_config
                fallidas.append(f"{cfg.type} {cfg.kwargs}")
    return fallidas


def quality_check(staging_dir: str | None = None, **kwargs) -> str:
    carpeta = ruta_staging(staging_dir)
    df = leer_parquet(carpeta / ARCHIVOS["staging_merged"])
    log.info("staging_merged: %s filas, %d columnas.", f"{len(df):,}", df.shape[1])

    # ojo: para probar con una sola fuente se puede pasar anios=[2020]
    pedidos = kwargs.get("anios")
    anios = {a: m for a, m in MIN_FILAS_POR_ANIO.items() if pedidos is None or a in pedidos}

    resultado = _correr_suite(df, anios)

    # ojo: el resultado se guarda aunque falle, para poder revisarlo
    salida = carpeta / ARCHIVO_RESULTADO
    salida.write_text(
        json.dumps(resultado.describe_dict(), indent=2, ensure_ascii=False, default=str),
        encoding="utf-8")
    log.info("Resultado de Great Expectations guardado en %s", salida)

    if not resultado.success:
        fallidas = _reglas_fallidas(resultado)
        raise CalidadError(
            f"La suite de calidad no pasó ({len(fallidas)} reglas fallidas):\n  - " + "\n  - ".join(fallidas))

    log.info("La suite de calidad pasó.")
    return str(salida)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--staging", default=None)
    ap.add_argument("--anios", type=int, nargs="*", default=None)
    args = ap.parse_args()
    print(quality_check(args.staging, anios=args.anios))

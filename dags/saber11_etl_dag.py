"""DAG de la entrega 2 — Saber 11 Valle del Cauca (2019 / 2020 / 2025).

Este archivo SOLO define tareas y dependencias. Todo el procesamiento vive en
el paquete etl/ y se importa dentro de cada tarea (así, si el módulo de otra
rama aún no está listo, el DAG igual se carga en la interfaz de Airflow).

Flujo:
  extract_api -> transform_api ┐
  read_csv    -> transform_csv ├-> merge -> quality_check -> load -> cleanup_staging -> dashboard_update
  read_db     -> transform_db  ┘
"""
from __future__ import annotations

import importlib
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

STAGING_DIR = os.getenv("STAGING_DIR", "/opt/airflow/data/staging")


def _ejecutar(funcion: str):
    """Importa etl.<modulo>.<funcion> en tiempo de ejecución y la llama.
    Solo devuelve mensajes cortos (la ruta del archivo): nada de tablas por XCom."""
    modulo, nombre = funcion.rsplit(".", 1)
    fn = getattr(importlib.import_module(modulo), nombre)
    logging.getLogger("etl").setLevel(logging.INFO)
    resultado = fn(staging_dir=STAGING_DIR)
    return str(resultado)[:500]


def tarea(task_id: str, funcion: str, **opciones) -> PythonOperator:
    return PythonOperator(
        task_id=task_id,
        python_callable=_ejecutar,
        op_kwargs={"funcion": funcion},
        **opciones,
    )


default_args = {
    "owner": "equipo_etl_g51",
    "retries": 0,  # reintento global: 0 (cada tarea define los suyos)
}

with DAG(
    dag_id="saber11_valle_etl",
    description="Saber 11 Valle del Cauca: antes, durante y después de la pandemia",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",   # ejecución diaria técnica: los datos son históricos (ver README)
    catchup=False,       # sin ejecución retroactiva
    max_active_runs=1,   # una sola ejecución activa a la vez
    default_args=default_args,
    tags=["saber11", "etl", "entrega2"],
) as dag:

    # Rama API 2020 (Lauren)
    extract_api = tarea("extract_api", "etl.api.extract_api.extract_api",
                        retries=3, retry_delay=timedelta(minutes=5),
                        retry_exponential_backoff=True,
                        max_retry_delay=timedelta(minutes=30),
                        execution_timeout=timedelta(minutes=30))
    transform_api = tarea("transform_api", "etl.api.transform_api.transform_api")

    # Rama CSV 2019 (Carlos)
    read_csv = tarea("read_csv", "etl.csv.read_csv.read_csv", retries=1,
                     retry_delay=timedelta(minutes=2))
    transform_csv = tarea("transform_csv", "etl.csv.transform_csv.transform_csv")

    # Rama DB 2025 (Valeria)
    read_db = tarea("read_db", "etl.db.read_db.read_db", retries=2,
                    retry_delay=timedelta(seconds=30))
    transform_db = tarea("transform_db", "etl.db.transform_db.transform_db")

    # Unión, calidad y carga
    merge = tarea("merge", "etl.merge.merge")
    quality_check = tarea("quality_check", "etl.calidad.quality_check.quality_check")
    load = tarea("load", "etl.carga.load.load")
    cleanup_staging = tarea("cleanup_staging", "etl.cleanup.cleanup_staging")
    dashboard_update = tarea("dashboard_update", "etl.dashboard.dashboard_update.dashboard_update")

    extract_api >> transform_api
    read_csv >> transform_csv
    read_db >> transform_db
    [transform_api, transform_csv, transform_db] >> merge
    merge >> quality_check >> load >> cleanup_staging >> dashboard_update

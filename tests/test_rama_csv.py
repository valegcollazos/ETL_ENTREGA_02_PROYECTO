"""Pruebas de la rama CSV (read_csv + transform_csv). Correr con:  pytest -q"""
import pandas as pd
import pytest

from etl.csv.read_csv import read_csv
from etl.csv.transform_csv import transform_csv
from etl.esquema import COLUMNAS, COLUMNAS_ENTRADA, validar_esquema
from etl.staging import leer_parquet


def fila(consecutivo, periodo="20191", depto="VALLE", **cambios):
    base = {c: "" for c in COLUMNAS_ENTRADA}
    base.update({
        "estu_consecutivo": consecutivo, "periodo": periodo,
        "estu_depto_reside": depto, "estu_mcpio_reside": "CALI",
        "cole_codigo_icfes": "101048", "cole_nombre_establecimiento": "COLEGIO A ",
        "cole_naturaleza": "NO OFICIAL", "cole_calendario": "B", "cole_area_ubicacion": "URBANO",
        "fami_estratovivienda": "Estrato 3", "fami_educacionpadre": "Postgrado",
        "fami_educacionmadre": "Postgrado", "fami_tieneinternet": "Si", "fami_tienecomputador": "Si",
        "fami_personashogar": "3 a 4", "estu_nse_individual": "3", "estu_inse_individual": "61.5",
        "punt_global": "300", "punt_matematicas": "60", "punt_lectura_critica": "60",
        "punt_c_naturales": "60", "punt_sociales_ciudadanas": "60", "punt_ingles": "60",
        "estu_genero": "F",  # columna extra del CSV real: read_csv no la trae
    })
    base.update(cambios)
    return base


@pytest.fixture
def csv_2019(tmp_path):
    filas = [
        fila("SB11201910000001"),
        fila("SB11201910000002", depto=" valle "),                 # espacios/minúsculas: se incluye
        fila("SB11201910000003", cole_codigo_icfes="", cole_nombre_establecimiento="",
             cole_naturaleza="", cole_calendario="", cole_area_ubicacion=""),  # inscripción individual
        fila("SB11201910000004", depto="CAUCA"),                   # otro departamento: fuera
        fila("SB11202510000005", periodo="20251"),                 # otro año: fuera
    ]
    ruta = tmp_path / "saber11_20191.csv"
    pd.DataFrame(filas).to_csv(ruta, index=False)
    return ruta


def test_read_csv_filtra_valle_y_anio(csv_2019, tmp_path):
    salida = read_csv(staging_dir=tmp_path / "staging", ruta_csv=csv_2019)
    raw = leer_parquet(salida)
    assert salida.endswith("raw_csv.parquet")
    assert sorted(raw["estu_consecutivo"]) == ["SB11201910000001", "SB11201910000002", "SB11201910000003"]
    assert list(raw.columns) == COLUMNAS_ENTRADA  # solo las 23 columnas, sin estu_genero


def test_read_csv_varios_archivos_y_variable_de_entorno(csv_2019, tmp_path, monkeypatch):
    segundo = tmp_path / "saber11_20192.csv"
    pd.DataFrame([fila("SB11201920000006", periodo="20192")]).to_csv(segundo, index=False)
    monkeypatch.setenv("CSV_2019_PATH", f"{csv_2019}, {segundo}")
    raw = leer_parquet(read_csv(staging_dir=tmp_path / "staging"))
    assert sorted(raw["periodo"].unique()) == ["20191", "20192"]
    assert len(raw) == 4


def test_read_csv_errores(csv_2019, tmp_path):
    with pytest.raises(FileNotFoundError):
        read_csv(staging_dir=tmp_path, ruta_csv=tmp_path / "no_existe.csv")
    with pytest.raises(RuntimeError, match="no trajo filas"):
        read_csv(staging_dir=tmp_path, ruta_csv=csv_2019, anio=2020)


def test_transform_csv_cumple_esquema_y_es_repetible(csv_2019, tmp_path):
    staging = tmp_path / "staging"
    read_csv(staging_dir=staging, ruta_csv=csv_2019)
    primera = leer_parquet(transform_csv(staging_dir=staging))
    segunda = leer_parquet(transform_csv(staging_dir=staging))

    validar_esquema(primera)
    assert list(primera.columns) == COLUMNAS
    assert primera.equals(segunda)
    assert set(primera["anio"]) == {2019}
    individual = primera[primera["estu_consecutivo"] == "SB11201910000003"].iloc[0]
    assert individual["cole_codigo_icfes"] == -1
    assert individual["cole_naturaleza"] == "No aplica"


def test_transform_csv_crea_en_nulo_columnas_faltantes(tmp_path):
    ruta = tmp_path / "sin_inse.csv"
    pd.DataFrame([fila("SB11201910000001")]).drop(columns="estu_inse_individual").to_csv(ruta, index=False)
    staging = tmp_path / "staging"
    read_csv(staging_dir=staging, ruta_csv=ruta)
    out = leer_parquet(transform_csv(staging_dir=staging))
    validar_esquema(out)
    assert out["estu_inse_individual"].isna().all()

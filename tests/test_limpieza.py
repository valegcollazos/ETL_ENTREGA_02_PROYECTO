"""Pruebas del módulo de limpieza compartido. Correr con:  pytest -q"""
import pandas as pd
import pytest

from etl.esquema import COLUMNAS, COLUMNAS_ENTRADA, EsquemaError, validar_esquema
from etl.limpieza import limpiar


def fila(**cambios):
    base = {c: None for c in COLUMNAS_ENTRADA}
    base.update({
        "estu_consecutivo": "SB11202510000001", "periodo": "20252",
        "estu_depto_reside": "VALLE", "estu_mcpio_reside": "CALI",
        "cole_codigo_icfes": "123456", "cole_nombre_establecimiento": "COLEGIO A",
        "cole_naturaleza": "OFICIAL", "cole_calendario": "A", "cole_area_ubicacion": "URBANO",
        "fami_estratovivienda": "Estrato 2", "fami_educacionpadre": "Secundaria (Bachillerato) completa",
        "fami_educacionmadre": "TÃ©cnica o tecnolÃ³gica completa",  # mojibake a propósito
        "fami_tieneinternet": "Si", "fami_tienecomputador": "No", "fami_personashogar": "3 a 4",
        "estu_nse_individual": "2", "estu_inse_individual": "48.7",
        "punt_global": "250", "punt_matematicas": "50", "punt_lectura_critica": "55",
        "punt_c_naturales": "48", "punt_sociales_ciudadanas": "47", "punt_ingles": "52",
    })
    base.update(cambios)
    return base


def test_salida_cumple_esquema():
    df = pd.DataFrame([fila(), fila(estu_consecutivo="SB11202510000002", fami_tieneinternet="  ")])
    out = limpiar(df, "prueba")
    validar_esquema(out)
    assert list(out.columns) == COLUMNAS
    assert out.loc[0, "anio"] == 2025 and out.loc[0, "semestre"] == 2
    assert out.loc[0, "fami_educacionmadre"] == "Técnica o tecnológica completa"
    assert out.loc[1, "fami_tieneinternet"] == "No informa"


def test_sin_colegio_y_nse_faltante():
    df = pd.DataFrame([fila(cole_codigo_icfes=None, estu_nse_individual=None, estu_inse_individual="nan")])
    out = limpiar(df, "prueba")
    assert out.loc[0, "cole_codigo_icfes"] == -1
    assert out.loc[0, "cole_nombre_establecimiento"] == "Sin colegio (inscripción individual)"
    assert out.loc[0, "cole_naturaleza"] == "No aplica"
    assert out.loc[0, "estu_nse_individual"] == -1
    assert pd.isna(out.loc[0, "estu_inse_individual"])


def test_doble_inscripcion_prioriza_colegio():
    df = pd.DataFrame([fila(cole_codigo_icfes=None, punt_global="200"), fila()])
    out = limpiar(df, "prueba")
    assert len(out) == 1 and out.loc[0, "cole_codigo_icfes"] == 123456


def test_dos_colegios_distintos_falla():
    df = pd.DataFrame([fila(), fila(cole_codigo_icfes="999")])
    with pytest.raises(EsquemaError):
        limpiar(df, "prueba")


def test_llaves_nulas_se_descartan():
    df = pd.DataFrame([fila(), fila(estu_consecutivo="  ", periodo="20252")])
    assert len(limpiar(df, "prueba")) == 1


def test_columna_faltante_falla():
    df = pd.DataFrame([fila()]).drop(columns="punt_ingles")
    with pytest.raises(EsquemaError, match="punt_ingles"):
        limpiar(df, "prueba")


def test_periodo_raro_falla():
    with pytest.raises(EsquemaError, match="AAAAS"):
        limpiar(pd.DataFrame([fila(periodo="2025-2")]), "prueba")


def test_periodo_numerico_float():
    out = limpiar(pd.DataFrame([fila(periodo=20191.0)]), "prueba")
    assert out.loc[0, "periodo"] == "20191" and out.loc[0, "semestre"] == 1


def test_puntaje_decimal_falla():
    with pytest.raises(EsquemaError, match="punt_global"):
        limpiar(pd.DataFrame([fila(punt_global="250.5")]), "prueba")

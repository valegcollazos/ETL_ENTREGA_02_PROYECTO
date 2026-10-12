from __future__ import annotations

import logging

import ftfy
import pandas as pd

from etl.esquema import (
    COD_ICFES_SIN_COLEGIO,
    COLUMNAS,
    COLUMNAS_ESQUEMA,
    COLUMNAS_ENTRADA,
    COLUMNAS_PUNTAJE,
    COLUMNAS_TEXTO_DESCRIPTIVO,
    DTYPE_PANDAS,
    NOMBRE_SIN_COLEGIO,
    NSE_SIN_DATO,
    TEXTO_NO_APLICA,
    TEXTO_NO_INFORMA,
    EsquemaError,
    validar_esquema,
)

log = logging.getLogger(__name__)

# Textos que en la práctica significan "vacío" (p. ej. un astype(str) sobre NaN)
_TEXTOS_NULOS = {"", "nan", "NaN", "NAN", "None", "none", "NULL", "null", "<NA>", "NA", "N/A"}
_PATRON_PERIODO = r"^\d{5}$"


# ----------------------------------------------------------------------------
# Utilidades de tipado
# ----------------------------------------------------------------------------
def _como_texto(serie: pd.Series) -> pd.Series:
    """Pasa a string de pandas, quita espacios y convierte los 'nulos escritos' en NA."""
    s = serie.astype("string").str.strip()
    return s.mask(s.isin(_TEXTOS_NULOS), pd.NA)


def a_numero(serie: pd.Series, tipo: str, columna: str) -> pd.Series:
    """Convierte a número. Los valores que no son número quedan nulos (y se reporta
    cuántos). Para tipo='int' falla si hay decimales reales (p. ej. 45.5)."""
    texto = _como_texto(serie)
    numerico = pd.to_numeric(texto, errors="coerce")
    no_convertibles = int((texto.notna() & numerico.isna()).sum())
    if no_convertibles:
        ejemplos = texto[texto.notna() & numerico.isna()].unique()[:5].tolist()
        log.warning("'%s': %d valores no numéricos quedaron nulos. Ejemplos: %s",
                    columna, no_convertibles, ejemplos)
    if tipo == "int":
        con_decimales = numerico.notna() & (numerico % 1 != 0)
        if con_decimales.any():
            ejemplos = numerico[con_decimales].unique()[:5].tolist()
            raise EsquemaError(f"'{columna}' debe ser entero y trae decimales: {ejemplos}")
        return numerico.astype("Int64")
    return numerico.astype("float64")


def _reparar_texto(serie: pd.Series) -> pd.Series:
    """ftfy sobre los valores únicos (mucho más rápido que fila por fila)."""
    s = _como_texto(serie)
    unicos = s.dropna().unique()
    mapa = {v: ftfy.fix_text(v).strip() for v in unicos}
    reparados = sum(1 for v, r in mapa.items() if v != r)
    if reparados:
        log.info("'%s': %d valores distintos reparados con ftfy.", serie.name, reparados)
    return s.map(mapa).astype("string")


def _normalizar_periodo(serie: pd.Series) -> pd.Series:
    """'20191', 20191 o '20191.0' -> '20191'. No cambia códigos raros: falla para que se discutan."""
    s = _como_texto(serie).str.replace(r"\.0+$", "", regex=True)
    malos = s.notna() & ~s.str.match(_PATRON_PERIODO).fillna(False)
    if malos.any():
        raise EsquemaError(
            f"periodo con formato distinto de AAAAS: {s[malos].unique()[:10].tolist()}. "
            "Avisar al grupo antes de seguir (guía, sección 4.6)."
        )
    return s


# ----------------------------------------------------------------------------
# Función pública
# ----------------------------------------------------------------------------
def limpiar(df: pd.DataFrame, fuente: str = "sin_nombre") -> pd.DataFrame:
    """Aplica la limpieza común y devuelve el DataFrame con las 25 columnas del esquema.

    Pasos (los mismos de la entrega 1):
      1. Revisa que lleguen exactamente las 23 columnas de entrada.
      2. Normaliza llaves (estu_consecutivo, periodo) y descarta filas con llaves nulas.
      3. Estudiantes sin colegio: código -1, nombre fijo y "No aplica".
      4. Texto: repara mojibake con ftfy, quita espacios, faltantes -> "No informa".
      5. Puntajes a entero, INSE a decimal, NSE a entero con -1 si falta.
      6. Calcula anio y semestre desde periodo.
      7. Deduplica por estu_consecutivo priorizando el registro con colegio.
      8. Ordena columnas, fija tipos y valida el esquema.
    """
    log.info("[%s] Limpieza compartida: %s filas de entrada.", fuente, f"{len(df):,}")

    # 1. Columnas de entrada -------------------------------------------------
    faltantes = [c for c in COLUMNAS_ENTRADA if c not in df.columns]
    extras = [c for c in df.columns if c not in COLUMNAS_ENTRADA]
    if faltantes or extras:
        raise EsquemaError(
            f"[{fuente}] la entrada a limpiar debe tener exactamente las 23 columnas "
            f"del esquema. Faltan: {faltantes}. Sobran: {extras}. "
            "Si tu fuente no trae una columna, créala en nulo antes de llamar a la función."
        )
    df = df[COLUMNAS_ENTRADA].copy().reset_index(drop=True)

    # 2. Llaves -------------------------------------------------------------
    df["estu_consecutivo"] = _como_texto(df["estu_consecutivo"])
    df["periodo"] = _normalizar_periodo(df["periodo"])
    filas_antes = len(df)
    df = df.dropna(subset=["estu_consecutivo", "periodo"]).reset_index(drop=True)
    log.info("[%s] Filas descartadas por llaves nulas (estudiante/periodo): %d",
             fuente, filas_antes - len(df))

    # 3. Estudiantes sin colegio -------------------------------------------
    df["cole_codigo_icfes"] = a_numero(df["cole_codigo_icfes"], "int", "cole_codigo_icfes")
    mask_individual = df["cole_codigo_icfes"].isna()
    log.info("[%s] Estudiantes con inscripción individual (sin colegio): %d",
             fuente, int(mask_individual.sum()))
    for col in COLUMNAS_TEXTO_DESCRIPTIVO:  # todo el texto a string antes de asignar
        df[col] = df[col].astype("string")
    df.loc[mask_individual, "cole_nombre_establecimiento"] = NOMBRE_SIN_COLEGIO
    df.loc[mask_individual, ["cole_naturaleza", "cole_calendario", "cole_area_ubicacion"]] = TEXTO_NO_APLICA
    df["cole_codigo_icfes"] = df["cole_codigo_icfes"].fillna(COD_ICFES_SIN_COLEGIO).astype("Int64")

    # 4. Texto ---------------------------------------------------------------
    for col in COLUMNAS_TEXTO_DESCRIPTIVO:
        df[col] = _reparar_texto(df[col]).fillna(TEXTO_NO_INFORMA)

    # 5. Numéricos -----------------------------------------------------------
    for col in COLUMNAS_PUNTAJE:
        df[col] = a_numero(df[col], "int", col)
    df["estu_inse_individual"] = a_numero(df["estu_inse_individual"], "float", "estu_inse_individual")
    df["estu_nse_individual"] = (
        a_numero(df["estu_nse_individual"], "int", "estu_nse_individual")
        .fillna(NSE_SIN_DATO).astype("Int64")
    )

    # 6. Tiempo -------------------------------------------------------------
    df["anio"] = pd.to_numeric(df["periodo"].str[:4]).astype("Int64")
    df["semestre"] = pd.to_numeric(df["periodo"].str[-1]).astype("Int64")
    log.info("[%s] Periodos encontrados: %s", fuente, sorted(df["periodo"].unique().tolist()))
    semestres_raros = sorted(set(df["semestre"].dropna().tolist()) - {1, 2})
    if semestres_raros:
        log.warning("[%s] Hay periodos que no terminan en 1 o 2 (semestre=%s). "
                    "Avisar al grupo (guía, 4.6).", fuente, semestres_raros)

    # 7. Duplicados -----------------------------------------------------------
    filas_antes = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log.info("[%s] Filas idénticas repetidas descartadas: %d", fuente, filas_antes - len(df))

    es_individual = df["cole_codigo_icfes"] == COD_ICFES_SIN_COLEGIO
    df = (df.assign(_es_individual=es_individual)
            .sort_values(["_es_individual", "estu_consecutivo"], kind="stable"))
    dup = df["estu_consecutivo"].duplicated(keep=False)
    # Duplicados "explicados": el mismo estudiante con 1 registro de colegio y el resto individuales
    grupos = df[dup].groupby("estu_consecutivo")["_es_individual"].agg(["sum", "count"])
    sin_explicar = grupos[(grupos["count"] - grupos["sum"]) > 1]
    if len(sin_explicar):
        raise EsquemaError(
            f"[{fuente}] {len(sin_explicar)} estu_consecutivo aparecen en más de un colegio con "
            f"datos distintos (ej.: {sin_explicar.index[:5].tolist()}). No se borran por criterio "
            "propio: revisar y discutir con el grupo."
        )
    filas_antes = len(df)
    df = (df.drop_duplicates(subset=["estu_consecutivo"], keep="first")
            .drop(columns="_es_individual").reset_index(drop=True))
    log.info("[%s] Filas descartadas por doble inscripción (individual + institución): %d",
             fuente, filas_antes - len(df))

    # Diagnóstico (no cambia datos)
    reales = df[df["cole_codigo_icfes"] != COD_ICFES_SIN_COLEGIO]
    if len(reales):
        cambios = reales.groupby("cole_codigo_icfes")[
            ["cole_naturaleza", "cole_calendario", "cole_area_ubicacion"]].nunique()
        log.info("[%s] Colegios con atributos distintos entre registros: %d",
                 fuente, int((cambios > 1).any(axis=1).sum()))

    # 8. Orden, tipos y validación -----------------------------------------------
    df = df[COLUMNAS]
    df = df.astype({c: DTYPE_PANDAS[t] for c, t in COLUMNAS_ESQUEMA.items()})
    validar_esquema(df, nombre=fuente)
    log.info("[%s] Limpieza OK: %s filas, %d columnas.", fuente, f"{len(df):,}", df.shape[1])
    return df

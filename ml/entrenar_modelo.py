"""Problema: clasificacion binaria. riesgo = 1 si el punt_global del estudiante
queda en el 25% mas bajo DE SU SEMESTRE. Solo usa el perfil del hogar y del colegio."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (average_precision_score, confusion_matrix,
                             f1_score, precision_recall_curve,
                             precision_score, recall_score, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

SEMILLA = 42
RAIZ = Path(__file__).resolve().parents[1]
CARPETA_MODELOS = RAIZ / "models"

# --- Contrato con el producer y el consumer
COLUMNAS_ENTRADA = [
    "semestre",
    "fami_estratovivienda",
    "fami_educacionpadre",
    "fami_educacionmadre",
    "fami_tieneinternet",
    "fami_tienecomputador",
    "fami_personashogar",
    "cole_naturaleza",
    "cole_calendario",
    "cole_area_ubicacion",
    "estu_mcpio_reside",
]
# semestre es numerico (1 o 2) pero se trata como categoria
COLUMNAS_CATEGORICAS = COLUMNAS_ENTRADA

SQL_DATASET = """
SELECT f.id_resultado,
       f.bk_estu_consecutivo,
       f.punt_global,
       t.periodo, t.anio, t.semestre,
       s.fami_estratovivienda, s.fami_educacionpadre, s.fami_educacionmadre,
       s.fami_tieneinternet, s.fami_tienecomputador, s.fami_personashogar,
       c.cole_cod_icfes, c.cole_nombre_establecimiento,
       c.cole_naturaleza, c.cole_calendario, c.cole_area_ubicacion,
       u.estu_mcpio_reside
FROM fact_resultadossaber11 f
JOIN dim_tiempo         t ON t.id_tiempo        = f.id_tiempo
JOIN dim_socioeconomica s ON s.id_socioeconomico = f.id_socioeconomico
JOIN dim_colegio        c ON c.id_colegio       = f.id_colegio
JOIN dim_ubicacion      u ON u.id_ubicacion     = f.id_ubicacion
WHERE UPPER(TRIM(u.estu_depto_reside)) LIKE 'VALLE%'
  AND f.punt_global IS NOT NULL
"""


def cargar_desde_dw() -> pd.DataFrame:
    from sqlalchemy import create_engine, text

    usuario = os.environ["MYSQL_USER"]
    clave = os.environ["MYSQL_PASSWORD"]
    host = os.getenv("MYSQL_HOST", "localhost")
    puerto = os.getenv("MYSQL_PORT", "3307")
    base = os.getenv("DW_DB_NAME", "saber11_dw_e2")
    url = f"mysql+pymysql://{usuario}:{clave}@{host}:{puerto}/{base}?charset=utf8mb4"
    engine = create_engine(url)
    with engine.connect() as con:
        return pd.read_sql(text(SQL_DATASET), con)


def cargar_env(ruta: Path) -> None:
    """Lee el .env sin depender de python-dotenv."""
    if not ruta.exists():
        return
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def calcular_umbrales(train: pd.DataFrame) -> dict[int, float]:
    """Percentil 25 de punt_global por semestre, SOLO con el train."""
    return {int(s): float(g["punt_global"].quantile(0.25))
            for s, g in train.groupby("semestre")}


def crear_etiqueta(df: pd.DataFrame, umbrales: dict[int, float]) -> pd.Series:
    corte = df["semestre"].astype(int).map(umbrales)
    return (df["punt_global"] <= corte).astype(int)


def construir_pipeline(modelo) -> Pipeline:
    pre = ColumnTransformer(
        [("cat",
          OneHotEncoder(handle_unknown="infrequent_if_exist",
                        min_frequency=50,        # municipios/categorias raras "otros"
                        sparse_output=False),
          COLUMNAS_CATEGORICAS)],
        remainder="drop",
    )
    return Pipeline([("pre", pre), ("modelo", modelo)])


def metricas(y, prob, umbral) -> dict:
    pred = (prob >= umbral).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, pred).ravel()
    return {
        "pr_auc": float(average_precision_score(y, prob)),
        "roc_auc": float(roc_auc_score(y, prob)),
        "recall": float(recall_score(y, pred)),
        "precision": float(precision_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred)),
        "umbral_decision": float(umbral),
        "matriz_confusion": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }


def mejor_umbral_f1(y, prob) -> float:
    p, r, u = precision_recall_curve(y, prob)
    f1 = 2 * p[:-1] * r[:-1] / np.clip(p[:-1] + r[:-1], 1e-9, None)
    return float(u[int(np.argmax(f1))])


def main(csv: str | None) -> int:
    cargar_env(RAIZ / ".env")
    df = pd.read_csv(csv) if csv else cargar_desde_dw()
    print(f"Filas con punt_global: {len(df):,}")

    df["semestre"] = df["semestre"].astype(int)
    for c in COLUMNAS_CATEGORICAS:
        if c != "semestre":          # semestre se queda como entero (1 o 2)
            df[c] = df[c].astype(str).str.strip()
    df = df.dropna(subset=["punt_global"]).reset_index(drop=True)

    # 1) Dividir PRIMERO: 60% train, 20% validation, 20% test 
    train, temp = train_test_split(df, test_size=0.40, random_state=SEMILLA,
                                   stratify=df["semestre"])
    val, test = train_test_split(temp, test_size=0.50, random_state=SEMILLA,
                                 stratify=temp["semestre"])
    train, val, test = train.copy(), val.copy(), test.copy()

    # 2) Umbral del 25% por semestre, calculado solo con el train
    umbrales = calcular_umbrales(train)
    for d in (train, val, test):
        d["riesgo"] = crear_etiqueta(d, umbrales)
    print("Umbrales p25 por semestre (train):", umbrales)
    print("% en riesgo  train={:.3f}  val={:.3f}  test={:.3f}".format(
        train["riesgo"].mean(), val["riesgo"].mean(), test["riesgo"].mean()))

    X_tr, y_tr = train[COLUMNAS_ENTRADA], train["riesgo"]
    X_va, y_va = val[COLUMNAS_ENTRADA], val["riesgo"]
    X_te, y_te = test[COLUMNAS_ENTRADA], test["riesgo"]

    # 3) Candidatos: se comparan en VALIDATION el test no se toca
    candidatos = {
        "regresion_logistica": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
        "random_forest": lambda: RandomForestClassifier(
            n_estimators=150, max_depth=12, min_samples_leaf=50,
            class_weight="balanced", random_state=SEMILLA, n_jobs=-1),
    }
    comparacion = {}
    for nombre, fabrica in candidatos.items():
        p = construir_pipeline(fabrica()).fit(X_tr, y_tr).predict_proba(X_va)[:, 1]
        comparacion[nombre] = {"validation_pr_auc": float(average_precision_score(y_va, p)),
                               "validation_roc_auc": float(roc_auc_score(y_va, p))}
        print(f"  {nombre}: {comparacion[nombre]}")
    elegido = max(comparacion, key=lambda k: comparacion[k]["validation_pr_auc"])
    print("Modelo elegido (mayor PR-AUC en validation):", elegido)

    # 4) Umbral de decision (max F1) con las probabilidades de VALIDATION
    p_va = construir_pipeline(candidatos[elegido]()).fit(X_tr, y_tr).predict_proba(X_va)[:, 1]
    umbral = mejor_umbral_f1(y_va, p_va)

    # 5) Modelo final: train + validation; evaluacion UNA sola vez en test
    X_dev = pd.concat([X_tr, X_va]); y_dev = pd.concat([y_tr, y_va])
    pipeline = construir_pipeline(candidatos[elegido]()).fit(X_dev, y_dev)
    prob_te = pipeline.predict_proba(X_te)[:, 1]
    res_modelo = metricas(y_te, prob_te, umbral)
    res_base = {"siempre_no": {"recall": 0.0, "precision": 0.0},
                "prevalencia_test_(PR-AUC_del_azar)": float(y_te.mean())}
    print("Metricas test:", json.dumps(res_modelo, indent=2))

    # 6) Importancia por permutacion
    from sklearn.inspection import permutation_importance
    muestra = X_te.sample(min(5000, len(X_te)), random_state=SEMILLA)
    imp = permutation_importance(pipeline, muestra, y_te.loc[muestra.index],
                                 scoring="average_precision", n_repeats=3,
                                 random_state=SEMILLA)
    importancias = dict(sorted(
        {c: float(v) for c, v in zip(COLUMNAS_ENTRADA, imp.importances_mean)}.items(),
        key=lambda kv: -kv[1]))

    # 7) Artefactos
    CARPETA_MODELOS.mkdir(exist_ok=True)
    metadata = {
        "model_name": "saber11-estudiante-en-riesgo",
        "model_version": "1.0.0",
        "model_type": elegido,
        "features": COLUMNAS_ENTRADA,
        "target": "riesgo",
        "positive_class": 1,
        "decision_threshold": umbral,
        "p25_por_semestre": {str(k): v for k, v in umbrales.items()},
        "selection_metric": "validation_pr_auc",
        "final_test_metrics": res_modelo,
        "scikit_learn_version": sklearn.__version__,
    }
    joblib.dump({"model": pipeline, "metadata": metadata}, CARPETA_MODELOS / "modelo_riesgo.joblib")
    test[["id_resultado"]].to_csv(CARPETA_MODELOS / "ids_test.csv", index=False)

    categorias = {c: sorted(df[c].unique().tolist()) for c in COLUMNAS_CATEGORICAS
                  if c != "estu_mcpio_reside"}
    meta = {
        "creado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "problema": "riesgo=1 si punt_global esta en el 25% mas bajo de su semestre (umbral calculado solo con train)",
        "columnas_entrada": COLUMNAS_ENTRADA,
        "columnas_metadato_no_entrada": ["event_id", "event_ts", "periodo", "cole_cod_icfes",
                                         "cole_nombre_establecimiento"],
        "columnas_prohibidas": ["punt_global", "punt_matematicas", "punt_lectura_critica",
                                "punt_naturales", "punt_sociales_ciudadanas", "punt_ingles", "riesgo"],
        "umbrales_p25_por_semestre": metadata["p25_por_semestre"],
        "umbral_decision": umbral,
        "modelo": elegido,
        "comparacion_validation": comparacion,
        "metricas_test": res_modelo,
        "linea_base": res_base,
        "importancia_permutacion_pr_auc": importancias,
        "n_train": int(len(train)), "n_validation": int(len(val)), "n_test": int(len(test)),
        "categorias_vistas": categorias,
        "versiones": {"python": sys.version.split()[0], "scikit_learn": sklearn.__version__,
                      "joblib": joblib.__version__, "pandas": pd.__version__, "numpy": np.__version__},
    }
    (CARPETA_MODELOS / "metricas.json").write_text(
        json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")

    # 8) Ejemplo de evento y mensaje de prediccion
    fila = X_te.iloc[[0]]
    p = float(pipeline.predict_proba(fila)[0, 1])
    pred = int(p >= umbral)
    ejemplo = {
        "evento_entrada": {"event_id": int(test.iloc[0]["id_resultado"]),
                           **{c: (int(fila.iloc[0][c]) if c == "semestre" else fila.iloc[0][c])
                              for c in COLUMNAS_ENTRADA}},
        "mensaje_prediccion_esperado": {
            "event_id": int(test.iloc[0]["id_resultado"]),
            "model_id": metadata["model_name"], "model_version": metadata["model_version"],
            "prediction": pred, "prediction_label": "en_riesgo" if pred else "sin_riesgo",
            "probability_riesgo": round(p, 4)}}
    (CARPETA_MODELOS / "ejemplo_mensaje.json").write_text(
        json.dumps(ejemplo, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Artefactos guardados en", CARPETA_MODELOS)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", help="CSV con el dataset (para probar sin base de datos)")
    sys.exit(main(ap.parse_args().csv))
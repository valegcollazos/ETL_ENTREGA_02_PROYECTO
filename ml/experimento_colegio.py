"""Experimento: Que pasa si el modelo sabe de que colegio viene el estudiante?

Compara el modelo actual (11 variables) contra el mismo modelo + UNA variable nueva:
`colegio_riesgo_hist` = proporcion historica de estudiantes en riesgo de ese colegio
(suavizada hacia el promedio global; colegios nuevos reciben el promedio global).

Dos pruebas:
  A) Misma particion 60/20/20 del entrenamiento (la historia se calcula SOLO con el train y,
     para las filas del train, "fuera de pliegue" para no ver la propia respuesta).
  B) Temporal: entrena con 2019 y 2020, prueba en 2025. La historia del colegio sale solo
     de 2019-2020. Es la prueba mas realista: en la vida real el historial viene de cohortes
     anteriores, no del mismo examen.
Ademas mide el resultado POR COLEGIO y POR MUNICIPIO, que es la decision del dashboard.

Uso:  python ml/experimento_colegio.py            (lee el DW, como entrenar_modelo.py)
      python ml/experimento_colegio.py --csv x.csv
No modifica models/modelo_riesgo.joblib. Escribe models/experimento_colegio.json.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import KFold, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

sys.path.insert(0, str(Path(__file__).resolve().parent))
import entrenar_modelo as base  # noqa: E402  (reutiliza la misma logica del modelo actual)

COL_HIST = "colegio_riesgo_hist"
SUAVIZADO = 20          # "estudiantes virtuales" con el promedio global
K_PLIEGUES = 5


# ---------- historial del colegio ----------
def estadisticas(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby("cole_cod_icfes")["riesgo"].agg(suma="sum", n="count")


def codificar(df: pd.DataFrame, est: pd.DataFrame, media_global: float) -> pd.Series:
    j = df[["cole_cod_icfes"]].join(est, on="cole_cod_icfes")
    suma = j["suma"].fillna(0.0)
    n = j["n"].fillna(0.0)
    return (suma + SUAVIZADO * media_global) / (n + SUAVIZADO)


def codificar_oof(train: pd.DataFrame) -> pd.Series:
    """Para filas del train: la historia sale de los OTROS pliegues (no ve su propia etiqueta)."""
    out = pd.Series(index=train.index, dtype=float)
    media = float(train["riesgo"].mean())
    for idx_fit, idx_apl in KFold(K_PLIEGUES, shuffle=True, random_state=base.SEMILLA).split(train):
        est = estadisticas(train.iloc[idx_fit])
        out.iloc[idx_apl] = codificar(train.iloc[idx_apl], est, media).values
    return out


# ---------- pipelines ----------
def pipeline_base() -> Pipeline:
    return base.construir_pipeline(LogisticRegression(max_iter=1000, class_weight="balanced"), False)


def pipeline_con_historial() -> Pipeline:
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=50,
                              sparse_output=False), base.COLUMNAS_CATEGORICAS),
        ("num", StandardScaler(), [COL_HIST]),
    ], remainder="drop")
    return Pipeline([("pre", pre), ("modelo",
                     LogisticRegression(max_iter=1000, class_weight="balanced"))])


# ---------- medicion ----------
def agregado(df: pd.DataFrame, prob: np.ndarray, col: str, min_n: int) -> dict:
    t = pd.DataFrame({col: df[col].values, "real": df["riesgo"].values, "pred": prob})
    g = t.groupby(col).agg(n=("real", "size"), real=("real", "mean"), pred=("pred", "mean"))
    g = g[g["n"] >= min_n]
    if len(g) < 5:
        return {"grupos": int(len(g)), "nota": "muy pocos grupos para medir"}
    k = max(3, min(30, len(g) // 5))
    top_real = set(g["real"].nlargest(k).index)
    top_pred = set(g["pred"].nlargest(k).index)
    return {"grupos": int(len(g)),
            "spearman": float(g["real"].corr(g["pred"], method="spearman")),
            "pearson": float(g["real"].corr(g["pred"])),
            f"coincidencia_top{k}": float(len(top_real & top_pred) / k)}


def medir(nombre, pipe, Xtr, ytr, Xte, yte, test_df, min_n):
    pipe.fit(Xtr, ytr)
    prob = pipe.predict_proba(Xte)[:, 1]
    return {
        "modelo": nombre,
        "pr_auc": float(average_precision_score(yte, prob)),
        "roc_auc": float(roc_auc_score(yte, prob)),
        "por_colegio": agregado(test_df, prob, "cole_cod_icfes", min_n),
        "por_municipio": agregado(test_df, prob, "estu_mcpio_reside", min_n * 3),
    }, prob


def preparar(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["punt_global"]).reset_index(drop=True).copy()
    df["semestre"] = df["semestre"].astype(int)
    for c in base.COLUMNAS_CATEGORICAS:
        if c != "semestre":
            df[c] = df[c].astype(str).str.strip()
    df["cole_cod_icfes"] = df["cole_cod_icfes"].astype(int)
    return df


def correr(train: pd.DataFrame, test: pd.DataFrame, etiqueta: str, min_n: int) -> dict:
    """train y test ya traen la columna riesgo (umbrales calculados solo con train)."""
    X_tr = train[base.COLUMNAS_ENTRADA]
    X_te = test[base.COLUMNAS_ENTRADA]
    res_base, _ = medir("actual (11 variables)", pipeline_base(), X_tr, train["riesgo"],
                        X_te, test["riesgo"], test, min_n)

    tr2, te2 = train.copy(), test.copy()
    tr2[COL_HIST] = codificar_oof(train)
    te2[COL_HIST] = codificar(test, estadisticas(train), float(train["riesgo"].mean()))
    cols = base.COLUMNAS_ENTRADA + [COL_HIST]
    res_nuevo, _ = medir("con historial del colegio", pipeline_con_historial(),
                         tr2[cols], tr2["riesgo"], te2[cols], te2["riesgo"], te2, min_n)

    print(f"\n=== {etiqueta} | train={len(train):,} test={len(test):,} | "
          f"% riesgo test={test['riesgo'].mean():.3f} ===")
    for r in (res_base, res_nuevo):
        print(f"\n{r['modelo']}")
        print(f"  PR-AUC {r['pr_auc']:.4f} | ROC-AUC {r['roc_auc']:.4f}")
        print("  por colegio  :", {k: round(v, 3) if isinstance(v, float) else v
                                   for k, v in r["por_colegio"].items()})
        print("  por municipio:", {k: round(v, 3) if isinstance(v, float) else v
                                   for k, v in r["por_municipio"].items()})
    return {"base": res_base, "con_historial": res_nuevo,
            "prevalencia_test": float(test["riesgo"].mean())}


def main(csv: str | None, min_n: int) -> int:
    base.cargar_env(base.RAIZ / ".env")
    df = pd.read_csv(csv) if csv else base.cargar_desde_dw()
    df = preparar(df)
    resultados = {}

    # A) misma particion que entrenar_modelo.py (60/20/20 -> aqui train vs test del 20%)
    train, temp = train_test_split(df, test_size=0.40, random_state=base.SEMILLA,
                                   stratify=df["semestre"])
    _, test = train_test_split(temp, test_size=0.50, random_state=base.SEMILLA,
                               stratify=temp["semestre"])
    train, test = train.copy(), test.copy()
    umb = base.calcular_umbrales(train)
    train["riesgo"] = base.crear_etiqueta(train, umb)
    test["riesgo"] = base.crear_etiqueta(test, umb)
    resultados["A_particion_aleatoria"] = correr(train, test, "A) Particion aleatoria", min_n)

    # B) temporal: 2019-2020 -> 2025
    if "anio" in df.columns and (df["anio"] == 2025).any() and (df["anio"] <= 2020).any():
        tr = df[df["anio"] <= 2020].copy()
        te = df[df["anio"] == 2025].copy()
        umb_t = base.calcular_umbrales(tr)
        tr["riesgo"] = base.crear_etiqueta(tr, umb_t)
        te["riesgo"] = base.crear_etiqueta(te, umb_t)
        resultados["B_temporal_2019_2020_a_2025"] = correr(
            tr, te, "B) Temporal: entrena 2019-2020, prueba 2025", min_n * 2)

    salida = base.CARPETA_MODELOS / "experimento_colegio.json"
    base.CARPETA_MODELOS.mkdir(exist_ok=True)
    salida.write_text(json.dumps(resultados, indent=2, ensure_ascii=False), encoding="utf-8")
    print("\nGuardado:", salida)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv")
    ap.add_argument("--min-n", type=int, default=20,
                    help="minimo de estudiantes de prueba por colegio para medir (default 20)")
    a = ap.parse_args()
    sys.exit(main(a.csv, a.min_n))

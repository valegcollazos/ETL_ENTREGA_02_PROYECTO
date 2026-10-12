"""Pruebas de las funciones estadísticas del informe (notebooks/analisis_utils.py)."""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
import analisis_utils as au  # noqa: E402


def test_t_de_welch_coincide_con_scipy():
    rng = np.random.default_rng(0)
    a, b = rng.normal(250, 50, 4000), rng.normal(260, 70, 3000)
    r, ref = au.diff_medias(a, b), stats.ttest_ind(a, b, equal_var=False)
    assert np.isclose(r["t"], ref.statistic) and np.isclose(r["p"], ref.pvalue) and np.isclose(r["gl"], ref.df)
    ic = ref.confidence_interval()
    assert np.isclose(r["ic_bajo"], ic.low) and np.isclose(r["ic_alto"], ic.high)


def test_anova_de_welch_parecida_a_f_oneway_con_varianzas_iguales():
    rng = np.random.default_rng(1)
    g = [rng.normal(m, 50, 3000) for m in (250, 255, 262)]
    assert abs(au.welch_anova(g)["F"] - stats.f_oneway(*g).statistic) < 1.0


def test_interaccion_detecta_cambio_y_ausencia_de_cambio():
    rng = np.random.default_rng(2)
    n = 6000
    df = pd.DataFrame({"anio": rng.choice([2019, 2020, 2025], n), "x": rng.choice(["Si", "No"], n)})
    df["con_cambio"] = 250 + 10 * (df.x == "Si") + 15 * ((df.x == "Si") & (df.anio == 2020)) + rng.normal(0, 40, n)
    df["sin_cambio"] = 250 + 10 * (df.x == "Si") + rng.normal(0, 40, n)
    assert au.interaccion_f(df, "con_cambio", "anio", "x")["p"] < 0.001
    assert au.interaccion_f(df, "sin_cambio", "anio", "x")["p"] > 0.01


def test_formatos():
    assert au.fmt_p(0.0004) == "< 0,001" and au.fmt_p(0.0345) == "0,035"
    assert au.num(1234567.89, 1) == "1.234.567,9"
    assert au.etiqueta_d(0.1) == "insignificante" and au.etiqueta_d(0.9) == "grande"

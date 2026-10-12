"""Funciones de apoyo del informe analítico (carga desde el DW, estadística y gráficos).

El informe (informe_saber11.ipynb) lee SOLO el Data Warehouse (saber11_dw_e2), es
decir, datos que ya pasaron quality_check (Great Expectations) y la carga.

Estadística usada (solo numpy/scipy):
  - Welch (t y ANOVA): no suponen varianzas iguales entre grupos.
  - Kruskal-Wallis: contraste no paramétrico, como chequeo de robustez.
  - Tamaños de efecto: d de Cohen (2 grupos) y eta cuadrado (k grupos).
  - Interacción: prueba F de modelos anidados (¿la brecha cambia entre años?).
  - Bonferroni para las comparaciones múltiples.
"""
from __future__ import annotations

import itertools
import os
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ALFA = 0.05
AREAS = {
    "punt_matematicas": "Matemáticas",
    "punt_lectura_critica": "Lectura crítica",
    "punt_naturales": "Ciencias naturales",
    "punt_sociales_ciudadanas": "Sociales y ciudadanas",
    "punt_ingles": "Inglés",
}
# Una paleta por año (misma que el dashboard): azul 2019, naranja 2020, verde 2025.
COLORES_ANIO = {2019: "#2a6fdb", 2020: "#e08a1e", 2025: "#2a9d6f"}

SQL_DW = """
SELECT t.periodo, t.anio, t.semestre, t.etapa_pandemia,
       f.punt_global, f.punt_matematicas, f.punt_lectura_critica, f.punt_naturales,
       f.punt_sociales_ciudadanas, f.punt_ingles,
       c.cole_naturaleza, c.cole_calendario, c.cole_area_ubicacion,
       s.fami_estratovivienda AS estrato,
       s.fami_tieneinternet   AS internet,
       s.fami_tienecomputador AS computador
FROM fact_resultadossaber11 f
JOIN dim_tiempo t          ON t.id_tiempo = f.id_tiempo
JOIN dim_colegio c         ON c.id_colegio = f.id_colegio
JOIN dim_socioeconomica s  ON s.id_socioeconomico = f.id_socioeconomico
"""


# --------------------------------------------------------------------------- datos
def conectar():
    """Conexión al DW. Usa las variables del .env (MYSQL_USER, MYSQL_PASSWORD,
    MYSQL_HOST, MYSQL_PORT, DW_DB_NAME). Para pruebas acepta DW_URL=sqlite:///ruta."""
    url = os.getenv("DW_URL", "")
    if url.startswith("sqlite:///"):
        return sqlite3.connect(url[len("sqlite:///"):])
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    def req(nombre: str) -> str:
        valor = os.getenv(nombre)
        if not valor:
            raise RuntimeError(f"Falta la variable de entorno {nombre} (revisa el .env).")
        return valor

    return create_engine(
        URL.create(
            "mysql+pymysql",
            username=req("MYSQL_USER"),
            password=req("MYSQL_PASSWORD"),
            host=os.getenv("MYSQL_HOST", "localhost"),
            port=int(os.getenv("MYSQL_PORT", "3306")),
            database=os.getenv("DW_DB_NAME", "saber11_dw_e2"),
            query={"charset": "utf8mb4"},
        ),
        pool_pre_ping=True,
    )


def cargar_dw(con) -> pd.DataFrame:
    """Un registro por estudiante, con las dimensiones ya unidas."""
    df = pd.read_sql_query(SQL_DW, con)
    if df.empty:
        raise RuntimeError("El DW está vacío: corre primero el DAG saber11_valle_etl.")
    for col in ("anio", "semestre"):
        df[col] = df[col].astype("Int64")
    for col in ["punt_global", *AREAS]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# --------------------------------------------------------------------------- formato
def num(x, dec: int = 1) -> str:
    """Número con formato español: 1.234,5"""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "–"
    s = f"{x:,.{dec}f}"
    return s.replace(",", "§").replace(".", ",").replace("§", ".")


def fmt_p(p: float) -> str:
    if p is None or np.isnan(p):
        return "–"
    return "< 0,001" if p < 0.001 else num(p, 3)


def etiqueta_d(d: float) -> str:
    """Convención de Cohen para el tamaño del efecto."""
    a = abs(d)
    return "insignificante" if a < 0.2 else "pequeño" if a < 0.5 else "mediano" if a < 0.8 else "grande"


def etiqueta_eta2(e: float) -> str:
    return "insignificante" if e < 0.01 else "pequeño" if e < 0.06 else "mediano" if e < 0.14 else "grande"


# --------------------------------------------------------------------------- pruebas
def _limpio(x) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    return x[~np.isnan(x)]


def diff_medias(a, b, alfa: float = ALFA) -> dict:
    """a - b con t de Welch: diferencia, IC 95 %, p y d de Cohen."""
    a, b = _limpio(a), _limpio(b)
    na, nb = len(a), len(b)
    if na < 2 or nb < 2:
        raise ValueError("Cada grupo necesita al menos 2 datos.")
    ma, mb, va, vb = a.mean(), b.mean(), a.var(ddof=1), b.var(ddof=1)
    se2 = va / na + vb / nb
    gl = se2**2 / ((va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1))
    diff = ma - mb
    t = diff / np.sqrt(se2)
    p = 2 * stats.t.sf(abs(t), gl)
    crit = stats.t.ppf(1 - alfa / 2, gl)
    sp = np.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2))
    d = diff / sp if sp > 0 else np.nan
    return dict(n_a=na, n_b=nb, media_a=ma, media_b=mb, diff=diff,
                ic_bajo=diff - crit * np.sqrt(se2), ic_alto=diff + crit * np.sqrt(se2),
                t=t, gl=gl, p=p, d=d)


def welch_anova(grupos) -> dict:
    """ANOVA de Welch para k grupos (varianzas distintas)."""
    g = [_limpio(x) for x in grupos]
    g = [x for x in g if len(x) >= 2]
    k = len(g)
    if k < 2:
        raise ValueError("Se necesitan al menos 2 grupos con 2 datos o más.")
    n = np.array([len(x) for x in g], float)
    m = np.array([x.mean() for x in g])
    v = np.array([x.var(ddof=1) for x in g])
    w = n / v
    W = w.sum()
    m_w = (w * m).sum() / W
    num_ = (w * (m - m_w) ** 2).sum() / (k - 1)
    lam = ((1 - w / W) ** 2 / (n - 1)).sum()
    den = 1 + 2 * (k - 2) / (k**2 - 1) * lam
    F = num_ / den
    gl2 = (k**2 - 1) / (3 * lam)
    return dict(F=F, gl1=k - 1, gl2=gl2, p=stats.f.sf(F, k - 1, gl2))


def eta_cuadrado(grupos) -> float:
    """Proporción de la varianza total explicada por el grupo."""
    g = [_limpio(x) for x in grupos]
    todo = np.concatenate(g)
    ss_tot = ((todo - todo.mean()) ** 2).sum()
    ss_ent = sum(len(x) * (x.mean() - todo.mean()) ** 2 for x in g)
    return ss_ent / ss_tot if ss_tot > 0 else np.nan


def comparar_grupos(df: pd.DataFrame, y: str, grupo: str) -> dict:
    """Resumen de k grupos: Welch ANOVA, Kruskal-Wallis, eta², Levene."""
    g = [x[y].dropna().to_numpy() for _, x in df.groupby(grupo, observed=True)]
    w = welch_anova(g)
    kw = stats.kruskal(*g)
    lev = stats.levene(*g)
    e2 = eta_cuadrado(g)
    return dict(k=len(g), n=sum(len(x) for x in g), **{f"welch_{k}": v for k, v in w.items()},
                kruskal_H=kw.statistic, kruskal_p=kw.pvalue, levene_p=lev.pvalue,
                eta2=e2, eta2_etiqueta=etiqueta_eta2(e2))


def pares(df: pd.DataFrame, y: str, grupo: str, orden=None) -> pd.DataFrame:
    """Todas las parejas de grupos con t de Welch y p ajustado por Bonferroni."""
    niveles = list(orden) if orden is not None else sorted(df[grupo].dropna().unique())
    filas = []
    for a, b in itertools.combinations(niveles, 2):
        r = diff_medias(df.loc[df[grupo] == a, y], df.loc[df[grupo] == b, y])
        filas.append(dict(comparacion=f"{a} vs {b}", **r))
    out = pd.DataFrame(filas)
    out["p_ajustado"] = np.minimum(out["p"] * len(out), 1.0)
    out["efecto"] = out["d"].map(etiqueta_d)
    return out


def interaccion_f(df: pd.DataFrame, y: str, a: str, b: str) -> dict:
    """Prueba F de la interacción a×b: compara el modelo y~a+b contra y~a+b+a:b.
    Responde: ¿el efecto de b cambia según a? (p. ej. ¿la brecha cambia entre años?)"""
    d = df[[y, a, b]].dropna()
    yv = d[y].to_numpy(float)
    A = pd.get_dummies(d[a].astype(str), drop_first=True, dtype=float)
    B = pd.get_dummies(d[b].astype(str), drop_first=True, dtype=float)
    uno = np.ones((len(d), 1))
    X_red = np.hstack([uno, A.to_numpy(), B.to_numpy()])
    prod = [A[ca].to_numpy() * B[cb].to_numpy() for ca in A for cb in B]
    prod = [p for p in prod if p.any()]          # descarta celdas vacías
    X_full = np.hstack([X_red, np.column_stack(prod)])

    def rss(X):
        beta, *_ = np.linalg.lstsq(X, yv, rcond=None)
        r = yv - X @ beta
        return float(r @ r), np.linalg.matrix_rank(X)

    rss_r, rk_r = rss(X_red)
    rss_f, rk_f = rss(X_full)
    gl1, gl2 = rk_f - rk_r, len(yv) - rk_f
    F = ((rss_r - rss_f) / gl1) / (rss_f / gl2)
    return dict(F=F, gl1=gl1, gl2=gl2, p=stats.f.sf(F, gl1, gl2),
                eta2_parcial=(rss_r - rss_f) / rss_r)


# --------------------------------------------------------------------------- tablas y gráficos
def resumen_celdas(df: pd.DataFrame, y: str, filas: str, columnas: str | None = None) -> pd.DataFrame:
    """n, media, desviación e IC 95 % de la media por grupo."""
    claves = [filas] + ([columnas] if columnas else [])
    r = df.groupby(claves, observed=True)[y].agg(n="size", media="mean", desv="std").reset_index()
    r["ic"] = 1.96 * r["desv"] / np.sqrt(r["n"])
    return r


def grafico_medias(ax, df: pd.DataFrame, y: str, x: str, hue: str, orden_x=None, colores=None,
                   ylabel: str = "Puntaje global (media)") -> None:
    """Medias con IC 95 % (puntos con barras de error). El eje no parte de 0, porque aquí
    importa la posición del punto y su intervalo, no el largo de una barra."""
    r = resumen_celdas(df, y, x, hue)
    niveles_x = list(orden_x) if orden_x is not None else sorted(r[x].unique())
    hues = sorted(r[hue].unique())
    ancho = 0.7 / max(len(hues), 1)
    for i, h in enumerate(hues):
        sub = r[r[hue] == h].set_index(x).reindex(niveles_x)
        pos = np.arange(len(niveles_x)) + (i - (len(hues) - 1) / 2) * ancho
        color = (colores or {}).get(h)
        ax.errorbar(pos, sub["media"], yerr=sub["ic"], fmt="o", capsize=3, label=str(h), color=color)
    ax.set_xticks(np.arange(len(niveles_x)))
    ax.set_xticklabels(niveles_x, rotation=0)
    ax.set_ylabel(ylabel)
    ax.set_xlabel("")
    ax.grid(axis="x", visible=False)
    ax.legend(title="Año" if hue == "anio" else hue, frameon=False)


def guardar_fig(fig, nombre: str, carpeta: str | Path = "../reports/figuras") -> Path:
    """Guarda la figura como PNG (para el README) y devuelve la ruta."""
    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)
    ruta = carpeta / f"{nombre}.png"
    fig.savefig(ruta, dpi=150, bbox_inches="tight")
    return ruta


# --------------------------------------------------------------------------- brechas
def brecha_por_anio(df: pd.DataFrame, y: str, var: str, alto: str, bajo: str) -> pd.DataFrame:
    """Diferencia de medias (alto - bajo) dentro de cada año, con IC 95 %, p ajustado y d."""
    for nivel in (alto, bajo):
        if nivel not in set(df[var].dropna().unique()):
            raise ValueError(f"No existe '{nivel}' en {var}. Valores: {sorted(df[var].dropna().unique())}")
    filas = []
    for anio, g in df.groupby("anio", observed=True):
        r = diff_medias(g.loc[g[var] == alto, y], g.loc[g[var] == bajo, y])
        filas.append(dict(anio=int(anio), **r))
    out = pd.DataFrame(filas)
    out["p_ajustado"] = np.minimum(out["p"] * len(out), 1.0)
    out["efecto"] = out["d"].map(etiqueta_d)
    return out


def formatear_comparaciones(res: pd.DataFrame, etiqueta: str = "comparacion") -> pd.DataFrame:
    """Tabla legible (en español) a partir de pares() o brecha_por_anio()."""
    t = pd.DataFrame({
        ("Año" if etiqueta == "anio" else etiqueta.capitalize()): res[etiqueta].astype(str),
        "Diferencia": res["diff"].map(lambda v: num(v, 1)),
        "IC 95 %": [f"[{num(a, 1)} ; {num(b, 1)}]" for a, b in zip(res["ic_bajo"], res["ic_alto"])],
        "p (Bonferroni)": res["p_ajustado"].map(fmt_p),
        "d de Cohen": res["d"].map(lambda v: num(v, 2)),
        "Efecto": res["efecto"],
    })
    return t


def grafico_brechas(ax, familias: dict[str, pd.DataFrame]) -> None:
    """Brecha (diferencia de medias con IC 95 %) por año, para varias familias de brechas."""
    nombres = list(familias)
    anios = sorted({a for t in familias.values() for a in t["anio"]})
    ancho = 0.7 / max(len(anios), 1)
    for i, anio in enumerate(anios):
        xs, ys, lo, hi = [], [], [], []
        for j, nombre in enumerate(nombres):
            fila = familias[nombre].loc[familias[nombre]["anio"] == anio]
            if fila.empty:
                continue
            f = fila.iloc[0]
            xs.append(j + (i - (len(anios) - 1) / 2) * ancho)
            ys.append(f["diff"])
            lo.append(f["diff"] - f["ic_bajo"])
            hi.append(f["ic_alto"] - f["diff"])
        ax.errorbar(xs, ys, yerr=[lo, hi], fmt="o", capsize=3, label=str(anio), color=COLORES_ANIO.get(anio))
    ax.set_xticks(range(len(nombres)))
    ax.set_xticklabels(nombres)
    ax.axhline(0, color="#888", lw=0.8)
    ax.set_ylabel("Brecha en puntaje global (puntos)")
    ax.grid(axis="x", visible=False)
    ax.legend(title="Año", frameon=False)

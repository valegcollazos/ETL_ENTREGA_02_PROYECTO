"""Reporte analítico estático de Saber 11 Valle del Cauca (una sola imagen).

Consulta el modelo dimensional validado (saber11_dw_e2), responde las cinco preguntas
del proyecto y dibuja un tablero de una página con hallazgos. Es un resultado en un
punto en el tiempo: no se actualiza solo ni lee mensajes de Kafka.

Uso, desde la carpeta notebooks:
    docker compose --profile analisis run --rm jupyter python reporte_estatico.py
Resultado: reports/reporte_estatico.png y reports/reporte_estatico.pdf
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from analisis_utils import (AREAS, brecha_por_anio, cargar_dw, conectar, diff_medias, num)

ESTRATOS = [f"Estrato {i}" for i in range(1, 7)]
ANIOS = [2019, 2020, 2025]
COLOR = {2019: "#0072B2", 2020: "#E69F00", 2025: "#009E73"}
MARCA = {2019: "o", 2020: "s", 2025: "^"}
TINTA, GRIS, AZUL_OSC = "#1b1f2a", "#5b6472", "#1E2761"


# --------------------------------------------------------------------------- cálculo
def calcular(df: pd.DataFrame) -> dict:
    """Todos los números del reporte salen del DataFrame leído del DW."""
    df = df.copy()
    sem = df.groupby(["semestre", "anio"], observed=True)["punt_global"].mean()
    p1 = {int(s): {int(a): float(sem[(s, a)]) for a in ANIOS if (s, a) in sem.index} for s in (1, 2)}

    def brecha(sub, var, alto, bajo):
        return brecha_por_anio(sub[sub[var].isin([alto, bajo])], "punt_global", var, alto, bajo)

    areas = {}
    for col, nombre in AREAS.items():
        base = df.loc[df["anio"] == 2019, col]
        areas[nombre] = {a: diff_medias(df.loc[df["anio"] == a, col], base) for a in (2020, 2025)}
    return dict(
        n=len(df),
        p1=p1,
        p2=brecha(df, "estrato", "Estrato 6", "Estrato 1"),
        internet=brecha(df, "internet", "Si", "No"),
        computador=brecha(df, "computador", "Si", "No"),
        p4=brecha(df, "cole_naturaleza", "NO OFICIAL", "OFICIAL"),
        areas=areas,
    )


# --------------------------------------------------------------------------- dibujo
def _panel(fig, rect, numero, pregunta, titulo_x=None):
    ax = fig.add_axes(rect)
    for lado in ("top", "right"):
        ax.spines[lado].set_visible(False)
    ax.tick_params(labelsize=8.5, colors=GRIS)
    ax.grid(axis="y", color="#e3e6ea", lw=0.7)
    ax.set_axisbelow(True)
    x0, y0, w, h = rect
    tx = x0 if titulo_x is None else titulo_x
    fig.text(tx, y0 + h + 0.03, f"{numero}  {pregunta}", fontsize=10.8, fontweight="bold",
             color=AZUL_OSC, va="bottom", ha="left")
    return ax


def _hallazgo(fig, x, y, texto, ancho=46):
    fig.text(x, y, "\n".join(textwrap.wrap(texto, ancho)), fontsize=9.6, color=TINTA, va="top",
             ha="left", linespacing=1.3)


def _barras_brecha(ax, t, etiqueta_y=True):
    x = np.arange(len(t))
    cols = [COLOR[int(a)] for a in t["anio"]]
    ax.bar(x, t["diff"], color=cols, width=0.58)
    ax.errorbar(x, t["diff"], yerr=[t["diff"] - t["ic_bajo"], t["ic_alto"] - t["diff"]],
                fmt="none", ecolor=TINTA, capsize=4, lw=1.2)
    for xi, v in zip(x, t["diff"]):
        ax.text(xi, v / 2, num(v, 1), ha="center", va="center", color="white", fontsize=10.5,
                fontweight="bold")
    ax.set_xticks(x)
    ax.set_xticklabels([str(int(a)) for a in t["anio"]], fontsize=9.5)
    ax.set_ylabel("Brecha en puntos" if etiqueta_y else "", fontsize=8.5, color=GRIS)


def dibujar(res: dict, ruta_png: Path, ruta_pdf: Path | None = None) -> None:
    plt.rcParams.update({"font.family": "DejaVu Sans"})
    fig = plt.figure(figsize=(13.333, 7.5), dpi=250, facecolor="white")

    # encabezado
    fig.text(0.055, 0.965, "Reporte analítico estático · Saber 11 · Valle del Cauca", fontsize=18,
             fontweight="bold", color=AZUL_OSC, va="top")
    fig.text(0.055, 0.915, f"Antes (2019), durante (2020) y después (2025) de la pandemia · "
             f"{num(res['n'], 0)} estudiantes · Datos del warehouse saber11_dw_e2, validados antes de cargar",
             fontsize=10, color=GRIS, va="top")

    w, h = 0.27, 0.19
    xs = [0.055, 0.385, 0.715]
    y_top, y_bot = 0.645, 0.25
    FT, FB = 0.585, 0.19  # arranque del texto de hallazgo
    # ---- P1
    r = [xs[0], y_top, w, h]
    ax = _panel(fig, r, "P1", "¿Cambió el puntaje global entre años?")
    for s, estilo, nom in ((1, "-", "Semestre 1"), (2, "--", "Semestre 2")):
        ys = [res["p1"][s][a] for a in ANIOS]
        ax.plot(range(3), ys, estilo, color=AZUL_OSC, marker="o", lw=2, label=nom)
        for i, v in enumerate(ys):
            ax.text(i, v + 5, num(v, 1), ha="center", fontsize=8.8, color=TINTA)
    ax.set_xticks(range(3)); ax.set_xticklabels([str(a) for a in ANIOS], fontsize=9.5)
    ax.set_ylim(225, 325); ax.set_ylabel("Puntaje medio", fontsize=8.5, color=GRIS)
    ax.legend(frameon=False, fontsize=8.5, loc="upper center", ncol=2)
    mayor = max(abs(res["p1"][s][2025] - res["p1"][s][2019]) for s in (1, 2))
    _hallazgo(fig, xs[0], FT, f"Casi no cambió. Dentro del mismo semestre el mayor cambio frente a 2019 es de "
              f"{num(mayor, 1)} puntos, con efecto insignificante.")
    # ---- P2
    r = [xs[1], y_top, w, h]
    ax = _panel(fig, r, "P2", "¿Qué tan grande es la brecha por estrato?")
    _barras_brecha(ax, res["p2"])
    ax.set_ylim(0, 75)
    t = res["p2"].set_index("anio")["diff"]
    _hallazgo(fig, xs[1], FT, f"Estrato 6 menos estrato 1 bajó de {num(t[2019], 0)} a {num(t[2025], 0)} puntos. "
              "Es el único cambio relevante: los estratos 1 y 2 subieron.")
    # ---- P3
    r = [xs[2], y_top, w, h]
    ax = _panel(fig, r, "P3", "¿Cuál es la brecha digital?")
    for k, (clave, nom, dx) in enumerate((("internet", "Internet", -0.12), ("computador", "Computador", 0.12))):
        t = res[clave]
        for i, a in enumerate(t["anio"]):
            f = t.iloc[i]
            ax.errorbar(i + dx, f["diff"], yerr=[[f["diff"] - f["ic_bajo"]], [f["ic_alto"] - f["diff"]]],
                        fmt=MARCA[2019] if k == 0 else MARCA[2020], color=AZUL_OSC if k == 0 else "#C0504D",
                        mfc="white" if k else AZUL_OSC, capsize=3, ms=7, lw=1.2,
                        label=nom if i == 0 else None)
    ax.set_xticks(range(3)); ax.set_xticklabels([str(a) for a in ANIOS], fontsize=9.5)
    ax.set_xlim(-0.5, 2.5); ax.set_ylim(15, 38); ax.set_ylabel("Brecha en puntos", fontsize=8.5, color=GRIS)
    ax.legend(frameon=False, fontsize=8.5, loc="upper right", ncol=2)
    mi, mc = res["internet"].set_index("anio")["diff"], res["computador"].set_index("anio")["diff"]
    _hallazgo(fig, xs[2], FT, f"Se mantiene entre {num(min(mi.min(), mc.min()), 0)} y {num(max(mi.max(), mc.max()), 0)} puntos. "
              f"La de internet bajó a {num(mi[2025], 0)} en 2025; la de computador casi no se movió.")
    # ---- P4
    r = [xs[0], y_bot, w, h]
    ax = _panel(fig, r, "P4", "¿Y entre colegios no oficiales y oficiales?")
    _barras_brecha(ax, res["p4"])
    ax.set_ylim(0, 36)
    t = res["p4"]["diff"]
    _hallazgo(fig, xs[0], FB, f"Se mantiene entre {num(t.min(), 0)} y {num(t.max(), 0)} puntos. Está mezclada con el "
              "estrato de las familias, no se atribuye al tipo de colegio.")
    # ---- P5
    r = [xs[1] + 0.115, y_bot, w - 0.115, h]
    ax = _panel(fig, r, "P5", "¿En qué áreas cambió más el puntaje?", titulo_x=xs[1])
    orden = sorted(res["areas"], key=lambda n: res["areas"][n][2025]["diff"])
    for i, nom in enumerate(orden):
        d25, d20 = res["areas"][nom][2025], res["areas"][nom][2020]
        ax.barh(i, d25["diff"], color=COLOR[2025], height=0.55)
        ax.errorbar(d25["diff"], i, xerr=[[d25["diff"] - d25["ic_bajo"]], [d25["ic_alto"] - d25["diff"]]],
                    fmt="none", ecolor=TINTA, capsize=3, lw=1.1)
        ax.plot(d20["diff"], i, MARCA[2020], color=COLOR[2020], ms=7, mec=TINTA, mew=0.6)
        ax.text(max(d25["ic_alto"], 0) + 0.15, i, f"+{num(d25['diff'], 1)}", va="center", fontsize=9, color=TINTA)
    ax.set_yticks(range(len(orden))); ax.set_yticklabels(orden, fontsize=8.8)
    ax.axvline(0, color=GRIS, lw=0.8)
    ax.grid(axis="y", visible=False); ax.grid(axis="x", color="#e3e6ea", lw=0.7)
    ax.set_xlim(-2.2, 5.6); ax.set_xlabel("Cambio frente a 2019, en puntos", fontsize=8.5, color=GRIS)
    from matplotlib.lines import Line2D
    ax.legend(handles=[Line2D([], [], marker="s", ls="", color=COLOR[2020], mec=TINTA, label="2020"),
                       plt.Rectangle((0, 0), 1, 1, color=COLOR[2025], label="2025")],
              frameon=False, fontsize=8.5, loc="lower right")
    ing = res["areas"]["Inglés"][2025]
    _hallazgo(fig, xs[1], FB, f"Inglés mejoró {num(ing['diff'], 1)} puntos en 2025, el único efecto pequeño. "
              "Las demás áreas cambiaron menos de 2 puntos.")
    # ---- conclusiones
    x0, yb, bw, bh = xs[2] - 0.012, 0.115, w + 0.024, 0.375
    fig.patches.append(plt.Rectangle((x0, yb), bw, bh, transform=fig.transFigure, facecolor="#F1F4FA",
                                     edgecolor="#D6DCEA", lw=1, zorder=0))
    fig.text(x0 + 0.012, yb + bh - 0.025, "Qué dice el reporte", fontsize=10.8, fontweight="bold",
             color=AZUL_OSC, va="top")
    puntos = ["La brecha por estrato fue lo único que cambió de verdad en 2025.",
              "Las brechas digitales y por tipo de colegio siguen casi iguales.",
              "El puntaje global es estable y la mejora más clara está en Inglés.",
              "Límites: estudio observacional, son asociaciones y no causas. Cerca del 25 % no informa "
              "estrato o internet en 2019 y 2025, y los promedios anuales no se comparan directo."]
    cuerpo = "\n\n".join("\n".join(textwrap.wrap(("• " if i < 3 else "") + t, 48, subsequent_indent="  " if i < 3 else ""))
                          for i, t in enumerate(puntos))
    fig.text(x0 + 0.012, yb + bh - 0.075, cuerpo, fontsize=9.4, color=TINTA, va="top", ha="left", linespacing=1.28)
    fig.text(0.055, 0.04, "Brechas: diferencia de medias con intervalo de confianza del 95 %. Comparaciones dentro del mismo "
             "semestre. Tamaño del efecto con d de Cohen.", fontsize=8, color=GRIS, va="bottom")
    ruta_png.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(ruta_png, dpi=250, facecolor="white")
    if ruta_pdf:
        fig.savefig(ruta_pdf, facecolor="white")
    plt.close(fig)


def main() -> None:
    carpeta = Path(__file__).resolve().parent.parent / "reports"
    df = cargar_dw(conectar())
    res = calcular(df)
    dibujar(res, carpeta / "reporte_estatico.png", carpeta / "reporte_estatico.pdf")
    print(f"Reporte guardado en {carpeta / 'reporte_estatico.png'}")


if __name__ == "__main__":
    main()

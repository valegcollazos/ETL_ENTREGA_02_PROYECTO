from __future__ import annotations
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

log = logging.getLogger("etl.dashboard")

NOMBRE_ARCHIVO = "saber11_valle.html"


SQL_PERIODOS = """
SELECT t.periodo, t.anio, t.semestre, t.etapa_pandemia,
       COUNT(*)                         AS estudiantes,
       AVG(f.punt_global)               AS p_global,
       AVG(f.punt_matematicas)          AS matematicas,
       AVG(f.punt_lectura_critica)      AS lectura,
       AVG(f.punt_naturales)            AS naturales,
       AVG(f.punt_sociales_ciudadanas)  AS sociales,
       AVG(f.punt_ingles)               AS ingles
FROM fact_resultadossaber11 f
JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
GROUP BY t.periodo, t.anio, t.semestre, t.etapa_pandemia
ORDER BY t.periodo
"""

SQL_ESTRATO = """
SELECT t.anio, s.fami_estratovivienda AS categoria,
       COUNT(*) AS estudiantes, AVG(f.punt_global) AS p_global
FROM fact_resultadossaber11 f
JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
JOIN dim_socioeconomica s ON s.id_socioeconomico = f.id_socioeconomico
GROUP BY t.anio, s.fami_estratovivienda
ORDER BY t.anio, s.fami_estratovivienda
"""

SQL_NATURALEZA = """
SELECT t.anio, c.cole_naturaleza AS categoria,
       COUNT(*) AS estudiantes, AVG(f.punt_global) AS p_global
FROM fact_resultadossaber11 f
JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
JOIN dim_colegio c ON c.id_colegio = f.id_colegio
GROUP BY t.anio, c.cole_naturaleza
ORDER BY t.anio, c.cole_naturaleza
"""

SQL_INTERNET = """
SELECT t.anio, s.fami_tieneinternet AS categoria,
       COUNT(*) AS estudiantes, AVG(f.punt_global) AS p_global
FROM fact_resultadossaber11 f
JOIN dim_tiempo t ON t.id_tiempo = f.id_tiempo
JOIN dim_socioeconomica s ON s.id_socioeconomico = f.id_socioeconomico
GROUP BY t.anio, s.fami_tieneinternet
ORDER BY t.anio, s.fami_tieneinternet
"""


def _carpeta_salida() -> Path:
    
    valor = os.getenv("DASHBOARD_DIR")
    if valor:
        return Path(valor)
    return Path(__file__).resolve().parents[2] / "dashboards"


def _json_seguro(df: pd.DataFrame) -> list[dict]:
   
    df = df.copy()
    for col in df.select_dtypes("number").columns:
        df[col] = df[col].astype(float).round(2)
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict(orient="records")


def _consultar(conexion) -> dict[str, pd.DataFrame]:
   
    datos = {
        "periodos": pd.read_sql_query(SQL_PERIODOS, conexion),
        "estrato": pd.read_sql_query(SQL_ESTRATO, conexion),
        "naturaleza": pd.read_sql_query(SQL_NATURALEZA, conexion),
        "internet": pd.read_sql_query(SQL_INTERNET, conexion),
    }
    if datos["periodos"].empty:
        raise RuntimeError("El DW no tiene filas en fact_resultadossaber11: no hay nada que graficar.")
    return datos


def _renderizar(datos: dict[str, pd.DataFrame]) -> str:
    payload = {k: _json_seguro(v) for k, v in datos.items()}
    total = int(datos["periodos"]["estudiantes"].sum())
    anios = sorted(int(a) for a in datos["periodos"]["anio"].unique())
    etapas = (
        datos["periodos"].drop_duplicates("anio").sort_values("anio")
        [["anio", "etapa_pandemia"]].to_dict(orient="records")
    )
    meta = {
        "total": total,
        "anios": anios,
        "etapas": {str(r["anio"]): r["etapa_pandemia"] for r in etapas},
        "generado": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }
    return (
        PLANTILLA
        .replace("__DATOS__", json.dumps(payload, ensure_ascii=False))
        .replace("__META__", json.dumps(meta, ensure_ascii=False))
    )


def _generar(conexion, salida: Path) -> str:
    datos = _consultar(conexion)
    salida.mkdir(parents=True, exist_ok=True)
    ruta = salida / NOMBRE_ARCHIVO
    ruta.write_text(_renderizar(datos), encoding="utf-8")  
    log.info("Dashboard escrito en %s (%s estudiantes, años %s).",
             ruta, f"{int(datos['periodos']['estudiantes'].sum()):,}",
             sorted(datos["periodos"]["anio"].unique().tolist()))
    return str(ruta)


def dashboard_update(staging_dir: str | None = None, **kwargs) -> str:
   
    from etl.config import get_engine_dw  

    engine = get_engine_dw()
    try:
        with engine.connect() as conexion:
            return _generar(conexion, _carpeta_salida())
    finally:
        engine.dispose()


PLANTILLA = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Saber 11 Valle del Cauca</title>
<style>
:root{--bg:#f7f7f5;--card:#fff;--tx:#1f2328;--mut:#5b6470;--bd:#dcdfe3;
      --c0:#2a6fdb;--c1:#e08a1e;--c2:#2a9d6f;--c3:#8f4fc4;--c4:#c43e5e}
@media (prefers-color-scheme:dark){:root{--bg:#14171a;--card:#1c2025;--tx:#e8eaed;--mut:#9aa3ad;--bd:#303640;
      --c0:#6aa3ff;--c1:#f0a850;--c2:#52c79a;--c3:#b98cf0;--c4:#ef7f99}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--tx);font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px 48px}
h1{font-size:22px;margin:0 0 4px}
.sub{color:var(--mut);margin:0 0 20px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:14px 16px}
.kpi b{display:block;font-size:24px}.kpi span{color:var(--mut);font-size:13px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(440px,1fr));gap:16px}
@media(max-width:520px){.grid{grid-template-columns:1fr}}
h2{font-size:15px;margin:0 0 2px}.nota{color:var(--mut);font-size:12.5px;margin:0 0 10px}
.chart{position:relative;height:290px}
table{width:100%;border-collapse:collapse;font-size:13.5px}
th,td{padding:6px 8px;border-bottom:1px solid var(--bd);text-align:right}
th:first-child,td:first-child{text-align:left}
footer{color:var(--mut);font-size:12.5px;margin-top:20px}
</style>
</head>
<body>
<main>
<h1>Resultados Saber 11 · Valle del Cauca</h1>
<p class="sub">Antes (2019), durante (2020) y después (2025) de la pandemia. Datos leídos del Data Warehouse, ya validados.</p>
<div class="kpis" id="kpis"></div>
<div class="grid">
  <div class="card"><h2>Puntaje global promedio por periodo</h2><p class="nota">Cada punto es un semestre.</p><div class="chart"><canvas id="c1"></canvas></div></div>
  <div class="card"><h2>Promedio por área y año</h2><p class="nota">Matemáticas, lectura crítica, naturales, sociales e inglés.</p><div class="chart"><canvas id="c2"></canvas></div></div>
  <div class="card"><h2>Puntaje global por estrato</h2><p class="nota">Promedio del puntaje global según el estrato de la vivienda.</p><div class="chart"><canvas id="c3"></canvas></div></div>
  <div class="card"><h2>Colegio oficial vs. no oficial</h2><p class="nota">Promedio del puntaje global según la naturaleza del colegio.</p><div class="chart"><canvas id="c4"></canvas></div></div>
  <div class="card"><h2>Acceso a internet en el hogar</h2><p class="nota">Promedio del puntaje global según si el hogar tiene internet.</p><div class="chart"><canvas id="c5"></canvas></div></div>
  <div class="card"><h2>Resumen por periodo</h2><p class="nota">Estudiantes y puntaje global promedio.</p><div style="overflow-x:auto"><table id="tabla"></table></div></div>
</div>
<footer id="pie"></footer>
</main>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<script>
const D = __DATOS__;
const M = __META__;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const col = i => css('--c' + (i % 5));
const fmt = n => n == null ? '–' : Number(n).toLocaleString('es-CO');
const etapa = a => M.etapas[a] ? ' (' + M.etapas[a] + ')' : '';
Chart.defaults.color = css('--mut');
Chart.defaults.borderColor = css('--bd');
Chart.defaults.font.family = 'system-ui, sans-serif';
const opts = (extra) => Object.assign({responsive:true, maintainAspectRatio:false,
  plugins:{legend:{position:'bottom'}}}, extra || {});
const yAxis = {scales:{y:{beginAtZero:false, title:{display:true, text:'Puntaje'}}}};

// KPIs
const gp = D.periodos.filter(r => r.p_global != null);
const gTot = gp.reduce((a, r) => a + r.estudiantes, 0);
document.getElementById('kpis').innerHTML = [
  [fmt(M.total), 'estudiantes en el DW'],
  [M.anios.join(' · '), 'años cargados'],
  [D.periodos.length, 'periodos'],
  [fmt(Math.round(gp.reduce((a, r) => a + r.p_global * r.estudiantes, 0) / gTot)), 'puntaje global promedio (todos los estudiantes)']
].map(k => `<div class="card kpi"><b>${k[0]}</b><span>${k[1]}</span></div>`).join('');

// 1. Línea por periodo
new Chart(document.getElementById('c1'), {type:'line',
  data:{labels:D.periodos.map(r => r.periodo),
        datasets:[{label:'Puntaje global', data:D.periodos.map(r => r.p_global),
                   borderColor:col(0), backgroundColor:col(0), tension:.25, pointRadius:5}]},
  options:opts(yAxis)});

// 2. Barras agrupadas por área (una serie por año)
const areas = [['matematicas','Matemáticas'],['lectura','Lectura crítica'],['naturales','Naturales'],
               ['sociales','Sociales'],['ingles','Inglés']];
const porAnio = {};
D.periodos.forEach(r => { (porAnio[r.anio] = porAnio[r.anio] || []).push(r); });
const media = (filas, k, w) => {
  const v = filas.filter(r => r[k] != null);
  const n = v.reduce((a, r) => a + r.estudiantes, 0);
  return n ? v.reduce((a, r) => a + r[k] * r.estudiantes, 0) / n : null;  // ponderado por estudiantes
};
new Chart(document.getElementById('c2'), {type:'bar',
  data:{labels:areas.map(a => a[1]),
        datasets:M.anios.map((a, i) => ({label:a + etapa(a),
          data:areas.map(ar => { const v = media(porAnio[a], ar[0]); return v == null ? null : +v.toFixed(2); }),
          backgroundColor:col(i)}))},
  options:opts(yAxis)});

// 3-5. Barras por categoría, una serie por año
function porCategoria(id, filas) {
  const cats = [...new Set(filas.map(r => r.categoria))];
  new Chart(document.getElementById(id), {type:'bar',
    data:{labels:cats,
          datasets:M.anios.map((a, i) => ({label:a + etapa(a),
            data:cats.map(c => { const r = filas.find(x => x.anio == a && x.categoria == c); return r ? r.p_global : null; }),
            backgroundColor:col(i)}))},
    options:opts(yAxis)});
}
porCategoria('c3', D.estrato);
porCategoria('c4', D.naturaleza);
porCategoria('c5', D.internet);

// Tabla resumen
document.getElementById('tabla').innerHTML =
  '<tr><th>Periodo</th><th>Etapa</th><th>Estudiantes</th><th>Global</th></tr>' +
  D.periodos.map(r => `<tr><td>${r.periodo}</td><td>${r.etapa_pandemia ?? ''}</td><td>${fmt(r.estudiantes)}</td><td>${fmt(r.p_global)}</td></tr>`).join('');

document.getElementById('pie').textContent =
  'Fuente: Data Warehouse saber11_dw_e2 (esquema estrella). Generado el ' + M.generado + '.';
</script>
</body>
</html>
"""
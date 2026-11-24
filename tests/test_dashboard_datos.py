"""Pruebas de la lógica de datos del tablero, sin Streamlit. Correr con:  pytest -q"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "streaming" / "dashboard"))
import datos  # noqa: E402


def evento(i, prob, cod="1", nombre="COLEGIO A", mun="CALI", estrato="Estrato 1"):
    return {"event_id": i, "event_ts": f"2026-11-23T09:59:{i % 60:02d}+00:00", "prediction": int(prob >= 0.5), "probability_riesgo": prob,
            "pred_ts": f"2026-11-23T10:00:{i % 60:02d}+00:00", "model_version": "2.0.0",
            "estu_mcpio_reside": mun, "fami_estratovivienda": estrato,
            "cole_cod_icfes": cod, "cole_nombre_establecimiento": nombre}


def cargar(tmp_path, filas):
    f = tmp_path / "p.jsonl"
    f.write_text("\n".join(json.dumps(x) for x in filas) + "\n", encoding="utf-8")
    return datos.cargar_predicciones(f)


def test_estudiantes_sin_colegio_no_entran_en_la_lista(tmp_path):
    df = cargar(tmp_path, [evento(1, 0.9, cod="-1", nombre="Sin colegio")] +
                [evento(i, 0.9, cod="7", nombre="COLEGIO B") for i in range(2, 5)])
    tabla = datos.prioridad_colegios(df, min_casos=1)
    assert list(tabla["Código"]) == ["7"]
    assert datos.resumen_general(df)["colegios"] == 1


def test_excluir_atendidos(tmp_path):
    df = cargar(tmp_path, [evento(1, 0.9, cod="1"), evento(2, 0.9, cod="2", nombre="B")])
    assert set(datos.excluir_atendidos(df, ["1"])["cod_colegio"]) == {"2"}
    assert len(datos.excluir_atendidos(df, [])) == 2


def test_estado_usa_solo_palabras(tmp_path):
    t = pd.DataFrame({"pct_riesgo": [80.0, 40.0, 10.0]})
    est = datos.con_estado(t, umbral_pct=50, promedio_pct=30)["Estado"].tolist()
    assert est == ["Alerta", "Vigilar", "Normal"]


def test_hora_colombia_resta_cinco_horas(tmp_path):
    df = cargar(tmp_path, [evento(1, 0.9)])
    assert datos.hora_colombia(df["ts"]).dt.hour.iloc[0] == 5


def test_evolucion_pct(tmp_path):
    assert datos.evolucion_pct(cargar(tmp_path, [evento(i, 0.9) for i in range(1, 5)])) == []
    df = cargar(tmp_path, [evento(i, 0.9 if i % 2 else 0.1) for i in range(1, 400)])
    serie = datos.evolucion_pct(df, max_puntos=50)
    assert 0 < len(serie) <= 50 and all(0 <= x <= 100 for x in serie)


def test_distribucion_y_acumulado(tmp_path):
    df = cargar(tmp_path, [evento(i, 0.9 if i % 4 == 0 else 0.1) for i in range(1, 201)])
    dist = datos.distribucion_categorias(df)
    assert dist["n"].sum() == 200 and dist.loc[0, "categoria"] == "En riesgo" and dist.loc[0, "pct"] == 25.0
    acum = datos.acumulado_por_categoria(df, max_puntos=40)
    ultimo = acum[acum["n"] == acum["n"].max()]
    assert ultimo["acum"].sum() == 200


def test_pct_esperado_y_estratos(tmp_path):
    met = {"metricas_test": {"matriz_confusion": {"tn": 60, "fp": 20, "fn": 10, "tp": 10}}}
    assert datos.pct_esperado_modelo(met) == 30.0
    assert datos.pct_esperado_modelo({}) is None
    filas = [evento(i, 0.9, estrato="Estrato 1") for i in range(1, 41)] + \
            [evento(i, 0.1, estrato="Estrato 5") for i in range(41, 81)]
    mayor, p1, menor, p2 = datos.lectura_estratos(cargar(tmp_path, filas))
    assert mayor == "Estrato 1" and menor == "Estrato 5" and p1 > p2


def test_monitoreo_del_streaming(tmp_path):
    df = cargar(tmp_path, [evento(i, 0.9) for i in range(1, 6)])
    ultimo = df["ts"].max()
    assert datos.estado_streaming(df, True, ultimo + pd.Timedelta(seconds=5)) == "Activo"
    assert datos.estado_streaming(df, True, ultimo + pd.Timedelta(minutes=10)) == "Sin actividad"
    assert datos.estado_streaming(df, False, ultimo) == "Datos de ejemplo"
    assert datos.estado_streaming(df.iloc[0:0], True) == "Sin datos"
    assert datos.mensajes_ultimo_minuto(df, ultimo + pd.Timedelta(seconds=10)) == 5
    assert datos.mensajes_ultimo_minuto(df, ultimo + pd.Timedelta(minutes=5)) == 0
    assert datos.retraso_mediano_seg(df) is not None and datos.retraso_mediano_seg(df) > 0
    ritmo = datos.mensajes_por_ventana(df, ventana_seg=10, ventanas=5)
    assert len(ritmo) == 5 and ritmo["mensajes"].sum() == 5

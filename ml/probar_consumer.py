"""Prueba del contrato modelo <-> consumer (v2.0, con historial del colegio).

Simula lo que hara el consumer de Kafka de Valeria, SIN importar entrenar_modelo.py:
solo usa models/modelo_riesgo.joblib y models/ejemplo_mensaje.json.

Uso (desde la raiz del proyecto):  python ml/probar_consumer.py
Sale con codigo 0 si todo pasa; 1 si algo falla.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import pandas as pd
import sklearn

MODELOS = Path(__file__).resolve().parents[1] / "models"
fallos: list[str] = []


def check(nombre: str, ok: bool, detalle: str = "") -> None:
    print(f"[{'OK' if ok else 'FALLA'}] {nombre}" + (f"  -> {detalle}" if detalle else ""))
    if not ok:
        fallos.append(nombre)


def predecir(artefacto: dict, evento: dict) -> dict:
    """Misma logica que debe tener el consumer."""
    modelo, meta = artefacto["model"], artefacto["metadata"]
    fila = {c: evento[c] for c in meta["features"] if c in evento}
    if meta.get("usa_historial_colegio"):
        tabla = meta["historial_colegios"]
        fila[meta["columna_historial"]] = tabla["colegios"].get(
            str(int(evento["cole_cod_icfes"])), tabla["media_global"])
    X = pd.DataFrame([fila])[meta["features"]]
    p = float(modelo.predict_proba(X)[0, 1])
    pred = int(p >= meta["decision_threshold"])
    return {"event_id": evento.get("event_id"), "model_id": meta["model_name"],
            "model_version": meta["model_version"], "prediction": pred,
            "prediction_label": "en_riesgo" if pred else "sin_riesgo",
            "probability_riesgo": round(p, 4)}


def main() -> int:
    art = joblib.load(MODELOS / "modelo_riesgo.joblib")
    meta = art["metadata"]
    ej = json.loads((MODELOS / "ejemplo_mensaje.json").read_text(encoding="utf-8"))
    evento, esperado = ej["evento_entrada"], ej["mensaje_prediccion_esperado"]

    check("version de scikit-learn igual a la del entrenamiento",
          sklearn.__version__ == meta["scikit_learn_version"],
          f"aqui={sklearn.__version__} entrenado={meta['scikit_learn_version']}")
    check("metadata trae lo necesario",
          all(k in meta for k in ("features", "event_features", "decision_threshold",
                                  "historial_colegios", "columna_historial", "model_version")))
    check("evento trae todas las columnas de event_features",
          set(meta["event_features"]) <= set(evento), str(set(meta["event_features"]) - set(evento)))
    prohibidas = {"punt_global", "punt_matematicas", "punt_lectura_critica", "punt_naturales",
                  "punt_sociales_ciudadanas", "punt_ingles", "riesgo"}
    check("el modelo no usa puntajes (sin fuga de datos)", not (prohibidas & set(meta["features"])))

    # 1) reproduce el mensaje de ejemplo
    msg = predecir(art, evento)
    check("reproduce la probabilidad del ejemplo",
          abs(msg["probability_riesgo"] - esperado["probability_riesgo"]) < 1e-4,
          f"{msg['probability_riesgo']} vs {esperado['probability_riesgo']}")
    check("misma etiqueta y version que el ejemplo",
          msg["prediction"] == esperado["prediction"]
          and msg["model_version"] == esperado["model_version"])

    # 2) colegio desconocido -> promedio global, no revienta
    ev2 = {**evento, "cole_cod_icfes": 999999999}
    try:
        m2 = predecir(art, ev2)
        check("colegio desconocido no rompe (usa media global)", 0 <= m2["probability_riesgo"] <= 1,
              f"p={m2['probability_riesgo']}")
    except Exception as e:  # noqa: BLE001
        check("colegio desconocido no rompe (usa media global)", False, repr(e))

    # 3) categoria nueva (municipio inexistente) -> no rompe
    try:
        m3 = predecir(art, {**evento, "estu_mcpio_reside": "MUNICIPIO_NUEVO"})
        check("municipio nuevo no rompe", 0 <= m3["probability_riesgo"] <= 1)
    except Exception as e:  # noqa: BLE001
        check("municipio nuevo no rompe", False, repr(e))

    # 4) el historial SI influye: colegio de mayor vs menor riesgo
    col = meta["historial_colegios"]["colegios"]
    if col:
        alto = max(col, key=col.get)
        bajo = min(col, key=col.get)
        pa = predecir(art, {**evento, "cole_cod_icfes": int(alto)})["probability_riesgo"]
        pb = predecir(art, {**evento, "cole_cod_icfes": int(bajo)})["probability_riesgo"]
        check("colegio de historial alto da mas riesgo que uno de historial bajo", pa > pb,
              f"alto={pa} bajo={pb}")

    # 5) semestre como texto: no rompe, pero cambia la prediccion en silencio (aviso)
    try:
        ms = predecir(art, {**evento, "semestre": str(evento["semestre"])})
        if abs(ms["probability_riesgo"] - msg["probability_riesgo"]) > 1e-4:
            print(f"[AVISO] semestre como texto NO rompe pero cambia la probabilidad "
                  f"({ms['probability_riesgo']} vs {msg['probability_riesgo']}): "
                  "el consumer debe hacer int(semestre)")
        else:
            print("[AVISO] semestre como texto da lo mismo")
    except Exception:  # noqa: BLE001
        print("[AVISO] semestre como texto rompe: el consumer debe convertirlo con int()")

    print("\nMensaje de salida de ejemplo:\n", json.dumps(msg, indent=2, ensure_ascii=False))
    print(f"\n{'TODO OK' if not fallos else 'FALLARON: ' + ', '.join(fallos)}")
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())

"""Consumidor Kafka: carga el modelo UNA vez y predice por cada evento.

- No reentrena nada: solo usa el artefacto models/modelo_riesgo.joblib.
- Escribe cada prediccion en data/streaming/predicciones.jsonl (lo lee el dashboard)
  y la publica en el topico saber11.predicciones.
- Si el modelo trae metadata['historial_colegios'] (modelo 2.0), agrega la columna
  colegio_riesgo_hist; si el colegio es nuevo usa la media global.
- Descarta event_id ya procesados (idempotencia ante re-ejecuciones).

Uso:  python streaming/consumidor.py
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
RUTA_MODELO = Path(os.getenv("RUTA_MODELO", RAIZ / "models" / "modelo_riesgo.joblib"))
RUTA_SALIDA = Path(os.getenv("RUTA_PREDICCIONES", RAIZ / "data" / "streaming" / "predicciones.jsonl"))
TOPICO_ENTRADA = os.getenv("TOPICO_ESTUDIANTES", "saber11.estudiantes")
TOPICO_SALIDA = os.getenv("TOPICO_PREDICCIONES", "saber11.predicciones")

COLUMNAS_ENTRADA = [
    "semestre",
    "fami_estratovivienda", "fami_educacionpadre", "fami_educacionmadre",
    "fami_tieneinternet", "fami_tienecomputador", "fami_personashogar",
    "cole_naturaleza", "cole_calendario", "cole_area_ubicacion",
    "estu_mcpio_reside",
]


def cargar_modelo(ruta: Path):
    """Devuelve (pipeline, metadata). Acepta un Pipeline solo o un dict con metadata."""
    obj = joblib.load(ruta)
    meta: dict = {}
    if hasattr(obj, "predict_proba"):
        pipe = obj
    elif isinstance(obj, dict):
        print(f"Claves del joblib: {list(obj.keys())}")
        meta = obj.get("metadata") or obj.get("meta") or {}
        pipe = next((v for v in obj.values() if hasattr(v, "predict_proba")), None)
        if pipe is None:
            raise SystemExit("El joblib es un dict pero no trae ningun objeto con predict_proba.")
    else:
        raise SystemExit(f"Formato de joblib no reconocido: {type(obj)}")
    return pipe, meta


def predecir(pipe, meta: dict, evento: dict) -> dict:
    fila = {c: evento[c] for c in COLUMNAS_ENTRADA}
    fila["semestre"] = int(fila["semestre"])
    tabla = meta.get("historial_colegios")
    if tabla:                                       # modelo 2.0
        fila["colegio_riesgo_hist"] = tabla["colegios"].get(
            str(evento.get("cole_cod_icfes")), tabla["media_global"])
    prob = float(pipe.predict_proba(pd.DataFrame([fila]))[0, 1])
    umbral = float(meta.get("decision_threshold", 0.5))
    pred = int(prob >= umbral)
    return {
        "event_id": evento["event_id"],
        "model_id": meta.get("model_id", "saber11-estudiante-en-riesgo"),
        "model_version": meta.get("model_version", "1.0.0"),
        "prediction": pred,
        "prediction_label": "en_riesgo" if pred else "sin_riesgo",
        "probability_riesgo": round(prob, 4),
    }


def ids_ya_procesados() -> set[int]:
    vistos: set[int] = set()
    if RUTA_SALIDA.exists():
        for linea in RUTA_SALIDA.read_text(encoding="utf-8").splitlines():
            try:
                vistos.add(int(json.loads(linea)["event_id"]))
            except (ValueError, KeyError):
                pass
    return vistos


def main() -> None:
    from kafka import KafkaConsumer, KafkaProducer

    pipe, meta = cargar_modelo(RUTA_MODELO)
    print(f"Modelo cargado: {RUTA_MODELO.name} | version={meta.get('model_version', '1.0.0')} "
          f"| umbral={meta.get('decision_threshold', 0.5)} "
          f"| historial_colegios={'si' if meta.get('historial_colegios') else 'no'}")

    RUTA_SALIDA.parent.mkdir(parents=True, exist_ok=True)
    vistos = ids_ya_procesados()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")

    consumidor = KafkaConsumer(
        TOPICO_ENTRADA,
        bootstrap_servers=bootstrap,
        group_id="saber11-consumidor",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    productor = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )
    print(f"Escuchando '{TOPICO_ENTRADA}' en {bootstrap} ... (Ctrl+C para parar)")

    n = 0
    with RUTA_SALIDA.open("a", encoding="utf-8") as salida:
        for msg in consumidor:
            ev = msg.value
            try:
                if int(ev["event_id"]) in vistos:
                    continue
                res = predecir(pipe, meta, ev)
                # Mensaje oficial + campos extra para el dashboard
                registro = {
                    **res,
                    "event_ts": ev.get("event_ts"),
                    "pred_ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "semestre": ev["semestre"],
                    "estu_mcpio_reside": ev["estu_mcpio_reside"],
                    "fami_estratovivienda": ev["fami_estratovivienda"],
                    "cole_cod_icfes": ev.get("cole_cod_icfes"),
                    "cole_nombre_establecimiento": ev.get("cole_nombre_establecimiento"),
                }
                salida.write(json.dumps(registro, ensure_ascii=False) + "\n")
                salida.flush()
                productor.send(TOPICO_SALIDA, key=str(res["event_id"]), value=registro)
                vistos.add(int(ev["event_id"]))
                n += 1
                if n % 100 == 0:
                    print(f"  predicciones: {n}")
            except Exception as e:                  # un mensaje malo no tumba el consumidor
                print(f"Error con event_id={ev.get('event_id')}: {e!r}")


if __name__ == "__main__":
    main()

"""Productor Kafka: lee estudiantes de la tabla de hechos (DW) y los publica como eventos.

- Publica SOLO datos (nunca el modelo). No incluye punt_global (el target no viaja).
- event_id = id_resultado de la fact (unico en el DW): una re-ejecucion reenvia el
  mismo event_id y el consumidor lo descarta si ya lo proceso (idempotencia).
- Por defecto solo envia estudiantes del conjunto de prueba (models/ids_test.csv),
  que el modelo no vio al entrenar.

Uso:  python streaming/productor.py --limit 1000 --delay 0.3
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

RAIZ = Path(__file__).resolve().parents[1]
TOPICO = os.getenv("TOPICO_ESTUDIANTES", "saber11.estudiantes")

# Mismas columnas que usa el modelo (contrato con ml/entrenar_modelo.py)
COLUMNAS_ENTRADA = [
    "semestre",
    "fami_estratovivienda", "fami_educacionpadre", "fami_educacionmadre",
    "fami_tieneinternet", "fami_tienecomputador", "fami_personashogar",
    "cole_naturaleza", "cole_calendario", "cole_area_ubicacion",
    "estu_mcpio_reside",
]
# Extras: cole_cod_icfes (historial del colegio en el modelo 2.0) y el nombre (dashboard)
COLUMNAS_EXTRA = ["cole_cod_icfes", "cole_nombre_establecimiento"]

SQL = """
SELECT f.id_resultado,
       t.semestre,
       s.fami_estratovivienda, s.fami_educacionpadre, s.fami_educacionmadre,
       s.fami_tieneinternet, s.fami_tienecomputador, s.fami_personashogar,
       c.cole_cod_icfes, c.cole_nombre_establecimiento,
       c.cole_naturaleza, c.cole_calendario, c.cole_area_ubicacion,
       u.estu_mcpio_reside
FROM fact_resultadossaber11 f
JOIN dim_tiempo         t ON t.id_tiempo         = f.id_tiempo
JOIN dim_socioeconomica s ON s.id_socioeconomico = f.id_socioeconomico
JOIN dim_colegio        c ON c.id_colegio        = f.id_colegio
JOIN dim_ubicacion      u ON u.id_ubicacion      = f.id_ubicacion
WHERE UPPER(TRIM(u.estu_depto_reside)) LIKE 'VALLE%'
  AND f.punt_global IS NOT NULL
"""


def leer_dw() -> pd.DataFrame:
    usuario = os.environ["MYSQL_USER"]
    clave = os.environ["MYSQL_PASSWORD"]
    host = os.getenv("MYSQL_HOST", "localhost")
    puerto = os.getenv("MYSQL_PORT", "3307")
    base = os.getenv("DW_DB_NAME", "saber11_dw_e2")
    url = f"mysql+pymysql://{usuario}:{clave}@{host}:{puerto}/{base}?charset=utf8mb4"
    with create_engine(url).connect() as con:
        return pd.read_sql(text(SQL), con)


def ids_de_prueba() -> set[int] | None:
    ruta = RAIZ / "models" / "ids_test.csv"
    if not ruta.exists():
        return None
    ids = pd.read_csv(ruta)
    for nombre in ("id_resultado", "event_id"):
        if nombre in ids.columns:
            return set(ids[nombre].astype(int))
    return set(ids.iloc[:, 0].astype(int))


def crear_evento(fila: pd.Series) -> dict:
    ev = {
        "event_id": int(fila["id_resultado"]),
        "event_ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "semestre": int(fila["semestre"]),            # entero: si va como texto predice distinto
    }
    for col in COLUMNAS_ENTRADA[1:] + COLUMNAS_EXTRA:
        valor = fila[col]
        ev[col] = "No informa" if pd.isna(valor) else str(valor)
    return ev


def crear_productor(bootstrap: str):
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable
    for intento in range(1, 11):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap,
                key_serializer=lambda k: k.encode("utf-8"),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka aun no responde en {bootstrap} (intento {intento}/10)...")
            time.sleep(3)
    raise SystemExit("No se pudo conectar a Kafka.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=1000, help="max. eventos (0 = todos)")
    ap.add_argument("--delay", type=float, default=0.3, help="segundos entre eventos")
    ap.add_argument("--todos", action="store_true",
                    help="enviar todos los estudiantes, no solo el conjunto de prueba")
    args = ap.parse_args()

    df = leer_dw()
    print(f"Filas leidas del DW (Valle): {len(df)}")
    if not args.todos:
        ids = ids_de_prueba()
        if ids is not None:
            df = df[df["id_resultado"].isin(ids)]
            print(f"Solo conjunto de prueba (models/ids_test.csv): {len(df)} filas")
    df = df.sample(frac=1, random_state=42)           # mezcla periodos y colegios
    if args.limit:
        df = df.head(args.limit)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
    productor = crear_productor(bootstrap)
    print(f"Publicando {len(df)} eventos en '{TOPICO}' ({bootstrap}), pausa {args.delay}s")
    for i, (_, fila) in enumerate(df.iterrows(), start=1):
        ev = crear_evento(fila)
        productor.send(TOPICO, key=str(ev["event_id"]), value=ev)
        if i % 100 == 0:
            print(f"  enviados {i}")
        time.sleep(args.delay)
    productor.flush()
    print("Listo: todos los eventos enviados.")


if __name__ == "__main__":
    main()

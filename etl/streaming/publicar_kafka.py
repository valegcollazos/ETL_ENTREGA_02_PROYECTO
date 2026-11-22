"""Tarea del DAG: publica en Kafka estudiantes de la tabla de hechos ya cargada.

Va DESPUES de `load`. Reutiliza streaming/productor.py. Los eventos llevan como
event_id el id_resultado de la fact: si el DAG se re-ejecuta, se reenvian los mismos
event_id y el consumidor descarta los que ya proceso (idempotencia).

Variables de entorno (opcionales):
  KAFKA_EVENTOS_POR_EJECUCION  (defecto 200)
  KAFKA_PAUSA_SEGUNDOS         (defecto 0.05)
  KAFKA_BOOTSTRAP              (en docker-compose: kafka:9092)
"""
from __future__ import annotations

import os


def publicar_kafka(staging_dir: str | None = None) -> str:
    from streaming.productor import publicar

    limite = int(os.getenv("KAFKA_EVENTOS_POR_EJECUCION", "200"))
    pausa = float(os.getenv("KAFKA_PAUSA_SEGUNDOS", "0.05"))
    enviados = publicar(limite=limite, pausa=pausa)
    return f"Eventos publicados en Kafka: {enviados}"

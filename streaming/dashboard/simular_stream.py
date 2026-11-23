"""Simulador de stream para la demo del dashboard.

Va agregando predicciones poco a poco a `data/streaming/predicciones.jsonl`
(con una pausa entre cada una), tomándolas de `streaming/ejemplo_predicciones.jsonl`.
Así, en la sustentación, se ve el tablero actualizarse EN VIVO sin tener que
levantar todo Kafka + MySQL + consumidor.

No reemplaza al flujo real (productor -> Kafka -> consumidor): es solo una ayuda
para demostrar la reactividad del dashboard de forma controlada.

Uso:
    python streaming/dashboard/simular_stream.py                 # una pasada, pausa 0.8 s
    python streaming/dashboard/simular_stream.py --delay 0.3     # más rápido
    python streaming/dashboard/simular_stream.py --loop          # no para: sigue generando
    python streaming/dashboard/simular_stream.py --no-reset      # agrega sin borrar lo que haya

Parar: Ctrl+C.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[2]
FUENTE = RAIZ / "streaming" / "ejemplo_predicciones.jsonl"
SALIDA = RAIZ / "data" / "streaming" / "predicciones.jsonl"


def cargar_ejemplos() -> list[dict]:
    if not FUENTE.exists():
        raise SystemExit(f"No encuentro el archivo de ejemplo: {FUENTE}")
    filas = []
    for linea in FUENTE.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea:
            filas.append(json.loads(linea))
    if not filas:
        raise SystemExit("El archivo de ejemplo está vacío.")
    return filas


def main(delay: float, reset: bool, loop: bool, limite: int | None) -> None:
    ejemplos = cargar_ejemplos()
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    if reset and SALIDA.exists():
        SALIDA.unlink()
    base_id = max(int(e.get("event_id", 0)) for e in ejemplos) + 1

    print(f"Escribiendo en {SALIDA} cada {delay}s. Abre el dashboard y míralo subir. (Ctrl+C para parar)")
    enviados, vuelta = 0, 0
    with SALIDA.open("a", encoding="utf-8") as f:
        while True:
            for ejemplo in ejemplos:
                ev = dict(ejemplo)
                # Timestamps al momento, para que "última predicción" se mueva.
                ahora = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
                ev["event_ts"] = ahora
                ev["pred_ts"] = ahora
                # En las vueltas siguientes, ids nuevos para que el conteo siga creciendo
                # (el dashboard descarta event_id repetidos).
                if vuelta:
                    ev["event_id"] = base_id + vuelta * len(ejemplos) + enviados
                f.write(json.dumps(ev, ensure_ascii=False) + "\n")
                f.flush()
                enviados += 1
                if enviados % 25 == 0:
                    print(f"  {enviados} predicciones escritas")
                if limite and enviados >= limite:
                    print(f"Listo: {enviados} predicciones.")
                    return
                time.sleep(delay)
            vuelta += 1
            if not loop:
                print(f"Listo: {enviados} predicciones (una pasada).")
                return


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--delay", type=float, default=0.8, help="segundos entre predicciones")
    ap.add_argument("--no-reset", dest="reset", action="store_false",
                    help="no borrar el archivo antes de empezar (por defecto sí se borra)")
    ap.add_argument("--loop", action="store_true", help="seguir generando sin parar")
    ap.add_argument("--limit", type=int, default=None, help="máximo de predicciones a escribir")
    a = ap.parse_args()
    try:
        main(a.delay, a.reset, a.loop, a.limit)
    except KeyboardInterrupt:
        print("\nSimulador detenido.")

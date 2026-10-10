"""Genera docs/muestra_referencia.parquet: una muestra pequeña de un staging VÁLIDO
para que Lauren pruebe GX y Carlos compare su salida sin esperar a nadie.

    python scripts/generar_muestra_referencia.py --staging data/staging --n 200
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from etl.esquema import ARCHIVOS, validar_esquema  # noqa: E402
from etl.staging import escribir_parquet, leer_parquet  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--staging", default="data/staging")
p.add_argument("--n", type=int, default=200)
a = p.parse_args()

df = leer_parquet(Path(a.staging) / ARCHIVOS["staging_db"])
# Se fuerza a incluir casos especiales (sin colegio, NSE -1, "No informa") si existen
especiales = df[(df["cole_codigo_icfes"] == -1) | (df["estu_nse_individual"] == -1)
                | (df["fami_tieneinternet"] == "No informa")].head(20)
resto = df.drop(especiales.index).sample(n=min(a.n - len(especiales), len(df) - len(especiales)), random_state=42)
muestra = (__import__("pandas").concat([especiales, resto]).sort_values("estu_consecutivo")
           .reset_index(drop=True))
validar_esquema(muestra, "muestra_referencia")
escribir_parquet(muestra, "docs/muestra_referencia.parquet")
print(f"Muestra de {len(muestra)} filas escrita en docs/muestra_referencia.parquet")

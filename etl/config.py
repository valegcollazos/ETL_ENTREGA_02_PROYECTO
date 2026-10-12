from __future__ import annotations

import os

from sqlalchemy import create_engine

try:  
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
from sqlalchemy.engine import URL, Engine


def _requerida(nombre: str) -> str:
    valor = os.getenv(nombre)
    if not valor:
        raise RuntimeError(f"Falta la variable de entorno {nombre}. Revisa tu archivo .env.")
    return valor


def _engine(base: str) -> Engine:
    url = URL.create(
        "mysql+pymysql",
        username=_requerida("MYSQL_USER"),
        password=_requerida("MYSQL_PASSWORD"),
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        database=base,
        query={"charset": "utf8mb4"},
    )
    return create_engine(url, pool_pre_ping=True)


def get_engine_raw() -> Engine:
    return _engine(os.getenv("RAW_DB_NAME", "saber11"))


def get_engine_dw() -> Engine:
    return _engine(os.getenv("DW_DB_NAME", "saber11_dw_e2"))

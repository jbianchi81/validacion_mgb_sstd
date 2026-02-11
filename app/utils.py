import json
import logging
logger = logging.getLogger(__name__)
import sys
import psycopg
from psycopg import sql
from typing import List

def loadConfig(config_path : str) -> dict:
    try:
        with open(config_path,"r",encoding="utf-8") as f:
            config = json.load(f)
    except FileNotFoundError:
        logger.error(f"Error: no se encontró el archivo de configuración: {config_path}", file=sys.stderr)
        sys.exit(1)

    except json.JSONDecodeError as e:
        logger.error(f"Error: JSON inválido en {config_path}: {e}", file=sys.stderr)
        sys.exit(2)

    except Exception as e:
        logger.error(f"Error al inentar leer {config_path}: {e}", file=sys.stderr)
        sys.exit(3) 

    if "base_url" not in config:
        raise ValueError("Falta base_url en config")

    return config

def execStmt(dsn, stmt : str, params : tuple=()):
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(stmt),
                params
            )
            return cur.fetchone()[0]

def execStmtMany(dsn, stmt : str, rows : List[tuple]):
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                sql.SQL(stmt),
                rows
            )
            return cur.rowcount # [row[0] for row in cur.fetchall()]

def execStmtFetchAll(dsn, stmt : str, params : tuple=()):
    with psycopg.connect(dsn) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                sql.SQL(stmt),
                params
            )
            return cur.fetchall()


import psycopg
from psycopg import sql
import logging

from .utils import loadConfig

logger = logging.getLogger(__name__)

config_path = "config/config.json"

config = loadConfig(config_path)

def createDb():
    with psycopg.connect(config["admin_dsn"], autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (config["db_name"],)
            )
            exists = cur.fetchone() is not None

            if not exists:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}")
                    .format(sql.Identifier(config["db_name"]))
                )
                logger.info(f"Database {config["db_name"]} creada")
            else:
                logger.info(f"Database {config["db_name"]} ya existe")

def createTables():
    # DB_DSN = f"dbname={config["db_name"]} user={config["database"]["user"]} password={config["database"]["password"]} host={config["database"]["host"]} port={config["database"]["port"]}"

    with psycopg.connect(config["user_dsn"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE EXTENSION postgis")
            )
            with open("schema.sql", "r", encoding="utf-8") as f:
                cur.execute(f.read())

        conn.commit()

def bootstrapDb():
    createDb()
    createTables()

if __name__ == '__main__':
    bootstrapDb()
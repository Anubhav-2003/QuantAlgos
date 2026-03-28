from __future__ import annotations

import getpass
import os
from pathlib import Path
from typing import Iterable

import argparse
import psycopg2
from psycopg2 import sql


def build_db_config(
    dbname: str | None = None,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
) -> dict[str, object]:
    return {
        "dbname": dbname or os.getenv("QUANT_DB_NAME", "quant_db"),
        "user": user or os.getenv("QUANT_DB_USER", getpass.getuser()),
        "password": password if password is not None else os.getenv("QUANT_DB_PASSWORD"),
        "host": host if host is not None else os.getenv("QUANT_DB_HOST", ""),
        "port": port if port is not None else int(os.getenv("QUANT_DB_PORT", "5432")),
    }


DEFAULT_DB_CONFIG = build_db_config()

SCHEMA_FILES = [
    Path(__file__).resolve().parent.parent / "sql" / "01_tickers_metadata.sql",
    Path(__file__).resolve().parent.parent / "sql" / "02_daily_prices.sql",
    Path(__file__).resolve().parent.parent / "sql" / "03_fundamentals.sql",
]


def create_database_if_missing(admin_config: dict[str, object], database_name: str) -> None:
    check_config = dict(admin_config)
    check_config["dbname"] = "postgres"
    conn = psycopg2.connect(**check_config)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database_name,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(database_name),
                        sql.Identifier(str(admin_config["user"])),
                    )
                )
                print(f"Created database: {database_name}")
            else:
                print(f"Database already exists: {database_name}")
    finally:
        conn.close()


def execute_schema_files(db_config: dict[str, object], schema_files: Iterable[Path]) -> None:
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            for schema_file in schema_files:
                with schema_file.open("r", encoding="utf-8") as handle:
                    cur.execute(handle.read())
                conn.commit()
                print(f"Executed: {schema_file.name}")


def initialize_database(db_config: dict[str, object] = DEFAULT_DB_CONFIG) -> None:
    create_database_if_missing(db_config, str(db_config["dbname"]))
    execute_schema_files(db_config, SCHEMA_FILES)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create the database and execute schema files.")
    parser.add_argument("--dbname", default=DEFAULT_DB_CONFIG["dbname"])
    parser.add_argument("--user", default=DEFAULT_DB_CONFIG["user"])
    parser.add_argument("--password", default=DEFAULT_DB_CONFIG["password"])
    parser.add_argument("--host", default=DEFAULT_DB_CONFIG["host"])
    parser.add_argument("--port", type=int, default=DEFAULT_DB_CONFIG["port"])
    args = parser.parse_args()

    db_config = build_db_config(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
        port=args.port,
    )

    initialize_database(db_config)


if __name__ == "__main__":
    main()
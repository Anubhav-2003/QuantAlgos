from __future__ import annotations

from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from init_database import initialize_database


DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "IWV_holdings25.csv"
DEFAULT_DB_CONFIG = {
    "dbname": "quant_db",
    "user": "quant_user",
    "password": "Lkjhg@127",
    "host": "localhost",
    "port": 5432,
}
DEFAULT_SNAPSHOT_DATE = "2025-06-18"


def load_iwv_universe(filepath: str | Path = DEFAULT_CSV_PATH) -> pd.DataFrame:
    """Parse IWV_holdings25.csv into a clean equity universe DataFrame."""
    df = pd.read_csv(filepath, skiprows=9, header=0)

    # Keep equity holdings only and drop non-standard tickers.
    df = df[df["Asset Class"] == "Equity"].copy()
    df = df[df["Ticker"].notna()]
    df = df[df["Ticker"].str.fullmatch(r"[A-Z]{1,5}", na=False)]

    universe = df[["Ticker", "Name", "Sector", "Exchange"]].copy()
    universe.columns = ["ticker", "company_name", "sector", "exchange"]
    universe = universe.drop_duplicates(subset="ticker").reset_index(drop=True)

    print(f"Universe loaded: {len(universe)} clean equity tickers")
    print(f"Sectors: {universe['sector'].nunique()} GICS sectors")
    return universe


def insert_universe_to_db(
    universe: pd.DataFrame,
    db_config: dict[str, object] = DEFAULT_DB_CONFIG,
    snapshot_date: str = DEFAULT_SNAPSHOT_DATE,
) -> None:
    """Insert the cleaned universe into an existing tickers_metadata table."""
    rows = [
        (row.ticker, row.company_name, row.sector, row.exchange, "FIXED_2025", snapshot_date)
        for row in universe.itertuples(index=False)
    ]

    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    execute_values(
        cur,
        """
        INSERT INTO tickers_metadata
            (ticker, company_name, sector, exchange, universe_type, snapshot_date)
        VALUES %s
        ON CONFLICT (ticker) DO NOTHING
        """,
        rows,
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {len(rows)} tickers into tickers_metadata")


def main() -> None:
    initialize_database()
    universe = load_iwv_universe()
    insert_universe_to_db(universe)


if __name__ == "__main__":
    main()
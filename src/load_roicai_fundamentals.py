from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import re
import sys
import threading
import time
from datetime import date
from io import StringIO
from typing import Iterable, Sequence
from urllib.parse import quote

import cloudscraper
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from init_database import build_db_config


LOGGER = logging.getLogger(__name__)

DEFAULT_DB_CONFIG = build_db_config()
DEFAULT_BASE_URL = "https://www.roic.ai/quote/{ticker}/financials"
DEFAULT_CLASSIC_URL = "https://www.roic.ai/quote/{ticker}/classic"
DEFAULT_DELAY_SECONDS = 0.25
DEFAULT_MAX_RETRIES = 3
DEFAULT_SOURCE = "ROICAI"
DEFAULT_BATCH_SIZE = 50
DEFAULT_WORKERS = 10
INSERT_PAGE_SIZE = 2_000

YEAR_COLUMN_RE = re.compile(r"^(?P<year>\d{4})\s*[Yy]$")

INCOME_ALIASES = {
    "revenue": ["sales revenue turnover", "sales services revenue", "total revenue"],
    "gross_profit": ["gross profit"],
    "net_income": ["net income gaap", "net income avail to common gaap", "income loss incl mi"],
    "eps_diluted": ["diluted eps gaap", "diluted eps from cont ops", "basic eps gaap"],
}

BALANCE_ALIASES = {
    "total_assets": ["total assets"],
    "total_equity": ["total equity", "equity before minority interest"],
    "long_term_debt": ["lt debt"],
    "shares_outstanding": ["shares outstanding"],
}

CASHFLOW_ALIASES = {
    "operating_cf": ["cash from operating activities"],
    "capex": ["capital expenditures"],
    "free_cash_flow": ["free cash flow"],
}

SCRAPER_LOCAL = threading.local()


def normalize_text(value: object) -> str:
    text = str(value).lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_year_column(column_name: object) -> int | None:
    match = YEAR_COLUMN_RE.match(str(column_name).strip())
    return int(match.group("year")) if match else None


def build_scraper() -> cloudscraper.CloudScraper:
    platform = "darwin" if sys.platform == "darwin" else "linux" if sys.platform.startswith("linux") else "windows"
    return cloudscraper.create_scraper(browser={"browser": "chrome", "platform": platform, "mobile": False})


def get_thread_scraper() -> cloudscraper.CloudScraper:
    scraper = getattr(SCRAPER_LOCAL, "scraper", None)
    if scraper is None:
        scraper = build_scraper()
        SCRAPER_LOCAL.scraper = scraper
    return scraper


def load_tickers_from_db(db_config: dict[str, object] = DEFAULT_DB_CONFIG) -> list[str]:
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM tickers_metadata ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]


def load_ticker_states(db_config: dict[str, object] = DEFAULT_DB_CONFIG) -> dict[str, tuple[int, date | None]]:
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker, COUNT(*), MAX(period_end_date)
                FROM fundamentals
                GROUP BY ticker
                """,
            )
            return {ticker: (int(count or 0), latest) for ticker, count, latest in cur.fetchall()}


def chunked(items: Sequence[str], batch_size: int) -> Iterable[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


def fetch_html(
    scraper: cloudscraper.CloudScraper,
    ticker: str,
    path: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> str:
    if max_retries <= 0:
        raise ValueError("max_retries must be greater than zero")

    safe_ticker = quote(ticker, safe="")
    url = path.format(ticker=safe_ticker)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = scraper.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
            LOGGER.warning(
                "Failed to fetch %s on attempt %d/%d: %s",
                url,
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(delay_seconds * attempt)

    raise RuntimeError(f"Failed to fetch ROIC.ai page for {ticker}") from last_error


def parse_statement_tables(html: str) -> list[pd.DataFrame]:
    tables = pd.read_html(StringIO(html), flavor="lxml")
    if len(tables) < 3:
        raise ValueError("Expected three financial tables on the ROIC.ai page")
    return tables[:3]


def prepare_statement_table(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    label_column = raw.columns[0]
    year_columns = [column for column in raw.columns[1:] if parse_year_column(column) is not None]

    if not year_columns:
        raise ValueError("No annual year columns found in ROIC.ai statement table")

    frame = raw[[label_column] + year_columns].copy()
    frame.columns = ["label"] + year_columns
    frame["label_norm"] = frame["label"].map(normalize_text)
    frame = frame.dropna(subset=["label_norm"])

    for column in year_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.set_index("label_norm")
    ordered_year_columns = sorted(year_columns, key=lambda column: parse_year_column(column) or 0)
    return frame, ordered_year_columns


def pick_value(statement: pd.DataFrame, aliases: Sequence[str], year_column: str) -> float | None:
    normalized_aliases = [normalize_text(alias) for alias in aliases]

    for alias in normalized_aliases:
        matches = statement[statement.index == alias]
        if matches.empty:
            continue

        value = matches.iloc[0][year_column]
        if pd.notna(value):
            return float(value)

    return None


def build_annual_records(ticker: str, tables: Sequence[pd.DataFrame]) -> list[dict[str, object]]:
    income_statement, balance_sheet, cash_flow = [prepare_statement_table(table) for table in tables[:3]]

    year_columns = sorted(
        set(income_statement[1]) | set(balance_sheet[1]) | set(cash_flow[1]),
        key=lambda column: parse_year_column(column) or 0,
    )

    records: list[dict[str, object]] = []
    for year_column in year_columns:
        year = parse_year_column(year_column)
        if year is None:
            continue

        record: dict[str, object] = {
            "ticker": ticker,
            "period_end_date": date(year, 12, 31),
            "revenue": pick_value(income_statement[0], INCOME_ALIASES["revenue"], year_column),
            "gross_profit": pick_value(income_statement[0], INCOME_ALIASES["gross_profit"], year_column),
            "net_income": pick_value(income_statement[0], INCOME_ALIASES["net_income"], year_column),
            "eps_diluted": pick_value(income_statement[0], INCOME_ALIASES["eps_diluted"], year_column),
            "total_assets": pick_value(balance_sheet[0], BALANCE_ALIASES["total_assets"], year_column),
            "total_equity": pick_value(balance_sheet[0], BALANCE_ALIASES["total_equity"], year_column),
            "long_term_debt": pick_value(balance_sheet[0], BALANCE_ALIASES["long_term_debt"], year_column),
            "operating_cf": pick_value(cash_flow[0], CASHFLOW_ALIASES["operating_cf"], year_column),
            "capex": pick_value(cash_flow[0], CASHFLOW_ALIASES["capex"], year_column),
            "book_value_per_share": None,
            "free_cash_flow": pick_value(cash_flow[0], CASHFLOW_ALIASES["free_cash_flow"], year_column),
            "source": DEFAULT_SOURCE,
        }

        if record["free_cash_flow"] is None:
            operating_cf = record["operating_cf"] or 0.0
            capex = record["capex"] or 0.0
            record["free_cash_flow"] = float(operating_cf) + float(capex)

        total_equity = record["total_equity"]
        shares_outstanding = pick_value(balance_sheet[0], BALANCE_ALIASES["shares_outstanding"], year_column)
        if total_equity is not None and shares_outstanding not in (None, 0):
            record["book_value_per_share"] = float(total_equity) / float(shares_outstanding)

        records.append(record)

    return records


def fetch_roicai_annual_records(
    ticker: str,
    scraper: cloudscraper.CloudScraper,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> tuple[list[dict[str, object]], str]:
    for path in (DEFAULT_BASE_URL, DEFAULT_CLASSIC_URL):
        html = fetch_html(scraper, ticker, path, max_retries=max_retries, delay_seconds=delay_seconds)
        try:
            tables = parse_statement_tables(html)
            records = build_annual_records(ticker, tables)
            if records:
                return records, path.format(ticker=quote(ticker, safe=""))
        except Exception as exc:
            LOGGER.warning("Parse failed for %s using %s: %s", ticker, path, exc)

    return [], DEFAULT_BASE_URL


def fetch_ticker_records(
    ticker: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
) -> tuple[str, list[dict[str, object]], str]:
    scraper = get_thread_scraper()
    records, source_path = fetch_roicai_annual_records(
        ticker,
        scraper=scraper,
        max_retries=max_retries,
        delay_seconds=delay_seconds,
    )
    return ticker, records, source_path


def upsert_fundamental_rows(
    cursor: psycopg2.extensions.cursor,
    rows: Sequence[dict[str, object]],
) -> None:
    if not rows:
        return

    values = [
        (
            row["ticker"],
            row["period_end_date"],
            row["revenue"],
            row["gross_profit"],
            row["net_income"],
            row["eps_diluted"],
            row["total_assets"],
            row["total_equity"],
            row["long_term_debt"],
            row["operating_cf"],
            row["capex"],
            row["book_value_per_share"],
            row["free_cash_flow"],
            row["source"],
        )
        for row in rows
    ]

    execute_values(
        cursor,
        """
        INSERT INTO fundamentals
            (ticker, period_end_date, revenue, gross_profit, net_income,
             eps_diluted, total_assets, total_equity, long_term_debt,
             operating_cf, capex, book_value_per_share, free_cash_flow, source)
        VALUES %s
        ON CONFLICT (ticker, period_end_date) DO UPDATE SET
            revenue = EXCLUDED.revenue,
            gross_profit = EXCLUDED.gross_profit,
            net_income = EXCLUDED.net_income,
            eps_diluted = EXCLUDED.eps_diluted,
            total_assets = EXCLUDED.total_assets,
            total_equity = EXCLUDED.total_equity,
            long_term_debt = EXCLUDED.long_term_debt,
            operating_cf = EXCLUDED.operating_cf,
            capex = EXCLUDED.capex,
            book_value_per_share = EXCLUDED.book_value_per_share,
            free_cash_flow = EXCLUDED.free_cash_flow,
            source = EXCLUDED.source
        """,
        values,
        page_size=INSERT_PAGE_SIZE,
    )


def load_fundamentals(
    db_config: dict[str, object] = DEFAULT_DB_CONFIG,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    resume: bool = False,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if workers <= 0:
        raise ValueError("workers must be greater than zero")

    all_tickers = list(tickers) if tickers is not None else load_tickers_from_db(db_config)
    selected_tickers = all_tickers[:limit] if limit is not None else all_tickers

    if not selected_tickers:
        raise ValueError("No tickers selected for fundamentals backfill")

    summary = {
        "tickers": len(selected_tickers),
        "loaded": 0,
        "skipped": 0,
        "failed": 0,
        "rows": 0,
    }

    resume_states = load_ticker_states(db_config) if resume else {}
    if resume and resume_states:
        completed_cutoff = date(date.today().year - 1, 12, 31)
        prefiltered_tickers: list[str] = []
        prefiltered_skipped = 0

        for ticker in selected_tickers:
            _, local_latest = resume_states.get(ticker, (0, None))
            if local_latest is not None and local_latest >= completed_cutoff:
                prefiltered_skipped += 1
                continue

            prefiltered_tickers.append(ticker)

        summary["skipped"] += prefiltered_skipped

        if prefiltered_skipped:
            LOGGER.info("Resume prefilter skipped %d tickers already loaded through %s", prefiltered_skipped, completed_cutoff)

        selected_tickers = prefiltered_tickers

        if not selected_tickers:
            return summary

    if resume and resume_states:
        LOGGER.info("Loaded resume state for %d tickers", len(resume_states))

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            total_batches = (len(selected_tickers) + batch_size - 1) // batch_size
            for batch_index, batch in enumerate(chunked(selected_tickers, batch_size), start=1):
                batch_results: list[tuple[str, list[dict[str, object]], str]] = []
                batch_skipped = 0
                batch_failed = 0

                with ThreadPoolExecutor(max_workers=min(workers, len(batch))) as executor:
                    future_to_ticker = {
                        executor.submit(fetch_ticker_records, ticker, max_retries, delay_seconds): ticker
                        for ticker in batch
                    }

                    for future in as_completed(future_to_ticker):
                        ticker = future_to_ticker[future]
                        try:
                            fetched_ticker, records, source_path = future.result()
                        except Exception as exc:  # pragma: no cover - network dependent
                            summary["failed"] += 1
                            batch_failed += 1
                            LOGGER.error("Failed to load fundamentals for %s: %s", ticker, exc)
                            continue

                        if not records:
                            summary["failed"] += 1
                            batch_failed += 1
                            LOGGER.warning("No annual records parsed for %s", fetched_ticker)
                            continue

                        if resume:
                            local_count, local_latest = resume_states.get(fetched_ticker, (0, None))
                            remote_count = len(records)
                            remote_latest = records[-1]["period_end_date"]
                            if local_count >= remote_count and local_latest == remote_latest:
                                summary["skipped"] += 1
                                batch_skipped += 1
                                continue

                        batch_results.append((fetched_ticker, records, source_path))

                if not batch_results:
                    LOGGER.info(
                        "Completed batch %d/%d: %d skipped, %d failed",
                        batch_index,
                        total_batches,
                        batch_skipped,
                        batch_failed,
                    )
                    continue

                batch_rows = [row for _, records, _ in batch_results for row in records]

                try:
                    upsert_fundamental_rows(cursor, batch_rows)
                    conn.commit()
                except Exception as exc:  # pragma: no cover - database dependent
                    conn.rollback()
                    summary["failed"] += len(batch_results)
                    LOGGER.error("Failed to commit batch %d/%d: %s", batch_index, total_batches, exc)
                    continue

                summary["loaded"] += len(batch_results)
                summary["rows"] += len(batch_rows)
                LOGGER.info(
                    "Committed batch %d/%d: %d loaded, %d skipped, %d failed, %d rows",
                    batch_index,
                    total_batches,
                    len(batch_results),
                    batch_skipped,
                    batch_failed,
                    len(batch_rows),
                )

    return summary


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill annual fundamentals from ROIC.ai.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on tickers to load.")
    parser.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker list to backfill instead of the full universe.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Tickers per processing batch.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="Concurrent fetch workers per batch.",
    )
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--resume", action="store_true", help="Skip tickers already loaded through the latest annual row.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    )
    return parser


def parse_ticker_list(tickers: str | None) -> list[str] | None:
    if not tickers:
        return None

    parsed = [ticker.strip().upper() for ticker in tickers.split(",") if ticker.strip()]
    return parsed or None


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    requested_tickers = parse_ticker_list(args.tickers)
    selected_tickers = requested_tickers if requested_tickers is not None else None

    summary = load_fundamentals(
        tickers=selected_tickers,
        limit=args.limit,
        batch_size=args.batch_size,
        workers=args.workers,
        max_retries=args.max_retries,
        delay_seconds=args.delay_seconds,
        resume=args.resume,
    )

    LOGGER.info(
        "Completed annual fundamentals backfill: %d tickers, %d loaded, %d skipped, %d failed, %d rows",
        summary["tickers"],
        summary["loaded"],
        summary["skipped"],
        summary["failed"],
        summary["rows"],
    )


if __name__ == "__main__":
    main()
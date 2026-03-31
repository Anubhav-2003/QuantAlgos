from __future__ import annotations

import argparse
import logging
import time
from datetime import date, datetime, timedelta
from typing import Iterable, Sequence

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from init_database import build_db_config

try:
    import yfinance as yf
except ImportError as exc:  # pragma: no cover - handled at runtime
    yf = None
    YFINANCE_IMPORT_ERROR = exc
else:
    YFINANCE_IMPORT_ERROR = None


LOGGER = logging.getLogger(__name__)

DEFAULT_DB_CONFIG = build_db_config()
DEFAULT_START_DATE = "2010-01-01"
DEFAULT_END_DATE = date.today().isoformat()
DEFAULT_BATCH_SIZE = 100
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 5.0
INSERT_PAGE_SIZE = 5_000

PRICE_COLUMN_RENAMES = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adjusted_close",
    "Volume": "volume",
}

PRICE_OUTPUT_COLUMNS = [
    "date",
    "ticker",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
]


def load_tickers_from_db(db_config: dict[str, object] = DEFAULT_DB_CONFIG) -> list[str]:
    """Load all universe tickers from tickers_metadata."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT ticker FROM tickers_metadata ORDER BY ticker")
            return [row[0] for row in cur.fetchall()]


def get_latest_loaded_date(db_config: dict[str, object] = DEFAULT_DB_CONFIG) -> date | None:
    """Return the most recent loaded daily_prices date, or None if the table is empty."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM daily_prices")
            row = cur.fetchone()
            return row[0] if row else None


def parse_ticker_list(tickers: str | None) -> list[str] | None:
    if not tickers:
        return None
    parsed = [ticker.strip().upper() for ticker in tickers.split(",") if ticker.strip()]
    return parsed or None


def filter_tickers(
    all_tickers: Sequence[str],
    requested_tickers: Sequence[str] | None = None,
    limit: int | None = None,
) -> list[str]:
    selected = list(all_tickers)

    if requested_tickers:
        requested = {ticker.upper() for ticker in requested_tickers}
        selected = [ticker for ticker in selected if ticker.upper() in requested]

    if limit is not None:
        selected = selected[:limit]

    return selected


def chunked(items: Sequence[str], batch_size: int) -> Iterable[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    for start in range(0, len(items), batch_size):
        yield list(items[start : start + batch_size])


def ensure_yfinance_available() -> None:
    if yf is None:
        raise RuntimeError(
            "yfinance is required for price backfill. Install dependencies first."
        ) from YFINANCE_IMPORT_ERROR


def download_price_batch(
    tickers: Sequence[str],
    start_date: str,
    end_date: str,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
) -> pd.DataFrame:
    """Download one batch of prices from yfinance with retry handling."""
    ensure_yfinance_available()

    if max_retries <= 0:
        raise ValueError("max_retries must be greater than zero")

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            frame = yf.download(  # type: ignore[union-attr]
                list(tickers),
                start=start_date,
                end=end_date,
                auto_adjust=False,
                actions=False,
                progress=False,
                group_by="column",
                threads=True,
            )
            if frame.empty:
                raise RuntimeError("yfinance returned an empty frame")
            return frame
        except Exception as exc:  # pragma: no cover - network dependent
            last_error = exc
            LOGGER.warning(
                "yfinance download failed for batch of %d tickers on attempt %d/%d: %s",
                len(tickers),
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(retry_delay_seconds * attempt)

    raise RuntimeError("yfinance download failed after retries") from last_error


def extract_ticker_frame(raw: pd.DataFrame, ticker: str) -> pd.DataFrame | None:
    """Return a single-ticker frame whether yfinance returned flat or MultiIndex columns."""
    if raw.empty:
        return None

    if isinstance(raw.columns, pd.MultiIndex):
        for level in (0, 1):
            try:
                level_values = raw.columns.get_level_values(level)
            except IndexError:
                continue

            if ticker in set(level_values):
                try:
                    return raw.xs(ticker, axis=1, level=level).copy()
                except KeyError:
                    continue
        return None

    return raw.copy()


def normalize_ticker_frame(frame: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """Convert a per-ticker yfinance frame into the daily_prices schema."""
    if frame.empty:
        return pd.DataFrame(columns=PRICE_OUTPUT_COLUMNS)

    normalized = frame.reset_index()
    index_column = normalized.columns[0]
    if index_column != "date":
        normalized = normalized.rename(columns={index_column: "date"})

    normalized = normalized.rename(columns=PRICE_COLUMN_RENAMES)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.date

    for column in ("open", "high", "low", "close", "adjusted_close", "volume"):
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    if "adjusted_close" not in normalized.columns:
        normalized["adjusted_close"] = pd.NA

    normalized["adjusted_close"] = normalized["adjusted_close"].fillna(normalized["close"])
    normalized["ticker"] = ticker
    normalized = normalized.dropna(subset=["date", "close"])
    normalized = normalized.sort_values("date")

    for column in PRICE_OUTPUT_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    return normalized[PRICE_OUTPUT_COLUMNS]


def normalize_price_batch(raw: pd.DataFrame, requested_tickers: Sequence[str]) -> pd.DataFrame:
    """Flatten the yfinance batch response into long-form rows."""
    if raw.empty:
        return pd.DataFrame(columns=PRICE_OUTPUT_COLUMNS)

    if not isinstance(raw.columns, pd.MultiIndex) and len(requested_tickers) > 1:
        raise RuntimeError("Unexpected flat yfinance response for a multi-ticker batch")

    frames: list[pd.DataFrame] = []
    for ticker in requested_tickers:
        ticker_frame = extract_ticker_frame(raw, ticker)
        if ticker_frame is None or ticker_frame.empty:
            continue
        frames.append(normalize_ticker_frame(ticker_frame, ticker))

    if not frames:
        return pd.DataFrame(columns=PRICE_OUTPUT_COLUMNS)

    normalized = pd.concat(frames, ignore_index=True)
    normalized = normalized.dropna(subset=["date", "ticker", "close"])
    return normalized[PRICE_OUTPUT_COLUMNS]


def build_insert_rows(prices: pd.DataFrame) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for row in prices.itertuples(index=False):
        if pd.isna(row.close):
            continue

        rows.append(
            (
                row.date,
                row.ticker,
                None if pd.isna(row.open) else float(row.open),
                None if pd.isna(row.high) else float(row.high),
                None if pd.isna(row.low) else float(row.low),
                float(row.close),
                float(row.adjusted_close) if not pd.isna(row.adjusted_close) else float(row.close),
                None if pd.isna(row.volume) else int(round(float(row.volume))),
            )
        )

    return rows


def upsert_price_rows(
    cursor: psycopg2.extensions.cursor,
    rows: Sequence[tuple[object, ...]],
) -> None:
    if not rows:
        return

    execute_values(
        cursor,
        """
        INSERT INTO daily_prices
            (date, ticker, open, high, low, close, adjusted_close, volume)
        VALUES %s
        ON CONFLICT (date, ticker) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            adjusted_close = EXCLUDED.adjusted_close,
            volume = EXCLUDED.volume
        """,
        rows,
        page_size=INSERT_PAGE_SIZE,
    )


def load_daily_prices(
    db_config: dict[str, object] = DEFAULT_DB_CONFIG,
    start_date: str = DEFAULT_START_DATE,
    end_date: str = DEFAULT_END_DATE,
    batch_size: int = DEFAULT_BATCH_SIZE,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
    tickers: Sequence[str] | None = None,
    limit: int | None = None,
    resume: bool = False,
) -> dict[str, int]:
    """Backfill daily OHLCV rows into daily_prices."""
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")

    if limit is not None and limit <= 0:
        raise ValueError("limit must be greater than zero when provided")

    ensure_yfinance_available()

    all_tickers = list(tickers) if tickers is not None else load_tickers_from_db(db_config)
    selected_tickers = filter_tickers(all_tickers, requested_tickers=None, limit=limit)

    if not selected_tickers:
        raise ValueError("No tickers selected for price backfill")

    start_dt = datetime.fromisoformat(start_date).date()
    end_dt = datetime.fromisoformat(end_date).date()
    if end_dt < start_dt:
        raise ValueError("end_date must be on or after start_date")

    if resume:
        latest_loaded_date = get_latest_loaded_date(db_config)
        if latest_loaded_date is not None:
            resume_start = latest_loaded_date + timedelta(days=1)
            if resume_start > start_dt:
                LOGGER.info(
                    "Resume mode active: moving start date from %s to %s based on daily_prices",
                    start_dt,
                    resume_start,
                )
                start_dt = resume_start

    if start_dt > end_dt:
        LOGGER.info(
            "No price backfill needed: effective start date %s is after end date %s",
            start_dt,
            end_dt,
        )
        return {
            "tickers": len(selected_tickers),
            "batches": 0,
            "failed_batches": 0,
            "rows": 0,
            "empty_batches": 0,
        }

    # yfinance's end parameter is exclusive, so add one day to include the requested end date.
    yfinance_end_date = (end_dt + timedelta(days=1)).isoformat()

    total_batches = (len(selected_tickers) + batch_size - 1) // batch_size
    summary = {
        "tickers": len(selected_tickers),
        "batches": 0,
        "failed_batches": 0,
        "rows": 0,
        "empty_batches": 0,
    }

    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cursor:
            for batch_number, batch in enumerate(chunked(selected_tickers, batch_size), start=1):
                try:
                    LOGGER.info(
                        "Downloading batch %d/%d (%d tickers)",
                        batch_number,
                        total_batches,
                        len(batch),
                    )

                    raw = download_price_batch(
                        batch,
                        start_date=start_dt.isoformat(),
                        end_date=yfinance_end_date,
                        max_retries=max_retries,
                        retry_delay_seconds=retry_delay_seconds,
                    )
                    prices = normalize_price_batch(raw, batch)

                    if prices.empty:
                        summary["empty_batches"] += 1
                        LOGGER.warning("No usable price rows returned for batch %d", batch_number)
                        continue

                    rows = build_insert_rows(prices)
                    if not rows:
                        summary["empty_batches"] += 1
                        LOGGER.warning("No insertable rows produced for batch %d", batch_number)
                        continue

                    upsert_price_rows(cursor, rows)
                    conn.commit()

                    summary["batches"] += 1
                    summary["rows"] += len(rows)
                    LOGGER.info("Upserted %d rows for batch %d", len(rows), batch_number)
                except Exception as exc:  # pragma: no cover - network and DB dependent
                    conn.rollback()
                    summary["failed_batches"] += 1
                    LOGGER.error("Skipping batch %d after failure: %s", batch_number, exc)
                    continue

    return summary


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill daily OHLC prices from yfinance.")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--retry-delay-seconds", type=float, default=DEFAULT_RETRY_DELAY_SECONDS)
    parser.add_argument("--resume", action="store_true", help="Resume from the latest loaded date.")
    parser.add_argument("--limit", type=int, default=None, help="Optional cap on tickers to load.")
    parser.add_argument(
        "--tickers",
        default=None,
        help="Optional comma-separated ticker list to backfill instead of the full universe.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
    )
    return parser


def main() -> None:
    parser = build_argument_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    requested_tickers = parse_ticker_list(args.tickers)

    all_tickers = load_tickers_from_db()
    tickers_to_load = filter_tickers(all_tickers, requested_tickers=requested_tickers, limit=args.limit)

    summary = load_daily_prices(
        start_date=args.start_date,
        end_date=args.end_date,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        retry_delay_seconds=args.retry_delay_seconds,
        tickers=tickers_to_load,
        resume=args.resume,
    )

    LOGGER.info(
        "Completed price backfill: %d tickers, %d batches, %d rows, %d failed batches, %d empty batches",
        summary["tickers"],
        summary["batches"],
        summary["rows"],
        summary["failed_batches"],
        summary["empty_batches"],
    )


if __name__ == "__main__":
    main()
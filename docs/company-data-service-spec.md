# Company Data Service Spec

## Purpose

This service is a read-only FastAPI microservice that exposes company data from the local PostgreSQL database used by QuantAlgos. It exists so downstream agents can stop scraping ROIC.ai directly and instead query the curated database through a stable contract.

The service is intentionally internal and local-first. In v1 it runs on `localhost:5454` with no auth and no write endpoints.

## Why REST Instead Of GraphQL

REST is the better fit for this first version because the data model is bounded and the main requirement is a flexible, batch-friendly query contract rather than arbitrary graph traversal. A compact JSON query DSL gives us most of the useful GraphQL behavior without adding schema stitching, resolver complexity, or a second query language.

## Data Sources

The service reads from three existing tables only:

- `tickers_metadata`
- `daily_prices`
- `fundamentals`

These tables already capture the fixed universe, OHLCV price history, and annual fundamentals that were loaded in earlier phases.

## Non-Goals

- No write API.
- No raw SQL passthrough.
- No authentication in v1.
- No third-party scraping.
- No arbitrary joins beyond the combinations defined by the contract.

## Base URL

The default base URL is:

`http://localhost:5454`

Versioned endpoints live under `/v1`.

## Service Rules

- Read-only.
- Internal-only.
- Parameterized SQL only.
- Allowlisted columns only.
- Hard limits on request size and row counts.
- Batch requests are supported through one HTTP call.

## Health And Readiness

### `GET /healthz`

Returns process liveness.

Example response:

```json
{ "status": "ok" }
```

### `GET /readyz`

Checks that the database pool can execute a simple query.

Example response:

```json
{
  "status": "ready",
  "database": "ok"
}
```

## Capabilities Discovery

### `GET /v1/capabilities`

Returns the supported entities, fields, operators, and limits so agents can discover the contract dynamically.

Key sections:

- `entities`: allowed entity names and column sets.
- `operators`: supported filter operators.
- `limits`: maximum query limits and batch sizes.
- `defaults`: default select lists and default row limits.

## Supported Entities

### `tickers_metadata`

Company metadata loaded from the IWV universe CSV.

Fields:

- `ticker`
- `company_name`
- `sector`
- `exchange`
- `universe_type`
- `snapshot_date`
- `created_at`

### `daily_prices`

Daily OHLCV history from yfinance.

Fields:

- `date`
- `ticker`
- `open`
- `high`
- `low`
- `close`
- `adjusted_close`
- `volume`

### `fundamentals`

Annual fundamentals loaded from ROIC.ai.

Fields:

- `ticker`
- `period_end_date`
- `revenue`
- `gross_profit`
- `net_income`
- `eps_diluted`
- `total_assets`
- `total_equity`
- `long_term_debt`
- `operating_cf`
- `capex`
- `book_value_per_share`
- `free_cash_flow`
- `source`
- `created_at`

### `company_bundle`

Composite entity that combines metadata plus optional latest or historical slices from the other entities for one or more tickers.

## Fundamentals Guide

The external consumer should treat fundamentals as the main valuation payload. In this system, fundamentals are annual rows loaded from ROIC.ai and stored in the `fundamentals` table. They are not quarterly rows in v1.

### Fundamentals Field Semantics

| Field | Meaning | Consumer Guidance |
| --- | --- | --- |
| `ticker` | Equity ticker symbol | Join key for every request pattern |
| `period_end_date` | Fiscal year-end date for the annual row | Use this as the row date in downstream models |
| `revenue` | Annual revenue / sales | Same numeric sign and units as the source row |
| `gross_profit` | Annual gross profit | May be null if the source page lacks it |
| `net_income` | Annual net income | Use as the core profitability field |
| `eps_diluted` | Diluted EPS | For valuation screens, ignore null or non-positive values |
| `total_assets` | Balance-sheet total assets | Useful for size and leverage features |
| `total_equity` | Balance-sheet equity | Used with share count to derive book value per share |
| `long_term_debt` | Long-term debt | Can be null if the source page does not expose it |
| `operating_cf` | Operating cash flow | Same source sign convention as the loader |
| `capex` | Capital expenditures | Usually negative in the source; preserve the loaded value |
| `book_value_per_share` | Derived equity-per-share metric | Derived when a reliable share count is available |
| `free_cash_flow` | Derived cash generation metric | Derived as operating cash flow plus capex |
| `source` | Source label | Currently `ROICAI` for every loaded row |
| `created_at` | Database insertion timestamp | Operational metadata only |

### Fundamental Semantics That Matter

- Fundamentals are annual only in v1. If you need quarterly data, this service does not expose it.
- `period_end_date` is the relaxed availability proxy used by the downstream strategy code. It is not a true SEC filing date.
- When a consumer wants the latest annual fundamentals row, the canonical pattern is to sort `period_end_date` descending and take `limit = 1`.
- When a consumer wants a history window, use `start_date` and `end_date` on the `fundamentals` entity.
- When a consumer wants many tickers with the same slice, use `ticker in (...)` in one query.
- When a consumer wants multiple independent ticker requests or per-ticker error isolation, use `/v1/batch`.
- Numeric values come from PostgreSQL `NUMERIC` columns. Treat response values as decimal-safe data and do not assume binary floats.

### Canonical Fundamentals Field Set

Use this full field list whenever you want the complete annual fundamentals row:

```json
[
  "ticker",
  "period_end_date",
  "revenue",
  "gross_profit",
  "net_income",
  "eps_diluted",
  "total_assets",
  "total_equity",
  "long_term_debt",
  "operating_cf",
  "capex",
  "book_value_per_share",
  "free_cash_flow",
  "source",
  "created_at"
]
```

Use this same list in both single-ticker and batch fundamentals requests unless you intentionally want a smaller payload.

## How To Fetch Fundamentals

### 1. One Ticker, Latest Annual Fundamentals

Use this when an external service needs the most recent annual row for a single company.

Recommended endpoint:

`GET /v1/companies/{ticker}`

Example:

```bash
curl -s http://localhost:5454/v1/companies/AAPL
```

This returns a company bundle. The latest annual fundamentals row is in `data[0].latest_fundamentals`.

Example response shape:

```json
{
  "entity": "company_bundle",
  "row_count": 1,
  "data": [
    {
      "ticker": "AAPL",
      "metadata": { "ticker": "AAPL", "sector": "Information Technology" },
      "latest_price": { "date": "2025-12-31", "adjusted_close": "271.6058" },
      "latest_fundamentals": {
        "ticker": "AAPL",
        "period_end_date": "2025-12-31",
        "revenue": "416161.00",
        "gross_profit": "195201.00",
        "net_income": "112010.00",
        "eps_diluted": "7.4600",
        "total_assets": "359241.00",
        "total_equity": "73733.00",
        "long_term_debt": "78328.00",
        "operating_cf": "111482.00",
        "capex": "-12715.00",
        "book_value_per_share": "4.9911",
        "free_cash_flow": "98767.00",
        "source": "ROICAI"
      },
      "price_history": [],
      "fundamentals_history": [],
      "warnings": []
    }
  ],
  "warnings": []
}
```

If the caller only wants fundamentals and not the bundle, the same result can be fetched directly with `POST /v1/query`:

```json
{
  "entity": "fundamentals",
  "fields": [
    "ticker",
    "period_end_date",
    "revenue",
    "gross_profit",
    "net_income",
    "eps_diluted",
    "total_assets",
    "total_equity",
    "long_term_debt",
    "operating_cf",
    "capex",
    "book_value_per_share",
    "free_cash_flow",
    "source",
    "created_at"
  ],
  "filters": [
    { "field": "ticker", "op": "eq", "value": "AAPL" }
  ],
  "sort": [
    { "field": "period_end_date", "direction": "desc" }
  ],
  "limit": 1
}
```

This returns one annual row in `data[0]` and is the most efficient raw-table request for a single latest fundamentals point.

The company bundle version intentionally focuses on the business fundamentals fields used by downstream models. If you need operational metadata such as `created_at`, query the raw `fundamentals` entity directly.

### 2. One Ticker, Full Annual History

Use this when an external service needs every loaded annual row for one ticker.

Example request:

```json
{
  "entity": "fundamentals",
  "fields": [
    "ticker",
    "period_end_date",
    "revenue",
    "gross_profit",
    "net_income",
    "eps_diluted",
    "total_assets",
    "total_equity",
    "long_term_debt",
    "operating_cf",
    "capex",
    "book_value_per_share",
    "free_cash_flow",
    "source",
    "created_at"
  ],
  "filters": [
    { "field": "ticker", "op": "eq", "value": "AAPL" }
  ],
  "sort": [
    { "field": "period_end_date", "direction": "asc" }
  ]
}
```

This returns the full annual series for the ticker. For AAPL that is currently 2011 through 2025 in the loaded database.

### 3. One Ticker, Date-Bounded Fundamentals History

Use this when the consumer only wants a sub-window of annual fundamentals, for example a backtest feature window.

Example request:

```json
{
  "entity": "fundamentals",
  "fields": [
    "ticker",
    "period_end_date",
    "revenue",
    "gross_profit",
    "net_income",
    "eps_diluted",
    "total_assets",
    "total_equity",
    "long_term_debt",
    "operating_cf",
    "capex",
    "book_value_per_share",
    "free_cash_flow",
    "source",
    "created_at"
  ],
  "filters": [
    { "field": "ticker", "op": "eq", "value": "AAPL" }
  ],
  "start_date": "2020-12-31",
  "end_date": "2025-12-31",
  "sort": [
    { "field": "period_end_date", "direction": "asc" }
  ]
}
```

This is useful when the downstream service wants to build rolling features but does not want to fetch older annual rows.

### 4. Many Tickers, One Fundamentals Table

If you want a single consolidated table of fundamentals for many tickers, use one `POST /v1/query` with `ticker in (...)`.

Example request:

```json
{
  "entity": "fundamentals",
  "fields": [
    "ticker",
    "period_end_date",
    "revenue",
    "gross_profit",
    "net_income",
    "eps_diluted",
    "total_assets",
    "total_equity",
    "long_term_debt",
    "operating_cf",
    "capex",
    "book_value_per_share",
    "free_cash_flow",
    "source",
    "created_at"
  ],
  "filters": [
    { "field": "ticker", "op": "in", "values": ["AAPL", "MSFT", "NVDA"] },
    { "field": "period_end_date", "op": "between", "values": ["2023-12-31", "2025-12-31"] }
  ],
  "sort": [
    { "field": "ticker", "direction": "asc" },
    { "field": "period_end_date", "direction": "asc" }
  ],
  "limit": 5000
}
```

Use this when the downstream consumer wants one big response table and does not need per-ticker failure isolation.

### 5. Many Tickers, Independent Fundamentals Requests

If you want one response item per ticker, use `/v1/batch`. This is the preferred shape when each ticker can be handled independently and you want the service to return partial success instead of failing the whole call.

Example request:

```json
{
  "requests": [
    {
      "request_id": "aapl-fundamentals",
      "query": {
        "entity": "fundamentals",
        "fields": [
          "ticker",
          "period_end_date",
          "revenue",
          "gross_profit",
          "net_income",
          "eps_diluted",
          "total_assets",
          "total_equity",
          "long_term_debt",
          "operating_cf",
          "capex",
          "book_value_per_share",
          "free_cash_flow",
          "source",
          "created_at"
        ],
        "filters": [
          { "field": "ticker", "op": "eq", "value": "AAPL" }
        ],
        "sort": [
          { "field": "period_end_date", "direction": "desc" }
        ],
        "limit": 1
      }
    },
    {
      "request_id": "msft-fundamentals",
      "query": {
        "entity": "fundamentals",
        "fields": [
          "ticker",
          "period_end_date",
          "revenue",
          "gross_profit",
          "net_income",
          "eps_diluted",
          "total_assets",
          "total_equity",
          "long_term_debt",
          "operating_cf",
          "capex",
          "book_value_per_share",
          "free_cash_flow",
          "source",
          "created_at"
        ],
        "filters": [
          { "field": "ticker", "op": "eq", "value": "MSFT" }
        ],
        "sort": [
          { "field": "period_end_date", "direction": "desc" }
        ],
        "limit": 1
      }
    }
  ]
}
```

Batch response shape:

```json
{
  "results": [
    {
      "request_id": "aapl-fundamentals",
      "ok": true,
      "result": {
        "entity": "fundamentals",
        "row_count": 1,
        "data": [
          {
            "ticker": "AAPL",
            "period_end_date": "2025-12-31",
            "eps_diluted": "7.4600",
            "free_cash_flow": "98767.00",
            "source": "ROICAI"
          }
        ],
        "warnings": []
      },
      "error": null
    },
    {
      "request_id": "msft-fundamentals",
      "ok": true,
      "result": {
        "entity": "fundamentals",
        "row_count": 1,
        "data": [
          {
            "ticker": "MSFT",
            "period_end_date": "2025-12-31",
            "eps_diluted": "...",
            "free_cash_flow": "...",
            "source": "ROICAI"
          }
        ],
        "warnings": []
      },
      "error": null
    }
  ]
}
```

Batch behavior to remember:

- Input order is preserved.
- One bad item does not fail the whole batch.
- `request_id` is echoed back so the caller can correlate results.
- `ok: false` means the item failed at validation or execution time.

### 6. Batch Fundamentals With Mixed Request Shapes

The batch endpoint is not limited to fundamentals-only calls. It can mix metadata, price, fundamentals, and company bundle requests in the same payload.

Example request:

```json
{
  "requests": [
    {
      "request_id": "aapl-meta",
      "query": {
        "entity": "tickers_metadata",
        "fields": ["ticker", "company_name", "sector"],
        "filters": [
          { "field": "ticker", "op": "eq", "value": "AAPL" }
        ],
        "limit": 1
      }
    },
    {
      "request_id": "aapl-latest-fundamentals",
      "query": {
        "entity": "fundamentals",
        "fields": ["ticker", "period_end_date", "net_income", "eps_diluted", "free_cash_flow"],
        "filters": [
          { "field": "ticker", "op": "eq", "value": "AAPL" }
        ],
        "sort": [
          { "field": "period_end_date", "direction": "desc" }
        ],
        "limit": 1
      }
    },
    {
      "request_id": "msft-bundle",
      "query": {
        "entity": "company_bundle",
        "tickers": ["MSFT"],
        "as_of": "2025-12-31"
      }
    }
  ]
}
```

This is the best pattern when an external agent needs a small number of heterogeneous lookups in one call.

### 7. When To Use `company_bundle` Versus Raw `fundamentals`

- Use `company_bundle` when you need metadata plus latest snapshot data in one response.
- Use raw `fundamentals` when you only need annual financial rows.
- Use raw `fundamentals` plus `ticker in (...)` when you want a single long table.
- Use `/v1/batch` when you want independent failure handling or you need to mix companies, prices, and fundamentals in one call.

### 8. Practical Decision Table

| Need | Best Endpoint | Why |
| --- | --- | --- |
| Latest annual fundamentals for one ticker | `GET /v1/companies/{ticker}` or `POST /v1/query` on `fundamentals` | Fastest way to retrieve a single company snapshot |
| Full annual history for one ticker | `POST /v1/query` on `fundamentals` | One table, easy to sort and filter |
| Historical annual fundamentals for a date window | `POST /v1/query` on `fundamentals` | Simple windowing on `period_end_date` |
| Many tickers, one combined table | `POST /v1/query` with `ticker in (...)` | Best when the caller wants one dataset |
| Many independent ticker lookups | `POST /v1/batch` | Best when you want isolated successes and failures |
| Metadata plus latest price plus latest fundamentals | `GET /v1/companies/{ticker}` | Convenience bundle for one company |

## Query Contract

### `POST /v1/query`

Generic table query or company bundle query.

#### Raw Table Query Example

```json
{
  "entity": "daily_prices",
  "fields": ["ticker", "date", "close", "adjusted_close"],
  "filters": [
    { "field": "ticker", "op": "in", "values": ["AAPL", "MSFT"] },
    { "field": "date", "op": "between", "values": ["2024-01-01", "2024-12-31"] }
  ],
  "sort": [
    { "field": "ticker", "direction": "asc" },
    { "field": "date", "direction": "desc" }
  ],
  "limit": 500
}
```

#### Company Bundle Example

```json
{
  "entity": "company_bundle",
  "tickers": ["AAPL", "MSFT"],
  "as_of": "2025-12-31",
  "include": {
    "metadata": true,
    "latest_price": true,
    "latest_fundamentals": true,
    "price_history": {
      "start_date": "2025-01-01",
      "end_date": "2025-12-31",
      "limit": 252
    },
    "fundamentals_history": {
      "start_date": "2020-12-31",
      "end_date": "2025-12-31"
    }
  }
}
```

#### Query Fields

- `entity`: target entity.
- `fields`: allowlisted columns for table entities.
- `filters`: list of field/operator/value predicates.
- `sort`: order by clauses.
- `limit`: maximum returned rows.
- `offset`: row offset for pagination.
- `as_of`: upper bound for snapshot-style reads.
- `start_date` and `end_date`: date window bounds for table reads.
- `tickers`: convenience field for ticker-constrained requests.
- `include`: only used for `company_bundle`.

#### Supported Operators

- `eq`
- `ne`
- `lt`
- `lte`
- `gt`
- `gte`
- `in`
- `not_in`
- `between`
- `like`
- `ilike`
- `contains`
- `startswith`
- `endswith`
- `is_null`
- `not_null`

## Batch Contract

### `POST /v1/batch`

Accepts a list of query envelopes and returns item-level success or failure.

Example request:

```json
{
  "requests": [
    {
      "request_id": "meta-aapl",
      "query": {
        "entity": "tickers_metadata",
        "filters": [{ "field": "ticker", "op": "eq", "value": "AAPL" }]
      }
    },
    {
      "request_id": "bundle-aapl",
      "query": {
        "entity": "company_bundle",
        "tickers": ["AAPL"],
        "as_of": "2025-12-31",
        "include": {
          "metadata": true,
          "latest_price": true,
          "latest_fundamentals": true
        }
      }
    }
  ]
}
```

Batch responses preserve input order and return a per-item status flag.

### Batch Error Example

If one item is invalid, the batch still returns the other items.

```json
{
  "results": [
    {
      "request_id": "good-item",
      "ok": true,
      "result": {
        "entity": "fundamentals",
        "row_count": 1,
        "data": [
          {
            "ticker": "AAPL",
            "period_end_date": "2025-12-31",
            "revenue": "416161.00",
            "gross_profit": "195201.00",
            "net_income": "112010.00",
            "eps_diluted": "7.4600",
            "total_assets": "359241.00",
            "total_equity": "73733.00",
            "long_term_debt": "78328.00",
            "operating_cf": "111482.00",
            "capex": "-12715.00",
            "book_value_per_share": "4.9911",
            "free_cash_flow": "98767.00",
            "source": "ROICAI",
            "created_at": "2026-03-31T00:00:00"
          }
        ],
        "warnings": []
      },
      "error": null
    },
    {
      "request_id": "bad-item",
      "ok": false,
      "result": null,
      "error": {
        "code": "invalid_request",
        "message": "Unknown field for fundamentals: foo",
        "details": null
      }
    }
  ]
}
```

For malformed request bodies that fail schema validation before the batch handler runs, the API will return a normal FastAPI validation error response. In other words, batch-level validation errors are split into two classes:

- Schema errors: HTTP 422 from FastAPI.
- Per-item semantic errors: `ok = false` inside the batch response.

## Company Bundle Contract

For `entity = company_bundle`, the response returns one bundle per requested ticker.

Bundle shape:

- `ticker`
- `metadata`
- `latest_price`
- `latest_fundamentals`
- `price_history`
- `fundamentals_history`
- `warnings`

`metadata`, `latest_price`, and `latest_fundamentals` are optional objects. Historical sections are arrays of row dictionaries.

## Error Model

Errors are returned as structured JSON with an HTTP status code and a machine-readable code where possible.

Suggested error codes:

- `invalid_request`
- `unknown_entity`
- `unknown_field`
- `invalid_operator`
- `limit_exceeded`
- `not_found`
- `database_error`

## Limits

Recommended v1 limits:

- Max batch items: 50
- Max table rows per query: 5,000
- Max bundle tickers per request: 100
- History requests should always be bounded by date range or per-ticker row cap.

## Deployment Contract

The service should start on port `5454` by default and bind to `0.0.0.0` for local or internal-network usage.

Recommended startup command:

```bash
python3 -m src.company_data_service
```

## Implementation Notes

- Use `build_db_config()` from `src/init_database.py` for database configuration.
- Use a threaded PostgreSQL connection pool.
- Use `GZipMiddleware` for large batch responses.
- Add a process-time header for observability.
- Keep the database contract allowlisted and explicit.

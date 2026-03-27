CREATE TABLE fundamentals (
    fundamental_id       BIGSERIAL   PRIMARY KEY,
    ticker               VARCHAR(12) REFERENCES tickers_metadata(ticker),
    period_end_date      DATE        NOT NULL,

    revenue              NUMERIC(18,2),
    gross_profit         NUMERIC(18,2),
    net_income           NUMERIC(18,2),
    eps_diluted          NUMERIC(12,4),

    total_assets         NUMERIC(18,2),
    total_equity         NUMERIC(18,2),
    long_term_debt       NUMERIC(18,2),

    operating_cf         NUMERIC(18,2),
    capex                NUMERIC(18,2),

    book_value_per_share NUMERIC(12,4),
    free_cash_flow       NUMERIC(18,2),

    source               VARCHAR(16)  DEFAULT 'YFINANCE',
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (ticker, period_end_date)
);

CREATE INDEX IF NOT EXISTS idx_fund_ticker_date ON fundamentals(ticker, period_end_date);

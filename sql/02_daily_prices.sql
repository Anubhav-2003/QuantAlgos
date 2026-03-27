CREATE TABLE daily_prices (
    price_id        BIGSERIAL    PRIMARY KEY,
    date            DATE         NOT NULL,
    ticker          VARCHAR(12)  REFERENCES tickers_metadata(ticker),
    open            NUMERIC(14,4),
    high            NUMERIC(14,4),
    low             NUMERIC(14,4),
    close           NUMERIC(14,4)  NOT NULL,
    adjusted_close  NUMERIC(14,4)  NOT NULL,
    volume          BIGINT,
    UNIQUE (date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON daily_prices(ticker, date);
CREATE INDEX IF NOT EXISTS idx_prices_date        ON daily_prices(date);

CREATE TABLE tickers_metadata (
    ticker        VARCHAR(12) PRIMARY KEY,
    company_name  VARCHAR(255) NOT NULL,
    sector        VARCHAR(64),
    exchange      VARCHAR(32),
    universe_type VARCHAR(32) NOT NULL DEFAULT 'FIXED_2025',
    snapshot_date DATE NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_meta_sector ON tickers_metadata(sector);
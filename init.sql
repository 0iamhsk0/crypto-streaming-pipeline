-- Windowed OHLCV + anomaly aggregates written by the Spark streaming job.
-- One row per (symbol, window) pair.

CREATE TABLE IF NOT EXISTS market_windows (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(20)     NOT NULL,
    window_start    TIMESTAMP       NOT NULL,
    window_end      TIMESTAMP       NOT NULL,
    open_price      NUMERIC(20, 8)  NOT NULL,
    high_price      NUMERIC(20, 8)  NOT NULL,
    low_price       NUMERIC(20, 8)  NOT NULL,
    close_price     NUMERIC(20, 8)  NOT NULL,
    volume          NUMERIC(20, 8)  NOT NULL,
    trade_count     INTEGER         NOT NULL,
    pct_change      NUMERIC(10, 4),
    volume_zscore   NUMERIC(10, 4),
    price_zscore    NUMERIC(10, 4),
    is_anomaly      BOOLEAN         DEFAULT FALSE,
    ai_commentary   TEXT,
    inserted_at     TIMESTAMP       DEFAULT NOW(),
    UNIQUE (symbol, window_start)
);

CREATE INDEX IF NOT EXISTS idx_market_windows_symbol_time
    ON market_windows (symbol, window_start DESC);

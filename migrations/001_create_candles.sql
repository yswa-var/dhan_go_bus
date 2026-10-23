CREATE TABLE IF NOT EXISTS candles (
    security_id BIGINT NOT NULL,
    exchange_segment TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    candle_start TIMESTAMPTZ NOT NULL,

    open NUMERIC(18, 5) NOT NULL,
    high NUMERIC(18, 5) NOT NULL,
    low NUMERIC(18, 5) NOT NULL,
    close NUMERIC(18, 5) NOT NULL,
    volume BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (
        security_id,
        exchange_segment,
        timeframe,
        candle_start
    )
);

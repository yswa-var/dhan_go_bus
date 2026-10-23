package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"yswa-var/dhan_go_bus/internals/ohlc"
)

const upsertCandleSQL = `
INSERT INTO candles (
    security_id, exchange_segment, timeframe, candle_start,
    open, high, low, close, volume
) VALUES ($1, $2, $3, $4, $5::numeric, $6::numeric, $7::numeric, $8::numeric, $9)
ON CONFLICT (security_id, exchange_segment, timeframe, candle_start)
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume`

const createCandlesTableSQL = `
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
    PRIMARY KEY (security_id, exchange_segment, timeframe, candle_start)
)`

// PostgresWriter persists candles using one transaction per batch. The
// conflict update makes retries safe after a connection failure.
type PostgresWriter struct {
	pool *pgxpool.Pool
}

const loadRecentCandlesSQL = `
SELECT security_id, exchange_segment, timeframe, candle_start,
       open::text, high::text, low::text, close::text, volume
FROM candles
WHERE security_id = $1
  AND exchange_segment = $2
  AND timeframe = $3
ORDER BY candle_start DESC`

// NewPostgresWriter opens and verifies a PostgreSQL connection pool.
func NewPostgresWriter(ctx context.Context, connectionString string) (*PostgresWriter, error) {
	if connectionString == "" {
		return nil, errors.New("postgres connection string is required")
	}
	pool, err := pgxpool.New(ctx, connectionString)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &PostgresWriter{pool: pool}, nil
}

// Close releases the PostgreSQL connection pool.
func (w *PostgresWriter) Close() {
	if w != nil && w.pool != nil {
		w.pool.Close()
	}
}

// EnsureSchema applies the single table needed by the candle writer. The SQL
// migration remains available for deployments that manage schema separately.
func (w *PostgresWriter) EnsureSchema(ctx context.Context) error {
	if w == nil || w.pool == nil {
		return errors.New("postgres writer is not initialized")
	}
	if _, err := w.pool.Exec(ctx, createCandlesTableSQL); err != nil {
		return fmt.Errorf("create candles table: %w", err)
	}
	return nil
}

// WriteBatch upserts all candles atomically. Prices are sent as decimal text
// derived from fixed-point integers, avoiding an intermediate float64 value.
func (w *PostgresWriter) WriteBatch(ctx context.Context, candles []ohlc.Candle) error {
	if len(candles) == 0 {
		return nil
	}
	if w == nil || w.pool == nil {
		return errors.New("postgres writer is not initialized")
	}

	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin candle batch: %w", err)
	}

	batch := &pgx.Batch{}
	for _, candle := range candles {
		batch.Queue(upsertCandleSQL,
			candle.SecurityID,
			candle.ExchangeSegment,
			ohlc.Timeframe(candle.Interval),
			candle.Start.UTC(),
			decimalPrice(candle.Open),
			decimalPrice(candle.High),
			decimalPrice(candle.Low),
			decimalPrice(candle.Close),
			candle.Volume,
		)
	}

	results := tx.SendBatch(ctx, batch)
	var batchErr error
	for range candles {
		if _, err := results.Exec(); err != nil && batchErr == nil {
			batchErr = err
		}
	}
	if err := results.Close(); err != nil && batchErr == nil {
		batchErr = err
	}
	if batchErr != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("write candle batch: %w", batchErr)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit candle batch: %w", err)
	}
	return nil
}

// LoadRecentCandles returns candles in chronological order. A positive limit
// returns the latest limit candles; zero returns the full stored history so an
// indicator can rebuild its original seed before live publication begins.
func (w *PostgresWriter) LoadRecentCandles(ctx context.Context, securityID int64, exchangeSegment string, interval time.Duration, limit int) ([]ohlc.Candle, error) {
	if ctx == nil {
		return nil, errors.New("load candles context is nil")
	}
	if w == nil || w.pool == nil {
		return nil, errors.New("postgres writer is not initialized")
	}
	if securityID <= 0 || exchangeSegment == "" || interval <= 0 || limit < 0 {
		return nil, errors.New("invalid candle history request")
	}

	query := loadRecentCandlesSQL
	args := []any{securityID, exchangeSegment, ohlc.Timeframe(interval)}
	if limit > 0 {
		query += "\nLIMIT $4"
		args = append(args, limit)
	}
	rows, err := w.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query candle history: %w", err)
	}
	defer rows.Close()

	newestFirst := make([]ohlc.Candle, 0, limit)
	for rows.Next() {
		var candle ohlc.Candle
		var timeframe string
		var open, high, low, close string
		if err := rows.Scan(&candle.SecurityID, &candle.ExchangeSegment, &timeframe, &candle.Start, &open, &high, &low, &close, &candle.Volume); err != nil {
			return nil, fmt.Errorf("scan candle history: %w", err)
		}
		candle.Interval = interval
		candle.Open, err = parseDecimalPrice(open)
		if err != nil {
			return nil, fmt.Errorf("parse candle open: %w", err)
		}
		candle.High, err = parseDecimalPrice(high)
		if err != nil {
			return nil, fmt.Errorf("parse candle high: %w", err)
		}
		candle.Low, err = parseDecimalPrice(low)
		if err != nil {
			return nil, fmt.Errorf("parse candle low: %w", err)
		}
		candle.Close, err = parseDecimalPrice(close)
		if err != nil {
			return nil, fmt.Errorf("parse candle close: %w", err)
		}
		newestFirst = append(newestFirst, candle)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read candle history: %w", err)
	}

	for left, right := 0, len(newestFirst)-1; left < right; left, right = left+1, right-1 {
		newestFirst[left], newestFirst[right] = newestFirst[right], newestFirst[left]
	}
	return newestFirst, nil
}

func decimalPrice(value int64) string {
	negative := value < 0
	magnitude := uint64(value)
	if negative {
		magnitude = uint64(-(value + 1)) + 1
	}

	whole := magnitude / uint64(ohlc.PriceScale)
	fraction := magnitude % uint64(ohlc.PriceScale)
	formatted := strconv.FormatUint(whole, 10) + "." + fmt.Sprintf("%05d", fraction)
	if negative {
		return "-" + formatted
	}
	return formatted
}

func parseDecimalPrice(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, errors.New("empty decimal price")
	}

	negative := false
	if value[0] == '-' {
		negative = true
		value = value[1:]
	} else if value[0] == '+' {
		value = value[1:]
	}
	if value == "" {
		return 0, errors.New("decimal price has no digits")
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" {
		return 0, fmt.Errorf("invalid decimal price %q", value)
	}
	whole, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid whole price %q: %w", value, err)
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > 5 {
		return 0, fmt.Errorf("decimal price has more than five places %q", value)
	}
	fraction += strings.Repeat("0", 5-len(fraction))
	minor, err := strconv.ParseUint(fraction, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid fractional price %q: %w", value, err)
	}
	if whole > uint64(math.MaxInt64)/uint64(ohlc.PriceScale) {
		return 0, errors.New("decimal price exceeds fixed-point range")
	}
	magnitude := whole*uint64(ohlc.PriceScale) + minor
	if magnitude > uint64(math.MaxInt64) {
		return 0, errors.New("decimal price exceeds fixed-point range")
	}
	if negative {
		return -int64(magnitude), nil
	}
	return int64(magnitude), nil
}

var _ interface {
	WriteBatch(context.Context, []ohlc.Candle) error
} = (*PostgresWriter)(nil)

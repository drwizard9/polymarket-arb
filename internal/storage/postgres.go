package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/mselser95/polymarket-arb/internal/arbitrage"
	"go.uber.org/zap"
)

// PostgresStorage implements Storage using PostgreSQL.
type PostgresStorage struct {
	db     *sql.DB
	logger *zap.Logger
}

// PostgresConfig holds PostgreSQL configuration.
type PostgresConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
	SSLMode  string
	Logger   *zap.Logger
}

// NewPostgresStorage creates a new PostgreSQL storage.
func NewPostgresStorage(cfg *PostgresConfig) (*PostgresStorage, error) {
	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Connection pool limits — prevent connection exhaustion under load.
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("ping database: %w", err)
	}

	cfg.Logger.Info("postgres-storage-connected",
		zap.String("host", cfg.Host),
		zap.String("database", cfg.Database))

	return &PostgresStorage{
		db:     db,
		logger: cfg.Logger,
	}, nil
}

// outcomeRow is the per-outcome data persisted in the outcomes_json column.
type outcomeRow struct {
	TokenID  string  `json:"token_id"`
	Outcome  string  `json:"outcome"`
	AskPrice float64 `json:"ask_price"`
	AskSize  float64 `json:"ask_size"`
}

// StoreOpportunity stores an arbitrage opportunity in PostgreSQL.
// All outcomes are stored as JSONB in outcomes_json; the legacy binary columns
// (yes_bid_price/no_bid_price) are populated for the first two outcomes so that
// existing dashboards and queries remain functional.
func (p *PostgresStorage) StoreOpportunity(ctx context.Context, opp *arbitrage.Opportunity) error {
	// Build outcomes JSON — captures all N outcomes without data loss.
	rows := make([]outcomeRow, len(opp.Outcomes))
	for i, o := range opp.Outcomes {
		rows[i] = outcomeRow{
			TokenID:  o.TokenID,
			Outcome:  o.Outcome,
			AskPrice: o.AskPrice,
			AskSize:  o.AskSize,
		}
	}

	outcomesJSON, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal outcomes: %w", err)
	}

	// Backward-compatible values for the legacy binary columns.
	var firstPrice, secondPrice, firstSize, secondSize float64
	if len(opp.Outcomes) >= 2 {
		firstPrice = opp.Outcomes[0].AskPrice
		firstSize = opp.Outcomes[0].AskSize
		secondPrice = opp.Outcomes[1].AskPrice
		secondSize = opp.Outcomes[1].AskSize
	}

	query := `
		INSERT INTO arbitrage_opportunities (
			id, market_id, market_slug, market_question, detected_at,
			yes_bid_price, yes_bid_size, no_bid_price, no_bid_size,
			price_sum, profit_margin, profit_bps, max_trade_size,
			estimated_profit, total_fees, net_profit, net_profit_bps,
			config_threshold, outcomes_json
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
		)
	`

	_, err = p.db.ExecContext(ctx, query,
		opp.ID,
		opp.MarketID,
		opp.MarketSlug,
		opp.MarketQuestion,
		opp.DetectedAt,
		firstPrice,
		firstSize,
		secondPrice,
		secondSize,
		opp.TotalPriceSum,
		opp.ProfitMargin,
		opp.ProfitBPS,
		opp.MaxTradeSize,
		opp.EstimatedProfit,
		opp.TotalFees,
		opp.NetProfit,
		opp.NetProfitBPS,
		opp.ConfigMaxPriceSum,
		outcomesJSON,
	)

	if err != nil {
		return fmt.Errorf("insert opportunity: %w", err)
	}

	p.logger.Debug("opportunity-stored",
		zap.String("opportunity-id", opp.ID),
		zap.String("market-slug", opp.MarketSlug),
		zap.Int("outcome-count", len(opp.Outcomes)))

	return nil
}

// Close closes the database connection.
func (p *PostgresStorage) Close() error {
	p.logger.Info("closing-postgres-storage")
	return p.db.Close()
}

package circuitbreaker

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/mselser95/polymarket-arb/pkg/wallet"
	"go.uber.org/zap"
)

// BalanceFetcher is an interface for fetching wallet balances.
// Both wallet.Client and test mocks can implement this interface.
type BalanceFetcher interface {
	GetBalances(ctx context.Context, address common.Address) (*wallet.Balances, error)
}

// BalanceCircuitBreaker monitors wallet balance and controls trade execution.
// It dynamically calculates thresholds based on recent trade history and uses
// hysteresis to prevent rapid state changes.
type BalanceCircuitBreaker struct {
	enabled atomic.Bool // Atomic for lock-free reads

	// Configuration
	checkInterval   time.Duration
	walletClient    BalanceFetcher
	address         common.Address
	logger          *zap.Logger
	tradeMultiplier float64 // Multiplier for avg trade size
	minAbsolute     float64 // Absolute minimum balance
	hysteresisRatio float64 // Re-enable at ratio * disable threshold

	// Protected by mutex
	mu               sync.RWMutex
	lastBalance      float64   // Last checked balance (USDC)
	lastCheck        time.Time // When we last checked
	recentTrades     []float64 // Rolling window of trade sizes
	disableThreshold float64   // Current disable threshold
	enableThreshold  float64   // Current enable threshold
}

// Config holds circuit breaker configuration.
type Config struct {
	CheckInterval   time.Duration
	TradeMultiplier float64
	MinAbsolute     float64
	HysteresisRatio float64
	WalletClient    BalanceFetcher
	Address         common.Address
	Logger          *zap.Logger
}

// Status holds current circuit breaker status for debugging.
type Status struct {
	Enabled          bool
	LastBalance      float64
	LastCheck        time.Time
	DisableThreshold float64
	EnableThreshold  float64
	AvgTradeSize     float64
	RecentTradeCount int
}

// New creates a new circuit breaker with the given configuration.
func New(cfg *Config) (breaker *BalanceCircuitBreaker, err error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if cfg.WalletClient == nil {
		return nil, fmt.Errorf("wallet client cannot be nil")
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}
	if cfg.CheckInterval <= 0 {
		return nil, fmt.Errorf("check interval must be positive")
	}
	if cfg.TradeMultiplier <= 0 {
		return nil, fmt.Errorf("trade multiplier must be positive")
	}
	if cfg.MinAbsolute <= 0 {
		return nil, fmt.Errorf("min absolute must be positive")
	}
	if cfg.HysteresisRatio < 1.0 {
		return nil, fmt.Errorf("hysteresis ratio must be >= 1.0")
	}

	breaker = &BalanceCircuitBreaker{
		checkInterval:    cfg.CheckInterval,
		walletClient:     cfg.WalletClient,
		address:          cfg.Address,
		logger:           cfg.Logger,
		tradeMultiplier:  cfg.TradeMultiplier,
		minAbsolute:      cfg.MinAbsolute,
		hysteresisRatio:  cfg.HysteresisRatio,
		recentTrades:     make([]float64, 0, 20),
		disableThreshold: cfg.MinAbsolute, // Start with minimum
		enableThreshold:  cfg.MinAbsolute * cfg.HysteresisRatio,
	}

	// Start enabled by default
	breaker.enabled.Store(true)

	// Initialize metrics
	CircuitBreakerEnabled.Set(1)
	CircuitBreakerDisableThreshold.Set(breaker.disableThreshold)
	CircuitBreakerEnableThreshold.Set(breaker.enableThreshold)
	CircuitBreakerAvgTradeSize.Set(0)

	return breaker, nil
}

// IsEnabled returns true if trades should be executed.
// This is lock-free and safe to call from hot paths.
func (b *BalanceCircuitBreaker) IsEnabled() (enabled bool) {
	return b.enabled.Load()
}

// RecordTrade adds a trade to the rolling window and recalculates thresholds.
// Call this after successful trade execution.
func (b *BalanceCircuitBreaker) RecordTrade(tradeSize float64) {
	if tradeSize <= 0 {
		b.logger.Warn("invalid-trade-size",
			zap.Float64("size", tradeSize))
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Add to rolling window (keep last 20 trades)
	b.recentTrades = append(b.recentTrades, tradeSize)
	if len(b.recentTrades) > 20 {
		b.recentTrades = b.recentTrades[1:]
	}

	// Calculate average
	sum := 0.0
	for _, size := range b.recentTrades {
		sum += size
	}
	avgTradeSize := sum / float64(len(b.recentTrades))

	// Calculate thresholds
	b.disableThreshold = math.Max(avgTradeSize*b.tradeMultiplier, b.minAbsolute)
	b.enableThreshold = b.disableThreshold * b.hysteresisRatio

	// Update metrics
	CircuitBreakerAvgTradeSize.Set(avgTradeSize)
	CircuitBreakerDisableThreshold.Set(b.disableThreshold)
	CircuitBreakerEnableThreshold.Set(b.enableThreshold)

	b.logger.Debug("thresholds-updated",
		zap.Float64("avg_trade_size", avgTradeSize),
		zap.Int("trade_count", len(b.recentTrades)),
		zap.Float64("disable_threshold", b.disableThreshold),
		zap.Float64("enable_threshold", b.enableThreshold))
}

// CheckBalance checks current balance and updates enabled state based on thresholds.
func (b *BalanceCircuitBreaker) CheckBalance(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		CircuitBreakerCheckDuration.Observe(duration)
	}()

	// Fetch balances
	balances, err := b.walletClient.GetBalances(ctx, b.address)
	if err != nil {
		b.logger.Error("failed-to-check-balance",
			zap.Error(err),
			zap.String("address", b.address.Hex()))
		return fmt.Errorf("get balances: %w", err)
	}

	// Convert USDC balance to float (6 decimals)
	usdcFloat := new(big.Float).Quo(
		new(big.Float).SetInt(balances.USDC),
		big.NewFloat(1e6))
	balance, _ := usdcFloat.Float64()

	// Hold write lock for the entire read-compare-update cycle to prevent TOCTOU
	// between RecordTrade (which updates thresholds) and the state decision here.
	b.mu.Lock()
	disableThreshold := b.disableThreshold
	enableThreshold := b.enableThreshold
	currentlyEnabled := b.enabled.Load()
	b.lastBalance = balance
	b.lastCheck = time.Now()

	shouldDisable := currentlyEnabled && balance < disableThreshold
	shouldEnable := !currentlyEnabled && balance >= enableThreshold

	if shouldDisable {
		b.enabled.Store(false)
	} else if shouldEnable {
		b.enabled.Store(true)
	}
	b.mu.Unlock()

	// Update metrics outside the lock
	CircuitBreakerBalance.Set(balance)

	if shouldDisable {
		CircuitBreakerEnabled.Set(0)
		CircuitBreakerStateChanges.Inc()

		b.logger.Warn("circuit-breaker-disabled",
			zap.Float64("balance", balance),
			zap.Float64("disable_threshold", disableThreshold),
			zap.Float64("enable_threshold", enableThreshold))
	} else if shouldEnable {
		CircuitBreakerEnabled.Set(1)
		CircuitBreakerStateChanges.Inc()

		b.logger.Info("circuit-breaker-enabled",
			zap.Float64("balance", balance),
			zap.Float64("disable_threshold", disableThreshold),
			zap.Float64("enable_threshold", enableThreshold))
	} else {
		b.logger.Debug("balance-checked",
			zap.Float64("balance", balance),
			zap.Bool("enabled", currentlyEnabled),
			zap.Float64("disable_threshold", disableThreshold),
			zap.Float64("enable_threshold", enableThreshold))
	}

	return nil
}

// Start begins the background monitoring loop that periodically checks balance.
// This runs until the context is cancelled.
func (b *BalanceCircuitBreaker) Start(ctx context.Context) {
	b.logger.Info("circuit-breaker-started",
		zap.Duration("check_interval", b.checkInterval),
		zap.Float64("trade_multiplier", b.tradeMultiplier),
		zap.Float64("min_absolute", b.minAbsolute),
		zap.Float64("hysteresis_ratio", b.hysteresisRatio))

	// Check balance immediately on startup
	if err := b.CheckBalance(ctx); err != nil {
		b.logger.Error("initial-balance-check-failed", zap.Error(err))
	}

	// Start background monitoring
	go b.monitorLoop(ctx)
}

// monitorLoop is the background goroutine that periodically checks balance.
func (b *BalanceCircuitBreaker) monitorLoop(ctx context.Context) {
	ticker := time.NewTicker(b.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Info("circuit-breaker-stopped")
			return
		case <-ticker.C:
			if err := b.CheckBalance(ctx); err != nil {
				// Log error but continue monitoring
				b.logger.Error("balance-check-error", zap.Error(err))
			}
		}
	}
}

// GetStatus returns current circuit breaker status for debugging and HTTP endpoints.
func (b *BalanceCircuitBreaker) GetStatus() (status Status) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	sum := 0.0
	for _, size := range b.recentTrades {
		sum += size
	}
	avgTradeSize := 0.0
	if len(b.recentTrades) > 0 {
		avgTradeSize = sum / float64(len(b.recentTrades))
	}

	status = Status{
		Enabled:          b.enabled.Load(),
		LastBalance:      b.lastBalance,
		LastCheck:        b.lastCheck,
		DisableThreshold: b.disableThreshold,
		EnableThreshold:  b.enableThreshold,
		AvgTradeSize:     avgTradeSize,
		RecentTradeCount: len(b.recentTrades),
	}

	return status
}

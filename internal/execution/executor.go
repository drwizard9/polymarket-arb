package execution

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/mselser95/polymarket-arb/internal/arbitrage"
	"github.com/mselser95/polymarket-arb/internal/circuitbreaker"
	"github.com/mselser95/polymarket-arb/pkg/types"
	"go.uber.org/zap"
)

// maxFillVerificationGoroutines limits concurrent fill-verification goroutines.
// Prevents memory exhaustion during high-frequency trading bursts.
const maxFillVerificationGoroutines = 100

// Executor executes trades for arbitrage opportunities.
type Executor struct {
	mode             string // "paper" or "live"
	logger           *zap.Logger
	opportunityChan  <-chan *arbitrage.Opportunity
	ctx              context.Context
	wg               sync.WaitGroup
	cumulativeProfit float64
	mu               sync.Mutex
	orderClient      OrderPlacer // For live trading (interface)
	circuitBreaker   *circuitbreaker.BalanceCircuitBreaker
	fillSemaphore    chan struct{} // Bounds concurrent fill-verification goroutines

	// Fill verification config
	aggressionTicks  int
	fillTimeout      time.Duration
	fillRetryInitial time.Duration
	fillRetryMax     time.Duration
	fillRetryMult    float64
	takerFee         float64
}

// Config holds executor configuration.
type Config struct {
	Mode               string
	MaxPositionSize    float64
	Logger             *zap.Logger
	OpportunityChannel <-chan *arbitrage.Opportunity
	OrderClient        OrderPlacer                           // Optional: for live trading (interface)
	CircuitBreaker     *circuitbreaker.BalanceCircuitBreaker // Optional: for balance monitoring

	// Fill verification config
	AggressionTicks  int
	FillTimeout      time.Duration
	FillRetryInitial time.Duration
	FillRetryMax     time.Duration
	FillRetryMult    float64
	TakerFee         float64
}

// New creates a new trade executor.
func New(cfg *Config) *Executor {
	return &Executor{
		mode:             cfg.Mode,
		logger:           cfg.Logger,
		opportunityChan:  cfg.OpportunityChannel,
		orderClient:      cfg.OrderClient,
		circuitBreaker:   cfg.CircuitBreaker,
		fillSemaphore:    make(chan struct{}, maxFillVerificationGoroutines),
		aggressionTicks:  cfg.AggressionTicks,
		fillTimeout:      cfg.FillTimeout,
		fillRetryInitial: cfg.FillRetryInitial,
		fillRetryMax:     cfg.FillRetryMax,
		fillRetryMult:    cfg.FillRetryMult,
		takerFee:         cfg.TakerFee,
	}
}

// Start starts the executor.
func (e *Executor) Start(ctx context.Context) error {
	e.ctx = ctx
	e.logger.Info("executor-starting", zap.String("mode", e.mode))

	e.wg.Add(1)
	go e.executionLoop()

	return nil
}

// executionLoop processes opportunities.
func (e *Executor) executionLoop() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info("executor-stopping")
			return
		case opp, ok := <-e.opportunityChan:
			if !ok {
				e.logger.Info("opportunity-channel-closed")
				return
			}

			// Track opportunity received
			OpportunitiesReceived.Inc()

			// Check circuit breaker before executing
			if e.circuitBreaker != nil && !e.circuitBreaker.IsEnabled() {
				e.logger.Warn("skipping-opportunity-circuit-breaker-disabled",
					zap.String("opportunity-id", opp.ID),
					zap.String("market-slug", opp.MarketSlug),
					zap.Float64("spread", opp.ProfitMargin))
				OpportunitiesSkippedTotal.WithLabelValues("circuit_breaker").Inc()
				continue
			}

			start := time.Now()
			result := e.execute(opp)
			ExecutionDurationSeconds.Observe(time.Since(start).Seconds())

			if result.Error != nil {
				e.logger.Error("execution-failed",
					zap.String("opportunity-id", opp.ID),
					zap.Error(result.Error))

				// Classify error type
				errorType := classifyError(result.Error)
				ExecutionErrorsTotal.Inc()
				ExecutionErrorsByType.WithLabelValues(errorType).Inc()
			} else {
				// Track successful execution
				OpportunitiesExecuted.Inc()

				e.logger.Info("execution-successful",
					zap.String("opportunity-id", opp.ID),
					zap.String("market-slug", opp.MarketSlug),
					zap.Float64("profit", result.RealizedProfit))

				// Record successful trade for circuit breaker threshold calculation
				if e.circuitBreaker != nil && e.mode == "live" {
					e.circuitBreaker.RecordTrade(opp.MaxTradeSize)
				}
			}
		}
	}
}

// execute executes an arbitrage opportunity.
func (e *Executor) execute(opp *arbitrage.Opportunity) *types.ExecutionResult {
	switch e.mode {
	case "paper":
		return e.executePaper(opp)
	case "live":
		return e.executeLive(opp)
	default:
		return &types.ExecutionResult{
			OpportunityID: opp.ID,
			MarketSlug:    opp.MarketSlug,
			ExecutedAt:    time.Now(),
			Success:       false,
			Error:         fmt.Errorf("unknown execution mode: %s", e.mode),
		}
	}
}

// executePaper executes a paper trade (simulated).
// Supports both binary (2 outcomes) and multi-outcome (3+) markets.
func (e *Executor) executePaper(opp *arbitrage.Opportunity) *types.ExecutionResult {
	now := time.Now()

	// Simulate buying all outcomes
	trades := make([]*types.Trade, len(opp.Outcomes))
	for i, outcome := range opp.Outcomes {
		trades[i] = &types.Trade{
			Outcome:   outcome.Outcome,
			Side:      "BUY",
			Price:     outcome.AskPrice,
			Size:      opp.MaxTradeSize,
			Timestamp: now,
		}

		// Update metrics for each outcome
		TradesTotal.WithLabelValues("paper", outcome.Outcome).Inc()
	}

	// Calculate realized profit
	realizedProfit := opp.MaxTradeSize * opp.ProfitMargin

	// Update metrics
	ProfitRealizedUSD.WithLabelValues("paper").Add(realizedProfit)

	// Update cumulative profit
	e.mu.Lock()
	e.cumulativeProfit += realizedProfit
	cumulativeProfit := e.cumulativeProfit
	e.mu.Unlock()

	// Build log fields for all outcomes
	outcomeFields := make([]zap.Field, 0, len(opp.Outcomes))
	for i, outcome := range opp.Outcomes {
		outcomeFields = append(outcomeFields,
			zap.Float64(fmt.Sprintf("outcome%d-price", i+1), outcome.AskPrice),
			zap.String(fmt.Sprintf("outcome%d-name", i+1), outcome.Outcome))
	}

	baseFields := []zap.Field{
		zap.String("market-slug", opp.MarketSlug),
		zap.String("question", opp.MarketQuestion),
		zap.Int("outcome-count", len(opp.Outcomes)),
		zap.Float64("size", opp.MaxTradeSize),
		zap.Int("profit-bps", opp.ProfitBPS),
		zap.Float64("profit-usd", realizedProfit),
		zap.Float64("cumulative-profit-usd", cumulativeProfit),
	}

	e.logger.Info("paper-trade-executed", append(baseFields, outcomeFields...)...)

	// Create execution result
	result := &types.ExecutionResult{
		OpportunityID:  opp.ID,
		MarketSlug:     opp.MarketSlug,
		ExecutedAt:     now,
		RealizedProfit: realizedProfit,
		Success:        true,
		Error:          nil,
		AllTrades:      trades, // Store all trades
	}

	// For backward compatibility with binary markets, set YesTrade/NoTrade
	if len(trades) == 2 {
		result.YesTrade = trades[0]
		result.NoTrade = trades[1]
	}

	return result
}

// adjustPriceForAggression adjusts the ask price upward by N ticks to improve fill probability.
func adjustPriceForAggression(askPrice, tickSize float64, aggressionTicks int) (adjustedPrice float64) {
	adjustedPrice = askPrice + (tickSize * float64(aggressionTicks))

	// Cap at 0.9999 (max valid price on Polymarket)
	if adjustedPrice > 0.9999 {
		adjustedPrice = 0.9999
	}

	// Round to tick size boundaries
	adjustedPrice = math.Round(adjustedPrice/tickSize) * tickSize

	return adjustedPrice
}

// calculateActualProfit computes profit from fill verification results.
// Returns (actualProfit, allFilled).
// Requires all orders to be 100% filled; partial fills return 0.0, false.
func calculateActualProfit(fills []types.FillStatus, takerFee float64) (actualProfit float64, allFilled bool) {
	allFilled = true
	totalCost := 0.0
	tokenCount := 0.0

	for i, fill := range fills {
		if !fill.FullyFilled {
			return 0.0, false // Require 100% fill
		}
		totalCost += fill.SizeFilled * fill.ActualPrice

		// All outcomes should have equal token counts (arbitrage strategy)
		if i == 0 {
			tokenCount = fill.SizeFilled
		}
	}

	// Revenue from winning outcome: tokenCount * $1.00
	revenue := tokenCount
	fees := totalCost * takerFee
	actualProfit = revenue - totalCost - fees

	return actualProfit, true
}

// executeLive executes a live trade via Polymarket CLOB API.
// Supports both binary (2 outcomes) and multi-outcome (3+) markets.
// All orders are submitted atomically via the batch API endpoint.
func (e *Executor) executeLive(opp *arbitrage.Opportunity) *types.ExecutionResult {
	now := time.Now()

	if e.orderClient == nil {
		e.logger.Error("order-client-not-configured")
		return &types.ExecutionResult{
			OpportunityID: opp.ID,
			MarketSlug:    opp.MarketSlug,
			ExecutedAt:    now,
			Success:       false,
			Error:         fmt.Errorf("order client not configured"),
		}
	}

	// Validate all outcomes have token IDs
	for i, outcome := range opp.Outcomes {
		if outcome.TokenID == "" {
			e.logger.Error("missing-token-id",
				zap.String("opportunity-id", opp.ID),
				zap.Int("outcome-index", i),
				zap.String("outcome", outcome.Outcome))
			return &types.ExecutionResult{
				OpportunityID: opp.ID,
				MarketSlug:    opp.MarketSlug,
				ExecutedAt:    now,
				Success:       false,
				Error:         fmt.Errorf("missing token ID for outcome %d (%s)", i, outcome.Outcome),
			}
		}
	}

	// Build log fields for all outcomes
	outcomeLogFields := make([]zap.Field, 0, len(opp.Outcomes)*2)
	for i, outcome := range opp.Outcomes {
		outcomeLogFields = append(outcomeLogFields,
			zap.String(fmt.Sprintf("outcome%d", i+1), outcome.Outcome),
			zap.Float64(fmt.Sprintf("price%d", i+1), outcome.AskPrice))
	}

	e.logger.Info("placing-multi-outcome-orders",
		append([]zap.Field{
			zap.String("market-slug", opp.MarketSlug),
			zap.Int("outcome-count", len(opp.Outcomes)),
			zap.Float64("size", opp.MaxTradeSize),
		}, outcomeLogFields...)...)

	// Build outcome parameters for order client with aggressive pricing
	outcomeParams := make([]types.OutcomeOrderParams, len(opp.Outcomes))
	adjustedPrices := make([]float64, len(opp.Outcomes))

	for i, outcome := range opp.Outcomes {
		// Adjust price upward by N ticks to jump queue and ensure fills
		adjustedPrice := adjustPriceForAggression(outcome.AskPrice, outcome.TickSize, e.aggressionTicks)
		adjustedPrices[i] = adjustedPrice

		outcomeParams[i] = types.OutcomeOrderParams{
			TokenID:  outcome.TokenID,
			Price:    adjustedPrice, // Use adjusted price, not raw ask
			TickSize: outcome.TickSize,
			MinSize:  outcome.MinSize,
		}
	}

	// Log aggressive pricing adjustment for monitoring
	// Profit calculation is based on orderbook prices, not adjusted prices
	// Aggressive pricing is to ensure fast fills, expecting to get filled near orderbook prices
	originalAskSum := 0.0
	for _, o := range opp.Outcomes {
		originalAskSum += o.AskPrice
	}

	adjustedAskSum := 0.0
	for _, p := range adjustedPrices {
		adjustedAskSum += p
	}

	e.logger.Info("aggressive-pricing-applied",
		zap.String("opportunity-id", opp.ID),
		zap.String("market-slug", opp.MarketSlug),
		zap.Int("aggression-ticks", e.aggressionTicks),
		zap.Float64("original-ask-sum", originalAskSum),
		zap.Float64("adjusted-ask-sum", adjustedAskSum),
		zap.Float64("adjustment", adjustedAskSum-originalAskSum))

	// Convert USD budget to token count per outcome.
	// We buy the same number of tokens for EVERY outcome, so the total cost is:
	//   tokensPerOutcome × sum(all adjusted prices)
	// Solving for tokensPerOutcome: budget / sum(all prices)
	adjustedPriceSum := 0.0
	for _, price := range adjustedPrices {
		adjustedPriceSum += price
	}
	tokensPerOutcome := opp.MaxTradeSize / adjustedPriceSum

	// Log token calculation for verification
	e.logger.Info("calculated-token-count",
		zap.String("opportunity-id", opp.ID),
		zap.Float64("usd-budget", opp.MaxTradeSize),
		zap.Float64("tokens-per-outcome", tokensPerOutcome))

	// Calculate and log estimated total USD cost for validation
	estimatedCost := 0.0
	for i, price := range adjustedPrices {
		cost := tokensPerOutcome * price
		estimatedCost += cost
		e.logger.Debug("outcome-cost-estimate",
			zap.Int("outcome-index", i),
			zap.Float64("price", price),
			zap.Float64("tokens", tokensPerOutcome),
			zap.Float64("cost-usd", cost))
	}

	e.logger.Info("total-cost-estimate",
		zap.String("opportunity-id", opp.ID),
		zap.Float64("estimated-total-usd", estimatedCost),
		zap.Float64("max-budget-usd", opp.MaxTradeSize),
		zap.Bool("within-budget", estimatedCost <= opp.MaxTradeSize))

	// Place orders using batch endpoint for atomic submission
	ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
	defer cancel()

	responses, err := e.orderClient.PlaceOrdersMultiOutcome(
		ctx,
		outcomeParams,
		tokensPerOutcome, // Pass token count, not USD amount
	)

	if err != nil {
		// Log detailed error information
		e.logger.Error("multi-outcome-order-placement-failed",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Int("outcome-count", len(opp.Outcomes)),
			zap.Error(err))

		ExecutionErrorsTotal.Inc()

		return &types.ExecutionResult{
			OpportunityID: opp.ID,
			MarketSlug:    opp.MarketSlug,
			ExecutedAt:    now,
			Success:       false,
			Error:         err,
		}
	}

	// Verify all orders succeeded AND have valid order IDs
	var failedOutcomes []string
	for i, resp := range responses {
		if !resp.Success {
			failedOutcomes = append(failedOutcomes,
				fmt.Sprintf("%s: %s", opp.Outcomes[i].Outcome, resp.ErrorMsg))
		} else if resp.OrderID == "" {
			// Order returned success but no order ID - this is a problem
			failedOutcomes = append(failedOutcomes,
				fmt.Sprintf("%s: empty order ID", opp.Outcomes[i].Outcome))
		}
	}

	if len(failedOutcomes) > 0 {
		errorMsg := strings.Join(failedOutcomes, "; ")
		e.logger.Error("some-orders-failed",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Strings("failed-outcomes", failedOutcomes))
		ExecutionErrorsTotal.Inc()
		return &types.ExecutionResult{
			OpportunityID: opp.ID,
			MarketSlug:    opp.MarketSlug,
			ExecutedAt:    now,
			Success:       false,
			Error:         fmt.Errorf("order failures: %s", errorMsg),
		}
	}

	// Extract order IDs for fill verification
	orderIDs := make([]string, len(responses))
	outcomes := make([]string, len(opp.Outcomes))
	expectedSizes := make([]float64, len(opp.Outcomes))

	for i, resp := range responses {
		orderIDs[i] = resp.OrderID
		outcomes[i] = opp.Outcomes[i].Outcome
		expectedSizes[i] = opp.MaxTradeSize // Total USDC to spend
	}

	// Calculate expected profit based on adjusted prices
	expectedProfit := opp.MaxTradeSize * opp.ProfitMargin

	// Build log fields for order IDs
	orderLogFields := make([]zap.Field, 0, len(responses)*2)
	for i, resp := range responses {
		orderLogFields = append(orderLogFields,
			zap.String(fmt.Sprintf("outcome%d", i+1), opp.Outcomes[i].Outcome),
			zap.String(fmt.Sprintf("order-id%d", i+1), resp.OrderID))
	}

	e.logger.Info("orders-placed-verifying-fills",
		append([]zap.Field{
			zap.String("market-slug", opp.MarketSlug),
			zap.Int("outcome-count", len(opp.Outcomes)),
			zap.Float64("size-usd", opp.MaxTradeSize),
			zap.Float64("expected-profit-usd", expectedProfit),
			zap.String("note", "spawning goroutine for fill verification"),
		}, orderLogFields...)...)

	// Spawn bounded goroutine for fill verification; drop if semaphore is full
	// to prevent memory exhaustion during high-frequency bursts.
	select {
	case e.fillSemaphore <- struct{}{}:
		go func() {
			defer func() { <-e.fillSemaphore }()
			e.verifyFillsAndUpdateMetrics(orderIDs, outcomes, expectedSizes, adjustedPrices, opp, expectedProfit, now)
		}()
	default:
		e.logger.Warn("fill-verification-skipped-semaphore-full",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Int("max-concurrent", maxFillVerificationGoroutines))
	}

	// Return immediately with partial result (orders placed but not yet verified)
	result := &types.ExecutionResult{
		OpportunityID:  opp.ID,
		MarketSlug:     opp.MarketSlug,
		ExecutedAt:     now,
		OrderIDs:       orderIDs,
		ExpectedProfit: expectedProfit,
		Success:        true, // Orders placed successfully
		Error:          nil,
	}

	return result
}

// verifyFillsAndUpdateMetrics runs in a goroutine to verify fills and update metrics asynchronously.
func (e *Executor) verifyFillsAndUpdateMetrics(
	orderIDs []string,
	outcomes []string,
	expectedSizes []float64,
	adjustedPrices []float64,
	opp *arbitrage.Opportunity,
	expectedProfit float64,
	executedAt time.Time,
) {
	// Create a new context for fill verification (independent of request context)
	ctx, cancel := context.WithTimeout(context.Background(), e.fillTimeout+10*time.Second)
	defer cancel()

	// Require order client to support fill queries; mocks that only implement
	// OrderPlacer (not OrderGetter) will log a warning and skip verification.
	getter, ok := e.orderClient.(OrderGetter)
	if !ok {
		e.logger.Warn("skipping-fill-verification-client-does-not-support-get-order",
			zap.String("opportunity-id", opp.ID))
		return
	}

	fillTracker := NewFillTracker(
		getter,
		e.logger,
		&FillTrackerConfig{
			InitialBackoff: e.fillRetryInitial,
			MaxBackoff:     e.fillRetryMax,
			BackoffMult:    e.fillRetryMult,
			FillTimeout:    e.fillTimeout,
		},
	)

	// Verify fills with exponential backoff
	fillStartTime := time.Now()
	fillStatuses, err := fillTracker.VerifyFills(ctx, orderIDs, outcomes, expectedSizes)
	fillDuration := time.Since(fillStartTime)

	// Update fill verification duration metric
	FillVerificationDurationSeconds.Observe(fillDuration.Seconds())

	if err != nil {
		e.logger.Error("fill-verification-failed",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Error(err))
		FillVerificationTotal.WithLabelValues("error").Inc()
		return
	}

	// Calculate actual profit from fill data
	actualProfit, allFilled := calculateActualProfit(fillStatuses, e.takerFee)

	// Update metrics and logs based on fill status
	if allFilled {
		FillVerificationTotal.WithLabelValues("success").Inc()

		// Update profit metrics ONLY after 100% fill confirmation
		ProfitRealizedUSD.WithLabelValues("live").Add(actualProfit)

		e.mu.Lock()
		e.cumulativeProfit += actualProfit
		cumulativeActualProfit := e.cumulativeProfit
		e.mu.Unlock()

		e.logger.Info("all-orders-fully-filled",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Float64("expected-profit-usd", expectedProfit),
			zap.Float64("actual-profit-usd", actualProfit),
			zap.Float64("profit-deviation-usd", actualProfit-expectedProfit),
			zap.Float64("cumulative-actual-profit-usd", cumulativeActualProfit),
			zap.Duration("fill-duration", fillDuration))

		// Update trade count metrics for each filled outcome
		for _, fill := range fillStatuses {
			if fill.FullyFilled {
				TradesTotal.WithLabelValues("live", fill.Outcome).Inc()
			}
		}
	} else {
		FillVerificationTotal.WithLabelValues("partial").Inc()

		e.logger.Warn("orders-not-fully-filled",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Duration("fill-duration", fillDuration))
	}

	// Track price deviation for each fill
	for i, fill := range fillStatuses {
		if fill.FullyFilled && i < len(adjustedPrices) {
			deviation := fill.ActualPrice - adjustedPrices[i]
			ActualFillPriceDeviation.Observe(deviation)
		}
	}
}

// Close gracefully closes the executor.
func (e *Executor) Close() error {
	e.logger.Info("closing-executor")
	e.wg.Wait()

	e.mu.Lock()
	finalProfit := e.cumulativeProfit
	e.mu.Unlock()

	e.logger.Info("executor-closed",
		zap.Float64("total-profit-usd", finalProfit),
		zap.String("mode", e.mode))

	return nil
}

// classifyError classifies an execution error by type.
func classifyError(err error) string {
	if err == nil {
		return "unknown"
	}

	errMsg := strings.ToLower(err.Error())

	// Network errors
	if strings.Contains(errMsg, "connection refused") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "dial") ||
		strings.Contains(errMsg, "eof") ||
		strings.Contains(errMsg, "network") {
		return "network"
	}

	// API/validation errors
	if strings.Contains(errMsg, "api error") ||
		strings.Contains(errMsg, "invalid") ||
		strings.Contains(errMsg, "bad request") ||
		strings.Contains(errMsg, "400") ||
		strings.Contains(errMsg, "403") ||
		strings.Contains(errMsg, "404") ||
		strings.Contains(errMsg, "500") {
		return "api"
	}

	// Validation errors (client-side)
	if strings.Contains(errMsg, "missing") ||
		strings.Contains(errMsg, "required") ||
		strings.Contains(errMsg, "not configured") {
		return "validation"
	}

	// Insufficient funds
	if strings.Contains(errMsg, "insufficient") ||
		strings.Contains(errMsg, "balance") ||
		strings.Contains(errMsg, "funds") {
		return "funds"
	}

	return "unknown"
}

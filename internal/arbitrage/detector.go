package arbitrage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mselser95/polymarket-arb/internal/discovery"
	"github.com/mselser95/polymarket-arb/internal/markets"
	"github.com/mselser95/polymarket-arb/internal/orderbook"
	"github.com/mselser95/polymarket-arb/pkg/types"
	"go.uber.org/zap"
)

// Storage is the interface for storing opportunities.
type Storage interface {
	StoreOpportunity(ctx context.Context, opp *Opportunity) error
	Close() error
}

// Detector detects arbitrage opportunities.
type Detector struct {
	obManager        *orderbook.Manager
	discoveryService *discovery.Service
	config           Config
	logger           *zap.Logger
	storage          Storage
	metadataClient   *markets.CachedMetadataClient
	opportunityChan  chan *Opportunity
	obUpdateChan     <-chan *types.OrderbookSnapshot
	ctx              context.Context
	wg               sync.WaitGroup
}

// Config holds detector configuration.
type Config struct {
	MaxPriceSum  float64 // Maximum acceptable YES + NO price sum (lower = stricter)
	MinTradeSize float64
	MaxTradeSize float64
	TakerFee     float64
	Logger       *zap.Logger
}

// New creates a new arbitrage detector.
func New(cfg Config, obManager *orderbook.Manager, discoveryService *discovery.Service, storage Storage, metadataClient *markets.CachedMetadataClient) *Detector {
	return &Detector{
		obManager:        obManager,
		discoveryService: discoveryService,
		config:           cfg,
		logger:           cfg.Logger,
		storage:          storage,
		metadataClient:   metadataClient,
		opportunityChan:  make(chan *Opportunity, 10000),
		obUpdateChan:     obManager.UpdateChan(),
	}
}

// Start starts the arbitrage detector.
func (d *Detector) Start(ctx context.Context) error {
	d.ctx = ctx
	d.logger.Info("arbitrage-detector-starting",
		zap.Float64("threshold", d.config.MaxPriceSum),
		zap.Float64("min-trade-size", d.config.MinTradeSize),
		zap.Float64("max-trade-size", d.config.MaxTradeSize))

	d.wg.Add(1)
	go d.detectionLoop()

	return nil
}

// detectionLoop listens for orderbook updates and checks for arbitrage.
func (d *Detector) detectionLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.ctx.Done():
			d.logger.Info("arbitrage-detector-stopping")
			close(d.opportunityChan)
			return
		case update := <-d.obUpdateChan:
			if update == nil {
				// Channel closed
				return
			}
			start := time.Now()
			d.checkArbitrageForToken(update)
			DetectionDurationSeconds.Observe(time.Since(start).Seconds())
		}
	}
}

// checkArbitrageForToken checks for arbitrage when a specific token's orderbook updates.
func (d *Detector) checkArbitrageForToken(update *types.OrderbookSnapshot) {
	// Find which market this token belongs to (O(1) lookup via reverse index)
	targetMarket, exists := d.discoveryService.GetMarketByTokenID(update.TokenID)
	if !exists {
		// Token not part of any subscribed market
		return
	}

	// Get orderbooks for ALL outcomes in this market
	orderbooks := make([]*types.OrderbookSnapshot, 0, len(targetMarket.Outcomes))
	for _, outcome := range targetMarket.Outcomes {
		snapshot, ok := d.obManager.GetSnapshot(outcome.TokenID)
		if !ok {
			// Missing orderbook for this outcome - skip entire market
			d.logger.Debug("orderbook-missing-for-outcome",
				zap.String("market-id", targetMarket.MarketID),
				zap.String("outcome", outcome.Outcome))
			return
		}
		orderbooks = append(orderbooks, snapshot)
	}

	// Check for arbitrage (works for both binary and multi-outcome)
	opp, exists := d.detectMultiOutcome(targetMarket, orderbooks)
	if !exists {
		return
	}

	// Track end-to-end latency (from orderbook update to opportunity detection)
	// Use the most recent update time across all orderbooks
	latestUpdate := orderbooks[0].LastUpdated
	for _, book := range orderbooks {
		if book.LastUpdated.After(latestUpdate) {
			latestUpdate = book.LastUpdated
		}
	}
	e2eLatency := time.Since(latestUpdate).Seconds()
	EndToEndLatencySeconds.Observe(e2eLatency)

	// Store opportunity
	err := d.storage.StoreOpportunity(d.ctx, opp)
	if err != nil {
		d.logger.Error("failed-to-store-opportunity",
			zap.String("opportunity-id", opp.ID),
			zap.Error(err))
	}

	// Send opportunity (non-blocking)
	select {
	case d.opportunityChan <- opp:
		d.logger.Info("arbitrage-opportunity-detected",
			zap.String("opportunity-id", opp.ID),
			zap.String("market-slug", opp.MarketSlug),
			zap.Int("net-profit-bps", opp.NetProfitBPS),
			zap.Float64("net-profit", opp.NetProfit),
			zap.Int("outcome-count", len(opp.Outcomes)))
	default:
		d.logger.Warn("opportunity-channel-full", zap.String("market-slug", targetMarket.MarketSlug))
	}
}

// detectOpportunities scans all markets for arbitrage opportunities.
// DEPRECATED: Use event-driven detection via checkArbitrageForToken instead.
func (d *Detector) detectOpportunities() {
	// Get all subscribed markets
	markets := d.discoveryService.GetSubscribedMarkets()

	for _, market := range markets {
		// Get orderbooks for all outcomes
		orderbooks := make([]*types.OrderbookSnapshot, 0, len(market.Outcomes))
		for _, outcome := range market.Outcomes {
			snapshot, exists := d.obManager.GetSnapshot(outcome.TokenID)
			if !exists {
				// Missing orderbook - skip this market
				continue
			}
			orderbooks = append(orderbooks, snapshot)
		}

		if len(orderbooks) != len(market.Outcomes) {
			// Some orderbooks missing
			continue
		}

		// Check for arbitrage
		opp, exists := d.detectMultiOutcome(market, orderbooks)
		if !exists {
			continue
		}

		// Store opportunity
		err := d.storage.StoreOpportunity(d.ctx, opp)
		if err != nil {
			d.logger.Error("failed-to-store-opportunity",
				zap.String("opportunity-id", opp.ID),
				zap.Error(err))
		}

		// Send opportunity (non-blocking)
		select {
		case d.opportunityChan <- opp:
			d.logger.Info("arbitrage-opportunity-detected",
				zap.String("opportunity-id", opp.ID),
				zap.String("market-slug", opp.MarketSlug),
				zap.Int("net-profit-bps", opp.NetProfitBPS),
				zap.Float64("net-profit", opp.NetProfit))
		default:
			d.logger.Warn("opportunity-channel-full", zap.String("market-slug", market.MarketSlug))
		}
	}
}

// detect is DEPRECATED. Use detectMultiOutcome instead.
// Kept for backward compatibility with old tests.
func (d *Detector) detect(
	market *types.MarketSubscription,
	yesBook *types.OrderbookSnapshot,
	noBook *types.OrderbookSnapshot,
) (*Opportunity, bool) {
	// Simply convert to multi-outcome format and call detectMultiOutcome
	orderbooks := []*types.OrderbookSnapshot{yesBook, noBook}
	return d.detectMultiOutcome(market, orderbooks)
}

// detectMultiOutcome checks for arbitrage in N-outcome markets (binary or multi-outcome).
// Works by checking if SUM(all outcome ASK prices) < threshold.
func (d *Detector) detectMultiOutcome(
	market *types.MarketSubscription,
	orderbooks []*types.OrderbookSnapshot,
) (*Opportunity, bool) {
	// Validate all orderbooks have valid prices and sizes
	for i, book := range orderbooks {
		if book.BestAskPrice <= 0 {
			d.logger.Debug("invalid-ask-price",
				zap.String("market-slug", market.MarketSlug),
				zap.Int("outcome-index", i),
				zap.Float64("price", book.BestAskPrice))
			OpportunitiesRejectedTotal.WithLabelValues("invalid_price").Inc()
			return nil, false
		}

		if book.BestAskSize <= 0 {
			d.logger.Debug("invalid-ask-size",
				zap.String("market-slug", market.MarketSlug),
				zap.Int("outcome-index", i),
				zap.Float64("size", book.BestAskSize))
			OpportunitiesRejectedTotal.WithLabelValues("invalid_size").Inc()
			return nil, false
		}
	}

	// Calculate sum of ALL ask prices
	priceSum := 0.0
	for _, book := range orderbooks {
		priceSum += book.BestAskPrice
	}

	// Check if arbitrage exists
	if priceSum >= d.config.MaxPriceSum {
		d.logger.Debug("price-above-threshold",
			zap.String("market-slug", market.MarketSlug),
			zap.Float64("price-sum", priceSum),
			zap.Float64("threshold", d.config.MaxPriceSum),
			zap.Float64("shortfall", priceSum-d.config.MaxPriceSum))
		OpportunitiesRejectedTotal.WithLabelValues("price_above_threshold").Inc()
		return nil, false
	}

	// POTENTIAL ARBITRAGE DETECTED - Print detailed analysis before validation
	d.printArbitrageAnalysis(market, orderbooks, priceSum)

	// Find minimum size across all outcomes (bottleneck for trade size)
	maxSize := orderbooks[0].BestAskSize
	for _, book := range orderbooks {
		if book.BestAskSize < maxSize {
			maxSize = book.BestAskSize
		}
	}

	// Apply maximum trade size cap
	if maxSize > d.config.MaxTradeSize {
		d.logger.Debug("trade-size-capped-by-max",
			zap.String("market-slug", market.MarketSlug),
			zap.Float64("calculated-size", maxSize),
			zap.Float64("max-size", d.config.MaxTradeSize))
		maxSize = d.config.MaxTradeSize
	}

	// Check minimum trade size
	if maxSize < d.config.MinTradeSize {
		d.logger.Info("opportunity-rejected-below-min-size",
			zap.String("market-slug", market.MarketSlug),
			zap.Float64("price-sum", priceSum),
			zap.Float64("spread", d.config.MaxPriceSum-priceSum),
			zap.Float64("calculated-size", maxSize),
			zap.Float64("min-size", d.config.MinTradeSize))
		OpportunitiesRejectedTotal.WithLabelValues("below_min_size").Inc()
		return nil, false
	}

	// Fetch market-specific metadata for all outcomes
	outcomes := make([]OpportunityOutcome, len(orderbooks))
	var requiredUSD float64

	for i, book := range orderbooks {
		var tickSize, minSize float64

		// Use metadata client if available, otherwise use defaults
		if d.metadataClient != nil {
			metaCtx, metaCancel := context.WithTimeout(context.Background(), 2*time.Second)
			var err error
			tickSize, minSize, err = d.metadataClient.GetTokenMetadata(metaCtx, book.TokenID)
			metaCancel()
			if err != nil {
				d.logger.Warn("failed-to-fetch-token-metadata",
					zap.String("token-id", book.TokenID),
					zap.Error(err))
				// Use defaults
				tickSize = 0.01
				minSize = 5.0
			}
		} else {
			// No metadata client available, use defaults
			tickSize = 0.01
			minSize = 5.0
		}

		// Calculate token size for this outcome
		tokenSize := maxSize / book.BestAskPrice

		// Check if this outcome meets minimum requirements
		if tokenSize < minSize {
			d.logger.Info("opportunity-rejected-below-market-minimum",
				zap.String("market-slug", market.MarketSlug),
				zap.String("outcome", market.Outcomes[i].Outcome),
				zap.Float64("price-sum", priceSum),
				zap.Float64("spread", d.config.MaxPriceSum-priceSum),
				zap.Float64("token-size", tokenSize),
				zap.Float64("market-min-size", minSize),
				zap.Float64("required-usd", minSize*book.BestAskPrice))
			OpportunitiesRejectedTotal.WithLabelValues("below_market_min").Inc()
			return nil, false
		}

		// Track the largest USD minimum across all outcomes
		requiredUSDForOutcome := minSize * book.BestAskPrice
		if requiredUSDForOutcome > requiredUSD {
			requiredUSD = requiredUSDForOutcome
		}

		// Build outcome structure
		outcomes[i] = OpportunityOutcome{
			TokenID:  book.TokenID,
			Outcome:  market.Outcomes[i].Outcome,
			AskPrice: book.BestAskPrice,
			AskSize:  book.BestAskSize,
			TickSize: tickSize,
			MinSize:  minSize,
		}
	}

	// Adjust maxSize upward to meet all minimum requirements
	if maxSize < requiredUSD {
		maxSize = requiredUSD
	}

	// Create opportunity using multi-outcome constructor
	opp := NewMultiOutcomeOpportunity(
		market.MarketID,
		market.MarketSlug,
		market.Question,
		outcomes,
		maxSize, // Pass calculated maxSize (includes all constraints)
		d.config.MaxPriceSum,
		d.config.TakerFee,
	)

	// Check if net profit is positive after fees
	if opp.NetProfit <= 0 {
		d.logger.Info("opportunity-rejected-negative-profit-after-fees",
			zap.String("market-slug", market.MarketSlug),
			zap.Float64("price-sum", opp.TotalPriceSum),
			zap.Float64("spread", d.config.MaxPriceSum-opp.TotalPriceSum),
			zap.Float64("trade-size", opp.MaxTradeSize),
			zap.Float64("gross-profit", opp.EstimatedProfit),
			zap.Float64("total-fees", opp.TotalFees),
			zap.Float64("net-profit", opp.NetProfit),
			zap.Float64("taker-fee-rate", d.config.TakerFee))
		OpportunitiesRejectedTotal.WithLabelValues("negative_profit_after_fees").Inc()
		return nil, false
	}

	// Update metrics
	OpportunitiesDetectedTotal.Inc()
	OpportunityProfitBPS.Observe(float64(opp.ProfitBPS))
	OpportunitySizeUSD.Observe(opp.MaxTradeSize)
	NetProfitBPS.Observe(float64(opp.NetProfitBPS))

	return opp, true
}

// OpportunityChan returns the channel for receiving opportunities.
func (d *Detector) OpportunityChan() <-chan *Opportunity {
	return d.opportunityChan
}

// Close gracefully closes the detector.
func (d *Detector) Close() error {
	d.logger.Info("closing-arbitrage-detector")
	d.wg.Wait()
	d.logger.Info("arbitrage-detector-closed")
	return nil
}

// printArbitrageAnalysis prints detailed components of potential arbitrage to console.
func (d *Detector) printArbitrageAnalysis(
	market *types.MarketSubscription,
	orderbooks []*types.OrderbookSnapshot,
	priceSum float64,
) {
	fmt.Println("\n" + "┌────────────────────────────────────────────────────────────────────────────┐")
	fmt.Printf("│ POTENTIAL ARBITRAGE: %s\n", market.MarketSlug)
	fmt.Println("└────────────────────────────────────────────────────────────────────────────┘")
	fmt.Printf("  Question: %s\n", market.Question)
	fmt.Printf("  Market ID: %s\n", market.MarketID)
	fmt.Println()

	// Print all outcomes with prices and sizes
	fmt.Println("  OUTCOMES:")
	for i, book := range orderbooks {
		outcome := market.Outcomes[i].Outcome
		fmt.Printf("    [%d] %-15s Ask: $%.4f × %.2f tokens\n",
			i+1, outcome, book.BestAskPrice, book.BestAskSize)
	}
	fmt.Println()

	// Calculate spread and potential profit
	spread := d.config.MaxPriceSum - priceSum
	spreadBPS := spread * 10000

	fmt.Println("  PRICE ANALYSIS:")
	fmt.Printf("    Sum of Ask Prices:  %.6f\n", priceSum)
	fmt.Printf("    Threshold:          %.6f\n", d.config.MaxPriceSum)
	fmt.Printf("    Spread:             %.6f (%.0f bps)\n", spread, spreadBPS)
	fmt.Println()

	// Find minimum available size
	minSize := orderbooks[0].BestAskSize
	bottleneckOutcome := market.Outcomes[0].Outcome
	for i, book := range orderbooks {
		if book.BestAskSize < minSize {
			minSize = book.BestAskSize
			bottleneckOutcome = market.Outcomes[i].Outcome
		}
	}

	// Calculate trade sizes for each outcome
	fmt.Println("  SIZE ANALYSIS:")
	fmt.Printf("    Available Sizes:\n")
	for i, book := range orderbooks {
		usdValue := book.BestAskSize * book.BestAskPrice
		fmt.Printf("      %-15s %.2f tokens = $%.2f\n",
			market.Outcomes[i].Outcome+":", book.BestAskSize, usdValue)
	}
	fmt.Printf("    Bottleneck:         %s (%.2f tokens)\n", bottleneckOutcome, minSize)
	fmt.Printf("    Max Trade Size:     $%.2f (before caps)\n", minSize)
	fmt.Println()

	// Apply size caps
	cappedSize := minSize
	if cappedSize > d.config.MaxTradeSize {
		fmt.Printf("    ⚠ Capped by MAX:    $%.2f → $%.2f\n", cappedSize, d.config.MaxTradeSize)
		cappedSize = d.config.MaxTradeSize
	}

	// Check minimum
	meetsMin := cappedSize >= d.config.MinTradeSize
	fmt.Printf("    Min Trade Size:     $%.2f %s\n",
		d.config.MinTradeSize,
		map[bool]string{true: "✓", false: "✗ FAILS"}[meetsMin])
	fmt.Printf("    Final Trade Size:   $%.2f\n", cappedSize)
	fmt.Println()

	// Calculate gross profit and fees
	grossProfit := cappedSize * spread
	feesPerOutcome := cappedSize * d.config.TakerFee
	totalFees := feesPerOutcome * float64(len(orderbooks))
	netProfit := grossProfit - totalFees

	fmt.Println("  PROFIT ANALYSIS:")
	fmt.Printf("    Gross Profit:       $%.4f (%.0f bps)\n", grossProfit, spreadBPS)
	fmt.Printf("    Taker Fee Rate:     %.2f%% per outcome\n", d.config.TakerFee*100)
	fmt.Printf("    Fees (%d outcomes):  $%.4f ($%.4f × %d)\n",
		len(orderbooks), totalFees, feesPerOutcome, len(orderbooks))
	fmt.Printf("    Net Profit:         $%.4f ", netProfit)
	if netProfit > 0 {
		netBPS := (netProfit / cappedSize) * 10000
		fmt.Printf("(%.0f bps) ✓\n", netBPS)
	} else {
		fmt.Printf("✗ UNPROFITABLE\n")
	}
	fmt.Println()

	// Check market minimums (estimate)
	fmt.Println("  MARKET MINIMUM CHECK:")
	for i, book := range orderbooks {
		// Use default minimum of 5 tokens as example
		minTokens := 5.0
		tokenAmount := cappedSize / book.BestAskPrice
		requiredUSD := minTokens * book.BestAskPrice
		meetsMarketMin := tokenAmount >= minTokens

		fmt.Printf("    %-15s %.2f tokens %s (min: %.0f, need: $%.2f)\n",
			market.Outcomes[i].Outcome+":",
			tokenAmount,
			map[bool]string{true: "✓", false: "✗"}[meetsMarketMin],
			minTokens,
			requiredUSD)
	}
	fmt.Println()

	// Print validation status
	fmt.Println("  VALIDATION:")
	fmt.Printf("    Price Check:        %s (sum < threshold)\n",
		map[bool]string{true: "✓ PASS", false: "✗ FAIL"}[priceSum < d.config.MaxPriceSum])
	fmt.Printf("    Size Check:         %s (size >= min)\n",
		map[bool]string{true: "✓ PASS", false: "✗ FAIL"}[cappedSize >= d.config.MinTradeSize])
	fmt.Printf("    Profit Check:       %s (net profit > 0)\n",
		map[bool]string{true: "✓ PASS", false: "✗ FAIL"}[netProfit > 0])

	fmt.Println("─────────────────────────────────────────────────────────────────────────────")
}

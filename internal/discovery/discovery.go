package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mselser95/polymarket-arb/pkg/cache"
	"github.com/mselser95/polymarket-arb/pkg/types"
	"go.uber.org/zap"
)

// Service discovers new markets by polling the Gamma API.
type Service struct {
	client            *Client
	cache             cache.Cache
	pollInterval      time.Duration
	marketLimit       int
	maxMarketDuration time.Duration
	logger            *zap.Logger
	subscribed        map[string]*types.MarketSubscription
	tokenToMarket     map[string]*types.MarketSubscription // Reverse index: tokenID -> market
	mu                sync.RWMutex
	newMarketsCh      chan *types.Market
	singleMarket      string // For debugging: if set, only track this one market
}

// Config holds discovery service configuration.
type Config struct {
	Client            *Client
	Cache             cache.Cache
	PollInterval      time.Duration
	MarketLimit       int
	MaxMarketDuration time.Duration
	Logger            *zap.Logger
	SingleMarket      string // For debugging: slug of single market to track
}

// New creates a new discovery service.
func New(cfg *Config) *Service {
	return &Service{
		client:            cfg.Client,
		cache:             cfg.Cache,
		pollInterval:      cfg.PollInterval,
		marketLimit:       cfg.MarketLimit,
		maxMarketDuration: cfg.MaxMarketDuration,
		logger:            cfg.Logger,
		subscribed:        make(map[string]*types.MarketSubscription),
		tokenToMarket:     make(map[string]*types.MarketSubscription),
		newMarketsCh:      make(chan *types.Market, 10000),
		singleMarket:      cfg.SingleMarket,
	}
}

// Run starts the discovery polling loop.
func (s *Service) Run(ctx context.Context) error {
	s.logger.Info("discovery-service-starting",
		zap.Duration("poll-interval", s.pollInterval),
		zap.Int("market-limit", s.marketLimit),
		zap.String("single-market", s.singleMarket))

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	// Initial poll
	err := s.poll(ctx)
	if err != nil {
		s.logger.Error("initial-poll-failed", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("discovery-service-stopping")
			close(s.newMarketsCh)
			return ctx.Err()
		case <-ticker.C:
			err = s.poll(ctx)
			if err != nil {
				s.logger.Error("poll-failed", zap.Error(err))
			}
		}
	}
}

// poll fetches markets from the API and identifies new ones.
func (s *Service) poll(ctx context.Context) error {
	start := time.Now()
	defer func() {
		PollDurationSeconds.Observe(time.Since(start).Seconds())
	}()

	// Single market mode for debugging
	if s.singleMarket != "" {
		return s.pollSingleMarket(ctx)
	}

	// Fetch active markets sorted by 24h volume (DESC) — most liquid first.
	// endDate ascending returns already-expired markets from the Gamma API, so
	// volume24hr is used instead. The duration filter in identifyNewMarkets then
	// retains only markets expiring within ARB_MAX_MARKET_DURATION.
	resp, err := s.client.FetchActiveMarkets(ctx, s.marketLimit, 0, "volume24hr")
	if err != nil {
		PollErrorsTotal.Inc()
		return fmt.Errorf("fetch active markets: %w", err)
	}

	MarketsDiscoveredTotal.Add(float64(len(resp.Data)))

	// Identify new markets
	newMarkets := s.identifyNewMarkets(resp.Data)

	// Cache and send new markets to channel (non-blocking)
	sentCount := 0
	droppedCount := 0
	for i := range newMarkets {
		// Cache the market
		s.cacheMarket(newMarkets[i])

		select {
		case s.newMarketsCh <- newMarkets[i]:
			NewMarketsTotal.Inc()
			sentCount++
			s.logger.Debug("new-market-discovered",
				zap.String("market-id", newMarkets[i].ID),
				zap.String("question", newMarkets[i].Question))
		default:
			droppedCount++
			s.logger.Warn("new-markets-channel-full",
				zap.String("market-id", newMarkets[i].ID))
		}
	}

	s.logger.Info("poll-complete",
		zap.Int("total-markets", len(resp.Data)),
		zap.Int("new-markets", len(newMarkets)),
		zap.Int("sent-to-channel", sentCount),
		zap.Int("dropped", droppedCount),
		zap.Duration("duration", time.Since(start)))

	return nil
}

// pollSingleMarket polls only a specific market (for debugging).
func (s *Service) pollSingleMarket(ctx context.Context) error {
	// Check if already subscribed
	s.mu.RLock()
	_, exists := s.subscribed[s.singleMarket]
	s.mu.RUnlock()

	if exists {
		// Already subscribed, nothing to do
		return nil
	}

	// Fetch the single market
	market, err := s.client.FetchMarketBySlug(ctx, s.singleMarket)
	if err != nil {
		PollErrorsTotal.Inc()
		return fmt.Errorf("fetch market by slug %q: %w", s.singleMarket, err)
	}

	MarketsDiscoveredTotal.Inc()

	// Check if market has at least 2 outcomes (binary or multi-outcome)
	if len(market.Tokens) < 2 {
		return fmt.Errorf("market %q has insufficient outcomes (%d, need 2+)",
			s.singleMarket, len(market.Tokens))
	}

	// Map all outcomes to OutcomeToken structs
	outcomes := make([]types.OutcomeToken, len(market.Tokens))
	for i, token := range market.Tokens {
		outcomes[i] = types.OutcomeToken{
			TokenID: token.TokenID,
			Outcome: token.Outcome,
		}
	}

	// Mark as subscribed
	s.mu.Lock()
	marketSub := &types.MarketSubscription{
		MarketID:     market.ID,
		MarketSlug:   market.Slug,
		Question:     market.Question,
		Outcomes:     outcomes,
		SubscribedAt: time.Now(),
	}
	s.subscribed[market.Slug] = marketSub
	// Build reverse index: tokenID -> market
	for _, outcome := range outcomes {
		s.tokenToMarket[outcome.TokenID] = marketSub
	}
	s.mu.Unlock()

	// Cache and send to channel
	s.cacheMarket(market)

	select {
	case s.newMarketsCh <- market:
		NewMarketsTotal.Inc()
		s.logger.Info("single-market-discovered",
			zap.String("slug", market.Slug),
			zap.String("question", market.Question))
	default:
		s.logger.Warn("new-markets-channel-full")
	}

	return nil
}

// identifyNewMarkets returns markets that haven't been subscribed yet.
func (s *Service) identifyNewMarkets(markets []types.Market) []*types.Market {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newMarkets []*types.Market

	for i := range markets {
		market := &markets[i]

		// Check if already subscribed
		if _, exists := s.subscribed[market.Slug]; exists {
			continue
		}

		// Check if market has at least 2 outcomes (binary or multi-outcome)
		if len(market.Tokens) < 2 {
			s.logger.Debug("skipping-market-insufficient-outcomes",
				zap.String("market-id", market.ID),
				zap.String("question", market.Question),
				zap.Int("outcome-count", len(market.Tokens)))
			continue
		}

		// Filter by EndDate (only subscribe to markets expiring within threshold)
		// If maxMarketDuration == 0, skip all duration checks (unlimited)
		if !market.EndDate.IsZero() && s.maxMarketDuration > 0 {
			timeUntilExpiry := time.Until(market.EndDate)

			if timeUntilExpiry < 0 {
				// Market already expired
				s.logger.Debug("skipping-market-already-expired",
					zap.String("slug", market.Slug),
					zap.String("market-id", market.ID),
					zap.Time("end-date", market.EndDate))
				continue
			}

			if timeUntilExpiry > s.maxMarketDuration {
				// Market expires too far in future
				s.logger.Info("market filtered by duration",
					zap.String("market", market.Slug),
					zap.Duration("time_until_expiry", timeUntilExpiry),
					zap.Duration("max_duration", s.maxMarketDuration))
				MarketsFilteredByEndDateTotal.Inc()
				continue
			}
		}

		// Map all outcomes to OutcomeToken structs
		outcomes := make([]types.OutcomeToken, len(market.Tokens))
		for i, token := range market.Tokens {
			outcomes[i] = types.OutcomeToken{
				TokenID: token.TokenID,
				Outcome: token.Outcome,
			}
		}

		// Mark as subscribed
		marketSub := &types.MarketSubscription{
			MarketID:     market.ID,
			MarketSlug:   market.Slug,
			Question:     market.Question,
			Outcomes:     outcomes,
			SubscribedAt: time.Now(),
		}
		s.subscribed[market.Slug] = marketSub
		// Build reverse index: tokenID -> market
		for _, outcome := range outcomes {
			s.tokenToMarket[outcome.TokenID] = marketSub
		}

		newMarkets = append(newMarkets, market)
	}

	return newMarkets
}

// NewMarketsChan returns the channel for receiving new markets.
func (s *Service) NewMarketsChan() <-chan *types.Market {
	return s.newMarketsCh
}

// GetSubscribedMarkets returns all currently subscribed markets.
func (s *Service) GetSubscribedMarkets() []*types.MarketSubscription {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs := make([]*types.MarketSubscription, 0, len(s.subscribed))
	for _, sub := range s.subscribed {
		subs = append(subs, sub)
	}

	return subs
}

// GetMarketBySlug retrieves a market subscription by its slug.
func (s *Service) GetMarketBySlug(slug string) (*types.MarketSubscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	sub, exists := s.subscribed[slug]
	return sub, exists
}

// GetMarketByTokenID retrieves a market subscription by token ID (O(1) lookup).
func (s *Service) GetMarketByTokenID(tokenID string) (*types.MarketSubscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	market, exists := s.tokenToMarket[tokenID]
	return market, exists
}

// cacheMarket stores a market in the cache.
func (s *Service) cacheMarket(market *types.Market) {
	if s.cache == nil {
		return
	}

	// Cache by market ID with 24 hour TTL
	const cacheTTL = 24 * time.Hour
	success := s.cache.Set(market.ID, market, cacheTTL)
	if !success {
		s.logger.Warn("failed-to-cache-market", zap.String("market-id", market.ID))
	}
}

// GetMarket retrieves a market from cache or returns nil if not found.
func (s *Service) GetMarket(marketID string) *types.Market {
	if s.cache == nil {
		return nil
	}

	value, found := s.cache.Get(marketID)
	if !found {
		return nil
	}

	market, ok := value.(*types.Market)
	if !ok {
		s.logger.Warn("invalid-market-type-in-cache",
			zap.String("market-id", marketID))
		return nil
	}

	return market
}

// RemoveMarkets removes markets from the subscribed map.
func (s *Service) RemoveMarkets(markets []*types.MarketSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, market := range markets {
		delete(s.subscribed, market.MarketSlug)

		// Also remove from cache if present
		if s.cache != nil {
			s.cache.Delete(market.MarketID)
		}
	}

	s.logger.Info("markets-removed",
		zap.Int("count", len(markets)))
}

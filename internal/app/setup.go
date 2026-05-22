package app

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/mselser95/polymarket-arb/internal/arbitrage"
	"github.com/mselser95/polymarket-arb/internal/circuitbreaker"
	"github.com/mselser95/polymarket-arb/internal/discovery"
	"github.com/mselser95/polymarket-arb/internal/execution"
	"github.com/mselser95/polymarket-arb/internal/markets"
	"github.com/mselser95/polymarket-arb/internal/orderbook"
	"github.com/mselser95/polymarket-arb/internal/storage"
	"github.com/mselser95/polymarket-arb/pkg/cache"
	"github.com/mselser95/polymarket-arb/pkg/config"
	"github.com/mselser95/polymarket-arb/pkg/healthprobe"
	"github.com/mselser95/polymarket-arb/pkg/httpserver"
	"github.com/mselser95/polymarket-arb/pkg/wallet"
	"github.com/mselser95/polymarket-arb/pkg/websocket"
	"go.uber.org/zap"
)

// New creates a new application instance.
func New(cfg *config.Config, logger *zap.Logger, opts *Options) (*App, error) {
	if opts == nil {
		opts = &Options{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize components
	healthChecker := setupHealthChecker()

	// Setup cache
	marketCache, err := setupCache(logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("setup cache: %w", err)
	}

	// Setup metadata client (needed for WebSocket pool to update tick sizes)
	metadataClient := markets.NewMetadataClient()
	cachedMetadataClient := markets.NewCachedMetadataClient(metadataClient, marketCache)

	discoveryService := setupDiscoveryService(cfg, logger, marketCache, opts)
	wsPool := setupWebSocketPool(cfg, logger, cachedMetadataClient)
	obManager := setupOrderbookManager(logger, wsPool)

	// Setup HTTP server (needs orderbook manager and discovery service)
	httpServer := setupHTTPServer(cfg, logger, healthChecker, obManager, discoveryService)

	// Setup storage
	arbStorage, err := setupStorage(cfg, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("setup storage: %w", err)
	}

	// Setup arbitrage detector
	arbDetector := setupArbitrageDetector(cfg, logger, obManager, discoveryService, arbStorage, cachedMetadataClient)

	// Setup executor
	executor, err := setupExecutor(ctx, cfg, logger, arbDetector)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("setup executor: %w", err)
	}

	return &App{
		cfg:              cfg,
		logger:           logger,
		healthChecker:    healthChecker,
		httpServer:       httpServer,
		discoveryService: discoveryService,
		wsPool:           wsPool,
		obManager:        obManager,
		arbDetector:      arbDetector,
		executor:         executor,
		storage:          arbStorage,
		ctx:              ctx,
		cancel:           cancel,
	}, nil
}

func setupHealthChecker() *healthprobe.HealthChecker {
	return healthprobe.New()
}

func setupHTTPServer(
	cfg *config.Config,
	logger *zap.Logger,
	healthChecker *healthprobe.HealthChecker,
	obManager *orderbook.Manager,
	discoveryService *discovery.Service,
) *httpserver.Server {
	return httpserver.New(&httpserver.Config{
		Port:             cfg.HTTPPort,
		Logger:           logger,
		HealthChecker:    healthChecker,
		OrderbookManager: obManager,
		DiscoveryService: discoveryService,
	})
}

func setupCache(logger *zap.Logger) (cache.Cache, error) {
	return cache.NewRistrettoCache(&cache.RistrettoConfig{
		NumCounters: 10000, // 10x expected max items (1000 markets)
		MaxCost:     1000,  // Maximum 1000 items in cache
		BufferItems: 64,    // Buffer size for Get operations
		Logger:      logger,
	})
}

func setupDiscoveryService(cfg *config.Config, logger *zap.Logger, marketCache cache.Cache, opts *Options) *discovery.Service {
	discoveryClient := discovery.NewClient(cfg.PolymarketGammaURL, logger)
	return discovery.New(&discovery.Config{
		Client:            discoveryClient,
		Cache:             marketCache,
		PollInterval:      cfg.DiscoveryPollInterval,
		MarketLimit:       cfg.DiscoveryMarketLimit,
		MaxMarketDuration: cfg.MaxMarketDuration,
		Logger:            logger,
		SingleMarket:      opts.SingleMarket,
	})
}

func setupWebSocketPool(cfg *config.Config, logger *zap.Logger, metadataUpdater websocket.MetadataUpdater) *websocket.Pool {
	return websocket.NewPool(websocket.PoolConfig{
		Size:                  cfg.WSPoolSize,
		WSUrl:                 cfg.PolymarketWSURL,
		DialTimeout:           cfg.WSDialTimeout,
		PongTimeout:           cfg.WSPongTimeout,
		PingInterval:          cfg.WSPingInterval,
		ReconnectInitialDelay: cfg.WSReconnectInitialDelay,
		ReconnectMaxDelay:     cfg.WSReconnectMaxDelay,
		ReconnectBackoffMult:  cfg.WSReconnectBackoffMult,
		MessageBufferSize:     cfg.WSMessageBufferSize,
		Logger:                logger,
		MetadataUpdater:       metadataUpdater,
	})
}

func setupOrderbookManager(logger *zap.Logger, wsPool *websocket.Pool) *orderbook.Manager {
	return orderbook.New(&orderbook.Config{
		Logger:         logger,
		MessageChannel: wsPool.MessageChan(),
	})
}

func setupStorage(cfg *config.Config, logger *zap.Logger) (arbitrage.Storage, error) {
	if cfg.StorageMode == "postgres" {
		pgStorage, err := storage.NewPostgresStorage(&storage.PostgresConfig{
			Host:     cfg.PostgresHost,
			Port:     cfg.PostgresPort,
			User:     cfg.PostgresUser,
			Password: cfg.PostgresPass,
			Database: cfg.PostgresDB,
			SSLMode:  cfg.PostgresSSL,
			Logger:   logger,
		})
		if err != nil {
			return nil, fmt.Errorf("create postgres storage: %w", err)
		}
		return pgStorage, nil
	}

	return storage.NewConsoleStorage(logger), nil
}

func setupArbitrageDetector(
	cfg *config.Config,
	logger *zap.Logger,
	obManager *orderbook.Manager,
	discoveryService *discovery.Service,
	arbStorage arbitrage.Storage,
	cachedMetadataClient *markets.CachedMetadataClient,
) *arbitrage.Detector {
	return arbitrage.New(
		arbitrage.Config{
			MaxPriceSum:  cfg.ArbMaxPriceSum,
			MinTradeSize: cfg.ArbMinTradeSize,
			MaxTradeSize: cfg.ArbMaxTradeSize,
			TakerFee:     cfg.ArbTakerFee,
			Logger:       logger,
		},
		obManager,
		discoveryService,
		arbStorage,
		cachedMetadataClient,
	)
}

func setupExecutor(
	ctx context.Context,
	cfg *config.Config,
	logger *zap.Logger,
	arbDetector *arbitrage.Detector,
) (executor *execution.Executor, err error) {
	// Don't create executor in dry-run mode
	if cfg.ExecutionMode == "dry-run" {
		logger.Info("executor-disabled-dry-run-mode",
			zap.String("mode", cfg.ExecutionMode),
			zap.String("note", "opportunities will be detected and logged only"))
		return nil, nil
	}

	// Create circuit breaker if enabled
	var breaker *circuitbreaker.BalanceCircuitBreaker
	if cfg.CircuitBreakerEnabled {
		// Parse wallet address for balance checking
		privateKeyHex := os.Getenv("POLYMARKET_PRIVATE_KEY")
		if privateKeyHex == "" {
			logger.Warn("circuit-breaker-disabled-no-private-key",
				zap.String("note", "POLYMARKET_PRIVATE_KEY not set, circuit breaker disabled"))
		} else {
			// Parse private key to derive address
			privateKey, parseErr := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
			if parseErr != nil {
				logger.Warn("circuit-breaker-disabled-invalid-key",
					zap.Error(parseErr))
			} else {
				publicKey := privateKey.Public()
				publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
				if !ok {
					logger.Warn("circuit-breaker-disabled-key-cast-failed")
				} else {
					address := crypto.PubkeyToAddress(*publicKeyECDSA)

					// Create wallet client for balance checking
					// Use Polygon mainnet RPC endpoint - could be made configurable
					rpcURL := os.Getenv("POLYGON_RPC_URL")
					if rpcURL == "" {
						rpcURL = "https://polygon-rpc.com"
					}

					walletClient, walletErr := wallet.NewClient(rpcURL, logger)
					if walletErr != nil {
						logger.Warn("circuit-breaker-disabled-wallet-client-failed",
							zap.Error(walletErr))
					} else {
						// Create circuit breaker
						breaker, err = circuitbreaker.New(&circuitbreaker.Config{
							CheckInterval:   cfg.CircuitBreakerCheckInterval,
							TradeMultiplier: cfg.CircuitBreakerTradeMultiplier,
							MinAbsolute:     cfg.CircuitBreakerMinAbsolute,
							HysteresisRatio: cfg.CircuitBreakerHysteresisRatio,
							WalletClient:    walletClient,
							Address:         address,
							Logger:          logger,
						})
						if err != nil {
							return nil, fmt.Errorf("create circuit breaker: %w", err)
						}

						// Start background monitoring
						breaker.Start(ctx)

						logger.Info("circuit-breaker-enabled",
							zap.Duration("check_interval", cfg.CircuitBreakerCheckInterval),
							zap.Float64("trade_multiplier", cfg.CircuitBreakerTradeMultiplier),
							zap.Float64("min_absolute", cfg.CircuitBreakerMinAbsolute),
							zap.Float64("hysteresis_ratio", cfg.CircuitBreakerHysteresisRatio))
					}
				}
			}
		}
	}

	// Create OrderClient for live trading
	var orderClient *execution.OrderClient
	if cfg.ExecutionMode == "live" {
		// Read credentials from environment
		privateKey := os.Getenv("POLYMARKET_PRIVATE_KEY")
		if privateKey == "" {
			logger.Warn("order-client-not-configured-missing-private-key")
		} else {
			// Parse signature type, default to 0 (EOA)
			signatureType := 0
			if sigTypeStr := os.Getenv("POLYMARKET_SIGNATURE_TYPE"); sigTypeStr != "" {
				if parsed, parseErr := strconv.Atoi(sigTypeStr); parseErr == nil {
					signatureType = parsed
				}
			}

			orderClientCfg := &execution.OrderClientConfig{
				APIKey:        cfg.PolymarketAPIKey,
				Secret:        cfg.PolymarketSecret,
				Passphrase:    cfg.PolymarketPassphrase,
				PrivateKey:    privateKey,
				Address:       os.Getenv("POLYMARKET_ADDRESS"),
				ProxyAddress:  os.Getenv("POLYMARKET_PROXY_ADDRESS"), // Used as maker/funder for proxy/1271 signatures
				SignatureType: signatureType,
				Logger:        logger,
			}

			orderClient, err = execution.NewOrderClient(orderClientCfg)
			if err != nil {
				return nil, fmt.Errorf("create order client: %w", err)
			}

			logger.Info("order-client-configured",
				zap.String("mode", "live"),
				zap.String("address", orderClientCfg.Address))
		}
	}

	executor = execution.New(&execution.Config{
		Mode:               cfg.ExecutionMode,
		MaxPositionSize:    cfg.ExecutionMaxPositionSize,
		Logger:             logger,
		OpportunityChannel: arbDetector.OpportunityChan(),
		OrderClient:        orderClient,
		CircuitBreaker:     breaker,
		// Fill verification config
		AggressionTicks:  cfg.ExecutionAggressionTicks,
		FillTimeout:      cfg.ExecutionFillTimeout,
		FillRetryInitial: cfg.ExecutionFillRetryInitial,
		FillRetryMax:     cfg.ExecutionFillRetryMax,
		FillRetryMult:    cfg.ExecutionFillRetryMult,
		TakerFee:         cfg.ArbTakerFee,
	})

	return executor, nil
}

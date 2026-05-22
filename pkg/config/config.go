package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// warnf prints a warning to stderr for invalid configuration values.
func warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "WARN config: "+format+"\n", args...)
}

// Config holds all application configuration.
type Config struct {
	// Application
	LogLevel string
	HTTPPort string

	// Polymarket API
	PolymarketWSURL      string
	PolymarketGammaURL   string
	PolymarketAPIKey     string
	PolymarketSecret     string
	PolymarketPassphrase string

	// Market Discovery
	DiscoveryPollInterval time.Duration
	DiscoveryMarketLimit  int
	MaxMarketDuration     time.Duration // Only subscribe to markets expiring within this duration

	// Market Cleanup
	CleanupInterval time.Duration // How often cleanup command checks for stale markets

	// WebSocket
	WSPoolSize              int // Number of WebSocket connections (default: 20)
	WSDialTimeout           time.Duration
	WSPongTimeout           time.Duration
	WSPingInterval          time.Duration
	WSReconnectInitialDelay time.Duration
	WSReconnectMaxDelay     time.Duration
	WSReconnectBackoffMult  float64
	WSMessageBufferSize     int

	// Arbitrage Detection
	ArbMaxPriceSum       float64 // Maximum acceptable YES + NO price sum (lower = stricter)
	ArbMinTradeSize      float64
	ArbMaxTradeSize      float64
	ArbDetectionInterval time.Duration
	ArbMakerFee          float64
	ArbTakerFee          float64

	// Execution
	ExecutionMode            string
	ExecutionMaxPositionSize float64

	// Execution - Fill Verification
	ExecutionAggressionTicks  int           // Ticks above ask to place order
	ExecutionFillTimeout      time.Duration // Max wait for 100% fill
	ExecutionFillRetryInitial time.Duration // Initial backoff for fill queries
	ExecutionFillRetryMax     time.Duration // Max backoff between queries
	ExecutionFillRetryMult    float64       // Exponential backoff multiplier

	// Circuit Breaker
	CircuitBreakerEnabled         bool
	CircuitBreakerCheckInterval   time.Duration
	CircuitBreakerTradeMultiplier float64
	CircuitBreakerMinAbsolute     float64
	CircuitBreakerHysteresisRatio float64

	// Storage
	StorageMode  string // "postgres" or "console"
	PostgresHost string
	PostgresPort string
	PostgresUser string
	PostgresPass string
	PostgresDB   string
	PostgresSSL  string
}

// LoadFromEnv loads configuration from environment variables with defaults.
func LoadFromEnv() (*Config, error) {
	cfg := &Config{
		// Application defaults
		LogLevel: getEnvOrDefault("LOG_LEVEL", "info"),
		HTTPPort: getEnvOrDefault("HTTP_PORT", "8080"),

		// Polymarket API defaults
		PolymarketWSURL:      getEnvOrDefault("POLYMARKET_WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
		PolymarketGammaURL:   getEnvOrDefault("POLYMARKET_GAMMA_API_URL", "https://gamma-api.polymarket.com"),
		PolymarketAPIKey:     os.Getenv("POLYMARKET_API_KEY"),
		PolymarketSecret:     os.Getenv("POLYMARKET_SECRET"),
		PolymarketPassphrase: os.Getenv("POLYMARKET_PASSPHRASE"),

		// Market Discovery defaults
		DiscoveryPollInterval: getDurationOrDefault("DISCOVERY_POLL_INTERVAL", 30*time.Second),
		DiscoveryMarketLimit:  getIntOrDefault("DISCOVERY_MARKET_LIMIT", 2500),
		MaxMarketDuration:     getDurationOrDefault("ARB_MAX_MARKET_DURATION", 0), // 0 = unlimited

		// Market Cleanup defaults
		CleanupInterval: getDurationOrDefault("CLEANUP_CHECK_INTERVAL", 5*time.Minute),

		// WebSocket defaults
		WSPoolSize:              getIntOrDefault("WS_POOL_SIZE", 20),
		WSDialTimeout:           getDurationOrDefault("WS_DIAL_TIMEOUT", 10*time.Second),
		WSPongTimeout:           getDurationOrDefault("WS_PONG_TIMEOUT", 15*time.Second),
		WSPingInterval:          getDurationOrDefault("WS_PING_INTERVAL", 10*time.Second),
		WSReconnectInitialDelay: getDurationOrDefault("WS_RECONNECT_INITIAL_DELAY", 1*time.Second),
		WSReconnectMaxDelay:     getDurationOrDefault("WS_RECONNECT_MAX_DELAY", 30*time.Second),
		WSReconnectBackoffMult:  getFloat64OrDefault("WS_RECONNECT_BACKOFF_MULTIPLIER", 2.0),
		WSMessageBufferSize:     getIntOrDefault("WS_MESSAGE_BUFFER_SIZE", 100000),

		// Arbitrage defaults
		ArbMaxPriceSum:       getFloat64OrDefault("ARB_MAX_PRICE_SUM", 0.995),
		ArbMinTradeSize:      getFloat64OrDefault("ARB_MIN_TRADE_SIZE", 1.0),
		ArbMaxTradeSize:      getFloat64OrDefault("ARB_MAX_TRADE_SIZE", 2.0),
		ArbDetectionInterval: getDurationOrDefault("ARB_DETECTION_INTERVAL", 100*time.Millisecond),
		ArbMakerFee:          getFloat64OrDefault("ARB_MAKER_FEE", 0.0000), // 0% maker fee on Polymarket
		ArbTakerFee:          getFloat64OrDefault("ARB_TAKER_FEE", 0.0100), // 1% taker fee

		// Execution defaults
		ExecutionMode:            getEnvOrDefault("EXECUTION_MODE", "paper"),
		ExecutionMaxPositionSize: getFloat64OrDefault("EXECUTION_MAX_POSITION_SIZE", 1000.0),

		// Execution - Fill Verification defaults
		ExecutionAggressionTicks:  getIntOrDefault("EXECUTION_AGGRESSION_TICKS", 5),
		ExecutionFillTimeout:      getDurationOrDefault("EXECUTION_FILL_TIMEOUT", 30*time.Second),
		ExecutionFillRetryInitial: getDurationOrDefault("EXECUTION_FILL_RETRY_INITIAL", 2*time.Second),
		ExecutionFillRetryMax:     getDurationOrDefault("EXECUTION_FILL_RETRY_MAX", 16*time.Second),
		ExecutionFillRetryMult:    getFloat64OrDefault("EXECUTION_FILL_RETRY_MULTIPLIER", 2.0),

		// Circuit Breaker defaults
		CircuitBreakerEnabled:         getBoolOrDefault("CIRCUIT_BREAKER_ENABLED", true),
		CircuitBreakerCheckInterval:   getDurationOrDefault("CIRCUIT_BREAKER_CHECK_INTERVAL", 300*time.Second),
		CircuitBreakerTradeMultiplier: getFloat64OrDefault("CIRCUIT_BREAKER_TRADE_MULTIPLIER", 3.0),
		CircuitBreakerMinAbsolute:     getFloat64OrDefault("CIRCUIT_BREAKER_MIN_ABSOLUTE", 5.0),
		CircuitBreakerHysteresisRatio: getFloat64OrDefault("CIRCUIT_BREAKER_HYSTERESIS_RATIO", 1.5),

		// Storage defaults
		StorageMode:  getEnvOrDefault("STORAGE_MODE", "console"),
		PostgresHost: getEnvOrDefault("POSTGRES_HOST", "localhost"),
		PostgresPort: getEnvOrDefault("POSTGRES_PORT", "5432"),
		PostgresUser: getEnvOrDefault("POSTGRES_USER", "polymarket"),
		PostgresPass: getEnvOrDefault("POSTGRES_PASSWORD", "polymarket123"),
		PostgresDB:   getEnvOrDefault("POSTGRES_DB", "polymarket_arb"),
		PostgresSSL:  getEnvOrDefault("POSTGRES_SSLMODE", "disable"),
	}

	err := cfg.Validate()
	if err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

// Validate checks that configuration values are valid.
func (c *Config) Validate() (err error) {
	if c.HTTPPort == "" {
		return errors.New("HTTP_PORT cannot be empty")
	}

	if c.PolymarketWSURL == "" {
		return errors.New("POLYMARKET_WS_URL cannot be empty")
	}

	if c.PolymarketGammaURL == "" {
		return errors.New("POLYMARKET_GAMMA_API_URL cannot be empty")
	}

	if c.ArbMaxPriceSum <= 0 || c.ArbMaxPriceSum > 1.10 {
		return fmt.Errorf("ARB_MAX_PRICE_SUM must be between 0 and 1.10 (values > 1.0 for research mode), got %f", c.ArbMaxPriceSum)
	}

	if c.ExecutionMode != "paper" && c.ExecutionMode != "live" && c.ExecutionMode != "dry-run" {
		return fmt.Errorf("EXECUTION_MODE must be 'paper', 'live', or 'dry-run', got %q", c.ExecutionMode)
	}

	// Validate trade size configuration
	if c.ArbMinTradeSize <= 0 {
		return fmt.Errorf("ARB_MIN_TRADE_SIZE must be positive, got %f", c.ArbMinTradeSize)
	}

	if c.ArbMaxTradeSize <= 0 {
		return fmt.Errorf("ARB_MAX_TRADE_SIZE must be positive, got %f", c.ArbMaxTradeSize)
	}

	if c.ArbMaxTradeSize < c.ArbMinTradeSize {
		return fmt.Errorf("ARB_MAX_TRADE_SIZE (%f) must be >= ARB_MIN_TRADE_SIZE (%f)",
			c.ArbMaxTradeSize, c.ArbMinTradeSize)
	}

	// Validate market filtering configuration
	if c.MaxMarketDuration < 0 {
		return fmt.Errorf("ARB_MAX_MARKET_DURATION must be non-negative (0 = unlimited), got %s", c.MaxMarketDuration)
	}

	if c.DiscoveryMarketLimit < 0 {
		return fmt.Errorf("DISCOVERY_MARKET_LIMIT must be non-negative (0 = unlimited), got %d", c.DiscoveryMarketLimit)
	}

	// Validate WebSocket pool configuration
	if c.WSPoolSize < 1 {
		return fmt.Errorf("WS_POOL_SIZE must be at least 1, got %d", c.WSPoolSize)
	}

	if c.WSPoolSize > 20 {
		return fmt.Errorf("WS_POOL_SIZE must not exceed 20, got %d", c.WSPoolSize)
	}

	// Validate cleanup configuration
	if c.CleanupInterval <= 0 {
		return fmt.Errorf("CLEANUP_CHECK_INTERVAL must be positive, got %s", c.CleanupInterval)
	}

	return nil
}

func getEnvOrDefault(key string, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func getIntOrDefault(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intVal, err := strconv.Atoi(value)
	if err != nil {
		warnf("%s=%q is not a valid integer, using default %d", key, value, defaultValue)
		return defaultValue
	}

	return intVal
}

func getFloat64OrDefault(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		warnf("%s=%q is not a valid float, using default %g", key, value, defaultValue)
		return defaultValue
	}

	return floatVal
}

func getDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		warnf("%s=%q is not a valid duration, using default %s", key, value, defaultValue)
		return defaultValue
	}

	return duration
}

func getBoolOrDefault(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	boolVal, err := strconv.ParseBool(value)
	if err != nil {
		warnf("%s=%q is not a valid bool, using default %v", key, value, defaultValue)
		return defaultValue
	}

	return boolVal
}

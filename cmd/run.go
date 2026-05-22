package cmd

import (
	"fmt"

	"github.com/joho/godotenv"
	"github.com/mselser95/polymarket-arb/internal/app"
	"github.com/mselser95/polymarket-arb/pkg/config"
	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals // Cobra boilerplate
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Start the arbitrage bot",
	Long: `Starts the Polymarket arbitrage bot, which will:
1. Discover new markets from the Gamma API
2. Subscribe to their orderbooks via WebSocket
3. Detect arbitrage opportunities (YES bid + NO bid < 1.0)
4. Execute trades in paper trading mode

Use --single-market to track only one market for debugging.`,
	RunE: runBot,
}

//nolint:gochecknoinits // Cobra boilerplate
func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP("single-market", "s", "", "Track only a single market by slug (for debugging)")
}

func runBot(cmd *cobra.Command, args []string) error {
	// Load .env if present. Environment variables already set by the shell
	// still take precedence, matching godotenv.Load() behavior.
	_ = godotenv.Load()

	// Load config
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Create logger
	logger, err := config.NewLogger()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer func() {
		_ = logger.Sync()
	}()

	// Get flags
	singleMarket, _ := cmd.Flags().GetString("single-market")

	// Create app with options
	opts := &app.Options{
		SingleMarket: singleMarket,
	}

	application, err := app.New(cfg, logger, opts)
	if err != nil {
		return fmt.Errorf("create app: %w", err)
	}

	// Run app
	err = application.Run()
	if err != nil {
		return fmt.Errorf("run app: %w", err)
	}

	return nil
}

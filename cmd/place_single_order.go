package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/mselser95/polymarket-arb/internal/execution"
	"github.com/mselser95/polymarket-arb/pkg/types"
)

//nolint:gochecknoglobals // Cobra boilerplate
var placeSingleOrderCmd = &cobra.Command{
	Use:   "place-single-order",
	Short: "Place a single FOK order by token ID",
	Long:  "Places a single Fill-or-Kill order for a given token ID. Used by expiry-sniper.",
	RunE:  runPlaceSingleOrder,
}

//nolint:gochecknoglobals // Cobra boilerplate
var (
	singleTokenID string
	singlePrice   float64
	singleTokens  float64 // number of tokens to buy/sell
	singleSide    string
)

//nolint:gochecknoinits // Cobra boilerplate
func init() {
	rootCmd.AddCommand(placeSingleOrderCmd)
	placeSingleOrderCmd.Flags().StringVar(&singleTokenID, "token-id", "", "Token ID to trade")
	placeSingleOrderCmd.Flags().Float64Var(&singlePrice, "price", 0, "Order price (0.01 - 0.99)")
	placeSingleOrderCmd.Flags().Float64Var(&singleTokens, "tokens", 0, "Number of tokens (size/price)")
	placeSingleOrderCmd.Flags().StringVar(&singleSide, "side", "BUY", "Order side: BUY or SELL")
	_ = placeSingleOrderCmd.MarkFlagRequired("token-id")
	_ = placeSingleOrderCmd.MarkFlagRequired("price")
	_ = placeSingleOrderCmd.MarkFlagRequired("tokens")
}

func runPlaceSingleOrder(_ *cobra.Command, _ []string) error {
	_ = godotenv.Load()

	cfg, err := loadPlaceOrdersConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	orderCfg := &execution.OrderClientConfig{
		APIKey:        cfg.APIKey,
		Secret:        cfg.Secret,
		Passphrase:    cfg.Passphrase,
		PrivateKey:    cfg.PrivateKey,
		SignatureType: int(cfg.SignatureType),
		Logger:        logger,
	}

	client, err := execution.NewOrderClient(orderCfg)
	if err != nil {
		return fmt.Errorf("create order client: %w", err)
	}

	outcome := types.OutcomeOrderParams{
		TokenID:  singleTokenID,
		Price:    singlePrice,
		TickSize: 0.01,
	}

	resps, err := client.PlaceOrdersMultiOutcome(context.Background(), []types.OutcomeOrderParams{outcome}, singleTokens)
	if err != nil {
		return fmt.Errorf("place order: %w", err)
	}

	result := map[string]any{
		"success":  false,
		"order_id": "",
		"status":   "",
		"error":    "",
	}
	if len(resps) > 0 && resps[0] != nil {
		result["order_id"] = resps[0].OrderID
		result["status"] = resps[0].Status
		result["error"] = resps[0].ErrorMsg
		result["success"] = resps[0].ErrorMsg == ""
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(result)
}

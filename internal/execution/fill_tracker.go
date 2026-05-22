package execution

import (
	"context"
	"fmt"
	"time"

	"github.com/mselser95/polymarket-arb/pkg/types"
	"go.uber.org/zap"
)

// FillTracker verifies order fills with exponential backoff.
type FillTracker struct {
	orderClient    OrderGetter
	logger         *zap.Logger
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffMult    float64
	fillTimeout    time.Duration
}

// FillTrackerConfig holds configuration for fill verification.
type FillTrackerConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffMult    float64
	FillTimeout    time.Duration
}

// NewFillTracker creates a new FillTracker instance.
func NewFillTracker(
	orderClient OrderGetter,
	logger *zap.Logger,
	cfg *FillTrackerConfig,
) *FillTracker {
	return &FillTracker{
		orderClient:    orderClient,
		logger:         logger,
		initialBackoff: cfg.InitialBackoff,
		maxBackoff:     cfg.MaxBackoff,
		backoffMult:    cfg.BackoffMult,
		fillTimeout:    cfg.FillTimeout,
	}
}

// VerifyFills checks if all orders are 100% filled with exponential backoff.
func (ft *FillTracker) VerifyFills(
	ctx context.Context,
	orderIDs []string,
	outcomes []string,
	expectedSizes []float64,
) (fillStatuses []types.FillStatus, err error) {
	if len(orderIDs) != len(outcomes) || len(orderIDs) != len(expectedSizes) {
		err = fmt.Errorf("mismatched lengths: %d orderIDs, %d outcomes, %d sizes",
			len(orderIDs), len(outcomes), len(expectedSizes))
		return fillStatuses, err
	}

	startTime := time.Now()
	timeout := time.NewTimer(ft.fillTimeout)
	defer timeout.Stop()

	// Initialize fill statuses
	fillStatuses = make([]types.FillStatus, len(orderIDs))
	for i := range fillStatuses {
		fillStatuses[i] = types.FillStatus{
			OrderID:      orderIDs[i],
			Outcome:      outcomes[i],
			OriginalSize: expectedSizes[i],
			FullyFilled:  false,
		}
	}

	backoff := ft.initialBackoff
	attempt := 1

	for {
		// Check if all orders are filled
		allFilled := true
		for i := range fillStatuses {
			if fillStatuses[i].FullyFilled {
				continue // Already verified
			}

			// Query order status
			orderResp, queryErr := ft.orderClient.GetOrder(ctx, orderIDs[i])
			if queryErr != nil {
				// Log error but continue retrying (transient errors)
				ft.logger.Warn("order-query-failed-retrying",
					zap.String("order-id", orderIDs[i]),
					zap.Error(queryErr),
					zap.Int("attempt", attempt))
				allFilled = false
				continue
			}

			// Update fill status
			fillStatuses[i].Status = orderResp.Status
			fillStatuses[i].SizeFilled = orderResp.SizeFilled
			fillStatuses[i].ActualPrice = orderResp.Price
			fillStatuses[i].VerifiedAt = time.Now()

			// Check if fully filled (with small tolerance for floating point)
			tolerance := 0.001
			if orderResp.SizeFilled >= orderResp.Size-tolerance {
				fillStatuses[i].FullyFilled = true
				ft.logger.Info("order-fully-filled",
					zap.String("order-id", orderIDs[i]),
					zap.String("outcome", outcomes[i]),
					zap.Float64("size-filled", orderResp.SizeFilled),
					zap.Float64("actual-price", orderResp.Price),
					zap.Duration("duration", time.Since(startTime)))
			} else {
				allFilled = false
				ft.logger.Debug("order-not-yet-filled",
					zap.String("order-id", orderIDs[i]),
					zap.String("outcome", outcomes[i]),
					zap.Float64("size-filled", orderResp.SizeFilled),
					zap.Float64("size-expected", orderResp.Size),
					zap.String("status", orderResp.Status))
			}
		}

		if allFilled {
			ft.logger.Info("all-orders-fully-filled",
				zap.Int("order-count", len(orderIDs)),
				zap.Duration("total-duration", time.Since(startTime)),
				zap.Int("attempts", attempt))
			return fillStatuses, nil
		}

		// Wait with exponential backoff
		select {
		case <-timeout.C:
			// Timeout reached
			ft.logger.Warn("fill-verification-timeout",
				zap.Int("order-count", len(orderIDs)),
				zap.Duration("timeout", ft.fillTimeout),
				zap.Int("attempts", attempt))

			// Mark unfilled orders with error
			for i := range fillStatuses {
				if !fillStatuses[i].FullyFilled {
					fillStatuses[i].Error = fmt.Errorf("fill verification timeout after %s", ft.fillTimeout)
				}
			}
			return fillStatuses, nil

		case <-ctx.Done():
			// Context canceled
			ft.logger.Warn("fill-verification-canceled",
				zap.Error(ctx.Err()),
				zap.Int("attempts", attempt))
			return fillStatuses, ctx.Err()

		case <-time.After(backoff):
			// Continue to next attempt
			attempt++
			ft.logger.Debug("fill-verification-retry",
				zap.Int("attempt", attempt),
				zap.Duration("backoff", backoff))

			// Exponential backoff with cap
			backoff = time.Duration(float64(backoff) * ft.backoffMult)
			if backoff > ft.maxBackoff {
				backoff = ft.maxBackoff
			}
		}
	}
}

package execution

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/polymarket/go-order-utils/pkg/builder"
	"github.com/polymarket/go-order-utils/pkg/model"
	"go.uber.org/zap"

	"github.com/mselser95/polymarket-arb/pkg/types"
)

// OrderPlacer defines the interface for placing orders on the CLOB.
// CRITICAL: tokenCount must be the SAME for all outcomes in arbitrage trades.
// This ensures we own complete "sets" - exactly one outcome wins and pays $1.00 per token.
type OrderPlacer interface {
	// PlaceOrdersMultiOutcome places orders for multiple outcomes with the same token count.
	// The tokenCount parameter applies to ALL outcomes (arbitrage requirement).
	PlaceOrdersMultiOutcome(
		ctx context.Context,
		outcomes []types.OutcomeOrderParams,
		tokenCount float64, // SAME count for ALL outcomes
	) ([]*types.OrderSubmissionResponse, error)
}

// OrderClient handles order submission to Polymarket CLOB
type OrderClient struct {
	apiKey        string
	secret        string
	passphrase    string
	privateKey    *ecdsa.PrivateKey
	address       string // EOA address (signer)
	proxyAddress  string // Proxy address (maker/funder)
	signatureType model.SignatureType
	orderBuilder  builder.ExchangeOrderBuilder
	logger        *zap.Logger
}

// OrderGetter can query the status of a submitted order.
// Separated from OrderPlacer so mocks can implement placement without fill verification.
type OrderGetter interface {
	GetOrder(ctx context.Context, orderID string) (*types.OrderQueryResponse, error)
}

// Compile-time checks that OrderClient implements both interfaces
var _ OrderPlacer = (*OrderClient)(nil)
var _ OrderGetter = (*OrderClient)(nil)

// OrderClientConfig holds configuration for the order client
type OrderClientConfig struct {
	APIKey        string
	Secret        string
	Passphrase    string
	PrivateKey    string
	Address       string
	ProxyAddress  string
	SignatureType int
	Logger        *zap.Logger
}

// OrderInfo represents an open order from GET /data/orders
type OrderInfo struct {
	OrderID      string `json:"id"`     // API uses "id" not "order_id"
	Market       string `json:"market"` // Market ID (conditionID)
	Side         string `json:"side"`   // BUY/SELL
	Price        string `json:"price"`
	OriginalSize string `json:"original_size"`
	Status       string `json:"status"`   // LIVE/RESTING
	AssetID      string `json:"asset_id"` // Token ID
	Outcome      string `json:"outcome"`  // Outcome name (Yes/No/candidate name)
}

// OpenOrdersResponse represents the wrapper response from GET /data/orders
type OpenOrdersResponse struct {
	Data       []OrderInfo `json:"data"`
	NextCursor string      `json:"next_cursor"`
	Limit      int         `json:"limit"`
	Count      int         `json:"count"`
}

// CancelAllResult represents response from DELETE /cancel-all
type CancelAllResult struct {
	Canceled    []string          `json:"canceled"`
	NotCanceled map[string]string `json:"not_canceled"`
}

// NewOrderClient creates a new order client
func NewOrderClient(cfg *OrderClientConfig) (*OrderClient, error) {
	// Parse private key
	privateKey, err := crypto.HexToECDSA(strings.TrimPrefix(cfg.PrivateKey, "0x"))
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}

	// Derive EOA address if not provided
	address := cfg.Address
	if address == "" {
		publicKey := privateKey.Public()
		publicKeyECDSA, _ := publicKey.(*ecdsa.PublicKey)
		address = crypto.PubkeyToAddress(*publicKeyECDSA).Hex()
	}

	chainID := big.NewInt(137) // Polygon mainnet
	orderBuilder := builder.NewExchangeOrderBuilderImpl(chainID, nil)

	return &OrderClient{
		apiKey:        cfg.APIKey,
		secret:        cfg.Secret,
		passphrase:    cfg.Passphrase,
		privateKey:    privateKey,
		address:       address,
		proxyAddress:  cfg.ProxyAddress,
		signatureType: model.SignatureType(cfg.SignatureType),
		orderBuilder:  orderBuilder,
		logger:        cfg.Logger,
	}, nil
}

// GetMakerAddress returns the maker address (proxy if set, otherwise EOA).
func (c *OrderClient) GetMakerAddress() (makerAddress string) {
	if c.proxyAddress != "" {
		return c.proxyAddress
	}
	return c.address
}

// GetSignerAddress returns the signer address (always the EOA).
func (c *OrderClient) GetSignerAddress() (signerAddress string) {
	return c.address
}

// GetSignatureType returns the signature type.
func (c *OrderClient) GetSignatureType() (signatureType model.SignatureType) {
	return c.signatureType
}

// PlaceSingleOrder places a single order with the given OrderData.
// This method is useful for closing positions or placing standalone orders.
func (c *OrderClient) PlaceSingleOrder(
	ctx context.Context,
	orderData *model.OrderData,
) (resp *types.OrderSubmissionResponse, err error) {
	// Build and sign the order
	signedOrder, err := c.orderBuilder.BuildSignedOrder(c.privateKey, orderData, model.CTFExchange)
	if err != nil {
		return nil, fmt.Errorf("build order: %w", err)
	}

	// Convert Side to string for logging
	sideStr := "BUY"
	if orderData.Side == model.SELL {
		sideStr = "SELL"
	}

	c.logger.Info("single-order-built",
		zap.String("maker", orderData.Maker),
		zap.String("signer", orderData.Signer),
		zap.String("token_id", orderData.TokenId),
		zap.String("side", sideStr))

	// Submit the order
	resp, err = c.submitOrder(ctx, signedOrder)
	if err != nil {
		return nil, fmt.Errorf("submit order: %w", err)
	}

	return resp, nil
}

// PlaceOrdersBatch places YES and NO orders atomically using the batch endpoint.
// This method is for explicit binary-only usage (2 outcomes: YES and NO tokens).
// For multi-outcome markets (3+ outcomes), use PlaceOrdersMultiOutcome instead.
// Both orders are submitted in a single API call for atomic execution.
// The size parameter specifies the number of tokens to buy for each outcome (not USD amount).
func (c *OrderClient) PlaceOrdersBatch(
	ctx context.Context,
	yesTokenID string,
	noTokenID string,
	size float64,
	yesPrice float64,
	noPrice float64,
	yesTickSize float64,
	yesMinSize float64,
	noTickSize float64,
	noMinSize float64,
) (yesResp *types.OrderSubmissionResponse, noResp *types.OrderSubmissionResponse, err error) {
	// For EOA, maker and signer are the same address.
	// For proxy/1271 signatures, maker/funder is the proxy address while signer remains the EOA.
	makerAddress := c.GetMakerAddress()
	signerAddress := c.GetSignerAddress()

	// Get rounding precision for each token
	yesSizePrecision, yesAmountPrecision := getRoundingConfig(yesTickSize)
	noSizePrecision, noAmountPrecision := getRoundingConfig(noTickSize)

	// size parameter is already in tokens (matches Python client behavior)
	yesTakerTokens := roundAmount(size, yesSizePrecision)
	noTakerTokens := roundAmount(size, noSizePrecision)

	// Validate against minimums
	if yesTakerTokens < yesMinSize {
		err = fmt.Errorf("YES order size %.2f below minimum %.2f tokens", yesTakerTokens, yesMinSize)
		return yesResp, noResp, err
	}
	if noTakerTokens < noMinSize {
		err = fmt.Errorf("NO order size %.2f below minimum %.2f tokens", noTakerTokens, noMinSize)
		return yesResp, noResp, err
	}

	// Build YES order with rounded amounts
	yesMakerUSD := roundAmount(yesTakerTokens*yesPrice, yesAmountPrecision)
	yesMakerAmount := usdToRawAmount(yesMakerUSD)
	yesTakerAmount := usdToRawAmount(yesTakerTokens)

	yesOrderData := &model.OrderData{
		Maker:         makerAddress,
		Taker:         "0x0000000000000000000000000000000000000000",
		TokenId:       yesTokenID,
		MakerAmount:   yesMakerAmount,
		TakerAmount:   yesTakerAmount,
		Side:          model.BUY,
		FeeRateBps:    "0",
		Nonce:         "0",
		Signer:        signerAddress,
		Expiration:    "0",
		SignatureType: c.signatureType,
	}

	yesSignedOrder, err := c.orderBuilder.BuildSignedOrder(c.privateKey, yesOrderData, model.CTFExchange)
	if err != nil {
		err = fmt.Errorf("build YES order: %w", err)
		return yesResp, noResp, err
	}

	// Build NO order with rounded amounts
	noMakerUSD := roundAmount(noTakerTokens*noPrice, noAmountPrecision)
	noMakerAmount := usdToRawAmount(noMakerUSD)
	noTakerAmount := usdToRawAmount(noTakerTokens)

	noOrderData := &model.OrderData{
		Maker:         makerAddress,
		Taker:         "0x0000000000000000000000000000000000000000",
		TokenId:       noTokenID,
		MakerAmount:   noMakerAmount,
		TakerAmount:   noTakerAmount,
		Side:          model.BUY,
		FeeRateBps:    "0",
		Nonce:         "0",
		Signer:        signerAddress,
		Expiration:    "0",
		SignatureType: c.signatureType,
	}

	noSignedOrder, err := c.orderBuilder.BuildSignedOrder(c.privateKey, noOrderData, model.CTFExchange)
	if err != nil {
		err = fmt.Errorf("build NO order: %w", err)
		return yesResp, noResp, err
	}

	c.logger.Info("batch-orders-built",
		zap.String("maker", makerAddress),
		zap.String("signer", signerAddress),
		zap.Float64("size", size))

	// DEBUG: Print YES order structure for comparison
	c.logger.Info("GO-YES-ORDER-STRUCTURE",
		zap.Int64("salt", yesSignedOrder.Salt.Int64()),
		zap.String("maker", yesSignedOrder.Maker.Hex()),
		zap.String("signer", yesSignedOrder.Signer.Hex()),
		zap.String("taker", yesSignedOrder.Taker.Hex()),
		zap.String("tokenId", yesSignedOrder.TokenId.String()),
		zap.String("makerAmount", yesSignedOrder.MakerAmount.String()),
		zap.String("takerAmount", yesSignedOrder.TakerAmount.String()),
		zap.Int64("side", yesSignedOrder.Side.Int64()),
		zap.String("feeRateBps", yesSignedOrder.FeeRateBps.String()),
		zap.String("nonce", yesSignedOrder.Nonce.String()),
		zap.String("expiration", yesSignedOrder.Expiration.String()),
		zap.Int64("signatureType", yesSignedOrder.SignatureType.Int64()),
		zap.String("signature", fmt.Sprintf("0x%x", yesSignedOrder.Signature)))

	// Convert signed orders to JSON format
	yesOrderJSON := c.convertToOrderJSON(yesSignedOrder)
	noOrderJSON := c.convertToOrderJSON(noSignedOrder)

	// Create batch request
	batchReq := types.BatchOrderRequest{
		{Order: yesOrderJSON, Owner: c.apiKey, OrderType: "GTC"},
		{Order: noOrderJSON, Owner: c.apiKey, OrderType: "GTC"},
	}

	// Submit batch
	batchResp, err := c.submitBatchOrder(ctx, batchReq)
	if err != nil {
		return yesResp, noResp, err
	}

	// Validate we got 2 responses
	if len(batchResp) != 2 {
		err = fmt.Errorf("expected 2 responses, got %d", len(batchResp))
		return yesResp, noResp, err
	}

	yesResp = &batchResp[0]
	noResp = &batchResp[1]

	// Check for errors
	if !yesResp.Success {
		err = &types.OrderError{
			Code:    yesResp.ErrorMsg,
			Message: yesResp.ErrorMsg,
			OrderID: yesResp.OrderID,
			Side:    "YES",
		}
		return yesResp, noResp, err
	}
	if !noResp.Success {
		err = &types.OrderError{
			Code:    noResp.ErrorMsg,
			Message: noResp.ErrorMsg,
			OrderID: noResp.OrderID,
			Side:    "NO",
		}
		return yesResp, noResp, err
	}

	return yesResp, noResp, nil
}

// PlaceOrdersMultiOutcome places orders for N outcomes atomically using the batch endpoint.
// This method supports binary (2 outcomes) and multi-outcome (3+) markets.
// All orders are submitted in a single API call for atomic execution.
// The size parameter specifies the number of tokens to buy for each outcome (not USD amount).
func (c *OrderClient) PlaceOrdersMultiOutcome(
	ctx context.Context,
	outcomes []types.OutcomeOrderParams,
	size float64,
) (responses []*types.OrderSubmissionResponse, err error) {
	if len(outcomes) < 2 {
		return nil, fmt.Errorf("at least 2 outcomes required, got %d", len(outcomes))
	}

	// For EOA, maker and signer are the same address.
	// For proxy/1271 signatures, maker/funder is the proxy address while signer remains the EOA.
	makerAddress := c.GetMakerAddress()
	signerAddress := c.GetSignerAddress()

	// Build signed orders for each outcome
	batchReq := make(types.BatchOrderRequest, 0, len(outcomes))

	for i, outcome := range outcomes {
		// Get rounding precision
		sizePrecision, amountPrecision := getRoundingConfig(outcome.TickSize)

		// size parameter is already in tokens (matches Python client behavior)
		takerTokens := roundAmount(size, sizePrecision)

		// Validate against minimum
		if takerTokens < outcome.MinSize {
			return nil, fmt.Errorf("outcome %d: order size %.2f below minimum %.2f tokens",
				i, takerTokens, outcome.MinSize)
		}

		// Build order with rounded amounts
		makerUSD := roundAmount(takerTokens*outcome.Price, amountPrecision)
		makerAmount := usdToRawAmount(makerUSD)
		takerAmount := usdToRawAmount(takerTokens)

		orderData := &model.OrderData{
			Maker:         makerAddress,
			Taker:         "0x0000000000000000000000000000000000000000",
			TokenId:       outcome.TokenID,
			MakerAmount:   makerAmount,
			TakerAmount:   takerAmount,
			Side:          model.BUY,
			FeeRateBps:    "0",
			Nonce:         "0",
			Signer:        signerAddress,
			Expiration:    "0",
			SignatureType: c.signatureType,
		}

		signedOrder, err := c.orderBuilder.BuildSignedOrder(c.privateKey, orderData, model.CTFExchange)
		if err != nil {
			return nil, fmt.Errorf("build order %d: %w", i, err)
		}

		// DEBUG: Print first order (YES) structure for comparison
		if i == 0 {
			c.logger.Info("GO-YES-ORDER-STRUCTURE",
				zap.Int64("salt", signedOrder.Salt.Int64()),
				zap.String("maker", signedOrder.Maker.Hex()),
				zap.String("signer", signedOrder.Signer.Hex()),
				zap.String("taker", signedOrder.Taker.Hex()),
				zap.String("tokenId", signedOrder.TokenId.String()),
				zap.String("makerAmount", signedOrder.MakerAmount.String()),
				zap.String("takerAmount", signedOrder.TakerAmount.String()),
				zap.Int64("side", signedOrder.Side.Int64()),
				zap.String("feeRateBps", signedOrder.FeeRateBps.String()),
				zap.String("nonce", signedOrder.Nonce.String()),
				zap.String("expiration", signedOrder.Expiration.String()),
				zap.Int64("signatureType", signedOrder.SignatureType.Int64()),
				zap.String("signature", fmt.Sprintf("0x%x", signedOrder.Signature)))
		}

		// Convert to JSON and add to batch
		orderJSON := c.convertToOrderJSON(signedOrder)
		batchReq = append(batchReq, types.OrderSubmissionRequest{
			Order:     orderJSON,
			Owner:     c.apiKey,
			OrderType: "GTC",
		})
	}

	c.logger.Info("multi-outcome-batch-orders-built",
		zap.String("maker", makerAddress),
		zap.String("signer", signerAddress),
		zap.Int("outcome-count", len(outcomes)),
		zap.Float64("size", size))

	// Submit batch
	batchResp, err := c.submitBatchOrder(ctx, batchReq)
	if err != nil {
		return nil, fmt.Errorf("submit batch: %w", err)
	}

	// Validate we got N responses
	if len(batchResp) != len(outcomes) {
		return nil, fmt.Errorf("expected %d responses, got %d", len(outcomes), len(batchResp))
	}

	// Convert to response pointers
	responses = make([]*types.OrderSubmissionResponse, len(batchResp))
	for i := range batchResp {
		responses[i] = &batchResp[i]
	}

	// Check for any errors
	var errMsgs []string
	for i, resp := range responses {
		if !resp.Success {
			errMsgs = append(errMsgs, fmt.Sprintf("outcome %d: %s", i, resp.ErrorMsg))
		}
	}

	if len(errMsgs) > 0 {
		return responses, &types.OrderError{
			Code:    "BATCH_ERROR",
			Message: strings.Join(errMsgs, "; "),
		}
	}

	return responses, nil
}

// convertToOrderJSON converts a signed order to JSON format
func (c *OrderClient) convertToOrderJSON(order *model.SignedOrder) types.SignedOrderJSON {
	sideStr := "BUY"
	if order.Side.Uint64() == uint64(model.SELL) {
		sideStr = "SELL"
	}

	return types.SignedOrderJSON{
		Salt:          order.Salt.Int64(),
		Maker:         order.Maker.Hex(),
		Signer:        order.Signer.Hex(),
		Taker:         order.Taker.Hex(),
		TokenID:       order.TokenId.String(),
		MakerAmount:   order.MakerAmount.String(),
		TakerAmount:   order.TakerAmount.String(),
		Side:          sideStr,
		Expiration:    order.Expiration.String(),
		Nonce:         order.Nonce.String(),
		FeeRateBps:    order.FeeRateBps.String(),
		SignatureType: int(order.SignatureType.Int64()),
		Signature:     "0x" + common.Bytes2Hex(order.Signature),
	}
}

// submitBatchOrder submits a batch of orders to POST /orders endpoint
func (c *OrderClient) submitBatchOrder(
	ctx context.Context,
	req types.BatchOrderRequest,
) (resp types.BatchOrderResponse, err error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		err = fmt.Errorf("marshal batch request: %w", err)
		return resp, err
	}

	// Create HMAC signature
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	method := "POST"
	requestPath := "/orders" // Note: plural for batch endpoint

	signaturePayload := timestamp + method + requestPath + string(reqBody)

	// Decode secret using URL-safe base64
	secretBytes, err := base64.URLEncoding.DecodeString(c.secret)
	if err != nil {
		err = fmt.Errorf("decode secret: %w", err)
		return resp, err
	}

	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(signaturePayload))
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// Make request
	url := "https://clob.polymarket.com" + requestPath
	httpReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(reqBody))
	if err != nil {
		err = fmt.Errorf("create request: %w", err)
		return resp, err
	}

	// Set headers (same as single order)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("POLY_API_KEY", c.apiKey)
	httpReq.Header.Set("POLY_SIGNATURE", signature)
	httpReq.Header.Set("POLY_TIMESTAMP", timestamp)
	httpReq.Header.Set("POLY_PASSPHRASE", c.passphrase)
	httpReq.Header.Set("POLY_ADDRESS", c.address)

	// Log the request being sent (at DEBUG level)
	c.logger.Debug("submitting-batch-order-request",
		zap.String("url", url),
		zap.String("request-body", string(reqBody)))

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		err = fmt.Errorf("send request: %w", err)
		return resp, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return resp, err
	}

	// Log the raw response (at DEBUG level for now, will be useful for troubleshooting)
	c.logger.Debug("batch-order-api-response",
		zap.Int("status-code", httpResp.StatusCode),
		zap.String("response-body", string(body)))

	if httpResp.StatusCode != http.StatusOK && httpResp.StatusCode != http.StatusCreated {
		// Log error responses at ERROR level
		c.logger.Error("batch-order-api-error",
			zap.Int("status-code", httpResp.StatusCode),
			zap.String("response-body", string(body)))
		err = fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, string(body))
		return resp, err
	}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		c.logger.Error("failed-to-parse-batch-response",
			zap.Error(err),
			zap.String("response-body", string(body)))
		err = fmt.Errorf("parse batch response: %w\nBody: %s", err, string(body))
		return resp, err
	}

	// Log the parsed response structure (resp is already a slice)
	c.logger.Info("batch-order-submitted",
		zap.Int("order-count", len(resp)),
		zap.Bool("has-orders", len(resp) > 0))

	// Log each order result
	for i, order := range resp {
		c.logger.Info("batch-order-result",
			zap.Int("order-index", i),
			zap.String("order-id", order.OrderID),
			zap.String("status", order.Status),
			zap.Bool("success", order.Success),
			zap.String("error", order.ErrorMsg))
	}

	return resp, nil
}

// GetOrder queries the status of a specific order by ID.
func (c *OrderClient) GetOrder(
	ctx context.Context,
	orderID string,
) (resp *types.OrderQueryResponse, err error) {
	// Create HMAC signature for GET request
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	method := "GET"
	requestPath := "/order/" + orderID

	signaturePayload := timestamp + method + requestPath // Empty body for GET

	// Decode secret using URL-safe base64
	secretBytes, err := base64.URLEncoding.DecodeString(c.secret)
	if err != nil {
		err = fmt.Errorf("decode secret: %w", err)
		return resp, err
	}

	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(signaturePayload))
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// Make request
	url := "https://clob.polymarket.com" + requestPath
	httpReq, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		err = fmt.Errorf("create request: %w", err)
		return resp, err
	}

	// Set headers (same as POST requests)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("POLY_API_KEY", c.apiKey)
	httpReq.Header.Set("POLY_SIGNATURE", signature)
	httpReq.Header.Set("POLY_TIMESTAMP", timestamp)
	httpReq.Header.Set("POLY_PASSPHRASE", c.passphrase)
	httpReq.Header.Set("POLY_ADDRESS", c.address)

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		err = fmt.Errorf("send request: %w", err)
		return resp, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return resp, err
	}

	// Log response for debugging
	c.logger.Debug("get-order-api-response",
		zap.String("order-id", orderID),
		zap.Int("status-code", httpResp.StatusCode),
		zap.String("response-body", string(body)))

	if httpResp.StatusCode != http.StatusOK {
		// Log error responses at ERROR level
		c.logger.Error("get-order-api-error",
			zap.String("order-id", orderID),
			zap.Int("status-code", httpResp.StatusCode),
			zap.String("response-body", string(body)))
		err = fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, string(body))
		return resp, err
	}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		c.logger.Error("failed-to-parse-order-response",
			zap.String("order-id", orderID),
			zap.Error(err),
			zap.String("response-body", string(body)))
		err = fmt.Errorf("parse order response: %w\nBody: %s", err, string(body))
		return resp, err
	}

	return resp, nil
}

func (c *OrderClient) submitOrder(
	ctx context.Context,
	order *model.SignedOrder,
) (resp *types.OrderSubmissionResponse, err error) {
	// Convert to JSON format using helper method
	jsonOrder := c.convertToOrderJSON(order)

	// Wrap order in the required structure
	// Note: "owner" is the API key, not the maker address (per Python client)
	orderRequest := types.OrderSubmissionRequest{
		Order:     jsonOrder,
		Owner:     c.apiKey,
		OrderType: "GTC",
	}

	reqBody, err := json.Marshal(orderRequest)
	if err != nil {
		err = fmt.Errorf("marshal request: %w", err)
		return resp, err
	}

	// Create HMAC signature
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	method := "POST"
	requestPath := "/order"

	signaturePayload := timestamp + method + requestPath + string(reqBody)

	// Decode secret using URL-safe base64 (Python client uses urlsafe_b64decode)
	secretBytes, err := base64.URLEncoding.DecodeString(c.secret)
	if err != nil {
		err = fmt.Errorf("decode secret: %w", err)
		return resp, err
	}

	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(signaturePayload))
	// Encode signature using URL-safe base64 (Python client uses urlsafe_b64encode)
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// Make request
	url := "https://clob.polymarket.com" + requestPath
	req, err := http.NewRequestWithContext(ctx, method, url, strings.NewReader(string(reqBody)))
	if err != nil {
		err = fmt.Errorf("create request: %w", err)
		return resp, err
	}

	// POLY_ADDRESS header should be the EOA address (per Python client: signer.address())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_API_KEY", c.apiKey)
	req.Header.Set("POLY_SIGNATURE", signature)
	req.Header.Set("POLY_TIMESTAMP", timestamp)
	req.Header.Set("POLY_PASSPHRASE", c.passphrase)
	req.Header.Set("POLY_ADDRESS", c.address) // EOA address from private key

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("send request: %w", err)
		return resp, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return resp, err
	}

	if httpResp.StatusCode != http.StatusOK && httpResp.StatusCode != http.StatusCreated {
		err = fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, string(body))
		return resp, err
	}

	err = json.Unmarshal(body, &resp)
	if err != nil {
		err = fmt.Errorf("parse response: %w", err)
		return resp, err
	}

	return resp, nil
}

func usdToRawAmount(usd float64) string {
	rawAmount := int64(usd * 1000000)
	return fmt.Sprintf("%d", rawAmount)
}

// getRoundingConfig returns the precision for size and amount based on tick size
// Matches Python client's ROUNDING_CONFIG
func getRoundingConfig(tickSize float64) (sizePrecision int, amountPrecision int) {
	switch tickSize {
	case 0.1:
		return 2, 3 // size=2, amount=3
	case 0.01:
		return 2, 4 // size=2, amount=4
	case 0.001:
		return 2, 5 // size=2, amount=5
	case 0.0001:
		return 2, 6 // size=2, amount=6
	default:
		return 2, 4 // Default to 0.01 tick size
	}
}

// roundAmount rounds an amount to the specified number of decimal places
func roundAmount(value float64, decimals int) float64 {
	multiplier := math.Pow(10, float64(decimals))
	return math.Round(value*multiplier) / multiplier
}

// GetOpenOrders fetches all open orders for the authenticated user
func (c *OrderClient) GetOpenOrders(ctx context.Context) (orders []OrderInfo, err error) {
	method := "GET"
	requestPath := "/data/orders"
	body := ""

	// Build HMAC signature
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	signaturePayload := timestamp + method + requestPath + body

	// Decode secret using URL-safe base64
	secretBytes, err := base64.URLEncoding.DecodeString(c.secret)
	if err != nil {
		err = fmt.Errorf("decode secret: %w", err)
		return orders, err
	}

	// Generate HMAC-SHA256 signature
	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(signaturePayload))
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// Create request
	url := "https://clob.polymarket.com" + requestPath
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		err = fmt.Errorf("create request: %w", err)
		return orders, err
	}

	// Set authentication headers
	req.Header.Set("POLY_API_KEY", c.apiKey)
	req.Header.Set("POLY_SIGNATURE", signature)
	req.Header.Set("POLY_TIMESTAMP", timestamp)
	req.Header.Set("POLY_PASSPHRASE", c.passphrase)
	req.Header.Set("POLY_ADDRESS", c.address)

	c.logger.Debug("fetching-open-orders",
		zap.String("endpoint", requestPath))

	// Make request
	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("send request: %w", err)
		return orders, err
	}
	defer httpResp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return orders, err
	}

	// Check status code
	if httpResp.StatusCode != http.StatusOK {
		c.logger.Error("fetch-orders-api-error",
			zap.Int("status-code", httpResp.StatusCode),
			zap.String("response-body", string(respBody)))
		err = fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, string(respBody))
		return orders, err
	}

	// Log raw response for debugging
	c.logger.Debug("fetch-orders-raw-response",
		zap.String("body", string(respBody)))

	// Parse response (API returns wrapper with "data" field)
	var response OpenOrdersResponse
	err = json.Unmarshal(respBody, &response)
	if err != nil {
		c.logger.Error("parse-orders-error",
			zap.Error(err),
			zap.String("response-body", string(respBody)))
		err = fmt.Errorf("parse response: %w", err)
		return orders, err
	}

	orders = response.Data
	c.logger.Info("fetched-open-orders",
		zap.Int("count", len(orders)))

	return orders, nil
}

// CancelAllOrders cancels all open orders atomically via DELETE /cancel-all
func (c *OrderClient) CancelAllOrders(ctx context.Context) (result CancelAllResult, err error) {
	method := "DELETE"
	requestPath := "/cancel-all"
	body := ""

	// Build HMAC signature
	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	signaturePayload := timestamp + method + requestPath + body

	// Decode secret using URL-safe base64
	secretBytes, err := base64.URLEncoding.DecodeString(c.secret)
	if err != nil {
		err = fmt.Errorf("decode secret: %w", err)
		return result, err
	}

	// Generate HMAC-SHA256 signature
	h := hmac.New(sha256.New, secretBytes)
	h.Write([]byte(signaturePayload))
	signature := base64.URLEncoding.EncodeToString(h.Sum(nil))

	// Create request
	url := "https://clob.polymarket.com" + requestPath
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		err = fmt.Errorf("create request: %w", err)
		return result, err
	}

	// Set authentication headers
	req.Header.Set("POLY_API_KEY", c.apiKey)
	req.Header.Set("POLY_SIGNATURE", signature)
	req.Header.Set("POLY_TIMESTAMP", timestamp)
	req.Header.Set("POLY_PASSPHRASE", c.passphrase)
	req.Header.Set("POLY_ADDRESS", c.address)

	c.logger.Info("canceling-all-orders")

	// Make request
	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("send request: %w", err)
		return result, err
	}
	defer httpResp.Body.Close()

	// Read response
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return result, err
	}

	// Check status code
	if httpResp.StatusCode != http.StatusOK {
		c.logger.Error("cancel-all-api-error",
			zap.Int("status-code", httpResp.StatusCode),
			zap.String("response-body", string(respBody)))
		err = fmt.Errorf("API error (status %d): %s", httpResp.StatusCode, string(respBody))
		return result, err
	}

	// Parse response
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		err = fmt.Errorf("parse response: %w", err)
		return result, err
	}

	c.logger.Info("cancellation-completed",
		zap.Int("canceled", len(result.Canceled)),
		zap.Int("not-canceled", len(result.NotCanceled)))

	return result, nil
}

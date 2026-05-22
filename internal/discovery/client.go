package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mselser95/polymarket-arb/pkg/types"
	"go.uber.org/zap"
)

// Client is an HTTP client for the Polymarket Gamma API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *zap.Logger
}

// NewClient creates a new Gamma API client.
func NewClient(baseURL string, logger *zap.Logger) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
	}
}

const (
	// MaxBatchSize is the maximum number of markets to fetch per API request.
	// Based on Polymarket's official Python client implementation.
	MaxBatchSize = 100
)

// FetchActiveMarkets fetches active markets from the Gamma API with automatic pagination.
// If limit > MaxBatchSize, multiple requests are made and results are aggregated.
// orderBy specifies the field to sort by: "volume24hr", "createdAt", or "endDate".
func (c *Client) FetchActiveMarkets(ctx context.Context, limit int, offset int, orderBy string) (*types.MarketsResponse, error) {
	// Handle unlimited: 0 means fetch all available markets
	fetchAll := limit == 0

	// If requesting more than one batch, use pagination
	if limit > MaxBatchSize || fetchAll {
		return c.fetchWithPagination(ctx, limit, offset, orderBy)
	}

	// Single request for small limits
	return c.fetchSinglePage(ctx, limit, offset, orderBy)
}

// fetchSinglePage fetches a single page of markets without pagination.
func (c *Client) fetchSinglePage(ctx context.Context, limit int, offset int, orderBy string) (*types.MarketsResponse, error) {
	return c.fetchSinglePageWithFilter(ctx, limit, offset, orderBy, false)
}

// fetchSinglePageClosed fetches a single page of closed markets.
func (c *Client) fetchSinglePageClosed(ctx context.Context, limit int, offset int, orderBy string) (*types.MarketsResponse, error) {
	return c.fetchSinglePageWithFilter(ctx, limit, offset, orderBy, true)
}

// fetchSinglePageWithFilter fetches a single page of markets with configurable closed filter.
func (c *Client) fetchSinglePageWithFilter(ctx context.Context, limit int, offset int, orderBy string, closed bool) (*types.MarketsResponse, error) {
	if limit == 0 {
		limit = MaxBatchSize
	}

	endpoint := fmt.Sprintf("%s/markets", c.baseURL)

	// Build query parameters
	params := url.Values{}
	params.Add("closed", strconv.FormatBool(closed))
	params.Add("active", strconv.FormatBool(!closed))
	params.Add("limit", strconv.Itoa(limit))
	params.Add("offset", strconv.Itoa(offset))
	params.Add("order", orderBy)

	// For endDate, use ascending to get markets expiring soonest
	// For volume24hr and createdAt, use descending to get highest/newest
	if orderBy == "endDate" {
		params.Add("ascending", "true")
	} else {
		params.Add("ascending", "false")
	}

	requestURL := fmt.Sprintf("%s?%s", endpoint, params.Encode())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "polymarket-arb/1.0")

	c.logger.Debug("fetching-markets",
		zap.String("url", requestURL),
		zap.Int("limit", limit),
		zap.Int("offset", offset))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	// Gamma API returns a direct array, not wrapped in an object
	var markets []types.Market
	err = json.Unmarshal(body, &markets)
	if err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	// Wrap in response object for consistency
	marketsResp := &types.MarketsResponse{
		Data:   markets,
		Count:  len(markets),
		Limit:  limit,
		Offset: offset,
	}

	c.logger.Debug("fetched-markets",
		zap.Int("count", len(markets)))

	return marketsResp, nil
}

// fetchWithPagination fetches markets across multiple pages and aggregates results.
// Automatically handles pagination when limit > MaxBatchSize or limit == 0 (fetch all).
func (c *Client) fetchWithPagination(ctx context.Context, limit int, offset int, orderBy string) (*types.MarketsResponse, error) {
	var (
		allMarkets   []types.Market
		currentPage  = 0
		batchSize    = MaxBatchSize
		totalFetched = 0
		fetchAll     = limit == 0
	)

	c.logger.Debug("starting-paginated-fetch",
		zap.Int("requested-limit", limit),
		zap.Int("offset", offset),
		zap.Int("batch-size", batchSize),
		zap.Bool("fetch-all", fetchAll))

	for {
		// Calculate batch limit for this page
		var pageBatchSize int
		if fetchAll {
			pageBatchSize = batchSize
		} else {
			remaining := limit - totalFetched
			if remaining <= 0 {
				break
			}
			if remaining < batchSize {
				pageBatchSize = remaining
			} else {
				pageBatchSize = batchSize
			}
		}

		// Calculate offset for this page
		pageOffset := offset + (currentPage * batchSize)

		// Fetch page
		resp, err := c.fetchSinglePage(ctx, pageBatchSize, pageOffset, orderBy)
		if err != nil {
			return nil, fmt.Errorf("fetch page %d: %w", currentPage, err)
		}

		// Append results
		allMarkets = append(allMarkets, resp.Data...)
		totalFetched += len(resp.Data)

		c.logger.Debug("fetched-page",
			zap.Int("page", currentPage),
			zap.Int("markets", len(resp.Data)),
			zap.Int("total", totalFetched))

		// Stop if we got fewer results than requested (no more data)
		if len(resp.Data) < pageBatchSize {
			c.logger.Debug("pagination-complete-no-more-data",
				zap.Int("total-fetched", totalFetched))
			break
		}

		// Stop if we've reached the requested limit
		if !fetchAll && totalFetched >= limit {
			c.logger.Debug("pagination-complete-limit-reached",
				zap.Int("total-fetched", totalFetched),
				zap.Int("requested", limit))
			break
		}

		currentPage++
	}

	return &types.MarketsResponse{
		Data:   allMarkets,
		Count:  len(allMarkets),
		Limit:  limit,
		Offset: offset,
	}, nil
}

// FetchMarketBySlug fetches a single market by its slug.
// Note: The Gamma API doesn't support /markets/{slug}, only /markets/{id}.
// This function searches through both active and closed markets lists to find the matching slug.
func (c *Client) FetchMarketBySlug(ctx context.Context, slug string) (*types.Market, error) {
	// Gamma supports direct slug filtering. Use it first so low-volume
	// short-duration markets (for example BTC 5m/15m) are not missed by
	// the volume-sorted fallback search.
	market, err := c.fetchMarketBySlugDirect(ctx, slug)
	if err == nil {
		return market, nil
	}

	// Fallback: Search active markets (legacy behavior)
	market, err = c.searchMarketsBySlug(ctx, slug, false)
	if err == nil {
		return market, nil
	}

	// Fallback: Search closed markets
	market, err = c.searchMarketsBySlug(ctx, slug, true)
	if err == nil {
		return market, nil
	}

	return nil, fmt.Errorf("market not found in active or closed markets: %s", slug)
}

func (c *Client) fetchMarketBySlugDirect(ctx context.Context, slug string) (*types.Market, error) {
	endpoint := fmt.Sprintf("%s/markets", c.baseURL)

	params := url.Values{}
	params.Add("slug", slug)
	params.Add("limit", "1")

	requestURL := fmt.Sprintf("%s?%s", endpoint, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "polymarket-arb/1.0")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	var markets []types.Market
	if err := json.Unmarshal(body, &markets); err != nil {
		return nil, fmt.Errorf("unmarshal response: %w", err)
	}

	for i := range markets {
		if markets[i].Slug == slug {
			return &markets[i], nil
		}
	}

	return nil, fmt.Errorf("market not found by slug: %s", slug)
}

// searchMarketsBySlug searches for a market by slug in either active or closed markets.
func (c *Client) searchMarketsBySlug(ctx context.Context, slug string, closed bool) (*types.Market, error) {
	limit := 100
	offset := 0
	maxPages := 10 // Search up to 1000 markets

	for range maxPages {
		var resp *types.MarketsResponse
		var err error

		if closed {
			resp, err = c.fetchSinglePageClosed(ctx, limit, offset, "volume24hr")
		} else {
			resp, err = c.FetchActiveMarkets(ctx, limit, offset, "volume24hr")
		}

		if err != nil {
			return nil, fmt.Errorf("fetch markets: %w", err)
		}

		// Search for matching slug
		for i := range resp.Data {
			if resp.Data[i].Slug == slug {
				return &resp.Data[i], nil
			}
		}

		// If we got fewer markets than the limit, we've reached the end
		if len(resp.Data) < limit {
			break
		}

		offset += limit
	}

	return nil, fmt.Errorf("market not found: %s", slug)
}

// orderbookBid represents a bid in the orderbook.
type orderbookBid struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

// orderbookResponse represents orderbook data from CLOB API.
type orderbookResponse struct {
	Market string         `json:"market"`
	Bids   []orderbookBid `json:"bids"`
}

// FetchTokenBidPrice fetches the current best bid price for a specific token.
func (c *Client) FetchTokenBidPrice(ctx context.Context, tokenID string) (bidPrice float64, err error) {
	// Use CLOB API instead of Gamma API
	clobURL := "https://clob.polymarket.com"
	endpoint := fmt.Sprintf("%s/book?token_id=%s", clobURL, tokenID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "polymarket-arb/1.0")

	c.logger.Debug("fetching-token-orderbook",
		zap.String("url", endpoint),
		zap.String("token_id", tokenID))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read response body: %w", err)
	}

	var orderbook orderbookResponse
	err = json.Unmarshal(body, &orderbook)
	if err != nil {
		return 0, fmt.Errorf("unmarshal response: %w", err)
	}

	// Get best bid (first bid in the list)
	if len(orderbook.Bids) == 0 {
		return 0, fmt.Errorf("no bids available in orderbook")
	}

	bidPrice, err = strconv.ParseFloat(orderbook.Bids[0].Price, 64)
	if err != nil {
		return 0, fmt.Errorf("parse bid price: %w", err)
	}

	c.logger.Debug("found-bid-price",
		zap.String("token_id", tokenID),
		zap.Float64("bid_price", bidPrice))

	return bidPrice, nil
}

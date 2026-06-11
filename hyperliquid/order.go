package hyperliquid

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	hyperliquid "github.com/sonirico/go-hyperliquid"
	"github.com/vmihailenco/msgpack/v5"
)

// orderAction is our own order action struct that correctly includes the dex field.
type orderAction struct {
	Type     string      `json:"type"          msgpack:"type"`
	Dex      string      `json:"dex,omitempty" msgpack:"dex,omitempty"`
	Orders   []orderWire `json:"orders"        msgpack:"orders"`
	Grouping string      `json:"grouping"      msgpack:"grouping"`
}

type orderWire struct {
	Asset      int           `json:"a" msgpack:"a"`
	IsBuy      bool          `json:"b" msgpack:"b"`
	LimitPx    string        `json:"p" msgpack:"p"`
	Size       string        `json:"s" msgpack:"s"`
	ReduceOnly bool          `json:"r" msgpack:"r"`
	OrderType  orderWireType `json:"t" msgpack:"t"`
}

type orderWireType struct {
	Limit *orderWireTypeLimit `json:"limit,omitempty" msgpack:"limit,omitempty"`
}

type orderWireTypeLimit struct {
	Tif string `json:"tif" msgpack:"tif"`
}

// placeOrder places an order directly, bypassing the SDK's MarketOpen/MarketClose
// which don't set the Dex field on the order action.
func (c *Client) placeOrder(ctx context.Context, isBuy bool, sz float64, px float64, slippage float64, reduceOnly bool) (*OrderResult, error) {
	if c.exchange == nil {
		return nil, ErrNoExchange
	}

	asset, ok := c.exchange.Info().CoinToAsset(c.qualifiedCoin())
	if !ok {
		return nil, fmt.Errorf("coin %s not found in asset map", c.qualifiedCoin())
	}

	// Apply slippage to price.
	if isBuy {
		px *= (1 + slippage)
	} else {
		px *= (1 - slippage)
	}

	// Round size to allowed decimals. roundSz uses round-half-away-from-zero
	// so sub-half-tick sizes still resolve to 0 — refuse those rather than
	// submit a zero-size order; HL would reject with a confusing
	// "deserialize" error that we'd misclassify as a transport failure.
	sz = c.roundSz(sz)
	if sz <= 0 {
		c.logger.Warn("order skipped, size rounded to zero",
			"is_buy", isBuy,
			"reduce_only", reduceOnly,
		)
		return &OrderResult{Status: "no_change"}, nil
	}

	// Idempotency guard: if a logically-identical order was just submitted,
	// short-circuit and return the prior result. Defends against accidental
	// caller-level retries doubling a position. Key uses the wire-format
	// strings so anything that round-trips to the same payload collides.
	cacheKey := fmt.Sprintf("%d|%t|%t|%s|%s",
		asset, isBuy, reduceOnly, floatToStr(sz), floatToStr(px))
	if cached, ok := c.recentOrderLookup(cacheKey); ok {
		c.logger.Warn("duplicate order suppressed by recent-order cache",
			"key", cacheKey,
			"prior_status", cached.Status,
		)
		return cached, nil
	}

	action := orderAction{
		Type:     "order",
		Grouping: "na",
		Orders: []orderWire{
			{
				Asset:      asset,
				IsBuy:      isBuy,
				LimitPx:    floatToStr(px),
				Size:       floatToStr(sz),
				ReduceOnly: reduceOnly,
				OrderType:  orderWireType{Limit: &orderWireTypeLimit{Tif: "Ioc"}},
			},
		},
	}

	c.logger.Info("placing order",
		"dex", c.dex,
		"asset", asset,
		"is_buy", isBuy,
		"size", floatToStr(sz),
		"limit_px", floatToStr(px),
		"reduce_only", reduceOnly,
	)

	isMainnet := strings.Contains(c.acct.apiURL, "hyperliquid.xyz") && !strings.Contains(c.acct.apiURL, "testnet")
	nonce, err := c.acct.nonces.next()
	if err != nil {
		return nil, fmt.Errorf("get nonce: %w", err)
	}

	sig, err := hyperliquid.SignL1Action(c.acct.privateKey, action, "", nonce, nil, isMainnet)
	if err != nil {
		return nil, fmt.Errorf("sign order: %w", err)
	}

	// Capture the exact msgpack bytes that were hashed for the EIP-712
	// signature. The HL gateway re-derives this from the JSON `action` it
	// receives — if the two encodings disagree (field reorder, wrong type)
	// the signature recovers a different address and the order is rejected
	// or, more confusingly, accepted but interpreted differently. Log at
	// INFO so the next split-fill anomaly (TODO #5) is debuggable from the
	// journal without needing wire-capture. ~150 bytes/order, single line.
	if actionMsgpack, mErr := msgpack.Marshal(action); mErr == nil {
		c.logger.Info("order signed payload",
			"nonce", nonce,
			"action_msgpack_hex", hex.EncodeToString(actionMsgpack),
		)
	} else {
		c.logger.Warn("order msgpack encode failed (continuing)", "error", mErr)
	}

	payload := map[string]any{
		"action":    action,
		"nonce":     nonce,
		"signature": sig,
	}

	var resp struct {
		Status   string `json:"status"`
		Response struct {
			Type string `json:"type"`
			Data struct {
				Statuses []json.RawMessage `json:"statuses"`
			} `json:"data"`
		} `json:"response"`
	}

	if err := c.postExchange(ctx, payload, &resp); err != nil {
		return nil, fmt.Errorf("post order: %w", err)
	}

	if resp.Status != "ok" {
		return nil, fmt.Errorf("order rejected: %s", resp.Status)
	}

	if len(resp.Response.Data.Statuses) == 0 {
		return nil, fmt.Errorf("no order status returned")
	}

	// Log every status so split fills are visible in the journal — we only
	// parse Statuses[0], but HL can return multiple (e.g. partial fill +
	// rest cancelled, or the 18:21 mystery where a 0.127-coin IOC came
	// back as two fills totalling 2.651). Without this log the rest of
	// the response is invisible after the parse.
	for i, raw := range resp.Response.Data.Statuses {
		c.logger.Info("order response",
			"index", i,
			"status_raw", string(raw),
		)
	}

	result, parseErr := parseOrderStatus(resp.Response.Data.Statuses[0])
	if parseErr == nil && result != nil {
		c.recentOrderStore(cacheKey, result)
	}
	return result, parseErr
}

// recentOrderLookup returns a cached result for the given key if present and
// not expired. Caller must hold no locks.
func (c *Client) recentOrderLookup(key string) (*OrderResult, bool) {
	c.recentOrders.mu.Lock()
	defer c.recentOrders.mu.Unlock()
	entry, ok := c.recentOrders.entries[key]
	if !ok || time.Now().After(entry.expires) {
		return nil, false
	}
	return entry.result, true
}

// recentOrderStore caches a successful order result and opportunistically
// evicts expired entries (cheap O(N) sweep — N is tiny in practice since
// orders are at most one per tick).
func (c *Client) recentOrderStore(key string, result *OrderResult) {
	c.recentOrders.mu.Lock()
	defer c.recentOrders.mu.Unlock()
	now := time.Now()
	c.recentOrders.entries[key] = recentOrderEntry{
		result:  result,
		expires: now.Add(recentOrderTTL),
	}
	for k, e := range c.recentOrders.entries {
		if now.After(e.expires) {
			delete(c.recentOrders.entries, k)
		}
	}
}

func (c *Client) postExchange(ctx context.Context, payload any, result any) error {
	jsonBody, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	body, err := c.postExchangeOnce(ctx, jsonBody)
	// Retry once, but ONLY when the request provably never reached the
	// exchange (dial/DNS failure). Anything past that point is ambiguous —
	// a timeout mid-response may mean the order landed, and resubmitting
	// would risk doubling the position. Info reads get a full retry.Do;
	// writes get exactly this one unambiguous case.
	if err != nil && requestNeverSent(err) {
		c.logger.Warn("exchange request failed before sending; retrying once", "error", err)
		select {
		case <-time.After(exchangeRetryDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
		body, err = c.postExchangeOnce(ctx, jsonBody)
	}
	if err != nil {
		return err
	}

	return json.Unmarshal(body, result)
}

// exchangeRetryDelay paces the single never-sent retry in postExchange —
// long enough for a transient connection refusal to clear, short enough not
// to stall the tick.
const exchangeRetryDelay = 500 * time.Millisecond

// postExchangeOnce performs a single POST to /exchange and returns the
// response body on HTTP 200.
func (c *Client) postExchangeOnce(ctx context.Context, jsonBody []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", c.acct.apiURL+"/exchange", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.acct.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// requestNeverSent reports whether err means the HTTP request failed before
// any bytes reached the server — a dial-phase failure (connection refused,
// DNS lookup, no route). Those are the only write errors safe to retry
// blindly: the exchange cannot have seen the order.
func requestNeverSent(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

func parseOrderStatus(raw json.RawMessage) (*OrderResult, error) {
	// Try filled format: {"filled": {"totalSz": "0.129", "avgPx": "392.5", "oid": 123}}
	var filled struct {
		Filled *struct {
			TotalSz string `json:"totalSz"`
			AvgPx   string `json:"avgPx"`
			Oid     uint64 `json:"oid"`
		} `json:"filled"`
		Resting *struct {
			Oid uint64 `json:"oid"`
		} `json:"resting"`
		Error *string `json:"error"`
	}

	if err := json.Unmarshal(raw, &filled); err != nil {
		return nil, fmt.Errorf("parse order status: %w (%s)", err, string(raw))
	}

	res := &OrderResult{}
	if filled.Filled != nil {
		res.Status = "filled"
		res.FilledSz = decimalFromStr(filled.Filled.TotalSz)
		res.AvgPrice = decimalFromStr(filled.Filled.AvgPx)
		res.OrderID = filled.Filled.Oid
	} else if filled.Resting != nil {
		res.Status = "resting"
		res.OrderID = filled.Resting.Oid
	} else if filled.Error != nil {
		return nil, fmt.Errorf("order error: %s", *filled.Error)
	} else {
		// None of filled/resting/error present. Returning an empty-status
		// result here would be treated (and cached) as success by the
		// caller while the order's fate is unknown.
		return nil, fmt.Errorf("unrecognized order status (none of filled/resting/error): %s", string(raw))
	}

	return res, nil
}

// floatToStr formats a float for the Hyperliquid wire format.
// Rounds to 5 significant figures and strips trailing zeros.
func floatToStr(x float64) string {
	// Round to 5 significant figures (matching Python SDK's float_to_wire).
	if x == 0 {
		return "0"
	}
	magnitude := math.Floor(math.Log10(math.Abs(x)))
	factor := math.Pow(10, 4-magnitude) // 5 sig figs
	rounded := math.Round(x*factor) / factor

	s := strconv.FormatFloat(rounded, 'f', -1, 64)
	return s
}

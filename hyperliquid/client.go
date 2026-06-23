package hyperliquid

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Mikelle/tradekit/retry"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	hyperliquid "github.com/sonirico/go-hyperliquid"
)

// Account holds everything scoped to one Hyperliquid account/signing key,
// shared by every per-pair Client: HTTP transport, account address, signer,
// and — critically — the nonce store. HL accepts a signed action only with a
// nonce above the signer's high-water mark, so all order signing in the
// process MUST flow through this one monotonic counter; a second store on
// the same key would race and produce rejected (or worse, replayed) orders.
type Account struct {
	apiURL      string
	accountAddr string
	httpClient  *http.Client
	logger      *slog.Logger

	// Signing state (nil privateKey = read-only account).
	privateKey *ecdsa.PrivateKey
	nonces     *nonceStore
}

// NewAccount creates the shared account state. privateKeyHex may be empty
// for a read-only account (no exchange operations). nonceFile persists the
// last nonce across restarts; pass "" to skip persistence.
func NewAccount(apiURL, accountAddr, privateKeyHex, nonceFile string, logger *slog.Logger) (*Account, error) {
	if apiURL == "" {
		apiURL = "https://api.hyperliquid.xyz"
	}
	a := &Account{
		apiURL:      apiURL,
		accountAddr: accountAddr,
		// The rebalancer's tick context has no deadline, so without a client
		// timeout a single hung HL request would freeze the tick loop (and
		// every auto-hook behind it) indefinitely.
		httpClient: &http.Client{Timeout: httpTimeout},
		logger:     logger,
	}
	if privateKeyHex != "" {
		pk, err := parsePrivateKey(privateKeyHex)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		a.privateKey = pk
		nonces, err := newNonceStore(nonceFile)
		if err != nil {
			return nil, fmt.Errorf("init nonce store: %w", err)
		}
		a.nonces = nonces
	}
	return a, nil
}

// Close releases resources held by the HTTP client. Call once per Account —
// per-pair Clients share it.
func (a *Account) Close() {
	a.httpClient.CloseIdleConnections()
}

// Client wraps Hyperliquid API for one coin/dex pair on a shared Account.
// Info (read) operations use direct HTTP with retry.
// Exchange (write) operations sign in-house via the Account's key + nonces.
type Client struct {
	acct       *Account
	coin       string
	dex        string // optional: "xyz" for trade[XYZ] markets
	szDecimals int    // size decimal places for the configured coin
	logger     *slog.Logger

	// Exchange client for asset-id resolution and SDK calls (nil if the
	// account has no private key). Per pair: it's built from the dex's meta.
	exchange *hyperliquid.Exchange

	// recentOrders short-circuits a duplicate placeOrder submission within
	// recentOrderTTL of the prior one. Defends against accidental
	// caller-level retries doubling the position; the current call sites
	// don't retry today, but the protection is cheap enough to keep.
	// Per pair, not per account: the cache key includes the dex-scoped
	// integer asset id, which could collide across dexes.
	recentOrders struct {
		mu      sync.Mutex
		entries map[string]recentOrderEntry
	}
}

type recentOrderEntry struct {
	result  *OrderResult
	expires time.Time
}

// recentOrderTTL is short enough not to block a legitimate same-direction
// follow-up trade after a price move, long enough to cover the
// network-blip + retry window a misbehaving caller might do.
const recentOrderTTL = 5 * time.Second

// httpTimeout bounds every HL HTTP request (info and exchange). Normal
// responses land well under a second; 30s is generous headroom for the
// heaviest paginated info reads while still guaranteeing a hung connection
// can't stall the caller forever.
const httpTimeout = 30 * time.Second

// Compile-time interface checks.
var (
	_ InfoReader = (*Client)(nil)
	_ Trader     = (*Client)(nil)
)

// NewPairClient creates the per-pair client for one coin/dex on this
// account. When the account can sign, the SDK exchange (asset-id map for the
// dex) is built here — that requires a meta fetch, so this constructor does
// network I/O.
func (a *Account) NewPairClient(coin, dex string, logger *slog.Logger) (*Client, error) {
	c := &Client{
		acct:   a,
		coin:   coin,
		dex:    dex,
		logger: logger,
	}
	c.recentOrders.entries = make(map[string]recentOrderEntry)

	if a.privateKey == nil {
		return c, nil
	}

	info := hyperliquid.NewInfo(context.Background(), a.apiURL, true, nil, nil, nil)
	// info.Meta is variadic: pass no args for the main dex, one arg for a
	// named perp dex (e.g. "xyz"). Forwarding an empty string works on
	// current SDK versions but is not documented behavior.
	var meta *hyperliquid.Meta
	var metaErr error
	if c.dex == "" {
		meta, metaErr = info.Meta(context.Background())
	} else {
		meta, metaErr = info.Meta(context.Background(), c.dex)
	}
	if metaErr != nil {
		return nil, fmt.Errorf("fetch meta: %w", metaErr)
	}

	var opts []hyperliquid.ExchangeOpt
	if c.dex != "" {
		opts = append(opts, hyperliquid.ExchangeOptPerpDex(c.dex))
	}

	c.exchange = hyperliquid.NewExchange(
		context.Background(), a.privateKey, a.apiURL, meta,
		"", a.accountAddr, nil, nil, opts...,
	)

	return c, nil
}

// NewClient creates a read-only single-pair client over its own Account.
// Convenience for tests and one-pair tooling; multi-pair callers build one
// Account and call NewPairClient per pair.
func NewClient(apiURL, accountAddr, coin, dex string, logger *slog.Logger) *Client {
	acct, _ := NewAccount(apiURL, accountAddr, "", "", logger) // no key, no nonce file: cannot fail
	c, _ := acct.NewPairClient(coin, dex, logger)              // read-only path: cannot fail
	return c
}

// NewClientWithKey creates a signing single-pair client over its own Account.
// nonceFile is the path to persist the last HL nonce; pass "" to skip
// persistence. Multi-pair callers MUST NOT use this per pair — each call
// creates a separate nonce store, and two stores on one signing key race;
// build one Account and call NewPairClient instead.
func NewClientWithKey(apiURL, accountAddr, coin, dex, privateKeyHex, nonceFile string, logger *slog.Logger) (*Client, error) {
	acct, err := NewAccount(apiURL, accountAddr, privateKeyHex, nonceFile, logger)
	if err != nil {
		return nil, err
	}
	return acct.NewPairClient(coin, dex, logger)
}

// Close releases resources held by the underlying account's HTTP client.
func (c *Client) Close() {
	c.acct.Close()
}

// qualifiedCoin returns the coin name with dex prefix (e.g. "xyz:TSLA") if a dex is set.
func (c *Client) qualifiedCoin() string {
	if c.dex != "" {
		return c.dex + ":" + c.coin
	}
	return c.coin
}

// infoBody builds a request body for the /info endpoint, adding the "dex" param if set.
func (c *Client) infoBody(fields map[string]any) map[string]any {
	if c.dex != "" {
		fields["dex"] = c.dex
	}
	return fields
}

// HasExchange returns true if the client can place orders.
func (c *Client) HasExchange() bool {
	return c.exchange != nil
}

// --- Exchange (write) endpoints ---

func (c *Client) requireExchange() error {
	if c.exchange == nil {
		return ErrNoExchange
	}
	return nil
}

// SetLeverage sets the leverage for the configured coin.
func (c *Client) SetLeverage(ctx context.Context, leverage int, isCross bool) error {
	if err := c.requireExchange(); err != nil {
		return err
	}

	qCoin := c.qualifiedCoin()
	_, err := c.exchange.UpdateLeverage(ctx, leverage, qCoin, isCross)
	if err != nil {
		return fmt.Errorf("update leverage: %w", err)
	}

	c.logger.Info("leverage updated", "coin", qCoin, "leverage", leverage, "cross", isCross)
	return nil
}

// ensureSzDecimals fetches the szDecimals for the configured coin if not already cached.
func (c *Client) ensureSzDecimals(ctx context.Context) error {
	if c.szDecimals > 0 {
		return nil
	}
	var resp []json.RawMessage
	if err := c.fetchInfo(ctx, map[string]any{"type": "metaAndAssetCtxs"}, &resp); err != nil {
		return err
	}
	var meta struct {
		Universe []struct {
			Name       string `json:"name"`
			SzDecimals int    `json:"szDecimals"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(resp[0], &meta); err != nil {
		return err
	}
	qCoin := c.qualifiedCoin()
	for _, u := range meta.Universe {
		if strings.EqualFold(u.Name, qCoin) {
			c.szDecimals = u.SzDecimals
			return nil
		}
	}
	return fmt.Errorf("coin %s not found in universe for szDecimals", qCoin)
}

// roundSz rounds a size to the market's allowed decimal places using
// round-half-away-from-zero (math.Round). Symmetric in both directions:
// grows and shrinks each carry at most a half-tick error per call instead
// of a one-tick error always biased toward zero — without symmetry the
// rebalancer accumulates a one-sided drift over many ticks. Sizes
// below a half-tick still resolve to 0, which placeOrder treats as
// "no_change", so sub-min-order amounts are still skipped at the boundary.
func (c *Client) roundSz(sz float64) float64 {
	factor := math.Pow(10, float64(c.szDecimals))
	return math.Round(sz*factor) / factor
}

// sizeFloat converts a decimal position size to float64 for order math.
// The Float64 exact flag is the wrong warning gate here: exactness is about
// binary representability, not magnitude — almost no fractional decimal
// (23.45, 0.1, …) is exactly representable, so gating on !exact warned on
// essentially every order (1,033× the week of 2026-06-02) while the actual
// conversion error was ~1 ulp. Warn only when the error is at least half a
// size tick — the resolution roundSz collapses to before submission, i.e.
// the point where the float could place a different size than the decimal
// said. Callers must run ensureSzDecimals first so the tick is real.
func (c *Client) sizeFloat(d decimal.Decimal) float64 {
	f, _ := d.Float64()
	halfTick := decimal.New(5, -int32(c.szDecimals)-1)
	if err := decimal.NewFromFloat(f).Sub(d).Abs(); err.GreaterThanOrEqual(halfTick) {
		c.logger.Warn("position size lost precision in float conversion",
			"size", d,
			"as_float", f,
		)
	}
	return f
}

// fetchMidPrice fetches the current mid price via our own API call.
// This is needed because the exchange SDK's internal AllMids doesn't support the dex param.
func (c *Client) fetchMidPrice(ctx context.Context) (*float64, error) {
	markPrice, err := c.GetMarkPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetch mid price: %w", err)
	}
	px, _ := markPrice.Float64()
	return &px, nil
}

// MarketShort opens a short position at market price.
func (c *Client) MarketShort(ctx context.Context, sizeInCoins float64, slippage float64) (*OrderResult, error) {
	if err := c.ensureSzDecimals(ctx); err != nil {
		return nil, err
	}

	px, err := c.fetchMidPrice(ctx)
	if err != nil {
		return nil, err
	}

	return c.placeOrder(ctx, false, sizeInCoins, *px, slippage, false)
}

// MarketClose closes the current position at market price.
func (c *Client) MarketClose(ctx context.Context, slippage float64) (*OrderResult, error) {
	pos, err := c.GetPosition(ctx)
	if err != nil {
		return nil, err
	}
	if pos.Size.IsZero() {
		return &OrderResult{Status: "no_change"}, nil
	}

	// Without this, a process whose first order is a close would run
	// placeOrder's roundSz with szDecimals=0 and round to whole coins.
	if err := c.ensureSzDecimals(ctx); err != nil {
		return nil, err
	}

	px, err := c.fetchMidPrice(ctx)
	if err != nil {
		return nil, err
	}

	sz := c.sizeFloat(pos.Size.Abs())
	// Buy closes a short; sell closes a long. The strategy only ever holds
	// shorts, but if a degenerate state leaves us long, a hardcoded buy
	// would be reduce-only rejected and the position would never close.
	isBuy := pos.Size.IsNegative()
	return c.placeOrder(ctx, isBuy, sz, *px, slippage, true)
}

// AdjustShort adjusts the short position to the target size (in coins).
func (c *Client) AdjustShort(ctx context.Context, targetSizeCoins float64, slippage float64) (*OrderResult, error) {
	if err := c.requireExchange(); err != nil {
		return nil, err
	}

	// Without this, a process whose first adjustment is a reduce would
	// reach placeOrder with szDecimals=0 and round to whole coins (the
	// increase branch was covered incidentally via MarketShort).
	if err := c.ensureSzDecimals(ctx); err != nil {
		return nil, err
	}

	pos, err := c.GetPosition(ctx)
	if err != nil {
		return nil, fmt.Errorf("get position for adjustment: %w", err)
	}

	// currentShort is the position in "short coins": positive when short
	// (the hedging direction), negative when long. |size| here would
	// mistake an accidental long for an existing short of the same size
	// and no-op the flip the rebalancer asked for — a long must instead
	// produce a sell of (target + |long|) that flips it through zero.
	currentShort := c.sizeFloat(pos.Size.Neg())
	diff := targetSizeCoins - currentShort

	if math.Abs(diff) < 0.0001 {
		c.logger.Debug("position already at target", "current_short", currentShort, "target", targetSizeCoins)
		return &OrderResult{Status: "no_change"}, nil
	}

	// HL requires minimum $10 order value. Without a usable mark price we
	// can't reason about the dollar size of the adjustment — refuse the
	// trade rather than risk submitting a sub-tick-rounded zero-size order
	// during a stale read (HL's clearinghouseState transiently returns
	// markPrice=0 right after a trade).
	markPrice, _ := pos.MarkPrice.Float64()
	if markPrice <= 0 {
		c.logger.Warn("hedge adjustment skipped, mark price unavailable",
			"mark_price", pos.MarkPrice,
			"diff_coins", diff,
		)
		return &OrderResult{Status: "no_change"}, nil
	}
	if math.Abs(diff)*markPrice < 10.0 {
		c.logger.Debug("adjustment below $10 minimum", "diff_coins", diff, "diff_usd", math.Abs(diff)*markPrice)
		return &OrderResult{Status: "no_change"}, nil
	}

	if pos.Size.IsZero() {
		c.logger.Info("opening new short", "size", targetSizeCoins)
		return c.MarketShort(ctx, targetSizeCoins, slippage)
	}

	if diff > 0 {
		// Also covers flipping an accidental long: diff includes the long
		// size, so one non-reduce-only sell closes the long and opens the
		// short remainder.
		c.logger.Info("increasing short", "add", diff, "current_short", currentShort)
		return c.MarketShort(ctx, diff, slippage)
	}

	// diff < 0 implies currentShort > target >= 0 — an actual oversized
	// short; a long can never reach this branch.
	reduceSz := -diff
	c.logger.Info("reducing short", "reduce", reduceSz)
	px, err := c.fetchMidPrice(ctx)
	if err != nil {
		return nil, err
	}
	return c.placeOrder(ctx, true, reduceSz, *px, slippage, true)
}

// --- Info (read) endpoints ---

// GetPosition fetches the current position for the configured coin.
// clearinghouseStateResp is the subset of the clearinghouseState response
// we consume. Fields added here are shared across GetPosition and
// GetAccountValue so both paths pay for the endpoint only once per caller.
type clearinghouseStateResp struct {
	AssetPositions []struct {
		Position clearinghousePosition `json:"position"`
	} `json:"assetPositions"`
	MarginSummary struct {
		AccountValue string `json:"accountValue"`
	} `json:"marginSummary"`
}

func (c *Client) getClearinghouseState(ctx context.Context) (*clearinghouseStateResp, error) {
	var resp clearinghouseStateResp
	if err := c.fetchInfo(ctx, map[string]any{
		"type": "clearinghouseState",
		"user": c.acct.accountAddr,
	}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) GetPosition(ctx context.Context) (*PositionInfo, error) {
	state, err := c.getClearinghouseState(ctx)
	if err != nil {
		return nil, err
	}

	qCoin := c.qualifiedCoin()
	for _, ap := range state.AssetPositions {
		if strings.EqualFold(ap.Position.Coin, qCoin) {
			size := decimalFromStr(ap.Position.Szi)

			markPrice, err := c.GetMarkPrice(ctx)
			if err != nil {
				return nil, fmt.Errorf("get mark price for position: %w", err)
			}

			return &PositionInfo{
				Coin:          ap.Position.Coin,
				Size:          size,
				EntryPrice:    decimalFromStr(ap.Position.EntryPx),
				MarkPrice:     markPrice,
				UnrealizedPnL: decimalFromStr(ap.Position.UnrealizedPnl),
				LiquidationPx: decimalFromStr(ap.Position.LiquidationPx),
				Leverage:      ap.Position.Leverage.Value,
				MarginUsed:    decimalFromStr(ap.Position.MarginUsed),
				Notional:      size.Abs().Mul(markPrice),
			}, nil
		}
	}

	// No open position, but still fetch mark price for delta calculations.
	// Propagate a fetch failure instead of returning a zero-price
	// PositionInfo with nil error — callers feed MarkPrice into delta and
	// divergence math, and a silent zero there reads as valid data.
	markPrice, err := c.GetMarkPrice(ctx)
	if err != nil {
		return nil, fmt.Errorf("get mark price (no open position): %w", err)
	}
	return &PositionInfo{Coin: qCoin, MarkPrice: markPrice}, nil
}

// GetAccountValue returns the dex-scoped marginSummary.accountValue. On a
// builder-code dex this is NOT account equity: it is the margin held for the
// open position (≈ totalMarginUsed, verified live 2026-06-07: accountValue
// 558.33 vs totalMarginUsed 557.67), auto-swept in/out as the position
// resizes — and the sweeps surface in no ledger endpoint. It therefore
// tracks hedge size, not P&L; use GetHLContributionSince for HL-side profit.
// On error the returned decimal is zero; callers must check err before using it.
func (c *Client) GetAccountValue(ctx context.Context) (decimal.Decimal, error) {
	state, err := c.getClearinghouseState(ctx)
	if err != nil {
		return decimal.Zero, err
	}
	return decimalFromStr(state.MarginSummary.AccountValue), nil
}

// GetSpotUSDC returns the account's spot USDC balance: total holdings and the
// portion held as margin for open positions (free = total − hold). This is the
// account's real cash on HL — on a builder-code dex the perp collateral is
// carried as held spot USDC, so GetAccountValue (the dex-scoped margin slice,
// ≈ hold) drastically understates what's actually on the account. The spot
// state is account-global, so this is account-scoped (identical across pairs)
// and the request must NOT carry the dex param — hence postInfoRetry directly
// rather than fetchInfo, which would inject c.dex.
func (c *Client) GetSpotUSDC(ctx context.Context) (total, hold decimal.Decimal, err error) {
	var resp struct {
		Balances []struct {
			Coin  string `json:"coin"`
			Total string `json:"total"`
			Hold  string `json:"hold"`
		} `json:"balances"`
	}
	if err := c.postInfoRetry(ctx, map[string]any{
		"type": "spotClearinghouseState",
		"user": c.acct.accountAddr,
	}, &resp); err != nil {
		return decimal.Zero, decimal.Zero, fmt.Errorf("fetch spot clearinghouse state: %w", err)
	}
	for _, b := range resp.Balances {
		if b.Coin == "USDC" {
			return decimalFromStr(b.Total), decimalFromStr(b.Hold), nil
		}
	}
	// No USDC balance entry — account holds no spot USDC.
	return decimal.Zero, decimal.Zero, nil
}

// GetPerpCapitalChangeSince returns the net USD capital moved into the perp
// account since the given timestamp via accountClassTransfer events
// (spot↔perp). Positive = net inflow; negative = net outflow.
//
// Caveat: this is blind to dex-scoped margin sweeps — the builder-code dex
// auto-moves margin in/out of its scoped account as the position resizes
// without emitting any ledger event — so (accountValue − baseline − this)
// does not measure HL P&L. Kept for the legacy comparison gauge; the real
// P&L path is GetHLContributionSince.
func (c *Client) GetPerpCapitalChangeSince(ctx context.Context, since time.Time) (decimal.Decimal, error) {
	if since.IsZero() {
		// No baseline set yet — caller should not depend on this method.
		return decimal.Zero, nil
	}
	var events []struct {
		Delta struct {
			Type   string `json:"type"`
			USDC   string `json:"usdc"`
			ToPerp bool   `json:"toPerp"`
		} `json:"delta"`
	}
	// HL's startTime filter is millisecond-granular and inclusive; nudge
	// forward 1ms so we don't re-count an event that happened exactly at
	// the baseline timestamp.
	startMs := since.UnixMilli() + 1
	if err := c.fetchInfo(ctx, map[string]any{
		"type":      "userNonFundingLedgerUpdates",
		"user":      c.acct.accountAddr,
		"startTime": startMs,
	}, &events); err != nil {
		return decimal.Zero, err
	}
	total := decimal.Zero
	for _, e := range events {
		if e.Delta.Type != "accountClassTransfer" {
			continue
		}
		amt := decimalFromStr(e.Delta.USDC)
		if !e.Delta.ToPerp {
			amt = amt.Neg()
		}
		total = total.Add(amt)
	}
	return total, nil
}

// HLContributionBreakdown decomposes the HL side's P&L into auditable
// primitives, computed from fills/funding/state instead of
// clearinghouseState.marginSummary.accountValue — which for builder-code
// dexes (e.g. xyz) is the margin held for the open position, auto-swept as
// the position resizes (sweeps invisible to ledger endpoints), and so can't
// measure P&L at all.
type HLContributionBreakdown struct {
	RealizedPnL decimal.Decimal // Σ closedPnl across all fills since `since`
	Funding     decimal.Decimal // Σ funding usdc since `since` (positive = received)
	Fees        decimal.Decimal // Σ trading fees paid since `since` (always positive)
	Unrealized  decimal.Decimal // Currently open position's unrealizedPnl
	NumFills    int
	NumFunding  int
}

// Total returns the net HL contribution since the baseline:
//
//	realized + funding − fees + unrealized
//
// This is the HL side of strategy P&L, audit-traceable to fills and funding
// events — and the only valid measure on a builder-code dex, where
// (accountValue − baseline − capitalChange) reflects margin sweeps rather
// than profit.
func (b HLContributionBreakdown) Total() decimal.Decimal {
	return b.RealizedPnL.Add(b.Funding).Sub(b.Fees).Add(b.Unrealized)
}

// GetHLContributionSince computes the fills-derived HL P&L since the given
// timestamp. This is the authoritative HL-side P&L: every component traces
// to a visible fill/funding event, whereas the dex-scoped accountValue is a
// position-margin gauge (see GetAccountValue) and can't be used for profit.
//
// On `since` zero, returns a zero breakdown (no baseline → caller has no
// reference point).
func (c *Client) GetHLContributionSince(ctx context.Context, since time.Time) (HLContributionBreakdown, error) {
	if since.IsZero() {
		return HLContributionBreakdown{}, nil
	}
	startMs := since.UnixMilli() + 1

	// Fills: closedPnl + fee per event.
	realized, fees, numFills, err := c.sumFillsBetween(ctx, startMs, 0)
	if err != nil {
		return HLContributionBreakdown{}, err
	}

	// Funding payments: positive = received, negative = paid.
	fundingTotal, numFunding, err := c.sumFundingBetween(ctx, startMs, 0)
	if err != nil {
		return HLContributionBreakdown{}, err
	}

	// Unrealized PnL on currently-open position (single coin path).
	pos, err := c.GetPosition(ctx)
	if err != nil {
		return HLContributionBreakdown{}, fmt.Errorf("fetch open position for unrealized: %w", err)
	}
	unrealized := pos.UnrealizedPnL

	return HLContributionBreakdown{
		RealizedPnL: realized,
		Funding:     fundingTotal,
		Fees:        fees,
		Unrealized:  unrealized,
		NumFills:    numFills,
		NumFunding:  numFunding,
	}, nil
}

// sumFillsBetween returns Σ closedPnl, Σ fee, and the fill count for fills
// of the configured coin with startMs <= time <= endMs (endMs 0 means "to
// now"), paginating past the API's 2000-fills-per-response cap. Without
// pagination the totals silently freeze once the account accumulates 2000
// fills after the baseline — userFillsByTime returns the *oldest* 2000 and
// drops the rest (observed live 2026-05-27).
//
// Fills are filtered to the qualified coin (e.g. "xyz:TSLA" — verified
// 2026-06-09 to be the exact form the fills carry): userFillsByTime is
// account-scoped, so without the filter a second strategy instance trading
// another coin on the same account (or any manual trade) would silently
// blend into this instance's P&L. Foreign-coin fills still advance the
// pagination cursor — only the sums skip them.
//
// Pages overlap by one millisecond (next startTime = last fill's timestamp,
// not +1) so a page boundary that splits same-millisecond fills doesn't drop
// any; the overlap is deduped by trade id. The end bound is enforced
// client-side so the result doesn't depend on the server's endTime
// inclusivity.
//
// Note: HL only serves the 10000 most recent fills. The fills checkpoint
// (Manager.rollHLFillsCheckpoint) folds old windows into persisted sums so
// the live recompute window stays under that horizon.
func (c *Client) sumFillsBetween(ctx context.Context, startMs, endMs int64) (realized, fees decimal.Decimal, numFills int, err error) {
	const pageCap = 2000 // API hard limit per response
	realized, fees = decimal.Zero, decimal.Zero
	qCoin := c.qualifiedCoin()
	seen := make(map[int64]struct{})
	for {
		var fills []struct {
			Coin      string `json:"coin"`
			ClosedPnL string `json:"closedPnl"`
			Fee       string `json:"fee"`
			Time      int64  `json:"time"`
			Tid       int64  `json:"tid"`
		}
		if err := c.fetchInfo(ctx, map[string]any{
			"type":      "userFillsByTime",
			"user":      c.acct.accountAddr,
			"startTime": startMs,
		}, &fills); err != nil {
			return decimal.Zero, decimal.Zero, 0, err
		}
		for _, f := range fills {
			if f.Coin != qCoin {
				continue
			}
			if endMs > 0 && f.Time > endMs {
				continue
			}
			if _, dup := seen[f.Tid]; dup {
				continue
			}
			seen[f.Tid] = struct{}{}
			realized = realized.Add(decimalFromStr(f.ClosedPnL))
			fees = fees.Add(decimalFromStr(f.Fee))
			numFills++
		}
		if len(fills) < pageCap {
			return realized, fees, numFills, nil
		}
		next := fills[len(fills)-1].Time
		if endMs > 0 && next > endMs {
			// The page already reached past the end bound — everything
			// later is out of window too (fills arrive time-ordered).
			return realized, fees, numFills, nil
		}
		if next <= startMs {
			// Full page within one millisecond — cursor can't advance.
			// Pathological; bail rather than loop forever.
			return decimal.Zero, decimal.Zero, 0, fmt.Errorf("fills pagination stuck at startTime %d", startMs)
		}
		startMs = next
	}
}

// sumFundingBetween returns Σ funding usdc (positive = received) and the
// event count for the configured coin's funding with startMs <= time <=
// endMs (endMs 0 means "to now"). Same 2000-records-per-response cap,
// pagination scheme, and qualified-coin filter as sumFillsBetween —
// userFunding is account-scoped, so another coin's funding on the same
// account would otherwise blend in. Funding has no trade id, so the
// one-millisecond page overlap is deduped by timestamp (single coin after
// filtering, so timestamps no longer collide across coins).
func (c *Client) sumFundingBetween(ctx context.Context, startMs, endMs int64) (total decimal.Decimal, numFunding int, err error) {
	const pageCap = 2000 // API hard limit per response
	total = decimal.Zero
	qCoin := c.qualifiedCoin()
	type fundingKey struct {
		time int64
		coin string
	}
	seen := make(map[fundingKey]struct{})
	for {
		var funding []struct {
			Time  int64 `json:"time"`
			Delta struct {
				Type string `json:"type"`
				Coin string `json:"coin"`
				USDC string `json:"usdc"`
			} `json:"delta"`
		}
		if err := c.fetchInfo(ctx, map[string]any{
			"type":      "userFunding",
			"user":      c.acct.accountAddr,
			"startTime": startMs,
		}, &funding); err != nil {
			return decimal.Zero, 0, err
		}
		for _, e := range funding {
			if e.Delta.Type != "funding" {
				continue
			}
			if e.Delta.Coin != qCoin {
				continue
			}
			if endMs > 0 && e.Time > endMs {
				continue
			}
			k := fundingKey{e.Time, e.Delta.Coin}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			total = total.Add(decimalFromStr(e.Delta.USDC))
			numFunding++
		}
		if len(funding) < pageCap {
			return total, numFunding, nil
		}
		next := funding[len(funding)-1].Time
		if endMs > 0 && next > endMs {
			return total, numFunding, nil
		}
		if next <= startMs {
			return decimal.Zero, 0, fmt.Errorf("funding pagination stuck at startTime %d", startMs)
		}
		startMs = next
	}
}

// SumLedgerBetween returns Σ closedPnl, Σ trading fees, and Σ funding for
// ledger events with startMs <= time <= endMs. Used to roll the persisted
// fills checkpoint forward: the window being folded away must use the same
// inclusive bounds as the live window's start (+1ms in
// GetHLContributionSince) so no event is counted twice or dropped at the
// boundary.
func (c *Client) SumLedgerBetween(ctx context.Context, startMs, endMs int64) (realized, fees, funding decimal.Decimal, err error) {
	realized, fees, _, err = c.sumFillsBetween(ctx, startMs, endMs)
	if err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, err
	}
	funding, _, err = c.sumFundingBetween(ctx, startMs, endMs)
	if err != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, err
	}
	return realized, fees, funding, nil
}

// GetHistoricalAccountValue returns the HL perp-account equity closest to
// the given target time, sampled from the "perpAllTime" bucket of the
// portfolio endpoint. Useful for seeding a profit baseline at strategy
// start — so reported profit covers the full strategy lifetime, not just
// since the first observation.
//
// Returns the sampled value plus its actual timestamp (gap from target may
// be hours due to coarse bucket granularity). Returns an error if the
// endpoint returns no history or no perpAllTime series.
func (c *Client) GetHistoricalAccountValue(ctx context.Context, target time.Time) (value decimal.Decimal, at time.Time, err error) {
	// Response shape: [[bucketName, {accountValueHistory: [[ts_ms, value], ...]}], ...]
	// We only care about perpAllTime (longest-range perp-specific bucket).
	var resp [][]json.RawMessage
	if err := c.fetchInfo(ctx, map[string]any{
		"type": "portfolio",
		"user": c.acct.accountAddr,
	}, &resp); err != nil {
		return decimal.Zero, time.Time{}, err
	}

	for _, entry := range resp {
		if len(entry) != 2 {
			continue
		}
		var name string
		if err := json.Unmarshal(entry[0], &name); err != nil || name != "perpAllTime" {
			continue
		}
		var body struct {
			AccountValueHistory [][2]json.RawMessage `json:"accountValueHistory"`
		}
		if err := json.Unmarshal(entry[1], &body); err != nil {
			return decimal.Zero, time.Time{}, fmt.Errorf("parse perpAllTime body: %w", err)
		}
		return pickClosestAccountValue(body.AccountValueHistory, target)
	}
	return decimal.Zero, time.Time{}, fmt.Errorf("portfolio: no perpAllTime bucket in response")
}

// pickClosestAccountValue scans the [ts_ms, value] pairs and returns the
// entry whose timestamp is closest to target.
func pickClosestAccountValue(history [][2]json.RawMessage, target time.Time) (decimal.Decimal, time.Time, error) {
	if len(history) == 0 {
		return decimal.Zero, time.Time{}, fmt.Errorf("portfolio: empty accountValueHistory")
	}
	targetMs := target.UnixMilli()
	var bestMs int64
	var bestVal string
	bestGap := int64(-1)
	for _, pair := range history {
		var ts int64
		if err := json.Unmarshal(pair[0], &ts); err != nil {
			continue
		}
		var val string
		if err := json.Unmarshal(pair[1], &val); err != nil {
			continue
		}
		gap := ts - targetMs
		if gap < 0 {
			gap = -gap
		}
		if bestGap < 0 || gap < bestGap {
			bestGap = gap
			bestMs = ts
			bestVal = val
		}
	}
	if bestGap < 0 {
		return decimal.Zero, time.Time{}, fmt.Errorf("portfolio: no usable accountValueHistory entries")
	}
	return decimalFromStr(bestVal), time.UnixMilli(bestMs), nil
}

// GetMarkPrice returns the current mark price for the configured coin.
func (c *Client) GetMarkPrice(ctx context.Context) (decimal.Decimal, error) {
	var resp map[string]string
	if err := c.fetchInfo(ctx, map[string]any{"type": "allMids"}, &resp); err != nil {
		return decimal.Zero, err
	}

	qCoin := c.qualifiedCoin()
	priceStr, ok := resp[qCoin]
	if !ok {
		return decimal.Zero, fmt.Errorf("coin %s not found in allMids response", qCoin)
	}

	return decimal.NewFromString(priceStr)
}

// GetFundingRate returns the current funding rate for the configured coin.
func (c *Client) GetFundingRate(ctx context.Context) (decimal.Decimal, error) {
	_, assetCtx, err := c.fetchAssetCtx(ctx)
	if err != nil {
		return decimal.Zero, err
	}
	return decimal.NewFromString(assetCtx.Funding)
}

// GetMarketInfo returns market metadata for the configured coin.
func (c *Client) GetMarketInfo(ctx context.Context) (*MarketInfo, error) {
	name, ac, err := c.fetchAssetCtx(ctx)
	if err != nil {
		return nil, err
	}
	return &MarketInfo{
		Coin:         name,
		MarkPrice:    decimalFromStr(ac.MarkPx),
		FundingRate:  decimalFromStr(ac.Funding),
		OpenInterest: decimalFromStr(ac.OpenInterest),
	}, nil
}

// fetchAssetCtx fetches metaAndAssetCtxs and returns the entry for c.coin.
func (c *Client) fetchAssetCtx(ctx context.Context) (string, *assetCtx, error) {
	var resp []json.RawMessage
	if err := c.fetchInfo(ctx, map[string]any{"type": "metaAndAssetCtxs"}, &resp); err != nil {
		return "", nil, err
	}

	if len(resp) < 2 {
		return "", nil, fmt.Errorf("unexpected metaAndAssetCtxs response length: %d", len(resp))
	}

	var meta struct {
		Universe []struct {
			Name string `json:"name"`
		} `json:"universe"`
	}
	if err := json.Unmarshal(resp[0], &meta); err != nil {
		return "", nil, fmt.Errorf("unmarshal meta: %w", err)
	}

	var assetCtxs []assetCtx
	if err := json.Unmarshal(resp[1], &assetCtxs); err != nil {
		return "", nil, fmt.Errorf("unmarshal asset ctxs: %w", err)
	}

	qCoin := c.qualifiedCoin()
	for i, u := range meta.Universe {
		if strings.EqualFold(u.Name, qCoin) && i < len(assetCtxs) {
			return u.Name, &assetCtxs[i], nil
		}
	}

	return "", nil, fmt.Errorf("coin %s not found in universe", qCoin)
}

// --- HTTP helpers ---

func (c *Client) postInfoRetry(ctx context.Context, body any, result any) error {
	return retry.DoVoid(ctx, "HL.postInfo", c.logger, func() error {
		return c.postInfo(ctx, body, result)
	})
}

// fetchInfo POSTs a query to the /info endpoint and unmarshals the response
// into result. It injects the configured dex via infoBody and, on failure,
// wraps the error with the request's "type" so callers don't each repeat the
// "fetch <type>: %w" boilerplate.
func (c *Client) fetchInfo(ctx context.Context, fields map[string]any, result any) error {
	if err := c.postInfoRetry(ctx, c.infoBody(fields), result); err != nil {
		return fmt.Errorf("fetch %v: %w", fields["type"], err)
	}
	return nil
}

func (c *Client) postInfo(ctx context.Context, body any, result any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.acct.apiURL+"/info", bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.acct.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}

	return json.Unmarshal(respBody, result)
}

// --- Utilities ---

func decimalFromStr(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}

func parsePrivateKey(hexKey string) (*ecdsa.PrivateKey, error) {
	return crypto.HexToECDSA(strings.TrimPrefix(hexKey, "0x"))
}

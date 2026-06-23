package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

// newTestClient wires the HL client against a custom handler and returns it.
func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewClient(srv.URL, "0xwallet", "TSLA", "xyz", logger)
}

// infoHandler routes POST /info requests by the "type" field in the body to
// per-type handlers, so a single test server can answer multiple endpoints.
func infoHandler(t *testing.T, routes map[string]http.HandlerFunc) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/info" {
			t.Errorf("unexpected path: %s", r.URL.Path)
			return
		}
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body)) // let routed handlers re-read it
		var head struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(body, &head); err != nil {
			t.Errorf("parse body: %v", err)
			return
		}
		h, ok := routes[head.Type]
		if !ok {
			t.Errorf("no route for type %q", head.Type)
			return
		}
		h(w, r)
	})
}

func TestGetAccountValue(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"clearinghouseState": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"marginSummary":{"accountValue":"53.42"}}`))
		},
	}))
	got, err := c.GetAccountValue(ctx)
	if err != nil {
		t.Fatalf("GetAccountValue: %v", err)
	}
	if got.String() != "53.42" {
		t.Errorf("got %s, want 53.42", got)
	}
}

// TestGetHistoricalAccountValue_PicksClosest confirms we select the
// perpAllTime entry whose timestamp is closest to the requested target.
func TestGetHistoricalAccountValue_PicksClosest(t *testing.T) {
	ctx := context.Background()
	// Fixture: portfolio with two buckets, perpAllTime carrying three points.
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"portfolio": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[
				["allTime", {"accountValueHistory": [[1000, "100.0"]]}],
				["perpAllTime", {"accountValueHistory": [
					[1000, "1.00"],
					[5000, "5.00"],
					[9000, "9.00"]
				]}]
			]`))
		},
	}))

	got, at, err := c.GetHistoricalAccountValue(ctx, time.UnixMilli(6000))
	if err != nil {
		t.Fatalf("GetHistoricalAccountValue: %v", err)
	}
	// 6000 is closer to 5000 (gap 1000) than 9000 (gap 3000).
	if got.String() != "5" {
		t.Errorf("value = %s, want 5", got)
	}
	if at.UnixMilli() != 5000 {
		t.Errorf("at = %d, want 5000", at.UnixMilli())
	}
}

// TestGetSpotUSDC_PicksUSDCBalance confirms we return the USDC entry's total
// and hold, ignoring other coins, and that the request is account-global (no
// dex param) — the spot ledger is not dex-scoped.
func TestGetSpotUSDC_PicksUSDCBalance(t *testing.T) {
	ctx := context.Background()
	var sawDex bool
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"spotClearinghouseState": func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			var b map[string]any
			_ = json.Unmarshal(body, &b)
			if _, ok := b["dex"]; ok {
				sawDex = true
			}
			_, _ = w.Write([]byte(`{"balances":[
				{"coin":"HYPE","total":"0.30","hold":"0.0"},
				{"coin":"USDC","total":"7833.58","hold":"1412.95"}
			]}`))
		},
	}))
	total, hold, err := c.GetSpotUSDC(ctx)
	if err != nil {
		t.Fatalf("GetSpotUSDC: %v", err)
	}
	if total.String() != "7833.58" {
		t.Errorf("total = %s, want 7833.58", total)
	}
	if hold.String() != "1412.95" {
		t.Errorf("hold = %s, want 1412.95", hold)
	}
	if sawDex {
		t.Error("spotClearinghouseState must not carry the dex param")
	}
}

// TestGetSpotUSDC_NoUSDCEntry returns zero when the account holds no spot USDC.
func TestGetSpotUSDC_NoUSDCEntry(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"spotClearinghouseState": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"balances":[{"coin":"HYPE","total":"0.30","hold":"0.0"}]}`))
		},
	}))
	total, hold, err := c.GetSpotUSDC(ctx)
	if err != nil {
		t.Fatalf("GetSpotUSDC: %v", err)
	}
	if !total.IsZero() || !hold.IsZero() {
		t.Errorf("got total=%s hold=%s, want 0/0", total, hold)
	}
}

// TestGetPerpCapitalChangeSince_SumsAccountClassTransfers sums deposits/
// withdrawals into/out of perp via accountClassTransfer, signed by toPerp.
func TestGetPerpCapitalChangeSince_SumsAccountClassTransfers(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userNonFundingLedgerUpdates": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[
				{"delta":{"type":"accountClassTransfer","usdc":"80.0","toPerp":true}},
				{"delta":{"type":"accountClassTransfer","usdc":"10.0","toPerp":false}},
				{"delta":{"type":"deposit","usdc":"1000.0"}},
				{"delta":{"type":"withdraw","usdc":"500.0"}},
				{"delta":{"type":"accountClassTransfer","usdc":"5.5","toPerp":true}}
			]`))
		},
	}))
	got, err := c.GetPerpCapitalChangeSince(ctx, time.UnixMilli(1))
	if err != nil {
		t.Fatalf("GetPerpCapitalChangeSince: %v", err)
	}
	// 80 - 10 + 5.5 = 75.5 — deposit/withdraw are spot-level and ignored.
	if got.String() != "75.5" {
		t.Errorf("got %s, want 75.5", got)
	}
}

// TestGetPerpCapitalChangeSince_ZeroSinceShortCircuits avoids hitting the
// backend when the baseline hasn't been seeded yet.
func TestGetPerpCapitalChangeSince_ZeroSinceShortCircuits(t *testing.T) {
	ctx := context.Background()
	called := false
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	got, err := c.GetPerpCapitalChangeSince(ctx, time.Time{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !got.IsZero() {
		t.Errorf("got %s, want 0", got)
	}
	if called {
		t.Error("backend should not be called for zero since")
	}
}

// TestRecentOrderCache_RoundTrip verifies the idempotency cache stores and
// retrieves results within the TTL.
func TestRecentOrderCache_RoundTrip(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	want := &OrderResult{Status: "filled", FilledSz: decimalFromStr("0.5")}
	c.recentOrderStore("k1", want)
	got, ok := c.recentOrderLookup("k1")
	if !ok {
		t.Fatal("expected cache hit, got miss")
	}
	if got.Status != want.Status || !got.FilledSz.Equal(want.FilledSz) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

// TestRecentOrderCache_ExpiresEntries verifies TTL eviction. We force expiry
// by writing a stale entry directly so the test stays sub-millisecond.
func TestRecentOrderCache_ExpiresEntries(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	c.recentOrders.mu.Lock()
	c.recentOrders.entries["k1"] = recentOrderEntry{
		result:  &OrderResult{Status: "filled"},
		expires: time.Now().Add(-time.Second),
	}
	c.recentOrders.mu.Unlock()
	if _, ok := c.recentOrderLookup("k1"); ok {
		t.Error("expected cache miss for expired entry")
	}
	// recentOrderStore also sweeps expired entries — confirm the stale key
	// is gone after a fresh write to a different key.
	c.recentOrderStore("k2", &OrderResult{Status: "filled"})
	c.recentOrders.mu.Lock()
	_, stillThere := c.recentOrders.entries["k1"]
	c.recentOrders.mu.Unlock()
	if stillThere {
		t.Error("recentOrderStore should sweep expired entries")
	}
}

// TestRoundSz_RoundsToNearest exercises the round-half-away-from-zero
// behaviour. The boundary at half-tick matters: sizes below it resolve
// to 0 (skipped by the placeOrder zero-size guard), sizes at or above it
// round to the next tick. Symmetric across grows/shrinks so the
// rebalancer doesn't accumulate a one-sided drift.
func TestRoundSz_RoundsToNearest(t *testing.T) {
	c := &Client{szDecimals: 3}
	cases := []struct {
		in, want float64
	}{
		{0.0001, 0},     // far below half-tick → 0
		{0.0004, 0},     // just below half-tick → 0
		{0.0005, 0.001}, // exactly half-tick → away from zero
		{0.0009, 0.001}, // above half-tick → 0.001
		{0.001, 0.001},  // exact tick
		{0.0014, 0.001}, // closer to 0.001 than 0.002
		{0.0016, 0.002}, // closer to 0.002
	}
	for _, tc := range cases {
		if got := c.roundSz(tc.in); got != tc.want {
			t.Errorf("roundSz(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// TestSizeFloat_WarnsOnlyOnMaterialError pins the warning gate to "could the
// float place a different size than the decimal said" — i.e. conversion
// error ≥ half a size tick — instead of the Float64 exact flag, which trips
// on any non-dyadic fraction (23.45) and warned on essentially every order.
func TestSizeFloat_WarnsOnlyOnMaterialError(t *testing.T) {
	cases := []struct {
		name     string
		size     string
		wantWarn bool
	}{
		// Typical position sizes: inexact in binary, error ~1 ulp — far
		// below half a tick. The old !exact gate warned on all of these.
		{"fractional size", "23.45", false},
		{"sub-tick noise from API", "0.8371", false},
		{"integer size", "42", false},
		// Size so large that a float64 ulp (~0.002 at 1e13) exceeds the
		// half-tick (0.0005): the submitted size genuinely differs from
		// the decimal — the case the warning exists for.
		{"size beyond float64 resolution", "12345678901234.5671", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := &Client{
				szDecimals: 3,
				logger:     slog.New(slog.NewTextHandler(&buf, nil)),
			}
			d := decimal.RequireFromString(tc.size)
			f := c.sizeFloat(d)
			if want, _ := d.Float64(); f != want {
				t.Errorf("sizeFloat(%s) = %v, want %v", tc.size, f, want)
			}
			gotWarn := strings.Contains(buf.String(), "lost precision")
			if gotWarn != tc.wantWarn {
				t.Errorf("sizeFloat(%s) warned=%v, want %v (log: %q)", tc.size, gotWarn, tc.wantWarn, buf.String())
			}
		})
	}
}

// TestGetHistoricalAccountValue_NoPerpAllTime errors cleanly when the bucket
// is missing from the response (e.g. account with no perp history).
func TestGetHistoricalAccountValue_NoPerpAllTime(t *testing.T) {
	ctx := context.Background()
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"portfolio": func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[["allTime", {"accountValueHistory": [[1000, "1"]]}]]`))
		},
	}))
	_, _, err := c.GetHistoricalAccountValue(ctx, time.UnixMilli(1000))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// fillsPageHandler answers userFillsByTime by startTime, simulating the
// API's 2000-fills-per-response cap with a one-millisecond boundary overlap.
func fillsPageHandler(t *testing.T, pages map[int64]any) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			StartTime int64 `json:"startTime"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("parse fills request: %v", err)
			return
		}
		page, ok := pages[req.StartTime]
		if !ok {
			t.Errorf("unexpected startTime %d", req.StartTime)
			return
		}
		_ = json.NewEncoder(w).Encode(page)
	}
}

type testFill struct {
	Coin      string `json:"coin"`
	ClosedPnL string `json:"closedPnl"`
	Fee       string `json:"fee"`
	Time      int64  `json:"time"`
	Tid       int64  `json:"tid"`
}

// ourCoin matches newTestClient's coin/dex in the qualified form fills carry.
const ourCoin = "xyz:TSLA"

// TestSumFillsSince_PaginatesPastResponseCap reproduces the live freeze: a
// first page capped at exactly 2000 fills must trigger a follow-up request
// from the last fill's timestamp, with the same-millisecond overlap deduped
// by trade id.
func TestSumFillsSince_PaginatesPastResponseCap(t *testing.T) {
	ctx := context.Background()

	// Page 1: exactly 2000 fills. Tids 1999 and 2000 share millisecond 2000
	// so the page boundary splits a timestamp.
	page1 := make([]testFill, 0, 2000)
	for i := int64(1); i <= 2000; i++ {
		ts := i
		if i == 1999 {
			ts = 2000
		}
		page1 = append(page1, testFill{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: ts, Tid: i})
	}
	// Page 2 (startTime = 2000): the two boundary fills again (must be
	// deduped) plus two genuinely new ones.
	page2 := []testFill{
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 1999},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 2000},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 2001},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2001, Tid: 2002},
	}

	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFillsByTime": fillsPageHandler(t, map[int64]any{1: page1, 2000: page2}),
	}))

	realized, fees, n, err := c.sumFillsBetween(ctx, 1, 0)
	if err != nil {
		t.Fatalf("sumFillsSince: %v", err)
	}
	if n != 2002 {
		t.Errorf("numFills = %d, want 2002", n)
	}
	if realized.String() != "2002" {
		t.Errorf("realized = %s, want 2002", realized)
	}
	if fees.String() != "1001" {
		t.Errorf("fees = %s, want 1001", fees)
	}
}

// TestSumFillsSince_StuckCursorErrors guards the infinite-loop bail-out: a
// full page whose last timestamp doesn't advance the cursor must error
// instead of refetching forever.
func TestSumFillsSince_StuckCursorErrors(t *testing.T) {
	ctx := context.Background()
	stuck := make([]testFill, 0, 2000)
	for i := int64(1); i <= 2000; i++ {
		stuck = append(stuck, testFill{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 5, Tid: i})
	}
	calls := 0
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFillsByTime": func(w http.ResponseWriter, r *http.Request) {
			calls++
			_ = json.NewEncoder(w).Encode(stuck)
		},
	}))
	if _, _, _, err := c.sumFillsBetween(ctx, 5, 0); err == nil {
		t.Fatal("expected stuck-pagination error, got nil")
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (no retry on stuck cursor)", calls)
	}
}

// TestSumFillsBetween_EndBoundTilesExactly pins the checkpoint-roll
// invariant: splitting a window at boundary B into [start, B] and [B+1, ∞)
// must reproduce the unsplit totals exactly — no event double-counted or
// dropped, including fills sharing the boundary millisecond.
func TestSumFillsBetween_EndBoundTilesExactly(t *testing.T) {
	ctx := context.Background()

	// Same fixture as the pagination test: tids 1999/2000 share ms 2000,
	// so the boundary B=2000 splits a millisecond with traffic on both
	// pages.
	page1 := make([]testFill, 0, 2000)
	for i := int64(1); i <= 2000; i++ {
		ts := i
		if i == 1999 {
			ts = 2000
		}
		page1 = append(page1, testFill{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: ts, Tid: i})
	}
	page2 := []testFill{
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 1999},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 2000},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2000, Tid: 2001},
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2001, Tid: 2002},
	}
	// The live side after a roll starts at B+1.
	page3 := []testFill{
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 2001, Tid: 2002},
	}

	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFillsByTime": fillsPageHandler(t, map[int64]any{1: page1, 2000: page2, 2001: page3}),
	}))

	wholeR, wholeF, wholeN, err := c.sumFillsBetween(ctx, 1, 0)
	if err != nil {
		t.Fatalf("whole window: %v", err)
	}
	ckptR, ckptF, ckptN, err := c.sumFillsBetween(ctx, 1, 2000)
	if err != nil {
		t.Fatalf("checkpoint side: %v", err)
	}
	liveR, liveF, liveN, err := c.sumFillsBetween(ctx, 2001, 0)
	if err != nil {
		t.Fatalf("live side: %v", err)
	}
	if ckptN+liveN != wholeN {
		t.Errorf("split counts %d+%d != whole %d", ckptN, liveN, wholeN)
	}
	if !ckptR.Add(liveR).Equal(wholeR) {
		t.Errorf("split realized %s+%s != whole %s", ckptR, liveR, wholeR)
	}
	if !ckptF.Add(liveF).Equal(wholeF) {
		t.Errorf("split fees %s+%s != whole %s", ckptF, liveF, wholeF)
	}
}

// TestSumFillsBetween_EndBoundStopsPaginating: when a full page already
// reaches past the end bound, no follow-up request may be issued — the
// fillsPageHandler errors on any startTime it has no page for, so a stray
// request fails the test by itself.
func TestSumFillsBetween_EndBoundStopsPaginating(t *testing.T) {
	ctx := context.Background()
	page1 := make([]testFill, 0, 2000)
	for i := int64(1); i <= 2000; i++ {
		page1 = append(page1, testFill{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: i, Tid: i})
	}
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFillsByTime": fillsPageHandler(t, map[int64]any{1: page1}),
	}))
	realized, _, n, err := c.sumFillsBetween(ctx, 1, 1500)
	if err != nil {
		t.Fatalf("sumFillsBetween: %v", err)
	}
	if n != 1500 {
		t.Errorf("numFills = %d, want 1500 (only t <= 1500)", n)
	}
	if realized.String() != "1500" {
		t.Errorf("realized = %s, want 1500", realized)
	}
}

type testFundingDelta struct {
	Type string `json:"type"`
	Coin string `json:"coin"`
	USDC string `json:"usdc"`
}

type testFunding struct {
	Time  int64            `json:"time"`
	Delta testFundingDelta `json:"delta"`
}

// TestSumFundingSince_FiltersAndPaginates: same pagination scheme as fills,
// filtered to delta.type == "funding" AND the configured coin — funding for
// another coin on the same account must not blend into this strategy's P&L.
func TestSumFundingSince_FiltersAndPaginates(t *testing.T) {
	ctx := context.Background()

	page1 := make([]testFunding, 0, 2000)
	for i := int64(1); i <= 2000; i++ {
		page1 = append(page1, testFunding{Time: i, Delta: testFundingDelta{Type: "funding", Coin: ourCoin, USDC: "0.01"}})
	}
	page2 := []testFunding{
		{Time: 2000, Delta: testFundingDelta{Type: "funding", Coin: ourCoin, USDC: "0.01"}},  // dup of page1 boundary
		{Time: 2000, Delta: testFundingDelta{Type: "funding", Coin: "xyz:AAPL", USDC: "77"}}, // foreign coin — excluded
		{Time: 2001, Delta: testFundingDelta{Type: "accountClassTransfer", USDC: "99"}},      // wrong type — excluded
		{Time: 2002, Delta: testFundingDelta{Type: "funding", Coin: ourCoin, USDC: "-0.02"}},
	}

	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFunding": fillsPageHandler(t, map[int64]any{1: page1, 2000: page2}),
	}))

	total, n, err := c.sumFundingBetween(ctx, 1, 0)
	if err != nil {
		t.Fatalf("sumFundingSince: %v", err)
	}
	if n != 2001 {
		t.Errorf("numFunding = %d, want 2001 (foreign coin and non-funding excluded)", n)
	}
	// 2000 × 0.01 − 0.02 = 19.98 — the foreign coin's $77 must not appear.
	if total.String() != "19.98" {
		t.Errorf("total = %s, want 19.98", total)
	}
}

// TestSumFillsBetween_FiltersForeignCoins pins the multi-pair isolation
// contract: userFillsByTime is account-scoped, so a second strategy trading
// another coin on the same HL account must not leak into this instance's
// realized P&L, fees, or fill count.
func TestSumFillsBetween_FiltersForeignCoins(t *testing.T) {
	ctx := context.Background()
	page := []testFill{
		{Coin: ourCoin, ClosedPnL: "1", Fee: "0.5", Time: 1, Tid: 1},
		{Coin: "xyz:AAPL", ClosedPnL: "500", Fee: "9", Time: 2, Tid: 2}, // foreign — excluded
		{Coin: "TSLA", ClosedPnL: "300", Fee: "7", Time: 3, Tid: 3},     // main-dex TSLA ≠ xyz:TSLA — excluded
		{Coin: ourCoin, ClosedPnL: "2", Fee: "0.5", Time: 4, Tid: 4},
	}
	c := newTestClient(t, infoHandler(t, map[string]http.HandlerFunc{
		"userFillsByTime": fillsPageHandler(t, map[int64]any{1: page}),
	}))
	realized, fees, n, err := c.sumFillsBetween(ctx, 1, 0)
	if err != nil {
		t.Fatalf("sumFillsBetween: %v", err)
	}
	if n != 2 {
		t.Errorf("numFills = %d, want 2", n)
	}
	if realized.String() != "3" {
		t.Errorf("realized = %s, want 3 (foreign fills leaked in)", realized)
	}
	if fees.String() != "1" {
		t.Errorf("fees = %s, want 1", fees)
	}
}

func TestParseOrderStatus_UnknownStatusErrors(t *testing.T) {
	// None of filled/resting/error present — must error, not return an
	// empty-status result the caller would cache as success.
	_, err := parseOrderStatus([]byte(`{"waitingForFill":{"oid":123}}`))
	if err == nil {
		t.Fatal("expected error for unrecognized order status, got nil")
	}

	res, err := parseOrderStatus([]byte(`{"filled":{"totalSz":"0.5","avgPx":"4500","oid":1}}`))
	if err != nil {
		t.Fatalf("filled status should parse: %v", err)
	}
	if res.Status != "filled" {
		t.Errorf("expected status filled, got %q", res.Status)
	}
}

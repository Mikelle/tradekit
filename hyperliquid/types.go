package hyperliquid

import (
	"context"
	"errors"
	"time"

	"github.com/shopspring/decimal"
)

// ErrNoExchange is returned when a write operation is attempted without a private key.
var ErrNoExchange = errors.New("exchange not initialized: no private key configured")

// InfoReader defines read operations on Hyperliquid.
type InfoReader interface {
	GetPosition(ctx context.Context) (*PositionInfo, error)
	GetMarkPrice(ctx context.Context) (decimal.Decimal, error)
	GetFundingRate(ctx context.Context) (decimal.Decimal, error)
	GetMarketInfo(ctx context.Context) (*MarketInfo, error)
	HasExchange() bool
}

// Trader defines write operations on Hyperliquid.
type Trader interface {
	SetLeverage(ctx context.Context, leverage int, isCross bool) error
	MarketShort(ctx context.Context, sizeInCoins float64, slippage float64) (*OrderResult, error)
	MarketClose(ctx context.Context, slippage float64) (*OrderResult, error)
	AdjustShort(ctx context.Context, targetSizeCoins float64, slippage float64) (*OrderResult, error)
}

// PositionInfo holds the current state of a perpetual position.
type PositionInfo struct {
	Coin          string
	Size          decimal.Decimal // negative for shorts
	EntryPrice    decimal.Decimal
	MarkPrice     decimal.Decimal
	UnrealizedPnL decimal.Decimal
	LiquidationPx decimal.Decimal
	Leverage      int
	MarginUsed    decimal.Decimal
	Notional      decimal.Decimal // abs(Size * MarkPrice)
}

// OrderResult holds the result of placing an order.
type OrderResult struct {
	OrderID  uint64
	AvgPrice decimal.Decimal
	FilledSz decimal.Decimal
	Status   string
}

// FundingPayment represents a single funding payment.
type FundingPayment struct {
	Coin      string
	Amount    decimal.Decimal
	Rate      decimal.Decimal
	Timestamp time.Time
}

// MarketInfo holds perpetual contract metadata.
type MarketInfo struct {
	Coin         string
	MarkPrice    decimal.Decimal
	FundingRate  decimal.Decimal
	OpenInterest decimal.Decimal
}

// clearinghousePosition maps the HL API response for a position.
type clearinghousePosition struct {
	Coin          string `json:"coin"`
	Szi           string `json:"szi"`
	EntryPx       string `json:"entryPx"`
	PositionValue string `json:"positionValue"`
	UnrealizedPnl string `json:"unrealizedPnl"`
	LiquidationPx string `json:"liquidationPx"`
	Leverage      struct {
		Value int    `json:"value"`
		Type  string `json:"type"`
	} `json:"leverage"`
	MarginUsed string `json:"marginUsed"`
}

// assetCtx maps the HL metaAndAssetCtxs response for one asset.
type assetCtx struct {
	Funding      string `json:"funding"`
	MarkPx       string `json:"markPx"`
	OpenInterest string `json:"openInterest"`
}

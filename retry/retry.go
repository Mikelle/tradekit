package retry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v5"
)

// ErrTxNotConfirmed signals a transient Solana confirmation failure: the tx
// was sent to the network but didn't appear on chain within the caller's
// deadline. The tx may still land later (mempool propagation) or may have
// been dropped — callers can't tell from the error alone. Use
// errors.Is(err, retry.ErrTxNotConfirmed) to distinguish this from real
// on-chain rejections, and avoid alerting on it (the next loop iteration
// retries naturally). Lives in the retry package because it's the most
// neutral place imported by the various tx-submitting packages.
var ErrTxNotConfirmed = errors.New("tx not confirmed within deadline")

// Do executes fn with exponential backoff on failure.
func Do[T any](ctx context.Context, name string, logger *slog.Logger, fn func() (T, error)) (T, error) {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 500 * time.Millisecond
	b.MaxInterval = 5 * time.Second

	return backoff.Retry(ctx, func() (T, error) {
		result, err := fn()
		if err != nil {
			logger.Debug("retrying", "call", name, "error", err)
		}
		return result, err
	},
		backoff.WithBackOff(b),
		backoff.WithMaxTries(3),
	)
}

// Permanent marks err so Do/DoVoid stop retrying immediately and return it
// as-is. Use for definitive RPC answers ("account not found") that retrying
// can't change.
func Permanent(err error) error {
	return backoff.Permanent(err)
}

// DoVoid is like Do but for functions that return only an error.
func DoVoid(ctx context.Context, name string, logger *slog.Logger, fn func() error) error {
	_, err := Do(ctx, name, logger, func() (struct{}, error) {
		return struct{}{}, fn()
	})
	return err
}

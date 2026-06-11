# hltools

Production-hardened Go building blocks for trading bots on [Hyperliquid](https://hyperliquid.xyz), extracted from a live delta-neutral bot.

These packages are the opinionated layer on top of [sonirico/go-hyperliquid](https://github.com/sonirico/go-hyperliquid) that you end up needing once real money is involved.

## Packages

### `hyperliquid`

A coin/dex-scoped client over a shared signing account:

- **`Account` / `Client` split** — one `Account` per process holds the signer and a persisted nonce store; cheap per-market `Client`s share it. Avoids nonce collisions when one wallet trades multiple markets concurrently.
- **Persisted nonces** — nonce state survives restarts, so a quick crash-loop can't replay or collide.
- **Fills-derived P&L** — paginated `userFillsByTime` (the API caps responses at 2000 rows; naive use silently freezes your P&L) with checkpointing so long-lived bots never re-scan history.
- **Builder-dex (HIP-3) support** — dex-scoped meta, asset ids, and order routing for markets like stock perps. Note: dex-scoped `accountValue` is margin, not equity — use fills-derived P&L for profit measurement.
- Order placement with EIP-712 signing, reduce-only support, and decimal-safe sizing.

### `retry`

Tiny backoff wrapper (over `cenkalti/backoff`) with two affordances that matter in trading loops: `retry.Permanent(err)` to short-circuit on non-retryable errors, and a sentinel for "tx sent but confirmation unknown" so callers can treat it as suppressed-at-WARN noise instead of a hard failure.

### `notify`

Telegram notifier with a critical-priority queue: routine messages are best-effort, critical ones are queued and retried so alerts about stuck positions don't get dropped with the noise. Includes a no-op implementation for tests.

## Install

```sh
go get github.com/Mikelle/hltools@latest
```

## Stability

`v0.x` — APIs may change between minor versions. Pin a tag.

## License

MIT

// Package notify delivers operational alerts out-of-band. Messages are
// enqueued and sent by a worker goroutine so callers never block on the
// network.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

const defaultTelegramAPI = "https://api.telegram.org"

const (
	// queueSize bounds the in-memory backlog of regular (Send) messages.
	// Oversized queues just mean very stale alerts; 64 is comfortably more
	// than one minute of tick events. Overflow drops the new message.
	queueSize = 64

	// criticalQueueSize bounds the dedicated critical channel. Critical
	// events (circuit breaker trips, persistence failures) are rare —
	// 16 slots is plenty of headroom for any plausible burst. Overflow
	// falls back to a synchronous post (see SendCritical) instead of
	// dropping.
	criticalQueueSize = 16

	// criticalSyncTimeout bounds how long SendCritical may block when both
	// the queue and the network are saturated. Long enough to let one HTTP
	// request complete; short enough that a sustained Telegram outage
	// doesn't pin the caller indefinitely.
	criticalSyncTimeout = 15 * time.Second

	// telegramMaxMessage is the Bot API's hard limit on sendMessage text
	// length. Messages over this are rejected by Telegram with a 400 error
	// and silently lost — observed 2026-05-11 when an auto-reopen failure
	// alert with 10K+ chars of embedded on-chain logs disappeared.
	// Truncate longer messages and append a marker so the operator knows
	// the message was cut, not just verbose.
	telegramMaxMessage = 4096
	telegramTruncMark  = "\n…[truncated]"
)

// Telegram posts text messages to a chat via the Bot API. Send is
// non-blocking; messages are delivered asynchronously by Run(). Routine
// messages enqueue on `queue` and may be dropped on overflow; critical
// messages enqueue on `criticalQueue`, which is drained ahead of `queue`
// and falls back to a synchronous post when full.
type Telegram struct {
	httpc  *http.Client
	base   string
	token  string
	chatID string
	logger *slog.Logger

	queue         chan string
	criticalQueue chan string

	stopOnce sync.Once
	stopped  chan struct{}
}

// Option customizes a Telegram notifier.
type Option func(*Telegram)

// WithHTTPClient injects an HTTP client. Useful for tests.
func WithHTTPClient(c *http.Client) Option { return func(t *Telegram) { t.httpc = c } }

// WithBaseURL overrides the Bot API base. Useful for tests.
func WithBaseURL(u string) Option { return func(t *Telegram) { t.base = u } }

// NewTelegram constructs a Telegram notifier. Caller must invoke Run in a
// goroutine and Close on shutdown. Returns nil when either credential is
// empty — use notify.NoOp{} in that case so call sites don't branch.
func NewTelegram(token, chatID string, logger *slog.Logger, opts ...Option) *Telegram {
	if token == "" || chatID == "" {
		return nil
	}
	t := &Telegram{
		httpc:         &http.Client{Timeout: 10 * time.Second},
		base:          defaultTelegramAPI,
		token:         token,
		chatID:        chatID,
		logger:        logger,
		queue:         make(chan string, queueSize),
		criticalQueue: make(chan string, criticalQueueSize),
		stopped:       make(chan struct{}),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Send enqueues text for delivery. Never blocks. Drops the message (with a
// warn log) if the backlog is full.
func (t *Telegram) Send(text string) {
	text = truncateForTelegram(text)
	select {
	case t.queue <- text:
	default:
		t.logger.Warn("telegram queue full, dropping message")
	}
}

// SendCritical enqueues a high-priority message. Run drains the critical
// queue ahead of the regular queue. If even the critical queue is full
// (sustained Telegram outage with several pending criticals), falls back
// to a synchronous post — the caller blocks for up to criticalSyncTimeout
// rather than dropping. Reserve for events where a missed alert masks a
// real problem (circuit breaker trip, persistence failure).
func (t *Telegram) SendCritical(text string) {
	text = truncateForTelegram(text)
	select {
	case t.criticalQueue <- text:
	default:
		t.logger.Warn("telegram critical queue full, sending synchronously",
			"msg_len", len(text))
		ctx, cancel := context.WithTimeout(context.Background(), criticalSyncTimeout)
		defer cancel()
		t.post(ctx, text)
	}
}

// truncateForTelegram caps text at the Bot API's 4096-byte sendMessage
// limit. Over-limit messages are otherwise rejected by Telegram with no
// indication to the caller — the alert vanishes. Appending an explicit
// "[truncated]" marker tells the operator the message was cut so they
// can pull the full payload from the bot's logs.
//
// Note: Telegram's limit is in UTF-16 code units, not bytes — but our
// alerts are ASCII-heavy (on-chain logs, error strings) so a byte-length
// cap is conservative enough.
func truncateForTelegram(text string) string {
	if len(text) <= telegramMaxMessage {
		return text
	}
	head := telegramMaxMessage - len(telegramTruncMark)
	return text[:head] + telegramTruncMark
}

// Run pumps queued messages to Telegram until ctx is cancelled or Close is
// called. Call this in a goroutine at startup. Critical messages are drained
// ahead of regular messages even when both channels have backlog.
func (t *Telegram) Run(ctx context.Context) {
	defer close(t.stopped)
	// Local aliases so we can disable a case (set to nil) once its source is
	// closed and drained — receives from a nil channel block forever, which
	// is exactly what we want from a select case we no longer need.
	cq, q := t.criticalQueue, t.queue
	for {
		// Non-blocking peek: drain any pending critical first so a burst of
		// regular messages can't delay an alert that the operator must see.
		select {
		case msg, ok := <-cq:
			if !ok {
				cq = nil
			} else {
				t.post(ctx, msg)
			}
			continue
		default:
		}
		if cq == nil && q == nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-cq:
			if !ok {
				cq = nil
			} else {
				t.post(ctx, msg)
			}
		case msg, ok := <-q:
			if !ok {
				q = nil
			} else {
				t.post(ctx, msg)
			}
		}
	}
}

// Close closes both queues; Run drains any remaining buffered messages and
// exits. Critical-first prioritization is preserved during drain.
func (t *Telegram) Close() {
	t.stopOnce.Do(func() {
		close(t.queue)
		close(t.criticalQueue)
	})
	<-t.stopped
}

func (t *Telegram) post(ctx context.Context, text string) {
	// Plain text — no parse_mode — so special chars in error messages don't
	// cause the whole alert to be rejected.
	body, err := json.Marshal(map[string]any{
		"chat_id": t.chatID,
		"text":    text,
	})
	if err != nil {
		t.logger.Warn("telegram marshal failed", "error", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, "POST", t.base+"/bot"+t.token+"/sendMessage", bytes.NewReader(body))
	if err != nil {
		t.logger.Warn("telegram request build failed", "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.httpc.Do(req)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.logger.Warn("telegram send failed", "error", err)
		}
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.logger.Warn("telegram send non-200", "status", resp.StatusCode, "body", string(b))
	}
}

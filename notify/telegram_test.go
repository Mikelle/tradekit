package notify

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// recordingServer returns an httptest.Server that decodes each request body and
// sends its "text" field to sink, in arrival order. For tests that only care
// about delivery order/content, not the full wire format.
func recordingServer(sink chan<- string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		sink <- payload["text"].(string)
		w.WriteHeader(http.StatusOK)
	}))
}

// TestTelegram_PostsExpectedRequest verifies the wire format: HTTP POST to
// /bot<token>/sendMessage with a JSON body containing chat_id and text.
func TestTelegram_PostsExpectedRequest(t *testing.T) {
	got := make(chan map[string]any, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("want POST, got %s", r.Method)
		}
		if !strings.HasSuffix(r.URL.Path, "/botTEST_TOKEN/sendMessage") {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("bad body: %v (%s)", err, body)
		}
		got <- payload
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	tg := NewTelegram("TEST_TOKEN", "42", testLogger(), WithBaseURL(srv.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tg.Run(ctx)

	tg.Send("hello")
	select {
	case payload := <-got:
		if payload["chat_id"] != "42" {
			t.Errorf("chat_id: want 42, got %v", payload["chat_id"])
		}
		if payload["text"] != "hello" {
			t.Errorf("text: want hello, got %v", payload["text"])
		}
		if _, hasParseMode := payload["parse_mode"]; hasParseMode {
			t.Errorf("parse_mode must be absent (plain text default) but was set")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no request after 2s")
	}
	tg.Close()
}

// TestTelegram_EmptyCredentialsReturnsNil — callers are expected to fall
// back to NoOp when NewTelegram returns nil.
func TestTelegram_EmptyCredentialsReturnsNil(t *testing.T) {
	if NewTelegram("", "42", testLogger()) != nil {
		t.Errorf("empty token should return nil")
	}
	if NewTelegram("abc", "", testLogger()) != nil {
		t.Errorf("empty chatID should return nil")
	}
}

// TestTelegram_QueueFullDrops — when the HTTP server is slow and the queue
// fills, Send should drop messages rather than block.
func TestTelegram_QueueFullDrops(t *testing.T) {
	// Server blocks on a channel; we control exactly how many requests get through.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	tg := NewTelegram("TOKEN", "1", testLogger(), WithBaseURL(srv.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tg.Run(ctx)

	// Send more than queueSize messages quickly. Should not block.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < queueSize*3; i++ {
			tg.Send("msg")
		}
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Send blocked; should drop when queue is full")
	}
}

// TestNoOp_Send — sanity check the NoOp doesn't panic on calls.
func TestNoOp_Send(t *testing.T) {
	NoOp{}.Send("anything")
	NoOp{}.SendCritical("anything")
}

// TestTelegram_CriticalDrainsBeforeRegular ensures that a burst of regular
// messages doesn't delay a critical one — the critical-first peek in Run
// must take effect on every loop iteration. We flood the regular queue,
// then send one critical, and assert it arrives first.
func TestTelegram_CriticalDrainsBeforeRegular(t *testing.T) {
	got := make(chan string, queueSize+criticalQueueSize+4)
	srv := recordingServer(got)
	defer srv.Close()

	tg := NewTelegram("TOKEN", "1", testLogger(), WithBaseURL(srv.URL))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Pre-load the regular queue before starting the worker so the worker
	// definitely sees a backlog when it starts. Then enqueue one critical;
	// it must be delivered first.
	for i := 0; i < 10; i++ {
		tg.Send("regular")
	}
	tg.SendCritical("critical")

	go tg.Run(ctx)

	select {
	case first := <-got:
		if first != "critical" {
			t.Fatalf("first delivery should be critical, got %q", first)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message after 2s")
	}
	tg.Close()
}

// TestTelegram_SendCriticalFallsBackOnFullQueue verifies that when the
// critical queue is saturated AND no worker is draining, SendCritical
// falls back to a synchronous post (rather than dropping silently).
func TestTelegram_SendCriticalFallsBackOnFullQueue(t *testing.T) {
	posted := make(chan string, criticalQueueSize+4)
	srv := recordingServer(posted)
	defer srv.Close()

	tg := NewTelegram("TOKEN", "1", testLogger(), WithBaseURL(srv.URL))
	// Deliberately do NOT start Run — the queue won't drain. Fill it.
	for i := 0; i < criticalQueueSize; i++ {
		tg.SendCritical("queued")
	}
	// This one overflows: should sync-post directly.
	tg.SendCritical("overflow-must-deliver")

	select {
	case got := <-posted:
		if got != "overflow-must-deliver" {
			t.Fatalf("want sync-posted overflow message, got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sync fallback did not deliver within 2s")
	}
}

func TestTruncateForTelegram(t *testing.T) {
	cases := []struct {
		name     string
		in       string
		wantLen  int
		wantTail string
	}{
		{"under limit unchanged", strings.Repeat("a", 100), 100, ""},
		{"exact limit unchanged", strings.Repeat("a", telegramMaxMessage), telegramMaxMessage, "aaaa"},
		{"over limit truncated", strings.Repeat("a", telegramMaxMessage+1000), telegramMaxMessage, telegramTruncMark},
		{"huge payload truncated", strings.Repeat("a", 50_000), telegramMaxMessage, telegramTruncMark},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := truncateForTelegram(tc.in)
			if len(got) != tc.wantLen {
				t.Errorf("len = %d, want %d", len(got), tc.wantLen)
			}
			if tc.wantTail != "" && !strings.HasSuffix(got, tc.wantTail) {
				t.Errorf("got tail %q, want suffix %q", got[len(got)-len(tc.wantTail):], tc.wantTail)
			}
		})
	}
}

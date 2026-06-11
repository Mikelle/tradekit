package notify

// NoOp drops every message. Use when notifications are disabled so callers
// don't need nil-checks or config-aware branching.
type NoOp struct{}

// Send is a no-op.
func (NoOp) Send(string) {}

// SendCritical is a no-op.
func (NoOp) SendCritical(string) {}

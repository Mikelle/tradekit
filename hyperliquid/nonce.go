package hyperliquid

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// nonceStore hands out monotonically-increasing nonces and persists the last
// value to disk so restarts don't hand out stale timestamps that the exchange
// would reject as duplicates.
type nonceStore struct {
	mu   sync.Mutex
	path string
	last int64
}

func newNonceStore(path string) (*nonceStore, error) {
	s := &nonceStore{path: path}
	if path == "" {
		return s, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil
		}
		return nil, fmt.Errorf("read nonce file: %w", err)
	}

	v, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse nonce file: %w", err)
	}
	s.last = v
	return s, nil
}

// next returns the next nonce: max(last+1, now_ms), and persists it.
func (s *nonceStore) next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate := time.Now().UnixMilli()
	if candidate <= s.last {
		candidate = s.last + 1
	}
	s.last = candidate

	if s.path != "" {
		tmp := s.path + ".tmp"
		if err := os.WriteFile(tmp, []byte(strconv.FormatInt(candidate, 10)), 0600); err != nil {
			return 0, fmt.Errorf("write nonce tmp: %w", err)
		}
		if err := os.Rename(tmp, s.path); err != nil {
			return 0, fmt.Errorf("rename nonce file: %w", err)
		}
	}
	return candidate, nil
}

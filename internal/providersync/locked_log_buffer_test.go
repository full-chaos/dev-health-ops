package providersync

import (
	"bytes"
	"sync"
)

// lockedLogBuffer is a log sink a test installs as the process default logger:
// another goroutine may log while the test reads it, so every access is locked.
type lockedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

package restprove

import (
	"context"
	"sync"
	"testing"
	"time"
)

// endableContext is a run context the test ends at the moment an event
// happens -- a leg reaches its server, a mint is asked for, the report
// write begins -- with the error the run's own deadline
// (DeadlineExceeded) or a signal (Canceled) would carry. A test that ends
// its run this way never depends on how fast the host reaches that
// point.
type endableContext struct {
	context.Context
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	err  error
}

func newEndableContext() *endableContext {
	return &endableContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *endableContext) Done() <-chan struct{} { return c.done }

func (c *endableContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *endableContext) end(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		close(c.done)
	})
}

// awaitEvent blocks until event happens. It has no wall-clock bound of
// its own: a slow host only makes it wait longer. When the event never
// happens it fails by name just before the test binary's own -timeout
// would end the process, instead of a bare panic.
func awaitEvent(t *testing.T, event <-chan struct{}, what string) {
	t.Helper()
	deadline, ok := t.Deadline()
	if !ok {
		<-event
		return
	}
	grace := time.Until(deadline) / 20
	stop := time.NewTimer(time.Until(deadline) - grace)
	defer stop.Stop()
	select {
	case <-event:
	case <-stop.C:
		t.Fatalf("%s never happened before the test binary's own deadline", what)
	}
}

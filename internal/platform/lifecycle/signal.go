package lifecycle

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// SignalContext roots process work in SIGINT/SIGTERM cancellation. Tests may
// pass an explicit signal list; production callers normally use the defaults.
func SignalContext(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	if len(signals) == 0 {
		signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
	}
	return signal.NotifyContext(parent, signals...)
}

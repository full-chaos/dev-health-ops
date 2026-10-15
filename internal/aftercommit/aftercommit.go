// Package aftercommit lets code that runs inside a caller-owned transaction
// queue work for after that transaction commits.
//
// A write staged inside a transaction can still be rolled back by a later
// step or by the commit itself, so a log line or a counter that says "this
// happened" must wait for the commit. The owner of the transaction puts a
// Hooks on the context it passes down, and calls Run once the commit has
// succeeded; a rollback simply never calls Run and the queued work is dropped.
package aftercommit

import (
	"context"
	"sync"
)

type contextKey struct{}

// Hooks is the work queued for one transaction.
type Hooks struct {
	mu  sync.Mutex
	fns []func()
}

// WithHooks returns a context that carries a new Hooks, and that Hooks.
func WithHooks(ctx context.Context) (context.Context, *Hooks) {
	hooks := &Hooks{}
	return context.WithValue(ctx, contextKey{}, hooks), hooks
}

// Defer queues fn to run after the transaction that owns ctx commits. It
// reports whether fn was queued. When ctx carries no Hooks it reports false and
// does nothing, and the caller runs fn itself (there is no commit to wait for).
func Defer(ctx context.Context, fn func()) bool {
	hooks, ok := ctx.Value(contextKey{}).(*Hooks)
	if !ok || hooks == nil {
		return false
	}
	hooks.mu.Lock()
	defer hooks.mu.Unlock()
	hooks.fns = append(hooks.fns, fn)
	return true
}

// Run runs the queued work in the order it was queued, once. The owner calls it
// only after the commit returned nil.
func (hooks *Hooks) Run() {
	if hooks == nil {
		return
	}
	hooks.mu.Lock()
	fns := hooks.fns
	hooks.fns = nil
	hooks.mu.Unlock()
	for _, fn := range fns {
		fn()
	}
}

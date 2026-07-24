// Package system contains bounded operational handlers. It deliberately
// depends on domain interfaces instead of a River client or queue state.
package system

import (
	"context"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// RetentionStore deletes rows older than a cutoff from exactly one table. It
// is intentionally bounded and returns a count rather than rows, arguments, or
// tenant data. Implementations own their table as a literal so a policy can
// never be pointed at a wider target.
type RetentionStore interface {
	DeleteBefore(context.Context, time.Time, int) (int64, error)
}

// TerminalOutboxStore is the worker-outbox retention capability. It predates
// the policy map and keeps its own method name because the outbox repository
// owns completion fences alongside the rows themselves.
type TerminalOutboxStore interface {
	DeleteTerminalBefore(context.Context, time.Time, int) (int64, error)
}

type terminalOutboxRetention struct {
	store TerminalOutboxStore
}

func (adapter terminalOutboxRetention) DeleteBefore(
	ctx context.Context,
	before time.Time,
	batchSize int,
) (int64, error) {
	return adapter.store.DeleteTerminalBefore(ctx, before, batchSize)
}

// RetentionHandler routes one retention job to the single store that owns the
// requested policy. There is no default store and no fallback: an unknown or
// unregistered policy is a permanent failure, so a policy whose table nobody
// constructed cannot silently delete from a neighbouring one.
type RetentionHandler struct {
	stores map[string]RetentionStore
}

// NewRetentionHandler binds each supported policy to its owning store. Every
// contract-declared policy must be present; a partially wired handler would
// let the ops profile look ready while one retention family never ran.
func NewRetentionHandler(stores map[string]RetentionStore) (*RetentionHandler, error) {
	if len(stores) == 0 {
		return nil, errors.New("retention stores are required")
	}
	bound := make(map[string]RetentionStore, len(stores))
	for policy, store := range stores {
		if store == nil || !jobcontract.SupportedRetentionPolicy(policy) {
			return nil, errors.New("unsupported retention policy binding")
		}
		bound[policy] = store
	}
	for _, policy := range jobcontract.RetentionPolicies() {
		if _, ok := bound[policy]; !ok {
			return nil, errors.New("retention policy has no store")
		}
	}
	return &RetentionHandler{stores: bound}, nil
}

// NewTerminalOutboxRetentionStore adapts the worker-outbox repository to the
// shared retention store contract.
func NewTerminalOutboxRetentionStore(store TerminalOutboxStore) (RetentionStore, error) {
	if store == nil {
		return nil, errors.New("terminal outbox store is required")
	}
	return terminalOutboxRetention{store: store}, nil
}

// Work deletes one policy's expired rows. The durable maintenance-run claim is
// acquired by jobruntime before Work runs; repeating a completed checkpoint is
// therefore impossible, while an interrupted checkpoint is safe because
// deletion is set-based and bounded by the immutable cutoff.
func (handler *RetentionHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.RetentionCleanupArgs]) error {
	if handler == nil || len(handler.stores) == 0 || execution == nil {
		return jobruntime.Permanent(errors.New("retention handler is not configured"))
	}
	payload := execution.Args.Payload
	store, ok := handler.stores[payload.RetentionPolicy]
	if !ok {
		return jobruntime.Permanent(errors.New("unsupported retention policy"))
	}
	deleteBefore, err := time.Parse(time.RFC3339, payload.DeleteBefore)
	if err != nil || deleteBefore.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("retention cutoff is invalid"))
	}
	if _, err := store.DeleteBefore(ctx, deleteBefore, payload.BatchSize); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

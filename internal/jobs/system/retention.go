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

// TerminalOutboxStore is the only persistence capability required for the
// first retention policy. It is intentionally bounded and returns a count
// rather than rows, arguments, or tenant data.
type TerminalOutboxStore interface {
	DeleteTerminalBefore(context.Context, time.Time, int) (int64, error)
}

type RetentionHandler struct {
	store TerminalOutboxStore
}

func NewRetentionHandler(store TerminalOutboxStore) (*RetentionHandler, error) {
	if store == nil {
		return nil, errors.New("terminal outbox store is required")
	}
	return &RetentionHandler{store: store}, nil
}

// Work deletes one checkpoint-sized batch. The durable maintenance-run claim
// is acquired by jobruntime before Work runs; repeating a completed checkpoint
// is therefore impossible, while an interrupted checkpoint is safe because
// deletion is set-based and bounded by the immutable cutoff.
func (handler *RetentionHandler) Work(ctx context.Context, execution *jobruntime.Execution[jobruntime.RetentionCleanupArgs]) error {
	if handler == nil || handler.store == nil || execution == nil {
		return jobruntime.Permanent(errors.New("retention handler is not configured"))
	}
	payload := execution.Args.Payload
	if payload.RetentionPolicy != jobcontract.RetentionWorkerTerminal {
		return jobruntime.Permanent(errors.New("unsupported retention policy"))
	}
	deleteBefore, err := time.Parse(time.RFC3339, payload.DeleteBefore)
	if err != nil || deleteBefore.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("retention cutoff is invalid"))
	}
	if _, err := handler.store.DeleteTerminalBefore(ctx, deleteBefore, payload.BatchSize); err != nil {
		return jobruntime.Retryable(err)
	}
	return nil
}

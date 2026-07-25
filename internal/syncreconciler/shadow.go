package syncreconciler

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Shadow adapts the dormant Kernel to the observer Stepper contract used by
// the command lifecycle. It is intentionally constructed without a begin
// function, so command wiring cannot retain a write-capable transaction path.
type Shadow struct {
	kernel *Kernel
}

// NewShadow constructs the reconciler's only command-wirable kernel mode.
// Construction opens no transaction; Step delegates to the bounded observer
// path and never claims, publishes, or marks an outbox row.
func NewShadow(pool *pgxpool.Pool, registry Registry) (*Shadow, error) {
	observer, err := NewObserver(pool, registry)
	if err != nil {
		return nil, err
	}
	return newShadow(registry, observer)
}

func newShadow(registry Registry, observer Stepper) (*Shadow, error) {
	kernel, err := newKernel(registry, KernelModeShadow, observer, nil)
	if err != nil {
		return nil, err
	}
	return &Shadow{kernel: kernel}, nil
}

// Step preserves the existing observer contract for loop readiness and
// metrics. The lease value is required by the dormant kernel's input
// validation but is unreachable in shadow mode.
func (shadow *Shadow) Step(ctx context.Context, now time.Time, limit int) (Observation, error) {
	if shadow == nil || shadow.kernel == nil {
		return Observation{}, ErrInvalidConfiguration
	}
	result, err := shadow.kernel.Step(ctx, now, limit, minimumLeaseDuration, nil, nil)
	return result.Observation, err
}

var _ Stepper = (*Shadow)(nil)

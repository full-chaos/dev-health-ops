package workerservice

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// transientDialError marks a startup dependency failure that a later attempt
// can outlast: the endpoint refused, reset or dropped the connection, or the
// pooler had no free slot. It satisfies health.TransientError.
type transientDialError struct{ cause error }

func (e transientDialError) Error() string { return e.cause.Error() }
func (e transientDialError) Unwrap() error { return e.cause }
func (transientDialError) Transient() bool { return true }

// isTransientDialError reports whether err is a dial failure a later attempt
// can outlast; the classification lives with the pool code that logs it.
func isTransientDialError(err error) bool { return postgres.IsTransientDialError(err) }

// startupRetryJitter spreads a retry wait by up to +/-20% so replicas that
// started together do not redial in lockstep. Production wiring only; a nil
// jitter leaves the wait unchanged.
func startupRetryJitter(wait time.Duration) time.Duration {
	if wait <= 0 {
		return wait
	}
	spread := int64(wait) / 5
	if spread == 0 {
		return wait
	}
	return wait + time.Duration(rand.Int64N(2*spread+1)-spread)
}

// transientCauseAttrs is the bounded cause attribute of a retry or give-up
// line whose failure was an unreachable endpoint, and nothing for any other
// failure. It never carries error text.
func transientCauseAttrs(err error) []any {
	if isTransientDialError(err) {
		return []any{"cause", health.CauseUnreachable}
	}
	return nil
}

// retryPresenceStart retries the presence registration while the failure is a
// transient dial error. Its log lines carry only bounded fields, never error
// text, which can name the dial address.
func retryPresenceStart(
	ctx context.Context, component workerProcessComponent, start func(context.Context) error,
) error {
	return startupRetryBudget(
		ctx, component.startBudget, component.now, component.sleep, component.jitter,
		start,
		isTransientDialError,
		func(attempt int, elapsed, wait time.Duration, _ error) {
			if component.logger == nil {
				return
			}
			component.logger.WarnContext(ctx, "worker presence registration unreachable, retrying",
				"error_category", "dependency_unavailable",
				"cause", health.CauseUnreachable,
				"attempt", attempt,
				"elapsed", elapsed.String(),
				"retry_in", wait.String(),
			)
		},
		func(attempt int, elapsed time.Duration, err error, budgetExhausted bool) {
			if component.logger == nil {
				return
			}
			reason := "presence_start_failed"
			if budgetExhausted {
				reason = "retry_budget_exhausted"
			}
			component.logger.ErrorContext(ctx, "worker presence registration refused",
				append([]any{
					"error_category", "dependency_unavailable",
					"reason", reason,
					"attempts", attempt,
					"elapsed", elapsed.String(),
				}, transientCauseAttrs(err)...)...,
			)
		},
	)
}

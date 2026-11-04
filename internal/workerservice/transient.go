package workerservice

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// isTransientDialError reports whether err is a dial failure a later attempt
// can outlast; the classification lives with the pool code that logs it.
func isTransientDialError(err error) bool { return postgres.IsTransientDialError(err) }

// retryPresenceStart retries the presence registration while the failure is a
// transient dial error. Its log lines carry only bounded fields, never error
// text, which can name the dial address.
func retryPresenceStart(
	ctx context.Context, component workerProcessComponent, start func(context.Context) error,
) error {
	var last error
	return postgres.DialRetry{
		Budget: component.startBudget, Now: component.now, Sleep: component.sleep, Jitter: component.jitter,
		Retryable: isTransientDialError,
		OnRetry: func(attempt int, elapsed, wait time.Duration) {
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
		OnGiveUp: func(attempt int, elapsed time.Duration, budgetExhausted bool) {
			if component.logger == nil {
				return
			}
			reason := "presence_start_failed"
			if budgetExhausted {
				reason = "retry_budget_exhausted"
			}
			if isTransientDialError(last) {
				component.logger.ErrorContext(ctx, "worker presence registration refused",
					"error_category", "dependency_unavailable",
					"cause", health.CauseUnreachable,
					"reason", reason,
					"attempts", attempt,
					"elapsed", elapsed.String(),
				)
				return
			}
			component.logger.ErrorContext(ctx, "worker presence registration refused",
				"error_category", "dependency_unavailable",
				"reason", reason,
				"attempts", attempt,
				"elapsed", elapsed.String(),
			)
		},
	}.Do(ctx, func(ctx context.Context) error {
		last = start(ctx)
		return last
	})
}

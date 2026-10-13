// Package llmbudgetstale counts and logs BYO LLM budget reservations that have
// stayed in `reserved` past an age threshold. It only reads: a reservation is
// never freed by age (llmbudget/operator.go), because the provider may still
// bill. The log is what an operator acts on with the void verb.
package llmbudgetstale

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

const (
	// ThresholdEnv sets the age, in whole seconds, past which a `reserved` row
	// counts as stale.
	ThresholdEnv = "LLM_BUDGET_STALE_RESERVATION_SECONDS"
	// DefaultThreshold is far beyond any provider call's own timeout.
	DefaultThreshold = 24 * time.Hour
	minThreshold     = time.Minute
	maxThreshold     = 90 * 24 * time.Hour
)

// ThresholdFromEnv reads ThresholdEnv. A value that is set but cannot be
// honoured is an error, so an operator never runs an age they did not choose.
func ThresholdFromEnv(lookup func(string) (string, bool)) (time.Duration, error) {
	raw, present := lookup(ThresholdEnv)
	raw = strings.TrimSpace(raw)
	if !present || raw == "" {
		return DefaultThreshold, nil
	}
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds < int64(minThreshold.Seconds()) || seconds > int64(maxThreshold.Seconds()) {
		return 0, fmt.Errorf("%s must be a whole number of seconds between %d and %d",
			ThresholdEnv, int64(minThreshold.Seconds()), int64(maxThreshold.Seconds()))
	}
	return time.Duration(seconds) * time.Second, nil
}

// Lister reads the stale reservations.
type Lister interface {
	ListStale(ctx context.Context, olderThan time.Duration, now time.Time) ([]llmbudget.StaleReservation, error)
}

// PgLister lists through llmbudget.ListStale.
type PgLister struct{ DB llmbudget.Querier }

func (lister PgLister) ListStale(ctx context.Context, olderThan time.Duration, now time.Time) ([]llmbudget.StaleReservation, error) {
	return llmbudget.ListStale(ctx, lister.DB, olderThan, now)
}

// Handler binds the scheduled count to the River job runtime.
type Handler struct {
	lister    Lister
	threshold time.Duration
	logger    *slog.Logger
	now       func() time.Time
}

// NewHandler constructs the system.llm_budget_stale_reservations job owner.
func NewHandler(lister Lister, threshold time.Duration, logger *slog.Logger) (*Handler, error) {
	if lister == nil || logger == nil {
		return nil, errors.New("llm budget stale reservations lister and logger are required")
	}
	if threshold <= 0 {
		return nil, errors.New("llm budget stale reservations threshold must be positive")
	}
	return &Handler{lister: lister, threshold: threshold, logger: logger, now: time.Now}, nil
}

type orgTotal struct {
	count    int
	reserved int64
	oldest   time.Time
}

// Work counts the stale reservations per organization and logs them.
func (handler *Handler) Work(
	ctx context.Context,
	execution *jobruntime.Execution[jobruntime.LLMBudgetStaleReservationsArgs],
) error {
	if handler == nil || handler.lister == nil || execution == nil {
		return jobruntime.Permanent(errors.New("llm budget stale reservations handler is not configured"))
	}
	scheduledFor, err := time.Parse(time.RFC3339, execution.Args.Payload.ScheduledFor)
	if err != nil || scheduledFor.Location() != time.UTC {
		return jobruntime.Permanent(errors.New("llm budget stale reservations request is invalid"))
	}
	now := handler.now().UTC()
	rows, err := handler.lister.ListStale(ctx, handler.threshold, now)
	if err != nil {
		handler.logger.ErrorContext(ctx, "llm_budget_stale_reservations.run",
			slog.String("outcome", "list_failed"),
			slog.Int64("threshold_seconds", int64(handler.threshold.Seconds())))
		return jobruntime.Retryable(err)
	}
	totals := make(map[string]*orgTotal)
	for _, row := range rows {
		total := totals[row.OrgID]
		if total == nil {
			total = &orgTotal{oldest: row.CreatedAt}
			totals[row.OrgID] = total
		}
		total.count++
		total.reserved += row.ReservedMicroUSD
		if row.CreatedAt.Before(total.oldest) {
			total.oldest = row.CreatedAt
		}
	}
	orgs := make([]string, 0, len(totals))
	for org := range totals {
		orgs = append(orgs, org)
	}
	sort.Strings(orgs)
	for _, org := range orgs {
		total := totals[org]
		handler.logger.WarnContext(ctx, "llm_budget_stale_reservations.org",
			slog.String("org_id", org),
			slog.Int("stale_count", total.count),
			slog.Int64("reserved_micro_usd", total.reserved),
			slog.Int64("oldest_age_seconds", int64(now.Sub(total.oldest).Seconds())),
			slog.Int64("threshold_seconds", int64(handler.threshold.Seconds())))
	}
	handler.logger.InfoContext(ctx, "llm_budget_stale_reservations.run",
		slog.String("outcome", "counted"),
		slog.String("scheduled_for", scheduledFor.Format(time.RFC3339)),
		slog.Int("orgs", len(orgs)),
		slog.Int("stale_total", len(rows)),
		slog.Int64("threshold_seconds", int64(handler.threshold.Seconds())))
	return nil
}

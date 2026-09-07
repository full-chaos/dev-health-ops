package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// CapacityExecutor is the NATIVE implementation of the capacity
// remaining-metric kind (CUT-20 R2), the second compute family to leave the
// HTTP compatibility bridge.
//
// Like the DORA executor it replaces the bridge for this kind wholesale --
// per kind, no environment switch, no fallback -- and a worker that cannot
// build it declines to serve THIS KIND ONLY, leaving its siblings registered.
//
// # What makes this family different from DORA
//
// Capacity is STOCHASTIC. Its product rows are Monte Carlo output, drawn from
// CPython's Mersenne Twister after random.seed(generation_seed). That is why
// the seed is mandatory for this family and forbidden for every other one
// (postgres.go:557, mirrored on the Python side at worker_metrics.py:892), and
// why the compute runs on the cpyrandom port rather than Go's math/rand: the
// same seed must produce the same draws, or the two implementations are not
// computing the same function and no comparison between them means anything.
//
// # Fidelity traps reproduced deliberately
//
//  1. THE BACKLOG FALLBACK IS A FALSY CHECK, NOT A NIL CHECK. Python writes
//     `items = target_items if target_items else backlog` (job_capacity.py:94),
//     so a scope carrying target_items = 0 falls back to the backlog rather
//     than forecasting zero items. Treating it as "set" is the natural port and
//     produces a different forecast.
//
//  2. A SCOPE WITH NO HISTORY, OR NO ITEMS, IS SKIPPED -- NOT FAILED. Python
//     logs and continues to the next scope. Turning either into an error would
//     convert a tolerated partial into a lost partition, which is the
//     conversion CHAOS-4130's ruling exists to prevent.
//
//  3. THE READ WINDOW IS WALL-CLOCK. load_throughput_from_sink computes
//     `utc_today() - history_days` (capacity_queries.py:26), so the window --
//     and therefore which rows are loaded at all -- moves at UTC midnight.
//     Reproduced rather than pinned, because pinning it in Go alone would be
//     the divergence.
type CapacityExecutor struct {
	conn      driver.Conn
	observer  CapacityObserver
	nowUTC    func() time.Time
	batchSize int
	// logger restores the per-(org,team,scope) observability the deleted
	// Python job (job_capacity.py) had and the native executor's aggregate-only
	// counters (CapacityObserver) do not carry (CHAOS-5382). Defaults to
	// slog.Default() at construction, following ReleaseImpactExecutor's
	// convention, so every call site here can log unconditionally.
	logger *slog.Logger
}

// CapacityObserver reports what the native capacity executor actually did.
type CapacityObserver interface {
	// ObserveCapacityPartition reports one completed partition: how many
	// scopes it forecast, how many rows it wrote, and how many scopes it
	// SKIPPED for want of history or of a positive item target. The skip count
	// is the one that matters -- Python tolerates those scopes silently, so
	// without a counter a run that forecast nothing looks like a run that had
	// nothing to forecast.
	ObserveCapacityPartition(scopes, rowsWritten, skipped int) error
}

const capacityDefaultBatchSize = 1000

var errCapacityUnavailable = errors.New("capacity native executor unavailable")

// ErrCapacitySeedMissing reports a capacity run that carries no generation
// seed.
//
// Fails closed rather than substituting one. An unseeded Monte Carlo still
// produces plausible-looking numbers -- it simply produces DIFFERENT ones on
// every run, and no comparison against Python could ever hold. Python refuses
// identically (worker_metrics.py:892).
var ErrCapacitySeedMissing = errors.New(
	"capacity run is missing its generation seed")

// NewCapacityExecutor fails closed on a nil connection or an incompatible
// schema.
func NewCapacityExecutor(
	ctx context.Context, conn driver.Conn, observer CapacityObserver, logger *slog.Logger,
) (*CapacityExecutor, error) {
	if conn == nil {
		return nil, errCapacityUnavailable
	}
	// Checked at CONSTRUCTION, so a database this code cannot compute against
	// refuses the kind once and loudly, rather than letting the handler claim
	// partitions and fail every one of them.
	if err := verifyCapacitySchema(ctx, conn); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &CapacityExecutor{
		conn:      conn,
		observer:  observer,
		nowUTC:    func() time.Time { return time.Now().UTC() },
		batchSize: capacityDefaultBatchSize,
		logger:    logger,
	}, nil
}

// stringOrEmpty renders a possibly-nil scope identifier for logging. nil
// (an explicit scope naming no team/work-scope) and "" (discovery finding an
// unteamed row, see capacityTarget's own doc) both print as "" here -- the
// distinction that matters to the WRITER (NULL vs "") is not a distinction a
// log line needs to preserve.
func stringOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// logScopeStart restores job_capacity.py:78's per-(team,scope) Info line
// ("Computing forecast for team=%s, scope=%s"), lost when the native executor
// replaced Python's per-scope logging with an aggregate-only counter
// (CapacityObserver, CHAOS-5382).
func (executor *CapacityExecutor) logScopeStart(orgID string, target capacityTarget) {
	executor.logger.Info("capacity: computing forecast",
		"org_id", orgID,
		"team_id", stringOrEmpty(target.TeamID),
		"work_scope_id", stringOrEmpty(target.WorkScopeID))
}

// logNoHistory restores job_capacity.py:85's Warning for a scope with no
// throughput history. Python logs and continues; ComputePartition already
// skips the scope the same way (the CapacityObserver skip counter) -- this
// restores WHICH scope and WHY, which the counter alone cannot say.
func (executor *CapacityExecutor) logNoHistory(orgID string, target capacityTarget) {
	executor.logger.Warn("capacity: no throughput history for team/scope",
		"org_id", orgID,
		"team_id", stringOrEmpty(target.TeamID),
		"work_scope_id", stringOrEmpty(target.WorkScopeID))
}

// logNoTargetItems restores job_capacity.py:92's Warning for a scope that
// resolves to zero (or fewer) target items after the backlog fallback.
func (executor *CapacityExecutor) logNoTargetItems(orgID string, target capacityTarget) {
	executor.logger.Warn("capacity: no target items for team/scope",
		"org_id", orgID,
		"team_id", stringOrEmpty(target.TeamID),
		"work_scope_id", stringOrEmpty(target.WorkScopeID))
}

// logInsufficientHistory restores job_capacity.py:108's Warning for a scope
// whose forecast ran on fewer than the sufficient-history threshold of days.
// Unlike the no-history/no-target-items cases, this scope is NOT skipped --
// Python still forecasts and persists it, flagged.
func (executor *CapacityExecutor) logInsufficientHistory(orgID string, target capacityTarget, historyDays int) {
	executor.logger.Warn("capacity: insufficient history for team/scope",
		"org_id", orgID,
		"team_id", stringOrEmpty(target.TeamID),
		"work_scope_id", stringOrEmpty(target.WorkScopeID),
		"history_days", historyDays)
}

// logHighVariance restores job_capacity.py:112's Warning for a scope whose
// throughput coefficient of variation crossed the high-variance threshold.
func (executor *CapacityExecutor) logHighVariance(orgID string, target capacityTarget) {
	executor.logger.Warn("capacity: high throughput variance detected for team/scope",
		"org_id", orgID,
		"team_id", stringOrEmpty(target.TeamID),
		"work_scope_id", stringOrEmpty(target.WorkScopeID))
}

// logWroteForecasts restores job_capacity.py:120's Info line
// ("Persisted %d forecast(s)"). Gated on written > 0 the same way Python's
// `if persist and results:` was -- a partition that forecasts nothing logs
// nothing here either.
func (executor *CapacityExecutor) logWroteForecasts(orgID string, written int) {
	if written == 0 {
		return
	}
	executor.logger.Info("capacity: wrote forecast rows",
		"org_id", orgID, "table", "capacity_forecasts", "rows_written", written)
}

// ComputePartition runs the capacity forecast for one partition.
func (executor *CapacityExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, errCapacityUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}
	// The seed is what makes this family reproducible at all, and the run
	// table enforces its presence for capacity (postgres.go:557). Checking
	// again here keeps the executor honest on its own terms rather than
	// trusting an invariant enforced elsewhere.
	if run.Seed == nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s", ErrCapacitySeedMissing, partition.ID))
	}
	var scope capacityScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		// CHAOS-4242: the same claim-path precondition failure DORA hit --
		// static format, partition ID plus the decoder's own message; no
		// upstream content. Safe to surface at WARN.
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}

	today, err := executor.nowOrRefuse()
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	scopes, err := executor.resolveScopes(ctx, run.OrganizationID, scope)
	if err != nil {
		return CompatibilityOutcome{}, err
	}

	var rows []capacityRow
	var skipped int
	for _, target := range scopes {
		executor.logScopeStart(run.OrganizationID, target)
		history, err := executor.loadThroughput(
			ctx, run.OrganizationID, target, scope.HistoryDays, today,
		)
		if err != nil {
			return CompatibilityOutcome{}, err
		}
		// Python logs and continues; a scope with no history is not an error.
		if len(history.DailyThroughputs) == 0 {
			executor.logNoHistory(run.OrganizationID, target)
			skipped++
			continue
		}
		backlog, err := executor.loadBacklog(ctx, run.OrganizationID, target)
		if err != nil {
			return CompatibilityOutcome{}, err
		}

		items := resolveTargetItems(scope.TargetItems, backlog)
		if items <= 0 {
			executor.logNoTargetItems(run.OrganizationID, target)
			skipped++
			continue
		}

		request := numerical.ForecastRequest{
			History:     history,
			TargetItems: &items,
			BacklogSize: backlog,
			Simulations: scope.Simulations,
			Seed:        *run.Seed,
		}
		if scope.TargetDate != nil && *scope.TargetDate != "" {
			parsed, err := time.Parse("2006-01-02", *scope.TargetDate)
			if err != nil {
				return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
					"%w: partition %s target_date %q", ErrInvalidState,
					partition.ID, *scope.TargetDate))
			}
			parsed = parsed.UTC()
			request.TargetDate = &parsed
		}

		forecast, err := numerical.ForecastCapacity(request, today)
		if err != nil {
			return CompatibilityOutcome{}, fmt.Errorf("capacity forecast: %w", err)
		}
		if forecast.InsufficientHistory {
			executor.logInsufficientHistory(run.OrganizationID, target, history.DaysOfHistory)
		}
		if forecast.HighVariance {
			executor.logHighVariance(run.OrganizationID, target)
		}
		rows = append(rows, capacityRow{
			OrgID:       run.OrganizationID,
			TeamID:      target.TeamID,
			WorkScopeID: target.WorkScopeID,
			BacklogSize: backlog,
			TargetItems: &items,
			TargetDate:  request.TargetDate,
			Forecast:    forecast,
			ComputedAt:  today,
		})
	}

	written, err := executor.writeForecasts(ctx, rows)
	if err != nil {
		return CompatibilityOutcome{}, err
	}
	executor.logWroteForecasts(run.OrganizationID, written)
	if executor.observer != nil {
		// Telemetry never fails the partition: the work is durably written and
		// losing a counter must not cause a retry that writes it again.
		_ = executor.observer.ObserveCapacityPartition(len(scopes), written, skipped)
	}
	return CompatibilityOutcome{RowsWritten: &written}, nil
}

// capacityTarget is one (team, work scope) pair to forecast.
type capacityTarget struct {
	TeamID      *string
	WorkScopeID *string
}

type capacityRow struct {
	OrgID       string
	TeamID      *string
	WorkScopeID *string
	BacklogSize int
	TargetItems *int
	TargetDate  *time.Time
	Forecast    numerical.ForecastResult
	ComputedAt  time.Time
}

// resolveTargetItems ports `items = target_items if target_items else backlog`
// (job_capacity.py:94).
//
// Python's check is FALSY, not None-aware, so a scope carrying target_items = 0
// falls back to the backlog rather than forecasting zero items. A nil-check
// port disagrees on exactly that one input -- the input a hand-written fixture
// is least likely to contain.
//
// Extracted rather than written inline so the test exercises THIS function
// instead of restating the rule: a test that re-implements the logic it checks
// passes whatever production does.
func resolveTargetItems(targetItems *int, backlog int) int {
	if targetItems != nil && *targetItems != 0 {
		return *targetItems
	}
	return backlog
}

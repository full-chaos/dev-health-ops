package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	ctx context.Context, conn driver.Conn, observer CapacityObserver,
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
	return &CapacityExecutor{
		conn:      conn,
		observer:  observer,
		nowUTC:    func() time.Time { return time.Now().UTC() },
		batchSize: capacityDefaultBatchSize,
	}, nil
}

// ComputePartition runs the capacity forecast for one partition.
func (executor *CapacityExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) error {
	if executor == nil || executor.conn == nil {
		return errCapacityUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}
	// The seed is what makes this family reproducible at all, and the run
	// table enforces its presence for capacity (postgres.go:557). Checking
	// again here keeps the executor honest on its own terms rather than
	// trusting an invariant enforced elsewhere.
	if run.Seed == nil {
		return jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s", ErrCapacitySeedMissing, partition.ID))
	}
	var scope capacityScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		// CHAOS-4242: the same claim-path precondition failure DORA hit --
		// static format, partition ID plus the decoder's own message; no
		// upstream content. Safe to surface at WARN.
		return jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}

	today := executor.nowUTC()
	scopes, err := executor.resolveScopes(ctx, run.OrganizationID, scope)
	if err != nil {
		return err
	}

	var rows []capacityRow
	var skipped int
	for _, target := range scopes {
		history, err := executor.loadThroughput(
			ctx, run.OrganizationID, target, scope.HistoryDays, today,
		)
		if err != nil {
			return err
		}
		// Python logs and continues; a scope with no history is not an error.
		if len(history.DailyThroughputs) == 0 {
			skipped++
			continue
		}
		backlog, err := executor.loadBacklog(ctx, run.OrganizationID, target)
		if err != nil {
			return err
		}

		items := resolveTargetItems(scope.TargetItems, backlog)
		if items <= 0 {
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
				return jobruntime.WithSafeCause(fmt.Errorf(
					"%w: partition %s target_date %q", ErrInvalidState,
					partition.ID, *scope.TargetDate))
			}
			parsed = parsed.UTC()
			request.TargetDate = &parsed
		}

		forecast, err := numerical.ForecastCapacity(request, today)
		if err != nil {
			return fmt.Errorf("capacity forecast: %w", err)
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
		return err
	}
	if executor.observer != nil {
		// Telemetry never fails the partition: the work is durably written and
		// losing a counter must not cause a retry that writes it again.
		_ = executor.observer.ObserveCapacityPartition(len(scopes), written, skipped)
	}
	return nil
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

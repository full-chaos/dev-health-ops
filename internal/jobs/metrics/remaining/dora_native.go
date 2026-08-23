package remaining

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// DORAExecutor is the NATIVE implementation of the dora remaining-metric kind
// (CHAOS-3092 R1) -- the first compute family to leave the HTTP compatibility
// bridge.
//
// It satisfies the same one-method CompatibilityExecutor contract
// HTTPCompatibilityExecutor does, so the swap happens where the handler is
// constructed, per kind, with no environment switch and no fallback: a worker
// either builds the native executor for this kind or refuses to start the
// family (see cmd/dev-health-worker/daily.go). The other seven kinds keep the
// bridge until each has its own parity proof.
//
// # Fidelity, and where it is NOT obvious
//
// The Python job is the authority (src/dev_health_ops/metrics/job_dora.py:
// run_dora_metrics_job). Two of its behaviours are easy to "improve" into a
// divergence, so they are reproduced deliberately:
//
//  1. THE LOADING WINDOW AND THE COUNTING WINDOW ARE DIFFERENT, ON PURPOSE.
//     _load_deployments selects rows whose
//     coalesce(deployed_at, finished_at, started_at, last_synced) falls in the
//     day, but compute_dora_metrics_daily (compute_dora.py:80) then counts a
//     row by `deployed_at or started_at` alone. The SQL filter is a superset
//     that decides what is FETCHED; the compute filter decides what COUNTS.
//     Narrowing the SQL to match the compute filter looks like a
//     simplification and is not: a deployment with deployed_at NULL, an
//     out-of-window finished_at and an IN-window started_at is dropped by the
//     coalesce (finished_at wins the coalesce before started_at is reached),
//     so Python never counts it -- while a "tidier" query would fetch it and
//     count it. Same kernel, different answer.
//
//  2. computed_at IS STAMPED ONCE PER JOB, NOT ONCE PER DAY.
//     job_dora.py takes datetime.now(UTC) before the day loop, so every row a
//     backfill writes shares one timestamp. It carries no product meaning --
//     the parity manifest declares it volatile -- but a per-day stamp would be
//     a real behavioural difference in a column somebody may yet key on.
//
// Row selection reproduces _has_valid_repo: a row whose repo_id is not a
// parseable UUID is skipped rather than failing the partition, because Python
// tolerates it and the plan's own risk note warns that turning a tolerated
// partial into an error is how a port becomes data loss.
type DORAExecutor struct {
	conn      driver.Conn
	observer  DORAObserver
	nowUTC    func() time.Time
	batchSize int
}

// DORAObserver reports what the native executor actually did.
//
// The bridge could not: HTTPCompatibilityExecutor posts and reads a status
// code, so today a reader sees a partition claimed and completed and nothing
// about the compute inside it. Going native is the moment that becomes
// observable, and the degraded paths below are exactly the ones that vanish
// silently through the bridge.
type DORAObserver interface {
	// ObserveDORAPartition reports one completed partition: how many days it
	// covered, how many metric rows it wrote, and how many candidate rows it
	// skipped as unusable.
	ObserveDORAPartition(days int, rowsWritten int, skippedRows int) error
}

const doraDefaultBatchSize = 1000

var errDORAUnavailable = errors.New("dora native executor unavailable")

// defaultDORAMetrics mirrors DEFAULT_DORA_METRICS (job_dora.py). A scope that
// names no metrics gets the full set, exactly as _parse_metrics does.
var defaultDORAMetrics = []string{
	"deployment_frequency",
	"lead_time_for_changes",
	"time_to_restore_service",
	"change_failure_rate",
}

// NewDORAExecutor fails closed. A nil connection means the worker cannot
// compute this kind natively, and the family must refuse to start rather than
// silently fall back to the bridge -- the two-plane rule is that the native
// implementation REPLACES the executor for a kind wholesale.
func NewDORAExecutor(
	ctx context.Context, conn driver.Conn, observer DORAObserver,
) (*DORAExecutor, error) {
	if conn == nil {
		return nil, errDORAUnavailable
	}
	// The configured ordering contract must match the schema actually
	// deployed, and this is checked at CONSTRUCTION so a mismatch refuses the
	// family rather than silently computing wrong numbers job after job.
	//
	// This is not defensive padding. Migration 067 does not merely change
	// which SQL is emitted -- it rewrites the table, moving the sorting key to
	// (org_id, id, source_revision, source_conflict_key). Measured against a
	// freshly migrated database with two versions of one incident: the
	// contract-2 projection returns ONE row, while the legacy FINAL projection
	// returns TWO, because the two versions are now distinct primary keys and
	// FINAL no longer collapses them at all. A worker on the wrong branch does
	// not pick a different winner; it sees every version of an incident as a
	// separate incident, inflating incident counts and therefore
	// time_to_restore_service. Legacy is the default when the environment
	// variable is unset, so this is reachable by omission.
	//
	// Python performs the equivalent check (guard_operational_writer_tables,
	// called from ClickHouseStore.__aenter__); Go had none, and a port missing
	// a guard the original has is a gap rather than restraint.
	if err := verifyOrderingContract(ctx, conn); err != nil {
		return nil, err
	}
	return &DORAExecutor{
		conn:      conn,
		observer:  observer,
		nowUTC:    func() time.Time { return time.Now().UTC() },
		batchSize: doraDefaultBatchSize,
	}, nil
}

// ComputePartition runs the dora computation for one partition.
func (executor *DORAExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) error {
	if executor == nil || executor.conn == nil {
		return errDORAUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return fmt.Errorf("%w: partition %s has no organization", ErrInvalidState, partition.ID)
	}
	var scope doraScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		return fmt.Errorf("%w: partition %s scope: %v", ErrInvalidState, partition.ID, err)
	}
	day, err := time.Parse("2006-01-02", scope.Day)
	if err != nil {
		return fmt.Errorf("%w: partition %s day %q", ErrInvalidState, partition.ID, scope.Day)
	}
	if scope.BackfillDays < 1 {
		return fmt.Errorf("%w: partition %s backfill_days", ErrInvalidState, partition.ID)
	}

	// One stamp for the whole partition, before the day loop -- see (2) above.
	computedAt := executor.nowUTC()
	wanted := metricFilter(scope.Metrics)

	var rowsWritten, skipped int
	for _, current := range dayRange(day, scope.BackfillDays) {
		deployments, deploymentSkips, err := executor.loadDeployments(
			ctx, run.OrganizationID, current, scope,
		)
		if err != nil {
			return err
		}
		incidents, incidentSkips, err := executor.loadIncidents(
			ctx, run.OrganizationID, current, scope,
		)
		if err != nil {
			return err
		}
		skipped += deploymentSkips + incidentSkips

		rows := make([]doraRow, 0, len(defaultDORAMetrics))
		for _, metric := range numerical.ComputeDORA(current, deployments, incidents) {
			if _, ok := wanted[metric.Name]; !ok {
				continue
			}
			rows = append(rows, doraRow{
				OrgID:      run.OrganizationID,
				RepoID:     metric.RepoID,
				Day:        current,
				MetricName: metric.Name,
				Value:      metric.Value,
				ComputedAt: computedAt,
			})
		}
		written, err := executor.writeMetrics(ctx, rows)
		if err != nil {
			return err
		}
		rowsWritten += written
	}

	if executor.observer != nil {
		// Telemetry failure never fails the partition: the work is already
		// durably written, and losing a counter must not cause a retry that
		// writes it a second time.
		_ = executor.observer.ObserveDORAPartition(scope.BackfillDays, rowsWritten, skipped)
	}
	return nil
}

// dayRange mirrors _date_range (job_dora.py): backfill_days <= 1 is just the
// end day, otherwise the inclusive window ENDING at it.
func dayRange(end time.Time, backfillDays int) []time.Time {
	if backfillDays <= 1 {
		return []time.Time{end}
	}
	days := make([]time.Time, 0, backfillDays)
	start := end.AddDate(0, 0, -(backfillDays - 1))
	for offset := 0; offset < backfillDays; offset++ {
		days = append(days, start.AddDate(0, 0, offset))
	}
	return days
}

// metricFilter mirrors _parse_metrics: an empty or absent list means the full
// default set, and a list that parses to nothing also falls back to it.
func metricFilter(raw *string) map[string]struct{} {
	wanted := make(map[string]struct{}, len(defaultDORAMetrics))
	if raw != nil {
		for _, name := range strings.Split(*raw, ",") {
			trimmed := strings.TrimSpace(name)
			if trimmed != "" {
				wanted[trimmed] = struct{}{}
			}
		}
	}
	if len(wanted) == 0 {
		for _, name := range defaultDORAMetrics {
			wanted[name] = struct{}{}
		}
	}
	return wanted
}

type doraRow struct {
	OrgID      string
	RepoID     string
	Day        time.Time
	MetricName string
	Value      float64
	ComputedAt time.Time
}

func utcDayWindow(day time.Time) (time.Time, time.Time) {
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

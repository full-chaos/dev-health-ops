package daily

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// IncidentMetricsDailyRecord is one incident_metrics_daily row -- the Go
// counterpart of IncidentMetricsDailyRecord (metrics/schemas.py:649).
type IncidentMetricsDailyRecord struct {
	RepoID         uuid.UUID
	Day            time.Time
	IncidentsCount uint32
	MTTRP50Hours   *float64
	MTTRP90Hours   *float64
}

// IncidentExecutor is the NATIVE implementation of the incident
// metrics.daily family (CHAOS-4295), landing WITH the CHAOS-4269 NULL-guard
// fix (port-with-fix -- chris/team-lead standing order: no Python patch).
//
// # Fidelity and the fix
//
// The Python job is the authority for everything except the one predicate
// this ticket exists to fix (compute_incidents.py:40
// compute_incident_metrics_daily, fed by loaders/clickhouse.py:1234
// load_incidents via active_incidents_query(window=STARTED, ...)).
// LoadIncidentsStarted's doc comment has the full root-cause trace and the
// executed evidence citation; ComputeFamily below ports
// compute_incident_metrics_daily byte-for-byte otherwise:
//
//  1. compute_incident_metrics_daily is called ONCE PER REPO in Python
//     (job_daily.py's `for repo_id in repo_ids` loop feeds
//     load_incidents(..., repo_id=repo_id) once per iteration). This
//     executor loads every repo in the partition in ONE query and buckets
//     by repo_id in-memory (computeIncidentMetricsDaily, mirroring the
//     Python function's own `by_repo` dict) -- provably equivalent, since
//     every filter compute_incident_metrics_daily applies (resolved_at day
//     window, mttr >= 0) is row-local, and Python's own loader already
//     scopes started_at to the partition's single day regardless of which
//     repo it's called for.
//  2. GO ≠ PYTHON HERE BY DESIGN (CHAOS-4269): Python's
//     active_incidents_query drops every mapping_kind="repository_derived"
//     row via `valid_from <= as_of` with no NULL-OK guard (map_issue_incidents
//     never sets valid_from, so the predicate is always NULL, always
//     false) -- the whole family computes zero rows, every day, in Python.
//     This executor adds `valid_from IS NULL OR valid_from <= as_of`,
//     mirroring the symmetric guard valid_to already has. The two paths
//     will therefore disagree on every partition that has ANY
//     repository-derived incident mapping: Python silently writes nothing
//     for those rows, Go writes the correct incidents_count/mttr rows. This
//     is the intended outcome, not a parity gap to close -- see this PR's
//     RISK-NOTES.
//  3. Percentiles use the SAME linear-interpolation kernel as Python's
//     module-level _percentile (compute_incidents.py:23) -- see
//     incidentPercentile's doc comment.
type IncidentExecutor struct {
	conn     driver.Conn
	nowUTC   func() time.Time
	observer jobruntime.IncidentValidFromGuardObserver
}

var errIncidentUnavailable = fmt.Errorf("incident native executor unavailable")

// NewIncidentExecutor fails closed on a nil connection, matching
// NewRepoUserCommitExecutor/NewTeamWellbeingExecutor's construction-time
// policy: a refused executor simply never enters PartitionHandler's native
// family map, and incident stays on the Python compatibility bridge --
// which still computes zero rows for repository-derived mappings, exactly
// as it does today, until the executor can be constructed.
func NewIncidentExecutor(conn driver.Conn) (*IncidentExecutor, error) {
	if conn == nil {
		return nil, errIncidentUnavailable
	}
	return &IncidentExecutor{
		conn:   conn,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// SetValidFromGuardObserver wires the optional CHAOS-4269 telemetry
// observer. Never required for construction: a nil observer (the default)
// simply means this deployment does not yet have the counter wired, exactly
// like every other optional observer in this package.
func (executor *IncidentExecutor) SetValidFromGuardObserver(observer jobruntime.IncidentValidFromGuardObserver) {
	if executor == nil {
		return
	}
	executor.observer = observer
}

// ComputeFamily implements NativeFamilyExecutor.
func (executor *IncidentExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errIncidentUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}
	if len(repoIDs) == 0 {
		return 0, nil
	}

	dayStart := time.Date(run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)
	computedAt := executor.nowUTC()

	incidents, err := LoadIncidentsStarted(ctx, executor.conn, run.OrganizationID, repoIDs, dayStart, dayEnd, computedAt, executor.observer)
	if err != nil {
		return 0, err
	}

	records := computeIncidentMetricsDaily(dayStart, incidents)
	if len(records) == 0 {
		return 0, nil
	}

	written, err := WriteIncidentMetricsDaily(ctx, executor.conn, run.OrganizationID, records, computedAt)
	if err != nil {
		return 0, err
	}
	return written, nil
}

// computeIncidentMetricsDaily ports compute_incident_metrics_daily
// (metrics/compute_incidents.py:40) exactly: incidents_count and MTTR
// distributions for incidents RESOLVED on `day`, bucketed by repo_id.
// started_at is used only to compute MTTR (resolved_at - started_at), never
// to gate inclusion here -- that filter already happened in the loader
// (LoadIncidentsStarted scopes started_at to `day`, matching
// active_incidents_query(window=STARTED, ...)'s own filter, which
// job_daily.py's load_incidents call already applies before Python's
// compute_incident_metrics_daily ever sees a row).
func computeIncidentMetricsDaily(day time.Time, incidents []IncidentRow) []IncidentMetricsDailyRecord {
	dayStart := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	type bucket struct {
		incidents int
		mttrHours []float64
	}
	byRepo := make(map[uuid.UUID]*bucket)
	for _, row := range incidents {
		if row.ResolvedAt == nil {
			continue
		}
		resolvedAt := row.ResolvedAt.UTC()
		if resolvedAt.Before(dayStart) || !resolvedAt.Before(dayEnd) {
			continue
		}
		b, ok := byRepo[row.RepoID]
		if !ok {
			b = &bucket{}
			byRepo[row.RepoID] = b
		}
		b.incidents++
		mttr := resolvedAt.Sub(row.StartedAt.UTC()).Hours()
		if mttr >= 0 {
			b.mttrHours = append(b.mttrHours, mttr)
		}
	}

	repoIDs := make([]uuid.UUID, 0, len(byRepo))
	for repoID := range byRepo {
		repoIDs = append(repoIDs, repoID)
	}
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i].String() < repoIDs[j].String() })

	records := make([]IncidentMetricsDailyRecord, 0, len(repoIDs))
	for _, repoID := range repoIDs {
		b := byRepo[repoID]
		record := IncidentMetricsDailyRecord{
			RepoID:         repoID,
			Day:            dayStart,
			IncidentsCount: uint32(b.incidents),
		}
		if len(b.mttrHours) > 0 {
			p50 := incidentPercentile(b.mttrHours, 50.0)
			p90 := incidentPercentile(b.mttrHours, 90.0)
			record.MTTRP50Hours = &p50
			record.MTTRP90Hours = &p90
		}
		records = append(records, record)
	}
	return records
}

// incidentPercentile ports compute_incidents.py's module-level _percentile:
// linear interpolation between closest ranks (NOT the truncating integer
// variant numerical.IntegerPercentiles uses for the capacity family --
// compute_incidents.py._percentile and compute_capacity._percentile are
// different kernels in Python and must stay different here too, same
// distinction repouser.percentile's doc comment already draws for the
// repo_user_commit family).
func incidentPercentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch {
	case pct <= 0:
		return minIncidentFloat(values)
	case pct >= 100:
		return maxIncidentFloat(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := float64(len(sorted)-1) * (pct / 100.0)
	lo := int(rank)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func minIncidentFloat(values []float64) float64 {
	min := values[0]
	for _, v := range values[1:] {
		if v < min {
			min = v
		}
	}
	return min
}

func maxIncidentFloat(values []float64) float64 {
	max := values[0]
	for _, v := range values[1:] {
		if v > max {
			max = v
		}
	}
	return max
}

var _ NativeFamilyExecutor = (*IncidentExecutor)(nil)

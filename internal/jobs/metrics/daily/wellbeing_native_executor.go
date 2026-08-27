package daily

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// TeamWellbeingExecutor is the NATIVE implementation of the team_wellbeing
// metrics.daily family (CHAOS-4276) -- the second family to leave the HTTP
// compatibility bridge (after CHAOS-3092 R1's native DORA executor for the
// remaining bridge), and the FIRST for the daily bridge's per-partition,
// per-family cutover mechanism (PartitionHandler.SetNativeFamilies).
//
// # Fidelity
//
// The Python job is the authority
// (src/dev_health_ops/metrics/compute_wellbeing.py:39,
// compute_team_wellbeing_metrics_daily, invoked from job_daily.py without an
// identity_resolver -- see numerical.ComputeTeamWellbeing's doc comment for
// why that matters). Two things about THIS executor specifically are easy to
// get wrong:
//
//  1. TEAM ATTRIBUTION'S MEMBERSHIP FALLBACK IS UNDER SEPARATE REVIEW
//     (CHAOS-4321, chris: membership-based team attribution may end up
//     applying only under a manual override). This executor intentionally
//     ports the CURRENT Python behavior (repo-pattern first, membership
//     fallback second, see numerical.ComputeTeamWellbeing) rather than
//     anticipating that ruling -- see this PR's RISK-NOTES.
//  2. computed_at IS STAMPED ONCE PER REPO GROUP, NOT ONCE PER PARTITION
//     (revised in codex round-2, see WriteTeamMetricsDailyPerRepo's doc
//     comment): team_metrics_daily's (org_id, team_id, day) reader dedup
//     picks a row via argMax(<col>, computed_at), so every repo's rows
//     sharing one partition-wide timestamp made that tie-break
//     implementation-defined for any team spanning more than one repo.
//     Distinct, real (never fabricated) timestamps per repo group restore
//     the same "whichever repo was processed last wins" determinism
//     Python's real per-repo_id call cadence already has.
//  3. TEAM BUCKETS RESET PER REPO, NOT PER PARTITION (codex round-1 finding,
//     see computeWellbeingPerRepo's doc comment) -- the Python bridge calls
//     compute_team_wellbeing_metrics_daily once per repo_id, so this executor
//     runs numerical.ComputeTeamWellbeing once per repo in the partition and
//     concatenates the results, rather than aggregating every repo's commits
//     into one call.
//  4. A TEAM SPANNING MULTIPLE REPOS NOW SURFACES EVERY REPO'S SLICE
//     (CHAOS-4329 -- was pre-existing in Python and mirrored, not fixed, by
//     this port's original CHAOS-4276 landing; codex round-3 on THAT PR
//     found and deliberately deferred it). team_metrics_daily gained a
//     repo_id column (migration 080, empty string on legacy rows written before it)
//     and every known reader (cognitive_load.py, native_team_workload.py,
//     metrics/scoring/wellbeing.py, recommendations/loader.py) now dedups
//     per (org_id, team_id, repo_id, day) THEN sums the additive counts
//     across repos and recomputes the ratio, instead of collapsing straight
//     to (org_id, team_id, day). This executor and job_daily.py's
//     write_team_metrics both still write one row PER REPO for a multi-repo
//     team -- that no longer loses data now that repo_id makes each row's
//     key distinct.
type TeamWellbeingExecutor struct {
	conn               driver.Conn
	businessTZ         *time.Location
	businessHoursStart int
	businessHoursEnd   int
	nowUTC             func() time.Time
	// repoCountObserver (CHAOS-4329) is optional -- set via
	// SetRepoCountObserver, mirroring PartitionHandler's
	// SetZeroRowsObserver/SetNativeFamilyObserver pattern (cmd/dev-health-worker/
	// daily.go asserts the runtime's Observer against the narrow
	// jobruntime.TeamMetricsDailyRepoCountObserver interface). nil means no
	// observer wired -- ComputeFamily degrades to not recording, never panics.
	repoCountObserver jobruntime.TeamMetricsDailyRepoCountObserver
}

// SetRepoCountObserver wires the optional per-team repo-fan-out observer
// (CHAOS-4329). Never required for construction: a nil observer (the
// default) simply means this deployment does not yet have telemetry wired,
// exactly like PartitionHandler's other optional observers.
func (executor *TeamWellbeingExecutor) SetRepoCountObserver(observer jobruntime.TeamMetricsDailyRepoCountObserver) {
	if executor == nil {
		return
	}
	executor.repoCountObserver = observer
}

var errTeamWellbeingUnavailable = errors.New("team_wellbeing native executor unavailable")

// NewTeamWellbeingExecutor fails closed. Business-hours configuration is
// resolved ONCE here, exactly like DORAExecutor's ordering contract: a
// mid-flight environment change must not split one partition's bucketing
// across two configurations, and an unparseable value must refuse
// construction rather than silently defaulting (job_daily.py's
// int(os.getenv(...)) would raise ValueError on the equivalent malformed
// input -- silently falling back to the default here would make the Go path
// MORE tolerant than the Python original it replaces, which is its own kind
// of drift).
func NewTeamWellbeingExecutor(conn driver.Conn) (*TeamWellbeingExecutor, error) {
	if conn == nil {
		return nil, errTeamWellbeingUnavailable
	}
	tzName := os.Getenv("BUSINESS_TIMEZONE")
	if tzName == "" {
		tzName = "UTC"
	}
	tz, err := time.LoadLocation(tzName)
	if err != nil {
		return nil, fmt.Errorf("%w: BUSINESS_TIMEZONE %q: %v", errTeamWellbeingUnavailable, tzName, err)
	}
	start, err := envInt("BUSINESS_HOURS_START", 9)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errTeamWellbeingUnavailable, err)
	}
	end, err := envInt("BUSINESS_HOURS_END", 17)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errTeamWellbeingUnavailable, err)
	}
	return &TeamWellbeingExecutor{
		conn: conn, businessTZ: tz, businessHoursStart: start, businessHoursEnd: end,
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

func envInt(key string, fallback int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s=%q is not an integer: %w", key, raw, err)
	}
	return value, nil
}

// ComputeFamily runs the team_wellbeing computation for one partition.
func (executor *TeamWellbeingExecutor) ComputeFamily(
	ctx context.Context, run Run, partition Partition,
) (int, error) {
	if executor == nil || executor.conn == nil {
		return 0, errTeamWellbeingUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: partition %s run has no organization or target day", ErrInvalidState, partition.ID)
	}

	// Validated before any ClickHouse round-trip: a malformed durable
	// partition scope is a precondition failure, not a transient dependency
	// error, and must not spend a query proving that.
	repoIDs, err := parseRepositoryUUIDs(partition.RepoIDs)
	if err != nil {
		return 0, fmt.Errorf("%w: partition %s repo_ids: %v", ErrInvalidState, partition.ID, err)
	}

	teams, err := LoadWellbeingTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	repoResolver := NewRepoPatternResolver(teams)
	memberResolver := NewMemberResolver(teams)

	repoNamesByID, err := LoadRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}

	day := time.Date(run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(), 0, 0, 0, 0, time.UTC)
	start := day
	end := start.Add(24 * time.Hour)
	commits, err := LoadWellbeingCommits(ctx, executor.conn, run.OrganizationID, start, end, repoIDs)
	if err != nil {
		return 0, err
	}

	perRepoMetrics := computeWellbeingPerRepo(
		day, repoIDs, commits, repoNamesByID, repoResolver, memberResolver,
		executor.businessTZ, executor.businessHoursStart, executor.businessHoursEnd,
	)

	// One HONEST, real-wall-clock timestamp per repo group, captured in
	// repoIDs' own deterministic order -- see WriteTeamMetricsDailyPerRepo's
	// doc comment (CHAOS-4276 codex round-2 finding 1). computed_at must
	// stay truthful: a codex round-3 pass proposed spacing these artificially
	// (base + index*1s) to survive team_metrics_daily.computed_at's
	// second-level storage precision, but chris/team-lead ruled that out --
	// see this file's package doc comment and this PR's RISK-NOTES for why
	// a tie here is a symptom of a DIFFERENT, pre-existing defect
	// (team_metrics_daily has no repo_id in its key at all, so Python has
	// the identical "last-repo-processed-wins" property already), not
	// something a fabricated timestamp should paper over.
	computedAtByRepo := make([]time.Time, len(perRepoMetrics))
	for index := range perRepoMetrics {
		computedAtByRepo[index] = executor.nowUTC()
	}

	written, err := WriteTeamMetricsDailyPerRepo(ctx, executor.conn, run.OrganizationID, day, perRepoMetrics, computedAtByRepo)
	if err != nil {
		return 0, err
	}

	// CHAOS-4329: observe AFTER the write durably lands (mirrors
	// ObserveZeroUnitFinalization's post-commit rule elsewhere in this
	// repo), once per team, from the EXACT rows just written -- grouping
	// perRepoMetrics by TeamID and counting each team's distinct RepoID,
	// exactly like Python's record_team_metrics_daily_repo_rows groups the
	// list it was handed. A nil observer (not yet wired) is a no-op, never
	// a failure.
	if executor.repoCountObserver != nil {
		reposByTeam := make(map[string]map[string]struct{})
		for _, group := range perRepoMetrics {
			for _, metric := range group {
				repos := reposByTeam[metric.TeamID]
				if repos == nil {
					repos = make(map[string]struct{})
					reposByTeam[metric.TeamID] = repos
				}
				repos[metric.RepoID] = struct{}{}
			}
		}
		for _, repos := range reposByTeam {
			_ = executor.repoCountObserver.ObserveTeamMetricsDailyRepoCount(len(repos))
		}
	}

	return written, nil
}

// computeWellbeingPerRepo groups commits by repo and runs
// numerical.ComputeTeamWellbeing once per repoID (in repoIDs' own order),
// returning one row-group PER REPO rather than a single concatenated slice
// -- callers that persist the result need each group kept separate so they
// can stamp it with its own write-time timestamp (WriteTeamMetricsDailyPerRepo,
// CHAOS-4276 codex round-2 finding 1). A repo with no commits that day, or
// whose commits produced no rows, contributes no group (never an empty one).
//
// CHAOS-4276 codex round-1 (finding 2): the Python bridge invokes
// compute_team_wellbeing_metrics_daily ONCE PER repo_id (worker_metrics.py's
// `for repo_id in repo_ids` loop, CHAOS-4264 -- "each run_daily_metrics_job
// call loads and releases only that repo's source rows"), scoping every
// call's commit set -- and therefore every team's commit/after-hours/weekend
// bucket -- to that one repo alone. Running ComputeTeamWellbeing once per
// repo here mirrors that loop's bucket-reset boundary exactly, so a
// multi-repo partition writes the same per-repo-scoped rows Python does.
// Iterating in repoIDs' own (sorted, deterministic -- see
// normalizeRepositoryPartitions) order keeps this reproducible run to run
// regardless of ClickHouse row-return order. A repo-less commit (repoIDs
// empty) never happens for a real dispatched partition -- partitionRepositoryIDs
// never emits an empty chunk -- so it is not specially handled here: an
// empty repoIDs simply loops zero times and writes nothing, matching
// Python's `for repo_id in repo_ids` loop body never running either.
//
// CHAOS-4329: this per-repo call structure is no longer load-bearing for
// correctness the way it originally was -- ComputeTeamWellbeing itself now
// buckets by (teamID, repoID) internally (see its doc comment), so a single
// call over every repo's commits combined would produce the identical
// per-repo-keyed rows this function's repo-by-repo loop produces. The loop
// is kept anyway: it is what gives WriteTeamMetricsDailyPerRepo a distinct
// computedAtByRepo group per repo (harmless, no longer required either --
// see that function's doc comment), and changing it is out of this ticket's
// scope.
func computeWellbeingPerRepo(
	day time.Time,
	repoIDs []uuid.UUID,
	commits []numerical.Commit,
	repoNamesByID map[string]string,
	repoResolver numerical.RepoTeamResolver,
	memberResolver numerical.MemberTeamResolver,
	businessTZ *time.Location,
	businessHoursStart, businessHoursEnd int,
) [][]numerical.TeamWellbeingMetric {
	commitsByRepo := make(map[string][]numerical.Commit, len(repoIDs))
	for _, commit := range commits {
		commitsByRepo[commit.RepoID] = append(commitsByRepo[commit.RepoID], commit)
	}

	var perRepo [][]numerical.TeamWellbeingMetric
	for _, repoID := range repoIDs {
		repoCommits := commitsByRepo[repoID.String()]
		if len(repoCommits) == 0 {
			continue
		}
		metrics := numerical.ComputeTeamWellbeing(
			day, repoCommits, repoNamesByID, repoResolver, memberResolver,
			businessTZ, businessHoursStart, businessHoursEnd,
		)
		if len(metrics) == 0 {
			continue
		}
		perRepo = append(perRepo, metrics)
	}
	return perRepo
}

func parseRepositoryUUIDs(ids []RepositoryID) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	result := make([]uuid.UUID, len(ids))
	for index, id := range ids {
		parsed, err := uuid.Parse(string(id))
		if err != nil {
			return nil, err
		}
		result[index] = parsed
	}
	return result, nil
}

var _ NativeFamilyExecutor = (*TeamWellbeingExecutor)(nil)

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
//  2. computed_at IS STAMPED ONCE PER (run, partition) CALL, mirroring
//     DORAExecutor's "one stamp for the whole partition" rule: it carries no
//     product meaning (the parity manifest already treats it as volatile
//     column-wise), but a per-row or per-query stamp would be a real
//     behavioural difference in a column somebody may key on.
type TeamWellbeingExecutor struct {
	conn               driver.Conn
	businessTZ         *time.Location
	businessHoursStart int
	businessHoursEnd   int
	nowUTC             func() time.Time
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

	metrics := numerical.ComputeTeamWellbeing(
		day, commits, repoNamesByID, repoResolver, memberResolver,
		executor.businessTZ, executor.businessHoursStart, executor.businessHoursEnd,
	)

	written, err := WriteTeamMetricsDaily(ctx, executor.conn, run.OrganizationID, day, executor.nowUTC(), metrics)
	if err != nil {
		return 0, err
	}
	return written, nil
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

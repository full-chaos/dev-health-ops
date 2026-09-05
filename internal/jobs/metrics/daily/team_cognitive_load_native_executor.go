package daily

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// TeamCognitiveLoadExecutor is the NATIVE, FINALIZE-SCOPE implementation of
// the team_cognitive_load family (CHAOS-5141, CHAOS-4365 item 2 / 4347-C).
//
// Unlike a pre_bridge/post_bridge PARTITION family, this runs once per RUN,
// via daily.FinalizeHandler's native-finalize-family mechanism (CHAOS-4290,
// #2241): Python's own equivalent (job_daily.py's
// _write_team_cognitive_load_for_day) aggregates THIS RUN's already-computed,
// in-process user_metrics_daily/team_metrics_daily rows -- a Go finalize job
// shares no memory with the daily_partition jobs that wrote them, so this
// executor reads them BACK from ClickHouse instead, deduped the same way
// team_complexity.go's finalize-scope sibling does (repo_complexity_daily's
// own argMax(tuple(...), computed_at) readback).
//
// Team resolution mirrors Python's repo_to_team build in
// _write_team_cognitive_load_for_day exactly: team_repo_ownership
// (teamownership.OwnedRepoIDs, the already-hardened bitemporal/matched-
// sentinel port of load_team_repo_ownership_map) wins where it resolves a
// repo, falling back to the repo-pattern resolver (LoadWellbeingTeams +
// NewRepoPatternResolver, already built for team_wellbeing) only for repos
// team_repo_ownership does not cover. CHAOS-4396 hard rule preserved: this
// executor never reads either input table's own team_id column.
type TeamCognitiveLoadExecutor struct {
	conn   driver.Conn
	writer *teamCognitiveLoadWriter
	nowUTC func() time.Time
}

// TeamCognitiveLoadFamilyName re-exports the single source of truth for this
// family's families.json/skip_families key, mirroring icfinalize.FamilyName's
// shape (#2243): a registration site indexes by this constant rather than
// restating the literal, so a future move of the registration (into the
// finalize map #2243 introduces) resolves it FROM the package instead of a
// second, driftable copy. The same literal is asserted against
// pythonRecognisedFinalizeFamilies (daily.go) and job_daily.py's gate line
// (finalize_family_gate_agreement_test.go) -- three places, one value.
const TeamCognitiveLoadFamilyName = "team_cognitive_load"

var errTeamCognitiveLoadUnavailable = fmt.Errorf("team_cognitive_load native executor unavailable")

// NewTeamCognitiveLoadExecutor fails closed on a nil connection, matching
// every other native family's construction-time policy.
func NewTeamCognitiveLoadExecutor(conn driver.Conn) (*TeamCognitiveLoadExecutor, error) {
	if conn == nil {
		return nil, errTeamCognitiveLoadUnavailable
	}
	return &TeamCognitiveLoadExecutor{
		conn:   conn,
		writer: &teamCognitiveLoadWriter{conn: conn},
		nowUTC: func() time.Time { return time.Now().UTC() },
	}, nil
}

// ComputeFinalizeFamily implements daily.NativeFinalizeFamilyExecutor.
//
// NO FAIL-OPEN (CHAOS-4290's finalize-scope policy): any error here must
// propagate so the finalize retries rather than let the Python bridge
// recompute and double-write. Idempotent by construction: re-running for the
// same (org_id, team_id, day) writes new rows with a later computed_at, and
// every read of this table is argMax(*, computed_at)-deduped (the sink's own
// doc comment) -- a redrive never accumulates.
func (executor *TeamCognitiveLoadExecutor) ComputeFinalizeFamily(
	ctx context.Context, run Run,
) (int, error) {
	if executor == nil || executor.conn == nil || executor.writer == nil {
		return 0, errTeamCognitiveLoadUnavailable
	}
	if run.OrganizationID == "" || run.TargetDay.IsZero() {
		return 0, fmt.Errorf("%w: run has no organization or target day", ErrInvalidState)
	}
	day := time.Date(
		run.TargetDay.UTC().Year(), run.TargetDay.UTC().Month(), run.TargetDay.UTC().Day(),
		0, 0, 0, 0, time.UTC,
	)

	userRows, err := loadUserMetricsCognitiveLoadInputsForDay(ctx, executor.conn, run.OrganizationID, day)
	if err != nil {
		return 0, err
	}
	teamRows, err := loadTeamMetricsCognitiveLoadInputsForDay(ctx, executor.conn, run.OrganizationID, day)
	if err != nil {
		return 0, err
	}
	if len(userRows) == 0 && len(teamRows) == 0 {
		// Python: `if not user_metrics_rows and not team_wellbeing_rows: return 0`.
		return 0, nil
	}

	teams, err := LoadWellbeingTeams(ctx, executor.conn, run.OrganizationID)
	if err != nil {
		return 0, err
	}
	patternResolver := NewRepoPatternResolver(teams)

	repoIDSet := make(map[uuid.UUID]struct{}, len(userRows)+len(teamRows))
	for _, row := range userRows {
		repoIDSet[row.RepoID] = struct{}{}
	}
	for _, row := range teamRows {
		if row.RepoID != uuid.Nil {
			repoIDSet[row.RepoID] = struct{}{}
		}
	}
	repoIDs := make([]uuid.UUID, 0, len(repoIDSet))
	for id := range repoIDSet {
		repoIDs = append(repoIDs, id)
	}

	repoNamesByID, err := LoadRepoNames(ctx, executor.conn, run.OrganizationID, repoIDs)
	if err != nil {
		return 0, err
	}

	// Python passes `as_of=datetime.combine(day, datetime.min.time(), tzinfo=utc)`
	// -- day-start midnight UTC, no sub-second component (the same value the
	// query-parameter truncation caveat on load_team_repo_ownership_map warns
	// callers to already be using).
	repoToTeam := resolveDailyFinalizeRepoToTeam(ctx, executor.conn, run.OrganizationID, day, teams, repoIDs, repoNamesByID, patternResolver)

	computedAt := executor.nowUTC()
	rows := buildTeamCognitiveLoadRows(run.OrganizationID, day, userRows, teamRows, repoToTeam, computedAt)
	if len(rows) == 0 {
		return 0, nil
	}
	if err := executor.writer.write(ctx, rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// resolveDailyFinalizeRepoToTeam builds {repo_id_str: team_id} exactly as
// _write_team_cognitive_load_for_day does: team_repo_ownership
// (teamownership.OwnedRepoIDs, per team) first, the repo-pattern resolver
// only for a repo it leaves unresolved AND that is still present in the
// org's current repos catalog (repoNamesByID) -- a repo repoNamesByID does
// not carry is never guessed, matching the Python guard's own comment.
func resolveDailyFinalizeRepoToTeam(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	teams []WellbeingTeam, repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) map[string]string {
	repoToTeam := make(map[string]string, len(repoIDs))
	for _, team := range teams {
		if team.ID == "" {
			continue
		}
		owned, err := teamownership.OwnedRepoIDs(ctx, conn, organizationID, team.ID, asOf)
		if err != nil {
			// Matches Python's defensive `except Exception` around this whole
			// resolution block: a resolution failure degrades to "no team
			// attribution this attempt", never to a partition/finalize error --
			// team_cognitive_load rows are diagnostic, not load-bearing for any
			// other family's correctness.
			continue
		}
		for _, repoID := range owned {
			key := repoID.String()
			if _, exists := repoToTeam[key]; !exists {
				repoToTeam[key] = team.ID
			}
		}
	}
	for _, repoID := range repoIDs {
		key := repoID.String()
		if _, resolved := repoToTeam[key]; resolved {
			continue
		}
		name, known := repoNamesByID[key]
		if !known {
			continue
		}
		teamID, _ := patternResolver.ResolveRepo(name)
		if teamID != "" {
			repoToTeam[key] = teamID
		}
	}
	return repoToTeam
}

var _ NativeFinalizeFamilyExecutor = (*TeamCognitiveLoadExecutor)(nil)

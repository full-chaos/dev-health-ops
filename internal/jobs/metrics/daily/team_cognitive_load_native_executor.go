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
	repoToTeam, err := resolveDailyFinalizeRepoToTeam(ctx, executor.conn, run.OrganizationID, day, repoIDs, repoNamesByID, patternResolver)
	if err != nil {
		return 0, fmt.Errorf("team_cognitive_load: %w", err)
	}

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

// authoritativeOwnersKnownToRepoCatalog filters an org-wide authoritative
// repo->team ownership map (teamownership.AuthoritativeOwnerByRepo) down to
// repos ALSO present in repoNamesByID -- CHAOS-4365 codex r2 P1 guard. An
// owned repo_id that repoNamesByID does not carry (removed/renamed since
// team_repo_ownership last ran; that table is INSERT-only, CHAOS-2610 tracks
// writer-side valid_to retirement) is never trusted, exactly matching
// Python's _repo_to_team_map_for_compounding_risk: "a repo is trusted from
// EITHER source only when it also appears in repo_names_by_id" -- ownership
// included, not just the pattern-resolver fallback below. A prior revision of
// this gate applied ONLY to the pattern fallback and let ownership resolve
// unconditionally, a real parity gap (found in #2256 review): a stale
// ownership row could attribute a repo that no longer exists in the org's
// current inventory, something the pattern-resolver path never did.
func authoritativeOwnersKnownToRepoCatalog(
	owners map[string]string, repoNamesByID map[string]string,
) map[string]string {
	filtered := make(map[string]string, len(owners))
	for repoID, teamID := range owners {
		if _, known := repoNamesByID[repoID]; !known {
			continue
		}
		filtered[repoID] = teamID
	}
	return filtered
}

// resolveDailyFinalizeRepoToTeam builds {repo_id_str: team_id} exactly as
// _write_team_cognitive_load_for_day does: team_repo_ownership
// (teamownership.AuthoritativeOwnerByRepo, ranked is_primary/specificity/
// updated_at, ONE authoritative owner per repo -- CHAOS-5141, #2255 r1
// finding 2: OwnedRepoIDs answers "is this repo one of this team's", not
// "who is its authoritative owner", and iterating it per-team let whichever
// team happened to be encountered first win a multi-claimed repo) first, the
// repo-pattern resolver only for a repo it leaves unresolved AND that is
// still present in the org's current repos catalog (repoNamesByID) -- a repo
// repoNamesByID does not carry is never guessed, matching the Python guard's
// own comment.
func resolveDailyFinalizeRepoToTeam(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) (map[string]string, error) {
	// CHAOS-5141, #2255 r1 finding 3: a resolution failure here is
	// PROPAGATED, never swallowed. team_cognitive_load is finalize-scope
	// (CHAOS-4290): a native runtime error must fail the family closed
	// (Retryable -> River redrive), not silently return a success outcome
	// with zero/partial rows that the finalize handler would mark Computed
	// and skip the Python bridge for.
	owners, err := teamownership.AuthoritativeOwnerByRepo(ctx, conn, organizationID, asOf)
	if err != nil {
		return nil, fmt.Errorf("resolve authoritative repo ownership: %w", err)
	}
	repoToTeam := authoritativeOwnersKnownToRepoCatalog(owners, repoNamesByID)
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
	return repoToTeam, nil
}

var _ NativeFinalizeFamilyExecutor = (*TeamCognitiveLoadExecutor)(nil)

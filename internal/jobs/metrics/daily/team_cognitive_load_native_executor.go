package daily

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
	"github.com/full-chaos/dev-health-ops/internal/teamresolve"
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
		// A day with no input produces no row; a team key that still holds
		// a measure for the day is stale.
		return executor.supersedeStaleKeys(ctx, run.OrganizationID, day, nil, executor.nowUTC(), 0)
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
	repoToTeam, err := resolveDailyFinalizeRepoOwnerShares(ctx, executor.conn, run.OrganizationID, day, repoIDs, repoNamesByID, patternResolver)
	if err != nil {
		return 0, fmt.Errorf("team_cognitive_load: %w", err)
	}

	computedAt := executor.nowUTC()
	rows := buildTeamCognitiveLoadRows(run.OrganizationID, day, userRows, teamRows, repoToTeam, computedAt)
	produced := make([]staleKey, 0, len(rows))
	for _, row := range rows {
		produced = append(produced, staleKey{row.TeamID})
	}
	if len(rows) == 0 {
		return executor.supersedeStaleKeys(ctx, run.OrganizationID, day, nil, computedAt, 0)
	}
	if err := executor.writer.write(ctx, rows); err != nil {
		return 0, err
	}
	return executor.supersedeStaleKeys(ctx, run.OrganizationID, day, produced, computedAt, len(rows))
}

// supersedeStaleKeys applies the stale-key rule (stale_team_keys.go) for the
// day: the family computes the organization's day whole, so every live team
// key of the day that this compute did not produce gets a row of zeros.
func (executor *TeamCognitiveLoadExecutor) supersedeStaleKeys(
	ctx context.Context, organizationID string, day time.Time, produced []staleKey, computedAt time.Time, written int,
) (int, error) {
	superseded, err := supersedeStaleTeamKeys(ctx, executor.conn, teamkeytables.TeamCognitiveLoadDaily, organizationID, day, nil, produced, computedAt)
	written += superseded
	if err != nil {
		if written == 0 {
			return 0, err
		}
		return written, fmt.Errorf("%w: team_cognitive_load failed after %d row(s) already landed: %w", ErrPartialWrite, written, err)
	}
	return written, nil
}

// resolveDailyFinalizeRepoOwnerShares builds {repo_id_str: owners with their
// linked shares} (teamownership.OwnerSharesByRepo, CHAOS-9410, D5988): the main
// owner (or every tied team of a full tie, D5990) and every lesser owner, each
// at its own share, for the finalize families that add over repositories
// (team_cognitive_load, team_complexity). The repo-pattern resolver only
// serves a repo no ownership row resolves AND that is still in the org's repos
// catalog (repoNamesByID).
func resolveDailyFinalizeRepoOwnerShares(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) (map[string][]teamownership.OwnerShare, error) {
	// A resolution failure is PROPAGATED, never swallowed (CHAOS-5141, #2255
	// r1 finding 3): a finalize-scope family must fail closed (Retryable ->
	// River redrive), not succeed with zero or partial rows.
	owners, err := teamownership.OwnerSharesByRepo(ctx, conn, organizationID, asOf)
	if err != nil {
		return nil, fmt.Errorf("resolve authoritative repo ownership: %w", err)
	}
	known := ownersKnownToRepoCatalog(owners, repoNamesByID)
	resolved := teamresolve.ResolveSharesFromOwnershipMap(known, repoIDs, repoNamesByID, patternResolver)
	slog.Debug("daily finalize repository owners with shares",
		"organization_id", organizationID, "repos_total", len(repoIDs), "repos_with_owners", len(resolved))
	return resolved, nil
}

// ownersKnownToRepoCatalog filters an org-wide repo->owners map
// (teamownership.OwnerSharesByRepo) down to repos ALSO present in
// repoNamesByID -- CHAOS-4365 codex r2 P1 guard. An owned repo_id that
// repoNamesByID does not carry (removed/renamed since team_repo_ownership last
// ran) is never trusted, exactly matching Python's
// _repo_to_team_map_for_compounding_risk: a repo is trusted from EITHER source
// only when it also appears in repo_names_by_id.
func ownersKnownToRepoCatalog(
	owners map[string][]teamownership.OwnerShare, repoNamesByID map[string]string,
) map[string][]teamownership.OwnerShare {
	filtered := make(map[string][]teamownership.OwnerShare, len(owners))
	for repoID, repoOwners := range owners {
		if _, known := repoNamesByID[repoID]; !known {
			continue
		}
		filtered[repoID] = repoOwners
	}
	return filtered
}

var _ NativeFinalizeFamilyExecutor = (*TeamCognitiveLoadExecutor)(nil)

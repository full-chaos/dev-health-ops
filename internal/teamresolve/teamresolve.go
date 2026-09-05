// Package teamresolve answers one question three call sites need the same
// way: given a repo, which team owns it, for a native Go finalize-family
// executor. Extracted verbatim (semantics + the CHAOS-5141 gating fix) from
// resolveDailyFinalizeRepoToTeam / applyOwnershipToRepoToTeam
// (internal/jobs/metrics/daily/team_cognitive_load_native_executor.go:150-215
// on #2255, commit 39445d877177baa09b2446a7a1cec64cbb24210b) — the first of
// three lanes (team_cognitive_load #2255, team_complexity #2256,
// compounding_risk team scope CHAOS-5084) to need this exact resolution, and
// each was about to write its own copy.
//
// Ports job_daily.py:497 _repo_to_team_map_for_compounding_risk — the SAME
// Python function all three Go call sites mirror (confirmed: team_cognitive_
// load and team_complexity both call the identical function name at that
// line, not three different Python resolvers). See ResolveOwnershipThenPatterns's
// doc for the exact precedence.
//
// A SECOND, narrower shape exists for ai_impact (CHAOS-4280, #2236):
// providers/teams.py's build_repo_pattern_resolver / RepoPatternTeamResolver
// has NO ownership lookup at all — patterns only, because ai_impact's own
// Python call site (job_daily.py:1809-1820) never wires an ownership-backed
// resolver (CHAOS-5117, a Python-side gap, out of scope here). That shape is
// NOT extracted into this package yet: #2236 already has its own correct,
// tested, pattern-only port (internal/jobs/metrics/aiimpact/repoteams.go),
// and migrating it to share this package's pattern engine is follow-up work,
// not a blocker for CHAOS-5084 (team-lead ruling, 2026-09-05). Documented
// here so the next reader knows the gap is known, not missed.
package teamresolve

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// applyOwnership merges one team's owned repos into repoToTeam, gated on
// repoNamesByID membership (CHAOS-4365 codex r2 P1 guard, CHAOS-5141
// generalization to the finalize-side callers): an owned repo_id that
// repoNamesByID does not carry (removed/renamed since team_repo_ownership
// last ran; that table is INSERT-only, CHAOS-2610 tracks writer-side
// valid_to retirement) is never trusted, exactly matching Python's
// _repo_to_team_map_for_compounding_risk: "a repo is trusted from EITHER
// source only when it also appears in repo_names_by_id" — ownership
// included, not just the pattern-resolver fallback below. An earlier
// revision of the team_cognitive_load copy of this function gated ONLY the
// pattern fallback on repoNamesByID and let ownership resolve
// unconditionally, a real parity gap (found in #2256 review, fixed at
// 39445d877177baa09b2446a7a1cec64cbb24210b): a stale ownership row could
// attribute a repo that no longer exists in the org's current inventory,
// something the pattern-resolver path never did.
func applyOwnership(
	repoToTeam map[string]string, teamID string, owned []uuid.UUID, repoNamesByID map[string]string,
) {
	for _, repoID := range owned {
		key := repoID.String()
		if _, known := repoNamesByID[key]; !known {
			continue
		}
		if _, exists := repoToTeam[key]; !exists {
			repoToTeam[key] = teamID
		}
	}
}

// ResolveOwnershipThenPatterns builds {repo_id_str: team_id} exactly as
// job_daily.py:497 _repo_to_team_map_for_compounding_risk does:
// team_repo_ownership (teamownership.OwnedRepoIDs, per team) first, the
// repo-pattern resolver only for a repo it leaves unresolved AND that is
// still present in the org's current repos catalog (repoNamesByID) — a repo
// repoNamesByID does not carry is never guessed, matching the Python guard's
// own comment. Both paths are gated on repoNamesByID (see applyOwnership).
//
// teamIDs is every team's id in the org (Python's get_all_teams() over just
// the id column — callers already hold the full team row for their own
// purposes and pass only what this function needs). repoIDs is the set of
// repos this run/partition scope covers. A resolution failure for one team
// (teamownership.OwnedRepoIDs returning an error) degrades that team's
// ownership contribution to empty, matching Python's defensive
// `except Exception` around this whole resolution block: team-scoped rows
// are diagnostic, never load-bearing for another family's correctness.
func ResolveOwnershipThenPatterns(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	teamIDs []string, repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) map[string]string {
	repoToTeam := make(map[string]string, len(repoIDs))
	for _, teamID := range teamIDs {
		if teamID == "" {
			continue
		}
		owned, err := teamownership.OwnedRepoIDs(ctx, conn, organizationID, teamID, asOf)
		if err != nil {
			continue
		}
		applyOwnership(repoToTeam, teamID, owned, repoNamesByID)
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
		if patternResolver == nil {
			continue
		}
		teamID, _ := patternResolver.ResolveRepo(name)
		if teamID != "" {
			repoToTeam[key] = teamID
		}
	}
	return repoToTeam
}

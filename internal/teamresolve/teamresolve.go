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
//
// This package's own tests prove cross-language equivalence for the
// precedence/gating logic (a frozen golden from the REAL Python function,
// plus a live-Python rot guard) — deliberately NOT a real-ClickHouse
// container test comparing this package end-to-end against Python: the
// ClickHouse QUERY correctness underneath (teamownership.OwnedRepoIDs /
// load_team_repo_ownership_map) is a separate, pre-existing, already-
// documented-as-equivalent component, not new logic this extraction
// introduces, and building a computeparity-style whole-container harness for
// a resolver function this narrow would be exactly the over-engineering
// ruled out (team-lead, 2026-09-05).
package teamresolve

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// ResolveFromOwnershipMap is the PURE core of ResolveOwnershipThenPatterns,
// decoupled from ClickHouse exactly the way job_daily.py:497
// _repo_to_team_map_for_compounding_risk itself is decoupled from
// load_team_repo_ownership_map: this function takes an ALREADY-RESOLVED
// {repo_id_str: team_id} ownership map (Python's own parameter shape) rather
// than querying for it, which is what makes it possible to run this exact
// logic against the REAL Python function in an oracle test with no
// ClickHouse involved on either side — the ClickHouse read itself is a
// separate, already-tested concern (teamownership.AuthoritativeOwnerByRepo
// on the Go side, load_team_repo_ownership_map on the Python side; both
// documented as mirroring the same hardened, ranked query).
//
// Precedence: ownershipMap wins where it resolves a repo (already gated on
// repoNamesByID by whoever built it — see ResolveOwnershipThenPatterns
// below); the pattern resolver is the fallback, for a repo ownershipMap
// leaves unresolved AND that is still present in repoNamesByID. A repo
// repoNamesByID does not carry is never guessed by either path.
func ResolveFromOwnershipMap(
	ownershipMap map[string]string, repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) map[string]string {
	repoToTeam := make(map[string]string, len(repoIDs))
	for _, repoID := range repoIDs {
		key := repoID.String()
		name, known := repoNamesByID[key]
		if !known {
			// Not in the current repos catalog -- neither source is trusted,
			// matching _repo_to_team_map_for_compounding_risk's own guard.
			continue
		}
		if teamID := ownershipMap[key]; teamID != "" {
			repoToTeam[key] = teamID
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

// ResolveOwnershipThenPatterns builds {repo_id_str: team_id} exactly as
// job_daily.py:497 _repo_to_team_map_for_compounding_risk's REAL production
// caller does (_write_compounding_risk_team_rows_for_day, job_daily.py:~635):
// it resolves the org-wide authoritative ownership map ONCE via
// teamownership.AuthoritativeOwnerByRepo (mirroring
// load_team_repo_ownership_map exactly, including its ranking), then
// delegates the precedence decision to ResolveFromOwnershipMap — the pure
// core this function and Python's own decoupled shape both funnel through.
//
// BUG FIXED HERE (CHAOS-5084/CHAOS-5141, codex round r1 on #2298,
// independently verified against the real Python call chain and
// teamownership.go's own source before fixing): this function used to build
// its OWN ownership map by looping teamIDs and calling per-team
// teamownership.OwnedRepoIDs, merging via a Go-side first-writer-wins
// applyOwnership helper (now REMOVED, along with its two tests). That is
// EXACTLY the pre-CHAOS-5141 bug shape teamownership.AuthoritativeOwnerByRepo
// was already built to fix (see that function's own doc comment,
// verbatim: "resolveDailyFinalizeRepoToTeam previously called per-team
// OwnedRepoIDs in a loop and kept whichever team happened to be encountered
// FIRST as if it were the sole owner... this function is the fix") --
// re-implementing the SAME already-fixed bug in this brand-new extraction
// was confirmed by tracing the REAL production Python call chain: job_daily.py's
// _write_compounding_risk_team_rows_for_day builds team_repo_ownership_map via
// load_team_repo_ownership_map (the RANKED loader,
// is_primary/specificity/updated_at/team_id -- confirmed present in this repo
// at internal/teamownership/teamownership.go:159's own ORDER BY, not a
// fabricated reference), never a per-team unranked loop.
// teamownership.OwnedRepoIDs itself explicitly documents (see its own doc
// comment) that it answers MEMBERSHIP, not precedence, and "was never meant
// to settle a multi-claimed repo's canonical owner" -- exactly the misuse
// this function made of it. Fixed by calling AuthoritativeOwnerByRepo
// directly; teamIDs is no longer a parameter (AuthoritativeOwnerByRepo is
// org-wide, not per-team, and this function had ZERO production callers to
// break -- confirmed via codegraph before removing it).
//
// A resolution failure (teamownership.AuthoritativeOwnerByRepo returning an
// error) is PROPAGATED, never swallowed (CHAOS-5084 r1 finding P1, codex,
// confirmed via a real repro: a ClickHouse ownership-query error used to
// degrade silently to an empty ownership map, which a finalize-scope caller
// like CompoundingRiskTeamExecutor cannot distinguish from "no repo in this
// org resolves to any team today" -- a legitimate, retry-pointless zero. The
// two states are semantically opposite (one is success, one is a transient
// backend failure CHAOS-4290's finalize-scope NO-FAIL-OPEN policy requires
// to retry, not silently succeed with zero rows) and this function is the
// only place that can still tell them apart, so it must not collapse them
// before returning. Matches team_cognitive_load's own
// resolveDailyFinalizeRepoToTeam precedent (team_cognitive_load_native_executor.go),
// which propagates the identical AuthoritativeOwnerByRepo error for the
// identical reason (CHAOS-5141, #2255 r1 finding 3) -- this was the ONE
// caller of this function that had drifted from that precedent by degrading
// instead of propagating, an earlier round's log-only mitigation
// (chaos-5084-2275-r1, P2) having stopped short of it because
// teamresolve.go was "owned by #2298, under separate review" at the time;
// #2298 has since merged, so that scope boundary no longer applies.
func ResolveOwnershipThenPatterns(
	ctx context.Context, conn driver.Conn, organizationID string, asOf time.Time,
	repoIDs []uuid.UUID, repoNamesByID map[string]string,
	patternResolver numerical.RepoTeamResolver,
) (map[string]string, error) {
	ownershipMap, err := teamownership.AuthoritativeOwnerByRepo(ctx, conn, organizationID, asOf)
	if err != nil {
		return nil, fmt.Errorf("teamresolve: resolve authoritative repo ownership: %w", err)
	}
	return ResolveFromOwnershipMap(ownershipMap, repoIDs, repoNamesByID, patternResolver), nil
}

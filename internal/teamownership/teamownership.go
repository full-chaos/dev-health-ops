// Package teamownership answers one narrow question: which repositories does
// a team own, as of a given instant. It exists so that a repo-keyed metrics
// table with no team_id column of its own -- repo_metrics_daily,
// repo_complexity_daily, file_hotspot_daily, and any future table shaped the
// same way -- can still be scoped to one team, by joining on repo_id against
// the owned-repository set this package resolves.
//
// # WHY THIS IS NOT teamattribution.ClickHouseFactSource.LoadRepos
//
// LoadRepos (internal/teamattribution/cascade.go) answers a HARDER question:
// given several teams that might all claim the same repo, which ONE team's
// candidacy should a work item's derivation trust -- ranked by
// is_primary/specificity/priority so exactly one candidate wins a conflict.
// That precedence machinery is for CONFLICT RESOLUTION, not membership.
//
// Scoping a metrics table needs only membership: "is this repo one of the
// repos this team owns," full stop. A repo simultaneously claimed by two
// teams (a shared library, a monorepo boundary that does not cleanly split)
// is not a conflict to resolve here -- both teams legitimately see their own
// activity on it, the same way two teams can both have commits in the same
// repository in reality. Resolving that down to a single winning owner would
// silently drop one team's real signal rather than fix anything.
//
// CHAOS-4897: this is the "shared internal/teamownership package" the Go
// recommendations loader's four org-wide queries (review latency, rework
// ratio, hotspot complexity delta, hotspot churn overlap) were left waiting
// on, because repo_metrics_daily/repo_complexity_daily/file_hotspot_daily
// carry repo_id and no team_id.
package teamownership

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// OwnedRepoIDs returns the distinct set of repo IDs team_repo_ownership links
// to teamID in orgID, active as of asOf.
//
// "Active" mirrors the bitemporal window every other reader of this table
// uses (teamattribution.ClickHouseFactSource.LoadRepos,
// team_repo_ownership_derivation.go's loadTeamRepoOwnershipActiveInferredRows):
// valid_from <= asOf AND (valid_to IS NULL OR valid_to > asOf). A row whose
// valid_from has not yet arrived, or whose valid_to has already passed, does
// not count -- an ownership claim is scoped to the instant it actually
// applied, not to whenever it happened to be written.
//
// A NULL repo_id does NOT mean unresolved. codex adversarial review
// (2026-09-04, round 3, P1): the GitHub team-autoimport producer
// (normalizeGitHubTeamRepoOwnership, internal/providersync/
// github_team_catalog.go) writes EVERY `provider_access` row with
// `RepoID: nil`, including exact, fully-resolved matches -- resolution is
// deferred to a join against `repos` by (org_id, provider,
// lower(repo_full_name)) at READ time, not stored on the row. An earlier
// version of this query filtered `repo_id IS NOT NULL` outright, which
// silently returned ZERO owned repos for every team whose ownership came
// from GitHub's own native team-repo permissions -- the single most common
// ownership source in practice, and a materially worse outcome than the
// CHAOS-4897 defect this package exists to close for teams using it.
//
// The join+coalesce below mirrors the canonical, already-hardened resolver
// (`load_team_repo_ownership_map`, src/dev_health_ops/providers/teams.py)
// exactly, including its `matched` sentinel: ClickHouse's default for an
// UNMATCHED LEFT JOIN column is the type's ZERO VALUE, not real NULL, and
// that codebase measured this live -- `r.id IS NOT NULL` is true even for a
// row that never matched, since the zero UUID is not NULL. Without the
// sentinel, dropping the join filter (or reverting to `r.id IS NOT NULL`)
// would let every UNRESOLVED repo_full_name silently resolve to the zero
// UUID and enter a team's owned set. A row counts as resolved only via
// `o.repo_id IS NOT NULL` (already-resolved, e.g. pattern rows with a
// concrete match) OR `r.matched = 1` (a genuine name join hit) -- a
// `match_type = 'pattern'` row that never resolved to a name `repos` holds,
// and has no repo_id of its own, has nothing to join a repo-keyed metrics
// table against and is correctly excluded either way.
//
// EVERY source and match_type counts otherwise -- native, provider_access,
// manual, inferred; exact or pattern-resolved. This package answers "is this
// repo one of this team's," not "who is its authoritative owner," so it does
// not rank is_primary/specificity/priority the way LoadRepos does (see the
// package doc for why that precedence does not apply to membership).
//
// An empty, non-nil-error result is a real answer, not degraded behaviour: a
// team with no team_repo_ownership rows owns no repos, and a caller scoping a
// query by this set should see NO rows for that team's repo-derived signals
// rather than falling back to an unscoped, org-wide read -- unscoped-on-empty
// is exactly the CHAOS-4897 defect this package exists to close.
func OwnedRepoIDs(
	ctx context.Context, conn driver.Conn, orgID, teamID string, asOf time.Time,
) ([]uuid.UUID, error) {
	repoIDTexts, err := ownedRepoIDText(ctx, conn, orgID, teamID, asOf)
	if err != nil {
		return nil, err
	}
	var repoIDs []uuid.UUID
	for _, repoIDText := range repoIDTexts {
		repoID, err := uuid.Parse(repoIDText)
		if err != nil {
			return nil, fmt.Errorf("owned repo id %q: %w", repoIDText, err)
		}
		// Defense in depth, NOT the mechanism that excludes an unresolvable
		// provider_access name -- that is the SQL layer's `matched` sentinel
		// below. A team-lead peer review (2026-09-04, gate-rounds) caught
		// this exact conflation: an earlier version of this package's own
		// test removed the sentinel from the SQL and still passed, because
		// this filter silently absorbed the zero UUID the broken query
		// leaked. ownedRepoIDText is exported (within the package) precisely
		// so a test can observe the SQL layer's raw output and prove the
		// sentinel is load-bearing on its own, independent of this filter.
		if repoID != uuid.Nil {
			repoIDs = append(repoIDs, repoID)
		}
	}
	return repoIDs, nil
}

// AuthoritativeOwnerByRepo answers the HARDER question OwnedRepoIDs
// deliberately does not: for every repo team_repo_ownership claims in orgID,
// active as of asOf, which ONE team is its authoritative owner. Mirrors
// `load_team_repo_ownership_map` (src/dev_health_ops/providers/teams.py:281,
// query at :375-:395) exactly, including its ranking: rows are ordered
// `is_primary DESC, specificity DESC, updated_at DESC, team_id ASC`, and the
// FIRST row per repo_id wins -- a `setdefault` in Python, a
// first-write-wins map build here.
//
// CHAOS-5141, #2255 r1 finding 2: resolveDailyFinalizeRepoToTeam previously
// called per-team OwnedRepoIDs in a loop and kept whichever team happened to
// be encountered FIRST as if it were the sole owner -- OwnedRepoIDs' own doc
// comment says it answers membership, not precedence, and was never meant to
// settle a multi-claimed repo's canonical owner. A repo claimed by both a
// low-ranked team and its actual primary owner could silently resolve to the
// low-ranked one depending on team iteration order. This function is the
// fix: the SAME query, ranking, and tie-break Python already uses, so a
// multi-claimed repo resolves identically in both languages.
func AuthoritativeOwnerByRepo(
	ctx context.Context, conn driver.Conn, orgID string, asOf time.Time,
) (map[string]string, error) {
	if orgID == "" {
		return nil, fmt.Errorf("AuthoritativeOwnerByRepo: orgID is required")
	}
	rows, err := conn.Query(ctx, `
        SELECT
            toString(coalesce(o.repo_id, r.id)) AS repo_id,
            o.team_id AS team_id
        FROM team_repo_ownership AS o FINAL
        LEFT JOIN (
            SELECT org_id, provider, id, repo, 1 AS matched
            FROM repos FINAL
        ) AS r
            ON r.org_id = o.org_id
               AND r.provider = o.provider
               AND lower(r.repo) = lower(o.repo_full_name)
        WHERE o.org_id = ?
          AND (o.repo_id IS NOT NULL OR r.matched = 1)
          AND o.valid_from <= ?
          AND (o.valid_to IS NULL OR o.valid_to > ?)
        ORDER BY o.is_primary DESC, o.specificity DESC, o.updated_at DESC, o.team_id ASC
    `, orgID, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	owners := map[string]string{}
	for rows.Next() {
		var repoIDText, teamID string
		if err := rows.Scan(&repoIDText, &teamID); err != nil {
			return nil, err
		}
		repoID, err := uuid.Parse(repoIDText)
		if err != nil || repoID == uuid.Nil {
			// Same defense-in-depth as OwnedRepoIDs: an unresolvable or zero
			// repo id can never scope a repo-keyed metrics row, skip it
			// rather than let it enter the map under a bogus key.
			continue
		}
		// ORDER BY already put the best (is_primary, specificity, updated_at)
		// row for a given repo first -- keep only the FIRST team seen per
		// repo, exactly matching Python's setdefault semantics.
		if _, exists := owners[repoIDText]; !exists {
			owners[repoIDText] = teamID
		}
	}
	return owners, rows.Err()
}

// ownedRepoIDText runs the owned-repo query and returns the RAW scanned
// repo_id strings, before uuid.Parse or the uuid.Nil defense-in-depth filter
// OwnedRepoIDs applies on top. Exists so a test can assert on the SQL
// layer's own output directly -- see OwnedRepoIDs' comment on why that
// distinction matters.
func ownedRepoIDText(
	ctx context.Context, conn driver.Conn, orgID, teamID string, asOf time.Time,
) ([]string, error) {
	// FINAL, not a raw WHERE on valid_to -- team_repo_ownership is a
	// ReplacingMergeTree(updated_at) keyed on (org_id, provider,
	// repo_full_name, team_id, source, valid_from), and a revocation is
	// written as a REPLACEMENT row under that same key with a newer
	// updated_at and valid_to=now (team_repo_ownership_derivation_clickhouse.go's
	// retractTeamRepoOwnershipRows). Immediately after such a write and
	// before the background merge collapses the two physical rows, a plain
	// WHERE (valid_to IS NULL OR valid_to > asOf) sees BOTH versions and
	// admits the row via its stale, not-yet-merged valid_to=NULL copy --
	// resurrecting a just-revoked repo (codex adversarial review, 2026-09-04,
	// P1 finding). FINAL forces the logical merge at query time, so the
	// WHERE clause below evaluates only the latest version's valid_to,
	// exactly matching the established pattern for reading this same table
	// in loadTeamRepoOwnershipActiveInferredRows
	// (team_repo_ownership_derivation_clickhouse.go).
	// toString(...), not a bare UUID/Nullable(UUID) select: matches the
	// Python reference exactly, and sidesteps having to reason about what
	// type clickhouse-go resolves coalesce(Nullable(UUID), UUID) to on the
	// wire -- a string round-trips unambiguously through uuid.Parse in the
	// caller regardless.
	rows, err := conn.Query(ctx, `
        SELECT DISTINCT toString(coalesce(o.repo_id, r.id)) AS repo_id
        FROM team_repo_ownership AS o FINAL
        LEFT JOIN (
            SELECT org_id, provider, id, repo, 1 AS matched
            FROM repos FINAL
        ) AS r
            ON r.org_id = o.org_id
               AND r.provider = o.provider
               AND lower(r.repo) = lower(o.repo_full_name)
        WHERE o.org_id = ?
          AND o.team_id = ?
          AND (o.repo_id IS NOT NULL OR r.matched = 1)
          AND o.valid_from <= ?
          AND (o.valid_to IS NULL OR o.valid_to > ?)
    `, orgID, teamID, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var repoIDTexts []string
	for rows.Next() {
		var repoIDText string
		if err := rows.Scan(&repoIDText); err != nil {
			return nil, err
		}
		repoIDTexts = append(repoIDTexts, repoIDText)
	}
	return repoIDTexts, rows.Err()
}

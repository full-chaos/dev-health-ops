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
// repo_id IS NOT NULL excludes pattern-unresolved rows: a `match_type =
// 'pattern'` claim that never resolved to a concrete repository (repo_id
// Nullable(UUID), NULL when unresolved) has nothing to join a repo-keyed
// metrics table against, so it is dropped here rather than at every caller.
//
// EVERY source and match_type counts -- native, provider_access, manual,
// inferred; exact or pattern-resolved. This package answers "is this repo
// one of this team's," not "who is its authoritative owner," so it does not
// rank is_primary/specificity/priority the way LoadRepos does (see the
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
	rows, err := conn.Query(ctx, `
        SELECT DISTINCT repo_id
        FROM team_repo_ownership
        WHERE org_id = ?
          AND team_id = ?
          AND repo_id IS NOT NULL
          AND valid_from <= ?
          AND (valid_to IS NULL OR valid_to > ?)
    `, orgID, teamID, asOf, asOf)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var repoIDs []uuid.UUID
	for rows.Next() {
		var repoID uuid.UUID
		if err := rows.Scan(&repoID); err != nil {
			return nil, err
		}
		if repoID != uuid.Nil {
			repoIDs = append(repoIDs, repoID)
		}
	}
	return repoIDs, rows.Err()
}

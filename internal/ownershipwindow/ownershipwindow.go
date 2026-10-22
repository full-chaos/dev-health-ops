// Package ownershipwindow is the one rule for which ownership rows of team_repo_ownership are valid at an instant
// (team-attribution.md section 0.4j, D5987 option C, CHAOS-9404). Every reader renders it: teamownership
// (OwnedRepoIDs, OwnersByRepo, OwnerSharesByRepo and AuthoritativeOwnersByRepo) and queryapi/teamscope.
//
// A row is valid at an instant when valid_from <= instant and valid_to is NULL or after it. For an instant BEFORE the
// first recorded ownership of a repository, the repository's FIRST recorded owners apply: the instant is clamped up to
// that first record, so the rows valid at the first record are the owners. An instant at or after the first record is
// read as before, and an instant in a gap between two records has no owner. The first record of a repository is the
// earliest valid_from over its rows of ACTIVE teams (an inactive team owns nothing, section 0.4g rule 1, so it does
// not set the first record) that name a team and resolve to a repository (a repo_id, or a name the repos table holds).
// The catalog guard stays with each reader: it keeps the rows of a repository the catalog no longer holds out of its own
// answer, and does not change which row is the first record.
//
// The package renders SQL text only. The caller binds the organization and the instant under the placeholder names it
// gives in Names; the instant is a DateTime64(3, 'UTC') parameter.
package ownershipwindow

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/teamactive"
)

// Names are the query parameter names of one statement: the organization (String) and the instant
// (DateTime64(3, 'UTC')).
type Names struct{ Org, AsOf string }

// RepoKey is the expression of the resolved repository id of one ownership row, over the aliases o (the ownership row)
// and r (the repos join of the statement).
const RepoKey = "coalesce(toString(o.repo_id), toString(r.id))"

// InactiveTeams is a subquery of the ids of the inactive teams of the organization (the newest teams row decides).
func InactiveTeams(n Names) string {
	return fmt.Sprintf("SELECT trimBoth(id) FROM teams WHERE org_id = {%s:String} GROUP BY id HAVING %s", n.Org, teamactive.NewestRowInactive)
}

// ActiveTeam is the predicate that the team of row o is active: o.team_id is no inactive team's id.
func ActiveTeam(n Names) string {
	return fmt.Sprintf("trimBoth(o.team_id) NOT IN (%s)", InactiveTeams(n))
}

// firstRecords is an uncorrelated scalar subquery: a Map(String, DateTime64) from the id of each repository to the
// first valid_from of its ownership rows of active teams.
func firstRecords(n Names) string {
	return fmt.Sprintf(`(
        SELECT mapFromArrays(groupArray(repo_key), groupArray(first_from))
        FROM (
            SELECT %[2]s AS repo_key, min(o.valid_from) AS first_from
            FROM team_repo_ownership AS o FINAL
            LEFT JOIN (
                SELECT org_id, provider, id, repo, 1 AS matched
                FROM repos FINAL
                WHERE org_id = {%[1]s:String}
            ) AS r
                ON r.org_id = o.org_id
                   AND r.provider = o.provider
                   AND lower(r.repo) = lower(o.repo_full_name)
            WHERE o.org_id = {%[1]s:String}
              AND o.team_id != ''
              AND %[3]s
              AND (o.repo_id IS NOT NULL OR r.matched = 1)
            GROUP BY repo_key
        )
    )`, n.Org, RepoKey, ActiveTeam(n))
}

// EffectiveAsOf is the instant the validity window of the row's repository is tested at: the bound instant, or the
// repository's first record when that is later. A repository with no record in the map reads the map's default (the
// epoch), which is before any instant, so it is tested at the bound instant.
func EffectiveAsOf(n Names) string {
	return fmt.Sprintf("greatest({%s:DateTime64(3, 'UTC')}, %s[%s])", n.AsOf, firstRecords(n), RepoKey)
}

// Window is the validity test of ownership row o at the bound instant, with the first recorded owners applied before
// the first record. It is the two AND-ed bounds, with no leading AND.
func Window(n Names) string {
	effective := EffectiveAsOf(n)
	return fmt.Sprintf("o.valid_from <= %[1]s\n      AND (o.valid_to IS NULL OR o.valid_to > %[1]s)", effective)
}

// Package teamscope answers one question for every query-api route that
// filters a repo-keyed read by a team scope: which repositories does that
// team own, as of one instant.
//
// The answer comes from team_repo_ownership, and the semantics are the ones
// internal/teamownership.OwnedRepoIDs already decided for the identical
// question outside this binary -- membership, not precedence. A repository
// two teams both work in belongs to both of them here; is_primary,
// specificity and priority rank a CONFLICT and are deliberately not consulted
// (internal/teamownership/teamownership.go's package doc comment states why
// ranking a shared repository down to a single owner deletes one team's real
// signal). The SQL is the same join providers/teams.py's
// load_team_repo_ownership_map established and
// cmd/query-api/internal/cognitiveload's fetchOwnershipCandidates already
// runs through this binary's own read-only client.
//
// One exported condition, consumed by every route. The rule is identical
// everywhere, so it lives in one place rather than in a copy per package:
// a route that resolves a team's repositories differently from its neighbour
// answers a different question under the same word.
//
// user_metrics_daily is not that source. Its team_id is per-author
// membership attribution, which falls back to author membership whenever
// repository-ownership resolution misses
// (migrations/clickhouse/081_team_cognitive_load_daily.sql records that
// finding, and the rule it states -- team means repository ownership, never
// person membership).
package teamscope

import (
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// Binding names carried by RepoCondition's own bindings. They are prefixed
// so that splicing this condition into a statement that already binds
// org_id, scope_ids or a date can never collide with the caller's own names.
const (
	BindingOrgID   = "team_scope_org_id"
	BindingTeamIDs = "team_scope_ids"
	BindingAsOf    = "team_scope_as_of"
)

// Marker is a fragment of RepoCondition's SQL that appears in no other
// statement in this binary. A test asserts on it to prove a route's emitted
// statement really carries the shared condition.
const Marker = "FROM team_repo_ownership AS o FINAL"

// RepoCondition returns repoColumn's team-ownership membership test as a
// standalone boolean SQL condition: no leading "AND", no trailing statement,
// so a caller splices it into a WHERE clause it already builds. Empty and
// blank team ids are dropped; the return is ("", nil) when nothing is left,
// which a caller reads as "this request has no team scope to apply", never
// as "this team owns nothing".
//
// asOf is ONE instant for the whole request, bound rather than inlined as
// now64(3): two reads inside one response must resolve the same membership,
// and a seeded test must be able to state the instant it is asserting about.
// Its parameter type names 'UTC', matching the columns it is compared with
// (valid_from/valid_to are DateTime64(3, 'UTC'), migrations/clickhouse/
// 051_team_attribution_dimensions.sql). A DateTime64 parameter that names no
// zone is parsed in the SERVER's zone, and the driver sends a bare wall-clock
// string, so on a server running anything but UTC the whole validity window
// would shift by that offset -- admitting a claim whose valid_from has not
// arrived, and applying a revocation early.
//
// FINAL, not a raw WHERE on valid_to. team_repo_ownership is
// ReplacingMergeTree(updated_at) keyed on (org_id, provider, repo_full_name,
// team_id, source, valid_from) (migrations/clickhouse/
// 051_team_attribution_dimensions.sql), and a revocation is a REPLACEMENT row
// under that same key carrying valid_to and a newer updated_at
// (internal/providersync/team_repo_ownership_derivation_clickhouse.go's
// retractTeamRepoOwnershipRows). Between that write and the background merge,
// a raw read sees both versions and the stale valid_to=NULL copy readmits a
// repository whose ownership was revoked. FINAL resolves the version first,
// so the window below tests only the current row's valid_to.
//
// valid_from is part of that sorting key. The pre-existing github/jira/
// linear/gitlab autoimport writers each stamp it with their own run instant
// on every run and never collapse an unchanged fact's prior row, so one
// team/repository pair from those sources can carry more than one open row
// at once (internal/providersync/team_repo_ownership_derivation_clickhouse.go's
// filterUnchangedTeamRepoOwnershipRows is the one writer that does not: it
// skips writing a new row for a fact whose active row already matches it
// exactly). This condition does not depend on which shape produced the
// rows -- SELECT DISTINCT is what makes the resolved set a set regardless
// of how many open rows one fact carries.
//
// A NULL repo_id does not mean unresolved. The GitHub team-autoimport writer
// (internal/providersync/github_team_catalog.go's
// normalizeGitHubTeamRepoOwnership) leaves repo_id nil on every row it writes
// and carries only repo_full_name, so resolution happens here, against repos
// by (org_id, provider, lower name). The join carries an explicit `1 AS
// matched` sentinel and the WHERE tests that, never `r.id IS NOT NULL`:
// ClickHouse fills an unmatched LEFT JOIN column with the type's zero value
// rather than NULL, so a name repos does not hold tests as non-NULL and would
// resolve to the zero UUID.
//
// The resolved id must also exist in repos for this org, on both branches.
// ClickHouse enforces no foreign key, so an ownership row that outlived its
// repository's removal from the catalog would otherwise stay readable for a
// team scope alone, while every org-scoped read -- which enumerates repos
// itself -- does not see it.
//
// Those two guards OVERLAP on the unmatched-name case: the catalog check
// rejects the zero UUID as well, so behaviour alone cannot tell them apart.
// The zero UUID's exclusion is pinned by TestTeamScopeOwnership_Resolution
// Matrix's wu-zero-repo-id case and the catalog check by its wu-orphan case,
// both against a real engine; the sentinel's own presence is pinned only by
// this package's condition-text test. Replacing the sentinel with `r.id IS
// NOT NULL` leaves every seeded case green, so a reader changing it has the
// text test and this paragraph, and nothing else.
//
// The whole set resolves inside this condition rather than as a query of its
// own, so the surrounding statement's own (small) result is the only thing
// that crosses back to the client: a team's ownership rows number in the tens
// of thousands where its repositories number in the tens.
func RepoCondition(orgID, repoColumn string, teamIDs []string, asOf time.Time) (condition string, bindings []dhclickhouse.Binding) {
	var teamList []string
	for _, id := range teamIDs {
		if id != "" {
			teamList = append(teamList, id)
		}
	}
	if len(teamList) == 0 {
		return "", nil
	}
	return fmt.Sprintf(`%s IN (
    SELECT DISTINCT coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
    FROM team_repo_ownership AS o FINAL
    LEFT JOIN (
        SELECT org_id, provider, id, repo, 1 AS matched
        FROM repos FINAL
        WHERE org_id = {%[2]s:String}
    ) AS r
        ON r.org_id = o.org_id
           AND r.provider = o.provider
           AND lower(r.repo) = lower(o.repo_full_name)
    WHERE o.org_id = {%[2]s:String}
      AND o.team_id IN {%[3]s:Array(String)}
      AND (o.repo_id IS NOT NULL OR r.matched = 1)
      AND o.valid_from <= {%[4]s:DateTime64(3, 'UTC')}
      AND (o.valid_to IS NULL OR o.valid_to > {%[4]s:DateTime64(3, 'UTC')})
      AND coalesce(toString(o.repo_id), toString(r.id)) IN (
          SELECT toString(id) AS id
          FROM repos FINAL
          WHERE org_id = {%[2]s:String}
      )
)`, repoColumn, BindingOrgID, BindingTeamIDs, BindingAsOf), []dhclickhouse.Binding{
		{Name: BindingOrgID, Value: orgID},
		{Name: BindingTeamIDs, Value: teamList},
		{Name: BindingAsOf, Value: asOf.UTC()},
	}
}

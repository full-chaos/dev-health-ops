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
// internal/queryapi/cognitiveload's fetchOwnershipCandidates already
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
	"context"
	"fmt"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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

// NarrowRepoScope combines the repository condition of a request into ONE
// " AND ..." fragment. Filters narrow, they never widen (CHAOS-9093): a request
// that names repositories and a team sees the repositories that are both named
// and owned by the team, not the union of the two; repositories that are named
// and resolve to nothing leave nothing, never the unfiltered set.
//
// named says the request names repositories (a repo-level scope's ids, or
// what.repos). explicitSQL is the membership fragment of the ones that resolved
// (" AND col IN {scope_ids}"; "" when none did), teamCondition the bare boolean
// of RepoCondition ("" when no team scope). The Python original built no
// condition for an empty id list and ORed the team's repositories with the named
// ones, so a filter that matched nothing served the unfiltered value and a named
// repository outside the team widened the team's scope.
func NarrowRepoScope(named bool, explicitSQL string, explicitBindings []dhclickhouse.Binding, teamCondition string, teamBindings []dhclickhouse.Binding) (filterSQL string, bindings []dhclickhouse.Binding) {
	explicitCondition := strings.TrimPrefix(explicitSQL, " AND ")
	switch {
	case named && explicitCondition == "":
		return " AND 1 = 0", nil
	case explicitCondition != "" && teamCondition != "":
		return " AND (" + explicitCondition + " AND " + teamCondition + ")", append(append([]dhclickhouse.Binding(nil), explicitBindings...), teamBindings...)
	case explicitCondition != "":
		return " AND " + explicitCondition, explicitBindings
	case teamCondition != "":
		return " AND " + teamCondition, teamBindings
	default:
		return "", nil
	}
}

// NamedRepoRefs is the one place that says which repositories a request names
// (CHAOS-9093, D5855): the ids of a repo-level scope plus what.repos, WITHOUT the
// empty strings. An empty string is not a repository name: the REST decoders drop
// it, so the GraphQL answer must too, and a list of only empty strings names no
// repository (the request is not filtered by one).
func NamedRepoRefs(scopeLevel string, scopeIDs, whatRepos []string) []string {
	var refs []string
	if scopeLevel == "repo" {
		for _, id := range scopeIDs {
			if id != "" {
				refs = append(refs, id)
			}
		}
	}
	for _, repo := range whatRepos {
		if repo != "" {
			refs = append(refs, repo)
		}
	}
	return refs
}

// RowQuerier is the read boundary ResolveRepoRef needs.
type RowQuerier interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// ResolveRepoRef ports resolve_repo_id (api/queries/scopes.py:19-50): a UUID-shaped
// reference is verified against repos.id of the org, anything else matches
// repos.repo. settings is the caller's own "SETTINGS ..." clause (each package keeps
// its own time budget) and errPrefix its own error wording: both are the only
// differences the seven copies of this function had. ok is false when nothing
// resolves.
func ResolveRepoRef(ctx context.Context, client RowQuerier, repoRef, orgID, settings, errPrefix string) (id string, ok bool, err error) {
	var query string
	var bindings []dhclickhouse.Binding
	if parsed, perr := pythonparity.ParseUUID(repoRef); perr == nil {
		query = fmt.Sprintf(`
SELECT toString(id) AS id
FROM repos FINAL
WHERE toString(id) = {repo_id:String}
  AND org_id = {org_id:String}
LIMIT 1
%s
`, settings)
		bindings = []dhclickhouse.Binding{
			{Name: "repo_id", Value: parsed.String()},
			{Name: "org_id", Value: orgID},
		}
	} else {
		query = fmt.Sprintf(`
SELECT toString(id) AS id
FROM repos FINAL
WHERE repo = {repo_name:String}
  AND org_id = {org_id:String}
LIMIT 1
%s
`, settings)
		bindings = []dhclickhouse.Binding{
			{Name: "repo_name", Value: repoRef},
			{Name: "org_id", Value: orgID},
		}
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", false, fmt.Errorf("%sresolve repo id: %w", errPrefix, err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", false, fmt.Errorf("%siterate resolve repo id rows: %w", errPrefix, err)
		}
		return "", false, nil
	}
	if err := rows.Scan(&id); err != nil {
		return "", false, fmt.Errorf("%sscan resolve repo id row: %w", errPrefix, err)
	}
	return id, true, nil
}

// ResolveRepoRefs ports resolve_repo_ids (api/queries/scopes.py:53-69): every
// non-empty reference, one lookup each (bounded by what the caller named), those
// that resolve in order.
func ResolveRepoRefs(ctx context.Context, client RowQuerier, repoRefs []string, orgID, settings, errPrefix string) ([]string, error) {
	var resolved []string
	for _, ref := range repoRefs {
		if ref == "" {
			continue
		}
		id, ok, err := ResolveRepoRef(ctx, client, ref, orgID, settings, errPrefix)
		if err != nil {
			return nil, err
		}
		if ok {
			resolved = append(resolved, id)
		}
	}
	return resolved, nil
}

// RepoScopeApplies is the ONE gate of the surfaces that read a repository filter
// (CHAOS-9104, D5844, D5900): the repository condition applies for a team or repo
// scope (as the reference does) and, beyond it, whenever the request NAMES
// repositories (teamscope.NamedRepoRefs) under any scope level: a repository
// filter narrows under any scope, never widens. The reference gated by the scope
// level alone, so an organization scope with what.repos was served unfiltered;
// that divergence is declared at the surfaces that read this gate.
func RepoScopeApplies(scopeLevel string, reposNamed bool) bool {
	return scopeLevel == "team" || scopeLevel == "repo" || reposNamed
}

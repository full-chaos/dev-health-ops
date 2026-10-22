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
// (internal/chmigrate/history/081_team_cognitive_load_daily.sql records that
// finding, and the rule it states -- team means repository ownership, never
// person membership).
package teamscope

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/ownershipwindow"
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
// (valid_from/valid_to are DateTime64(3, 'UTC'), internal/chmigrate/history/
// 051_team_attribution_dimensions.sql). A DateTime64 parameter that names no
// zone is parsed in the SERVER's zone, and the driver sends a bare wall-clock
// string, so on a server running anything but UTC the whole validity window
// would shift by that offset -- admitting a claim whose valid_from has not
// arrived, and applying a revocation early.
//
// FINAL, not a raw WHERE on valid_to. team_repo_ownership is
// ReplacingMergeTree(updated_at) keyed on (org_id, provider, repo_full_name,
// team_id, source, valid_from) (internal/chmigrate/history/
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
	return ownedRepoCondition(orgID, repoColumn, teamList, asOf)
}

// AnyTeamRepoCondition is RepoCondition without the team filter: repoColumn is owned by SOME team as of asOf. It is
// the same statement (the same join, validity window, FINAL and catalog guards), so "owned" means one thing for a
// team scope and for the coverage of teams (CHAOS-9264); only the o.team_id test is left out, and no team binding is
// carried. Every other reader of ownership stays on RepoCondition.
func AnyTeamRepoCondition(orgID, repoColumn string, asOf time.Time) (condition string, bindings []dhclickhouse.Binding) {
	return ownedRepoCondition(orgID, repoColumn, nil, asOf)
}

// ownedRepoCondition renders the one ownership statement; teamList nil = any team.
func ownedRepoCondition(orgID, repoColumn string, teamList []string, asOf time.Time) (condition string, bindings []dhclickhouse.Binding) {
	names := bindingNames{org: BindingOrgID, teams: BindingTeamIDs, asOf: BindingAsOf}
	bindings = ownershipBindings(names, orgID, teamList, asOf)
	return fmt.Sprintf(`%s IN (
    SELECT DISTINCT coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id
%s
)`, repoColumn, ownershipFromWhere(names, teamList != nil)), bindings
}

type bindingNames struct{ org, teams, asOf string }

func ownershipBindings(names bindingNames, orgID string, teamList []string, asOf time.Time) []dhclickhouse.Binding {
	bindings := []dhclickhouse.Binding{{Name: names.org, Value: orgID}}
	if teamList != nil {
		bindings = append(bindings, dhclickhouse.Binding{Name: names.teams, Value: teamList})
	}
	return append(bindings, dhclickhouse.Binding{Name: names.asOf, Value: asOf.UTC()})
}

// ownershipFromWhere is the FROM ... WHERE of the one ownership statement: every reader of "who owns which repository
// as of one instant" (the membership condition and the owner shares below) renders it here, so the validity window,
// FINAL and the catalog guards are one definition. The select list stays with the caller: it reads o.team_id,
// o.repo_id and r.id.
func ownershipFromWhere(names bindingNames, teamFiltered bool) string {
	window := ownershipwindow.Names{Org: names.org, AsOf: names.asOf}
	teamFilter := ""
	if teamFiltered {
		teamFilter = "\n      AND o.team_id IN {" + names.teams + ":Array(String)}"
	}
	return fmt.Sprintf(`    FROM team_repo_ownership AS o FINAL
    LEFT JOIN (
        SELECT org_id, provider, id, repo, 1 AS matched
        FROM repos FINAL
        WHERE org_id = {%[1]s:String}
    ) AS r
        ON r.org_id = o.org_id
           AND r.provider = o.provider
           AND lower(r.repo) = lower(o.repo_full_name)
    WHERE o.org_id = {%[1]s:String}%[2]s
      AND o.team_id != ''
      AND %[4]s
      AND (o.repo_id IS NOT NULL OR r.matched = 1)
      AND %[5]s
      AND coalesce(toString(o.repo_id), toString(r.id)) IN (
          SELECT toString(id) AS id
          FROM repos FINAL
          WHERE org_id = {%[1]s:String}
      )`, names.org, teamFilter, names.asOf, ownershipwindow.ActiveTeam(window), ownershipwindow.Window(window))
}

// Binding names of OwnerShares and RepoShare. They differ from RepoCondition's, so a statement that carries a team
// scope AND the owner shares binds each name once, never twice. RepoShare has its own names for the same reason: a
// statement may carry it next to OwnerShares.
const (
	OwnerSharesBindingOrgID   = "owner_shares_org_id"
	OwnerSharesBindingTeamIDs = "owner_shares_team_ids"
	OwnerSharesBindingAsOf    = "owner_shares_as_of"

	RepoShareBindingOrgID   = "repo_share_org_id"
	RepoShareBindingTeamIDs = "repo_share_team_ids"
	RepoShareBindingAsOf    = "repo_share_as_of"
)

// OwnerShares is the team side of a repository-keyed grouping: one row per (repository, owning team) as of asOf, by
// the one ownership rule of RepoCondition, with the columns
//
//	owner_repo_id    text of the repository id (join with toString(<repo column>))
//	owner_team_id    the owning team
//	owner_team_label the team's name, its id when the catalog holds no name
//	owner_share      the owner's linked share of the repository (team_repo_ownership.share, D5988: its
//	                 issue<->PR links of every tier over all links of the repository), renormalised over ALL
//	                 owners of the repository so the shares of one repository sum to 1; equal among the owners
//	                 (1 / number of owners) only when the owners hold no complete measure (see below)
//	owner_matched    1 (an unmatched LEFT JOIN reads the type's zero, so a caller tests this, never NULL)
//
// A repository owned by several teams is shared among them, not given to one (is_primary and priority are not
// consulted, as in RepoCondition): a grouping that sums its groups gets the repository's effort once. The main
// owner keeps its own share and each lesser owner keeps its own lesser share, never an equal split (chris
// 2026-10-12, D5988, team-attribution.md section 0.4d); a full tie is stored as equal shares (D5990), so it reads
// equal. A team's share is the largest non-NULL share of its open rows (an inferred row carries it; a
// provider_access, native or manual row has NULL = not measured, which is not zero). When any owner of the
// repository has no measured share, or the measured shares sum to nothing, no weight is invented: the owners split
// equally (the known limit). teamIDs narrows the ROWS to the teams in view (a team scope), AFTER the shares are
// computed over every owner: a co-owner in scope keeps its own share of the repository, never the whole
// repository (CHAOS-9413, D5988); no ids = every owning team. The source is a derived table WITHOUT alias; the
// caller aliases it.
func OwnerShares(orgID string, teamIDs []string, asOf time.Time) (source string, bindings []dhclickhouse.Binding) {
	names := bindingNames{org: OwnerSharesBindingOrgID, teams: OwnerSharesBindingTeamIDs, asOf: OwnerSharesBindingAsOf}
	return ownerShares(names, orgID, teamIDs, asOf)
}

func ownerShares(names bindingNames, orgID string, teamIDs []string, asOf time.Time) (source string, bindings []dhclickhouse.Binding) {
	teamList := nonEmpty(teamIDs)
	bindings = ownershipBindings(names, orgID, teamList, asOf)
	narrow := ""
	if teamList != nil {
		narrow = "\nWHERE owner_team_id IN {" + names.teams + ":Array(String)}"
	}
	// The window functions run over every owner of the repository (the unfiltered ownership statement); the team
	// narrowing is the outer WHERE, so it removes rows and never re-weighs the ones that stay.
	return fmt.Sprintf(`(
SELECT owner_repo_id, owner_team_id, owner_team_label, owner_share, owner_matched
FROM (
    SELECT
        owned.owner_repo_id AS owner_repo_id,
        owned.owner_team_id AS owner_team_id,
        if(nt.team_name != '', nt.team_name, owned.owner_team_id) AS owner_team_label,
        if(
            sum(if(owned.stored_share IS NULL, 1, 0)) OVER (PARTITION BY owned.owner_repo_id) = 0
                AND sum(owned.stored_share) OVER (PARTITION BY owned.owner_repo_id) > 0,
            owned.stored_share / sum(owned.stored_share) OVER (PARTITION BY owned.owner_repo_id),
            1.0 / count() OVER (PARTITION BY owned.owner_repo_id)
        ) AS owner_share,
        1 AS owner_matched
    FROM (
        SELECT
            coalesce(toString(o.repo_id), toString(r.id)) AS owner_repo_id,
            o.team_id AS owner_team_id,
            max(o.share) AS stored_share
%[1]s
        GROUP BY owner_repo_id, owner_team_id
    ) AS owned
    LEFT JOIN (
        SELECT id AS team_id, max(name) AS team_name
        FROM teams FINAL
        WHERE org_id = {%[2]s:String}
        GROUP BY id
    ) AS nt ON nt.team_id = owned.owner_team_id
)%[3]s
)`, ownershipFromWhere(names, false), names.org, narrow), bindings
}

// RepoShare is the share of a repository the teams of a team scope hold, as a SQL expression of Float64 over
// repoColumn (D5988, CHAOS-9413): the sum of their own linked shares of it (OwnerShares), 0 for a repository none
// of them owns. A reader that SUMS a measure over the repositories of a team scope multiplies each repository's
// measure by it, so a co-owner holding 0.4 of a repository reads 0.4 of its measure, a main owner its own share,
// and the selected teams together never more than the repository once. RepoCondition stays the membership test
// (a listing, a per-entity read, a median, a rate); RepoShare is for the readers that accumulate. ("", nil) when no
// team id is left, which a caller reads as no team scope (weight 1).
//
// The expression is a lookup in ONE constant map the server builds once per statement (an uncorrelated scalar
// subquery), so it adds no join to the statement it is spliced into and repoColumn may be a UUID or a String column.
func RepoShare(orgID, repoColumn string, teamIDs []string, asOf time.Time) (expr string, bindings []dhclickhouse.Binding) {
	teamList := nonEmpty(teamIDs)
	if len(teamList) == 0 {
		return "", nil
	}
	slog.Debug("team scope weighs a summed read by the owners' share", "org_id", orgID, "repo_column", repoColumn, "teams", len(teamList), "as_of", asOf)
	names := bindingNames{org: RepoShareBindingOrgID, teams: RepoShareBindingTeamIDs, asOf: RepoShareBindingAsOf}
	shares, bindings := ownerShares(names, orgID, teamList, asOf)
	return fmt.Sprintf(`ifNull((
    SELECT mapFromArrays(groupArray(repo_key), groupArray(repo_share))
    FROM (
        SELECT owner_repo_id AS repo_key, sum(owner_share) AS repo_share
        FROM %s AS scope_owner_shares
        GROUP BY owner_repo_id
    )
)[toString(%s)], 0.0)`, shares, repoColumn), bindings
}

func nonEmpty(ids []string) []string {
	var out []string
	for _, id := range ids {
		if id != "" {
			out = append(out, id)
		}
	}
	return out
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
		// Two membership tests on one repository column. A String list AND an ownership
		// subquery that also yields Strings fail analysis on ClickHouse 26.7.1.1315 (prod)
		// with code 169 "Cannot compare UUID with String" on a UUID column (CHAOS-9183).
		// The subquery side yields UUIDs here (toUUIDOrNull), so the column keeps its
		// type and the primary key can prune by repo_id (CHAOS-9187; proven on the prod
		// server by EXPLAIN indexes=1). A column that is already text keeps the
		// text comparison on both sides.
		if isTextRepoColumn(explicitCondition) {
			return " AND (" + textRepoMembership(explicitCondition) + " AND " + textRepoMembership(teamCondition) + ")", append(append([]dhclickhouse.Binding(nil), explicitBindings...), teamBindings...)
		}
		return " AND (" + explicitCondition + " AND " + uuidOwnedRepoSubquery(teamCondition) + ")", append(append([]dhclickhouse.Binding(nil), explicitBindings...), teamBindings...)
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

// textRepoMembership rewrites "<column> IN ..." to "toString(<column>) IN ...": the
// membership test of a repository column compared as text. A column that is already
// toString(...) is left as it is.
func textRepoMembership(condition string) string {
	column, rest, found := strings.Cut(condition, " IN ")
	if !found || strings.HasPrefix(strings.TrimSpace(column), "toString(") {
		return condition
	}
	return "toString(" + column + ") IN " + rest
}

// UnitScope widens the repository fragment of a work-unit read that COUNTS or LISTS units (the " AND ..." that
// NarrowRepoScope returns, written over the column repo_id) from the unit's OWN repo_id column to the unit's
// repository ROWS: a unit is in a repository scope when its own repo_id satisfies the fragment OR any
// repository row of its LATEST allocation generation does. latestEffortSource is the one latest-generation source of
// the repository rows (analytics.LatestWorkUnitRepoEffortSource: this package cannot import analytics); it binds
// {org_id}. The fragment is wrapped ONCE for ALL its conditions (a repository filter AND a team filter): they must
// hold on the SAME row, never on two different rows of one unit. A unit with a NULL own repo_id, or touching several
// repositories, is no longer lost to a repository scope (CHAOS-9221). Effort-summing readers do not use this:
// they run on the per-repository allocation source. The caller reads work_unit_investments under that alias.
// An empty fragment stays empty (no scope).
func UnitScope(filterSQL, latestEffortSource string) string {
	if strings.TrimSpace(filterSQL) == "" {
		return filterSQL
	}
	condition := strings.TrimPrefix(filterSQL, " AND ")
	if strings.TrimSpace(condition) == "1 = 0" {
		return filterSQL // a named repository that resolved to nothing leaves nothing
	}
	// Inside the subquery the column is the repository row's own repo_id: a caller that qualifies the column with the
	// unit alias ("work_unit_investments.repo_id") is rewritten for the inner copy only.
	inner := strings.ReplaceAll(condition, "work_unit_investments.repo_id", "repo_id")
	return " AND (" + condition + ` OR work_unit_investments.work_unit_id IN (
    SELECT work_unit_id FROM ` + latestEffortSource + ` AS latest_repo_rows
    WHERE repo_id IS NOT NULL AND ` + inner + "))"
}

// ownedRepoIDSelect is the select list of the ownership subquery of ownedRepoCondition.
const ownedRepoIDSelect = "SELECT DISTINCT coalesce(toString(o.repo_id), toString(r.id)) AS owned_repo_id"

// uuidOwnedRepoSubquery makes the ownership subquery of a team condition yield UUIDs
// (toUUIDOrNull: a value that is not a UUID becomes NULL and matches no repository).
func uuidOwnedRepoSubquery(teamCondition string) string {
	return strings.Replace(teamCondition, ownedRepoIDSelect,
		"SELECT DISTINCT toUUIDOrNull(coalesce(toString(o.repo_id), toString(r.id))) AS owned_repo_id", 1)
}

// isTextRepoColumn reports whether the membership test compares text already.
func isTextRepoColumn(condition string) bool {
	column, _, found := strings.Cut(condition, " IN ")
	return found && strings.HasPrefix(strings.TrimSpace(strings.TrimLeft(column, "(")), "toString(")
}

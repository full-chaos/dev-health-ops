package teamscope

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// EmptyReason says why a filter combination that names repositories matched
// nothing. It is one value for a whole answer, served by every route that takes
// a repository beside a team scope, so a client never derives it (CHAOS-9098):
// the words below are the wire values, and no route keeps a copy of them.
type EmptyReason string

const (
	// ReasonRepositoryNotInTeam: every named repository exists, and none of
	// them is held by the selected teams.
	ReasonRepositoryNotInTeam EmptyReason = "repository_not_in_team"
	// ReasonRepositoryNotFound: a named repository resolved to nothing, and
	// no named repository matched the filter.
	ReasonRepositoryNotFound EmptyReason = "repository_not_found"
)

// ClassifyEmptyFilter is the one rule for the reason. named is the number of
// repository references the request names (an empty string is not one),
// resolved how many of them resolved to a repository of the organization, and
// ownedByTeam how many of those resolved repositories the selected teams hold;
// teamScope says the request carries a team scope (ownedByTeam is read only
// then). The filters AND (NarrowRepoScope): what matched is the resolved
// repositories, narrowed to the team's when there is a team scope.
//
// The reason is nil whenever something matched: a filter set that resolved
// normally, also when only part of the named repositories is in the team or
// resolves (the value covers the part that matched), and also when the scope
// resolved and the window has no rows. It is not nil only when NOTHING matched:
// then it is ReasonRepositoryNotFound when a named repository did not resolve,
// else ReasonRepositoryNotInTeam (every named repository exists and the team
// holds none of them).
func ClassifyEmptyFilter(named, resolved int, teamScope bool, ownedByTeam int) *EmptyReason {
	if named <= 0 {
		return nil
	}
	matched := resolved
	if teamScope {
		matched = ownedByTeam
	}
	if matched > 0 {
		return nil
	}
	reason := ReasonRepositoryNotFound
	if resolved >= named {
		// Every named repository exists, and nothing matched: only a team scope
		// can have taken them away (without one, a resolved repository matches).
		reason = ReasonRepositoryNotInTeam
	}
	return &reason
}

// EmptyReasonText is the wire string of a reason; nil stays nil.
func EmptyReasonText(reason *EmptyReason) *string {
	if reason == nil {
		return nil
	}
	text := string(*reason)
	return &text
}

// QueryClient is the narrow ClickHouse read capability CountTeamHeldRepos needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// CountTeamHeldRepos is how many of the given repository ids (already resolved
// to repositories of the organization) the selected teams hold at asOf, by the
// one ownership rule of RepoCondition.
func CountTeamHeldRepos(ctx context.Context, client QueryClient, orgID string, repoIDs, teamIDs []string, asOf time.Time) (int, error) {
	if len(repoIDs) == 0 {
		return 0, nil
	}
	condition, bindings := RepoCondition(orgID, "toString(id)", teamIDs, asOf)
	if condition == "" {
		return 0, nil
	}
	statement := fmt.Sprintf(`
SELECT toUInt64(count()) AS held
FROM (
    SELECT DISTINCT toString(id) AS repo_id
    FROM repos FINAL
    WHERE org_id = {%s:String}
      AND toString(id) IN {named_repo_ids:Array(String)}
      AND %s
)
SETTINGS max_execution_time = 30
`, BindingOrgID, condition)
	rows, err := client.Query(ctx, statement, append([]dhclickhouse.Binding{{Name: "named_repo_ids", Value: repoIDs}}, bindings...))
	if err != nil {
		return 0, fmt.Errorf("teamscope: count team-held repositories: %w", err)
	}
	defer rows.Close()
	var held uint64
	if rows.Next() {
		if err := rows.Scan(&held); err != nil {
			return 0, fmt.Errorf("teamscope: scan team-held repositories: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("teamscope: iterate team-held repositories: %w", err)
	}
	return int(held), nil
}

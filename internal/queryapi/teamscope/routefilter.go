package teamscope

import (
	"context"
	"time"
)

// RouteFilter is the filter set of a REST route that takes selected teams and
// selected repositories beside its own scope (quadrant, heatmap; CHAOS-9159),
// resolved once for the whole answer: the team and repository filters AND and
// never widen (D5844), a named repository that resolves to nothing leaves no rows
// (D5841), and the reason an empty answer is served (the one shared definition,
// ClassifyEmptyFilter).
type RouteFilter struct {
	// TeamIDs are the selected teams (empty strings dropped), "" scope when none.
	TeamIDs []string
	// RepoRefs are the repository references the request names (empty strings
	// dropped, D5855); Named says there is at least one.
	RepoRefs []string
	Named    bool
	// RepoIDs are the named repositories that resolved to repositories of the
	// organization.
	RepoIDs []string
	// EmptyReason is non-nil only when NOTHING matched (ClassifyEmptyFilter).
	EmptyReason *EmptyReason
}

// ResolveRouteFilter resolves the selected repositories (one lookup each, bounded by
// what the request names), counts how many of them the selected teams hold when
// there is a team scope, and classifies the empty reason. settings is the caller's
// own SETTINGS clause, errPrefix its error prefix.
func ResolveRouteFilter(ctx context.Context, client RowQuerier, orgID string, teamIDs, repoRefs []string, asOf time.Time, settings, errPrefix string) (RouteFilter, error) {
	filter := RouteFilter{TeamIDs: nonEmptyStrings(teamIDs), RepoRefs: NamedRepoRefs("", nil, repoRefs)}
	filter.Named = len(filter.RepoRefs) > 0
	if !filter.Named {
		return filter, nil
	}
	ids, err := ResolveRepoRefs(ctx, client, filter.RepoRefs, orgID, settings, errPrefix)
	if err != nil {
		return RouteFilter{}, err
	}
	filter.RepoIDs = ids
	held := 0
	if len(filter.TeamIDs) > 0 && len(ids) > 0 {
		held, err = CountTeamHeldRepos(ctx, client, orgID, ids, filter.TeamIDs, asOf)
		if err != nil {
			return RouteFilter{}, err
		}
	}
	filter.EmptyReason = ClassifyEmptyFilter(len(filter.RepoRefs), len(ids), len(filter.TeamIDs) > 0, held)
	return filter, nil
}

func nonEmptyStrings(values []string) []string {
	var out []string
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

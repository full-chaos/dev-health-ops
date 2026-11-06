package filteroptions

import (
	"context"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/activeteams"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkeditems"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/linkscope"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// The options of a request held to a repository ceiling (CHAOS-13088). Each list is read from rows that name an
// allowed repository: by a repository column (repos, user_metrics_daily), or by the link of a work item to an allowed
// repository (work_item_cycle_times, and the status history of the linked items in place of
// work_item_state_durations_daily, which holds no item and no repository). A list whose rows name no repository is not
// served, and the answer lists it with the reason.

// Kinds of the lists that are not served under a ceiling.
const (
	// OmittedTeamCatalog: the teams table names no repository, so a team with no row in an allowed repository is not
	// listed.
	OmittedTeamCatalog = "team_catalog"
	// OmittedIssueType: issue_type_metrics_daily carries the work item's own stored repository, which never places an
	// item in a repository (only the link does).
	OmittedIssueType = "issue_type"
	// NarrowedOptions marks the lists that are served as held to the ceiling.
	NarrowedOptions = "filter_options"
)

// statusHistoryStart opens the window of the status history read: every item of the organization was created after it.
var statusHistoryStart = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// ceilingTeamsQuery is teamsQuery under a ceiling: the user_metrics_daily branch held to the allowed repositories, and
// the work-item branch read from work_item_cycle_times of the linked items (the per-person work_item table holds no
// item and no repository). The teams catalog branch is not read.
func ceilingTeamsQuery(repoTerm, itemTerm string) string {
	return `
        SELECT DISTINCT value
        FROM (
            SELECT team_id AS value
            FROM user_metrics_daily FINAL
            WHERE team_id != ''
              AND org_id = {org_id:String}
              AND ` + liverow.Predicate("user_metrics_daily", "") + `
              AND ` + activeteams.ListablePredicate("team_id") + repoTerm + `

            UNION ALL

            SELECT ifNull(team_id, '') AS value
            FROM work_item_cycle_times FINAL
            WHERE ifNull(team_id, '') != ''
              AND org_id = {org_id:String}
              AND ` + activeteams.ListablePredicate("ifNull(team_id, '')") + itemTerm + `
        )
        WHERE value != ''
        ORDER BY value
    `
}

func ceilingReposQuery(repoTerm string) string {
	return `
        SELECT DISTINCT repo AS value
        FROM repos FINAL
        WHERE repo != '' AND org_id = {org_id:String}` + repoTerm + `
        ORDER BY repo
    `
}

func ceilingRepoNamesQuery(repoTerm string) string {
	return `
        SELECT toString(id) AS repo_id, trim(repo) AS repo_name
        FROM repos FINAL
        WHERE org_id = {org_id:String}
          AND trim(repo) != ''` + repoTerm + `
        ORDER BY id
    `
}

func ceilingDevelopersQuery(repoTerm string) string {
	return `
        SELECT DISTINCT author_email AS value
        FROM user_metrics_daily FINAL
        WHERE author_email != '' AND org_id = {org_id:String}
          AND ` + liverow.Predicate("user_metrics_daily", "") + repoTerm + `
        ORDER BY author_email
    `
}

func restrictedResponse(ctx context.Context, client QueryClient, orgID string, ceiling teamscope.Ceiling, now time.Time) (Response, error) {
	resp := Response{
		Teams: []string{}, Repos: []string{}, Services: []string{}, Developers: []string{},
		WorkCategory: workCategory(), IssueType: []string{}, FlowStage: []string{},
		OmittedSignals: []linkscope.OmittedSignal{
			{Kind: OmittedIssueType, Reason: linkscope.ReasonNotScopedToRepositories},
			{Kind: OmittedTeamCatalog, Reason: linkscope.ReasonNotScopedToRepositories},
		},
		NarrowedCounts: []linkscope.NarrowedCount{{Kind: NarrowedOptions, Scope: linkscope.ScopeRepositoryCeiling}},
	}
	ids := ceiling.IDs()
	if len(ids) == 0 {
		// Nothing allowed: no list has a row to read.
		return resp, nil
	}
	org := dhclickhouse.Binding{Name: "org_id", Value: orgID}
	repoTerm, repoBindings := ceiling.RepoTerm("toString(repo_id)")
	idTerm, idBindings := ceiling.RepoTerm("toString(id)")
	itemTerm, itemBindings := linkscope.ItemCondition(ceiling, "work_item_id")
	with := func(extra []dhclickhouse.Binding) []dhclickhouse.Binding {
		return append([]dhclickhouse.Binding{org}, extra...)
	}

	teams, err := distinctValues(ctx, client, ceilingTeamsQuery(repoTerm, itemTerm), "filteroptions: teams", with(append(append([]dhclickhouse.Binding(nil), repoBindings...), itemBindings...)))
	if err != nil {
		return Response{}, err
	}
	listedTeams := make(map[string]struct{}, len(teams))
	for _, team := range teams {
		listedTeams[team] = struct{}{}
	}
	teamNames, err := namePairs(ctx, client, teamNamesQuery, "team names", with(nil), listedTeams)
	if err != nil {
		return Response{}, err
	}
	repos, err := distinctValues(ctx, client, ceilingReposQuery(idTerm), "filteroptions: repos", with(idBindings))
	if err != nil {
		return Response{}, err
	}
	rawDevelopers, err := distinctValues(ctx, client, ceilingDevelopersQuery(repoTerm), "filteroptions: developers", with(repoBindings))
	if err != nil {
		return Response{}, err
	}
	flowStages, err := linkeditems.Statuses(ctx, linkeditems.Loader{
		Client: client, OrgID: orgID, RepoIDs: ids,
		WindowStart: statusHistoryStart, WindowEnd: utcDay(now).AddDate(0, 0, 1), Budget: linkeditems.StateBudget,
	}, now)
	if err != nil {
		return Response{}, err
	}
	repoNames, err := namePairs(ctx, client, ceilingRepoNamesQuery(idTerm), "repo names", with(idBindings), nil)
	if err != nil {
		return Response{}, err
	}
	developers := []string{}
	listed := map[string]struct{}{}
	for _, value := range rawDevelopers {
		if isEmailValue(value) {
			developers = append(developers, value)
			listed[value] = struct{}{}
		}
	}
	developerNames, err := namePairs(ctx, client, developerNamesQuery, "developer names", with(nil), listed)
	if err != nil {
		return Response{}, err
	}

	resp.Teams, resp.TeamNames = teams, teamNames
	resp.Repos, resp.RepoNames = repos, repoNames
	resp.Developers, resp.DeveloperNames = developers, developerNames
	resp.FlowStage = flowStages
	return resp, nil
}

func utcDay(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

package teamsidentity

import "github.com/full-chaos/dev-health-ops/internal/api/pyjson"

// discoveredTeam mirrors DiscoveredTeam (schemas_flat.py:578-585): one
// team-like unit read live from a provider. associations is a dict[str,
// Any] whose shape differs per provider (repo_patterns/provider_org for
// github/gitlab, project_keys/provider_org for jira/linear) -- built
// directly as a *pyjson.Object by each provider's own discovery function,
// never a generic map[string]any, so key order matches what that
// provider's own code constructs, the same discipline
// pybody.OrderedStringListDict exists for elsewhere in this package.
type discoveredTeam struct {
	ProviderType   string
	ProviderTeamID string
	Name           string
	Description    *string
	MemberCount    *int64
	Associations   *pyjson.Object
}

// discoveredTeamJSON mirrors DiscoveredTeam's field order exactly
// (provider_type, provider_team_id, name, description, member_count,
// associations).
func discoveredTeamJSON(team discoveredTeam) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("provider_type", team.ProviderType)
	out.Set("provider_team_id", team.ProviderTeamID)
	out.Set("name", team.Name)
	setOptionalString(out, "description", team.Description)
	if team.MemberCount != nil {
		out.Set("member_count", *team.MemberCount)
	} else {
		out.Set("member_count", nil)
	}
	associations := team.Associations
	if associations == nil {
		associations = pyjson.NewObject()
	}
	out.Set("associations", associations)
	return out
}

// teamDiscoverResponseJSON mirrors TeamDiscoverResponse (schemas_flat.py:
// 587-594): provider, teams, total, truncated, warnings.
func teamDiscoverResponseJSON(provider string, teams []discoveredTeam, truncated bool, warnings []string) *pyjson.Object {
	teamValues := make([]pyjson.Value, len(teams))
	for index, team := range teams {
		teamValues[index] = discoveredTeamJSON(team)
	}
	out := pyjson.NewObject()
	out.Set("provider", provider)
	out.Set("teams", teamValues)
	out.Set("total", int64(len(teams)))
	out.Set("truncated", truncated)
	out.Set("warnings", stringsToValues(warnings))
	return out
}

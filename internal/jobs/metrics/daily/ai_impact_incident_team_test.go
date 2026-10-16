package daily

import "testing"

// The source's team reference is mapped through the team catalog (provider and
// native_team_key). A reference that only equals a team id never attributes.
func TestMapAIImpactProviderTeam(t *testing.T) {
	catalog := map[AIImpactProviderTeamRef][]string{
		{Provider: "pagerduty", Ref: "PX1"}:    {"team-s"},
		{Provider: "pagerduty", Ref: "PX2"}:    {"team-a", "team-b"},
		{Provider: "pagerduty", Ref: "PX3"}:    {"team-gone"},
		{Provider: "jira", Ref: "PX1"}:         {"team-j"},
		{Provider: "pagerduty", Ref: "team-b"}: {"team-a"},
	}
	active := map[string]struct{}{"team-s": {}, "team-a": {}, "team-b": {}, "team-j": {}}
	for name, tc := range map[string]struct {
		ref  AIImpactProviderTeamRef
		want string
	}{
		"mapped":                                 {AIImpactProviderTeamRef{"pagerduty", "PX1"}, "team-s"},
		"same ref, other provider":               {AIImpactProviderTeamRef{"jira", "PX1"}, "team-j"},
		"unmapped ref":                           {AIImpactProviderTeamRef{"pagerduty", "PX9"}, ""},
		"ref equals a team id, unmapped":         {AIImpactProviderTeamRef{"pagerduty", "team-s"}, ""},
		"ref equals a team id, mapped elsewhere": {AIImpactProviderTeamRef{"pagerduty", "team-b"}, "team-a"},
		"two teams share the key":                {AIImpactProviderTeamRef{"pagerduty", "PX2"}, ""},
		"mapped team inactive":                   {AIImpactProviderTeamRef{"pagerduty", "PX3"}, ""},
		"provider differs":                       {AIImpactProviderTeamRef{"gitlab", "PX1"}, ""},
	} {
		got, ok := MapAIImpactProviderTeam(tc.ref, catalog, active)
		if got != tc.want || ok != (tc.want != "") {
			t.Errorf("%s: got (%q, %v), want %q", name, got, ok, tc.want)
		}
	}
}

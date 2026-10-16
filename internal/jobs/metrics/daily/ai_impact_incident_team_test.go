package daily

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/aiimpact"
)

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

// An incident of a service mapped to several repositories is placed on one:
// the triggering pull request's repository when it is mapped, else the lowest
// repository id, whatever order the mapping arrives in.
func TestAIImpactIncidentRepo(t *testing.T) {
	low := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	mid := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	high := uuid.MustParse("00000000-0000-4000-8000-000000000003")
	other := uuid.MustParse("00000000-0000-4000-8000-0000000000ff")
	if _, ok := AIImpactIncidentRepo(nil, nil); ok {
		t.Error("an incident with no mapped repository has a home")
	}
	for _, mapped := range [][]uuid.UUID{{high, low, mid}, {mid, high, low}, {low, mid, high}} {
		if got, _ := AIImpactIncidentRepo(nil, mapped); got != low {
			t.Errorf("no trigger: home %v, want the lowest %v", got, low)
		}
		if got, _ := AIImpactIncidentRepo(&aiimpact.IncidentTrigger{RepoID: high}, mapped); got != high {
			t.Errorf("trigger in a mapped repository: home %v, want %v", got, high)
		}
		if got, _ := AIImpactIncidentRepo(&aiimpact.IncidentTrigger{RepoID: other}, mapped); got != low {
			t.Errorf("trigger in an unmapped repository: home %v, want the lowest %v", got, low)
		}
	}
}

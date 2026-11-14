package aianalytics

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Provider coverage (the provider x entity matrix of AGENTS.md): the owner rule
// of the AI analytics team scope and of the row team holds for the
// repositories and projects of every provider. Per provider, the team owns one
// repository, and its pattern selects another that no owner row claims.
var ownerRuleProviders = []struct {
	provider           string
	ownedName          string
	patternOnlyName    string
	ownedID, patternID string
}{
	{"github", "acme/gh-owned", "acme/gh-pattern-only", "cccccccc-0000-0000-0001-000000000001", "cccccccc-0000-0000-0001-000000000002"},
	{"gitlab", "group/sub/gl-owned", "group/sub/gl-pattern-only", "cccccccc-0000-0000-0002-000000000001", "cccccccc-0000-0000-0002-000000000002"},
	{"jira", "acme.atlassian.net/OWN", "acme.atlassian.net/PAT", "cccccccc-0000-0000-0003-000000000001", "cccccccc-0000-0000-0003-000000000002"},
	{"linear", "acme/linear-owned", "acme/linear-pattern-only", "cccccccc-0000-0000-0004-000000000001", "cccccccc-0000-0000-0004-000000000002"},
}

func TestOwnerRule_EveryProvider(t *testing.T) {
	for _, p := range ownerRuleProviders {
		t.Run(p.provider, func(t *testing.T) {
			client := &ownerClient{
				repos:    map[string]string{p.ownedID: p.ownedName, p.patternID: p.patternOnlyName},
				owners:   map[string][]string{p.ownedID: {scopedTeamID}},
				patterns: map[string][]string{scopedTeamID: {p.patternOnlyName}},
			}
			ids, ok := teamRepoIDs(context.Background(), client, "org-1", scopedTeamID, "test")
			if !ok {
				t.Fatal("the team scope must resolve")
			}
			if fmt.Sprint(ids) != fmt.Sprint([]string{p.ownedID}) {
				t.Fatalf("team scope = %v, want only the owned %s repository %s", ids, p.provider, p.ownedID)
			}
			got, err := ownerTeams(context.Background(), client, "org-1", []string{p.ownedID, p.patternID}, "", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if nameOrNil(got[p.ownedID]) != scopedTeamID {
				t.Errorf("owned %s repository has team %s, want %s", p.provider, nameOrNil(got[p.ownedID]), scopedTeamID)
			}
			if got[p.patternID] != nil {
				t.Errorf("pattern-only %s repository has team %s, want none", p.provider, *got[p.patternID])
			}
		})
	}
}

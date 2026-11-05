package policycatalog

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// filteredActions are the actions that read repository-bound data (ruling answers-12656-1, Q7). The list is
// written out here on purpose: a new action does not join it by accident.
var filteredActions = []string{
	"acr.context.read", "acr.evidence.read", "analytics.export", "analytics.read", "git.read", "metrics.read", "work_items.read",
}

type rawAction struct {
	Action          string  `json:"action"`
	ResourceKind    string  `json:"resource_kind"`
	RepositoryScope *string `json:"repository_scope"`
}

func rawActions(t *testing.T, raw []byte) []rawAction {
	t.Helper()
	var doc struct {
		Actions []rawAction `json:"actions"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc.Actions
}

// explicitChoiceMissing lists the actions that must carry an explicit repository_scope and do not: every
// action whose id ends in .read or .export, and every repository-kind action.
func explicitChoiceMissing(actions []rawAction) []string {
	var bad []string
	for _, a := range actions {
		needs := strings.HasSuffix(a.Action, ".read") || strings.HasSuffix(a.Action, ".export") || a.ResourceKind == "repository"
		if needs && a.RepositoryScope == nil {
			bad = append(bad, a.Action)
		}
	}
	return bad
}

func TestRepositoryScopeIsExactlyTheNamedActions(t *testing.T) {
	var got []string
	for _, a := range Default().Actions() {
		switch a.RepositoryScope {
		case RepositoryScopeFiltered:
			got = append(got, a.ID)
		case RepositoryScopeNone:
		default:
			t.Errorf("action %s has repository_scope %q, want %q or %q", a.ID, a.RepositoryScope, RepositoryScopeFiltered, RepositoryScopeNone)
		}
	}
	slices.Sort(got)
	if !slices.Equal(got, filteredActions) {
		t.Fatalf("filtered actions %v, want %v", got, filteredActions)
	}
}

func TestEveryReadExportAndRepositoryActionChoosesItsScope(t *testing.T) {
	if bad := explicitChoiceMissing(rawActions(t, vocabulary.ActionsV1)); len(bad) != 0 {
		t.Fatalf("actions with no explicit repository_scope: %v", bad)
	}
}

func TestTheExplicitChoiceCensusHasTeeth(t *testing.T) {
	none := "none"
	planted := []rawAction{
		{Action: "reports.read", ResourceKind: "organization"},
		{Action: "reports.export", ResourceKind: "organization", RepositoryScope: &none},
		{Action: "reports.repo", ResourceKind: "repository"},
		{Action: "reports.write", ResourceKind: "organization"},
	}
	got := explicitChoiceMissing(planted)
	if !slices.Equal(got, []string{"reports.read", "reports.repo"}) {
		t.Fatalf("census flagged %v, want the .read and the repository-kind action with no choice", got)
	}
}

func TestLoadRefusesAnUnknownRepositoryScope(t *testing.T) {
	actions := strings.Replace(string(vocabulary.ActionsV1), `"repository_scope": "filtered"`, `"repository_scope": "everything"`, 1)
	if _, err := Load([]byte(actions), vocabulary.RolesV1); err == nil {
		t.Fatal("a catalog with repository_scope everything loaded")
	}
}

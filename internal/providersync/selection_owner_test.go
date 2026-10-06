package providersync

import (
	"reflect"
	"testing"
)

// gatedTargets is the legacy targets the canonical-incident gate refuses
// when the feature is off.
var gatedTargets = []string{"incidents", "operational"}

// TestRowsOwnSyncSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty:
// one clause per row.
func TestRowsOwnSyncSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty(t *testing.T) {
	for _, testCase := range []struct {
		name                      string
		provider                  string
		hasIntegration, hasSource bool
		want                      bool
	}{
		{"whole-integration github", "github", true, false, true},
		{"whole-integration Jira, mixed case", "Jira", true, false, true},
		{"child pinned to a source", "github", true, true, false},
		{"no integration", "linear", false, false, false},
		{"no integration, a source", "gitlab", false, true, false},
		{"pagerduty", "pagerduty", true, false, false},
		{"PagerDuty, mixed case", "PagerDuty", true, false, false},
	} {
		if got := RowsOwnSyncSelection(testCase.provider, testCase.hasIntegration, testCase.hasSource); got != testCase.want {
			t.Errorf("%s: %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// TestPassthroughSyncTargetsKeepsEachDatasetLessTargetOnce: the targets with
// no dataset of the provider, in order, each once.
func TestPassthroughSyncTargetsKeepsEachDatasetLessTargetOnce(t *testing.T) {
	for _, testCase := range []struct {
		provider string
		targets  []string
		want     []string
	}{
		{"github", []string{"git", "incidents", "prs", "no-such-target"}, []string{"incidents", "no-such-target"}},
		{"github", []string{"incidents", "git", "incidents", "x", "incidents", "x"}, []string{"incidents", "x"}},
		{"gitlab", []string{"git", "incidents", "operational"}, []string{"operational"}},
		{"jira", []string{"work-items", "operational", "incidents"}, []string{"incidents"}},
		{"linear", []string{"work-items", "incidents", "operational"}, []string{"incidents", "operational"}},
		{"github", nil, []string{}},
	} {
		if got := PassthroughSyncTargets(testCase.provider, testCase.targets); !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s %v: %v, want %v", testCase.provider, testCase.targets, got, testCase.want)
		}
	}
}

// TestIncidentGateTargetsLeavesOutOnlyAMirroredTarget is the gate-target
// table for every provider of the matrix and every gated target. For a config
// whose rows own its selection, a gated target that has a dataset of the
// provider can be in the stored list as the mirror of a row and is left out;
// a gated target with no dataset is there only because a request asked for
// it and stays. Every other config gets its list back as it is.
func TestIncidentGateTargetsLeavesOutOnlyAMirroredTarget(t *testing.T) {
	// The gated targets that have a dataset, per provider: the only items
	// the rule may leave out of a gate. A new gated dataset changes this
	// table on purpose.
	mirrorable := map[string][]string{
		"github": nil, "linear": nil, "launchdarkly": nil,
		"gitlab": {"incidents"}, "jira": {"operational"}, "pagerduty": {"operational"},
	}
	providers := MatrixProviders()
	if len(providers) != len(mirrorable) {
		t.Fatalf("the matrix has %d providers, the table %d", len(providers), len(mirrorable))
	}
	leftOut := 0
	for _, provider := range providers {
		var withDataset []string
		for _, target := range gatedTargets {
			if SyncTargetHasDataset(provider, target) {
				withDataset = append(withDataset, target)
			}
		}
		if !reflect.DeepEqual(withDataset, mirrorable[provider]) {
			t.Errorf("%s: gated targets with a dataset %v, want %v", provider, withDataset, mirrorable[provider])
		}
		stored := []string{"git", "incidents", "work-items", "operational", "incidents", "Incidents", " operational", "no-such-target"}
		for _, shape := range []struct {
			name                      string
			hasIntegration, hasSource bool
		}{
			{"whole integration", true, false}, {"child", true, true}, {"no integration", false, false},
		} {
			got := IncidentGateTargets(provider, shape.hasIntegration, shape.hasSource, stored)
			var want []string
			rowsOwn := shape.hasIntegration && !shape.hasSource && provider != "pagerduty"
			for _, target := range stored {
				if rowsOwn && SyncTargetHasDataset(provider, target) {
					leftOut++
					continue
				}
				want = append(want, target)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("%s, %s: gate targets %v, want %v", provider, shape.name, got, want)
			}
			for _, target := range gatedTargets {
				gated := false
				for _, item := range got {
					gated = gated || item == target
				}
				wantGated := !(rowsOwn && SyncTargetHasDataset(provider, target))
				if gated != wantGated {
					t.Errorf("%s, %s: %q in the gate targets: %v, want %v", provider, shape.name, target, gated, wantGated)
				}
				if got := StoredTargetIsMirrored(provider, shape.hasIntegration, shape.hasSource, target); got == wantGated {
					t.Errorf("%s, %s: StoredTargetIsMirrored(%q) = %v, want %v", provider, shape.name, target, got, !wantGated)
				}
			}
		}
	}
	if leftOut == 0 {
		t.Fatal("no case left a target out: the table measured nothing")
	}
	// The named cases, spelled out.
	for _, testCase := range []struct {
		provider string
		stored   []string
		want     []string
	}{
		{"jira", []string{"work-items", "operational"}, []string{}},
		{"gitlab", []string{"git", "incidents"}, []string{}},
		{"github", []string{"git", "incidents"}, []string{"incidents"}},
		{"linear", []string{"work-items", "incidents"}, []string{"incidents"}},
		{"jira", []string{"work-items", "Operational"}, []string{"Operational"}},
	} {
		if got := IncidentGateTargets(testCase.provider, true, false, testCase.stored); !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s whole integration %v: %v, want %v", testCase.provider, testCase.stored, got, testCase.want)
		}
	}
	pagerduty := []string{"operational"}
	if got := IncidentGateTargets("pagerduty", true, false, pagerduty); !reflect.DeepEqual(got, pagerduty) {
		t.Errorf("pagerduty: %v, want the stored list %v", got, pagerduty)
	}
}

// TestCascadedSyncTargetsNeverCarriesTheMirrorToAChild: what a save of a
// parent gives one child. One case for each reason an item stays or goes.
func TestCascadedSyncTargetsNeverCarriesTheMirrorToAChild(t *testing.T) {
	for _, testCase := range []struct {
		name                         string
		provider                     string
		hasIntegration, hasSource    bool
		submitted, added, childHolds []string
		want                         []string
	}{
		{"jira: an item that only mirrors a row does not reach the child", "jira", true, false,
			[]string{"work-items", "operational"}, nil, []string{"work-items"}, []string{"work-items"}},
		{"gitlab: an item that only mirrors a row does not reach the child", "gitlab", true, false,
			[]string{"git", "incidents"}, nil, []string{"git"}, []string{"git"}},
		{"an item this save adds reaches the child", "jira", true, false,
			[]string{"work-items", "operational"}, []string{"operational"}, []string{"work-items"}, []string{"work-items", "operational"}},
		{"an item the child holds stays, in the submitted order", "jira", true, false,
			[]string{"operational", "work-items"}, nil, []string{"work-items", "operational"}, []string{"operational", "work-items"}},
		{"an item the save drops leaves the child", "jira", true, false,
			[]string{"work-items"}, nil, []string{"work-items", "operational"}, []string{"work-items"}},
		{"an item only the child holds leaves the child", "gitlab", true, false,
			[]string{"git"}, nil, []string{"git", "prs"}, []string{"git"}},
		{"github: an item with no dataset reaches the child", "github", true, false,
			[]string{"git", "incidents"}, nil, []string{"git"}, []string{"git", "incidents"}},
		{"linear: an item with no dataset reaches the child", "linear", true, false,
			[]string{"work-items", "operational"}, nil, []string{"work-items"}, []string{"work-items", "operational"}},
		{"an item not spelled as the registry spells it is not a mirrored item", "jira", true, false,
			[]string{"work-items", "Operational"}, nil, []string{"work-items"}, []string{"work-items", "Operational"}},
		{"pagerduty: the parent's list is its selection, the child gets it whole", "pagerduty", true, false,
			[]string{"operational"}, nil, nil, []string{"operational"}},
		{"a parent with no integration has no rows, the child gets the list whole", "jira", false, false,
			[]string{"work-items", "operational"}, nil, nil, []string{"work-items", "operational"}},
		{"a parent pinned to a source has its own selection, the child gets the list whole", "jira", true, true,
			[]string{"work-items", "operational"}, nil, nil, []string{"work-items", "operational"}},
		{"duplicates of a kept item stay", "github", true, false,
			[]string{"incidents", "incidents"}, nil, nil, []string{"incidents", "incidents"}},
		{"an empty list empties the child", "jira", true, false, []string{}, nil, []string{"work-items"}, []string{}},
	} {
		got := CascadedSyncTargets(testCase.provider, testCase.hasIntegration, testCase.hasSource, testCase.submitted, testCase.added, testCase.childHolds)
		if !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s: %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

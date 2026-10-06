package providersync

import (
	"reflect"
	"sort"
	"testing"
)

// formTargets is the checkbox list of the config form per provider (web
// PROVIDER_SYNC_TARGETS), in the registry's target order. GitHub "incidents"
// has no dataset: no row can show it, so it is not a derived target.
var formTargets = map[string][]string{
	"github":       {"git", "prs", "cicd", "deployments", "tests", "work-items"},
	"gitlab":       {"git", "prs", "cicd", "deployments", "incidents", "tests", "work-items", "feature-flags"},
	"jira":         {"work-items", "operational"},
	"linear":       {"work-items"},
	"launchdarkly": {"feature-flags"},
	"pagerduty":    {"operational"},
}

func providerKeys(provider string) []string {
	keys := []string{}
	for _, capability := range Capabilities(provider) {
		keys = append(keys, capability.Dataset)
	}
	return keys
}

// TestDerivedSyncTargetsShowsATargetWhenOneOfItsKeysIsEnabled is the
// derivation table for every provider (CHAOS-8816): one enabled key shows
// exactly its own target; every key of the provider enabled shows every form
// target; no enabled key shows nothing. "blame" and "security" never show; a
// key the provider does not support is ignored.
func TestDerivedSyncTargetsShowsATargetWhenOneOfItsKeysIsEnabled(t *testing.T) {
	providers := MatrixProviders()
	if len(providers) != len(formTargets) {
		t.Fatalf("the matrix has %d providers, the table %d", len(providers), len(formTargets))
	}
	cases := 0
	for _, provider := range providers {
		keys := providerKeys(provider)
		if len(keys) == 0 {
			t.Fatalf("%s: no dataset in the registry; the case proves nothing", provider)
		}
		if got := DerivedSyncTargets(provider, nil); len(got) != 0 {
			t.Errorf("%s: no enabled key shows %v, want none", provider, got)
		}
		if got := DerivedSyncTargets(provider, keys); !reflect.DeepEqual(got, formTargets[provider]) {
			t.Errorf("%s: every key enabled shows %v, want %v", provider, got, formTargets[provider])
		}
		for _, key := range keys {
			cases++
			capability, _ := Capability(provider, key)
			want := []string{}
			if target := capability.LegacyTargets[0]; target != "blame" && target != "security" {
				want = []string{target}
			}
			if len(capability.LegacyTargets) != 1 {
				t.Fatalf("%s/%s has %d legacy targets; the derivation needs exactly one per key", provider, key, len(capability.LegacyTargets))
			}
			// One member of the family on, no other row: the target shows.
			if got := DerivedSyncTargets(provider, []string{key}); !reflect.DeepEqual(got, want) {
				t.Errorf("%s: only %s enabled shows %v, want %v", provider, key, got, want)
			}
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
	for _, testCase := range []struct {
		name, provider string
		enabled        []string
		want           []string
	}{
		{"github security and blame never show", "github", []string{"security", "blame"}, []string{}},
		{"gitlab security and blame never show", "gitlab", []string{"security", "blame"}, []string{}},
		{"github incidents row is an unsupported key", "github", []string{"incidents", "commits"}, []string{"git"}},
		{"jira incidents row shows operational", "jira", []string{"incidents"}, []string{"operational"}},
		{"linear has no incidents dataset", "linear", []string{"incidents"}, []string{}},
		{"canonical work-items key off, one member on", "github", []string{"work-item-labels"}, []string{"work-items"}},
		{"prs member on, canonical off", "gitlab", []string{"pr-comments"}, []string{"prs"}},
		{"tests is its own target, not cicd", "github", []string{"tests"}, []string{"tests"}},
		{"provider case does not matter", "GitHub", []string{"commits"}, []string{"git"}},
		{"an unknown provider shows nothing", "bitbucket", []string{"commits"}, []string{}},
		{"an unknown key shows nothing", "github", []string{"not-a-dataset"}, []string{}},
	} {
		if got := DerivedSyncTargets(testCase.provider, testCase.enabled); !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s: %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// TestSyncTargetHasDatasetNamesThePassthroughTargets: the targets no dataset
// of the provider answers to. GitHub "incidents" is the one the form offers.
func TestSyncTargetHasDatasetNamesThePassthroughTargets(t *testing.T) {
	for _, testCase := range []struct {
		provider, target string
		want             bool
	}{
		{"github", "incidents", false}, {"gitlab", "incidents", true}, {"jira", "operational", true},
		{"jira", "incidents", false}, {"github", "git", true}, {"github", "blame", true}, {"github", "security", true},
		{"github", "Git", false}, {"linear", "git", false}, {"pagerduty", "operational", true},
	} {
		if got := SyncTargetHasDataset(testCase.provider, testCase.target); got != testCase.want {
			t.Errorf("SyncTargetHasDataset(%s, %s) = %v, want %v", testCase.provider, testCase.target, got, testCase.want)
		}
	}
	// Every selectable target some provider supports is selectable; blame and
	// security are not.
	selectable := []string{}
	for target := range operatorSelectableSyncTargets {
		if !OperatorSelectableSyncTarget(target) {
			t.Errorf("%s is in the table and not selectable", target)
		}
		selectable = append(selectable, target)
	}
	sort.Strings(selectable)
	if len(selectable) == 0 || OperatorSelectableSyncTarget("blame") || OperatorSelectableSyncTarget("security") {
		t.Fatalf("selectable = %v; blame and security must not be", selectable)
	}
}

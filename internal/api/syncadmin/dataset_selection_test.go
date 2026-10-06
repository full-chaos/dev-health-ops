package syncadmin

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// rowOwnedProviders are the providers whose rows own the selection: every
// matrix provider but PagerDuty.
var rowOwnedProviders = []string{"github", "gitlab", "jira", "linear", "launchdarkly"}

func registryKeys(provider string) []string {
	keys := []string{}
	for _, capability := range providersync.Capabilities(provider) {
		keys = append(keys, capability.Dataset)
	}
	return keys
}

func sortedStrings(values []string) []string {
	out := append([]string{}, values...)
	sort.Strings(out)
	return out
}

func mustPlan(t *testing.T, provider string, enabled, passthrough, submitted, base []string, baseSet bool) selectionChange {
	t.Helper()
	change, err := planSelectionChange(provider, enabled, passthrough, submitted, base, baseSet)
	if err != nil {
		t.Fatalf("%s: plan: %v", provider, err)
	}
	return change
}

// TestSavingTheShownListChangesNoRowForEveryEnabledSubset is the round trip
// (CHAOS-8816): for every provider and EVERY subset of its dataset keys as
// the enabled rows (the other keys are rows that exist and are off, or no
// row: the plan reads enabled keys only), sending back the list the config
// shows writes no row and keeps the passthrough targets. With the list as
// its own base the same holds.
func TestSavingTheShownListChangesNoRowForEveryEnabledSubset(t *testing.T) {
	cases := 0
	for _, provider := range rowOwnedProviders {
		keys := registryKeys(provider)
		if len(keys) == 0 || len(keys) > 20 {
			t.Fatalf("%s: %d keys; the subset walk needs 1..20", provider, len(keys))
		}
		for _, passthrough := range [][]string{{}, {"no-such-target"}} {
			step := subsetWalkStep
			if len(passthrough) > 0 {
				// The passthrough variant samples: it does not depend on the subset.
				step *= 16
			}
			for mask := 0; mask < 1<<len(keys); mask += step {
				enabled := []string{}
				for index, key := range keys {
					if mask&(1<<index) != 0 {
						enabled = append(enabled, key)
					}
				}
				shown := shownTargets(provider, enabled, passthrough, nil)
				// The shown list holds every passthrough target, and nothing the
				// enabled keys do not give: a save of a list that lost one would
				// read as "the user removed it".
				if want := append(providersync.DerivedSyncTargets(provider, enabled), passthrough...); !reflect.DeepEqual(shown, want) {
					t.Fatalf("%s enabled=%v passthrough=%v: shown %v, want %v", provider, enabled, passthrough, shown, want)
				}
				for _, baseSet := range []bool{false, true} {
					cases++
					change := mustPlan(t, provider, enabled, passthrough, shown, shown, baseSet)
					if len(change.enableKeys)+len(change.disableKeys)+len(change.added)+len(change.removed) != 0 || change.baseDiffers ||
						strings.Join(change.passthrough, ",") != strings.Join(passthrough, ",") {
						t.Fatalf("%s enabled=%v passthrough=%v base=%v: saving the shown list %v is not a no-op: %+v",
							provider, enabled, passthrough, baseSet, shown, change)
					}
				}
			}
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
	t.Logf("%d round-trip cases", cases)
}

// TestCheckAndUncheckWriteExactlyTheKeysOfTheTarget: for every provider and
// every form target, a check switches on exactly PlannerDatasetKeys of that
// target and an uncheck switches off exactly those; "git" moves "blame" on
// GitHub and GitLab; no key of another target is named.
func TestCheckAndUncheckWriteExactlyTheKeysOfTheTarget(t *testing.T) {
	cases := 0
	for _, provider := range rowOwnedProviders {
		all := registryKeys(provider)
		for _, target := range providersync.SupportedLegacyTargets(provider) {
			if !providersync.OperatorSelectableSyncTarget(target) {
				continue
			}
			cases++
			want, err := providersync.PlannerDatasetKeys(provider, []string{target})
			if err != nil || len(want) == 0 {
				t.Fatalf("%s/%s: keys %v err %v", provider, target, want, err)
			}
			// Check: nothing enabled, the user checks the target.
			check := mustPlan(t, provider, nil, nil, []string{target}, nil, false)
			if !reflect.DeepEqual(sortedStrings(check.enableKeys), sortedStrings(want)) || len(check.disableKeys) != 0 {
				t.Errorf("%s: check %s enables %v disables %v, want enable %v only", provider, target, check.enableKeys, check.disableKeys, want)
			}
			// Uncheck: everything enabled, the user unchecks the target.
			shown := shownTargets(provider, all, nil, nil)
			rest := []string{}
			for _, other := range shown {
				if other != target {
					rest = append(rest, other)
				}
			}
			uncheck := mustPlan(t, provider, all, nil, rest, nil, false)
			if !reflect.DeepEqual(sortedStrings(uncheck.disableKeys), sortedStrings(want)) || len(uncheck.enableKeys) != 0 {
				t.Errorf("%s: uncheck %s disables %v enables %v, want disable %v only", provider, target, uncheck.disableKeys, uncheck.enableKeys, want)
			}
			if (provider == "github" || provider == "gitlab") && target == "git" && !strings.Contains(strings.Join(want, ","), "blame") {
				t.Errorf("%s: git does not move blame: %v", provider, want)
			}
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
}

// TestSelectionChangeReadsTheBaseListWhenTheRequestCarriesIt is the base-list
// protocol as a table: equal, stale, absent, a name that is no target.
func TestSelectionChangeReadsTheBaseListWhenTheRequestCarriesIt(t *testing.T) {
	gitOn := []string{"repo-metadata", "commits", "commit-stats", "files", "blame"}
	prsKeys := []string{"pr-comments", "pr-reviews", "prs"}
	for _, testCase := range []struct {
		name                    string
		enabled, submitted      []string
		base                    []string
		baseSet                 bool
		wantEnable, wantDisable []string
		wantDiffers             bool
	}{
		{name: "stale base: another save switched prs off; this save changed nothing",
			enabled: gitOn, submitted: []string{"git", "prs"}, base: []string{"git", "prs"}, baseSet: true, wantDiffers: true},
		{name: "no base: the stale form re-applies its list (the documented fallback)",
			enabled: gitOn, submitted: []string{"git", "prs"}, wantEnable: prsKeys},
		{name: "stale base: the dataset endpoint switched cicd on; this save changed nothing",
			enabled: append(append([]string{}, gitOn...), "cicd"), submitted: []string{"git"}, base: []string{"git"}, baseSet: true, wantDiffers: true},
		{name: "no base: the same request switches cicd off (the documented fallback)",
			enabled: append(append([]string{}, gitOn...), "cicd"), submitted: []string{"git"}, wantDisable: []string{"cicd"}},
		{name: "base equal to the rows: only the user's uncheck writes",
			enabled: append(append([]string{}, gitOn...), prsKeys...), submitted: []string{"git"}, base: []string{"git", "prs"}, baseSet: true, wantDisable: prsKeys},
		{name: "stale base: the user's own uncheck is still applied",
			enabled: append(append([]string{}, gitOn...), "prs", "cicd"), submitted: []string{"git"}, base: []string{"git", "prs"}, baseSet: true,
			wantDisable: prsKeys, wantDiffers: true},
		{name: "base holds a name that is no target: no effect",
			enabled: gitOn, submitted: []string{"git"}, base: []string{"git", "no-such-target"}, baseSet: true, wantDiffers: true},
		{name: "an empty base is a base: everything submitted is added",
			enabled: gitOn, submitted: []string{"git", "prs"}, base: []string{}, baseSet: true,
			wantEnable: append(append([]string{}, gitOn...), prsKeys...), wantDiffers: true},
	} {
		change := mustPlan(t, "github", testCase.enabled, nil, testCase.submitted, testCase.base, testCase.baseSet)
		if !reflect.DeepEqual(sortedStrings(change.enableKeys), sortedStrings(testCase.wantEnable)) ||
			!reflect.DeepEqual(sortedStrings(change.disableKeys), sortedStrings(testCase.wantDisable)) || change.baseDiffers != testCase.wantDiffers {
			t.Errorf("%s:\n got enable %v disable %v differs %v\nwant enable %v disable %v differs %v", testCase.name,
				change.enableKeys, change.disableKeys, change.baseDiffers, testCase.wantEnable, testCase.wantDisable, testCase.wantDiffers)
		}
	}
}

// TestSelectionChangeNeverWritesARowTheFormDoesNotOffer: "security" and
// "blame" in a submitted or base list write no row and are not kept; a
// target with no dataset moves in and out of the passthrough list.
func TestSelectionChangeNeverWritesARowTheFormDoesNotOffer(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		added := mustPlan(t, provider, nil, nil, []string{"security", "blame"}, nil, false)
		removed := mustPlan(t, provider, []string{"security", "blame"}, nil, []string{}, []string{"security", "blame"}, true)
		for name, change := range map[string]selectionChange{"added": added, "removed": removed} {
			if len(change.enableKeys)+len(change.disableKeys)+len(change.passthrough) != 0 {
				t.Errorf("%s: security and blame %s: %+v, want no row write and no passthrough", provider, name, change)
			}
		}
	}
	add := mustPlan(t, "github", []string{"commits"}, nil, []string{"git", "incidents"}, nil, false)
	if !reflect.DeepEqual(add.passthrough, []string{"incidents"}) || len(add.enableKeys)+len(add.disableKeys) != 0 {
		t.Errorf("github incidents added: %+v, want passthrough [incidents] and no row write", add)
	}
	drop := mustPlan(t, "github", []string{"commits"}, []string{"incidents"}, []string{"git"}, nil, false)
	if len(drop.passthrough) != 0 || len(drop.enableKeys)+len(drop.disableKeys) != 0 {
		t.Errorf("github incidents unchecked: %+v, want no passthrough and no row write", drop)
	}
	// On GitLab "incidents" is a real dataset: a row write, not passthrough.
	gitlab := mustPlan(t, "gitlab", nil, nil, []string{"incidents"}, nil, false)
	if !reflect.DeepEqual(gitlab.enableKeys, []string{"incidents"}) || len(gitlab.passthrough) != 0 {
		t.Errorf("gitlab incidents added: %+v, want the incidents row enabled", gitlab)
	}
}

// TestGateReadsOnlyAddedTargetsAndPassthrough: the canonical-incident gate's
// input. A gated target that shows because its row is on is not in it.
func TestGateReadsOnlyAddedTargetsAndPassthrough(t *testing.T) {
	gated := func(change selectionChange) []string {
		out := []string{}
		for _, value := range change.gatedTargets() {
			out = append(out, value.(string))
		}
		return out
	}
	jiraOn := []string{"work-items", "incidents"}
	for _, testCase := range []struct {
		name, provider                  string
		enabled, passthrough, submitted []string
		want                            []string
	}{
		{"jira incidents row on, the list echoed back", "jira", jiraOn, nil, []string{"work-items", "operational"}, []string{}},
		{"gitlab incidents row on, the list echoed back", "gitlab", []string{"commits", "incidents"}, nil, []string{"git", "incidents"}, []string{}},
		{"gitlab: the user adds incidents", "gitlab", []string{"commits"}, nil, []string{"git", "incidents"}, []string{"incidents"}},
		{"jira: the user adds operational", "jira", []string{"work-items"}, nil, []string{"work-items", "operational"}, []string{"operational"}},
		{"github stored incidents, list unchanged", "github", []string{"commits"}, []string{"incidents"}, []string{"git", "incidents"}, []string{"incidents"}},
		{"github stored incidents, unchecked", "github", []string{"commits"}, []string{"incidents"}, []string{"git"}, []string{}},
		{"github: the user adds incidents", "github", []string{"commits"}, nil, []string{"git", "incidents"}, []string{"incidents"}},
	} {
		got := gated(mustPlan(t, testCase.provider, testCase.enabled, testCase.passthrough, testCase.submitted, nil, false))
		if !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s: gate reads %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// TestShownTargetsKeepTheOrderOfAnAgreeingList: a list that agrees with the
// rows is returned as it is; a target the rows add comes after it in the
// registry's order; a target whose rows are all off is dropped.
func TestShownTargetsKeepTheOrderOfAnAgreeingList(t *testing.T) {
	enabled := []string{"work-items", "commits", "prs", "cicd"}
	if got := shownTargets("github", enabled, []string{"incidents"}, []string{"work-items", "incidents", "git"}); !reflect.DeepEqual(got,
		[]string{"work-items", "incidents", "git", "prs", "cicd"}) {
		t.Errorf("preferred order: %v", got)
	}
	if got := shownTargets("github", []string{"commits"}, nil, []string{"prs", "git", "git"}); !reflect.DeepEqual(got, []string{"git"}) {
		t.Errorf("a target whose rows are off is dropped, a repeat is shown once: %v", got)
	}
	if got := shownTargets("github", nil, nil, nil); got == nil || len(got) != 0 {
		t.Errorf("nothing enabled: %#v, want an empty list (not null)", got)
	}
}

// TestRowsOwnSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty.
func TestRowsOwnSelectionOnlyForAWholeIntegrationConfigThatIsNotPagerDuty(t *testing.T) {
	integration, source := uuid.New(), uuid.New()
	for _, testCase := range []struct {
		name   string
		config syncConfig
		want   bool
	}{
		{"whole-integration github", syncConfig{Provider: "github", IntegrationID: &integration}, true},
		{"whole-integration Jira, mixed case", syncConfig{Provider: "Jira", IntegrationID: &integration}, true},
		{"child pinned to a source", syncConfig{Provider: "github", IntegrationID: &integration, SourceID: &source}, false},
		{"no integration", syncConfig{Provider: "linear"}, false},
		{"pagerduty", syncConfig{Provider: "pagerduty", IntegrationID: &integration}, false},
		{"PagerDuty, mixed case", syncConfig{Provider: "PagerDuty", IntegrationID: &integration}, false},
	} {
		if got := rowsOwnSelection(&testCase.config); got != testCase.want {
			t.Errorf("%s: %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// TestDecodeSyncConfigUpdateReadsTheBaseList: absent and null are no base; a
// list of strings is the base (an empty list too); any other value is a 422
// problem at body.sync_targets_base, as a malformed sync_targets is.
func TestDecodeSyncConfigUpdateReadsTheBaseList(t *testing.T) {
	decode := func(text string) (syncConfigUpdate, pybody.Errors) {
		t.Helper()
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("parse %s: %v", text, err)
		}
		return decodeSyncConfigUpdate(pybody.Body{Value: value})
	}
	for _, text := range []string{`{"sync_targets":["git"]}`, `{"sync_targets":["git"],"sync_targets_base":null}`} {
		in, problems := decode(text)
		if len(problems) != 0 || in.syncTargetsBaseSet || in.syncTargetsBase != nil || !in.syncTargetsSet {
			t.Errorf("%s: base set %v %v, problems %v; want no base and no problem", text, in.syncTargetsBaseSet, in.syncTargetsBase, problems)
		}
	}
	for text, want := range map[string][]string{
		`{"sync_targets":["git"],"sync_targets_base":["git","prs"]}`: {"git", "prs"},
		`{"sync_targets":["git"],"sync_targets_base":[]}`:            {},
		`{"sync_targets_base":["git"]}`:                              {"git"},
	} {
		in, problems := decode(text)
		if len(problems) != 0 || !in.syncTargetsBaseSet || !reflect.DeepEqual(append([]string{}, in.syncTargetsBase...), want) {
			t.Errorf("%s: base %v set %v problems %v; want %v", text, in.syncTargetsBase, in.syncTargetsBaseSet, problems, want)
		}
	}
	for _, text := range []string{
		`{"sync_targets":["git"],"sync_targets_base":"git"}`,
		`{"sync_targets":["git"],"sync_targets_base":["git",7]}`,
		`{"sync_targets":["git"],"sync_targets_base":{"git":true}}`,
		`{"sync_targets":["git"],"sync_targets_base":3}`,
		`{"sync_targets":["git"],"sync_targets_base":[null]}`,
	} {
		_, problems := decode(text)
		if len(problems) != 1 {
			t.Errorf("%s: %d problems, want exactly one", text, len(problems))
			continue
		}
		if loc := problems[0].Loc; len(loc) < 2 || loc[0] != "body" || loc[1] != "sync_targets_base" {
			t.Errorf("%s: problem at %v, want body.sync_targets_base", text, loc)
		}
	}
}

// TestIncidentGateTargetsKeepEveryItemThatIsNotAMirroredString: the gate
// input of the routes keeps an item that is not a string (the gate answers
// it as it always did) and every item of a config with no integration.
func TestIncidentGateTargetsKeepEveryItemThatIsNotAMirroredString(t *testing.T) {
	integration := uuid.New()
	stored := []pyjson.Value{"work-items", float64(7), "operational", nil}
	whole := incidentGateTargets(&syncConfig{Provider: "jira", IntegrationID: &integration}, stored)
	if want := []pyjson.Value{float64(7), nil}; !reflect.DeepEqual(whole, want) {
		t.Errorf("whole-integration jira: %v, want %v (the two mirrored strings left out, the other items kept)", whole, want)
	}
	if none := incidentGateTargets(&syncConfig{Provider: "jira"}, stored); !reflect.DeepEqual(none, stored) {
		t.Errorf("jira with no integration: %v, want every item %v", none, stored)
	}
}

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

func mustPlan(t *testing.T, provider string, enabled, stored, submitted []string) selectionChange {
	t.Helper()
	change, err := planSelectionChange(provider, enabled, stored, submitted)
	if err != nil {
		t.Fatalf("%s: plan: %v", provider, err)
	}
	return change
}

// storedVariants is the stored lists the subset walks run over for one
// provider: nothing, a name that is no target, and two real targets of the
// provider (the first one twice: a stored list keeps its duplicates).
func storedVariants(provider string) [][]string {
	targets := []string{}
	for _, target := range providersync.SupportedLegacyTargets(provider) {
		if providersync.OperatorSelectableSyncTarget(target) {
			targets = append(targets, target)
		}
	}
	variants := [][]string{{}, {"no-such-target"}}
	if len(targets) > 0 {
		variants = append(variants, []string{targets[len(targets)-1], "no-such-target", targets[0], targets[0]})
	}
	return variants
}

// TestSavingTheShownListChangesNoRowAndKeepsTheStoredList is the round trip
// (CHAOS-8816): for every provider, EVERY subset of its dataset keys as the
// enabled rows (the other keys are rows that exist and are off, or no row:
// the plan reads enabled keys only) and each stored list, sending back the
// list the config shows writes no row and stores the list that was stored,
// item for item. So a target the list shows only because a row is on is
// never stored, and the canonical-incident gate reads what it read before.
func TestSavingTheShownListChangesNoRowAndKeepsTheStoredList(t *testing.T) {
	cases := 0
	for _, provider := range rowOwnedProviders {
		keys := registryKeys(provider)
		if len(keys) == 0 || len(keys) > 20 {
			t.Fatalf("%s: %d keys; the subset walk needs 1..20", provider, len(keys))
		}
		for variant, stored := range storedVariants(provider) {
			step := subsetWalkStep
			if variant > 0 {
				// The other stored lists sample the subsets.
				step *= 16
			}
			for mask := 0; mask < 1<<len(keys); mask += step {
				enabled := []string{}
				for index, key := range keys {
					if mask&(1<<index) != 0 {
						enabled = append(enabled, key)
					}
				}
				shown := shownTargets(provider, enabled, stored)
				// The shown list is the stored list, then the targets only the
				// enabled keys give: a save of a list that lost one would read
				// as "the user removed it".
				want := append([]string{}, stored...)
				for _, target := range providersync.DerivedSyncTargets(provider, enabled) {
					if !stringSet(stored)[target] {
						want = append(want, target)
					}
				}
				if !reflect.DeepEqual(shown, want) {
					t.Fatalf("%s enabled=%v stored=%v: shown %v, want %v", provider, enabled, stored, shown, want)
				}
				cases++
				change := mustPlan(t, provider, enabled, stored, shown)
				if len(change.enableKeys)+len(change.disableKeys)+len(change.added)+len(change.removed) != 0 ||
					strings.Join(change.stored, ",") != strings.Join(stored, ",") {
					t.Fatalf("%s enabled=%v stored=%v: saving the shown list %v is not a no-op: %+v", provider, enabled, stored, shown, change)
				}
			}
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
	t.Logf("%d round-trip cases", cases)
}

// TestASaveStoresOnlyWhatWasStoredOrWhatItAdds is the stored-list invariant
// over every provider, sampled subsets of enabled keys, each stored list and
// EVERY one-target change of the shown list (one target dropped, one target
// added): each stored item was stored before or is added by this save, an
// added item is not in the list the server showed, and no item the submitted
// list names only because a row is on is stored.
func TestASaveStoresOnlyWhatWasStoredOrWhatItAdds(t *testing.T) {
	cases := 0
	for _, provider := range rowOwnedProviders {
		keys := registryKeys(provider)
		candidates := append(providersync.SupportedLegacyTargets(provider), "no-such-target", "incidents")
		for _, stored := range storedVariants(provider) {
			for mask := 0; mask < 1<<len(keys); mask += subsetWalkStep * 16 {
				enabled := []string{}
				for index, key := range keys {
					if mask&(1<<index) != 0 {
						enabled = append(enabled, key)
					}
				}
				shown := shownTargets(provider, enabled, stored)
				inShown, inStored := stringSet(shown), stringSet(stored)
				submissions := [][]string{{}}
				for _, drop := range uniqueStrings(shown) {
					rest := []string{}
					for _, target := range shown {
						if target != drop {
							rest = append(rest, target)
						}
					}
					submissions = append(submissions, rest)
				}
				for _, add := range candidates {
					if !inShown[add] {
						submissions = append(submissions, append(append([]string{}, shown...), add))
					}
				}
				for _, submitted := range submissions {
					cases++
					change := mustPlan(t, provider, enabled, stored, submitted)
					inAdded, inSubmitted := stringSet(change.added), stringSet(submitted)
					for _, target := range change.added {
						if inShown[target] || !inSubmitted[target] {
							t.Fatalf("%s enabled=%v stored=%v submitted=%v: added %q is shown already or not submitted", provider, enabled, stored, submitted, target)
						}
					}
					for _, target := range change.stored {
						if !inSubmitted[target] || (!inStored[target] && !inAdded[target]) {
							t.Fatalf("%s enabled=%v stored=%v submitted=%v: the save stores %q, which was not stored and is not added (stored %v)",
								provider, enabled, stored, submitted, target, change.stored)
						}
					}
					for _, target := range submitted {
						if (inStored[target] || !inShown[target]) && !stringSet(change.stored)[target] {
							t.Fatalf("%s enabled=%v stored=%v submitted=%v: the save does not store %q (stored %v)", provider, enabled, stored, submitted, target, change.stored)
						}
					}
				}
			}
		}
	}
	if cases == 0 {
		t.Fatal("no case ran")
	}
	t.Logf("%d save cases", cases)
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
			check := mustPlan(t, provider, nil, nil, []string{target})
			if !reflect.DeepEqual(sortedStrings(check.enableKeys), sortedStrings(want)) || len(check.disableKeys) != 0 ||
				!reflect.DeepEqual(check.stored, []string{target}) {
				t.Errorf("%s: check %s enables %v disables %v stores %v, want enable %v only and the target stored", provider, target,
					check.enableKeys, check.disableKeys, check.stored, want)
			}
			// Uncheck: everything enabled, the user unchecks the target; once
			// with the target shown by its rows only, once stored as well.
			for _, stored := range [][]string{nil, {target}} {
				shown := shownTargets(provider, all, stored)
				rest := []string{}
				for _, other := range shown {
					if other != target {
						rest = append(rest, other)
					}
				}
				uncheck := mustPlan(t, provider, all, stored, rest)
				if !reflect.DeepEqual(sortedStrings(uncheck.disableKeys), sortedStrings(want)) || len(uncheck.enableKeys) != 0 || len(uncheck.stored) != 0 {
					t.Errorf("%s stored=%v: uncheck %s disables %v enables %v stores %v, want disable %v only and nothing stored", provider, stored, target,
						uncheck.disableKeys, uncheck.enableKeys, uncheck.stored, want)
				}
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

// TestSelectionChangeReadsOnlyTheListTheServerShows is the save rule as a
// table: the reference of a save is the stored list plus what the enabled
// rows add, and nothing else.
func TestSelectionChangeReadsOnlyTheListTheServerShows(t *testing.T) {
	gitOn := []string{"repo-metadata", "commits", "commit-stats", "files", "blame"}
	prsKeys := []string{"pr-comments", "pr-reviews", "prs"}
	for _, testCase := range []struct {
		name                       string
		enabled, stored, submitted []string
		wantEnable, wantDisable    []string
		wantStored                 []string
	}{
		{name: "a form loaded before another save switched prs off re-applies its list",
			enabled: gitOn, submitted: []string{"git", "prs"}, wantEnable: prsKeys, wantStored: []string{"prs"}},
		{name: "a form loaded before the dataset endpoint switched cicd on switches it off",
			enabled: append(append([]string{}, gitOn...), "cicd"), submitted: []string{"git"}, wantDisable: []string{"cicd"}, wantStored: []string{}},
		{name: "a stored target whose rows are on is unchecked: rows off, item dropped",
			enabled: append(append([]string{}, gitOn...), prsKeys...), stored: []string{"git", "prs"}, submitted: []string{"git"},
			wantDisable: prsKeys, wantStored: []string{"git"}},
		{name: "a stored target whose rows are off is unchecked: item dropped (its keys are named; the write changes only a row that is on)",
			enabled: gitOn, stored: []string{"git", "prs"}, submitted: []string{"git"}, wantDisable: prsKeys, wantStored: []string{"git"}},
		{name: "a stored target whose rows are off is sent back: it stays stored and its rows stay off",
			enabled: gitOn, stored: []string{"git", "prs"}, submitted: []string{"git", "prs"}, wantStored: []string{"git", "prs"}},
		{name: "a row-only target is sent back with a new one: only the new one is stored",
			enabled: gitOn, submitted: []string{"prs", "git"}, wantEnable: prsKeys, wantStored: []string{"prs"}},
		{name: "the submitted order and duplicates are kept",
			enabled: nil, stored: []string{"git"}, submitted: []string{"prs", "git", "prs"}, wantEnable: prsKeys, wantStored: []string{"prs", "git", "prs"}},
	} {
		change := mustPlan(t, "github", testCase.enabled, testCase.stored, testCase.submitted)
		if !reflect.DeepEqual(sortedStrings(change.enableKeys), sortedStrings(testCase.wantEnable)) ||
			!reflect.DeepEqual(sortedStrings(change.disableKeys), sortedStrings(testCase.wantDisable)) ||
			!reflect.DeepEqual(change.stored, testCase.wantStored) {
			t.Errorf("%s:\n got enable %v disable %v stored %v\nwant enable %v disable %v stored %v", testCase.name,
				change.enableKeys, change.disableKeys, change.stored, testCase.wantEnable, testCase.wantDisable, testCase.wantStored)
		}
	}
}

// TestSelectionChangeNeverWritesARowTheFormDoesNotOffer: "security" and
// "blame" in a submitted list write no row (they are stored, as any target a
// request asks for); a target with no dataset moves in and out of the stored
// list and writes no row.
func TestSelectionChangeNeverWritesARowTheFormDoesNotOffer(t *testing.T) {
	for _, provider := range []string{"github", "gitlab"} {
		added := mustPlan(t, provider, nil, nil, []string{"security", "blame"})
		if len(added.enableKeys)+len(added.disableKeys) != 0 || !reflect.DeepEqual(added.stored, []string{"security", "blame"}) {
			t.Errorf("%s: security and blame added: %+v, want no row write and both stored", provider, added)
		}
		removed := mustPlan(t, provider, []string{"security", "blame"}, []string{"security", "blame"}, []string{})
		if len(removed.enableKeys)+len(removed.disableKeys)+len(removed.stored) != 0 {
			t.Errorf("%s: security and blame removed: %+v, want no row write and nothing stored", provider, removed)
		}
	}
	add := mustPlan(t, "github", []string{"commits"}, nil, []string{"git", "incidents"})
	if !reflect.DeepEqual(add.stored, []string{"incidents"}) || len(add.enableKeys)+len(add.disableKeys) != 0 {
		t.Errorf("github incidents added: %+v, want [incidents] stored and no row write", add)
	}
	drop := mustPlan(t, "github", []string{"commits"}, []string{"incidents"}, []string{"git"})
	if len(drop.stored) != 0 || len(drop.enableKeys)+len(drop.disableKeys) != 0 {
		t.Errorf("github incidents unchecked: %+v, want nothing stored and no row write", drop)
	}
	// On GitLab "incidents" is a real dataset: a row write and a stored item.
	gitlab := mustPlan(t, "gitlab", nil, nil, []string{"incidents"})
	if !reflect.DeepEqual(gitlab.enableKeys, []string{"incidents"}) || !reflect.DeepEqual(gitlab.stored, []string{"incidents"}) {
		t.Errorf("gitlab incidents added: %+v, want the incidents row enabled and the target stored", gitlab)
	}
}

// TestTheSaveStoresWhatTheGateReads: the canonical-incident gate's input is
// the list the save stores. A gated target that shows only because its row
// is on is not in it; a gated target a request asked for is, whether this
// save adds it or an earlier one stored it.
func TestTheSaveStoresWhatTheGateReads(t *testing.T) {
	jiraOn := []string{"work-items", "incidents"}
	for _, testCase := range []struct {
		name, provider             string
		enabled, stored, submitted []string
		want                       []string
	}{
		{"jira incidents row on, the list echoed back", "jira", jiraOn, nil, []string{"work-items", "operational"}, []string{}},
		{"gitlab incidents row on, the list echoed back", "gitlab", []string{"commits", "incidents"}, nil, []string{"git", "incidents"}, []string{}},
		{"gitlab incidents row on and stored, the list echoed back", "gitlab", []string{"commits", "incidents"}, []string{"git", "incidents"},
			[]string{"git", "incidents"}, []string{"git", "incidents"}},
		{"gitlab: the user adds incidents", "gitlab", []string{"commits"}, nil, []string{"git", "incidents"}, []string{"incidents"}},
		{"jira: the user adds operational", "jira", []string{"work-items"}, []string{"work-items"}, []string{"work-items", "operational"},
			[]string{"work-items", "operational"}},
		{"jira: operational stored, its row off, the list echoed back", "jira", []string{"work-items"}, []string{"operational"},
			[]string{"operational", "work-items"}, []string{"operational"}},
		{"github stored incidents, list unchanged", "github", []string{"commits"}, []string{"incidents"}, []string{"incidents", "git"}, []string{"incidents"}},
		{"github stored incidents, unchecked", "github", []string{"commits"}, []string{"incidents"}, []string{"git"}, []string{}},
		{"github: the user adds incidents", "github", []string{"commits"}, nil, []string{"git", "incidents"}, []string{"incidents"}},
	} {
		got := mustPlan(t, testCase.provider, testCase.enabled, testCase.stored, testCase.submitted).stored
		if !reflect.DeepEqual(got, testCase.want) {
			t.Errorf("%s: the save stores and gates %v, want %v", testCase.name, got, testCase.want)
		}
	}
}

// TestShownTargetsAreTheStoredListThenWhatTheRowsAdd: the stored list is
// shown as it is (a target whose rows are off and a repeat included); a
// target only the rows give comes after it in the registry's order.
func TestShownTargetsAreTheStoredListThenWhatTheRowsAdd(t *testing.T) {
	enabled := []string{"work-items", "commits", "prs", "cicd"}
	if got := shownTargets("github", enabled, []string{"work-items", "incidents", "git"}); !reflect.DeepEqual(got,
		[]string{"work-items", "incidents", "git", "prs", "cicd"}) {
		t.Errorf("stored order first: %v", got)
	}
	if got := shownTargets("github", []string{"commits"}, []string{"prs", "git", "git"}); !reflect.DeepEqual(got, []string{"prs", "git", "git"}) {
		t.Errorf("the stored list is shown as stored: %v", got)
	}
	if got := shownTargets("github", []string{"commits"}, nil); !reflect.DeepEqual(got, []string{"git"}) {
		t.Errorf("a row-only target: %v", got)
	}
	if got := shownTargets("github", nil, nil); got == nil || len(got) != 0 {
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

// TestDecodeSyncConfigUpdateIgnoresABaseList: the body field
// sync_targets_base has no meaning. Whatever its value, the body decodes
// with no problem and to the same update as the body without it: what a save
// adds is decided against the list the server shows, never a list the
// request brings.
func TestDecodeSyncConfigUpdateIgnoresABaseList(t *testing.T) {
	decode := func(text string) (syncConfigUpdate, pybody.Errors) {
		t.Helper()
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatalf("parse %s: %v", text, err)
		}
		return decodeSyncConfigUpdate(pybody.Body{Value: value})
	}
	want, problems := decode(`{"sync_targets":["git"]}`)
	if len(problems) != 0 || !want.syncTargetsSet {
		t.Fatalf("plain body: %+v problems %v", want, problems)
	}
	for _, base := range []string{`null`, `["git","prs"]`, `[]`, `"git"`, `["git",7]`, `{"git":true}`, `3`, `[null]`} {
		text := `{"sync_targets":["git"],"sync_targets_base":` + base + `}`
		got, problems := decode(text)
		if len(problems) != 0 || !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %+v problems %v; want the update of the body without the field and no problem", text, got, problems)
		}
	}
}

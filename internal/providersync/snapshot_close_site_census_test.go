package providersync

import (
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// snapshotKindCensus is every fact kind a snapshot may close, by the name on
// its log line and metric: the constructor that makes it, what an empty
// answer of the kind means, and why. A new kind, or a change of a kind's
// empty-answer policy, is a deliberate edit of this table.
var snapshotKindCensus = map[string]struct {
	constructor, empty, why string
}{
	// Scope: EVERY kind below closes only behind the one scope gate
	// (ProveSoleScope: no other active integration of the provider in the
	// organization). The rows of no kind carry an integration key, and
	// public.integrations has no unique (org, provider) rule, so no kind can
	// say "two integrations cannot exist": snapshotKindScope is the same for
	// all, and TestSnapshotKindPolicyTableIsTheDocumentedOne pins it in the
	// two documented tables.
	"linear_project_ownership": {"internal/providersync.LinearProjectOwnershipKind", "EmptyClosesNothing",
		"one projects walk for the workspace: an answer with no project ownership is an access change before it is a removal"},
	"linear_team_key_ownership": {"internal/providersync.LinearTeamKeyOwnershipKind", "EmptyClosesNothing",
		"one teams walk for the workspace: an answer with no team is an access change before it is a removal"},
	"jira_legacy_ownership": {"internal/providersync.JiraLegacyOwnershipKind", "EmptyClosesNothing",
		"one project search for the site: no live project is far more often an access change than a removal"},
	"gitlab_group_project_grants": {"internal/providersync.GitLabGroupProjectGrantKind", "EmptyIsAnAnswer",
		"one listing per group, each with its own proven end, in a scope no other integration lists: a group with no project is an answer"},
	"github_team_repo_grants": {"internal/providersync.GitHubTeamRepoGrantKind", "EmptyIsAnAnswer",
		"one listing per team, each with its own proven end, in a scope no other integration lists: a team with no repository is an answer"},
	"atlassian_team_project_links": {"internal/providersync.AtlassianTeamLinkKind", "EmptyIsAnAnswer",
		"one link read per team, each to its end: a team with no link is an answer; a team outside the search answer is in scope " +
			"only through atlassian_team_catalog"},
	"atlassian_team_memberships": {"internal/providersync.AtlassianTeamMembershipKind", "EmptyIsAnAnswer",
		"one member read per team, each to its end: a team with no member is an answer; a team outside the search answer is in " +
			"scope only through atlassian_team_catalog"},
	"atlassian_team_catalog": {"internal/providersync.AtlassianTeamCatalogKind", "EmptyClosesNothing",
		"one team search for the organization: a search that answers no team is an access change before every team was deleted"},
}

// snapshotCloseSites is every production function that turns the retractions
// of a snapshot plan into rows to write: the only places a provider snapshot
// closes a row. Each one names the rule it calls and the proof it takes.
var snapshotCloseSites = map[string]string{
	"internal/providersync.linearOwnershipSnapshot":     "KindSnapshot arguments from linearOwnershipKindSnapshots (two kinds, each with the terms of its own walk)",
	"internal/providersync.jiraOwnershipSnapshot":       "KindSnapshot argument: JiraLegacyOwnershipKind with the project search and legacy links terms",
	"internal/providersync.gitlabOwnershipSnapshot":     "KindSnapshot argument from ownershipCloseDecision.snapshot (closable teams only, the gate's terms)",
	"internal/providersync.githubRepoOwnershipSnapshot": "KindSnapshot argument from ownershipCloseDecision.snapshot (closable teams only, the gate's terms)",
	"internal/atlassianteams.planOwnership":             "KindSnapshot argument: AtlassianTeamLinkKind with the Rows.ProjectLinksComplete term",
	"internal/atlassianteams.planMemberships":           "KindSnapshot argument: AtlassianTeamMembershipKind with the Rows.MembershipsComplete term",
	"internal/atlassianteams.teamsInScope":              "makes its proof in place: AtlassianTeamCatalogKind with the Rows.TeamSearchComplete term",
}

// validToOutsideTheSnapshotRule is every production function that sets a
// valid_to and is NOT a close site of the snapshot rule, with the proof it
// closes on. A new one is a deliberate edit here.
var validToOutsideTheSnapshotRule = map[string]string{
	"internal/providersync.pagerDutyServiceMappingTombstone": "PagerDuty reference tombstone (the ported _reference_tombstone): the sink " +
		"is given a CompleteRouteBatch only, and the route returns an error and no batch for a failed page or the page bound. An " +
		"empty complete answer tombstones every active row: ported behaviour of an operational table, not under the team catalog rule",
}

// TestSnapshotKindCensus pins the fact kinds: every NewSnapshotKind call of
// the production tree is in snapshot_kinds.go, has a literal name and a named
// empty-answer policy, and is the kind the table says.
func TestSnapshotKindCensus(t *testing.T) {
	census := ownershipCensusOfTheTree(t)
	if !reflect.DeepEqual(census.kindFiles, map[string]bool{"internal/providersync/snapshot_kinds.go": true}) {
		t.Errorf("NewSnapshotKind is called in %v, want only internal/providersync/snapshot_kinds.go: a fact kind is made in one file",
			census.kindFiles)
	}
	got := map[string][2]string{}
	for constructor, kinds := range census.kinds {
		for _, kind := range kinds {
			if _, twice := got[kind[0]]; twice {
				t.Errorf("the kind name %q is made twice", kind[0])
			}
			got[kind[0]] = [2]string{constructor, kind[1]}
		}
	}
	want := map[string][2]string{}
	for name, entry := range snapshotKindCensus {
		want[name] = [2]string{entry.constructor, entry.empty}
		if strings.TrimSpace(entry.why) == "" {
			t.Errorf("the kind %q does not say why its empty answer means %s", name, entry.empty)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("the fact kinds changed.\n got  %v\n want %v\nA kind and what its empty answer means are named in snapshotKindCensus.", got, want)
	}
}

// stampOnlySnapshotPlanners call the snapshot rule for the valid_from stamps of
// the rows a run holds again, and for nothing else: no kind, no retraction.
// They are not close sites, and the census checks that they are not.
var stampOnlySnapshotPlanners = map[string]string{
	"internal/providersync.firstSeenMembershipValidFrom": "reuses the earliest open valid_from of a membership fact the run holds " +
		"again (CHAOS-9007, membership_first_seen.go); it passes no KindSnapshot and closes nothing",
}

// TestEveryCloseSiteTakesTheTypedSnapshot fails when a function closes rows
// from a snapshot without the typed per-kind proof: a new close site, a close
// site that goes back to a bool, a close that does not come from the rule, or
// a proof term made from a constant.
func TestEveryCloseSiteTakesTheTypedSnapshot(t *testing.T) {
	census := ownershipCensusOfTheTree(t)

	closeSites := []string{}
	for function := range census.closes {
		closeSites = append(closeSites, function)
	}
	sort.Strings(closeSites)
	want := make([]string, 0, len(snapshotCloseSites))
	for function, proof := range snapshotCloseSites {
		want = append(want, function)
		if strings.TrimSpace(proof) == "" {
			t.Errorf("%s does not name the proof it takes", function)
		}
	}
	sort.Strings(want)
	if !reflect.DeepEqual(closeSites, want) {
		t.Fatalf("the functions that turn a plan's retractions into rows changed.\n got  %v\n want %v\n"+
			"A close site is named in snapshotCloseSites with the proof it takes.", closeSites, want)
	}
	for _, function := range closeSites {
		callsRule := false
		for entry := range ownershipSnapshotEntryPoints {
			callsRule = callsRule || census.calls[function][entry]
		}
		if !callsRule {
			t.Errorf("%s closes rows of a plan it did not make: a close site calls the snapshot rule itself", function)
		}
		if !census.takesKind[function] && !census.calls[function]["ProveSnapshot"] {
			t.Errorf("%s takes no KindSnapshot and makes no proof: a close site takes the typed per-kind snapshot", function)
		}
		if len(census.boolParams[function]) != 0 {
			t.Errorf("%s has the bool parameters %v: completeness is a SnapshotProof inside a KindSnapshot, never a bool",
				function, census.boolParams[function])
		}
	}
	// Every caller of the rule is a close site: nobody plans a snapshot and
	// drops the plan, or closes through another path.
	// ...unless it is NAMED as a stamp-only planner: it calls the rule for the
	// valid_from stamps alone, passes no kind, turns no retraction into a row
	// and sets no valid_to. What that means is checked, not trusted.
	for _, planner := range census.planners {
		reason, stampOnly := stampOnlySnapshotPlanners[planner]
		switch {
		case stampOnly:
			if strings.TrimSpace(reason) == "" {
				t.Errorf("%s is named in stampOnlySnapshotPlanners and gives no reason", planner)
			}
			if census.closes[planner] || census.setsValidTo[planner] || census.takesKind[planner] {
				t.Errorf("%s is named in stampOnlySnapshotPlanners and closes rows or takes a kind: it is a close site, name it in snapshotCloseSites", planner)
			}
		case !census.closes[planner]:
			t.Errorf("%s calls the snapshot rule and is not a close site (a planner that only reuses stamps is named in stampOnlySnapshotPlanners)", planner)
		}
	}
	for planner := range stampOnlySnapshotPlanners {
		known := false
		for _, candidate := range census.planners {
			known = known || candidate == planner
		}
		if !known {
			t.Errorf("stampOnlySnapshotPlanners names %s, which does not call the snapshot rule", planner)
		}
	}
	// A row's valid_to is set from a plan's retraction, in a close site.
	for function, proof := range validToOutsideTheSnapshotRule {
		if !census.setsValidTo[function] || census.closes[function] || strings.TrimSpace(proof) == "" {
			t.Errorf("%s is named in validToOutsideTheSnapshotRule and sets no valid_to, is a close site, or names no proof", function)
		}
	}
	for function := range census.setsValidTo {
		if _, named := validToOutsideTheSnapshotRule[function]; named {
			continue
		}
		if !census.closes[function] {
			t.Errorf("%s sets a valid_to and is not a close site of the snapshot rule: name it in snapshotCloseSites and plan the "+
				"close through PlanSnapshot", function)
		}
	}
	if len(census.setsValidTo) < 4 {
		t.Fatalf("the scan found %d functions that set a valid_to: it measured nothing", len(census.setsValidTo))
	}
	if len(census.constantTerms) != 0 {
		t.Errorf("a snapshot proof term is made from a constant or has no named reason: %v", census.constantTerms)
	}
	// One scope gate, used by every provider: a proven ScopeProof is made in
	// proveSoleScope and nowhere else, and it is the only reader of the
	// integration census. A kind cannot be stated without a ScopeProof (it is
	// an argument of SnapshotKind.Snapshot), so every close site is behind it.
	gate := "internal/providersync.proveSoleScope"
	for _, maker := range census.scopeMakers {
		if maker != gate {
			t.Errorf("%s makes a ScopeProof: the scope gate %s is the only maker", maker, gate)
		}
	}
	if len(census.scopeMakers) == 0 {
		t.Fatal("the scan found no maker of a ScopeProof: it measured nothing")
	}
	if !reflect.DeepEqual(census.censusCallers, []string{gate}) {
		t.Errorf("the integration census is read by %v, want only %s", census.censusCallers, gate)
	}
}

// snapshotKindScope is the scope proof every fact kind needs, as the two
// documented tables word it.
const snapshotKindScope = "sole integration"

// TestSnapshotKindPolicyTableIsTheDocumentedOne compares the three places
// that state what an empty answer of a fact kind means: the census (which
// TestSnapshotKindCensus compares with the code that makes the kinds), the
// table in the doc comment of snapshot_kinds.go, and the table of the
// architecture document. A kind or a policy that is in one and not in the
// others fails.
func TestSnapshotKindPolicyTableIsTheDocumentedOne(t *testing.T) {
	wording := map[string]string{"EmptyClosesNothing": "closes nothing", "EmptyIsAnAnswer": "is an answer"}
	want := map[string]string{}
	for name, entry := range snapshotKindCensus {
		policy, known := wording[entry.empty]
		if !known {
			t.Fatalf("the kind %q has the empty-answer policy %q, which this test has no wording for", name, entry.empty)
		}
		want[name] = snapshotKindScope + "; " + policy
	}
	read := func(path string, row *regexp.Regexp) map[string]string {
		t.Helper()
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		got := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			if match := row.FindStringSubmatch(line); match != nil {
				if _, twice := got[match[1]]; twice {
					t.Errorf("%s names the kind %q twice", path, match[1])
				}
				got[match[1]] = match[2] + "; " + match[3]
			}
		}
		if len(got) == 0 {
			t.Fatalf("%s holds no row of the policy table: the test measured nothing", path)
		}
		return got
	}
	code := read("snapshot_kinds.go", regexp.MustCompile(`^//\t([a-z_]+)\s+(sole integration)\s+(closes nothing|is an answer)$`))
	if !reflect.DeepEqual(code, want) {
		t.Errorf("the policy table in the doc comment of snapshot_kinds.go differs from the census.\n got  %v\n want %v", code, want)
	}
	document := read("../../docs/contribute/architecture/team-attribution.md",
		regexp.MustCompile("^\\s*\\| `([a-z_]+)` \\|[^|]*\\|[^|]*\\| (sole integration) \\| (closes nothing|is an answer)\\b[^|]*\\|$"))
	if !reflect.DeepEqual(document, want) {
		t.Errorf("the kinds table of docs/contribute/architecture/team-attribution.md differs from the census.\n got  %v\n want %v", document, want)
	}
}

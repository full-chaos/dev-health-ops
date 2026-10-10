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
	// absence is what the kind takes as the proof that an open fact the run
	// does not hold is gone (snapshotAbsenceByKind).
	absence string
}{
	// Scope: EVERY kind below closes only behind the one scope gate
	// (ProveSoleScope: no other active integration of the provider in the
	// organization). The rows of no kind carry an integration key, and
	// public.integrations has no unique (org, provider) rule, so no kind can
	// say "two integrations cannot exist": snapshotKindScope is the same for
	// all, and TestSnapshotKindPolicyTableIsTheDocumentedOne pins it in the
	// two documented tables.
	"linear_project_ownership": {"internal/providersync.LinearProjectOwnershipKind", "EmptyClosesNothing",
		"one projects walk for the workspace: an answer with no project ownership is an access change before it is a removal", "cursor walk"},
	"linear_team_key_ownership": {"internal/providersync.LinearTeamKeyOwnershipKind", "EmptyClosesNothing",
		"one teams walk for the workspace: an answer with no team is an access change before it is a removal", "cursor walk"},
	"jira_legacy_ownership": {"internal/providersync.JiraLegacyOwnershipKind", "EmptyClosesNothing",
		"one project search for the site: no live project is far more often an access change than a removal", "one response or direct answer"},
	"gitlab_group_project_grants": {"internal/providersync.GitLabGroupProjectGrantKind", "EmptyIsAnAnswer",
		"one listing per group, each with its own proven end, in a scope no other integration lists: a group with no project is an answer", "one response or direct answer"},
	"github_team_repo_grants": {"internal/providersync.GitHubTeamRepoGrantKind", "EmptyIsAnAnswer",
		"one listing per team, each with its own proven end, in a scope no other integration lists: a team with no repository is an answer", "one response or direct answer"},
	"atlassian_team_project_links": {"internal/providersync.AtlassianTeamLinkKind", "EmptyIsAnAnswer",
		"one link read per team, each to its end: a team with no link is an answer; a team outside the search answer is in scope " +
			"only through atlassian_team_catalog", "cursor walk"},
	"atlassian_team_memberships": {"internal/providersync.AtlassianTeamMembershipKind", "EmptyIsAnAnswer",
		"one member read per team, each to its end: a team with no member is an answer; a team outside the search answer is in " +
			"scope only through atlassian_team_catalog", "cursor walk"},
	"atlassian_team_catalog": {"internal/providersync.AtlassianTeamCatalogKind", "EmptyClosesNothing",
		"one team search for the organization: a search that answers no team is an access change before every team was deleted", "cursor walk"},
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
	for _, planner := range census.planners {
		if !census.closes[planner] {
			t.Errorf("%s calls the snapshot rule and is not a close site", planner)
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
		want[name] = snapshotKindScope + "; " + policy + "; " + entry.absence
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
				got[match[1]] = match[2] + "; " + match[3] + "; " + match[4]
			}
		}
		if len(got) == 0 {
			t.Fatalf("%s holds no row of the policy table: the test measured nothing", path)
		}
		return got
	}
	code := read("snapshot_kinds.go", regexp.MustCompile(`^//\t([a-z_]+)\s+(sole integration)\s+(closes nothing|is an answer)\s+(cursor walk|one response or direct answer)$`))
	if !reflect.DeepEqual(code, want) {
		t.Errorf("the policy table in the doc comment of snapshot_kinds.go differs from the census.\n got  %v\n want %v", code, want)
	}
	document := read("../../docs/contribute/architecture/team-attribution.md",
		regexp.MustCompile("^\\s*\\| `([a-z_]+)` \\|[^|]*\\|[^|]*\\| (sole integration) \\| (closes nothing|is an answer)\\b[^|]*\\| (cursor walk|one response or direct answer) \\|$"))
	if !reflect.DeepEqual(document, want) {
		t.Errorf("the kinds table of docs/contribute/architecture/team-attribution.md differs from the census.\n got  %v\n want %v", document, want)
	}
}

// snapshotAbsenceCalls is every production file that states the proof of an
// absence for a kind snapshot, and how often it makes each statement. The
// kinds behind each call:
//
//   - linear_team_catalog_collector.go: linear_project_ownership and
//     linear_team_key_ownership, both by the cursor walk.
//   - internal/atlassianteams/write.go: atlassian_team_memberships,
//     atlassian_team_project_links and atlassian_team_catalog, by the cursor walk.
//   - ownership_close_gate.go: github_team_repo_grants and
//     gitlab_group_project_grants (one call, ownershipCloseDecision.snapshot),
//     by one response or the direct answer.
//   - jira_team_catalog_route.go: jira_legacy_ownership, by one response (or a
//     project the search holds) or the direct answer.
//
// A new call, or a kind that moves from one statement to the other, is a
// deliberate edit of this table, of snapshotKindCensus and of the two
// documented tables.
var snapshotAbsenceCalls = map[string]map[string]int{
	"internal/providersync/linear_team_catalog_collector.go": {"AbsenceByWalk": 2},
	"internal/atlassianteams/write.go":                       {"AbsenceByWalk": 3},
	"internal/providersync/ownership_close_gate.go":          {"AbsenceByListing": 1},
	"internal/providersync/jira_team_catalog_route.go":       {"AbsenceByListing": 1},
}

// TestAbsenceProofCensus pins who states the proof of an absence, and that
// the only walk taken as that proof is the cursor walk. It reads the source of
// the two packages that hold a close site.
func TestAbsenceProofCensus(t *testing.T) {
	call := regexp.MustCompile(`\b(?:providersync\.)?(AbsenceByWalk|AbsenceByListing)(?:\[[^\]]+\])?\(`)
	walk := regexp.MustCompile(`AbsenceByWalk(?:\[[^\]]+\])?\((?:providersync\.)?(\w+)\)`)
	got := map[string]map[string]int{}
	files := 0
	for _, directory := range []string{".", "../atlassianteams"} {
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatalf("read %s: %v", directory, err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == "ownership_snapshot.go" {
				continue
			}
			raw, err := os.ReadFile(directory + "/" + name)
			if err != nil {
				t.Fatal(err)
			}
			files++
			path := "internal/providersync/" + name
			if directory != "." {
				path = "internal/atlassianteams/" + name
			}
			var code []string
			for _, line := range strings.Split(string(raw), "\n") {
				if !strings.HasPrefix(strings.TrimSpace(line), "//") {
					code = append(code, line)
				}
			}
			source := strings.Join(code, "\n")
			for _, match := range call.FindAllStringSubmatch(source, -1) {
				if got[path] == nil {
					got[path] = map[string]int{}
				}
				got[path][match[1]]++
			}
			for _, match := range walk.FindAllStringSubmatch(source, -1) {
				if match[1] != "AbsenceWalkByCursor" {
					t.Errorf("%s takes the walk %s as the proof of an absence: the only named walk is AbsenceWalkByCursor", path, match[1])
				}
			}
		}
	}
	if files < 20 {
		t.Fatalf("the census read %d source file(s): it measured nothing", files)
	}
	if !reflect.DeepEqual(got, snapshotAbsenceCalls) {
		t.Errorf("the statements of an absence proof changed.\n got  %v\n want %v", got, snapshotAbsenceCalls)
	}
	// The table of the kinds and the calls agree: five kinds by the cursor
	// walk (five calls), three by the listing rule (two calls, one of them the
	// gate's, for the two grant kinds).
	byAbsence := map[string]int{}
	for _, entry := range snapshotKindCensus {
		byAbsence[entry.absence]++
	}
	if byAbsence["cursor walk"] != 5 || byAbsence["one response or direct answer"] != 3 || len(byAbsence) != 2 {
		t.Errorf("the kinds by absence proof are %v, want 5 by the cursor walk and 3 by one response or the direct answer", byAbsence)
	}
}

// heldSetWalk is one list walk of a catalog collector: what it reads, which
// kind's HELD SET its answer feeds (the facts a run takes as still there), and
// what proves an absence for that kind.
type heldSetWalk struct{ reads, feeds, proof string }

// heldSetWalks is EVERY list walk of the five catalog collectors, in source
// order, by the call that makes it. The proof of an absence belongs to the
// held set, so to every walk that feeds it: a kind closed by "one response or
// direct answer" takes its walks as proof only when each of them was one
// response, and its direct answer asks for every state those walks admit.
//
// A new walk in one of these files changes the count and fails
// TestHeldSetWalkCensus until it is named here with the held set it feeds and
// its proof. Per kind:
//
//   - github_team_repo_grants: ONE walk per team (the team's repositories;
//     GitHub lists archived repositories in the same walk).
//   - gitlab_group_project_grants: ONE walk per group (the group's projects,
//     with the provider's default filters; the direct answer uses the same
//     endpoint and filters). The walk of all projects with subgroups feeds the
//     catalog's project rows, not a grant.
//   - jira_legacy_ownership: THREE walks (live, archived, live again); the
//     archived one feeds the held set because an archived project keeps its
//     open rows. A row whose project the live answer holds is decided by the
//     legacy links, one read of the store.
//   - linear_team_key_ownership: ONE walk (teams), by cursor.
//   - linear_project_ownership: TWO walks by cursor (projects, with archived
//     projects in the same walk; and the continuation of one project's teams).
//   - atlassian_team_catalog, atlassian_team_memberships,
//     atlassian_team_project_links: ONE walk each, by cursor (the team search;
//     one member read per team; one link read per team).
var heldSetWalks = map[string]struct {
	call  string
	walks []heldSetWalk
}{
	"internal/providersync/github_team_catalog_route.go": {`providerfoundation\.CollectGitHubLinkPages\(`, []heldSetWalk{
		{"the teams of the organization", "no held set: a team that is not listed closes nothing", "none needed"},
		{"the repositories of one team", "github_team_repo_grants", "one response, or GitHub's answer for the grant"},
		{"the members of one team", "no close", "none needed"},
	}},
	"internal/providersync/gitlab_team_catalog_route.go": {`providerfoundation\.CollectGitLabPageParamPages\(`, []heldSetWalk{
		{"the subgroups of the root group", "no held set: a group that is not listed closes nothing", "none needed"},
		{"the projects of one group", "gitlab_group_project_grants", "one response, or GitLab's answer for the project"},
		{"the members of one group", "no close", "none needed"},
		{"all projects with subgroups", "no held set of a grant: the catalog's project rows", "none needed"},
	}},
	"internal/providersync/jira_team_catalog_route.go": {`:= jiraTeamCatalogSearchProjects\(`, []heldSetWalk{
		{"the live project search", "jira_legacy_ownership", "every walk one response, or Jira's answer for live and archived"},
		{"the archived project search", "jira_legacy_ownership (an archived project keeps its open rows)", "the same"},
		{"the live project search, again", "jira_legacy_ownership", "the same"},
	}},
	"internal/providersync/linear_reference_catalog_route.go": {`:= collectLinearReferenceConnection\(`, []heldSetWalk{
		{"the teams of the workspace", "linear_team_key_ownership", "cursor walk"},
		{"the projects of the workspace, archived ones too", "linear_project_ownership", "cursor walk"},
		{"the continuation of one project's teams", "linear_project_ownership", "cursor walk"},
		{"the continuation of one team's members", "no close", "none needed"},
	}},
	"internal/atlassianteams/collect.go": {`client\.(SearchTeams|IterTeamUsers|IterTeamConnectedContainers)\(`, []heldSetWalk{
		{"the team search", "atlassian_team_catalog", "cursor walk"},
		{"the members of one team", "atlassian_team_memberships", "cursor walk"},
		{"the project links of one team", "atlassian_team_project_links", "cursor walk"},
	}},
}

// TestHeldSetWalkCensus pins every list walk of the catalog collectors and
// the held set each feeds. It also holds the Jira walks of the code to the
// table: the three project searches are the three walks the close names.
func TestHeldSetWalkCensus(t *testing.T) {
	read := func(path string) string {
		t.Helper()
		raw, err := os.ReadFile("../../" + path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var code []string
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "//") {
				code = append(code, line)
			}
		}
		return strings.Join(code, "\n")
	}
	feeds := map[string]int{}
	for path, entry := range heldSetWalks {
		got := len(regexp.MustCompile(entry.call).FindAllString(read(path), -1))
		if got != len(entry.walks) {
			t.Errorf("%s makes %d list walk(s) by %s, the census names %d: a walk is named with the held set it feeds and its proof",
				path, got, entry.call, len(entry.walks))
		}
		for _, walk := range entry.walks {
			if strings.TrimSpace(walk.reads) == "" || strings.TrimSpace(walk.feeds) == "" || strings.TrimSpace(walk.proof) == "" {
				t.Errorf("%s: a walk of the census has no text: %+v", path, walk)
			}
			for kind := range snapshotKindCensus {
				if strings.HasPrefix(walk.feeds, kind) {
					feeds[kind]++
				}
			}
		}
	}
	// Every kind has a walk that feeds it, and the count per kind is the one
	// the comment above states.
	want := map[string]int{
		"github_team_repo_grants": 1, "gitlab_group_project_grants": 1, "jira_legacy_ownership": 3,
		"linear_team_key_ownership": 1, "linear_project_ownership": 2,
		"atlassian_team_catalog": 1, "atlassian_team_memberships": 1, "atlassian_team_project_links": 1,
	}
	if !reflect.DeepEqual(feeds, want) {
		t.Errorf("the walks that feed each kind's held set are %v, want %v", feeds, want)
	}
	// The Jira close names its walks in code: as many as the searches.
	jira := read("internal/providersync/jira_team_catalog_route.go")
	literal := regexp.MustCompile(`(?s)projectSearchWalks := \[\]ListWalk\{(.*?)\n\t\}`).FindStringSubmatch(jira)
	if literal == nil {
		t.Fatal("the Jira route does not name its project search walks")
	}
	if named := strings.Count(literal[1], "Responses:"); named != len(heldSetWalks["internal/providersync/jira_team_catalog_route.go"].walks) {
		t.Errorf("the Jira close names %d project search walk(s), the route makes %d: every search feeds the held set",
			named, len(heldSetWalks["internal/providersync/jira_team_catalog_route.go"].walks))
	}
	// The Jira answer asks for every state those walks admit.
	if answer := read("internal/providersync/ownership_absence.go"); !strings.Contains(answer, `[]string{"live", jiraTeamCatalogProjectStatusArchived}`) {
		t.Error("the Jira direct answer does not ask for live AND archived projects: the held set admits both")
	}
}

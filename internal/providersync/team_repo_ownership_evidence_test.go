package providersync

import (
	"reflect"
	"sort"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

func evidenceInputs(keys map[teamRepoOwnershipEvidenceKey]struct{}) []string {
	var out []string
	for key := range keys {
		out = append(out, key.Input+" "+key.Key)
	}
	sort.Strings(out)
	return out
}

// Each path records the rows of the chain that gave the link: the pull
// request and its own project link; the issue link row, the issue and its
// link; the pull request, the chosen donor edge (raw target), the donor and
// the donor's team row. A refused donor records nothing.
func TestDerivationRecordsTheEvidenceOfTheChosenChain(t *testing.T) {
	repo := uuid.NewString()
	links := []TeamRepoOwnershipProjectLink{
		{Provider: "jira", ProjectID: "P", TeamID: "team-p", IsPrimary: true},
		{Provider: "github", ProjectID: "G", TeamID: "team-g", IsPrimary: true},
	}
	known := []TeamRepoOwnershipKnownTeam{{Provider: "linear", ID: "linear:ENG", NativeTeamKey: "ENG"}}
	for _, tc := range []struct {
		name  string
		items []TeamRepoOwnershipWorkItem
		edges []TeamRepoOwnershipDependencyEdge
		prs   []TeamRepoOwnershipIssuePRLink
		team  string
		want  []string
	}{
		{name: "own project",
			items: []TeamRepoOwnershipWorkItem{{WorkItemID: "ghpr:a/r#1", Provider: "github", Type: "pr", RepoID: repo, ProjectID: "G"}},
			team:  "team-g",
			want:  []string{`project_link ["github","G","team-g"]`, `work_item ["` + repo + `","ghpr:a/r#1"]`}},
		{name: "issue link",
			items: []TeamRepoOwnershipWorkItem{{WorkItemID: "jira:P-1", Provider: "jira", Type: "issue", ProjectID: "P"}},
			prs:   []TeamRepoOwnershipIssuePRLink{{WorkItemID: "jira:P-1", RepoID: repo, PRNumber: 4, Provenance: "native"}},
			team:  "team-p",
			want: []string{`issue_pr ["` + repo + `","jira:P-1","4"]`, `project_link ["jira","P","team-p"]`,
				`work_item ["","jira:P-1"]`}},
		{name: "extkey donor of the linear arm",
			items: []TeamRepoOwnershipWorkItem{
				{WorkItemID: "ghpr:a/r#1", Provider: "github", Type: "pr", RepoID: repo},
				{WorkItemID: "linear:ENG-7", Provider: "linear", Type: "issue", NativeTeamKey: "ENG"},
			},
			edges: []TeamRepoOwnershipDependencyEdge{{SourceWorkItemID: "ghpr:a/r#1", TargetWorkItemID: "extkey:eng-7", RelationshipType: "external_issue_key"}},
			team:  "linear:ENG",
			want: []string{`dependency ["ghpr:a/r#1","extkey:eng-7","external_issue_key"]`, `linear_team ["linear:ENG"]`,
				`work_item ["","linear:ENG-7"]`, `work_item ["` + repo + `","ghpr:a/r#1"]`}},
		{name: "a refused donor with the smaller id records nothing",
			items: []TeamRepoOwnershipWorkItem{
				{WorkItemID: "ghpr:a/r#1", Provider: "github", Type: "pr", RepoID: repo},
				{WorkItemID: "gh:a/a#2", Provider: "github", Type: "pr", ProjectID: "G"},
				{WorkItemID: "jira:P-1", Provider: "jira", Type: "issue", ProjectID: "P"},
			},
			edges: []TeamRepoOwnershipDependencyEdge{
				{SourceWorkItemID: "ghpr:a/r#1", TargetWorkItemID: "gh:a/a#2", RelationshipType: "relates_to"},
				{SourceWorkItemID: "ghpr:a/r#1", TargetWorkItemID: "jira:P-1", RelationshipType: "relates_to"},
			},
			team: "team-p",
			want: []string{`dependency ["ghpr:a/r#1","jira:P-1","relates_to"]`, `project_link ["jira","P","team-p"]`,
				`work_item ["","jira:P-1"]`, `work_item ["` + repo + `","ghpr:a/r#1"]`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			derivation := deriveTeamRepoOwnership("org", links, tc.items, tc.edges, tc.prs, known)
			if len(derivation.Evidence) != 1 {
				t.Fatalf("evidence of %d claims, want 1: %v", len(derivation.Evidence), derivation.Evidence)
			}
			got := evidenceInputs(derivation.Evidence[teamRepoOwnershipClaimRef{TeamID: tc.team, RepoID: repo}])
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("evidence\n got  %v\n want %v", got, tc.want)
			}
		})
	}
}

// A remapped GitLab link records the project_id it is stored under.
func TestProjectLinkEvidenceUsesTheStoredProjectID(t *testing.T) {
	links := []TeamRepoOwnershipProjectLink{{Provider: "gitlab", ProjectID: "org:gitlab:42", StoredProjectID: "group/app", TeamID: "team-a"}}
	evidence := teamRepoOwnershipTeamEvidence(links, resolveProjectToTeam(links))
	got := evidence(TeamRepoOwnershipWorkItem{Provider: "gitlab", ProjectID: "org:gitlab:42"}, "team-a", TeamRepoOwnershipResolutionArmProjectID)
	if want := []teamRepoOwnershipEvidenceKey{teamRepoOwnershipProjectLinkEvidence("gitlab", "group/app", "team-a")}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestEvidenceAbsenceProof(t *testing.T) {
	repo := uuid.New()
	claim := InferredRepoOwnershipClaim{Provider: "github", RepoFullName: "a/r", TeamID: "team-a"}
	item := TeamRepoOwnershipWorkItem{WorkItemID: "jira:A-1"}
	fallback := TeamRepoOwnershipIssuePRLink{RepoID: repo.String(), WorkItemID: "jira:A-1", PRNumber: 5}
	recorded := map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}{
		claim: {teamRepoOwnershipWorkItemEvidence(item): {}, teamRepoOwnershipIssuePREvidence(fallback): {}},
	}
	row := teamRepoOwnershipActiveRow{Provider: "github", RepoFullName: "a/r", TeamID: "team-a"}
	for _, tc := range []struct {
		name    string
		items   []TeamRepoOwnershipWorkItem
		links   []TeamRepoOwnershipIssuePRLink
		proven  bool
		reasons []string
	}{
		{"every row held (any value)", []TeamRepoOwnershipWorkItem{{WorkItemID: "jira:A-1", ProjectID: "changed"}}, []TeamRepoOwnershipIssuePRLink{fallback}, true, nil},
		{"missing item", nil, []TeamRepoOwnershipIssuePRLink{fallback}, false, []string{"evidence_row_missing_work_item"}},
		{"missing link", []TeamRepoOwnershipWorkItem{item}, nil, false, []string{"evidence_row_missing_issue_pr"}},
		{"link removed beside a native link of its pull request", []TeamRepoOwnershipWorkItem{item},
			[]TeamRepoOwnershipIssuePRLink{{RepoID: repo.String(), WorkItemID: "jira:N-1", PRNumber: 5, Provenance: "native"}}, true, nil},
		{"link removed beside a native link of its issue", []TeamRepoOwnershipWorkItem{item},
			[]TeamRepoOwnershipIssuePRLink{{RepoID: uuid.NewString(), WorkItemID: "jira:A-1", PRNumber: 9, Provenance: "native"}}, true, nil},
		{"link removed beside a fallback link only", []TeamRepoOwnershipWorkItem{item},
			[]TeamRepoOwnershipIssuePRLink{{RepoID: repo.String(), WorkItemID: "jira:N-1", PRNumber: 5, Provenance: "heuristic"}}, false,
			[]string{"evidence_row_missing_issue_pr"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := newTeamRepoOwnershipEvidence(recorded, newTeamRepoOwnershipEvidenceHeld(tc.items, nil, tc.links, nil, nil))
			if evidence.proves(claim) != tc.proven {
				t.Fatalf("proves = %v, want %v", !tc.proven, tc.proven)
			}
			kept, held, kinds := evidence.retain([]teamRepoOwnershipActiveRow{row})
			if tc.proven != (len(kept) == 1 && held == 0) {
				t.Fatalf("retain kept %d held %d", len(kept), held)
			}
			if !tc.proven && (len(kinds) != 1 || kinds[0].Kind != "input_evidence" || !reflect.DeepEqual(kinds[0].Reasons, tc.reasons)) {
				t.Fatalf("held kinds = %+v, want reasons %v", kinds, tc.reasons)
			}
		})
	}
	// No recorded key: the kind guard alone decides, and the close is counted.
	bare := newTeamRepoOwnershipEvidence(nil, newTeamRepoOwnershipEvidenceHeld(nil, nil, nil, nil, nil))
	if !bare.proves(claim) || bare.withoutRecord([]teamRepoOwnershipActiveRow{row}) != 1 {
		t.Fatal("a claim with no recorded key is not proven by the evidence or not counted")
	}
	if AbsenceByInputEvidence[InferredRepoOwnershipClaim](nil).of(claim) != SnapshotAbsenceNotProven {
		t.Fatal("a nil missing proves an absence")
	}
}

// Upkeep: new chain keys are written; a held key no chain uses is retired; a
// missing key stays; a closed claim retires every key.
func TestEvidenceUpkeep(t *testing.T) {
	claim := InferredRepoOwnershipClaim{Provider: "github", RepoFullName: "a/r", TeamID: "team-a"}
	closed := InferredRepoOwnershipClaim{Provider: "github", RepoFullName: "a/x", TeamID: "team-a"}
	kept := teamRepoOwnershipWorkItemEvidence(TeamRepoOwnershipWorkItem{WorkItemID: "jira:A-1"})
	unused := teamRepoOwnershipWorkItemEvidence(TeamRepoOwnershipWorkItem{WorkItemID: "jira:A-2"})
	missing := teamRepoOwnershipWorkItemEvidence(TeamRepoOwnershipWorkItem{WorkItemID: "jira:A-3"})
	fresh := teamRepoOwnershipWorkItemEvidence(TeamRepoOwnershipWorkItem{WorkItemID: "jira:A-4"})
	recorded := map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}{
		claim:  {kept: {}, unused: {}, missing: {}},
		closed: {kept: {}},
	}
	held := newTeamRepoOwnershipEvidenceHeld([]TeamRepoOwnershipWorkItem{{WorkItemID: "jira:A-1"}, {WorkItemID: "jira:A-2"}, {WorkItemID: "jira:A-4"}}, nil, nil, nil, nil)
	writes := newTeamRepoOwnershipEvidence(recorded, held).upkeep(
		map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}{claim: {kept: {}, fresh: {}}, closed: {kept: {}}},
		map[InferredRepoOwnershipClaim]struct{}{closed: {}})
	want := []teamRepoOwnershipEvidenceWrite{
		{claim: claim, key: unused, deleted: true},
		{claim: claim, key: fresh},
		{claim: closed, key: kept, deleted: true},
	}
	sort.Slice(want, func(i, j int) bool {
		if want[i].claim.RepoFullName != want[j].claim.RepoFullName {
			return want[i].claim.RepoFullName < want[j].claim.RepoFullName
		}
		return want[i].key.Key < want[j].key.Key
	})
	if !reflect.DeepEqual(writes, want) {
		t.Fatalf("upkeep\n got  %+v\n want %+v", writes, want)
	}
}

// The witness rule restated in providersync agrees with the native-first rule
// of issueprlinks (Outranks and IsPullRequestWorkItem).
func TestEvidenceWitnessAgreesWithIssuePRLinks(t *testing.T) {
	repo, other := uuid.New(), uuid.New()
	stored := []TeamRepoOwnershipIssuePRLink{
		{RepoID: repo.String(), WorkItemID: "jira:N-1", PRNumber: 5, Provenance: "native"},
		{RepoID: other.String(), WorkItemID: "jira:N-2", PRNumber: 7, Provenance: "heuristic"},
	}
	links := make([]issueprlinks.Link, 0, len(stored))
	for _, link := range stored {
		links = append(links, issueprlinks.Link{RepoID: uuid.MustParse(link.RepoID), WorkItemID: link.WorkItemID, PRNumber: link.PRNumber, Provenance: link.Provenance})
	}
	native := issueprlinks.NewNativeLinks(links)
	held := newTeamRepoOwnershipEvidenceHeld(nil, nil, stored, nil, nil)
	for _, cell := range []struct {
		repo   uuid.UUID
		number uint32
		item   string
	}{
		{repo, 5, "jira:X-1"}, {repo, 6, "jira:N-1"}, {repo, 6, "jira:X-1"}, {other, 7, "jira:X-2"}, {other, 5, "jira:X-3"},
		{repo, 6, "ghpr:acme/app#12"}, {repo, 6, "gitlab:group/app!3"}, {repo, 6, "ghpr:acme/app#0"}, {repo, 6, "ghpr:#4"},
		{repo, 6, "gitlab:group/app!x"}, {repo, 6, "gh:acme/app#12"}, {repo, 6, "ghpr:acme/app#4294967296"},
	} {
		want := native.Outranks(cell.repo, cell.number, cell.item) || issueprlinks.IsPullRequestWorkItem(cell.item)
		missing := teamRepoOwnershipIssuePREvidence(TeamRepoOwnershipIssuePRLink{RepoID: cell.repo.String(), WorkItemID: cell.item, PRNumber: cell.number})
		if got := held.holds(missing); got != want {
			t.Errorf("%v: providersync witness %v, issueprlinks %v", cell, got, want)
		}
	}
}

package issueprlinks

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestParsePRSource(t *testing.T) {
	repoID := "owner/repo"
	cases := []struct {
		name  string
		value string
		want  PRSource
		ok    bool
	}{
		{"github", "ghpr:" + repoID + "#12", PRSource{RepoSlug: repoID, PRNumber: 12, Provider: ProviderGitHub}, true},
		{"gitlab", "gitlab:group/proj!7", PRSource{RepoSlug: "group/proj", PRNumber: 7, Provider: ProviderGitLab}, true},
		{"gitlab nested group", "gitlab:a/b/c!3", PRSource{RepoSlug: "a/b/c", PRNumber: 3, Provider: ProviderGitLab}, true},

		// Python rsplit(sep, 1) takes the LAST separator, so a slug containing
		// the separator keeps it. A left-split port would silently mint links
		// against a truncated repo slug.
		{"last separator wins", "ghpr:a/b#c#12", PRSource{RepoSlug: "a/b#c", PRNumber: 12, Provider: ProviderGitHub}, true},

		{"tracker id is not a PR", "linear:CHAOS-1", PRSource{}, false},
		{"github issue is not a PR", "gh:owner/repo#12", PRSource{}, false},
		{"jira id is not a PR", "jira:ABC-1", PRSource{}, false},
		{"no separator", "ghpr:owner/repo", PRSource{}, false},
		{"wrong separator for provider", "ghpr:owner/repo!12", PRSource{}, false},
		{"empty slug", "ghpr:#12", PRSource{}, false},
		{"empty number", "ghpr:owner/repo#", PRSource{}, false},
		{"non-numeric", "ghpr:owner/repo#abc", PRSource{}, false},
		{"zero is not a PR number", "ghpr:owner/repo#0", PRSource{}, false},
		{"negative", "ghpr:owner/repo#-1", PRSource{}, false},
		{"signed positive", "ghpr:owner/repo#+1", PRSource{}, false},
		{"empty", "", PRSource{}, false},

		// DOCUMENTED DIVERGENCE from Python, see ParsePRSource's doc comment.
		// Python's str.isdigit() accepts both of these; int() then converts the
		// Arabic-Indic form to 3 and RAISES on the superscript, so Python today
		// would mint a link for the first and crash on the second. Go rejects
		// both. Unreachable on real data -- every ghpr:/gitlab: id in
		// work_item_dependencies is minted by Go providersync from an ASCII
		// integer -- and asserted here so the divergence is a decision on the
		// record rather than a surprise.
		{"arabic-indic digits rejected", "ghpr:owner/repo#٣", PRSource{}, false},
		{"superscript digits rejected", "ghpr:owner/repo#²", PRSource{}, false},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, ok := ParsePRSource(testCase.value)
			if ok != testCase.ok {
				t.Fatalf("ParsePRSource(%q) ok = %v, want %v", testCase.value, ok, testCase.ok)
			}
			if got != testCase.want {
				t.Fatalf("ParsePRSource(%q) = %+v, want %+v", testCase.value, got, testCase.want)
			}
		})
	}
}

const (
	testOrg  = "org-1"
	testSlug = "owner/repo"
)

var (
	testRepoID  = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	testSynced  = time.Date(2026, 8, 30, 10, 0, 0, 0, time.UTC)
	testLinear  = "linear:CHAOS-1"
	testGHPRRef = "ghpr:" + testSlug + "#12"
)

// baseInputs is one admissible dependency with every lookup satisfied, so each
// test below can knock out exactly one gate and see exactly one rejection.
func baseInputs() Inputs {
	return Inputs{
		OrgID: testOrg,
		Dependencies: []DependencyRow{{
			OrgID:               testOrg,
			SourceWorkItemID:    testGHPRRef,
			TargetWorkItemID:    testLinear,
			RelationshipTypeRaw: "linear_attachment",
			LastSynced:          testSynced,
		}},
		Repos:        []RepoRow{{OrgID: testOrg, ID: testRepoID, Repo: testSlug}},
		PullRequests: []PullRequestRow{{OrgID: testOrg, RepoID: testRepoID, Number: 12}},
		WorkItems:    []WorkItemRow{{OrgID: testOrg, WorkItemID: testLinear}},
	}
}

func assertSingleRejection(t *testing.T, result Result, reason RejectionReason) {
	t.Helper()
	if got := result.Written(); got != 0 {
		t.Fatalf("wrote %d links, want 0", got)
	}
	if got := result.Rejected[reason]; got != 1 {
		t.Fatalf("rejected[%s] = %d, want 1 (full breakdown %v)", reason, got, result.Rejected)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

func TestDeriveWritesTheAdmissibleRow(t *testing.T) {
	result := Derive(baseInputs())
	if result.Written() != 1 {
		t.Fatalf("wrote %d links, want 1 (rejections %v)", result.Written(), result.Rejected)
	}
	link := result.Links[0]
	want := Link{
		OrgID:      testOrg,
		RepoID:     testRepoID,
		WorkItemID: testLinear,
		PRNumber:   12,
		Confidence: NativeConfidence,
		Provenance: ProvenanceNative,
		Evidence:   "linear_attachment",
		LastSynced: testSynced,
	}
	if link != want {
		t.Fatalf("link = %+v, want %+v", link, want)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

// TestDeriveRejectsUnknownWorkItem covers the gate the real-data golden cannot
// exercise (see the falsification suite's recorded limit).
func TestDeriveRejectsUnknownWorkItem(t *testing.T) {
	inputs := baseInputs()
	inputs.WorkItems = nil
	assertSingleRejection(t, Derive(inputs), ReasonUnknownWorkItem)
}

func TestDeriveRejectsUnknownRepo(t *testing.T) {
	inputs := baseInputs()
	inputs.Repos = nil
	assertSingleRejection(t, Derive(inputs), ReasonUnknownRepo)
}

func TestDeriveRejectsMissingPullRequest(t *testing.T) {
	inputs := baseInputs()
	inputs.PullRequests = nil
	assertSingleRejection(t, Derive(inputs), ReasonPRNotFound)
}

func TestDeriveRejectsUnparseableSource(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].SourceWorkItemID = "linear:CHAOS-2"
	assertSingleRejection(t, Derive(inputs), ReasonUnparseableSource)
}

func TestDeriveRejectsWrongRawKind(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].RelationshipTypeRaw = "linear_relation:related"
	assertSingleRejection(t, Derive(inputs), ReasonNotAdmissible)
}

// TestDeriveRejectsRawKindPointingAtTheWrongIDSpace is the reason Admission
// pairs a raw kind WITH a target prefix instead of matching the kind alone: a
// `linear_attachment` row whose target is a GitHub issue is malformed, and
// admitting it would mint a link into the wrong id space.
func TestDeriveRejectsRawKindPointingAtTheWrongIDSpace(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].TargetWorkItemID = "gh:owner/repo#5"
	inputs.WorkItems = []WorkItemRow{{OrgID: testOrg, WorkItemID: "gh:owner/repo#5"}}
	assertSingleRejection(t, Derive(inputs), ReasonNotAdmissible)
}

// TestDeriveRejectsGithubClosingReferenceRawKindPointingAtTheWrongIDSpace is
// the github_closing_reference counterpart to the linear_attachment case
// above (codex round 1 on #2174, P3: the wrong-id-space test existed for
// linear_attachment only). A github_closing_reference row whose target is a
// Linear id is malformed the same way, in the other direction.
func TestDeriveRejectsGithubClosingReferenceRawKindPointingAtTheWrongIDSpace(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].RelationshipTypeRaw = "github_closing_reference"
	inputs.Dependencies[0].TargetWorkItemID = testLinear
	assertSingleRejection(t, Derive(inputs), ReasonNotAdmissible)
}

// TestDeriveIsFirstWinsOnDuplicateIdentity pins Python's first-wins behaviour
// (builder.py:764-767). Two dependency rows reaching the same
// (work_item, repo, pr) identity must produce ONE row, and it must be the
// first one -- the second's evidence and timestamp are discarded.
func TestDeriveIsFirstWinsOnDuplicateIdentity(t *testing.T) {
	inputs := baseInputs()
	later := inputs.Dependencies[0]
	later.LastSynced = testSynced.Add(time.Hour)
	later.SourceWorkItemID = "gitlab:" + testSlug + "!12"
	inputs.Dependencies = append(inputs.Dependencies, later)
	// The GitLab source resolves to the same repo and PR number, so both rows
	// land on one identity.
	inputs.Repos = append(inputs.Repos, RepoRow{OrgID: testOrg, ID: testRepoID, Repo: testSlug})

	result := Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf("wrote %d links, want 1", result.Written())
	}
	if !result.Links[0].LastSynced.Equal(testSynced) {
		t.Fatalf("kept last_synced %s, want the FIRST row's %s", result.Links[0].LastSynced, testSynced)
	}
	if got := result.Rejected[ReasonDuplicateIdentity]; got != 1 {
		t.Fatalf("rejected[duplicate_identity] = %d, want 1", got)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

// TestDeriveStampsTheBuildOrgOnEveryRow pins builder.py:205 -- the written
// org_id is the BUILD's org, not the dependency row's. The two can only differ
// for a row whose own org_id is empty, which the org-filtered read cannot
// return; the assertion keeps that irrelevance proven rather than assumed.
func TestDeriveStampsTheBuildOrgOnEveryRow(t *testing.T) {
	inputs := baseInputs()
	inputs.Dependencies[0].OrgID = ""
	inputs.Repos[0].OrgID = ""
	inputs.PullRequests[0].OrgID = ""
	inputs.WorkItems[0].OrgID = ""

	result := Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf("wrote %d links, want 1 (rejections %v)", result.Written(), result.Rejected)
	}
	if result.Links[0].OrgID != testOrg {
		t.Fatalf("org_id = %q, want the build org %q", result.Links[0].OrgID, testOrg)
	}
}

// TestDeriveIgnoresRowsFromAnotherOrg guards the lookup keys' org component. A
// repo or PR belonging to a different org must never satisfy this org's gates,
// even when the slug and number match -- the same cross-org confusion
// team_repo_ownership_derivation_integration_test.go:750-780 guards for its own
// read of this table.
func TestDeriveIgnoresRowsFromAnotherOrg(t *testing.T) {
	inputs := baseInputs()
	inputs.Repos = []RepoRow{{OrgID: "other-org", ID: testRepoID, Repo: testSlug}}
	assertSingleRejection(t, Derive(inputs), ReasonUnknownRepo)
}

func TestDeriveOnEmptyInput(t *testing.T) {
	result := Derive(Inputs{OrgID: testOrg})
	if result.Written() != 0 || result.DependenciesRead != 0 {
		t.Fatalf("unexpected result %+v", result)
	}
	if !result.Balanced() {
		t.Fatalf("empty accounting does not balance: %+v", result)
	}
}

// TestGoldenRejectionBreakdown documents which gates org 70d529e0's real data
// actually exercises. It is the denominator for "we wrote N rows": without it,
// a gate that started rejecting everything would still look healthy.
//
// The numbers are the frozen inputs' own, not the live table's -- see
// TestDeriveMatchesFrozenPythonGoldenExhaustively on why those differ.
func TestGoldenRejectionBreakdown(t *testing.T) {
	golden := loadGolden(t)
	result := Derive(golden.inputs(t))

	expected := map[RejectionReason]int{
		ReasonNotAdmissible:     3548,
		ReasonUnknownRepo:       298,
		ReasonPRNotFound:        26,
		ReasonUnparseableSource: 0,
		ReasonEmptyTarget:       0,
		ReasonUnknownWorkItem:   0,
		ReasonDuplicateIdentity: 0,
	}
	for _, reason := range AllRejectionReasons {
		if got := result.Rejected[reason]; got != expected[reason] {
			t.Errorf("rejected[%s] = %d, want %d", reason, got, expected[reason])
		}
	}
	if got, want := result.Written(), 2493; got != want {
		t.Errorf("wrote %d links, want %d", got, want)
	}
	if got, want := result.AdmittedByRawKind["linear_attachment"], 2817; got != want {
		t.Errorf("admitted %d linear_attachment rows, want %d", got, want)
	}
	// The admitted-to-written gap must be fully explained by the two gates that
	// fire downstream of admission. This is the identity the live proof repeats
	// against ClickHouse.
	admitted := result.AdmittedByRawKind["linear_attachment"]
	gap := admitted - result.Written()
	if want := result.Rejected[ReasonUnknownRepo] + result.Rejected[ReasonPRNotFound] +
		result.Rejected[ReasonUnknownWorkItem] + result.Rejected[ReasonEmptyTarget] +
		result.Rejected[ReasonDuplicateIdentity]; gap != want {
		t.Errorf("admitted-to-written gap %d is not explained by the post-admission gates (%d)", gap, want)
	}
}

// TestAdmittedCounterCountsRowsThatPassedAdmission is codex round-1 finding F2.
//
// `AdmittedByRawKind` is documented as counting rows that passed the ADMISSION
// TABLE, and it is the signal that lane-4757's GitHub/Jira slices have started
// producing rows at all. Incrementing it only after ParsePRSource succeeds
// makes a provider whose rows are ALL malformed look like a provider that is
// not writing yet -- the two states are operationally opposite, and the second
// one is the one nobody investigates.
func TestAdmittedCounterCountsRowsThatPassedAdmission(t *testing.T) {
	inputs := baseInputs()
	// Passes the admission table (raw kind + target prefix) but its source is a
	// tracker id, not a PR reference.
	inputs.Dependencies[0].SourceWorkItemID = "gh:owner/repo#not-a-pr"

	result := Derive(inputs)
	if got := result.Rejected[ReasonUnparseableSource]; got != 1 {
		t.Fatalf("rejected[unparseable_source] = %d, want 1", got)
	}
	if got := result.AdmittedByRawKind["linear_attachment"]; got != 1 {
		t.Errorf(
			"admitted[linear_attachment] = %d, want 1: the row passed admission and was "+
				"rejected later, so it must still be counted as admitted", got,
		)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

// TestDeriveKeepsAnAllZeroRepoUUID is codex round-6 F6.
//
// Python's repo filter is `if not repo_slug or not repo_id` (builder.py:700).
// `uuid.UUID` defines no `__bool__`, so an all-zero UUID is TRUTHY and Python
// keeps the row; only a missing (None) repo_id is skipped. Since
// `repos.id` is a non-nullable `UUID` in the live schema, None is unreachable
// from ClickHouse — so the sole behaviour that filter can change is the
// all-zero case, and Python keeps it.
//
// A Go `repo.ID == uuid.Nil` exclusion was therefore a gate PYTHON DOES NOT
// HAVE: it turned a row Python maps into `unknown_repo`. A false negative, not
// an unaccounted drop, which is exactly the shape that survives an accounting
// assertion. The frozen golden cannot catch it — it contains no zero UUID.
func TestDeriveKeepsAnAllZeroRepoUUID(t *testing.T) {
	inputs := baseInputs()
	inputs.Repos = []RepoRow{{OrgID: testOrg, ID: uuid.Nil, Repo: testSlug}}
	inputs.PullRequests = []PullRequestRow{{OrgID: testOrg, RepoID: uuid.Nil, Number: 12}}

	result := Derive(inputs)
	if result.Written() != 1 {
		t.Fatalf(
			"wrote %d links, want 1: Python keeps an all-zero repo UUID (rejections %v)",
			result.Written(), result.Rejected,
		)
	}
	if got := result.Links[0].RepoID; got != uuid.Nil {
		t.Errorf("repo_id = %s, want the all-zero UUID", got)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

// TestDeriveSkipsARepoWithNoSlug keeps the surviving half of Python's filter
// pinned: `not repo_slug` really does skip, so removing the UUID half must not
// remove this one too.
func TestDeriveSkipsARepoWithNoSlug(t *testing.T) {
	inputs := baseInputs()
	inputs.Repos = []RepoRow{{OrgID: testOrg, ID: testRepoID, Repo: ""}}
	assertSingleRejection(t, Derive(inputs), ReasonUnknownRepo)
}

// TestDeriveRefusesAnEmptyOrg is codex round-7 F7, the fourth instance of the
// gate-divergence meta-class.
//
// Python's very first statement is `if not self.config.org_id: return 0`
// (builder.py:645-646) — an org-less build reads nothing and writes nothing.
// Derive had no such guard, so it would map rows and stamp `OrgID: ""` on every
// link. A row written with an empty org_id lands in the wrong partition of
// `work_graph_issue_pr`'s (org_id, repo_id, work_item_id, pr_number) merge key.
//
// `Load` already rejects an empty org, so the Service path was never exposed —
// but `Derive` is EXPORTED and is what the golden test drives directly, so the
// parity claim is about Derive, not only about the path that happens to call
// it. The golden cannot reach this: its generator hard-codes a non-empty org.
func TestDeriveRefusesAnEmptyOrg(t *testing.T) {
	inputs := baseInputs()
	inputs.OrgID = ""
	// The rows still carry their own org, so only the BUILD org is missing —
	// the exact shape that would otherwise derive and stamp an empty org_id.

	result := Derive(inputs)
	if result.Written() != 0 {
		t.Fatalf(
			"wrote %d links for an org-less build; Python returns 0 and writes nothing (links %+v)",
			result.Written(), result.Links,
		)
	}
	if result.DependenciesRead != 0 {
		t.Errorf(
			"DependenciesRead = %d, want 0: Python returns before reading dependencies at all",
			result.DependenciesRead,
		)
	}
	if !result.Balanced() {
		t.Fatalf("accounting does not balance: %+v", result)
	}
}

package providersync

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func githubProjectV2DiffTestClaim() Claim {
	return githubWorkItemOracleClaim()
}

func githubProjectV2TestRepoID(t *testing.T) uuid.UUID {
	t.Helper()
	identity, err := repositoryIdentity("acme/api")
	if err != nil {
		t.Fatal(err)
	}
	repoID, err := uuid.Parse(identity)
	if err != nil {
		t.Fatal(err)
	}
	return repoID
}

// TestDiffGitHubProjectV2SnapshotAddsOnlyNewWorkItems is ruling A case (2)'s
// addition half: a work_item subject not previously active is added, and a
// pull_request subject in the same position is NOT -- ruling A case (1)
// already ships pull_request additions through githubProjectV2MembershipRow,
// so reproducing them here would mint a second, diff-time row for the same
// real event.
func TestDiffGitHubProjectV2SnapshotAddsOnlyNewWorkItems(t *testing.T) {
	claim := githubProjectV2DiffTestClaim()
	repoID := githubProjectV2TestRepoID(t)
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	current := []githubProjectV2SnapshotSubject{
		{SubjectKind: "work_item", SubjectID: "gh:acme/api#7", RepoID: repoID},
		{SubjectKind: "pull_request", SubjectID: "42", RepoID: repoID},
	}
	rows, counts := diffGitHubProjectV2Snapshot(claim, "ghprojv2:acme#3", current, nil, true, normalizedAt)
	if counts.IssueAdditions != 1 || counts.Removals != 0 {
		t.Fatalf("counts=%+v, want 1 addition and 0 removals", counts)
	}
	if len(rows) != 1 || rows[0].SubjectKind != "work_item" || rows[0].SubjectID != "gh:acme/api#7" ||
		rows[0].ToProjectID != "ghprojv2:acme#3" || rows[0].FromProjectID != "" ||
		!rows[0].OccurredAt.Equal(normalizedAt) {
		t.Fatalf("rows=%+v", rows)
	}
}

// TestDiffGitHubProjectV2SnapshotRemovesEitherSubjectKind is the removal
// half: both a work_item and a pull_request that were previously active and
// are now absent from the current board must each produce exactly one
// removal row -- nothing else in this producer retires either kind.
func TestDiffGitHubProjectV2SnapshotRemovesEitherSubjectKind(t *testing.T) {
	claim := githubProjectV2DiffTestClaim()
	repoID := githubProjectV2TestRepoID(t)
	normalizedAt := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	prior := []githubProjectV2SnapshotSubject{
		{SubjectKind: "work_item", SubjectID: "gh:acme/api#7", RepoID: repoID},
		{SubjectKind: "pull_request", SubjectID: "42", RepoID: repoID},
	}
	rows, counts := diffGitHubProjectV2Snapshot(claim, "ghprojv2:acme#3", nil, prior, true, normalizedAt)
	if counts.IssueAdditions != 0 || counts.Removals != 2 {
		t.Fatalf("counts=%+v, want 0 additions and 2 removals", counts)
	}
	removed := map[string]bool{}
	for _, row := range rows {
		if row.ToProjectID != "" || row.FromProjectID != "ghprojv2:acme#3" || !row.OccurredAt.Equal(normalizedAt) {
			t.Fatalf("removal row malformed: %+v", row)
		}
		removed[row.SubjectKind+":"+row.SubjectID] = true
	}
	if !removed["work_item:gh:acme/api#7"] || !removed["pull_request:42"] {
		t.Fatalf("removed=%+v, want both subjects", removed)
	}
}

// TestDiffGitHubProjectV2SnapshotUnchangedBoardEmitsNothing is the ruled
// "no change -> emit nothing": a subject present in both current and prior
// must not accumulate a row every time an unchanged board is re-diffed.
func TestDiffGitHubProjectV2SnapshotUnchangedBoardEmitsNothing(t *testing.T) {
	claim := githubProjectV2DiffTestClaim()
	repoID := githubProjectV2TestRepoID(t)
	subject := githubProjectV2SnapshotSubject{SubjectKind: "work_item", SubjectID: "gh:acme/api#7", RepoID: repoID}
	rows, counts := diffGitHubProjectV2Snapshot(
		claim, "ghprojv2:acme#3",
		[]githubProjectV2SnapshotSubject{subject}, []githubProjectV2SnapshotSubject{subject},
		true, time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	)
	if len(rows) != 0 || counts.IssueAdditions != 0 || counts.Removals != 0 {
		t.Fatalf("rows=%+v counts=%+v, want nothing", rows, counts)
	}
}

// TestDiffGitHubProjectV2SnapshotIncompleteSnapshotSuppressesRemovalsOnly is
// codex round 1's High finding, CHAOS-4193d: when this sync could not
// identify every real subject on the board (complete=false), a prior subject
// missing from `current` might just be one this sync failed to name, not one
// that left. Removals must be suppressed entirely in that case. Additions are
// unaffected -- a subject THIS sync DID positively identify is genuinely new
// regardless of what else on the board went unidentified.
func TestDiffGitHubProjectV2SnapshotIncompleteSnapshotSuppressesRemovalsOnly(t *testing.T) {
	claim := githubProjectV2DiffTestClaim()
	repoID := githubProjectV2TestRepoID(t)
	normalizedAt := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	prior := []githubProjectV2SnapshotSubject{
		{SubjectKind: "work_item", SubjectID: "gh:acme/api#7", RepoID: repoID},
	}
	current := []githubProjectV2SnapshotSubject{
		{SubjectKind: "work_item", SubjectID: "gh:acme/api#9", RepoID: repoID},
	}
	rows, counts := diffGitHubProjectV2Snapshot(claim, "ghprojv2:acme#3", current, prior, false, normalizedAt)
	if counts.Removals != 0 {
		t.Fatalf("an incomplete snapshot produced a removal: counts=%+v rows=%+v", counts, rows)
	}
	if counts.IssueAdditions != 1 || len(rows) != 1 || rows[0].SubjectID != "gh:acme/api#9" {
		t.Fatalf("the identifiable addition was lost: counts=%+v rows=%+v", counts, rows)
	}
}

// TestDiffGitHubProjectV2SnapshotDistinguishesSubjectsByRepo proves the key
// includes repo_id: two different repos coincidentally sharing a subject_id
// string must not collapse into one entry, or a removal in one repo would
// silently cancel a real addition in another.
func TestDiffGitHubProjectV2SnapshotDistinguishesSubjectsByRepo(t *testing.T) {
	claim := githubProjectV2DiffTestClaim()
	apiRepoID := githubProjectV2TestRepoID(t)
	webIdentity, err := repositoryIdentity("acme/web")
	if err != nil {
		t.Fatal(err)
	}
	webRepoID, err := uuid.Parse(webIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if apiRepoID == webRepoID {
		t.Fatal("premise failed, the two repos hashed to the same id")
	}
	prior := []githubProjectV2SnapshotSubject{{SubjectKind: "pull_request", SubjectID: "42", RepoID: apiRepoID}}
	current := []githubProjectV2SnapshotSubject{{SubjectKind: "pull_request", SubjectID: "42", RepoID: webRepoID}}
	rows, counts := diffGitHubProjectV2Snapshot(
		claim, "ghprojv2:acme#3", current, prior, true, time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC),
	)
	if counts.Removals != 1 {
		t.Fatalf("counts=%+v rows=%+v, want the api-repo PR removed despite the web-repo PR sharing its number", counts, rows)
	}
	if rows[0].RepoID != apiRepoID {
		t.Fatalf("removal named repo_id=%s, want the api repo %s", rows[0].RepoID, apiRepoID)
	}
}

// TestGitHubProjectV2ItemSubjectIdentifiesBothTrackedContentTypes proves the
// shared identification helper this file and the fetcher both call.
func TestGitHubProjectV2ItemSubjectIdentifiesBothTrackedContentTypes(t *testing.T) {
	repoID := githubProjectV2TestRepoID(t)
	pr := gitHubProjectV2ItemPayload{
		Content: gitHubProjectV2ContentPayload{
			Typename: "PullRequest", Number: 42,
			Repository: struct {
				NameWithOwner string `json:"nameWithOwner"`
			}{NameWithOwner: "acme/api"},
		},
	}
	subject, ok := githubProjectV2ItemSubject(pr)
	if !ok || subject.SubjectKind != "pull_request" || subject.SubjectID != "42" || subject.RepoID != repoID {
		t.Fatalf("pull request subject=%+v ok=%t", subject, ok)
	}

	issue := gitHubProjectV2ItemPayload{
		Content: gitHubProjectV2ContentPayload{
			Typename: "Issue", Number: 7,
			Repository: struct {
				NameWithOwner string `json:"nameWithOwner"`
			}{NameWithOwner: "acme/api"},
		},
	}
	subject, ok = githubProjectV2ItemSubject(issue)
	if !ok || subject.SubjectKind != "work_item" || subject.SubjectID != "gh:acme/api#7" || subject.RepoID != repoID {
		t.Fatalf("issue subject=%+v ok=%t", subject, ok)
	}

	draft := gitHubProjectV2ItemPayload{Content: gitHubProjectV2ContentPayload{Typename: "DraftIssue"}}
	if _, ok := githubProjectV2ItemSubject(draft); ok {
		t.Fatal("a draft issue produced a subject, but it has none to name")
	}

	incompletePR := gitHubProjectV2ItemPayload{Content: gitHubProjectV2ContentPayload{Typename: "PullRequest", Number: 8}}
	if _, ok := githubProjectV2ItemSubject(incompletePR); ok {
		t.Fatal("a pull request with no repository produced a subject")
	}

	unknown := gitHubProjectV2ItemPayload{Content: gitHubProjectV2ContentPayload{Typename: "SomeFutureContentType"}}
	if _, ok := githubProjectV2ItemSubject(unknown); ok {
		t.Fatal("an unrecognised content type produced a subject")
	}
}

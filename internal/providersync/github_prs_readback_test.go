package providersync

import (
	"testing"
	"time"
)

func pullRequestReadbackFixture(now time.Time) pullRequestRow {
	title, body, headBranch, baseBranch := "Add widget support",
		"This PR adds widget support.", "feature/widgets", "main"
	mergedAt, closedAt := now, now
	firstReviewAt := now.Add(-30 * time.Minute)
	return pullRequestRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Number: 42,
		Title: &title, Body: &body, State: "merged", AuthorName: "octocat",
		CreatedAt: now.Add(-time.Hour), MergedAt: &mergedAt, ClosedAt: &closedAt,
		HeadBranch: &headBranch, BaseBranch: &baseBranch,
		Additions: 120, Deletions: 30, ChangedFiles: 5, CommentsCount: 3,
		FirstReviewAt: &firstReviewAt, ReviewsCount: 2, ChangesRequestedCount: 1,
		LastSynced: now, OrgID: "org-acme",
	}
}

func pullRequestReadbackVersion(expected pullRequestRow) pullRequestVersion {
	return pullRequestVersion{
		Row: expected, SourceID: nil, OrgID: expected.OrgID,
		LastSynced: expected.LastSynced, Found: true,
	}
}

// TestPullRequestReadbackToleratesPreMergeHistory is the wedge regression
// (mirrors TestRepositoryReadbackToleratesPreMergeHistory): ReplacingMergeTree
// keeps every unmerged version of a key, so earlier sync occurrences
// legitimately sit next to this one. Only the winning version may decide.
func TestPullRequestReadbackToleratesPreMergeHistory(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	expected := pullRequestReadbackFixture(now)

	if got := comparePullRequestVersion(
		expected, pullRequestReadbackVersion(expected),
	); got != EffectExact {
		t.Fatalf("winning version = %s want %s", got, EffectExact)
	}

	duplicate := pullRequestReadbackVersion(expected)
	if got := comparePullRequestVersion(expected, duplicate); got != EffectExact {
		t.Fatalf("duplicate = %s want %s", got, EffectExact)
	}
}

// TestPullRequestReadbackToleratesOpenPRNullFields proves a row with every
// nullable field unset (an open PR with no reviews) still reads back exact
// -- nil must compare equal to nil, not to a sentinel value.
func TestPullRequestReadbackToleratesOpenPRNullFields(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	expected := pullRequestReadbackFixture(now)
	expected.State = "open"
	expected.MergedAt, expected.ClosedAt = nil, nil
	expected.FirstReviewAt, expected.FirstCommentAt = nil, nil
	expected.ReviewsCount, expected.ChangesRequestedCount = 0, 0
	expected.AuthorEmail, expected.HeadBranch, expected.BaseBranch = nil, nil, nil
	expected.Title, expected.Body = nil, nil

	actual := pullRequestReadbackVersion(expected)
	if got := comparePullRequestVersion(expected, actual); got != EffectExact {
		t.Fatalf("open PR readback = %s want %s", got, EffectExact)
	}
}

// TestPullRequestReadbackClassifiesEveryVersionRelationship mutation-tests
// each comparison clause independently (codex M6): every sub-test changes
// exactly ONE field on an otherwise-identical actual version, so a survivor
// here identifies precisely which clause in comparePullRequestVersion failed
// to fire -- the "mutate compound predicates clause by clause" discipline
// applied to the test fixture itself, not only to the harness's mutations.
func TestPullRequestReadbackClassifiesEveryVersionRelationship(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	expected := pullRequestReadbackFixture(now)

	stale := pullRequestReadbackVersion(expected)
	stale.LastSynced = now.Add(-time.Hour)

	newer := pullRequestReadbackVersion(expected)
	newer.LastSynced = now.Add(time.Hour)

	differentTitle := pullRequestReadbackVersion(expected)
	title := "Renamed"
	differentTitle.Row.Title = &title

	differentBody := pullRequestReadbackVersion(expected)
	body := "Different body"
	differentBody.Row.Body = &body

	differentState := pullRequestReadbackVersion(expected)
	differentState.Row.State = "closed"

	differentAuthorName := pullRequestReadbackVersion(expected)
	differentAuthorName.Row.AuthorName = "someone-else"

	differentAuthorEmail := pullRequestReadbackVersion(expected)
	email := "someone@example.test"
	differentAuthorEmail.Row.AuthorEmail = &email

	differentCreatedAt := pullRequestReadbackVersion(expected)
	differentCreatedAt.Row.CreatedAt = now.Add(-48 * time.Hour)

	// codex M5: a merged_at that differs only in sub-millisecond precision
	// must still read as a conflict -- this proves the comparison itself is
	// precise, independent of parseGitHubPullTime's own truncation.
	differentMergedAt := pullRequestReadbackVersion(expected)
	shiftedMerged := expected.MergedAt.Add(time.Millisecond)
	differentMergedAt.Row.MergedAt = &shiftedMerged

	differentClosedAt := pullRequestReadbackVersion(expected)
	shiftedClosed := expected.ClosedAt.Add(time.Millisecond)
	differentClosedAt.Row.ClosedAt = &shiftedClosed

	differentHeadBranch := pullRequestReadbackVersion(expected)
	head := "other-branch"
	differentHeadBranch.Row.HeadBranch = &head

	differentBaseBranch := pullRequestReadbackVersion(expected)
	base := "develop"
	differentBaseBranch.Row.BaseBranch = &base

	differentAdditions := pullRequestReadbackVersion(expected)
	differentAdditions.Row.Additions = expected.Additions + 1

	differentDeletions := pullRequestReadbackVersion(expected)
	differentDeletions.Row.Deletions = expected.Deletions + 1

	differentChangedFiles := pullRequestReadbackVersion(expected)
	differentChangedFiles.Row.ChangedFiles = expected.ChangedFiles + 1

	// The four review-derived columns: omitted entirely from the readback
	// comparison before the M6 fix, so a divergence here previously read as
	// EffectExact no matter what the persisted row actually held.
	differentFirstReviewAt := pullRequestReadbackVersion(expected)
	shiftedReview := expected.FirstReviewAt.Add(time.Hour)
	differentFirstReviewAt.Row.FirstReviewAt = &shiftedReview

	differentFirstCommentAt := pullRequestReadbackVersion(expected)
	someComment := now.Add(-15 * time.Minute)
	differentFirstCommentAt.Row.FirstCommentAt = &someComment

	differentChangesRequestedCount := pullRequestReadbackVersion(expected)
	differentChangesRequestedCount.Row.ChangesRequestedCount = expected.ChangesRequestedCount + 1

	differentReviewsCount := pullRequestReadbackVersion(expected)
	differentReviewsCount.Row.ReviewsCount = expected.ReviewsCount + 1

	differentCommentsCount := pullRequestReadbackVersion(expected)
	differentCommentsCount.Row.CommentsCount = expected.CommentsCount + 1

	externallyStamped := pullRequestReadbackVersion(expected)
	stampedSourceID := "11111111-1111-4111-8111-111111111111"
	externallyStamped.SourceID = &stampedSourceID

	staleTenant := pullRequestReadbackVersion(expected)
	staleTenant.OrgID = "org-other"

	for name, test := range map[string]struct {
		actual pullRequestVersion
		want   EffectInspection
	}{
		"absent key":                  {pullRequestVersion{}, EffectAbsent},
		"zero timestamp aggregate":    {pullRequestVersion{Found: true}, EffectAbsent},
		"only an older version":       {stale, EffectAbsent},
		"a newer occurrence wins":     {newer, EffectConflict},
		"different title":             {differentTitle, EffectConflict},
		"different body":              {differentBody, EffectConflict},
		"different state":             {differentState, EffectConflict},
		"different author name":       {differentAuthorName, EffectConflict},
		"different author email":      {differentAuthorEmail, EffectConflict},
		"different created_at":        {differentCreatedAt, EffectConflict},
		"merged_at off by 1ms":        {differentMergedAt, EffectConflict},
		"closed_at off by 1ms":        {differentClosedAt, EffectConflict},
		"different head branch":       {differentHeadBranch, EffectConflict},
		"different base branch":       {differentBaseBranch, EffectConflict},
		"different additions":         {differentAdditions, EffectConflict},
		"different deletions":         {differentDeletions, EffectConflict},
		"different changed files":     {differentChangedFiles, EffectConflict},
		"different first_review_at":   {differentFirstReviewAt, EffectConflict},
		"different first_comment_at":  {differentFirstCommentAt, EffectConflict},
		"different changes_requested": {differentChangesRequestedCount, EffectConflict},
		"different reviews_count":     {differentReviewsCount, EffectConflict},
		"different comments_count":    {differentCommentsCount, EffectConflict},
		"external ingest stamped it":  {externallyStamped, EffectConflict},
		"stale tenant row":            {staleTenant, EffectConflict},
	} {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := comparePullRequestVersion(expected, test.actual); got != test.want {
				t.Fatalf("%s = %s want %s", name, got, test.want)
			}
		})
	}
}

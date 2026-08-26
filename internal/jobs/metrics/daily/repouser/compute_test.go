package repouser

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The fixture below mirrors, field for field,
// tests/fixtures/generate_repo_user_commit_python_golden.py -- the values
// asserted here were read out of that generator's frozen output,
// tests/fixtures/repo_user_commit_python_golden.json, produced by running
// REAL Python (compute_daily_metrics, compute_rework_churn_ratio,
// compute_single_owner_file_ratio, compute_bus_factor,
// compute_code_ownership_gini) against this exact dataset. This is the fast,
// no-interpreter-required half of the parity guard; the live half
// (golden_rot_guard_test.go) re-runs the Python generator and diffs it
// against the checked-in file, so a Python behaviour change that would make
// these hardcoded expectations wrong gets caught there instead of silently
// passing here forever.
var (
	repoA = uuid.MustParse("00000000-0000-4000-8000-00000000000a")
	repoB = uuid.MustParse("00000000-0000-4000-8000-00000000000b")
	day   = time.Date(2026, 7, 20, 0, 0, 0, 0, time.UTC)
)

func fixtureCommits() []CommitStatRow {
	dt := func(hour, minute int) time.Time {
		return time.Date(2026, 7, 20, hour, minute, 0, 0, time.UTC)
	}
	return []CommitStatRow{
		{RepoID: repoA, CommitHash: "c1", AuthorEmail: "alice@example.com", AuthorName: "Alice",
			CommitterWhen: dt(9, 0), FilePath: "shared.py", Additions: 200, Deletions: 150},
		{RepoID: repoA, CommitHash: "c2", AuthorEmail: "alice@example.com", AuthorName: "Alice",
			CommitterWhen: dt(10, 0), FilePath: "alice_only.py", Additions: 10, Deletions: 5},
		{RepoID: repoA, CommitHash: "c3", AuthorEmail: "bob@example.com", AuthorName: "Bob",
			CommitterWhen: dt(11, 0), FilePath: "shared.py", Additions: 5, Deletions: 5},
		{RepoID: repoA, CommitHash: "c3", AuthorEmail: "bob@example.com", AuthorName: "Bob",
			CommitterWhen: dt(11, 0), FilePath: "bob_only.py", Additions: 3, Deletions: 1},
		{RepoID: repoB, CommitHash: "d1", AuthorEmail: "carol@example.com", AuthorName: "Carol",
			CommitterWhen: dt(14, 0), FilePath: "b_only.py", Additions: 4, Deletions: 2},
	}
}

func fixturePullRequests() []PullRequestRow {
	dt := func(hour, minute int) time.Time {
		return time.Date(2026, 7, 20, hour, minute, 0, 0, time.UTC)
	}
	firstReview := dt(9, 30)
	merged1 := dt(13, 0)
	merged2 := dt(12, 0)
	firstComment := dt(9, 0)
	return []PullRequestRow{
		{
			RepoID: repoA, Number: 1, AuthorEmail: "alice@example.com", AuthorName: "Alice",
			CreatedAt: dt(8, 0), MergedAt: &merged1, FirstReviewAt: &firstReview, FirstCommentAt: &firstComment,
			ChangesRequestedCount: 1, ReviewsCount: 2, CommentsCount: 3,
			Additions: 250, Deletions: 150, ChangedFiles: 4,
		},
		{
			RepoID: repoA, Number: 2, AuthorEmail: "bob@example.com", AuthorName: "Bob",
			CreatedAt: dt(10, 0), MergedAt: &merged2,
			Additions: 20, Deletions: 5, ChangedFiles: 1,
		},
		{
			RepoID: repoA, Number: 3, AuthorEmail: "alice@example.com", AuthorName: "Alice",
			CreatedAt: dt(15, 0), MergedAt: nil,
		},
	}
}

func fixtureReviews() []PullRequestReviewRow {
	return []PullRequestReviewRow{
		{RepoID: repoA, Number: 1, Reviewer: "bob@example.com",
			SubmittedAt: time.Date(2026, 7, 20, 9, 30, 0, 0, time.UTC), State: "APPROVED"},
	}
}

func fixtureBugItems() []BugWorkItem {
	return []BugWorkItem{
		{RepoID: repoA,
			StartedAt:   time.Date(2026, 7, 18, 6, 0, 0, 0, time.UTC),
			CompletedAt: time.Date(2026, 7, 20, 10, 0, 0, 0, time.UTC)},
	}
}

func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	commits := fixtureCommits()
	computedAt := time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC)

	reworkByRepo := map[uuid.UUID]float64{
		repoA: ReworkChurnRatio(repoA, commits),
		repoB: ReworkChurnRatio(repoB, commits),
	}
	singleOwnerByRepo := map[uuid.UUID]float64{
		repoA: SingleOwnerFileRatio(repoA, commits, 0.75, DefaultNormalizeIdentity),
		repoB: SingleOwnerFileRatio(repoB, commits, 0.75, DefaultNormalizeIdentity),
	}
	busFactorByRepo := map[uuid.UUID]int{
		repoA: BusFactor(repoA, commits, 0.5),
		repoB: BusFactor(repoB, commits, 0.5),
	}
	giniByRepo := map[uuid.UUID]float64{
		repoA: CodeOwnershipGini(repoA, commits),
		repoB: CodeOwnershipGini(repoB, commits),
	}
	mttrByRepo := MTTRByRepo(day, fixtureBugItems())

	assertFloat(t, "rework_churn_ratio_by_repo[A]", reworkByRepo[repoA], 0.9498680738786279)
	assertFloat(t, "rework_churn_ratio_by_repo[B]", reworkByRepo[repoB], 0.0)
	assertFloat(t, "single_owner_file_ratio_by_repo[A]", singleOwnerByRepo[repoA], 0.6666666666666666)
	assertFloat(t, "single_owner_file_ratio_by_repo[B]", singleOwnerByRepo[repoB], 1.0)
	if busFactorByRepo[repoA] != 1 || busFactorByRepo[repoB] != 1 {
		t.Errorf("bus factor: got A=%d B=%d, want A=1 B=1", busFactorByRepo[repoA], busFactorByRepo[repoB])
	}
	assertFloat(t, "code_ownership_gini_by_repo[A]", giniByRepo[repoA], 0.46306068601583106)
	assertFloat(t, "code_ownership_gini_by_repo[B]", giniByRepo[repoB], 0.0)
	assertFloat(t, "mttr_by_repo[A]", mttrByRepo[repoA], 52.0)
	if _, ok := mttrByRepo[repoB]; ok {
		t.Errorf("mttr_by_repo[B] should be absent (no bug items for repo B), got %v", mttrByRepo[repoB])
	}

	result := Compute(
		day, commits, fixturePullRequests(), fixtureReviews(), computedAt,
		DefaultNormalizeIdentity, 1000,
		mttrByRepo, reworkByRepo, singleOwnerByRepo, busFactorByRepo, giniByRepo,
	)

	if len(result.CommitMetrics) != 4 {
		t.Fatalf("commit metrics: got %d rows, want 4", len(result.CommitMetrics))
	}
	c1 := findCommit(t, result.CommitMetrics, repoA, "c1")
	if c1.TotalLOC != 350 || c1.SizeBucket != "large" || c1.FilesChanged != 1 {
		t.Errorf("c1: got total_loc=%d size_bucket=%q files_changed=%d, want 350/large/1",
			c1.TotalLOC, c1.SizeBucket, c1.FilesChanged)
	}
	c3 := findCommit(t, result.CommitMetrics, repoA, "c3")
	if c3.TotalLOC != 14 || c3.FilesChanged != 2 || c3.SizeBucket != "small" {
		t.Errorf("c3: got total_loc=%d files_changed=%d size_bucket=%q, want 14/2/small",
			c3.TotalLOC, c3.FilesChanged, c3.SizeBucket)
	}

	if len(result.RepoMetrics) != 2 {
		t.Fatalf("repo metrics: got %d rows, want 2", len(result.RepoMetrics))
	}
	repoAMetric := findRepo(t, result.RepoMetrics, repoA)
	if repoAMetric.CommitsCount != 3 || repoAMetric.TotalLOCTouched != 379 || repoAMetric.PRsMerged != 2 {
		t.Errorf("repo A: got commits=%d loc=%d prs_merged=%d, want 3/379/2",
			repoAMetric.CommitsCount, repoAMetric.TotalLOCTouched, repoAMetric.PRsMerged)
	}
	assertFloat(t, "repo A avg_commit_size_loc", repoAMetric.AvgCommitSizeLOC, 126.33333333333333)
	assertFloat(t, "repo A large_commit_ratio", repoAMetric.LargeCommitRatio, 0.3333333333333333)
	// 0.0, not the intuitive-looking 0.5: revert-PR detection is parity-dead
	// (see PullRequestRow.Title's doc comment) -- Title is never populated
	// from ClickHouse, matching Python's own "title" column never being
	// selected by loaders/clickhouse.py's real pr_query.
	assertFloat(t, "repo A change_failure_rate", repoAMetric.ChangeFailureRate, 0.0)
	assertFloat(t, "repo A median_pr_cycle_hours", repoAMetric.MedianPRCycleHours, 3.5)
	assertFloat(t, "repo A pr_cycle_p75_hours", repoAMetric.PRCycleP75Hours, 4.25)
	assertFloat(t, "repo A pr_cycle_p90_hours", repoAMetric.PRCycleP90Hours, 4.7)
	assertFloatPtr(t, "repo A pr_first_review_p50_hours", repoAMetric.PRFirstReviewP50Hours, 1.5)
	assertFloatPtr(t, "repo A pr_pickup_time_p50_hours", repoAMetric.PRPickupTimeP50Hours, 1.0)
	assertFloatPtr(t, "repo A pr_review_time_p50_hours", repoAMetric.PRReviewTimeP50Hours, 3.5)
	assertFloatPtr(t, "repo A pr_comments_per_100_loc", repoAMetric.PRCommentsPer100LOC, 0.7058823529411765)
	assertFloatPtr(t, "repo A pr_reviews_per_100_loc", repoAMetric.PRReviewsPer100LOC, 0.4705882352941176)
	assertFloatPtr(t, "repo A pr_size_p50_loc", repoAMetric.PRSizeP50LOC, 212.5)
	assertFloatPtr(t, "repo A pr_size_p90_loc", repoAMetric.PRSizeP90LOC, 362.5)
	assertFloatPtr(t, "repo A mttr_hours", repoAMetric.MTTRHours, 52.0)
	assertFloat(t, "repo A review_load_top_reviewer_ratio", repoAMetric.ReviewLoadTopReviewerRatio, 1.0)

	repoBMetric := findRepo(t, result.RepoMetrics, repoB)
	if repoBMetric.CommitsCount != 1 || repoBMetric.PRsMerged != 0 {
		t.Errorf("repo B: got commits=%d prs_merged=%d, want 1/0", repoBMetric.CommitsCount, repoBMetric.PRsMerged)
	}
	if repoBMetric.MTTRHours != nil {
		t.Errorf("repo B mttr_hours: got %v, want nil", *repoBMetric.MTTRHours)
	}
	if repoBMetric.PRSizeP50LOC != nil || repoBMetric.PRCommentsPer100LOC != nil {
		t.Errorf("repo B: expected nil PR-derived percentile fields for a PR-less repo")
	}

	if len(result.UserMetrics) != 3 {
		t.Fatalf("user metrics: got %d rows, want 3", len(result.UserMetrics))
	}
	alice := findUser(t, result.UserMetrics, repoA, "alice@example.com")
	assertFloat(t, "alice active_hours", alice.ActiveHours, 7.0)
	if alice.PRsAuthored != 2 || alice.PRsMerged != 1 || alice.ReviewsReceived != 1 {
		t.Errorf("alice: got prs_authored=%d prs_merged=%d reviews_received=%d, want 2/1/1",
			alice.PRsAuthored, alice.PRsMerged, alice.ReviewsReceived)
	}
	if alice.TeamID != "unassigned" || alice.TeamName != "Unassigned" {
		t.Errorf("alice: team should default to unassigned/Unassigned, got %q/%q", alice.TeamID, alice.TeamName)
	}

	bob := findUser(t, result.UserMetrics, repoA, "bob@example.com")
	assertFloat(t, "bob active_hours", bob.ActiveHours, 1.5)
	if bob.ReviewsGiven != 1 || bob.PRInterruptionLoad != 1 {
		t.Errorf("bob: got reviews_given=%d pr_interruption_load=%d, want 1/1", bob.ReviewsGiven, bob.PRInterruptionLoad)
	}

	carol := findUser(t, result.UserMetrics, repoB, "carol@example.com")
	if carol.CommitsCount != 1 || carol.PRsAuthored != 0 {
		t.Errorf("carol: got commits=%d prs_authored=%d, want 1/0", carol.CommitsCount, carol.PRsAuthored)
	}
}

func TestCommitSizeBucket(t *testing.T) {
	cases := []struct {
		loc  int
		want string
	}{{0, "small"}, {50, "small"}, {51, "medium"}, {300, "medium"}, {301, "large"}}
	for _, tc := range cases {
		if got := CommitSizeBucket(tc.loc); got != tc.want {
			t.Errorf("CommitSizeBucket(%d) = %q, want %q", tc.loc, got, tc.want)
		}
	}
}

func TestDefaultNormalizeIdentity(t *testing.T) {
	cases := []struct{ email, name, want string }{
		{" a@b.com ", "A B", "a@b.com"},
		{"", " A B ", "A B"},
		{"", "", "unknown"},
	}
	for _, tc := range cases {
		if got := DefaultNormalizeIdentity(tc.email, tc.name); got != tc.want {
			t.Errorf("DefaultNormalizeIdentity(%q, %q) = %q, want %q", tc.email, tc.name, got, tc.want)
		}
	}
}

func assertFloat(t *testing.T, label string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s: got %v, want %v", label, got, want)
	}
}

func assertFloatPtr(t *testing.T, label string, got *float64, want float64) {
	t.Helper()
	if got == nil {
		t.Errorf("%s: got nil, want %v", label, want)
		return
	}
	assertFloat(t, label, *got, want)
}

func findCommit(t *testing.T, rows []CommitMetric, repoID uuid.UUID, hash string) CommitMetric {
	t.Helper()
	for _, row := range rows {
		if row.RepoID == repoID && row.CommitHash == hash {
			return row
		}
	}
	t.Fatalf("commit metric %s/%s not found", repoID, hash)
	return CommitMetric{}
}

func findRepo(t *testing.T, rows []RepoMetric, repoID uuid.UUID) RepoMetric {
	t.Helper()
	for _, row := range rows {
		if row.RepoID == repoID {
			return row
		}
	}
	t.Fatalf("repo metric %s not found", repoID)
	return RepoMetric{}
}

func findUser(t *testing.T, rows []UserMetric, repoID uuid.UUID, email string) UserMetric {
	t.Helper()
	for _, row := range rows {
		if row.RepoID == repoID && row.AuthorEmail == email {
			return row
		}
	}
	t.Fatalf("user metric %s/%s not found", repoID, email)
	return UserMetric{}
}

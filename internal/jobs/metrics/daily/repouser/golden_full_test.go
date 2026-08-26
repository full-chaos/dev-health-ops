package repouser

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// TestComputeMatchesFrozenGoldenExhaustively decodes the ENTIRE frozen
// golden file (every row, every field) and compares it against Compute's
// live output, rather than the hand-picked subset
// TestComputeMatchesFrozenPythonGolden asserts (compute_test.go). That test
// stays -- it is a fast, readable smoke check -- but on its own it cannot
// catch a regression in an unchecked field/row, which is exactly what a
// "test that cannot fail" looks like (AGENTS.md's verification-rules
// standard). This test is the row-complete guard.
func TestComputeMatchesFrozenGoldenExhaustively(t *testing.T) {
	goldenPath := repositoryRootPath(t)
	raw, err := os.ReadFile(filepath.Join(goldenPath, "tests", "fixtures", "repo_user_commit_python_golden.json"))
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}

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

	result := Compute(
		day, commits, fixturePullRequests(), fixtureReviews(), computedAt,
		DefaultNormalizeIdentity, 1000,
		mttrByRepo, reworkByRepo, singleOwnerByRepo, busFactorByRepo, giniByRepo,
	)

	liveRepoMetrics := make([]goldenRepoMetric, len(result.RepoMetrics))
	for i, row := range result.RepoMetrics {
		liveRepoMetrics[i] = toGoldenRepoMetric(t, row)
	}
	liveUserMetrics := make([]goldenUserMetric, len(result.UserMetrics))
	for i, row := range result.UserMetrics {
		liveUserMetrics[i] = toGoldenUserMetric(t, row)
	}
	liveCommitMetrics := make([]goldenCommitMetric, len(result.CommitMetrics))
	for i, row := range result.CommitMetrics {
		liveCommitMetrics[i] = toGoldenCommitMetric(t, row)
	}

	sortGoldenRepoMetrics(golden.RepoMetrics)
	sortGoldenRepoMetrics(liveRepoMetrics)
	sortGoldenUserMetrics(golden.UserMetrics)
	sortGoldenUserMetrics(liveUserMetrics)
	sortGoldenCommitMetrics(golden.CommitMetrics)
	sortGoldenCommitMetrics(liveCommitMetrics)

	if !reflect.DeepEqual(golden.RepoMetrics, liveRepoMetrics) {
		t.Errorf("repo_metrics mismatch:\nfrozen: %+v\nlive:   %+v", golden.RepoMetrics, liveRepoMetrics)
	}
	if !reflect.DeepEqual(golden.UserMetrics, liveUserMetrics) {
		t.Errorf("user_metrics mismatch:\nfrozen: %+v\nlive:   %+v", golden.UserMetrics, liveUserMetrics)
	}
	if !reflect.DeepEqual(golden.CommitMetrics, liveCommitMetrics) {
		t.Errorf("commit_metrics mismatch:\nfrozen: %+v\nlive:   %+v", golden.CommitMetrics, liveCommitMetrics)
	}
}

// goldenDocument mirrors the top-level shape
// generate_repo_user_commit_python_golden.py renders. computed_at is parsed
// as a real timestamp (not compared as a literal string) since Python's
// isoformat() ("+00:00") and Go's time formatting never render identically
// byte-for-byte even for the same instant.
type goldenDocument struct {
	RepoMetrics   []goldenRepoMetric   `json:"repo_metrics"`
	UserMetrics   []goldenUserMetric   `json:"user_metrics"`
	CommitMetrics []goldenCommitMetric `json:"commit_metrics"`
}

type goldenRepoMetric struct {
	RepoID                     string         `json:"repo_id"`
	Day                        string         `json:"day"`
	CommitsCount               int            `json:"commits_count"`
	TotalLOCTouched            int            `json:"total_loc_touched"`
	AvgCommitSizeLOC           float64        `json:"avg_commit_size_loc"`
	LargeCommitRatio           float64        `json:"large_commit_ratio"`
	PRsMerged                  int            `json:"prs_merged"`
	MedianPRCycleHours         float64        `json:"median_pr_cycle_hours"`
	PRCycleP75Hours            float64        `json:"pr_cycle_p75_hours"`
	PRCycleP90Hours            float64        `json:"pr_cycle_p90_hours"`
	PRsWithFirstReview         int            `json:"prs_with_first_review"`
	PRFirstReviewP50Hours      *float64       `json:"pr_first_review_p50_hours"`
	PRFirstReviewP90Hours      *float64       `json:"pr_first_review_p90_hours"`
	PRReviewTimeP50Hours       *float64       `json:"pr_review_time_p50_hours"`
	PRPickupTimeP50Hours       *float64       `json:"pr_pickup_time_p50_hours"`
	LargePRRatio               float64        `json:"large_pr_ratio"`
	PRReworkRatio              float64        `json:"pr_rework_ratio"`
	PRSizeP50LOC               *float64       `json:"pr_size_p50_loc"`
	PRSizeP90LOC               *float64       `json:"pr_size_p90_loc"`
	PRCommentsPer100LOC        *float64       `json:"pr_comments_per_100_loc"`
	PRReviewsPer100LOC         *float64       `json:"pr_reviews_per_100_loc"`
	ReworkChurnRatio30d        float64        `json:"rework_churn_ratio_30d"`
	SingleOwnerFileRatio30d    float64        `json:"single_owner_file_ratio_30d"`
	ReviewLoadTopReviewerRatio float64        `json:"review_load_top_reviewer_ratio"`
	BusFactor                  int            `json:"bus_factor"`
	CodeOwnershipGini          float64        `json:"code_ownership_gini"`
	MTTRHours                  *float64       `json:"mttr_hours"`
	ChangeFailureRate          float64        `json:"change_failure_rate"`
	ComputedAt                 comparableTime `json:"computed_at"`
	OrgID                      string         `json:"org_id"`
}

type goldenUserMetric struct {
	RepoID                string         `json:"repo_id"`
	Day                   string         `json:"day"`
	AuthorEmail           string         `json:"author_email"`
	CommitsCount          int            `json:"commits_count"`
	LOCAdded              int            `json:"loc_added"`
	LOCDeleted            int            `json:"loc_deleted"`
	FilesChanged          int            `json:"files_changed"`
	LargeCommitsCount     int            `json:"large_commits_count"`
	AvgCommitSizeLOC      float64        `json:"avg_commit_size_loc"`
	PRsAuthored           int            `json:"prs_authored"`
	PRsMerged             int            `json:"prs_merged"`
	AvgPRCycleHours       float64        `json:"avg_pr_cycle_hours"`
	MedianPRCycleHours    float64        `json:"median_pr_cycle_hours"`
	PRCycleP75Hours       float64        `json:"pr_cycle_p75_hours"`
	PRCycleP90Hours       float64        `json:"pr_cycle_p90_hours"`
	PRsWithFirstReview    int            `json:"prs_with_first_review"`
	PRFirstReviewP50Hours *float64       `json:"pr_first_review_p50_hours"`
	PRFirstReviewP90Hours *float64       `json:"pr_first_review_p90_hours"`
	PRReviewTimeP50Hours  *float64       `json:"pr_review_time_p50_hours"`
	PRPickupTimeP50Hours  *float64       `json:"pr_pickup_time_p50_hours"`
	ReviewsGiven          int            `json:"reviews_given"`
	ChangesRequestedGiven int            `json:"changes_requested_given"`
	ReviewsReceived       int            `json:"reviews_received"`
	ReviewReciprocity     float64        `json:"review_reciprocity"`
	PRInterruptionLoad    int            `json:"pr_interruption_load"`
	ContextSpreadCount    int            `json:"context_spread_count"`
	ReviewRequestLoad     int            `json:"review_request_load"`
	TeamID                string         `json:"team_id"`
	TeamName              string         `json:"team_name"`
	ActiveHours           float64        `json:"active_hours"`
	WeekendDays           int            `json:"weekend_days"`
	IdentityID            string         `json:"identity_id"`
	ComputedAt            comparableTime `json:"computed_at"`
	OrgID                 string         `json:"org_id"`
}

type goldenCommitMetric struct {
	RepoID       string         `json:"repo_id"`
	CommitHash   string         `json:"commit_hash"`
	Day          string         `json:"day"`
	AuthorEmail  string         `json:"author_email"`
	TotalLOC     int            `json:"total_loc"`
	FilesChanged int            `json:"files_changed"`
	SizeBucket   string         `json:"size_bucket"`
	ComputedAt   comparableTime `json:"computed_at"`
	OrgID        string         `json:"org_id"`
}

// comparableTime unmarshals either Python's isoformat() ("...+00:00") or
// Go's time.Time JSON encoding, and compares by instant (time.Time.Equal),
// not by literal string -- the two languages never render an identical UTC
// instant byte-for-byte.
type comparableTime struct{ time.Time }

func (t *comparableTime) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return fmt.Errorf("parse timestamp %q: %w", raw, err)
	}
	// .UTC(): reflect.DeepEqual on time.Time compares internal
	// wall/ext/loc fields directly, and a fixed +00:00 offset location
	// (what time.Parse returns for that literal) is NOT the same value as
	// the time.UTC singleton location Go's own computed_at carries, even
	// for the identical instant. Normalizing both sides to time.UTC is
	// what makes DeepEqual mean what this test wants it to mean here.
	t.Time = parsed.UTC()
	return nil
}

func toGoldenRepoMetric(t *testing.T, row RepoMetric) goldenRepoMetric {
	t.Helper()
	return goldenRepoMetric{
		RepoID: row.RepoID.String(), Day: row.Day.Format("2006-01-02"),
		CommitsCount: row.CommitsCount, TotalLOCTouched: row.TotalLOCTouched,
		AvgCommitSizeLOC: row.AvgCommitSizeLOC, LargeCommitRatio: row.LargeCommitRatio,
		PRsMerged: row.PRsMerged, MedianPRCycleHours: row.MedianPRCycleHours,
		PRCycleP75Hours: row.PRCycleP75Hours, PRCycleP90Hours: row.PRCycleP90Hours,
		PRsWithFirstReview: row.PRsWithFirstReview, PRFirstReviewP50Hours: row.PRFirstReviewP50Hours,
		PRFirstReviewP90Hours: row.PRFirstReviewP90Hours, PRReviewTimeP50Hours: row.PRReviewTimeP50Hours,
		PRPickupTimeP50Hours: row.PRPickupTimeP50Hours, LargePRRatio: row.LargePRRatio,
		PRReworkRatio: row.PRReworkRatio, PRSizeP50LOC: row.PRSizeP50LOC, PRSizeP90LOC: row.PRSizeP90LOC,
		PRCommentsPer100LOC: row.PRCommentsPer100LOC, PRReviewsPer100LOC: row.PRReviewsPer100LOC,
		ReworkChurnRatio30d: row.ReworkChurnRatio30d, SingleOwnerFileRatio30d: row.SingleOwnerFileRatio30d,
		ReviewLoadTopReviewerRatio: row.ReviewLoadTopReviewerRatio, BusFactor: row.BusFactor,
		CodeOwnershipGini: row.CodeOwnershipGini, MTTRHours: row.MTTRHours,
		ChangeFailureRate: row.ChangeFailureRate, ComputedAt: comparableTime{row.ComputedAt},
		OrgID: "",
	}
}

func toGoldenUserMetric(t *testing.T, row UserMetric) goldenUserMetric {
	t.Helper()
	return goldenUserMetric{
		RepoID: row.RepoID.String(), Day: row.Day.Format("2006-01-02"), AuthorEmail: row.AuthorEmail,
		CommitsCount: row.CommitsCount, LOCAdded: row.LOCAdded, LOCDeleted: row.LOCDeleted,
		FilesChanged: row.FilesChanged, LargeCommitsCount: row.LargeCommitsCount,
		AvgCommitSizeLOC: row.AvgCommitSizeLOC, PRsAuthored: row.PRsAuthored, PRsMerged: row.PRsMerged,
		AvgPRCycleHours: row.AvgPRCycleHours, MedianPRCycleHours: row.MedianPRCycleHours,
		PRCycleP75Hours: row.PRCycleP75Hours, PRCycleP90Hours: row.PRCycleP90Hours,
		PRsWithFirstReview: row.PRsWithFirstReview, PRFirstReviewP50Hours: row.PRFirstReviewP50Hours,
		PRFirstReviewP90Hours: row.PRFirstReviewP90Hours, PRReviewTimeP50Hours: row.PRReviewTimeP50Hours,
		PRPickupTimeP50Hours: row.PRPickupTimeP50Hours, ReviewsGiven: row.ReviewsGiven,
		ChangesRequestedGiven: row.ChangesRequestedGiven, ReviewsReceived: row.ReviewsReceived,
		ReviewReciprocity: row.ReviewReciprocity, PRInterruptionLoad: row.PRInterruptionLoad,
		ContextSpreadCount: row.ContextSpreadCount, ReviewRequestLoad: row.ReviewRequestLoad,
		TeamID: row.TeamID, TeamName: row.TeamName, ActiveHours: row.ActiveHours,
		WeekendDays: row.WeekendDays, IdentityID: row.IdentityID,
		ComputedAt: comparableTime{row.ComputedAt}, OrgID: "",
	}
}

func toGoldenCommitMetric(t *testing.T, row CommitMetric) goldenCommitMetric {
	t.Helper()
	return goldenCommitMetric{
		RepoID: row.RepoID.String(), CommitHash: row.CommitHash, Day: row.Day.Format("2006-01-02"),
		AuthorEmail: row.AuthorEmail, TotalLOC: row.TotalLOC, FilesChanged: row.FilesChanged,
		SizeBucket: row.SizeBucket, ComputedAt: comparableTime{row.ComputedAt}, OrgID: "",
	}
}

func sortGoldenRepoMetrics(rows []goldenRepoMetric) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].RepoID < rows[j].RepoID })
}

func sortGoldenUserMetrics(rows []goldenUserMetric) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RepoID != rows[j].RepoID {
			return rows[i].RepoID < rows[j].RepoID
		}
		return rows[i].AuthorEmail < rows[j].AuthorEmail
	})
}

func sortGoldenCommitMetrics(rows []goldenCommitMetric) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].RepoID != rows[j].RepoID {
			return rows[i].RepoID < rows[j].RepoID
		}
		return rows[i].CommitHash < rows[j].CommitHash
	})
}

// repositoryRootPath walks up from this package to the checkout root (the
// directory containing go.mod), matching golden_rot_guard_test.go's helper
// but returning a plain path (no *testing.T requirement beyond Fatal).
func repositoryRootPath(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

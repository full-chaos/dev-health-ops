package reviewedges

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// goldenRecord is ReviewEdgeDailyRecord as the now-deleted CHAOS-4279 Python
// generator serialized it: dataclass field names verbatim, date/datetime as
// isoformat strings, UUID as str.
type goldenRecord struct {
	RepoID       string `json:"repo_id"`
	Day          string `json:"day"`
	Reviewer     string `json:"reviewer"`
	Author       string `json:"author"`
	ReviewsCount uint32 `json:"reviews_count"`
	ComputedAt   string `json:"computed_at"`
	OrgID        string `json:"org_id"`
}

type goldenDocument struct {
	Records []goldenRecord `json:"records"`
}

var (
	repoA = uuid.MustParse("00000000-0000-4000-8000-00000000000a")
	repoB = uuid.MustParse("00000000-0000-4000-8000-00000000000b")
)

func at(hour, minute, second, day int) time.Time {
	return time.Date(2026, 8, day, hour, minute, second, 0, time.UTC)
}

func goldenDay() time.Time   { return time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC) }
func goldenStamp() time.Time { return time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC) }

// goldenPullRequests / goldenReviews mirror the Python generator's two corpora
// exactly -- same rows, same order. A divergence shows up as a mismatch in the
// frozen comparison, not as a silently skipped case.
func goldenPullRequests() []PullRequestRow {
	return []PullRequestRow{
		{repoA, 1, "ann@example.com", "Ann"},
		{repoA, 2, "", "Bob"},
		{repoA, 3, "", ""},
		{repoA, 4, "  ann@example.com  ", "Ann"},
		{repoB, 1, "dee@example.com", "Dee"},
	}
}

func goldenReviews() []ReviewRow {
	return []ReviewRow{
		{repoA, 1, "Bob", at(9, 0, 0, 24)},
		{repoA, 1, "Cal", at(10, 0, 0, 24)},
		{repoA, 1, "Bob", at(11, 0, 0, 24)},
		{repoA, 4, "Bob", at(11, 30, 0, 24)},
		{repoA, 4, "  Cal  ", at(11, 45, 0, 24)},
		{repoA, 99, "Bob", at(12, 0, 0, 24)},
		{repoA, 3, "Cal", at(13, 0, 0, 24)},
		{repoA, 2, "   ", at(14, 0, 0, 24)},
		{repoA, 2, "\x1c", at(14, 30, 0, 24)},
		{repoB, 1, "Eve", at(0, 0, 0, 24)},
		{repoB, 1, "Eve", at(0, 0, 0, 25)},
		{repoB, 1, "Eve", at(23, 59, 59, 23)},
	}
}

func render(record Record) goldenRecord {
	return goldenRecord{
		RepoID:       record.RepoID.String(),
		Day:          record.Day.Format("2006-01-02"),
		Reviewer:     record.Reviewer,
		Author:       record.Author,
		ReviewsCount: record.ReviewsCount,
		// Python's datetime.isoformat() renders a tz-aware UTC value's offset
		// as "+00:00", not "Z".
		ComputedAt: record.ComputedAt.Format("2006-01-02T15:04:05-07:00"),
		OrgID:      record.OrgID,
	}
}

func repositoryRoot(t *testing.T) string {
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

// TestComputeMatchesFrozenPythonGolden is the differential oracle: the whole
// corpus through the Go compute, compared field-for-field against the frozen
// output of the REAL Python producer.
//
// The Python record carries org_id="" (its dataclass default; the sink injects
// the real org at write time, sinks/clickhouse/core.go:579-586), so the Go
// side is fed "" here too and the writer is what supplies the org -- see
// Writer.WriteRecords, which fails closed on an empty one.
func TestComputeMatchesFrozenPythonGolden(t *testing.T) {
	path := filepath.Join(
		repositoryRoot(t), "tests", "fixtures", "daily_review_edges_python_golden.json",
	)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}

	live := ComputeReviewEdgesDaily(
		goldenDay(), goldenPullRequests(), goldenReviews(), goldenStamp(), "",
	)
	rendered := make([]goldenRecord, 0, len(live))
	for _, record := range live {
		rendered = append(rendered, render(record))
	}

	if !reflect.DeepEqual(rendered, golden.Records) {
		t.Errorf("Go output does not match the frozen Python golden.\n got %+v\nwant %+v",
			rendered, golden.Records)
	}
}

// TestUnitSeparatorReviewerCollapsesToUnknownLikePython is the pin on
// pythonparity.Strip, and the reason strings.TrimSpace is not good enough.
//
// Python's str.strip() treats U+001C..U+001F as whitespace; Go's
// unicode.IsSpace does not. A reviewer whose entire name is "\x1c" therefore
// strips to "" in Python and falls through to "unknown", while TrimSpace
// leaves it intact as a one-rune identity. The golden above already folds that
// review together with a whitespace-only one into a single unknown->Bob edge
// with count 2; this test states the mechanism directly so the failure is
// diagnosable rather than showing up as an off-by-one count.
func TestUnitSeparatorReviewerCollapsesToUnknownLikePython(t *testing.T) {
	const unitSeparator = "\x1c"

	if got := NormalizeGitIdentity("", unitSeparator); got != "unknown" {
		t.Errorf("NormalizeGitIdentity(\"\", %q) = %q, want \"unknown\" (Python strips U+001C)", unitSeparator, got)
	}
	// The control: prove TrimSpace really would have kept it, so this test
	// cannot quietly become a tautology if the helper changes.
	if strings.TrimSpace(unitSeparator) == "" {
		t.Fatal("precondition failed: strings.TrimSpace now strips U+001C, so this guard no longer distinguishes the two")
	}

	reviews := []ReviewRow{
		{repoA, 1, unitSeparator, at(9, 0, 0, 24)},
		{repoA, 1, "   ", at(10, 0, 0, 24)},
	}
	records := ComputeReviewEdgesDaily(
		goldenDay(), []PullRequestRow{{repoA, 1, "ann@example.com", "Ann"}}, reviews, goldenStamp(), "",
	)
	if len(records) != 1 {
		t.Fatalf("got %d edges, want 1 -- both reviewers must collapse to \"unknown\"", len(records))
	}
	if records[0].Reviewer != "unknown" || records[0].ReviewsCount != 2 {
		t.Errorf("got reviewer=%q count=%d, want unknown/2", records[0].Reviewer, records[0].ReviewsCount)
	}
}

// TestReviewOnAnAbsentPullRequestIsDropped pins the quirk the PR loader's
// narrow window produces: a review of a PR neither created nor merged that day
// finds no author and its edge vanishes. Reproduced deliberately (reviews.py:
// 52-54), so a future "fix" has to come through this test.
func TestReviewOnAnAbsentPullRequestIsDropped(t *testing.T) {
	records := ComputeReviewEdgesDaily(
		goldenDay(),
		[]PullRequestRow{{repoA, 1, "ann@example.com", "Ann"}},
		[]ReviewRow{
			{repoA, 1, "Bob", at(9, 0, 0, 24)},
			{repoA, 77, "Bob", at(9, 30, 0, 24)}, // PR 77 is absent
		},
		goldenStamp(), "",
	)
	if len(records) != 1 || records[0].ReviewsCount != 1 {
		t.Fatalf("got %d edge(s) %+v, want exactly 1 with count 1", len(records), records)
	}
}

// TestAnUnknownAuthorIsStillAnEdge separates the two "unknown" cases: a PR
// present with both author fields empty resolves to the literal "unknown",
// which is TRUTHY in Python, so its edges are KEPT -- unlike an absent PR,
// whose lookup miss drops the edge.
func TestAnUnknownAuthorIsStillAnEdge(t *testing.T) {
	records := ComputeReviewEdgesDaily(
		goldenDay(),
		[]PullRequestRow{{repoA, 1, "", ""}},
		[]ReviewRow{{repoA, 1, "Bob", at(9, 0, 0, 24)}},
		goldenStamp(), "",
	)
	if len(records) != 1 || records[0].Author != "unknown" {
		t.Fatalf("got %+v, want one edge with author \"unknown\"", records)
	}
}

// TestWindowIsHalfOpen pins [start, end): a review exactly on midnight counts,
// one exactly on the next midnight does not.
func TestWindowIsHalfOpen(t *testing.T) {
	prs := []PullRequestRow{{repoA, 1, "ann@example.com", "Ann"}}
	for _, testCase := range []struct {
		name      string
		submitted time.Time
		want      int
	}{
		{"lower bound included", at(0, 0, 0, 24), 1},
		{"upper bound excluded", at(0, 0, 0, 25), 0},
		{"one second before", at(23, 59, 59, 23), 0},
		{"last second of the day", at(23, 59, 59, 24), 1},
	} {
		records := ComputeReviewEdgesDaily(
			goldenDay(), prs,
			[]ReviewRow{{repoA, 1, "Bob", testCase.submitted}},
			goldenStamp(), "",
		)
		if len(records) != testCase.want {
			t.Errorf("%s: got %d edge(s), want %d", testCase.name, len(records), testCase.want)
		}
	}
}

// TestNoReviewsReturnsNilWithoutReadingPullRequests pins Python's early return
// (reviews.py:33-34) -- the executor relies on it to skip the PR query
// entirely.
func TestNoReviewsReturnsNilWithoutReadingPullRequests(t *testing.T) {
	if records := ComputeReviewEdgesDaily(
		goldenDay(), goldenPullRequests(), nil, goldenStamp(), "",
	); records != nil {
		t.Errorf("got %+v, want nil for an empty review set", records)
	}
}

// TestLastPullRequestRowWinsOnADuplicateKey pins the dict-assignment semantics
// Python has (reviews.py:44). The loader deduplicates upstream so this is not
// reachable in production, but the compute must still be defined on it -- and
// defined the SAME way, since the golden corpus is fed straight to both sides.
func TestLastPullRequestRowWinsOnADuplicateKey(t *testing.T) {
	records := ComputeReviewEdgesDaily(
		goldenDay(),
		[]PullRequestRow{
			{repoA, 1, "first@example.com", "First"},
			{repoA, 1, "second@example.com", "Second"},
		},
		[]ReviewRow{{repoA, 1, "Bob", at(9, 0, 0, 24)}},
		goldenStamp(), "",
	)
	if len(records) != 1 || records[0].Author != "second@example.com" {
		t.Fatalf("got %+v, want the LAST row's author to win", records)
	}
}

package daily

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/testops"
)

func TestReportRunVerdict(t *testing.T) {
	for name, tc := range map[string]struct {
		counted, markAttempts []uint32
		markCompletes         []uint8
		want                  reportRunState
	}{
		"no row":                                       {[]uint32{1}, nil, nil, reportRunUnmarked},
		"legacy row, no row":                           {[]uint32{0}, nil, nil, reportRunUnmarked},
		"complete":                                     {[]uint32{1}, []uint32{1}, []uint8{1}, reportRunComplete},
		"partial":                                      {[]uint32{1}, []uint32{1}, []uint8{0}, reportRunPartial},
		"newest counted attempt partial":               {[]uint32{2}, []uint32{1, 2}, []uint8{1, 0}, reportRunPartial},
		"older partial attempt not counted":            {[]uint32{2}, []uint32{1, 2}, []uint8{0, 1}, reportRunComplete},
		"counted attempt without its own row":          {[]uint32{2}, []uint32{1}, []uint8{1}, reportRunUnmarked},
		"newest attempt without suites is partial":     {[]uint32{1}, []uint32{1, 2}, []uint8{1, 0}, reportRunPartial},
		"newest attempt without suites, complete":      {[]uint32{1}, []uint32{1, 2}, []uint8{1, 1}, reportRunComplete},
		"re-run failed jobs keeps a partial attempt 1": {[]uint32{1, 2}, []uint32{1, 2}, []uint8{0, 1}, reportRunPartial},
		"re-run failed jobs, both complete":            {[]uint32{1, 2}, []uint32{1, 2}, []uint8{1, 1}, reportRunComplete},
		"partial wins over a counted attempt w/o row":  {[]uint32{1, 2}, []uint32{1}, []uint8{0}, reportRunPartial},
		"partial wins, unmarked attempt seen first":    {[]uint32{2, 1}, []uint32{1}, []uint8{0}, reportRunPartial},
		"newer row only, counted older without row":    {[]uint32{1}, []uint32{2}, []uint8{1}, reportRunUnmarked},
	} {
		if got := reportRunVerdict(tc.counted, tc.markAttempts, tc.markCompletes); got != tc.want {
			t.Errorf("%s: verdict %d, want %d", name, got, tc.want)
		}
	}
}

// The completeness read counts the kept suites of the day (the suite read's own rule), reads the table FINAL and scopes both
// halves to the organization and the repository list.
func TestTestopsReportCompletenessStatementShape(t *testing.T) {
	statement := testopsReportCompletenessStatement
	for _, want := range []string{
		testopsKeptSuitesToday,
		"FROM testops_run_report_completeness FINAL",
		"WHERE org_id = {org_id:String} AND repo_id IN {repo_ids:Array(UUID)}",
		"run_id IN (SELECT run_id FROM kept)",
		"groupUniqArray(run_attempt) AS counted_attempts",
	} {
		if !strings.Contains(statement, want) {
			t.Errorf("statement lacks %q:\n%s", want, statement)
		}
	}
}

type failingQueryConn struct{ err error }

func (conn failingQueryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, conn.err
}

// A failed read returns its error and no counts: the caller writes no row.
func TestLoadNativeTestopsReportRunCountsFailedReadReturnsTheError(t *testing.T) {
	want := errors.New("read refused")
	counts, err := loadNativeTestopsReportRunCounts(context.Background(), failingQueryConn{err: want}, "org-1", uuid.New(),
		time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC))
	if !errors.Is(err, want) || counts != nil {
		t.Fatalf("counts %v, error %v; want nil and the read error", counts, err)
	}
}

// The writer names the three completeness columns last and writes the counts, NULL only for a record whose completeness was not
// read.
func TestWriteTestopsTestMetricsWritesReportRunCounts(t *testing.T) {
	day := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for name, test := range map[string]struct {
		counts *testops.ReportRunCounts
		want   []uint32
	}{
		"measured":     {&testops.ReportRunCounts{Runs: 3, Partial: 1, Unmarked: 1}, []uint32{3, 1, 1}},
		"measured 0":   {&testops.ReportRunCounts{Runs: 1}, []uint32{1, 0, 0}},
		"not measured": {nil, nil},
	} {
		conn := &queryRecordingBatchConn{batch: &recordingBatch{}}
		metric := testops.TestMetric{RepoID: uuid.New(), TotalCases: 4, OrgID: "org-1", ReportRuns: test.counts}
		if _, err := writeTestopsTestMetrics(context.Background(), conn, "org-1", day, day, []testops.TestMetric{metric}); err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(strings.Join(strings.Fields(conn.query), " "), "pooled_samples_stored, report_runs, partial_report_runs, unmarked_report_runs)") {
			t.Fatalf("%s: INSERT does not name the completeness columns last: %s", name, conn.query)
		}
		row := conn.batch.appended[0]
		for index, value := range row[len(row)-3:] {
			got := value.(*uint32)
			if test.want == nil {
				if got != nil {
					t.Errorf("%s: column %d = %d, want NULL", name, index, *got)
				}
				continue
			}
			if got == nil || *got != test.want[index] {
				t.Errorf("%s: column %d = %v, want %d", name, index, got, test.want[index])
			}
		}
	}
}

func TestRepeatedAttempt(t *testing.T) {
	if attempt, twice := repeatedAttempt([]uint32{1, 2, 1}); !twice || attempt != 1 {
		t.Errorf("repeatedAttempt([1 2 1]) = %d, %v; want 1, true", attempt, twice)
	}
	if _, twice := repeatedAttempt([]uint32{1, 2}); twice {
		t.Error("repeatedAttempt([1 2]) found a repeat")
	}
}

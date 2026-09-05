package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// TestRepoUserCommitPartialWriteGuardPinsBothDirections is the codex sweep
// red-first proof (CHAOS-5190 r3 follow-up, team-lead-requested): before
// this fix, repouser.Writer.WriteResult's own correctly-threaded partial
// counts (repoRows on a userMetrics failure, repoRows+userRows on a
// commitMetrics failure) were discarded by ComputeFamily's bare
// `return 0, err` -- exactly the class already fixed in
// work_item_state/work_item/work_item_estimate/work_graph_edges/
// ai_governance. Mirrors those tests' exact shape: wrap only when
// something already landed, never when nothing did.
func TestRepoUserCommitPartialWriteGuardPinsBothDirections(t *testing.T) {
	cause := errors.New("simulated ClickHouse send failure")

	t.Run("failure after rows landed is a partial write", func(t *testing.T) {
		rows, err := wrapRepoUserCommitPartialWrite(5, cause)
		if !errors.Is(err, ErrPartialWrite) {
			t.Errorf("a failure after 5 rows landed must wrap ErrPartialWrite; got %v", err)
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must survive wrapping; got %v", err)
		}
		if rows != 5 {
			t.Errorf("the TRUE rows-written count must be reported, got %d, want 5", rows)
		}
	})

	t.Run("failure with nothing written is an ordinary failure", func(t *testing.T) {
		rows, err := wrapRepoUserCommitPartialWrite(0, cause)
		if errors.Is(err, ErrPartialWrite) {
			t.Error("a failure with nothing written must NOT wrap ErrPartialWrite")
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must be returned unchanged; got %v", err)
		}
		if rows != 0 {
			t.Errorf("rows=%d, want 0", rows)
		}
	})
}

// repoUserCommitQueryConn dispatches Query() by matching a distinguishing
// substring in the query text (commits/PRs/reviews/bug-work-items all share
// this executor's loader, so they must be told apart), and PrepareBatch() by
// matching the target table name -- failing the Nth PrepareBatch call so a
// later table's failure after an earlier one already landed rows is
// observable without a real ClickHouse. Mirrors ai_governance_native_test.go's
// orderRecordingConn shape.
type repoUserCommitQueryConn struct {
	stubDriverConn
	targets  []string
	failFrom int // 1-based PrepareBatch call number; 0 means never fail
}

func (conn *repoUserCommitQueryConn) Query(_ context.Context, query string, _ ...any) (chdriver.Rows, error) {
	if strings.Contains(query, "git_commits") {
		return &oneCommitStatRow{}, nil
	}
	return &emptyRepoUserRows{}, nil
}

func (conn *repoUserCommitQueryConn) PrepareBatch(_ context.Context, query string, _ ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	table := "unknown"
	switch {
	case strings.Contains(query, "repo_metrics_daily"):
		table = "repo_metrics_daily"
	case strings.Contains(query, "user_metrics_daily"):
		table = "user_metrics_daily"
	case strings.Contains(query, "commit_metrics"):
		table = "commit_metrics"
	}
	conn.targets = append(conn.targets, table)
	if conn.failFrom > 0 && len(conn.targets) >= conn.failFrom {
		return nil, errors.New("simulated ClickHouse write failure")
	}
	return &recordingBatch{}, nil
}

// emptyRepoUserRows is a real driver.Rows that yields nothing -- used for
// every query in this executor's loader except the one commit-stats query a
// given test wants populated.
type emptyRepoUserRows struct{}

func (*emptyRepoUserRows) Next() bool                         { return false }
func (*emptyRepoUserRows) Scan(...any) error                  { return errors.New("no rows") }
func (*emptyRepoUserRows) Err() error                         { return nil }
func (*emptyRepoUserRows) Close() error                       { return nil }
func (*emptyRepoUserRows) HasData() bool                      { return false }
func (*emptyRepoUserRows) Columns() []string                  { return nil }
func (*emptyRepoUserRows) ColumnTypes() []chdriver.ColumnType { return nil }
func (*emptyRepoUserRows) Totals(...any) error                { return errors.New("no totals") }
func (*emptyRepoUserRows) ScanStruct(any) error               { return errors.New("no rows") }

// oneCommitStatRow yields exactly one commit row shaped so repouser.Compute
// produces at least one repo_metrics_daily AND one user_metrics_daily row --
// enough for a nonzero repoRows+userRows before the (failing) commit_metrics
// write.
type oneCommitStatRow struct {
	emptyRepoUserRows
	done bool
}

func (rows *oneCommitStatRow) HasData() bool { return true }

func (rows *oneCommitStatRow) Next() bool {
	if rows.done {
		return false
	}
	rows.done = true
	return true
}

func (rows *oneCommitStatRow) Scan(dest ...any) error {
	if len(dest) != 8 {
		return fmt.Errorf("unexpected commit stat column count %d", len(dest))
	}
	*(dest[0].(*uuid.UUID)) = repoUserCommitTestRepoID
	*(dest[1].(*string)) = "abc123"
	authorEmail := "alice@example.com"
	authorName := "Alice"
	*(dest[2].(**string)) = &authorEmail
	*(dest[3].(**string)) = &authorName
	*(dest[4].(*time.Time)) = time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	filePath := "main.go"
	*(dest[5].(**string)) = &filePath
	additions := int32(10)
	deletions := int32(2)
	*(dest[6].(**int32)) = &additions
	*(dest[7].(**int32)) = &deletions
	return nil
}

var repoUserCommitTestRepoID = uuid.MustParse("33333333-3333-3333-3333-333333333333")

// TestRepoUserCommitComputeFamilyReportsPartialWriteAtTheCallSite is the
// codex confirmation-pass F2 fix: TestRepoUserCommitPartialWriteGuardPinsBothDirections
// above only exercises wrapRepoUserCommitPartialWrite directly -- reverting
// ComputeFamily's own call site back to `return 0, err` would NOT be caught
// by that test. This drives the REAL ComputeFamily with a conn that lets
// repo_metrics_daily and user_metrics_daily land (from one commit row) and
// fails commit_metrics (the 3rd PrepareBatch call) immediately after,
// asserting ComputeFamily itself reports ErrPartialWrite with the true
// accumulated count, not Refused/0.
func TestRepoUserCommitComputeFamilyReportsPartialWriteAtTheCallSite(t *testing.T) {
	conn := &repoUserCommitQueryConn{failFrom: 3}
	executor, err := NewRepoUserCommitExecutor(conn)
	if err != nil {
		t.Fatal(err)
	}
	run := Run{OrganizationID: "org-42", TargetDay: time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC)}
	partition := Partition{ID: "p1", RepoIDs: []RepositoryID{RepositoryID(repoUserCommitTestRepoID.String())}}

	written, err := executor.ComputeFamily(context.Background(), run, partition)
	if !errors.Is(err, ErrPartialWrite) {
		t.Fatalf("ComputeFamily error = %v, want it to wrap ErrPartialWrite -- "+
			"a call-site revert to `return 0, err` would not be caught by this test failing", err)
	}
	if written == 0 {
		t.Fatalf("ComputeFamily written = %d, want > 0 (repo_metrics_daily/user_metrics_daily "+
			"rows that landed before the commit_metrics write failed) -- reporting 0 here is "+
			"exactly the bug this confirmation pass exists to catch", written)
	}
	if len(conn.targets) != 3 || conn.targets[2] != "commit_metrics" {
		t.Fatalf("write targets = %v, want exactly 3 ending with commit_metrics (the failing write)", conn.targets)
	}
}

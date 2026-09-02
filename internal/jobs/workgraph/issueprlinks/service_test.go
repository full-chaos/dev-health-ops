package issueprlinks

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// stubConn is the narrow ClickHouse seam: it answers each of the four reads in
// the order Loader issues them and captures the insert.
type stubConn struct {
	responses [][][]any
	queries   []string
	inserted  [][]any
	prepared  string
	queryErr  error
	batchErr  error
}

func (stub *stubConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if stub.queryErr != nil {
		return nil, stub.queryErr
	}
	stub.queries = append(stub.queries, query)
	if len(stub.responses) == 0 {
		return &stubRows{}, nil
	}
	next := stub.responses[0]
	stub.responses = stub.responses[1:]
	return &stubRows{rows: next}, nil
}

func (stub *stubConn) PrepareBatch(_ context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	if stub.batchErr != nil {
		return nil, stub.batchErr
	}
	stub.prepared = query
	return &stubBatch{conn: stub}, nil
}

type stubRows struct {
	rows   [][]any
	cursor int
}

func (rows *stubRows) Next() bool {
	if rows.cursor >= len(rows.rows) {
		return false
	}
	rows.cursor++
	return true
}

func (rows *stubRows) Scan(dest ...any) error {
	row := rows.rows[rows.cursor-1]
	if len(row) != len(dest) {
		return fmt.Errorf("stub row has %d values, scan wants %d", len(row), len(dest))
	}
	for index, value := range row {
		switch target := dest[index].(type) {
		case *string:
			*target = value.(string)
		case *uint32:
			*target = value.(uint32)
		case *uuid.UUID:
			*target = value.(uuid.UUID)
		case *time.Time:
			*target = value.(time.Time)
		default:
			return fmt.Errorf("stub cannot scan into %T", dest[index])
		}
	}
	return nil
}

func (rows *stubRows) Close() error { return nil }
func (rows *stubRows) Err() error   { return nil }
func (rows *stubRows) ScanStruct(any) error {
	return errors.New("unused")
}
func (rows *stubRows) ColumnTypes() []driver.ColumnType { return nil }
func (rows *stubRows) Totals(...any) error              { return nil }
func (rows *stubRows) Columns() []string                { return nil }
func (rows *stubRows) HasData() bool                    { return len(rows.rows) > 0 }

type stubBatch struct{ conn *stubConn }

func (batch *stubBatch) Abort() error { return nil }
func (batch *stubBatch) Append(values ...any) error {
	batch.conn.inserted = append(batch.conn.inserted, values)
	return nil
}
func (batch *stubBatch) AppendStruct(any) error        { return errors.New("unused") }
func (batch *stubBatch) Column(int) driver.BatchColumn { return nil }
func (batch *stubBatch) Flush() error                  { return nil }
func (batch *stubBatch) Send() error                   { return nil }
func (batch *stubBatch) IsSent() bool                  { return true }
func (batch *stubBatch) Rows() int                     { return len(batch.conn.inserted) }
func (batch *stubBatch) Close() error                  { return nil }
func (batch *stubBatch) Columns() []column.Interface   { return nil }

type recordingObserver struct{ outcomes []Outcome }

func (observer *recordingObserver) ObserveIssuePRLinks(outcome Outcome) {
	observer.outcomes = append(observer.outcomes, outcome)
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func stubResponsesForOneLink() [][][]any {
	return [][][]any{
		{{testOrg, testGHPRRef, testLinear, "linear_attachment", testSynced}}, // dependencies
		{{testOrg, testRepoID, testSlug}},                                     // repos
		{{testOrg, testRepoID, uint32(12)}},                                   // pull requests
		{{testOrg, testLinear}},                                               // work items
	}
}

func newTestService(t *testing.T, conn *stubConn) (*Service, *recordingObserver) {
	t.Helper()
	loader, err := NewLoader(conn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(loader, writer, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingObserver{}
	service.SetObserver(observer)
	return service, observer
}

// TestProduceWritesAndReportsTheOutcome is the end-to-end shape of the
// production path: four reads, one derivation, one insert, one telemetry
// outcome. The insert is asserted COLUMN BY COLUMN in the statement's declared
// order, because a positional mismatch against `work_graph_issue_pr` would
// write plausible-looking garbage rather than fail.
func TestProduceWritesAndReportsTheOutcome(t *testing.T) {
	conn := &stubConn{responses: stubResponsesForOneLink()}
	service, observer := newTestService(t, conn)

	outcome, err := service.Produce(context.Background(), testOrg, Window{})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if outcome.Written != 1 || outcome.DependenciesRead != 1 {
		t.Fatalf("outcome = %+v, want 1 read / 1 written", outcome)
	}
	if !outcome.Balanced {
		t.Fatalf("outcome does not balance: %+v", outcome)
	}
	if len(observer.outcomes) != 1 {
		t.Fatalf("observer saw %d outcomes, want 1", len(observer.outcomes))
	}
	if observer.outcomes[0].OrganizationID != testOrg {
		t.Fatalf("observed org %q, want %q", observer.outcomes[0].OrganizationID, testOrg)
	}

	if len(conn.inserted) != 1 {
		t.Fatalf("inserted %d rows, want 1", len(conn.inserted))
	}
	want := []any{
		testRepoID, testLinear, uint32(12), NativeConfidence,
		ProvenanceNative, "linear_attachment", testSynced, testOrg,
	}
	for index, value := range want {
		if conn.inserted[0][index] != value {
			t.Errorf("insert column %d = %v (%T), want %v (%T)",
				index, conn.inserted[0][index], conn.inserted[0][index], value, value)
		}
	}
}

// TestProduceSkipsTheRemainingReadsWhenThereAreNoDependencies pins the
// short-circuit Python has (builder.py:706-707). Three wasted full-table scans
// per org with no dependencies is the cost of getting this wrong.
func TestProduceSkipsTheRemainingReadsWhenThereAreNoDependencies(t *testing.T) {
	conn := &stubConn{responses: [][][]any{{}}}
	service, _ := newTestService(t, conn)

	outcome, err := service.Produce(context.Background(), testOrg, Window{})
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	if outcome.Written != 0 {
		t.Fatalf("wrote %d rows for an org with no dependencies", outcome.Written)
	}
	if len(conn.queries) != 1 {
		t.Fatalf("issued %d queries, want 1 (the dependency read alone): %v", len(conn.queries), conn.queries)
	}
	if len(conn.inserted) != 0 {
		t.Fatalf("inserted %d rows, want none", len(conn.inserted))
	}
}

// TestProduceStampsTheClockOnlyForAnUnusableTimestamp proves the fallback
// exists AND that it does not fire on a normal row -- a fallback that quietly
// replaced every last_synced with "now" would pass a test that only checked
// "no zero timestamps were written", and would be exactly the CHAOS-4769
// hazard this port refuses to introduce.
func TestProduceStampsTheClockOnlyForAnUnusableTimestamp(t *testing.T) {
	frozen := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

	t.Run("zero timestamp gets the clock", func(t *testing.T) {
		responses := stubResponsesForOneLink()
		responses[0][0][4] = time.Time{}
		conn := &stubConn{responses: responses}
		service, _ := newTestService(t, conn)
		service.SetClock(func() time.Time { return frozen })

		if _, err := service.Produce(context.Background(), testOrg, Window{}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if got := conn.inserted[0][6]; got != frozen {
			t.Fatalf("last_synced = %v, want the clock %v", got, frozen)
		}
	})

	t.Run("a real timestamp is preserved", func(t *testing.T) {
		conn := &stubConn{responses: stubResponsesForOneLink()}
		service, _ := newTestService(t, conn)
		service.SetClock(func() time.Time { return frozen })

		if _, err := service.Produce(context.Background(), testOrg, Window{}); err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if got := conn.inserted[0][6]; got != testSynced {
			t.Fatalf("last_synced = %v, want the dependency row's %v", got, testSynced)
		}
	})
}

func TestProducePropagatesReadFailure(t *testing.T) {
	conn := &stubConn{queryErr: errors.New("clickhouse is down")}
	service, observer := newTestService(t, conn)

	if _, err := service.Produce(context.Background(), testOrg, Window{}); err == nil {
		t.Fatal("Produce returned nil on a failed read")
	}
	if len(observer.outcomes) != 0 {
		t.Fatal("a failed run must not report a success outcome")
	}
}

func TestProducePropagatesWriteFailure(t *testing.T) {
	conn := &stubConn{responses: stubResponsesForOneLink(), batchErr: errors.New("batch refused")}
	service, observer := newTestService(t, conn)

	if _, err := service.Produce(context.Background(), testOrg, Window{}); err == nil {
		t.Fatal("Produce returned nil on a failed write")
	}
	if len(observer.outcomes) != 0 {
		t.Fatal("a failed run must not report a success outcome")
	}
}

// TestWindowFiltersAreAppliedOnlyWhenSet keeps the PR read's optional clauses
// honest: an always-on window would silently drop every PR outside a default
// range, and an always-off one would ignore the build's scope.
func TestWindowFiltersAreAppliedOnlyWhenSet(t *testing.T) {
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	repoID := testRepoID

	cases := []struct {
		name    string
		window  Window
		absent  []string
		present []string
	}{
		{"unbounded", Window{}, []string{"{from_ts", "{to_ts", "{repo_id"}, nil},
		{"from only", Window{From: &from}, []string{"{to_ts", "{repo_id"}, []string{"created_at >= {from_ts"}},
		{"to only", Window{To: &to}, []string{"{from_ts", "{repo_id"}, []string{"created_at <= {to_ts"}},
		{"repo only", Window{RepoID: &repoID}, []string{"{from_ts", "{to_ts"}, []string{"repo_id = {repo_id"}},
		{
			"all three",
			Window{From: &from, To: &to, RepoID: &repoID},
			nil,
			[]string{"created_at >= {from_ts", "created_at <= {to_ts", "repo_id = {repo_id"},
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			conn := &stubConn{responses: stubResponsesForOneLink()}
			service, _ := newTestService(t, conn)
			if _, err := service.Produce(context.Background(), testOrg, testCase.window); err != nil {
				t.Fatalf("Produce: %v", err)
			}
			if len(conn.queries) != 4 {
				t.Fatalf("issued %d queries, want 4", len(conn.queries))
			}
			prQuery := conn.queries[2]
			for _, fragment := range testCase.present {
				if !strings.Contains(prQuery, fragment) {
					t.Errorf("pull-request query is missing %q:\n%s", fragment, prQuery)
				}
			}
			for _, fragment := range testCase.absent {
				if strings.Contains(prQuery, fragment) {
					t.Errorf("pull-request query unexpectedly contains %q:\n%s", fragment, prQuery)
				}
			}
		})
	}
}

func TestConstructorsRejectNilDependencies(t *testing.T) {
	if _, err := NewLoader(nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewLoader(nil) = %v, want ErrUnavailable", err)
	}
	if _, err := NewWriter(nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewWriter(nil) = %v, want ErrUnavailable", err)
	}
	if _, err := NewService(nil, nil, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewService(nil, nil, nil) = %v, want ErrUnavailable", err)
	}
}

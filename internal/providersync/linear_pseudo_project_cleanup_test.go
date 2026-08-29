package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeLinearCleanupRows is a driver.Rows double over the (org_id, id, name,
// is_active) shape RetireLinearPseudoProjectRows' SELECT reads.
type fakeLinearCleanupRows struct {
	driver.Rows
	rows  [][4]any // org_id, id, name, is_active
	index int
}

func (r *fakeLinearCleanupRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeLinearCleanupRows) Scan(dest ...any) error {
	orgID, ok1 := dest[0].(*string)
	id, ok2 := dest[1].(*string)
	name, ok3 := dest[2].(*string)
	isActive, ok4 := dest[3].(*uint8)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return fmt.Errorf("fakeLinearCleanupRows: unexpected Scan dest shape")
	}
	*orgID = r.rows[r.index][0].(string)
	*id = r.rows[r.index][1].(string)
	*name = r.rows[r.index][2].(string)
	*isActive = r.rows[r.index][3].(uint8)
	return nil
}

func (r *fakeLinearCleanupRows) Close() error { return nil }
func (r *fakeLinearCleanupRows) Err() error   { return nil }

// fakeLinearCleanupConn is a driver.Conn double covering RetireLinearPseudo
// ProjectRows' SELECT (Query) and DELETE (Exec) calls, recording which
// mutation (if any) was actually issued so a test can assert dry-run never
// executes one.
type fakeLinearCleanupConn struct {
	driver.Conn
	rows        [][4]any
	queryErr    error
	execErr     error
	execQueries []string
}

func (f *fakeLinearCleanupConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeLinearCleanupRows{rows: f.rows, index: -1}, nil
}

func (f *fakeLinearCleanupConn) Exec(_ context.Context, query string, _ ...any) error {
	f.execQueries = append(f.execQueries, query)
	return f.execErr
}

func TestRetireLinearPseudoProjectRowsRejectsNilConn(t *testing.T) {
	_, err := RetireLinearPseudoProjectRows(context.Background(), nil, "", false)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("err=%v, want ErrInvalidConfiguration", err)
	}
}

func TestRetireLinearPseudoProjectRowsDryRunFindsButNeverDeletes(t *testing.T) {
	conn := &fakeLinearCleanupConn{rows: [][4]any{
		{"org-1", "org-1:linear:ENG", "Engineering", uint8(1)},
		{"org-1", "org-1:linear:OPS", "Operations", uint8(0)},
	}}
	outcome, err := RetireLinearPseudoProjectRows(context.Background(), conn, "", true)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.DryRun || len(outcome.Rows) != 2 || outcome.DeletedRows != 0 {
		t.Fatalf("outcome=%+v", outcome)
	}
	if len(conn.execQueries) != 0 {
		t.Fatalf("dry-run must never execute a mutation, got: %v", conn.execQueries)
	}
}

func TestRetireLinearPseudoProjectRowsRealRunDeletesAndReportsCounts(t *testing.T) {
	conn := &fakeLinearCleanupConn{rows: [][4]any{
		{"org-1", "org-1:linear:ENG", "Engineering", uint8(1)},
	}}
	outcome, err := RetireLinearPseudoProjectRows(context.Background(), conn, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.DryRun || len(outcome.Rows) != 1 || outcome.DeletedRows != 1 {
		t.Fatalf("outcome=%+v", outcome)
	}
	if len(conn.execQueries) != 1 || !strings.Contains(conn.execQueries[0], "ALTER TABLE projects DELETE") {
		t.Fatalf("expected exactly one ALTER TABLE DELETE, got: %v", conn.execQueries)
	}
	if strings.Contains(conn.execQueries[0], "org_id") {
		t.Fatalf("unscoped call (orgID=\"\") must not add an org_id predicate: %s", conn.execQueries[0])
	}
}

func TestRetireLinearPseudoProjectRowsRealRunFindsNothingSkipsMutation(t *testing.T) {
	conn := &fakeLinearCleanupConn{rows: nil}
	outcome, err := RetireLinearPseudoProjectRows(context.Background(), conn, "org-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.DryRun || len(outcome.Rows) != 0 || outcome.DeletedRows != 0 {
		t.Fatalf("outcome=%+v", outcome)
	}
	if len(conn.execQueries) != 0 {
		t.Fatalf("no matching rows -- must not issue a no-op mutation, got: %v", conn.execQueries)
	}
}

func TestRetireLinearPseudoProjectRowsScopedByOrgAddsThePredicate(t *testing.T) {
	conn := &fakeLinearCleanupConn{rows: [][4]any{
		{"org-1", "org-1:linear:ENG", "Engineering", uint8(1)},
	}}
	_, err := RetireLinearPseudoProjectRows(context.Background(), conn, "org-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(conn.execQueries) != 1 || !strings.Contains(conn.execQueries[0], "org_id") {
		t.Fatalf("org-scoped call must add an org_id predicate to the mutation, got: %v", conn.execQueries)
	}
}

func TestRetireLinearPseudoProjectRowsPropagatesQueryFailure(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeLinearCleanupConn{queryErr: boom}
	_, err := RetireLinearPseudoProjectRows(context.Background(), conn, "", false)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the query failure propagated", err)
	}
}

func TestRetireLinearPseudoProjectRowsPropagatesDeleteFailureButStillReportsFoundRows(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeLinearCleanupConn{
		rows: [][4]any{
			{"org-1", "org-1:linear:ENG", "Engineering", uint8(1)},
		},
		execErr: boom,
	}
	outcome, err := RetireLinearPseudoProjectRows(context.Background(), conn, "", false)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the delete failure propagated", err)
	}
	// The caller still learns what WOULD have been deleted, for diagnosis --
	// but DeletedRows must stay 0 since the mutation did not actually land.
	if len(outcome.Rows) != 1 || outcome.DeletedRows != 0 {
		t.Fatalf("outcome=%+v", outcome)
	}
}

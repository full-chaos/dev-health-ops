package providersync

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// fakeStaleOwnershipRows is a driver.Rows double over the (org_id,
// project_id, project_key, team_id) shape
// RetireStaleLinearProjectOwnershipRows' SELECT reads. project_key scans
// into a *string destination (Nullable(String)); every fixture row here
// carries a non-nil key since the predicate itself requires one.
type fakeStaleOwnershipRows struct {
	driver.Rows
	rows  [][4]string // org_id, project_id, project_key, team_id
	index int
}

func (r *fakeStaleOwnershipRows) Next() bool {
	r.index++
	return r.index < len(r.rows)
}

func (r *fakeStaleOwnershipRows) Scan(dest ...any) error {
	orgID, ok1 := dest[0].(*string)
	projectID, ok2 := dest[1].(*string)
	projectKey, ok3 := dest[2].(**string)
	teamID, ok4 := dest[3].(*string)
	if !ok1 || !ok2 || !ok3 || !ok4 {
		return fmt.Errorf("fakeStaleOwnershipRows: unexpected Scan dest shape")
	}
	*orgID = r.rows[r.index][0]
	*projectID = r.rows[r.index][1]
	key := r.rows[r.index][2]
	*projectKey = &key
	*teamID = r.rows[r.index][3]
	return nil
}

func (r *fakeStaleOwnershipRows) Close() error { return nil }
func (r *fakeStaleOwnershipRows) Err() error   { return nil }

// fakeStaleOwnershipConn is a driver.Conn double covering
// RetireStaleLinearProjectOwnershipRows' SELECT (Query) and DELETE (Exec)
// calls, recording which mutation (if any) was actually issued so a test can
// assert dry-run never executes one.
type fakeStaleOwnershipConn struct {
	driver.Conn
	rows        [][4]string
	queryErr    error
	execErr     error
	execQueries []string
}

func (f *fakeStaleOwnershipConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	return &fakeStaleOwnershipRows{rows: f.rows, index: -1}, nil
}

func (f *fakeStaleOwnershipConn) Exec(_ context.Context, query string, _ ...any) error {
	f.execQueries = append(f.execQueries, query)
	return f.execErr
}

func TestRetireStaleLinearProjectOwnershipRowsRejectsNilConn(t *testing.T) {
	_, err := RetireStaleLinearProjectOwnershipRows(context.Background(), nil, "", false)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("err=%v, want ErrInvalidConfiguration", err)
	}
}

func TestRetireStaleLinearProjectOwnershipRowsDryRunFindsButNeverDeletes(t *testing.T) {
	conn := &fakeStaleOwnershipConn{rows: [][4]string{
		{"org-1", "6241316a-85be-42ce-b243-8e41f2b18c8d", "CHAOS", "CHAOS"},
		{"org-1", "13e65c04-40ec-4a95-8216-f7c2ce233244", "CHAOS", "CHAOS"},
	}}
	outcome, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "", true)
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

func TestRetireStaleLinearProjectOwnershipRowsRealRunDeletesAndReportsCounts(t *testing.T) {
	conn := &fakeStaleOwnershipConn{rows: [][4]string{
		{"org-1", "6241316a-85be-42ce-b243-8e41f2b18c8d", "CHAOS", "CHAOS"},
	}}
	outcome, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.DryRun || len(outcome.Rows) != 1 || outcome.DeletedRows != 1 {
		t.Fatalf("outcome=%+v", outcome)
	}
	if outcome.Rows[0].ProjectKey != "CHAOS" {
		t.Fatalf("expected project_key to round-trip from the Nullable(String) scan, got: %+v", outcome.Rows[0])
	}
	if len(conn.execQueries) != 1 || !strings.Contains(conn.execQueries[0], "ALTER TABLE team_project_ownership DELETE") {
		t.Fatalf("expected exactly one ALTER TABLE DELETE, got: %v", conn.execQueries)
	}
	if strings.Contains(conn.execQueries[0], "{org_id:String}") {
		t.Fatalf("unscoped call (orgID=\"\") must not add a bound org_id filter: %s", conn.execQueries[0])
	}
}

func TestRetireStaleLinearProjectOwnershipRowsRealRunFindsNothingSkipsMutation(t *testing.T) {
	conn := &fakeStaleOwnershipConn{rows: nil}
	outcome, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "org-1", false)
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

func TestRetireStaleLinearProjectOwnershipRowsScopedByOrgAddsThePredicate(t *testing.T) {
	conn := &fakeStaleOwnershipConn{rows: [][4]string{
		{"org-1", "6241316a-85be-42ce-b243-8e41f2b18c8d", "CHAOS", "CHAOS"},
	}}
	_, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "org-1", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(conn.execQueries) != 1 || !strings.Contains(conn.execQueries[0], "{org_id:String}") {
		t.Fatalf("org-scoped call must add a bound org_id filter to the mutation, got: %v", conn.execQueries)
	}
}

func TestRetireStaleLinearProjectOwnershipRowsPropagatesQueryFailure(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeStaleOwnershipConn{queryErr: boom}
	_, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "", false)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the query failure propagated", err)
	}
}

func TestRetireStaleLinearProjectOwnershipRowsPropagatesDeleteFailureButStillReportsFoundRows(t *testing.T) {
	boom := errors.New("boom")
	conn := &fakeStaleOwnershipConn{
		rows: [][4]string{
			{"org-1", "6241316a-85be-42ce-b243-8e41f2b18c8d", "CHAOS", "CHAOS"},
		},
		execErr: boom,
	}
	outcome, err := RetireStaleLinearProjectOwnershipRows(context.Background(), conn, "", false)
	if !errors.Is(err, boom) {
		t.Fatalf("err=%v, want the delete failure propagated", err)
	}
	if len(outcome.Rows) != 1 || outcome.DeletedRows != 0 {
		t.Fatalf("outcome=%+v", outcome)
	}
}

// TestLinearStaleProjectOwnershipPredicateExcludesThePseudoIdentityRow pins
// the CHAOS-4560 boundary: this verb must never match the
// "{org_id}:linear:{team_key}" pseudo-identity row that RetireLinearPseudo
// ProjectRows (the `projects` table cleanup) already owns and that
// CHAOS-4560 has not yet cleared for a writer-side removal.
func TestLinearStaleProjectOwnershipPredicateExcludesThePseudoIdentityRow(t *testing.T) {
	if !strings.Contains(linearStaleProjectOwnershipPredicate, "NOT startsWith(project_id, concat(org_id, ':linear:'))") {
		t.Fatalf("predicate must exclude the own-org_id pseudo-identity shape, got: %s", linearStaleProjectOwnershipPredicate)
	}
	if !strings.Contains(linearStaleProjectOwnershipPredicate, "provider = 'linear'") {
		t.Fatalf("predicate must scope to provider='linear', got: %s", linearStaleProjectOwnershipPredicate)
	}
}

// TestLinearStaleProjectOwnershipPredicateRequiresAReplacementRow is the
// red-first proof for codex review P1 (2026-08-30): an org that has not
// re-synced since CHAOS-4530's writer fix has ONLY the stale CHAOS-keyed
// row for a real project -- deleting it unconditionally would remove that
// project's only team_project_ownership signal. The predicate must require
// a sibling project_key IS NULL row for the SAME (org_id, project_id)
// before a stale row is even a candidate.
func TestLinearStaleProjectOwnershipPredicateRequiresAReplacementRow(t *testing.T) {
	if !strings.Contains(linearStaleProjectOwnershipPredicate, "(org_id, project_id) IN (SELECT org_id, project_id FROM team_project_ownership WHERE provider = 'linear' AND project_key IS NULL)") {
		t.Fatalf("predicate must require a project_key IS NULL sibling row for the same (org_id, project_id) before matching, got: %s", linearStaleProjectOwnershipPredicate)
	}
}

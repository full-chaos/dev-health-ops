package providersync

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// contentSkipStoredConn answers the CHAOS-5045 readback with rows the test
// supplies, and records whether an INSERT was ever prepared. A batch that never
// reaches PrepareBatch is a suppressed write.
type contentSkipStoredConn struct {
	driver.Conn
	rows     [][]any
	prepared int
}

func (c *contentSkipStoredConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return &contentSkipRows{rows: c.rows}, nil
}

func (c *contentSkipStoredConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	c.prepared++
	return &contentSkipBatch{}, nil
}

type contentSkipRows struct {
	rows [][]any
	idx  int
}

func (r *contentSkipRows) Next() bool { return r.idx < len(r.rows) }
func (r *contentSkipRows) Scan(dest ...any) error {
	row := r.rows[r.idx]
	r.idx++
	for i := range dest {
		if i >= len(row) {
			break
		}
		switch target := dest[i].(type) {
		case *string:
			*target = row[i].(string)
		case **string:
			if row[i] == nil {
				*target = nil
			} else {
				value := row[i].(string)
				*target = &value
			}
		case **float64:
			if row[i] == nil {
				*target = nil
			} else {
				value := row[i].(float64)
				*target = &value
			}
		case *uint32:
			*target = row[i].(uint32)
		case *int64:
			*target = row[i].(int64)
		case *bool:
			*target = row[i].(bool)
		}
	}
	return nil
}
func (r *contentSkipRows) ScanStruct(any) error             { return nil }
func (r *contentSkipRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *contentSkipRows) Totals(...any) error              { return nil }
func (r *contentSkipRows) Columns() []string                { return nil }
func (r *contentSkipRows) Close() error                     { return nil }
func (r *contentSkipRows) Err() error                       { return nil }
func (r *contentSkipRows) HasData() bool                    { return len(r.rows) > 0 }

type contentSkipBatch struct{ driver.Batch }

func (contentSkipBatch) Append(...any) error { return nil }
func (contentSkipBatch) Abort() error        { return nil }
func (contentSkipBatch) Send() error         { return nil }

func contentSkipClaim(t *testing.T) Claim {
	t.Helper()
	return nativeTestClaim("github", "tests")
}

func contentSkipCaseRow(claim Claim, duration *float64) testCaseResultRow {
	return testCaseResultRow{
		OrgID: claim.OrgID, RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", RunID: "9001",
		SuiteID: "s1", CaseID: "c1", CaseName: "passes", Status: "passed",
		DurationSeconds: duration, LastSynced: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC),
	}
}

// storedCaseRowFor mirrors storedTestCaseRows' scan order for one stored row.
func storedCaseRowFor(row testCaseResultRow, duration any) []any {
	return []any{row.SuiteID, row.CaseID, row.CaseName, nil, row.Status, duration,
		int64(row.RetryAttempt), nil, nil, nil, row.IsQuarantined}
}

func contentSkipEffect(t *testing.T, rows []testCaseResultRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("test_case_results", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

// CHAOS-5045 r2 P1. The content skip is an OPTIMISATION; validateGitHubTestsRow
// is a fail-closed tenancy and stamp guard. When the skip ran first, a foreign
// or unstamped effect could match a stored row, return success and be reported
// as committed -- the guard stopped guarding exactly when the skip fired.
//
// Both cases below must be REFUSED, and must never reach the readback: an
// effect that fails validation is not a candidate for "already stored".
func TestGitHubTestsContentSkipCannotBypassRowValidation(t *testing.T) {
	t.Parallel()
	claim := contentSkipClaim(t)

	for _, testCase := range []struct {
		name string
		row  testCaseResultRow
	}{
		{"foreign org", func() testCaseResultRow {
			row := contentSkipCaseRow(claim, nil)
			row.OrgID = "foreign-org"
			return row
		}()},
		{"zero last_synced", func() testCaseResultRow {
			row := contentSkipCaseRow(claim, nil)
			row.LastSynced = time.Time{}
			return row
		}()},
		{"empty case id", func() testCaseResultRow {
			row := contentSkipCaseRow(claim, nil)
			row.CaseID = ""
			return row
		}()},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			// The store is primed to MATCH, so a skip-before-validate ordering
			// would return success here.
			conn := &contentSkipStoredConn{rows: [][]any{storedCaseRowFor(testCase.row, nil)}}
			sink := TestOpsClickHouseEffects{
				Conn:  conn,
				Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			}
			err := sink.WriteEffect(context.Background(), claim, contentSkipEffect(t, []testCaseResultRow{testCase.row}))
			if err == nil {
				t.Fatalf("an invalid row was ACCEPTED (%s): validation ran after the "+
					"content skip, so the fail-closed guard was bypassed", testCase.name)
			}
			if conn.prepared != 0 {
				t.Fatalf("an invalid row reached PrepareBatch (%s)", testCase.name)
			}
		})
	}
}

// Reachability positive control for the test above: with the SAME priming and a
// VALID row, the skip really does fire. Without this, the refusals above could
// pass simply because the skip never runs at all.
func TestGitHubTestsContentSkipIsReachableForValidRows(t *testing.T) {
	t.Parallel()
	claim := contentSkipClaim(t)
	row := contentSkipCaseRow(claim, nil)
	conn := &contentSkipStoredConn{rows: [][]any{storedCaseRowFor(row, nil)}}
	var skipped int
	sink := TestOpsClickHouseEffects{
		Conn:          conn,
		Lease:         providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		OnContentSkip: func(string, int, int) { skipped++ },
	}
	if err := sink.WriteEffect(context.Background(), claim, contentSkipEffect(t, []testCaseResultRow{row})); err != nil {
		t.Fatalf("a valid, already-stored row was refused: %v", err)
	}
	if conn.prepared != 0 {
		t.Fatal("an already-stored batch was still inserted; the skip did not fire")
	}
	// CHAOS-5045 r2 P3: a suppressed insert must be observable. Without this the
	// skip is indistinguishable from a real write.
	if skipped != 1 {
		t.Fatalf("content skip observed %d times, want 1 -- a suppressed insert is "+
			"otherwise reported exactly like a physical write", skipped)
	}
}

// CHAOS-5045 r2 P2. Go's == treats -0 and +0 as equal, and both report parsers
// accept a signed zero, so a duration flipping sign would have compared equal
// and the write would have been suppressed.
func TestGitHubTestsContentSkipDetectsASignedZeroChange(t *testing.T) {
	t.Parallel()
	claim := contentSkipClaim(t)
	negativeZero := math.Copysign(0, -1)
	row := contentSkipCaseRow(claim, &negativeZero)

	// Stored as +0; incoming is -0. Different bit patterns, so this is a change.
	conn := &contentSkipStoredConn{rows: [][]any{storedCaseRowFor(row, float64(0))}}
	sink := TestOpsClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, contentSkipEffect(t, []testCaseResultRow{row})); err != nil {
		t.Fatal(err)
	}
	if conn.prepared != 1 {
		t.Fatalf("a signed-zero duration change was suppressed (PrepareBatch called %d times): "+
			"the float comparison is using == rather than the bit pattern", conn.prepared)
	}
}

// Control for the signed-zero test: an IDENTICAL float must still skip, so the
// test above proves discrimination rather than a comparator that never matches.
func TestGitHubTestsContentSkipStillFiresOnAnIdenticalFloat(t *testing.T) {
	t.Parallel()
	claim := contentSkipClaim(t)
	duration := 2.25
	row := contentSkipCaseRow(claim, &duration)
	conn := &contentSkipStoredConn{rows: [][]any{storedCaseRowFor(row, 2.25)}}
	sink := TestOpsClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
	if err := sink.WriteEffect(context.Background(), claim, contentSkipEffect(t, []testCaseResultRow{row})); err != nil {
		t.Fatal(err)
	}
	if conn.prepared != 0 {
		t.Fatal("an identical float was treated as a change; the skip no longer fires at all")
	}
}

package reviewedges

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// CHAOS-7786: totalCount is the number of deduplicated (pair, day) rows the filters match, not
// the number returned, and truncated says the list was cut.

func norm(statement string) string { return strings.Join(strings.Fields(statement), " ") }

func rowsFor(n int) [][]any {
	day := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{"rev", "auth", uint32(n - i), day, "repo-a"}
	}
	return rows
}

func resolveWith(t *testing.T, client *fakeClient, scope Scope, limit int) (int, bool, int) {
	t.Helper()
	got, err := ResolveScoped(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), scope, limit)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	return got.TotalCount, got.Truncated, len(got.Edges)
}

func TestTotalCount_IsTheCountQueryNotTheReturnedRows(t *testing.T) {
	total := uint64(1820)
	client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(3)}, total: &total}
	gotTotal, truncated, returned := resolveWith(t, client, Scope{}, 3)
	if gotTotal != 1820 || returned != 3 || !truncated {
		t.Fatalf("total=%d truncated=%v returned=%d, want 1820/true/3", gotTotal, truncated, returned)
	}
}

func TestTruncated_IsFalseWhenEveryRowIsReturned(t *testing.T) {
	// Exactly at the cap: 3 rows available, limit 3: nothing is cut.
	total := uint64(3)
	client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(3)}, total: &total}
	gotTotal, truncated, returned := resolveWith(t, client, Scope{}, 3)
	if gotTotal != 3 || returned != 3 || truncated {
		t.Fatalf("total=%d truncated=%v returned=%d, want 3/false/3 (a list that is not cut must not say it is)", gotTotal, truncated, returned)
	}
	// And one more available than the cap is cut.
	total = 4
	client = &fakeClient{response: &fakeRowScanner{rows: rowsFor(3)}, total: &total}
	if _, truncated, _ := resolveWith(t, client, Scope{}, 3); !truncated {
		t.Error("4 rows available with a cap of 3 must be truncated")
	}
}

func TestTotalCount_NeverBelowTheReturnedRows(t *testing.T) {
	// Two reads, eventual merges: a count smaller than the rows read must not report fewer than
	// were returned, and must not claim a cut.
	total := uint64(1)
	client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(3)}, total: &total}
	gotTotal, truncated, returned := resolveWith(t, client, Scope{}, 500)
	if gotTotal != 3 || returned != 3 || truncated {
		t.Fatalf("total=%d truncated=%v returned=%d, want 3/false/3", gotTotal, truncated, returned)
	}
}

func TestTotalCount_EmptyResultIsZeroAndNotTruncated(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	gotTotal, truncated, returned := resolveWith(t, client, Scope{}, 500)
	if gotTotal != 0 || returned != 0 || truncated {
		t.Fatalf("total=%d truncated=%v returned=%d, want 0/false/0", gotTotal, truncated, returned)
	}
}

// The count must read the same rows the list is cut from: same org, window, repo and team
// filters, same dedup. Compared as text, for every kind of scope.
func TestCountQuery_UsesTheSameWhereAsTheRows(t *testing.T) {
	scopes := map[string]Scope{
		"no scope":    {},
		"repo scope":  {RepoIDs: []string{"repo-a", "repo-b"}},
		"team scope":  {TeamIDs: []string{"team-a"}},
		"repo + team": {RepoIDs: []string{"repo-a"}, TeamIDs: []string{"team-a"}},
		"team + asOf": {TeamIDs: []string{"team-a"}, AsOf: time.Date(2026, 8, 15, 0, 0, 0, 0, time.UTC)},
	}
	for name, scope := range scopes {
		t.Run(name, func(t *testing.T) {
			client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(1)}}
			resolveWith(t, client, scope, 500)
			if client.countCalls != 1 {
				t.Fatalf("count query issued %d times, want once", client.countCalls)
			}
			count := norm(client.countStatement)
			const prefix = "SELECT count() FROM ("
			if !strings.HasPrefix(count, prefix) || !strings.HasSuffix(count, ")") {
				t.Fatalf("count statement shape: %s", count)
			}
			inner := strings.TrimSuffix(strings.TrimPrefix(count, prefix), ")")
			if !strings.Contains(norm(client.statement), inner) {
				t.Errorf("the count's inner query is not the rows' inner query:\n count inner: %s\n rows:        %s", inner, norm(client.statement))
			}
			for _, banned := range []string{"ORDER BY", "LIMIT"} {
				if strings.Contains(count, banned) {
					t.Errorf("the count statement must not %s: %s", banned, count)
				}
			}
			// Same bindings as the rows, except the limit.
			var wantBindings []clickhouse.Binding
			for _, b := range client.bindings {
				if b.Name != "limit" {
					wantBindings = append(wantBindings, b)
				}
			}
			if !reflect.DeepEqual(client.countBindings, wantBindings) {
				t.Errorf("count bindings %#v, want the rows' bindings without limit: %#v", client.countBindings, wantBindings)
			}
		})
	}
}

func TestCountQuery_ErrorPropagatesWithNoDegradedPath(t *testing.T) {
	boom := errors.New("count failed")
	client := &fakeClient{response: &fakeRowScanner{rows: rowsFor(1)}, countErr: boom}
	_, err := ResolveScoped(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 500)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap the count error (no silent fallback to len(edges))", err)
	}
}

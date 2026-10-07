package daily

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
)

func workItemScopeSet(count int, scope func(int) string) map[workItemScopeKey]struct{} {
	scopes := make(map[workItemScopeKey]struct{}, count)
	for index := 0; index < count; index++ {
		scopes[workItemScopeKey{provider: "github", scope: scope(index)}] = struct{}{}
	}
	return scopes
}

// The count bound: the filter is used at the bound and dropped one value
// above it, and the reason names the count.
func TestWorkItemScopeFilterIsDroppedAboveTheValueBound(t *testing.T) {
	short := func(index int) string { return fmt.Sprintf("s%04d", index) }

	values, emptyScope, above := workItemScopeFilter(workItemScopeSet(maxWorkItemScopeFilterValues, short))
	if above != "" || len(values) != maxWorkItemScopeFilterValues || emptyScope {
		t.Fatalf("at the bound: values = %d, emptyScope = %v, above = %q; want %d values and a filter",
			len(values), emptyScope, above, maxWorkItemScopeFilterValues)
	}
	if size := workItemScopeFilterRenderedBytes(values); size > maxWorkItemScopeFilterBytes {
		t.Fatalf("the case is above the byte bound too (%d bytes): it does not isolate the value bound", size)
	}

	values, _, above = workItemScopeFilter(workItemScopeSet(maxWorkItemScopeFilterValues+1, short))
	if above != workItemScopeFilterAboveValues || values != nil {
		t.Fatalf("one above the bound: values = %d, above = %q; want no values and %q",
			len(values), above, workItemScopeFilterAboveValues)
	}
}

// The value bound is 2000, the number the pipeline document gives. The
// numbers are written out here: a test that takes them from the constant
// passes for any value of it.
func TestWorkItemScopeFilterValueBoundIsTwoThousand(t *testing.T) {
	short := func(index int) string { return fmt.Sprintf("s%04d", index) }

	values, emptyScope, above := workItemScopeFilter(workItemScopeSet(2000, short))
	if above != "" || len(values) != 2000 || emptyScope {
		t.Fatalf("2000 scopes: values = %d, emptyScope = %v, above = %q; want a filter of 2000 values",
			len(values), emptyScope, above)
	}
	if size := workItemScopeFilterRenderedBytes(values); size > maxWorkItemScopeFilterBytes {
		t.Fatalf("the case is above the byte bound too (%d bytes): it does not isolate the value bound", size)
	}

	values, _, above = workItemScopeFilter(workItemScopeSet(2001, short))
	if above != workItemScopeFilterAboveValues || values != nil {
		t.Fatalf("2001 scopes: values = %d, above = %q; want no filter and %q",
			len(values), above, workItemScopeFilterAboveValues)
	}
}

// The byte bound is the rendered size of the array literal: a set whose
// literal is exactly the bound is filtered, one byte more is not, and the
// reason names the bytes. The values need an escape, so a raw byte count would
// call both cases small.
func TestWorkItemScopeFilterIsDroppedAboveTheRenderedByteBound(t *testing.T) {
	// 100 values. Each renders as 2 quotes + 2 bytes for each of the quote
	// characters in it; the separators and brackets add 2 * 100 bytes.
	const count = 100
	build := func(total int) map[workItemScopeKey]struct{} {
		perValue := (total - 2*count) / count
		rest := (total - 2*count) % count
		return workItemScopeSet(count, func(index int) string {
			rendered := perValue
			if index < rest {
				rendered++
			}
			// A 3-byte prefix keeps the values distinct; the body is quote
			// characters (2 rendered bytes each) and one filler byte when the
			// length is odd.
			body := rendered - 2 - 3
			return fmt.Sprintf("%03d", index) + strings.Repeat("'", body/2) + strings.Repeat("x", body%2)
		})
	}

	atBound := build(maxWorkItemScopeFilterBytes)
	values, _, above := workItemScopeFilter(atBound)
	if size := workItemScopeFilterRenderedBytes(values); above != "" || size != maxWorkItemScopeFilterBytes {
		t.Fatalf("at the bound: rendered = %d, above = %q; want %d and a filter", size, above, maxWorkItemScopeFilterBytes)
	}
	raw := 0
	for _, value := range values {
		raw += len(value)
	}
	if raw >= maxWorkItemScopeFilterBytes*2/3 {
		t.Fatalf("raw size %d: the case does not tell a rendered count from a raw count", raw)
	}

	values, _, above = workItemScopeFilter(build(maxWorkItemScopeFilterBytes + 1))
	if above != workItemScopeFilterAboveBytes || values != nil {
		t.Fatalf("one byte above: values = %d, above = %q; want no values and %q",
			len(values), above, workItemScopeFilterAboveBytes)
	}
}

// The rendered size is the size of the literal clickhouse-go writes.
func TestWorkItemScopeFilterRenderedBytesIsTheArrayLiteral(t *testing.T) {
	for _, values := range [][]string{nil, {"a"}, {"a", `b'c`, `d\e`}} {
		rendered := make([]string, 0, len(values))
		for _, value := range values {
			rendered = append(rendered, strings.Repeat("?", querybound.RenderedStringLen(value)))
		}
		want := len("[" + strings.Join(rendered, ", ") + "]")
		if got := workItemScopeFilterRenderedBytes(values); got != want {
			t.Errorf("rendered bytes of %q = %d, want %d", values, got, want)
		}
	}
}

func TestWorkItemScopeFilterValues(t *testing.T) {
	values, emptyScope, above := workItemScopeFilter(map[workItemScopeKey]struct{}{
		{provider: "github", scope: "shared"}: {},
		{provider: "jira", scope: "shared"}:   {},
		{provider: "jira", scope: "ALPHA"}:    {},
		{provider: "linear", scope: ""}:       {},
	})
	if got := strings.Join(values, ","); got != "ALPHA,shared" || !emptyScope || above != "" {
		t.Fatalf("values = %q, emptyScope = %v, above = %q; want one sorted value for each scope id, "+
			"the empty scope as a flag and not as a value", got, emptyScope, above)
	}
	if _, emptyScope, _ = workItemScopeFilter(map[workItemScopeKey]struct{}{{provider: "jira", scope: "ALPHA"}: {}}); emptyScope {
		t.Fatal("emptyScope is set for a set with no empty scope")
	}
}

func TestWorkItemScopeFilterSQL(t *testing.T) {
	if got := workItemScopeFilterSQL(true, false); got != "" {
		t.Fatalf("an unfiltered read has filter text %q", got)
	}
	columns := []string{"has(scopes, project_key)", "has(scopes, project_id)", "has(scopes, project_name)", "has(scopes, native_team_key)"}
	emptyArm := "project_key = '' AND project_id = '' AND project_name = '' AND native_team_key = ''"
	withoutEmpty := workItemScopeFilterSQL(false, true)
	withEmpty := workItemScopeFilterSQL(true, true)
	for _, column := range columns {
		if !strings.Contains(withoutEmpty, column) || !strings.Contains(withEmpty, column) {
			t.Errorf("the filter does not test %s", column)
		}
	}
	if strings.Contains(withoutEmpty, emptyArm) {
		t.Error("the empty-scope arm is in a filter that has no empty scope")
	}
	if !strings.Contains(withEmpty, emptyArm) {
		t.Error("the empty-scope arm is missing")
	}
}

func scopedRow(provider, id, projectID string, repoID uuid.UUID, lastSynced time.Time) workItemScopedRow {
	row := workItemScopedRow{RepoID: repoID, LastSynced: lastSynced}
	row.Provider, row.WorkItemID, row.ProjectID = provider, id, projectID
	return row
}

// The provider is a part of a work scope: an item of another provider with
// the same scope id is not an item of the scope.
func TestKeepWorkItemsOfScopesKeepsTheProviderAndTheScope(t *testing.T) {
	now := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	rows := []workItemScopedRow{
		scopedRow("github", "wanted", "shared", uuid.Nil, now),
		scopedRow("gitlab", "other-provider", "shared", uuid.Nil, now),
		scopedRow("github", "other-scope", "elsewhere", uuid.Nil, now),
	}
	kept := keepWorkItemsOfScopes(rows, map[workItemScopeKey]struct{}{{provider: "github", scope: "shared"}: {}})
	if len(kept) != 1 || kept[0].WorkItemID != "wanted" {
		t.Fatalf("kept = %+v, want the github item of scope shared only", kept)
	}
}

func TestCountWorkItemOncePerProviderAndID(t *testing.T) {
	older := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	lowRepo := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	highRepo := uuid.MustParse("00000000-0000-0000-0000-00000000000b")

	for _, test := range []struct {
		name           string
		rows           []workItemScopedRow
		wantRepo       uuid.UUID
		wantItems      int
		wantDuplicates int
	}{
		{"the newer version is the item, read first",
			[]workItemScopedRow{scopedRow("github", "1", "p", highRepo, newer), scopedRow("github", "1", "p", lowRepo, older)}, highRepo, 1, 1},
		{"the newer version is the item, read last",
			[]workItemScopedRow{scopedRow("github", "1", "p", lowRepo, older), scopedRow("github", "1", "p", highRepo, newer)}, highRepo, 1, 1},
		{"equal versions: the lower repository id, read first",
			[]workItemScopedRow{scopedRow("github", "1", "p", lowRepo, newer), scopedRow("github", "1", "p", highRepo, newer)}, lowRepo, 1, 1},
		{"equal versions: the lower repository id, read last",
			[]workItemScopedRow{scopedRow("github", "1", "p", highRepo, newer), scopedRow("github", "1", "p", lowRepo, newer)}, lowRepo, 1, 1},
		{"one id of two providers is two items",
			[]workItemScopedRow{scopedRow("github", "1", "p", lowRepo, newer), scopedRow("gitlab", "1", "p", highRepo, newer)}, lowRepo, 2, 0},
		{"three stored versions are one item and two duplicates",
			[]workItemScopedRow{scopedRow("github", "1", "p", lowRepo, older), scopedRow("github", "1", "p", highRepo, newer), scopedRow("github", "1", "p", uuid.Nil, older)}, highRepo, 1, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			kept, duplicates := countWorkItemOncePerProviderAndID(test.rows)
			if len(kept) != test.wantItems || duplicates != test.wantDuplicates {
				t.Fatalf("items = %d, duplicates = %d; want %d and %d", len(kept), duplicates, test.wantItems, test.wantDuplicates)
			}
			if kept[0].RepoID != test.wantRepo {
				t.Fatalf("the kept row is of repository %s, want %s", kept[0].RepoID, test.wantRepo)
			}
		})
	}
}

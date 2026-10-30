package repoinventory

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalKeepsWellFormedSlugsMergedAndSorted(t *testing.T) {
	got, err := Canonical([]Row{
		{"Acme/Widget", 2},
		{" acme/widget ", 3},
		{"acme/other", 1},
		{"no-slash", 5},
		{"a/b/c", 5},
		{"-bad/name", 5},
		{"ok/na me", 5},
		{"", 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Item{{Slug: "acme/other", Count: 1}, {Slug: "acme/widget", Count: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Canonical = %v, want %v", got, want)
	}
}

func TestCanonicalEmptyInventoryIsANonNilEmptySlice(t *testing.T) {
	for _, rows := range [][]Row{nil, {}, {{"not a slug", 1}}} {
		got, err := Canonical(rows)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("Canonical(%v) = %#v, %v; want a non-nil empty slice", rows, got, err)
		}
	}
}

func TestCanonicalRefusesAnInventoryAboveTheLimit(t *testing.T) {
	var rows []Row
	for i := 0; i < Limit; i++ {
		rows = append(rows, Row{fmt.Sprintf("org/repo%d", i), 1})
	}
	if got, err := Canonical(rows); err != nil || len(got) != Limit {
		t.Fatalf("exactly %d slugs must be accepted: %d, %v", Limit, len(got), err)
	}
	rows = append(rows, Row{"org/one-too-many", 1})
	if got, err := Canonical(rows); !errors.Is(err, ErrOverLimit) || got != nil {
		t.Fatalf("Canonical above the limit = %v, %v; want ErrOverLimit", got, err)
	}
	if QueryLimit != Limit+1 {
		t.Fatalf("QueryLimit = %d, want Limit+1 so an over-limit inventory is seen", QueryLimit)
	}
}

func TestStatementReadsTheOrganizationsReposAndEndsWithTheSettings(t *testing.T) {
	sql := Statement("SETTINGS max_execution_time = 7")
	for _, want := range []string{"FROM repos FINAL", "WHERE org_id = {org_id:String}", "LIMIT {limit_n:UInt32}", "ORDER BY value"} {
		if !strings.Contains(sql, want) {
			t.Errorf("Statement lacks %q:\n%s", want, sql)
		}
	}
	if !strings.HasSuffix(strings.TrimSpace(sql), "SETTINGS max_execution_time = 7") {
		t.Errorf("Statement does not end with its settings clause:\n%s", sql)
	}
}

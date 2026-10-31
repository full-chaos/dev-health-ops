package projectinventory

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestCanonicalKeepsPairsAsStoredSortedAndUnique(t *testing.T) {
	got, err := Canonical([]Item{
		{Provider: "linear", ID: "P-2"}, {Provider: "jira", ID: "10001"}, {Provider: "linear", ID: "P-2"},
		{Provider: "", ID: "orphan"}, {Provider: "jira", ID: ""}, {Provider: "Linear", ID: "p-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []Item{{Provider: "Linear", ID: "p-1"}, {Provider: "jira", ID: "10001"}, {Provider: "linear", ID: "P-2"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Canonical = %+v, want %+v (no lowercasing, no trimming, empty pairs dropped, duplicates merged)", got, want)
	}
}

func TestCanonicalEmptyIsNonNilEmpty(t *testing.T) {
	for name, rows := range map[string][]Item{"nil": nil, "only empty pairs": {{Provider: "", ID: ""}}} {
		got, err := Canonical(rows)
		if err != nil || got == nil || len(got) != 0 {
			t.Fatalf("%s: Canonical = %#v, %v; want a non-nil empty slice", name, got, err)
		}
	}
}

func TestCanonicalRefusesAnInventoryAboveTheLimit(t *testing.T) {
	rows := make([]Item, 0, Limit+1)
	for i := 0; i <= Limit; i++ {
		rows = append(rows, Item{Provider: "jira", ID: fmt.Sprintf("%04d", i)})
	}
	if got, err := Canonical(rows[:Limit]); err != nil || len(got) != Limit {
		t.Fatalf("exactly Limit projects: %d, %v", len(got), err)
	}
	if got, err := Canonical(rows); !errors.Is(err, ErrOverLimit) || got != nil {
		t.Fatalf("Limit+1 projects: %d, %v; want ErrOverLimit and no list", len(got), err)
	}
	if QueryLimit != Limit+1 {
		t.Fatalf("QueryLimit = %d, want Limit+1 so an over-limit inventory is seen", QueryLimit)
	}
}

func TestStatementReadsTheOrgsProjectsRows(t *testing.T) {
	sql := Statement("SETTINGS max_execution_time = 5")
	for _, part := range []string{"FROM projects FINAL", "org_id = {org_id:String}", "provider != ''", "id != ''",
		"ORDER BY provider, id", "LIMIT {limit_n:UInt32}", "SETTINGS max_execution_time = 5"} {
		if !strings.Contains(sql, part) {
			t.Fatalf("statement lacks %q:\n%s", part, sql)
		}
	}
}

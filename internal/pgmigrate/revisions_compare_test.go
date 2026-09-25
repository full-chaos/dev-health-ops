package pgmigrate_test

import (
	"sort"
	"strings"
	"testing"
)

// compareRevisionText compares dho's `current`/`heads` text with what Alembic
// printed for the same state. Two rules, and the reason for each:
//
//   - Alembic's LINE ORDER is not a contract. `alembic current` and `alembic
//     heads` iterate a set of Script objects, so with two heads the order
//     changes between processes (12 fresh processes over the two real heads
//     printed "0066 0138" 11 times and "0138 0066" once). An exact text compare
//     against the live producer therefore flakes; the lines are compared as a
//     multiset.
//   - dho's order IS a contract, and it is pinned exactly here: dho's lines are
//     compared, unsorted, with Alembic's lines sorted by revision, so dho must
//     print them sorted by revision. The frozen golden
//     (TestRevisionsMatchTheFrozenAlembicOutput) pins the same order byte for
//     byte, so a swap of dho's two heads fails there and here.
//
// It returns "" when the text agrees, else a description of the difference.
func compareRevisionText(got, want string) string {
	gotLines, wantLines := splitLines(got), splitLines(want)
	sortedWant := append([]string(nil), wantLines...)
	sort.Strings(sortedWant)
	if strings.Join(gotLines, "\n") != strings.Join(sortedWant, "\n") {
		return "the lines differ or dho's are not sorted by revision (Alembic's own order is ignored): dho " + strings.Join(gotLines, " | ") + ", Alembic " + strings.Join(wantLines, " | ")
	}
	return ""
}

func splitLines(text string) []string {
	if text == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(text, "\n"), "\n")
}

func TestCompareRevisionText(t *testing.T) {
	const sorted = "0066 (head)\n0138 (head)\n"
	const swapped = "0138 (head)\n0066 (head)\n"
	tests := []struct {
		name      string
		got, want string
		ok        bool
	}{
		{"same text", sorted, sorted, true},
		{"Alembic printed the heads in the other order", sorted, swapped, true},
		{"dho printed the heads swapped", swapped, sorted, false},
		{"dho printed the heads swapped, Alembic too", swapped, swapped, false},
		{"a different line", "0066 (head)\n0137\n", sorted, false},
		{"a missing line", "0138 (head)\n", sorted, false},
		{"an extra line", sorted + "0139\n", sorted, false},
		{"both empty", "", "", true},
		{"dho empty, Alembic not", "", sorted, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := compareRevisionText(tt.got, tt.want); (diff == "") != tt.ok {
				t.Fatalf("compareRevisionText(%q, %q) = %q, want ok=%v", tt.got, tt.want, diff, tt.ok)
			}
		})
	}
}

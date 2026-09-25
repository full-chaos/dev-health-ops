package pgmigrate_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// compareRevisionText compares dho's text for one verb with what Alembic printed
// for the same state. Rules, and the reason for each:
//
//   - `heads`: exact. Alembic lists them from the script directory's sorted head
//     tuple (30 fresh processes over the two real heads printed the same order
//     every time), so a wrong order between two heads must fail.
//   - `current`: Alembic's LINE ORDER is not a contract. `alembic current` iterates
//     a set of Script objects, so with two recorded heads the order changes
//     between processes (12 fresh processes over the two real heads printed
//     "0066 0138" 11 times and "0138 0066" once). An exact text compare against
//     the live producer therefore flakes; the lines are compared as a multiset.
//   - dho's order IS a contract, and it is pinned exactly for both: dho's lines are
//     compared, unsorted, with Alembic's lines sorted by revision, so dho must
//     print them sorted by revision. The frozen golden
//     (TestRevisionsMatchTheFrozenAlembicOutput) pins the same order byte for
//     byte, so a swap of dho's two heads fails there and here.
//
// It returns "" when the text agrees, else a description of the difference.
func compareRevisionText(verb, got, want string) string {
	if verb == "heads" {
		if got != want {
			return fmt.Sprintf("dho printed %q, Alembic printed %q", got, want)
		}
		return ""
	}
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
		verb      string
		got, want string
		ok        bool
	}{
		{"same text", "current", sorted, sorted, true},
		{"Alembic printed the heads in the other order", "current", sorted, swapped, true},
		{"dho printed the heads swapped", "current", swapped, sorted, false},
		{"dho printed the heads swapped, Alembic too", "current", swapped, swapped, false},
		{"a different line", "current", "0066 (head)\n0137\n", sorted, false},
		{"a missing line", "current", "0138 (head)\n", sorted, false},
		{"an extra line", "current", sorted + "0139\n", sorted, false},
		{"both empty", "current", "", "", true},
		{"dho empty, Alembic not", "current", "", sorted, false},
		{"heads: same text", "heads", sorted, sorted, true},
		{"heads: Alembic printed the other order", "heads", sorted, swapped, false},
		{"heads: dho printed the other order", "heads", swapped, sorted, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if diff := compareRevisionText(tt.verb, tt.got, tt.want); (diff == "") != tt.ok {
				t.Fatalf("compareRevisionText(%q, %q, %q) = %q, want ok=%v", tt.verb, tt.got, tt.want, diff, tt.ok)
			}
		})
	}
}

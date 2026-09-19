package goapiproof

import "testing"

// TestCandidateAccounting_CheckNamesEachViolation runs one cell per clause
// of the invariant and asserts the exact reason check reports.
func TestCandidateAccounting_CheckNamesEachViolation(t *testing.T) {
	row := func(id, title string, rank int) any {
		return map[string]any{"id": id, "title": title, "created_at": pageCutTS(rank)}
	}
	accounting := func(limit int) *CandidateAccounting {
		return &CandidateAccounting{IDField: "id", SortField: "created_at", CopyRule: &DuplicateCopyRule{RewrittenFields: []string{"title"}}, PageLimit: limit}
	}
	base := []any{row("A", "old", 1), row("A", "new", 1), row("B", "x", 2)}
	cases := []struct {
		name       string
		accounting *CandidateAccounting
		base, cand []any
		wantOK     bool
		wantReason string
	}{
		{"accounted, shorter", accounting(50), base, []any{row("A", "new", 1), row("B", "x", 2)}, true, ""},
		{"accounted, equal after a cut", accounting(3), base, []any{row("A", "old", 1), row("B", "x", 2), row("C", "x", 3)}, true, ""},
		{"nil accounting", nil, base, []any{row("Z", "x", 9)}, true, ""},
		{"over the limit", accounting(3), base, []any{row("A", "new", 1), row("B", "x", 2), row("C", "x", 3), row("D", "x", 4)}, false, "candidate carries 4 rows, over the request's limit 3"},
		{"baseline row without id", accounting(50), []any{map[string]any{"title": "x"}}, []any{}, false, "baseline row 0 carries no id"},
		{"reordered", accounting(50), base, []any{row("B", "x", 2), row("A", "new", 1)}, false, "candidate rows are not ordered created_at DESC"},
		{"candidate row without id", accounting(50), base, []any{map[string]any{"created_at": pageCutTS(1)}}, false, "candidate row 0 carries no id"},
		{"candidate repeats an id", accounting(50), base, []any{row("A", "new", 1), row("A", "new", 1), row("B", "x", 2)}, false, `candidate repeats id "A"`},
		{"altered row", accounting(50), base, []any{row("A", "other", 1), row("B", "x", 2)}, false, `candidate row "A" equals no baseline copy under the declared rewritten title rule`},
		{"invented row on an empty baseline", accounting(50), []any{}, []any{row("Z", "x", 1)}, false, "candidate carries rows the empty baseline never reached"},
		{"empty baseline, empty candidate", accounting(50), []any{}, []any{}, true, ""},
		{"dropped row", accounting(50), base, []any{row("A", "new", 1)}, false, `baseline id "B" is absent from the candidate`},
		{"invented row on an uncut page", accounting(50), base, []any{row("A", "new", 1), row("B", "x", 2), row("C", "x", 3)}, false, "candidate row 2 carries an id the baseline lacks on an uncut page"},
		{"invented row inside the shared rows", accounting(3), base, []any{row("A", "new", 1), row("C", "x", 1), row("B", "x", 2)}, false, "candidate row 1 carries an id the baseline lacks inside the shared rows"},
		{"no page limit claims no cut", accounting(0), base, []any{row("A", "new", 1), row("B", "x", 2), row("C", "x", 3)}, false, "candidate row 2 carries an id the baseline lacks on an uncut page"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, reason := c.accounting.check(c.base, c.cand)
			if ok != c.wantOK || reason != c.wantReason {
				t.Fatalf("check = %v %q, want %v %q", ok, reason, c.wantOK, c.wantReason)
			}
			if got := c.accounting.holds(c.base, c.cand); got != c.wantOK {
				t.Fatalf("holds = %v, want %v", got, c.wantOK)
			}
		})
	}
}

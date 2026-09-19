package goapiproof

import (
	"strings"
	"testing"
)

// TestTimestampRenderingShape_InputDomain runs every cell of the shape's
// input domain in one pass through Compare: one leaf "ts" under a cited
// path, the baseline and candidate values as raw JSON.
func TestTimestampRenderingShape_InputDomain(t *testing.T) {
	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.ts"},
		Intermittent: true, IntermittentReason: "test fixture",
		TimestampRenderingShape: &TimestampRenderingShape{},
	}}}
	cases := []struct {
		name                string
		baseline, candidate string
		wantOutside         int
	}{
		{"same instant, naive and Z", `"2024-01-02T03:04:05"`, `"2024-01-02T03:04:05Z"`, 0},
		{"same instant, naive and offset", `"2024-01-02T03:04:05"`, `"2024-01-02T05:04:05+02:00"`, 0},
		{"same instant, fractional seconds", `"2024-01-02T03:04:05.5"`, `"2024-01-02T03:04:05.500Z"`, 0},
		{"different instant", `"2024-01-02T03:04:05"`, `"2024-01-02T03:04:06Z"`, 1},
		{"different instant by one day", `"2024-01-02T03:04:05"`, `"2024-01-03T03:04:05Z"`, 1},
		{"null baseline", `null`, `"2024-01-02T03:04:05Z"`, 1},
		{"null candidate", `"2024-01-02T03:04:05"`, `null`, 1},
		{"empty string", `""`, `"2024-01-02T03:04:05Z"`, 1},
		{"date only", `"2024-01-02"`, `"2024-01-02T00:00:00Z"`, 1},
		{"not a timestamp", `"ABC-123"`, `"ABC-124"`, 1},
		{"trailing bytes", `"2024-01-02T03:04:05x"`, `"2024-01-02T03:04:05Z"`, 1},
		{"number and string", `1`, `"2024-01-02T03:04:05Z"`, 1},
		{"two numbers", `1`, `2`, 1},
		{"container", `["2024-01-02T03:04:05"]`, `["2024-01-02T03:04:06Z"]`, 1},
		{"equal values", `"2024-01-02T03:04:05Z"`, `"2024-01-02T03:04:05Z"`, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline := restSnapshotFromJSON(t, `{"ts":`+c.baseline+`}`)
			candidate := restSnapshotFromJSON(t, `{"ts":`+c.candidate+`}`)
			result := Compare(baseline, candidate, opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// TestTimestampRenderingShape_KeyedListLeaves covers a leaf reached through
// an order-insensitive keyed list, where the finding's own index is not a
// body position.
func TestTimestampRenderingShape_KeyedListLeaves(t *testing.T) {
	opts := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{{Path: "data.items", KeyFields: []string{"id"}, Reason: "test fixture", Ticket: "ABC-123"}},
		BaselineDefects: []BaselineDefect{{
			Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.items.ts"},
			Intermittent: true, IntermittentReason: "test fixture",
			TimestampRenderingShape: &TimestampRenderingShape{},
		}},
	}
	baseline := restSnapshotFromJSON(t, `{"items":[{"id":"a","ts":"2024-01-02T03:04:05"},{"id":"b","ts":"2024-01-02T03:04:06"}]}`)
	sameInstant := restSnapshotFromJSON(t, `{"items":[{"id":"b","ts":"2024-01-02T03:04:06Z"},{"id":"a","ts":"2024-01-02T03:04:05Z"}]}`)
	if got := Compare(baseline, sameInstant, opts).DifferencesOutsideBaselineDefect; got != 0 {
		t.Fatalf("same instants, reordered: outside = %d, want 0", got)
	}
	baseline = restSnapshotFromJSON(t, `{"items":[{"id":"a","ts":"2024-01-02T03:04:05"},{"id":"b","ts":"2024-01-02T03:04:06"}]}`)
	moved := restSnapshotFromJSON(t, `{"items":[{"id":"b","ts":"2024-01-02T03:04:06Z"},{"id":"a","ts":"2024-01-02T03:04:07Z"}]}`)
	if got := Compare(baseline, moved, opts).DifferencesOutsideBaselineDefect; got != 1 {
		t.Fatalf("one moved instant, reordered: outside = %d, want 1", got)
	}
}

// TestMonotoneDescending_InputDomain pins the exact-collapse order rule.
func TestMonotoneDescending_InputDomain(t *testing.T) {
	row := func(ts string) any { return map[string]any{"created_at": ts} }
	cases := []struct {
		name string
		list []any
		want bool
	}{
		{"descending", []any{row("2024-01-03T00:00:00Z"), row("2024-01-02T00:00:00Z")}, true},
		{"ties", []any{row("2024-01-02T00:00:00Z"), row("2024-01-02T00:00:00")}, true},
		{"ascending", []any{row("2024-01-02T00:00:00Z"), row("2024-01-03T00:00:00Z")}, false},
		{"empty", []any{}, true},
		{"single", []any{row("2024-01-02T00:00:00Z")}, true},
		{"missing field", []any{map[string]any{}}, false},
		{"null field", []any{map[string]any{"created_at": nil}}, false},
		{"unparseable", []any{row("ABC-123")}, false},
		{"not an object", []any{"2024-01-02T00:00:00Z"}, false},
	}
	for _, c := range cases {
		if got := monotoneDescending(c.list, "created_at"); got != c.want {
			t.Errorf("%s: monotoneDescending = %v, want %v", c.name, got, c.want)
		}
	}
}

// TestDrilldownIssuesParity_DatetimeEntryAdmitsOnlyTheSameInstant runs the
// issue routes' shared datetime entry: a rendering difference of one
// instant is admitted, a different instant stays outside.
func TestDrilldownIssuesParity_DatetimeEntryAdmitsOnlyTheSameInstant(t *testing.T) {
	body := func(completedAt string) string {
		return `{"items":[{"work_item_id":"ABC-123","provider":"github","status":"done","team_id":null,"cycle_time_hours":null,"lead_time_hours":null,"started_at":null,"completed_at":"` + completedAt + `"}]}`
	}
	for _, c := range []struct {
		candidate   string
		wantOutside int
	}{{"2024-01-02T03:04:05Z", 0}, {"2024-01-02T03:04:06Z", 1}} {
		result := Compare(restSnapshotFromJSON(t, body("2024-01-02T03:04:05")), restSnapshotFromJSON(t, body(c.candidate)), personDrilldownIssuesParity)
		if result.DifferencesOutsideBaselineDefect != c.wantOutside {
			t.Errorf("candidate %s: outside = %d, want %d -- findings %+v", c.candidate, result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
		}
	}
}

// TestTimestampRenderingShape_AccountingGatesRepeatedCandidateRows: a
// candidate repeating the baseline's duplicated rows exactly, differing
// only in timestamp rendering, is refused on both pull-request routes and
// names the violation.
func TestTimestampRenderingShape_AccountingGatesRepeatedCandidateRows(t *testing.T) {
	body := func(suffix string) string {
		return `{"items":[{"repo_id":"ABC","number":1,"title":"t","created_at":"2024-01-02T00:00:00` + suffix + `"},{"repo_id":"ABC","number":1,"title":"t","created_at":"2024-01-02T00:00:00` + suffix + `"},{"repo_id":"ABC","number":2,"title":"t","created_at":"2024-01-01T00:00:00` + suffix + `"}]}`
	}
	for name, opts := range map[string]Options{"scope route": drilldownPRsParity, "person route": personDrilldownPRsParity} {
		baseline := restSnapshotFromJSON(t, body(""))
		candidate := restSnapshotFromJSON(t, body("Z"))
		baseline.Data = InjectRESTDedupKeys(baseline.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
		candidate.Data = InjectRESTDedupKeys(candidate.Data, drilldownPRsDedup.ListPath, drilldownPRsDedup.KeyFields)
		result := Compare(baseline, candidate, opts)
		if result.DifferencesOutsideBaselineDefect != 3 {
			t.Fatalf("%s: outside = %d, want 3 -- findings %+v", name, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
		for _, f := range result.Findings {
			if f.Kind == FindingMismatch && !strings.Contains(f.Detail, `candidate accounting refused: candidate repeats id`) {
				t.Errorf("%s: finding %s does not name the violation: %s", name, f.Path, f.Detail)
			}
		}
	}
}

// TestTimestampRenderingShape_CursorAdmittedWhenNeitherPlaneCarriesTheList:
// with no list on either plane there is no row to account for, and the
// cursor's rendering difference is admitted.
func TestTimestampRenderingShape_CursorAdmittedWhenNeitherPlaneCarriesTheList(t *testing.T) {
	baseline := restSnapshotFromJSON(t, `{"next_cursor":"2024-01-02T03:04:05"}`)
	candidate := restSnapshotFromJSON(t, `{"next_cursor":"2024-01-02T03:04:05Z"}`)
	result := Compare(baseline, candidate, personDrilldownPRsParity)
	if result.DifferencesOutsideBaselineDefect != 0 || !hasMismatch(result) {
		t.Fatalf("outside = %d, want 0 with the cursor finding admitted -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

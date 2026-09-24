package aggflame

import "testing"

func TestSanitizeLabel(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", "(unknown)"},
		{"   ", "(unknown)"},
		{"  Backlog  ", "Backlog"},
		{"in_progress", "in_progress"},
	}
	for _, tc := range cases {
		if got := sanitizeLabel(tc.in); got != tc.want {
			t.Errorf("sanitizeLabel(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCapitalizeWorkType(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"feature", "Feature"},
		{"BUG", "Bug"},
		{"chOre", "Chore"},
	}
	for _, tc := range cases {
		if got := capitalizeWorkType(tc.in); got != tc.want {
			t.Errorf("capitalizeWorkType(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCycleStatusCategory(t *testing.T) {
	cases := []struct{ status, want string }{
		{"in_progress", "Active Work"},
		{"in progress", "Active Work"},
		{"coding", "Active Work"},
		{"development", "Active Work"},
		{"review", "Review"},
		{"in_review", "Review"},
		{"code review", "Review"},
		{"pr_review", "Review"},
		{"backlog", "Waiting"},
		{"ready", "Waiting"},
		{"waiting", "Waiting"},
		{"queue", "Waiting"},
		{"pending", "Waiting"},
		{"blocked", "Blocked"},
		{"on_hold", "Blocked"},
		{"on hold", "Blocked"},
		{"done", "Other"},
		{"", "Other"},
	}
	for _, tc := range cases {
		if got := cycleStatusCategory(tc.status); got != tc.want {
			t.Errorf("cycleStatusCategory(%q) = %q, want %q", tc.status, got, tc.want)
		}
	}
}

// TestBuildCycleBreakdownTreeStableTieBreak pins the stable-sort
// requirement _build_cycle_breakdown_tree's Python `sorted()` carries
// (Timsort is stable): two rows with EQUAL total_hours in the same
// category must keep their original row order, matching
// sort.SliceStable rather than sort.Slice.
func TestBuildCycleBreakdownTreeStableTieBreak(t *testing.T) {
	rows := []cycleBreakdownRow{
		{Status: "review", TotalHours: 10},
		{Status: "in_review", TotalHours: 10},
	}
	got := buildCycleBreakdownTree(rows)
	if len(got.Children) != 1 || got.Children[0].Name != "Review" {
		t.Fatalf("expected a single Review category, got %+v", got.Children)
	}
	children := got.Children[0].Children
	if len(children) != 2 || children[0].Name != "review" || children[1].Name != "in_review" {
		t.Fatalf("expected stable tie order [review, in_review], got %+v", children)
	}
}

// TestBuildCycleBreakdownTreeFiltersNonPositiveHours pins the `if hours <=
// 0: continue` skip (services/aggregated_flame.py:87-89) -- a zero or
// negative total_hours row contributes nothing, not even an empty node.
func TestBuildCycleBreakdownTreeFiltersNonPositiveHours(t *testing.T) {
	rows := []cycleBreakdownRow{
		{Status: "backlog", TotalHours: 0},
		{Status: "blocked", TotalHours: -5},
		{Status: "review", TotalHours: 3},
	}
	got := buildCycleBreakdownTree(rows)
	if got.Value != 3 || len(got.Children) != 1 || got.Children[0].Name != "Review" {
		t.Fatalf("expected only Review(3) to survive, got %+v", got)
	}
}

// TestBuildCodeHotspotsTreeMergesSharedPrefixes pins the path-tree merge
// _build_code_hotspots_tree's nested dict performs (services/
// aggregated_flame.py:164-184): two files under the same directory share
// one intermediate node, whose value is the sum of its descendants, never
// its own separately-tracked churn.
func TestBuildCodeHotspotsTreeMergesSharedPrefixes(t *testing.T) {
	rows := []hotspotRow{
		{RepoID: "r1", FilePath: "a/b/one.go", TotalChurn: 4},
		{RepoID: "r1", FilePath: "a/b/two.go", TotalChurn: 6},
	}
	got := buildCodeHotspotsTree(rows, map[string]string{"r1": "Repo One"})
	if got.Value != 10 {
		t.Fatalf("root value = %v, want 10", got.Value)
	}
	repo := got.Children[0]
	if repo.Name != "Repo One" || repo.Value != 10 {
		t.Fatalf("repo node = %+v", repo)
	}
	aNode := repo.Children[0]
	if aNode.Name != "a" || aNode.Value != 10 || len(aNode.Children) != 1 {
		t.Fatalf("a node = %+v", aNode)
	}
	bNode := aNode.Children[0]
	if bNode.Name != "b" || bNode.Value != 10 || len(bNode.Children) != 2 {
		t.Fatalf("b node = %+v", bNode)
	}
}

// TestBuildThroughputTreeUnclassifiedFallback pins the `row.get("work_type")
// or "unclassified"` default (services/aggregated_flame.py:247) for a
// blank work_type.
func TestBuildThroughputTreeUnclassifiedFallback(t *testing.T) {
	rows := []throughputRow{{WorkType: "", TeamName: "Team X", ItemsCompleted: 7}}
	got := buildThroughputTree(rows)
	if len(got.Children) != 1 || got.Children[0].Name != "Unclassified" {
		t.Fatalf("expected a single Unclassified type node, got %+v", got.Children)
	}
}

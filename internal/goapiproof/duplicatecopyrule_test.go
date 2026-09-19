package goapiproof

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// copyRow decodes one JSON object the way DecodeRESTSnapshot does
// (numbers as json.Number).
func copyRow(t *testing.T, raw string) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var row map[string]any
	if err := decoder.Decode(&row); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	return row
}

// TestDuplicateCopyRule_CandidateIsServedCopyInputDomain runs every cell
// of candidateIsServedCopy's input domain in one pass, under the rule the
// pull-request drilldown routes declare.
func TestDuplicateCopyRule_CandidateIsServedCopyInputDomain(t *testing.T) {
	rule := &DuplicateCopyRule{WriteOnceFields: []string{"merged_at"}, RewrittenFields: []string{"title"}}
	row := func(title, mergedAt string) string {
		return `{"id":"ABC-123","title":` + title + `,"created_at":"2024-01-01T00:00:00Z","merged_at":` + mergedAt + `}`
	}
	const merged = `"2024-01-02T00:00:00Z"`
	cases := []struct {
		name      string
		rule      *DuplicateCopyRule
		group     []string
		candidate string
		want      bool
	}{
		{"canonical: two titles, candidate equals the second", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"b"`, "null"), true},
		{"canonical: two titles, candidate equals the first", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"a"`, "null"), true},
		{"single copy equal to the candidate", rule, []string{row(`"a"`, "null")}, row(`"a"`, "null"), true},
		{"duplicate copies, all equal", rule, []string{row(`"a"`, "null"), row(`"a"`, "null")}, row(`"a"`, "null"), true},
		{"candidate title carried by no copy", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"c"`, "null"), false},
		{"candidate title empty string, carried by no copy", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`""`, "null"), false},
		{"candidate title null, carried by no copy", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row("null", "null"), false},
		{"copy title null, candidate equals it", rule, []string{row("null", "null"), row(`"b"`, "null")}, row("null", "null"), true},
		{"candidate title wrong scalar type", rule, []string{row(`"1"`, "null"), row(`"b"`, "null")}, row("1", "null"), false},
		{"candidate title wrong container type", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`["a"]`, "null"), false},
		{"write-once null then populated, candidate populated with the newer title", rule, []string{row(`"a"`, "null"), row(`"b"`, merged)}, row(`"b"`, merged), true},
		{"write-once populated copy has the other title", rule, []string{row(`"a"`, "null"), row(`"b"`, merged)}, row(`"a"`, "null"), false},
		{"candidate mixes the two copies", rule, []string{row(`"a"`, "null"), row(`"b"`, merged)}, row(`"a"`, merged), false},
		{"write-once two populated values", rule, []string{row(`"a"`, merged), row(`"a"`, `"2024-01-03T00:00:00Z"`)}, row(`"a"`, merged), false},
		{"write-once same instant, naive and aware", rule, []string{row(`"a"`, "null"), row(`"b"`, `"2024-01-02T00:00:00"`)}, row(`"b"`, merged), true},
		{"write-once zero value is populated", rule, []string{row(`"a"`, "null"), row(`"b"`, "0")}, row(`"b"`, "0"), true},
		{"field outside the rule differs between copies", rule, []string{`{"id":"ABC-123","title":"a","created_at":"2024-01-01T00:00:00Z","merged_at":null}`, `{"id":"ABC-123","title":"a","created_at":"2024-01-01T00:00:01Z","merged_at":null}`}, `{"id":"ABC-123","title":"a","created_at":"2024-01-01T00:00:00Z","merged_at":null}`, false},
		{"copy carries an extra field", rule, []string{`{"id":"ABC-123","title":"a","created_at":"2024-01-01T00:00:00Z","merged_at":null,"extra":1}`, row(`"a"`, "null")}, row(`"a"`, "null"), false},
		{"copy lacks a rewritten field", rule, []string{`{"id":"ABC-123","created_at":"2024-01-01T00:00:00Z","merged_at":null}`, row(`"a"`, "null")}, row(`"a"`, "null"), false},
		{"candidate lacks a rewritten field every copy carries", rule, []string{row(`"a"`, "null"), row(`"b"`, "null")}, `{"id":"ABC-123","created_at":"2024-01-01T00:00:00Z","merged_at":null}`, false},
		{"declared fields absent from every row", rule, []string{`{"id":"ABC-123","x":1}`, `{"id":"ABC-123","x":1}`}, `{"id":"ABC-123","x":1}`, true},
		{"empty objects", rule, []string{`{}`, `{}`}, `{}`, true},
		{"empty group", rule, nil, row(`"a"`, "null"), false},
		{"nil rule, copies disagree in title", nil, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"b"`, "null"), false},
		{"nil rule, copies agree", nil, []string{row(`"a"`, "null"), row(`"a"`, "null")}, row(`"a"`, "null"), true},
		{"empty rule, copies disagree in title", &DuplicateCopyRule{}, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"b"`, "null"), false},
		{"rewritten only, write-once field differs", &DuplicateCopyRule{RewrittenFields: []string{"title"}}, []string{row(`"a"`, "null"), row(`"a"`, merged)}, row(`"a"`, merged), false},
		{"write-once only, title differs", &DuplicateCopyRule{WriteOnceFields: []string{"merged_at"}}, []string{row(`"a"`, "null"), row(`"b"`, "null")}, row(`"b"`, "null"), false},
		{"copies carry different rewritten keys of equal count", &DuplicateCopyRule{RewrittenFields: []string{"title", "body"}}, []string{`{"id":"ABC-123","title":"a"}`, `{"id":"ABC-123","body":"a"}`}, `{"id":"ABC-123","title":"a"}`, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			group := make([]map[string]any, len(c.group))
			for i, raw := range c.group {
				group[i] = copyRow(t, raw)
			}
			if got := c.rule.candidateIsServedCopy(group, copyRow(t, c.candidate)); got != c.want {
				t.Fatalf("candidateIsServedCopy = %v, want %v", got, c.want)
			}
		})
	}
	if rule.candidateIsServedCopy([]map[string]any{{}}, nil) {
		t.Fatal("a nil candidate must not be served, even against an empty copy")
	}
}

// TestDuplicateCopyRule_Describe pins the refusal wording per field set.
func TestDuplicateCopyRule_Describe(t *testing.T) {
	cases := []struct {
		rule *DuplicateCopyRule
		want string
	}{
		{nil, "duplicate-row"},
		{&DuplicateCopyRule{}, "duplicate-row"},
		{&DuplicateCopyRule{WriteOnceFields: []string{"a", "b"}}, "write-once a/b"},
		{&DuplicateCopyRule{RewrittenFields: []string{"c"}}, "rewritten c"},
		{&DuplicateCopyRule{WriteOnceFields: []string{"a"}, RewrittenFields: []string{"c"}}, "write-once a and rewritten c"},
	}
	for _, c := range cases {
		if got := c.rule.describe(); got != c.want {
			t.Errorf("describe(%+v) = %q, want %q", c.rule, got, c.want)
		}
	}
}

var retitleRule = &DuplicateCopyRule{RewrittenFields: []string{"title"}}

func retitleSnapshots(t *testing.T, baseItems, candItems []string) (Snapshot, Snapshot) {
	t.Helper()
	base, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(baseItems)))
	if err != nil {
		t.Fatal(err)
	}
	cand, err := DecodeRESTSnapshot([]byte(dedupCollapseBody(candItems)))
	if err != nil {
		t.Fatal(err)
	}
	return base, cand
}

// TestDuplicateCollapseLengthShape_CopyRuleJudgesRetitledCopies: one id's
// two copies carry different titles; with the rule the length finding is
// admitted when the candidate equals one copy, and refused when it
// equals neither or when no rule is declared.
func TestDuplicateCollapseLengthShape_CopyRuleJudgesRetitledCopies(t *testing.T) {
	base := []string{dedupCollapseItem("ABC-1", "old"), dedupCollapseItem("ABC-1", "new"), dedupCollapseItem("ABC-2", "x")}
	cases := []struct {
		name      string
		rule      *DuplicateCopyRule
		candTitle string
		want      bool
	}{
		{"rule, candidate equals the newer copy", retitleRule, "new", true},
		{"rule, candidate equals the older copy", retitleRule, "old", true},
		{"rule, candidate equals neither copy", retitleRule, "other", false},
		{"no rule", nil, "new", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline, candidate := retitleSnapshots(t, base, []string{dedupCollapseItem("ABC-1", c.candTitle), dedupCollapseItem("ABC-2", "x")})
			shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField, CopyRule: c.rule}
			if got := buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data).applies; got != c.want {
				t.Fatalf("applies = %v, want %v", got, c.want)
			}
		})
	}
	baseline, candidate := retitleSnapshots(t, base, []string{dedupCollapseItem("ABC-1", "new"), dedupCollapseItem("ABC-2", "changed")})
	shape := &DuplicateCollapseLengthShape{ListPath: "data.items", IDField: RESTDedupKeyField, CopyRule: retitleRule}
	if buildDuplicateCollapseLengthPlan(shape, baseline.Data, candidate.Data).applies {
		t.Fatal("a single-copy id whose candidate differs must still refuse under the rule")
	}
}

// TestDuplicateCollapsePageCutShape_CopyRuleJudgesRetitledCopies is the
// page-cut counterpart: a limit-4 baseline page with one retitled id
// duplicated, the candidate reaching one row further.
func TestDuplicateCollapsePageCutShape_CopyRuleJudgesRetitledCopies(t *testing.T) {
	base := []string{pageCutItem("ABC-1", "old", 1), pageCutItem("ABC-1", "new", 1), pageCutItem("ABC-2", "x", 2), pageCutItem("ABC-3", "x", 3)}
	cases := []struct {
		name      string
		rule      *DuplicateCopyRule
		candTitle string
		want      bool
	}{
		{"rule, candidate equals the newer copy", retitleRule, "new", true},
		{"rule, candidate equals the older copy", retitleRule, "old", true},
		{"rule, candidate equals neither copy", retitleRule, "other", false},
		{"no rule", nil, "new", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline, candidate := retitleSnapshots(t, base, []string{pageCutItem("ABC-1", c.candTitle, 1), pageCutItem("ABC-2", "x", 2), pageCutItem("ABC-3", "x", 3), pageCutItem("ABC-4", "x", 4)})
			shape := pageCutShape(4)
			shape.CopyRule = c.rule
			if got := buildDuplicateCollapsePageCutPlan(shape, baseline.Data, candidate.Data).applies; got != c.want {
				t.Fatalf("applies = %v, want %v", got, c.want)
			}
		})
	}
}

// TestWorkGraphEdgeDedupShape_RewrittenFieldsAloneJudgeDisagreeingIDs: a
// shape declaring only RewrittenFields judges the retitled id, admits it
// when the candidate equals one copy, names it when not, and leaves the
// agreeing id to a sibling entry.
func TestWorkGraphEdgeDedupShape_RewrittenFieldsAloneJudgeDisagreeingIDs(t *testing.T) {
	base := []string{dedupCollapseItem("ABC-1", "old"), dedupCollapseItem("ABC-1", "new"), dedupCollapseItem("ABC-2", "x"), dedupCollapseItem("ABC-2", "x")}
	shape := &WorkGraphEdgeDedupShape{EdgesListPath: "data.items", IDField: RESTDedupKeyField, RewrittenFields: []string{"title"}}
	for _, c := range []struct {
		candTitle string
		want      bool
	}{{"new", true}, {"old", true}, {"other", false}} {
		t.Run(c.candTitle, func(t *testing.T) {
			baseline, candidate := retitleSnapshots(t, base, []string{dedupCollapseItem("ABC-1", c.candTitle), dedupCollapseItem("ABC-2", "x")})
			plan := buildWorkGraphEdgeDedupPlan(shape, baseline.Data, candidate.Data)
			if !plan.applies {
				t.Fatal("plan does not apply")
			}
			if plan.admittedIDs["ABC-1"] != c.want {
				t.Fatalf("admitted ABC-1 = %v, want %v", plan.admittedIDs["ABC-1"], c.want)
			}
			if plan.admittedIDs["ABC-2"] || plan.judgedIDs["ABC-2"] {
				t.Fatal("the agreeing id ABC-2 was judged by the rewritten-field entry")
			}
			id, named := plan.uncoveredEdgeID(Finding{Path: "$.data.items[0].title"})
			if !named || id != "ABC-1" {
				t.Fatalf("uncoveredEdgeID = %q/%v, want ABC-1", id, named)
			}
			wantDetail := fmt.Sprintf(" (dedup id %q not admitted by the declared rewritten title rule", "ABC-1")
			if !strings.HasPrefix(plan.refusalDetail("ABC-1"), wantDetail) {
				t.Fatalf("refusalDetail = %q, want prefix %q", plan.refusalDetail("ABC-1"), wantDetail)
			}
		})
	}
}

// corpusTicketMatched reports whether ticket is among result's matched
// declarations.
func corpusTicketMatched(result Result, ticket string) bool {
	for _, matched := range result.BaselineDefectsMatched {
		if matched == ticket {
			return true
		}
	}
	return false
}

// TestPullRequestDrilldownDefects_EveryLengthEntryJudgesRetitledCopies
// runs a retitled duplicate through each corpus entry that admits a
// length finding: the page-cut entry at a request's own limit, the
// exact-collapse entry under it, and the team-scoped uncut entry. Each
// must match by name, so an entry declared without the copy rule goes
// idle and fails here.
func TestPullRequestDrilldownDefects_EveryLengthEntryJudgesRetitledCopies(t *testing.T) {
	pageBase := []string{pageCutItem("ABC-1", "old", 1), pageCutItem("ABC-1", "new", 1), pageCutItem("ABC-2", "x", 2), pageCutItem("ABC-3", "x", 3)}
	pageCand := []string{pageCutItem("ABC-1", "old", 1), pageCutItem("ABC-2", "x", 2), pageCutItem("ABC-3", "x", 3)}
	collapseBase := []string{pageCutItem("ABC-1", "old", 1), pageCutItem("ABC-1", "new", 1), pageCutItem("ABC-2", "x", 2)}
	collapseCand := []string{pageCutItem("ABC-1", "new", 1), pageCutItem("ABC-2", "x", 2)}
	pageCut, collapse, uncut := drilldownPRsPageCutTicket(t), drilldownPRsLengthCollapseTicket(t), drilldownPRsUncutTicket(t)
	cases := []struct {
		name       string
		opts       Options
		base, cand []string
		ticket     string
	}{
		{"page cut, scope route", drilldownPRsParityWithLimit(4), pageBase, pageCand, pageCut},
		{"page cut, person route", parityWithPageCutLimit(personDrilldownPRsParity, 4), pageBase, pageCand, pageCut},
		{"exact collapse, scope route", drilldownPRsParity, collapseBase, collapseCand, collapse},
		{"exact collapse, person route", personDrilldownPRsParity, collapseBase, collapseCand, collapse},
		{"uncut, team-scoped route", drilldownPRsTeamScopedParity, collapseBase, collapseCand, uncut},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			baseline, candidate := retitleSnapshots(t, c.base, c.cand)
			result := Compare(baseline, candidate, c.opts)
			if result.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside = %d, want 0 -- findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
			}
			if !corpusTicketMatched(result, c.ticket) {
				t.Fatalf("matched = %v, want %s among them (idle %v)", result.BaselineDefectsMatched, c.ticket, result.IdleIntermittentBaselineDefects)
			}
		})
	}
}

// TestPersonDrilldownPRs_EachRequestCarriesItsOwnPageLimit pins every
// person-route request's page-cut limit to the page size both planes
// serve it: the ceiling for a limit above it, the default otherwise.
func TestPersonDrilldownPRs_EachRequestCarriesItsOwnPageLimit(t *testing.T) {
	want := map[string]int{
		"drilldown_prs_default":            drilldownPRsDefaultLimit,
		"valid_cursor":                     drilldownPRsDefaultLimit,
		"limit_above_ceiling":              personDrilldownPRsCeilingLimit,
		"limit_zero_falls_back_to_default": drilldownPRsDefaultLimit,
	}
	for name, limit := range want {
		req := personDrilldownPRsRequest(t, name)
		found := 0
		for _, defect := range req.Parity.BaselineDefects {
			if defect.DuplicateCollapsePageCutShape == nil {
				continue
			}
			found++
			if defect.DuplicateCollapsePageCutShape.Limit != limit {
				t.Errorf("%s page-cut Limit = %d, want %d", name, defect.DuplicateCollapsePageCutShape.Limit, limit)
			}
		}
		if found != 1 {
			t.Errorf("%s carries %d page-cut entries, want 1", name, found)
		}
	}
}

// TestPullRequestDrilldownDefects_EveryLengthEntryRefusesAReorderedCandidate:
// the right rows in the wrong order leave the length finding outside
// under every length entry that can admit an exact collapse.
func TestPullRequestDrilldownDefects_EveryLengthEntryRefusesAReorderedCandidate(t *testing.T) {
	base := []string{pageCutItem("ABC-1", "x", 1), pageCutItem("ABC-1", "x", 1), pageCutItem("ABC-2", "x", 2)}
	reordered := []string{pageCutItem("ABC-2", "x", 2), pageCutItem("ABC-1", "x", 1)}
	for name, opts := range map[string]Options{
		"scope route":       drilldownPRsParity,
		"person route":      personDrilldownPRsParity,
		"team-scoped route": drilldownPRsTeamScopedParity,
	} {
		baseline, candidate := retitleSnapshots(t, base, reordered)
		result := Compare(baseline, candidate, opts)
		if result.DifferencesOutsideBaselineDefect != 1 {
			t.Errorf("%s: outside = %d, want 1 -- findings %+v", name, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
	}
}

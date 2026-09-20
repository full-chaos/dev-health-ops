package goapiproof

import (
	"fmt"
	"testing"
)

// analyticsBody is one analytics answer carrying one series and one breakdown
// item, each leaf given as raw JSON, beside a second bucket and item whose
// leaves are non-null (an answer whose every leaf is null is an empty
// result, which no citation covers).
func analyticsBody(dimensionValue, date, value, key, label, itemValue string) string {
	return fmt.Sprintf(`{"data":{"analytics":{"timeseries":[{"dimension":"TEAM","dimensionValue":%s,"measure":"M","buckets":[{"date":%s,"value":%s},{"date":"2026-06-02","value":2}]}],"breakdowns":[{"dimension":"TEAM","measure":"M","items":[{"key":%s,"value":%s,"label":%s},{"key":"k2","value":1,"label":"l2"}]}]}}}`,
		dimensionValue, date, value, key, itemValue, label)
}

// TestAnalyticsBatchDeclarations_InputDomain runs, through the real batch
// declaration, every leaf the declarations name against the canonical pair,
// the pair reversed, another text, another number, null on either side and a
// different day: only the canonical pair is covered.
func TestAnalyticsBatchDeclarations_InputDomain(t *testing.T) {
	opts := analyticsBatchParity(analyticsBatch{
		Series:     []analyticsSeries{{"TEAM", "A"}, {"TEAM", "B"}},
		Breakdowns: []analyticsBreakdown{{"TEAM", "A", 10}, {"TEAM", "B", 10}},
	})
	const (
		dv, date, val, key, label, itemVal = 0, 1, 2, 3, 4, 5
	)
	base := [6]string{`"t1"`, `"2026-06-01"`, `1.5`, `"k1"`, `"l1"`, `1`}
	cases := []struct {
		name                string
		leaf                int
		baseline, candidate string
		wantOutside         int
	}{
		{"dimensionValue None -> empty", dv, `"None"`, `""`, 0},
		{"dimensionValue empty -> None", dv, `""`, `"None"`, 1},
		{"dimensionValue other text", dv, `"team-1"`, `""`, 1},
		{"dimensionValue None -> other", dv, `"None"`, `"team-1"`, 1},
		{"dimensionValue null candidate", dv, `"None"`, `null`, 1},
		{"dimensionValue None case", dv, `"none"`, `""`, 1},

		{"date datetime -> date", date, `"2026-06-01T00:00:00"`, `"2026-06-01"`, 0},
		{"date offset midnight -> date", date, `"2026-06-01T00:00:00Z"`, `"2026-06-01"`, 0},
		{"date next day", date, `"2026-06-02T00:00:00"`, `"2026-06-01"`, 1},
		{"date with time of day", date, `"2026-06-01T05:00:00"`, `"2026-06-01"`, 1},
		{"date reversed", date, `"2026-06-01"`, `"2026-06-01T00:00:00"`, 1},
		{"date null candidate", date, `"2026-06-01T00:00:00"`, `null`, 1},
		{"date candidate is datetime", date, `"2026-06-01T00:00:00"`, `"2026-06-01T00:00:01"`, 1},
		{"date malformed candidate", date, `"2026-06-01T00:00:00"`, `"2026-06-01x"`, 1},
		{"date zero-day malformed candidate", date, `"0001-01-01T00:00:00"`, `"0001-01-01x"`, 1},
		{"date unparseable baseline, zero-day candidate", date, `"garbage"`, `"0001-01-01"`, 1},
		{"date baseline is a plain date", date, `"2026-06-01"`, `"2026-06-01"`, 0},

		{"value 0 -> null", val, `0.0`, `null`, 0},
		{"value 0 int -> null", val, `0`, `null`, 0},
		{"value null -> 0", val, `null`, `0.0`, 1},
		{"value 1 -> null", val, `1.0`, `null`, 1},
		{"value 0 -> text", val, `0.0`, `"0"`, 1},

		{"key None -> empty", key, `"None"`, `""`, 0},
		{"key None -> null", key, `"None"`, `null`, 1},
		{"key empty -> None", key, `""`, `"None"`, 1},
		{"key other text", key, `"repo"`, `""`, 1},

		{"item value 0 -> null", itemVal, `0.0`, `null`, 0},
		{"item value 0 int -> null", itemVal, `0`, `null`, 0},
		{"item value null -> 0", itemVal, `null`, `0.0`, 1},
		{"item value 1 -> null", itemVal, `1.0`, `null`, 1},
		{"item value 0 -> text", itemVal, `0.0`, `"0"`, 1},

		{"label None -> null", label, `"None"`, `null`, 0},
		{"label None -> empty", label, `"None"`, `""`, 1},
		{"label null -> None", label, `null`, `"None"`, 1},
		{"label other text", label, `"repo"`, `null`, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, cand := base, base
			b[c.leaf], cand[c.leaf] = c.baseline, c.candidate
			baseline := snapshotFromJSON(t, analyticsBody(b[dv], b[date], b[val], b[key], b[label], b[itemVal]))
			candidate := snapshotFromJSON(t, analyticsBody(cand[dv], cand[date], cand[val], cand[key], cand[label], cand[itemVal]))
			result := Compare(baseline, candidate, opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// TestAnalyticsBatchDeclarations_OnlyTheAskedListsAreDeclared pins that a
// batch declares a Tier-B leaf and defects only for the lists it carries: a
// breakdown-only batch declares nothing over timeseries and a series-only
// batch nothing over breakdowns.
func TestAnalyticsBatchDeclarations_OnlyTheAskedListsAreDeclared(t *testing.T) {
	breakdownOnly := analyticsBatchParity(analyticsBatch{Breakdowns: []analyticsBreakdown{{"TEAM", "A", 10}}})
	if _, ok := breakdownOnly.FloatTierB["data.analytics.timeseries.buckets.value"]; ok {
		t.Fatalf("breakdown-only batch declares a timeseries Tier-B leaf: %v", breakdownOnly.FloatTierB)
	}
	seriesOnly := analyticsBatchParity(analyticsBatch{Series: []analyticsSeries{{"TEAM", "A"}}})
	if _, ok := seriesOnly.FloatTierB["data.analytics.breakdowns.items.value"]; ok {
		t.Fatalf("series-only batch declares a breakdown Tier-B leaf: %v", seriesOnly.FloatTierB)
	}
	for name, o := range map[string]Options{"breakdown-only": breakdownOnly, "series-only": seriesOnly} {
		for _, d := range o.BaselineDefects {
			for _, p := range d.Paths {
				other := "data.analytics.timeseries."
				if name == "series-only" {
					other = "data.analytics.breakdowns."
				}
				if len(p) >= len(other) && p[:len(other)] == other {
					t.Errorf("%s batch declares %s over the list it does not carry", name, p)
				}
			}
		}
	}
}

// TestLeafPairShape_NeverCoversStructure pins that a leaf pair never admits a
// finding that is not a leaf difference.
func TestLeafPairShape_NeverCoversStructure(t *testing.T) {
	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.rows.k"},
		LeafPairShape: &LeafPairShape{Pairs: []LeafPair{{Baseline: "None", Candidate: nil}}},
	}}}
	cases := []struct {
		name                string
		baseline, candidate string
		wantOutside         int
	}{
		{"the pair as a leaf", `[{"k":"None"},{"k":"x"}]`, `[{"k":null},{"k":"x"}]`, 0},
		{"key absent", `[{"k":"None"},{"k":"x"}]`, `[{},{"k":"x"}]`, 1},
		{"container vs null", `[{"k":{"a":1}},{"k":"x"}]`, `[{"k":null},{"k":"x"}]`, 1},
		{"list vs null", `[{"k":["None"]},{"k":"x"}]`, `[{"k":null},{"k":"x"}]`, 1},
		{"boolean vs null", `[{"k":true},{"k":"x"}]`, `[{"k":null},{"k":"x"}]`, 1},
		{"list length", `[{"k":"None"},{"k":"x"}]`, `[{"k":null}]`, 1},
		{"every candidate leaf null", `[{"k":"None"},{"k":"x"}]`, `[{"k":null},{"k":null}]`, 2},
	}
	for _, c := range cases {
		result := Compare(snapshotFromJSON(t, `{"data":{"rows":`+c.baseline+`}}`), snapshotFromJSON(t, `{"data":{"rows":`+c.candidate+`}}`), opts)
		if result.DifferencesOutsideBaselineDefect != c.wantOutside {
			t.Errorf("%s: outside = %d, want %d -- %+v", c.name, result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
		}
	}
}

// TestLeafPairShape_NullPairNeverAdmitsAPresenceFinding pins that a (null,
// null) pair does not admit a presence finding, which records no leaves.
func TestLeafPairShape_NullPairNeverAdmitsAPresenceFinding(t *testing.T) {
	opts := Options{BaselineDefects: []BaselineDefect{{
		Ticket: "ABC-123", Reason: "test fixture", Paths: []string{"data.rows.k"},
		LeafPairShape: &LeafPairShape{Pairs: []LeafPair{{Baseline: nil, Candidate: nil}}},
	}}}
	result := Compare(snapshotFromJSON(t, `{"data":{"rows":[{"k":"a"},{"k":"x"}]}}`), snapshotFromJSON(t, `{"data":{"rows":[{},{"k":"x"}]}}`), opts)
	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 -- %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestAnalyticsBatchDeclarations_LabelNullSubtree runs the label declaration
// over a breakdown whose every candidate label is null (Go sends no label for
// a NULL dimension value, and the item beside it has none either): the
// declared ("None", null) pair is covered even though the candidate's label
// subtree has no non-null leaf, and any other baseline text stays outside.
// The production ladder for testOpsCoverage showed the "None" != null finding
// relabelled empty_result and refused (PAGE_PIPELINES, PAGE_TESTS).
func TestAnalyticsBatchDeclarations_LabelNullSubtree(t *testing.T) {
	opts := analyticsBatchParity(analyticsBatch{Breakdowns: []analyticsBreakdown{{"TEAM", "A", 10}}})
	body := func(firstLabel string) string {
		return fmt.Sprintf(`{"data":{"analytics":{"breakdowns":[{"dimension":"TEAM","measure":"A","items":[{"key":"k1","value":1,"label":%s},{"key":"k2","value":1,"label":null}]}]}}}`, firstLabel)
	}
	cases := []struct {
		name                string
		baseline, candidate string
		wantOutside         int
		wantShape           string // shape of the one outside finding
	}{
		{"None -> null, every candidate label null", `"None"`, `null`, 0, ""},
		{"other text -> null, every candidate label null", `"repo"`, `null`, 1, ShapeEmptyResult},
		{"None -> empty text", `"None"`, `""`, 1, ShapeValue},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, body(c.baseline)), snapshotFromJSON(t, body(c.candidate)), opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
			if c.wantOutside == 1 && (len(result.Findings) != 1 || result.Findings[0].Shape != c.wantShape) {
				t.Fatalf("outside finding shape: want %q, findings %+v", c.wantShape, result.Findings)
			}
		})
	}
}

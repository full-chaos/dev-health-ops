package goapiproof

import (
	"fmt"
	"strings"
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

// CHAOS-9111: the percent differences are admitted ONLY beside the state that explains
// them, judged on the row's own siblings: a window with no stored value (a flag false),
// or a prior measured 0 against a current value that is NOT 0 (both flags true, value
// not 0). A TRUE 0 % served as null (both flags true, value 0) is a disagreement.

// deltaRow is one row of a list of deltas.
type deltaRow struct {
	id    string
	pct   string
	value string
	flags string // `"has_data":true,"has_prior_data":true`, or "" for none
}

func deltaBody(rows ...deltaRow) string {
	var items []string
	for _, r := range rows {
		item := fmt.Sprintf(`{"id":%q,"delta_pct":%s,"value":%s`, r.id, r.pct, r.value)
		if r.flags != "" {
			item += "," + r.flags
		}
		items = append(items, item+"}")
	}
	return "[" + strings.Join(items, ",") + "]"
}

const (
	flagsBoth    = `"has_data":true,"has_prior_data":true`
	flagsNoPrior = `"has_data":true,"has_prior_data":false`
	flagsNoData  = `"has_data":false,"has_prior_data":true`
)

type percentCase struct {
	name        string
	baseline    []deltaRow
	candidate   []deltaRow
	wantOutside int
}

func runPercentCases(t *testing.T, opts Options, wrap func(list string) string, cases []percentCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, wrap(deltaBody(c.baseline...))), snapshotFromJSON(t, wrap(deltaBody(c.candidate...))), opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// percentRows builds, per case, the reference rows (percent 0.0) and the candidate rows
// (percent null) of the row under test plus a measured row beside it.
func percentRows(flags, value string) (baseline, candidate []deltaRow) {
	other := deltaRow{id: "b", pct: "5.0", value: "9", flags: flagsBoth}
	return []deltaRow{{id: "a", pct: "0.0", value: value, flags: flags}, other}, []deltaRow{{id: "a", pct: "null", value: value, flags: flags}, other}
}

func TestMetricPercentDefects_AdmitOnlyTheStateTheyName(t *testing.T) {
	opts := Options{BaselineDefects: metricPercentDefects("data.deltas", "data.deltas.delta_pct", "metric")}
	wrap := func(list string) string { return `{"data":{"deltas":` + list + `}}` }
	var cases []percentCase
	add := func(name, flags, value string, outside int) {
		b, c := percentRows(flags, value)
		cases = append(cases, percentCase{name, b, c, outside})
	}
	add("no current data: 0 -> null", flagsNoData, "0", 0)
	add("no prior data: 0 -> null", flagsNoPrior, "5", 0)
	add("from a measured zero (value not 0): 0 -> null", flagsBoth, "5", 0)
	add("a TRUE 0 % served as null (both flags true, value 0) is not covered", flagsBoth, "0", 1)
	add("a null percent beside no flags is not covered", "", "5", 1)
	// the from-zero state needs a current value that is present and numeric and not 0:
	// a row with no `value` key, or a non-numeric one, is not covered.
	b2, c2 := percentRows(flagsBoth, "5")
	for _, rows := range [][]deltaRow{b2, c2} {
		rows[0].value = "null"
	}
	cases = append(cases, percentCase{"from zero: a missing or null value is not covered", b2, c2, 1})
	b3, c3 := percentRows(flagsBoth, `"x"`)
	cases = append(cases, percentCase{"from zero: a non-numeric value is not covered", b3, c3, 1})
	add("a null percent beside a non-boolean flag is not covered", `"has_data":"yes","has_prior_data":true`, "5", 1)
	b, c := percentRows(flagsNoData, "0")
	c[0].pct = "3.0"
	cases = append(cases, percentCase{"a number where the reference has 0 is not covered", b, c, 1})
	b, c = percentRows(flagsNoData, "0")
	b[0].pct = "12.0"
	cases = append(cases, percentCase{"a real percent served as null is not covered", b, c, 1})
	// each row is judged by ITS OWN flags: row 0 has no data (covered), row 1 is a true 0 % (not).
	cases = append(cases, percentCase{"a row is judged by its own flags, not the first row's",
		[]deltaRow{{"a", "0.0", "0", flagsNoData}, {"b", "0.0", "0", flagsBoth}},
		[]deltaRow{{"a", "null", "0", flagsNoData}, {"b", "null", "0", flagsBoth}}, 1})
	runPercentCases(t, opts, wrap, cases)
}

// Each declared difference covers its own state alone.
func TestMetricPercentDefects_EachCoversItsOwnStateOnly(t *testing.T) {
	defects := metricPercentDefects("data.deltas", "data.deltas.delta_pct", "metric")
	wrap := func(list string) string { return `{"data":{"deltas":` + list + `}}` }
	for _, c := range []struct {
		name        string
		defect      int
		flags       string
		value       string
		wantOutside int
	}{
		{"from zero covers both flags true, value not 0", 0, flagsBoth, "5", 0},
		{"from zero does not cover a true 0 %", 0, flagsBoth, "0", 1},
		{"from zero does not cover no current data", 0, flagsNoData, "5", 1},
		{"from zero does not cover no prior data", 0, flagsNoPrior, "5", 1},
		{"no data covers no current data", 1, flagsNoData, "0", 0},
		{"no data covers no prior data", 1, flagsNoPrior, "5", 0},
		{"no data does not cover both flags true", 1, flagsBoth, "5", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, cand := percentRows(c.flags, c.value)
			result := Compare(snapshotFromJSON(t, wrap(deltaBody(b...))), snapshotFromJSON(t, wrap(deltaBody(cand...))), Options{BaselineDefects: []BaselineDefect{defects[c.defect]}})
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// The operating review's percent, covered only beside its own week's flags (the
// delta's hasPriorData, or the metric's hasData one level up); a missing flag covers
// nothing.
func TestOperatingReviewPercentDefect_AdmitsOnlyAWeekWithoutData(t *testing.T) {
	spec, err := SpecFor("operatingReview")
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Parity.BaselineDefects) != 1 || spec.Parity.BaselineDefects[0].Ticket != "CHAOS-9111" {
		t.Fatalf("the operatingReview parity declares %d defects, want the one CHAOS-9111 difference", len(spec.Parity.BaselineDefects))
	}
	defect := spec.Parity.BaselineDefects[0]
	body := func(pct, metricFlag, deltaFlag string) string {
		metric := `"key":"a"`
		if metricFlag != "" {
			metric += "," + metricFlag
		}
		delta := `"percent":` + pct
		if deltaFlag != "" {
			delta += "," + deltaFlag
		}
		return `{"data":{"operatingReview":{"sections":[{"metrics":[{` + metric + `,"delta":{` + delta + `}},{"key":"b","hasData":true,"delta":{"percent":5.0,"hasPriorData":true}}]}]}}}`
	}
	for _, c := range []struct {
		name                        string
		metricFlag, deltaFlag, cand string
		wantOutside                 int
	}{
		{"no prior week: 0 -> null", `"hasData":true`, `"hasPriorData":false`, "null", 0},
		{"no current week: 0 -> null", `"hasData":false`, `"hasPriorData":true`, "null", 0},
		{"two measured weeks: 0 -> null is not covered", `"hasData":true`, `"hasPriorData":true`, "null", 1},
		{"no prior week: a number is not covered", `"hasData":true`, `"hasPriorData":false`, "3.0", 1},
		{"no flags at all: not covered", "", "", "null", 1},
		{"a non-boolean flag: not covered", `"hasData":"no"`, `"hasPriorData":"no"`, "null", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, body("0.0", c.metricFlag, c.deltaFlag)), snapshotFromJSON(t, body(c.cand, c.metricFlag, c.deltaFlag)), Options{BaselineDefects: []BaselineDefect{defect}})
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// The explain metric's own percent, a driver's and a contributor's (keyed by id).
func TestExplainPercentDefects_AdmitOnlyTheStateTheyName(t *testing.T) {
	opts := Options{
		BaselineDefects: explainPercentDefects(),
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.drivers", KeyFields: []string{"id"}}, {Path: "data.contributors", KeyFields: []string{"id"}},
		},
	}
	object := func(pct, value, flags string) string {
		return fmt.Sprintf(`{"data":{"delta_pct":%s,"value":%s,%s,"drivers":[],"contributors":[]}}`, pct, value, flags)
	}
	for _, c := range []struct {
		name                string
		baseline, candidate string
		wantOutside         int
	}{
		{"metric: no prior window, 0 -> null", object("0.0", "5", flagsNoPrior), object("null", "5", flagsNoPrior), 0},
		{"metric: both windows, value not 0, 0 -> null (from a measured zero)", object("0.0", "5", flagsBoth), object("null", "5", flagsBoth), 0},
		{"metric: a TRUE 0 % served as null is not covered", object("0.0", "0", flagsBoth), object("null", "0", flagsBoth), 1},
		{"metric: a number is not covered", object("0.0", "5", flagsNoPrior), object("3.0", "5", flagsNoPrior), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
	list := func(pct, value, flags string) string {
		return fmt.Sprintf(`{"data":{"delta_pct":4.0,"value":1,"has_data":true,"has_prior_data":true,"drivers":[{"id":"r1","delta_pct":%s,"value":%s,%s},{"id":"r2","delta_pct":4.0,"value":9,%s}],"contributors":[{"id":"r1","delta_pct":%s,"value":%s,%s}]}}`,
			pct, value, flags, flagsBoth, pct, value, flags)
	}
	for _, c := range []struct {
		name                string
		baseline, candidate string
		wantOutside         int
	}{
		{"driver and contributor: no comparison row, 0 -> null", list("0.0", "7", flagsNoPrior), list("null", "7", flagsNoPrior), 0},
		{"driver and contributor: a TRUE 0 % served as null is not covered", list("0.0", "0", flagsBoth), list("null", "0", flagsBoth), 2},
		{"driver and contributor: a real percent -> null is not covered", list("7.0", "7", flagsNoPrior), list("null", "7", flagsNoPrior), 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), opts)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

// The same through the REAL parity options of Home: a null for a true 0 % is a
// disagreement. NOT asserted for the person summary and the explain metric/drivers:
// there older blanket citations of other tickets (CHAOS-5812 data.deltas.delta_pct;
// CHAOS-5813 and CHAOS-5819 data.delta_pct and data.drivers.delta_pct, restcorpus.go)
// cover ANY value of the leaf for their own mechanism (an unmerged ReplacingMergeTree
// version, a dropped scope); the percent declarations narrow only what they add.
func TestPercentDifferencesThroughTheRealParityOptionsOfHome(t *testing.T) {
	wrap := func(list string) string { return `{"data":{"deltas":` + list + `}}` }
	for _, c := range []struct {
		name        string
		flags       string
		value       string
		wantOutside int
	}{
		{"no prior data", flagsNoPrior, "5", 0},
		{"from zero", flagsBoth, "5", 0},
		{"a true 0 %", flagsBoth, "0", 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			b, cand := percentRows(c.flags, c.value)
			result := Compare(snapshotFromJSON(t, wrap(deltaBody(b...))), snapshotFromJSON(t, wrap(deltaBody(cand...))), homeNumericLeaves)
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

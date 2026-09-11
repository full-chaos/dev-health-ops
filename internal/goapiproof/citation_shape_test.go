package goapiproof

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A citation covers leaf VALUE differences under its path, and nothing else.
//
// `hotspots` cites the whole `data.hotspots.rows` subtree for
// CHAOS-5447 (Python selects a different physical row per file), and a cited
// path covered EVERY finding beneath it -- including the list's own LENGTH
// finding, which is reported at the list path. So a Go build returning `[]`,
// `null` or rows of empty objects was a "fully-cited mismatch" with outside=0:
// primary enablement proof, executed end to end by the reviewer through the
// real writer, CLI and matrix. None of those is the cited defect. A citation
// declares that a VALUE is known to be wrong on the Python side; it cannot
// declare that Go is allowed to return a different SHAPE.
//
// This drives the real hotspots declaration (operationSpecs, not a copy) over
// every shape a candidate can take, through the real comparator. The same
// cells run through the writer, WriteAtomic and the production reader on each
// PostgreSQL major (TestAShapeDifferenceUnderACitationNeverBecomesProof).
//
// The rule: a citation covers a difference
// only where BOTH sides are leaves -- a scalar or null -- at the differing
// path. A NULL leaf is covered: hotspots' defect is row selection, and a
// different row surfaces as null in one plane and a value in the other.
// A list length, a container against null or a scalar, and a key present on
// one side only are structural and never covered.

type citationShapeCell struct {
	name      string
	candidate string
	outside   int // differences NO citation may cover
}

func hotspotsRow(file, repo string, churn int, blame string) string {
	return `{"filePath":"` + file + `","repoId":"` + repo + `","churnCommits30d":` + itoa(churn) + `,"blameConcentration":` + blame + `,"riskScore":1.5}`
}

func hotspotsBody(rows ...string) string {
	return `{"data":{"hotspots":{"rows":[` + strings.Join(rows, ",") + `]}}}`
}

// hotspotsCitationCells is the baseline (the Python plane's answer) and every
// candidate shape, with the number of differences outside the citation.
func hotspotsCitationCells() (string, []citationShapeCell) {
	a, b := hotspotsRow("a.go", "r1", 5, "0.5"), hotspotsRow("b.go", "r1", 2, "0.25")
	return hotspotsBody(a, b), []citationShapeCell{
		{"leaf values differ (the declared defect: another physical row)", hotspotsBody(hotspotsRow("a.go", "r1", 9, "0.5"), hotspotsRow("c.go", "r2", 2, "0.75")), 0},
		{"a leaf null where Python has a value", hotspotsBody(hotspotsRow("a.go", "r1", 5, "null"), b), 0},
		{"leaf values differ and a leaf is null", hotspotsBody(hotspotsRow("z.go", "r9", 7, "null"), hotspotsRow("b.go", "r1", 3, "0.25")), 0},
		{"a leaf of another scalar type (string for number)", hotspotsBody(hotspotsRow("a.go", "r1", 5, `"0.5"`), b), 0},
		// Under a cited path, a candidate with NO non-null
		// leaf while the baseline has one is an empty result in disguise --
		// structural, outside. One non-null leaf left anywhere under the
		// path keeps the null leaves covered (hotspots' real receipt: 7 null
		// of 391 leaves).
		{"every leaf null, same shape (an empty result in disguise)", `{"data":{"hotspots":{"rows":[` +
			`{"filePath":null,"repoId":null,"churnCommits30d":null,"blameConcentration":null,"riskScore":null},` +
			`{"filePath":null,"repoId":null,"churnCommits30d":null,"blameConcentration":null,"riskScore":null}]}}}`, 10},
		{"every leaf null but one", `{"data":{"hotspots":{"rows":[` +
			`{"filePath":null,"repoId":null,"churnCommits30d":null,"blameConcentration":null,"riskScore":null},` +
			`{"filePath":null,"repoId":null,"churnCommits30d":null,"blameConcentration":null,"riskScore":1.5}]}}}`, 0},
		{"rows empty (a length difference)", `{"data":{"hotspots":{"rows":[]}}}`, 1},
		{"one extra row (a length difference)", hotspotsBody(a, b, hotspotsRow("c.go", "r1", 1, "0.1")), 1},
		{"rows null (a list against null)", `{"data":{"hotspots":{"rows":null}}}`, 1},
		{"rows of empty objects (every key missing)", `{"data":{"hotspots":{"rows":[{},{}]}}}`, 10},
		{"a row that is null (an object against null)", hotspotsBody("null", b), 1},
		{"a row that is an array (an object against a list)", hotspotsBody("[]", b), 1},
		{"a leaf that is an object (a scalar against a container)", hotspotsBody(hotspotsRow("a.go", "r1", 5, `{}`), b), 1},
		{"an extra key in a row", hotspotsBody(strings.TrimSuffix(a, "}")+`,"extra":1}`, b), 1},
	}
}

func TestACitationCoversOnlyLeafValueDifferences(t *testing.T) {
	spec, err := SpecFor("hotspots")
	if err != nil {
		t.Fatalf("SpecFor(hotspots): %v", err)
	}
	opts := spec.Parity
	baseline, cells := hotspotsCitationCells()
	for _, c := range cells {
		result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, c.candidate), opts)
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("%s: terminal %q, want mismatch", c.name, result.TerminalState)
		}
		if result.DifferencesOutsideBaselineDefect != c.outside {
			t.Fatalf("%s: outside=%d, want %d -- findings %+v", c.name, result.DifferencesOutsideBaselineDefect, c.outside, result.Findings)
		}
		if len(result.BaselineDefectsMatched) != 1 || result.BaselineDefectsMatched[0] != "CHAOS-5447" || len(result.StaleBaselineDefects) != 0 {
			t.Fatalf("%s: matched=%v stale=%v, want CHAOS-5447 matched and nothing stale (a citation with differences beneath it is live, whether or not it may cover them)",
				c.name, result.BaselineDefectsMatched, result.StaleBaselineDefects)
		}
		shapes := map[string]int{}
		for _, f := range result.Findings {
			shapes[f.Shape]++
		}
		t.Logf("cell %-60s outside=%-2d findings by shape=%v", c.name, result.DifferencesOutsideBaselineDefect, shapes)
	}

	// Control: identical bodies -- the citation covers nothing and is stale,
	// so the run refuses (declared_baseline_defect_matched_nothing). A cited
	// mismatch is therefore the only admissible receipt hotspots can produce,
	// which is exactly why the citation's reach decides the gate.
	same := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, baseline), opts)
	if !same.IsMatch() || len(same.StaleBaselineDefects) != 1 {
		t.Fatalf("identical bodies: terminal=%s stale=%v, want match with CHAOS-5447 stale", same.TerminalState, same.StaleBaselineDefects)
	}
}

func itoa(n int) string {
	const digits = "0123456789"
	if n < 10 {
		return string(digits[n])
	}
	return itoa(n/10) + string(digits[n%10])
}

// The same rule through the REAL writer: Runner -> admission -> Compare ->
// ReceiptsFor, with the real hotspots declaration. This is the path the
// reviewer executed end to end: a Go build returning no rows
// produced a receipt with outside=0 that `enable --mode primary` admitted.
// Every cell here must yield a receipt whose outside count keeps it out of
// the enablement rule, except the ones the citation genuinely covers.
func TestTheWriterRecordsShapeDifferencesOutsideTheCitation(t *testing.T) {
	python := `{"data":{"hotspots":{"rows":[{"filePath":"a.go","repoId":"r1","churnCommits30d":3,"blameConcentration":0.5},{"filePath":"b.go","repoId":"r1","churnCommits30d":2,"blameConcentration":0.25}]}}}`
	for _, cell := range []struct {
		name      string
		goBody    string
		outsideGE int  // the receipt's outside count is at least this
		eligible  bool // mismatch + cited + outside==0 + bound: what the enablement rule reads as proof
	}{
		{"rows empty", `{"data":{"hotspots":{"rows":[]}}}`, 1, false},
		{"rows null", `{"data":{"hotspots":{"rows":null}}}`, 1, false},
		{"rows key absent", `{"data":{"hotspots":{"total":0}}}`, 1, false},
		{"rows of empty objects", `{"data":{"hotspots":{"rows":[{},{}]}}}`, 1, false},
		{"a row that is null", `{"data":{"hotspots":{"rows":[null,{"filePath":"b.go","repoId":"r1","churnCommits30d":2,"blameConcentration":0.25}]}}}`, 1, false},
		// A leaf null is a LEAF difference: hotspots' row selection surfaces
		// as null in one plane and a value in the other (JOB 5 shows both
		// directions). Covered.
		{"a leaf null where Python has a value", `{"data":{"hotspots":{"rows":[{"filePath":"a.go","repoId":"r1","churnCommits30d":3,"blameConcentration":null},{"filePath":"b.go","repoId":"r1","churnCommits30d":2,"blameConcentration":0.25}]}}}`, 0, true},
		// The declared defect itself: Python selected another physical row,
		// so values differ, shape identical. Covered -- this is what the
		// citation exists to say.
		{"value differences only (the declared defect)", `{"data":{"hotspots":{"rows":[{"filePath":"a.go","repoId":"r1","churnCommits30d":9,"blameConcentration":0.75},{"filePath":"c.go","repoId":"r2","churnCommits30d":2,"blameConcentration":0.25}]}}}`, 0, true},
		// Same shape, every value different: another org's files. Leaf
		// differences, indistinguishable from the declared defect's (the real
		// JOB 5 hotspots diff already differs in filePath/repoId), so the
		// citation covers them. Recorded as the contract: the guard for this
		// class is the org scoping of the proof request, not the citation.
		{"same-shape rows with foreign values", `{"data":{"hotspots":{"rows":[{"filePath":"zz/x.go","repoId":"other","churnCommits30d":999,"blameConcentration":0.99},{"filePath":"zz/y.go","repoId":"other","churnCommits30d":0,"blameConcentration":0.01}]}}}`, 0, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			build := "b18e56fa79cfe20ce0f75df148144b832d92be36"
			edge := &fakeEdge{goBody: cell.goBody, pythonBody: python, goBuild: build}
			runner := newRunner(t, edge, "primary")
			runner.Documents = map[string]string{"hotspots": "query Hotspots { hotspots { rows { filePath } } }"}
			runner.Registry = RegistryView{SchemaDigest: "sha256:29d509cd", BuildIdentity: build, DocumentDigest: map[string]string{"hotspots": "6ccfcc78"}}
			runner.Routing = map[string]RoutingRow{"hotspots": {Mode: "primary", CandidateBuild: build}}
			outcomes, _, err := runner.Run(context.Background())
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			receipts, err := runner.ReceiptsFor(time.Unix(1757000000, 0).UTC())
			if err != nil || len(receipts) != 1 {
				t.Fatalf("ReceiptsFor: %d receipts, err=%v", len(receipts), err)
			}
			r := receipts[0]
			eligible := r.TerminalState == TerminalStateMismatch && r.DifferencesOutsideBaselineDefect == 0 &&
				len(r.BaselineDefects) > 0 && r.BuildBinding == EdgeBuildPresent
			if r.DifferencesOutsideBaselineDefect < cell.outsideGE || eligible != cell.eligible {
				t.Fatalf("%s: receipt terminal=%s outside=%d cited=%v binding=%s -> enablement-eligible=%v, want outside>=%d eligible=%v (findings %+v)",
					cell.name, r.TerminalState, r.DifferencesOutsideBaselineDefect, r.BaselineDefects, r.BuildBinding, eligible, cell.outsideGE, cell.eligible, outcomes[0].Findings)
			}
			t.Logf("cell %-46s receipt terminal=%s outside=%d cited=%v -> enablement-eligible=%v", cell.name, r.TerminalState, r.DifferencesOutsideBaselineDefect, r.BaselineDefects, eligible)
		})
	}
}

// Every finding site the comparator has, under a citation that reaches it.
// The hotspots table above exercises lists, dict keys, nulls and types; this
// one covers the remaining sites -- an order-insensitive (keyed) list, `data`
// itself, the `errors` list, every container/leaf kind pair, a non-finite
// number -- and the leaf kinds a citation MUST still cover (bool, integer,
// string, null, a scalar type change), so the rule is pinned in both
// directions at every site: a structural difference is never covered, a leaf
// difference always is.
func TestEveryFindingSiteHonoursTheCitationRule(t *testing.T) {
	keyed := Options{
		OrderInsensitiveLists: []OrderInsensitiveList{{Path: "data.x.rows", KeyFields: []string{"id"}, Reason: "order carries no signal", Ticket: "CHAOS-0001"}},
		BaselineDefects:       []BaselineDefect{{Ticket: "CHAOS-0002", Reason: "the cited defect", Paths: []string{"data.x.rows"}}},
	}
	cite := func(path string) Options {
		return Options{BaselineDefects: []BaselineDefect{{Ticket: "CHAOS-0002", Reason: "the cited defect", Paths: []string{path}}}}
	}
	for _, c := range []struct {
		name                string
		opts                Options
		baseline, candidate string
		outside             int
		counts              string // FormatShapeCounts: WHICH shapes were covered and outside
	}{
		{"keyed list: one key on each side only (presence)", keyed,
			`{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"2","v":2}]}}}`, `{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"3","v":2}]}}}`, 2, "covered[] outside[presence=2]"},
		{"keyed list: a value under a shared key (covered)", keyed,
			`{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"2","v":2}]}}}`, `{"data":{"x":{"rows":[{"id":"2","v":2},{"id":"1","v":9}]}}}`, 0, "covered[value=1] outside[]"},
		{"data absent on the candidate (presence), citing data", cite("data"),
			`{"data":{"x":1}}`, `{}`, 1, "covered[] outside[presence=1]"},
		{"an error on one side only (presence), citing errors", cite("errors"),
			`{"data":{"x":1},"errors":[{"message":"boom","path":["x"]}]}`, `{"data":{"x":1}}`, 1, "covered[] outside[presence=1]"},
		{"a non-finite number (1e400) where Python has 1", cite("data.x"),
			`{"data":{"x":1}}`, `{"data":{"x":1e400}}`, 1, "covered[] outside[non_finite=1]"},
		{"a leaf null against a scalar, a non-null leaf beside it (covered)", cite("data.x"),
			`{"data":{"x":["a","k"]}}`, `{"data":{"x":[null,"k"]}}`, 0, "covered[null=1] outside[]"},
		{"the only leaf null (an empty result in disguise)", cite("data.x"),
			`{"data":{"x":"a"}}`, `{"data":{"x":null}}`, 1, "covered[] outside[empty_result=1]"},
		{"a scalar of another type (covered)", cite("data.x"),
			`{"data":{"x":"1"}}`, `{"data":{"x":1}}`, 0, "covered[scalar_type=1] outside[]"},
		{"an object against null (structure)", cite("data.x"),
			`{"data":{"x":{"k":1}}}`, `{"data":{"x":null}}`, 1, "covered[] outside[structure=1]"},
		{"a list against a scalar (structure)", cite("data.x"),
			`{"data":{"x":[1]}}`, `{"data":{"x":1}}`, 1, "covered[] outside[structure=1]"},
		{"an object against a list (structure)", cite("data.x"),
			`{"data":{"x":{"k":1}}}`, `{"data":{"x":[1]}}`, 1, "covered[] outside[structure=1]"},
		{"a non-finite number against null (never covered: forbidden in Go output)", cite("data.x"),
			`{"data":{"x":1e400}}`, `{"data":{"x":null}}`, 1, "covered[] outside[non_finite=1]"},
		{"a bool against a number (scalar type, covered)", cite("data.x"),
			`{"data":{"x":[true,"k"]}}`, `{"data":{"x":[1,"k"]}}`, 0, "covered[scalar_type=1] outside[]"},
		{"an error on the candidate only (presence), citing errors", cite("errors"),
			`{"data":{"x":1}}`, `{"data":{"x":1},"errors":[{"message":"boom","path":["x"]}]}`, 1, "covered[] outside[presence=1]"},
		{"a Tier-B float beyond tolerance (a leaf value, covered)", Options{
			FloatTierB:      map[string]string{"data.x": "CHAOS-0003 merged Float64 aggregate"},
			BaselineDefects: []BaselineDefect{{Ticket: "CHAOS-0002", Reason: "the cited defect", Paths: []string{"data.x"}}},
		}, `{"data":{"x":[1.0,2.0]}}`, `{"data":{"x":[1.5,2.0]}}`, 0, "covered[value=1] outside[]"},
		{"every candidate leaf null under the citation (an empty result in disguise)", cite("data.x"),
			`{"data":{"x":{"a":1,"b":"s"}}}`, `{"data":{"x":{"a":null,"b":null}}}`, 2, "covered[] outside[empty_result=2]"},
		{"the BASELINE has no non-null leaf (the candidate's values are covered)", cite("data.x"),
			`{"data":{"x":{"a":null}}}`, `{"data":{"x":{"a":"v"}}}`, 0, "covered[null=1] outside[]"},
		{"a bool value (covered)", cite("data.x"),
			`{"data":{"x":true}}`, `{"data":{"x":false}}`, 0, "covered[value=1] outside[]"},
		{"an integer value (covered)", cite("data.x"),
			`{"data":{"x":1}}`, `{"data":{"x":2}}`, 0, "covered[value=1] outside[]"},
		{"a string value (covered)", cite("data.x"),
			`{"data":{"x":"a"}}`, `{"data":{"x":"b"}}`, 0, "covered[value=1] outside[]"},
	} {
		result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), c.opts)
		if result.TerminalState != TerminalStateMismatch || result.DifferencesOutsideBaselineDefect != c.outside ||
			len(result.BaselineDefectsMatched) != 1 {
			t.Fatalf("%s: terminal=%s outside=%d matched=%v, want mismatch outside=%d with the citation matched -- findings %+v",
				c.name, result.TerminalState, result.DifferencesOutsideBaselineDefect, result.BaselineDefectsMatched, c.outside, result.Findings)
		}
		if got := FormatShapeCounts(result.CoveredByShape, result.OutsideByShape); got != c.counts {
			t.Fatalf("%s: shape counts %s, want %s -- findings %+v", c.name, got, c.counts, result.Findings)
		}
		t.Logf("cell %-56s outside=%d %s", c.name, result.DifferencesOutsideBaselineDefect, c.counts)
	}
}

// F1 (CHAOS-5484 opus-r9): citedSegments resolved a citation of exactly
// "data" the same way as one not under `data` at all -- nil, "nothing to
// count" -- so nonNullLeaves saw 0 on both sides of the empty-result
// addendum's guard and it always short-circuited to "nothing to
// relabel". defectCovers meanwhile matches "data" against every finding
// (every finding path starts "data."), so a candidate that returned no
// data at all covered its own absence: outside=0, admitted.
//
// This is the axis the rest of the suite never sweeps: the shape of the
// CITED PATH STRING itself, independent of which payload it points at.
// One payload -- every leaf under `x.y` null in the candidate, present in
// the baseline -- swept over the path domain: the root ("data"), a
// malformed root (trailing dot, double dot), a path one and two segments
// deep, and a path not under `data` at all (bare segment, empty string).
// Only "data" ever disagreed with what defectCovers matched -- fixed by
// treating "data" as the empty segment list (walk everything) rather than
// as "not under data" (nil, walk nothing).
func TestCitationPathDomainForTheEmptyResultAddendum(t *testing.T) {
	baseline := `{"data":{"x":{"y":"python"}}}`
	candidate := `{"data":{"x":{"y":null}}}`
	for _, c := range []struct {
		cited   string
		matched bool // the citation is live (defectCovers hits the finding) rather than stale
	}{
		{"data", true},     // the root: every leaf under it, addendum applies
		{"data.x", true},   // one segment in: contains the null leaf
		{"data.x.y", true}, // the exact leaf
		{"data.", false},   // malformed: an empty final segment matches no key
		{"data..x", false}, // malformed: an empty segment before "x" matches no key
		{"x", false},       // not under `data` at all
		{"", false},        // not under `data` at all
	} {
		opts := Options{BaselineDefects: []BaselineDefect{{Ticket: "CHAOS-5484", Reason: "path-domain sweep", Paths: []string{c.cited}}}}
		result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), opts)
		if result.TerminalState != TerminalStateMismatch || result.DifferencesOutsideBaselineDefect != 1 {
			t.Fatalf("cited=%q: terminal=%s outside=%d, want mismatch outside=1 (an empty result must never admit) -- findings %+v",
				c.cited, result.TerminalState, result.DifferencesOutsideBaselineDefect, result.Findings)
		}
		matched := len(result.BaselineDefectsMatched) == 1
		if matched != c.matched {
			t.Fatalf("cited=%q: matched=%v stale=%v, want matched=%v", c.cited, result.BaselineDefectsMatched, result.StaleBaselineDefects, c.matched)
		}
		counts := FormatShapeCounts(result.CoveredByShape, result.OutsideByShape)
		wantCounts := "covered[] outside[null=1]"
		if c.matched {
			wantCounts = "covered[] outside[empty_result=1]"
		}
		if counts != wantCounts {
			t.Fatalf("cited=%q: shape counts %s, want %s -- findings %+v", c.cited, counts, wantCounts, result.Findings)
		}
		t.Logf("cited=%-10q outside=1 matched=%v %s", c.cited, matched, counts)
	}
}

// Every BaselineDefect path DECLARED in operationSpecs, not only hotspots':
// for each, eight candidates at that exact path -- three leaf differences (a
// value, a null leaf beside a non-null one, another scalar type: covered)
// and five structural ones (the only leaf null -- an empty result in
// disguise -- key absent, list length, container against
// null, container against a scalar: outside). A new declaration is swept by
// this test the day it is added.
func TestEveryDeclaredCitationCoversOnlyValueDifferences(t *testing.T) {
	nest := func(path string, leaf any, present bool) string {
		segments := strings.Split(path, ".")
		var value any = map[string]any{}
		if present {
			value = map[string]any{segments[len(segments)-1]: leaf}
		}
		for i := len(segments) - 2; i >= 0; i-- {
			value = map[string]any{segments[i]: value}
		}
		body, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return string(body)
	}
	swept := 0
	for _, operation := range KnownOperations() {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("SpecFor(%s): %v", operation, err)
		}
		for _, defect := range spec.Parity.BaselineDefects {
			for _, cited := range defect.Paths {
				if !strings.HasPrefix(cited, "data.") {
					t.Fatalf("%s %s: cited path %q is not under data", operation, defect.Ticket, cited)
				}
				opts := Options{BaselineDefects: []BaselineDefect{defect}}
				for _, c := range []struct {
					name                string
					baseline, candidate string
					outside             int
				}{
					{"leaf value", nest(cited, "python", true), nest(cited, "go", true), 0},
					{"a null leaf beside a non-null one", nest(cited, []any{"python", "k"}, true), nest(cited, []any{nil, "k"}, true), 0},
					{"the only leaf null (an empty result in disguise)", nest(cited, "python", true), nest(cited, nil, true), 1},
					{"leaf of another scalar type", nest(cited, "python", true), nest(cited, 7, true), 0},
					{"key absent", nest(cited, "python", true), nest(cited, nil, false), 1},
					{"list length", nest(cited, []any{"python"}, true), nest(cited, []any{}, true), 1},
					{"container against null", nest(cited, map[string]any{"k": "python"}, true), nest(cited, nil, true), 1},
					{"container against a scalar", nest(cited, []any{"python"}, true), nest(cited, "go", true), 1},
				} {
					result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), opts)
					if result.DifferencesOutsideBaselineDefect != c.outside || len(result.BaselineDefectsMatched) != 1 {
						t.Fatalf("%s %s %q, %s: outside=%d matched=%v, want outside=%d with %s matched -- findings %+v",
							operation, defect.Ticket, cited, c.name, result.DifferencesOutsideBaselineDefect, result.BaselineDefectsMatched, c.outside, defect.Ticket, result.Findings)
					}
				}
				swept++
				t.Logf("path %-18s %-10s %-48s 3 leaf covered; every leaf null, key absent, list length, container vs null, container vs scalar outside", operation, defect.Ticket, cited)
			}
		}
	}
	if swept == 0 {
		t.Fatal("no declared BaselineDefect path was swept")
	}
	t.Logf("declared citation paths swept: %d", swept)
}

// The receipt stored only the outside COUNT and the tickets,
// so what a citation covered was invisible after the run. Every receipt's
// provenance now carries the per-shape counts, and they add up: outside by
// shape sums to differences_outside_baseline_defect -- including the
// transport differences no citation can express -- and covered + outside is
// every mismatch finding.
func TestTheReceiptRecordsWhatItsCountsAreMadeOf(t *testing.T) {
	python := `{"data":{"hotspots":{"rows":[{"filePath":"a.go","churnCommits30d":3},{"filePath":"b.go","churnCommits30d":2}]}}}`
	build := "b18e56fa79cfe20ce0f75df148144b832d92be36"
	for _, cell := range []struct {
		name    string
		edge    *fakeEdge
		covered string
	}{
		{"leaf values differ", &fakeEdge{goBody: `{"data":{"hotspots":{"rows":[{"filePath":"a.go","churnCommits30d":9},{"filePath":"c.go","churnCommits30d":2}]}}}`, pythonBody: python, goBuild: build},
			"covered[value=2] outside[]"},
		{"rows empty", &fakeEdge{goBody: `{"data":{"hotspots":{"rows":[]}}}`, pythonBody: python, goBuild: build},
			"covered[] outside[length=1]"},
		{"a leaf value and an HTTP status difference", &fakeEdge{goBody: `{"data":{"hotspots":{"rows":[{"filePath":"a.go","churnCommits30d":9},{"filePath":"b.go","churnCommits30d":2}]}}}`, pythonBody: python, goBuild: build, goStatus: 203},
			"covered[value=1] outside[http=1]"},
		{"a leaf value on an unbound edge measurement", &fakeEdge{goBody: `{"data":{"hotspots":{"rows":[{"filePath":"a.go","churnCommits30d":9},{"filePath":"b.go","churnCommits30d":2}]}}}`, pythonBody: python},
			"covered[value=1] outside[unbound_edge=1]"},
	} {
		runner := newRunner(t, cell.edge, "primary")
		runner.Documents = map[string]string{"hotspots": "query Hotspots { hotspots { rows { filePath } } }"}
		runner.Registry = RegistryView{SchemaDigest: "sha256:29d509cd", BuildIdentity: build, DocumentDigest: map[string]string{"hotspots": "6ccfcc78"}}
		runner.Routing = map[string]RoutingRow{"hotspots": {Mode: "primary", CandidateBuild: build}}
		outcomes, _, err := runner.Run(context.Background())
		if err != nil {
			t.Fatalf("%s: Run: %v", cell.name, err)
		}
		receipts, err := runner.ReceiptsFor(time.Unix(1757000000, 0).UTC())
		if err != nil || len(receipts) != 1 {
			t.Fatalf("%s: ReceiptsFor: %d receipts, err=%v", cell.name, len(receipts), err)
		}
		var provenance ReceiptProvenance
		if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil {
			t.Fatalf("%s: provenance is not JSON: %v (%q)", cell.name, err, receipts[0].ReviewEvidence)
		}
		// The key names are the contract `enable` (Python) reads; pinned on
		// the raw text, not only through this package's own struct tags.
		for key, counts := range map[string]map[string]int{`"covered_by_shape":`: provenance.CoveredByShape, `"outside_by_shape":`: provenance.OutsideByShape} {
			if (len(counts) > 0) != strings.Contains(receipts[0].ReviewEvidence, key) {
				t.Fatalf("%s: provenance %s: key %s present=%v, counts %v", cell.name, receipts[0].ReviewEvidence, key, strings.Contains(receipts[0].ReviewEvidence, key), counts)
			}
		}
		got := FormatShapeCounts(provenance.CoveredByShape, provenance.OutsideByShape)
		if got != cell.covered {
			t.Fatalf("%s: receipt provenance %s, want %s", cell.name, got, cell.covered)
		}
		sum := func(counts map[string]int) int {
			total := 0
			for _, n := range counts {
				total += n
			}
			return total
		}
		mismatches := 0
		for _, f := range outcomes[0].Findings {
			if f.Kind == FindingMismatch {
				mismatches++
			}
		}
		if sum(provenance.OutsideByShape) != receipts[0].DifferencesOutsideBaselineDefect ||
			sum(provenance.CoveredByShape)+sum(provenance.OutsideByShape) != mismatches {
			t.Fatalf("%s: counts do not add up: %s, outside=%d, mismatch findings=%d", cell.name, got, receipts[0].DifferencesOutsideBaselineDefect, mismatches)
		}
		t.Logf("cell %-44s receipt outside=%d provenance %s", cell.name, receipts[0].DifferencesOutsideBaselineDefect, got)
	}
}

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
// opus r7 (P1-1): `hotspots` cites the whole `data.hotspots.rows` subtree for
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
// The rule (team-lead ruling on opus r7 P1-1): a citation covers a difference
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
// reviewer executed end to end (opus r7 P1-1): a Go build returning no rows
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
		// directions). Covered, by the team-lead ruling on opus r7 P1-1.
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
	}{
		{"keyed list: one key on each side only (presence)", keyed,
			`{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"2","v":2}]}}}`, `{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"3","v":2}]}}}`, 2},
		{"keyed list: a value under a shared key (covered)", keyed,
			`{"data":{"x":{"rows":[{"id":"1","v":1},{"id":"2","v":2}]}}}`, `{"data":{"x":{"rows":[{"id":"2","v":2},{"id":"1","v":9}]}}}`, 0},
		{"data absent on the candidate (presence), citing data", cite("data"),
			`{"data":{"x":1}}`, `{}`, 1},
		{"an error on one side only (presence), citing errors", cite("errors"),
			`{"data":{"x":1},"errors":[{"message":"boom","path":["x"]}]}`, `{"data":{"x":1}}`, 1},
		{"a non-finite number (1e400) where Python has 1", cite("data.x"),
			`{"data":{"x":1}}`, `{"data":{"x":1e400}}`, 1},
		{"a leaf null against a scalar (covered)", cite("data.x"),
			`{"data":{"x":"a"}}`, `{"data":{"x":null}}`, 0},
		{"a scalar of another type (covered)", cite("data.x"),
			`{"data":{"x":"1"}}`, `{"data":{"x":1}}`, 0},
		{"an object against null (structure)", cite("data.x"),
			`{"data":{"x":{"k":1}}}`, `{"data":{"x":null}}`, 1},
		{"a list against a scalar (structure)", cite("data.x"),
			`{"data":{"x":[1]}}`, `{"data":{"x":1}}`, 1},
		{"an object against a list (structure)", cite("data.x"),
			`{"data":{"x":{"k":1}}}`, `{"data":{"x":[1]}}`, 1},
		{"a non-finite number against null (never covered)", cite("data.x"),
			`{"data":{"x":1e400}}`, `{"data":{"x":null}}`, 1},
		{"a bool value (covered)", cite("data.x"),
			`{"data":{"x":true}}`, `{"data":{"x":false}}`, 0},
		{"an integer value (covered)", cite("data.x"),
			`{"data":{"x":1}}`, `{"data":{"x":2}}`, 0},
		{"a string value (covered)", cite("data.x"),
			`{"data":{"x":"a"}}`, `{"data":{"x":"b"}}`, 0},
	} {
		result := Compare(snapshotFromJSON(t, c.baseline), snapshotFromJSON(t, c.candidate), c.opts)
		if result.TerminalState != TerminalStateMismatch || result.DifferencesOutsideBaselineDefect != c.outside ||
			len(result.BaselineDefectsMatched) != 1 {
			t.Fatalf("%s: terminal=%s outside=%d matched=%v, want mismatch outside=%d with the citation matched -- findings %+v",
				c.name, result.TerminalState, result.DifferencesOutsideBaselineDefect, result.BaselineDefectsMatched, c.outside, result.Findings)
		}
		t.Logf("cell %-56s outside=%d", c.name, result.DifferencesOutsideBaselineDefect)
	}
}

// Every BaselineDefect path DECLARED in operationSpecs, not only hotspots':
// for each, seven candidates at that exact path -- three leaf differences (a
// value, a null, another scalar type: covered) and four structural ones (key
// absent, list length, container against null, container against a scalar:
// outside). A new declaration is swept by this test the day it is added.
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
					{"leaf null", nest(cited, "python", true), nest(cited, nil, true), 0},
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
				t.Logf("path %-18s %-10s %-48s leaf value, leaf null, scalar type covered; key absent, list length, container vs null, container vs scalar outside", operation, defect.Ticket, cited)
			}
		}
	}
	if swept == 0 {
		t.Fatal("no declared BaselineDefect path was swept")
	}
	t.Logf("declared citation paths swept: %d", swept)
}

package goapiproof

import (
	"context"
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
// every shape a candidate can take, through the real comparator.
func TestACitationCoversOnlyLeafValueDifferences(t *testing.T) {
	spec, err := SpecFor("hotspots")
	if err != nil {
		t.Fatalf("SpecFor(hotspots): %v", err)
	}
	opts := spec.Parity
	row := func(file, repo string, churn int, blame string) string {
		return `{"filePath":"` + file + `","repoId":"` + repo + `","churnCommits30d":` + itoa(churn) + `,"blameConcentration":` + blame + `,"riskScore":1.5}`
	}
	baseline := `{"data":{"hotspots":{"rows":[` + row("a.go", "r1", 5, "0.5") + `,` + row("b.go", "r1", 2, "0.25") + `]}}}`

	for _, c := range []struct {
		name      string
		candidate string
		outside   int  // differences NO citation may cover
		matched   bool // CHAOS-5447 is recorded as matched (not stale)
	}{
		{"leaf value differences only (the declared defect: another physical row)",
			`{"data":{"hotspots":{"rows":[` + row("a.go", "r1", 9, "0.5") + `,` + row("c.go", "r2", 2, "0.75") + `]}}}`, 0, true},
		{"rows empty (a length difference)", `{"data":{"hotspots":{"rows":[]}}}`, 1, true},
		{"rows null (null where a list was)", `{"data":{"hotspots":{"rows":null}}}`, 1, true},
		{"rows of empty objects (every key missing)", `{"data":{"hotspots":{"rows":[{},{}]}}}`, 10, true},
		{"one extra row (a length difference)",
			`{"data":{"hotspots":{"rows":[` + row("a.go", "r1", 5, "0.5") + `,` + row("b.go", "r1", 2, "0.25") + `,` + row("c.go", "r1", 1, "0.1") + `]}}}`, 1, true},
		{"a leaf null where Python has a value",
			`{"data":{"hotspots":{"rows":[` + row("a.go", "r1", 5, "null") + `,` + row("b.go", "r1", 2, "0.25") + `]}}}`, 1, true},
		{"a leaf of another JSON type (string for number)",
			`{"data":{"hotspots":{"rows":[` + row("a.go", "r1", 5, `"0.5"`) + `,` + row("b.go", "r1", 2, "0.25") + `]}}}`, 1, true},
		{"a row that is an array, not an object",
			`{"data":{"hotspots":{"rows":[[],` + row("b.go", "r1", 2, "0.25") + `]}}}`, 1, true},
		{"an extra key in a row",
			`{"data":{"hotspots":{"rows":[` + strings.TrimSuffix(row("a.go", "r1", 5, "0.5"), "}") + `,"extra":1}` + `,` + row("b.go", "r1", 2, "0.25") + `]}}}`, 1, true},
		{"value differences AND one null: the null alone is outside",
			`{"data":{"hotspots":{"rows":[` + row("z.go", "r9", 7, "null") + `,` + row("b.go", "r1", 3, "0.25") + `]}}}`, 1, true},
	} {
		result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, c.candidate), opts)
		if result.TerminalState != TerminalStateMismatch {
			t.Fatalf("%s: terminal %q, want mismatch", c.name, result.TerminalState)
		}
		if result.DifferencesOutsideBaselineDefect != c.outside {
			t.Fatalf("%s: outside=%d, want %d -- findings %+v", c.name, result.DifferencesOutsideBaselineDefect, c.outside, result.Findings)
		}
		gotMatched := len(result.BaselineDefectsMatched) == 1 && result.BaselineDefectsMatched[0] == "CHAOS-5447"
		if gotMatched != c.matched || len(result.StaleBaselineDefects) != 0 {
			t.Fatalf("%s: matched=%v stale=%v, want CHAOS-5447 matched=%v and nothing stale (a citation with differences beneath it is live, whether or not it may cover them)",
				c.name, result.BaselineDefectsMatched, result.StaleBaselineDefects, c.matched)
		}
		shapes := map[string]int{}
		for _, f := range result.Findings {
			shapes[f.Shape]++
		}
		t.Logf("cell %-72s outside=%-2d findings by shape=%v", c.name, result.DifferencesOutsideBaselineDefect, shapes)
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
		{"a null where Python has a value", `{"data":{"hotspots":{"rows":[{"filePath":"a.go","repoId":"r1","churnCommits30d":3,"blameConcentration":null},{"filePath":"b.go","repoId":"r1","churnCommits30d":2,"blameConcentration":0.25}]}}}`, 1, false},
		// The declared defect itself: Python selected another physical row,
		// so values differ, shape identical. Covered -- this is what the
		// citation exists to say.
		{"value differences only (the declared defect)", `{"data":{"hotspots":{"rows":[{"filePath":"a.go","repoId":"r1","churnCommits30d":9,"blameConcentration":0.75},{"filePath":"c.go","repoId":"r2","churnCommits30d":2,"blameConcentration":0.25}]}}}`, 0, true},
		// Same shape, every value different: indistinguishable BY SHAPE from
		// the declared defect (the real JOB 5 hotspots diff already differs
		// in filePath/repoId), so the citation covers it. Recorded as the
		// contract, not hidden: separating it would need a per-path rule.
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

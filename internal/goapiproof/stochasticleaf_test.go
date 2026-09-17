package goapiproof

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// registeredDocumentConst reads one registered document constant from the
// query-api route table, so the document checks run against the text the
// binary registers rather than a copy.
func registeredDocumentConst(t *testing.T, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), queryRouteSource, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", queryRouteSource, err)
	}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, ident := range value.Names {
				if ident.Name != name || i >= len(value.Values) {
					continue
				}
				literal, ok := value.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s is not a string literal", name)
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", name, err)
				}
				return text
			}
		}
	}
	t.Fatalf("%s not found in %s", name, queryRouteSource)
	return ""
}

func capacityForecastDocument(t *testing.T) string {
	return registeredDocumentConst(t, "registeredCapacityForecastDocument")
}

// capacityForecastParity is the committed parity of capacityForecast.
func capacityForecastParity(t *testing.T) Options {
	t.Helper()
	spec, err := SpecFor("capacityForecast")
	if err != nil {
		t.Fatalf("SpecFor: %v", err)
	}
	if spec.Parity.StochasticLeaves == nil {
		t.Fatal("capacityForecast declares no stochastic leaf class")
	}
	return spec.Parity
}

// forecastBase is the synthetic computedAt the test bodies are built on.
var forecastBase = time.Date(2031, time.January, 6, 16, 40, 0, 0, time.UTC)

// dayAt renders base's UTC date plus n days.
func dayAt(base time.Time, n int) string {
	return base.AddDate(0, 0, n).Format(stochasticDateLayout)
}

func forecastDay(n int) string { return dayAt(forecastBase, n) }

// forecastBody renders one capacityForecast response on forecastBase.
func forecastBody(t *testing.T, plane string, overrides map[string]any) string {
	t.Helper()
	return forecastBodyAt(t, forecastBase, plane, overrides)
}

// forecastBodyAt renders one capacityForecast response. Every field the
// registered document selects is present. The percentile values are
// self-consistent against base: p50 2 days, p85 3, p95 5. The items leaves
// are null, as they are for a request with no targetDate. computedAt takes
// each plane's own wire form.
func forecastBodyAt(t *testing.T, base time.Time, plane string, overrides map[string]any) string {
	t.Helper()
	fields := map[string]any{
		"forecastId":          "forecast-" + plane,
		"computedAt":          base.Add(123456 * time.Microsecond).Format("2006-01-02T15:04:05.000000+00:00"),
		"teamId":              nil,
		"workScopeId":         nil,
		"backlogSize":         12,
		"targetItems":         12,
		"targetDate":          nil,
		"p50Date":             dayAt(base, 2),
		"p85Date":             dayAt(base, 3),
		"p95Date":             dayAt(base, 5),
		"p50Days":             2,
		"p85Days":             3,
		"p95Days":             5,
		"p50Items":            nil,
		"p85Items":            nil,
		"p95Items":            nil,
		"throughputMean":      3.5,
		"throughputStddev":    1.25,
		"historyDays":         90,
		"insufficientHistory": false,
		"highVariance":        false,
		"__typename":          "CapacityForecast",
	}
	if plane == "python" {
		fields["computedAt"] = base.Add(654321 * time.Microsecond).Format("2006-01-02 15:04:05.000000+00:00")
	}
	for key, value := range overrides {
		fields[key] = value
	}
	encoded, err := json.Marshal(map[string]any{"data": map[string]any{"capacityForecast": fields}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(encoded)
}

func compareForecast(t *testing.T, baseline, candidate map[string]any, opts Options) Result {
	t.Helper()
	return compareForecastAt(t, forecastBase, baseline, candidate, opts)
}

func compareForecastAt(t *testing.T, base time.Time, baseline, candidate map[string]any, opts Options) Result {
	t.Helper()
	return Compare(
		snapshotFromJSON(t, forecastBodyAt(t, base, "python", baseline)),
		snapshotFromJSON(t, forecastBodyAt(t, base, "go", candidate)),
		opts,
	)
}

func shapesOf(result Result, kind string) []string {
	var out []string
	for _, finding := range result.Findings {
		if finding.Kind == kind {
			out = append(out, finding.Shape+" "+finding.Path)
		}
	}
	sort.Strings(out)
	return out
}

// requireNotProven asserts a comparison cannot stand as proof: at least one
// difference is outside every citation, and, when wantShape is not empty,
// one of them has that shape.
func requireNotProven(t *testing.T, result Result, wantShape string) {
	t.Helper()
	if len(result.StochasticLeafRefusals) > 0 {
		t.Fatalf("unexpected refusal: %v", result.StochasticLeafRefusals)
	}
	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("expected a difference outside every citation, got none; findings %v", shapesOf(result, FindingMismatch))
	}
	if wantShape != "" && result.OutsideByShape[wantShape] == 0 {
		t.Fatalf("expected an outside %q finding, got outside %v; findings %v", wantShape, result.OutsideByShape, shapesOf(result, FindingMismatch))
	}
}

var capturedDetail = regexp.MustCompile(`^(\S+) != (\S+)`)

func leafValue(raw string) any {
	if number, err := strconv.Atoi(raw); err == nil {
		return number
	}
	return raw
}

// (a) The two captured production failures pass under the class.
//
// The captured reports carry each differing leaf's two values, not the
// response bodies, so each pair is rebuilt: the leaves the report names take
// the reported values, computedAt's date is the reported date minus the
// reported days, and every other leaf takes forecastBodyAt's values, chosen
// to be consistent with them. The first half of the test proves the rebuild
// reproduces the captured findings exactly without the class.
func TestCapturedCapacityForecastPairsPassUnderTheStochasticLeafClass(t *testing.T) {
	parity := capacityForecastParity(t)
	withoutClass := parity
	withoutClass.StochasticLeaves = nil

	for _, c := range []struct {
		fixture string
		// days overrides forecastBodyAt's days for leaves the report does
		// not name; each date follows its days.
		days map[string]int
	}{
		{"capacityforecast_stochastic_first_report_4f6d2039.json", map[string]int{}},
		{"capacityforecast_stochastic_second_report_c1ad30cd.json", map[string]int{"p50": 3, "p85": 4}},
	} {
		t.Run(c.fixture, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", c.fixture))
			if err != nil {
				t.Fatalf("read fixture: %v", err)
			}
			var captured struct {
				Operation string    `json:"operation"`
				Findings  []Finding `json:"findings"`
			}
			if err := json.Unmarshal(raw, &captured); err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if captured.Operation != "capacityForecast" || len(captured.Findings) == 0 {
				t.Fatalf("fixture is not a failing capacityForecast outcome: %+v", captured)
			}
			baseline, candidate := map[string]any{}, map[string]any{}
			for _, finding := range captured.Findings {
				match := capturedDetail.FindStringSubmatch(finding.Detail)
				if match == nil || !strings.HasPrefix(finding.Path, "$.data.capacityForecast.") {
					t.Fatalf("cannot read captured finding %+v", finding)
				}
				leaf := strings.TrimPrefix(finding.Path, "$.data.capacityForecast.")
				baseline[leaf], candidate[leaf] = leafValue(match[1]), leafValue(match[2])
			}
			var base time.Time
			for leaf := range baseline {
				prefix, isDate := strings.CutSuffix(leaf, "Date")
				if !isDate {
					continue
				}
				for _, plane := range []map[string]any{baseline, candidate} {
					date, err := time.Parse(stochasticDateLayout, plane[leaf].(string))
					if err != nil {
						t.Fatalf("captured %s: %v", leaf, err)
					}
					days, ok := plane[prefix+"Days"].(int)
					if !ok {
						t.Fatalf("captured %s has no %sDays beside it", leaf, prefix)
					}
					planeBase := date.AddDate(0, 0, -days)
					if !base.IsZero() && !base.Equal(planeBase) {
						t.Fatalf("captured planes imply different base dates %s and %s", base, planeBase)
					}
					base = planeBase
				}
			}
			if base.IsZero() {
				t.Fatal("captured findings carry no date/days pair to derive the base date from")
			}
			base = base.Add(16*time.Hour + 40*time.Minute)
			for prefix, days := range c.days {
				for _, plane := range []map[string]any{baseline, candidate} {
					plane[prefix+"Days"], plane[prefix+"Date"] = days, dayAt(base, days)
				}
			}

			before := compareForecastAt(t, base, baseline, candidate, withoutClass)
			if before.TerminalState != TerminalStateMismatch || before.DifferencesOutsideBaselineDefect != len(captured.Findings) {
				t.Fatalf("without the class the rebuilt pair must fail as captured, got %s outside=%d", before.TerminalState, before.DifferencesOutsideBaselineDefect)
			}
			if len(before.Findings) != len(captured.Findings) {
				t.Fatalf("rebuilt findings %v, captured %v", before.Findings, captured.Findings)
			}
			for i := range captured.Findings {
				if before.Findings[i] != captured.Findings[i] {
					t.Fatalf("rebuilt finding %+v, captured %+v", before.Findings[i], captured.Findings[i])
				}
			}

			after := compareForecastAt(t, base, baseline, candidate, parity)
			if len(after.StochasticLeafRefusals) > 0 {
				t.Fatalf("class refused: %v", after.StochasticLeafRefusals)
			}
			if len(after.UnusedExclusions) > 0 {
				t.Fatalf("volatile fields went unused: %v", after.UnusedExclusions)
			}
			if got := shapesOf(after, FindingMismatch); len(got) > 0 {
				t.Fatalf("under the class no mismatch may remain, got %v", got)
			}
			if after.DifferencesOutsideBaselineDefect != 0 {
				t.Fatalf("outside=%d, want 0", after.DifferencesOutsideBaselineDefect)
			}
			if after.TerminalState != TerminalStateMismatch || after.IsMatch() {
				t.Fatalf("a class-proven comparison must never read as a match, got %s", after.TerminalState)
			}
			if after.StochasticLeafCitation != parity.StochasticLeaves.Citation() {
				t.Fatalf("citation %q, want %q", after.StochasticLeafCitation, parity.StochasticLeaves.Citation())
			}
			if after.CoveredByShape[ShapeStochasticLeaf] != len(captured.Findings) {
				t.Fatalf("covered %v, want %s=%d", after.CoveredByShape, ShapeStochasticLeaf, len(captured.Findings))
			}
		})
	}
}

// Draws that happen to agree are still not proof by equality.
func TestAgreeingDrawsAreStillProvenOnlyUnderTheClass(t *testing.T) {
	result := compareForecast(t, nil, nil, capacityForecastParity(t))
	if result.IsMatch() || result.StochasticLeafCitation == "" || result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("identical draws: state=%s citation=%q outside=%d", result.TerminalState, result.StochasticLeafCitation, result.DifferencesOutsideBaselineDefect)
	}
}

// (b) An inverted percentile on either plane fails.
func TestAnInvertedPercentileFailsOnEitherPlane(t *testing.T) {
	parity := capacityForecastParity(t)
	inverted := map[string]any{"p85Days": 1, "p85Date": forecastDay(1)}
	items := map[string]any{"p50Items": 9, "p85Items": 11, "p95Items": 7}
	itemsOK := map[string]any{"p50Items": 11, "p85Items": 9, "p95Items": 7}
	for _, c := range []struct {
		name                string
		baseline, candidate map[string]any
	}{
		{"p85Days below p50Days on the candidate", nil, inverted},
		{"p85Days below p50Days on the baseline", inverted, nil},
		{"p85Items above p50Items on the candidate", itemsOK, items},
		{"p85Items above p50Items on the baseline", items, itemsOK},
	} {
		t.Run(c.name, func(t *testing.T) {
			requireNotProven(t, compareForecast(t, c.baseline, c.candidate, parity), ShapeStochasticOrder)
		})
	}
	t.Run("items in the kernel's order pass", func(t *testing.T) {
		result := compareForecast(t, itemsOK, map[string]any{"p50Items": 12, "p85Items": 10, "p95Items": 6}, parity)
		if result.DifferencesOutsideBaselineDefect != 0 || result.StochasticLeafCitation == "" {
			t.Fatalf("p95Items <= p85Items <= p50Items must pass, got outside %v", result.OutsideByShape)
		}
	})
}

// (c) A null on one plane against a value on the other fails.
func TestANullAgainstAValueFails(t *testing.T) {
	parity := capacityForecastParity(t)
	absent := map[string]any{"p85Days": nil, "p85Date": nil}
	requireNotProven(t, compareForecast(t, absent, nil, parity), ShapeNull)
	requireNotProven(t, compareForecast(t, nil, absent, parity), ShapeNull)
	requireNotProven(t, compareForecast(t, nil, map[string]any{"p50Items": 3, "p85Items": 2, "p95Items": 1}, parity), ShapeNull)

	t.Run("null on both planes passes", func(t *testing.T) {
		result := compareForecast(t, absent, absent, parity)
		if result.DifferencesOutsideBaselineDefect != 0 || result.StochasticLeafCitation == "" {
			t.Fatalf("got outside %v", result.OutsideByShape)
		}
	})
	t.Run("a date null beside a non-null days leaf fails", func(t *testing.T) {
		dateOnly := map[string]any{"p85Date": nil}
		requireNotProven(t, compareForecast(t, dateOnly, dateOnly, parity), ShapeStochasticDateOffset)
	})
}

// (d) A leaf of the wrong type fails, even where both planes agree on it.
func TestAWronglyTypedStochasticLeafFails(t *testing.T) {
	parity := capacityForecastParity(t)
	for _, c := range []struct {
		name                string
		baseline, candidate map[string]any
	}{
		{"days as a string on both planes", map[string]any{"p50Days": "2"}, map[string]any{"p50Days": "2"}},
		{"days as a fraction on the candidate", nil, map[string]any{"p95Days": 5.5}},
		{"a date in another layout on the candidate", nil, map[string]any{"p85Date": forecastBase.AddDate(0, 0, 3).Format("02-01-2006")}},
		{"a date as a timestamp on the baseline", map[string]any{"p50Date": forecastDay(2) + "T00:00:00Z"}, nil},
		{"items as a boolean on both planes", map[string]any{"p50Items": true}, map[string]any{"p50Items": true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			requireNotProven(t, compareForecast(t, c.baseline, c.candidate, parity), ShapeStochasticType)
		})
	}
}

// The date offset: each date is its plane's computedAt date plus its days.
func TestADateInconsistentWithItsDaysFails(t *testing.T) {
	parity := capacityForecastParity(t)
	requireNotProven(t, compareForecast(t, nil, map[string]any{"p95Date": forecastDay(6)}, parity), ShapeStochasticDateOffset)
	requireNotProven(t, compareForecast(t, map[string]any{"p50Date": forecastDay(1)}, nil, parity), ShapeStochasticDateOffset)
	requireNotProven(t, compareForecast(t, map[string]any{"computedAt": "not a timestamp"}, nil, parity), ShapeStochasticType)

	t.Run("the base date is the UTC date of computedAt", func(t *testing.T) {
		// 23:30 at -02:00 on the day before is 01:30 UTC on the base date.
		shifted := map[string]any{
			"computedAt": forecastDay(-1) + " 23:30:00-02:00",
		}
		result := compareForecast(t, shifted, nil, parity)
		if result.DifferencesOutsideBaselineDefect != 0 {
			t.Fatalf("got outside %v: %v", result.OutsideByShape, shapesOf(result, FindingMismatch))
		}
	})
}

// (e) Every leaf the class does not name is still compared exactly.
func TestANonStochasticLeafDifferenceStillFails(t *testing.T) {
	parity := capacityForecastParity(t)
	for _, c := range []struct {
		name     string
		override map[string]any
	}{
		{"backlogSize", map[string]any{"backlogSize": 13}},
		{"targetItems", map[string]any{"targetItems": 13}},
		{"throughputMean", map[string]any{"throughputMean": 3.75}},
		{"historyDays", map[string]any{"historyDays": 89}},
		{"insufficientHistory", map[string]any{"insufficientHistory": true}},
		{"teamId", map[string]any{"teamId": "ABC-123"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := compareForecast(t, nil, c.override, parity)
			requireNotProven(t, result, "")
		})
	}
}

// (f) A declaration that could reach more than its named leaves is refused.
func TestAStochasticLeafDeclarationReachingBeyondNamedLeavesIsRefused(t *testing.T) {
	document := capacityForecastDocument(t)
	parity := capacityForecastParity(t)
	if err := ValidateStochasticLeafClassAgainstDocument(parity, document); err != nil {
		t.Fatalf("the committed declaration must validate against the registered document: %v", err)
	}

	withLeaves := func(leaves ...string) Options {
		opts := Options{VolatileFields: parity.VolatileFields}
		opts.StochasticLeaves = &StochasticLeafClass{
			Ticket: "ABC-123", Reason: "test",
			Orderings: []StochasticOrdering{{Type: StochasticTypeInteger, NonDecreasing: leaves}},
		}
		return opts
	}
	// static marks a defect visible without a document. Compare, which has
	// no document, must refuse those on its own.
	for _, c := range []struct {
		name     string
		opts     Options
		document string
		static   bool
	}{
		{"the operation root (a non-leaf)", withLeaves("data.capacityForecast", "data.capacityForecast.p50Days"), document, true},
		{"a field with a sub-selection (a non-leaf)", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.window"),
			"query Q { capacityForecast { p50Days window { start } } }", false},
		{"a path below a leaf", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p85Days.value"), document, false},
		{"a field the document does not select", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p99Days"), document, false},
		{"a wildcard", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.*"), document, true},
		{"a list index", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p85Days[0]"), document, true},
		{"a root marker", withLeaves("$.data.capacityForecast.p50Days", "data.capacityForecast.p85Days"), document, true},
		{"a parent of another named leaf", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p50Days.x"), document, true},
		{"a leaf named twice", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p50Days"), document, true},
		{"a single leaf, which orders nothing", withLeaves("data.capacityForecast.p50Days"), document, true},
		{"a volatile field", withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.computedAt"), document, true},
		{"an unselected base timestamp", func() Options {
			opts := capacityForecastParity(t)
			class := *opts.StochasticLeaves
			class.BaseTimestampPath = "data.capacityForecast.createdAt"
			opts.StochasticLeaves = &class
			return opts
		}(), document, false},
		{"a blank ticket", func() Options {
			opts := withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p85Days")
			opts.StochasticLeaves.Ticket = " "
			return opts
		}(), document, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			err := ValidateStochasticLeafClassAgainstDocument(c.opts, c.document)
			if err == nil {
				t.Fatal("declaration was accepted")
			}
			t.Logf("refused: %v", err)
			if !c.static {
				return
			}
			if staticErr := validateStochasticLeafClass(c.opts); staticErr == nil {
				t.Fatal("the declaration is accepted without a document")
			}
			result := compareForecast(t, nil, nil, c.opts)
			if len(result.StochasticLeafRefusals) == 0 || result.StochasticLeafCitation != "" {
				t.Fatalf("Compare applied an invalid declaration: %+v", result)
			}
		})
	}

	t.Run("a named leaf that is a container in the response", func(t *testing.T) {
		result := compareForecast(t, map[string]any{"p50Days": map[string]any{"n": 2}}, map[string]any{"p50Days": map[string]any{"n": 2}}, parity)
		if len(result.StochasticLeafRefusals) == 0 || result.StochasticLeafCitation != "" {
			t.Fatalf("a container under a named leaf must refuse, got %+v", result)
		}
	})
	t.Run("a named leaf neither plane carries", func(t *testing.T) {
		opts := withLeaves("data.capacityForecast.p50Days", "data.capacityForecast.p99Days")
		result := compareForecast(t, nil, nil, opts)
		if len(result.StochasticLeafRefusals) == 0 || result.StochasticLeafCitation != "" {
			t.Fatalf("an unreached leaf must refuse, got %+v", result)
		}
	})
}

// No other operation declares the class.
func TestOnlyCapacityForecastDeclaresAStochasticLeafClass(t *testing.T) {
	for _, name := range KnownOperations() {
		spec, err := SpecFor(name)
		if err != nil {
			t.Fatalf("SpecFor(%s): %v", name, err)
		}
		parities := []Options{spec.Parity}
		for _, variant := range spec.Variants {
			parities = append(parities, variant.Parity)
		}
		for _, parity := range parities {
			if parity.StochasticLeaves != nil && name != "capacityForecast" {
				t.Errorf("%s declares a stochastic leaf class", name)
			}
		}
	}
	got := capacityForecastParity(t).StochasticLeaves.Leaves()
	want := []string{
		"data.capacityForecast.p50Date", "data.capacityForecast.p50Days", "data.capacityForecast.p50Items",
		"data.capacityForecast.p85Date", "data.capacityForecast.p85Days", "data.capacityForecast.p85Items",
		"data.capacityForecast.p95Date", "data.capacityForecast.p95Days", "data.capacityForecast.p95Items",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("covered leaves %v, want %v", got, want)
	}
}

// capacityForecastRunner measures capacityForecast through the real runner
// and the registered document.
func capacityForecastRunner(t *testing.T, edge *fakeEdge) *Runner {
	t.Helper()
	runner := newRunner(t, edge, "canary")
	edge.goBuild = runner.Registry.BuildIdentity
	runner.Documents = map[string]string{"capacityForecast": capacityForecastDocument(t)}
	runner.Registry.DocumentDigest = map[string]string{"capacityForecast": "b4fb8f07"}
	runner.Routing = map[string]RoutingRow{"capacityForecast": {Mode: "canary", CandidateBuild: runner.Registry.BuildIdentity}}
	return runner
}

// The report and the receipt say the operation was proven under the class,
// and the receipt is in the shape `enable` admits: a mismatch with a
// non-blank citation and nothing outside it.
func TestTheRunReportsAndRecordsAClassProvenOperation(t *testing.T) {
	parity := capacityForecastParity(t)
	edge := &fakeEdge{
		pythonBody: forecastBody(t, "python", nil),
		goBody:     forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}),
	}
	runner := capacityForecastRunner(t, edge)
	outcomes, summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	outcome := outcomes[0]
	if !outcome.Executed || outcome.ProvenUnder != ProvenUnderStochasticLeafClass {
		t.Fatalf("expected a class-proven execution, got executed=%v proven_under=%q refusal=%s %s findings=%v",
			outcome.Executed, outcome.ProvenUnder, outcome.RefusalReason, outcome.RefusalDetail, outcome.Findings)
	}
	if outcome.TerminalState != TerminalStateMismatch || outcome.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("state=%s outside=%d", outcome.TerminalState, outcome.DifferencesOutsideBaselineDefect)
	}
	if strings.Join(outcome.BaselineDefects, ",") != parity.StochasticLeaves.Citation() {
		t.Fatalf("citations %v", outcome.BaselineDefects)
	}
	if summary.ProvenUnderStochasticLeafClass != 1 {
		t.Fatalf("summary counter %d", summary.ProvenUnderStochasticLeafClass)
	}
	report, err := json.Marshal(outcome)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(report), `"proven_under":"stochastic_leaf_class"`) {
		t.Fatalf("JSON report does not name the class: %s", report)
	}

	receipts, err := runner.ReceiptsFor(time.Unix(1757000000, 0).UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	receipt := receipts[0]
	if receipt.TerminalState != EnablementCitedMismatchState || receipt.DifferencesOutsideBaselineDefect != 0 ||
		len(receipt.BaselineDefects) != 1 || NamesNothing(receipt.BaselineDefects[0]) ||
		!strings.HasPrefix(receipt.BaselineDefects[0], StochasticLeafCitationPrefix) ||
		receipt.BuildBinding != EdgeBuildPresent {
		t.Fatalf("receipt is not in the cited-mismatch shape: %+v", receipt)
	}
}

// A class-covered pair beside any other difference is not class-proven.
func TestTheRunDoesNotReportClassProofBesideAnotherDifference(t *testing.T) {
	for _, c := range []struct {
		name string
		edge *fakeEdge
	}{
		{"a non-stochastic body difference", &fakeEdge{
			pythonBody: forecastBody(t, "python", nil),
			goBody:     forecastBody(t, "go", map[string]any{"backlogSize": 13, "p85Days": 4, "p85Date": forecastDay(4)}),
		}},
		{"an HTTP header difference", &fakeEdge{
			pythonBody: forecastBody(t, "python", nil),
			goBody:     forecastBody(t, "go", map[string]any{"p85Days": 4, "p85Date": forecastDay(4)}),
			goHeaders:  map[string]string{"Content-Type": "application/graphql-response+json"},
			pyHeaders:  map[string]string{"Content-Type": "application/json"},
		}},
	} {
		t.Run(c.name, func(t *testing.T) {
			runner := capacityForecastRunner(t, c.edge)
			outcomes, summary, err := runner.Run(context.Background())
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if outcomes[0].ProvenUnder != "" || summary.ProvenUnderStochasticLeafClass != 0 || outcomes[0].DifferencesOutsideBaselineDefect == 0 {
				t.Fatalf("proven_under=%q outside=%d", outcomes[0].ProvenUnder, outcomes[0].DifferencesOutsideBaselineDefect)
			}
		})
	}
}

// A declaration naming a field the registered document does not select is
// refused before any request is sent.
func TestTheRunRefusesAClassTheDocumentDoesNotSelect(t *testing.T) {
	edge := &fakeEdge{pythonBody: forecastBody(t, "python", nil), goBody: forecastBody(t, "go", nil)}
	runner := capacityForecastRunner(t, edge)
	runner.Documents["capacityForecast"] = strings.Replace(capacityForecastDocument(t), "    p95Items\n", "", 1)
	outcomes, _, _ := runner.Run(context.Background())
	if outcomes[0].RefusalReason != RefusalInvalidStochasticLeafClass {
		t.Fatalf("got %s (%s)", outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
	}
	if len(edge.seen) != 0 {
		t.Fatalf("a request was sent under an invalid declaration: %d", len(edge.seen))
	}
}

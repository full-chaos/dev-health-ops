package goapiproof

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// The consequence on a percent of a baseline defect that changes the value the
// percent is made of is declared in two shapes, each judged on the candidate's
// own row. Everything else at the leaf is outside: this is what the citation
// could not do while it named the percent's path with no shape.
func TestValueDrivenPercentDefects_AdmitOnlyTheirTwoShapes(t *testing.T) {
	base := BaselineDefect{Ticket: "CHAOS-0000", Reason: "the reference reads other rows than the candidate.", Intermittent: true, IntermittentReason: "test fixture"}
	type row struct{ pct, flags string }
	cases := []struct {
		name                string
		baseline, candidate row
		wantOutside         int
	}{
		{"two numbers beside two windows that hold a value", row{"-86.1", flagsBoth}, row{"12.5", flagsBoth}, 0},
		{"a number against null beside no comparison window", row{"-86.1", flagsNoPrior}, row{"null", flagsNoPrior}, 0},
		{"a number against null beside no current window", row{"-86.1", flagsNoData}, row{"null", flagsNoData}, 0},
		{"a number against null beside two windows that hold a value: a real percent served as null", row{"-86.1", flagsBoth}, row{"null", flagsBoth}, 1},
		{"two numbers beside a window that holds no value: a percent served for nothing", row{"-86.1", flagsNoPrior}, row{"12.5", flagsNoPrior}, 1},
		{"two numbers and no flag on the row", row{"-86.1", ""}, row{"12.5", ""}, 1},
		{"a number against null and no flag on the row", row{"-86.1", ""}, row{"null", ""}, 1},
		{"null on the reference's side against a number", row{"null", flagsBoth}, row{"12.5", flagsBoth}, 1},
		{"a number against a string", row{"-86.1", flagsBoth}, row{`"12.5"`, flagsBoth}, 1},
	}
	object := func(r row) string {
		flags := ""
		if r.flags != "" {
			flags = "," + r.flags
		}
		return fmt.Sprintf(`{"data":{"delta_pct":%s,"value":3%s}}`, r.pct, flags)
	}
	list := func(r row) string {
		flags := ""
		if r.flags != "" {
			flags = "," + r.flags
		}
		return fmt.Sprintf(`{"data":{"drivers":[{"id":"r1","delta_pct":%s,"value":3%s},{"id":"r2","delta_pct":4.0,"value":9,%s}]}}`, r.pct, flags, flagsBoth)
	}
	ordered := func(r row) string {
		flags := ""
		if r.flags != "" {
			flags = "," + r.flags
		}
		return fmt.Sprintf(`{"data":{"deltas":[{"metric":"a","delta_pct":4.0,"value":9,%s},{"metric":"b","delta_pct":%s,"value":3%s}]}}`, flagsBoth, r.pct, flags)
	}
	for _, where := range []struct {
		name string
		opts Options
		body func(row) string
	}{
		{"the one object", Options{BaselineDefects: valueDrivenPercentDefects(base, "data.delta_pct", func() *SiblingCondition {
			return &SiblingCondition{ObjectPath: "data"}
		})}, object},
		{"a list keyed by id", Options{
			BaselineDefects: valueDrivenPercentDefects(base, "data.drivers.delta_pct", func() *SiblingCondition {
				return &SiblingCondition{ListPath: "data.drivers", KeyFields: []string{"id"}}
			}),
			OrderInsensitiveLists: []OrderInsensitiveList{{Path: "data.drivers", KeyFields: []string{"id"}}},
		}, list},
		{"an ordered list", Options{BaselineDefects: valueDrivenPercentDefects(base, "data.deltas.delta_pct", func() *SiblingCondition {
			return &SiblingCondition{ListPath: "data.deltas"}
		})}, ordered},
	} {
		for _, c := range cases {
			t.Run(where.name+"/"+c.name, func(t *testing.T) {
				// The flags are the candidate's; the reference never had them. Both
				// bodies carry the candidate's flags so that the percent is the only
				// difference.
				baseline := c.baseline
				baseline.flags = c.candidate.flags
				result := Compare(snapshotFromJSON(t, where.body(baseline)), snapshotFromJSON(t, where.body(c.candidate)), where.opts)
				if result.DifferencesOutsideBaselineDefect != c.wantOutside {
					t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
				}
			})
		}
	}
}

// shapeOf names the shape a declared difference carries, "" for a blanket one
// (any leaf difference under its paths is covered).
func shapeOf(defect BaselineDefect) string {
	value := reflect.ValueOf(defect)
	for i := 0; i < value.NumField(); i++ {
		field := value.Type().Field(i)
		if strings.HasSuffix(field.Name, "Shape") && value.Field(i).Kind() == reflect.Pointer && !value.Field(i).IsNil() {
			return field.Name
		}
	}
	return ""
}

// TestNoDeclaredDifferenceCoversAPercentWithoutAShapeCensus walks every
// declared difference of the REST corpus and of the GraphQL operations. A
// percent of a delta (a path that ends in delta_pct) is a leaf whose exact
// differences are declared pair by pair (metricPercentDefects) or shape by
// shape (valueDrivenPercentDefects). A declaration that names such a path, or
// an object above it, with NO shape covers any difference there and makes
// those declarations empty words: three citations did so.
func TestNoDeclaredDifferenceCoversAPercentWithoutAShapeCensus(t *testing.T) {
	type declared struct {
		where  string
		defect BaselineDefect
	}
	var all []declared
	for name, spec := range restEndpointSpecs {
		for _, request := range spec.Requests {
			for _, defect := range request.Parity.BaselineDefects {
				all = append(all, declared{"REST " + name + " / " + request.Name, defect})
			}
		}
	}
	for name, spec := range operationSpecs {
		for _, defect := range spec.Parity.BaselineDefects {
			all = append(all, declared{"GraphQL " + name, defect})
		}
		for _, variant := range spec.Variants {
			for _, defect := range variant.Parity.BaselineDefects {
				all = append(all, declared{"GraphQL " + name + " / " + variant.Name, defect})
			}
		}
	}
	if len(all) < 100 {
		t.Fatalf("the census read %d declared differences: it measured nothing", len(all))
	}
	// Every path of the corpus that ends in a percent of a delta.
	percentPaths := map[string]bool{}
	for _, entry := range all {
		for _, path := range entry.defect.Paths {
			if strings.HasSuffix(path, "delta_pct") {
				percentPaths[path] = true
			}
		}
	}
	if len(percentPaths) < 3 {
		t.Fatalf("the corpus names %d percent paths: the census has nothing to judge", len(percentPaths))
	}
	shaped, blanket := 0, []string{}
	for _, entry := range all {
		shape := shapeOf(entry.defect)
		for _, path := range entry.defect.Paths {
			covers := false
			for percent := range percentPaths {
				// A path reaches everything beneath it.
				if percent == path || strings.HasPrefix(percent, path+".") {
					covers = true
				}
			}
			if !covers {
				continue
			}
			if shape == "" {
				blanket = append(blanket, fmt.Sprintf("%s: %s names %q with no shape", entry.where, entry.defect.Ticket, path))
				continue
			}
			shaped++
		}
	}
	sort.Strings(blanket)
	if len(blanket) > 0 {
		t.Errorf("%d declared difference(s) cover a percent of a delta with no shape (any difference there is then covered):\n%s", len(blanket), strings.Join(blanket, "\n"))
	}
	if shaped == 0 {
		t.Fatal("no shaped declaration reaches a percent path: the census found nothing to hold")
	}
}

// explainWithMetricFlags is explainParity with the explain answer's own
// has_data / has_prior_data declared as keys the recorded reference never had
// (explainParity declares the flags of a driver and of a contributor only), so
// that a test body which carries them asks only about the percent.
func explainWithMetricFlags(base Options) Options {
	out := base
	out.GoOnlyKeys = map[string]GoOnlyKey{}
	for key, declared := range base.GoOnlyKeys {
		out.GoOnlyKeys[key] = declared
	}
	for _, key := range []string{"data.has_data", "data.has_prior_data"} {
		out.GoOnlyKeys[key] = GoOnlyKey{Ticket: "CHAOS-8491", Reason: "Go-only: the window holds a stored value for the explained metric; the Python reference never served it."}
	}
	return out
}

// The same two shapes through the REAL options of the explain route, where the
// percent is a float leaf with a tolerance (so the comparator's float branch
// makes the finding): a value-driven difference of the explained metric's
// percent and of a driver's is covered beside two windows that hold a value,
// and beside a missing window only as a number against null. The reference's
// body has no flags, as a recorded answer has none.
func TestExplainParity_APercentDifferenceIsCoveredOnlyInItsTwoShapes(t *testing.T) {
	body := func(pct, flags, driverPct, driverFlags string) string {
		with := func(f string) string {
			if f == "" {
				return ""
			}
			return "," + f
		}
		return fmt.Sprintf(`{"metric":"cycle_time","label":"Cycle Time","unit":"hours","value":41.5,"delta_pct":%s%s,`+
			`"drivers":[{"id":"r1","label":"r1","value":7.25,"delta_pct":%s,"evidence_link":"","display_name":null%s}],"contributors":[],"drilldown_links":{}}`,
			pct, with(flags), driverPct, with(driverFlags))
	}
	for _, c := range []struct {
		name                string
		baseline, candidate string
		wantOutside         int
	}{
		{"the metric: two numbers beside two full windows", body("-86.16", "", "4.0", ""), body("12.5", flagsBoth, "4.0", flagsBoth), 0},
		{"the metric: a number against null beside no comparison window", body("-86.16", "", "4.0", ""), body("null", flagsNoPrior, "4.0", flagsBoth), 0},
		{"the metric: a number against null beside two full windows", body("-86.16", "", "4.0", ""), body("null", flagsBoth, "4.0", flagsBoth), 1},
		{"the metric: two numbers beside no comparison window", body("-86.16", "", "4.0", ""), body("12.5", flagsNoPrior, "4.0", flagsBoth), 1},
		{"a driver: two numbers beside two full windows", body("5.0", "", "-30.0", ""), body("5.0", flagsBoth, "9.75", flagsBoth), 0},
		{"a driver: a number against null beside no comparison row", body("5.0", "", "-30.0", ""), body("5.0", flagsBoth, "null", flagsNoPrior), 0},
		{"a driver: two numbers beside no comparison row", body("5.0", "", "-30.0", ""), body("5.0", flagsBoth, "9.75", flagsNoPrior), 1},
		{"a driver: a number against null beside two full windows", body("5.0", "", "-30.0", ""), body("5.0", flagsBoth, "null", flagsBoth), 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			result := Compare(explainRESTSnapshot(t, c.baseline), explainRESTSnapshot(t, c.candidate), explainWithMetricFlags(explainParity))
			if result.DifferencesOutsideBaselineDefect != c.wantOutside {
				t.Fatalf("outside = %d, want %d -- findings %+v", result.DifferencesOutsideBaselineDefect, c.wantOutside, result.Findings)
			}
		})
	}
}

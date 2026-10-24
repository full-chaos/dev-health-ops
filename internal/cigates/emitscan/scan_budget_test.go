package emitscan_test

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/emitscan"
)

func repeat(n int, format string, args ...func(int) any) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		values := make([]any, len(args))
		for j, arg := range args {
			values[j] = arg(i)
		}
		fmt.Fprintf(&b, format, values...)
	}
	return b.String()
}

func idx(i int) any { return i }

type budgetSite struct {
	name   string
	source func() string
	budget int // trips only while every charge of the named sites is made
}

func tripped(findings []emitscan.Finding) bool {
	for _, finding := range findings {
		if finding.Rule == "R1" && finding.Detail == "census incomplete: work budget" {
			return true
		}
	}
	return false
}

var budgetSites = []budgetSite{
	{"const declaration pass", func() string { return repeat(200, "const ()\n") }, 618},
	{"raw-function fixpoint", func() string { return repeat(200, "func f%d() {}\n", idx) }, 2818},
	{"dependant constants and fold nodes", func() string { return repeat(200, "const c%d = a%d\n", idx, idx) }, 3218},
	{"key bytes", func() string { return repeat(50, "const c%d = \""+strings.Repeat("W", 200)+"\"\n", idx) }, 16043},
	{"taint value spec", func() string { return "func f() {\n" + repeat(200, "\tvar x = y\n") + "}\n" }, 5932},
	{"taint worklist", func() string {
		return "func f(r *http.Request) {\n\tx0 := r.URL.Path\n" + repeat(200, "\tx%d := x%d\n", func(i int) any { return i + 1 }, idx) + "}\n"
	}, 5083},
	{"carried identifiers", func() string { return "func f() {\n\tx = a" + strings.Repeat(" + a", 200) + "\n}\n" }, 2953},
	{"returned taint", func() string { return "func f() string {\n\treturn a" + strings.Repeat(" + a", 200) + "\n}\n" }, 2348},
	{"composite alternatives", func() string {
		return "var a = slog.String\nvar x = []any{a" + strings.Repeat(", a", 200) + "}\n"
	}, 1179},
	{"call through a name", func() string {
		return "var a = slog.String\nfunc f(v string) {\n" + repeat(200, "\t_ = a(\"x\", v)\n") + "}\n"
	}, 8616},
	{"call through a field", func() string {
		return "type h struct{ mk func(string, string) slog.Attr }\nvar x = h{mk: slog.String}\nfunc f(v string) {\n" + repeat(200, "\t_ = x.mk(\"x\", v)\n") + "}\n"
	}, 12929},
	{"call through an index", func() string {
		return "var m = []func(string, string) slog.Attr{slog.String}\nfunc f(v string) {\n" + repeat(200, "\t_ = m[0](\"x\", v)\n") + "}\n"
	}, 11645},
	{"alias entries kept", func() string { return repeat(50, "var a = slog.F%d\n", idx) + repeat(100, "var d%d = a\n", idx) }, 327418},
	{"error text walk", func() string {
		return "func f(v string) { _ = attribute.String(\"k\", v" + strings.Repeat(" + v", 300) + ") }\n"
	}, 5898},
	{"computed key walks", func() string {
		return "func f(v string) { _ = attribute.String(k" + strings.Repeat(" + k", 300) + ", v) }\n"
	}, 5298},
	{"flow carried names", func() string {
		return "func f() {\n" + repeat(100, "\tx = append(a, b, c, d)\n") + "}\n"
	}, 5182},
	{"flow wrapper chain", func() string {
		return flowSink + repeat(100, "func w%d(a []attribute.KeyValue) { w%d(a) }\n", func(i int) any { return i + 1 }, idx) +
			"func w0(a []attribute.KeyValue) { record(nil, a) }\n"
	}, 15172},
	{"flow origin chain", func() string {
		return flowSink + "func f(c metric.Int64Counter, v string) {\n\tx0 := []attribute.KeyValue{attribute.String(\"k\", v)}\n" +
			repeat(100, "\tx%d := x%d\n", func(i int) any { return i + 1 }, idx) + "\trecord(c, x100)\n}\n"
	}, 7224},
	{"flow many origins", func() string {
		return flowSink + "func f(c metric.Int64Counter, v string) {\n\tx := []attribute.KeyValue{}\n" +
			repeat(100, "\tx = append(x, attribute.String(\"k%d\", v))\n", idx) + "\trecord(c, x)\n}\n"
	}, 27068},
	{"syntax node walk", func() string { return "func f() {\n" + repeat(1000, "\tx\n") + "}\n" }, 7132},
}

// Each budget below was measured on its input: re-measure the rows after any change to a charge. Each row is a small input whose cost sits mostly in one charge site; the budget is set so the package trips only while
// that site charges. A mutant that drops the charge makes the row end without the census finding.
func TestEveryChargeSiteIsReachedByARow(t *testing.T) {
	for _, site := range budgetSites {
		restore := emitscan.SetWorkBudget(site.budget, 0)
		got := tripped(scanSource(t, header+site.source()))
		restore()
		if !got {
			t.Errorf("%s: budget %d did not trip", site.name, site.budget)
		}
		if tripped(scanSource(t, header+site.source())) {
			t.Errorf("%s: the default budget tripped on a small input", site.name)
		}
	}
}

// The three plants that once cost gigabytes or seconds: each must end fast with the census finding or a clean scan.
func TestCostPlantsEndFast(t *testing.T) {
	plants := map[string]string{
		"4000 alternatives in a composite": repeat(4000, "var a = slog.F%d\n", idx) + "var x = []any{a" + strings.Repeat(", a", 3999) + "}\n",
		"nested closures depth 900":        "func f() {\n" + strings.Repeat("_ = func() {\n", 900) + strings.Repeat("}\n", 900) + "}\n",
		"20000 calls through one alias":    "var a = slog.String\n" + repeat(300, "var a = slog.F%d\n", idx) + "func f(v string) {\n" + repeat(20000, "\t_ = a(\"x\", v)\n") + "}\n",
	}
	for id, body := range plants {
		timed(t, id, time.Second, func() string { scanSource(t, header+body); return "" })
	}
}

// Alias entries are kept in memory: a name copied many times from an alias with many constructors must trip the budget
// early and small, not after gigabytes.
func TestAliasEntryMemoryIsBounded(t *testing.T) {
	body := "func f() {\n" + repeat(2000, "\ta = slog.F%d\n", idx) + repeat(5000, "\td%d = a\n", idx) + "}\n"
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	var got []emitscan.Finding
	timed(t, "alias entries 85 KB", time.Second, func() string { got = scanSource(t, header+body); return "" })
	runtime.ReadMemStats(&after)
	if !tripped(got) {
		t.Errorf("findings = %v, want the census finding", got)
	}
	if grown := (after.TotalAlloc - before.TotalAlloc) >> 20; grown > 200 {
		t.Errorf("the scan allocated %d MiB, want < 200", grown)
	}
	t.Logf("%d byte plant allocated %d MiB", len(body), (after.TotalAlloc-before.TotalAlloc)>>20)
}

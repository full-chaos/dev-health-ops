package emitscan_test

import (
	"fmt"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/emitscan"
)

// probeCase is one planted source; want is the rule it must produce ("" = no assertion except where strict).
type probeCase struct {
	id, want, body string
	files          map[string]string
}

// TestPlantedEvasionRows runs every planted evasion of the emit-path scan: each row with a rule must produce it. Rows
// without a rule are noise-tolerant (scope is not tracked), except the one marked strict.
func TestPlantedEvasionRows(t *testing.T) {
	cases := []probeCase{
		// const inheritance
		{id: "C1 multi-name inherit", want: "R1", body: "const (\n\ta, b = \"x\", \"api_token\"\n\tc, d\n)\nfunc f(v string) { _ = attribute.String(d, v) }\n"},
		{id: "C2 typed group inherit", want: "R1", body: "type K string\nconst (\n\ta K = \"api_token\"\n\tb\n)\nfunc f(v string) { _ = attribute.String(string(b), v) }\n"},
		{id: "C3 skipped _ then inherit", want: "R1", body: "const (\n\t_ = \"api_token\"\n\tb\n)\nfunc f(v string) { _ = attribute.String(b, v) }\n"},
		{id: "C4 cross-group concat inherit", want: "R1", body: "const a = \"api_\"\nconst (\n\tb = a + \"token\"\n\tc\n)\nfunc f(v string) { _ = attribute.String(c, v) }\n"},
		{id: "C5 iota ints then string inherit", want: "R1", body: "const (\n\tx = iota\n\ty\n\tk = \"api_token\"\n\tk2\n)\nfunc f(v string) { _ = attribute.String(k2, v) }\n"},
		{id: "C6 local const group inherit", want: "R1", body: "func f(v string) {\n\tconst (\n\t\ta = \"api_token\"\n\t\tb\n\t)\n\t_ = attribute.String(b, v)\n}\n"},
		{id: "C7 iota shifted 1<<iota", want: "", body: "const (\n\ta = 1 << iota\n\tb\n)\nfunc f(v string) { _ = attribute.Int(\"count\", b) }\n"},
		{id: "C8 typed conversion const K(\"api_token\")", want: "R1", body: "type K string\nconst k = K(\"api_token\")\nfunc f(v string) { _ = attribute.String(string(k), v) }\n"},
		{id: "C9 semconv const attribute.Key(\"api_token\").String", want: "R1", body: "const k = attribute.Key(\"api_token\")\nfunc f(v string) { _ = k.String(v) }\n"},
		{id: "C10 inherited typed conversion", want: "R1", body: "type K string\nconst (\n\ta = K(\"api_token\")\n\tb\n)\nfunc f(v string) { _ = attribute.String(string(b), v) }\n"},
		{id: "C11 cross-file inherit", want: "R1", files: map[string]string{
			"a.go": "package sample\n\nconst (\n\tx = \"api_token\"\n\ty\n)\n",
			"b.go": "package sample\n\nimport \"go.opentelemetry.io/otel/attribute\"\n\nfunc f(v string) { _ = attribute.String(y, v) }\n"}},
		// scoping
		{id: "S1 if init alias", want: "R1", body: "func f(v string) {\n\tif str := attribute.String; v != \"\" {\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n"},
		{id: "S2 for init alias", want: "R1", body: "func f(v string) {\n\tfor str := attribute.String; ; {\n\t\t_ = str(\"api_token\", v)\n\t\tbreak\n\t}\n}\n"},
		{id: "S3 switch init alias", want: "R1", body: "func f(v string) {\n\tswitch str := attribute.String; v {\n\tcase \"\":\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n"},
		{id: "S4 case clause shadow then outer call", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tswitch {\n\tcase v == \"\":\n\t\tstr := slog.String\n\t\t_ = str\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S5 type switch var shadow then outer call", want: "R1", body: "func f(v string, x any) {\n\tstr := attribute.String\n\tswitch str := x.(type) {\n\tdefault:\n\t\t_ = str\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S6 select comm shadow then outer call", want: "R1", body: "func f(v string, ch chan int) {\n\tstr := attribute.String\n\tselect {\n\tcase str := <-ch:\n\t\t_ = str\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S7 deferred closure reassign", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tdefer func() { str = slog.String }()\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S8 conditional reassign", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tif v == \"\" {\n\t\tstr = slog.String\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S9 range var shadow then outer call", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tfor _, str := range []string{\"a\"} {\n\t\t_ = str\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S9b range var non-ctor inside (noise ok)", want: "", body: "func f(v string, fns []func(string, string) string) {\n\tstr := attribute.String\n\t_ = str\n\tfor _, str := range fns {\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n"},
		{id: "S10 range over ctor slice", want: "R1", body: "func f(v string) {\n\tfor _, str := range []func(string, string) attribute.KeyValue{attribute.String} {\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n"},
		{id: "S11 receiver shadows pkg alias, other func", want: "R1", body: "var str = attribute.String\ntype T struct{}\nfunc (str T) m() {}\nfunc f(v string) { _ = str(\"api_token\", v) }\n"},
		{id: "S12 pkg alias before use", want: "R1", body: "var str = attribute.String\nfunc f(v string) { _ = str(\"api_token\", v) }\n"},
		{id: "S13 pkg alias AFTER use same file", want: "R1", body: "func f(v string) { _ = str(\"api_token\", v) }\nvar str = attribute.String\n"},
		{id: "S14 pkg alias other file", want: "R1", files: map[string]string{
			"a.go": "package sample\n\nimport \"go.opentelemetry.io/otel/attribute\"\n\nvar str = attribute.String\n",
			"b.go": "package sample\n\nfunc f(v string) { _ = str(\"api_token\", v) }\n"}},
		{id: "S15 typed local var alias", want: "R1", body: "func f(v string) {\n\tvar g func(string, string) attribute.KeyValue = attribute.String\n\t_ = g(\"api_token\", v)\n}\n"},
		{id: "S15b declared then assigned", want: "R1", body: "func f(v string) {\n\tvar g func(string, string) attribute.KeyValue\n\tg = attribute.String\n\t_ = g(\"api_token\", v)\n}\n"},
		{id: "S16 struct field ctor", want: "R1", body: "type h struct{ mk func(string, string) attribute.KeyValue }\nfunc f(v string) {\n\tx := h{mk: attribute.String}\n\t_ = x.mk(\"api_token\", v)\n}\n"},
		{id: "S17 map of ctors", want: "R1", body: "func f(v string) {\n\tm := map[string]func(string, string) attribute.KeyValue{\"s\": attribute.String}\n\t_ = m[\"s\"](\"api_token\", v)\n}\n"},
		{id: "S18 goto/label", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\nL:\n\t_ = str(\"api_token\", v)\n\tif v == \"\" {\n\t\tgoto L\n\t}\n}\n"},
		{id: "S19 inner copy str := str", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tfunc() {\n\t\tstr := str\n\t\t_ = str(\"api_token\", v)\n\t}()\n}\n"},
		{id: "S19b block copy str := str", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\t{\n\t\tstr := str\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n"},
		{id: "S20 same-stmt RHS uses outer", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\t{\n\t\tstr, kv := slog.String, str(\"api_token\", v)\n\t\t_, _ = str, kv\n\t}\n}\n"},
		{id: "S21 pkg alias reassigned in earlier func", want: "R1", body: "var str = attribute.String\nfunc a() { str = slog.String }\nfunc f(v string) { _ = str(\"api_token\", v) }\n"},
		{id: "S21b local func param reassigned (other func)", want: "R1", body: "var str = attribute.String\nfunc a(str func(string, string) slog.Attr) { str = slog.String; _ = str }\nfunc f(v string) { _ = str(\"api_token\", v) }\n"},
		{id: "S22 named result shadows pkg alias (noise ok)", want: "", body: "var str = attribute.String\nfunc f(v string) (str func(string, string) string) {\n\t_ = str(\"api_token\", v)\n\treturn\n}\n"},
		{id: "S23 paren alias", want: "R1", body: "func f(v string) {\n\tstr := (attribute.String)\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S24 alias of alias", want: "R1", body: "func f(v string) {\n\ta := attribute.String\n\tb := a\n\t_ = b(\"api_token\", v)\n}\n"},
		{id: "S25 alias through renamed import", files: map[string]string{
			"a.go": "package sample\n\nimport attr \"go.opentelemetry.io/otel/attribute\"\n\nfunc f(v string) {\n\tstr := attr.String\n\t_ = str(\"api_token\", v)\n}\n"}, want: "R1"},
		{id: "S26 closure captures outer, inner block rebinds after", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tg := func() {\n\t\t_ = str(\"api_token\", v)\n\t\tstr := slog.String\n\t\t_ = str\n\t}\n\tg()\n}\n"},
		{id: "S27 multi-assign rebind", want: "R1", body: "func f(v string) {\n\tvar str func(string, string) attribute.KeyValue\n\tvar n int\n\tstr, n = attribute.String, 1\n\t_ = n\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S28 short-var redeclare keeps alias", want: "R1", body: "func f(v string) (error) {\n\tstr, err := attribute.String, error(nil)\n\t_ = err\n\tstr, err2 := str, error(nil)\n\t_ = err2\n\t_ = str(\"api_token\", v)\n\treturn nil\n}\n"},
		{id: "S29 if-else inner rebind then outer", want: "R1", body: "func f(v string) {\n\tstr := attribute.String\n\tif v == \"\" {\n\t} else {\n\t\tstr := slog.String\n\t\t_ = str\n\t}\n\t_ = str(\"api_token\", v)\n}\n"},
		{id: "S30 alias in pkg var block after a function using it in var initializer", want: "R1", body: "var kv = str(\"api_token\", \"x\")\nvar str = attribute.String\n"},
		// With
		{id: "W1 any receiver With", want: "R6", body: "type b struct{}\nfunc (b) With(k, v string) b { return b{} }\nfunc f(r *http.Request) { var x b; _ = x.With(\"path\", r.URL.Path) }\n"},
		{id: "W2 slog.With", want: "R6", body: "func f(r *http.Request) { _ = slog.With(\"path\", r.URL.Path) }\n"},
		{id: "W3 deep selector With", want: "R6", body: "type s struct{ a struct{ l *slog.Logger } }\nfunc (x s) f(r *http.Request) { _ = x.a.l.With(\"path\", r.URL.Path) }\n"},
		{id: "W4 index receiver With", want: "R6", body: "func f(r *http.Request, ls []*slog.Logger) { _ = ls[0].With(\"path\", r.URL.Path) }\n"},
		{id: "W5 With method value", want: "R6", body: "func f(r *http.Request, l *slog.Logger) { w := l.With; _ = w(\"path\", r.URL.Path) }\n"},
		{id: "W6 With pair after non-key", want: "R6", body: "func f(r *http.Request, l *slog.Logger) { _ = l.With(slog.Int(\"n\", 1), \"path\", r.URL.Path) }\n"},
		{id: "W7 With on local var named attribute", want: "R6", body: "type b struct{}\nfunc (b) With(k, v string) b { return b{} }\nfunc f(r *http.Request) { var attribute b; _ = attribute.With(\"path\", r.URL.Path) }\n"},
		{id: "W8 With variadic spread", want: "R6", body: "func f(r *http.Request, l *slog.Logger) { args := []any{\"path\", r.URL.Path}; _ = l.With(args...) }\n"},
		{id: "W9 With non-const key", want: "R6", body: "func f(r *http.Request, l *slog.Logger) { k := \"path\"; _ = l.With(k, r.URL.Path) }\n"},
		{id: "P1 FuncDecl param shadows pkg alias (no finding)", want: "", body: "var str = attribute.String\nfunc f(str func(string, string) string, v string) { _ = str(\"api_token\", v) }\n"},
	}
	for _, c := range cases {
		var got string
		if c.files != nil {
			got = scanFiles(t, c.files)
		} else {
			got = rules(scanSource(t, header+c.body))
		}
		switch {
		case c.want != "" && !strings.Contains(got, c.want):
			t.Errorf("%s: rules = %q, want %s", c.id, got, c.want)
		case c.want == "" && got != "" && strings.HasPrefix(c.id, "C7"):
			t.Errorf("%s: rules = %q, want none", c.id, got)
		}
	}
}

// TestConstFixpointBounded plants constants whose folding feeds on itself. Each must end fast and, being saturated, be
// reported as an unfoldable const key (R1) and never skipped.
func TestConstFixpointBounded(t *testing.T) {
	rows := []struct{ id, body string }{
		{"B1 function-scope self reference", "const a = \"x\"\nfunc f(v string) {\n\tconst a = a + \"y\"\n\t_ = attribute.String(a, v)\n}\n"},
		{"B2 package const a = a + a", "const a = \"x\"\nconst a = a + a\nfunc f(v string) { _ = attribute.String(a, v) }\n"},
		{"B3 two names reference each other", "const a = \"s\"\nconst b = a + \"x\"\nconst a = b + \"y\"\nfunc f(v string) { _ = attribute.String(b, v) }\n"},
	}
	for _, row := range rows {
		start := time.Now()
		got := rules(scanSource(t, header+row.body))
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Errorf("%s: took %s, want < 100ms", row.id, elapsed)
		}
		if !strings.Contains(got, "R1") {
			t.Errorf("%s: rules = %q, want R1", row.id, got)
		}
	}
}

// cpuTime is the CPU time this process has used: a busy host stretches wall time, not the work a scan does.
func cpuTime(t *testing.T) time.Duration {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}

func timed(t *testing.T, id string, limit time.Duration, run func() string) string {
	t.Helper()
	start := cpuTime(t)
	got := run()
	elapsed := cpuTime(t) - start
	t.Logf("%s: %s cpu", id, elapsed)
	if elapsed > limit*raceSlowdown {
		t.Errorf("%s: used %s cpu, want < %s", id, elapsed, limit)
	}
	return got
}

// A right-nested concatenation must cost linear work, and the key it builds must still be read.
func TestDeepRightNestedConcatIsLinearAndStillRead(t *testing.T) {
	parts := strings.Split("api_token", "")
	expr := `""`
	for i := 0; i < 30; i++ {
		expr = `"" + (` + expr + `)`
	}
	for i := len(parts) - 1; i >= 0; i-- {
		expr = fmt.Sprintf(`"%s" + (%s)`, parts[i], expr)
	}
	body := "const k = " + expr + "\nfunc f(v string) { _ = attribute.String(k, v) }\n"
	got := timed(t, "depth 40 concat", 100*time.Millisecond, func() string { return rules(scanSource(t, header+body)) })
	if !strings.Contains(got, "R1") {
		t.Errorf("rules = %q, want R1", got)
	}
}

// A reverse chain longer than the pass bound must not leave a partial value in a constant built from it.
func TestLongReverseConstChainSaturatesItsDependants(t *testing.T) {
	var b strings.Builder
	b.WriteString("const C0 = C1\n")
	for i := 1; i < 80; i++ {
		fmt.Fprintf(&b, "const C%d = C%d\n", i, i+1)
	}
	b.WriteString("const C80 = \"api_token\"\nfunc g() { const C1 = \"safe\"; _ = C1 }\nfunc f(v string) { _ = attribute.String(C0, v) }\n")
	got := timed(t, "reverse chain", 100*time.Millisecond, func() string { return rules(scanSource(t, header+b.String())) })
	if !strings.Contains(got, "R1") {
		t.Errorf("rules = %q, want R1", got)
	}
}

func TestLongAliasChainIsFast(t *testing.T) {
	var b strings.Builder
	b.WriteString("func f(v string) {\n\ta0 := attribute.String\n")
	for i := 1; i < 2000; i++ {
		fmt.Fprintf(&b, "\ta%d := a%d\n", i, i-1)
	}
	b.WriteString("\t_ = a1999(\"api_token\", v)\n}\n")
	got := timed(t, "alias chain n=2000", time.Second, func() string { return rules(scanSource(t, header+b.String())) })
	if !strings.Contains(got, "R1") {
		t.Errorf("rules = %q, want R1", got)
	}
}

func TestACallInSpanKeyPositionIsADynamicKey(t *testing.T) {
	head := strings.Replace(header, "\"net/http\"", "\"net/http\"\n\t\"fmt\"\n\t\"strings\"", 1)
	for id, body := range map[string]string{
		"ToLower of a literal":  "func f(v string) { _ = attribute.String(strings.ToLower(\"API_TOKEN\"), v) }\n",
		"Sprint of a variable":  "func f(v string, n int) { _ = attribute.String(fmt.Sprint(n), v) }\n",
		"call under conversion": "func f(v string) { _ = attribute.String(string(strings.ToLower(\"x\")), v) }\n",
	} {
		got := scanSource(t, head+body)
		if len(got) != 1 || got[0].Rule != "R2" || got[0].Detail != "dynamic key" {
			t.Errorf("%s: findings = %v, want one R2 dynamic key", id, got)
		}
	}
	for id, body := range map[string]string{
		"conversion of a variable": "func f(v, k string) { _ = attribute.String(string(k), v) }\n",
		"Itoa of a constant":       "func f(v string) { _ = attribute.String(fmt.Sprint(3), v) }\n",
	} {
		if got := scanSource(t, head+body); len(got) != 0 {
			t.Errorf("%s: findings = %v, want none", id, got)
		}
	}
}

// An expression larger than the work bound is unreadable and must be reported, never skipped.
func TestOversizedKeyExpressionIsAFinding(t *testing.T) {
	big := strings.Repeat(`"a" + `, 5000) + `"a"`
	for id, body := range map[string]string{
		"const":                         "const k = " + big + "\nfunc f(v string) { _ = attribute.String(k, v) }\n",
		"const beside a readable value": "const k = \"safe\"\nconst k = " + big + "\nfunc f(v string) { _ = attribute.String(k, v) }\n",
		"direct":                        "func f(v string) { _ = attribute.String(" + big + ", v) }\n",
	} {
		got := timed(t, id, time.Second, func() string { return rules(scanSource(t, header+body)) })
		if !strings.Contains(got, "R1") {
			t.Errorf("%s: rules = %q, want R1", id, got)
		}
	}
}

// A ring of aliases, each holding its own constructor, would hold every constructor in every name: the package must
// stop on its work budget and say so.
func TestAliasRingExhaustsTheBudgetAndFailsClosed(t *testing.T) {
	const n = 1000
	var b strings.Builder
	b.WriteString("func f(v string) {\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "\tr%d = r%d\n\tr%d = slog.F%d\n", i, (i+1)%n, i, i)
	}
	b.WriteString("\t_ = r0(\"x\", v)\n}\n")
	var got []emitscan.Finding
	timed(t, "ring n=1000", time.Second, func() string { got = scanSource(t, header+b.String()); return "" })
	if len(got) != 1 || got[0].Rule != "R1" || got[0].Detail != "census incomplete: work budget" {
		t.Errorf("findings = %v, want one R1 census incomplete: work budget", got)
	}
}

// A key longer than a key can be is unreadable and reported, once per site, fast, even at 45 sites.
func TestLongKeySitesAreFindingsAndFast(t *testing.T) {
	long := strings.Repeat("W", 2041)
	var b strings.Builder
	for i := 0; i < 45; i++ {
		fmt.Fprintf(&b, "func f%d(v string) { _ = attribute.String(\"%s\", v) }\n", i, long)
	}
	var got []emitscan.Finding
	timed(t, "45 long keys", time.Second, func() string { got = scanSource(t, header+b.String()); return "" })
	if len(got) != 45 {
		t.Fatalf("findings = %d, want 45", len(got))
	}
	for _, finding := range got {
		if finding.Rule != "R1" {
			t.Errorf("finding = %v, want R1", finding)
		}
	}
}

// A name declared twice, once readable and once not, is unreadable.
func TestPartiallyReadableConstIsAFinding(t *testing.T) {
	body := "const B = \"x\"\nfunc g(v string) {\n\tconst B = string(rune(97)) + \"pi_token\"\n\t_ = attribute.String(B, v)\n}\n"
	if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
		t.Errorf("rules = %q, want R1", got)
	}
}

const (
	ratioSamples = 5
	hangCeiling  = 5 * time.Second
)

// linearByRatio times one scan at small and at factor*small in this process, so a busy host stretches both alike:
// linear work gives a ratio near factor, quadratic work near factor squared; bound sits between, with margin both
// sides (the fixed cost of a scan pulls both ratios down). Each size takes the least of ratioSamples CPU times after a
// warm-up; a failing pair is measured once more, since a spike can only raise a ratio by chance, a quadratic always does.
func linearByRatio(t *testing.T, id string, small, factor int, bound float64, source func(int) string) {
	t.Helper()
	costRatio(t, id, small, small*factor, bound, source)
}

// costRatio is linearByRatio for two named sizes: source(n) builds the input of size n, and the cost of the big size over
// the small one must stay under bound. A source that does not parse, or that trips the census budget, ends the scan
// early and measures nothing, so it fails the test.
func costRatio(t *testing.T, id string, small, big int, bound float64, source func(int) string) {
	t.Helper()
	hung := false
	least := func(n int) time.Duration {
		src := header + source(n)
		best := time.Duration(1<<63 - 1)
		for i := 0; i < ratioSamples; i++ {
			start := cpuTime(t)
			got := scanSource(t, src)
			d := cpuTime(t) - start
			if strings.Contains(rules(got), "R0") || tripped(got) {
				t.Fatalf("%s: n=%d: the scan did not read the whole source (findings %v)", id, n, got)
			}
			if d > hangCeiling*raceSlowdown {
				t.Errorf("%s: n=%d one scan used %s cpu, want < %s", id, n, d, hangCeiling*raceSlowdown)
				hung = true
				return d
			}
			if d < best {
				best = d
			}
		}
		return best
	}
	scanSource(t, header+source(small))
	var ratio float64
	for attempt := 0; attempt < 2; attempt++ {
		lo, hi := least(small), least(big)
		if hung {
			return
		}
		ratio = float64(hi) / float64(lo)
		t.Logf("%s: n=%d %s cpu, n=%d %s cpu, ratio %.2f (bound %.2f)", id, small, lo, big, hi, ratio, bound)
		if ratio < bound {
			return
		}
	}
	t.Errorf("%s: n=%d over n=%d cost %.2fx the cpu, want < %.2fx", id, big, small, ratio, bound)
}

// Finding the function that holds a metric-options call costs O(1) per node, however deep non-function nesting goes, and
// a metric call in a closure still marks the constructors of every function around it, and only those.
func TestMetricFunctionMarkingIsLinearAndKeepsItsScope(t *testing.T) {
	const depth = 20000
	deepAt := func(n int) string {
		return "func f(c metric.Int64Counter, v string) {\n\tx := attribute.String(\"user_id\", v)\n\t_ = x\n\t_ = " +
			strings.Repeat("metric.WithAttributes(", n) + strings.Repeat(")", n) + "\n\t_ = " +
			strings.Repeat("metric.WithAttributes(", n) + strings.Repeat(")", n) + "\n}\n"
	}
	deep := deepAt(depth)
	var got []emitscan.Finding
	linearByRatio(t, "2 x N nested metric calls", depth/4, 6, 18, deepAt)
	got = scanSource(t, header+deep)
	if !hasFinding(got, "R2", "user_id") && !tripped(got) {
		t.Errorf("deep nesting: findings = %v, want R2 user_id or the census finding", got)
	}

	linearByRatio(t, "N nested calls each holding a metric closure", 6000, 8, 20, func(n int) string {
		return "func f(c metric.Int64Counter, v string) {\n\t_ = " + strings.Repeat("g(func() { _ = metric.WithAttributes() }, ", n) + "0" + strings.Repeat(")", n) + "\n}\n"
	})

	// The same 24000 closures, in chains of depth n (the parser stops near depth 1000): work that is linear in the closures costs the same at any depth, a
	// scan of the open functions at every metric call costs more as the chains deepen.
	costRatio(t, "24000 closures in chains of depth n", 25, 800, 1.6, func(n int) string {
		var b strings.Builder
		for i := 0; i < 24000/n; i++ {
			fmt.Fprintf(&b, "func f%d(c metric.Int64Counter) {\n%s%s}\n", i, strings.Repeat("func() {\n\t_ = metric.WithAttributes()\n", n), strings.Repeat("}()\n", n))
		}
		return b.String()
	})

	closure := "func f(c metric.Int64Counter, v string) {\n\tx := attribute.String(\"user_id\", v)\n\t_ = x\n\tfunc() {\n\t\t_ = metric.WithAttributes()\n\t}()\n}\n"
	if got := scanSource(t, header+closure); !hasFinding(got, "R2", "user_id") {
		t.Errorf("closure: findings = %v, want R2 user_id (the metric call in the closure marks the function around it)", got)
	}

	scoped := "func f(c metric.Int64Counter) {\n\tfunc() {\n\t\tfunc() {\n\t\t\t_ = metric.WithAttributes()\n\t\t}()\n\t}()\n}\n" +
		"func g(v string) {\n\t_ = attribute.String(\"user_id\", v)\n}\n"
	if got := scanSource(t, header+scoped); hasFinding(got, "R2", "user_id") {
		t.Errorf("scoped: findings = %v, a function without a metric call must stay unmarked", got)
	}
}

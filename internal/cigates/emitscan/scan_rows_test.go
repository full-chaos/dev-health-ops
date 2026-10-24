package emitscan_test

import (
	"strings"
	"testing"
	"time"
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

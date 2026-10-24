package emitscan_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/emitscan"
)

func scanSource(t *testing.T, source string) []emitscan.Finding {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "sample")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sample.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	findings, err := emitscan.Scan(root, []string{"internal"})
	if err != nil {
		t.Fatal(err)
	}
	return findings
}

func rules(findings []emitscan.Finding) string {
	var out []string
	for _, finding := range findings {
		out = append(out, finding.Rule)
	}
	return strings.Join(out, ",")
}

const header = "package sample\n\nimport (\n\t\"context\"\n\t\"crypto/sha256\"\n\t\"log/slog\"\n\t\"net/http\"\n\t\"github.com/riverqueue/river\"\n\t\"go.opentelemetry.io/otel/attribute\"\n\t\"go.opentelemetry.io/otel/metric\"\n)\n\nvar _ = context.Background\nvar _ = sha256.Sum256\nvar _ = slog.Info\nvar _ = http.StatusOK\nvar _ = river.NewClient\nvar _ = attribute.String\nvar _ metric.Int64Counter\n\n"

func TestRedProofs(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"planted secret-named attribute", "func f(v string) { _ = attribute.String(\"api_token\", v) }", "R1"},
		{"planted unbounded metric label", "func f(c metric.Int64Counter, org string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"org_id\", org))) }", "R2"},
		{"planted free-text metric label key", "func f(c metric.Int64Counter, v string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"detail_text\", v))) }", "R2"},
		{"metric label count over budget", "func f(c metric.Int64Counter) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.Bool(\"a\", true), attribute.Bool(\"b\", true), attribute.Bool(\"c\", true), attribute.Bool(\"d\", true), attribute.Bool(\"e\", true), attribute.Bool(\"f\", true), attribute.Bool(\"g\", true))) }", "R2"},
		{"planted error text as attribute", "func f(err error) { _ = attribute.String(\"reason\", err.Error()) }", "R3"},
		{"planted RecordError", "type span interface{ RecordError(error) }\nfunc f(s span, err error) { s.RecordError(err) }", "R3"},
		{"planted raw handler", "func f() { _ = slog.NewJSONHandler(nil, nil) }", "R4"},
		{"planted river client without a logger", "func f() { _, _ = river.NewClient(nil, &river.Config{Schema: \"s\"}) }", "R5"},
		{"planted raw request path", "func f(r *http.Request) { slog.Info(\"x\", slog.String(\"path\", r.URL.Path)) }", "R6"},
		{"planted raw request path in loose pairs", "func f(r *http.Request, l *slog.Logger) { l.Error(\"x\", \"path\", r.URL.Path) }", "R6"},
		{"planted raw query", "func f(r *http.Request, l *slog.Logger) { l.Error(\"x\", \"q\", r.URL.RawQuery) }", "R6"},
		{"aliased attribute and metric packages", "func f(c metric.Int64Counter, org string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"org_id\", org))) }", "R2"},
		{"attributes built in a slice", "func f(c metric.Int64Counter, org string) { attrs := []attribute.KeyValue{attribute.String(\"org_id\", org)}; c.Add(context.Background(), 1, metric.WithAttributes(attrs...)) }", "R2"},
		{"attributes built in a set", "func f(c metric.Int64Counter, org string) { set := attribute.NewSet(attribute.String(\"org_id\", org)); c.Add(context.Background(), 1, metric.WithAttributeSet(set)) }", "R2"},
		{"identity key with a status suffix", "func f(c metric.Int64Counter, org string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"org_id_status\", org))) }", "R2"},
		{"escaped constant key", "const k = \"org\\u005fid\"\nfunc f(c metric.Int64Counter, org string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(k, org))) }", "R2"},
		{"raw path through a local variable", "func f(r *http.Request, l *slog.Logger) { p := r.URL.Path; l.Error(\"x\", \"path\", p) }", "R6"},
		{"raw path variable in an attribute", "func f(r *http.Request) { var p = r.URL.RawQuery; slog.Info(\"x\", slog.String(\"q\", p)) }", "R6"},
		{"discard-named writer is not io.Discard", "type discard struct{}\nfunc (discard) Write(p []byte) (int, error) { return len(p), nil }\nfunc f() { _ = slog.NewJSONHandler(discard{}, nil) }", "R4"},
		{"two-hop path variable", "func f(r *http.Request, l *slog.Logger) { p := r.URL.Path; q := p; l.Error(\"x\", \"path\", q) }", "R6"},
		{"path variable used in a closure", "func f(r *http.Request, l *slog.Logger) { p := r.URL.Path; defer func() { l.Info(\"done\", \"path\", p) }() }", "R6"},
		{"path variable derived in a closure", "func f(r *http.Request, l *slog.Logger) { p := r.URL.Path; func() { q := p + \"/x\"; l.Info(\"done\", \"path\", q) }() }", "R6"},
		{"function-variable alias of a constructor", "var str = attribute.String\nfunc f(v string) { _ = str(\"api_token\", v) }", "R1"},
		{"attribute.Key form", "func f(v string) { _ = attribute.Key(\"api_token\").String(v) }", "R1"},
		{"concatenated constant key", "const k = \"api_\" + \"token\"\nfunc f(v string) { _ = attribute.String(k, v) }", "R1"},
		{"function-local constant key", "func f(v string) { const k = \"api_token\"; _ = attribute.String(k, v) }", "R1"},
		{"camel-case identity label", "func f(c metric.Int64Counter, o string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"orgId\", o))) }", "R2"},
		{"attribute.Key metric label", "func f(c metric.Int64Counter, o string) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.Key(\"org_id\").String(o))) }", "R2"},
		{"raw path through a same-package helper", "func p(r *http.Request) string { return r.URL.Path }\nfunc f(r *http.Request, l *slog.Logger) { l.Error(\"x\", \"path\", p(r)) }", "R6"},
		{"raw path through a helper of a helper", "func p(r *http.Request) string { return r.URL.Path }\nfunc q(r *http.Request) string { x := p(r); return x + \"/\" }\nfunc f(r *http.Request, l *slog.Logger) { l.Error(\"x\", \"path\", q(r)) }", "R6"},
		{"raw URL string", "func f(r *http.Request, l *slog.Logger) { l.Error(\"x\", \"u\", r.URL.String()) }", "R6"},
		{"raw path bound with With", "func f(r *http.Request, l *slog.Logger) { l.With(\"path\", r.URL.Path).Info(\"x\") }", "R6"},
		{"planted bare digest as attribute", "func f(v []byte) { _ = attribute.String(\"who\", string(sha256.Sum256(v)[:])) }", "R7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rules(scanSource(t, header+tc.body+"\n"))
			if !strings.Contains(got, tc.want) {
				t.Fatalf("rules = %q, want %s", got, tc.want)
			}
		})
	}
}

func TestDotImportOfAWatchedPackageIsAFinding(t *testing.T) {
	for _, path := range []string{"log/slog", "go.opentelemetry.io/otel/attribute", "go.opentelemetry.io/otel/metric"} {
		source := "package sample\n\nimport . \"" + path + "\"\n"
		if got := rules(scanSource(t, source)); got != "R8" {
			t.Fatalf("rules = %q for a dot-import, want R8", got)
		}
	}
	if got := rules(scanSource(t, "package sample\n\nimport . \"strings\"\n")); got != "" {
		t.Fatalf("rules = %q for an unwatched dot-import", got)
	}
}

func TestPathTaintDoesNotFollowOrdinaryCalls(t *testing.T) {
	body := "func f(r *http.Request, l *slog.Logger, do func(string) (int, error)) {\n\tp := r.URL.Path\n\tstatus, _ := do(p)\n\tl.Info(\"x\", \"status\", status)\n}\n"
	if got := scanSource(t, header+body); len(got) != 0 {
		t.Fatalf("findings: %v", got)
	}
}

func TestInheritedConstantExpressionIsResolved(t *testing.T) {
	body := "const (\n\tfirst = \"api_\" + \"token\"\n\tinherited\n)\nfunc f(v string) { _ = attribute.String(inherited, v) }\n"
	if got := rules(scanSource(t, header+body)); got != "R1" {
		t.Fatalf("rules = %q, want R1", got)
	}
}

func TestNestedConstructorAliasDoesNotHideTheOuterOne(t *testing.T) {
	body := "func f(v string) {\n\tstr := attribute.String\n\tfunc() {\n\t\tstr := slog.String\n\t\t_ = str\n\t}()\n\t_ = str(\"api_token\", v)\n}\n"
	if got := rules(scanSource(t, header+body)); got != "R1" {
		t.Fatalf("rules = %q, want R1", got)
	}
}

func TestWithOnAnyReceiverIsReadAsALogCall(t *testing.T) {
	body := "type builder struct{}\nfunc (builder) With(k string, v string) builder { return builder{} }\nfunc f(r *http.Request) { var b builder; _ = b.With(\"path\", r.URL.Path) }\n"
	if got := rules(scanSource(t, header+body)); got != "R6" {
		t.Fatalf("rules = %q, want R6", got)
	}
}

func TestWithOnTracedLoggersIsALogCall(t *testing.T) {
	cases := map[string]string{
		"default":     "func f(r *http.Request) { slog.Default().With(\"path\", r.URL.Path).Info(\"x\") }",
		"new":         "func f(r *http.Request, h slog.Handler) { slog.New(h).With(\"path\", r.URL.Path).Info(\"x\") }",
		"field":       "type srv struct{ logger *slog.Logger }\nfunc (s srv) f(r *http.Request) { s.logger.With(\"path\", r.URL.Path).Info(\"x\") }",
		"assigned":    "func f(r *http.Request, l *slog.Logger) { m := l.With(\"a\", 1); m.With(\"path\", r.URL.Path).Info(\"x\") }",
		"helper func": "func get() *slog.Logger { return slog.Default() }\nfunc f(r *http.Request) { get().With(\"path\", r.URL.Path).Info(\"x\") }",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body+"\n")); !strings.Contains(got, "R6") {
			t.Fatalf("%s: rules = %q, want R6", name, got)
		}
	}
}

func TestIotaOffsetIsCarriedThroughAConstGroup(t *testing.T) {
	body := "const (\n\tfirst = \"api_token\" + strconv.Itoa(iota)\n\tsecond\n)\nfunc f(v string) { _ = attribute.String(second, v) }\n"
	source := strings.Replace(header, "\t\"net/http\"\n", "\t\"net/http\"\n\t\"strconv\"\n", 1) + "var _ = strconv.Itoa\n" + body
	got := scanSource(t, source)
	if len(got) != 1 || got[0].Rule != "R1" || got[0].Detail != "api_token1" {
		t.Fatalf("findings = %v, want R1 for the key of the second spec (iota 1)", got)
	}
}

// A function variable keeps every constructor it was ever bound to in its scope (fail closed): a later assignment adds to
// the set and never removes from it.
func TestReassignedAliasKeepsEveryValue(t *testing.T) {
	cases := map[string]string{
		"slog then attribute":  "func f(v string) {\n\tstr := slog.String\n\tstr = attribute.String\n\t_ = str(\"api_token\", v)\n}\n",
		"attribute then other": "func f(v string) {\n\tstr := attribute.String\n\t_ = str\n\tstr = func(k, v string) string { return k + v }\n\t_ = str(\"api_token\", v)\n}\n",
		"conditional":          "func f(c bool, v string) {\n\tvar str func(k, v string) attribute.KeyValue\n\tif c {\n\t\tstr = attribute.String\n\t}\n\t_ = str(\"api_token\", v)\n}\n",
		"deferred closure":     "func f(v string) {\n\tstr := slog.String\n\tdefer func() { str = attribute.String }()\n\t_ = str(\"api_token\", v)\n}\n",
		"package alias reassigned in another function": "var str = slog.String\nfunc g() { str = attribute.String }\nfunc f(v string) { _ = str(\"api_token\", v) }\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestPackageScopeAliasIsOrderFree(t *testing.T) {
	cases := map[string]string{
		"function above the declaration":            "func f(v string) { _ = str(\"api_token\", v) }\nvar str = attribute.String\n",
		"package initializer above the declaration": "var _ = str(\"api_token\", \"x\")\nvar str = attribute.String\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestAliasCopiedFromAnAliasKeepsTheConstructor(t *testing.T) {
	cases := map[string]string{
		"alias of an alias":        "func f(v string) {\n\ta := attribute.String\n\tb := a\n\t_ = b(\"api_token\", v)\n}\n",
		"shadow copy in a closure": "func f(v string) {\n\tstr := attribute.String\n\tfunc() {\n\t\tstr := str\n\t\t_ = str(\"api_token\", v)\n\t}()\n}\n",
		"shadow copy in a block":   "func f(v string) {\n\tstr := attribute.String\n\t{\n\t\tstr, e2 := str, 1\n\t\t_ = e2\n\t\t_ = str(\"api_token\", v)\n\t}\n}\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestConvertedConstantKeysAreFolded(t *testing.T) {
	cases := map[string]string{
		"typed conversion":       "type K string\nconst k = K(\"api_token\")\nfunc f(v string) { _ = attribute.String(string(k), v) }\n",
		"conversion as key":      "type K string\nconst k = K(\"api_token\")\nfunc f(v string) { _ = attribute.Key(k).String(v) }\n",
		"attribute.Key constant": "const k = attribute.Key(\"api_token\")\nfunc f(v string) { _ = k.String(v) }\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestUnfoldableConstantKeyIsAFinding(t *testing.T) {
	body := "const k = http.MethodGet\nfunc f(v string) { _ = attribute.String(k, v) }\n"
	got := scanSource(t, header+body)
	if len(got) != 1 || got[0].Rule != "R1" || got[0].Detail != "unfoldable const key" {
		t.Fatalf("findings = %v, want one R1 unfoldable const key", got)
	}
}

func TestCrossFileConstantKeyIsResolved(t *testing.T) {
	got := scanFiles(t, map[string]string{
		"a.go": "package sample\n\nconst Key = \"api_\" + \"token\"\n",
		"b.go": "package sample\n\nimport \"go.opentelemetry.io/otel/attribute\"\n\nfunc f(v string) { _ = attribute.String(Key, v) }\n",
	})
	if got != "R1" {
		t.Fatalf("rules = %q, want R1", got)
	}
}

func TestVersionedAliasedRiverImportIsResolved(t *testing.T) {
	source := "package sample\n\nimport riverv2 \"github.com/riverqueue/river/v2\"\n\nfunc f() { _, _ = riverv2.NewClient(nil, &riverv2.Config{}) }\n"
	if got := rules(scanSource(t, source)); got != "R5" {
		t.Fatalf("rules = %q, want R5", got)
	}
}

func TestBoundedHelperIsNotARawPath(t *testing.T) {
	body := "func class(r *http.Request) string { if r.URL.Path == \"/q\" { return \"query\" }; return \"other\" }\nfunc f(r *http.Request, l *slog.Logger) { l.Info(\"x\", \"path_class\", class(r)) }\n"
	if got := scanSource(t, header+body); len(got) != 0 {
		t.Fatalf("findings: %v", got)
	}
}

func TestFindingsCarryTheirFunctionForSiteKeyedPins(t *testing.T) {
	body := "func first(r *http.Request, l *slog.Logger) { l.Info(\"x\", \"path\", r.URL.Path) }\nfunc second(r *http.Request, l *slog.Logger) { l.Info(\"x\", \"path\", r.URL.Path) }\n"
	got := scanSource(t, header+body)
	if len(got) != 2 || got[0].Func != "first" || got[1].Func != "second" {
		t.Fatalf("findings do not name their functions: %v", got)
	}
}

func TestAliasedImportsAreResolved(t *testing.T) {
	source := "package sample\n\nimport (\n\t\"context\"\n\t\"net/http\"\n\tattrx \"go.opentelemetry.io/otel/attribute\"\n\tmetricx \"go.opentelemetry.io/otel/metric\"\n\tslogx \"log/slog\"\n)\n\n" +
		"func f(c metricx.Int64Counter, r *http.Request) {\n" +
		"\tc.Add(context.Background(), 1, metricx.WithAttributes(attrx.String(\"org_id\", \"x\")))\n" +
		"\t_ = attrx.String(\"api_token\", \"x\")\n" +
		"\tslogx.Info(\"x\", slogx.String(\"path\", r.URL.Path))\n" +
		"\t_ = slogx.NewJSONHandler(nil, nil)\n}\n"
	got := rules(scanSource(t, source))
	for _, want := range []string{"R1", "R2", "R4", "R6"} {
		if !strings.Contains(got, want) {
			t.Fatalf("rules = %q, want %s", got, want)
		}
	}
}

func TestTestSupportSourceIsScanned(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "testsupport", "helper")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	source := header + "func f(r *http.Request) { slog.Info(\"x\", slog.String(\"path\", r.URL.Path)) }\n"
	if err := os.WriteFile(filepath.Join(dir, "helper.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := emitscan.Scan(root, []string{"internal"})
	if err != nil || rules(got) != "R6" {
		t.Fatalf("rules = %q err = %v, want R6", rules(got), err)
	}
}

func TestCleanSourceHasNoFinding(t *testing.T) {
	body := "func f(c metric.Int64Counter, l *slog.Logger, r *http.Request) {\n" +
		"\tc.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"outcome\", \"ok\"), attribute.String(\"error_class\", \"x\")))\n" +
		"\tl.Info(\"x\", \"route\", r.Pattern)\n" +
		"\t_, _ = river.NewClient(nil, &river.Config{Logger: slog.Default()})\n" +
		"\t_ = slog.NewTextHandler(nil, nil)\n}\n"
	got := scanSource(t, header+strings.Replace(body, "_ = slog.NewTextHandler(nil, nil)\n", "", 1))
	if len(got) != 0 {
		t.Fatalf("findings on clean source: %v", got)
	}
}

func TestFindingsNameRuleAndLocationNeverTheValue(t *testing.T) {
	const marker = "planted-value-marker"
	source := header + "func f() { _ = attribute.String(\"api_token\", \"" + marker + "\") }\n"
	got := scanSource(t, source)
	if len(got) == 0 {
		t.Fatal("no finding")
	}
	for _, finding := range got {
		if strings.Contains(finding.String(), marker) {
			t.Fatalf("a finding carries the planted value: %s", finding.Rule)
		}
	}
}

func TestUnparsableFileFailsClosed(t *testing.T) {
	got := scanSource(t, "package sample\nfunc (")
	if rules(got) != "R0" {
		t.Fatalf("rules = %q, want R0", rules(got))
	}
}

func TestEmptyScanFailsClosed(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "internal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := emitscan.Scan(root, []string{"internal"}); err == nil {
		t.Fatal("a scan that read no file did not fail")
	}
}

func scanFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "internal", "sample")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, source := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	got, err := emitscan.Scan(root, []string{"internal"})
	if err != nil {
		t.Fatal(err)
	}
	return rules(got)
}

// Scope and order are not tracked (fail closed): a name bound to a constructor anywhere in the package makes every call
// through that name count as the constructor, so a same-named non-constructor is noise, a named limit.
func TestAliasIsReadWhereverTheNameIsBound(t *testing.T) {
	cases := map[string]string{
		"inner non-constructor binding": "func f(v string) {\n\tstr := attribute.String\n\t_ = str\n\tfunc() {\n\t\tstr := func(k, v string) string { return k }\n\t\t_ = str(\"api_token\", v)\n\t}()\n}\n",
		"parameter of the same name":    "var str = attribute.String\nfunc f(str func(k, v string) string) { _ = str(\"api_token\", \"x\") }\n",
		"use before the declaration":    "func f() {\n\t_ = str(\"api_token\", \"x\")\n\tstr := attribute.String\n\t_ = str\n}\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestEveryAlternativeOfAnAliasIsChecked(t *testing.T) {
	body := "func f(c bool, r *http.Request) {\n\tstr := attribute.String\n\tif c {\n\t\tstr = slog.String\n\t}\n\t_ = str(\"path\", r.URL.Path)\n}\n"
	got := rules(scanSource(t, header+body))
	if !strings.Contains(got, "R6") {
		t.Fatalf("rules = %q, want R6 through the slog alternative", got)
	}
	both := "func f(c bool, r *http.Request) {\n\tstr := attribute.String\n\tif c {\n\t\tstr = slog.String\n\t}\n\t_ = str(\"api_token\", r.URL.Path)\n}\n"
	if got := rules(scanSource(t, header+both)); !strings.Contains(got, "R1") || !strings.Contains(got, "R6") {
		t.Fatalf("rules = %q, want R1 (attribute alternative) and R6 (slog alternative)", got)
	}
}

func TestAliasSetsAreDeduplicatedAndReportedOncePerSite(t *testing.T) {
	body := "func f(r *http.Request) {\n\tstr := slog.String\n\tstr = slog.String\n\t_ = str(\"path\", r.URL.Path)\n}\n"
	got := scanSource(t, header+body)
	if len(got) != 1 || got[0].Rule != "R6" {
		t.Fatalf("findings = %v, want one R6", got)
	}
}

func TestSelfReferencingAliasChainsAreFast(t *testing.T) {
	var b strings.Builder
	b.WriteString("func f() {\n\ta := attribute.String\n\tc := a\n")
	for i := 0; i < 40; i++ {
		b.WriteString("\ta = a\n\ta = c\n\tc = a\n")
	}
	b.WriteString("\t_ = c(\"api_token\", \"x\")\n}\n")
	start := time.Now()
	got := rules(scanSource(t, header+b.String()))
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("scan took %s on a self-referencing alias chain", elapsed)
	}
	if !strings.Contains(got, "R1") {
		t.Fatalf("rules = %q, want R1", got)
	}
}

func TestACallInKeyPositionIsADynamicMetricKey(t *testing.T) {
	body := "func labelKey(s string) string { return s }\nfunc f(c metric.Int64Counter) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(labelKey(\"status\"), \"ok\"))) }\n"
	got := scanSource(t, header+body)
	if len(got) != 1 || got[0].Rule != "R2" || got[0].Detail != "dynamic key" {
		t.Fatalf("findings = %v, want one R2 dynamic key", got)
	}
}

func TestConversionOfAnUnfoldableConstantIsAFinding(t *testing.T) {
	body := "type K string\nconst k = K(http.MethodGet)\nfunc f(v string) { _ = attribute.String(string(k), v) }\n"
	got := scanSource(t, header+body)
	if len(got) != 1 || got[0].Detail != "unfoldable const key" {
		t.Fatalf("findings = %v, want one unfoldable const key", got)
	}
}

func TestWithChecksEveryArgumentIncludingASpread(t *testing.T) {
	cases := map[string]string{
		"spread":                   "func f(r *http.Request, l *slog.Logger) { args := []any{\"path\", r.URL.Path}; _ = l.With(args...) }\n",
		"variable key":             "func f(r *http.Request, l *slog.Logger) { k := \"path\"; _ = l.With(k, r.URL.Path) }\n",
		"package With":             "func f(r *http.Request) { _ = slog.With(\"path\", r.URL.Path) }\n",
		"method value":             "func f(r *http.Request, l *slog.Logger) { w := l.With; _ = w(\"path\", r.URL.Path) }\n",
		"receiver named attribute": "type b struct{}\nfunc (b) With(k, v string) b { return b{} }\nfunc f(r *http.Request) { var attribute b; _ = attribute.With(\"path\", r.URL.Path) }\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R6") {
			t.Fatalf("%s: rules = %q, want R6", name, got)
		}
	}
}

// The alias sets are a fixpoint over the whole package: the order of the declarations does not matter.
func TestAliasCopiesAreResolvedWhateverTheDeclarationOrder(t *testing.T) {
	cases := map[string]string{
		"copy above the source": "func f(v string) {\n\tstr := str\n\t_ = str(\"api_token\", v)\n}\nvar str = attribute.String\n",
		"package copy above":    "var b = a\nvar a = attribute.String\nfunc f(v string) { _ = b(\"api_token\", v) }\n",
		"assigned then copied":  "func f(v string) {\n\tvar a func(k, v string) attribute.KeyValue\n\tb := a\n\ta = attribute.String\n\t_ = b(\"api_token\", v)\n}\n",
	}
	for name, body := range cases {
		if got := rules(scanSource(t, header+body)); !strings.Contains(got, "R1") {
			t.Fatalf("%s: rules = %q, want R1", name, got)
		}
	}
}

func TestOneFindingPerCallSitePerRule(t *testing.T) {
	body := "func f(r *http.Request) { slog.Info(\"x\", slog.String(\"path\", r.URL.Path), \"k\", \"v\") }\n"
	got := scanSource(t, header+body)
	if len(got) != 1 || got[0].Rule != "R6" {
		t.Fatalf("findings = %v, want exactly one R6", got)
	}
}

func TestEveryAlternativeSetsItsOwnRule(t *testing.T) {
	first := "func f() {\n\tmk := attribute.String\n\tmk = slog.NewJSONHandler\n\t_ = mk(nil, nil)\n}\n"
	if got := rules(scanSource(t, header+first)); !strings.Contains(got, "R4") {
		t.Fatalf("rules = %q, want R4 through the second alternative", got)
	}
	last := "func f(v string) {\n\tmk := slog.NewJSONHandler\n\tmk = attribute.String\n\t_ = mk(\"api_token\", v)\n}\n"
	if got := rules(scanSource(t, header+last)); !strings.Contains(got, "R1") || !strings.Contains(got, "R4") {
		t.Fatalf("rules = %q, want R1 and R4", got)
	}
}

func TestOnlyConversionsAndConstantFormattersFoldIntoAKey(t *testing.T) {
	body := "func f(c metric.Int64Counter) { c.Add(context.Background(), 1, metric.WithAttributes(attribute.String(strings.ToLower(\"status\"), \"ok\"))) }\n"
	got := scanSource(t, strings.Replace(header, "\"net/http\"", "\"net/http\"\n\t\"strings\"", 1)+body)
	if len(got) != 1 || got[0].Rule != "R2" || got[0].Detail != "dynamic key" {
		t.Fatalf("findings = %v, want one R2 dynamic key", got)
	}
}

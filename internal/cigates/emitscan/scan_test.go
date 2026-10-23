package emitscan_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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

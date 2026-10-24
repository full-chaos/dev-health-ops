package emitscan_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cigates/emitscan"
)

const flowSink = "func record(c metric.Int64Counter, attrs []attribute.KeyValue) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n"

func hasFinding(findings []emitscan.Finding, rule, detail string) bool {
	for _, finding := range findings {
		if finding.Rule == rule && finding.Detail == detail {
			return true
		}
	}
	return false
}

// A label key that reaches a metric sink through a package-local call is a metric label whatever function builds it.
func TestLabelOriginFlowsThroughPackageLocalCalls(t *testing.T) {
	cases := map[string]string{
		"literal slice argument": flowSink + "func f(c metric.Int64Counter) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"variadic parameter": "func record(c metric.Int64Counter, attrs ...attribute.KeyValue) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord(c, attribute.String(\"user_id\", \"S3CRET\"))\n}\n",
		"name built with append": flowSink + "func f(c metric.Int64Counter) {\n\tattrs := []attribute.KeyValue{}\n\tattrs = append(attrs, attribute.String(\"user_id\", \"S3CRET\"))\n\trecord(c, attrs)\n}\n",
		"two hops": flowSink + "func outer(c metric.Int64Counter, a []attribute.KeyValue) { record(c, a) }\n" +
			"func f(c metric.Int64Counter) {\n\touter(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method wrapper": "type rec struct{ c metric.Int64Counter }\nfunc (r rec) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec) {\n\tr.add([]attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"attribute set": "func record(c metric.Int64Counter, attrs []attribute.KeyValue) {\n\tc.Add(context.Background(), 1, metric.WithAttributeSet(attribute.NewSet(attrs...)))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"generic wrapper, one type argument": "func record[T any](c metric.Int64Counter, attrs []attribute.KeyValue, t T) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord[int](c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")}, 0)\n}\n",
		"generic wrapper, two type arguments": "func record[T, U any](c metric.Int64Counter, attrs []attribute.KeyValue, t T, u U) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord[int, string](c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")}, 0, \"\")\n}\n",
		"generic wrapper called without type arguments": "func record[T any](c metric.Int64Counter, attrs []attribute.KeyValue, t T) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")}, 0)\n}\n",
		"method of a generic type": "type rec[T any] struct{ c metric.Int64Counter }\nfunc (r rec[T]) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec[int]) {\n\tr.add([]attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression on a generic type": "type rec[T any] struct{ c metric.Int64Counter }\nfunc (r rec[T]) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec[int]) {\n\trec[int].add(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression on a type name": "type rec struct{ c metric.Int64Counter }\nfunc (r rec) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec) {\n\trec.add(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression through a pointer type": "type rec[T any] struct{ c metric.Int64Counter }\nfunc (r *rec[T]) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec[int]) {\n\t(*rec[int]).add(&r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression forwarding a parameter": "type rec[T any] struct{ c metric.Int64Counter }\nfunc (r rec[T]) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func outer(r rec[int], attrs []attribute.KeyValue) { rec[int].add(r, attrs) }\n" +
			"func f(r rec[int]) {\n\touter(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression on a type with two type arguments": "type rec[T, U any] struct{ c metric.Int64Counter }\nfunc (r rec[T, U]) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec[int, string]) {\n\trec[int, string].add(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"parenthesised method expression": "type rec struct{ c metric.Int64Counter }\nfunc (r rec) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec) {\n\t(rec.add)(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"method expression on a parenthesised type": "type rec struct{ c metric.Int64Counter }\nfunc (r rec) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec) {\n\t(rec).add(r, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"parenthesised callee": flowSink + "func f(c metric.Int64Counter) {\n\t(record)(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
		"name copied through a local": "func record(c metric.Int64Counter, attrs []attribute.KeyValue) {\n\tcopyOf := attrs\n\tc.Add(context.Background(), 1, metric.WithAttributes(copyOf...))\n}\n" +
			"func f(c metric.Int64Counter) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n",
	}
	for id, body := range cases {
		got := scanSource(t, header+body)
		if !hasFinding(got, "R2", "user_id") {
			t.Errorf("%s: findings = %v, want R2 user_id", id, got)
		}
		for _, finding := range got {
			if strings.Contains(finding.String(), "S3CRET") {
				t.Errorf("%s: finding text carries the value: %v", id, finding)
			}
		}
	}
}

func TestLabelOriginFlowsAcrossFilesOfAPackage(t *testing.T) {
	got := scanFiles(t, map[string]string{
		"a.go": "package sample\n\nimport (\n\t\"context\"\n\n\t\"go.opentelemetry.io/otel/attribute\"\n\t\"go.opentelemetry.io/otel/metric\"\n)\n\n" + flowSink,
		"b.go": "package sample\n\nimport (\n\t\"go.opentelemetry.io/otel/attribute\"\n\t\"go.opentelemetry.io/otel/metric\"\n)\n\nfunc f(c metric.Int64Counter, v string) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"user_id\", v)})\n}\n",
	})
	if !strings.Contains(got, "R2") {
		t.Errorf("rules = %q, want R2", got)
	}
}

func TestLabelFlowKeepsBoundedKeysAndOtherSinksQuiet(t *testing.T) {
	cases := map[string]string{
		"bounded key through the wrapper": flowSink + "func f(c metric.Int64Counter, v string) {\n\trecord(c, []attribute.KeyValue{attribute.String(\"status\", v)})\n}\n",
		"parameter that reaches no metric sink": "func logit(attrs []attribute.KeyValue) { slog.Info(\"x\", attrs...) }\n" +
			"func f(v string) {\n\tlogit([]attribute.KeyValue{attribute.String(\"user_id\", v)})\n}\n",
		"value parameter is not a label carrier": "func observe(c metric.Int64Counter, route string) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attribute.String(\"route\", route)))\n}\n" +
			"func f(c metric.Int64Counter, v string) {\n\tobserve(c, v)\n}\n",
	}
	for id, body := range cases {
		if got := scanSource(t, header+body); len(got) != 0 {
			t.Errorf("%s: findings = %v, want none", id, got)
		}
	}
}

func TestLabelFlowReportsADynamicKeyThroughTheWrapper(t *testing.T) {
	body := flowSink + "func f(c metric.Int64Counter, k, v string) {\n\trecord(c, []attribute.KeyValue{attribute.String(k, v)})\n}\n"
	if got := scanSource(t, header+body); !hasFinding(got, "R2", "dynamic key") {
		t.Errorf("findings = %v, want R2 dynamic key", got)
	}
}

// The named limits of the label flow: each one is asserted so that the doc stays honest.
func TestLabelFlowNamedLimits(t *testing.T) {
	plant := "[]attribute.KeyValue{attribute.String(\"user_id\", v)}"
	cases := map[string]string{
		"a slice returned by a call": flowSink + "func build(v string) []attribute.KeyValue { return " + plant + " }\n" +
			"func f(c metric.Int64Counter, v string) {\n\trecord(c, build(v))\n}\n",
		"a slice held in a struct field": flowSink + "type h struct{ attrs []attribute.KeyValue }\n" +
			"func f(c metric.Int64Counter, v string) {\n\tx := h{}\n\tx.attrs = " + plant + "\n\trecord(c, x.attrs)\n}\n",
		"a function literal bound to a package name": "var rec = func(c metric.Int64Counter, attrs []attribute.KeyValue) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter, v string) {\n\trec(c, " + plant + ")\n}\n",
		"a slice read from a parameter field": "type holder struct{ attrs []attribute.KeyValue }\n" +
			"func record2(c metric.Int64Counter, h holder) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(h.attrs...))\n}\n" +
			"func f(c metric.Int64Counter, v string) {\n\trecord2(c, holder{attrs: " + plant + "})\n}\n",
		"a function value bound to a name, from a generic function": "func record[T any](c metric.Int64Counter, attrs []attribute.KeyValue, t T) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(c metric.Int64Counter, v string) {\n\tg := record[int]\n\tg(c, " + plant + ", 0)\n}\n",
		"a method value bound to a name": "type rec struct{ c metric.Int64Counter }\nfunc (r rec) add(attrs []attribute.KeyValue) {\n\tr.c.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
			"func f(r rec, v string) {\n\tg := r.add\n\tg(" + plant + ")\n}\n",
		"a function value parameter": "func apply(do func([]attribute.KeyValue), v string) { do(" + plant + ") }\n",
	}
	for id, body := range cases {
		got := scanSource(t, header+body)
		for _, finding := range got {
			if finding.Rule == "R2" && finding.Detail == "user_id" {
				t.Errorf("%s: findings = %v, the limit is documented as unseen", id, got)
			}
		}
	}
	other := scanFiles(t, map[string]string{
		"a.go": "package sample\n\nimport (\n\t\"example.com/other\"\n\n\t\"go.opentelemetry.io/otel/attribute\"\n\t\"go.opentelemetry.io/otel/metric\"\n)\n\nfunc f(c metric.Int64Counter, v string) {\n\tother.Record(c, " + plant + ")\n}\n",
	})
	if strings.Contains(other, "R2") {
		t.Errorf("cross-package call: rules = %q, the limit is documented as unseen", other)
	}
}

func TestLabelFlowStaysCheapOnLongChains(t *testing.T) {
	var chain strings.Builder
	chain.WriteString("func w0(c metric.Int64Counter, a []attribute.KeyValue) { w1(c, a) }\n")
	for i := 1; i < 2000; i++ {
		fmt.Fprintf(&chain, "func w%d(c metric.Int64Counter, a []attribute.KeyValue) { w%d(c, a) }\n", i, i+1)
	}
	chain.WriteString("func w2000(c metric.Int64Counter, a []attribute.KeyValue) { c.Add(context.Background(), 1, metric.WithAttributes(a...)) }\n")
	chain.WriteString("func f(c metric.Int64Counter, v string) { w0(c, []attribute.KeyValue{attribute.String(\"user_id\", v)}) }\n")
	var got []emitscan.Finding
	timed(t, "2000-function wrapper chain", time.Second, func() string { got = scanSource(t, header+chain.String()); return "" })
	if !hasFinding(got, "R2", "user_id") && !tripped(got) {
		t.Errorf("findings = %v, want R2 user_id or the census finding", got)
	}

	mutual := "func a(c metric.Int64Counter, x []attribute.KeyValue) { b(c, x); c.Add(context.Background(), 1, metric.WithAttributes(x...)) }\n" +
		"func b(c metric.Int64Counter, y []attribute.KeyValue) { a(c, y) }\n" +
		"func f(c metric.Int64Counter, v string) { a(c, []attribute.KeyValue{attribute.String(\"user_id\", v)}) }\n"
	timed(t, "mutual recursion", 100*time.Millisecond, func() string { got = scanSource(t, header+mutual); return "" })
	if !hasFinding(got, "R2", "user_id") {
		t.Errorf("mutual recursion: findings = %v, want R2 user_id", got)
	}

	var names strings.Builder
	names.WriteString(flowSink + "func f(c metric.Int64Counter, v string) {\n\tx0 := []attribute.KeyValue{attribute.String(\"user_id\", v)}\n")
	for i := 1; i < 2000; i++ {
		fmt.Fprintf(&names, "\tx%d := x%d\n", i, i-1)
	}
	names.WriteString("\trecord(c, x1999)\n}\n")
	timed(t, "2000-name origin chain", time.Second, func() string { got = scanSource(t, header+names.String()); return "" })
	if !hasFinding(got, "R2", "user_id") && !tripped(got) {
		t.Errorf("findings = %v, want R2 user_id or the census finding", got)
	}
}

// A package function and an indexed function value of the same name are told apart by name only: the call through the
// value is read as a call of the function (a known false positive, noise accepted).
func TestLabelFlowKnownFalsePositives(t *testing.T) {
	body := "func m(c metric.Int64Counter, attrs []attribute.KeyValue) {\n\tc.Add(context.Background(), 1, metric.WithAttributes(attrs...))\n}\n" +
		"func f(c metric.Int64Counter, m map[string]func(metric.Int64Counter, []attribute.KeyValue)) {\n\tm[\"x\"](c, []attribute.KeyValue{attribute.String(\"user_id\", \"S3CRET\")})\n}\n"
	if got := scanSource(t, header+body); !hasFinding(got, "R2", "user_id") {
		t.Errorf("findings = %v, want the known false positive R2 user_id", got)
	}
}

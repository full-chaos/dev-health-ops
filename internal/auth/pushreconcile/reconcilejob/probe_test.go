package reconcilejob

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
)

type harness struct {
	t      *testing.T
	reader *metric.ManualReader
	meter  *metric.MeterProvider
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	return &harness{t: t, reader: reader, meter: provider}
}

func (h *harness) collect() map[string][]metricdata.DataPoint[int64] {
	h.t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		h.t.Fatal(err)
	}
	out := map[string][]metricdata.DataPoint[int64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				out[m.Name] = data.DataPoints
			case metricdata.Sum[int64]:
				out[m.Name] = data.DataPoints
			}
		}
	}
	return out
}

func label(dp metricdata.DataPoint[int64], key string) string {
	v, _ := dp.Attributes.Value(attribute.Key(key))
	return v.AsString()
}

func (h *harness) drift() map[string]int64 {
	h.t.Helper()
	out := map[string]int64{}
	for _, dp := range h.collect()[DriftGaugeName] {
		out[label(dp, ClassLabel)] = dp.Value
	}
	return out
}

func (h *harness) runs() map[string]int64 {
	h.t.Helper()
	out := map[string]int64{}
	for _, dp := range h.collect()[RunsCounterName] {
		out[label(dp, OutcomeLabel)] = dp.Value
	}
	return out
}

// script is a pass func that answers the next scripted result per call.
type script struct {
	results []any // Pass or error
	calls   int
}

func (s *script) pass(context.Context) (Pass, error) {
	r := s.results[min(s.calls, len(s.results)-1)]
	s.calls++
	if err, ok := r.(error); ok {
		return Pass{}, err
	}
	return r.(Pass), nil
}

func findings(fs ...pushreconcile.Finding) Pass { return Pass{Findings: fs} }

func register(t *testing.T, h *harness, s *script) *Probe {
	t.Helper()
	p, err := Register(h.meter.Meter("probe-test"), s.pass, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// C43: off by default; nothing registered, nothing run.
func TestC43ProbeIsOffByDefaultAndRegistersNothing(t *testing.T) {
	for name, lookup := range map[string]func(string) (string, bool){
		"nil":     nil,
		"unset":   func(string) (string, bool) { return "", false },
		"empty":   func(string) (string, bool) { return "", true },
		"off":     func(string) (string, bool) { return "off", true },
		"true":    func(string) (string, bool) { return "true", true },
		"wrong":   func(k string) (string, bool) { return "on", k == "OTHER" },
		"mixed":   func(string) (string, bool) { return "ON", true },
		"padding": func(string) (string, bool) { return " on", true },
	} {
		h := newHarness(t)
		s := &script{results: []any{findings()}}
		p, err := MaybeStart(context.Background(), lookup, h.meter.Meter("probe-test"), s.pass, nil)
		if err != nil || p != nil {
			t.Fatalf("%s: probe %v err %v, want none", name, p, err)
		}
		if got := h.collect(); len(got) != 0 || s.calls != 0 {
			t.Fatalf("%s: instruments %v, pass calls %d; want none", name, got, s.calls)
		}
	}
}

func TestC43bProbeOnRegistersTheGaugeAndTheRunsCounterSeededAtZero(t *testing.T) {
	h := newHarness(t)
	register(t, h, &script{results: []any{findings()}})
	runs := h.runs()
	if v, ok := runs[OutcomeOK]; !ok || v != 0 {
		t.Fatalf("runs = %v, want ok seeded at 0", runs)
	}
	if v, ok := runs[OutcomeFailed]; !ok || v != 0 {
		t.Fatalf("runs = %v, want failed seeded at 0", runs)
	}
	if got := h.drift(); len(got) != 0 {
		t.Fatalf("drift before any pass = %v, want no sample", got)
	}
}

func TestC43bMaybeStartOnStartsTheLoopAndStopEndsIt(t *testing.T) {
	h := newHarness(t)
	s := &script{results: []any{findings()}}
	p, err := MaybeStart(context.Background(), func(k string) (string, bool) { return "on", k == ProbeEnv }, h.meter.Meter("probe-test"), s.pass, nil)
	if err != nil || p == nil {
		t.Fatalf("probe %v err %v, want a running probe", p, err)
	}
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if s.calls < 1 {
		t.Fatal("the loop ran no pass")
	}
	if _, ok := h.runs()[OutcomeOK]; !ok {
		t.Fatal("no runs series after a pass")
	}
}

// C43c: a failed run never publishes 0.
func TestC43cFailedRunRecordsNoDriftSampleAndCountsFailed(t *testing.T) {
	h := newHarness(t)
	s := &script{results: []any{findings(f("status", "a")), findings(f("status", "a")), errors.New("read failed")}}
	p := register(t, h, s)
	p.RunOnce(context.Background())
	p.RunOnce(context.Background())
	if got := h.drift(); got["status"] != 1 {
		t.Fatalf("drift after two agreeing passes = %v, want status 1", got)
	}
	p.RunOnce(context.Background())
	if got := h.drift(); len(got) != 0 {
		t.Fatalf("drift after a failed run = %v, want no sample at all (not zeros)", got)
	}
	if got := h.runs(); got[OutcomeFailed] != 1 || got[OutcomeOK] != 2 {
		t.Fatalf("runs = %v, want ok 2 failed 1", got)
	}
}

// C43d: a finding is published only after two passes agree; the memory is in the process.
func TestC43dConfirmAcrossRunsInMemory(t *testing.T) {
	h := newHarness(t)
	s := &script{results: []any{
		findings(f("status", "a")),
		findings(f("status", "a"), f("scopes", "b")),
		findings(),
	}}
	p := register(t, h, s)
	p.RunOnce(context.Background())
	if got := h.drift(); len(got) != 0 {
		t.Fatalf("drift after the first pass = %v, want no sample (nothing to confirm against)", got)
	}
	p.RunOnce(context.Background())
	got := h.drift()
	if got["status"] != 1 || got["scopes"] != 0 {
		t.Fatalf("drift after the second pass = %v, want status 1 and scopes 0 (b is in one pass only)", got)
	}
	p.RunOnce(context.Background())
	if got := h.drift(); got["status"] != 0 || len(got) != len(pushreconcile.Classes) {
		t.Fatalf("drift after a clean pass = %v, want every class at 0", got)
	}
}

func TestC43eRestartClearsTheConfirmState(t *testing.T) {
	h := newHarness(t)
	same := findings(f("status", "a"))
	first := register(t, h, &script{results: []any{same}})
	first.RunOnce(context.Background())
	first.RunOnce(context.Background())
	if got := h.drift(); got["status"] != 1 {
		t.Fatalf("drift = %v, want status 1 before the restart", got)
	}
	h2 := newHarness(t)
	restarted := register(t, h2, &script{results: []any{same}})
	restarted.RunOnce(context.Background())
	if got := h2.drift(); len(got) != 0 {
		t.Fatalf("drift after a restart and one pass = %v, want no sample", got)
	}
	// A failed run also drops the memory: the pass after it cannot confirm against the one before it.
	h3 := newHarness(t)
	s := &script{results: []any{same, errors.New("read failed"), same, same}}
	p := register(t, h3, s)
	for range 3 {
		p.RunOnce(context.Background())
	}
	if got := h3.drift(); len(got) != 0 {
		t.Fatalf("drift = %v, want no sample: the pass after a failure has nothing to confirm against", got)
	}
	p.RunOnce(context.Background())
	if got := h3.drift(); got["status"] != 1 {
		t.Fatalf("drift = %v, want status 1 after two agreeing passes", got)
	}
}

// C44: the labels are a closed set: class on the gauge (the 7 classes), outcome on the counter (ok, failed).
func TestC44LabelsAreAClosedSet(t *testing.T) {
	h := newHarness(t)
	p := register(t, h, &script{results: []any{findings(f("status", "tok-1"))}})
	p.RunOnce(context.Background())
	p.RunOnce(context.Background())
	all := h.collect()
	var classes []string
	for _, dp := range all[DriftGaugeName] {
		if dp.Attributes.Len() != 1 {
			t.Errorf("drift point has %d labels, want only %q", dp.Attributes.Len(), ClassLabel)
		}
		classes = append(classes, label(dp, ClassLabel))
	}
	want := []string{}
	for _, c := range pushreconcile.Classes {
		want = append(want, string(c))
	}
	slices.Sort(classes)
	slices.Sort(want)
	if !slices.Equal(classes, want) {
		t.Errorf("classes = %v, want %v", classes, want)
	}
	var outcomes []string
	for _, dp := range all[RunsCounterName] {
		if dp.Attributes.Len() != 1 {
			t.Errorf("runs point has %d labels, want only %q", dp.Attributes.Len(), OutcomeLabel)
		}
		outcomes = append(outcomes, label(dp, OutcomeLabel))
	}
	slices.Sort(outcomes)
	if !slices.Equal(outcomes, []string{OutcomeFailed, OutcomeOK}) {
		t.Errorf("outcomes = %v", outcomes)
	}
}

// C45: the code literals are the names the alert rules of the group auth_push_projection read
// (CHAOS-10897 PR-D, alerts/rules.yml rules PushCredentialProjectionDrift, ...DriftLive, PushReconcileNotRunning).
func TestC45CodeLiteralsMatchTheNamesTheAlertRulesRead(t *testing.T) {
	for got, want := range map[string]string{
		DriftGaugeName:  "devhealth_auth_push_reconcile_drift",
		RunsCounterName: "devhealth_auth_push_reconcile_runs_total",
		ClassLabel:      "class",
		OutcomeLabel:    "outcome",
		OutcomeOK:       "ok",
		OutcomeFailed:   "failed",
		ProbeEnv:        "PUSH_RECONCILE_PROBE",
		ProbeOn:         "on",
	} {
		if got != want {
			t.Errorf("literal %q, want %q", got, want)
		}
	}
	// The rules of rules 2 select class=~"status|scopes": both must be classes the diff emits.
	for _, c := range []pushreconcile.Class{pushreconcile.ClassStatus, pushreconcile.ClassScopes} {
		if !slices.Contains(pushreconcile.Classes, c) {
			t.Errorf("class %q is not a drift class", c)
		}
	}
	// When the rules file holds the group, every expression names the same series and labels.
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "alerts", "rules.yml"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "name: auth_push_projection") {
		t.Logf("alerts/rules.yml has no auth_push_projection group yet (PR-D); the literal pins above stand")
		return
	}
	for _, expr := range []string{
		`max by (class) (devhealth_auth_push_reconcile_drift) > 0`,
		`max by (class) (devhealth_auth_push_reconcile_drift{class=~"status|scopes"}) > 0`,
		`sum(increase(devhealth_auth_push_reconcile_runs_total{outcome="ok"}[3h])) == 0`,
	} {
		if !strings.Contains(text, expr) {
			t.Errorf("alerts/rules.yml lacks %q", expr)
		}
	}
}

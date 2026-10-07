package jobruntime

import (
	"strings"
	"testing"
	"time"
)

// Every series of the shadow phase is rendered on every scrape, zero included:
// with the phase off (the default) a missing series and a zero are not the same
// answer.
func TestInvestmentShadowSeriesArePreSeededAtZero(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	rendered := collector.PrometheusText()
	for _, want := range investmentShadowSeries(0) {
		if !strings.Contains(rendered, want+"\n") {
			t.Errorf("missing zero series: %s", want)
		}
	}
	for _, name := range InvestmentShadowMetricNames() {
		if !strings.Contains(rendered, "# TYPE "+name+" ") {
			t.Errorf("metric %s has no TYPE line", name)
		}
	}
}

// The census of the metric names of the shadow phase: a new name, a renamed
// one or a new label value changes this list in review.
func TestInvestmentShadowMetricNamesAndLabelSetsAreTheDeclaredOnes(t *testing.T) {
	want := []string{
		"dev_health_investment_shadow_attempts_total",
		"dev_health_investment_shadow_attempt_latency_seconds",
		"dev_health_investment_shadow_phase_stops_total",
		"dev_health_investment_shadow_phase_cancelled_runs_total",
		"dev_health_investment_shadow_panics_recovered_total",
		"dev_health_investment_shadow_attempt_write_errors_total",
		"dev_health_investment_shadow_attempt_rows_dropped_total",
	}
	if got := InvestmentShadowMetricNames(); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("metric names = %v, want %v", got, want)
	}
	if got := strings.Join(InvestmentShadowStopReasons(), ","); got != "done,budget,cap,deterministic_failure,cancelled,table_missing,store_error,panic" {
		t.Fatalf("stop reasons = %s", got)
	}
	if got := strings.Join(InvestmentShadowAttemptStates(), ","); got != "ok,zero_support,question_refused,answer_missing,answer_invalid,evidence_none,evidence_unanswered,request_failed,adapter_defect,retried" {
		t.Fatalf("attempt states = %s", got)
	}
	if got := strings.Join(investmentShadowModels, ","); got != "jev-1.13.0,other" {
		t.Fatalf("model labels = %s", got)
	}
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	// Every shadow line of the rendered text belongs to a declared name, and
	// no label of one names an organization or a work unit.
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if !strings.Contains(line, "investment_shadow") {
			continue
		}
		declared := false
		for _, name := range want {
			if strings.HasPrefix(line, name) || strings.HasPrefix(line, "# HELP "+name+" ") || strings.HasPrefix(line, "# TYPE "+name+" ") {
				declared = true
			}
		}
		if !declared {
			t.Errorf("a shadow line of an undeclared metric: %s", line)
		}
		if !strings.HasPrefix(line, "#") {
			for _, banned := range []string{"org", "work_unit", "run_id", "request_id"} {
				if strings.Contains(line, banned+"=") || strings.Contains(line, banned+"_id=") {
					t.Errorf("a shadow series has the label %s: %s", banned, line)
				}
			}
		}
	}
}

func TestObserveInvestmentShadowPhaseRecordsEveryCount(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentShadowPhase(InvestmentShadowPhase{
		Model: "jev-1.13.0", StopReason: "cancelled",
		AttemptsByState:  map[string]int{"ok": 5, "retried": 2, "request_failed": 1},
		AttemptLatencies: []time.Duration{80 * time.Millisecond, 200 * time.Millisecond, 3 * time.Second},
		PanicsRecovered:  1, AttemptWriteErrors: 1, AttemptRowsDropped: 7,
	}); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentShadowPhase(InvestmentShadowPhase{
		Model: "jev-9.9.9", StopReason: "done", AttemptsByState: map[string]int{"ok": 3},
	}); err != nil {
		t.Fatal(err)
	}
	rendered := collector.PrometheusText()
	for _, want := range []string{
		`dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="jev-1.13.0",state="ok"} 5`,
		`dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="jev-1.13.0",state="retried"} 2`,
		`dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="jev-1.13.0",state="request_failed"} 1`,
		// A model outside the closed set is counted, under "other": never as a new label value.
		`dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="other",state="ok"} 3`,
		`dev_health_investment_shadow_attempt_latency_seconds_bucket{role="shadow",le="0.1"} 1`,
		`dev_health_investment_shadow_attempt_latency_seconds_bucket{role="shadow",le="0.25"} 2`,
		`dev_health_investment_shadow_attempt_latency_seconds_bucket{role="shadow",le="+Inf"} 3`,
		`dev_health_investment_shadow_attempt_latency_seconds_count{role="shadow"} 3`,
		`dev_health_investment_shadow_phase_stops_total{reason="cancelled"} 1`,
		`dev_health_investment_shadow_phase_stops_total{reason="done"} 1`,
		`dev_health_investment_shadow_phase_stops_total{reason="budget"} 0`,
		`dev_health_investment_shadow_phase_cancelled_runs_total 1`,
		`dev_health_investment_shadow_panics_recovered_total 1`,
		`dev_health_investment_shadow_attempt_write_errors_total 1`,
		`dev_health_investment_shadow_attempt_rows_dropped_total 7`,
	} {
		if !strings.Contains(rendered, want+"\n") {
			t.Errorf("missing: %s", want)
		}
	}
	if strings.Contains(rendered, "jev-9.9.9") {
		t.Error("a model outside the closed set became a label value")
	}
}

// A label outside its closed set or a negative count is refused WHOLE: nothing
// of that phase is recorded.
func TestObserveInvestmentShadowPhaseRefusesAnUnregisteredLabelOrANegativeCount(t *testing.T) {
	for name, phase := range map[string]InvestmentShadowPhase{
		"stop reason":        {StopReason: "because", AttemptsByState: map[string]int{"ok": 1}},
		"empty stop reason":  {AttemptsByState: map[string]int{"ok": 1}},
		"attempt state":      {StopReason: "done", AttemptsByState: map[string]int{"ok": 1, "org-123": 1}},
		"negative attempts":  {StopReason: "done", AttemptsByState: map[string]int{"ok": -1}},
		"negative panics":    {StopReason: "done", AttemptsByState: map[string]int{"ok": 1}, PanicsRecovered: -1},
		"negative errors":    {StopReason: "done", AttemptsByState: map[string]int{"ok": 1}, AttemptWriteErrors: -1},
		"negative dropped":   {StopReason: "done", AttemptsByState: map[string]int{"ok": 1}, AttemptRowsDropped: -1},
		"negative latencies": {StopReason: "done", AttemptsByState: map[string]int{"ok": 1}, AttemptLatencies: []time.Duration{-time.Second}},
	} {
		collector, err := NewMetricsCollector(MetricDimensions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := collector.ObserveInvestmentShadowPhase(phase); err == nil {
			t.Errorf("%s: accepted", name)
		}
		rendered := collector.PrometheusText()
		for _, want := range investmentShadowSeries(0) {
			if !strings.Contains(rendered, want+"\n") {
				t.Errorf("%s: a refused phase moved a series: want %s", name, want)
			}
		}
	}
}

// investmentShadowSeries is every counter series of the shadow phase with the
// given value.
func investmentShadowSeries(value int) []string {
	suffix := " " + string(rune('0'+value))
	series := []string{}
	for _, model := range investmentShadowModels {
		for _, state := range InvestmentShadowAttemptStates() {
			series = append(series, `dev_health_investment_shadow_attempts_total{role="shadow",provider="typesafe",model="`+model+`",state="`+state+`"}`+suffix)
		}
	}
	for _, reason := range InvestmentShadowStopReasons() {
		series = append(series, `dev_health_investment_shadow_phase_stops_total{reason="`+reason+`"}`+suffix)
	}
	return append(series,
		`dev_health_investment_shadow_attempt_latency_seconds_count{role="shadow"}`+suffix,
		`dev_health_investment_shadow_phase_cancelled_runs_total`+suffix,
		`dev_health_investment_shadow_panics_recovered_total`+suffix,
		`dev_health_investment_shadow_attempt_write_errors_total`+suffix,
		`dev_health_investment_shadow_attempt_rows_dropped_total`+suffix,
	)
}

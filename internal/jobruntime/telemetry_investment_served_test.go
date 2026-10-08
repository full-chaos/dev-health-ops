package jobruntime

import (
	"fmt"
	"strings"
	"testing"
)

func investmentServedSeries(model, outcome string, value int) string {
	return fmt.Sprintf(`dev_health_investment_served_outcomes_total{provider="typesafe",model=%q,outcome=%q} %d`, model, outcome, value)
}

// Every series of the served decision mode is rendered on every scrape, zero
// included: with the mode off (the default) a missing series and a zero are not
// the same answer.
func TestInvestmentServedSeriesArePreSeededAtZero(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	rendered := collector.PrometheusText()
	for _, model := range investmentShadowModels {
		for _, outcome := range InvestmentServedOutcomes() {
			if want := investmentServedSeries(model, outcome, 0); !strings.Contains(rendered, want+"\n") {
				t.Errorf("missing zero series: %s", want)
			}
		}
	}
	if !strings.Contains(rendered, "# TYPE dev_health_investment_served_outcomes_total counter\n") {
		t.Error("the served metric has no TYPE line")
	}
}

// The census of the metric names and the outcome label set of the served
// decision mode: a new name or a new value changes this list in review.
func TestInvestmentServedMetricNamesAndLabelSetsAreTheDeclaredOnes(t *testing.T) {
	if got := strings.Join(InvestmentServedMetricNames(), ","); got != "dev_health_investment_served_outcomes_total" {
		t.Fatalf("metric names = %s", got)
	}
	if got := strings.Join(InvestmentServedOutcomes(), ","); got != "ok,zero_support,evidence_none,invalid_answer,adapter_defect,timeout,refused,server_error,rate_limited,rejected,transport_other" {
		t.Fatalf("outcomes = %s", got)
	}
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, line := range strings.Split(collector.PrometheusText(), "\n") {
		if !strings.Contains(line, "investment_served") {
			continue
		}
		lines++
		if !strings.HasPrefix(line, "dev_health_investment_served_outcomes_total") &&
			!strings.HasPrefix(line, "# HELP dev_health_investment_served_outcomes_total ") &&
			!strings.HasPrefix(line, "# TYPE dev_health_investment_served_outcomes_total ") {
			t.Errorf("a served line of an undeclared metric: %s", line)
		}
		if !strings.HasPrefix(line, "#") {
			for _, banned := range []string{"org", "work_unit", "run_id", "request_id", "org_id"} {
				if strings.Contains(line, banned+"=") {
					t.Errorf("a served series has the label %s: %s", banned, line)
				}
			}
		}
	}
	if want := 2 + len(investmentShadowModels)*len(InvestmentServedOutcomes()); lines != want {
		t.Fatalf("%d served lines, want %d", lines, want)
	}
}

func TestObserveInvestmentServedRunRecordsEveryCount(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for index, outcome := range InvestmentServedOutcomes() {
		counts[outcome] = index + 1
	}
	if err := collector.ObserveInvestmentServedRun(InvestmentServedRun{Model: "jev-1.13.0", Outcomes: counts}); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveInvestmentServedRun(InvestmentServedRun{Model: "jev-9.9.9", Outcomes: map[string]int{"refused": 7}}); err != nil {
		t.Fatal(err)
	}
	rendered := collector.PrometheusText()
	for outcome, count := range counts {
		if want := investmentServedSeries("jev-1.13.0", outcome, count); !strings.Contains(rendered, want+"\n") {
			t.Errorf("missing: %s", want)
		}
	}
	if want := investmentServedSeries("other", "refused", 7); !strings.Contains(rendered, want+"\n") {
		t.Errorf("a model outside the set is not counted as other: %s", want)
	}
	if strings.Contains(rendered, "jev-9.9.9") {
		t.Error("a model outside the set is a label value")
	}
}

func TestObserveInvestmentServedRunRefusesAnUnregisteredOutcomeOrANegativeCount(t *testing.T) {
	for name, run := range map[string]InvestmentServedRun{
		"unregistered outcome": {Model: "jev-1.13.0", Outcomes: map[string]int{"ok": 1, "llm_error": 1}},
		"negative count":       {Model: "jev-1.13.0", Outcomes: map[string]int{"ok": 1, "timeout": -1}},
	} {
		t.Run(name, func(t *testing.T) {
			collector, err := NewMetricsCollector(MetricDimensions{})
			if err != nil {
				t.Fatal(err)
			}
			if err := collector.ObserveInvestmentServedRun(run); err == nil {
				t.Fatal("the run was accepted")
			}
			if want := investmentServedSeries("jev-1.13.0", "ok", 0); !strings.Contains(collector.PrometheusText(), want+"\n") {
				t.Fatal("a refused run was partly recorded")
			}
		})
	}
}

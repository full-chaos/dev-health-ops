package goapiproof

import (
	"testing"
	"time"
)

// Every testopsRisk request declares a period delta as Tier B only when its
// range holds at least two days: a shorter range gives every spark a single
// point and every delta null on both planes, and a declared path that reaches
// no float leaf refuses the case. The seven-day range must hold seven days.
func TestTestopsRiskDeltasAreDeclaredOnlyWhereTheRangePopulatesThem(t *testing.T) {
	spec, ok := operationSpecs["testopsRisk"]
	if !ok {
		t.Fatal("testopsRisk has no corpus entry")
	}
	w := DefaultWindow()
	type request struct {
		name string
		vars map[string]any
		opts Options
	}
	requests := []request{{"base", spec.Variables("org-1", w), spec.Parity}}
	for _, v := range spec.Variants {
		requests = append(requests, request{v.Name, v.Variables("org-1", w), v.Parity})
	}
	if len(requests) != 3 {
		t.Fatalf("expected the base request and two variants, got %d", len(requests))
	}
	wantSpan := map[string]int{"SINGLE_DAY": 1, "WEEK": 7}
	deltas := []string{"data.testopsRisk.confidenceDelta", "data.testopsRisk.dragDelta", "data.testopsRisk.stabilityDelta"}
	for _, r := range requests {
		input := r.vars["input"].(map[string]any)
		start, err := time.Parse("2006-01-02", input["startDate"].(string))
		if err != nil {
			t.Fatalf("%s: start %v", r.name, err)
		}
		end, err := time.Parse("2006-01-02", input["endDate"].(string))
		if err != nil {
			t.Fatalf("%s: end %v", r.name, err)
		}
		span := int(end.Sub(start).Hours()/24) + 1
		if want, ok := wantSpan[r.name]; ok && span != want {
			t.Errorf("%s spans %d days, want %d", r.name, span, want)
		}
		declared := 0
		for _, path := range deltas {
			reason, isDeclared := r.opts.FloatTierB[path]
			if !isDeclared {
				continue
			}
			declared++
			if reason == "" {
				t.Errorf("%s: %s is declared without a reason", r.name, path)
			}
		}
		if span < 2 && declared != 0 {
			t.Errorf("%s spans %d day(s) but declares %d delta(s) that are null by construction", r.name, span, declared)
		}
		if span >= 2 && declared == 0 {
			t.Errorf("%s spans %d days and declares no delta", r.name, span)
		}
	}
	if got := testopsRiskWeekStart(w.UntilDate); got != "2026-08-26" {
		t.Errorf("week start for %s = %s, want 2026-08-26", w.UntilDate, got)
	}
	if got := testopsRiskWeekStart("not-a-date"); got != "not-a-date" {
		t.Errorf("an unparseable date must come back unchanged, got %q", got)
	}
}

package home

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestLimitingFactorClaimHedgesOnce(t *testing.T) {
	for name, top := range map[string]Signal{
		"metric signal":    {Title: "Blocked Work appears up", ClaimSubject: "Blocked Work"},
		"risk signal":      {Title: "Compounding risk appears high for Delivery", ClaimSubject: "Compounding risk for Delivery"},
		"no prior period":  {Title: "Cycle Time has no prior period to compare", ClaimSubject: "Cycle Time"},
		"no claim subject": {Title: "WIP saturation has been elevated for a week"},
	} {
		claim := BuildLimitingFactor([]Signal{top}).Claim
		if got := strings.Count(claim, "appears"); got != 1 {
			t.Errorf("%s: claim %q holds %d hedge words, want 1", name, claim, got)
		}
		if !strings.HasSuffix(claim, " appears to be the current limiting factor.") {
			t.Errorf("%s: claim %q", name, claim)
		}
	}
	if got, want := BuildLimitingFactor([]Signal{{Title: "Blocked Work appears up", ClaimSubject: "Blocked Work"}}).Claim, "Blocked Work appears to be the current limiting factor."; got != want {
		t.Errorf("claim = %q, want %q", got, want)
	}
}

var hedgeWord = regexp.MustCompile(`(?i)\b(appears?|leans?|suggests?)\b`)

// A served sentence carries at most one hedge word; a semicolon starts a new
// clause with its own hedge.
func assertHedgedOncePerSentence(t *testing.T, where string, prose string) {
	t.Helper()
	for _, clause := range regexp.MustCompile(`[.;!?]`).Split(prose, -1) {
		if hits := hedgeWord.FindAllString(clause, -1); len(hits) > 1 {
			t.Errorf("%s: sentence %q holds %d hedge words %v", where, strings.TrimSpace(clause), len(hits), hits)
		}
	}
}

func walkServedProse(t *testing.T, name string, r Response) {
	t.Helper()
	var prose = map[string]string{
		"health_state.headline": r.HealthState.Headline,
		"health_state.summary":  r.HealthState.Summary,
		"limiting_factor.claim": r.LimitingFactor.Claim,
		"limiting_factor.why":   r.LimitingFactor.WhyItMatters,
		"limiting_factor.act":   r.LimitingFactor.RecommendedAction,
	}
	if r.Constraint != nil {
		prose["constraint.title"] = r.Constraint.Title
		prose["constraint.claim"] = r.Constraint.Claim
	}
	for i, s := range r.Summary {
		prose[fmt.Sprintf("summary[%d]", i)] = s.Text
	}
	for i, e := range r.Events {
		prose[fmt.Sprintf("events[%d]", i)] = e.Text
	}
	for i, s := range r.Signals {
		prose[fmt.Sprintf("signals[%d].title", i)] = s.Title
		prose[fmt.Sprintf("signals[%d].why", i)] = s.WhyItMatters
		prose[fmt.Sprintf("signals[%d].action", i)] = s.RecommendedAction
		if s.Driver != nil {
			prose[fmt.Sprintf("signals[%d].driver", i)] = *s.Driver
		}
	}
	for where, text := range prose {
		assertHedgedOncePerSentence(t, name+" "+where, text)
	}
}

func TestEveryServedHomeSentenceHedgesOnce(t *testing.T) {
	for name, file := range map[string]string{"org": "org_default.json", "repo": "repo_scoped.json", "team": "team_scoped.json"} {
		walkServedProse(t, name+" golden", loadGolden(t, file))
	}
	metric := Signal{Title: "Blocked Work appears up", ClaimSubject: "Blocked Work", Severity: "critical", AffectedScope: "org",
		WhyItMatters: whyForMetric("blocked_work", "Blocked Work", "up")}
	parts := whyPartsForMetric("blocked_work", "Blocked Work", "up")
	metric.Why = &parts
	for _, dir := range []string{"up", "down", "flat"} {
		for metricName := range metricReasons {
			p := whyPartsForMetric(metricName, "Label", dir)
			sig := Signal{Title: "Label appears " + dir, ClaimSubject: "Label", Severity: "low", AffectedScope: "org",
				WhyItMatters: whyForMetric(metricName, "Label", dir), Why: &p,
				Driver: driverCaption("Label", []string{"Ops", "Web", "Payments"})}
			walkServedProse(t, metricName+"/"+dir, Response{
				HealthState:    BuildHealthState([]Signal{sig}, DataConfidence{Level: "high"}, nil),
				LimitingFactor: BuildLimitingFactor([]Signal{sig}),
				Signals:        []Signal{sig},
			})
		}
	}
	walkServedProse(t, "blocked", Response{
		HealthState:    BuildHealthState([]Signal{metric}, DataConfidence{Level: "high"}, nil),
		LimitingFactor: BuildLimitingFactor([]Signal{metric}),
		Signals:        []Signal{metric},
	})
}

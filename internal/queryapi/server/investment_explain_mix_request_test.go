package server

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain"
)

func decodeExplainBodyForTest(t *testing.T, body string) (investmentExplainRequestBody, []pydanticErrorDetail) {
	t.Helper()
	return decodeInvestmentExplainRequestBody([]any{"body"}, []byte(body))
}

func TestExplainRequestNamesThePagesMeasureDateRangeAndUseInvestment(t *testing.T) {
	body, errs := decodeExplainBodyForTest(t, `{"measure":"COUNT","use_investment":true,"date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"scope":{"level":"team","ids":[]}}}`)
	if len(errs) > 0 {
		t.Fatalf("errors: %+v", errs)
	}
	if body.Measure == nil || *body.Measure != "COUNT" || body.UseInvestment == nil || !*body.UseInvestment {
		t.Fatalf("measure/use_investment = %v/%v", body.Measure, body.UseInvestment)
	}
	wantStart := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	if body.DateRange == nil || !body.DateRange.StartDate.Equal(wantStart) || !body.DateRange.EndDate.Equal(wantEnd) {
		t.Fatalf("date range = %+v, want [%s, %s)", body.DateRange, wantStart, wantEnd)
	}
}

func TestExplainRequestRefusesABadMeasureOrDateRange(t *testing.T) {
	for name, body := range map[string]string{
		"unknown measure":           `{"measure":"HOURS"}`,
		"effort measure":            `{"measure":"EFFORT"}`,
		"lines of code measure":     `{"measure":"CHURN_LOC"}`,
		"blank measure":             `{"measure":" "}`,
		"measure not a string":      `{"measure":3}`,
		"use_investment type":       `{"use_investment":"maybe"}`,
		"range without end":         `{"date_range":{"start_date":"2026-09-30"}}`,
		"range end first":           `{"date_range":{"start_date":"2026-10-14","end_date":"2026-09-30"}}`,
		"range bad date":            `{"date_range":{"start_date":"yesterday","end_date":"2026-10-14"}}`,
		"range over the page limit": `{"date_range":{"start_date":"1990-01-01","end_date":"2026-10-14"}}`,
		"range not an object":       `{"date_range":"2026-09-30"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, errs := decodeExplainBodyForTest(t, body); len(errs) == 0 {
				t.Fatalf("body %s was accepted", body)
			}
		})
	}
}

func TestExplainOptionsDefaultToThePagesMeasureAndTakeItsWindow(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	build := func(body string) investmentexplain.ExplainInvestmentMixOptions {
		t.Helper()
		decoded, errs := decodeExplainBodyForTest(t, body)
		if len(errs) > 0 {
			t.Fatalf("errors: %+v", errs)
		}
		opts, err := buildExplainOptions(context.Background(), reader, "org-1", decoded, "auto", false)
		if err != nil {
			t.Fatal(err)
		}
		return opts
	}

	relative := build(`{"filters":{"time":{"range_days":14,"compare_days":14},"scope":{"level":"org","ids":[]}}}`)
	if relative.Measure != investmentexplain.MeasureIssueSize {
		t.Errorf("measure with none sent = %q, want the page's ISSUE_SIZE", relative.Measure)
	}
	if want := relative.StartTS.AddDate(0, 0, 14); !relative.EndTS.Equal(want) {
		t.Errorf("relative window = [%s, %s), want 14 days", relative.StartTS, relative.EndTS)
	}

	absolute := build(`{"measure":"THROUGHPUT","date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"time":{"range_days":3},"scope":{"level":"team","ids":[]}}}`)
	if absolute.Measure != investmentexplain.MeasureCount {
		t.Errorf("measure = %q, want COUNT (THROUGHPUT is COUNT on the investment path)", absolute.Measure)
	}
	if !absolute.StartTS.Equal(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)) || !absolute.EndTS.Equal(time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("absolute window = [%s, %s), want the page's [2026-09-30, 2026-10-14) over filters.time", absolute.StartTS, absolute.EndTS)
	}

	// Scope level org and team with no ids read the same rows: neither narrows the mix.
	org := build(`{"date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"scope":{"level":"org","ids":[]}}}`)
	team := build(`{"date_range":{"start_date":"2026-09-30","end_date":"2026-10-14"},"filters":{"scope":{"level":"team","ids":[]}}}`)
	same := func(a, b investmentexplain.ExplainInvestmentMixOptions) bool {
		return reflect.DeepEqual(a.RepoIDs, b.RepoIDs) && a.ReposNamed == b.ReposNamed &&
			a.TeamScopeCondition == b.TeamScopeCondition && reflect.DeepEqual(a.TeamIDs, b.TeamIDs) && a.Measure == b.Measure
	}
	if !same(org, team) {
		t.Errorf("org scope and team scope with no ids differ: %+v vs %+v", org, team)
	}
	if team.ScopeLevel != "team" || org.ScopeLevel != "org" {
		t.Errorf("scope levels = %q/%q", org.ScopeLevel, team.ScopeLevel)
	}
}

package server

import (
	"context"
	"reflect"
	"strings"
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
	if !absolute.StartTS.Equal(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)) || !absolute.EndTS.Equal(time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("absolute window = [%s, %s), want the page's days 2026-09-30 to 2026-10-14, both counted (D6101), i.e. [2026-09-30, 2026-10-15), over filters.time", absolute.StartTS, absolute.EndTS)
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

// CHAOS-9809: the explain request accepts ISSUE_SIZE, and the refusal of an unknown measure names it among the
// accepted values, so a client reading the error learns the whole contract.
func TestExplainRequestAcceptsIssueSizeAndNamesItInTheRefusal(t *testing.T) {
	body, errs := decodeExplainBodyForTest(t, `{"measure":"ISSUE_SIZE"}`)
	if len(errs) > 0 || body.Measure == nil || *body.Measure != "ISSUE_SIZE" {
		t.Fatalf("ISSUE_SIZE: body %v, errors %+v", body.Measure, errs)
	}
	_, errs = decodeExplainBodyForTest(t, `{"measure":"HOURS"}`)
	if len(errs) != 1 || errs[0].Type != "literal_error" || !strings.Contains(errs[0].Msg, "'ISSUE_SIZE'") || !strings.Contains(errs[0].Msg, "'COUNT'") || !strings.Contains(errs[0].Msg, "'THROUGHPUT'") {
		t.Fatalf("HOURS refusal = %+v, want one literal_error naming ISSUE_SIZE, COUNT and THROUGHPUT", errs)
	}
}

// TestExplainDateRangeEndIsInclusiveAndNeverToday pins D6101 on the explain route: the page's dateRange names its last day,
// that day counts, and an end of today or later is the same window as an end of yesterday: [start, today).
func TestExplainDateRangeEndIsInclusiveAndNeverToday(t *testing.T) {
	reader, err := investmentexplain.NewReader(unusedQueryClient{})
	if err != nil {
		t.Fatal(err)
	}
	today := time.Now().UTC().Truncate(24 * time.Hour)
	start := today.AddDate(0, 0, -30)
	for _, c := range []struct {
		name    string
		end     time.Time
		wantEnd time.Time
	}{
		{"two days ago counts that day", today.AddDate(0, 0, -2), today.AddDate(0, 0, -1)},
		{"yesterday", today.AddDate(0, 0, -1), today},
		{"today is not in the window", today, today},
		{"tomorrow is clipped", today.AddDate(0, 0, 1), today},
	} {
		body := `{"date_range":{"start_date":"` + start.Format(time.DateOnly) + `","end_date":"` + c.end.Format(time.DateOnly) + `"},"filters":{"scope":{"level":"org","ids":[]}}}`
		decoded, errs := decodeExplainBodyForTest(t, body)
		if len(errs) > 0 {
			t.Fatalf("%s: errors: %+v", c.name, errs)
		}
		opts, err := buildExplainOptions(context.Background(), reader, "org-1", decoded, "auto", false)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !opts.EndTS.Equal(c.wantEnd) || !opts.StartTS.Equal(start) {
			t.Errorf("%s: window [%s, %s), want [%s, %s)", c.name, opts.StartTS.Format(time.DateOnly), opts.EndTS.Format(time.DateOnly), start.Format(time.DateOnly), c.wantEnd.Format(time.DateOnly))
		}
	}
}

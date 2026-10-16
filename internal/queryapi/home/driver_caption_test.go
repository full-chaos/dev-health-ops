package home

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

func TestDriverCaptionNamesTheDriversWithOneHedge(t *testing.T) {
	for _, tc := range []struct {
		names []string
		want  string
	}{
		{[]string{"Ops"}, "Cycle Time appears to have moved most in Ops."},
		{[]string{"Ops", "Web"}, "Cycle Time appears to have moved most in Ops and Web."},
		{[]string{"Ops", "Web", "Payments"}, "Cycle Time appears to have moved most in Ops, Web and Payments."},
	} {
		got := driverCaption("Cycle Time", tc.names)
		if got == nil || *got != tc.want {
			t.Errorf("driverCaption(%v) = %v, want %q", tc.names, got, tc.want)
			continue
		}
		if n := len(hedgeWord.FindAllString(*got, -1)); n != 1 {
			t.Errorf("caption %q holds %d hedge words, want exactly 1", *got, n)
		}
	}
}

// No driver fact, no caption: a nil, never an empty string or a placeholder.
func TestDriverCaptionIsNilWithoutAFact(t *testing.T) {
	if got := driverCaption("Cycle Time", nil); got != nil {
		t.Errorf("no driver name: caption %q, want nil", *got)
	}
	if got := driverCaption("Cycle Time", []string{}); got != nil {
		t.Errorf("empty driver names: caption %q, want nil", *got)
	}
	if got := driverCaption("", []string{"Ops"}); got != nil {
		t.Errorf("no metric label: caption %q, want nil", *got)
	}
}

// Wording census: every metric label of the Home spec list, with one, two and
// three driver names, passes the one-hedge rule of the served prose and ends as
// a sentence. The caption is built from the label, never the hedged Title.
func TestDriverCaptionWordingCensus(t *testing.T) {
	for _, m := range metrics {
		for n := 1; n <= driverCaptionLimit; n++ {
			var names []string
			for i := 0; i < n; i++ {
				names = append(names, fmt.Sprintf("Driver %d", i))
			}
			caption := driverCaption(m.Label, names)
			if caption == nil {
				t.Errorf("%s/%d: nil caption", m.Metric, n)
				continue
			}
			assertHedgedOncePerSentence(t, m.Metric, *caption)
			if got := len(hedgeWord.FindAllString(*caption, -1)); got != 1 {
				t.Errorf("%s/%d: %q holds %d hedge words, want exactly 1", m.Metric, n, *caption, got)
			}
			if !strings.HasPrefix(*caption, m.Label+" appears to have moved most in ") || !strings.HasSuffix(*caption, ".") || strings.Contains(*caption, "\n") {
				t.Errorf("%s/%d: caption %q is not one sentence about the label", m.Metric, n, *caption)
			}
			if strings.Contains(*caption, " appears up") || strings.Contains(*caption, " appears down") {
				t.Errorf("%s/%d: caption %q was built from the hedged title", m.Metric, n, *caption)
			}
		}
	}
}

func TestStatesDriverMove(t *testing.T) {
	pct := func(v float64) *float64 { return &v }
	prior := 5.0
	yes := true
	for name, tc := range map[string]struct {
		d    MetricDelta
		want bool
	}{
		"a measured move":          {MetricDelta{Metric: "churn", Value: 12, DeltaPct: pct(20), HasData: true, HasPriorData: true, PriorValue: &prior}, true},
		"a measured 0 percent":     {MetricDelta{Metric: "churn", Value: 5, DeltaPct: pct(0), HasData: true, HasPriorData: true, PriorValue: &prior}, false},
		"a rise from a measured 0": {MetricDelta{Metric: "churn", Value: 5, HasData: true, HasPriorData: true, PriorValue: new(float64)}, false},
		"no prior window":          {MetricDelta{Metric: "churn", Value: 5, HasData: true}, false},
		"no value":                 {MetricDelta{Metric: "churn", DeltaPct: pct(20)}, false},
		// A work-item metric under a repository filter is the repo-linked calculation; the team daily table
		// that names its drivers has no repository dimension, so the teams would not be the filtered ones.
		"a work-item metric narrowed by a repository filter":  {MetricDelta{Metric: "cycle_time", Value: 12, DeltaPct: pct(20), HasData: true, HasPriorData: true, RepoFilterApplied: &yes}, false},
		"a work-item metric, no repository filter":            {MetricDelta{Metric: "cycle_time", Value: 12, DeltaPct: pct(20), HasData: true, HasPriorData: true}, true},
		"a repository metric narrowed by a repository filter": {MetricDelta{Metric: "churn", Value: 12, DeltaPct: pct(20), HasData: true, HasPriorData: true, RepoFilterApplied: &yes}, true},
	} {
		if got := statesDriverMove(tc.d); got != tc.want {
			t.Errorf("%s: statesDriverMove = %v, want %v", name, got, tc.want)
		}
	}
}

// Through the real producer (BuildResponse over the recorded org fixture): the
// metric signals of a stated move carry the caption of the batched read, read
// in ONE statement for the whole request; a signal the batch names no driver
// for, and every non-metric signal, carries none.
func TestBuildResponseServesTheDriverCaptionFromOneBatchedRead(t *testing.T) {
	batches := 0
	base := orgGoldenHandler(t)
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(q, "UNION ALL") && strings.Contains(q, "LIMIT {limit:UInt32} BY metric") {
			batches++
		}
		return base(t, q, b)
	}}
	resp, err := BuildResponse(context.Background(), client, nil, "org-1", Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: "org"},
	}, time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if batches != 1 {
		t.Errorf("the driver read ran %d times for one request, want 1 batched statement", batches)
	}
	served := 0
	for _, s := range resp.Signals {
		switch {
		case s.ID == "metric:churn":
			if s.Driver == nil || *s.Driver != "Code Churn appears to have moved most in alpha-service." {
				t.Errorf("churn driver = %v, want the caption of the batch row", s.Driver)
			}
			served++
		case s.Driver != nil:
			t.Errorf("signal %s carries the driver %q, but the batch named none for it", s.ID, *s.Driver)
		}
	}
	if served != 1 {
		t.Errorf("the churn signal was not in the response")
	}
}

// The caption is a decoration of a signal that is right without it: a failed
// batched read (or a failed scope read for it) leaves the captions out, logged,
// and never fails the request (REST Home shares the builder and does not serve
// the field).
func TestBuildResponseSurvivesAFailedDriverCaptionRead(t *testing.T) {
	base := orgGoldenHandler(t)
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(q, "UNION ALL") && strings.Contains(q, "LIMIT {limit:UInt32} BY metric") {
			return nil, fmt.Errorf("simulated driver-caption read outage")
		}
		return base(t, q, b)
	}}
	resp, err := BuildResponse(context.Background(), client, nil, "org-1", Filters{
		Time:  TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))},
		Scope: ScopeFilter{Level: "org"},
	}, time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("a failed caption read failed the request: %v", err)
	}
	if len(resp.Signals) == 0 {
		t.Fatal("no signals")
	}
	for _, s := range resp.Signals {
		if s.Driver != nil {
			t.Errorf("signal %s carries the caption %q after a failed read", s.ID, *s.Driver)
		}
	}
}

// A stored display name can hold a line break or runs of spaces: the caption is
// one line, so names are collapsed to single spaces and an empty one is dropped.
func TestDriverNamesAreCollapsedToOneLine(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, q string, b []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(q, "FROM repos FINAL"):
			return &fixtureRowScanner{rows: [][]any{{"r1", "Team A\nTeam  B"}, {"r2", " \n "}, {"r3", "Plain"}}}, nil
		}
		t.Fatalf("unexpected query:\n%s", q)
		return nil, nil
	}}
	got := driverNamesByMetric(context.Background(), client, "org-1", map[string][]driverRow{"churn": {{ID: "r1"}, {ID: "r2"}, {ID: "r3"}}})
	caption := driverCaption("Code Churn", got["churn"])
	if caption == nil || *caption != "Code Churn appears to have moved most in Team A Team B and Plain." {
		t.Errorf("caption = %v, names %q", caption, got["churn"])
	}
}

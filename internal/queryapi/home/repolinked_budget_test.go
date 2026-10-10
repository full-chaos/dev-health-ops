package home

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// A read past its time budget is a STATED state: no data and "timed_out", never an
// empty tile that reads as healthy and never the unfiltered value.
func TestRepoLinkedReadPastItsBudgetSaysTimedOut(t *testing.T) {
	var linkedQueries int
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM repos FINAL"):
			return &fixtureRowScanner{rows: [][]any{{"11111111-1111-4111-8111-111111111111"}}}, nil
		case strings.Contains(query, "link_repo_ids"):
			linkedQueries++
			if !strings.Contains(query, "max_execution_time") {
				t.Errorf("a repo-linked statement carries no max_execution_time:\n%s", query)
			}
			return nil, errors.New("code: 159, message: Timeout exceeded (TIMEOUT_EXCEEDED)")
		}
		t.Fatalf("unexpected statement: %s", query)
		return nil, nil
	}}
	spec := metricSpecByName(t, "throughput")
	start := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	got, err := computeMetricDelta(context.Background(), client, spec, start, start.AddDate(0, 0, 7), start.AddDate(0, 0, -7), start,
		Filters{What: WhatFilter{Repos: []string{"acme/checkout"}}}, "org-1", start)
	if err != nil {
		t.Fatalf("a timeout is a state, not an error: %v", err)
	}
	if got.RepoLinkState == nil || *got.RepoLinkState != repoLinkTimedOut || got.HasData || got.HasPriorData || got.Value != 0 {
		t.Fatalf("delta = %+v, want timed_out, no data, value 0", got)
	}
	if got.RepoFilterApplied == nil || !*got.RepoFilterApplied {
		t.Errorf("repoFilterApplied = %v, want true (the filter was applied)", got.RepoFilterApplied)
	}
	if linkedQueries == 0 {
		t.Fatal("no repo-linked statement ran")
	}
}

func metricSpecByName(t *testing.T, name string) metricSpec {
	t.Helper()
	for _, spec := range metrics {
		if spec.Metric == name {
			return spec
		}
	}
	t.Fatalf("no metric %s", name)
	return metricSpec{}
}

// Named repositories that resolve to nothing read no linked item at all: the
// filter matches nothing, so no repo-linked statement runs.
func TestRepoLinkedUnresolvedRepositoryRunsNoLinkedStatement(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM repos FINAL") {
			return &fixtureRowScanner{rows: nil}, nil
		}
		t.Errorf("a statement ran for repositories that resolved to nothing:\n%s", query)
		return &fixtureRowScanner{rows: nil}, nil
	}}
	start := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		got, err := computeMetricDelta(context.Background(), client, metricSpecByName(t, name), start, start.AddDate(0, 0, 7), start.AddDate(0, 0, -7), start,
			Filters{What: WhatFilter{Repos: []string{"acme/nothing"}}}, "org-1", start)
		if err != nil {
			t.Fatal(err)
		}
		if got.HasData || got.Value != 0 || got.RepoLinkState == nil || *got.RepoLinkState != repoLinkNoLinks {
			t.Errorf("%s: %+v, want no data, no_links", name, got)
		}
	}
}

// D5855: a repository list of empty strings names no repository: the request is not
// filtered by one (no flag, no statement of the repository read), whichever surface
// decoded it.
func TestRepositoryListOfEmptyStringsNamesNoRepository(t *testing.T) {
	for name, f := range map[string]Filters{
		"what.repos [\"\"]":       {What: WhatFilter{Repos: []string{""}}},
		"repo scope [\"\", \"\"]": {Scope: ScopeFilter{Level: "repo", IDs: []string{"", ""}}},
	} {
		if repoFilterRequested(f) {
			t.Errorf("%s: a repository filter is requested", name)
		}
		if flag := repoFilterApplied(f, "repo"); flag != nil {
			t.Errorf("%s: repoFilterApplied = %v, want null", name, *flag)
		}
	}
	if !repoFilterRequested(Filters{What: WhatFilter{Repos: []string{"", "acme/a"}}}) {
		t.Error("an empty string beside a real name hides the name")
	}
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		t.Errorf("a statement ran for a list of empty strings:\n%s", query)
		return &fixtureRowScanner{rows: nil}, nil
	}}
	filter, bindings, err := repoScopeFilter(context.Background(), client, Filters{What: WhatFilter{Repos: []string{""}}}, "org-1", "repo_id", time.Now())
	if err != nil || filter != "" || len(bindings) != 0 {
		t.Errorf("repoScopeFilter = %q %v %v, want no condition", filter, bindings, err)
	}
}

// D5863: a row bound hit is a stated state of its own, never an error that fails the
// Home document, and never mistaken for a time bound (or the reverse).
func TestRepoLinkedBoundsAreClassified(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"time":            {errors.New("code: 159, message: Timeout exceeded (TIMEOUT_EXCEEDED)"), repoLinkTimedOut},
		"rows (code 396)": {errors.New("code: 396, message: Limit for result exceeded, max rows: 1.00 thousand, current rows: 1.40 thousand"), repoLinkTooLarge},
		"rows by name":    {errors.New("clickhouse: TOO_MANY_ROWS_OR_BYTES"), repoLinkTooLarge},
		"another error":   {errors.New("code: 60, message: Table does not exist"), ""},
	} {
		if got := repoLinkBoundHit(tc.err); got != tc.want {
			t.Errorf("%s: state %q, want %q", name, got, tc.want)
		}
	}
}

func TestRepoLinkedReadPastItsRowBoundSaysTooLarge(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		switch {
		case strings.Contains(query, "FROM repos FINAL"):
			return &fixtureRowScanner{rows: [][]any{{"11111111-1111-4111-8111-111111111111"}}}, nil
		case strings.Contains(query, "link_repo_ids"):
			if !strings.Contains(query, "max_result_rows = ") {
				t.Errorf("a repo-linked statement carries no max_result_rows:\n%s", query)
			}
			return nil, errors.New("code: 396, message: Limit for result exceeded")
		}
		t.Fatalf("unexpected statement: %s", query)
		return nil, nil
	}}
	start := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	got, err := computeMetricDelta(context.Background(), client, metricSpecByName(t, "throughput"), start, start.AddDate(0, 0, 7), start.AddDate(0, 0, -7), start,
		Filters{What: WhatFilter{Repos: []string{"acme/checkout"}}}, "org-1", start)
	if err != nil {
		t.Fatalf("a row bound is a state, not an error: %v", err)
	}
	if got.RepoLinkState == nil || *got.RepoLinkState != repoLinkTooLarge || got.HasData || got.Value != 0 {
		t.Fatalf("delta = %+v, want too_large with no data", got)
	}
}

// A timeout while resolving the named repository degrades the four metrics only
// (D5863): a stated state, never an error that fails the whole Home document.
func TestRepoLinkedRepositoryResolutionPastItsBudgetSaysTimedOut(t *testing.T) {
	client := fakeQueryClient{t: t, handler: func(t *testing.T, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
		if strings.Contains(query, "FROM repos FINAL") {
			return nil, errors.New("code: 159, message: Timeout exceeded (TIMEOUT_EXCEEDED)")
		}
		t.Fatalf("no statement may follow a timed-out resolution: %s", query)
		return nil, nil
	}}
	spec := metricSpecByName(t, "throughput")
	start := time.Date(2026, 8, 18, 0, 0, 0, 0, time.UTC)
	got, err := computeMetricDelta(context.Background(), client, spec, start, start.AddDate(0, 0, 7), start.AddDate(0, 0, -7), start,
		Filters{What: WhatFilter{Repos: []string{"acme/checkout"}}}, "org-1", start)
	if err != nil {
		t.Fatalf("a resolution timeout is a state, not an error: %v", err)
	}
	if got.RepoLinkState == nil || *got.RepoLinkState != repoLinkTimedOut || got.HasData || got.Value != 0 {
		t.Fatalf("delta = %+v, want timed_out, no data", got)
	}
}

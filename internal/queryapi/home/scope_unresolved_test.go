package home

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// existenceClient answers the scope-existence reads (CHAOS-6119) from a set of ids that exist per organization and records every
// statement and its bindings. Any other statement gets no rows.
type existenceClient struct {
	mu         sync.Mutex
	known      map[string]map[string]bool // org -> kind -> id set; kind "repo" or "developer"
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *existenceClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	kind := ""
	switch {
	case strings.Contains(statement, "FROM repos FINAL") && strings.Contains(statement, "scope_ids"):
		kind = "repo"
	case strings.Contains(statement, "user_metrics_daily") && strings.Contains(statement, "scope_ids"):
		kind = "developer"
	}
	if kind == "" {
		return &fixtureRowScanner{}, nil
	}
	org, ids := "", []string(nil)
	for _, b := range bindings {
		switch b.Name {
		case "org_id":
			org = b.Value.(string)
		case "scope_ids":
			ids = b.Value.([]string)
		}
	}
	var rows [][]any
	for _, id := range ids {
		if c.known[org+"/"+kind][id] {
			rows = append(rows, []any{id})
		}
	}
	return &fixtureRowScanner{rows: rows}, nil
}

func (c *existenceClient) resolveQueries() int {
	n := 0
	for _, s := range c.statements {
		if strings.Contains(s, "scope_ids") && (strings.Contains(s, "FROM repos FINAL") || strings.Contains(s, "user_metrics_daily")) {
			n++
		}
	}
	return n
}

func newExistenceClient() *existenceClient {
	return &existenceClient{known: map[string]map[string]bool{
		"org-a/repo":      {"aaaaaaaa-1111-4111-8111-111111111111": true, "api-server": true},
		"org-a/developer": {"dev@example.com": true, "identity-7": true},
		"org-b/repo":      {"bbbbbbbb-2222-4222-8222-222222222222": true},
		"org-b/developer": {"other@example.com": true},
	}}
}

func resolved(t *testing.T, c *existenceClient, org, level string, ids ...string) Filters {
	t.Helper()
	f, err := ResolveScope(context.Background(), c, org, Filters{Scope: ScopeFilter{Level: level, IDs: ids}})
	if err != nil {
		t.Fatalf("ResolveScope(%s %v): %v", level, ids, err)
	}
	return f
}

func TestResolveScopeAnswersUnresolvedForAnUnknownIDAtEveryLevel(t *testing.T) {
	for level, id := range map[string]string{"repo": "nope", "service": "svc-1", "developer": "nobody@example.com"} {
		c := newExistenceClient()
		if f := resolved(t, c, "org-a", level, id); !f.Scope.Unresolved() {
			t.Errorf("%s %q: Unresolved() = false, want true", level, id)
		}
	}
}

func TestResolveScopeKeepsAKnownIDAndAMixedList(t *testing.T) {
	cases := map[string]struct {
		level string
		ids   []string
	}{
		"repo by uuid":           {"repo", []string{"aaaaaaaa-1111-4111-8111-111111111111"}},
		"repo by uppercase uuid": {"repo", []string{"AAAAAAAA-1111-4111-8111-111111111111"}},
		"repo by name":           {"repo", []string{"api-server"}},
		"repo mixed":             {"repo", []string{"nope", "api-server"}},
		"developer by email":     {"developer", []string{"dev@example.com"}},
		"developer by identity":  {"developer", []string{"identity-7"}},
		"developer mixed":        {"developer", []string{"nobody@example.com", "dev@example.com"}},
	}
	for name, tc := range cases {
		c := newExistenceClient()
		if f := resolved(t, c, "org-a", tc.level, tc.ids...); f.Scope.Unresolved() {
			t.Errorf("%s: Unresolved() = true, want false", name)
		}
	}
}

func TestResolveScopeCountsAnIDOfAnotherOrganizationAsUnknown(t *testing.T) {
	for level, id := range map[string]string{"repo": "bbbbbbbb-2222-4222-8222-222222222222", "developer": "other@example.com"} {
		c := newExistenceClient()
		if f := resolved(t, c, "org-a", level, id); !f.Scope.Unresolved() {
			t.Errorf("%s: an id that exists only in org-b is known to org-a", level)
		}
		if f := resolved(t, c, "org-b", level, id); f.Scope.Unresolved() {
			t.Errorf("%s: the id of org-b is unknown to org-b", level)
		}
	}
}

func TestResolveScopeIsOneBoundedOrgScopedQueryPerLevel(t *testing.T) {
	for level, want := range map[string]int{"repo": 1, "developer": 1, "service": 0, "team": 0, "org": 0} {
		c := newExistenceClient()
		resolved(t, c, "org-a", level, "a", "b", "c", "a")
		if got := c.resolveQueries(); got != want {
			t.Errorf("%s: %d resolve queries, want %d", level, got, want)
		}
		for i, stmt := range c.statements {
			var count uint64
			for _, b := range c.bindings[i] {
				if b.Name == "id_count" {
					count = b.Value.(uint64)
				}
			}
			if count != 3 || strings.Count(stmt, "LIMIT {id_count:UInt64}") < 1 {
				t.Errorf("%s: id_count = %d (want 3 unique ids) or no LIMIT in\n%s", level, count, stmt)
			}
			if got, branches := strings.Count(stmt, "org_id = {org_id:String}"), strings.Count(stmt, "FINAL"); got != branches {
				t.Errorf("%s: %d org predicates for %d FINAL sources:\n%s", level, got, branches, stmt)
			}
			if strings.Contains(stmt, "{start_day") || strings.Contains(stmt, "day >=") {
				t.Errorf("%s: the existence read has a time window:\n%s", level, stmt)
			}
		}
	}
}

func TestResolveScopeIsDoneOnce(t *testing.T) {
	c := newExistenceClient()
	f := resolved(t, c, "org-a", "repo", "api-server")
	if _, err := ResolveScope(context.Background(), c, "org-a", f); err != nil || c.resolveQueries() != 1 {
		t.Fatalf("second ResolveScope: err %v, %d queries, want 1 in all", err, c.resolveQueries())
	}
}

func TestAnUnresolvedScopeFiltersEveryMetricScopeToNothingAndAKnownRepoKeepsTodaysAnswer(t *testing.T) {
	now := time.Now()
	unknown := resolved(t, newExistenceClient(), "org-a", "repo", "nope")
	for _, metricScope := range []string{"repo", "team", "org"} {
		sql, _, err := scopeFilterForMetric(context.Background(), nil, metricScope, unknown, "org-a", "team_id", "repo_id", now)
		if err != nil || sql != " AND 1 = 0" {
			t.Errorf("unknown repo, %s metric: %q %v, want AND 1 = 0", metricScope, sql, err)
		}
	}
	known := resolved(t, newExistenceClient(), "org-a", "repo", "api-server")
	for _, metricScope := range []string{"team", "org"} {
		if sql, _, err := scopeFilterForMetric(context.Background(), nil, metricScope, known, "org-a", "team_id", "repo_id", now); err != nil || sql != "" {
			t.Errorf("known repo, %s metric: %q %v, want today's answer (no filter)", metricScope, sql, err)
		}
	}
	for _, level := range []string{"service", "developer"} {
		f := resolved(t, newExistenceClient(), "org-a", level, "nope-"+level)
		if sql, _, _ := repoScopeFilter(context.Background(), nil, f, "org-a", "repo_id", now); sql != " AND 1 = 0" {
			t.Errorf("unknown %s: repoScopeFilter = %q, want AND 1 = 0", level, sql)
		}
	}
}

func TestHomeAnswersNoMetricRowsForAnUnresolvedScopeAtEveryLevel(t *testing.T) {
	for level, id := range map[string]string{"repo": "nope", "service": "svc-1", "developer": "nobody@example.com"} {
		c := newExistenceClient()
		f := Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))}, Scope: ScopeFilter{Level: level, IDs: []string{id}}}
		resp, err := BuildResponse(context.Background(), c, nil, "org-a", f, goldenNow)
		if err != nil {
			t.Fatalf("%s: BuildResponse: %v", level, err)
		}
		metricReads := 0
		for _, stmt := range c.statements {
			for _, table := range []string{"repo_metrics_daily", "work_item_metrics_daily", "deploy_metrics_daily"} {
				if strings.Contains(stmt, table) && strings.Contains(stmt, "AND 1 = 0") {
					metricReads++
				}
			}
		}
		if metricReads == 0 {
			t.Errorf("%s: no metric read carried AND 1 = 0", level)
		}
		for _, d := range resp.Deltas {
			if d.HasData {
				t.Errorf("%s: metric %s has data for an unresolved scope", level, d.Metric)
			}
		}
	}
}

// The reason of an empty filter is the existing one for an unresolved service or developer scope (CHAOS-6119); a known scope and an
// org scope carry none, and nothing is read.
func TestFilterEmptyReasonForAnUnresolvedServiceOrDeveloperScope(t *testing.T) {
	for _, level := range []string{"service", "developer"} {
		unknown := resolved(t, newExistenceClient(), "org-a", level, "nope")
		reason, err := filterEmptyReason(context.Background(), nil, unknown, "org-a", time.Now())
		if err != nil || reason == nil || string(*reason) != "repository_not_found" {
			t.Errorf("%s unknown: reason %v err %v, want repository_not_found", level, reason, err)
		}
	}
	known := resolved(t, newExistenceClient(), "org-a", "developer", "dev@example.com")
	if reason, err := filterEmptyReason(context.Background(), nil, known, "org-a", time.Now()); err != nil || reason != nil {
		t.Errorf("known developer: reason %v err %v, want null", reason, err)
	}
}

// BuildResponseWithFilterEmptyReason resolves the scope once and hands the resolved scope to both halves.
func TestBuildResponseWithFilterEmptyReasonCarriesTheReasonOfAnUnresolvedScope(t *testing.T) {
	c := newExistenceClient()
	f := Filters{Time: TimeFilter{RangeDays: 7, CompareDays: 7, EndDate: ptrTime(day(2024, 1, 8))}, Scope: ScopeFilter{Level: "service", IDs: []string{"svc-1"}}}
	resp, err := BuildResponseWithFilterEmptyReason(context.Background(), c, nil, "org-a", f, goldenNow)
	if err != nil || resp.FilterEmptyReason == nil || *resp.FilterEmptyReason != "repository_not_found" {
		t.Fatalf("response reason %v err %v, want repository_not_found", resp, err)
	}
	if c.resolveQueries() != 0 {
		t.Errorf("%d resolve queries for a service scope, want 0", c.resolveQueries())
	}
}

package investmentexplain

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

const (
	wcRepo  = "11111111-2222-4333-8444-555555555555"
	wcIssue = "github:acme/api#1"
	wcPR    = wcRepo + "#pr7"
)

// chainClient answers every lookup of BuildWorkUnitInvestments and records the calls.
// The three chain heads (quotes, repo scopes, repo identities) wait for each other: a
// sequential build never lets the second one start, and the wait times out.
type chainClient struct {
	mu          sync.Mutex
	inFlight    int
	maxInFlight int
	events      []string
	heads       sync.WaitGroup
	headTimeout time.Duration
	timedOut    bool
	failOn      string
	failErr     error
	cancelSeen  map[string]bool
}

func newChainClient() *chainClient {
	c := &chainClient{headTimeout: 2 * time.Second, cancelSeen: map[string]bool{}}
	c.heads.Add(3)
	return c
}

func (c *chainClient) Query(ctx context.Context, query string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	name := ""
	switch {
	case strings.Contains(query, "AS work_unit_count"):
		name = "units"
	case strings.Contains(query, "FROM work_unit_investment_quotes"):
		name = "quotes"
	case strings.Contains(query, "FROM repos FINAL"):
		name = "scopes"
	case strings.Contains(query, "FROM repos"):
		name = "identities"
	case strings.Contains(query, "FROM work_item_team_attributions"):
		name = "assignments"
	case strings.Contains(query, "FROM work_items"):
		name = "issueTitles"
	case strings.Contains(query, "FROM git_pull_requests"):
		name = "prTitles"
	default:
		return &reflectScanner{}, nil
	}
	c.mu.Lock()
	c.inFlight++
	if c.inFlight > c.maxInFlight {
		c.maxInFlight = c.inFlight
	}
	c.events = append(c.events, "start:"+name)
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.inFlight--
		c.events = append(c.events, "end:"+name)
		c.mu.Unlock()
	}()

	if name == "quotes" || name == "scopes" || name == "identities" {
		arrived := make(chan struct{})
		go func() { c.heads.Done(); c.heads.Wait(); close(arrived) }()
		select {
		case <-arrived:
		case <-ctx.Done():
			c.mu.Lock()
			c.cancelSeen[name] = true
			c.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(c.headTimeout):
			c.mu.Lock()
			c.timedOut = true
			c.mu.Unlock()
		}
	}
	if name == c.failOn {
		return nil, c.failErr
	}
	if c.failOn != "" && (name == "quotes" || name == "scopes") {
		select {
		case <-ctx.Done():
			c.mu.Lock()
			c.cancelSeen[name] = true
			c.mu.Unlock()
			return nil, ctx.Err()
		case <-time.After(c.headTimeout):
		}
	}

	switch name {
	case "units":
		structural := `{"issues":["` + wcIssue + `"],"prs":["` + wcPR + `"]}`
		return &reflectScanner{rows: [][]any{{
			"wu-1", "work_unit", "Unit", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
			wcRepo, nil, "churn_loc", 3.0, 1.0,
			[]string{"feature_delivery"}, []float64{1}, []string{"feature_delivery.customer"}, []float64{1},
			structural, 0.8, "high", "ok", "v1", "run-1", time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC),
		}}}, nil
	case "quotes":
		return &reflectScanner{rows: [][]any{{"wu-1", "quote one", "issue", wcIssue, "run-1"}}}, nil
	case "scopes":
		return &reflectScanner{rows: [][]any{{wcRepo, "acme/api"}}}, nil
	case "identities":
		return &reflectScanner{rows: [][]any{{wcRepo, "acme/api", "github"}}}, nil
	case "assignments":
		return &reflectScanner{rows: [][]any{{wcIssue, "team-1", "Team One"}}}, nil
	case "issueTitles":
		return &reflectScanner{rows: [][]any{{wcIssue, "Fix the login bug"}}}, nil
	}
	return &reflectScanner{}, nil
}

func (c *chainClient) indexOf(event string) int {
	for i, e := range c.events {
		if e == event {
			return i
		}
	}
	return -1
}

func buildChain(t *testing.T, client *chainClient) ([]WorkUnitInvestment, error) {
	t.Helper()
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	return reader.BuildWorkUnitInvestments(context.Background(), BuildWorkUnitInvestmentsOptions{
		OrgID: "org-1", StartTS: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), EndTS: time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC),
		Limit: 10, IncludeText: true,
	})
}

func TestBuildWorkUnitInvestmentsRunsTheIndependentLookupsAtOnce(t *testing.T) {
	client := newChainClient()
	units, err := buildChain(t, client)
	if err != nil {
		t.Fatalf("BuildWorkUnitInvestments: %v", err)
	}
	if client.timedOut {
		t.Fatal("quotes, repo scopes and repo identities did not run at the same time")
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1", len(units))
	}
	if client.maxInFlight > workUnitLookupChains {
		t.Errorf("max concurrent statements = %d, want <= %d (the pool bound)", client.maxInFlight, workUnitLookupChains)
	}
	// Each chain keeps its own order: the team assignments need the identities, the titles need the quotes.
	if client.indexOf("end:identities") > client.indexOf("start:assignments") || client.indexOf("start:assignments") < 0 {
		t.Errorf("assignments must start after identities ended: %v", client.events)
	}
	if client.indexOf("end:quotes") > client.indexOf("start:issueTitles") || client.indexOf("start:issueTitles") < 0 {
		t.Errorf("titles must start after quotes ended: %v", client.events)
	}
	// The served rows hold what each lookup found (the same as the sequential build).
	got := units[0].Evidence
	if len(got.Textual) != 1 || got.Textual[0]["source_title"] != "Fix the login bug" {
		t.Errorf("textual evidence = %v, want one quote titled with the issue title", got.Textual)
	}
	wantScope := map[string]any{"type": "repo_scope", "repo_ids": []string{"acme/api"}}
	wantTeam := map[string]any{"type": "team_scope", "team_ids": []string{"team-1"}, "team_names": []string{"Team One"}}
	if len(got.Contextual) != 3 || !reflect.DeepEqual(got.Contextual[1], wantScope) || !reflect.DeepEqual(got.Contextual[2], wantTeam) {
		t.Errorf("contextual evidence = %v, want repo scope acme/api and team Team One", got.Contextual)
	}
}

func TestBuildWorkUnitInvestmentsOneFailedLookupCancelsTheOthers(t *testing.T) {
	client := newChainClient()
	client.failOn, client.failErr = "identities", errors.New("identities down")
	_, err := buildChain(t, client)
	if err == nil || !strings.Contains(err.Error(), "identities down") {
		t.Fatalf("err = %v, want the identities error", err)
	}
	if client.indexOf("start:assignments") >= 0 {
		t.Errorf("assignments must not start after identities failed: %v", client.events)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if !client.cancelSeen["quotes"] || !client.cancelSeen["scopes"] {
		t.Errorf("the quotes and scopes lookups must see their context cancelled: %v", client.cancelSeen)
	}
}

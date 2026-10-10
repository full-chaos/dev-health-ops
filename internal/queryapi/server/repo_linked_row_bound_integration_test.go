//go:build integration

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// D5863: a repository's linked items give one row per item and day, far more than the
// 1,000 rows REST's client allows by default. The read carries its own bound on both
// routes (the same per-statement setting), so REST and GraphQL answer alike; a bound
// that IS hit degrades the four work-item metrics to a stated state and never fails
// the Home document.
func TestRepoLinkedReadAnswersOnTheProductionClientsOfBothRoutes(t *testing.T) {
	instance, err := containers.StartClickHouse(context.Background())
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(context.Background(), t, instance)
	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	dsn := instance.URI
	const org = "repo-linked-row-bound"
	repo := uuid.New()
	seedTeamScopeRepo(t, conn, org, "acme/big", repo)
	const items = 1500 // open items: one item-day row each per day of the 14-day window
	seedLinkedOpenItems(t, conn, org, repo, items)

	restClient, err := dhclickhouse.NewClickHouseQueryClientWithOptions(newUnrestrictedReadClickHouseOptions(dsn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restClient.Close() })
	graphqlClient, err := newQueryRouteClickHouseClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = graphqlClient.Close() })

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(restClient, nil))
	target := "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25&scope_type=repo&scope_id=" + repo.String()
	read := func() map[string]restDelta {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("REST Home = HTTP %d: %s", rec.Code, rec.Body.String())
		}
		return restSummaryDeltas(t, mux, org, target)
	}

	// Under the bound: the same answer on both clients; the metrics are served.
	rest := read()
	if d := rest["wip_saturation"]; d.HasData == nil || !*d.HasData {
		t.Fatalf("REST: wip_saturation has_data %s over %d linked items, want data", flag(d.HasData), items)
	}
	f := home.DefaultFilters()
	f.Scope.Level, f.Scope.IDs = "repo", []string{repo.String()}
	f.Time.EndDate = func() *time.Time { v := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC); return &v }()
	f.Time.RangeDays, f.Time.CompareDays = 7, 7
	gql, err := home.BuildResponse(context.Background(), graphqlClient, nil, org, f, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GraphQL client: %v", err)
	}
	for _, d := range gql.Deltas {
		if d.Metric == "wip_saturation" {
			if !d.HasData || d.RepoLinkState == nil || *d.RepoLinkState != "linked" || rest["wip_saturation"].Value != d.Value {
				t.Errorf("GraphQL wip_saturation = %+v, REST value %v: want the same linked answer", d, rest["wip_saturation"].Value)
			}
		}
	}

	// Past the bound: the document is served, the four metrics say too_large and have no
	// data, never the unfiltered value; the other metrics keep theirs. Both clients.
	previous := home.RepoLinkedMaxResultRows
	home.RepoLinkedMaxResultRows = 100
	t.Cleanup(func() { home.RepoLinkedMaxResultRows = previous })
	rest = read()
	for _, metric := range []string{"cycle_time", "throughput", "wip_saturation", "blocked_work"} {
		if d := rest[metric]; d.HasData == nil || *d.HasData || d.Value != 0 {
			t.Errorf("REST past the bound: %s has_data %s value %v, want no data", metric, flag(d.HasData), d.Value)
		}
	}
	gql, err = home.BuildResponse(context.Background(), graphqlClient, nil, org, f, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("GraphQL client past the bound: %v", err)
	}
	for _, d := range gql.Deltas {
		switch d.Metric {
		case "cycle_time", "throughput", "wip_saturation", "blocked_work":
			if d.HasData || d.Value != 0 || d.RepoLinkState == nil || *d.RepoLinkState != "too_large" {
				t.Errorf("GraphQL past the bound: %+v, want too_large with no data", d)
			}
		}
	}
}

func seedLinkedOpenItems(t *testing.T, conn chdriver.Conn, org string, repo uuid.UUID, n int) {
	t.Helper()
	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (repo_id, work_item_id, provider, status, type, project_id, created_at, started_at, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	links, err := conn.PrepareBatch(ctx, `INSERT INTO work_graph_issue_pr (repo_id, work_item_id, pr_number, confidence, provenance, evidence, last_synced, org_id)`)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("gh:acme/big#%d", i)
		if err := batch.Append(repo, id, "github", "in_progress", "task", "acme/big", created, created.Add(time.Hour), org, created); err != nil {
			t.Fatal(err)
		}
		if err := links.Append(repo, id, uint32(1000+i), float32(1), "native", "", created, org); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	if err := links.Send(); err != nil {
		t.Fatal(err)
	}
}

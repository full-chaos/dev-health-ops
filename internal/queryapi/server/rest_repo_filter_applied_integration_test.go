//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
)

// repoFilterApplied says per metric whether the request's repository filter
// narrowed it (CHAOS-9093, CHAOS-9094): null with no repository named (also for a
// team scope alone); true for every metric when one is: a repository-keyed metric
// through its repo_id, a work-item metric through the items linked to the
// repository's pull requests (this seed holds none, so those say no data). Real
// handler, real ClickHouse.
func TestRESTHomeSaysWhetherTheRepositoryFilterNarrowedEachMetric(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))

	const org = "repo-filter-applied"
	repo := uuid.New()
	seedTeamScopeRepo(t, conn, org, "acme/checkout", repo)
	validFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seedTeamScopeOwnership(t, conn, org, "team-one", "acme/checkout", "exact", "inferred", &repo, validFrom, nil, validFrom)

	// Churn rows in the window: an unfiltered read would serve 105, so a filter that matched nothing
	// and still served it would show.
	other := uuid.New()
	seedTeamScopeRepo(t, conn, org, "acme/other", other)
	for r, churn := range map[uuid.UUID]uint32{repo: 5, other: 100} {
		if err := conn.Exec(context.Background(),
			`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			r, time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), churn, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
			t.Fatal(err)
		}
	}
	if got := read0(t, mux, org, ""); got != 105 {
		t.Fatalf("the unfiltered churn = %v, want 105 (the seed)", got)
	}
	if got := read0(t, mux, org, "&scope_type=repo&scope_id="+url.QueryEscape(repo.String())); got != 5 {
		t.Fatalf("churn of the one repository = %v, want 5", got)
	}

	teamKeyed := map[string]bool{"cycle_time": true, "throughput": true, "wip_saturation": true, "blocked_work": true}
	window := "range_days=7&compare_days=7&end_date=2026-08-25"
	read := func(extra string) map[string]restDelta {
		return restSummaryDeltas(t, mux, org, "/api/v1/home?"+window+extra)
	}
	check := func(name string, deltas map[string]restDelta, repoMetrics, teamMetrics *bool) {
		t.Helper()
		if len(deltas) != 11 {
			t.Fatalf("%s: %d deltas, want 11", name, len(deltas))
		}
		for metric, delta := range deltas {
			want := repoMetrics
			if teamKeyed[metric] {
				want = teamMetrics
			}
			if (delta.RepoFilterApplied == nil) != (want == nil) || (want != nil && *delta.RepoFilterApplied != *want) {
				got := "null"
				if delta.RepoFilterApplied != nil {
					got = map[bool]string{true: "true", false: "false"}[*delta.RepoFilterApplied]
				}
				t.Errorf("%s: %s repo_filter_applied = %s, want %v", name, metric, got, want)
			}
		}
	}
	yes := true

	// No repository named: null on every metric.
	check("no filter", read(""), nil, nil)
	// A team scope alone is not a repository filter: null too.
	check("team scope only", read("&scope_type=team&scope_id=team-one"), nil, nil)
	// A repository that resolves: every metric is narrowed.
	check("repository scope", read("&scope_type=repo&scope_id="+url.QueryEscape(repo.String())), &yes, &yes)
	// Repositories named in what.repos (POST body) behave the same.
	check("what.repos", postDeltas(t, client, org, `{"filters":{"what":{"repos":["`+repo.String()+`"]},"time":{"range_days":7,"compare_days":7,"end_date":"2026-08-25"}}}`), &yes, &yes)
	// Filters narrow, they never widen: a team scope that also names a repository sees the
	// repositories that are both named and owned by the team. team-one owns acme/checkout
	// (churn 5) and not acme/other (churn 100).
	team := func(extraRepo string) string {
		return `{"filters":{"scope":{"level":"team","ids":["team-one"]},"what":{"repos":["` + extraRepo + `"]},"time":{"range_days":7,"compare_days":7,"end_date":"2026-08-25"}}}`
	}
	teamAndOwned := postDeltas(t, client, org, team(repo.String()))
	check("team scope and an owned repository", teamAndOwned, &yes, &yes)
	if d := teamAndOwned["churn"]; d.HasData == nil || !*d.HasData || d.Value != 5 {
		t.Errorf("team scope and an owned repository: churn has_data %s value %v, want 5", flag(d.HasData), d.Value)
	}
	teamAndOther := postDeltas(t, client, org, team(other.String()))
	check("team scope and a repository the team does not own", teamAndOther, &yes, &yes)
	teamAndUnknown := postDeltas(t, client, org, team(uuid.New().String()))
	check("team scope and an unresolved repository", teamAndUnknown, &yes, &yes)
	for name, deltas := range map[string]map[string]restDelta{"not owned": teamAndOther, "unresolved": teamAndUnknown} {
		d := deltas["churn"]
		if d.HasData == nil || *d.HasData || d.Value != 0 {
			t.Errorf("team scope and a repository (%s): churn has_data %s value %v, want no data (the intersection is empty), never the team's 5 or the repository's 100", name, flag(d.HasData), d.Value)
		}
	}
	// A repository that resolves to nothing: the filter was applied and found nothing, so the
	// metrics are narrowed (true) AND have no data; they never serve the unfiltered value.
	unresolved := read("&scope_type=repo&scope_id=" + url.QueryEscape(uuid.New().String()))
	check("unresolved repository", unresolved, &yes, &yes)
	unknownName := read("&scope_type=repo&scope_id=" + url.QueryEscape("acme/nothing"))
	check("unresolved repository name", unknownName, &yes, &yes)
	for _, deltas := range []map[string]restDelta{unresolved, unknownName} {
		for metric, delta := range deltas {
			if delta.HasData == nil || *delta.HasData || delta.Value != 0 {
				t.Errorf("a metric whose repositories resolved to nothing: %s has_data %s value %v, want no data and 0", metric, flag(delta.HasData), delta.Value)
			}
		}
	}
}

func postDeltas(t *testing.T, client home.QueryClient, org, body string) map[string]restDelta {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/home", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(authctx.WithClaims(context.Background(), authctx.Claims{OrgID: org, Role: "owner"}))
	rec := httptest.NewRecorder()
	newHomePostHandler(client, nil)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/home = HTTP %d: %s", rec.Code, rec.Body.String())
	}
	var out struct {
		Deltas []restDelta `json:"deltas"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	byMetric := map[string]restDelta{}
	for _, d := range out.Deltas {
		byMetric[d.Metric] = d
	}
	return byMetric
}

// read0 reads the churn value of a Home request.
func read0(t *testing.T, mux *http.ServeMux, org, extra string) float64 {
	t.Helper()
	return restSummaryDeltas(t, mux, org, "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"+extra)["churn"].Value
}

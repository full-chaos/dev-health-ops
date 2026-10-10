//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/explain"
)

// CHAOS-9101 (D5846): through the real GET /api/v1/explain handler, for the
// ratio class (change_failure_rate) and the merged-weighted class (revert_rate)
// as well as the others: a driver with no stored value on BOTH sides is not
// listed; one with a value on ONE side stays (has_data / has_prior_data say
// which); a contributor, which has a current side only, is listed only with a
// current value.
//
//	repo both     current and prior measured
//	repo current  current measured, no prior row
//	repo prior    current row UNDEFINED (no evidence / no rate), prior measured
//	repo neither  current row undefined, prior undefined
//
// and, in a second organization (the driver list holds 3 places), CHAOS-9121:
//
//	repo gone     NO current row, prior measured: listed like "prior"
func TestRESTExplainDriversNeedAValueOnOneSideForEveryAggregatorClass(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	const org = "explain-drivers-both-sides"
	prior := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	current := prior.AddDate(0, 0, 1)
	computedAt := time.Date(2026, 9, 5, 6, 0, 0, 0, time.UTC)
	repos := map[string]uuid.UUID{"both": uuid.New(), "current": uuid.New(), "prior": uuid.New(), "neither": uuid.New()}
	for name, id := range repos {
		if err := conn.Exec(context.Background(), fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', 'acme/%s', 'github', '%s', now64(3), now64(3))", id, name, org)); err != nil {
			t.Fatal(err)
		}
	}
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	measured := changefailure.Counts{Deployments: 10, FailedHeuristic: 2, IncidentsDirect: 1}
	unknown := changefailure.Counts{Deployments: 4} // deployments, no incident evidence: the rate is undefined
	if _, err := writer.WriteChangeFailure(context.Background(), []repouser.ChangeFailureDaily{
		{RepoID: repos["both"], Day: prior, Counts: measured, ComputedAt: computedAt},
		{RepoID: repos["both"], Day: current, Counts: measured, ComputedAt: computedAt},
		{RepoID: repos["current"], Day: current, Counts: measured, ComputedAt: computedAt},
		{RepoID: repos["prior"], Day: prior, Counts: measured, ComputedAt: computedAt},
		{RepoID: repos["prior"], Day: current, Counts: unknown, ComputedAt: computedAt},
		{RepoID: repos["neither"], Day: prior, Counts: unknown, ComputedAt: computedAt},
		{RepoID: repos["neither"], Day: current, Counts: unknown, ComputedAt: computedAt},
	}, org); err != nil {
		t.Fatal(err)
	}
	rate := func(v float64) *float64 { return &v }
	if _, _, _, err := writer.WriteResult(context.Background(), repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: repos["both"], Day: prior, PRsMerged: 4, RevertRate: rate(0.25), ComputedAt: computedAt},
		{RepoID: repos["both"], Day: current, PRsMerged: 5, RevertRate: rate(0.2), ComputedAt: computedAt},
		{RepoID: repos["current"], Day: current, PRsMerged: 5, RevertRate: rate(0.2), ComputedAt: computedAt},
		{RepoID: repos["prior"], Day: prior, PRsMerged: 4, RevertRate: rate(0.25), ComputedAt: computedAt},
		{RepoID: repos["prior"], Day: current, PRsMerged: 5, ComputedAt: computedAt},
		{RepoID: repos["neither"], Day: prior, PRsMerged: 4, ComputedAt: computedAt},
		{RepoID: repos["neither"], Day: current, PRsMerged: 5, ComputedAt: computedAt},
	}}, org); err != nil {
		t.Fatal(err)
	}

	// CHAOS-9121: a group with a prior row and no current row.
	const orgGone = "explain-drivers-prior-no-current-row"
	reposGone := map[string]uuid.UUID{"both": uuid.New(), "gone": uuid.New(), "neither": uuid.New()}
	for name, id := range reposGone {
		if err := conn.Exec(context.Background(), fmt.Sprintf(
			"INSERT INTO repos (id, repo, provider, org_id, created_at, last_synced) VALUES ('%s', 'acme/%s', 'github', '%s', now64(3), now64(3))", id, name, orgGone)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.WriteChangeFailure(context.Background(), []repouser.ChangeFailureDaily{
		{RepoID: reposGone["both"], Day: prior, Counts: measured, ComputedAt: computedAt},
		{RepoID: reposGone["both"], Day: current, Counts: measured, ComputedAt: computedAt},
		{RepoID: reposGone["gone"], Day: prior, Counts: measured, ComputedAt: computedAt},
		{RepoID: reposGone["neither"], Day: prior, Counts: unknown, ComputedAt: computedAt},
		{RepoID: reposGone["neither"], Day: current, Counts: unknown, ComputedAt: computedAt},
	}, orgGone); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := writer.WriteResult(context.Background(), repouser.Result{RepoMetrics: []repouser.RepoMetric{
		{RepoID: reposGone["both"], Day: prior, PRsMerged: 4, RevertRate: rate(0.25), ComputedAt: computedAt},
		{RepoID: reposGone["both"], Day: current, PRsMerged: 5, RevertRate: rate(0.2), ComputedAt: computedAt},
		{RepoID: reposGone["gone"], Day: prior, PRsMerged: 4, RevertRate: rate(0.25), ComputedAt: computedAt},
		{RepoID: reposGone["neither"], Day: prior, PRsMerged: 4, ComputedAt: computedAt},
		{RepoID: reposGone["neither"], Day: current, PRsMerged: 5, ComputedAt: computedAt},
	}}, orgGone); err != nil {
		t.Fatal(err)
	}
	reader, err := explain.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	handler := newExplainGetHandler(reader)
	list := func(org string, repos map[string]uuid.UUID, metric, group string) string {
		t.Helper()
		target := fmt.Sprintf("/api/v1/explain?metric=%s&range_days=1&compare_days=1&end_date=%s", metric, current.Format("2006-01-02"))
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s = HTTP %d: %s", target, rec.Code, rec.Body.String())
		}
		var raw map[string]json.RawMessage
		if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		var items []struct {
			ID           string `json:"id"`
			HasData      bool   `json:"has_data"`
			HasPriorData bool   `json:"has_prior_data"`
		}
		if err := json.Unmarshal(raw[group], &items); err != nil {
			t.Fatal(err)
		}
		names := map[string]string{}
		for name, id := range repos {
			names[id.String()] = name
		}
		var out []string
		for _, item := range items {
			out = append(out, fmt.Sprintf("%s(data=%t,prior=%t)", names[item.ID], item.HasData, item.HasPriorData))
		}
		sort.Strings(out)
		return strings.Join(out, " ")
	}
	for _, metric := range []string{"change_failure_rate", "revert_rate"} {
		wantDrivers := "both(data=true,prior=true) current(data=true,prior=false) prior(data=false,prior=true)"
		if got := list(org, repos, metric, "drivers"); got != wantDrivers {
			t.Errorf("%s drivers = %s\n  want %s", metric, got, wantDrivers)
		}
		wantGone := "both(data=true,prior=true) gone(data=false,prior=true)"
		if got := list(orgGone, reposGone, metric, "drivers"); got != wantGone {
			t.Errorf("%s drivers (a group with a prior row and no current row) = %s\n  want %s", metric, got, wantGone)
		}
		wantContributors := "both(data=true,prior=false) current(data=true,prior=false)"
		if got := list(org, repos, metric, "contributors"); got != wantContributors {
			t.Errorf("%s contributors = %s\n  want %s", metric, got, wantContributors)
		}
	}
}

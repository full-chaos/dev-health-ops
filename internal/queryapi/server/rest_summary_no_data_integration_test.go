//go:build integration

// The REST summary deltas (Home, person summary) through the production
// handlers against a real ClickHouse migrated by the real chain (CHAOS-9044).
// One organization per case; rows come from the real writers or the table's own
// columns. Each delta must tell a window with no stored value from a measured
// zero, and serve no delta (0) when either window has none: the one rule of
// internal/queryapi/deltarule, the same contract GraphQL Home, /explain and the
// operating review serve.
package server

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily/repouser"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
)

type restDelta struct {
	Metric       string  `json:"metric"`
	Value        float64 `json:"value"`
	DeltaPct     float64 `json:"delta_pct"`
	HasData      *bool   `json:"has_data"`
	HasPriorData *bool   `json:"has_prior_data"`
	RateState    *string `json:"rate_state"`
}

func restSummaryDeltas(t *testing.T, mux *http.ServeMux, org, target string) map[string]restDelta {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = HTTP %d: %s", target, rec.Code, rec.Body.String())
	}
	var body struct {
		Deltas []restDelta `json:"deltas"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %s: %v", target, err)
	}
	out := map[string]restDelta{}
	for _, delta := range body.Deltas {
		out[delta.Metric] = delta
	}
	return out
}

func flag(p *bool) string {
	if p == nil {
		return "<absent>"
	}
	return fmt.Sprint(*p)
}

// cell is one case: the values of the current and prior window (nil = no row).
type restCell struct {
	name           string
	current, prior *float64
	hasData        bool
	hasPriorData   bool
	value          float64
	deltaPct       float64
}

func restCells() []restCell {
	f := func(v float64) *float64 { return &v }
	return []restCell{
		{"no row in either window", nil, nil, false, false, 0, 0},
		{"prior window only", nil, f(10), false, true, 0, 0},
		{"current window only", f(5), nil, true, false, 5, 0},
		{"a measured zero in both windows", f(0), f(0), true, true, 0, 0},
		{"a measured zero now, a value before", f(0), f(10), true, true, 0, -100},
		{"two measured values", f(5), f(10), true, true, 5, -50},
	}
}

func TestRESTHomeDeltasTellNoDataFromAMeasuredZero(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	handler := newHomeGetHandler(client, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", handler)

	// The home window of the request: 7 days ending 2026-08-25, compared with
	// the 7 days before. A churn row (repo_metrics_daily.total_loc_touched) of
	// 2026-08-22 is in the current window, one of 2026-08-14 in the prior one.
	const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	for index, cell := range restCells() {
		org := fmt.Sprintf("rest-home-%d", index)
		repo := uuid.New()
		for day, v := range map[time.Time]*float64{current: cell.current, prior: cell.prior} {
			if v == nil {
				continue
			}
			if err := conn.Exec(context.Background(),
				`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, uint32(*v), time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
				t.Fatal(err)
			}
		}
		delta, ok := restSummaryDeltas(t, mux, org, target)["churn"]
		if !ok {
			t.Fatalf("%s: the response has no churn delta", cell.name)
		}
		if delta.HasData == nil || delta.HasPriorData == nil || *delta.HasData != cell.hasData || *delta.HasPriorData != cell.hasPriorData {
			t.Errorf("%s: has_data %s has_prior_data %s, want %v %v", cell.name, flag(delta.HasData), flag(delta.HasPriorData), cell.hasData, cell.hasPriorData)
		}
		if delta.Value != cell.value || delta.DeltaPct != cell.deltaPct {
			t.Errorf("%s: value %v delta_pct %v, want %v %v", cell.name, delta.Value, delta.DeltaPct, cell.value, cell.deltaPct)
		}
	}
}

// Change failure rate on REST Home: a measured prior and an unknown current
// window used to read as the same body as a measured zero. It now carries the
// state and no delta.
func TestRESTHomeChangeFailureRateCarriesItsState(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	writer, err := repouser.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	computedAt := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	measured := changefailure.Counts{Deployments: 10, FailedHeuristic: 5, IncidentsDirect: 1}
	for _, tc := range []struct {
		org       string
		current   *changefailure.Counts
		state     string // "" = null
		hasData   bool
		value     float64
		deltaPct  float64
		wantState bool
	}{
		{"rest-cfr-unknown", &changefailure.Counts{Deployments: 4}, "unknown_no_incident_evidence", false, 0, 0, true},
		{"rest-cfr-not-applicable", &changefailure.Counts{IncidentsDirect: 1}, "not_applicable_no_deployments", false, 0, 0, true},
		{"rest-cfr-none", nil, "", false, 0, 0, false},
		{"rest-cfr-measured-zero", &changefailure.Counts{Deployments: 4, IncidentsDirect: 1}, "measured", true, 0, -100, true},
		{"rest-cfr-measured", &changefailure.Counts{Deployments: 4, FailedHeuristic: 1, IncidentsDirect: 1}, "measured", true, 25, -50, true},
	} {
		repo := uuid.New()
		rows := []repouser.ChangeFailureDaily{{RepoID: repo, Day: prior, Counts: measured, ComputedAt: computedAt}}
		if tc.current != nil {
			rows = append(rows, repouser.ChangeFailureDaily{RepoID: repo, Day: current, Counts: *tc.current, ComputedAt: computedAt})
		}
		if _, err := writer.WriteChangeFailure(context.Background(), rows, tc.org); err != nil {
			t.Fatal(err)
		}
		delta, ok := restSummaryDeltas(t, mux, tc.org, target)["change_failure_rate"]
		if !ok {
			t.Fatalf("%s: no change_failure_rate delta", tc.org)
		}
		state := ""
		if delta.RateState != nil {
			state = *delta.RateState
		}
		if delta.HasData == nil || *delta.HasData != tc.hasData || state != tc.state || delta.Value != tc.value || delta.DeltaPct != tc.deltaPct {
			t.Errorf("%s: has_data %s rate_state %q value %v delta_pct %v, want %v %q %v %v", tc.org, flag(delta.HasData), state, delta.Value, delta.DeltaPct, tc.hasData, tc.state, tc.value, tc.deltaPct)
		}
	}
}

func TestRESTPersonSummaryDeltasTellNoDataFromAMeasuredZero(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	conn, client := startTeamScopeClickHouse(t)
	reader, err := people.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/people/{person_id}/summary", newPeopleSummaryHandler(reader))

	now := time.Now().UTC()
	day := func(daysAgo int) time.Time {
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysAgo)
	}
	const identity = "alice@example.com"
	personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
	insert := func(conn chdriver.Conn, org string, repo uuid.UUID, at time.Time, locTouched uint32) {
		t.Helper()
		if err := conn.Exec(context.Background(),
			`INSERT INTO user_metrics_daily (repo_id, day, identity_id, author_email, loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			repo, at, identity, identity, locTouched, now, org); err != nil {
			t.Fatal(err)
		}
	}
	for index, cell := range restCells() {
		org := fmt.Sprintf("rest-person-%d", index)
		repo := uuid.New()
		// The person exists through a row long before both windows (14 + 14 days).
		insert(conn, org, repo, day(90), 1)
		if cell.current != nil {
			insert(conn, org, repo, day(3), uint32(*cell.current))
		}
		if cell.prior != nil {
			insert(conn, org, repo, day(20), uint32(*cell.prior))
		}
		delta, ok := restSummaryDeltas(t, mux, org, "/api/v1/people/"+personID+"/summary?range_days=14&compare_days=14")["churn"]
		if !ok {
			t.Fatalf("%s: the response has no churn delta", cell.name)
		}
		if delta.HasData == nil || delta.HasPriorData == nil || *delta.HasData != cell.hasData || *delta.HasPriorData != cell.hasPriorData {
			t.Errorf("%s: has_data %s has_prior_data %s, want %v %v", cell.name, flag(delta.HasData), flag(delta.HasPriorData), cell.hasData, cell.hasPriorData)
		}
		if delta.Value != cell.value || delta.DeltaPct != cell.deltaPct {
			t.Errorf("%s: value %v delta_pct %v, want %v %v", cell.name, delta.Value, delta.DeltaPct, cell.value, cell.deltaPct)
		}
	}
}

// The strip the venue oracle applies (withoutHomeDeltaGoOnlyFields) must hold on
// the body the real handler writes: every delta ends with the three keys, and
// what is left is the frozen shape in the writer's own key order.
func TestRESTHomeBodyStripsToTheFrozenShape(t *testing.T) {
	_, client := startTeamScopeClickHouse(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25", nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: "rest-home-strip", Role: "owner"}))
	rec := httptest.NewRecorder()
	newHomeGetHandler(client, nil)(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("HTTP %d: %s", rec.Code, rec.Body.String())
	}
	stripped, err := withoutHomeDeltaGoOnlyFields(rec.Body.String())
	if err != nil {
		t.Fatalf("the real Home body does not strip: %v\n%s", err, rec.Body.String())
	}
	for _, key := range homeDeltaGoOnlyKeys {
		if strings.Contains(stripped, `"`+key+`"`) {
			t.Errorf("%s is still in the stripped body", key)
		}
	}
	if !strings.Contains(stripped, `"constraint":{"title":"","claim":"","evidence":[],"experiments":[]}`) {
		t.Errorf("the stripped body does not keep the writer's key order for the constraint card: %s", stripped)
	}
}

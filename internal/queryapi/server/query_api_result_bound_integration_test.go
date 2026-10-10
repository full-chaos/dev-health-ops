//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/chclient"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/investment"
)

// CHAOS-9126, D5879: a REST read of more than 1,000 rows is served (the shared
// bound is /query's 500,000), and a read that hits a bound answers a NAMED cause,
// not the anonymous "Data unavailable" of a store that is down. Real handlers on
// a real ClickHouse; the client is built through the one constructor path.
func TestRESTReadsOfMoreThanAThousandRowsAreServed(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	ctx := context.Background()
	const org = "bound-9126"
	day := time.Now().UTC().AddDate(0, 0, -1).Format("2006-01-02")
	exec := func(q string, a ...any) {
		t.Helper()
		if err := conn.Exec(ctx, q, a...); err != nil {
			t.Fatal(err)
		}
	}
	const n = 1001
	exec(`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
SELECT generateUUIDv4(), concat('acme/repo-', toString(number)), now(), now(), ?, 'github' FROM numbers(?)`, org, uint64(n))
	exec(`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, prs_merged, computed_at, org_id)
SELECT id, toDate(?), 10, 2, now(), org_id FROM repos WHERE org_id = ?`, day, org)
	exec(`INSERT INTO user_metrics_daily (repo_id, day, author_email, computed_at, org_id)
SELECT id, toDate(?), concat('dev', toString(rowNumberInAllBlocks()), '@example.com'), now(), org_id FROM repos WHERE org_id = ?`, day, org)
	exec(`INSERT INTO work_unit_investments
(work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json,
 categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, work_unit_type, work_unit_name, org_id)
SELECT concat('wu-', toString(number)), now() - INTERVAL 3 DAY, now() - INTERVAL 1 DAY, NULL, NULL, 'fte_days', 1.0,
 map('feature_delivery', 1.0), map('feature_delivery.roadmap', 1.0), '{}', 0.8, 'high', 'ok', '', 'v1', 'hash', 'run-1', now(), 'issue', 'unit', ?
FROM numbers(?)`, org, uint64(n))

	serve := func(h http.HandlerFunc, target string) (int, string) {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code, rec.Body.String()
	}

	t.Run("filters/options: 1,001 repositories and developers", func(t *testing.T) {
		code, body := serve(newFilterOptionsWorkHandler(client), "/api/v1/filters/options")
		var out struct {
			Repos      []string `json:"repos"`
			Developers []string `json:"developers"`
		}
		if code != 200 || json.Unmarshal([]byte(body), &out) != nil || len(out.Repos) != n || len(out.Developers) != n {
			t.Fatalf("HTTP %d, %d repos, %d developers; want 200 with %d each (body %.120s)", code, len(out.Repos), len(out.Developers), n, body)
		}
	})
	t.Run("quadrant: 1,001 repositories", func(t *testing.T) {
		code, body := serve(newQuadrantWorkHandler(client), "/api/v1/quadrant?type=churn_throughput&scope_type=org&range_days=7")
		var out struct {
			Points []map[string]any `json:"points"`
		}
		if code != 200 || json.Unmarshal([]byte(body), &out) != nil || len(out.Points) != n {
			t.Fatalf("HTTP %d, %d points; want 200 with %d (body %.120s)", code, len(out.Points), n, body)
		}
	})
	t.Run("investment: 1,001 work units", func(t *testing.T) {
		reader, err := investment.NewReader(client)
		if err != nil {
			t.Fatal(err)
		}
		code, body := serve(newInvestmentGetHandler(reader), "/api/v1/investment?range_days=7")
		if code != 200 || !strings.Contains(body, "feature_delivery") {
			t.Fatalf("HTTP %d; want 200 with the unit's theme (body %.200s)", code, body)
		}
	})

	t.Run("a bound hit names its cause; a store that is down does not", func(t *testing.T) {
		const org = "bound-named-9126"
		if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
SELECT generateUUIDv4(), concat('acme/repo-', toString(number)), now(), now(), ?, 'github' FROM numbers(50)`, org); err != nil {
			t.Fatal(err)
		}
		dsn := teamScopeDSN
		opts := chclient.Options(dsn)
		rows := uint(10)
		opts.MaxResultRows = &rows
		tight, err := dhclickhouse.NewClickHouseQueryClientWithOptions(opts)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tight.Close() })

		var logs bytes.Buffer
		log.SetOutput(&logs)
		t.Cleanup(func() { log.SetOutput(nil) })
		req := httptest.NewRequest(http.MethodGet, "/api/v1/filters/options?scope_id=a&scope_id=b", nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec := httptest.NewRecorder()
		newFilterOptionsWorkHandler(tight)(rec, req)
		if rec.Code != 503 || !strings.Contains(rec.Body.String(), "Result too large") {
			t.Fatalf("bound hit: HTTP %d %s, want 503 with the named cause", rec.Code, rec.Body.String())
		}
		if l := logs.String(); !strings.Contains(l, "WARN filteroptions: read hit the result_rows bound") || !strings.Contains(l, "scope_ids=2") || strings.Contains(l, "acme/repo") {
			t.Fatalf("WARN line = %q, want operation, bound, scope id count, no values", l)
		}

		if err := tight.Close(); err != nil {
			t.Fatal(err)
		}
		req = httptest.NewRequest(http.MethodGet, "/api/v1/filters/options", nil)
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
		rec = httptest.NewRecorder()
		newFilterOptionsWorkHandler(tight)(rec, req)
		if rec.Code != 503 || !strings.Contains(rec.Body.String(), "Data unavailable") {
			t.Fatalf("store down: HTTP %d %s, want 503 Data unavailable", rec.Code, rec.Body.String())
		}
	})
}

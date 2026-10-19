//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

// TestOperatingReviewInvestmentIsIssueSizeOnlyForADocumentThatAsksForItsSizing pins CHAOS-9873 (D6027, D6032)
// against a real ClickHouse, through the generated schema: the current registered text selects the section's
// measure and whole sizing and reads the investment section in ISSUE_SIZE with its coverage; the previous text
// (V5) cannot label an issue-size value and reads the same units in work units.
//
// Four units end in the week: feature_delivery sized 2 and unsized, maintenance sized 5 and unsized.
// ISSUE_SIZE: feature_delivery 2+1 = 3, maintenance 5+1 = 6; WORK_UNITS: 2 and 2. Coverage 2 of 4.
func TestOperatingReviewInvestmentIsIssueSizeOnlyForADocumentThatAsksForItsSizing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, client := crossorg.Start(ctx, t)
	const org = "operating-review-issue-size-org"
	const emptyOrg = "operating-review-issue-size-empty-org"
	week := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	computed := week.AddDate(0, 0, 8)
	for i, u := range []struct {
		subcategory string
		size        float64
	}{
		{"feature_delivery.roadmap", 2}, {"feature_delivery.roadmap", 0},
		{"maintenance.debt", 5}, {"maintenance.debt", 0},
	} {
		id := org + "-wu-" + string(rune('a'+i))
		theme := u.subcategory[:indexDot(u.subcategory)]
		crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_investments (work_unit_id, from_ts, to_ts, repo_id, provider, effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json, structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status, categorization_errors_json, categorization_model_version, categorization_input_hash, categorization_run_id, computed_at, org_id)
			VALUES (?, ?, ?, ?, 'jira', 'churn_loc', 4000, map(?, 1.0), map(?, 1.0), '{}', 0.7, 'moderate', 'ok', '{}', 'v1', 'hash', 'run-1', ?, ?)`,
			id, week.AddDate(0, 0, 1), week.AddDate(0, 0, 3), uuid.New(), theme, u.subcategory, computed, org)
		if u.size > 0 {
			crossorg.Exec(ctx, t, admin, `INSERT INTO work_unit_issue_size (org_id, work_unit_id, issue_size, sized_issue_count, issue_count, categorization_run_id, computed_at) VALUES (?, ?, ?, 1, 1, 'run-1', ?)`,
				org, id, u.size, computed)
		}
	}

	server := handler.NewDefaultServer(graph.NewExecutableSchema(graph.Config{
		Resolvers: &graph.Resolver{ClickHouse: client, Postgres: noSyncRunPostgres{}},
	}))
	run := func(t *testing.T, org, document string) map[string]any {
		t.Helper()
		body, err := json.Marshal(map[string]any{"query": document, "variables": map[string]any{
			"orgId": org, "input": map[string]any{"weekStart": "2026-08-24"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org}))
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, req)
		var out struct {
			Data   map[string]any `json:"data"`
			Errors []any          `json:"errors"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode %s: %v", rec.Body.String(), err)
		}
		if len(out.Errors) != 0 || out.Data == nil {
			t.Fatalf("the document did not run: %s", rec.Body.String())
		}
		return out.Data
	}
	investment := func(data map[string]any) (section, metrics map[string]any) {
		review, _ := data["operatingReview"].(map[string]any)
		sections, _ := review["sections"].([]any)
		metrics = map[string]any{}
		for _, item := range sections {
			if s, _ := item.(map[string]any); s["key"] == "investment" {
				section = s
				for _, m := range s["metrics"].([]any) {
					metric := m.(map[string]any)
					metrics[metric["key"].(string)] = metric
				}
			}
		}
		if section == nil {
			t.Fatalf("no investment section: %v", data)
		}
		return section, metrics
	}

	t.Run("current text asks for the sizing: ISSUE_SIZE with its coverage", func(t *testing.T) {
		section, metrics := investment(run(t, org, registeredOperatingReviewDocument))
		for key, want := range map[string]float64{"feature_delivery_work": 3, "maintenance_work": 6, "quality_work": 0} {
			metric := metrics[key].(map[string]any)
			if metric["value"] != want || metric["unit"] != "issue size" {
				t.Errorf("%s = %v %v, want %v issue size", key, metric["value"], metric["unit"], want)
			}
		}
		sizing, _ := section["sizing"].(map[string]any)
		if sizing["measure"] != "ISSUE_SIZE" || sizing["sizingCoverage"] != 0.5 || sizing["unsizedUnitCount"] != float64(2) || sizing["workUnitCount"] != float64(4) {
			t.Errorf("sizing = %v, want ISSUE_SIZE 0.5 coverage, 2 unsized of 4", sizing)
		}
	})
	t.Run("previous text cannot label an issue size: work units, no sizing", func(t *testing.T) {
		section, metrics := investment(run(t, org, registeredOperatingReviewV5Document))
		for key, want := range map[string]float64{"feature_delivery_work": 2, "maintenance_work": 2} {
			metric := metrics[key].(map[string]any)
			if metric["value"] != want || metric["unit"] != "work units" {
				t.Errorf("%s = %v %v, want %v work units", key, metric["value"], metric["unit"], want)
			}
		}
		if _, asked := section["sizing"]; asked {
			t.Errorf("the V5 text got a sizing it did not select: %v", section["sizing"])
		}
	})
	t.Run("a week with no unit is not measured, never 0", func(t *testing.T) {
		section, _ := investment(run(t, emptyOrg, registeredOperatingReviewDocument))
		sizing, _ := section["sizing"].(map[string]any)
		if sizing["measure"] != "ISSUE_SIZE" || sizing["sizingCoverage"] != nil || sizing["unsizedUnitCount"] != nil || sizing["workUnitCount"] != nil {
			t.Errorf("sizing = %v, want ISSUE_SIZE with null sizing fields", sizing)
		}
	})
}

func indexDot(s string) int {
	for i := range s {
		if s[i] == '.' {
			return i
		}
	}
	return len(s)
}

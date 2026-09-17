//go:build integration

// POST /api/v1/work-units/{work_unit_id}/explain's own seeded-real-
// ClickHouse guard for the one thing a fake client cannot demonstrate:
// that a team-scoped request REFUSES a work unit outside that team.
//
// This route shipped a defect where it resolved repo-level scope ids and
// then applied no team-level narrowing at all. Every unit test drove the
// handler through a zero-rows client, where a missing narrowing predicate
// changes neither status nor body, so the defect was invisible until the
// emitted SQL was inspected. Against a REAL engine holding two units in
// the same org -- one inside the team's repositories and one outside --
// the narrowing becomes observable as the thing a caller actually sees:
// 404 versus 200 for the identical work_unit_id.
//
// Org isolation was never the exposure here; this is intra-org, between
// two teams of the SAME org, which is exactly why the surrounding
// org_id predicates could all be correct while the answer was still
// wrong.
//
// Reuses startSeededWorkUnitsClickHouse and workUnitsSeededOrgID from
// this package's own work-units seeded file: the same DDL, with the same
// prod-confirmed engines and sorting keys, backs both routes because both
// read through the same assembled function.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
)

const (
	workUnitExplainMemberRepoID = "11111111-1111-1111-1111-111111111111"
	workUnitExplainOtherRepoID  = "22222222-2222-2222-2222-222222222222"
	workUnitExplainMemberUnit   = "wu-inside-the-team"
	workUnitExplainOutsideUnit  = "wu-outside-the-team"
)

// seedWorkUnitExplainTeamFixture puts two work units in ONE org: one in a
// repository team-1 owns (a team_repo_ownership row, which is what
// establishes a team's repositories) and one in a repository no team owns.
func seedWorkUnitExplainTeamFixture(t *testing.T, conn interface {
	Exec(ctx context.Context, query string, args ...any) error
}, fromTS, toTS time.Time) {
	t.Helper()
	ctx := context.Background()
	const computedAt = "2026-01-06 00:00:00"

	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider)
		VALUES ('%s', 'acme/member-repo', toDateTime64('%s',3), toDateTime64('%s',3), '%s', 'github')`,
		workUnitExplainMemberRepoID, computedAt, computedAt, workUnitsSeededOrgID,
	)); err != nil {
		t.Fatalf("seed repos: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf(
		`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type,
			source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES ('%s', 'github', 'team-1', toUUID('%s'), 'acme/member-repo', 'exact', 'inferred',
			0, 0, 0, toDateTime64('%s',3), NULL, toDateTime64('%s',3))`,
		workUnitsSeededOrgID, workUnitExplainMemberRepoID, computedAt, computedAt,
	)); err != nil {
		t.Fatalf("seed team_repo_ownership: %v", err)
	}

	for _, seed := range []struct{ unitID, repoID string }{
		{workUnitExplainMemberUnit, workUnitExplainMemberRepoID},
		{workUnitExplainOutsideUnit, workUnitExplainOtherRepoID},
	} {
		stmt := fmt.Sprintf(
			`INSERT INTO work_unit_investments
				(work_unit_id, work_unit_type, work_unit_name, from_ts, to_ts, repo_id, provider,
				 effort_metric, effort_value, theme_distribution_json, subcategory_distribution_json,
				 structural_evidence_json, evidence_quality, evidence_quality_band, categorization_status,
				 categorization_errors_json, categorization_model_version, categorization_input_hash,
				 categorization_run_id, computed_at, org_id)
			VALUES
				('%s', 'issue', 'Team scope fixture', toDateTime64('%s',3), toDateTime64('%s',3),
				 '%s', 'github', 'churn_loc', 1.0, map('feature_delivery', 1.0), map('feature_delivery.customer', 1.0), '{}',
				 0.5, 'moderate', 'ok', '', 'v1', 'hash', '', toDateTime64('%s',3), '%s')`,
			seed.unitID, fromTS.Format("2006-01-02 15:04:05"), toTS.Format("2006-01-02 15:04:05"),
			seed.repoID, computedAt, workUnitsSeededOrgID,
		)
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("seed work_unit_investments (%s): %v", seed.unitID, err)
		}
	}
}

// TestWorkUnitExplainSeededRealClickHouse_TeamScopeRefusesAUnitOutsideTheTeam
// is the recurrence guard for the dropped team scope. All three cases
// below ran against the SAME seeded rows, so the only thing that differs
// between the 404 and the 200 is the scope the request declared.
func TestWorkUnitExplainSeededRealClickHouse_TeamScopeRefusesAUnitOutsideTheTeam(t *testing.T) {
	t.Setenv("LLM_PROVIDER", "mock")

	conn, reader, cleanup := startSeededWorkUnitsClickHouse(t)
	defer cleanup()

	fromTS := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	toTS := time.Date(2024, 1, 20, 0, 0, 0, 0, time.UTC)
	seedWorkUnitExplainTeamFixture(t, conn, fromTS, toTS)

	// The seeded window is historical, so every request names it
	// explicitly rather than relying on the endpoint's own range_days
	// default.
	window := "start_date=2024-01-15&end_date=2024-01-21&llm_provider=mock"
	handler := newWorkUnitExplainHandler(reader, nil, nil)

	cases := []struct {
		name       string
		workUnitID string
		query      string
		wantStatus int
	}{
		{
			// The defect: this answered 200 with an explanation for a unit
			// the team does not own.
			name:       "team scope refuses a unit outside the team",
			workUnitID: workUnitExplainOutsideUnit,
			query:      window + "&scope_type=team&scope_id=team-1",
			wantStatus: http.StatusNotFound,
		},
		{
			// The same unit, same rows, org scope: found. This is what
			// proves the 404 above comes from the team narrowing and not
			// from the unit being absent or the window excluding it.
			name:       "org scope finds the same unit",
			workUnitID: workUnitExplainOutsideUnit,
			query:      window,
			wantStatus: http.StatusOK,
		},
		{
			// And the team narrowing is not simply refusing everything: a
			// unit the team DOES own is still explained under team scope.
			name:       "team scope still explains a unit inside the team",
			workUnitID: workUnitExplainMemberUnit,
			query:      window + "&scope_type=team&scope_id=team-1",
			wantStatus: http.StatusOK,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost,
				"/api/v1/work-units/"+testCase.workUnitID+"/explain?"+testCase.query, nil)
			req.SetPathValue("work_unit_id", testCase.workUnitID)
			req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: workUnitsSeededOrgID}))

			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != testCase.wantStatus {
				t.Fatalf("status = %d, want %d\nbody=%s", rec.Code, testCase.wantStatus, rec.Body.String())
			}
			if testCase.wantStatus != http.StatusOK {
				return
			}
			var explanation struct {
				WorkUnitID  string `json:"work_unit_id"`
				AIGenerated bool   `json:"ai_generated"`
				Summary     string `json:"summary"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &explanation); err != nil {
				t.Fatalf("decode 200 body: %v\nbody=%s", err, rec.Body.String())
			}
			if explanation.WorkUnitID != testCase.workUnitID {
				t.Errorf("work_unit_id = %q, want %q", explanation.WorkUnitID, testCase.workUnitID)
			}
			if !explanation.AIGenerated || explanation.Summary == "" {
				t.Errorf("a 200 carried no explanation: ai_generated=%v summary=%q",
					explanation.AIGenerated, explanation.Summary)
			}
		})
	}
}

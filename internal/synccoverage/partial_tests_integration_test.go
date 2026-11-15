//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	partialCapResult  = `{"reports_complete":false,"incomplete":[{"component":"run_reports","cause":"per_run_cap","count":1}]}`
	partialSkipResult = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"artifact_oversized","count":1}],"skipped_artifacts":[{"run_id":"1","artifact_id":"2","cause":"artifact_oversized"}]}`
	completeResult    = `{"reports_complete":true,"incomplete":[]}`
	gitlabResult      = `{"coverage_complete":false,"inventory_truncated":true}`
)

type partialUnit struct {
	dataset, provider, flags, status, result string
	since, before                            time.Duration
}

func TestPartialTestsUnitCoverage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	now := time.Date(2026, time.August, 12, 8, 0, 0, 0, time.UTC)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	// units are inserted in order, so a later unit has a later created_at.
	unit := func(dataset, status, result string, since, before time.Duration) partialUnit {
		return partialUnit{dataset: dataset, provider: "github", flags: "{}", status: status, result: result, since: since, before: before}
	}
	// The result the real GitLab producer writes for a skipped report, and for a complete one.
	realGitLabSkipped := realGitLabResult(t, realGitLabSkippedBody())
	realGitLabComplete := realGitLabResult(t, realGitLabReport(3))
	cases := []struct {
		name    string
		targets string
		units   []partialUnit
		check   func(t *testing.T, tests, overall map[string]any)
	}{
		{"partial per_run_cap is a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantNoLatest(t, overall)
				if overall["health"] != "gaps" {
					t.Fatalf("overall = %#v", overall)
				}
			}},
		{"partial skipped artifact is a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", partialSkipResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 1); wantCovered(t, tests, 0) }},
		{"complete control is healthy", `["tests"]`,
			[]partialUnit{unit("tests", "success", completeResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				if overall["latest_successful_run_at"] == nil {
					t.Fatalf("complete unit must set latest_successful_run_at: %#v", overall)
				}
			}},
		{"legacy unit without the key is healthy", `["tests"]`,
			[]partialUnit{unit("tests", "success", `{}`, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 0); wantCovered(t, tests, 1) }},
		{"gitlab-shaped unit without the key is unchanged", `["tests"]`,
			[]partialUnit{{dataset: "tests", provider: "gitlab", flags: "{}", status: "success", result: gitlabResult, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 0); wantCovered(t, tests, 1) }},
		{"gitlab tests unit with reports_complete=false is a gap (real producer result)", `["tests"]`,
			[]partialUnit{{dataset: "tests", provider: "gitlab", flags: "{}", status: "success", result: realGitLabSkipped, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantNoLatest(t, overall)
			}},
		{"gitlab tests unit complete (real producer result) is healthy", `["tests"]`,
			[]partialUnit{{dataset: "tests", provider: "gitlab", flags: "{}", status: "success", result: realGitLabComplete, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 0); wantCovered(t, tests, 1) }},
		{"gitlab failed then reports_complete=false success reads gaps", `["tests"]`,
			[]partialUnit{
				{dataset: "tests", provider: "gitlab", flags: "{}", status: "failed", result: `{}`, since: -24 * time.Hour, before: -2 * time.Hour},
				{dataset: "tests", provider: "gitlab", flags: "{}", status: "success", result: realGitLabSkipped, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				if got, _ := tests["failed_ranges"].([]any); len(got) != 0 || tests["status"] != "gaps" {
					t.Fatalf("status = %v failed_ranges = %#v, want gaps", tests["status"], tests["failed_ranges"])
				}
			}},
		{"folded cicd+tests partial: tests gap, cicd covered, latest advances", `["cicd","tests"]`,
			[]partialUnit{{dataset: "cicd", provider: "github", flags: `{"family_dataset_cicd":true,"family_dataset_tests":true}`, status: "success", result: partialCapResult, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				if overall["latest_successful_run_at"] == nil {
					t.Fatalf("cicd key must still advance latest_successful_run_at: %#v", overall)
				}
			}},
		{"stray key on a non-tests dataset is ignored", `["git"]`,
			[]partialUnit{unit("commits", "success", partialCapResult, -24*time.Hour, -time.Hour)},
			nil},
		{"failed then partial reads gaps with no failed range", `["tests"]`,
			[]partialUnit{
				unit("tests", "failed", `{}`, -24*time.Hour, -2*time.Hour),
				unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				if got, _ := tests["failed_ranges"].([]any); len(got) != 0 {
					t.Fatalf("failed_ranges = %#v, want none: the later capped success superseded the failure", tests["failed_ranges"])
				}
				if tests["status"] != "gaps" || overall["health"] != "gaps" || overall["failed_range_count"] != float64(0) {
					t.Fatalf("status = %v, overall = %#v, want gaps", tests["status"], overall)
				}
			}},
		{"failed with no later success stays failed", `["tests"]`,
			[]partialUnit{unit("tests", "failed", `{}`, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				if got, _ := tests["failed_ranges"].([]any); len(got) != 1 || tests["status"] != "failed" {
					t.Fatalf("status = %v failed_ranges = %#v, want failed", tests["status"], tests["failed_ranges"])
				}
			}},
		{"failed then complete over a wider window is healthy", `["tests"]`,
			[]partialUnit{
				unit("tests", "failed", `{}`, -24*time.Hour, -2*time.Hour),
				unit("tests", "success", completeResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				if got, _ := tests["failed_ranges"].([]any); len(got) != 0 || tests["status"] != "healthy" {
					t.Fatalf("status = %v failed_ranges = %#v, want healthy", tests["status"], tests["failed_ranges"])
				}
			}},
		{"partial then complete re-covers the window", `["tests"]`,
			[]partialUnit{
				unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour),
				unit("tests", "success", completeResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 0); wantCovered(t, tests, 1) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetProjectorTables(t, ctx, pool)
			f := seedProjectorFixture(t, ctx, pool, "org-partial", now)
			must := func(_ any, err error) {
				t.Helper()
				if err != nil {
					t.Fatal(err)
				}
			}
			must(pool.Exec(ctx, `UPDATE sync_configurations SET sync_targets=$2::json WHERE id=$1`, f.ConfigID, tc.targets))
			must(pool.Exec(ctx, `DELETE FROM sync_run_units`))
			for i, u := range tc.units {
				created := now.Add(-time.Hour + time.Duration(i)*time.Minute)
				// One sync run per unit with a later completed_at: units of one run sort by random id, so the
				// order of a failed and a later unit would not be fixed.
				runID := uuid.New()
				must(pool.Exec(ctx, `INSERT INTO sync_runs (id,org_id,integration_id,triggered_by,mode,status,total_units,completed_units,failed_units,started_at,completed_at,created_at) VALUES ($1,$2,$3,'test','incremental','success',0,0,0,$4,$4,$4)`,
					runID, f.OrgID, f.IntegrationID, created))
				must(pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,result,created_at,updated_at,cost_class,mode,attempts)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8::json,$9,$10,$11,$12::jsonb,$13,$13,'rest_core','incremental',0)`,
					uuid.New(), f.OrgID, runID, f.IntegrationID, f.SourceID, u.provider, u.dataset, u.flags,
					now.Add(u.since), now.Add(u.before), u.status, u.result, created))
			}
			raw, err := projector.Rebuild(ctx, f.OrgID, f.ConfigID)
			if err != nil {
				t.Fatal(err)
			}
			var p map[string]any
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			if tc.check == nil {
				for _, d := range p["datasets"].([]any) {
					if d.(map[string]any)["dataset_key"] == "commits" {
						wantGaps(t, d.(map[string]any), 0)
						wantCovered(t, d.(map[string]any), 1)
						return
					}
				}
				t.Fatal("commits dataset missing")
			}
			for _, d := range p["datasets"].([]any) {
				if m := d.(map[string]any); m["dataset_key"] == "tests" {
					tc.check(t, m, p["overall"].(map[string]any))
					return
				}
			}
			t.Fatal("tests dataset missing")
		})
	}
}

func wantGaps(t *testing.T, dataset map[string]any, want int) {
	t.Helper()
	got, _ := dataset["gaps"].([]any)
	if len(got) != want {
		t.Fatalf("gaps = %#v, want %d", dataset["gaps"], want)
	}
}

func wantCovered(t *testing.T, dataset map[string]any, want int) {
	t.Helper()
	got, _ := dataset["covered_ranges"].([]any)
	if len(got) != want {
		t.Fatalf("covered_ranges = %#v, want %d", dataset["covered_ranges"], want)
	}
}

func wantNoLatest(t *testing.T, overall map[string]any) {
	t.Helper()
	if overall["latest_successful_run_at"] != nil {
		t.Fatalf("a partial unit must not advance latest_successful_run_at: %#v", overall)
	}
}

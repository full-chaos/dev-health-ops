//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The unit results the GitHub chunked producer writes for a listing it could not walk past GitHub's cap of results
// (reports_complete false and the long observation of the cursor flag) and for a unit whose open phase stopped under
// the rate-limit floor (a marker only: the unit is complete). Every key and name is seeded from the constants the
// producer writes them with, so a rename breaks this build; the producer cells in internal/providersync
// (TestPeelProgressGuardEndsAStuckRange, TestOpenPhaseStopsUnderTheRateLimitFloor) assert the values.
func runSaturatedResult() string {
	return producerResultJSON(false, nil, providersync.GitHubTestsIncomplete{
		Component: providersync.GitHubTestsRunInventorySaturated, Cause: providersync.GitHubTestsListingSaturated, Count: 1})
}

func artifactSaturatedResult() string {
	return producerResultJSON(false, nil, providersync.GitHubTestsIncomplete{
		Component: providersync.GitHubTestsArtifactInventorySaturated, Cause: providersync.GitHubTestsListingSaturated, Count: 1})
}

func rateFloorResult() string {
	return producerResultJSON(true, map[string]any{
		providersync.GitHubTestsResultKeyOpenRunsReread: 0, providersync.GitHubTestsResultKeyOpenPhaseRateFloor: 499})
}

func producerResultJSON(complete bool, extra map[string]any, incomplete ...providersync.GitHubTestsIncomplete) string {
	result := map[string]any{
		providersync.GitHubTestsResultKeyReportsComplete: complete,
		providersync.GitHubTestsResultKeyIncomplete:      append([]providersync.GitHubTestsIncomplete{}, incomplete...),
	}
	for key, value := range extra {
		result[key] = value
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

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
		{"a saturated runs listing is a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", runSaturatedResult(), -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantNoLatest(t, overall)
			}},
		{"a saturated artifacts listing is a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", artifactSaturatedResult(), -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantNoLatest(t, overall)
			}},
		{"the open phase rate-floor marker alone is not a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", rateFloorResult(), -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) { wantGaps(t, tests, 0); wantCovered(t, tests, 1) }},
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
		{"failed then partial keeps the gap and the failed range", `["tests"]`,
			[]partialUnit{
				unit("tests", "failed", `{}`, -24*time.Hour, -time.Hour),
				unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				if got, _ := tests["failed_ranges"].([]any); len(got) != 1 {
					t.Fatalf("failed_ranges = %#v, want the earlier failure kept", tests["failed_ranges"])
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
				must(pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,result,created_at,updated_at,cost_class,mode,attempts)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8::json,$9,$10,$11,$12::jsonb,$13,$13,'rest_core','incremental',0)`,
					uuid.New(), f.OrgID, f.RunID, f.IntegrationID, f.SourceID, u.provider, u.dataset, u.flags,
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

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

// Results whose only lost reports are closed with a reason (CHAOS-13988): the report_member causes
// artifact_expired (GitHub deleted the artifact after its retention period), malformed (the member is broken) and
// unsafe_doctype (a member refused by design).
// The strings are seeded literally: synccoverage hardcodes the same pairs and does not import providersync.
const (
	closedExpiredResult   = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"artifact_expired","count":2}]}`
	closedMalformedResult = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"malformed","count":1}]}`
	closedBothResult      = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"malformed","count":1},{"component":"report_member","cause":"artifact_expired","count":3}]}`
	expiredAndCapResult   = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"artifact_expired","count":1},{"component":"run_reports","cause":"per_run_cap","count":1}]}`
	malformedAndOversized = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"malformed","count":1},{"component":"report_member","cause":"artifact_oversized","count":1}]}`
	unsafeDoctypeResult   = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"unsafe_doctype","count":1}]}`
	unsafeDoctypeAndCap   = `{"reports_complete":false,"incomplete":[{"component":"report_member","cause":"unsafe_doctype","count":1},{"component":"run_reports","cause":"per_run_cap","count":1}]}`
	aggregateOfPartsOnly  = `{"reports_complete":false,"incomplete":[{"component":"run_reports","cause":"aggregate_of_parts","count":1}]}`
	emptyIncompleteResult = `{"reports_complete":false,"incomplete":[]}`
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
		{"order guard: capped success then a LATER failed unit reads failed", `["tests"]`,
			[]partialUnit{
				unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour),
				unit("tests", "failed", `{}`, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				if got, _ := tests["failed_ranges"].([]any); len(got) != 1 || tests["status"] != "failed" || overall["health"] != "failed" {
					t.Fatalf("status = %v failed_ranges = %#v overall = %#v, want failed: the later failure is not superseded by an earlier capped success", tests["status"], tests["failed_ranges"], overall)
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
		{"expired-only unit is covered with reason", `["tests"]`,
			[]partialUnit{unit("tests", "success", closedExpiredResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "artifact_expired")
				if tests["status"] == "gaps" || overall["health"] == "gaps" || overall["latest_successful_run_at"] == nil {
					t.Fatalf("status = %v overall = %#v, want covered and latest advanced", tests["status"], overall)
				}
			}},
		{"malformed-only unit is covered with reason", `["tests"]`,
			[]partialUnit{unit("tests", "success", closedMalformedResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "malformed")
			}},
		{"expired and malformed unit is covered with both reasons", `["tests"]`,
			[]partialUnit{unit("tests", "success", closedBothResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "artifact_expired", "malformed")
			}},
		{"folded cicd+tests expired-only: tests covered with reason", `["cicd","tests"]`,
			[]partialUnit{{dataset: "cicd", provider: "github", flags: `{"family_dataset_cicd":true,"family_dataset_tests":true}`, status: "success", result: closedExpiredResult, since: -24 * time.Hour, before: -time.Hour}},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "artifact_expired")
			}},
		{"expired beside per_run_cap stays a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", expiredAndCapResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantClosed(t, tests)
				wantNoLatest(t, overall)
			}},
		{"malformed beside artifact_oversized stays a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", malformedAndOversized, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantClosed(t, tests)
			}},
		{"unsafe_doctype only is covered with reason", `["tests"]`,
			[]partialUnit{unit("tests", "success", unsafeDoctypeResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "unsafe_doctype")
			}},
		{"unsafe_doctype beside per_run_cap stays a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", unsafeDoctypeAndCap, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantClosed(t, tests)
				wantNoLatest(t, overall)
			}},
		{"aggregate_of_parts only stays a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", aggregateOfPartsOnly, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantClosed(t, tests)
			}},
		{"failed then expired-only success is covered, failed cleared", `["tests"]`,
			[]partialUnit{
				unit("tests", "failed", `{}`, -24*time.Hour, -2*time.Hour),
				unit("tests", "success", closedExpiredResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "artifact_expired")
				if got, _ := tests["failed_ranges"].([]any); len(got) != 0 || overall["failed_range_count"] != float64(0) {
					t.Fatalf("failed_ranges = %#v overall = %#v, want none", tests["failed_ranges"], overall)
				}
			}},
		{"empty incomplete with reports_complete=false stays a gap", `["tests"]`,
			[]partialUnit{unit("tests", "success", emptyIncompleteResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 1)
				wantCovered(t, tests, 0)
				wantClosed(t, tests)
			}},
		{"expired-only then complete over the window: complete wins", `["tests"]`,
			[]partialUnit{
				unit("tests", "success", closedExpiredResult, -24*time.Hour, -time.Hour),
				unit("tests", "success", completeResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests)
			}},
		{"expired-only then a gap-class partial: once closed, the window stays closed", `["tests"]`,
			[]partialUnit{
				unit("tests", "success", closedExpiredResult, -24*time.Hour, -time.Hour),
				unit("tests", "success", partialCapResult, -24*time.Hour, -time.Hour)},
			func(t *testing.T, tests, overall map[string]any) {
				wantGaps(t, tests, 0)
				wantCovered(t, tests, 1)
				wantClosed(t, tests, "artifact_expired")
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

// wantClosed checks closed_with_reason_ranges: none when no reasons are given, else exactly one range carrying
// those sorted reasons.
func wantClosed(t *testing.T, dataset map[string]any, reasons ...string) {
	t.Helper()
	raw, present := dataset["closed_with_reason_ranges"]
	got, ok := raw.([]any)
	if !present || !ok {
		t.Fatalf("closed_with_reason_ranges = %#v (present %v), want a list", raw, present)
	}
	if len(reasons) == 0 {
		if len(got) != 0 {
			t.Fatalf("closed_with_reason_ranges = %#v, want none", got)
		}
		return
	}
	if len(got) != 1 {
		t.Fatalf("closed_with_reason_ranges = %#v, want 1", got)
	}
	item := got[0].(map[string]any)
	gotReasons, _ := item["reasons"].([]any)
	if len(gotReasons) != len(reasons) {
		t.Fatalf("reasons = %#v, want %v", item["reasons"], reasons)
	}
	for index, reason := range reasons {
		if gotReasons[index] != reason {
			t.Fatalf("reasons = %#v, want %v", item["reasons"], reasons)
		}
	}
	if sources, _ := item["source_ids"].([]any); len(sources) != 1 || item["since"] == nil || item["before"] == nil {
		t.Fatalf("closed range = %#v, want since, before and one source", item)
	}
}

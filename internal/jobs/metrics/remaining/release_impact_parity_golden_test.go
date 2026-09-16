//go:build integration

package remaining

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// releaseImpactGoldenRow is one release_impact_daily row, every column the
// native writer computes (excluding computed_at, a write timestamp rather
// than a compute output, same exclusion icfinalize's parity golden makes).
// The three score/ratio columns are float32 at rest (migration 034's
// write-boundary narrowing, release_impact_native_clickhouse.go's
// writeReleaseImpactRows) -- the golden stores the wider float64 the
// producer computed and this test narrows before comparing, so a real
// divergence in the narrowing itself still fails loudly.
type releaseImpactGoldenRow struct {
	ReleaseRef                       string   `json:"release_ref"`
	Environment                      string   `json:"environment"`
	ReleaseUserFrictionDelta         *float64 `json:"release_user_friction_delta"`
	ReleasePostFrictionRate          *float64 `json:"release_post_friction_rate"`
	ReleaseErrorRateDelta            *float64 `json:"release_error_rate_delta"`
	ReleasePostErrorRate             *float64 `json:"release_post_error_rate"`
	TimeToFirstUserIssueAfterRelease *float64 `json:"time_to_first_user_issue_after_release"`
	ReleaseImpactConfidenceScore     float64  `json:"release_impact_confidence_score"`
	ReleaseImpactCoverageRatio       float64  `json:"release_impact_coverage_ratio"`
	CoverageRatio                    float64  `json:"coverage_ratio"`
	MissingRequiredFieldsCount       uint32   `json:"missing_required_fields_count"`
	DataCompleteness                 float64  `json:"data_completeness"`
	ConcurrentDeployCount            uint32   `json:"concurrent_deploy_count"`
}

// TestReleaseImpactMatchesTheFrozenPythonGolden is the family parity proof
// for remaining/release_impact: it seeds the SAME fixtures
// TestReleaseImpactParityClassBattery's "normal_computation_with_resolved_
// deltas" and "insufficient_samples_deltas_stay_nil" classes use (the two
// classes that write a row; the other two write zero rows by construction
// and have nothing to diff), runs the real native executor against a real
// ClickHouse, reads back every column release_impact_daily's writer sets,
// and compares against testdata/release_impact_parity_golden.json -- a
// FROZEN golden captured once by running release_impact.py's own
// _compute_day against an identically-seeded ClickHouse instance (still
// live on the module -- see release_impact.py's module docstring on why
// _compute_day is kept: src/dev_health_ops/fixtures/runner.py imports it
// directly). Capture steps (re-run only if releaseImpactParityClasses' seed
// data changes):
//
//  1. A throwaway, uncommitted integration test in this package starts
//     containers.StartClickHouse, applies chschema.Apply, seeds
//     releaseImpactParityClasses(), and prints the container's
//     containers.ClickHouseHTTPDSN() to a scratch file, then blocks.
//
//  2. While that process holds the container open, run against the SAME
//     DSN:
//
//     PYTHONPATH=src .venv/bin/python - <<'PY'
//     from datetime import date, datetime, timezone
//     from urllib.parse import urlparse
//     import clickhouse_connect
//     from dev_health_ops.metrics.release_impact import _compute_day
//     client = clickhouse_connect.get_client(...)  # host/port/user/pass from the DSN
//     records = _compute_day(client, org_id, date(2026, 8, 20), computed_at)
//     PY
//
//     for each class's seeded org_id, dumping the returned
//     ReleaseImpactDailyRecord rows.
//
//  3. Freeze the two non-empty classes' rows as this file's golden; delete
//     the throwaway harness.
//
// The two other named parity classes (quiet_day_no_deployments_no_telemetry,
// degraded_missing_telemetry) are proven separately by
// TestReleaseImpactParityClassBattery's row-count/degraded-signal assertions
// -- there is no row for either class to diff a value against.
func TestReleaseImpactMatchesTheFrozenPythonGolden(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)

	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	golden := loadReleaseImpactGolden(t)

	day, err := time.Parse("2006-01-02", releaseImpactFixtureDay)
	if err != nil {
		t.Fatalf("parse fixture day: %v", err)
	}

	classes := map[string]bool{
		"normal_computation_with_resolved_deltas": true,
		"insufficient_samples_deltas_stay_nil":    true,
	}

	for _, class := range releaseImpactParityClasses() {
		if !classes[class.name] {
			continue
		}
		t.Run(class.name, func(t *testing.T) {
			orgID := deterministicOrgIDForClass(class.name)
			class.seed(t, ctx, conn, orgID, day)

			executor, err := NewReleaseImpactExecutor(ctx, conn, nil, nil)
			if err != nil {
				t.Fatalf("construct executor: %v", err)
			}
			scope, err := json.Marshal(releaseImpactScope{
				Version: ScopeVersion, Day: releaseImpactFixtureDay,
				BackfillDays: 1, RecomputationWindowDays: 1,
			})
			if err != nil {
				t.Fatalf("marshal scope: %v", err)
			}
			run := Run{OrganizationID: orgID}
			partition := Partition{ID: "golden-" + class.name, Scope: scope}
			if _, err := executor.ComputePartition(ctx, run, partition); err != nil {
				t.Fatalf("ComputePartition: %v", err)
			}

			got := readReleaseImpactRow(t, ctx, conn, orgID)
			want, ok := golden[class.name]
			if !ok {
				t.Fatalf("golden has no entry for class %q", class.name)
			}
			assertReleaseImpactRowMatches(t, class.name, got, want)
		})
	}
}

func loadReleaseImpactGolden(t *testing.T) map[string]releaseImpactGoldenRow {
	t.Helper()
	raw, err := os.ReadFile("testdata/release_impact_parity_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var golden map[string]releaseImpactGoldenRow
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	if len(golden) == 0 {
		t.Fatal("golden is empty -- an empty fixture agrees with every implementation")
	}
	return golden
}

// readReleaseImpactRow reads back the single row a golden-comparison class
// writes. Every seeded fixture in this file produces at most one row (the
// two zero-row classes are excluded from this test -- see the file comment).
func readReleaseImpactRow(t *testing.T, ctx context.Context, conn driver.Conn, orgID string) releaseImpactGoldenRow {
	t.Helper()
	row := conn.QueryRow(ctx, `SELECT release_ref, environment,
		release_user_friction_delta, release_post_friction_rate,
		release_error_rate_delta, release_post_error_rate,
		time_to_first_user_issue_after_release,
		release_impact_confidence_score, release_impact_coverage_ratio,
		coverage_ratio, missing_required_fields_count,
		data_completeness, concurrent_deploy_count
		FROM release_impact_daily WHERE org_id = ?`, orgID)

	var (
		releaseRef, environment                                  string
		frictionDelta, postFriction, errorDelta, postError, ttfi *float64
		confidence, coverageTop, coverageRatio, dataCompleteness float32
		missingFields, concurrentDeploys                         uint32
	)
	if err := row.Scan(&releaseRef, &environment,
		&frictionDelta, &postFriction, &errorDelta, &postError, &ttfi,
		&confidence, &coverageTop, &coverageRatio, &missingFields,
		&dataCompleteness, &concurrentDeploys); err != nil {
		t.Fatalf("read release_impact_daily row for org %s: %v", orgID, err)
	}
	return releaseImpactGoldenRow{
		ReleaseRef: releaseRef, Environment: environment,
		ReleaseUserFrictionDelta:         frictionDelta,
		ReleasePostFrictionRate:          postFriction,
		ReleaseErrorRateDelta:            errorDelta,
		ReleasePostErrorRate:             postError,
		TimeToFirstUserIssueAfterRelease: ttfi,
		ReleaseImpactConfidenceScore:     float64(confidence),
		ReleaseImpactCoverageRatio:       float64(coverageTop),
		CoverageRatio:                    float64(coverageRatio),
		MissingRequiredFieldsCount:       missingFields,
		DataCompleteness:                 float64(dataCompleteness),
		ConcurrentDeployCount:            concurrentDeploys,
	}
}

func assertReleaseImpactRowMatches(t *testing.T, className string, got, want releaseImpactGoldenRow) {
	t.Helper()
	if got.ReleaseRef != want.ReleaseRef {
		t.Errorf("%s: release_ref = %q, want %q", className, got.ReleaseRef, want.ReleaseRef)
	}
	if got.Environment != want.Environment {
		t.Errorf("%s: environment = %q, want %q", className, got.Environment, want.Environment)
	}
	assertFloatPointerEqual(t, className, "release_user_friction_delta", got.ReleaseUserFrictionDelta, want.ReleaseUserFrictionDelta)
	assertFloatPointerEqual(t, className, "release_post_friction_rate", got.ReleasePostFrictionRate, want.ReleasePostFrictionRate)
	assertFloatPointerEqual(t, className, "release_error_rate_delta", got.ReleaseErrorRateDelta, want.ReleaseErrorRateDelta)
	assertFloatPointerEqual(t, className, "release_post_error_rate", got.ReleasePostErrorRate, want.ReleasePostErrorRate)
	assertFloatPointerEqual(t, className, "time_to_first_user_issue_after_release", got.TimeToFirstUserIssueAfterRelease, want.TimeToFirstUserIssueAfterRelease)
	if float32(got.ReleaseImpactConfidenceScore) != float32(want.ReleaseImpactConfidenceScore) {
		t.Errorf("%s: release_impact_confidence_score = %v, want %v", className, got.ReleaseImpactConfidenceScore, want.ReleaseImpactConfidenceScore)
	}
	if float32(got.ReleaseImpactCoverageRatio) != float32(want.ReleaseImpactCoverageRatio) {
		t.Errorf("%s: release_impact_coverage_ratio = %v, want %v", className, got.ReleaseImpactCoverageRatio, want.ReleaseImpactCoverageRatio)
	}
	if float32(got.CoverageRatio) != float32(want.CoverageRatio) {
		t.Errorf("%s: coverage_ratio = %v, want %v", className, got.CoverageRatio, want.CoverageRatio)
	}
	if got.MissingRequiredFieldsCount != want.MissingRequiredFieldsCount {
		t.Errorf("%s: missing_required_fields_count = %d, want %d", className, got.MissingRequiredFieldsCount, want.MissingRequiredFieldsCount)
	}
	if float32(got.DataCompleteness) != float32(want.DataCompleteness) {
		t.Errorf("%s: data_completeness = %v, want %v", className, got.DataCompleteness, want.DataCompleteness)
	}
	if got.ConcurrentDeployCount != want.ConcurrentDeployCount {
		t.Errorf("%s: concurrent_deploy_count = %d, want %d", className, got.ConcurrentDeployCount, want.ConcurrentDeployCount)
	}
}

func assertFloatPointerEqual(t *testing.T, className, field string, got, want *float64) {
	t.Helper()
	if (got == nil) != (want == nil) {
		t.Errorf("%s: %s = %v, want %v (nil-ness differs)", className, field, got, want)
		return
	}
	if got == nil {
		return
	}
	if float32(*got) != float32(*want) {
		t.Errorf("%s: %s = %v, want %v", className, field, *got, *want)
	}
}

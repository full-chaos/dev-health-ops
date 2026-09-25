//go:build integration

// CHAOS-6842: the feature-flag timeseries kernel, executed against a REAL
// ClickHouse engine over the REAL migrated release_impact_daily table, with
// seeded NON-EMPTY data.
//
// featureFlagTimeseries is a go-served operation whose Python body is
// deleted, and its go-served ledger entry is the unproven form: no two-plane
// run ever compared a leaf. Its named guards were tests over a canned row
// scanner (TestResolve_HappyPath_TimeseriesAndBreakdown,
// TestExecuteTimeseries_AllNullBucketYieldsNilValue_NotZero): they prove what
// the code does with rows somebody typed, never what the engine returns for
// the SQL the compiler wrote. This test closes that: the four measures the
// FeatureFlagTimeseries document requests (FLAG_ACTIVATION_RATE,
// FLAG_FRICTION_DELTA, FLAG_ERROR_RATE_DELTA, FLAG_COVERAGE_RATIO, REPO
// dimension, DAY interval) are compiled by CompileTimeseries and run by
// ExecuteTimeseries on the schema chain's own end state (chschema.Apply: no
// hand-typed DDL), and every number is asserted.
//
// What it pins, each with a seeded row that would move it:
//   - the AVG over several rows of one repo and day (two release_refs);
//   - the *100 scaling of the ratio measures;
//   - an all-NULL bucket comes back nil, not 0 (a measured zero stays 0);
//   - the org predicate (a foreign org's rows never count);
//   - the date window is inclusive at both ends and excludes the day before
//     and the day after;
//   - WEEK and MONTH bucketing truncate the day the way date_trunc does.
//
// Named limit, not asserted here: release_impact_daily is read without FINAL
// and is unregistered in the read-side dedup registry (tracked as CHAOS-4536,
// Python has the same gap), so a re-run of a day is visible as duplicates
// until a merge. Every seeded key below is unique for that reason.
package analytics

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// releaseImpactSeed is one release_impact_daily row. Only the columns the four
// flag measures read vary; every other NOT NULL column gets a fixed filler.
type releaseImpactSeed struct {
	org, day, release, repo string
	// The three Nullable(Float64) inputs; nil is SQL NULL.
	friction, errorRate, activation *float64
	// coverage_ratio is a non-nullable Float32.
	coverage float32
}

func fp(v float64) *float64 { return &v }

func seedReleaseImpact(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, rows []releaseImpactSeed) {
	t.Helper()
	nullable := func(v *float64) string {
		if v == nil {
			return "NULL"
		}
		return fmt.Sprintf("%v", *v)
	}
	values := ""
	for i, r := range rows {
		if i > 0 {
			values += ", "
		}
		values += fmt.Sprintf("('%s', toDate('%s'), '%s', 'prod', '%s', %s, %s, %s, %v, now64(3))",
			r.org, r.day, r.release, r.repo, nullable(r.friction), nullable(r.errorRate), nullable(r.activation), r.coverage)
	}
	insert := `INSERT INTO release_impact_daily
		(org_id, day, release_ref, environment, repo_id,
		 release_user_friction_delta, release_error_rate_delta, flag_activation_rate, coverage_ratio,
		 computed_at) VALUES ` + values
	if err := conn.Exec(ctx, insert); err != nil {
		t.Fatalf("seed release_impact_daily: %v", err)
	}
}

func TestFeatureFlagTimeseries_SeededRealClickHouse_ExactBuckets(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	defer func() { _ = inst.Close(context.Background()) }()
	chschema.Apply(ctx, t, inst)

	opts, err := stdclickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatalf("open raw ClickHouse connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: inst.URI})
	if err != nil {
		t.Fatalf("construct ClickHouse query client: %v", err)
	}
	defer func() { _ = client.Close() }()

	const org, other = "chaos-6842-flags", "chaos-6842-flags-OTHER-ORG"
	seedReleaseImpact(t, ctx, conn, []releaseImpactSeed{
		// repo-a, 2026-01-05 (Monday): one row of every measure.
		{org: org, day: "2026-01-05", release: "r1", repo: "repo-a", friction: fp(0.03), errorRate: fp(0.015), activation: fp(0.40), coverage: 0.5},
		// repo-a, 2026-01-06: every nullable input NULL: the all-NULL bucket.
		{org: org, day: "2026-01-06", release: "r2", repo: "repo-a", friction: nil, errorRate: nil, activation: nil, coverage: 0.25},
		// repo-a, 2026-01-07: two releases the same day: AVG of the two.
		{org: org, day: "2026-01-07", release: "r3", repo: "repo-a", friction: fp(0.02), errorRate: fp(0.01), activation: fp(0.25), coverage: 0.5},
		{org: org, day: "2026-01-07", release: "r4", repo: "repo-a", friction: fp(0.06), errorRate: fp(0.03), activation: fp(0.75), coverage: 0.75},
		// repo-a, 2026-01-08: the last day of the window (inclusive).
		{org: org, day: "2026-01-08", release: "r5", repo: "repo-a", friction: fp(0.05), errorRate: fp(0.02), activation: fp(0.5), coverage: 1},
		// repo-b, 2026-01-05: a second dimension value.
		{org: org, day: "2026-01-05", release: "r6", repo: "repo-b", friction: fp(-0.01), errorRate: fp(0), activation: fp(0), coverage: 0.125},
		// Outside the window on both sides: must never count.
		{org: org, day: "2026-01-04", release: "r7", repo: "repo-a", friction: fp(9), errorRate: fp(9), activation: fp(9), coverage: 9},
		{org: org, day: "2026-01-09", release: "r8", repo: "repo-a", friction: fp(9), errorRate: fp(9), activation: fp(9), coverage: 9},
		// Another organisation, inside the window: must never count.
		{org: other, day: "2026-01-05", release: "r9", repo: "repo-a", friction: fp(9), errorRate: fp(9), activation: fp(9), coverage: 9},
	})

	// value is the expected bucket value; nil means SQL NULL.
	type bucket struct {
		date  string
		value *float64
	}
	run := func(measure Measure, interval BucketInterval, start, end string) map[string][]bucket {
		t.Helper()
		q, err := CompileTimeseries(TimeseriesRequest{
			Dimension: DimensionRepo, Measure: measure, Interval: interval,
			StartDate: mustGraphQLDate(start), EndDate: mustGraphQLDate(end),
		}, org, queryTimeoutSecs, false, nil)
		if err != nil {
			t.Fatalf("CompileTimeseries(%s): %v", measure, err)
		}
		results, err := ExecuteTimeseries(ctx, client, q, "REPO", string(measure))
		if err != nil {
			t.Fatalf("ExecuteTimeseries(%s): %v", measure, err)
		}
		out := map[string][]bucket{}
		for _, result := range results {
			for _, b := range result.Buckets {
				out[result.DimensionValue] = append(out[result.DimensionValue], bucket{date: b.Date.Time().Format("2006-01-02"), value: b.Value})
			}
		}
		return out
	}
	check := func(name string, got map[string][]bucket, want map[string][]bucket) {
		t.Helper()
		if len(got) != len(want) {
			t.Errorf("%s: dimension values %v, want %v", name, keys(got), keys(want))
		}
		for repo, wantBuckets := range want {
			gotBuckets := got[repo]
			byDate := map[string]*float64{}
			for _, b := range gotBuckets {
				byDate[b.date] = b.value
			}
			if len(gotBuckets) != len(wantBuckets) {
				t.Errorf("%s %s: %d buckets %v, want %d", name, repo, len(gotBuckets), gotBuckets, len(wantBuckets))
				continue
			}
			for _, w := range wantBuckets {
				g, present := byDate[w.date]
				switch {
				case !present:
					t.Errorf("%s %s %s: no bucket", name, repo, w.date)
				case w.value == nil && g != nil:
					t.Errorf("%s %s %s = %v, want NULL (an all-NULL bucket is nil, never 0)", name, repo, w.date, *g)
				case w.value != nil && g == nil:
					t.Errorf("%s %s %s = NULL, want %v", name, repo, w.date, *w.value)
				case w.value != nil && math.Abs(*g-*w.value) > 1e-6:
					t.Errorf("%s %s %s = %v, want %v", name, repo, w.date, *g, *w.value)
				}
			}
		}
	}

	const start, end = "2026-01-05", "2026-01-08"
	check("FLAG_ACTIVATION_RATE", run(MeasureFlagActivationRate, BucketIntervalDay, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-05", fp(40)}, {"2026-01-06", nil}, {"2026-01-07", fp(50)}, {"2026-01-08", fp(50)}},
		"repo-b": {{"2026-01-05", fp(0)}},
	})
	check("FLAG_FRICTION_DELTA", run(MeasureFlagFrictionDelta, BucketIntervalDay, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-05", fp(3)}, {"2026-01-06", nil}, {"2026-01-07", fp(4)}, {"2026-01-08", fp(5)}},
		"repo-b": {{"2026-01-05", fp(-1)}},
	})
	check("FLAG_ERROR_RATE_DELTA", run(MeasureFlagErrorRateDelta, BucketIntervalDay, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-05", fp(1.5)}, {"2026-01-06", nil}, {"2026-01-07", fp(2)}, {"2026-01-08", fp(2)}},
		"repo-b": {{"2026-01-05", fp(0)}},
	})
	// coverage_ratio is non-nullable, so no bucket is ever NULL.
	check("FLAG_COVERAGE_RATIO", run(MeasureFlagCoverageRatio, BucketIntervalDay, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-05", fp(50)}, {"2026-01-06", fp(25)}, {"2026-01-07", fp(62.5)}, {"2026-01-08", fp(100)}},
		"repo-b": {{"2026-01-05", fp(12.5)}},
	})
	// WEEK truncates to the Monday (2026-01-05 is one): all four repo-a days
	// fall in one bucket, whose AVG is over the five non-NULL rows of the
	// measure's rows that week: (0.40 + 0.25 + 0.75 + 0.5) / 4 * 100 = 47.5 (the NULL row is ignored).
	check("FLAG_ACTIVATION_RATE by WEEK", run(MeasureFlagActivationRate, BucketIntervalWeek, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-05", fp(47.5)}},
		"repo-b": {{"2026-01-05", fp(0)}},
	})
	// MONTH truncates to the 1st; the window still bounds the rows.
	check("FLAG_COVERAGE_RATIO by MONTH", run(MeasureFlagCoverageRatio, BucketIntervalMonth, start, end), map[string][]bucket{
		"repo-a": {{"2026-01-01", fp((0.5 + 0.25 + 0.5 + 0.75 + 1) / 5 * 100)}},
		"repo-b": {{"2026-01-01", fp(12.5)}},
	})
	// A window that holds no seeded day yields no buckets, not zeros.
	if got := run(MeasureFlagActivationRate, BucketIntervalDay, "2026-02-01", "2026-02-28"); len(got) != 0 {
		t.Errorf("an empty window returned %v, want no series", got)
	}
}

func keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

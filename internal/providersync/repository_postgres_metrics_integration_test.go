//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// syncRunRollupBumpedCount reads the current value of one
// dev_health_sync_run_rollup_bumped_total{outcome,path} series from the
// process-wide singleton (CHAOS-4586 moved this metric off the per-instance
// providerfoundation.Metrics -- see budget.go's SyncRunRollupBumpedMetricsSource
// doc comment for why). It is process-wide, not per-repository, so every
// integration test in this package that exercises a real Fail/Complete adds
// to the SAME counters; a before/after delta is the only assertion that is
// not sensitive to test execution order within the shared test binary.
// Missing series read as 0, matching Prometheus's own "absent == zero"
// convention.
func syncRunRollupBumpedCount(t *testing.T, outcome, path string) uint64 {
	t.Helper()
	var output bytes.Buffer
	if err := providerfoundation.SyncRunRollupBumpedMetricsSource().WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf(`dev_health_sync_run_rollup_bumped_total{outcome=%q,path=%q} `, outcome, path)
	for _, line := range strings.Split(output.String(), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 10, 64)
		if err != nil {
			t.Fatalf("parse counter value from %q: %v", line, err)
		}
		return value
	}
	return 0
}

// TestPostgresRepositoryRecordsClaimAndFailMetrics pins CHAOS-4078's
// telemetry requirement end to end: a PostgresRepository constructed WITH a
// providerfoundation.Metrics instance actually records a claim on Claim
// success and a failure-with-reason on Fail, through the real SQL paths --
// not just the pure Metrics.Record* unit tests in providerfoundation, which
// prove the counter logic but not that this package's repository wires it.
func TestPostgresRepositoryRecordsClaimAndFailMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	metrics := providerfoundation.NewMetrics()
	repository, err := NewPostgresRepository(pool, metrics)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claim.Provider != "github" || claim.Dataset != "commits" {
		t.Fatalf("fixture claim provider/dataset drifted: %+v", claim)
	}
	rollupBefore := syncRunRollupBumpedCount(t, "failed", "provider_unit")

	failedAt := now.Add(time.Second)
	if err := repository.Fail(ctx, claim, "feature_disabled", now, failedAt); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		`dev_health_provider_unit_claimed_total{provider="github",dataset="commits"} 1`,
		`dev_health_provider_unit_failed_total{provider="github",dataset="commits",reason="feature_disabled"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	// CHAOS-4586: the rollup-bumped counter moved off the per-instance
	// Metrics.WritePrometheus onto a process-wide singleton (two Metrics
	// instances in one worker process must not each declare their own
	// # HELP/# TYPE for the same metric name -- see budget.go). This
	// instance's own render must no longer carry it.
	if strings.Contains(rendered, "dev_health_sync_run_rollup_bumped_total") {
		t.Fatalf("dev_health_sync_run_rollup_bumped_total must not render from the per-instance Metrics any more:\n%s", rendered)
	}

	// CHAOS-4559: Fail's terminal commit must bump the run's live rollup
	// counter, not just the pre-existing claim/fail-reason counters above.
	// Read via the process-wide singleton (CHAOS-4586's path label), and as
	// a delta since other integration tests in this package's shared test
	// binary bump the same series.
	if got, want := syncRunRollupBumpedCount(t, "failed", "provider_unit"), rollupBefore+1; got != want {
		t.Fatalf("dev_health_sync_run_rollup_bumped_total{outcome=\"failed\",path=\"provider_unit\"} = %d, want %d (before=%d)", got, want, rollupBefore)
	}
}

// TestPostgresRepositoryClaimAndFailToleratesNilMetrics pins the nil-safe
// default (every existing one-argument NewPostgresRepository call site):
// omitting metrics must never panic Claim or Fail.
func TestPostgresRepositoryClaimAndFailToleratesNilMetrics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	if repository.Metrics != nil {
		t.Fatalf("expected nil Metrics with no argument, got %+v", repository.Metrics)
	}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Fail(ctx, claim, "feature_disabled", now, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
}

// TestPostgresRepositoryFailWithDuplicateKeyDetailPersistsStructuredKey pins
// CHAOS-4557 end to end against real Postgres: a duplicate_natural_key
// termination must leave the destination table and colliding natural-key
// fields readable from sync_run_units.result -- not just the bare category
// Fail alone persisted before this fix, and not just a stdout log line that
// does not survive a worker restart.
func TestPostgresRepositoryFailWithDuplicateKeyDetailPersistsStructuredKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)

	metrics := providerfoundation.NewMetrics()
	repository, err := NewPostgresRepository(pool, metrics)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	claim, err := repository.Claim(ctx, ClaimRequest{
		UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
		LeaseDuration: time.Minute, AllowExpiredRecovery: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	failedAt := now.Add(time.Second)
	fields := []DuplicateNaturalKeyField{
		{Name: "org_id", Value: "70d529e0-3c06-4597-8480-794fd02328b6"},
		{Name: "repo_id", Value: "7b9583ee-4d24-2be7-4d09-34f815bebdd7"},
		{Name: "run_id", Value: "33248832747"},
		{Name: "suite_id", Value: "39e6a65dcda47e162038d43836b45a156ff06a315b32bcf344a94aadf754f35b"},
		{Name: "case_id", Value: "00785b22f65e05dd2a7b4741d0cb288890317956440b1dc8fde05ffac989d8c9"},
	}
	if err := repository.FailWithDuplicateKeyDetail(
		ctx, claim, "duplicate_natural_key", "test_case_results", fields, now, failedAt,
	); err != nil {
		t.Fatal(err)
	}

	var rawResult, rawError string
	row := pool.QueryRow(ctx, `SELECT result::text, error FROM sync_run_units WHERE id = $1`, claim.ID)
	if err := row.Scan(&rawResult, &rawError); err != nil {
		t.Fatal(err)
	}
	if rawError != "duplicate_natural_key" {
		t.Fatalf("error column=%q, want %q", rawError, "duplicate_natural_key")
	}
	var decoded struct {
		ErrorCategory string `json:"error_category"`
		DuplicateKey  struct {
			Table  string            `json:"table"`
			Fields map[string]string `json:"fields"`
		} `json:"duplicate_key"`
	}
	if err := json.Unmarshal([]byte(rawResult), &decoded); err != nil {
		t.Fatalf("result is not the expected shape: %v (raw=%s)", err, rawResult)
	}
	if decoded.ErrorCategory != "duplicate_natural_key" {
		t.Fatalf("result.error_category=%q, want %q", decoded.ErrorCategory, "duplicate_natural_key")
	}
	if decoded.DuplicateKey.Table != "test_case_results" {
		t.Fatalf("result.duplicate_key.table=%q, want test_case_results", decoded.DuplicateKey.Table)
	}
	for _, field := range fields {
		if got := decoded.DuplicateKey.Fields[field.Name]; got != field.Value {
			t.Fatalf("result.duplicate_key.fields[%s]=%q, want %q", field.Name, got, field.Value)
		}
	}

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(),
		`dev_health_provider_unit_failed_total{provider="github",dataset="commits",reason="duplicate_natural_key"} 1`,
	) {
		t.Fatalf("missing the standard unit-failed counter in:\n%s", output.String())
	}
}

// A failed unit keeps its outcome label in error/result.error_category and
// names the cause class in result.cause_class, through the real SQL. A writer
// without a class (an older binary, a path that has none) leaves the key out;
// a class outside the vocabulary shape is stored as "unclassified", never as
// the raw text.
func TestPostgresRepositoryFailWithCauseClassStoresTheClass(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	metrics := providerfoundation.NewMetrics()
	repository, err := NewPostgresRepository(pool, metrics)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	cases := []struct {
		name       string
		category   string
		class      string
		wantClass  string
		wantHasKey bool
		detail     *CauseDetail
		wantDetail map[string]any
	}{
		{"exhausted names its class", "provider_unit_exhausted", "provider_transient", "provider_transient", true, nil, nil},
		{"terminal class", "response_too_large", "response_too_large", "response_too_large", true, nil, nil},
		{"raw text is never stored", "provider_unit_exhausted", "/repos/acme/api failed", CauseClassUnclassified, true, nil, nil},
		{"no class from an older writer", "provider_unit_exhausted", "", "", false, nil, nil},
		{"size refusal detail", "result_too_large", "result_too_large", "result_too_large", true,
			&CauseDetail{Limit: "rows", Table: "work_items", Rows: 100001, Bytes: 7},
			map[string]any{"limit": "rows", "table": "work_items", "rows": float64(100001), "bytes": float64(7)}},
		{"free text in a detail label is never stored", "result_too_large", "result_too_large", "result_too_large", true,
			&CauseDetail{Limit: "rows", Table: "acme/secret table", Rows: -1, Bytes: 3},
			map[string]any{"limit": "rows", "table": "unclassified", "rows": float64(0), "bytes": float64(3)}},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `UPDATE sync_run_units SET status='dispatching', error=NULL, result='{}'::jsonb WHERE id=$1`, firstUnitID); err != nil {
				t.Fatal(err)
			}
			claim, err := repository.Claim(ctx, ClaimRequest{
				UnitID: firstUnitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: now,
				LeaseDuration: time.Minute, AllowExpiredRecovery: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			failedAt := now.Add(time.Second)
			if testCase.class == "" {
				err = repository.Fail(ctx, claim, testCase.category, now, failedAt)
			} else {
				err = repository.FailWithCauseClass(ctx, claim, testCase.category, testCase.class, testCase.detail, now, failedAt)
			}
			if err != nil {
				t.Fatal(err)
			}
			var rawError string
			var rawResult string
			if err := pool.QueryRow(ctx, `SELECT error, result::text FROM sync_run_units WHERE id=$1`, claim.ID).Scan(&rawError, &rawResult); err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal([]byte(rawResult), &decoded); err != nil {
				t.Fatal(err)
			}
			if gotDetail := decoded["cause_detail"]; fmt.Sprint(gotDetail) != fmt.Sprint(testCase.wantDetail) &&
				!(gotDetail == nil && testCase.wantDetail == nil) {
				t.Fatalf("cause_detail=%v, want %v", gotDetail, testCase.wantDetail)
			}
			got, has := decoded["cause_class"]
			if rawError != testCase.category || decoded["error_category"] != testCase.category ||
				has != testCase.wantHasKey || (has && got != testCase.wantClass) {
				t.Fatalf("error=%q result=%s, want category %q class %q (key present %v)",
					rawError, rawResult, testCase.category, testCase.wantClass, testCase.wantHasKey)
			}
		})
	}
}

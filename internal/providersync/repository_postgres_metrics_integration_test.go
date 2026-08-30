//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
		// CHAOS-4559: Fail's terminal commit must bump the run's live rollup
		// counter, not just the pre-existing claim/fail-reason counters above.
		`dev_health_sync_run_rollup_bumped_total{outcome="failed"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
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

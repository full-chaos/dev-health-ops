//go:build integration

package providersync

import (
	"bytes"
	"context"
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

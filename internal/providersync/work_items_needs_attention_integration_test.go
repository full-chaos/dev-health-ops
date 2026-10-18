//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8802: the third equal work-items failure of a source stamps the
// work-item dataset rows "needs_attention:<class>" for the owner-facing API,
// a failure of another class does not extend the streak, and the next success
// clears the stamp.
func TestPostgresWorkItemsNeedsAttentionStampAfterThreeEqualFailures(t *testing.T) {
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
	for _, key := range workitemcontract.FamilyDatasets() {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, 'org-acme', $2, $3, '{}'::jsonb)`, uuid.NewString(), firstIntegrationID, key); err != nil {
			t.Fatal(err)
		}
	}
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)

	claimWorkItemsUnit := func(at time.Time) Claim {
		t.Helper()
		unitID := uuid.NewString()
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, since_at, before_at, status,
    processor_flags, updated_at
) VALUES (
    $1, 'org-acme', $2, $3, $4, 'github', 'work-items', 'medium',
    'incremental', '2026-07-22T12:00:00Z', '2026-07-23T12:00:00Z',
    'dispatching',
    '{"sync_prs":true,
      "family_dataset_work_items":true,
      "family_dataset_work_item_labels":true,
      "family_dataset_work_item_history":true,
      "family_dataset_work_item_comments":true,
      "family_dataset_work_item_projects":true}'::jsonb,
    NOW()
)`, unitID, firstRunID, firstIntegrationID, firstSourceID); err != nil {
			t.Fatal(err)
		}
		claim, err := repository.Claim(ctx, ClaimRequest{
			UnitID: unitID, OrgID: "org-acme", Owner: uuid.NewString(), Now: at,
			LeaseDuration: time.Minute, AllowExpiredRecovery: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		return claim
	}
	fail := func(class string, hours int) {
		t.Helper()
		at := base.Add(time.Duration(hours) * time.Hour)
		claim := claimWorkItemsUnit(at)
		if err := repository.FailWithCauseClass(ctx, claim, class, class, nil, at, at.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	reasons := func() map[string]string {
		t.Helper()
		rows, err := pool.Query(ctx, `
SELECT dataset_key, COALESCE(unavailable_reason, '')
FROM public.integration_datasets
WHERE org_id = 'org-acme' AND integration_id = $1 AND dataset_key = ANY($2::text[])`,
			firstIntegrationID, workitemcontract.FamilyDatasets())
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var key, reason string
			if err := rows.Scan(&key, &reason); err != nil {
				t.Fatal(err)
			}
			out[key] = reason
		}
		return out
	}
	assertAll := func(step, want string) {
		t.Helper()
		got := reasons()
		if len(got) != len(workitemcontract.FamilyDatasets()) {
			t.Fatalf("%s: %d dataset rows, want %d", step, len(got), len(workitemcontract.FamilyDatasets()))
		}
		for key, reason := range got {
			wantReason := ""
			if key == "work-items" {
				wantReason = want
			}
			if reason != wantReason {
				t.Fatalf("%s: %s unavailable_reason=%q, want %q (only the canonical row is stamped)", step, key, reason, wantReason)
			}
		}
	}

	fail("result_too_large", 1)
	fail("result_too_large", 2)
	assertAll("after two failures", "")

	fail("time_limit", 3)
	assertAll("a failure of another class does not extend the streak", "")

	fail("time_limit", 4)
	fail("time_limit", 5)
	assertAll("after three time_limit failures in a row", "needs_attention:time_limit")

	// The stamp does not block dispatch: a unit planned for the daily try is
	// claimable while the dataset row still carries it.
	if stamped := claimWorkItemsUnit(base.Add(10 * time.Hour)); stamped.ID == "" {
		t.Fatal("a work-items unit could not be claimed while the needs_attention stamp is set")
	}
	assertAll("claiming does not clear the stamp", "needs_attention:time_limit")

	var count int
	var lastClass string
	if err := pool.QueryRow(ctx, `
SELECT count(*), max(result::jsonb ->> 'cause_class') FROM public.sync_run_units
WHERE dataset_key = 'work-items' AND status = 'failed'`).Scan(&count, &lastClass); err != nil || count != 5 {
		t.Fatalf("failed work-items units = %d (err %v), want 5", count, err)
	}

	success := claimWorkItemsUnit(base.Add(30 * time.Hour))
	windowClose := base.Add(30 * time.Hour)
	routeResult, routeWatermark, err := applyGitHubWorkItemsIncompletePolicy(
		success.Provider, success.Dataset,
		map[string]any{"records": 1, githubWorkItemsIncompleteResultKey: []GitHubWorkItemsIncomplete{}},
		&windowClose,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(ctx, success, routeResult, routeWatermark,
		base.Add(30*time.Hour), base.Add(30*time.Hour+2*time.Second)); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertAll("after a success", "")
}

// Units that failed before the cause class was stored carry none. They read as
// "unclassified", so the marker is a valid reason and the streak still counts.
func TestPostgresWorkItemsNeedsAttentionReadsLegacyFailuresAsUnclassified(t *testing.T) {
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
	if _, err := pool.Exec(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, options)
VALUES ($1, 'org-acme', $2, 'work-items', '{}'::jsonb)`, uuid.NewString(), firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	for i, result := range []string{`{"error_category":"provider_unit_exhausted"}`, `null`, `{"error_category":"x","cause_class":"Bad Class!"}`} {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (
    id, org_id, sync_run_id, integration_id, source_id, provider,
    dataset_key, cost_class, mode, status, result, updated_at
) VALUES ($1, 'org-acme', $2, $3, $4, 'github', 'work-items', 'medium', 'incremental', 'failed', $5::json, $6)`,
			uuid.NewString(), firstRunID, firstIntegrationID, firstSourceID, result, base.Add(time.Duration(i)*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	claim := Claim{Unit: Unit{OrgID: "org-acme", IntegrationID: firstIntegrationID, SourceID: firstSourceID, Dataset: "work-items", Provider: "github"}}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	streak, err := sourceFailureStreak(ctx, tx, claim)
	if err != nil || streak.Count != 3 || streak.Class != CauseClassUnclassified {
		t.Fatalf("streak = %+v, %v, want 3 failures of class %q", streak, err, CauseClassUnclassified)
	}
	stampNeedsAttention(ctx, tx, claim)
	var reason string
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(unavailable_reason, '') FROM public.integration_datasets
WHERE org_id = 'org-acme' AND integration_id = $1 AND dataset_key = 'work-items'`, firstIntegrationID).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason != "needs_attention:unclassified" {
		t.Fatalf("unavailable_reason = %q, want needs_attention:unclassified", reason)
	}
}

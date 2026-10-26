//go:build integration

package providersync

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const legacyRecentTerminalUnitsSQL = `
SELECT source_id::text, status = 'failed', COALESCE(result::jsonb ->> 'cause_class', ''), updated_at
FROM (
  SELECT source_id, status, result, updated_at,
         row_number() OVER (PARTITION BY source_id ORDER BY updated_at DESC, id DESC) AS position
  FROM public.sync_run_units
  WHERE org_id = $1 AND integration_id = $2::uuid AND dataset_key = $3
    AND status IN ('success', 'failed')
) AS recent
WHERE position <= $4
ORDER BY source_id, position`

// CHAOS-11391 (acp C3), caller 2: sourceFailureStreak reads the same SQL as the
// materializer. With more than eight units per source and ties on updated_at
// it returns the rows, in the order, of the window query it replaces, and the
// streak of the claimed source is unchanged.
func TestPostgresSourceFailureStreakMatchesTheLegacyWindowQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeContext)
	}()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	secondSource := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO public.integration_sources
		  (id, org_id, integration_id, external_id, full_name, metadata)
		  VALUES ($1, 'org-acme', $2, 'acme/web', 'acme/web', '{}')`, secondSource, firstIntegrationID); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{firstSourceID, secondSource} {
		if _, err := pool.Exec(ctx, `
INSERT INTO public.sync_run_units (id, org_id, sync_run_id, integration_id, source_id, provider, dataset_key, cost_class, mode, status, result, updated_at)
SELECT gen_random_uuid(), 'org-acme', $1, $2, $3, 'github',
       CASE WHEN g % 11 = 0 THEN 'commits' ELSE 'work-items' END, 'medium', 'incremental',
       CASE WHEN g % 7 = 0 THEN 'running' WHEN g % 3 = 0 THEN 'success' ELSE 'failed' END,
       CASE WHEN g % 3 = 1 THEN '{"cause_class":"rate_limited"}'::json ELSE '{}'::json END,
       timestamptz '2026-08-01 00:00:00+00' + ((g / 3) * interval '1 minute')
FROM generate_series(1, 60) AS g`, firstRunID, firstIntegrationID, source); err != nil {
			t.Fatal(err)
		}
	}
	type row struct {
		Source string
		Failed bool
		Cause  string
		At     time.Time
	}
	read := func(sql string) []row {
		t.Helper()
		rows, err := pool.Query(ctx, sql, "org-acme", firstIntegrationID, workItemsCanonicalDataset, RecentTerminalUnitsLimit)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []row
		for rows.Next() {
			var r row
			if err := rows.Scan(&r.Source, &r.Failed, &r.Cause, &r.At); err != nil {
				t.Fatal(err)
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	legacy, current := read(legacyRecentTerminalUnitsSQL), read(RecentTerminalUnitsSQL)
	if len(legacy) != 2*RecentTerminalUnitsLimit || !reflect.DeepEqual(legacy, current) {
		t.Fatalf("rows differ\nlegacy:  %+v\ncurrent: %+v", legacy, current)
	}
	for _, source := range []string{firstSourceID, secondSource} {
		var outcomes []UnitOutcome
		for _, r := range legacy {
			if r.Source == source {
				outcomes = append(outcomes, UnitOutcome{Failed: r.Failed, CauseClass: r.Cause, At: r.At})
			}
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err := sourceFailureStreak(ctx, tx, Claim{Unit: Unit{OrgID: "org-acme", IntegrationID: firstIntegrationID, SourceID: source}})
		_ = tx.Rollback(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if want := FailureStreakOf(outcomes); !reflect.DeepEqual(got, want) {
			t.Fatalf("source %s streak = %+v, want %+v", source, got, want)
		}
	}
}

//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

type observationRow struct {
	orgID, provider, integrationID, runID, unitID string
	attribution, dimension, reason                *string
	routeFamily                                   *string
	retryAfter                                    *float64
	observedAt                                    time.Time
}

func readObservations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []observationRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT org_id, provider, integration_id::text, sync_run_id::text, sync_run_unit_id::text,
       route_family, route_family_attribution, dimension, retry_after_seconds, reason, observed_at
FROM public.provider_rate_limit_observations ORDER BY observed_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []observationRow
	for rows.Next() {
		var row observationRow
		if err := rows.Scan(&row.orgID, &row.provider, &row.integrationID, &row.runID, &row.unitID,
			&row.routeFamily, &row.attribution, &row.dimension, &row.retryAfter, &row.reason, &row.observedAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	return out
}

// TestDeferForRateLimitWritesTheObservationInItsOwnTransaction: the RETRYING
// stamp and the observation row commit together; a deferral that loses its
// lease writes no row; a limit with no dimension stamps the unit and writes none.
func TestDeferForRateLimitWritesTheObservationInItsOwnTransaction(t *testing.T) {
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
	createProviderSyncFixture(t, ctx, pool)
	seedProviderSyncFixture(t, ctx, pool)
	repository, err := NewPostgresRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
	claim := claimForBudgetEpisode(t, ctx, repository, now)

	t.Run("a lost lease writes no observation", func(t *testing.T) {
		lost := claim
		lost.Owner = "00000000-0000-4000-8000-00000000dead"
		err := repository.DeferForRateLimit(ctx, lost, now.Add(time.Minute), now,
			RateLimitObservation{Dimension: "rest_core", RetryAfter: time.Minute})
		if err == nil {
			t.Fatal("a deferral without the lease succeeded")
		}
		if rows := readObservations(t, ctx, pool); len(rows) != 0 {
			t.Fatalf("lost lease left %d observation rows", len(rows))
		}
	})

	t.Run("a limit with no dimension stamps the unit and writes no row", func(t *testing.T) {
		if err := repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now, RateLimitObservation{}); err != nil {
			t.Fatal(err)
		}
		if rows := readObservations(t, ctx, pool); len(rows) != 0 {
			t.Fatalf("dimensionless limit wrote %d rows", len(rows))
		}
		if state := readBudgetEpisode(t, ctx, pool, firstUnitID); state.status != "dispatching" {
			t.Fatalf("unit status = %q after the deferral", state.status)
		}
	})

	t.Run("a dimensioned limit writes one ambiguous-dimension row with the unit's identity", func(t *testing.T) {
		seedBudgetEpisode(t, ctx, pool, firstUnitID, now)
		claim := claimForBudgetEpisode(t, ctx, repository, now)
		if err := repository.DeferForRateLimit(ctx, claim, now.Add(2*time.Minute), now,
			RateLimitObservation{Dimension: "graphql_cost", RetryAfter: 2 * time.Minute}); err != nil {
			t.Fatal(err)
		}
		rows := readObservations(t, ctx, pool)
		if len(rows) != 1 {
			t.Fatalf("rows = %d, want 1", len(rows))
		}
		row := rows[0]
		if row.orgID != claim.OrgID || row.provider != claim.Provider || row.integrationID != claim.IntegrationID ||
			row.runID != claim.SyncRunID || row.unitID != claim.ID {
			t.Fatalf("row identity = %+v, claim = %+v", row, claim)
		}
		if row.routeFamily != nil || row.attribution == nil || *row.attribution != AmbiguousDimensionAttribution ||
			row.dimension == nil || *row.dimension != "graphql_cost" {
			t.Fatalf("row attribution = %v family = %v dimension = %v", row.attribution, row.routeFamily, row.dimension)
		}
		if row.retryAfter == nil || *row.retryAfter != 120 || row.reason == nil || *row.reason != rateLimitObservationReason ||
			!row.observedAt.Equal(now) {
			t.Fatalf("row retry_after = %v reason = %v observed_at = %v", row.retryAfter, row.reason, row.observedAt)
		}
	})
}

//go:build integration

package acr

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-3695: the entitlement route's agent_context_runtime is ask_dev OR the
// legacy agent_context_runtime grant, decided through the real licensing read
// on migrated tables: each org holds an org override for none, one or both.
func TestPostgresEntitlementStoreLookupFollowsAskDevOrTheLegacyGrant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schemaFor(ctx, t, pool)

	var legacyID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = 'agent_context_runtime'`).Scan(&legacyID); err != nil {
		t.Fatalf("the migrations register agent_context_runtime: %v", err)
	}
	askDevID := uuid.NewString()
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'ask_dev'`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, askDevID, "ask_dev", "community", true)

	store := PostgresEntitlementStore{Pool: pool, Now: func() time.Time { return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC) }}
	for _, tc := range []struct{ askDev, legacy bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("ask_dev=%v agent_context_runtime=%v", tc.askDev, tc.legacy), func(t *testing.T) {
			org := uuid.NewString()
			pgseed.Org(ctx, t, pool, org, "community")
			if tc.askDev {
				pgseed.OrgOverride(ctx, t, pool, org, askDevID, true)
			}
			if tc.legacy {
				pgseed.OrgOverride(ctx, t, pool, org, legacyID, true)
			}
			got, err := store.Lookup(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			if want := tc.askDev || tc.legacy; got.AgentContextRuntime != want {
				t.Fatalf("AgentContextRuntime = %v, want %v", got.AgentContextRuntime, want)
			}
			wantBasis := map[[2]bool]string{{false, false}: "none", {true, false}: "ask_dev", {false, true}: "agent_context_runtime", {true, true}: "both"}[[2]bool{tc.askDev, tc.legacy}]
			if got.ACRBasis != wantBasis {
				t.Fatalf("ACRBasis = %q, want %q", got.ACRBasis, wantBasis)
			}
		})
	}
}

//go:build integration

package admin

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-3695: the self-service acr_service_model toggle's ACR precondition is
// ask_dev OR the legacy agent_context_runtime grant, through the real state
// load on migrated tables. The refusal reason keeps its wire string.
func TestServiceModelACRPreconditionIsAskDevOrTheLegacyGrant(t *testing.T) {
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
	pgschema.Apply(ctx, t, pool)

	var legacyID string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = 'agent_context_runtime'`).Scan(&legacyID); err != nil {
		t.Fatalf("the migrations register agent_context_runtime: %v", err)
	}
	askDevID, serviceModelID := uuid.NewString(), uuid.NewString()
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key IN ('ask_dev', 'acr_service_model')`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, askDevID, "ask_dev", "community", true)
	pgseed.FeatureFlag(ctx, t, pool, serviceModelID, "acr_service_model", "community", true)

	h := &handlers{store: pgStore{Pool: pool, Now: func() time.Time { return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC) }}}
	for _, tc := range []struct{ askDev, legacy bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("ask_dev=%v agent_context_runtime=%v", tc.askDev, tc.legacy), func(t *testing.T) {
			org := uuid.New()
			pgseed.Org(ctx, t, pool, org.String(), "team")
			if tc.askDev {
				pgseed.OrgOverride(ctx, t, pool, org.String(), askDevID, true)
			}
			if tc.legacy {
				pgseed.OrgOverride(ctx, t, pool, org.String(), legacyID, true)
			}
			state, err := h.loadServiceModelState(ctx, pool, org, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := tc.askDev || tc.legacy
			if state.acrAllowed != want {
				t.Fatalf("acrAllowed = %v, want %v", state.acrAllowed, want)
			}
			wantBasis := map[[2]bool]string{{false, false}: "none", {true, false}: "ask_dev", {false, true}: "agent_context_runtime", {true, true}: "both"}[[2]bool{tc.askDev, tc.legacy}]
			if state.acrBasis != wantBasis {
				t.Fatalf("acrBasis = %q, want %q", state.acrBasis, wantBasis)
			}
			if wantReason := map[bool]string{true: "", false: "agent_context_runtime_required"}[want]; state.ineligibleReason() != wantReason {
				t.Fatalf("ineligibleReason = %q, want %q", state.ineligibleReason(), wantReason)
			}
		})
	}
}

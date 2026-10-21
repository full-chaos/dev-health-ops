//go:build integration

package llmorgsettings

import (
	"context"
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedServiceModelFlags(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (fallbackFlag string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key IN ('acr_service_model', 'platform_model_fallback')`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), ACRServiceModelFeatureKey, "community", true)
	fallbackFlag = uuid.NewString()
	pgseed.FeatureFlag(ctx, t, pool, fallbackFlag, PlatformModelFallbackFeatureKey, "community", true)
	return fallbackFlag
}

// An org admin turned the saved BYO config off: explain treats the org as one
// with no usable own model. With the platform_model_fallback grant it runs on
// the platform model; without it it is refused own_model_turned_off. A closed
// byo_llm gate still wins, and an org with the toggle on keeps its own model.
// The explain readers resolve no org provider while it is off.
func TestExplainGateTreatsATurnedOffOwnModelAsNoOwnModel(t *testing.T) {
	ctx := context.Background()
	store, pool := newTestStore(t)
	seedByoLLMFeature(ctx, t, pool, "team", true)
	fallbackFlag := seedServiceModelFlags(ctx, t, pool)
	seed := func(tier, toggle string, fallback bool) uuid.UUID {
		orgID := seedOrg(ctx, t, pool, tier)
		insertSetting(ctx, t, pool, orgID, "provider", "anthropic")
		insertSetting(ctx, t, pool, orgID, "model", "big-model")
		insertEncryptedSetting(ctx, t, store, pool, orgID, "api_key", "sk-default")
		if toggle != "" {
			insertSetting(ctx, t, pool, orgID, "own_model_enabled", toggle)
		}
		if fallback {
			pgseed.OrgOverride(ctx, t, pool, orgID.String(), fallbackFlag, true)
		}
		return orgID
	}
	explain := store.ForPurpose(PurposeExplain)
	gate := Gate{Source: PgSource{Licensing: licensing.PostgresStore{Pool: pool}, Queryer: pool, Config: explain}}
	for _, tc := range []struct {
		name   string
		org    uuid.UUID
		kind   Kind
		code   string
		status int
	}{
		{name: "turned off, no fallback grant", org: seed("enterprise", "false", false), kind: Refuse, code: "own_model_turned_off", status: http.StatusForbidden},
		{name: "turned off with the fallback grant", org: seed("enterprise", "false", true), kind: Platform},
		{name: "turned off behind a closed gate", org: seed("community", "false", true), kind: Refuse, code: CodeOwnModelGateClosed, status: http.StatusForbidden},
		{name: "turned on", org: seed("enterprise", "true", false), kind: Own},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := gate.Decide(ctx, tc.org.String())
			if d.Kind != tc.kind || d.Code != tc.code || d.Status != tc.status {
				t.Fatalf("decision = %+v, want kind %d code %q status %d", d, tc.kind, tc.code, tc.status)
			}
		})
	}
	t.Run("explain readers resolve no org provider while it is off", func(t *testing.T) {
		orgID := seed("enterprise", "false", true)
		provider, err := explain.ResolveUsableProvider(ctx, orgID.String())
		if err != nil || provider != "" {
			t.Fatalf("ResolveUsableProvider = %q, %v; want no org provider", provider, err)
		}
		if _, ok, err := explain.Credentials(ctx, orgID.String(), "anthropic"); err != nil || ok {
			t.Fatalf("Credentials ok = %t, %v; want none", ok, err)
		}
	})
}

//go:build integration

package admin

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The explain model gate and the org LLM status route read the same saved rows and must agree on
// whether they can serve a call: the gate admits the own model exactly when the status route reports
// active, and a gate refusal of a saved config carries the status route's reason.
func TestExplainGateAndLLMStatusRouteAgreeOnSavedConfigs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	if _, err := pool.Exec(ctx, `DELETE FROM feature_flags WHERE key = 'byo_llm'`); err != nil {
		t.Fatal(err)
	}
	pgseed.FeatureFlag(ctx, t, pool, uuid.NewString(), "byo_llm", "team", true)

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue("test-master-key"), "")
	if err != nil {
		t.Fatal(err)
	}
	h := &handlers{store: pgStore{Pool: pool}, decryptor: decryptor}
	gate := llmorgsettings.Gate{Source: llmorgsettings.PgSource{
		Licensing: licensing.PostgresStore{Pool: pool},
		Queryer:   pool,
		Config:    llmorgsettings.Store{Pool: pool, Decryptor: decryptor}.ForPurpose(llmorgsettings.PurposeExplain),
	}}

	for _, shape := range []struct {
		name                      string
		provider, apiKey, baseURL string
	}{
		{"complete", "anthropic", "k", ""},
		{"no key", "openai", "", ""},
		{"empty key with a url", "anthropic", "", "https://api.example.com/v1"},
		{"base url refused", "openai", "k", "http://169.254.169.254/v1"},
		{"unknown provider", "no-such-provider", "k", ""},
		{"auto", "auto", "k", ""},
		{"mock", "mock", "", ""},
	} {
		t.Run(shape.name, func(t *testing.T) {
			orgID := uuid.NewString()
			pgseed.Org(ctx, t, pool, orgID, "team")
			for key, value := range map[string]string{"provider": shape.provider, "api_key": shape.apiKey, "base_url": shape.baseURL} {
				if value != "" {
					pgseed.Setting(ctx, t, pool, orgID, "llm", key, value, false)
				}
			}
			status, err := h.evaluateOrgLLMStatus(ctx, orgID)
			if err != nil {
				t.Fatal(err)
			}
			decision := gate.Decide(ctx, orgID)
			if own := decision.Kind == llmorgsettings.Own; own != status.Active {
				t.Fatalf("gate decision %+v, status route active=%t reason=%q", decision, status.Active, status.ReasonCode)
			}
			if status.Configured && !status.Active && decision.Code != status.ReasonCode {
				t.Fatalf("gate detail %q, status route reason %q", decision.Code, status.ReasonCode)
			}
		})
	}
}

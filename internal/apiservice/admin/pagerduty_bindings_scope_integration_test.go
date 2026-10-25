//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestPagerDutyBindingOrganizationScope(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pd-binding-scope-32-b"
	const prefix = "/api/v1/admin/integrations/pagerduty/webhook-bindings"

	orgMain, orgOther := uuid.New(), uuid.New()
	adminID := uuid.New()
	srcMain, srcOther := uuid.New(), uuid.New()
	credMain, credOther := uuid.New(), uuid.New()
	intMain, intOther := uuid.New(), uuid.New()
	bindMain, bindOther := uuid.New(), uuid.New()
	unknown := uuid.New()
	srcNoCred, intNoCred := uuid.New(), uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for slug, org := range map[string]uuid.UUID{"main": orgMain, "other": orgOther} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "pd-scope-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'pd-scope-admin@example.com', true, true, false, 0, now(), now())`, adminID)
			for _, row := range []struct{ org, cred, integ, src, bind uuid.UUID }{
				{orgMain, credMain, intMain, srcMain, bindMain},
				{orgOther, credOther, intOther, srcOther, bindOther},
			} {
				exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, true, NULL, '{}'::json, now(), now())`, row.cred, row.org.String(), "cred-"+row.cred.String())
				exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, credential_id, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, '{}'::json, true, $4, now(), now())`, row.integ, row.org.String(), "int-"+row.integ.String(), row.cred)
				exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'pagerduty', 'service', $4, $4, $4, '{}'::json, true, now(), now())`, row.src, row.org.String(), row.integ, "src-"+row.src.String())
				exec(`INSERT INTO pagerduty_webhook_bindings (id, org_id, integration_source_id, credential_id, provider_subscription_id, signing_secret_encrypted, signing_secret_key_version, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, 'v1:seed-not-decrypted', 'v1', 'active', now(), now())`, row.bind, row.org, row.src, row.cred, "sub-"+row.bind.String())
			}
			exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, credential_id, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'int-no-credential', '{}'::json, true, NULL, now(), now())`, intNoCred, orgMain.String())
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'pagerduty', 'service', 'src-no-credential', 'src-no-credential', 'src-no-credential', '{}'::json, true, now(), now())`, srcNoCred, orgMain.String(), intNoCred)
			return map[string]map[string]any{
				"admin": {"user_id": adminID.String(), "email": "pd-scope-admin@example.com", "org_id": orgMain.String(), "role": "admin"},
			}
		},
	})
	base, pool := startGoServer(t, ctx, venue, jwtKey)
	headers := map[string]string{"Authorization": "Bearer " + venue.Tokens["admin"]}
	body := func(source, credential uuid.UUID) string {
		return fmt.Sprintf(`{"integration_source_id":"%s","credential_id":"%s","provider_subscription_id":"sub-x","signing_secret":"s"}`, source, credential)
	}
	post := func(path, payload string) routeResponse {
		return doJSONRequest(t, base, http.MethodPost, path, payload, headers)
	}
	equal := func(name string, missing, other routeResponse) {
		t.Helper()
		if missing.status != other.status || missing.body != other.body {
			t.Errorf("%s: missing id answered %d %s, other id answered %d %s", name, missing.status, missing.body, other.status, other.body)
		}
	}

	t.Run("create answers the same for a missing and an other-org graph", func(t *testing.T) {
		equal("source", post(prefix, body(unknown, credMain)), post(prefix, body(srcOther, credMain)))
		equal("credential", post(prefix, body(srcMain, unknown)), post(prefix, body(srcMain, credOther)))
		equal("both", post(prefix, body(unknown, unknown)), post(prefix, body(srcOther, credOther)))
		equal("integration without a credential", post(prefix, body(unknown, credMain)), post(prefix, body(srcNoCred, credMain)))
		if resp := post(prefix, body(srcNoCred, credMain)); resp.status != http.StatusBadRequest {
			t.Errorf("integration without a credential: %d %s", resp.status, resp.body)
		}
	})
	t.Run("rotate answers the same for a missing and an other-org binding", func(t *testing.T) {
		equal("binding", post(prefix+"/"+unknown.String()+"/rotate", body(srcMain, credMain)), post(prefix+"/"+bindOther.String()+"/rotate", body(srcMain, credMain)))
		equal("graph", post(prefix+"/"+bindMain.String()+"/rotate", body(unknown, credMain)), post(prefix+"/"+bindMain.String()+"/rotate", body(srcOther, credMain)))
	})
	t.Run("revoke answers the same for a missing and an other-org binding", func(t *testing.T) {
		equal("binding", post(prefix+"/"+unknown.String()+"/revoke", ""), post(prefix+"/"+bindOther.String()+"/revoke", ""))
		var status string
		if err := pool.QueryRow(ctx, `SELECT status FROM pagerduty_webhook_bindings WHERE id = $1`, bindOther).Scan(&status); err != nil || status != "active" {
			t.Errorf("other-org binding after a revoke attempt: status=%q err=%v", status, err)
		}
	})

	holdOtherOrgLocks := func(t *testing.T) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
		if _, err := tx.Exec(ctx, `SELECT id FROM pagerduty_webhook_bindings WHERE org_id = $1 FOR UPDATE`, orgOther); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))`, srcOther.String()); err != nil {
			t.Fatal(err)
		}
	}
	timed := func(t *testing.T, path, payload string) routeResponse {
		t.Helper()
		done := make(chan routeResponse, 1)
		go func() { done <- post(path, payload) }()
		select {
		case resp := <-done:
			return resp
		case <-time.After(5 * time.Second):
			t.Fatalf("%s blocked on a other-org lock", path)
			return routeResponse{}
		}
	}
	t.Run("an other-org id takes no lock on rotate", func(t *testing.T) {
		holdOtherOrgLocks(t)
		resp := timed(t, prefix+"/"+bindOther.String()+"/rotate", body(srcMain, credMain))
		if resp.status != http.StatusNotFound {
			t.Errorf("rotate with a other-org id: %d %s", resp.status, resp.body)
		}
	})
	t.Run("an other-org id takes no lock on revoke", func(t *testing.T) {
		holdOtherOrgLocks(t)
		resp := timed(t, prefix+"/"+bindOther.String()+"/revoke", "")
		if resp.status != http.StatusNotFound {
			t.Errorf("revoke with a other-org id: %d %s", resp.status, resp.body)
		}
	})
	venueoracle.WriteGoOnlyProof(t, "Go-only rule test: every binding lookup is filtered by the caller's organization and a foreign id takes no lock; there is no Python answer to compare, the answers and locks are asserted directly")
}

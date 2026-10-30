//go:build integration

package admin_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestPagerDutyDisconnectPendingRevocations covers CHAOS-12011 and the shared claim
// rule of CHAOS-12010 on the Go plane only: a disconnect of a credential whose
// integration_credentials row is gone still sends the credential's pending
// revocation rows, and a row another sender holds is skipped, never waited on or
// sent twice.
func TestPagerDutyDisconnectPendingRevocations(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-pd-pending-revocations-32"

	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(pagerDutyDisconnectVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build cipher: %v", err)
	}
	seal := func(token string) string {
		sealed, err := cipher.Encrypt([]byte(token))
		if err != nil {
			t.Fatalf("encrypt: %v", err)
		}
		return sealed.Reveal()
	}

	fake := &pagerDutyDisconnectFakeServer{failTokens: map[string]bool{"pd-pending-fails-token": true}}
	fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(fakeServer.Close)

	slugs := []string{"missing-ok", "missing-fails", "missing-empty", "missing-noclient", "held"}
	orgs := map[string]uuid.UUID{}
	for _, slug := range slugs {
		orgs[slug] = uuid.MustParse(venueoracle.StableUUID("pd-pending-org-" + slug))
	}
	adminID := uuid.MustParse(venueoracle.StableUUID("pd-pending-admin"))
	heldRow := uuid.MustParse(venueoracle.StableUUID("pd-pending-held-row"))

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
			for slug, id := range orgs {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, id, "pd-pending-"+slug)
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'pd-pending-admin@example.com', true, true, false, 0, now(), now())`, adminID)
			revocation := func(id uuid.UUID, org uuid.UUID, name, token string) {
				exec(`INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, 'disconnect', $4, 'v1', 'pending', 0, now() - interval '1 day', now())`, id, org.String(), name, seal(token))
			}
			// No integration_credentials row in the missing-* orgs: the credential was
			// connected after a restore point (11835 requeued_blocked) or removed.
			revocation(uuid.New(), orgs["missing-ok"], "default", "pd-pending-ok-token")
			revocation(uuid.New(), orgs["missing-ok"], "other", "pd-pending-other-token")
			revocation(uuid.New(), orgs["missing-fails"], "default", "pd-pending-fails-token")
			revocation(uuid.New(), orgs["missing-noclient"], "default", "pd-pending-noclient-token")
			exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'default', true, NULL, '{"auth_mode":"oauth","region":"us","subdomain":"acme"}'::json, now(), now())`, uuid.New(), orgs["held"].String())
			revocation(heldRow, orgs["held"], "default", "pd-pending-held-token")
			tokens := map[string]map[string]any{}
			for slug, id := range orgs {
				tokens[slug] = map[string]any{"user_id": adminID.String(), "email": "pd-pending-admin@example.com", "org_id": id.String(), "role": "admin"}
			}
			return tokens
		},
	})

	configured := func(deps *apiservice.Deps) {
		deps.Decryptor = cipher
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: pagerDutyDisconnectVenueClientID, RevokeURL: fakeServer.URL}
	}
	base, pool := startGoServer(t, ctx, venue, jwtKey, configured)
	noClientBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = cipher
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{RevokeURL: fakeServer.URL}
	})

	disconnect := func(base, slug string) (int, string) {
		t.Helper()
		requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, base+"/api/v1/admin/integrations/pagerduty/disconnect", strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+venue.Tokens[slug])
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("disconnect %s: %v (a held row must be skipped, not waited on)", slug, err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return response.StatusCode, string(body)
	}
	pending := func(slug, name string) (rows, attempts int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*), COALESCE(max(attempts), 0) FROM provider_oauth_revocations WHERE org_id = $1 AND credential_name = $2`,
			orgs[slug].String(), name).Scan(&rows, &attempts); err != nil {
			t.Fatal(err)
		}
		return rows, attempts
	}
	calls := func(token string) int {
		fake.mu.Lock()
		defer fake.mu.Unlock()
		n := 0
		for _, call := range fake.calls {
			if call == token {
				n++
			}
		}
		return n
	}

	t.Run("missing credential with a pending row sends it", func(t *testing.T) {
		status, body := disconnect(base, "missing-ok")
		if status != http.StatusOK || !strings.Contains(body, `"disconnected":true`) {
			t.Fatalf("status %d body %s, want 200 disconnected", status, body)
		}
		if rows, _ := pending("missing-ok", "default"); rows != 0 {
			t.Fatalf("pending rows %d, want 0: the row was not sent", rows)
		}
		if calls("pd-pending-ok-token") != 1 {
			t.Fatalf("revoke calls %d, want 1", calls("pd-pending-ok-token"))
		}
		if rows, _ := pending("missing-ok", "other"); rows != 1 || calls("pd-pending-other-token") != 0 {
			t.Fatalf("another credential's row was touched: rows %d calls %d", rows, calls("pd-pending-other-token"))
		}
	})

	t.Run("missing credential with a refused revoke answers pending retry", func(t *testing.T) {
		status, body := disconnect(base, "missing-fails")
		if status != http.StatusServiceUnavailable || !strings.Contains(body, "pending retry") {
			t.Fatalf("status %d body %s, want 503 pending retry", status, body)
		}
		if rows, attempts := pending("missing-fails", "default"); rows != 1 || attempts != 1 {
			t.Fatalf("rows %d attempts %d, want 1 row at 1 attempt", rows, attempts)
		}
	})

	t.Run("missing credential with nothing pending stays 200", func(t *testing.T) {
		before := fake.count()
		status, body := disconnect(base, "missing-empty")
		if status != http.StatusOK || !strings.Contains(body, `"disconnected":true`) {
			t.Fatalf("status %d body %s, want 200 disconnected", status, body)
		}
		if fake.count() != before {
			t.Fatalf("a disconnect with nothing pending called the provider")
		}
	})

	t.Run("missing credential without a client id answers pending retry", func(t *testing.T) {
		status, body := disconnect(noClientBase, "missing-noclient")
		if status != http.StatusServiceUnavailable || !strings.Contains(body, "pending retry") {
			t.Fatalf("status %d body %s, want 503 pending retry", status, body)
		}
		if rows, attempts := pending("missing-noclient", "default"); rows != 1 || attempts != 0 || calls("pd-pending-noclient-token") != 0 {
			t.Fatalf("rows %d attempts %d calls %d, want the row untouched", rows, attempts, calls("pd-pending-noclient-token"))
		}
	})

	t.Run("a row another sender holds is skipped and sent once", func(t *testing.T) {
		holder, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var held uuid.UUID
		if err := holder.QueryRow(ctx, `SELECT id FROM provider_oauth_revocations WHERE id = $1 FOR UPDATE`, heldRow).Scan(&held); err != nil {
			_ = holder.Rollback(ctx)
			t.Fatal(err)
		}
		status, body := disconnect(base, "held")
		_ = holder.Rollback(ctx)
		if status != http.StatusServiceUnavailable {
			t.Fatalf("status %d body %s, want 503 while another sender holds the row", status, body)
		}
		if calls("pd-pending-held-token") != 0 {
			t.Fatalf("the held row was sent by a second sender")
		}
		if rows, attempts := pending("held", "default"); rows != 1 || attempts != 0 {
			t.Fatalf("rows %d attempts %d, want the held row untouched", rows, attempts)
		}
		status, body = disconnect(base, "held")
		if status != http.StatusOK {
			t.Fatalf("status %d body %s, want 200 once the row is free", status, body)
		}
		if calls("pd-pending-held-token") != 1 {
			t.Fatalf("revoke calls %d, want exactly 1", calls("pd-pending-held-token"))
		}
	})
	venueoracle.WriteGoOnlyProof(t, "Go-only rule test: a disconnect of a missing credential sends its pending revocation rows (Go-only rows, a named divergence) and a row another sender holds is skipped and sent once; there is no Python answer to compare, the answers, rows and provider calls are asserted directly")
}

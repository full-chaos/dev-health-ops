//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// orgDeletionVenueEncryptionKey/Salt are a fixed, throwaway key for this
// test only -- both planes must agree on it (Python's SETTINGS_ENCRYPTION_KEY
// env var, Go's providerfoundation.FernetDecryptor), never a real secret.
const (
	orgDeletionVenueEncryptionKey = "venue-orgdeletion-pagerduty-fernet-key-32-bytes"
	orgDeletionVenuePagerDutyID   = "venue-pagerduty-client-id"
)

// fakePagerDutyRevokeServer records every revoke POST it receives (form
// body: token, client_id) so a test can assert both planes actually called
// it, not just that a credential row disappeared.
type fakePagerDutyRevokeServer struct {
	mu    sync.Mutex
	calls []string // client_id per call
}

func (f *fakePagerDutyRevokeServer) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	values, _ := url.ParseQuery(string(body))
	f.mu.Lock()
	f.calls = append(f.calls, values.Get("client_id"))
	f.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (f *fakePagerDutyRevokeServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// TestOrgDeletionRevokesPagerDutyOnBothPlanes is CHAOS-6306 approval
// condition 2 (ruling 27): no production Python change for a test seam.
// The venue Python runner (venueoracle.go's own pythonProgram, testdata
// side, gated on an env var no production code ever sets) monkeypatches
// PagerDutyOAuthConfig.from_env() to point revoke_url at a fake local
// server; the Go plane's admin.Deps.PagerDuty.RevokeURL points at the
// SAME server. A live PagerDuty credential, encrypted the way the real
// Python encrypt_value does (via venue.CallPython, never a hand-rolled
// ciphertext), is seeded for a target org and a control org; a real
// (non-dry-run) delete is sent to BOTH planes, and this test asserts the
// fake server saw exactly one revoke call per plane and that the
// credential row is gone for the target org, present for the control org,
// on both planes.
func TestOrgDeletionRevokesPagerDutyOnBothPlanes(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-deletion-pd-flow-32-byt"

	fake := &fakePagerDutyRevokeServer{}
	fakeServer := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(fakeServer.Close)

	targetOrgID := uuid.New()
	controlOrgID := uuid.New()
	superID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		PythonEnv: []string{
			"PAGER_DUTY_CLIENT_ID=" + orgDeletionVenuePagerDutyID,
			"SETTINGS_ENCRYPTION_KEY=" + orgDeletionVenueEncryptionKey,
			"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE=" + fakeServer.URL,
		},
		Seed: func(t *testing.T, ctx context.Context, adminPool *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := adminPool.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for _, org := range []uuid.UUID{targetOrgID, controlOrgID} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "venue-pd-"+org.String()[:8])
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-pd-super@example.com', true, true, true, 0, now(), now())`, superID)

			// Encrypt a real OAuthTokens payload the way the Python api
			// itself does (dev_health_ops.core.encryption.encrypt_value),
			// through the live interpreter -- never a hand-rolled
			// ciphertext, so a real Decrypt (Python or Go) is what this
			// test actually exercises.
			plaintext := `{"access_token":"venue-pd-access-token","refresh_token":"venue-pd-refresh-token","expires_at":"2099-01-01T00:00:00Z","granted_scopes":[]}`
			results := v.CallPython(t, venueoracle.PythonCall{
				Target: "dev_health_ops.core.encryption:encrypt_value",
				Args:   []any{plaintext},
			})
			var ciphertext string
			if err := json.Unmarshal(results[0], &ciphertext); err != nil {
				t.Fatalf("decode encrypt_value result: %v\n%s", err, results[0])
			}

			for _, org := range []uuid.UUID{targetOrgID, controlOrgID} {
				exec(`INSERT INTO provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, has_refresh_token)
VALUES ($1, 'pagerduty', 'default', $2, 1, now(), now(), true)`, org.String(), ciphertext)
			}

			return map[string]map[string]any{
				"super": {"user_id": superID.String(), "email": "venue-pd-super@example.com", "is_superuser": true},
			}
		},
	})

	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(orgDeletionVenueEncryptionKey), "")
	if err != nil {
		t.Fatalf("build decryptor: %v", err)
	}

	bearer := "Bearer " + venue.Tokens["super"]
	requests := []venueoracle.Request{
		{Name: "delete org real (pagerduty revoke)", Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + targetOrgID.String(), Headers: map[string]string{"Authorization": bearer}},
	}
	python := venue.ServePython(t, requests)
	goBase, _ := startGoServer(t, ctx, venue, jwtKey, func(deps *apiservice.Deps) {
		deps.Decryptor = decryptor
		deps.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: orgDeletionVenuePagerDutyID, RevokeURL: fakeServer.URL}
	})

	receipt := venueoracle.Diff(t, goBase, requests, python, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			return redactField(t, body, "timestamp")
		},
	})
	t.Log(receipt)

	if got := fake.count(); got != 2 {
		t.Errorf("fake pagerduty revoke server saw %d calls, want 2 (one per plane)", got)
	}

	credentialQuery := func(orgID uuid.UUID) string {
		return "SELECT count(*) FROM provider_oauth_credentials WHERE org_id = '" + orgID.String() + "' AND provider = 'pagerduty'"
	}
	for _, check := range []struct {
		label string
		org   uuid.UUID
		want  string
	}{
		{"target", targetOrgID, "0"},
		{"control", controlOrgID, "1"},
	} {
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), credentialQuery(check.org))
		got := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), credentialQuery(check.org))
		if source != got {
			t.Errorf("pagerduty credential row count differs for %s org:\n python: %s\n go:     %s", check.label, source, got)
		}
		if got != check.want {
			t.Errorf("pagerduty credential row count for %s org = %s, want %s", check.label, got, check.want)
		}
	}
}

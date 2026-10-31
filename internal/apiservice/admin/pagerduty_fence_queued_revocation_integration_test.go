//go:build integration

package admin

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// A row in the shape the restore fence queues from a deleted credential (the stored OAuthTokens JSON sealed as is, key
// version oauth-v1) is sent by the disconnect of its name as its refresh_token, never as the JSON. A row of an unknown
// key version is not sent: it stays pending with one more attempt.
func TestTheDisconnectDecodesARowTheFenceQueuedFromACredential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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

	key := strings.Join([]string{"fence", "queued", "fernet", "key", "for", "tests", "only"}, "-")
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	fake := &requeuedRevokeServer{}
	server := httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(server.Close)
	h := &handlers{
		store:     pgStore{Pool: pool},
		logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		decryptor: decryptor,
		pagerDuty: providerfoundation.PagerDutyRevokeConfig{ClientID: "fence-queued-client", RevokeURL: server.URL},
		httpDoer:  server.Client(),
	}
	org := "org-fence-queued"
	refresh := strings.Join([]string{"fence", "queued", "refresh", "4b7d"}, "-")
	stored := `{"access_token":"fence-queued-access-1","refresh_token":"` + refresh + `","expires_at":"2026-01-01T00:00:00Z","granted_scopes":["read"]}`
	insert := func(credential, plaintext, keyVersion string) uuid.UUID {
		t.Helper()
		sealed, err := decryptor.Encrypt([]byte(plaintext))
		if err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, 'disconnect', $4, $5, 'pending', 0, NULL, clock_timestamp(), clock_timestamp())`, id, org, credential, sealed.Reveal(), keyVersion); err != nil {
			t.Fatal(err)
		}
		return id
	}
	queued := insert("queued", stored, providerfoundation.RevocationKeyOAuth)
	unknown := insert("unknown", "fence-unknown-token-2e9c", "v9")

	done, err := h.disconnectPagerDutyCredential(ctx, org, "queued")
	if err != nil || done == nil || !*done {
		t.Fatalf("disconnect = %v, %v; want a completed drain", done, err)
	}
	if fake.count(refresh) != 1 || fake.got(stored) {
		t.Errorf("sent %v; want the refresh_token once, never the JSON", fake.tokens)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.provider_oauth_revocations WHERE id = $1`, queued).Scan(&n); err != nil || n != 0 {
		t.Errorf("the sent row is still there (%d, %v)", n, err)
	}

	if _, err := h.disconnectPagerDutyCredential(ctx, org, "unknown"); err != nil {
		t.Fatalf("disconnect of the unknown-version row: %v", err)
	}
	if fake.got("fence-unknown-token-2e9c") {
		t.Error("a row of an unknown key version was sent")
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT attempts FROM public.provider_oauth_revocations WHERE id = $1 AND status = 'pending'`, unknown).Scan(&attempts); err != nil || attempts != 1 {
		t.Errorf("the unknown-version row: attempts %d, %v; want pending at 1", attempts, err)
	}
}

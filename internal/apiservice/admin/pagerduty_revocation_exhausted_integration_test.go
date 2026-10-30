//go:build integration

package admin

import (
	"bytes"
	"context"
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

// A route sender (the disconnect or callback retry, the setup drain, the callback's own compensating revoke) that
// pushes a row to the worker's attempt limit names it once: one WARN line with the row id and the reason, and none
// for a later refusal of the same row. The worker no longer claims such a row, so this line is where it is named.
func TestARouteSenderNamesARowItPushesToTheAttemptLimitOnce(t *testing.T) {
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

	key := strings.Join([]string{"exhausted", "route", "fernet", "key", "for", "tests", "only", "x"}, "-")
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(refusing.Close)
	logs := &bytes.Buffer{}
	h := &handlers{
		store:     pgStore{Pool: pool},
		logger:    slog.New(slog.NewJSONHandler(logs, nil)),
		decryptor: cipher,
		pagerDuty: providerfoundation.PagerDutyRevokeConfig{ClientID: "exhausted-route-client", RevokeURL: refusing.URL},
		httpDoer:  refusing.Client(),
	}
	org := uuid.NewString()
	seed := func(purpose, name, token string, attempts int) uuid.UUID {
		t.Helper()
		sealed, err := cipher.Encrypt([]byte(token))
		if err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, $4, $5, 'v1', 'pending', $6, now() - interval '1 day', now() - interval '1 day')`,
			id, org, name, purpose, sealed.Reveal(), attempts); err != nil {
			t.Fatal(err)
		}
		return id
	}
	named := func(id uuid.UUID) int {
		n := 0
		for _, record := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			if strings.Contains(record, `"level":"WARN"`) && strings.Contains(record, "provider_oauth_revocations.exhausted") &&
				strings.Contains(record, id.String()) && strings.Contains(record, `"reason":"remote_revoke_failed"`) {
				n++
			}
		}
		return n
	}
	attempts := func(id uuid.UUID) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT attempts FROM provider_oauth_revocations WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	// The disconnect/callback retry of a credential name.
	disconnectRow := seed("disconnect", "default", "exhausted-disconnect-token", pagerDutyRevocationMaxAttempts-1)
	for range 2 {
		if _, err := h.retryPagerDutyRevocations(ctx, org, "default"); err != nil {
			t.Fatal(err)
		}
	}
	if got := attempts(disconnectRow); got != pagerDutyRevocationMaxAttempts+1 {
		t.Fatalf("disconnect row attempts %d, want %d", got, pagerDutyRevocationMaxAttempts+1)
	}
	if n := named(disconnectRow); n != 1 {
		t.Errorf("the retry path named the row it pushed to the limit %d time(s), want exactly once:\n%s", n, logs.String())
	}

	// The callback's own compensating revoke of its setup token.
	setupRow := seed("setup", "", "exhausted-setup-token", pagerDutyRevocationMaxAttempts-1)
	tokens := providerfoundation.PagerDutyOAuthTokens{AccessToken: "exhausted-setup-token"}
	h.revokePagerDutySetupToken(ctx, setupRow, tokens)
	if got := attempts(setupRow); got != pagerDutyRevocationMaxAttempts {
		t.Fatalf("setup row attempts %d, want %d", got, pagerDutyRevocationMaxAttempts)
	}
	if n := named(setupRow); n != 1 {
		t.Errorf("the compensating revoke named the row it pushed to the limit %d time(s), want exactly once:\n%s", n, logs.String())
	}
	for _, secret := range []string{"exhausted-disconnect-token", "exhausted-setup-token"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatalf("a token reached the log: %s", logs.String())
		}
	}
}

//go:build integration

package providerrevocation

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// fenceQueue copies a stored credential row into a revocation row the way the restore fence does: token_encrypted as
// stored, key version oauth-v1, pending, attempts 0.
func (c *cell) fenceQueue(t *testing.T, name, storedJSON string) uuid.UUID {
	t.Helper()
	sealed, err := c.cipher.Encrypt([]byte(storedJSON))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := c.admin.Exec(ctx, `INSERT INTO public.provider_oauth_credentials (org_id, provider, credential_name, token_encrypted, version, created_at, updated_at, binding_id)
VALUES ($1, 'pagerduty', $2, $3, 1, now(), now(), $4)`, c.org, name, sealed.Reveal(), strings.ReplaceAll(uuid.NewString(), "-", "")); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if _, err := c.admin.Exec(ctx, `INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
SELECT $1, org_id, provider, credential_name, 'disconnect', token_encrypted, 'oauth-v1', 'pending', 0, NULL, clock_timestamp(), clock_timestamp()
FROM public.provider_oauth_credentials WHERE org_id = $2 AND credential_name = $3`, id, c.org, name); err != nil {
		t.Fatal(err)
	}
	return id
}

// A row the fence queued from a stored credential is due at the first run and sent as its refresh_token (the access
// token when there is none). One PagerDuty answers 400 to every time gains one attempt per due run, stops at
// MaxAttempts, is named, and is never claimed again.
func TestWorkerSendsAFenceQueuedCredentialRowAndEndsARefusedOne(t *testing.T) {
	c := newCell(t)
	var mu sync.Mutex
	calls := map[string]int{}
	refuse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		values, _ := url.ParseQuery(string(body))
		mu.Lock()
		calls[values.Get("token")]++
		mu.Unlock()
		if values.Get("token") == "fq-refresh-400" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(refuse.Close)
	count := func(token string) int { mu.Lock(); defer mu.Unlock(); return calls[token] }
	revokeConfig := providerfoundation.PagerDutyRevokeConfig{ClientID: integrationClientID, RevokeURL: refuse.URL}
	logs := &bytes.Buffer{}
	handler, err := NewHandler(PgStore{DB: c.worker}, c.cipher, func(ctx context.Context, token string) error {
		return providerfoundation.RevokePagerDutyOAuthToken(ctx, &http.Client{}, revokeConfig, token)
	}, true, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	sent := c.fenceQueue(t, "fq-sent", `{"access_token":"fq-access-1","refresh_token":"fq-refresh-1","expires_at":"2026-01-01T00:00:00Z","granted_scopes":["read"]}`)
	accessOnly := c.fenceQueue(t, "fq-access", `{"access_token":"fq-access-2","refresh_token":null,"expires_at":"2026-01-01T00:00:00Z","granted_scopes":["read"]}`)
	refused := c.fenceQueue(t, "fq-refused", `{"access_token":"fq-access-3","refresh_token":"fq-refresh-400","expires_at":"2026-01-01T00:00:00Z","granted_scopes":["read"]}`)

	c.run(t, handler)
	if c.state(t, sent).present || count("fq-refresh-1") != 1 || count("fq-access-1") != 0 {
		t.Fatalf("the queued row was not sent once as its refresh_token: %+v %v", c.state(t, sent), calls)
	}
	if c.state(t, accessOnly).present || count("fq-access-2") != 1 {
		t.Fatalf("the access-only row was not sent as its access_token: %v", calls)
	}
	for run := 1; run < MaxAttempts; run++ {
		c.age(t, refused, 7*time.Hour)
		c.run(t, handler)
	}
	if s := c.state(t, refused); !s.present || s.attempts != MaxAttempts || s.lastError != ReasonRemoteRevokeFailed || count("fq-refresh-400") != MaxAttempts {
		t.Fatalf("refused row %+v after %d calls; want pending at %d, remote_revoke_failed", s, count("fq-refresh-400"), MaxAttempts)
	}
	c.age(t, refused, 7*time.Hour)
	c.run(t, handler)
	if count("fq-refresh-400") != MaxAttempts {
		t.Fatalf("an exhausted row was claimed again: %d calls", count("fq-refresh-400"))
	}
	if !strings.Contains(logs.String(), `"revocation_id":"`+refused.String()+`"`) {
		t.Fatalf("the exhausted row is not named: %s", logs.String())
	}
	for _, leak := range []string{"fq-refresh-400", "fq-access-3", "access_token"} {
		if strings.Contains(logs.String(), leak) {
			t.Fatalf("the log carries %q", leak)
		}
	}
}

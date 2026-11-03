//go:build integration

package admin

import (
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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

type requeuedRevokeServer struct {
	mu     sync.Mutex
	tokens []string
}

func (s *requeuedRevokeServer) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	values, _ := url.ParseQuery(string(body))
	s.mu.Lock()
	s.tokens = append(s.tokens, values.Get("token"))
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *requeuedRevokeServer) got(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, t := range s.tokens {
		if t == token {
			return true
		}
	}
	return false
}

func (s *requeuedRevokeServer) count(token string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, t := range s.tokens {
		if t == token {
			n++
		}
	}
	return n
}

// A revocation row in the shape the restore fence re-queues from the shadow restore (same ciphertext, status pending,
// attempts 0, last_error NULL, created_at of the source) is sent by the real disconnect of its credential when the
// credential row exists, and also when the credential was connected after the restore target (no credential row,
// the fence report's blocked split): ticket 12011 makes that disconnect send the name's pending rows, and the
// revocation worker sends them too.
func TestTheDisconnectSendsARequeuedRevocationAlsoWithoutItsCredentialRow(t *testing.T) {
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

	key := strings.Join([]string{"requeue", "fence", "fernet", "key", "for", "tests", "only", "x"}, "-")
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
		pagerDuty: providerfoundation.PagerDutyRevokeConfig{ClientID: "requeue-fence-client", RevokeURL: server.URL},
		httpDoer:  server.Client(),
	}

	org := "org-requeue"
	keptToken := strings.Join([]string{"requeued", "kept", "token", "9f3a"}, "-")
	lateToken := strings.Join([]string{"requeued", "late", "token", "5c1e"}, "-")
	requeued := func(credential, token string) uuid.UUID {
		t.Helper()
		// A revocation row holds the sealed revoke token itself (pagerduty_disconnect.go seals revokeCandidate).
		sealed, err := decryptor.Encrypt([]byte(token))
		if err != nil {
			t.Fatal(err)
		}
		id := uuid.New()
		if _, err := pool.Exec(ctx, `INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, 'disconnect', $4, 'v1', 'pending', 0, NULL, '2026-10-20T00:00:00Z', clock_timestamp())`, id, org, credential, sealed.Reveal()); err != nil {
			t.Fatal(err)
		}
		return id
	}
	// The T-era credential: the fence never deletes its integration_credentials row (it retires it).
	if _, err := pool.Exec(ctx, `INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'kept', false, now(), now())`, uuid.New(), org); err != nil {
		t.Fatal(err)
	}
	keptID := requeued("kept", keptToken)
	lateID := requeued("late", lateToken)

	pending := func(id uuid.UUID) bool {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM public.provider_oauth_revocations WHERE id = $1 AND status = 'pending'`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}

	done, err := h.disconnectPagerDutyCredential(ctx, org, "kept")
	if err != nil || done == nil || !*done {
		t.Fatalf("disconnect of the credential with a row = %v, %v; want a completed drain", done, err)
	}
	if !fake.got(keptToken) {
		t.Error("the disconnect did not send the requeued revocation of its credential to PagerDuty")
	}
	if pending(keptID) {
		t.Error("the sent revocation row is still pending")
	}

	done, err = h.disconnectPagerDutyCredential(ctx, org, "late")
	if err != nil || done == nil || !*done {
		t.Fatalf("disconnect of a credential with no row = %v, %v; want a completed drain (ticket 12011)", done, err)
	}
	if n := fake.count(lateToken); n != 1 {
		t.Errorf("the requeued revocation of a credential with no row was sent %d time(s), want exactly 1", n)
	}
	if pending(lateID) {
		t.Error("the requeued revocation of a credential with no row is still pending")
	}
}

//go:build integration

package admin

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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/providerrevocation"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// barrierPagerDuty answers every revoke 200; the first revoke of hold signals inFlight and waits for release.
type barrierPagerDuty struct {
	mu       sync.Mutex
	calls    map[string]int
	hold     string
	held     bool
	inFlight chan struct{}
	release  chan struct{}
}

func (b *barrierPagerDuty) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	values, _ := url.ParseQuery(string(body))
	token := values.Get("token")
	b.mu.Lock()
	b.calls[token]++
	first := token == b.hold && !b.held
	if first {
		b.held = true
	}
	b.mu.Unlock()
	if first {
		close(b.inFlight)
		<-b.release
	}
	w.WriteHeader(http.StatusOK)
}

func (b *barrierPagerDuty) count(token string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls[token]
}

// countingDeprovision is auth-service's deprovision: the first call succeeds, a repeat answers organization_not_found
// (the organization is gone there), which lets a repeated delete go on.
type countingDeprovision struct {
	*fakeProvisioner
	calls int
}

func (p *countingDeprovision) Deprovision(ctx context.Context, org, actor uuid.UUID) error {
	p.calls++
	if p.calls > 1 {
		return refusedWith(404, "organization_not_found")
	}
	return p.fakeProvisioner.Deprovision(ctx, org, actor)
}

// Org deletion and the revocation worker claim pending revocation rows the same way (FOR UPDATE SKIP LOCKED): while the
// worker holds a row mid-send, a delete of its organization sends nothing for it, refuses (503, nothing purged) and
// leaves the row to the worker; a repeat of the delete after the worker's send completes (the auth deprovision already
// done, the row sent exactly once, the purge done). No token or ciphertext reaches a log record or the answer.
func TestOrgDeletionSkipsARevocationTheWorkerHoldsAndARetryCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close(context.Background()) })
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(ch.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Exec(ctx, `CREATE TABLE deletion_probe (org_id String, n UInt8) ENGINE = MergeTree ORDER BY org_id`); err != nil {
		t.Fatal(err)
	}

	key := strings.Join([]string{"orgdeletion", "claim", "fernet", "key", "for", "tests", "only", "x"}, "-")
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Join([]string{"orgdeletion", "held", "token", "7a2c"}, "-")
	barrier := &barrierPagerDuty{calls: map[string]int{}, hold: token, inFlight: make(chan struct{}), release: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(barrier.handle))
	t.Cleanup(server.Close)
	released := false
	releaseBarrier := func() {
		if !released {
			released = true
			close(barrier.release)
		}
	}
	t.Cleanup(releaseBarrier)
	revokeConfig := providerfoundation.PagerDutyRevokeConfig{ClientID: "orgdeletion-claim-client", RevokeURL: server.URL}

	super := uuid.New()
	pgseed.User(ctx, t, pool, super.String())
	org := uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "community")
	pgseed.Membership(ctx, t, pool, uuid.NewString(), super.String(), org.String(), "owner")
	if err := conn.Exec(ctx, `INSERT INTO deletion_probe VALUES (?, 1)`, org.String()); err != nil {
		t.Fatal(err)
	}
	sealed, err := cipher.Encrypt([]byte(token))
	if err != nil {
		t.Fatal(err)
	}
	rowID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', 'default', 'disconnect', $3, 'v1', 'pending', 0, now() - interval '1 day', now() - interval '1 day')`,
		rowID, org.String(), sealed.Reveal()); err != nil {
		t.Fatal(err)
	}

	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	deprovision := &countingDeprovision{fakeProvisioner: &fakeProvisioner{db: &provisionDB{}}}
	h := &handlers{store: pgStore{Pool: pool, Now: time.Now}, logger: logger, clickHouseDSN: ch.URI, orgProvision: deprovision,
		decryptor: cipher, pagerDuty: revokeConfig, httpDoer: server.Client()}

	pending := func() bool {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM provider_oauth_revocations WHERE id = $1`, rowID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 1
	}
	orgRows := func() (orgs int, probe uint64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM organizations WHERE id = $1`, org).Scan(&orgs); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRow(ctx, `SELECT count() FROM deletion_probe WHERE org_id = ?`, org.String()).Scan(&probe); err != nil {
			t.Fatal(err)
		}
		return orgs, probe
	}
	// deleteOrg runs the route, bounded: a delete that waits on the held row fails the cell instead of hanging it.
	deleteOrg := func(t *testing.T) *httptest.ResponseRecorder {
		t.Helper()
		requestCtx, requestCancel := context.WithTimeout(ctx, 20*time.Second)
		defer requestCancel()
		request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+org.String(), nil)
		request = request.WithContext(policy.WithUser(requestCtx, &policy.User{IsSuperuser: true, UserID: super.String(), ID: super}))
		request.SetPathValue("org_id", org.String())
		recorder := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			h.deleteOrganization(recorder, request)
		}()
		select {
		case <-done:
		case <-time.After(25 * time.Second):
			t.Fatal("the delete did not answer while the worker held the row: it waits instead of skipping")
		}
		return recorder
	}

	// The worker claims the row and holds it across its revoke.
	worker, err := providerrevocation.NewHandler(providerrevocation.PgStore{DB: pool}, cipher, func(ctx context.Context, tok string) error {
		return providerfoundation.RevokePagerDutyOAuthToken(ctx, server.Client(), revokeConfig, tok)
	}, true, logger)
	if err != nil {
		t.Fatal(err)
	}
	workerDone := make(chan error, 1)
	go func() {
		workerDone <- worker.Work(ctx, &jobruntime.Execution[jobruntime.ProviderOAuthRevocationsArgs]{
			Args: jobruntime.ProviderOAuthRevocationsArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderOAuthRevocationsPayload]{
				Payload: jobcontract.ProviderOAuthRevocationsPayload{ScheduledFor: "2026-10-30T12:00:00Z"},
			}},
		})
	}()
	select {
	case <-barrier.inFlight:
	case <-time.After(30 * time.Second):
		t.Fatal("the worker never sent the row")
	}

	first := deleteOrg(t)
	if first.Code != http.StatusServiceUnavailable || !strings.Contains(first.Body.String(), orgDeletionRevocationInFlightDetail) {
		t.Fatalf("delete while the worker holds the row = %d %s, want 503 %q", first.Code, first.Body.String(), orgDeletionRevocationInFlightDetail)
	}
	if n := barrier.count(token); n != 1 {
		t.Fatalf("the held row's token was sent %d times while the worker held it, want 1 (the worker's)", n)
	}
	if orgs, probe := orgRows(); orgs != 1 || probe != 1 || !pending() {
		t.Fatalf("a refused delete purged: orgs=%d clickhouse=%d revocation pending=%v", orgs, probe, pending())
	}
	if deprovision.calls != 1 {
		t.Fatalf("%d deprovision calls, want 1", deprovision.calls)
	}

	releaseBarrier()
	select {
	case err := <-workerDone:
		if err != nil {
			t.Fatalf("worker: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the worker did not finish after the release")
	}
	if pending() {
		t.Fatal("the worker did not delete the row it sent")
	}

	retry := deleteOrg(t)
	if retry.Code != http.StatusOK {
		t.Fatalf("the repeat delete = %d %s, want 200", retry.Code, retry.Body.String())
	}
	if deprovision.calls != 2 {
		t.Fatalf("%d deprovision calls after the repeat, want 2", deprovision.calls)
	}
	if orgs, probe := orgRows(); orgs != 0 || probe != 0 {
		t.Fatalf("the repeat delete did not purge: orgs=%d clickhouse=%d", orgs, probe)
	}
	if n := barrier.count(token); n != 1 {
		t.Fatalf("the token was sent %d times in all, want exactly 1", n)
	}
	for _, surface := range []string{logs.String(), first.Body.String(), retry.Body.String()} {
		if strings.Contains(surface, token) || strings.Contains(surface, sealed.Reveal()) {
			t.Fatalf("a token or ciphertext reached a log or an answer: %s", surface)
		}
	}
}

//go:build integration

package providerrevocation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
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

const (
	integrationKey      = "providerrevocation-integration-fernet-key-32"
	integrationClientID = "providerrevocation-integration-client"
	workerRole          = "revocation_worker_probe"
)

// fakePagerDuty answers a revoke 200, or 500 for a token in fail; delay holds
// each call so two senders overlap.
type fakePagerDuty struct {
	mu    sync.Mutex
	calls map[string]int
	fail  map[string]bool
	delay time.Duration
}

func (fake *fakePagerDuty) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	values, _ := url.ParseQuery(string(body))
	token := values.Get("token")
	fake.mu.Lock()
	fake.calls[token]++
	fail := fake.fail[token]
	delay := fake.delay
	fake.mu.Unlock()
	time.Sleep(delay)
	if fail || values.Get("client_id") != integrationClientID {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (fake *fakePagerDuty) count(token string) int {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.calls[token]
}

type cell struct {
	admin  *pgxpool.Pool
	worker *pgxpool.Pool
	cipher providerfoundation.FernetDecryptor
	fake   *fakePagerDuty
	server *httptest.Server
	org    string
}

// newCell starts PostgreSQL with the real schema, and a worker login holding
// exactly the domain role's grant on the table: SELECT, UPDATE, DELETE.
func newCell(t *testing.T) *cell {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	for _, statement := range []string{
		"CREATE ROLE " + workerRole + " LOGIN PASSWORD 'probe'",
		"GRANT USAGE ON SCHEMA public TO " + workerRole,
		"GRANT SELECT, UPDATE, DELETE ON TABLE public.provider_oauth_revocations TO " + workerRole,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	config, err := pgxpool.ParseConfig(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.User, config.ConnConfig.Password = workerRole, "probe"
	worker, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(worker.Close)
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(integrationKey), "")
	if err != nil {
		t.Fatal(err)
	}
	fake := &fakePagerDuty{calls: map[string]int{}, fail: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	t.Cleanup(server.Close)
	return &cell{admin: admin, worker: worker, cipher: cipher, fake: fake, server: server, org: uuid.NewString()}
}

func (c *cell) handler(t *testing.T, configured bool, logs io.Writer) *Handler {
	t.Helper()
	revokeConfig := providerfoundation.PagerDutyRevokeConfig{ClientID: integrationClientID, RevokeURL: c.server.URL}
	revoke := func(ctx context.Context, token string) error {
		return providerfoundation.RevokePagerDutyOAuthToken(ctx, &http.Client{}, revokeConfig, token)
	}
	handler, err := NewHandler(PgStore{DB: c.worker}, c.cipher, revoke, configured, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func (c *cell) run(t *testing.T, handler *Handler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := handler.Work(ctx, execution(due)); err != nil {
		t.Fatalf("Work: %v", err)
	}
}

// seed writes one pending row. age is how long ago it was created; touched how
// long ago its last attempt was.
func (c *cell) seed(t *testing.T, purpose, name, token string, attempts int, age, touched time.Duration) uuid.UUID {
	t.Helper()
	sealed, err := c.cipher.Encrypt([]byte(token))
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if _, err := c.admin.Exec(context.Background(),
		`INSERT INTO provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, created_at, updated_at)
VALUES ($1, $2, 'pagerduty', $3, $4, $5, 'v1', 'pending', $6, now() - make_interval(secs => $7), now() - make_interval(secs => $8))`,
		id, c.org, name, purpose, sealed.Reveal(), attempts, age.Seconds(), touched.Seconds()); err != nil {
		t.Fatal(err)
	}
	return id
}

type state struct {
	present   bool
	attempts  int
	lastError string
	touched   time.Duration
}

func (c *cell) state(t *testing.T, id uuid.UUID) state {
	t.Helper()
	var s state
	var lastError *string
	var touchedSeconds float64
	err := c.admin.QueryRow(context.Background(),
		`SELECT attempts, last_error, EXTRACT(EPOCH FROM now() - updated_at) FROM provider_oauth_revocations WHERE id = $1`, id).
		Scan(&s.attempts, &lastError, &touchedSeconds)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return state{}
		}
		t.Fatal(err)
	}
	s.present = true
	if lastError != nil {
		s.lastError = *lastError
	}
	s.touched = time.Duration(touchedSeconds * float64(time.Second))
	return s
}

func (c *cell) age(t *testing.T, id uuid.UUID, touched time.Duration) {
	t.Helper()
	if _, err := c.admin.Exec(context.Background(), `UPDATE provider_oauth_revocations SET updated_at = now() - make_interval(secs => $2) WHERE id = $1`, id, touched.Seconds()); err != nil {
		t.Fatal(err)
	}
}

// The ticket's cell: two pending rows, one sent, one refused and retried after
// its backoff; a row at the last attempt stops and is counted.
func TestWorkerSendsRetriesAndStops(t *testing.T) {
	c := newCell(t)
	c.fake.fail["tok-b-5xx"] = true
	c.fake.fail["tok-c-last"] = true
	a := c.seed(t, "disconnect", "default", "tok-a-ok", 0, time.Hour, time.Hour)
	b := c.seed(t, "replacement", "default", "tok-b-5xx", 0, time.Hour, time.Hour)
	last := c.seed(t, "disconnect", "other", "tok-c-last", MaxAttempts-1, 30*24*time.Hour, 7*time.Hour)
	logs := &bytes.Buffer{}
	handler := c.handler(t, true, logs)

	c.run(t, handler)
	if c.state(t, a).present || c.fake.count("tok-a-ok") != 1 {
		t.Fatalf("row A not sent once: %+v calls %d", c.state(t, a), c.fake.count("tok-a-ok"))
	}
	if s := c.state(t, b); !s.present || s.attempts != 1 || s.lastError != ReasonRemoteRevokeFailed || s.touched > time.Minute {
		t.Fatalf("row B = %+v, want 1 attempt, remote_revoke_failed, just touched", s)
	}
	if s := c.state(t, last); !s.present || s.attempts != MaxAttempts {
		t.Fatalf("last row = %+v, want pending at %d attempts", s, MaxAttempts)
	}
	if !strings.Contains(logs.String(), `"revocation_id":"`+last.String()+`"`) || !strings.Contains(logs.String(), `"exhausted_total":1`) {
		t.Fatalf("exhausted row not named or counted: %s", logs.String())
	}

	c.run(t, handler)
	if c.fake.count("tok-b-5xx") != 1 {
		t.Fatalf("row B sent again inside its backoff: %d calls", c.fake.count("tok-b-5xx"))
	}

	c.age(t, b, 2*time.Minute+time.Second)
	c.age(t, last, 7*time.Hour)
	c.run(t, handler)
	if c.fake.count("tok-b-5xx") != 2 || c.state(t, b).attempts != 2 {
		t.Fatalf("row B not retried after its backoff: calls %d state %+v", c.fake.count("tok-b-5xx"), c.state(t, b))
	}
	if c.fake.count("tok-c-last") != 1 {
		t.Fatalf("an exhausted row was claimed again: %d calls", c.fake.count("tok-c-last"))
	}
	pending, exhausted, err := (PgStore{DB: c.worker}).Totals(context.Background())
	if err != nil || pending != 2 || exhausted != 1 {
		t.Fatalf("Totals = %d, %d, %v; want 2 pending, 1 exhausted", pending, exhausted, err)
	}
}

// A setup row of a callback still in flight is left; a stale one is sent.
func TestWorkerKeepsALiveCallbacksSetupRow(t *testing.T) {
	c := newCell(t)
	young := c.seed(t, "setup", "", "tok-setup-young", 0, time.Minute, time.Hour)
	stale := c.seed(t, "setup", "", "tok-setup-stale", 0, SetupStaleAfter+time.Minute, time.Hour)
	c.run(t, c.handler(t, true, io.Discard))
	if !c.state(t, young).present || c.fake.count("tok-setup-young") != 0 {
		t.Fatal("a young setup row was revoked under its callback")
	}
	if c.state(t, stale).present || c.fake.count("tok-setup-stale") != 1 {
		t.Fatal("a stale setup row was not sent")
	}
}

// 11835's requeued_blocked class: the credential was connected after the
// restore point, so no integration_credentials row exists for it. No route
// sends the row; the worker does.
func TestWorkerSendsARequeuedRowOfAMissingCredential(t *testing.T) {
	c := newCell(t)
	var credentials int
	if err := c.admin.QueryRow(context.Background(), `SELECT count(*) FROM integration_credentials WHERE org_id = $1`, c.org).Scan(&credentials); err != nil || credentials != 0 {
		t.Fatalf("credentials = %d, %v", credentials, err)
	}
	// The requeue keeps created_at and writes attempts 0, updated_at now: both
	// rows are due in the first run, the setup one too (it is older than the
	// callback cutoff). A setup row created a minute ago still waits.
	requeued := c.seed(t, "disconnect", "connected-after-restore", "tok-requeued", 0, 10*24*time.Hour, 0)
	requeuedSetup := c.seed(t, "setup", "", "tok-requeued-setup", 0, 10*24*time.Hour, 0)
	fresh := c.seed(t, "setup", "", "tok-fresh-setup", 0, time.Minute, 0)
	c.run(t, c.handler(t, true, io.Discard))
	if c.state(t, requeued).present || c.fake.count("tok-requeued") != 1 {
		t.Fatalf("requeued row not sent: %+v calls %d", c.state(t, requeued), c.fake.count("tok-requeued"))
	}
	if c.state(t, requeuedSetup).present || c.fake.count("tok-requeued-setup") != 1 {
		t.Fatalf("requeued setup row not sent: %+v calls %d", c.state(t, requeuedSetup), c.fake.count("tok-requeued-setup"))
	}
	if !c.state(t, fresh).present || c.fake.count("tok-fresh-setup") != 0 {
		t.Fatal("a setup row younger than the callback cutoff was sent")
	}
}

// A row another sender holds (a disconnect mid-call) is skipped, not waited on.
func TestWorkerSkipsARowAnotherSenderHolds(t *testing.T) {
	c := newCell(t)
	held := c.seed(t, "disconnect", "default", "tok-held", 0, time.Hour, time.Hour)
	free := c.seed(t, "disconnect", "other", "tok-free", 0, time.Hour, time.Hour)
	holder, err := c.admin.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(context.Background(), `SELECT 1 FROM provider_oauth_revocations WHERE id = $1 FOR UPDATE`, held); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.handler(t, true, io.Discard).Work(ctx, execution(due)); err != nil {
		t.Fatalf("Work waited on or failed over a held row: %v", err)
	}
	if c.fake.count("tok-held") != 0 || !c.state(t, held).present || c.state(t, held).attempts != 0 {
		t.Fatal("a held row was sent or touched")
	}
	if c.state(t, free).present {
		t.Fatal("the free row was not sent")
	}
}

// Two senders on the same rows: every token gets exactly one provider call.
func TestTwoSendersSendEachRowOnce(t *testing.T) {
	c := newCell(t)
	c.fake.delay = 200 * time.Millisecond
	tokens := []string{"tok-race-1", "tok-race-2", "tok-race-3", "tok-race-4"}
	for _, token := range tokens {
		c.seed(t, "disconnect", token, token, 0, time.Hour, time.Hour)
	}
	first, second := c.handler(t, true, io.Discard), c.handler(t, true, io.Discard)
	var wg sync.WaitGroup
	for _, handler := range []*Handler{first, second} {
		wg.Add(1)
		go func(handler *Handler) {
			defer wg.Done()
			if err := handler.Work(context.Background(), execution(due)); err != nil {
				t.Errorf("Work: %v", err)
			}
		}(handler)
	}
	wg.Wait()
	for _, token := range tokens {
		if got := c.fake.count(token); got != 1 {
			t.Fatalf("%s: %d provider calls, want exactly 1", token, got)
		}
	}
	pending, _, err := (PgStore{DB: c.worker}).Totals(context.Background())
	if err != nil || pending != 0 {
		t.Fatalf("pending = %d, %v; want 0", pending, err)
	}
}

// Without a client id the worker changes no row.
func TestWorkerWithoutAClientIDChangesNothing(t *testing.T) {
	c := newCell(t)
	row := c.seed(t, "disconnect", "default", "tok-unconfigured", 0, time.Hour, time.Hour)
	before := c.state(t, row)
	c.run(t, c.handler(t, false, io.Discard))
	after := c.state(t, row)
	if !after.present || after.attempts != before.attempts || c.fake.count("tok-unconfigured") != 0 {
		t.Fatalf("unconfigured run changed the row: %+v -> %+v", before, after)
	}
}

// No token or ciphertext reaches any log record of a run that refuses a row,
// exhausts it and names it. The ciphertext checked is the one stored in the
// row (Fernet ciphertexts differ per encryption), read before the run.
func TestWorkerLogsCountsOnly(t *testing.T) {
	c := newCell(t)
	c.fake.fail[sentinelToken] = true
	id := c.seed(t, "disconnect", "default", sentinelToken, MaxAttempts-1, time.Hour, 7*time.Hour)
	var stored string
	if err := c.admin.QueryRow(context.Background(), `SELECT token_encrypted FROM provider_oauth_revocations WHERE id = $1`, id).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	c.run(t, c.handler(t, true, logs))
	records := strings.Split(strings.TrimSpace(logs.String()), "\n")
	named := false
	for _, record := range records {
		for _, secret := range []string{sentinelToken, stored} {
			if strings.Contains(record, secret) {
				t.Errorf("a token or ciphertext reached a log record: %s", record)
			}
		}
		if strings.Contains(record, "provider_oauth_revocations.exhausted") && strings.Contains(record, id.String()) {
			named = true
		}
	}
	if !named || len(records) < 2 {
		t.Fatalf("the run did not log the exhausted row by id (%d records): %s", len(records), logs.String())
	}
}

// D6501 r2 P2a: rows a route sender pushed to the limit (seeded at MaxAttempts, never claimed by the worker) are
// named every run in one WARN line: the total, the first ExhaustedNameLimit by id, each with its reason class.
func TestEveryRunNamesRoutePushedExhaustedRows(t *testing.T) {
	c := newCell(t)
	var ids []string
	for index := 0; index < ExhaustedNameLimit+5; index++ {
		id := c.seed(t, "disconnect", fmt.Sprintf("route-%02d", index), fmt.Sprintf("tok-route-%02d", index), MaxAttempts, time.Hour, time.Minute)
		ids = append(ids, id.String())
	}
	if _, err := c.admin.Exec(context.Background(), `UPDATE provider_oauth_revocations SET last_error = 'remote_revoke_failed'`); err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	logs := &bytes.Buffer{}
	handler := c.handler(t, true, logs)
	c.run(t, handler)
	c.run(t, handler)
	var lines []string
	for _, record := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(record, `"msg":"provider_oauth_revocations.exhausted_rows"`) {
			lines = append(lines, record)
		}
	}
	if len(lines) != 2 {
		t.Fatalf("%d exhausted_rows lines after 2 runs, want 2:\n%s", len(lines), logs.String())
	}
	for _, line := range lines {
		if !strings.Contains(line, `"level":"WARN"`) || !strings.Contains(line, fmt.Sprintf(`"exhausted_total":%d`, len(ids))) {
			t.Fatalf("line lacks WARN or the total: %s", line)
		}
		for index, id := range ids {
			if listed := strings.Contains(line, id+":"+ReasonRemoteRevokeFailed); listed != (index < ExhaustedNameLimit) {
				t.Fatalf("row %d (%s) listed=%v, want %v: %s", index, id, listed, index < ExhaustedNameLimit, line)
			}
		}
		if strings.Contains(line, "tok-route-") {
			t.Fatalf("a token reached the log: %s", line)
		}
	}
	c.fake.mu.Lock()
	calls := len(c.fake.calls)
	c.fake.mu.Unlock()
	if calls != 0 {
		t.Fatalf("the worker sent %d exhausted rows, want none", calls)
	}
}

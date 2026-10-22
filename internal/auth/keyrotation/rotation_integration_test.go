//go:build integration

package keyrotation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

type env struct {
	admin       *pgxpool.Pool
	runtime     *pgxpool.Pool
	runtimeRole string
	schema      authschema.ValidatedIdentifier
}

// newEnv applies the auth schema to a container of its own. admin owns the
// schema (the migration role); runtime connects as the runtime role.
func newEnv(t *testing.T, ctx context.Context) *env {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)
	role, err := containers.RoleName("keyrotation_runtime", instance)
	if err != nil {
		t.Fatalf("derive role: %v", err)
	}
	const password = "keyrotation-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User = role
	cfg.ConnConfig.Password = password
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return &env{admin: admin, runtime: runtime, runtimeRole: role, schema: schema}
}

func writeKey(t *testing.T, dir, name string) ed25519.PublicKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return public
}

// writeManifest writes the key-set manifest by an atomic rename. pairs are
// kid, state, file.
func writeManifest(t *testing.T, dir string, pairs ...string) string {
	t.Helper()
	var keys []map[string]string
	for i := 0; i+2 < len(pairs); i += 3 {
		keys = append(keys, map[string]string{"kid": pairs[i], "state": pairs[i+1], "file": pairs[i+2]})
	}
	body, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		t.Fatal(err)
	}
	staged := filepath.Join(dir, ".staged")
	if err := os.WriteFile(staged, body, 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "keyset.json")
	if err := os.Rename(staged, path); err != nil {
		t.Fatal(err)
	}
	return path
}

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = at
}

func refusedWith(t *testing.T, err error, reason Reason) {
	t.Helper()
	var refusal *Refusal
	if !errors.As(err, &refusal) || refusal.Reason != reason {
		t.Fatalf("err = %v, want refusal %s", err, reason)
	}
}

// signedBy signs a token and returns the kid its header names; the token
// verifies under that kid's key in the published set.
func signedBy(t *testing.T, ctx context.Context, signer *token.Signer) (string, string) {
	t.Helper()
	compact, kid, err := signer.Sign(ctx, "at+jwt", []byte(`{"sub":"prn_x"}`))
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	if !verifies(t, ctx, signer, compact) {
		t.Fatalf("a fresh token under %s does not verify under the published set", kid)
	}
	return compact, kid
}

// verifies reports whether compact's signature verifies under the published
// key its header names.
func verifies(t *testing.T, ctx context.Context, signer *token.Signer, compact string) bool {
	t.Helper()
	parts := strings.Split(compact, ".")
	rawHeader, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	var header struct {
		KeyID string `json:"kid"`
	}
	if err := json.Unmarshal(rawHeader, &header); err != nil {
		t.Fatal(err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	set, err := signer.KeySet(ctx)
	if err != nil {
		return false
	}
	for _, key := range set.Published() {
		if key.KeyID == header.KeyID {
			return ed25519.Verify(key.PublicKey, []byte(parts[0]+"."+parts[1]), signature)
		}
	}
	return false
}

func published(t *testing.T, ctx context.Context, signer *token.Signer) map[string]token.State {
	t.Helper()
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatalf("KeySet: %v", err)
	}
	out := map[string]token.State{}
	for _, key := range set.Published() {
		out[key.KeyID] = key.State
	}
	return out
}

func (e *env) session(t *testing.T, ctx context.Context, principal string, issued time.Time, refreshIssued *time.Time, revoked bool) string {
	t.Helper()
	id := uuid.NewString()
	var revokedAt any
	var reason any
	if revoked {
		revokedAt, reason = issued, "session_revoked"
	}
	if _, err := e.admin.Exec(ctx, `
		INSERT INTO auth.sessions (id, principal_id, token_hash, issued_at, expires_at, revoked_at, revoked_reason)
		VALUES ($1, $2, $3, $4, $4::timestamptz + interval '30 days', $5, $6)`,
		id, principal, []byte(id), issued, revokedAt, reason); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	if refreshIssued != nil {
		if _, err := e.admin.Exec(ctx, `
			INSERT INTO auth.refresh_credentials (session_id, principal_id, token_hash, issued_at, expires_at)
			VALUES ($1, $2, $3, $4, $4::timestamptz + interval '30 days')`,
			id, principal, []byte("rt-"+id), *refreshIssued); err != nil {
			t.Fatalf("seed refresh credential: %v", err)
		}
	}
	return id
}

func (e *env) revokedReason(t *testing.T, ctx context.Context, id string) string {
	t.Helper()
	var reason *string
	if err := e.admin.QueryRow(ctx, `SELECT revoked_reason FROM auth.sessions WHERE id = $1`, id).Scan(&reason); err != nil {
		t.Fatal(err)
	}
	if reason == nil {
		return ""
	}
	return *reason
}

// TestLiveRotation is the acceptance run: publish, issue, overlap, retire and
// compromise against a real auth schema, with the runtime signer reading the
// durable states through the runtime role.
func TestLiveRotation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	dir := t.TempDir()
	publicA := writeKey(t, dir, "a.pem")
	writeKey(t, dir, "b.pem")
	writeKey(t, dir, "c.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem", "b", "pending", "b.pem", "c", "pending", "c.pem")

	start := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Second)
	at := &clock{now: start}
	operator := &Operator{Pool: e.admin, Schema: e.schema, Now: at.Now}
	states := token.NewKeyStates(token.KeyStatesConfig{Source: NewReader(e.runtime, e.schema), RefreshEvery: time.Nanosecond})
	signer := token.NewFileSigner(token.FileConfig{KeySetPath: manifest, States: states})

	// An empty table: custody's states stand.
	if _, kid := signedBy(t, ctx, signer); kid != "a" {
		t.Fatalf("before adoption the signer used %s", kid)
	}
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := operator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	refusedWith(t, func() error { _, err := operator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); return err }(), ReasonTableNotEmpty)

	// Publish: b and c are published (pending) and do not sign; a signs.
	tokenA, kid := signedBy(t, ctx, signer)
	if kid != "a" {
		t.Fatalf("issue: signer used %s, want a", kid)
	}
	if got := published(t, ctx, signer); len(got) != 3 || got["b"] != token.StatePending || got["c"] != token.StatePending {
		t.Fatalf("published = %v, want a, b and c", got)
	}

	// Publish before issue: b activates only after the JWKS cache TTL.
	at.set(start.Add(PendingFloor - time.Second))
	refusedWith(t, func() error { _, err := operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: "b"}); return err }(), ReasonFloorNotReached)
	if plan, err := operator.Plan(ctx, Request{Verb: VerbActivate, KeyID: "b"}); err == nil || plan.FloorRemaining != time.Second {
		t.Fatalf("dry run before the floor = %+v, %v; want 1 s remaining", plan, err)
	}
	activatedB := start.Add(PendingFloor)
	at.set(activatedB)
	result, err := operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: "b"})
	if err != nil || len(result.Moves) != 2 {
		t.Fatalf("activate b = %+v, %v", result, err)
	}

	// Overlap: b signs; a is retiring, still published, and a's token still
	// verifies.
	if _, kid := signedBy(t, ctx, signer); kid != "b" {
		t.Fatalf("after activation the signer used %s, want b", kid)
	}
	if got := published(t, ctx, signer); got["a"] != token.StateRetiring || got["b"] != token.StateActive {
		t.Fatalf("published during the overlap = %v", got)
	}
	if !verifies(t, ctx, signer, tokenA) {
		t.Fatal("a token signed by the retiring key no longer verifies during the overlap")
	}

	// Retire after expiry: a leaves the set only after access TTL + skew +
	// cache TTL.
	at.set(activatedB.Add(RetiringFloor - time.Second))
	refusedWith(t, func() error { _, err := operator.Apply(ctx, Request{Verb: VerbRetire, KeyID: "a"}); return err }(), ReasonFloorNotReached)
	at.set(activatedB.Add(RetiringFloor))
	if _, err := operator.Apply(ctx, Request{Verb: VerbRetire, KeyID: "a"}); err != nil {
		t.Fatalf("retire a: %v", err)
	}
	if got := published(t, ctx, signer); got["a"] != "" {
		t.Fatalf("a retired key is still published: %v", got)
	}
	if verifies(t, ctx, signer, tokenA) {
		t.Fatal("a token of the retired key still verifies under the published set")
	}

	// Compromise b with c as the successor.
	compromisedAt := activatedB.Add(RetiringFloor + time.Minute)
	at.set(compromisedAt)
	principal := uuid.NewString()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'keyrotation')`, principal); err != nil {
		t.Fatal(err)
	}
	refreshInWindow := compromisedAt.Add(-2 * time.Minute)
	mintedUnderB := e.session(t, ctx, principal, compromisedAt.Add(-5*time.Minute), nil, false)
	refreshedUnderB := e.session(t, ctx, principal, start.Add(-time.Hour), &refreshInWindow, false)
	mintedUnderA := e.session(t, ctx, principal, activatedB.Add(-time.Minute), nil, false)
	expiredUnderB := e.session(t, ctx, principal, compromisedAt.Add(-RevokeWindow-time.Second), nil, false)
	alreadyRevoked := e.session(t, ctx, principal, compromisedAt.Add(-time.Minute), nil, true)

	plan, err := operator.Plan(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || plan.Sessions != 2 {
		t.Fatalf("compromise dry run = %+v, %v; want 2 sessions", plan, err)
	}
	result, err = operator.Apply(ctx, Request{Verb: VerbCompromise, KeyID: "b", Successor: "c"})
	if err != nil || result.Sessions != 2 {
		t.Fatalf("compromise b = %+v, %v; want 2 sessions revoked", result, err)
	}
	if _, kid := signedBy(t, ctx, signer); kid != "c" {
		t.Fatalf("after the compromise the signer used %s, want c", kid)
	}
	if got := published(t, ctx, signer); got["b"] != "" || got["c"] != token.StateActive {
		t.Fatalf("after the compromise published = %v; b must be gone at once", got)
	}
	for id, want := range map[string]string{
		mintedUnderB: "compromised", refreshedUnderB: "compromised",
		mintedUnderA: "", expiredUnderB: "", alreadyRevoked: "session_revoked",
	} {
		if got := e.revokedReason(t, ctx, id); got != want {
			t.Errorf("session %s revoked_reason = %q, want %q", id, got, want)
		}
	}
	denied, err := NewReader(e.runtime, e.schema).Denylist(ctx)
	if err != nil || len(denied) != 1 || denied[0].KeyID != "b" {
		t.Fatalf("denylist = %+v, %v; want b only", denied, err)
	}
	var liveRefresh int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.refresh_credentials WHERE session_id = $1 AND revoked_at IS NULL`,
		refreshedUnderB).Scan(&liveRefresh); err != nil || liveRefresh != 0 {
		t.Fatalf("live refresh credentials of a revoked session = %d, %v", liveRefresh, err)
	}

	// A rollback never re-enables a compromised kid: not by a verb, not by a
	// manifest that names it active again, not under a new kid.
	refusedWith(t, func() error { _, err := operator.Apply(ctx, Request{Verb: VerbRollback, KeyID: "b"}); return err }(), ReasonCompromisedTerminal)
	refusedWith(t, func() error { _, err := operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: "b"}); return err }(), ReasonCompromisedTerminal)
	writeManifest(t, dir, "a", "retired", "a.pem", "b", "active", "b.pem", "c", "pending", "c.pem")
	if _, kid := signedBy(t, ctx, signer); kid != "c" {
		t.Fatalf("a manifest naming the compromised key active made the signer use %s", kid)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "b.pem"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b2.pem"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest = writeManifest(t, dir, "a", "retired", "a.pem", "c", "active", "c.pem", "b2", "pending", "b2.pem")
	custody, err = token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	refusedWith(t, func() error {
		_, err := operator.Apply(ctx, Request{Verb: VerbRegister, KeyID: "b2", Custody: custody})
		return err
	}(), ReasonKeyRegistered)
	if got := published(t, ctx, signer); got["b2"] != "" {
		t.Fatalf("the compromised public key is published under a new kid: %v", got)
	}

	// Rollback to an earlier uncompromised kid: a is re-published first.
	if _, err := operator.Apply(ctx, Request{Verb: VerbRollback, KeyID: "a"}); err != nil {
		t.Fatalf("rollback a (retired -> pending): %v", err)
	}
	if got := published(t, ctx, signer); got["a"] != token.StatePending || !publicIn(t, ctx, signer, publicA) {
		t.Fatalf("a rolled back is not published as pending: %v", got)
	}

	// The audit trail: one row per applied verb and one per refusal.
	counts := map[string]int{}
	rows, err := e.admin.Query(ctx, `SELECT event_type, outcome FROM auth.security_audit_events WHERE event_type LIKE 'auth.signing_key.%'`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var event, outcome string
		if err := rows.Scan(&event, &outcome); err != nil {
			t.Fatal(err)
		}
		counts[event+"/"+outcome]++
	}
	rows.Close()
	want := map[string]int{
		"auth.signing_key.adopted/allowed": 1, "auth.signing_key.activated/allowed": 1,
		"auth.signing_key.retired/allowed": 1, "auth.signing_key.compromised/allowed": 1,
		"auth.signing_key.rolled_back/allowed": 1, "auth.signing_key.refused/denied": 6,
	}
	for key, n := range want {
		if counts[key] != n {
			t.Errorf("audit %s = %d, want %d (all: %v)", key, counts[key], n, counts)
		}
	}
	var attributes map[string]any
	if err := e.admin.QueryRow(ctx, `SELECT attributes FROM auth.security_audit_events WHERE event_type = 'auth.signing_key.compromised'`).
		Scan(&attributes); err != nil {
		t.Fatal(err)
	}
	if attributes["key_id"] != "b" || attributes["related_key_id"] != "c" || attributes["reason_code"] != "compromised" ||
		attributes["previous_status"] != "active" || attributes["new_status"] != "compromised" {
		t.Fatalf("compromise audit attributes = %v", attributes)
	}
}

func publicIn(t *testing.T, ctx context.Context, signer *token.Signer, public ed25519.PublicKey) bool {
	t.Helper()
	set, err := signer.KeySet(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range set.Published() {
		if key.PublicKey.Equal(public) {
			return true
		}
	}
	return false
}

// TestOnlyTheMigrationRoleWritesKeyStates: the operator over a runtime-role
// pool is refused by PostgreSQL, and the command's role check names it.
func TestOnlyTheMigrationRoleWritesKeyStates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	dir := t.TempDir()
	writeKey(t, dir, "a.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem")
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	runtimeOperator := &Operator{Pool: e.runtime, Schema: e.schema}
	if _, err := runtimeOperator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err == nil {
		t.Fatal("the runtime role adopted keys")
	}
	var count int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.signing_keys`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("signing_keys rows after a runtime write = %d, %v", count, err)
	}
	if err := CheckNotRuntimeRole(ctx, e.runtime, e.runtimeRole); !errors.Is(err, ErrRuntimeRole) {
		t.Fatalf("CheckNotRuntimeRole(runtime) = %v, want ErrRuntimeRole", err)
	}
	if err := CheckNotRuntimeRole(ctx, e.admin, e.runtimeRole); err != nil {
		t.Fatalf("CheckNotRuntimeRole(migration) = %v", err)
	}
	for what, sql := range map[string]string{
		"insert": `INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind, status)
			VALUES ('rt', 'EdDSA', '{"x":"rt"}'::jsonb, 'rt', 'file', 'active')`,
		"update": `UPDATE auth.signing_keys SET status = 'compromised'`,
		"delete": `DELETE FROM auth.signing_keys`,
	} {
		if _, err := e.runtime.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "42501") {
			t.Errorf("runtime %s = %v, want SQLSTATE 42501", what, err)
		}
	}
	// The runtime reads.
	if _, err := NewReader(e.runtime, e.schema).KeyStates(ctx); err != nil {
		t.Fatalf("runtime read: %v", err)
	}
}

// TestAStaleSnapshotFailsClosed: with the table unreachable past the bound,
// the signer neither signs nor publishes.
func TestAStaleSnapshotFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	dir := t.TempDir()
	writeKey(t, dir, "a.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem")
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (&Operator{Pool: e.admin, Schema: e.schema}).Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err != nil {
		t.Fatal(err)
	}
	at := &clock{now: time.Unix(0, 0)}
	cfg := e.runtime.Config().Copy()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	states := token.NewKeyStates(token.KeyStatesConfig{Source: NewReader(pool, e.schema), RefreshEvery: time.Second, Now: at.Now})
	signer := token.NewFileSigner(token.FileConfig{KeySetPath: manifest, States: states})
	if _, kid := signedBy(t, ctx, signer); kid != "a" {
		t.Fatalf("signer used %s", kid)
	}
	pool.Close()
	at.set(time.Unix(0, 0).Add(token.JWKSCacheTTL))
	if _, kid := signedBy(t, ctx, signer); kid != "a" {
		t.Fatalf("inside the bound the last good snapshot was not used: %s", kid)
	}
	at.set(time.Unix(0, 0).Add(token.JWKSCacheTTL + time.Second))
	if _, _, err := signer.Sign(ctx, "at+jwt", []byte(`{}`)); err == nil {
		t.Fatal("past the bound with no readable table the signer still signed")
	}
	if _, err := signer.KeySet(ctx); err == nil {
		t.Fatal("past the bound with no readable table the key set was still published")
	}
}

// raceMarker is what `check_go.sh auth race-integration` counts.
const raceMarker = "AUTH_RACE_EXECUTED"

// TestRaceConcurrentVerbsMoveAKeyOnce: verbs racing on one key serialize; one
// applies, the others are refused or see the state changed, and the table
// never holds two active keys.
func TestRaceConcurrentVerbsMoveAKeyOnce(t *testing.T) {
	if raceEnabled {
		t.Logf("%s %s", raceMarker, t.Name())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newEnv(t, ctx)
	dir := t.TempDir()
	writeKey(t, dir, "a.pem")
	writeKey(t, dir, "b.pem")
	writeKey(t, dir, "c.pem")
	manifest := writeManifest(t, dir, "a", "active", "a.pem", "b", "pending", "b.pem", "c", "pending", "c.pem")
	custody, err := token.CustodyKeys(ctx, token.FileConfig{KeySetPath: manifest})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Add(-time.Hour)
	at := &clock{now: start}
	operator := &Operator{Pool: e.admin, Schema: e.schema, Now: at.Now}
	if _, err := operator.Apply(ctx, Request{Verb: VerbAdopt, Custody: custody}); err != nil {
		t.Fatal(err)
	}
	at.set(start.Add(PendingFloor))

	const racers = 8
	var wg sync.WaitGroup
	results := make([]error, racers)
	for i := range racers {
		kid := "b"
		if i%2 == 1 {
			kid = "c"
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, results[i] = operator.Apply(ctx, Request{Verb: VerbActivate, KeyID: kid})
		}()
	}
	wg.Wait()
	applied := 0
	for _, err := range results {
		var refusal *Refusal
		switch {
		case err == nil:
			applied++
		case errors.As(err, &refusal), errors.Is(err, ErrStateChanged):
		default:
			t.Errorf("a racing verb failed outside the state machine: %v", err)
		}
	}
	var active int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.signing_keys WHERE status = 'active'`).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 || applied < 1 {
		t.Fatalf("after %d racing activations: %d applied, %d active keys; want at least 1 applied and exactly 1 active", racers, applied, active)
	}
	var audited int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.signing_key.activated'`).
		Scan(&audited); err != nil || audited != applied {
		t.Fatalf("activation audit rows = %d, %v; want one per applied verb (%d)", audited, err, applied)
	}
}

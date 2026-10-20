//go:build integration

package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

func linkEnv(t *testing.T, ctx context.Context) (*pgEnv, *PGLinks) {
	t.Helper()
	env := newPGEnv(t, ctx)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return env, NewPGLinks(env.runtime, schema)
}

func (e *pgEnv) principal(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'user', 'link test')`, id.String()); err != nil {
		t.Fatalf("seed principal: %v", err)
	}
	return id
}

func linkHash(s string) [32]byte { return sha256.Sum256([]byte(s)) }

// preAged inserts a credential whose clock already ran out (or still runs):
// no trigger is disabled.
func (e *pgEnv) preAged(t *testing.T, ctx context.Context, p uuid.UUID, purpose Purpose, secret, expires string) {
	t.Helper()
	h := linkHash(secret)
	if _, err := e.admin.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at, created_at)
		VALUES ($1, $2, $3, `+expires+`, now() - interval '3 hours')`, p.String(), string(purpose), h[:]); err != nil {
		t.Fatalf("seed credential: %v", err)
	}
}

func (e *pgEnv) count(t *testing.T, ctx context.Context, query string, args ...any) int {
	t.Helper()
	var n int
	if err := e.admin.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestLinkCredentialIsSingleUseAndStoredAsAHashOnly(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("s1"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	if err := env.admin.QueryRow(ctx, `SELECT token_hash FROM auth.link_credentials WHERE principal_id = $1`, p.String()).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if h := linkHash("s1"); string(stored) != string(h[:]) {
		t.Fatalf("stored hash %x, want sha256 of the secret", stored)
	}
	got, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("s1")})
	if err != nil || !ok || got != p {
		t.Fatalf("first Redeem = %v, %v, %v", got, ok, err)
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("s1")}); err != nil || ok {
		t.Fatalf("replay = ok %v, err %v; want a refusal", ok, err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND consumed_at IS NOT NULL`, p.String()); n != 1 {
		t.Errorf("consumed rows = %d, want 1 (consume marks the row, it does not delete it)", n)
	}
}

func TestLinkCredentialExpiryIsDecidedByTheDatabaseClock(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	env.preAged(t, ctx, p, PurposeEmailVerification, "expired", "now() - interval '1 second'")
	env.preAged(t, ctx, p, PurposeEmailVerification, "live", "now() + interval '1 hour'")
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: linkHash("expired")}); err != nil || ok {
		t.Fatalf("expired Redeem = ok %v, err %v", ok, err)
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: linkHash("live")}); err != nil || !ok {
		t.Fatalf("live Redeem = ok %v, err %v", ok, err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE consumed_at IS NOT NULL AND expires_at < now()`); n != 0 {
		t.Errorf("an expired credential was consumed")
	}
}

func TestLinkCredentialOfAnotherPurposeIsNotRedeemed(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposeEmailVerification, TokenHash: linkHash("v"), TTL: VerificationTTL}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("v")}); err != nil || ok {
		t.Fatalf("cross-purpose Redeem = ok %v, err %v", ok, err)
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: linkHash("v")}); err != nil || !ok {
		t.Fatalf("own-purpose Redeem after the refusal = ok %v, err %v", ok, err)
	}
}

func TestReissueMarksOlderCredentialsOfThePurposeConsumed(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	for _, s := range []string{"first", "second"} {
		if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash(s), TTL: ResetTTL}); err != nil {
			t.Fatal(err)
		}
	}
	other := IssueRequest{PrincipalID: p, Purpose: PurposeEmailVerification, TokenHash: linkHash("other-purpose"), TTL: VerificationTTL}
	if err := links.Issue(ctx, other); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("first")}); ok {
		t.Error("the superseded credential still redeems")
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("second")}); err != nil || !ok {
		t.Errorf("the newest credential does not redeem: ok %v, err %v", ok, err)
	}
	if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposeEmailVerification, TokenHash: linkHash("other-purpose")}); err != nil || !ok {
		t.Errorf("a credential of another purpose was superseded: ok %v, err %v", ok, err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1`, p.String()); n != 3 {
		t.Errorf("rows = %d, want 3 (a reissue never deletes)", n)
	}
}

func TestDecoyIssueWritesNothing(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	if err := links.Issue(ctx, IssueRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("decoy"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials`); n != 0 {
		t.Errorf("a decoy wrote %d rows", n)
	}
}

func TestRedeemCommitsAuditRowAndOutboxEvent(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	cases := []struct {
		purpose          Purpose
		eventType, event string
		reason           string
	}{
		{PurposeEmailVerification, "auth.email.verified", "principal.email_verified", "email_verified"},
		{PurposePasswordReset, "auth.password.reset", "principal.password_reset", "password_reset"},
	}
	for _, c := range cases {
		secret := string(c.purpose)
		if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: c.purpose, TokenHash: linkHash(secret), TTL: time.Hour}); err != nil {
			t.Fatal(err)
		}
		if _, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: c.purpose, TokenHash: linkHash(secret)}); err != nil || !ok {
			t.Fatalf("%s: Redeem = ok %v, err %v", c.purpose, ok, err)
		}
		if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events
			WHERE event_type = $1 AND outcome = 'allowed' AND subject_principal_id = $2 AND attributes->>'reason_code' = $3
			AND attributes->>'target_kind' = 'principal'`, c.eventType, p.String(), c.reason); n != 1 {
			t.Errorf("%s: audit rows = %d, want 1", c.purpose, n)
		}
		if n := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE event_type = $1 AND aggregate_id = $2`, c.event, p.String()); n != 1 {
			t.Errorf("%s: outbox rows = %d, want 1", c.purpose, n)
		}
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE attributes::text LIKE '%' || $1 || '%'`, "reset"+"-secret"); n != 0 {
		t.Error("a secret reached the audit row")
	}
}

// TestRaceRedeemReplay: many callers present one credential at once; exactly
// one wins.
func TestRaceRedeemReplay(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("race"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	const callers = 24
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, ok, err := links.Redeem(ctx, RedeemRequest{Purpose: PurposePasswordReset, TokenHash: linkHash("race")})
			if err != nil {
				errs <- err
				return
			}
			if ok {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("a replay ended in an error, not a refusal: %v", err)
	}
	if wins.Load() != 1 {
		t.Fatalf("winners = %d, want exactly 1", wins.Load())
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.password.reset'`); n != 1 {
		t.Errorf("audit rows = %d, want 1", n)
	}
	markRace(t)
}

// liveAccounts is a flag-on Accounts that counts the writes.
type liveAccounts struct {
	mu     sync.Mutex
	users  map[string]*User
	byID   map[uuid.UUID]*User
	hashes int
}

func (a *liveAccounts) ByEmail(_ context.Context, e string) (*User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.users[e], nil
}
func (a *liveAccounts) ByID(_ context.Context, id uuid.UUID) (*User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.byID[id], nil
}
func (a *liveAccounts) ProvisionLocal(context.Context, ProvisionLocalRequest) (uuid.UUID, error) {
	return uuid.Nil, identity.ErrEmailTaken
}
func (a *liveAccounts) SetEmailVerified(context.Context, uuid.UUID) error { return nil }
func (a *liveAccounts) SetPasswordHash(context.Context, uuid.UUID, string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hashes++
	return nil
}

type liveMail struct {
	mu   sync.Mutex
	sent []Message
}

func (m *liveMail) Send(_ context.Context, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, msg)
	return nil
}

func (m *liveMail) last(t *testing.T) Message {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sent) == 0 {
		t.Fatal("no mail sent")
	}
	return m.sent[len(m.sent)-1]
}

func liveFlows(t *testing.T, links *PGLinks, accounts *liveAccounts, mail *liveMail, rec *flowRecorder) *Flows {
	t.Helper()
	f, err := NewFlows(FlowsConfig{Accounts: accounts, Links: links, Sessions: &flowSessions{rec: rec}, Mail: mail, Locker: links, UsersOwnedByAuth: true,
		Hash:    func(p string) (string, error) { return "hash-of:" + p, nil },
		Compare: func(h, p string) bool { return h == "hash-of:"+p }})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestRaceResetReplay: one reset link submitted by many callers at once
// changes the password and revokes the sessions once.
func TestRaceResetReplay(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	accounts := &liveAccounts{users: map[string]*User{}, byID: map[uuid.UUID]*User{}}
	rec := &flowRecorder{}
	sessions := &flowSessions{rec: rec}
	f, err := NewFlows(FlowsConfig{Accounts: accounts, Links: links, Sessions: sessions, Mail: &liveMail{}, Locker: links, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash("flow-race"), TTL: ResetTTL}); err != nil {
		t.Fatal(err)
	}
	const callers = 16
	var ok atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			out, err := f.Reset(ctx, "flow-race", "Brand-new-pass-42")
			if err == nil && out.Kind == FlowOK {
				ok.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if ok.Load() != 1 || accounts.hashes != 1 || len(sessions.revoked) != 1 {
		t.Fatalf("successes %d, password writes %d, revokes %d; want 1 each", ok.Load(), accounts.hashes, len(sessions.revoked))
	}
	markRace(t)
}

func TestForgotAndResendAnswerAlikeForKnownAndUnknownAddresses(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	known := env.principal(t, ctx)
	accounts := &liveAccounts{users: map[string]*User{}, byID: map[uuid.UUID]*User{}}
	pw, provider := "x", "local"
	u := &User{ID: known, IsActive: true, PasswordHash: &pw, AuthProvider: &provider}
	accounts.users["known@example.com"] = u
	mail := &liveMail{}
	f := liveFlows(t, links, accounts, mail, &flowRecorder{})

	for _, address := range []string{"known@example.com", "unknown@example.com"} {
		if err := f.Forgot(ctx, address); err != nil {
			t.Fatalf("Forgot(%s): %v", address, err)
		}
		if err := f.Resend(ctx, address); err != nil {
			t.Fatalf("Resend(%s): %v", address, err)
		}
	}
	if len(mail.sent) != 4 {
		t.Fatalf("Send calls = %d, want 4 (one per request, decoys included)", len(mail.sent))
	}
	delivered := 0
	for _, m := range mail.sent {
		if m.Deliver {
			delivered++
		}
	}
	if delivered != 2 {
		t.Errorf("delivered = %d, want 2 (known address, both flows)", delivered)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials`); n != 2 {
		t.Errorf("credential rows = %d, want 2: the unknown address wrote none", n)
	}
}

// authFlowHandler is the test-only HTTP face of the flows: it maps typed
// outcomes to the status and body of the corpus, for the decision rows of the
// A3 auth-flow corpus only.
func authFlowHandler(f *Flows, mail *liveMail) http.Handler {
	generic := func(w http.ResponseWriter, status int, body string) {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	}
	violations := func(w http.ResponseWriter, v []string) {
		list, _ := json.Marshal(v)
		generic(w, http.StatusUnprocessableEntity, `{"detail":{"message":"Password validation failed","errors":`+string(list)+`}}`)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /register", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Email, Password string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		out, err := f.Register(r.Context(), RegisterRequest{Email: in.Email, Password: in.Password})
		switch {
		case err != nil:
			generic(w, 500, `{"detail":"Internal Server Error"}`)
		case out.Kind == FlowEmailTaken:
			generic(w, 400, `{"detail":{"message":"Email already registered"}}`)
		case out.Kind == FlowPolicy:
			violations(w, out.Violations)
		default:
			generic(w, 201, `{}`)
		}
	})
	mux.HandleFunc("GET /verify", func(w http.ResponseWriter, r *http.Request) {
		out, err := f.Verify(r.Context(), r.URL.Query().Get("token"))
		switch {
		case err != nil:
			generic(w, 500, `{"detail":"Internal Server Error"}`)
		case out.Kind == FlowOK:
			generic(w, 200, `{"message":"Email verified successfully","verified":true}`)
		default:
			generic(w, 400, `{"detail":{"message":"Invalid or expired verification token"}}`)
		}
	})
	mux.HandleFunc("POST /resend-verification", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Email string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		if err := f.Resend(r.Context(), in.Email); err != nil {
			generic(w, 500, `{"detail":"Internal Server Error"}`)
			return
		}
		generic(w, 200, `{"message":"If an account exists with that email, a verification link has been sent","verified":null}`)
	})
	mux.HandleFunc("POST /forgot-password", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Email string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		if err := f.Forgot(r.Context(), in.Email); err != nil {
			generic(w, 500, `{"detail":"Internal Server Error"}`)
			return
		}
		generic(w, 200, `{"message":"If the account exists, a password reset email has been sent","verified":null}`)
	})
	mux.HandleFunc("POST /reset-password", func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Token       string `json:"token"`
			NewPassword string `json:"new_password"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		out, err := f.Reset(r.Context(), in.Token, in.NewPassword)
		switch {
		case err != nil:
			generic(w, 500, `{"detail":"Internal Server Error"}`)
		case out.Kind == FlowOK:
			generic(w, 200, `{"message":"Password reset successful","verified":null}`)
		case out.Kind == FlowPolicy:
			violations(w, out.Violations)
		default:
			generic(w, 400, `{"detail":{"message":"Invalid or expired token"}}`)
		}
	})
	return mux
}

// TestCorpusAuthFlowDecisionRows replays the decision rows of the A3 auth-flow
// corpus (frozen Python answers) against the flows behind a test-only handler:
// status and body, exact.
func TestCorpusAuthFlowDecisionRows(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	accounts := &liveAccounts{users: map[string]*User{}, byID: map[uuid.UUID]*User{}}
	pw, provider := "x", "local"
	mk := func(email string, verified bool) uuid.UUID {
		id := env.principal(t, ctx)
		u := &User{ID: id, IsActive: true, IsVerified: verified, PasswordHash: &pw, AuthProvider: &provider}
		accounts.users[email], accounts.byID[id] = u, u
		return id
	}
	taken := mk("taken@example.com", true)
	_ = taken
	nina, alice := mk("nina@example.com", false), mk("alice@example.com", true)
	mk("verified@example.com", true)
	mk("pending@example.com", false)
	mail := &liveMail{}
	f := liveFlows(t, links, accounts, mail, &flowRecorder{})
	server := httptest.NewServer(authFlowHandler(f, mail))
	t.Cleanup(server.Close)

	golden := corpusAuthFlowRows(t)
	post := func(path, body string) (int, string) {
		resp, err := http.Post(server.URL+path, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var sb strings.Builder
		_, _ = fmt.Fprint(&sb, readAll(resp))
		return resp.StatusCode, sb.String()
	}
	get := func(path string) (int, string) {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode, readAll(resp)
	}
	check := func(name string, status int, body string, divergence string) {
		t.Helper()
		want, ok := golden[name]
		if !ok {
			t.Fatalf("the corpus has no row %q", name)
		}
		if divergence != "" {
			if status == want.Status {
				t.Errorf("%s: declared divergence (%s) but the status matches the corpus %d", name, divergence, want.Status)
			}
			t.Logf("deliberate divergence %q: corpus %d, here %d: %s", name, want.Status, status, divergence)
			return
		}
		if status != want.Status || body != want.Body {
			t.Errorf("%s: %d %s; corpus %d %s", name, status, body, want.Status, want.Body)
		}
	}

	s, b := post("/register", `{"email":"TAKEN@example.com","password":"`+sessionscenario.Password+`xxxx"}`)
	check("register: taken, other case", s, b, "")
	s, b = post("/register", `{"email":"new@example.com","password":"Short-pass1"}`)
	check("register: policy, too short", s, b, "")
	s, b = post("/register", `{"email":"new@example.com","password":"password1234"}`)
	check("register: common password", s, b, "")
	s, b = post("/register", `{"email":"new@example.com","password":"abcdefghijklmnop"}`)
	check("register: no digit", s, b, "")
	s, b = post("/register", `{"email":"new@example.com","password":"Aa1-`+strings.Repeat("x", 100)+`"}`)
	check("register: password over 72 bytes", s, b, "the corpus row records 500; the flows refuse an overlong password with a 422 policy violation")

	for _, row := range []struct{ name, email string }{
		{"resend: unknown address", "nobody@example.com"},
		{"resend: already verified", "verified@example.com"},
		{"resend: unverified, upper case", "PENDING@example.com"},
	} {
		s, b = post("/resend-verification", `{"email":"`+row.email+`"}`)
		check(row.name, s, b, "")
	}
	s, b = post("/forgot-password", `{"email":"nobody@example.com"}`)
	check("forgot: unknown address", s, b, "")
	s, b = post("/forgot-password", `{"email":"alice@example.com"}`)
	check("forgot: alice (member: audited)", s, b, "")
	resetSecret := mail.last(t).Secret.Reveal()

	s, b = get("/verify?token=garbage")
	check("verify: garbage", s, b, "")
	env.preAged(t, ctx, nina, PurposeEmailVerification, "old-link", "now() - interval '1 hour'")
	s, b = get("/verify?token=old-link")
	check("verify: expired", s, b, "")
	s, b = get("/verify?token=40000000000040008000000000000003.sig")
	check("verify: signed, no such token", s, b, "")
	s, b = get("/verify?token=40000000000040008000000000000003.%C3%A9")
	check("verify: non-ascii signature", s, b, "the corpus row records 500; an opaque secret has no signature, so any unknown secret is the same 400 refusal")

	if err := f.Resend(ctx, "nina@example.com"); err != nil {
		t.Fatal(err)
	}
	s, b = get("/verify?token=" + mail.last(t).Secret.Reveal())
	check("verify: nina's link", s, b, "")
	s, b = get("/verify?token=" + mail.last(t).Secret.Reveal())
	check("verify: nina's link again", s, b, "")

	s, b = post("/reset-password", `{"token":"garbage","new_password":"Brand-new-pass-42"}`)
	check("reset: garbage", s, b, "")
	env.preAged(t, ctx, alice, PurposePasswordReset, "old-reset", "now() - interval '1 hour'")
	s, b = post("/reset-password", `{"token":"old-reset","new_password":"Brand-new-pass-42"}`)
	check("reset: expired", s, b, "")
	s, b = post("/reset-password", `{"token":"`+resetSecret+`","new_password":"Brand-new-pass-42"}`)
	check("reset: alice's link", s, b, "")
	s, b = post("/reset-password", `{"token":"`+resetSecret+`","new_password":"Brand-new-pass-42"}`)
	check("reset: alice's link again", s, b, "")
	if accounts.hashes != 1 {
		t.Errorf("password writes = %d, want 1", accounts.hashes)
	}
}

// TestRaceIssueLeavesOneLiveCredential: many issuers of one principal and
// purpose at once leave exactly one unconsumed credential.
func TestRaceIssueLeavesOneLiveCredential(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	const callers = 24
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs <- links.Issue(ctx, IssueRequest{PrincipalID: p, Purpose: PurposePasswordReset, TokenHash: linkHash(fmt.Sprintf("issue-%d", i)), TTL: ResetTTL})
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("Issue: %v", err)
		}
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1 AND purpose = 'password_reset' AND consumed_at IS NULL`, p.String()); n != 1 {
		t.Fatalf("unconsumed credentials = %d, want exactly 1", n)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.link_credentials WHERE principal_id = $1`, p.String()); n != callers {
		t.Errorf("rows = %d, want %d (every issue stores one, older ones consumed)", n, callers)
	}
	markRace(t)
}

// stateSessions is a SessionAdmin whose live set is observable.
type stateSessions struct {
	mu   sync.Mutex
	live map[uuid.UUID]bool
}

func (s *stateSessions) Create(_ context.Context, _ uuid.UUID, _ *uuid.UUID, _ []string) (session.Issued, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := uuid.New()
	s.live[id] = true
	return session.Issued{Session: session.Record{ID: id}}, nil
}

func (s *stateSessions) RevokeAll(context.Context, uuid.UUID, session.Actor) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.live {
		s.live[id] = false
	}
	return len(s.live), nil
}

// statefulAccounts keeps one password hash; ByID reads it live.
type statefulAccounts struct {
	mu   sync.Mutex
	user *User
}

func (a *statefulAccounts) ByEmail(context.Context, string) (*User, error) { return nil, nil }
func (a *statefulAccounts) ByID(context.Context, uuid.UUID) (*User, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	u := *a.user
	return &u, nil
}
func (a *statefulAccounts) ProvisionLocal(context.Context, ProvisionLocalRequest) (uuid.UUID, error) {
	return uuid.Nil, identity.ErrEmailTaken
}
func (a *statefulAccounts) SetEmailVerified(context.Context, uuid.UUID) error { return nil }
func (a *statefulAccounts) SetPasswordHash(_ context.Context, _ uuid.UUID, hash string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.user.PasswordHash = &hash
	return nil
}

// TestRaceConcurrentChangeKeepsTheReturnedSession: concurrent changes made
// with the same current password: one wins, the rest are refused, and the
// session the winner was given is live.
func TestRaceConcurrentChangeKeepsTheReturnedSession(t *testing.T) {
	ctx := context.Background()
	env, links := linkEnv(t, ctx)
	p := env.principal(t, ctx)
	start0 := "hash-of:" + flowPassword
	provider := "local"
	accounts := &statefulAccounts{user: &User{ID: p, IsActive: true, PasswordHash: &start0, AuthProvider: &provider}}
	sessions := &stateSessions{live: map[uuid.UUID]bool{uuid.New(): true}}
	f, err := NewFlows(FlowsConfig{Accounts: accounts, Links: links, Sessions: sessions, Mail: &liveMail{}, Locker: links, UsersOwnedByAuth: true,
		Hash:    func(pw string) (string, error) { time.Sleep(20 * time.Millisecond); return "hash-of:" + pw, nil },
		Compare: func(h, pw string) bool { return h == "hash-of:"+pw }})
	if err != nil {
		t.Fatal(err)
	}
	const callers = 12
	type result struct {
		out FlowOutcome
		err error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			out, err := f.Change(ctx, ChangeRequest{PrincipalID: p, Current: flowPassword, New: fmt.Sprintf("Brand-new-pass-%d", 10+i)})
			results <- result{out, err}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	var winners []FlowOutcome
	for r := range results {
		if r.err != nil {
			t.Fatalf("Change: %v", r.err)
		}
		if r.out.Kind == FlowOK {
			winners = append(winners, r.out)
		} else if r.out.Kind != FlowCurrentRejected {
			t.Errorf("a refused change answered %s, want %s", r.out.Kind, FlowCurrentRejected)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("successful changes = %d, want exactly 1", len(winners))
	}
	if !sessions.live[winners[0].Issued.Session.ID] {
		t.Error("the session the winning change returned was revoked by another change")
	}
	markRace(t)
}

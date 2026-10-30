package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// probeSigner counts the signs and checks that reach a real file signer.
type probeSigner struct {
	inner  *token.Signer
	signs  atomic.Int64
	checks atomic.Int64
}

func (p *probeSigner) Sign(ctx context.Context, typ string, payload []byte) (string, string, error) {
	p.signs.Add(1)
	return p.inner.Sign(ctx, typ, payload)
}

func (p *probeSigner) Check(ctx context.Context) error {
	p.checks.Add(1)
	return p.inner.Check(ctx)
}

// unreadableKeySigner is a file signer whose active key file is gone: the key
// set still lists it, as after a bad Secret value or a rotation window.
func unreadableKeySigner(t *testing.T) *probeSigner {
	t.Helper()
	signer, dir, _ := fileSigner(t)
	if err := os.Remove(filepath.Join(dir, "k1.pem")); err != nil {
		t.Fatal(err)
	}
	return &probeSigner{inner: signer}
}

func readableKeySigner(t *testing.T) *probeSigner {
	t.Helper()
	signer, _, _ := fileSigner(t)
	return &probeSigner{inner: signer}
}

// exchangeCounter counts Exchange, the only call that consumes a parent
// credential.
type exchangeCounter struct {
	*fakeSessions
	exchanges atomic.Int64
}

func (c *exchangeCounter) Exchange(ctx context.Context, credential contracts.SecretString) (session.Result, error) {
	c.exchanges.Add(1)
	return c.fakeSessions.Exchange(ctx, credential)
}

func (c *exchangeCounter) ExchangeGuarded(ctx context.Context, credential contracts.SecretString, guard session.Guard) (session.Result, error) {
	c.exchanges.Add(1)
	return c.fakeSessions.ExchangeGuarded(ctx, credential, guard)
}

// newEdDSARefreshFixture is newRefreshFixtureWith over an EdDSA issuer on signer.
func newEdDSARefreshFixture(t *testing.T, signer AccessSigner) (*fixture, *exchangeCounter) {
	t.Helper()
	return newRefreshFixtureWith(t, newEdDSAIssuer(t, signer, testAudience))
}

// newRefreshFixtureWith serves the session routes with issuer and an Exchange
// that would rotate the presented credential.
func newRefreshFixtureWith(t *testing.T, issuer *Issuer) (*fixture, *exchangeCounter) {
	t.Helper()
	f := newFixtureWith(t, issuer)
	f.sessions.result = session.Result{Reason: session.ReasonAccepted, Issued: session.Issued{
		Session: f.record, Credential: contracts.NewSecretString(credential()), ExpiresAt: time.Now().Add(7*24*time.Hour - time.Second),
	}}
	counter := &exchangeCounter{fakeSessions: f.sessions}
	routes, err := Routes(Deps{Sessions: counter, Users: f.users, Issuer: f.issuer,
		Logger: slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	f.server = mux
	return f, counter
}

func TestRefreshRefusesBeforeExchangeWhenSignerUnavailable(t *testing.T) {
	signer := unreadableKeySigner(t)
	f, counter := newEdDSARefreshFixture(t, signer)
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
	if n := counter.exchanges.Load(); n != 0 {
		t.Fatalf("Exchange ran %d times before the token could be signed: the parent credential is consumed", n)
	}
	if len(f.sessions.revoked) != 0 {
		t.Fatalf("revoked %v", f.sessions.revoked)
	}
	if n := signer.signs.Load(); n != 0 {
		t.Fatalf("%d signs after the refusal", n)
	}
}

func TestIssueRefusesBeforeSessionWriteWhenSignerUnavailable(t *testing.T) {
	signer := unreadableKeySigner(t)
	f := newCompatFixture(t)
	f.issuer = newEdDSAIssuer(t, signer, testAudience)
	f.buildCompat()
	response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer)
	expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
	if len(f.sessions.created) != 0 {
		t.Fatalf("%d sessions written before the token could be signed", len(f.sessions.created))
	}
	if len(f.projector.userCalls) != 0 || len(f.projector.orgCalls) != 0 {
		t.Fatalf("projected users %v orgs %v before the token could be signed", f.projector.userCalls, f.projector.orgCalls)
	}
	if n := signer.signs.Load(); n != 0 {
		t.Fatalf("%d signs after the refusal", n)
	}
}

// fakeClock is a settable clock for the signer check window.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func withClock(t *testing.T, issuer *Issuer) *fakeClock {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	issuer.eddsa.check.clock = clock.Now
	return clock
}

func TestIssueRefusalsBeforePreflightKeepTheirCodes(t *testing.T) {
	stranger := uuid.New()
	for name, tc := range map[string]struct {
		mutate func(*compatFixture)
		header map[string]string
		body   func(*compatFixture) string
		status int
		reason string
	}{
		"no bearer": {header: nil, body: func(f *compatFixture) string { return issueBody(f.principal, "", "pwd") },
			status: http.StatusUnauthorized, reason: reasonInvalidCredentials},
		"refused service credential": {mutate: func(f *compatFixture) { f.creds.refusal = servicecred.RefusedRevoked }, header: issuerBearer,
			body: func(f *compatFixture) string { return issueBody(f.principal, "", "pwd") }, status: http.StatusUnauthorized, reason: reasonInvalidCredentials},
		"malformed body": {header: issuerBearer, body: func(*compatFixture) string { return `{"principal_id":1}` },
			status: http.StatusUnauthorized, reason: reasonMalformed},
		"unknown principal": {header: issuerBearer, body: func(*compatFixture) string { return issueBody(stranger, "", "pwd") },
			status: http.StatusForbidden, reason: reasonPrincipalNotFound},
		"not a member": {header: issuerBearer, body: func(f *compatFixture) string { return issueBody(f.principal, uuid.NewString(), "pwd") },
			status: http.StatusForbidden, reason: reasonNotEntitled},
	} {
		t.Run(name, func(t *testing.T) {
			signer := unreadableKeySigner(t)
			f := newCompatFixture(t)
			if tc.mutate != nil {
				tc.mutate(f)
			}
			f.issuer = newEdDSAIssuer(t, signer, testAudience)
			f.buildCompat()
			response := f.do(http.MethodPost, PatternIssueSession, tc.body(f), tc.header)
			expectError(t, response, tc.status, tc.reason)
			if n := signer.checks.Load(); n != 0 {
				t.Fatalf("%d signer checks for a request refused before any write", n)
			}
			if len(f.sessions.created) != 0 {
				t.Fatalf("%d sessions created", len(f.sessions.created))
			}
		})
	}
}

// C1 (ii): a refresh body that is not refresh-request.v1, or whose credential
// is outside the alphabet, is refused before the signer is checked.
func TestMalformedRefreshesNeverCheckTheSigner(t *testing.T) {
	signer := unreadableKeySigner(t)
	f, counter := newEdDSARefreshFixture(t, signer)
	bodies := []string{
		``, `not json`, `[]`, `{}`,
		refreshBody("short"),
		refreshBody(credential() + "!"),
		`{"schema_version":"refresh-request.v2","refresh_credential":"` + credential() + `"}`,
		`{"schema_version":"refresh-request.v1","refresh_credential":"` + credential() + `","extra":1}`,
	}
	for range 10 {
		for _, body := range bodies {
			expectError(t, f.do(http.MethodPost, PatternRefresh, body, nil), http.StatusUnauthorized, reasonMalformed)
		}
	}
	if n := signer.checks.Load(); n != 0 {
		t.Fatalf("%d signer checks for %d malformed refreshes, want 0", n, 10*len(bodies))
	}
	if n := counter.exchanges.Load(); n != 0 {
		t.Fatalf("%d exchanges for malformed refreshes", n)
	}
}

// C1 (i): one check answers for every request of the Issuer for signerCheckTTL,
// failure included; the next request after the window checks again.
func TestSignerCheckFailureIsMemoizedForTheWindow(t *testing.T) {
	signer := unreadableKeySigner(t)
	f, counter := newEdDSARefreshFixture(t, signer)
	clock := withClock(t, f.issuer)
	for range 25 {
		expectError(t, f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil), http.StatusServiceUnavailable, reasonDependency)
	}
	if n := signer.checks.Load(); n != 1 {
		t.Fatalf("%d signer checks for 25 refreshes inside the window, want 1", n)
	}
	clock.Advance(signerCheckTTL - time.Nanosecond)
	expectError(t, f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil), http.StatusServiceUnavailable, reasonDependency)
	if n := signer.checks.Load(); n != 1 {
		t.Fatalf("%d signer checks just inside the window, want 1", n)
	}
	clock.Advance(time.Nanosecond)
	expectError(t, f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil), http.StatusServiceUnavailable, reasonDependency)
	if n := signer.checks.Load(); n != 2 {
		t.Fatalf("%d signer checks after the window, want 2", n)
	}
	if n := counter.exchanges.Load(); n != 0 || signer.signs.Load() != 0 {
		t.Fatalf("%d exchanges, %d signs while the signer is unavailable", n, signer.signs.Load())
	}
}

// A success is memoized too: a key that turns bad inside the window is seen by
// the mint (503 after the write, the residual race) and by the next check
// after the window.
func TestSignerCheckSuccessIsMemoizedForTheWindow(t *testing.T) {
	signer, dir, _ := fileSigner(t)
	probe := &probeSigner{inner: signer}
	issuer := newEdDSAIssuer(t, probe, testAudience)
	clock := withClock(t, issuer)
	for range 10 {
		if err := issuer.Preflight(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := probe.checks.Load(); n != 1 {
		t.Fatalf("%d checks, want 1", n)
	}
	if err := os.Remove(filepath.Join(dir, "k1.pem")); err != nil {
		t.Fatal(err)
	}
	if err := issuer.Preflight(context.Background()); err != nil {
		t.Fatalf("inside the window the last success answers: %v", err)
	}
	clock.Advance(signerCheckTTL)
	if err := issuer.Preflight(context.Background()); !errors.Is(err, errIssue) {
		t.Fatalf("after the window: %v, want errIssue", err)
	}
	if n := probe.checks.Load(); n != 2 {
		t.Fatalf("%d checks, want 2", n)
	}
}

// A wall-clock step back is not a fresh answer: a negative age checks again.
func TestSignerCheckRunsAgainWhenTheClockGoesBack(t *testing.T) {
	probe := readableKeySigner(t)
	issuer := newEdDSAIssuer(t, probe, testAudience)
	clock := withClock(t, issuer)
	_ = issuer.Preflight(context.Background())
	clock.Advance(-time.Hour)
	_ = issuer.Preflight(context.Background())
	if n := probe.checks.Load(); n != 2 {
		t.Fatalf("%d checks, want 2", n)
	}
}

// ctxSigner fails its check with the context's error.
type ctxSigner struct{ checks atomic.Int64 }

func (s *ctxSigner) Sign(context.Context, string, []byte) (string, string, error) {
	return "", "", errors.New("unused")
}

func (s *ctxSigner) Check(ctx context.Context) error {
	s.checks.Add(1)
	return ctx.Err()
}

// A caller's cancelled request says nothing about the signer: its failure is
// not memoized for the next caller.
func TestSignerCheckDoesNotMemoizeACallersCancellation(t *testing.T) {
	signer := &ctxSigner{}
	issuer := newEdDSAIssuer(t, signer, testAudience)
	withClock(t, issuer)
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := issuer.Preflight(cancelled); !errors.Is(err, errIssue) {
		t.Fatalf("cancelled: %v", err)
	}
	if err := issuer.Preflight(context.Background()); err != nil {
		t.Fatalf("live caller after a cancelled one: %v", err)
	}
	if n := signer.checks.Load(); n != 2 {
		t.Fatalf("%d checks, want 2", n)
	}
}

// A burst of concurrent requests runs one check.
func TestSignerCheckBurstRunsOneCheck(t *testing.T) {
	probe := readableKeySigner(t)
	issuer := newEdDSAIssuer(t, probe, testAudience)
	withClock(t, issuer)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if err := issuer.Preflight(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if n := probe.checks.Load(); n != 1 {
		t.Fatalf("%d checks for a burst of 32, want 1", n)
	}
}

func TestPreflightIsANoOpUnderHS256(t *testing.T) {
	issuer, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, ctx := range []context.Context{context.Background(), cancelled} {
		if err := issuer.Preflight(ctx); err != nil {
			t.Fatalf("hs256 preflight: %v", err)
		}
	}
	if issuer.eddsa != nil {
		t.Fatal("an hs256 issuer holds an EdDSA minter")
	}
}

func TestPreflightRefusesAnIssuerWithoutExactlyOneClass(t *testing.T) {
	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	ed := newEdDSAIssuer(t, readableKeySigner(t), testAudience)
	for name, issuer := range map[string]*Issuer{
		"none": {verifier: hs.verifier, now: time.Now},
		"both": {signer: hs.signer, eddsa: ed.eddsa, verifier: hs.verifier, now: time.Now},
	} {
		if err := issuer.Preflight(context.Background()); !errors.Is(err, errIssue) {
			t.Fatalf("%s: %v, want errIssue", name, err)
		}
	}
}

// hs256_compat: the routes call the same collaborators as before and answer
// 200; the issuer holds no signer to check.
func TestRefreshAndIssueUnderHS256AreUnchanged(t *testing.T) {
	hs, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	f, counter := newRefreshFixtureWith(t, hs)
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code != http.StatusOK || counter.exchanges.Load() != 1 {
		t.Fatalf("refresh status %d, %d exchanges: %s", response.Code, counter.exchanges.Load(), response.Body)
	}
	if _, err := contracts.ParseRefreshResponse(repoRoot(t), response.Body.Bytes()); err != nil {
		t.Fatalf("not refresh-response.v1: %v", err)
	}
	c := newCompatFixture(t)
	issued := c.do(http.MethodPost, PatternIssueSession, issueBody(c.principal, c.org.String(), "pwd"), issuerBearer)
	if issued.Code != http.StatusOK || len(c.sessions.created) != 1 || len(c.projector.userCalls) != 1 || len(c.projector.orgCalls) != 1 {
		t.Fatalf("issue status %d, created %d, projected %v %v", issued.Code, len(c.sessions.created), c.projector.userCalls, c.projector.orgCalls)
	}
}

// eddsa with a readable key: one check, then the write, then one sign.
func TestRefreshAndIssueUnderEdDSACheckThenWriteThenSign(t *testing.T) {
	signer := readableKeySigner(t)
	f, counter := newEdDSARefreshFixture(t, signer)
	response := f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil)
	if response.Code != http.StatusOK || counter.exchanges.Load() != 1 || signer.checks.Load() != 1 || signer.signs.Load() != 1 {
		t.Fatalf("refresh status %d, exchanges %d, checks %d, signs %d: %s",
			response.Code, counter.exchanges.Load(), signer.checks.Load(), signer.signs.Load(), response.Body)
	}
	issueSigner := readableKeySigner(t)
	c := newCompatFixture(t)
	c.issuer = newEdDSAIssuer(t, issueSigner, testAudience)
	c.buildCompat()
	issued := c.do(http.MethodPost, PatternIssueSession, issueBody(c.principal, c.org.String(), "pwd"), issuerBearer)
	if issued.Code != http.StatusOK || len(c.sessions.created) != 1 || issueSigner.checks.Load() != 1 || issueSigner.signs.Load() != 1 {
		t.Fatalf("issue status %d, created %d, checks %d, signs %d: %s",
			issued.Code, len(c.sessions.created), issueSigner.checks.Load(), issueSigner.signs.Load(), issued.Body)
	}
}

// Outage behaviour (acp, class level): while the eddsa signer cannot sign,
// a refresh that a healthy signer would refuse with 401 answers 503 with
// nothing written, because Exchange, the step that finds the refusal, writes.
func TestRefreshOfARefusedCredentialAnswers503WhileTheSignerIsUnavailable(t *testing.T) {
	for _, reason := range []session.Reason{session.ReasonRefreshUnknown, session.ReasonSessionRevoked} {
		t.Run(string(reason), func(t *testing.T) {
			signer := unreadableKeySigner(t)
			f, counter := newEdDSARefreshFixture(t, signer)
			f.sessions.result = session.Result{Reason: reason}
			expectError(t, f.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil), http.StatusServiceUnavailable, reasonDependency)
			if counter.exchanges.Load() != 0 || len(f.sessions.revoked) != 0 || signer.signs.Load() != 0 {
				t.Fatalf("exchanges %d, revoked %v, signs %d", counter.exchanges.Load(), f.sessions.revoked, signer.signs.Load())
			}
			healthy, _ := newEdDSARefreshFixture(t, readableKeySigner(t))
			healthy.sessions.result = session.Result{Reason: reason}
			if code := healthy.do(http.MethodPost, PatternRefresh, refreshBody(credential()), nil).Code; code != http.StatusUnauthorized {
				t.Fatalf("control with a healthy signer: %d, want 401", code)
			}
		})
	}
}

// The issue twin: a user or organization the projector would find gone (403
// with a healthy signer) answers 503 with nothing projected or created.
func TestIssueForAGoneUserOrOrganizationAnswers503WhileTheSignerIsUnavailable(t *testing.T) {
	for name, mutate := range map[string]func(*compatFixture){
		"user gone":         func(f *compatFixture) { f.projector.gone[f.principal] = true },
		"organization gone": func(f *compatFixture) { delete(f.projector.orgs, f.org) },
	} {
		t.Run(name, func(t *testing.T) {
			signer := unreadableKeySigner(t)
			f := newCompatFixture(t)
			mutate(f)
			f.issuer = newEdDSAIssuer(t, signer, testAudience)
			f.buildCompat()
			response := f.do(http.MethodPost, PatternIssueSession, issueBody(f.principal, f.org.String(), "pwd"), issuerBearer)
			expectError(t, response, http.StatusServiceUnavailable, reasonDependency)
			if len(f.sessions.created) != 0 || len(f.projector.userCalls) != 0 || len(f.projector.orgCalls) != 0 || signer.signs.Load() != 0 {
				t.Fatalf("created %d, projected %v %v, signs %d", len(f.sessions.created), f.projector.userCalls, f.projector.orgCalls, signer.signs.Load())
			}
			healthy := newCompatFixture(t)
			mutate(healthy)
			healthy.issuer = newEdDSAIssuer(t, readableKeySigner(t), testAudience)
			healthy.buildCompat()
			if code := healthy.do(http.MethodPost, PatternIssueSession, issueBody(healthy.principal, healthy.org.String(), "pwd"), issuerBearer).Code; code != http.StatusForbidden {
				t.Fatalf("control with a healthy signer: %d, want 403", code)
			}
		})
	}
}

// blockedSigner's Check blocks until release is closed, like a key read the
// request deadline cannot interrupt.
type blockedSigner struct {
	entered chan struct{}
	release chan struct{}
	checks  atomic.Int64
}

func (s *blockedSigner) Sign(context.Context, string, []byte) (string, string, error) {
	return "", "", errors.New("unused")
}

func (s *blockedSigner) Check(context.Context) error {
	if s.checks.Add(1) == 1 {
		close(s.entered)
	}
	<-s.release
	return nil
}

// While one caller's check is blocked: (i) a waiter whose context ends returns
// before the release, with errIssue and its context error, and memoizes
// nothing; (ii) the holder's result is memoized; (iii) a waiter that gets the
// slot after that uses the memo, with no second check.
func TestSignerCheckWaitersHonourTheirContext(t *testing.T) {
	signer := &blockedSigner{entered: make(chan struct{}), release: make(chan struct{})}
	issuer := newEdDSAIssuer(t, signer, testAudience)
	withClock(t, issuer)

	holder := make(chan error, 1)
	go func() { holder <- issuer.Preflight(context.Background()) }()
	select {
	case <-signer.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the holder never reached the check")
	}

	patient := make(chan error, 1)
	go func() { patient <- issuer.Preflight(context.Background()) }()

	waiterCtx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() { waiter <- issuer.Preflight(waiterCtx) }()
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, errIssue) || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter: %v, want errIssue and context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("(i) the cancelled waiter did not return while the check was blocked")
	}
	select {
	case err := <-holder:
		t.Fatalf("the holder returned %v before the release", err)
	default:
	}

	close(signer.release)
	if err := <-holder; err != nil {
		t.Fatalf("holder: %v", err)
	}
	if err := <-patient; err != nil {
		t.Fatalf("(iii) waiter after the holder: %v", err)
	}
	if err := issuer.Preflight(context.Background()); err != nil {
		t.Fatalf("(ii) after the release: %v", err)
	}
	if n := signer.checks.Load(); n != 1 {
		t.Fatalf("%d checks, want 1: the holder's answer is the memo for every later caller", n)
	}
}

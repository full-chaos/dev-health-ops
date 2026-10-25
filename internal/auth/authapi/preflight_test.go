package authapi

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
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

// newEdDSARefreshFixture serves the session routes with an EdDSA issuer over
// signer and an Exchange that would rotate the presented credential.
func newEdDSARefreshFixture(t *testing.T, signer AccessSigner) (*fixture, *exchangeCounter) {
	t.Helper()
	f := newFixtureWith(t, newEdDSAIssuer(t, signer, testAudience))
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

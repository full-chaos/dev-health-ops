package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

// preflightSessions is a SessionIssuer whose Preflight answers err.
type preflightSessions struct {
	stubSessions
	mu     sync.Mutex
	err    error
	checks int
	issued int
}

func (p *preflightSessions) Preflight(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checks++
	return p.err
}

func (p *preflightSessions) Issue(context.Context, SessionRequest) (Tokens, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.issued++
	return Tokens{}, errUnused
}

// countingFlows consumes each flow once and counts every Consume and every
// first use.
type countingFlows struct {
	mu       sync.Mutex
	used     map[[32]byte]bool
	consumes int
	first    int
}

func (f *countingFlows) Begin(context.Context, Flow) error { return nil }

func (f *countingFlows) Consume(_ context.Context, hash [32]byte, _ string, _ uuid.UUID, _ time.Time) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.consumes++
	if f.used[hash] {
		return false, nil
	}
	f.used[hash] = true
	f.first++
	return true, nil
}

// offlineTransport records and fails every IdP call, so a callback that got
// past the flow store stops at its first IdP call without the network.
type offlineTransport struct {
	mu    sync.Mutex
	posts int
}

func (o *offlineTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if req.Method == http.MethodPost {
		o.posts++
	}
	return nil, errors.New("oidc test: offline")
}

// sealedCallback seals one state and login-nonce cookie and returns a builder
// of identical callback requests for it.
func (env *unitEnv) sealedCallback(t *testing.T, c routeCase, row storedRow) func() *http.Request {
	t.Helper()
	token, name := env.sealCallback(t, c, row)
	kind := "oidc"
	if c.oauth {
		kind = "oauth"
	}
	return func() *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/"+kind+"/"+row.ID.String()+"/callback",
			strings.NewReader(`{"code":"c","state":"`+token+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.AddCookie(&http.Cookie{Name: name, Value: "cookie-1"})
		return req
	}
}

// unavailableIssue is a SessionIssuer whose Issue cannot mint now.
type unavailableIssue struct{ stubSessions }

func (unavailableIssue) Issue(context.Context, SessionRequest) (Tokens, error) {
	return Tokens{}, fmt.Errorf("%w: signer", ErrSessionUnavailable)
}

// An Issue that reports ErrSessionUnavailable answers go-api's 503, not 500.
func TestResolveAnswers503WhenTheSessionCannotBeIssuedNow(t *testing.T) {
	svc := &Service{cfg: Config{Identities: &resolveIdentities{principal: Principal{ID: uuid.New(), Active: true}},
		Memberships: resolveMembers{found: true, role: "admin"}, Sessions: unavailableIssue{}}}
	_, failure := svc.resolve(context.Background(), Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "oidc"},
		validated{profile: profile{Email: "p@example.com", EmailVerified: true}}, nil)
	if failure == nil || failure.Status != http.StatusServiceUnavailable || failure.Reason != ReasonDependency ||
		!reflect.DeepEqual(failure.Detail, map[string]any{"message": sessionissue.UnavailableText}) {
		t.Fatalf("failure %+v, want go-api's 503", failure)
	}
	if UnavailableText != sessionissue.UnavailableText {
		t.Fatalf("UnavailableText %q is not go-api's %q", UnavailableText, sessionissue.UnavailableText)
	}
}

// A JIT new account whose session cannot be issued now answers 503, not 500.
func TestNewAccountAnswers503WhenTheSessionCannotBeIssuedNow(t *testing.T) {
	ids := &resolveIdentities{err: ErrIdentityNotFound}
	jit := &resolveJIT{err: fmt.Errorf("%w: signer", ErrSessionUnavailable)}
	_, failure, sessions := resolveRun("oidc", ids, resolveMembers{}, jit)
	if failure == nil || failure.Status != http.StatusServiceUnavailable || failure.Reason != ReasonDependency ||
		!reflect.DeepEqual(failure.Detail, map[string]any{"message": sessionissue.UnavailableText}) {
		t.Fatalf("failure %+v, want go-api's 503", failure)
	}
	if jit.writes != 0 || len(sessions.issued) != 0 {
		t.Fatalf("writes %d, sessions %d", jit.writes, len(sessions.issued))
	}
	other := &resolveJIT{err: errors.New("tx failed")}
	if _, failure, _ := resolveRun("oidc", &resolveIdentities{err: ErrIdentityNotFound}, resolveMembers{}, other); failure == nil ||
		failure.Status != http.StatusInternalServerError {
		t.Fatalf("another JIT failure %+v, want 500", failure)
	}
}

// A callback checks the signer after its state and nonce checks and before it
// consumes the flow: on a failed check it answers go-api's 503, consumes
// nothing and keeps the nonce cookie, and the same callback succeeds past the
// flow store once the signer is back.
func TestCallbackChecksTheSignerBeforeTheFlowIsConsumed(t *testing.T) {
	for _, c := range routeCases {
		if !c.callback {
			continue
		}
		t.Run(c.route, func(t *testing.T) {
			row := c.activeRow()
			if !c.oauth {
				// Endpoints in the row: the first IdP call is the token exchange.
				row.Config = `{"client_id":"client-1","issuer":"https://idp.example",` +
					`"token_endpoint":"https://idp.example/token","jwks_uri":"https://idp.example/jwks"}`
			}
			env := newUnitEnv(t, &fakeProviders{before: row})
			flows := &countingFlows{used: map[[32]byte]bool{}}
			sessions := &preflightSessions{err: errors.New("signer unavailable")}
			transport := &offlineTransport{}
			env.svc.cfg.Flows, env.svc.cfg.Sessions = flows, sessions
			env.svc.cfg.HTTPClient = &http.Client{Transport: transport}
			callback := env.sealedCallback(t, c, row)

			rec := env.serve(c, callback())
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("status %d, want 503: %s", rec.Code, rec.Body)
			}
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if want := map[string]any{"detail": map[string]any{"message": sessionissue.UnavailableText}}; !reflect.DeepEqual(body, want) {
				t.Fatalf("body %s, want %v", rec.Body, want)
			}
			if got := rec.Header().Values("Set-Cookie"); len(got) != 0 {
				t.Fatalf("Set-Cookie %v on a refusal that consumed nothing", got)
			}
			if flows.consumes != 0 || sessions.checks != 1 || sessions.issued != 0 || transport.posts != 0 {
				t.Fatalf("consumes %d, checks %d, issued %d, IdP posts %d", flows.consumes, sessions.checks, sessions.issued, transport.posts)
			}

			sessions.err = nil
			rec = env.serve(c, callback())
			if flows.consumes != 1 || flows.first != 1 {
				t.Fatalf("retry: consumes %d, first uses %d; want the same flow consumed once, for the first time", flows.consumes, flows.first)
			}
			if rec.Code == http.StatusServiceUnavailable {
				t.Fatalf("retry answered 503 with a healthy signer: %s", rec.Body)
			}
			if transport.posts != 1 {
				t.Fatalf("retry: %d IdP token exchanges after the flow was consumed, want 1", transport.posts)
			}
		})
	}
}

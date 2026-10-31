package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// stateRowsDocument is an endpoint-profile document that holds the session
// state route's row, as B's contract change will.
var stateRowsDocument = []byte(`{"rows":[{"service":"` + AccessService + `","method":"POST","route":"` + PatternSessionState +
	`","accepted_credential_classes":["internal_svc_authsess_token","workload_token"]}]}`)

// countingCurrent counts the session reads of the state route.
type countingCurrent struct {
	CompatSessions
	mu    sync.Mutex
	calls int
}

func (c *countingCurrent) Current(ctx context.Context, id uuid.UUID) (session.Record, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.CompatSessions.Current(ctx, id)
}

func (c *countingCurrent) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// stateServer mounts the session state route over f's sessions, issuer and
// credentials as CompatRoutes would with rows as the embedded document.
func stateServer(t *testing.T, f *compatFixture, rows []byte) http.Handler {
	server, _ := stateServerCounting(t, f, rows)
	return server
}

func stateServerCounting(t *testing.T, f *compatFixture, rows []byte) (http.Handler, *countingCurrent) {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(f.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	meter := f.meter
	if meter == nil {
		meter = otel.Meter("session-state-test")
	}
	rec, err := newRecorder(logger, meter)
	if err != nil {
		t.Fatal(err)
	}
	sessions := &countingCurrent{CompatSessions: f.sessions}
	deps := CompatDeps{Sessions: sessions, Users: f.users, Issuer: f.issuer, IssueWorkload: f.workload, Logger: logger}
	if f.creds != nil {
		deps.ServiceCredentials = f.creds
	}
	h := compatHandlers{CompatDeps: deps, rec: rec}
	mux := http.NewServeMux()
	for _, route := range MarkResponseModels(h.withSessionStateRouteRows(nil, rows)) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(flaggedHandler(route)))
	}
	return mux, sessions
}

func stateBody(id string) string { return `{"session_id":"` + id + `"}` }

func doState(t *testing.T, server http.Handler, body string, header map[string]string) (int, map[string]any) {
	t.Helper()
	f := &fixture{server: server}
	response := f.do(http.MethodPost, PatternSessionState, body, header)
	var answer map[string]any
	if strings.HasPrefix(response.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(response.Body.Bytes(), &answer); err != nil {
			t.Fatalf("answer %q: %v", response.Body, err)
		}
	}
	return response.Code, answer
}

// The route mounts only when the embedded endpoint-profile document holds its
// row. This build's document has no row yet, so CompatRoutes does not serve it
// and says so.
func TestSessionStateRouteIsNotMountedWithoutItsProfileRow(t *testing.T) {
	f := newCompatFixture(t)
	if response := f.do(http.MethodPost, PatternSessionState, stateBody(f.record.ID.String()), issuerBearer); response.Code != http.StatusNotFound {
		t.Fatalf("POST %s = %d, want 404 (no endpoint-profile row in this build)", PatternSessionState, response.Code)
	}
	if !strings.Contains(f.logs.String(), "auth session route not mounted: no endpoint-profile row") {
		t.Fatalf("no line names the unmounted route: %s", f.logs)
	}
	if code, _ := doState(t, stateServer(t, f, []byte(`{"rows":[]}`)), stateBody(f.record.ID.String()), issuerBearer); code != http.StatusNotFound {
		t.Fatalf("with a document without the row the route answers %d, want 404", code)
	}
	if code, _ := doState(t, stateServer(t, f, stateRowsDocument), stateBody(f.record.ID.String()), issuerBearer); code != http.StatusOK {
		t.Fatalf("with the row the route answers %d, want 200", code)
	}
}

func TestSessionStateAnswersTheSessionAsAuthServiceDecidesIt(t *testing.T) {
	f := newCompatFixture(t)
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	f.issuer.now = func() time.Time { return now }
	pin := int64(7)
	org := uuid.New()
	revokedAt := now.Add(-time.Minute)
	live := session.Record{ID: uuid.New(), PrincipalID: f.principal, OrganizationID: &org, ExpiresAt: now.Add(time.Nanosecond), LegacyTokenVersion: &pin}
	revoked := live
	revoked.ID, revoked.RevokedAt = uuid.New(), &revokedAt
	atExpiry := live
	atExpiry.ID, atExpiry.ExpiresAt = uuid.New(), now
	unbound := live
	unbound.ID, unbound.OrganizationID, unbound.LegacyTokenVersion = uuid.New(), nil, nil
	f.sessions.records = map[uuid.UUID]session.Record{live.ID: live, revoked.ID: revoked, atExpiry.ID: atExpiry, unbound.ID: unbound}
	server := stateServer(t, f, stateRowsDocument)
	for name, tc := range map[string]struct {
		id   uuid.UUID
		want string
	}{
		"live":                   {live.ID, `{"principal_id":"` + f.principal.String() + `","organization_id":"` + org.String() + `","revoked":false,"expired":false,"legacy_token_version":7}`},
		"revoked":                {revoked.ID, `{"principal_id":"` + f.principal.String() + `","organization_id":"` + org.String() + `","revoked":true,"expired":false,"legacy_token_version":7}`},
		"expiry not after now":   {atExpiry.ID, `{"principal_id":"` + f.principal.String() + `","organization_id":"` + org.String() + `","revoked":false,"expired":true,"legacy_token_version":7}`},
		"no organization or pin": {unbound.ID, `{"principal_id":"` + f.principal.String() + `","organization_id":null,"revoked":false,"expired":false,"legacy_token_version":null}`},
	} {
		t.Run(name, func(t *testing.T) {
			response := (&fixture{server: server}).do(http.MethodPost, PatternSessionState, stateBody(tc.id.String()), issuerBearer)
			if response.Code != http.StatusOK || strings.TrimSpace(response.Body.String()) != tc.want {
				t.Fatalf("%d %s, want 200 %s", response.Code, response.Body, tc.want)
			}
		})
	}
	if len(f.sessions.created) != 0 || len(f.sessions.revoked) != 0 || len(f.sessions.revokedAll) != 0 || len(f.appended) != 0 {
		t.Fatalf("a state read wrote: created %d, revoked %v, revoked all %v, audit %d", len(f.sessions.created), f.sessions.revoked, f.sessions.revokedAll, len(f.appended))
	}
}

func TestSessionStateRefusesAndReadsNothing(t *testing.T) {
	f := newCompatFixture(t)
	server, sessions := stateServerCounting(t, f, stateRowsDocument)
	id := f.record.ID.String()
	for name, tc := range map[string]struct {
		body   string
		header map[string]string
		status int
		reason string
	}{
		"no bearer":                {stateBody(id), nil, http.StatusUnauthorized, reasonInvalidCredentials},
		"not a service credential": {stateBody(id), map[string]string{"Authorization": "Bearer fcacr_0123456789abcdef"}, http.StatusUnauthorized, reasonInvalidCredentials},
		"not json":                 {`session_id`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"empty object":             {`{}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"another member":           {`{"session_id":"` + id + `","principal_id":"` + id + `"}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"twice":                    {`{"session_id":"` + id + `","session_id":"` + id + `"}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"not a uuid":               {stateBody("ses_x"), issuerBearer, http.StatusBadRequest, reasonMalformed},
		"upper case uuid":          {stateBody(strings.ToUpper(id)), issuerBearer, http.StatusBadRequest, reasonMalformed},
		"a number":                 {`{"session_id":7}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"null":                     {`{"session_id":null}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
		"trailing data":            {stateBody(id) + `{}`, issuerBearer, http.StatusBadRequest, reasonMalformed},
	} {
		t.Run(name, func(t *testing.T) {
			before := sessions.count()
			code, answer := doState(t, server, tc.body, tc.header)
			if code != tc.status || answer["reason_code"] != tc.reason {
				t.Fatalf("%d %v, want %d %s", code, answer, tc.status, tc.reason)
			}
			if got := sessions.count() - before; got != 0 {
				t.Fatalf("a refused request read %d session(s)", got)
			}
		})
	}
	f.creds.refusal = "revoked"
	if code, answer := doState(t, server, stateBody(id), issuerBearer); code != http.StatusUnauthorized || answer["reason_code"] != reasonInvalidCredentials {
		t.Fatalf("a refused service credential: %d %v, want 401 invalid_credentials", code, answer)
	}
}

func TestSessionStateNamesAnUnknownSessionAndFailsClosedOnTheStore(t *testing.T) {
	f := newCompatFixture(t)
	server := stateServer(t, f, stateRowsDocument)
	if code, answer := doState(t, server, stateBody(uuid.NewString()), issuerBearer); code != http.StatusNotFound || answer["reason_code"] != reasonSessionNotFound {
		t.Fatalf("an unknown session: %d %v, want 404 %s", code, answer, reasonSessionNotFound)
	}
	f.sessions.currentErr = errors.New("connection refused")
	if code, answer := doState(t, server, stateBody(f.record.ID.String()), issuerBearer); code != http.StatusServiceUnavailable || answer["reason_code"] != reasonStoreUnavailable {
		t.Fatalf("a store failure: %d %v, want 503 %s", code, answer, reasonStoreUnavailable)
	}
	f.sessions.currentErr = nil
	f.creds.err = errors.New("credential store down")
	if code, answer := doState(t, server, stateBody(f.record.ID.String()), issuerBearer); code != http.StatusServiceUnavailable {
		t.Fatalf("a credential store failure: %d %v, want 503", code, answer)
	}
}

// The go-api workload token is admitted as on the session issue route, and
// without a service credential lookup.
func TestSessionStateAcceptsTheGoAPIWorkloadToken(t *testing.T) {
	w := newWorkloadIssueFixture(t)
	server := stateServer(t, w.compatFixture, stateRowsDocument)
	header, _ := w.mint("workload+jwt", nil)
	if code, answer := doState(t, server, stateBody(w.record.ID.String()), header); code != http.StatusOK || answer["principal_id"] != w.principal.String() {
		t.Fatalf("%d %v, want 200 for the go-api workload token", code, answer)
	}
	if len(w.creds.seen) != 0 {
		t.Fatalf("the workload path checked a service credential: %v", w.creds.seen)
	}
	wrongAudience, _ := w.mint("workload+jwt", func(c *contracts.WorkloadTokenClaims) { c.Aud = "dev-health-api" })
	if code, _ := doState(t, server, stateBody(w.record.ID.String()), wrongAudience); code != http.StatusUnauthorized {
		t.Fatalf("a workload token for another audience: %d, want 401", code)
	}
}

package authapi

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

// The three auth-service refresh rows (POST /v1/token/refresh, POST /api/v1/auth/refresh and POST
// /api/v1/auth/logout) take a refresh credential. An ACCESS token of any kind (the legacy HS256 access
// token, the EdDSA access token of this package's real issuer, every accept vector of the access-token.v1
// corpus) is refused at the credential gate: the real session.Store runs over a pool that refuses and counts
// every connection, so "no store lookup" is a measured zero, and stubs count the user reads, the principal
// projections and the audit rows. The refusal is the same status and the same bytes as the refusal of a
// meaningless value, so a caller cannot tell an access token from garbage.

type countingUsers struct {
	Users
	calls atomic.Int64
}

func (u *countingUsers) State(ctx context.Context, id uuid.UUID) (UserState, bool, error) {
	u.calls.Add(1)
	return u.Users.State(ctx, id)
}

func (u *countingUsers) Role(ctx context.Context, user, org uuid.UUID) (*string, bool, error) {
	u.calls.Add(1)
	return u.Users.Role(ctx, user, org)
}

// countingProjector accepts every principal and organization and counts the calls.
type countingProjector struct{ users, orgs atomic.Int64 }

func (p *countingProjector) Project(context.Context, uuid.UUID) error {
	p.users.Add(1)
	return nil
}

func (p *countingProjector) ProjectOrganization(context.Context, uuid.UUID) error {
	p.orgs.Add(1)
	return nil
}

var requestStamp = regexp.MustCompile(`"request_id":"[^"]*"|"occurred_at":"[^"]*"`)

// sameBytes is the body with the two fields that differ per call (the request id and the clock) removed.
func sameBytes(body string) string { return requestStamp.ReplaceAllString(body, "") }

type refreshRig struct {
	t         *testing.T
	connects  *usertokens.Counter
	base      int64
	users     *countingUsers
	projector *countingProjector
	issuer    *Issuer
	mints     atomic.Int64
	audited   atomic.Int64
	logs      *bytes.Buffer
	server    http.Handler
}

func newRefreshRig(t *testing.T) *refreshRig {
	t.Helper()
	pool, connects := usertokens.CountingPool(t)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	// The store logs into the same buffer as the handlers, so the leak check covers its lines too.
	store, err := session.New(session.Config{Pool: pool, Schema: schema, Logger: logger,
		SuccessorKey: contracts.NewSecretString("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")})
	if err != nil {
		t.Fatal(err)
	}
	// The issuer shares the key, issuer and audience of the minted legacy tokens, so a refusal of one of them
	// is the refresh gate's decision and not a signature mismatch.
	issuer, err := NewIssuer(usertokens.Secret, usertokens.LegacyIssuer, usertokens.Audience)
	if err != nil {
		t.Fatal(err)
	}
	principal := uuid.New()
	r := &refreshRig{
		t: t, connects: connects, logs: logs, issuer: issuer,
		users: &countingUsers{Users: fakeUsers{
			users: map[uuid.UUID]UserState{principal: {Email: "ada@example.com", IsActive: true}},
			roles: map[[2]uuid.UUID]*string{},
		}},
		projector: &countingProjector{},
	}
	// Every mint reads the issuer's clock, so a counted clock counts mints.
	issuer.now = func() time.Time { r.mints.Add(1); return time.Now().UTC() }
	routes, err := Routes(Deps{Sessions: store, Users: r.users, Issuer: issuer, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	compat, err := CompatRoutes(CompatDeps{
		Sessions: store, Users: r.users, Issuer: issuer, Logger: logger, Projector: r.projector,
		AppendAudit: func(context.Context, audit.AuditEvent) error { r.audited.Add(1); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range append(routes, compat...) {
		mux.Handle(route.Method+" "+route.Pattern, requestid.Middleware(route.Handler))
	}
	r.server = mux
	r.base = connects.Count()
	return r
}

func (r *refreshRig) post(pattern, body string) (int, string) {
	request := httptest.NewRequest(http.MethodPost, pattern, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	r.server.ServeHTTP(response, request)
	return response.Code, response.Body.String()
}

func (r *refreshRig) lookups() int64 { return r.connects.Count() - r.base }

type refreshRow struct {
	name, pattern string
	body          func(token string) string
	// control is the kind that is this row's genuine credential shape, not a refused one.
	control string
	// refusedStatus is the status every refused input gets.
	refusedStatus int
	garbage       string
}

func refreshRows() []refreshRow {
	native := func(token string) string {
		return `{"schema_version":"refresh-request.v1","refresh_credential":` + quoteJSON(token) + `}`
	}
	compat := func(token string) string { return `{"refresh_token":` + quoteJSON(token) + `}` }
	return []refreshRow{
		{name: "POST /v1/token/refresh", pattern: PatternRefresh, body: native, refusedStatus: http.StatusUnauthorized, garbage: "not-a-credential"},
		// A go-api refresh JWT is this row's genuine credential (exchanged once): "legacy-refresh" is its control.
		{name: "POST /api/v1/auth/refresh", pattern: PatternCompatRefresh, body: compat, control: "legacy-refresh", refusedStatus: http.StatusUnauthorized, garbage: "aaaa.bbbb.cccc"},
		{name: "POST /api/v1/auth/logout", pattern: PatternCompatLogout, body: compat, refusedStatus: http.StatusOK, garbage: "not-a-credential"},
	}
}

func quoteJSON(s string) string { return `"` + strings.ReplaceAll(s, `"`, `\"`) + `"` }

func TestRefreshRowsRefuseUserTokens(t *testing.T) {
	tokens := userTokensWithIssuer(t)
	for _, row := range refreshRows() {
		t.Run(row.name, func(t *testing.T) {
			r := newRefreshRig(t)
			wantStatus, wantBody := r.post(row.pattern, row.body(row.garbage))
			if wantStatus != row.refusedStatus {
				t.Fatalf("the reference refusal (a meaningless value) is %d %s, want %d", wantStatus, wantBody, row.refusedStatus)
			}
			for _, tok := range tokens {
				if tok.Kind == row.control {
					continue
				}
				status, body := r.post(row.pattern, row.body(tok.Value))
				if status != wantStatus || sameBytes(body) != sameBytes(wantBody) {
					t.Errorf("%s: %d %s, want the refusal of a meaningless value: %d %s", tok.Kind, status, body, wantStatus, wantBody)
				}
				usertokens.NoLeak(t, tok.Kind+" body", body, tokens)
				if strings.Contains(body, "access_token") || strings.Contains(body, "refresh_credential") || strings.Contains(body, "refresh_token") {
					t.Errorf("%s: the refusal body carries a token field: %s", tok.Kind, body)
				}
			}
			if got := r.lookups(); got != 0 {
				t.Errorf("an access token reached the store: %d connection attempt(s), want 0", got)
			}
			if n := r.users.calls.Load(); n != 0 {
				t.Errorf("an access token caused %d user/role read(s), want 0", n)
			}
			if n := r.projector.users.Load() + r.projector.orgs.Load(); n != 0 {
				t.Errorf("an access token projected %d principal(s)/organization(s), want 0", n)
			}
			if n := r.mints.Load(); n != 0 {
				t.Errorf("an access token caused %d token mint(s), want 0", n)
			}
			if n := r.audited.Load(); n != 0 {
				t.Errorf("an access token wrote %d audit row(s), want 0", n)
			}
			usertokens.NoLeak(t, "the logs", r.logs.String(), tokens)
		})
	}
}

// The mint counter sees a mint: the counted clock moves when the real issuer mints a token.
func TestRefreshRowsControlTheMintCounterSeesAMint(t *testing.T) {
	r := newRefreshRig(t)
	if _, err := r.issuer.Access(Grant{PrincipalID: uuid.New(), SessionID: uuid.New(), Role: "member"}); err != nil {
		t.Fatal(err)
	}
	if r.mints.Load() == 0 {
		t.Fatal("a minted token left the mint counter at zero")
	}
}

// The controls: each row's genuine credential passes the same gate and moves the counters, so a zero above is
// the gate's work and the counters can see a lookup. For logout a zero connection count also means no session
// was looked up, revoked or written: RevokeByCredential reads and writes only through the counted pool.
func TestRefreshRowsControlAGenuineCredentialReachesTheStore(t *testing.T) {
	opaque := "dhr1_" + strings.Repeat("A", 43)
	for _, row := range refreshRows() {
		t.Run(row.name, func(t *testing.T) {
			r := newRefreshRig(t)
			credential := opaque
			if row.control != "" {
				for _, tok := range usertokens.All(t) {
					if tok.Kind == row.control {
						credential = tok.Value
					}
				}
			}
			r.post(row.pattern, row.body(credential))
			if r.lookups() == 0 {
				t.Fatal("a genuine credential never reached the store: the counting pool detects nothing")
			}
			if row.control != "" && r.projector.users.Load() == 0 {
				t.Fatal("a genuine go-api refresh token was not projected: the projector counter detects nothing")
			}
		})
	}
}

package apiservice

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"log/slog"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/ingestpg"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/usertokens"
)

const (
	pushRoutePrefix        = "/api/v1/external-ingest"
	pushAttemptLimitID     = "external_ingest_auth_attempt"
	pushFailureLimitID     = "external_ingest_auth_failure"
	pushInvalidTokenBody   = `{"error":{"code":"invalid_token","message":"Missing or invalid ingest token"}}`
	pushIngestionID        = "11111111-1111-4111-8111-111111111111"
	pushSchemaVersion      = "external-ingest.v1"
	pushFailureLimitPerMin = 30
)

// pushLimiterStore is the limiter's counter store with every call counted by
// limit ID. reset swaps in a fresh window, so each matrix cell starts at zero
// and the 30-per-minute failure limit never turns a later cell into a 429.
type pushLimiterStore struct {
	mu         sync.Mutex
	inner      httpapi.CounterStore
	increments map[string]int
	peeks      map[string]int
}

func (s *pushLimiterStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inner = httpapi.NewMemoryCounters(time.Now)
	s.increments, s.peeks = map[string]int{}, map[string]int{}
}

func (s *pushLimiterStore) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	s.mu.Lock()
	s.increments[hit.Limit.ID]++
	inner := s.inner
	s.mu.Unlock()
	return inner.Increment(ctx, hit)
}

func (s *pushLimiterStore) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	s.mu.Lock()
	s.peeks[hit.Limit.ID]++
	inner := s.inner
	s.mu.Unlock()
	return inner.Peek(ctx, hit)
}

func (s *pushLimiterStore) Backend() string { return "memory" }

func (s *pushLimiterStore) counts() (increments map[string]int, peeks map[string]int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	increments, peeks = map[string]int{}, map[string]int{}
	for id, n := range s.increments {
		increments[id] = n
	}
	for id, n := range s.peeks {
		peeks[id] = n
	}
	return increments, peeks
}

type pushLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *pushLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *pushLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type pushRig struct {
	handler http.Handler
	counts  *lookupCounts
	minter  *eddsaMinter
	limiter *pushLimiterStore
	logs    *pushLogBuffer
	classes map[string]matrixClass
}

// newPushRig is the api listener as configureWith builds it (the real row
// tables, the real access validator in routed mode, the three scope layers)
// with the REAL externalingest routes in place of probeEcho. The key source,
// the HS256 path, the store and the limiter store are counting doubles; pool
// is the database the ingest handlers get.
func newPushRig(t *testing.T, pool *pgxpool.Pool) *pushRig {
	t.Helper()
	counts := &lookupCounts{}
	minter := newEdDSAMinter(t)
	logs := &pushLogBuffer{}
	logger := slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	store := matrixStore{probeStore: newProbeStore(), counts: counts}
	hs, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(hs, store, logger)
	if err != nil {
		t.Fatal(err)
	}
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: matrixKeys{inner: minter.keys, counts: counts}})
	if err != nil {
		t.Fatal(err)
	}
	legacy := legacyVerifier(auth.AuthenticateLegacy)
	validator, err := accessverify.New(accessverify.Config{
		Rows:  realRows(t),
		EdDSA: eddsa,
		Legacy: func(ctx context.Context, token string) (any, error) {
			counts.add(&counts.legacy)
			return legacy(ctx, token)
		},
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
		Logger: logger,
		Now:    func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(matrixValidator{inner: routedAccessVerify{validator}, counts: counts}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	scopeRows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	scope.DeclaredRows(scopeRows)
	limiter := &pushLimiterStore{}
	limiter.reset()
	routes := externalingest.Routes(externalingest.Deps{Pool: pool, Logger: logger, Counters: limiter})
	if len(routes) != 7 {
		t.Fatalf("externalingest mounts %d routes, want 7", len(routes))
	}
	server, err := NewServer(apiTestConfig(), logger, routes, scope.Layers()...)
	if err != nil {
		t.Fatal(err)
	}
	classes := map[string]matrixClass{}
	for _, class := range loadMatrixClasses(t) {
		classes[class.id] = class
	}
	return &pushRig{handler: server.Handler(), counts: counts, minter: minter, limiter: limiter, logs: logs, classes: classes}
}

type pushRoute struct{ method, pattern string }

var pushProtectedRoutes = []pushRoute{
	{"GET", pushRoutePrefix + "/availability"},
	{"POST", pushRoutePrefix + "/validate"},
	{"POST", pushRoutePrefix + "/batches"},
	{"GET", pushRoutePrefix + "/batches"},
	{"GET", pushRoutePrefix + "/batches/{ingestion_id}"},
}

var pushRouteScopes = map[string]string{
	"GET " + pushRoutePrefix + "/availability":           "schema:read",
	"POST " + pushRoutePrefix + "/validate":              "schema:read",
	"POST " + pushRoutePrefix + "/batches":               "ingest:write",
	"GET " + pushRoutePrefix + "/batches":                "ingest:status",
	"GET " + pushRoutePrefix + "/batches/{ingestion_id}": "ingest:status",
}

var pushPublicRoutes = []pushRoute{
	{"GET", pushRoutePrefix + "/schemas"},
	{"GET", pushRoutePrefix + "/schemas/{schema_version}"},
}

func (r pushRoute) path() string {
	return strings.NewReplacer("{ingestion_id}", pushIngestionID, "{schema_version}", pushSchemaVersion).Replace(r.pattern)
}

type pushAnswer struct {
	status int
	body   string
	header http.Header
}

func (rig *pushRig) serve(route pushRoute, headers map[string]string) pushAnswer {
	rig.counts.reset()
	var body *strings.Reader
	if route.method == http.MethodPost {
		body = strings.NewReader(`{}`)
	} else {
		body = strings.NewReader("")
	}
	request := httptest.NewRequest(route.method, route.path(), body)
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	rig.handler.ServeHTTP(recorder, request)
	return pushAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

type pushCell struct {
	name    string
	headers map[string]string
}

// pushWrongClassCells is one request head per credential class (and the
// credential-less head) that is NOT an external_push_ingest_token.
func pushWrongClassCells(t *testing.T, rig *pushRig) []pushCell {
	t.Helper()
	carriers := &matrixRig{minter: rig.minter}
	carrier := func(class string) map[string]string {
		c, ok := rig.classes[class]
		if !ok {
			t.Fatalf("credential class %s is not in credential-classes.json", class)
		}
		headers, driven := carriers.matrixCarrier(t, c)
		if !driven {
			t.Fatalf("no carrier for class %s", class)
		}
		return headers
	}
	signer, err := edgetoken.NewSigner(probeKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := signer.Refresh(edgetoken.RefreshClaims{UserID: probeUser.String(), OrgID: probeOrg.String(), FamilyID: uuid.NewString()}, probeNow, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	opaque := make([]byte, 32)
	for i := range opaque {
		opaque[i] = byte(i + 1)
	}
	retiredKey := "retired-ingest-api-key-" + strings.Repeat("K", 24)
	return []pushCell{
		{"ops_access_token_hs256", carrier("ops_access_token_hs256")},
		{"user_access_token", carrier("user_access_token")},
		{"ops_refresh_token", bearer(refresh)},
		{"refresh_credential", bearer("dhr1_" + base64.RawURLEncoding.EncodeToString(opaque))},
		{"workload_token", carrier("workload_token")},
		{"acr_client_credential", carrier("acr_client_credential")},
		{"internal_svc_acr_token", carrier("internal_svc_acr_token")},
		{"internal_svc_worker_token", carrier("internal_svc_worker_token")},
		{"internal_svc_authsess_token", carrier("internal_svc_authsess_token")},
		{"legacy_ingest_key_x_api_key", map[string]string{"X-API-Key": retiredKey}},
		{"legacy_ingest_key_bearer", bearer(retiredKey)},
		{"stripe_webhook_signature", carrier("stripe_webhook_signature")},
		{"github_webhook_signature", carrier("github_webhook_signature")},
		{"pagerduty_webhook_signature", carrier("pagerduty_webhook_signature")},
		{"no_credential", map[string]string{}},
	}
}

func pushTokens(cells []pushCell) []usertokens.Token {
	var out []usertokens.Token
	for _, cell := range cells {
		for name, value := range cell.headers {
			value = strings.TrimPrefix(value, "Bearer ")
			if len(value) >= 8 {
				out = append(out, usertokens.Token{Kind: cell.name + ":" + name, Value: value})
			}
		}
	}
	return out
}

func pushMustHeaderNotEcho(t *testing.T, where string, header http.Header, tokens []usertokens.Token) {
	t.Helper()
	var flat strings.Builder
	for name, values := range header {
		flat.WriteString(name + ": " + strings.Join(values, ",") + "\n")
	}
	usertokens.NoLeak(t, where+" headers", flat.String(), tokens)
}

// TestExternalIngestWrongClassMatrix drives the five protected push routes
// with every credential class that is not an fcpush_ token, through the api
// listener's production chain and the real externalingest handlers. Each
// cell must answer the one canonical invalid_token 401, reach no database
// and no other class's validator, count exactly one attempt-limiter hit and
// one failure-limiter hit, and echo nothing.
func TestExternalIngestWrongClassMatrix(t *testing.T) {
	pg, pool := ingestpg.Start(t)
	connects := pg.Connects
	rig := newPushRig(t, pool)
	cells := pushWrongClassCells(t, rig)
	tokens := pushTokens(cells)
	for _, route := range pushProtectedRoutes {
		for _, cell := range cells {
			t.Run(route.method+" "+route.pattern+"/"+cell.name, func(t *testing.T) {
				rig.limiter.reset()
				before := connects()
				got := rig.serve(route, cell.headers)
				if got.status != http.StatusUnauthorized {
					t.Fatalf("status %d, want 401; body %s", got.status, got.body)
				}
				if got.body != pushInvalidTokenBody {
					t.Errorf("body %q, want the canonical %q", got.body, pushInvalidTokenBody)
				}
				if d := connects() - before; d != 0 {
					t.Errorf("%d database connect(s), want 0", d)
				}
				if n := rig.counts.total(); n != 0 {
					t.Errorf("%d key/legacy/store lookup(s), want 0 (keys %d, legacy %d, store %d)", n, rig.counts.keys, rig.counts.legacy, rig.counts.store)
				}
				if rig.counts.validate != 0 {
					t.Errorf("the access validator was called %d time(s), want 0", rig.counts.validate)
				}
				increments, _ := rig.limiter.counts()
				if increments[pushAttemptLimitID] != 1 || increments[pushFailureLimitID] != 1 || len(increments) != 2 {
					t.Errorf("limiter hits %v, want exactly one attempt and one failure hit", increments)
				}
				if rows := pg.AuditRows(); len(rows) != 0 {
					t.Errorf("%d audit row(s) for a wrong class, want 0", len(rows))
				}
				usertokens.NoLeak(t, "body", got.body, tokens)
				pushMustHeaderNotEcho(t, "response", got.header, tokens)
			})
		}
	}
	usertokens.NoLeak(t, "the logs", rig.logs.String(), tokens)
	if n := connects(); n != 0 {
		t.Errorf("a wrong class reached the database: %d connect(s)", n)
	}
	if n := pg.TokenSelects(); n != 0 {
		t.Errorf("a wrong class ran %d token lookup(s)", n)
	}
}

// The failure limit is per address: the 31st failure from one address is the
// 429, still without a database connect.
func TestExternalIngestWrongClassMatrixThrottlesThe31stFailure(t *testing.T) {
	pg, pool := ingestpg.Start(t)
	rig := newPushRig(t, pool)
	rig.limiter.reset()
	route := pushProtectedRoutes[0]
	for i := 1; i <= pushFailureLimitPerMin; i++ {
		if got := rig.serve(route, nil); got.status != http.StatusUnauthorized || got.body != pushInvalidTokenBody {
			t.Fatalf("failure %d: %d %s, want the canonical 401", i, got.status, got.body)
		}
	}
	got := rig.serve(route, nil)
	if got.status != http.StatusTooManyRequests || !strings.Contains(got.body, `"rate_limited"`) {
		t.Errorf("failure 31: %d %s, want 429 rate_limited", got.status, got.body)
	}
	if pg.Connects() != 0 || len(pg.AuditRows()) != 0 {
		t.Errorf("%d database connect(s) and %d audit row(s), want 0 and 0", pg.Connects(), len(pg.AuditRows()))
	}
}

// Controls: an fcpush_ token reaches the external_ingest_tokens SELECT, so the
// zeros of the matrix mean something. An unknown one gets the same canonical
// 401 as every wrong class; a known one without the route's scope gets the
// 403 insufficient_scope.
func TestExternalIngestWrongClassMatrixControls(t *testing.T) {
	pg, pool := ingestpg.Start(t)
	rig := newPushRig(t, pool)
	pushToken := "fcpush_" + strings.Repeat("P", 40)
	for _, route := range pushProtectedRoutes {
		t.Run(route.method+" "+route.pattern+"/unknown_fcpush", func(t *testing.T) {
			pg.SetToken(nil)
			rig.limiter.reset()
			selects, auditsBefore := pg.TokenSelects(), len(pg.AuditRows())
			got := rig.serve(route, bearer(pushToken))
			if got.status != http.StatusUnauthorized || got.body != pushInvalidTokenBody {
				t.Fatalf("%d %s, want the canonical 401", got.status, got.body)
			}
			if d := pg.TokenSelects() - selects; d != 1 {
				t.Errorf("%d external_ingest_tokens SELECT(s), want 1", d)
			}
			if rows := pg.AuditRows(); len(rows) != auditsBefore {
				t.Errorf("%d new audit row(s) for an unknown token, want 0", len(rows)-auditsBefore)
			}
			increments, peeks := rig.limiter.counts()
			if increments[pushAttemptLimitID] != 1 || increments[pushFailureLimitID] != 1 || peeks[pushFailureLimitID] != 1 {
				t.Errorf("limiter increments %v peeks %v, want 1 attempt, 1 failure, 1 failure test", increments, peeks)
			}
			usertokens.NoLeak(t, "body", got.body, []usertokens.Token{{Kind: "fcpush", Value: pushToken}})
		})
		t.Run(route.method+" "+route.pattern+"/missing_scope", func(t *testing.T) {
			pg.SetToken(&ingestpg.Token{ID: "tok-1", OrgID: probeOrg.String(), Scopes: "[]"})
			defer pg.SetToken(nil)
			audits := len(pg.AuditRows())
			rig.limiter.reset()
			selects := pg.TokenSelects()
			got := rig.serve(route, bearer(pushToken))
			if got.status != http.StatusForbidden || !strings.Contains(got.body, `"insufficient_scope"`) {
				t.Fatalf("%d %s, want 403 insufficient_scope", got.status, got.body)
			}
			rows := pg.AuditRows()
			if len(rows) != audits+1 {
				t.Fatalf("%d audit row(s), want one more than %d", len(rows), audits)
			}
			row := rows[len(rows)-1]
			var metadata map[string]any
			_ = json.Unmarshal([]byte(row.RequestMetadata), &metadata)
			wantMetadata := map[string]any{"method": route.method, "route": route.pattern, "required_scope": pushRouteScopes[route.method+" "+route.pattern]}
			if row.OrgID != probeOrg.String() || row.UserID != nil || row.Action != "ingest_auth_failed" || row.ResourceType != "external_ingest_token" ||
				row.ResourceID != "tok-1" || row.Status != "failure" || row.ErrorMessage == nil || *row.ErrorMessage != "insufficient_scope" ||
				!reflect.DeepEqual(metadata, wantMetadata) {
				t.Errorf("audit row %+v (metadata %v), want the insufficient_scope row with %v", row, metadata, wantMetadata)
			}
			if d := pg.TokenSelects() - selects; d != 1 {
				t.Errorf("%d external_ingest_tokens SELECT(s), want 1", d)
			}
			usertokens.NoLeak(t, "body", got.body, []usertokens.Token{{Kind: "fcpush", Value: pushToken}})
		})
	}
	usertokens.NoLeak(t, "the logs", rig.logs.String(), []usertokens.Token{{Kind: "fcpush", Value: pushToken}})
}

// The two schema routes are public: any wrong credential answers what an
// anonymous caller gets, reads nothing, and counts no auth-limiter hit.
func TestExternalIngestWrongClassMatrixPublicSchemaRows(t *testing.T) {
	pg, pool := ingestpg.Start(t)
	rig := newPushRig(t, pool)
	cells := pushWrongClassCells(t, rig)
	tokens := pushTokens(cells)
	for _, route := range pushPublicRoutes {
		rig.limiter.reset()
		anon := rig.serve(route, nil)
		if anon.status != http.StatusOK || anon.body == "" {
			t.Fatalf("%s anonymous: %d %q, want 200 with the schema document", route.pattern, anon.status, anon.body)
		}
		for _, cell := range cells {
			t.Run(route.pattern+"/"+cell.name, func(t *testing.T) {
				rig.limiter.reset()
				got := rig.serve(route, cell.headers)
				if got.status != anon.status || got.body != anon.body || got.header.Get("ETag") != anon.header.Get("ETag") {
					t.Errorf("answer differs from the anonymous one: %d (bytes equal %v)", got.status, got.body == anon.body)
				}
				if n := rig.counts.total() + rig.counts.validate; n != 0 {
					t.Errorf("%d lookup(s), want 0", n)
				}
				increments, _ := rig.limiter.counts()
				if increments[pushAttemptLimitID] != 0 || increments[pushFailureLimitID] != 0 {
					t.Errorf("auth limiter hits %v on a public row, want none", increments)
				}
				usertokens.NoLeak(t, "body", got.body, tokens)
			})
		}
	}
	if pg.Connects() != 0 || len(pg.AuditRows()) != 0 {
		t.Errorf("schema discovery touched the database: %d connect(s), %d audit row(s)", pg.Connects(), len(pg.AuditRows()))
	}
	usertokens.NoLeak(t, "the logs", rig.logs.String(), tokens)
}

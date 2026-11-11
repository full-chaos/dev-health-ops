package authruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authstore"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadexchange"
	"github.com/full-chaos/dev-health-ops/internal/cigates/exceptionledger"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/wrongclass"
	tokenpkg "github.com/full-chaos/dev-health-ops/internal/token"
)

// dialCounter is the auth store's Postgres: a TCP listener that counts every connection a request makes and closes
// it, so each store read is one counted dial that then fails. A pgx cancel request is not a dial of a request: pgx
// sends it from a goroutine of its own after a failed connect, at a time the request does not wait for, so it can land
// after any settle window. It is told by its first packet (cancelRequestCode) and not counted: not as a dial and
// not as a late one. Every other connection is counted before it is closed, so the request cannot answer before its
// dial is counted. A dial while no cell runs is late: it is kept, with the cell it followed, and fails the gate; so
// does a cell whose dials never settle.
type dialCounter struct {
	listener net.Listener
	mu       sync.Mutex
	cell     string
	last     string
	dials    int
	late     []string
	unsettle []string
}

// cancelRequestCode is the protocol code of a Postgres CancelRequest startup packet.
const cancelRequestCode = 80877102

func isCancelRequest(conn net.Conn) bool {
	var packet [8]byte
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(conn, packet[:]); err != nil {
		return false
	}
	return binary.BigEndian.Uint32(packet[4:]) == cancelRequestCode
}

func newDialCounter(t *testing.T) *dialCounter {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	counter := &dialCounter{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			cancel := isCancelRequest(conn)
			counter.mu.Lock()
			switch {
			case cancel:
			case counter.cell == "":
				counter.late = append(counter.late, "after "+counter.last)
			default:
				counter.dials++
			}
			counter.mu.Unlock()
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return counter
}

func (d *dialCounter) begin(cell string) {
	d.mu.Lock()
	d.cell, d.dials = cell, 0
	d.mu.Unlock()
}

func (d *dialCounter) end() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.cell, d.last = "", d.cell
	return d.dials
}

func (d *dialCounter) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

// settle waits until no dial has arrived for 25ms, so a store read the answer did not wait for still lands inside
// its cell. A cell still dialling after 1s is kept and fails the gate: there is no silent timing fallback.
func (d *dialCounter) settle() {
	last, quiet := d.count(), time.Now()
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
		if n := d.count(); n != last {
			last, quiet = n, time.Now()
			continue
		}
		//abstime:allow D6934: the settle is a wait, never a pass/fail on time; backstop re-reads the dial counter, so a late dial fails by cell name
		if time.Since(quiet) >= 25*time.Millisecond {
			return
		}
	}
	d.mu.Lock()
	d.unsettle = append(d.unsettle, d.cell)
	d.mu.Unlock()
}

// backstop waits once more and returns every dial that landed outside its cell's window and every cell that never
// settled, each named.
func (d *dialCounter) backstop() []string {
	time.Sleep(250 * time.Millisecond)
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, cell := range d.late {
		out = append(out, "a store dial landed "+cell+", after that cell's window")
	}
	for _, cell := range d.unsettle {
		out = append(out, "cell "+cell+" still dialled the store 1s after its answer")
	}
	return out
}

// countingSigner is the file signer with every key-set read counted: each token verification resolves its kid there.
type countingSigner struct {
	*tokenpkg.Signer
	reads *atomic.Int64
}

func (s countingSigner) KeySet(ctx context.Context) (tokenpkg.KeySet, error) {
	s.reads.Add(1)
	return s.Signer.KeySet(ctx)
}

// freshCounters is the keyed limits' counter store, emptied before each cell so no cell meets another's window.
type freshCounters struct {
	mu    sync.Mutex
	inner *httpapi.MemoryCounters
}

func (f *freshCounters) store() *httpapi.MemoryCounters {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inner
}

func (f *freshCounters) reset() {
	f.mu.Lock()
	f.inner = httpapi.NewMemoryCounters(nil)
	f.mu.Unlock()
}

func (f *freshCounters) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	return f.store().Increment(ctx, hit)
}

func (f *freshCounters) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	return f.store().Peek(ctx, hit)
}

func (f *freshCounters) Refund(ctx context.Context, hit httpapi.Hit) error {
	return f.store().Refund(ctx, hit)
}

func (f *freshCounters) Backend() string { return "memory" }

// authGateRig is auth-service's three listeners. The API and internal listeners are the builders configureWithRoutes
// calls, in its order, over the same Config; the one change is the keyed limits' counter store (in memory, not Valkey:
// with no Valkey every limited route answers 500 to every caller). TestWrongClassGateAuthServiceRigMatchesConfigure
// pins that mirror. The operator listener is configure's own. The store is dialCounter; the key set is countingSigner.
type authGateRig struct {
	handlers map[string]http.Handler
	mounted  map[string]bool
	dials    *dialCounter
	keys     *atomic.Int64
	counters *freshCounters
	signer   *tokenpkg.Signer
	subject  string
}

// gateEnvironment is the route probe's full-feature configuration (every row mounted) with the internal listener on,
// a signing key the rig holds, the go-api workload subject as the one authorize caller, and limits no cell reaches.
func gateEnvironment(t *testing.T, databaseURI, keyDir string) map[string]string {
	t.Helper()
	writeSigningKey(t, keyDir, "k1.pem")
	env := map[string]string{
		authconfig.EnvDatabaseURI:             databaseURI,
		authconfig.EnvDatabaseConnectTimeout:  "1s",
		authconfig.EnvSigningKeyFile:          filepath.Join(keyDir, "k1.pem"),
		authconfig.EnvSigningKeyID:            "k1",
		authconfig.EnvRateLimitPerSecond:      "100000",
		authconfig.EnvRateLimitBurst:          "100000",
		authconfig.EnvPublicRateLimit:         "100000",
		authconfig.EnvPublicRateLimitBurst:    "100000",
		authconfig.EnvPDPCallerRateLimit:      "100000",
		authconfig.EnvPDPCallerRateLimitBurst: "100000",
		authconfig.EnvPDPRouteRateLimit:       "100000",
		authconfig.EnvPDPRouteRateLimitBurst:  "100000",
		authconfig.EnvAPIAddress:              "127.0.0.1:0",
		authconfig.EnvOperatorAddress:         "127.0.0.1:1",
		authconfig.EnvSocialLinkRoutes:        "true",
		authconfig.EnvUsersOwned:              "true",
		authconfig.EnvTokenIssuerURL:          wellKnownIssuer,
		authconfig.EnvAuthorizeCallerSubjects: testIssueSubject,
		internalAddressEnv:                    "127.0.0.1:0",
	}
	exchangeEnvironment(t, acrEventsEnvironment(workloadIssueEnvironment(env)))
	return provisioningEnvironment(ssoEnvironment(compatEnvironment(env)))
}

func newAuthGateRig(t *testing.T) *authGateRig {
	t.Helper()
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dials := newDialCounter(t)
	useKeyStates(t, staticKeyStates(nil))
	cfg, err := authconfig.Load(authconfig.Spec{LookupEnv: lookupFrom(gateEnvironment(t,
		"postgres://auth@"+dials.listener.Addr().String()+"/devhealth?sslmode=disable", t.TempDir()))})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	store, err := authstore.Open(ctx, authstore.Config{
		URI: cfg.DatabaseURI.Reveal(), Schema: cfg.DatabaseSchema, MaxConns: cfg.DatabaseMaxConns, ConnectTimeout: cfg.DatabaseConnectTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Shutdown(context.Background()) })
	schemaID, err := authschema.NewValidatedIdentifier(cfg.DatabaseSchema)
	if err != nil {
		t.Fatal(err)
	}
	keyStates := tokenpkg.NewKeyStates(tokenpkg.KeyStatesConfig{Source: keyStateSource(store.Pool(), schemaID)})
	t.Cleanup(keyStates.Close)
	fileSigner := tokenpkg.NewFileSigner(tokenpkg.FileConfig{
		KeySetPath: cfg.SigningKeySetPath, KeyPath: cfg.SigningKeyPath, KeyID: cfg.SigningKeyID, States: keyStates,
	})
	keys := &atomic.Int64{}
	signer := countingSigner{Signer: fileSigner, reads: keys}
	jwks := authapi.NewJWKSDocument(authapi.JWKSDocumentConfig{Keys: fileSigner, Logger: logger})
	counters := &freshCounters{}
	counters.reset()
	limits, err := authapi.NewSessionLimits(counters, rateLimitSecret(cfg), cfg.Session.SuccessorKey.Reveal())
	if err != nil {
		t.Fatal(err)
	}

	routes, err := sessionRoutes(cfg, logger, store.Pool(), schemaID, signer, limits, jwks)
	if err != nil {
		t.Fatal(err)
	}
	credRoutes, credComponents, err := credentialRoutes(cfg, logger, store.Pool(), schemaID, limits, signer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, component := range credComponents {
			_ = component.Shutdown(context.Background())
		}
	})
	routes = append(routes, credRoutes...)
	workloadRoutes, err := exchangeRoutes(cfg, logger, store.Pool(), schemaID, fileSigner)
	if err != nil {
		t.Fatal(err)
	}
	routes = append(routes, workloadRoutes...)
	feed, err := revisionFeed(cfg, logger, store.Pool(), schemaID)
	if err != nil {
		t.Fatal(err)
	}
	decisionRoutes, err := authorizeRoutes(cfg, logger, store.Pool(), schemaID, signer, feed.decisions)
	if err != nil {
		t.Fatal(err)
	}
	routes = append(routes, decisionRoutes...)
	acrRoutes, err := acrEventRoutes(cfg, logger, store.Pool(), schemaID, signer)
	if err != nil {
		t.Fatal(err)
	}
	routes = append(routes, acrRoutes...)
	routes = append(routes, authapi.WellKnownRoutes(authapi.WellKnownDeps{JWKS: jwks, IssuerURL: cfg.TokenIssuerURL, Logger: logger})...)
	if err := checkInternalOnlyRoutes(routes); err != nil {
		t.Fatal(err)
	}

	rig := &authGateRig{handlers: map[string]http.Handler{}, mounted: map[string]bool{}, dials: dials, keys: keys,
		counters: counters, signer: fileSigner, subject: testIssueSubject}
	mount := func(listener string, options httpapi.ServerOptions) {
		server, err := httpapi.NewServer(options)
		if err != nil {
			t.Fatalf("%s listener: %v", listener, err)
		}
		rig.handlers[listener] = server.Handler()
		for _, route := range options.Routes {
			rig.mounted[route.Method+" "+route.Pattern+"@"+listener] = true
		}
	}
	mount("api", apiServerOptions(cfg, logger, publicRoutes(routes)))
	mount("internal", internalServerOptions(cfg, logger, routes))

	components, err := configure(ctx, cfg, logger)
	if err != nil {
		t.Fatalf("configure: %v", err)
	}
	t.Cleanup(func() {
		for _, component := range components {
			_ = component.Shutdown(context.Background())
		}
	})
	for _, component := range components {
		if announced, ok := component.(announcedListener); ok {
			if server, ok := announced.listener.(*health.Server); ok {
				rig.handlers["operator"] = server.Handler()
			}
		}
		// The readiness gate is opened as a started process opens it, so /readyz runs the required checks.
		if gate, ok := component.(health.Gate); ok {
			if err := gate.Start(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if rig.handlers["operator"] == nil {
		t.Fatal("configure built no operator listener")
	}
	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			rig.mounted[method+" "+path+"@operator"] = true
		}
	}
	return rig
}

type authGateCell struct {
	answer wrongclass.Answer
	dials  int
	keys   int64
}

// serve sends one request with a fresh limiter window and counts the store dials and key-set reads it causes.
func (rig *authGateRig) serve(listener string, request *http.Request) authGateCell {
	rig.counters.reset()
	rig.keys.Store(0)
	rig.dials.begin(listener + " " + request.Method + " " + request.URL.Path)
	recorder := httptest.NewRecorder()
	rig.handlers[listener].ServeHTTP(recorder, request)
	rig.dials.settle()
	dials := rig.dials.end()
	return authGateCell{answer: wrongclass.Capture(maskAuthEnvelope(recorder)), dials: dials, keys: rig.keys.Load()}
}

var occurredAt = regexp.MustCompile(`"occurred_at":"[^"]*"`)

// maskAuthEnvelope masks what differs on every answer of the auth listeners: the server-minted X-Auth-Request-ID in
// the body, the error envelope's occurred_at stamp, and the sample values of the operator's metrics exposition.
func maskAuthEnvelope(recorder *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	body := recorder.Body.String()
	if id := recorder.Header().Get(requestid.Header); id != "" {
		body = strings.ReplaceAll(body, id, "<request-id>")
	}
	body = occurredAt.ReplaceAllString(body, `"occurred_at":"<time>"`)
	if strings.HasPrefix(recorder.Header().Get("Content-Type"), "text/plain; version=0.0.4") {
		lines := strings.Split(body, "\n")
		for i, line := range lines {
			if cut := strings.LastIndexByte(line, ' '); cut > 0 && !strings.HasPrefix(line, "#") {
				lines[i] = line[:cut] + " <value>"
			}
		}
		body = strings.Join(lines, "\n")
	}
	recorder.Body.Reset()
	recorder.Body.WriteString(body)
	return recorder
}

// sign signs claims under typ with the rig's own key: the key set the routes trust.
func (rig *authGateRig) sign(t *testing.T, typ string, claims any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	compact, _, err := rig.signer.Sign(context.Background(), typ, payload)
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

// workloadToken is the go-api workload token every workload row and the authorize routes accept.
func (rig *authGateRig) workloadToken(t *testing.T, typ string) string {
	now := time.Now()
	return rig.sign(t, typ, contracts.WorkloadTokenClaims{Iss: wellKnownIssuer, Aud: "auth-service", Sub: rig.subject,
		PrincipalType: "workload", Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 240})
}

// userToken is a live-shaped user access token for audience.
func (rig *authGateRig) userToken(t *testing.T, audience string) string {
	t.Helper()
	issuer, err := authapi.NewEdDSAIssuer(audience, rig.signer, wellKnownIssuer)
	if err != nil {
		t.Fatal(err)
	}
	bearer, err := issuer.Access(authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
	if err != nil {
		t.Fatal(err)
	}
	return bearer
}

// mint is the gate's minter: the rig's EdDSA key under any typ (a user access token for at+jwt, the go-api workload
// claims otherwise), or the retired HS256 class.
func (rig *authGateRig) mint(t *testing.T) wrongclass.Mint {
	return func(alg, typ string) string {
		switch {
		case alg == "EdDSA" && typ == "at+jwt":
			return rig.userToken(t, authapi.AccessTokenAudience)
		case alg == "EdDSA":
			return rig.workloadToken(t, typ)
		case alg == "HS256":
			return legacyAccess(t, testAccessSecret, wellKnownIssuer, authapi.AccessTokenAudience, authapi.Grant{PrincipalID: uuid.New(), SessionID: uuid.New()})
		}
		return ""
	}
}

// gateListeners are the listeners a row is driven on: the ones its id names, and the internal listener for a route
// that listener also serves (AUTH_SERVICE_INTERNAL_HTTP_ADDR set).
func gateListeners(row wrongclass.Row) []string {
	listeners := slices.Clone(row.Listeners)
	if slices.Contains(pepRoutes, row.Method+" "+row.Route) {
		listeners = append(listeners, "internal")
	}
	return listeners
}

type listenerCensus struct{ rows, wrong, accept int }

// inValidateBody is each Authorization carrier moved into the validate route's body field "token", where that route
// reads the credential it validates (the header is not read there).
func inValidateBody(carriers []wrongclass.Carrier) []wrongclass.Carrier {
	var out []wrongclass.Carrier
	for _, carrier := range carriers {
		if value, ok := strings.CutPrefix(carrier.Value, "Bearer "); ok && carrier.Header == wrongclass.Authorization {
			out = append(out, wrongclass.Carrier{Class: carrier.Class, BodyField: "token", Value: value})
		}
	}
	return out
}

// gateRequest is carrier on row. On the refresh route a body carrier rides in refresh-request.v1 (schema_version set),
// and on the exchange route the subject_token carrier rides in the RFC 8693 form of a bootstrap subject, so the body
// reaches the credential check and is not refused for its envelope.
func gateRequest(row wrongclass.Row, carrier wrongclass.Carrier) *http.Request {
	if carrier.BodyField == "subject_token" && row.Route == workloadexchange.Pattern {
		form := url.Values{"grant_type": {workloadexchange.GrantTypeTokenExchange}, "subject_token": {carrier.Value},
			"subject_token_type": {workloadexchange.TokenTypeWorkloadBootstrap}, "audience": {"auth-service"}}
		request := httptest.NewRequest(row.Method, row.Path(), strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return request
	}
	if carrier.BodyField == "" || row.Route != authapi.PatternRefresh {
		return carrier.Request(row.Method, row.Path())
	}
	raw, _ := json.Marshal(map[string]string{"schema_version": "refresh-request.v1", carrier.BodyField: carrier.Value})
	request := httptest.NewRequest(row.Method, row.Path(), strings.NewReader(string(raw)))
	request.Header.Set("Content-Type", "application/json")
	return request
}

// runAuthServiceGate drives every auth-service endpoint-profile row, on each listener that serves it, with every
// credential class of credential-classes.json the row does not accept. A wrong class answers exactly what a garbage
// credential in the same carrier answers (no credential, for a carrier other than the Authorization header). It
// causes no store dial and no key-set read that the garbage credential does not cause, and none at all on a protected
// row; a protected row refuses it with a 4xx. An accepted class (the controls) must reach the store and answer other
// than a garbage credential: the rig tells an accepted class from a refused one.
func runAuthServiceGate(t *testing.T, rig *authGateRig, errorf func(format string, args ...any)) (wrongclass.Census, map[string]*listenerCensus, []gateFinding, map[string]bool) {
	rows, err := wrongclass.LoadRows(authv1.AuthServiceEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	classes, err := wrongclass.LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	census := wrongclass.Census{Service: "auth-service", Rows: len(rows), Classes: len(classes)}
	perListener := map[string]*listenerCensus{}
	var carriers []wrongclass.Carrier
	for _, class := range classes {
		carrier, ok, err := wrongclass.CarrierFor(class, wrongclass.Options{Mint: rig.mint(t)})
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			census.NotDriven = append(census.NotDriven, class.ID)
			continue
		}
		carriers = append(carriers, carrier)
	}
	census.Driven = len(carriers)
	if census.Driven == 0 {
		t.Fatal("no class has a carrier")
	}

	controls := []wrongclass.Carrier{
		{Class: "user_access_token", Header: wrongclass.Authorization, Value: "Bearer " + rig.userToken(t, authapi.AccessTokenAudience)},
		{Class: "user_access_token", Header: wrongclass.Authorization, Value: "Bearer " + rig.userToken(t, contracts.PlatformAPIAudience)},
		{Class: "workload_token", Header: wrongclass.Authorization, Value: "Bearer " + rig.workloadToken(t, contracts.WorkloadTokenTyp)},
		{Class: workload.BootstrapClass, BodyField: "subject_token", Value: workload.BootstrapPrefix + wrongclass.Filler},
	}
	for _, class := range classes {
		if class.ID != "refresh_credential" {
			continue
		}
		for _, marker := range class.Markers {
			if marker.Kind == "body_field" {
				value, ok := strings.CutPrefix(marker.Shape, "prefix:")
				if !ok {
					value, _ = strings.CutPrefix(marker.Shape, "opaque:")
				}
				controls = append(controls, wrongclass.Carrier{Class: class.ID, BodyField: marker.Name, Value: value + wrongclass.Filler})
			}
		}
	}
	accepts, storeAccepts := map[string]int{}, 0
	var findings []gateFinding
	unmeasured := map[string]bool{}
	for _, row := range rows {
		for _, listener := range gateListeners(row) {
			if rig.handlers[listener] == nil {
				errorf("%s: listener %s is not an auth-service listener", row.ID, listener)
				continue
			}
			if !rig.mounted[row.Method+" "+row.Route+"@"+listener] {
				errorf("%s: no route %s %s is mounted on the %s listener: the row is not measured", row.ID, row.Method, row.Route, listener)
				continue
			}
			counts := perListener[listener]
			if counts == nil {
				counts = &listenerCensus{}
				perListener[listener] = counts
			}
			counts.rows++
			cellID := row.ID
			if listener != row.Listeners[0] {
				cellID += " on " + listener
			}
			rowCarriers := slices.Clone(carriers)
			if row.Route == authapi.PatternCompatValidate {
				rowCarriers = append(rowCarriers, inValidateBody(carriers)...)
			}
			rowControls := slices.Clone(controls)
			if row.Route == authapi.PatternCompatValidate {
				rowControls = append(rowControls, inValidateBody(controls)...)
			}
			baselines := map[string]authGateCell{}
			baselineOf := func(carrier wrongclass.Carrier) authGateCell {
				base := carrier.Baseline()
				if cell, ok := baselines[base.Class]; ok {
					return cell
				}
				cell := rig.serve(listener, gateRequest(row, base))
				baselines[base.Class] = cell
				return cell
			}
			for _, carrier := range rowCarriers {
				if row.Accepts(carrier.Class) {
					continue
				}
				census.WrongCells++
				counts.wrong++
				got := rig.serve(listener, gateRequest(row, carrier))
				want := baselineOf(carrier)
				if got.answer.Status == http.StatusTooManyRequests || want.answer.Status == http.StatusTooManyRequests {
					errorf("%s: wrong class %s met the limiter (%s, garbage %s): the cell measured nothing", cellID, carrier.Class, got.answer, want.answer)
					continue
				}
				if got.answer != want.answer {
					errorf("%s: wrong class %s answered %s, a garbage credential in its carrier answers %s", cellID, carrier.Class, got.answer, want.answer)
				}
				if got.dials != want.dials || got.keys != want.keys {
					errorf("%s: wrong class %s made %d store dials and %d key reads, a garbage credential %d and %d", cellID, carrier.Class, got.dials, got.keys, want.dials, want.keys)
				}
				if row.Protected() && (got.dials != 0 || got.keys != 0) {
					findings = append(findings, gateFinding{profile: row.Method + " " + row.Route, rule: ruleStrictG2,
						text: fmt.Sprintf("%s: wrong class %s on a protected row made %d store dials and %d key reads, want none", cellID, carrier.Class, got.dials, got.keys)})
				}
				if row.Protected() && got.answer.Status/100 != 4 {
					findings = append(findings, gateFinding{profile: row.Method + " " + row.Route, rule: ruleG3,
						text: fmt.Sprintf("%s: wrong class %s on a protected row answered %s, want a 4xx refusal", cellID, carrier.Class, got.answer)})
				}
			}
			controlled, seen := map[string]bool{}, map[string]string{}
			for _, control := range rowControls {
				if !row.Accepts(control.Class) {
					continue
				}
				got := rig.serve(listener, gateRequest(row, control))
				want := baselineOf(control)
				if got.dials > 0 {
					storeAccepts++
				}
				// An accepted user or refresh credential reaches the store and answers other than garbage. A workload row
				// reads its body before the store, so a body-less accepted workload token is refused there for its body
				// (reason malformed), not for its credential (garbage: 401 invalid_credentials): its answer alone tells.
				// The exchange answers one invalid_grant to every refusal and to a store it cannot read, so a bootstrap
				// credential is told by its registry read alone (garbage is refused for its shape before any read).
				accepted := got.dials > 0 && got.answer != want.answer
				switch control.Class {
				case "workload_token":
					accepted = got.answer != want.answer
				case workload.BootstrapClass:
					accepted = got.dials > 0 && want.dials == 0
				}
				if accepted {
					controlled[control.Class] = true
				}
				seen[control.Class] += fmt.Sprintf(" [%s, %d dials; garbage %s]", got.answer, got.dials, want.answer)
			}
			controlCells := 0
			for _, class := range row.Accepted {
				if !slices.ContainsFunc(controls, func(c wrongclass.Carrier) bool { return c.Class == class }) {
					continue
				}
				controlCells++
				census.AcceptCells++
				counts.accept++
				accepts[class]++
				if !controlled[class] {
					errorf("%s: accepted class %s did not show the accept its control rule wants (user, refresh: a store read and another answer than garbage; workload: another answer; bootstrap: a registry read garbage does not make):%s: the rig cannot tell accept from refuse", cellID, class, seen[class])
				}
			}
			if len(row.Accepted) > 0 && controlCells == 0 {
				unmeasured[row.Method+" "+row.Route] = true
			}
		}
	}
	// A row whose accepted classes have no control is not measured: the rig never showed it can accept there. The
	// list is pinned, so a row joins it, or leaves it, only with this file's change.
	for profile := range unmeasured {
		if !slices.Contains(notMeasuredRows, profile) {
			errorf("%s: no accepted class of the row has an accept control, and the row is not in notMeasuredRows: the row is not measured", profile)
		}
	}
	for _, profile := range notMeasuredRows {
		if !unmeasured[profile] {
			errorf("%s is in notMeasuredRows, but an accepted class of the row has an accept control (or no such row): remove it from the list", profile)
		}
	}
	for _, class := range []string{"user_access_token", "workload_token", "refresh_credential", workload.BootstrapClass} {
		if accepts[class] == 0 {
			errorf("accept controls: no %s cell: the class is not measured", class)
		}
	}
	if storeAccepts == 0 {
		errorf("no accept cell dialled the store: the store seam is not live, so zero on a wrong cell proves nothing")
	}
	for _, late := range rig.dials.backstop() {
		errorf("%s: a late read is not attributed to its cell", late)
	}
	if census.WrongCells == 0 {
		errorf("no wrong-class cell ran")
	}
	return census, perListener, findings, unmeasured
}

// notMeasuredRows are the rows whose accepted classes have no accept control, so the rig never shows it can accept
// there; their wrong-class cells still run. Each class needs what a test rig cannot make: social_oauth_exchange a
// provider's own access token, sso_oidc_exchange an authorization code from the provider with the signed state of a
// configured provider, sso_saml_exchange a SAML response signed by a configured identity provider.
var notMeasuredRows = []string{
	"POST /api/v1/auth/social-login",
	"POST /api/v1/auth/oidc/{provider_id}/callback",
	"POST /api/v1/auth/oauth/{provider_id}/callback",
	"POST /api/v1/auth/saml/{provider_id}/acs",
}

// The two rules a ledger row can excuse on the rows it names; G1 and the equal-to-garbage G2 are never excused.
const (
	ruleStrictG2 = "strict-G2"
	ruleG3       = "G3"
)

// gateFinding is a strict-G2 or G3 failure of one cell, on the profile (METHOD route) of its row.
type gateFinding struct{ profile, rule, text string }

// gateLedgerFile is the gate's own path in the exception ledger. A ledger row anchored here names profile rows
// (METHOD route, "|" between) after one of the prefixes below, which says which rules it excuses there. A named row
// none of whose cells fails an excused rule fails the gate: the divergence is gone and its row must go.
const gateLedgerFile = "internal/auth/authruntime/wrong_class_gate_test.go"

var gateDivergencePrefixes = map[string][]string{
	"wrong-class G3 known divergence: ":           {ruleG3},
	"wrong-class G2-strict+G3 known divergence: ": {ruleStrictG2, ruleG3},
}

// The ledger rows anchored here (each needle is the line below, verbatim):
//
//	wrong-class G3 known divergence: POST /api/v1/auth/logout
//	wrong-class G3 known divergence: POST /api/v1/auth/validate
//	wrong-class G2-strict+G3 known divergence: POST /api/v1/auth/social-login

type gateExcusal struct {
	owner string
	rules []string
}

func gateDivergences(t *testing.T) map[string]gateExcusal {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(exceptionledger.LedgerPath)))
	if err != nil {
		t.Fatal(err)
	}
	rows, defects := exceptionledger.ParseLedger(raw)
	if len(defects) > 0 {
		t.Fatalf("exception ledger: %v", defects)
	}
	out := map[string]gateExcusal{}
	for _, row := range rows {
		if row.Path != gateLedgerFile {
			continue
		}
		var rules []string
		var profiles string
		for prefix, excused := range gateDivergencePrefixes {
			if rest, ok := strings.CutPrefix(row.Needle, prefix); ok {
				rules, profiles = excused, rest
			}
		}
		if rules == nil || row.OwnerState != "open" {
			t.Fatalf("ledger %s: a row anchored on the gate must be an open row whose needle starts with a gate prefix", row.ID)
		}
		for _, profile := range strings.Split(profiles, "|") {
			if _, dup := out[profile]; dup {
				t.Fatalf("ledger %s: %s is named by two rows", row.ID, profile)
			}
			out[profile] = gateExcusal{owner: row.ID + " (" + row.Owner + ")", rules: rules}
		}
	}
	return out
}

// judgeFindings fails every finding the ledger does not excuse, and every ledger profile with no excused finding.
func judgeFindings(t *testing.T, findings []gateFinding, divergences map[string]gateExcusal, errorf func(format string, args ...any)) {
	excused := map[string]int{}
	for _, finding := range findings {
		if excusal, ok := divergences[finding.profile]; ok && slices.Contains(excusal.rules, finding.rule) {
			excused[finding.profile]++
			continue
		}
		errorf("%s", finding.text)
	}
	profiles := make([]string, 0, len(divergences))
	for profile := range divergences {
		profiles = append(profiles, profile)
	}
	sort.Strings(profiles)
	for _, profile := range profiles {
		if excused[profile] == 0 {
			errorf("ledger %s names %s, none of whose wrong-class cells fails an excused rule (or no such row): remove the ledger row", divergences[profile].owner, profile)
		} else {
			t.Logf("%s: %d %v cells excused, ledger %s", profile, excused[profile], divergences[profile].rules, divergences[profile].owner)
		}
	}
}

// authGateRun is the one gate run of the package: the cells that judge the ledger read its findings.
var authGateRun struct {
	once        sync.Once
	done        bool
	census      wrongclass.Census
	perListener map[string]*listenerCensus
	hard        []string
	findings    []gateFinding
	unmeasured  map[string]bool
}

func runAuthGateOnce(t *testing.T) {
	t.Helper()
	authGateRun.once.Do(func() {
		census, perListener, findings, unmeasured := runAuthServiceGate(t, newAuthGateRig(t), func(format string, args ...any) {
			authGateRun.hard = append(authGateRun.hard, fmt.Sprintf(format, args...))
		})
		authGateRun.census, authGateRun.perListener, authGateRun.findings, authGateRun.unmeasured, authGateRun.done = census, perListener, findings, unmeasured, true
	})
	if !authGateRun.done {
		t.Fatal("the gate run did not finish")
	}
}

func TestWrongClassGateAuthService(t *testing.T) {
	runAuthGateOnce(t)
	for _, failure := range authGateRun.hard {
		t.Error(failure)
	}
	judgeFindings(t, authGateRun.findings, gateDivergences(t), t.Errorf)
	census, perListener := authGateRun.census, authGateRun.perListener
	t.Log(census.Line())
	unmeasured := make([]string, 0, len(authGateRun.unmeasured))
	for profile := range authGateRun.unmeasured {
		unmeasured = append(unmeasured, profile)
	}
	sort.Strings(unmeasured)
	t.Logf("WRONG_CLASS_GATE_ACCEPT_COVERAGE service=auth-service rows=%d not_measured: %d (%s)", census.Rows, len(unmeasured), strings.Join(unmeasured, ", "))
	listeners := make([]string, 0, len(perListener))
	for listener := range perListener {
		listeners = append(listeners, listener)
	}
	sort.Strings(listeners)
	for _, listener := range listeners {
		counts := perListener[listener]
		t.Logf("WRONG_CLASS_GATE_LISTENER service=auth-service listener=%s rows=%d wrong_cells=%d accept_cells=%d", listener, counts.rows, counts.wrong, counts.accept)
	}
	for _, listener := range []string{"api", "operator", "internal"} {
		if perListener[listener] == nil || perListener[listener].wrong == 0 {
			t.Errorf("no wrong-class cell ran on the %s listener", listener)
		}
	}
}

// A ledger row that names a row with no excused failure fails the gate, so the fix of the divergence forces its row
// out.
func TestWrongClassGateAuthServiceFailsOnAStaleDivergence(t *testing.T) {
	runAuthGateOnce(t)
	const stale = "GET /v1/sessions/current"
	divergences := gateDivergences(t)
	if _, ok := divergences[stale]; ok {
		t.Fatalf("the ledger names %s", stale)
	}
	divergences[stale] = gateExcusal{owner: "stale-probe (D0)", rules: []string{ruleG3}}
	var failures []string
	judgeFindings(t, authGateRun.findings, divergences, func(format string, args ...any) { failures = append(failures, fmt.Sprintf(format, args...)) })
	want := "ledger stale-probe (D0) names " + stale + ", none of whose wrong-class cells fails an excused rule (or no such row): remove the ledger row"
	if len(failures) != 1 || failures[0] != want {
		t.Fatalf("failures %q, want exactly %q", failures, want)
	}
}

// With no ledger row, the excusable findings are exactly the named rows' cells of the rules their rows excuse, and
// every rule a row excuses has one there: the ledger excuses its named rows and rules and nothing wider.
func TestWrongClassGateAuthServiceExcusesOnlyTheNamedRows(t *testing.T) {
	runAuthGateOnce(t)
	named := gateDivergences(t)
	failed := map[string]bool{}
	for _, finding := range authGateRun.findings {
		excusal, ok := named[finding.profile]
		if !ok || !slices.Contains(excusal.rules, finding.rule) {
			t.Errorf("a failure outside the named rows and rules: %s", finding.text)
		}
		failed[finding.profile+" "+finding.rule] = true
	}
	for profile, excusal := range named {
		for _, rule := range excusal.rules {
			if !failed[profile+" "+rule] {
				t.Errorf("%s: ledger %s excuses %s, which no cell of the row fails without it", profile, excusal.owner, rule)
			}
		}
	}
}

// configureRouteCalls is the ordered list of the route-set and listener calls configureWithRoutes makes: the rig
// mirrors exactly these, in this order. The first NewServer is the operator listener (the rig takes configure's own);
// limitStore is the one call the rig replaces (in-memory counters).
var configureRouteCalls = []string{
	"NewServer", "limitStore", "sessionRoutes", "credentialRoutes", "exchangeRoutes", "revisionFeed", "authorizeRoutes",
	"acrEventRoutes", "WellKnownRoutes", "logAPIRoutes", "checkInternalOnlyRoutes", "NewServer", "apiServerOptions",
	"publicRoutes", "NewServer", "internalServerOptions",
}

// routeCalls lists, in source order, the calls in fn whose callee names a route set, a listener or its options.
func routeCalls(fn *ast.FuncDecl) []string {
	watched := map[string]bool{"limitStore": true, "revisionFeed": true, "NewServer": true}
	var out []string
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			name = callee.Name
		case *ast.SelectorExpr:
			name = callee.Sel.Name
		}
		if watched[name] || strings.HasSuffix(name, "Routes") || strings.HasSuffix(name, "ServerOptions") {
			out = append(out, name)
		}
		return true
	})
	return out
}

// The rig is the production build: the routes it mounts on each listener are the routes configure serves (the route
// probe's set), and configureWithRoutes still makes the route-set and listener calls the rig mirrors, in its order.
func TestWrongClassGateAuthServiceRigMatchesConfigure(t *testing.T) {
	rig := newAuthGateRig(t)
	served := servedAuthRoutes(t)
	mounted := map[string]bool{}
	for key := range rig.mounted {
		if route, internal := strings.CutSuffix(key, "@internal"); !internal || isInternalOnly(route) {
			mounted[key] = true
		}
	}
	var onlyRig, onlyServed []string
	for key := range mounted {
		if !served[key] {
			onlyRig = append(onlyRig, key)
		}
	}
	for key := range served {
		if !mounted[key] {
			onlyServed = append(onlyServed, key)
		}
	}
	sort.Strings(onlyRig)
	sort.Strings(onlyServed)
	if len(onlyRig) > 0 || len(onlyServed) > 0 {
		t.Errorf("the rig mounts %v that configure does not serve, and configure serves %v that the rig does not mount", onlyRig, onlyServed)
	}
	var internal []string
	for key := range rig.mounted {
		if route, ok := strings.CutSuffix(key, "@internal"); ok {
			internal = append(internal, route)
		}
	}
	sort.Strings(internal)
	want := append(slices.Clone(pepRoutes), acrEventsKey)
	sort.Strings(want)
	if !slices.Equal(internal, want) {
		t.Errorf("the rig's internal listener mounts %v, configure's serves %v", internal, want)
	}

	file, err := parser.ParseFile(token.NewFileSet(), "runtime.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var calls []string
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == "configureWithRoutes" {
			calls = routeCalls(fn)
		}
	}
	if !slices.Equal(calls, configureRouteCalls) {
		t.Errorf("configureWithRoutes makes the route and listener calls %q, the rig mirrors %q: update the rig first", calls, configureRouteCalls)
	}
}

// auth-service has no shadow beside its guard, so the wrong-class gate's no-shadow rule holds by construction. The
// auth-service build does import go-api's policy package (its compat routes share its wire helpers), so the rule is on
// use: no package of the build but policy itself names a policy shadow (NewShadow, Shadow, ParseShadowRows or any
// Shadow* identifier) or calls SetShadow. Its PDP decisions and audit rows are store writes, which the dials count.
func TestWrongClassGateAuthServiceHasNoShadow(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}\t{{.Dir}}\t{{join .GoFiles \",\"}}{{end}}", "./cmd/auth-service")
	command.Dir = root
	out, err := command.Output()
	if err != nil {
		t.Fatalf("go list -deps ./cmd/auth-service: %v", err)
	}
	const policy = "github.com/full-chaos/dev-health-ops/internal/api/policy"
	packages, importers, uses := 0, 0, []string{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		importPath, rest, _ := strings.Cut(line, "\t")
		dir, files, _ := strings.Cut(rest, "\t")
		if importPath == "" {
			continue
		}
		packages++
		if importPath == policy {
			continue
		}
		imports := false
		for _, name := range strings.Split(files, ",") {
			if name == "" {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			local := ""
			for _, spec := range file.Imports {
				if path := strings.Trim(spec.Path.Value, `"`); path == policy {
					local = "policy"
					if spec.Name != nil {
						local = spec.Name.Name
					}
				}
			}
			imports = imports || local != ""
			ast.Inspect(file, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				ident, isIdent := selector.X.(*ast.Ident)
				name := selector.Sel.Name
				if name == "SetShadow" || (isIdent && local != "" && ident.Name == local &&
					(name == "NewShadow" || name == "ParseShadowRows" || strings.HasPrefix(name, "Shadow"))) {
					uses = append(uses, fmt.Sprintf("%s: %s", fset.Position(selector.Pos()), name))
				}
				return true
			})
		}
		if imports {
			importers++
		}
	}
	if packages == 0 || importers == 0 {
		t.Fatalf("go list named %d packages, %d import policy: the build was not read", packages, importers)
	}
	if len(uses) > 0 {
		t.Errorf("the auth-service build uses a policy shadow: %v", uses)
	}
	t.Logf("WRONG_CLASS_GATE_NO_SHADOW service=auth-service packages=%d policy_importers=%d", packages, importers)
}

// The social-login ledger row excuses its 500 with the store down (the refusal audit row cannot be written). That
// state is not Ready: the required postgres check (runtime.go, RegisterRequired(CheckPostgres)) fails /readyz, so a
// load balancer sends the route no traffic while it lasts.
func TestWrongClassGateAuthServiceStoreDownIsNotReady(t *testing.T) {
	rig := newAuthGateRig(t)
	got := rig.serve("operator", httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if got.answer.Status != http.StatusServiceUnavailable || !strings.Contains(got.answer.Body, CheckPostgres) {
		t.Fatalf("/readyz with the store down answered %s, want 503 naming the %s check", got.answer, CheckPostgres)
	}
	if got.dials == 0 {
		t.Fatal("/readyz made no store dial: the readiness check did not reach the rig's store")
	}
}

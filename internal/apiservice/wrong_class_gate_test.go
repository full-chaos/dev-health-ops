package apiservice

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/billing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/cigates/exceptionledger"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/wrongclass"
)

// gateDecider is the shadow's PDP: it counts every decision the shadow asks for.
type gateDecider struct {
	mu    sync.Mutex
	calls int
}

func (d *gateDecider) Decide(context.Context, pep.Request) pep.Decision {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return pep.Decision{}
}

func (d *gateDecider) DecideBatch(ctx context.Context, requests []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(requests))
	for i, request := range requests {
		out[i] = d.Decide(ctx, request)
	}
	return out
}

func (d *gateDecider) take() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	calls := d.calls
	d.calls = 0
	return calls
}

// gateRig is go-api's three listeners as configureWith builds them: the real route builders (Routes,
// InternalRoutes, billing.EdgeRoutes), newServer with the intake gate and the scope layers over the real row
// tables, the routed access validator, the delegated rows, the acr workload verifier, and the shadow beside the
// guard. The doubles are the key source, the store, the PDP and the limiter's counter store; Postgres and
// ClickHouse dial a closed port, so a cell that reaches either answers 5xx.
type gateRig struct {
	handlers map[string]http.Handler
	mounted  map[string]bool
	counts   *lookupCounts
	limiter  *pushLimiterStore
	decider  *gateDecider
	shadow   *policy.Shadow
	minter   *eddsaMinter
}

func newGateRig(t *testing.T) *gateRig {
	t.Helper()
	logger := quietLogger()
	counts := &lookupCounts{}
	minter := newEdDSAMinter(t)
	keys := matrixKeys{inner: minter.keys, counts: counts}
	auth, err := policy.NewAuthenticator(matrixStore{probeStore: newProbeStore(), counts: counts}, logger)
	if err != nil {
		t.Fatal(err)
	}
	delegated, err := goAPIDelegatedRows()
	if err != nil {
		t.Fatal(err)
	}
	auth.DelegatedRows(delegated)
	eddsa, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows: realRows(t), EdDSA: eddsa, Meter: sdkmetric.NewMeterProvider().Meter("gate"), Logger: logger,
		Now: func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RouteAccess(matrixValidator{inner: routedAccessVerify{validator}, counts: counts}, goAPIAccessService); err != nil {
		t.Fatal(err)
	}
	workload, err := buildACRWorkload(acrWorkloadLookup(map[string]string{envAccessTokenIssuer: probeIssuer}), keys, acr.StaticSubject(acrWorkloadSubject))
	if err != nil || workload == nil {
		t.Fatalf("acr workload verifier %v: %v", workload, err)
	}
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	clickHouse, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:1"}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickHouse.Close() })
	limiter := &pushLimiterStore{}
	limiter.reset()
	guard := policy.NewGuard(auth, logger)
	decider := &gateDecider{}
	shadowRows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	shadow, err := policy.NewShadow(policy.ShadowConfig{Decider: decider, Rows: shadowRows, HighRisk: true, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	guard.SetShadow(shadow)
	deps := Deps{Pool: pool, ClickHouse: clickHouse, Auth: auth, Guard: guard, Limits: limiter, ACRWorkload: workload, ServiceAccounts: stubAccounts{}}
	scopeRows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	scope := policy.NewScope(auth, logger)
	scope.DeclaredRows(scopeRows)
	cfg := apiTestConfig()
	cfg.APIInternalAddress, cfg.APIBillingEdgeAddress = "127.0.0.1:0", "127.0.0.1:0"

	rig := &gateRig{handlers: map[string]http.Handler{}, mounted: map[string]bool{}, counts: counts, limiter: limiter, decider: decider, shadow: shadow, minter: minter}
	mount := func(listener string, routes []httpapi.Route, build func([]httpapi.Route) (*httpapi.Server, error)) {
		server, err := build(routes)
		if err != nil {
			t.Fatalf("%s listener: %v", listener, err)
		}
		rig.handlers[listener] = server.Handler()
		for _, route := range routes {
			rig.mounted[route.Method+" "+route.Pattern+" @"+listener] = true
		}
	}
	mount("api", Routes(deps, logger), func(routes []httpapi.Route) (*httpapi.Server, error) {
		return newServer(cfg, logger, routes, NewIntakeGate(&deps, logger), deps.AuthProxy.ScopeExemptRoutes(), scope.Layers()...)
	})
	// The acr routes audit every refusal before they answer (an audit that cannot be written is 503, by design),
	// so the internal listener's routes are internalRoutesWith, the builder InternalRoutes ends in, over an audit
	// store that records; the patterns must be InternalRoutes' own.
	internalRoutes := internalRoutesWith(acr.Deps{Logger: logger, Auth: gateAudit{}, LLMConfig: gateLLMConfig{}, Workload: workload,
		Limiter: httpapi.NewKeyedLimiter(limiter, acr.AuthAttemptLimit), AccessLimiter: httpapi.NewKeyedLimiter(limiter, acr.AccessAttemptLimit)})
	if got, want := routeKeys(internalRoutes), routeKeys(InternalRoutes(deps, logger)); !slices.Equal(got, want) {
		t.Fatalf("the gate's internal routes %v, InternalRoutes mounts %v", got, want)
	}
	mount("internal", internalRoutes, func(routes []httpapi.Route) (*httpapi.Server, error) {
		return NewInternalServer(cfg, logger, routes)
	})
	mount("billing-edge", billing.EdgeRoutes(billing.Deps{Pool: pool, Logger: logger}), func(routes []httpapi.Route) (*httpapi.Server, error) {
		return NewEdgeServer(cfg, logger, routes)
	})
	return rig
}

// gateAudit is the acr routes' audit store: every row is written.
type gateAudit struct{}

func (gateAudit) Audit(context.Context, string, string, string, string) error { return nil }

// gateLLMConfig is the llm-config route's source: configured, every read fails.
type gateLLMConfig struct{}

func (gateLLMConfig) OrgConfig(context.Context, string) (llmorgsettings.OrgConfig, bool, error) {
	return llmorgsettings.OrgConfig{}, false, errors.New("gate: no store")
}

func routeKeys(routes []httpapi.Route) []string {
	var out []string
	for _, route := range routes {
		out = append(out, route.Method+" "+route.Pattern)
	}
	slices.Sort(out)
	return out
}

type gateCell struct {
	answer   wrongclass.Answer
	lookups  int
	store    int
	keys     int
	decision int
}

// serve sends one request on a listener with fresh counters and a fresh limiter window, and waits for the
// shadow's asynchronous decisions before it reads them.
func (rig *gateRig) serve(listener string, request *http.Request) gateCell {
	rig.counts.reset()
	rig.limiter.reset()
	rig.decider.take()
	recorder := httptest.NewRecorder()
	rig.handlers[listener].ServeHTTP(recorder, request)
	rig.shadow.Wait()
	rig.counts.mu.Lock()
	keys, store := rig.counts.keys, rig.counts.store
	rig.counts.mu.Unlock()
	return gateCell{answer: wrongclass.Capture(recorder), lookups: rig.counts.total(), store: store, keys: keys, decision: rig.decider.take()}
}

// mint is the gate's minter: an EdDSA token for the probe's live user and session under any typ, or an HS256
// token of the retired class.
func (rig *gateRig) mint(t *testing.T) wrongclass.Mint {
	return func(alg, typ string) string {
		switch alg {
		case "EdDSA":
			return matrixMint(rig.minter, typ)
		case "HS256":
			return probeHS256(t, probeKey, map[string]any{"sub": probeUser.String(), "org_id": probeOrg.String(), "role": "admin", "tv": 3})
		}
		return ""
	}
}

// gateWorkloadToken is the acr-api workload token the internal routes accept.
func gateWorkloadToken(m *eddsaMinter, now time.Time) string {
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": "workload+jwt", "kid": "probe-kid"})
	payload, _ := json.Marshal(map[string]any{
		"iss": probeIssuer, "aud": accessTokenAudienceAPI, "sub": acrWorkloadSubject, "principal_type": "workload",
		"jti": strings.Repeat("w", 24), "iat": now.Unix() - 5, "nbf": now.Unix() - 5, "exp": now.Unix() + 200,
	})
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(payload)
	return input + "." + enc.EncodeToString(ed25519.Sign(m.priv, []byte(input)))
}

// gateLedgerFile is the gate's own path in the exception ledger: a ledger row anchored here whose needle starts with
// gateDivergencePrefix names the profile rows (METHOD route, "|" between) whose G3 (4xx on a protected row) refusal
// is a known divergence of an open ticket. G1, G2 and G4 still hold there, and a named row whose G3 cells all pass
// fails the gate: the divergence is fixed and its row must go.
const (
	gateLedgerFile       = "internal/apiservice/wrong_class_gate_test.go"
	gateDivergencePrefix = "wrong-class G3 known divergence: "
)

// The ledger rows anchored here (each needle is the line below, verbatim):
//
//	wrong-class G3 known divergence: GET /api/v1/billing/plans|GET /api/v1/billing/plans/{plan_id}
func gateDivergences(t *testing.T) map[string]string {
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
	out := map[string]string{}
	for _, row := range rows {
		if row.Path != gateLedgerFile {
			continue
		}
		profiles, ok := strings.CutPrefix(row.Needle, gateDivergencePrefix)
		if !ok || row.OwnerState != "open" {
			t.Fatalf("ledger %s: a row anchored on the gate must be an open %q row", row.ID, gateDivergencePrefix)
		}
		for _, profile := range strings.Split(profiles, "|") {
			out[profile] = row.ID + " (" + row.Owner + ")"
		}
	}
	return out
}

// runGoAPIGate drives every go-api endpoint-profile row, on the listener its row names, with every
// credential class of credential-classes.json the row does not accept, through the production listeners. A wrong
// class answers exactly what a garbage credential in the same carrier answers (no credential, for a carrier other
// than the Authorization header), reads no signing key and no store row, asks the shadow PDP nothing, and on a
// protected row is a 4xx. The accept controls show the rig tells an accepted class from a refused one.
func runGoAPIGate(t *testing.T, divergences map[string]string, errorf func(format string, args ...any)) wrongclass.Census {
	rig := newGateRig(t)
	rows, err := wrongclass.LoadRows(authv1.GoAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	classes, err := wrongclass.LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	census := wrongclass.Census{Service: "go-api", Rows: len(rows), Classes: len(classes)}
	divergent := map[string]int{}
	var carriers []wrongclass.Carrier
	for _, class := range classes {
		carrier, ok, err := wrongclass.CarrierFor(class, wrongclass.Options{Mint: rig.mint(t), HeaderValue: func(name string) string {
			if name == "X-Org-Id" {
				return probeOrg.String()
			}
			return ""
		}})
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

	userToken := wrongclass.Carrier{Class: matrixUserClass, Header: wrongclass.Authorization, Value: "Bearer " + matrixMint(rig.minter, "at+jwt")}
	workloadToken := wrongclass.Carrier{Class: "workload_token", Header: wrongclass.Authorization, Value: "Bearer " + gateWorkloadToken(rig.minter, time.Now())}
	userAccepts, workloadAccepts, shadowAsked := 0, 0, 0
	for _, row := range rows {
		if len(row.Listeners) != 1 || rig.handlers[row.Listeners[0]] == nil {
			errorf("%s: listener %v is not one go-api listener", row.ID, row.Listeners)
			continue
		}
		listener := row.Listeners[0]
		if !rig.mounted[row.Method+" "+row.Route+" @"+listener] {
			errorf("%s: no route %s %s is mounted on the %s listener: the row is not measured", row.ID, row.Method, row.Route, listener)
			continue
		}
		baselines := map[string]gateCell{}
		baselineOf := func(carrier wrongclass.Carrier) gateCell {
			base := carrier.Baseline()
			if cell, ok := baselines[base.Class]; ok {
				return cell
			}
			cell := rig.serve(listener, base.Request(row.Method, row.Path()))
			baselines[base.Class] = cell
			return cell
		}
		for _, carrier := range carriers {
			if row.Accepts(carrier.Class) {
				continue
			}
			census.WrongCells++
			got := rig.serve(listener, carrier.Request(row.Method, row.Path()))
			want := baselineOf(carrier)
			if got.answer != want.answer {
				errorf("%s: wrong class %s answered %s, a garbage credential in its carrier answers %s", row.ID, carrier.Class, got.answer, want.answer)
			}
			if got.lookups != 0 {
				errorf("%s: wrong class %s made %d reads (keys %d, store %d)", row.ID, carrier.Class, got.lookups, got.keys, got.store)
			}
			if got.decision != 0 {
				errorf("%s: wrong class %s made the shadow ask the PDP %d times", row.ID, carrier.Class, got.decision)
			}
			if row.Protected() && got.answer.Status/100 != 4 {
				if owner, known := divergences[row.Method+" "+row.Route]; known {
					divergent[row.Method+" "+row.Route]++
					t.Logf("%s: wrong class %s on a protected row answered %s: known divergence, ledger %s", row.ID, carrier.Class, got.answer, owner)
				} else {
					errorf("%s: wrong class %s on a protected row answered %s, want a 4xx refusal", row.ID, carrier.Class, got.answer)
				}
			}
		}
		for _, control := range []struct {
			carrier wrongclass.Carrier
			count   *int
		}{{userToken, &userAccepts}, {workloadToken, &workloadAccepts}} {
			if !row.Accepts(control.carrier.Class) {
				continue
			}
			census.AcceptCells++
			*control.count++
			got := rig.serve(listener, control.carrier.Request(row.Method, row.Path()))
			shadowAsked += got.decision
			if want := baselineOf(control.carrier); got.store == 0 && got.answer == want.answer {
				errorf("%s: accepted class %s answered %s with no store read, the garbage answer: the rig cannot tell accept from refuse", row.ID, control.carrier.Class, got.answer)
			}
		}
	}
	if userAccepts == 0 || workloadAccepts == 0 {
		errorf("accept controls: %d user_access_token, %d workload_token: a class with no control cell is not measured", userAccepts, workloadAccepts)
	}
	if shadowAsked == 0 {
		t.Error("no accept cell made the shadow ask the PDP: the shadow seam is not live, so zero on a wrong cell proves nothing")
	}
	for profile, owner := range divergences {
		if divergent[profile] == 0 {
			errorf("ledger %s names %s, whose wrong-class cells all answer 4xx (or no such row): remove the ledger row", owner, profile)
		}
	}
	if census.WrongCells == 0 {
		t.Error("no wrong-class cell ran")
	}
	return census
}

func TestWrongClassGateGoAPI(t *testing.T) {
	census := runGoAPIGate(t, gateDivergences(t), t.Errorf)
	t.Log(census.Line())
}

// A ledger row that names a row whose wrong-class cells all answer 4xx fails the gate, so the fix of the divergence
// forces its row out; the ledger's own rows are excused for G3 only and fail nothing else.
func TestWrongClassGateFailsOnAStaleDivergence(t *testing.T) {
	const stale = "GET /api/v1/auth/me"
	divergences := gateDivergences(t)
	if _, ok := divergences[stale]; ok {
		t.Fatalf("the ledger names %s", stale)
	}
	divergences[stale] = "stale-probe (CHAOS-0)"
	var failures []string
	runGoAPIGate(t, divergences, func(format string, args ...any) { failures = append(failures, fmt.Sprintf(format, args...)) })
	want := "ledger stale-probe (CHAOS-0) names " + stale + ", whose wrong-class cells all answer 4xx (or no such row): remove the ledger row"
	if len(failures) != 1 || failures[0] != want {
		t.Fatalf("failures %q, want exactly %q", failures, want)
	}
}

// With no ledger row, every G3 cell of the rows the ledger names fails, and nothing else does: the ledger excuses
// exactly its named rows.
func TestWrongClassGateExcusesOnlyTheNamedRows(t *testing.T) {
	named := gateDivergences(t)
	var failures []string
	runGoAPIGate(t, map[string]string{}, func(format string, args ...any) { failures = append(failures, fmt.Sprintf(format, args...)) })
	failed := map[string]bool{}
	for _, failure := range failures {
		profile, _, _ := strings.Cut(failure, " [")
		if _, ok := named[profile]; !ok || !strings.HasSuffix(failure, "want a 4xx refusal") {
			t.Errorf("a failure outside the named G3 cells: %s", failure)
		}
		failed[profile] = true
	}
	for profile := range named {
		if !failed[profile] {
			t.Errorf("%s: no G3 failure without its ledger row", profile)
		}
	}
}

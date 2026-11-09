package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-go/pep"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/cigates/exceptionledger"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/wrongclass"
)

// The rigs of the query-api wrong-class gate. A' is the production REST route set: each restGroups builder
// (rest_routes.go) built from env, mounted the way BuildWithLookup mounts it (server.go:427-460: one
// mux.HandleFunc(pattern, withProofProvenance(handler, runningBuild())) per mount, then the /api/v1/ catch-all), behind
// the PDP gate (pdp_gate.go:28-44) and the real Listeners/MCPListener. Its ClickHouse and Postgres point at a loopback
// listener that counts each dial and closes it, and its access-token issuer at a second one, so a store dial or a key
// fetch by a request is seen. B is the rest: the rows BuildWithLookup cannot build without a live ClickHouse (its
// /query route pings ClickHouse at construction, query_route.go:6126-6133, and two REST builders ping a ClickHouse
// write connection), on the listener composition endpoint_profile_behaviour_test.go:66 builds, with a counting key
// source and store.
const (
	rigA = "A'"
	rigB = "B"
)

// connCounter accepts and closes every connection and counts them.
type connCounter struct {
	ln    net.Listener
	conns atomic.Int64
}

func newConnCounter(t *testing.T) *connCounter {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &connCounter{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			c.conns.Add(1)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return c
}

// qGateDecider is the shadow's PDP: it counts every decision the shadow asks for.
type qGateDecider struct {
	mu    sync.Mutex
	calls int
}

func (d *qGateDecider) Decide(context.Context, pep.Request) pep.Decision {
	d.mu.Lock()
	d.calls++
	d.mu.Unlock()
	return pep.Decision{}
}

func (d *qGateDecider) DecideBatch(ctx context.Context, requests []pep.Request) []pep.Decision {
	out := make([]pep.Decision, len(requests))
	for i, request := range requests {
		out[i] = d.Decide(ctx, request)
	}
	return out
}

func (d *qGateDecider) take() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	calls := d.calls
	d.calls = 0
	return calls
}

// qGateRig is one rig: its listeners, what it mounts, and its read counters.
type qGateRig struct {
	name     string
	handlers map[string]http.Handler
	gate     *pdp.Gate
	decider  *qGateDecider
	// A'
	stores  *connCounter
	keys    *connCounter
	issuer  string
	mounted map[string]string // "METHOD pattern" -> builder
	unbuilt map[string]string // "METHOD pattern" -> builder error
	// B
	counts *matrixCounts
	minter *accessMinter
}

type qGateCell struct {
	answer   wrongclass.Answer
	keys     int64
	stores   int64
	decision int
}

func (rig *qGateRig) serve(listener string, request *http.Request) qGateCell {
	var keys0, stores0 int64
	if rig.counts != nil {
		rig.counts.reset()
	} else {
		keys0, stores0 = rig.keys.conns.Load(), rig.stores.conns.Load()
	}
	rig.decider.take()
	recorder := httptest.NewRecorder()
	rig.handlers[listener].ServeHTTP(recorder, request)
	if shadow := rig.gate.Shadow(); shadow != nil {
		shadow.Wait()
	}
	cell := qGateCell{answer: wrongclass.Capture(recorder), decision: rig.decider.take()}
	if rig.counts != nil {
		keys, store := rig.counts.snapshot()
		cell.keys, cell.stores = int64(keys), int64(store)
	} else {
		cell.keys, cell.stores = rig.keys.conns.Load()-keys0, rig.stores.conns.Load()-stores0
	}
	return cell
}

func qGateShadowGate(t *testing.T, decider *qGateDecider) *pdp.Gate {
	t.Helper()
	gate := pdp.New(pdp.Config{Mode: pdp.ModeShadow, Decider: decider, ExtraShadowRows: graphQLShadowRows(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if gate.Shadow() == nil {
		t.Fatal("the shadow-mode gate holds no shadow")
	}
	return gate
}

// newRigA builds the production REST route set from env over counting loopback stores and key issuer.
func newRigA(t *testing.T) *qGateRig {
	t.Helper()
	rig := &qGateRig{name: rigA, decider: &qGateDecider{}, stores: newConnCounter(t), keys: newConnCounter(t),
		mounted: map[string]string{}, unbuilt: map[string]string{}}
	rig.issuer = "https://" + rig.keys.ln.Addr().String()
	addr := rig.stores.ln.Addr().String()
	env := map[string]string{
		"CLICKHOUSE_URI":               "clickhouse://u:p@" + addr + "/db?dial_timeout=1s",
		"GO_API_REGISTRY_POSTGRES_URI": "postgres://u:p@" + addr + "/db?sslmode=disable&connect_timeout=1",
		accessTokenIssuerEnvVar:        rig.issuer,
	}
	getenv := getenvFunc(func(name string) string { return env[name] })
	pool, err := pgxpool.New(context.Background(), env["GO_API_REGISTRY_POSTGRES_URI"])
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	users, err := newEdgeUserStore(getenv, pool)
	if err != nil || users == nil {
		t.Fatalf("edge users store %v: %v", users, err)
	}
	mux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	for _, group := range restGroups {
		handlers, cleanup, ok, err := group.Build(getenv, users)
		if err != nil {
			for _, mount := range group.Mounts {
				for _, method := range mount.Methods {
					rig.unbuilt[method+" "+mount.Pattern] = group.Builder + ": " + err.Error()
				}
			}
			continue
		}
		if !ok {
			t.Fatalf("%s: not configured with ClickHouse, Postgres and the issuer set", group.Builder)
		}
		if cleanup != nil {
			t.Cleanup(cleanup)
		}
		if len(handlers) != len(group.Mounts) {
			t.Fatalf("%s returned %d handler(s) for %d mount(s)", group.Builder, len(handlers), len(group.Mounts))
		}
		for i, mount := range group.Mounts {
			mux.HandleFunc(mount.Pattern, withProofProvenance(handlers[i], runningBuild()))
			for _, method := range mount.Methods {
				rig.mounted[method+" "+mount.Pattern] = group.Builder
			}
		}
	}
	mux.HandleFunc("/api/v1/", apiV1CatchAllNotFound)
	rig.gate = qGateShadowGate(t, rig.decider)
	handler := planeHandler(mux, rig.gate)
	internalMux.Handle("/", handler)
	plane := &Plane{Handler: handler, InternalHandler: withGate(internalMux, rig.gate), MCPHandler: withGate(mcpMux, rig.gate)}
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, nil)
	mcp := MCPListener("127.0.0.1:0", plane, nil)
	if internal == nil || mcp == nil {
		t.Fatal("expected internal and MCP listeners")
	}
	rig.handlers = map[string]http.Handler{"query": public.server.Handler, "internal": internal.server.Handler, "mcp": mcp.server.Handler}
	return rig
}

// newRigB is the listener composition of newAccessStackOn (endpoint_profile_behaviour_test.go:66) with the PDP gate
// of gatedListenerHandlers (pdp_gate.go:28) over it, a counting key source and store, and the REST rows A' cannot
// build on the REST authenticator (the old matrix's form).
func newRigB(t *testing.T, restB []string) *qGateRig {
	t.Helper()
	counts := &matrixCounts{}
	rig := &qGateRig{name: rigB, decider: &qGateDecider{}, counts: counts, minter: newAccessMinter(t)}
	base := matrixStoreBase{accessStore: newAccessStore(), counts: counts}
	store := queryAPIUserStore{queryAPIStoreBase: base}
	setup := testAccess(matrixKeySource{inner: rig.minter.keys, counts: counts}, nil, accessTestNow)
	getenv := getenvFunc(func(name string) string {
		switch name {
		case deploymentEnvEnv:
			return "development"
		case proofRouteEnabledEnv, proofWriteRouteEnabledEnv:
			return "true"
		}
		return ""
	})
	verifier, _ := iaVerifier(t)
	auth := ecEdgeAuth(t, store)
	if err := routeAuthenticator(auth, setup.opts); err != nil {
		t.Fatal(err)
	}
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	query := newDocumentDispatchHandler(getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, store, "", nil)
	publicMux, internalMux, mcpMux := http.NewServeMux(), http.NewServeMux(), http.NewServeMux()
	mountQueryRouteSets(getenv, publicMux, internalMux, mcpMux, queryRouteHandlers{
		Query:        query,
		Registry:     newRegistryHandler("digest", map[string]string{"probe": digestHex(iaDocument)}),
		BuildInfo:    newBuildInfoHandler(verifier, auth, store),
		Proof:        query,
		ProofWrite:   query,
		RunOperation: query,
		MCP:          newMCPHandlerWithLimits(&countingMCPClient{}, nil, allMCPRootsEnabled(), getenv, mcpDefaultLimits()),
	}, graphQLEdgeDeps{auth: auth, corsOrigins: []string{"https://app.example"}, maxBytes: defaultGraphQLMaxQueryBytes})
	edge, err := principal.NewEdgeVerifier(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := routeEdgeVerifier(edge, setup.opts); err != nil {
		t.Fatal(err)
	}
	restVerifier, _ := iaVerifier(t)
	for _, pattern := range restB {
		publicMux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if _, ok := authenticateRESTRequest(w, r, restVerifier, edge, "wrong-class-gate"); ok {
				w.WriteHeader(http.StatusOK)
			}
		})
	}
	rig.gate = qGateShadowGate(t, rig.decider)
	handler := planeHandler(publicMux, rig.gate)
	internalMux.Handle("/", handler)
	plane := &Plane{Handler: handler, InternalHandler: withGate(internalMux, rig.gate), MCPHandler: withGate(mcpMux, rig.gate)}
	registry := health.NewRegistry(time.Second)
	registry.SetReady(true)
	operator, err := health.NewServer(health.ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test"})
	if err != nil {
		t.Fatal(err)
	}
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, OperatorCompat(registry, operator, "dev-health-query-api"), nil)
	mcp := MCPListener("127.0.0.1:0", plane, nil)
	if internal == nil || mcp == nil {
		t.Fatal("expected internal and MCP listeners")
	}
	rig.handlers = map[string]http.Handler{
		"query": public.server.Handler, "internal": internal.server.Handler, "mcp": mcp.server.Handler, "operator": operator.Handler(),
	}
	return rig
}

// qGateRequest is the row's request on a carrier: the GraphQL and /query routes get their document (the way
// behaviourStack.sendWith sends it, endpoint_profile_behaviour_test.go:141), an api_route row a GET.
func qGateRequest(row wrongclass.Row, carrier wrongclass.Carrier) *http.Request {
	method, path := row.Method, row.Path()
	if method == "api_route" {
		method = http.MethodGet
	}
	var request *http.Request
	switch {
	case carrier.BodyField != "":
		request = carrier.Request(method, path)
	case path == "/graphql" && (method == http.MethodGet || method == http.MethodHead):
		request = httptest.NewRequest(method, "/graphql?org_id="+ecOrg.String()+"&query="+url.QueryEscape(iaDocument), nil)
	case path == "/query" || path == "/graphql" || strings.HasPrefix(path, "/query/"):
		request = httptest.NewRequest(method, path, strings.NewReader(`{"query":`+jsonQuoteString(iaDocument)+`}`))
		request.Header.Set("Content-Type", "application/json")
	default:
		request = httptest.NewRequest(method, path, nil)
	}
	if carrier.Header != "" {
		request.Header.Set(carrier.Header, carrier.Value)
	}
	return request
}

// qGateLedgerFile is the gate's own path in the exception ledger; a row anchored here whose needle starts with
// gateDivergencePrefixQ names the profile rows (METHOD route, "|" between) whose G3 refusal is a known divergence of
// an open ticket. G1, G2 and G4 still hold there, and a named row whose G3 cells all pass fails the gate.
const (
	qGateLedgerFile       = "internal/queryapi/server/wrong_class_gate_test.go"
	gateDivergencePrefixQ = "wrong-class G3 known divergence: "
)

func qGateDivergences(t *testing.T) map[string]string {
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
		if row.Path != qGateLedgerFile {
			continue
		}
		profiles, ok := strings.CutPrefix(row.Needle, gateDivergencePrefixQ)
		if !ok || row.OwnerState != "open" {
			t.Fatalf("ledger %s: a row anchored on the gate must be an open %q row", row.ID, gateDivergencePrefixQ)
		}
		for _, profile := range strings.Split(profiles, "|") {
			out[profile] = row.ID + " (" + row.Owner + ")"
		}
	}
	return out
}

// qGateRigOf is the rig a row runs on and why: A' for a row the production REST route set mounts (and the /api/v1/
// catch-all), B for a REST row whose builder cannot build without a live ClickHouse, and for the rows of the /query
// route set and the operator, which BuildWithLookup builds only after a live ClickHouse ping.
func qGateRigOf(row wrongclass.Row, a *qGateRig) (rig, reason string) {
	key := row.Method + " " + row.Route
	switch {
	case a.mounted[key] != "":
		return rigA, "production REST builder " + a.mounted[key]
	case row.Method == "api_route" && row.Route == "/api/v1/":
		return rigA, "the /api/v1/ catch-all (server.go:475)"
	case a.unbuilt[key] != "":
		return rigB, "build-time ClickHouse write ping: " + strings.SplitN(a.unbuilt[key], ":", 2)[0]
	case !strings.HasPrefix(row.Route, "/api/v1/"):
		return rigB, "buildQueryRoute pings ClickHouse at construction (query_route.go:6131)"
	}
	return "", "no rig mounts it"
}

// runQueryAPIGate drives every query-api endpoint-profile row, on each listener the row names, with every credential
// class of credential-classes.json the row does not accept. A wrong class answers exactly what a garbage credential in
// the same carrier answers (no credential, for a carrier other than the Authorization header), dials no store and
// fetches no key (A': counted at the loopback listeners; B: the counting key source and store), asks the shadow PDP
// nothing, and on a protected row is a 4xx. Every A' wrong-class cell runs before the first A' accept cell, so a key
// fetch by a wrong class is the first fetch of its JWKS client and is seen. The accept controls show each rig tells an
// accepted user_access_token from a refused one.
func runQueryAPIGate(t *testing.T, divergences map[string]string, errorf func(format string, args ...any)) (wrongclass.Census, []string) {
	rows, err := wrongclass.LoadRows(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	classes, err := wrongclass.LoadClasses(authv1.CredentialClasses)
	if err != nil {
		t.Fatal(err)
	}
	a := newRigA(t)
	var restB []string
	for key := range a.unbuilt {
		_, pattern, _ := strings.Cut(key, " ")
		if !slices.Contains(restB, pattern) {
			restB = append(restB, pattern)
		}
	}
	b := newRigB(t, restB)
	mintFor := func(m *accessMinter, issuer string, now time.Time) wrongclass.Mint {
		return func(alg, typ string) string {
			if alg != "EdDSA" {
				return ecMintHS256Token(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
			}
			return m.mint(ecUser, map[string]any{"iss": issuer, "iat": now.Unix() - 60, "nbf": now.Unix() - 60, "exp": now.Unix() + 600},
				map[string]any{"typ": typ})
		}
	}
	census := wrongclass.Census{Service: "query-api", Rows: len(rows), Classes: len(classes)}
	carriersOf := func(mint wrongclass.Mint) []wrongclass.Carrier {
		var out []wrongclass.Carrier
		census.NotDriven = nil
		for _, class := range classes {
			carrier, ok, err := wrongclass.CarrierFor(class, wrongclass.Options{Mint: mint, HeaderValue: func(name string) string {
				if name == "X-Org-Id" {
					return ecOrg.String()
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
			out = append(out, carrier)
		}
		return out
	}
	aMinter := newAccessMinter(t)
	carriers := map[string][]wrongclass.Carrier{
		rigA: carriersOf(mintFor(aMinter, a.issuer, time.Now())),
		rigB: carriersOf(mintFor(b.minter, accessTestIssuer, accessTestNow)),
	}
	census.Driven = len(carriers[rigB])
	if census.Driven == 0 || len(carriers[rigA]) != census.Driven {
		t.Fatalf("driven classes: A' %d, B %d", len(carriers[rigA]), census.Driven)
	}
	accept := map[string]wrongclass.Carrier{
		rigA: {Class: classUserToken, Header: wrongclass.Authorization, Value: "Bearer " + mintFor(aMinter, a.issuer, time.Now())("EdDSA", "at+jwt")},
		rigB: {Class: classUserToken, Header: wrongclass.Authorization, Value: "Bearer " + b.minter.mint(ecUser, nil, nil)},
	}
	rigs := map[string]*qGateRig{rigA: a, rigB: b}

	type place struct {
		row      wrongclass.Row
		listener string
		rig      string
	}
	var places []place
	var rigLines []string
	for _, row := range rows {
		rig, reason := qGateRigOf(row, a)
		if rig == "" {
			errorf("%s: %s: the row is not measured", row.ID, reason)
			continue
		}
		rigLines = append(rigLines, fmt.Sprintf("%s -> %s (%s)", row.ID, rig, reason))
		for _, listener := range row.Listeners {
			if rigs[rig].handlers[listener] == nil {
				errorf("%s: rig %s has no %s listener: the row is not measured", row.ID, rig, listener)
				continue
			}
			places = append(places, place{row, listener, rig})
		}
	}
	divergent := map[string]int{}
	baselines, volatile := map[string]qGateCell{}, map[string]bool{}
	baselineOf := func(p place, carrier wrongclass.Carrier) qGateCell {
		base := carrier.Baseline()
		key := p.rig + "|" + p.listener + "|" + p.row.ID + "|" + base.Class
		if cell, ok := baselines[key]; ok {
			return cell
		}
		cell := rigs[p.rig].serve(p.listener, qGateRequest(p.row, base))
		if cell.keys != 0 || cell.stores != 0 || cell.decision != 0 {
			errorf("%s @%s: the garbage baseline %s made %d key reads, %d store reads, %d PDP asks", p.row.ID, p.listener, base.Class, cell.keys, cell.stores, cell.decision)
		}
		// A body that differs between two identical requests (a live metrics scrape) is compared on status and
		// headers only; the status and headers must still repeat.
		again := rigs[p.rig].serve(p.listener, qGateRequest(p.row, base))
		if again.answer.Status != cell.answer.Status || again.answer.Header != cell.answer.Header {
			errorf("%s @%s: two garbage baselines answered %s and %s", p.row.ID, p.listener, cell.answer, again.answer)
		}
		if again.answer.Body != cell.answer.Body {
			volatile[key] = true
		}
		baselines[key] = cell
		return cell
	}
	sameAnswer := func(p place, carrier wrongclass.Carrier, got, want wrongclass.Answer) bool {
		key := p.rig + "|" + p.listener + "|" + p.row.ID + "|" + carrier.Baseline().Class
		if got != want && got.Status == want.Status && got.Header == want.Header && !volatile[key] {
			// Only the body differs: a body that changes between identical garbage requests is volatile.
			if again := rigs[p.rig].serve(p.listener, qGateRequest(p.row, carrier.Baseline())); again.answer.Body != want.Body {
				volatile[key] = true
			}
		}
		if volatile[key] {
			return got.Status == want.Status && got.Header == want.Header
		}
		return got == want
	}
	for _, p := range places {
		profile := p.row.Method + " " + p.row.Route
		for _, carrier := range carriers[p.rig] {
			if p.row.Accepts(carrier.Class) {
				continue
			}
			census.WrongCells++
			want := baselineOf(p, carrier)
			got := rigs[p.rig].serve(p.listener, qGateRequest(p.row, carrier))
			if !sameAnswer(p, carrier, got.answer, want.answer) {
				errorf("%s @%s: wrong class %s answered %s, a garbage credential in its carrier answers %s", p.row.ID, p.listener, carrier.Class, got.answer, want.answer)
			}
			if got.keys != 0 || got.stores != 0 {
				errorf("%s @%s: wrong class %s made %d key reads and %d store reads (rig %s)", p.row.ID, p.listener, carrier.Class, got.keys, got.stores, p.rig)
			}
			if got.decision != 0 {
				errorf("%s @%s: wrong class %s made the shadow ask the PDP %d times", p.row.ID, p.listener, carrier.Class, got.decision)
			}
			if p.row.Protected() && got.answer.Status/100 != 4 {
				if owner, known := excusedBy(divergences, p.row); known {
					divergent[profile]++
					t.Logf("%s @%s: wrong class %s on a protected row answered %s: known divergence, ledger %s", p.row.ID, p.listener, carrier.Class, got.answer, owner)
				} else {
					errorf("%s @%s: wrong class %s on a protected row answered %s, want a 4xx refusal", p.row.ID, p.listener, carrier.Class, got.answer)
				}
			}
		}
	}
	// The accept controls run after every wrong-class cell: a valid user access token is told apart from a refusal.
	// On A' its JWKS fetch cannot complete (the issuer's TLS is not trusted in this test binary), so it is told apart by
	// its answer and by its key fetch; on B by its store read or its answer.
	acceptCells, fetched, controlled, shadowAsked := map[string]int{}, map[string]bool{}, map[string]bool{}, 0
	for _, p := range places {
		if !p.row.Accepts(classUserToken) {
			continue
		}
		census.AcceptCells++
		acceptCells[p.rig]++
		control := accept[p.rig]
		want := baselineOf(p, control)
		got := rigs[p.rig].serve(p.listener, qGateRequest(p.row, control))
		shadowAsked += got.decision
		switch p.rig {
		case rigA:
			builder := a.mounted[p.row.Method+" "+p.row.Route]
			controlled[builder] = true
			if got.keys > 0 {
				fetched[builder] = true
			}
			if sameAnswer(p, control, got.answer, want.answer) {
				errorf("%s @%s: accepted class %s answered %s, the garbage answer: the rig cannot tell accept from refuse", p.row.ID, p.listener, control.Class, got.answer)
			}
		case rigB:
			if got.stores == 0 {
				errorf("%s @%s: accepted class %s answered %s with no store read: the rig cannot tell accept from refuse", p.row.ID, p.listener, control.Class, got.answer)
			}
		}
	}
	for builder := range controlled {
		if !fetched[builder] {
			errorf("A': no accept cell of %s fetched a key: the key-fetch count cannot see a wrong class there", builder)
		}
	}
	if shadowAsked == 0 {
		errorf("no accept cell made the shadow ask the PDP: the shadow seam is not live, so zero on a wrong cell proves nothing")
	}
	if acceptCells[rigA] == 0 || acceptCells[rigB] == 0 {
		errorf("accept controls: A' %d, B %d: a rig with no control cell is not measured", acceptCells[rigA], acceptCells[rigB])
	}
	for profile, owner := range divergences {
		if divergent[profile] == 0 {
			errorf("ledger %s names %s, whose wrong-class cells all answer 4xx (or no such row): remove the ledger row", owner, profile)
		}
	}
	if census.WrongCells == 0 {
		errorf("no wrong-class cell ran")
	}
	for key := range volatile {
		rigLines = append(rigLines, "volatile body, compared on status and headers: "+key)
	}
	return census, rigLines
}

// excusedBy is the ledger row that excuses the row's G3 cells: only a row the ledger names by method and route.
func excusedBy(divergences map[string]string, row wrongclass.Row) (string, bool) {
	owner, known := divergences[row.Method+" "+row.Route]
	return owner, known
}

func TestWrongClassGateQueryAPIExcusalIsExact(t *testing.T) {
	named := map[string]string{"GET /api/v1/home": "probe (CHAOS-0)"}
	for _, c := range []struct {
		row  wrongclass.Row
		want bool
	}{
		{wrongclass.Row{Method: "GET", Route: "/api/v1/home"}, true},
		{wrongclass.Row{Method: "POST", Route: "/api/v1/home"}, false},
		{wrongclass.Row{Method: "GET", Route: "/api/v1/home/x"}, false},
		{wrongclass.Row{Method: "GET", Route: "/api/v1/meta"}, false},
	} {
		if _, got := excusedBy(named, c.row); got != c.want {
			t.Errorf("%s %s: excused %v, want %v", c.row.Method, c.row.Route, got, c.want)
		}
	}
}

func TestWrongClassGateQueryAPI(t *testing.T) {
	census, rigLines := runQueryAPIGate(t, qGateDivergences(t), t.Errorf)
	a, b := 0, 0
	for _, line := range rigLines {
		switch {
		case strings.Contains(line, " -> "+rigA+" "):
			a++
		case strings.Contains(line, " -> "+rigB+" "):
			b++
		}
		t.Log("RIG " + line)
	}
	if a+b != census.Rows {
		t.Errorf("rig census: %d rows on A', %d on B, of %d rows", a, b, census.Rows)
	}
	t.Logf("%s rig_a=%d rig_b=%d", census.Line(), a, b)
}

// A ledger row that names a row whose wrong-class cells all answer 4xx fails the gate.
func TestWrongClassGateQueryAPIFailsOnAStaleDivergence(t *testing.T) {
	const stale = "GET /api/v1/home"
	divergences := qGateDivergences(t)
	if _, ok := divergences[stale]; ok {
		t.Fatalf("the ledger names %s", stale)
	}
	divergences[stale] = "stale-probe (CHAOS-0)"
	var failures []string
	runQueryAPIGate(t, divergences, func(format string, args ...any) { failures = append(failures, fmt.Sprintf(format, args...)) })
	want := "ledger stale-probe (CHAOS-0) names " + stale + ", whose wrong-class cells all answer 4xx (or no such row): remove the ledger row"
	if len(failures) != 1 || failures[0] != want {
		t.Fatalf("failures %q, want exactly %q", failures, want)
	}
}

// With no ledger row, every G3 cell of the rows the ledger names fails, and nothing else does.
func TestWrongClassGateQueryAPIExcusesOnlyTheNamedRows(t *testing.T) {
	named := qGateDivergences(t)
	var failures []string
	runQueryAPIGate(t, map[string]string{}, func(format string, args ...any) { failures = append(failures, fmt.Sprintf(format, args...)) })
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

// The A' rig mounts BuildWithLookup's REST route set: every restGroups mount (RESTRoutes) is on A' or named on B with
// the builder error, and every query-api profile row under /api/v1/ is a RESTRoutes route or the catch-all.
// BuildWithLookup's own mount lines are pinned by source: a change there must change the rig with it.
func TestWrongClassGateQueryAPIRigMatchesBuild(t *testing.T) {
	a := newRigA(t)
	routes := map[string]bool{}
	for _, route := range RESTRoutes() {
		key := route.Method + " " + route.Pattern
		routes[key] = true
		if a.mounted[key] == "" && a.unbuilt[key] == "" {
			t.Errorf("%s: a RESTRoutes route the A' rig neither mounts nor names", key)
		}
	}
	if len(routes) == 0 {
		t.Fatal("RESTRoutes is empty")
	}
	for key := range a.mounted {
		if !routes[key] {
			t.Errorf("%s: mounted by the A' rig but not a RESTRoutes route", key)
		}
	}
	rows, err := wrongclass.LoadRows(authv1.QueryAPIEndpointProfiles)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if strings.HasPrefix(row.Route, "/api/v1/") && row.Method != "api_route" && !routes[row.Method+" "+row.Route] {
			t.Errorf("%s: a /api/v1/ row that is no RESTRoutes route", row.ID)
		}
	}
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.ReadFile(filepath.Join(root, "internal/queryapi/server/server.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"for _, group := range restGroups {",
		"handlers, cleanup, ok, buildErr := group.Build(getenv, edgeUsers)",
		"mux.HandleFunc(mount.Pattern, withProofProvenance(handlers[i], runningBuild()))",
		`mux.HandleFunc("/api/v1/", apiV1CatchAllNotFound)`,
		"handler, internal, mcp, gate := gatedListenerHandlers(lookup, mux, internalMux, mcpMux)",
	} {
		if strings.Count(string(source), line) != 1 {
			t.Errorf("server.go no longer holds %q once: the A' rig copies BuildWithLookup's REST mount and must follow it", line)
		}
	}
}

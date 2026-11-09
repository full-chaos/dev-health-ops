//go:build integration

package authruntime

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	goast "go/ast"
	"go/parser"
	gotoken "go/token"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/vektah/gqlparser/v2"
	gqlast "github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-13311 (F9 L5): query-api answers every caller class the same bytes with the PDP enforce config on (E) as
// with the gate off (O) and with the rollback config (R, shadow mode, both cell lists empty). Each config is one real
// query-api Build over one PostgreSQL (go-api's tables and the auth schema) and one ClickHouse (the real migration
// chain). The PDP is the auth-service stack configure builds (authorize routes, decision state, live catalog), served
// by httptest: query-api reaches it through its production settings, AUTH_PROXY_URL and a workload bootstrap
// credential it exchanges for its workload token. A pass-through wrapper counts the authorize calls of each build.

const (
	qpEnforceCells     = "analytics,metrics,git,work_items,integrations,reports"
	qpEnvelopeIssuer   = "dev-health-ops-edge"
	qpEnvelopeAudience = "query-api"
	qpEnvelopeKID      = "parity-envelope-1"
	qpWorkloadName     = "query-api-parity"
	// qpTokenUses is how many requests one signed user access token carries before it is re-signed: a class sends a
	// few hundred requests over a run of minutes, so 100 keeps every token far inside its 600 s life.
	qpTokenUses = 100
)

// qpConfigs are the three builds, E first: its PDP count is read on every cell.
var qpConfigs = []struct{ name, mode, userCells string }{
	{"E", pdp.ModeEnforce, qpEnforceCells},
	{"O", pdp.ModeOff, ""},
	{"R", pdp.ModeShadow, ""},
}

// knownStricterRow is a cell where the enforce build answers stricter than legacy, accepted under its ticket. The
// cell first holds E to exactly enforced, and O and R to the legacy status, then skips through the ledger
// (internal/testsupport/venueoracle/testdata/known_divergences.tsv: ticket, TestQueryAPIEnforceParity, the row).
type knownStricterRow struct {
	ticket string // "CHAOS-n: why"
	// legacyLabel and enforcedLabel are the skip line's python= and go= answers (no blanks or colons).
	legacyLabel, enforcedLabel string
	legacy, enforced           qpAnswer
}

// knownStricter is keyed by the cell path under the top test (class/plane/namespace/root, spaces as _).
var knownStricter = map[string]knownStricterRow{}

func TestQueryAPIEnforceParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()
	v := newQPVenue(t, ctx)
	ops := qpGraphQLOperations(t, v.orgA.String())
	planes := []struct {
		name string
		rows []qpRow
	}{
		{"rest", v.restRows(t)},
		{"ingress", v.ingressRows(t, ops.ingress)},
		{"graphql", v.graphQLRows(ops, "graphql")},
		{"query", v.graphQLRows(ops, "query")},
		{"mcp", v.mcpRows(ops)},
	}
	for _, class := range v.classes(t, ctx) {
		t.Run(class.name, func(t *testing.T) {
			for _, plane := range planes {
				t.Run(plane.name, func(t *testing.T) {
					byNamespace := map[string][]qpRow{}
					for i, row := range plane.rows {
						// The retired classes (HS256, the envelope) get one generic-401 cell per plane, and /buildinfo.
						if !class.retired() || i == 0 || row.buildInfo {
							byNamespace[row.namespace] = append(byNamespace[row.namespace], row)
						}
					}
					for _, namespace := range qpSortedKeys(byNamespace) {
						t.Run(namespace, func(t *testing.T) {
							for _, row := range byNamespace[namespace] {
								t.Run(row.name, func(t *testing.T) { v.cell(t, class, row) })
							}
						})
					}
				})
			}
		})
	}
	v.checkServedCatalog(t)
}

// qpVenue is the auth-service stack and the three query-api builds over it.
type qpVenue struct {
	az     *azRouterEnv
	issuer string
	orgA   uuid.UUID
	member uuid.UUID
	builds []*qpBuild
	client *http.Client
	envKey ed25519.PrivateKey
}

// qpBuild is one query-api Build: its three listeners and the counter in front of its PDP.
type qpBuild struct {
	name                  string
	public, internal, mcp string
	pdp                   *qpCounter
}

func newQPVenue(t *testing.T, ctx context.Context) *qpVenue {
	t.Helper()
	qpQuietLogs(t)
	// The issuer is the https origin query-api fetches the access-token JWKS from, so its listener exists (port known)
	// before configure reads AUTH_TOKEN_ISSUER_URL.
	issuerFront := httptest.NewUnstartedServer(nil)
	issuer := "https://" + issuerFront.Listener.Addr().String()
	workloadID := uuid.New()
	az := newAuthorizeRouterEnv(t, ctx, map[string]string{
		authconfig.EnvTokenIssuerURL:          issuer,
		authconfig.EnvAuthorizeCallerSubjects: session.WirePrincipalID(workloadID),
		authconfig.EnvWorkloadBootstrap:       "true",
	})
	az.caller = session.WirePrincipalID(workloadID)
	api := az.front.Config.Handler
	issuerFront.Config.Handler = api
	issuerFront.StartTLS()
	t.Cleanup(issuerFront.Close)
	qpTrust(t, issuerFront.Certificate())

	v := &qpVenue{az: az, issuer: issuer, client: &http.Client{Timeout: 60 * time.Second}}
	v.orgA = az.azOrg(t, ctx, true, true)
	bootstrap := v.seedWorkload(t, ctx, workloadID)
	for _, root := range mcpclass.SortedRoots() {
		pgseed.ClassDecision(ctx, t, az.admin, mcpclass.Operation(root), "primary")
	}
	jwksPath := v.envelopeJWKS(t)

	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	chschema.Apply(ctx, t, ch)

	base := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_REGISTRY_POSTGRES_URI": az.admin.Config().ConnString(),
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       qpEnvelopeIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     qpEnvelopeAudience,
		"DEV_HEALTH_ENV":               "ci",
		"AUTH_ACCESS_TOKEN_ISSUER":     issuer,
		workloadinput.EnvIssuer:        issuer,
		workloadinput.EnvBootstrapFile: bootstrap,
	}
	for _, name := range qpRouteSwitches(t) {
		base[name] = "true"
	}
	for _, config := range qpConfigs {
		counter := &qpCounter{next: api, revisions: map[int]int{}}
		proxy := httptest.NewServer(counter)
		t.Cleanup(proxy.Close)
		settings := qpMerge(base, map[string]string{
			pdp.EnvMode:                        config.mode,
			pdp.EnvAuthProxyURL:                proxy.URL,
			authzenforce.QueryAPI.Env():        config.userCells,
			authzenforce.QueryAPIHeaders.Env(): "",
		})
		v.builds = append(v.builds, v.build(t, ctx, config.name, settings, counter))
	}
	return v
}

// build is one query-api Build and its public, internal and MCP listeners, started on loopback ports.
func (v *qpVenue) build(t *testing.T, ctx context.Context, name string, settings map[string]string, counter *qpCounter) *qpBuild {
	t.Helper()
	plane, err := server.Build(func(key string) string { return settings[key] })
	if err != nil {
		t.Fatalf("build %s: %v", name, err)
	}
	t.Cleanup(plane.Close)
	public, internal := server.Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, nil)
	mcp := server.MCPListener("127.0.0.1:0", plane, nil)
	b := &qpBuild{name: name, pdp: counter}
	for _, l := range []struct {
		listener *server.Listener
		into     *string
	}{{public, &b.public}, {internal, &b.internal}, {mcp, &b.mcp}} {
		if l.listener == nil {
			t.Fatalf("build %s: a listener is missing", name)
		}
		if err := l.listener.Start(ctx); err != nil {
			t.Fatalf("build %s: %v", name, err)
		}
		listener := l.listener
		t.Cleanup(func() { _ = listener.Shutdown(context.Background()) })
		*l.into = "http://" + listener.Address()
	}
	return b
}

// seedWorkload registers query-api's workload principal with the id configure names as the authorize caller, gives
// it a bootstrap credential through the workload store, and returns the 0600 file query-api reads it from. The
// principal and workload rows are written as Register writes them (the id must exist before configure runs).
func (v *qpVenue) seedWorkload(t *testing.T, ctx context.Context, id uuid.UUID) string {
	t.Helper()
	v.az.armed(t, ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', $2)`, id, qpWorkloadName)
	v.az.armed(t, ctx, `INSERT INTO auth.workloads (principal_id, name, audiences, allowed_actions) VALUES ($1, $2, $3, $4)`,
		id, qpWorkloadName, []string{authprovision.WorkloadAudience}, []string{})
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := workload.New(workload.Config{Pool: v.az.admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	value, err := workload.MintBootstrap()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateBootstrap(ctx, qpWorkloadName, value, []string{authprovision.WorkloadAudience}, time.Hour, workload.Actor{}); err != nil {
		t.Fatalf("create the bootstrap credential: %v", err)
	}
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// envelopeJWKS writes the envelope verifier's key set and keeps its private key for the envelope class.
func (v *qpVenue) envelopeJWKS(t *testing.T) string {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	v.envKey = priv
	doc, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub), "kid": qpEnvelopeKID, "use": "sig", "alg": "EdDSA",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "envelope-jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// qpTrust makes the test process trust the issuer listener's certificate: query-api's JWKS client uses the default
// transport, and prod trusts the issuer's real certificate the same way (the system pool).
func qpTrust(t *testing.T, cert *x509.Certificate) {
	t.Helper()
	original := http.DefaultTransport
	base, ok := original.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is a %T", original)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		pool = x509.NewCertPool()
	}
	pool.AddCert(cert)
	trusting := base.Clone()
	trusting.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	http.DefaultTransport = trusting
	t.Cleanup(func() { http.DefaultTransport = original })
}

// qpQuietLogs sends query-api's per-request log lines to a bounded tail, printed only when the test fails.
func qpQuietLogs(t *testing.T) {
	t.Helper()
	tail := &qpTail{}
	previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewTextHandler(tail, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() {
		slog.SetDefault(previous)
		log.SetOutput(writer)
		log.SetFlags(flags)
		if t.Failed() {
			t.Logf("query-api log tail:\n%s", tail.String())
		}
	})
}

type qpTail struct {
	mu  sync.Mutex
	buf []byte
}

func (b *qpTail) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if over := len(b.buf) - 64<<10; over > 0 {
		b.buf = b.buf[over:]
	}
	return len(p), nil
}

func (b *qpTail) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}

// qpRouteSwitches are query-api's GO_API_*_ENABLED route switches, read from its source as
// build_routes_integration_test.go reads them, so a new REST route is mounted here too.
func qpRouteSwitches(t *testing.T) []string {
	t.Helper()
	dir := filepath.Join(qpRepoRoot(t), "internal", "queryapi", "server")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	pattern := regexp.MustCompile(`"(GO_API_[A-Z_]+_ENABLED)"`)
	found := map[string]bool{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range pattern.FindAllSubmatch(raw, -1) {
			found[string(match[1])] = true
		}
	}
	if len(found) < 15 {
		t.Fatalf("found %d route switches, want at least 15", len(found))
	}
	return qpSortedKeys(found)
}

func qpRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := contractsRoot()
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// qpCounter passes every request to auth-service and counts the authorize calls (single and batch) with the
// policy_revision each decision names. It never answers itself.
type qpCounter struct {
	next      http.Handler
	mu        sync.Mutex
	asks      int
	revisions map[int]int
}

func (c *qpCounter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost || (r.URL.Path != authapi.PatternAuthorize && r.URL.Path != authapi.PatternAuthorizeBatch) {
		c.next.ServeHTTP(w, r)
		return
	}
	tee := &qpTee{ResponseWriter: w}
	c.next.ServeHTTP(tee, r)
	var document any
	_ = json.Unmarshal(tee.body.Bytes(), &document)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.asks++
	qpRevisions(document, c.revisions)
}

func (c *qpCounter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.asks
}

func (c *qpCounter) snapshot() (int, map[int]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	served := make(map[int]int, len(c.revisions))
	for rev, n := range c.revisions {
		served[rev] = n
	}
	return c.asks, served
}

// qpRevisions collects every policy_revision member of a decision document (a batch holds one per item).
func qpRevisions(node any, into map[int]int) {
	switch n := node.(type) {
	case map[string]any:
		for key, value := range n {
			if number, ok := value.(float64); ok && key == "policy_revision" {
				into[int(number)]++
				continue
			}
			qpRevisions(value, into)
		}
	case []any:
		for _, item := range n {
			qpRevisions(item, into)
		}
	}
}

type qpTee struct {
	http.ResponseWriter
	body bytes.Buffer
}

func (w *qpTee) Write(p []byte) (int, error) {
	w.body.Write(p)
	return w.ResponseWriter.Write(p)
}

// checkServedCatalog holds every decision auth-service served to the catalog version of the policy catalog
// package, and requires that the enforce build asked at all.
func (v *qpVenue) checkServedCatalog(t *testing.T) {
	t.Helper()
	want := policycatalog.Default().Version()
	for i, b := range v.builds {
		asks, served := b.pdp.snapshot()
		t.Logf("build %s asked the PDP %d times; policy_revision served: %v (catalog_version %d)", b.name, asks, served, want)
		for rev := range served {
			if rev != want {
				t.Errorf("build %s: auth-service served policy_revision %d, want the live catalog_version %d", b.name, rev, want)
			}
		}
		if i == 0 && served[want] == 0 {
			t.Errorf("the enforce build never got a decision at catalog_version %d: the matrix proves nothing about the PDP", want)
		}
	}
}

// qpClass is one caller class: how a request carries its credential.
type qpClass struct {
	name string
	// member is an active member of org A with a session: its E cell of a decided row must ask the PDP.
	member bool
	hs256  bool
	carry  func(r *http.Request)
}

// retired reports a class query-api no longer accepts: the HS256 token and the effective-principal envelope.
func (c qpClass) retired() bool { return c.hs256 || c.name == "envelope" }

func (v *qpVenue) classes(t *testing.T, ctx context.Context) []qpClass {
	t.Helper()
	a := v.az
	a.exec(t, ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('platform_admin', 'platform_admin', 'platform') ON CONFLICT (role_key) DO NOTHING`)
	org := v.orgA
	var classes []qpClass
	for _, role := range []string{"owner", "admin", "member", "viewer"} {
		user := a.azUser(t, ctx, false)
		a.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`,
			user, org, role)
		if role == "member" {
			v.member = user
		}
		classes = append(classes, qpClass{name: role, member: true, carry: v.bearer(t, ctx, user, &org)})
	}
	// Superusers: is_superuser on go-api's row, which is admin.orgs for query-api (AUTH_PLATFORM_ROLES_OWNED unset) and
	// platform_admin for the decision state (as the authorize corpus seeds it).
	superNoOrg, superInOrg, admin := a.azUser(t, ctx, true), a.azUser(t, ctx, true), a.azUser(t, ctx, true)
	outsider := a.azUser(t, ctx, false)
	// A superuser who is also a viewer or member of org A: the legacy guards that read admin.stats (dataHealth) admit
	// it whatever its membership role, so the decision must too.
	for _, role := range []string{"viewer", "member"} {
		user := a.azUser(t, ctx, true)
		a.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`,
			user, org, role)
		classes = append(classes, qpClass{name: "superuser_" + role, member: true, carry: v.bearer(t, ctx, user, &org)})
	}
	// Legacy impersonation: the admin's own token while an impersonation_sessions row names a member of org A.
	pgseed.ImpersonationSession(ctx, t, a.admin, uuid.NewString(), admin.String(), v.member.String(), org.String(), "member",
		time.Now().Add(2*time.Hour), nil)
	classes = append(classes,
		qpClass{name: "superuser_no_org", carry: v.bearer(t, ctx, superNoOrg, nil)},
		qpClass{name: "superuser_in_org", carry: v.bearer(t, ctx, superInOrg, &org)},
		qpClass{name: "outsider", carry: v.bearer(t, ctx, outsider, &org)},
		qpClass{name: "legacy_impersonation", carry: v.bearer(t, ctx, admin, nil)},
		qpClass{name: "hs256", hs256: true, carry: qpStaticBearer(qpHS256Token(t, v.member, org))},
		qpClass{name: "envelope", carry: func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+v.envelope(t)) }},
		qpClass{name: "headers", carry: func(r *http.Request) {
			r.Header.Set(internalidentity.HeaderOrgID, org.String())
			r.Header.Set(internalidentity.HeaderRole, "member")
			r.Header.Set(internalidentity.HeaderSuperuser, "false")
			r.Header.Set(internalidentity.HeaderImpersonationActive, "false")
		}},
	)
	return classes
}

func qpStaticBearer(token string) func(*http.Request) {
	return func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+token) }
}

// bearer is a user access token (access-token.v1) of a real session of user, signed by the stack's key for its
// issuer, with org as its org claim. It is re-signed for the same session every qpTokenUses requests (a count, not a
// clock: D6418), well inside its 10-minute life.
func (v *qpVenue) bearer(t *testing.T, ctx context.Context, user uuid.UUID, org *uuid.UUID) func(*http.Request) {
	t.Helper()
	issued, err := v.az.sessions.Create(ctx, user, org, []string{"pwd"})
	if err != nil {
		t.Fatalf("start a session: %v", err)
	}
	scope, ok := contracts.AccessTokenScope(contracts.PlatformAPIAudience)
	if !ok {
		t.Fatal("the platform API audience has no access token scope")
	}
	var mu sync.Mutex
	var token string
	var uses int
	return func(r *http.Request) {
		mu.Lock()
		if uses%qpTokenUses == 0 {
			now := time.Now()
			claims := contracts.AccessTokenClaims{Iss: v.issuer, Aud: contracts.PlatformAPIAudience, Sub: session.WirePrincipalID(user),
				PrincipalType: "human", Sid: session.WireSessionID(issued.Session.ID), Jti: uuid.NewString(),
				Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 600, Scope: scope}
			if org != nil {
				id := org.String()
				claims.OrgID = &id
			}
			token = v.az.sign(t, "at+jwt", claims)
		}
		uses++
		current := token
		mu.Unlock()
		r.Header.Set("Authorization", "Bearer "+current)
	}
}

// envelope is a fresh effective-principal envelope of the member in org A (a retired class: query-api refuses it).
func (v *qpVenue) envelope(t *testing.T) string {
	t.Helper()
	claims := principal.Claims{SchemaVersion: principal.SupportedSchemaVersion, OrgID: v.orgA.String(), Role: "member",
		RegisteredClaims: jwt.RegisteredClaims{Issuer: qpEnvelopeIssuer, Audience: jwt.ClaimStrings{qpEnvelopeAudience},
			Subject: v.member.String(), ID: uuid.NewString(), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}}
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = qpEnvelopeKID
	signed, err := token.SignedString(v.envKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// qpHS256Token is the retired HS256 edge token's shape: no verifier accepts it any more.
func qpHS256Token(t *testing.T, user, org uuid.UUID) string {
	t.Helper()
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	payload, err := json.Marshal(map[string]any{"sub": user.String(), "org_id": org.String(), "role": "member", "type": "access",
		"exp": time.Now().Add(time.Hour).Unix()})
	if err != nil {
		t.Fatal(err)
	}
	input := header + "." + enc.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte("retired-hs256-edge-secret-not-a-credential"))
	mac.Write([]byte(input))
	return input + "." + enc.EncodeToString(mac.Sum(nil))
}

// qpRow is one request every class sends to the three builds.
type qpRow struct {
	namespace, name string
	listener        string // public, internal or mcp
	method, target  string
	body            []byte
	// askPDP: a member's E cell asks the PDP at least once (a decided row on a plane that reaches the gate).
	askPDP bool
	// buildInfo: the E cell never asks the PDP (GET /buildinfo is never decided).
	buildInfo bool
	// volatile: the answer names an id or a time minted per call; those are masked before the compare.
	volatile bool
	// clock: the answer names the time it was computed at; only the times are masked.
	clock bool
}

// decided reports whether action is decided under E: organization-scoped and its namespace cell on.
func qpDecided(action string) bool {
	catalogued, err := policycatalog.Default().Action(action)
	if err != nil || catalogued.ResourceKind != policycatalog.ScopeOrganization {
		return false
	}
	namespace, _, _ := strings.Cut(action, ".")
	return slices.Contains(strings.Split(qpEnforceCells, ","), namespace)
}

func qpNamespace(action string) string {
	namespace, _, _ := strings.Cut(action, ".")
	return namespace
}

var qpPathParam = regexp.MustCompile(`\{[a-z_]+\}`)

// qpRowName is a row's leaf name: the method and path with "/" as "." (a "/" would read as one more level).
func qpRowName(method, path string) string {
	return method + " " + strings.ReplaceAll(strings.TrimPrefix(path, "/"), "/", ".")
}

// restRows are the /api/v1 rows of query-api's route action table, on the public listener.
func (v *qpVenue) restRows(t *testing.T) []qpRow {
	t.Helper()
	var rows []qpRow
	for key, action := range server.RouteActionTable() {
		method, rest, _ := strings.Cut(key, " ")
		route, listeners, _ := strings.Cut(rest, " @")
		if !strings.HasPrefix(route, "/api/v1/") || !strings.Contains(listeners, "query") {
			continue
		}
		row := qpRow{namespace: qpNamespace(action), name: qpRowName(method, route), listener: "public", method: method,
			target: qpPathParam.ReplaceAllString(route, "00000000-0000-4000-8000-000000000001"), askPDP: qpDecided(action)}
		if method == http.MethodPost {
			row.body = []byte(`{}`)
		}
		rows = append(rows, row)
	}
	if len(rows) < 29 {
		t.Fatalf("found %d /api/v1 rows, want at least 29", len(rows))
	}
	qpSortRows(rows)
	return rows
}

// ingressRows are the other rows of the route action table: the GraphQL ingresses (one registered document) and
// GET /buildinfo. A row this table does not know fails, so a new ingress gets a cell.
func (v *qpVenue) ingressRows(t *testing.T, doc qpOperation) []qpRow {
	t.Helper()
	body := doc.body(t)
	var rows []qpRow
	for key, action := range server.RouteActionTable() {
		method, rest, _ := strings.Cut(key, " ")
		route, listeners, _ := strings.Cut(rest, " @")
		if strings.HasPrefix(route, "/api/v1/") {
			continue
		}
		row := qpRow{namespace: qpNamespace(action), name: qpRowName(method, route) + " @" + listeners, method: method, target: route}
		switch {
		case listeners == "mcp":
			row.listener = "mcp"
		case route == "/graphql":
			row.listener = "public"
		default:
			row.listener = "internal"
		}
		switch {
		case route == "/buildinfo" && method == http.MethodGet:
			row.buildInfo = true
		case route == "/graphql" && method == http.MethodGet:
			row.target = route + "?" + url.Values{"query": {doc.document}, "variables": {string(doc.variablesJSON(t))}}.Encode()
		case (route == "/graphql" || strings.HasPrefix(route, "/query")) && method == http.MethodPost:
			row.body = body
		default:
			t.Errorf("ingress row %q has no request shape in this test: add one", key)
			continue
		}
		// Only the product path and /query hand the bearer to the decision; the class-gated, proof and MCP routes
		// are compared without the ask check.
		if (route == "/graphql" || (route == "/query" && listeners != "mcp")) && doc.registered {
			row.askPDP = qpDecided(action)
		}
		rows = append(rows, row)
	}
	qpSortRows(rows)
	return rows
}

// graphQLRows are one operation per GraphQL root plus the mixed one, on /graphql (public) or /query (internal).
func (v *qpVenue) graphQLRows(ops qpOperations, plane string) []qpRow {
	listener, target := "public", "/graphql"
	if plane == "query" {
		listener, target = "internal", "/query"
	}
	var rows []qpRow
	for _, op := range append(slices.Clone(ops.roots), ops.mixed) {
		rows = append(rows, qpRow{namespace: op.namespace, name: op.key, listener: listener, method: http.MethodPost, target: target,
			body: op.bodyBytes, askPDP: op.registered && qpDecided(op.action), volatile: qpVolatileRoots[op.key], clock: qpClockRoots[op.key]})
	}
	return rows
}

// mcpRows are one operation per action namespace (an MCP root where one serves it; the admin namespace is the
// platform dashboard) plus the mixed one, on the MCP listener.
func (v *qpVenue) mcpRows(ops qpOperations) []qpRow {
	allowed := mcpclass.AllowedRoots()
	chosen := map[string]qpOperation{}
	for _, op := range ops.roots {
		_, field, _ := strings.Cut(op.key, ".")
		current, seen := chosen[op.namespace]
		if !seen || (allowed[field] && !allowed[strings.SplitN(current.key, ".", 2)[1]]) {
			chosen[op.namespace] = op
		}
	}
	var rows []qpRow
	for _, namespace := range qpSortedKeys(chosen) {
		op := chosen[namespace]
		rows = append(rows, qpRow{namespace: namespace, name: op.key, listener: "mcp", method: http.MethodPost, target: "/query", body: op.bodyBytes})
	}
	return append(rows, qpRow{namespace: ops.mixed.namespace, name: ops.mixed.key, listener: "mcp", method: http.MethodPost, target: "/query",
		body: ops.mixed.bodyBytes})
}

// qpVolatileRoots answer an id the call itself mints (a new saved report): masked before the compare.
var qpVolatileRoots = map[string]bool{"Mutation.createSavedReport": true}

// qpClockRoots answer the time they were computed at (generatedAt, computedAt): only the times are masked.
var qpClockRoots = map[string]bool{"Query.compoundingRisk": true, "Query.throughputForecast": true}

func qpSortRows(rows []qpRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].namespace != rows[j].namespace {
			return rows[i].namespace < rows[j].namespace
		}
		return rows[i].name < rows[j].name
	})
}

// qpAnswer is what a build answered: the compared bytes.
type qpAnswer struct {
	status      int
	contentType string
	body        []byte
}

func (a qpAnswer) String() string {
	body := a.body
	if len(body) > 2048 {
		body = append(slices.Clone(body[:2048]), "..."...)
	}
	return fmt.Sprintf("%d %q %s", a.status, a.contentType, body)
}

func (a qpAnswer) equal(b qpAnswer) bool {
	return a.status == b.status && a.contentType == b.contentType && bytes.Equal(a.body, b.body)
}

var (
	qpUUID = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	qpTime = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?`)
)

func (a qpAnswer) masked() qpAnswer {
	a.body = qpTime.ReplaceAll(qpUUID.ReplaceAll(a.body, []byte("<uuid>")), []byte("<time>"))
	return a
}

// send is one request of class to build b.
func (v *qpVenue) send(t *testing.T, b *qpBuild, class qpClass, row qpRow, requestID string) qpAnswer {
	t.Helper()
	base := map[string]string{"public": b.public, "internal": b.internal, "mcp": b.mcp}[row.listener]
	var body io.Reader
	if row.body != nil {
		body = bytes.NewReader(row.body)
	}
	request, err := http.NewRequest(row.method, base+row.target, body)
	if err != nil {
		t.Fatal(err)
	}
	if row.body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("X-Request-ID", requestID)
	class.carry(request)
	response, err := v.client.Do(request)
	if err != nil {
		t.Fatalf("%s %s on build %s: %v", row.method, row.target, b.name, err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read the answer of build %s: %v", b.name, err)
	}
	answer := qpAnswer{status: response.StatusCode, contentType: response.Header.Get("Content-Type"), body: raw}
	switch {
	case row.volatile:
		answer = answer.masked()
	case row.clock:
		answer.body = qpTime.ReplaceAll(answer.body, []byte("<time>"))
	}
	return answer
}

// triple sends the cell to E, O and R in that order and returns the answers and E's PDP asks.
func (v *qpVenue) triple(t *testing.T, class qpClass, row qpRow, requestID string) ([]qpAnswer, int) {
	t.Helper()
	answers := make([]qpAnswer, len(v.builds))
	asks := 0
	for i, b := range v.builds {
		before := b.pdp.count()
		answers[i] = v.send(t, b, class, row, requestID)
		if i == 0 {
			asks = b.pdp.count() - before
		}
	}
	return answers, asks
}

func (v *qpVenue) cell(t *testing.T, class qpClass, row qpRow) {
	t.Helper()
	name := strings.TrimPrefix(t.Name(), "TestQueryAPIEnforceParity/")
	requestID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(name)).String()
	answers, asks := v.triple(t, class, row, requestID)
	same := answers[0].equal(answers[1]) && answers[0].equal(answers[2])
	if !same {
		// A difference must repeat: a body that names the second it was served in may cross one between builds.
		answers, asks = v.triple(t, class, row, requestID)
		same = answers[0].equal(answers[1]) && answers[0].equal(answers[2])
	}
	if row.buildInfo && asks != 0 {
		t.Errorf("E asked the PDP %d times for GET /buildinfo: it must never be decided", asks)
	}
	// The zero-ask proof needs a request /buildinfo admits: a platform admin acting as itself with a membership row in
	// its token's org (the edge refuses a token org with no row) gets the build.
	if row.buildInfo && class.name == "superuser_member" && answers[0].status != http.StatusOK {
		t.Errorf("GET /buildinfo refused %s under E (%s): the zero-ask proof would hold for a refusal only", class.name, answers[0])
	}
	if row.askPDP && class.member && asks == 0 {
		t.Errorf("E answered a decided row without asking the PDP: %s", answers[0])
	}
	if class.retired() && row.listener != "mcp" && answers[0].status != http.StatusUnauthorized {
		t.Errorf("the retired %s class got %s, want the generic 401", class.name, answers[0])
	}
	known, isKnown := knownStricter[name]
	switch {
	case same && isKnown:
		t.Errorf("known stricter row %s now answers the same in E, O and R: remove it (%s)", name, known.ticket)
	case same:
	case isKnown:
		if !answers[0].equal(known.enforced) {
			t.Fatalf("E = %s, want the known enforced answer %s (%s)", answers[0], known.enforced, known.ticket)
		}
		if !answers[1].equal(known.legacy) || !answers[2].equal(known.legacy) {
			t.Fatalf("O = %s, R = %s, want the known legacy answer %s in both (%s)", answers[1], answers[2], known.legacy, known.ticket)
		}
		venueoracle.KnownDivergenceSkip(t, known.ticket, known.legacyLabel, known.enforcedLabel,
			fmt.Sprintf("E = %s; O = R = %s", answers[0], answers[1]))
	default:
		t.Errorf("E, O and R differ:\n  E: %s\n  O: %s\n  R: %s", answers[0], answers[1], answers[2])
	}
}

// qpOperation is one GraphQL operation: a registered document (the only kind /graphql and /query run) with
// variables built from its definitions.
type qpOperation struct {
	key, action, namespace string
	document               string
	variables              map[string]any
	registered             bool
	bodyBytes              []byte
}

func (o qpOperation) variablesJSON(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(o.variables)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func (o qpOperation) body(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"query": o.document, "variables": o.variables})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type qpOperations struct {
	roots   []qpOperation // one per FieldActionTable root, sorted by key
	mixed   qpOperation   // an analytics root and a metrics root in one operation
	ingress qpOperation   // the shortest registered analytics document an MCP class row also lights
}

// qpGraphQLOperations derives the operations at run time: the roots from graph.FieldActionTable, each root's
// document from query-api's registered documents (parsed from its source, as cmd/registrydump does). A root no
// registered document selects gets a built one, which /graphql and /query refuse as unregistered in every config.
func qpGraphQLOperations(t *testing.T, org string) qpOperations {
	t.Helper()
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	registered := qpRegisteredDocuments(t)
	byRoot := map[string][]string{} // root key -> registered operation names
	parsed := map[string]*gqlast.QueryDocument{}
	for _, name := range qpSortedKeys(registered) {
		doc, errs := gqlparser.LoadQuery(schema, registered[name])
		if errs != nil {
			t.Fatalf("registered document %s does not validate: %v", name, errs)
		}
		parsed[name] = doc
		for _, key := range qpRootKeys(doc) {
			byRoot[key] = append(byRoot[key], name)
		}
	}
	var out qpOperations
	allowed := mcpclass.AllowedRoots()
	table := graph.FieldActionTable()
	for _, key := range qpSortedKeys(table) {
		op := qpOperation{key: key, action: table[key], namespace: qpNamespace(table[key])}
		_, field, _ := strings.Cut(key, ".")
		if names := byRoot[key]; len(names) > 0 {
			chosen := names[0]
			if slices.Contains(names, field) {
				chosen = field
			}
			op.document, op.registered = registered[chosen], true
			op.variables = qpVariables(schema, parsed[chosen].Operations[0], org)
		} else {
			op.document, op.variables = qpBuiltDocument(t, schema, key, org)
		}
		op.bodyBytes = op.body(t)
		out.roots = append(out.roots, op)
		if op.registered && op.namespace == "analytics" && allowed[field] &&
			(out.ingress.document == "" || len(op.document) < len(out.ingress.document)) {
			out.ingress = op
		}
	}
	if out.ingress.document == "" {
		t.Fatal("no registered analytics document an MCP class row lights: the ingress rows have no document")
	}
	out.mixed = qpMixedOperation(t, schema, out.roots, parsed, byRoot, org)
	return out
}

// qpRootKeys are the root field keys ("Query.x", "Mutation.x") of a document's operations.
func qpRootKeys(doc *gqlast.QueryDocument) []string {
	var keys []string
	for _, op := range doc.Operations {
		prefix := "Query."
		if op.Operation == gqlast.Mutation {
			prefix = "Mutation."
		}
		for _, selection := range op.SelectionSet {
			if field, ok := selection.(*gqlast.Field); ok && field.Name != "__typename" {
				keys = append(keys, prefix+field.Name)
			}
		}
	}
	return keys
}

// qpRegisteredDocuments maps each operation of query-api's digestByOperation to its registered document text.
func qpRegisteredDocuments(t *testing.T) map[string]string {
	t.Helper()
	path := filepath.Join(qpRepoRoot(t), "internal", "queryapi", "server", "query_route.go")
	file, err := parser.ParseFile(gotoken.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	texts, constOf := map[string]string{}, map[string]string{}
	goast.Inspect(file, func(node goast.Node) bool {
		switch n := node.(type) {
		case *goast.ValueSpec:
			for i, ident := range n.Names {
				if lit, ok := qpValueAt(n.Values, i).(*goast.BasicLit); ok && strings.HasPrefix(ident.Name, "registered") {
					if text, err := strconv.Unquote(lit.Value); err == nil {
						texts[ident.Name] = text
					}
				}
			}
		case *goast.AssignStmt:
			if ident, ok := n.Lhs[0].(*goast.Ident); ok && ident.Name == "digestByOperation" && len(n.Rhs) == 1 {
				literal, _ := n.Rhs[0].(*goast.CompositeLit)
				for _, element := range qpElements(literal) {
					pair, _ := element.(*goast.KeyValueExpr)
					key, _ := pair.Key.(*goast.BasicLit)
					call, _ := pair.Value.(*goast.CallExpr)
					if key == nil || call == nil || len(call.Args) != 1 {
						continue
					}
					if arg, ok := call.Args[0].(*goast.Ident); ok {
						name, _ := strconv.Unquote(key.Value)
						constOf[name] = arg.Name
					}
				}
			}
		}
		return true
	})
	out := map[string]string{}
	for operation, constant := range constOf {
		text, ok := texts[constant]
		if !ok {
			t.Fatalf("operation %s names %s, which is no registered document literal", operation, constant)
		}
		out[operation] = text
	}
	if len(out) < 50 {
		t.Fatalf("read %d registered documents from %s, want at least 50", len(out), path)
	}
	return out
}

func qpValueAt(values []goast.Expr, i int) goast.Expr {
	if i < len(values) {
		return values[i]
	}
	return nil
}

func qpElements(literal *goast.CompositeLit) []goast.Expr {
	if literal == nil {
		return nil
	}
	return literal.Elts
}

// qpVariables builds a value for every required variable of op, from the schema: org A for orgId, a date window for
// the date scalars, the first enum value, and each required field of an input object.
func qpVariables(schema *gqlast.Schema, op *gqlast.OperationDefinition, org string) map[string]any {
	out := map[string]any{}
	for _, definition := range op.VariableDefinitions {
		if definition.Type.NonNull && definition.DefaultValue == nil {
			out[definition.Variable] = qpValue(schema, definition.Variable, definition.Type, org)
		}
	}
	return out
}

var qpLaterBound = regexp.MustCompile(`(?i)(until|end|to)$`)

func qpValue(schema *gqlast.Schema, name string, typ *gqlast.Type, org string) any {
	if typ.Elem != nil {
		return []any{qpValue(schema, name, typ.Elem, org)}
	}
	definition := schema.Types[typ.NamedType]
	switch {
	case definition != nil && definition.Kind == gqlast.Enum:
		return definition.EnumValues[0].Name
	case definition != nil && definition.Kind == gqlast.InputObject:
		fields := map[string]any{}
		for _, field := range definition.Fields {
			if field.Type.NonNull && field.DefaultValue == nil {
				fields[field.Name] = qpValue(schema, field.Name, field.Type, org)
			}
		}
		return fields
	}
	later := qpLaterBound.MatchString(name)
	switch typ.NamedType {
	case "Int":
		return 1
	case "Float":
		return 1.0
	case "Boolean":
		return false
	case "Date":
		if later {
			return "2026-09-08"
		}
		return "2026-09-01"
	case "DateTime":
		if later {
			return "2026-09-08T00:00:00Z"
		}
		return "2026-09-01T00:00:00Z"
	}
	if name == "orgId" {
		return org
	}
	return "parity"
}

// qpBuiltDocument is an operation over one root no registered document selects: every required argument as a
// variable, __typename as the selection.
func qpBuiltDocument(t *testing.T, schema *gqlast.Schema, key, org string) (string, map[string]any) {
	t.Helper()
	kind, field, _ := strings.Cut(key, ".")
	parent := schema.Query
	if kind == "Mutation" {
		parent = schema.Mutation
	}
	definition := parent.Fields.ForName(field)
	if definition == nil {
		t.Fatalf("%s is in the field action table but not in the schema", key)
	}
	var params, args []string
	variables := map[string]any{}
	for _, arg := range definition.Arguments {
		if arg.Type.NonNull && arg.DefaultValue == nil {
			params = append(params, "$"+arg.Name+": "+arg.Type.String())
			args = append(args, arg.Name+": $"+arg.Name)
			variables[arg.Name] = qpValue(schema, arg.Name, arg.Type, org)
		}
	}
	text := strings.ToLower(kind) + " Parity"
	if len(params) > 0 {
		text += "(" + strings.Join(params, ", ") + ")"
	}
	text += " { " + field
	if len(args) > 0 {
		text += "(" + strings.Join(args, ", ") + ")"
	}
	if target := schema.Types[definition.Type.Name()]; target != nil && (target.Kind == gqlast.Object || target.Kind == gqlast.Interface || target.Kind == gqlast.Union) {
		text += " { __typename }"
	}
	return text + " }", variables
}

// qpMixedOperation joins the registered documents of one analytics root and one metrics root (MCP roots first) into
// one operation. No registered document mixes the two namespaces, so /graphql and /query answer it unregistered; the
// MCP listener runs it.
func qpMixedOperation(t *testing.T, schema *gqlast.Schema, roots []qpOperation, parsed map[string]*gqlast.QueryDocument,
	byRoot map[string][]string, org string) qpOperation {
	t.Helper()
	allowed := mcpclass.AllowedRoots()
	candidates := func(namespace string) []string {
		var mcp, other []string
		for _, op := range roots {
			_, field, _ := strings.Cut(op.key, ".")
			if op.namespace != namespace || !op.registered || !strings.HasPrefix(op.key, "Query.") {
				continue
			}
			if allowed[field] {
				mcp = append(mcp, op.key)
			} else {
				other = append(other, op.key)
			}
		}
		return append(mcp, other...)
	}
	for _, left := range candidates("analytics") {
		for _, right := range candidates("metrics") {
			text, ok := qpJoin(parsed[byRoot[left][0]], parsed[byRoot[right][0]])
			if !ok {
				continue
			}
			doc, errs := gqlparser.LoadQuery(schema, text)
			if errs != nil {
				continue
			}
			op := qpOperation{key: "Query.mixed_analytics_metrics", action: "analytics.read", namespace: "mixed", document: text,
				variables: qpVariables(schema, doc.Operations[0], org)}
			op.bodyBytes = op.body(t)
			return op
		}
	}
	t.Fatal("no analytics and metrics documents join into one valid operation")
	return qpOperation{}
}

// qpJoin is one query operation with the variables and root selections of both documents; false when a variable
// name is shared with another type.
func qpJoin(left, right *gqlast.QueryDocument) (string, bool) {
	a, b := *left.Operations[0], *right.Operations[0]
	a.Name = "ParityMixed"
	a.VariableDefinitions = slices.Clone(a.VariableDefinitions)
	for _, variable := range b.VariableDefinitions {
		if have := a.VariableDefinitions.ForName(variable.Variable); have != nil {
			if have.Type.String() != variable.Type.String() {
				return "", false
			}
			continue
		}
		a.VariableDefinitions = append(a.VariableDefinitions, variable)
	}
	a.SelectionSet = append(slices.Clone(a.SelectionSet), b.SelectionSet...)
	fragments := slices.Clone(left.Fragments)
	for _, fragment := range right.Fragments {
		if fragments.ForName(fragment.Name) == nil {
			fragments = append(fragments, fragment)
		}
	}
	var buf bytes.Buffer
	formatter.NewFormatter(&buf).FormatQueryDocument(&gqlast.QueryDocument{Operations: gqlast.OperationList{&a}, Fragments: fragments})
	return buf.String(), true
}

// maps is a copy of base with over applied.
func qpMerge(base, over map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(over))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range over {
		out[k] = v
	}
	return out
}

func qpSortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

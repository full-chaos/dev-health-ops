//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The /graphql edge, differentially (CHAOS-6263). One side is the REAL Python
// api: its /graphql app through TestClient, the GoApiDispatchRouter in front,
// forwarding to query-api over the internal-identity carrier exactly as prod
// configures it (QUERY_API_INTERNAL_URL, GO_API_PLANE_HEADER_ENABLED). The
// other side is query-api's own /graphql on its public listener, the path the
// Ingress will name. Both sides reach the SAME query-api process -- built by
// the real Build over the real settings, served by the real Listeners -- so a
// resolver answers identically on both by construction, and every difference
// this test can see is the edge's: method, transport, carrier, refusal,
// status, body and headers.
//
// The requests are the product's own: every registered document (the text a
// real urql client puts on the wire, pinned by the web repo's wire-parity
// gate) with the variables the go-api-prove ladder sends for it, as a POST
// and -- for a query -- as the GET urql sends when the URL fits, in the
// browser client's URL shape (org_id first). Around them, the refusal
// domain: every credential state the edge decides on, every way a request can
// carry no usable document, and every method.
//
// A declared divergence is asserted on both sides, never skipped: each names
// what each plane answers, and why query-api is the one to keep.

const (
	edgeOracleOrigin = "https://app.example"
	edgeOracleJWTKey = "venue-oracle-graphql-edge-secret-key-32bytes!"
	edgeOrgA         = "0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0a"
	edgeOrgB         = "0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0b"
)

// edgeUser is one seeded principal.
type edgeUser struct {
	name        string
	id          uuid.UUID
	superuser   bool
	active      bool
	dbVersion   int    // users.token_version
	tokenOrg    string // the org the token names
	tokenRole   string
	memberships map[string]string // org -> role
}

func edgeUsers() []edgeUser {
	return []edgeUser{
		{name: "member", id: uuid.MustParse("e0000000-0000-4000-8000-000000000001"), active: true, tokenOrg: edgeOrgA, tokenRole: "admin", memberships: map[string]string{edgeOrgA: "admin"}},
		{name: "viewer", id: uuid.MustParse("e0000000-0000-4000-8000-000000000002"), active: true, tokenOrg: edgeOrgA, tokenRole: "viewer", memberships: map[string]string{edgeOrgA: "viewer"}},
		{name: "super", id: uuid.MustParse("e0000000-0000-4000-8000-000000000003"), superuser: true, active: true, tokenOrg: edgeOrgA, tokenRole: "owner", memberships: map[string]string{edgeOrgA: "owner"}},
		{name: "impersonator", id: uuid.MustParse("e0000000-0000-4000-8000-000000000004"), superuser: true, active: true, tokenOrg: edgeOrgB, tokenRole: "owner", memberships: map[string]string{edgeOrgB: "owner"}},
		{name: "nonmember", id: uuid.MustParse("e0000000-0000-4000-8000-000000000005"), active: true, tokenOrg: edgeOrgA, tokenRole: "admin", memberships: map[string]string{edgeOrgB: "admin"}},
		{name: "inactive", id: uuid.MustParse("e0000000-0000-4000-8000-000000000006"), active: false, tokenOrg: edgeOrgA, tokenRole: "admin", memberships: map[string]string{edgeOrgA: "admin"}},
		{name: "revoked", id: uuid.MustParse("e0000000-0000-4000-8000-000000000007"), active: true, dbVersion: 1, tokenOrg: edgeOrgA, tokenRole: "admin", memberships: map[string]string{edgeOrgA: "admin"}},
		// A member of two orgs with a different role in each: the token names
		// A and A's role; the web selects B with X-Org-Id.
		{name: "multi", id: uuid.MustParse("e0000000-0000-4000-8000-000000000008"), active: true, tokenOrg: edgeOrgA, tokenRole: "member", memberships: map[string]string{edgeOrgA: "member", edgeOrgB: "admin"}},
	}
}

// registeredEdgeDocument is one entry of cmd/registrydump's output: the same
// producer the tools image runs to write documents.json for the prover.
type registeredEdgeDocument struct {
	Operation string `json:"operation"`
	Document  string `json:"document"`
	Digest    string `json:"digest"`
	Kind      string `json:"kind"`
	// Legacy marks a text the operation accepted BEFORE its current one (CHAOS-8000 dual accept).
	Legacy bool `json:"legacy"`
}

// postFreezeOperations are registered operations with no frozen Python answer (CHAOS-8598: capacityCompletionDistribution, an MCP-only read).
var postFreezeOperations = map[string]bool{"capacityCompletionDistribution": true}

func registeredEdgeDocuments(t *testing.T, root string) []registeredEdgeDocument {
	t.Helper()
	command := exec.Command("go", "run", "./cmd/registrydump", "-file", "internal/queryapi/server/query_route.go")
	command.Dir = root
	out, err := command.Output()
	if err != nil {
		t.Fatalf("registrydump: %v", err)
	}
	var docs []registeredEdgeDocument
	if err := json.Unmarshal(out, &docs); err != nil {
		t.Fatal(err)
	}
	if len(docs) == 0 {
		t.Fatal("registrydump listed no registered documents: nothing would be measured")
	}
	// CHAOS-8000: registrydump lists an operation's legacy texts after its current one. The frozen Python
	// answers this oracle compares with were recorded for the text each operation had BEFORE its current one;
	// the current text has no frozen answer. So an operation with a legacy text is measured on its OLDEST legacy
	// text (the one the golden saw), one document per operation as before. The recordings are stopped, so the
	// golden is not recorded again: the current text of such an operation is pinned by the Go tests of its
	// registered document, and this selection goes away when the legacy text is removed (CHAOS-8001).
	measured := map[string]registeredEdgeDocument{}
	for _, doc := range docs {
		held, seen := measured[doc.Operation]
		switch {
		case !seen:
			measured[doc.Operation] = doc
		case doc.Legacy && !held.Legacy:
			measured[doc.Operation] = doc
		}
	}
	// An operation registered after the freeze has no Python answer and the recordings are stopped, so it cannot be in the golden. Its
	// document is pinned by the Go tests of the operation itself. A name here that is not registered is stale and fails the run.
	for operation := range postFreezeOperations {
		if _, registered := measured[operation]; !registered {
			t.Fatalf("postFreezeOperations names %q, which registrydump does not list: remove the stale entry", operation)
		}
		delete(measured, operation)
	}
	// CHAOS-8513: an operation that was Go-only from its first day has no Python answer, live or frozen, so it
	// is not a case of this oracle (edgeGoOnlyFromBirth, whose own test keeps the list honest).
	for operation := range edgeGoOnlyFromBirth {
		delete(measured, operation)
	}
	docs = docs[:0]
	for _, doc := range measured {
		docs = append(docs, doc)
	}
	sort.Slice(docs, func(i, j int) bool { return docs[i].Operation < docs[j].Operation })
	return docs
}

// edgeOracleJWKS writes an Ed25519 JWKS for the envelope verifier Build
// requires and returns its path and the private key.
func edgeOracleJWKS(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := json.Marshal(map[string]any{"keys": []map[string]any{{
		"kty": "OKP", "crv": "Ed25519", "x": base64.RawURLEncoding.EncodeToString(pub),
		"kid": "edge-oracle", "use": "sig", "alg": "EdDSA",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "jwks.json")
	if err := os.WriteFile(path, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	return path, priv
}

// edgeOracleToken signs an access token the way Python's create_access_token
// does, for the two states the mint mode cannot express (an expired token,
// one signed with another key).
func edgeOracleToken(t *testing.T, key string, user edgeUser, expires time.Time) string {
	t.Helper()
	claims := jwt.MapClaims{
		"sub": user.id.String(), "email": user.name + "@example.com", "org_id": user.tokenOrg, "role": user.tokenRole,
		"is_superuser": user.superuser, "type": "access", "iss": defaultEdgeJWTIssuer, "aud": defaultEdgeJWTAudience,
		"exp": expires.Unix(), "iat": time.Now().Add(-2 * time.Hour).Unix(), "jti": uuid.NewString(), "tv": 0,
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// edgeCase is one request and how the two planes must relate on it.
type edgeCase struct {
	request venueoracle.Request
	// declared names a ruled divergence; pyWant/goWant then state each
	// plane's answer (status, and a substring of the body).
	declared       string
	pyWant, goWant edgeAnswer
	// header, when set, declares a divergence in that one header only: its
	// value on each plane is pyWant.header / goWant.header ("" = absent), and
	// every other part of the two answers must be the same.
	header string
}

type edgeAnswer struct {
	status int
	body   string
	header string
}

// nosniffOnly is the declared divergence of the size middleware's own
// refusals: they run outside the security headers, as in the Python app, and
// the Python answer carries none; query-api's writer states nosniff itself.
func nosniffOnly(request venueoracle.Request) edgeCase {
	return edgeCase{
		request:  request,
		declared: "size-middleware refusal: query-api's writer adds X-Content-Type-Options: nosniff, which the Python answer (outside its security headers) lacked",
		header:   "x-content-type-options",
		pyWant:   edgeAnswer{header: ""},
		goWant:   edgeAnswer{header: "nosniff"},
	}
}

func edgePost(name, token string, body string, headers map[string]string) venueoracle.Request {
	h := map[string]string{"Content-Type": "application/json", "Accept": "*/*"}
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	for k, v := range headers {
		h[k] = v
	}
	return venueoracle.Request{Name: name, Method: "POST", Path: "/graphql", Headers: h, Body: venueoracle.B64(body)}
}

// urqlBody is the JSON urql's fetchExchange POSTs: operationName, query,
// variables, in that order (makeFetchBody), compact.
func urqlBody(t *testing.T, operationName, query string, variables map[string]any) string {
	t.Helper()
	parts := []string{}
	if operationName != "" {
		parts = append(parts, `"operationName":`+jsonString(operationName))
	}
	parts = append(parts, `"query":`+jsonString(query))
	if variables != nil {
		raw, err := json.Marshal(variables)
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, `"variables":`+string(raw))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// urqlGETPath is the URL urql's makeFetchURL builds for the browser client
// (urqlClient.ts puts org_id on the client URL first): each body key set in
// URLSearchParams form, objects stringified.
func urqlGETPath(t *testing.T, orgID, operationName, query string, variables map[string]any) string {
	t.Helper()
	params := []string{}
	if orgID != "" {
		params = append(params, "org_id="+url.QueryEscape(orgID))
	}
	if operationName != "" {
		params = append(params, "operationName="+url.QueryEscape(operationName))
	}
	params = append(params, "query="+url.QueryEscape(query))
	if variables != nil {
		raw, err := json.Marshal(variables)
		if err != nil {
			t.Fatal(err)
		}
		params = append(params, "variables="+url.QueryEscape(string(raw)))
	}
	return "/graphql?" + strings.Join(params, "&")
}

func edgeGet(name, token, path string, headers map[string]string) venueoracle.Request {
	// TestClient sends "Accept: */*" unless told otherwise; Go's client sends
	// none. Named here so both planes read the same header.
	h := map[string]string{"Accept": "*/*"}
	if token != "" {
		h["Authorization"] = "Bearer " + token
	}
	for k, v := range headers {
		h[k] = v
	}
	return venueoracle.Request{Name: name, Method: "GET", Path: path, Headers: h}
}

var operationNamePattern = regexp.MustCompile(`^\s*(?:query|mutation)\s+([A-Za-z_][A-Za-z0-9_]*)`)

func documentOperationName(document string) string {
	if match := operationNamePattern.FindStringSubmatch(document); match != nil {
		return match[1]
	}
	return ""
}

// edgeSeed seeds the venue databases: the orgs, the principals, and the impersonation (query-api reads no routing row).
func edgeSeed(users []edgeUser, docs []registeredEdgeDocument, schemaDigest string) func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
	return func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
		exec := func(sql string, args ...any) {
			t.Helper()
			if _, err := admin.Exec(ctx, sql, args...); err != nil {
				t.Fatalf("seed: %v\n%s", err, sql)
			}
		}
		for i, org := range []string{edgeOrgA, edgeOrgB} {
			exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'community', 'stripe', true, now(), now())`, org, fmt.Sprintf("edge-%d", i), fmt.Sprintf("Edge %d", i))
		}
		specs := map[string]map[string]any{}
		for _, u := range users {
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, $4, $5, now(), now())`, u.id, u.name+"@example.com", u.active, u.superuser, u.dbVersion)
			for org, role := range u.memberships {
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, now(), now(), now())`, uuid.New(), org, u.id, role)
			}
			specs[u.name] = map[string]any{"user_id": u.id.String(), "email": u.name + "@example.com",
				"org_id": u.tokenOrg, "role": u.tokenRole, "is_superuser": u.superuser, "token_version": 0}
		}
		// The impersonator is impersonating the viewer, in org A.
		exec(`INSERT INTO impersonation_sessions (id, admin_user_id, target_user_id, target_org_id, target_role, created_at, expires_at)
VALUES ($1, $2, $3, $4, 'viewer', now(), now() + interval '1 day')`,
			uuid.New(), users[3].id, users[1].id, edgeOrgA)
		return specs
	}
}

// enterRepositoryRoot makes the repository root the working directory until the test ends. The report writer stages
// its job contracts from a path relative to the working directory, as the image lays them out. It is os.Chdir, not
// t.Chdir: t.Chdir also sets PWD, and a golden's key must not hold the absolute path of a checkout.
//
// It returns the function that goes back to the previous directory (also run at the end of the test): a frozen test
// calls it before it finishes its golden, whose file path is relative to the package directory.
func enterRepositoryRoot(t *testing.T, root string) (leave func()) {
	t.Helper()
	previous, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	leave = func() { _ = os.Chdir(previous) }
	t.Cleanup(leave)
	return leave
}

// startEdgePlane builds query-api over the venue as prod does (the real Build over the real settings, the
// real Listeners) and returns its settings, the base of its public listener and the environment the Python
// plane needs to reach it.
func startEdgePlane(t *testing.T, ctx context.Context, venue *venueoracle.Venue, root string) (map[string]string, string, []string, func()) {
	t.Helper()
	// query-api: the real Build over the real settings, the real Listeners.
	jwksPath, _ := edgeOracleJWKS(t)
	settings := map[string]string{
		"CLICKHOUSE_URI":               venue.AdminClickHouseURI(t, venue.GoClickHouseDB),
		"GO_API_REGISTRY_POSTGRES_URI": venue.AdminURI(t, venue.GoDB),
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       "edge-oracle-issuer",
		"GO_API_ENVELOPE_AUDIENCE":     "edge-oracle-audience",
		edgeJWTSecretEnvVar:            edgeOracleJWTKey,
		"CORS_ALLOWED_ORIGINS":         edgeOracleOrigin,
	}
	// The report writer stages its job contracts from a path relative to the
	// working directory, as the image lays them out: run from the repo root
	// so triggerReport is the real, contract-backed mutation.
	leave := enterRepositoryRoot(t, root)
	plane, err := Build(func(key string) string { return settings[key] })
	if err != nil {
		t.Fatalf("build query-api: %v", err)
	}
	t.Cleanup(plane.Close)
	_, loopback, _ := net.ParseCIDR("127.0.0.1/32")
	public, internal := Listeners("127.0.0.1:0", "127.0.0.1:0", plane, nil, []*net.IPNet{loopback})
	for _, listener := range []*Listener{public, internal} {
		if err := listener.Start(ctx); err != nil {
			t.Fatal(err)
		}
		listener := listener
		t.Cleanup(func() { _ = listener.Shutdown(context.Background()) })
	}
	goBase := "http://" + public.Address()
	pythonEnv := []string{
		"GO_API_QUERY_API_URL=" + goBase,
		"QUERY_API_INTERNAL_URL=http://" + internal.Address(),
		"GO_API_PLANE_HEADER_ENABLED=true",
		"GO_API_DISPATCH_TIMEOUT_SECONDS=60",
		"VENUE_RESET_ATTRS_PER_REQUEST=dev_health_ops.api.graphql.go_api_dispatcher:_http_client,dev_health_ops.api.graphql.go_api_dispatcher:_internal_http_client",
		"VENUE_PY_LOGGING=1",
		"CORS_ALLOWED_ORIGINS=" + edgeOracleOrigin,
	}
	return settings, goBase, pythonEnv, leave
}

// edgeSuite is the requests of the edge oracle and the documents its later phases use.
type edgeSuite struct {
	cases, writes   []edgeCase
	query, mutation registeredEdgeDocument
	queryBody       string
	member          string
}

// edgeOracleCases builds the requests of the edge oracle. mint signs the two time-bound tokens the venue's mint
// mode cannot express: one signed with another key (valid in time) and one expired.
//
// schemeToken is the credential of the two cases that present a valid token under another scheme ("bearer", "Basic"):
// empty means the venue's member token (the live oracle); the frozen oracle gives a valid token with fixed instants,
// because the golden's request key holds a token sent under another scheme as written, and the venue's own token is
// new in every run.
func edgeOracleCases(t *testing.T, docs []registeredEdgeDocument, users []edgeUser, venue *venueoracle.Venue, mint func(key string, user edgeUser, expired bool) string, schemeToken string) edgeSuite {
	t.Helper()
	token := func(name string) string {
		value, ok := venue.Tokens[name]
		if !ok {
			t.Fatalf("no token minted for %q", name)
		}
		return value
	}
	member := token("member")
	schemeCredential := member
	if schemeToken != "" {
		schemeCredential = schemeToken
	}

	var cases []edgeCase
	var writes []edgeCase
	for _, doc := range docs {
		spec, err := goapiproof.SpecFor(doc.Operation)
		if err != nil {
			t.Fatal(err)
		}
		variables := spec.Variables(edgeOrgA, goapiproof.DefaultWindow())
		name := documentOperationName(doc.Document)
		post := edgeCase{request: edgePost("POST "+doc.Operation, member, urqlBody(t, name, doc.Document, variables), nil)}
		get := edgeCase{request: edgeGet("GET "+doc.Operation, member, urqlGETPath(t, edgeOrgA, name, doc.Document, variables), nil)}
		if doc.Operation == "createSavedReport" {
			writes = append(writes, post)
			continue
		}
		cases = append(cases, post)
		// A mutation as GET too: both planes must refuse it, never run it.
		cases = append(cases, get)
	}

	// The credential domain, on one query and one mutation.
	probe := func(op string) registeredEdgeDocument {
		for _, doc := range docs {
			if doc.Operation == op {
				return doc
			}
		}
		t.Fatalf("no registered document %q", op)
		return registeredEdgeDocument{}
	}
	query, mutation := probe("catalogValues"), probe("deleteSavedReport")
	for _, doc := range []registeredEdgeDocument{query, mutation} {
		spec, _ := goapiproof.SpecFor(doc.Operation)
		body := urqlBody(t, documentOperationName(doc.Document), doc.Document, spec.Variables(edgeOrgA, goapiproof.DefaultWindow()))
		for _, c := range []struct{ name, token string }{
			{"no credential", ""},
			{"garbage bearer", "not-a-jwt"},
			{"wrong key", mint("another-key-another-key-another-key-32b", users[0], false)},
			{"expired", mint(edgeOracleJWTKey, users[0], true)},
			{"inactive user", token("inactive")},
			{"revoked token version", token("revoked")},
			{"viewer", token("viewer")},
			{"superuser", token("super")},
			{"impersonating superuser", token("impersonator")},
		} {
			cases = append(cases, edgeCase{request: edgePost(doc.Operation+" as "+c.name, c.token, body, nil)})
		}
		cases = append(cases,
			edgeCase{request: edgePost(doc.Operation+" lower-case scheme", "", body, map[string]string{"Authorization": "bearer " + schemeCredential})},
			edgeCase{request: edgePost(doc.Operation+" basic scheme", "", body, map[string]string{"Authorization": "Basic " + schemeCredential})},
			edgeCase{request: edgePost(doc.Operation+" text/plain", member, body, map[string]string{"Content-Type": "text/plain"})},
		)
	}
	querySpec, _ := goapiproof.SpecFor(query.Operation)
	queryVariables := querySpec.Variables(edgeOrgA, goapiproof.DefaultWindow())
	queryBody := urqlBody(t, "CatalogValues", query.Document, queryVariables)
	cases = append(cases, edgeCase{request: edgePost("catalogValues as non-member", token("nonmember"), queryBody, nil),
		declared: "membership: a token naming an org its user is no longer a member of -- Python served it, query-api checks the membership live and refuses it",
		pyWant:   edgeAnswer{status: 200, body: `"catalog"`}, goWant: edgeAnswer{status: 401, body: "Authentication required"}})
	// X-Org-Id (OrgIdMiddleware): the caller's own org, a member org, a
	// foreign org (403 before anything else), a superuser naming any org.
	for _, c := range []struct{ name, token, org string }{
		{"own org", member, edgeOrgA}, {"foreign org", member, edgeOrgB}, {"blank", member, "  "},
		{"superuser foreign org", token("super"), edgeOrgB}, {"no credential foreign org", "", edgeOrgB},
		{"foreign org, oversize", member, edgeOrgB},
	} {
		body := queryBody
		if strings.HasSuffix(c.name, "oversize") {
			body = urqlBody(t, "CatalogValues", query.Document+strings.Repeat(" ", defaultGraphQLMaxQueryBytes), queryVariables)
		}
		cases = append(cases, edgeCase{request: edgePost("X-Org-Id "+c.name, c.token, body, map[string]string{"X-Org-Id": c.org})})
	}
	// CORS (CORSMiddleware): a simple request with an allowed and a foreign
	// Origin, and preflights.
	for _, c := range []struct{ name, origin string }{{"allowed origin", edgeOracleOrigin}, {"foreign origin", "https://evil.example"}} {
		cases = append(cases,
			edgeCase{request: edgePost("POST "+c.name, member, queryBody, map[string]string{"Origin": c.origin})},
			edgeCase{request: venueoracle.Request{Name: "preflight " + c.name, Method: "OPTIONS", Path: "/graphql",
				Headers: map[string]string{"Origin": c.origin, "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization,content-type"}}},
		)
	}
	cases = append(cases,
		// A preflight that asks for a method outside the allowed list (GET POST PUT PATCH DELETE OPTIONS) is a 400
		// "Disallowed CORS method": every other preflight here asks POST.
		edgeCase{request: venueoracle.Request{Name: "preflight allowed origin, method outside the list", Method: "OPTIONS", Path: "/graphql",
			Headers: map[string]string{"Origin": edgeOracleOrigin, "Access-Control-Request-Method": "TRACE", "Access-Control-Request-Headers": "authorization,content-type"}}},
		edgeCase{request: venueoracle.Request{Name: "OPTIONS without a preflight", Method: "OPTIONS", Path: "/graphql"}},
		edgeCase{request: venueoracle.Request{Name: "HEAD", Method: "HEAD", Path: "/graphql", Headers: map[string]string{"Authorization": "Bearer " + member}}},
		nosniffOnly(venueoracle.Request{Name: "PATCH oversize", Method: "PATCH", Path: "/graphql", Body: venueoracle.B64(strings.Repeat("x", defaultGraphQLMaxQueryBytes+1))}),
		edgeCase{request: edgePost("X-Request-ID given", member, queryBody, map[string]string{"X-Request-ID": "edge-oracle-request"})},
		// Strawberry's refusals for a body the edge did not forward.
		edgeCase{request: edgePost("POST unusable, text/plain", member, `{}`, map[string]string{"Content-Type": "text/plain"})},
		edgeCase{request: edgePost("POST JSON scalar", member, `5`, nil)},
		edgeCase{request: edgePost("POST JSON null", member, `null`, nil)},
		edgeCase{request: edgePost("POST invalid UTF-8", member, "{\"query\": \"\xff\"}", nil)},
		edgeCase{request: edgePost("POST integer past 4300 digits", member, `{"query": `+jsonString(query.Document)+`, "variables": {"n": `+strings.Repeat("9", 4301)+`}}`, nil)},
		edgeCase{request: edgePost("POST variables not an object, no query", member, `{"query": null, "variables": []}`, nil)},
		edgeCase{request: edgePost("POST extensions not an object, no query", member, `{"query": null, "extensions": 1}`, nil)},
		edgeCase{request: edgeGet("GET no query, JSON accept", member, "/graphql?org_id="+edgeOrgA, map[string]string{"Accept": "application/json"})},
		edgeCase{request: edgeGet("GET variables not an object, no query", member, "/graphql?variables=5", nil)},
		edgeCase{request: edgeGet("GET extensions unparseable, registered query", member,
			urqlGETPath(t, edgeOrgA, "CatalogValues", query.Document, queryVariables)+"&extensions=%7Bbad", nil)},
		// A registered document query-api refuses (declared divergence):
		// the Python edge wrapped query-api's answer in a 200 typed error;
		// query-api answers its own status.
		edgeCase{request: edgePost("registered, variables missing", member, urqlBody(t, "CatalogValues", query.Document, map[string]any{"orgId": edgeOrgA}), nil),
			declared: "query-api refusal: the Python edge re-wrapped it as a 200 typed error; query-api answers its own status",
			pyWant:   edgeAnswer{status: 200, body: `"go_unexpected_status"`}, goWant: edgeAnswer{status: 422, body: "GRAPHQL_VALIDATION_FAILED"}},
		edgeCase{request: edgeGet("GET registered, variables not an object", member, urqlGETPath(t, edgeOrgA, "CatalogValues", query.Document, nil)+"&variables=5", nil),
			declared: "query-api refusal: the Python edge re-wrapped it as a 200 typed error; query-api answers its own status",
			pyWant:   edgeAnswer{status: 200, body: `"go_unexpected_status"`}, goWant: edgeAnswer{status: 400, body: "json request body could not be decoded"}},
	)

	// Privileged and impersonation-sensitive documents, per principal.
	for _, op := range []string{"productTelemetryPlatformDashboard", "connectorsDataHealth"} {
		doc := probe(op)
		spec, _ := goapiproof.SpecFor(op)
		body := urqlBody(t, documentOperationName(doc.Document), doc.Document, spec.Variables(edgeOrgA, goapiproof.DefaultWindow()))
		for _, who := range []string{"viewer", "super", "impersonator"} {
			cases = append(cases, edgeCase{request: edgePost(op+" as "+who, token(who), body, nil)})
		}
	}

	// One identity per request: the org the client selects (X-Org-Id, after
	// the membership check) and the role held IN THAT ORG are what the
	// resolvers read. The Python dispatcher forwarded the token's org and role
	// whatever X-Org-Id the org scope had accepted
	// (go_api_dispatcher.py _internal_identity_headers(context.user)), so on
	// main a member selecting B got A's identity: B's data was refused and
	// B's role never applied. That was the defect; query-api serves B.
	multi := token("multi")
	catalogB := urqlBody(t, documentOperationName(query.Document), query.Document, func() map[string]any {
		spec, _ := goapiproof.SpecFor(query.Operation)
		return spec.Variables(edgeOrgB, goapiproof.DefaultWindow())
	}())
	catalogA := urqlBody(t, documentOperationName(query.Document), query.Document, func() map[string]any {
		spec, _ := goapiproof.SpecFor(query.Operation)
		return spec.Variables(edgeOrgA, goapiproof.DefaultWindow())
	}())
	dataHealthDoc := probe("connectorsDataHealth")
	dataHealthBody := urqlBody(t, documentOperationName(dataHealthDoc.Document), dataHealthDoc.Document, func() map[string]any {
		spec, _ := goapiproof.SpecFor(dataHealthDoc.Operation)
		return spec.Variables(edgeOrgA, goapiproof.DefaultWindow())
	}())
	cases = append(cases,
		edgeCase{request: edgePost("member of A and B, no X-Org-Id: catalogValues of A", multi, catalogA, nil)},
		edgeCase{request: edgePost("member of A and B, no X-Org-Id: role of A", multi, dataHealthBody, nil)},
		edgeCase{request: edgePost("member of A and B, X-Org-Id A: catalogValues of A", multi, catalogA, map[string]string{"X-Org-Id": edgeOrgA})},
		edgeCase{request: edgePost("member of A and B, X-Org-Id B: catalogValues of B", multi, catalogB, map[string]string{"X-Org-Id": edgeOrgB}),
			declared: "selected org: the Python dispatcher forwarded the token's org A, so B's data was refused; query-api serves B, the org the scope verified",
			pyWant:   edgeAnswer{status: 200, body: "Access denied: cannot query org"}, goWant: edgeAnswer{status: 200, body: `{"data":{"catalog":{`}},
		edgeCase{request: edgePost("member of A and B, X-Org-Id B: role of B", multi, dataHealthBody, map[string]string{"X-Org-Id": edgeOrgB}),
			declared: "selected org's role: the Python dispatcher forwarded the token's role in A (member), so B's admin role never applied; query-api uses the role held in B",
			pyWant:   edgeAnswer{status: 200, body: "Data health requires operator access"}, goWant: edgeAnswer{status: 200, body: `{"data":{"dataHealth":`}},
		edgeCase{request: edgePost("member of A and B, X-Org-Id of an org it is not in", multi, catalogA, map[string]string{"X-Org-Id": "0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0c"})},
	)

	// No usable document, and the size limit.
	limit := defaultGraphQLMaxQueryBytes
	oversize := urqlBody(t, "CatalogValues", query.Document+strings.Repeat(" ", limit), map[string]any{"orgId": edgeOrgA})
	cases = append(cases,
		edgeCase{request: edgePost("POST empty object", member, `{}`, nil)},
		edgeCase{request: edgePost("POST null query", member, `{"query": null}`, nil)},
		edgeCase{request: edgePost("POST query not a string", member, `{"query": 5}`, nil)},
		edgeCase{request: edgePost("POST array body", member, `[]`, nil)},
		edgeCase{request: edgePost("POST not JSON", member, `query { x }`, nil)},
		edgeCase{request: edgePost("POST empty body", member, ``, nil)},
		edgeCase{request: edgePost("POST not JSON, no credential", "", `query { x }`, nil)},
		nosniffOnly(edgePost("POST oversize", member, oversize, nil)),
		nosniffOnly(edgePost("POST oversize, no credential", "", oversize, nil)),
		edgeCase{request: edgeGet("GET no query", member, "/graphql?org_id="+edgeOrgA, nil)},
		edgeCase{request: edgeGet("GET no query, no credential", "", "/graphql", nil)},
		edgeCase{request: edgeGet("GET variables not JSON", member, "/graphql?query="+url.QueryEscape(query.Document)+"&variables=%7Bnot", nil)},
		nosniffOnly(edgeGet("GET browser accept", member, urqlGETPath(t, edgeOrgA, "CatalogValues", query.Document, map[string]any{"orgId": edgeOrgA}), map[string]string{"Accept": "text/html,application/xhtml+xml"})),
		nosniffOnly(edgeGet("GET browser accept, no credential", "", "/graphql", map[string]string{"Accept": "text/html"})),
		edgeCase{request: edgeGet("GET repeated query, last wins", member,
			"/graphql?query=garbage&"+strings.TrimPrefix(urqlGETPath(t, "", "", query.Document, queryVariables), "/graphql?"), nil)},
		edgeCase{request: venueoracle.Request{Name: "PUT", Method: "PUT", Path: "/graphql", Headers: map[string]string{"Authorization": "Bearer " + member}, Body: venueoracle.B64(`{}`)}},
		edgeCase{request: venueoracle.Request{Name: "DELETE no credential", Method: "DELETE", Path: "/graphql"}},
	)

	// Unregistered documents (declared divergence): Python let Strawberry
	// answer them (200 with errors); query-api refuses them (404).
	for _, c := range []struct{ name, body string }{
		{"unregistered query", `{"query": "query Other { catalog(orgId: \"` + edgeOrgA + `\") { dimensions { name } } }"}`},
		{"introspection", `{"query": "{ __schema { queryType { name } } }"}`},
		{"typename", `{"query": "{ __typename }"}`},
		{"registered text, reflowed", `{"query": ` + jsonString(strings.ReplaceAll(query.Document, "\n", " ")) + `, "variables": {"orgId": "` + edgeOrgA + `"}}`},
	} {
		cases = append(cases, edgeCase{request: edgePost(c.name, member, c.body, nil),
			declared: "unregistered document: Python's Strawberry answers it, query-api serves registered documents only",
			pyWant:   edgeAnswer{status: 200, body: map[bool]string{true: `"__typename"`, false: `"errors"`}[c.name == "typename"]}, goWant: edgeAnswer{status: 404}})
	}
	return edgeSuite{cases: cases, writes: writes, query: query, mutation: mutation, queryBody: queryBody, member: member}
}

// edgeRequests is the requests of the cases, in order.
func edgeRequests(cs []edgeCase) []venueoracle.Request {
	out := make([]venueoracle.Request, len(cs))
	for i, c := range cs {
		out[i] = c.request
	}
	return out
}

// edgeCompare compares the Python plane's answers with the Go plane's: a declared divergence is asserted on both
// sides, every other case goes through venueoracle.Diff. golden is the frozen golden the answers came from (nil
// for the live oracle): the answers of the declared cases are then declared inspected, and Diff compares the rest
// as the golden requires.
func edgeCompare(t *testing.T, goBase string, cs []edgeCase, python []venueoracle.Response, normalize func(venueoracle.Request, string) string, golden *venueoracle.Golden) {
	t.Helper()
	var parity []venueoracle.Request
	var parityPython []venueoracle.Response
	var receipt strings.Builder
	for i, c := range cs {
		if key, ok := chaos8169GraphQLEdgeHomeLedgerKey(c.request.Name); ok {
			goResponse := venueoracle.Do(t, goBase, c.request)
			if golden != nil {
				golden.Consumed(t, python[i])
			}
			statusEqual := python[i].Status == goResponse.Status
			if !statusEqual {
				t.Errorf("%s: D4840 Home ledger needs equal status; python=%d go=%d", c.request.Name, python[i].Status, goResponse.Status)
			}
			differing := headersThatDiffer(c.request, python[i], goResponse)
			headersEqual := len(differing) == 0
			if !headersEqual {
				t.Errorf("%s: D4840 Home ledger has undeclared header differences %v\n python %v\n go     %v",
					c.request.Name, differing, python[i].Headers, goResponse.Headers)
			}
			if chaos8509HomeCaptureAllowed(statusEqual, headersEqual) {
				captureCHAOS8509HomePair(t, key, chaos8169GraphQLHomeBody(t, python[i].Body), chaos8169GraphQLHomeBody(t, goResponse.Body))
			}
			policy, ok := chaos8509HomeCapturePolicies[key]
			if !ok {
				t.Fatalf("CHAOS-8509 has no GraphQL Home policy for %s", c.request.Name)
			}
			assertCHAOS8509HomeCapturePolicy(t, key, policy, chaos8169GraphQLHomeBody(t, python[i].Body), chaos8169GraphQLHomeBody(t, goResponse.Body))
			fmt.Fprintf(&receipt, "%-58s python=%d go=%d D4840 Home ledger validated\n", c.request.Name, python[i].Status, goResponse.Status)
			continue
		}
		if c.declared == "" {
			parity = append(parity, c.request)
			parityPython = append(parityPython, python[i])
			continue
		}
		goResponse := venueoracle.Do(t, goBase, c.request)
		if golden != nil {
			golden.Consumed(t, python[i])
		}
		var holds bool
		if c.header != "" {
			// One header differs, as declared; the rest must be SAME.
			pyRest, goRest := withoutHeader(python[i], c.header), withoutHeader(goResponse, c.header)
			same, _, _, _ := venueoracle.Compare(c.request, pyRest, goRest, venueoracle.DiffOptions{})
			holds = same && python[i].Headers[c.header] == c.pyWant.header && goResponse.Headers[c.header] == c.goWant.header
		} else {
			holds = python[i].Status == c.pyWant.status && strings.Contains(python[i].Body, c.pyWant.body) &&
				goResponse.Status == c.goWant.status && strings.Contains(goResponse.Body, c.goWant.body)
			// The status and body differ as declared; every header must
			// still be the same (content-length aside: the bodies differ).
			// A header that differs is a divergence nobody declared: it
			// fails here, by name, never passes unseen.
			if differing := headersThatDiffer(c.request, python[i], goResponse); len(differing) > 0 {
				holds = false
				t.Errorf("%s: declared divergence (%s) also differs in undeclared headers %v\n python %v\n go     %v",
					c.request.Name, c.declared, differing, python[i].Headers, goResponse.Headers)
			}
		}
		fmt.Fprintf(&receipt, "%-58s python=%d go=%d DECLARED holds=%v\n", c.request.Name, python[i].Status, goResponse.Status, holds)
		if !holds {
			t.Errorf("%s: declared divergence (%s) does not hold\n python %d %s\n go     %d %s", c.request.Name, c.declared,
				python[i].Status, truncateOracleBody(python[i].Body), goResponse.Status, truncateOracleBody(goResponse.Body))
		}
	}
	base := normalize
	receipt.WriteString(venueoracle.Diff(t, goBase, parity, parityPython, venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			body = coverageDivergenceNormalize(request, body)
			body = noDataStatusNormalize(request, body, operatingReviewTablesEmpty)
			body = goOnlyMetricsNormalize(request, body)
			if base != nil {
				body = base(request, body)
			}
			return body
		},
		Inspect: func(request venueoracle.Request, goResponse venueoracle.Response) {
			if ok, why := noDataStatusInspect(request, goResponse.Body); !ok {
				t.Errorf("%s: %s", request.Name, why)
			}
			if ok, why := goOnlyMetricsInspect(request, goResponse.Body); !ok {
				t.Errorf("%s: %s", request.Name, why)
			}
			if isThroughputForecastRequest(request) && !strings.Contains(goResponse.Body, goZeroCoverage) {
				t.Errorf("%s: query-api must answer the all-zero estimateCoverage object (D4373/D4376), got: %s",
					request.Name, truncateOracleBody(goResponse.Body))
			}
		},
		Golden: golden,
	}))
	t.Log("\n" + receipt.String())
}

// withoutHeader is response with one header removed.
func withoutHeader(response venueoracle.Response, name string) venueoracle.Response {
	headers := make(map[string]string, len(response.Headers))
	for key, value := range response.Headers {
		if key != name {
			headers[key] = value
		}
	}
	response.Headers = headers
	return response
}

// headersThatDiffer compares every header of two answers whose status and body
// are allowed to differ: venueoracle.Compare with the status and body made
// equal and content-length skipped, so only the headers decide.
func headersThatDiffer(request venueoracle.Request, python, goResponse venueoracle.Response) []string {
	goSame := goResponse
	goSame.Status, goSame.Body = python.Status, python.Body
	same, compared, pyShown, goShown := venueoracle.Compare(request, python, goSame, venueoracle.DiffOptions{
		SkipContentLength: func(venueoracle.Request) bool { return true },
	})
	if same {
		return nil
	}
	var differing []string
	for _, name := range compared {
		if pyShown.Headers[name] != goShown.Headers[name] {
			differing = append(differing, name)
		}
	}
	return differing
}

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

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
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
	}
}

// registeredEdgeDocument is one entry of cmd/registrydump's output: the same
// producer the tools image runs to write documents.json for the prover.
type registeredEdgeDocument struct {
	Operation string `json:"operation"`
	Document  string `json:"document"`
	Digest    string `json:"digest"`
	Kind      string `json:"kind"`
}

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
}

type edgeAnswer struct {
	status int
	body   string
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

func TestGraphQLEdgeVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRootFromHere(t)
	docs := registeredEdgeDocuments(t, root)
	users := edgeUsers()
	schemaDigest := digest.Schema(schemav1.SDL)

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: edgeOracleJWTKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
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
			// Every registered operation routed to Go, as prod runs it.
			for _, doc := range docs {
				exec(`INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
VALUES ($1, $2, $3, 'venue') ON CONFLICT DO NOTHING`, schemaDigest, doc.Digest, doc.Operation)
				exec(`INSERT INTO go_api_routing_state
(schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, review_evidence, recorded_by, updated_at)
VALUES ($1, $2, $3, 'venue', 'go', 'canary', 100, 'venue', 'venue', now())`, schemaDigest, doc.Digest, doc.Operation)
			}
			return specs
		},
	})

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
	t.Chdir(root)
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

	token := func(name string) string {
		value, ok := venue.Tokens[name]
		if !ok {
			t.Fatalf("no token minted for %q", name)
		}
		return value
	}
	member := token("member")

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
		if doc.Operation == "createSavedReport" {
			writes = append(writes, post)
			continue
		}
		cases = append(cases, post)
		// A mutation as GET too: both planes must refuse it, never run it.
		cases = append(cases, edgeCase{request: edgeGet("GET "+doc.Operation, member, urqlGETPath(t, edgeOrgA, name, doc.Document, variables), nil)})
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
			{"wrong key", edgeOracleToken(t, "another-key-another-key-another-key-32b", users[0], time.Now().Add(time.Hour))},
			{"expired", edgeOracleToken(t, edgeOracleJWTKey, users[0], time.Now().Add(-time.Hour))},
			{"inactive user", token("inactive")},
			{"revoked token version", token("revoked")},
			{"viewer", token("viewer")},
			{"superuser", token("super")},
			{"impersonating superuser", token("impersonator")},
		} {
			cases = append(cases, edgeCase{request: edgePost(doc.Operation+" as "+c.name, c.token, body, nil)})
		}
		cases = append(cases,
			edgeCase{request: edgePost(doc.Operation+" lower-case scheme", "", body, map[string]string{"Authorization": "bearer " + member})},
			edgeCase{request: edgePost(doc.Operation+" basic scheme", "", body, map[string]string{"Authorization": "Basic " + member})},
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
		edgeCase{request: venueoracle.Request{Name: "OPTIONS without a preflight", Method: "OPTIONS", Path: "/graphql"}},
		edgeCase{request: venueoracle.Request{Name: "HEAD", Method: "HEAD", Path: "/graphql", Headers: map[string]string{"Authorization": "Bearer " + member}}},
		edgeCase{request: venueoracle.Request{Name: "PATCH oversize", Method: "PATCH", Path: "/graphql", Body: venueoracle.B64(strings.Repeat("x", defaultGraphQLMaxQueryBytes+1))}},
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
		edgeCase{request: edgePost("POST oversize", member, oversize, nil)},
		edgeCase{request: edgePost("POST oversize, no credential", "", oversize, nil)},
		edgeCase{request: edgeGet("GET no query", member, "/graphql?org_id="+edgeOrgA, nil)},
		edgeCase{request: edgeGet("GET no query, no credential", "", "/graphql", nil)},
		edgeCase{request: edgeGet("GET variables not JSON", member, "/graphql?query="+url.QueryEscape(query.Document)+"&variables=%7Bnot", nil)},
		edgeCase{request: edgeGet("GET browser accept", member, urqlGETPath(t, edgeOrgA, "CatalogValues", query.Document, map[string]any{"orgId": edgeOrgA}), map[string]string{"Accept": "text/html,application/xhtml+xml"})},
		edgeCase{request: edgeGet("GET browser accept, no credential", "", "/graphql", map[string]string{"Accept": "text/html"})},
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

	requests := func(cs []edgeCase) []venueoracle.Request {
		out := make([]venueoracle.Request, len(cs))
		for i, c := range cs {
			out[i] = c.request
		}
		return out
	}

	run := func(cs []edgeCase, normalize func(venueoracle.Request, string) string) {
		reqs := requests(cs)
		python := venue.ServePythonWithEnv(t, pythonEnv, reqs)
		var parity []venueoracle.Request
		var parityPython []venueoracle.Response
		var receipt strings.Builder
		for i, c := range cs {
			if c.declared == "" {
				parity = append(parity, c.request)
				parityPython = append(parityPython, python[i])
				continue
			}
			goResponse := venueoracle.Do(t, goBase, c.request)
			holds := python[i].Status == c.pyWant.status && strings.Contains(python[i].Body, c.pyWant.body) &&
				goResponse.Status == c.goWant.status && strings.Contains(goResponse.Body, c.goWant.body)
			fmt.Fprintf(&receipt, "%-58s python=%d go=%d DECLARED holds=%v\n", c.request.Name, python[i].Status, goResponse.Status, holds)
			if !holds {
				t.Errorf("%s: declared divergence (%s) does not hold\n python %d %s\n go     %d %s", c.request.Name, c.declared,
					python[i].Status, truncateOracleBody(python[i].Body), goResponse.Status, truncateOracleBody(goResponse.Body))
			}
		}
		receipt.WriteString(venueoracle.Diff(t, goBase, parity, parityPython, venueoracle.DiffOptions{
			Normalize: normalize,
		}))
		t.Log("\n" + receipt.String())
	}
	// Both legs reach the same resolvers, so a timestamp that differs between
	// them is a clock reading taken per call (generatedAt, computedAt), never
	// an edge difference: blanked in a 200 body, and only there.
	clock := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
	run(cases, func(_ venueoracle.Request, body string) string {
		if !strings.HasPrefix(body, `{"data":{`) {
			return body
		}
		return clock.ReplaceAllString(body, "<clock>")
	})

	// A registered operation whose routing row is off: the Python edge fell
	// back to Strawberry (a query's resolver raises "served by query-api"; a
	// mutation ran its Python body); query-api answers a GraphQL error and
	// runs nothing. Both planes' rows are turned off, as an operator would.
	for _, database := range []string{venue.SourceDB, venue.GoDB} {
		pool, err := pgxpool.New(ctx, venue.AdminURI(t, database))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE go_api_routing_state SET mode = 'disabled' WHERE selected_operation = ANY($1)`,
			[]string{query.Operation, mutation.Operation}); err != nil {
			t.Fatal(err)
		}
		pool.Close()
	}
	mutationSpec, _ := goapiproof.SpecFor(mutation.Operation)
	run([]edgeCase{
		{request: edgePost("row off: "+query.Operation, member, queryBody, nil),
			declared: "routing row off: Python's Strawberry fallback raised for the query; query-api answers a GraphQL error",
			pyWant:   edgeAnswer{status: 200, body: `"errors"`}, goWant: edgeAnswer{status: 200, body: "OPERATION_NOT_ENABLED"}},
		{request: edgePost("row off: "+mutation.Operation, member, urqlBody(t, documentOperationName(mutation.Document), mutation.Document, mutationSpec.Variables(edgeOrgA, goapiproof.DefaultWindow())), nil),
			declared: "routing row off: Python ran its own mutation body; query-api answers a GraphQL error and runs nothing",
			pyWant:   edgeAnswer{status: 200, body: `"deleteSavedReport"`}, goWant: edgeAnswer{status: 200, body: "OPERATION_NOT_ENABLED"}},
	}, nil)

	// A write last, on each plane in turn: createSavedReport mints an id and
	// a timestamp per call, so those are blanked; nothing else is.
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	timePattern := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?`)
	run(writes, func(_ venueoracle.Request, body string) string {
		return timePattern.ReplaceAllString(uuidPattern.ReplaceAllString(body, "<uuid>"), "<time>")
	})

	// Postgres unreachable on both planes: the caller cannot be read, and
	// both answer the unhandled 500. A second query-api over a dead DSN.
	deadSettings := map[string]string{}
	for key, value := range settings {
		deadSettings[key] = value
	}
	deadSettings["GO_API_REGISTRY_POSTGRES_URI"] = "postgres://nobody:nothing@127.0.0.1:1/none?connect_timeout=2"
	deadPlane, err := Build(func(key string) string { return deadSettings[key] })
	if err != nil {
		t.Fatalf("build query-api over a dead Postgres: %v", err)
	}
	t.Cleanup(deadPlane.Close)
	deadPublic, _ := Listeners("127.0.0.1:0", "", deadPlane, nil, nil)
	if err := deadPublic.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = deadPublic.Shutdown(context.Background()) })
	deadRequests := []venueoracle.Request{edgePost("Postgres unreachable", member, queryBody, nil)}
	deadPython := venue.ServePythonWithEnv(t, append(append([]string(nil), pythonEnv...),
		"POSTGRES_URI=postgresql+asyncpg://nobody:nothing@127.0.0.1:1/none"), deadRequests)
	t.Log("\n" + venueoracle.Diff(t, "http://"+deadPublic.Address(), deadRequests, deadPython, venueoracle.DiffOptions{}))
}

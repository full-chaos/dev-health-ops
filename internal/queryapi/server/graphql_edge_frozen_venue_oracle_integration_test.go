//go:build integration

package server

import (
	"context"
	"encoding/json"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// loopbackPort is the random loopback port of a run: the Python edge's typed errors name the query-api it forwarded to.
var loopbackPort = regexp.MustCompile(`127\.0\.0\.1:\d+`)

// edgeFixedToken is an HS256 access token of the recording's class with instants written in the test (valid until
// 2100, or expired in 2020) and a fixed issue time and id: the same bytes in every run. The recording sent such
// tokens for the time-bound cases (one signed with another key and valid in time, one expired) and for the cases
// that present a valid token under another scheme. The replay sends user access tokens instead and keys each
// request as the recorded one (venueoracle.KeyAs); an HS256 token reaches the plane only in the retired-class cell.
func edgeFixedToken(t *testing.T, key string, user edgeUser, expired bool) string {
	t.Helper()
	expires := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if expired {
		expires = time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	}
	claims := jwt.MapClaims{
		"sub": user.id.String(), "email": user.name + "@example.com", "org_id": user.tokenOrg, "role": user.tokenRole,
		"is_superuser": user.superuser, "type": "access", "iss": "dev-health-ops", "aud": "dev-health-api",
		"exp": expires.Unix(), "iat": time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC).Unix(), "jti": "edge-oracle-fixed-instant-token", "tv": 0,
	}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// TestGraphQLEdgeFrozenVenueOracle is the /graphql edge oracle (TestGraphQLEdgeVenueOracle) against the frozen
// answers of the real Python edge: the same requests, the same declared divergences, the same four phases
// (every registered document and the refusal domain, routing rows off, a write, an unreachable Postgres). The
// Python plane runs only while recording; a frozen run sends the requests to query-api and compares what it
// answers with the recorded Python answers (status, body, headers). The requests are the same bytes in every
// run: the venue's principals' tokens are projected to the recorded callers' claims, and the time-bound and
// other-scheme tokens are keyed as the recorded HS256 tokens (edgeFixedToken).
//
// The tokens are the live class: user access tokens of an in-test HTTPS issuer (startEdgeIssuer) that query-api
// verifies through AUTH_ACCESS_TOKEN_ISSUER, with their auth-service sessions written (WriteAuthSessions). One
// more cell, outside the golden, sends an HS256 access token: the retired class is the generic 401.
//
// The live oracle stays until the Python delete (CHAOS-7308) with one final live pass.
func TestGraphQLEdgeFrozenVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRootFromHere(t)
	users := edgeUsers()
	// The seeded principals and orgs have the shape of a generated id and are deterministic: they stay as written.
	for _, user := range users {
		seededIDs[user.id.String()] = true
	}
	seededIDs[edgeOrgA], seededIDs[edgeOrgB] = true, true
	seededIDs["0e0e0e0e-0e0e-4e0e-8e0e-0e0e0e0e0e0c"] = true
	// The Python edge's typed errors name the loopback address of the query-api it forwarded to: a random port
	// of the run. It is a placeholder in the golden and in the Go answers compared with it.
	spec := venueGolden("graphql-edge-decisions", t.Name(), "82c051c8510237b2bde6258545895ee20314678a14858d8e3284fbbb9cdb818d")
	runValues := spec.Scrub
	spec.Scrub = func(text string) string { return loopbackPort.ReplaceAllString(runValues(text), "127.0.0.1:<port>") }
	golden := venueoracle.OpenGolden(t, spec)
	docs := registeredEdgeDocuments(t, root)
	schemaDigest := digest.Schema(schemav1.SDL)

	issuer := startEdgeIssuer(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: root, JWTKey: edgeOracleJWTKey, TokenIssuer: issuer.tokens,
		Seed: edgeSeed(users, docs, schemaDigest),
	})
	// The member's tokens the seed cannot express, each keyed as the HS256 token the recording sent for it.
	expiredClaims := edgeUserClaims(users[0])
	expiredClaims.IssuedAt, expiredClaims.Lifetime = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), 24*time.Hour
	wrongKey := issuer.other.MustMint(t, edgeUserClaims(users[0]))
	expired := issuer.tokens.MustMint(t, expiredClaims)
	scheme := issuer.tokens.MustMint(t, edgeUserClaims(users[0]))
	venueoracle.KeyAs(wrongKey, edgeFixedToken(t, "another-key-another-key-another-key-32b", users[0], false))
	venueoracle.KeyAs(expired, edgeFixedToken(t, edgeOracleJWTKey, users[0], true))
	venueoracle.KeyAs(scheme, edgeFixedToken(t, edgeOracleJWTKey, users[0], false))
	venue.WriteAuthSessions(t, ctx)
	operatingReviewFixtureHasNoRows(t, ctx, venue.AdminClickHouseURI(t, venue.GoClickHouseDB))
	settings, goBase, leaveRoot := startEdgePlane(t, ctx, venue, root, issuer.url)
	suite := edgeOracleCases(t, docs, venue, wrongKey, expired, scheme)
	query, mutation, queryBody, member := suite.query, suite.mutation, suite.queryBody, suite.member

	run := func(cs []edgeCase, normalize func(venueoracle.Request, string) string) {
		python := golden.Python(t, venue, edgeRequests(cs))
		edgeCompare(t, goBase, cs, python, normalize, golden)
	}
	clock := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
	run(suite.cases, func(_ venueoracle.Request, body string) string {
		if !strings.HasPrefix(body, `{"data":{`) {
			return body
		}
		return clock.ReplaceAllString(body, "<clock>")
	})

	// A registered operation whose routing row is off, as an operator would turn it off on the Python plane
	// (recorded). The go_api_routing_state table is gone (CHAOS-8706) and query-api read no routing row since CHAOS-8702, so it serves both operations.
	mutationSpec, _ := goapiproof.SpecFor(mutation.Operation)
	run([]edgeCase{
		{request: edgePost("row off: "+query.Operation, member, queryBody, nil),
			declared: "routing row off: Python's Strawberry fallback raised for the query; query-api reads no routing row and serves it",
			pyWant:   edgeAnswer{status: 200, body: `"errors"`}, goWant: edgeAnswer{status: 200, body: `"catalog"`}},
		{request: edgePost("row off: "+mutation.Operation, member, urqlBody(t, documentOperationName(mutation.Document), mutation.Document, mutationSpec.Variables(edgeOrgA, goapiproof.DefaultWindow())), nil)},
	}, func(_ venueoracle.Request, body string) string {
		// Both planes now run the mutation and answer the same error; only the key order of the body differs.
		var decoded any
		if json.Unmarshal([]byte(body), &decoded) != nil {
			return body
		}
		canonical, err := json.Marshal(decoded)
		if err != nil {
			return body
		}
		return string(canonical)
	})

	// A write last: createSavedReport mints an id and a timestamp per call, so those are blanked; nothing else is.
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	timePattern := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?`)
	run(suite.writes, func(_ venueoracle.Request, body string) string {
		return timePattern.ReplaceAllString(uuidPattern.ReplaceAllString(body, "<uuid>"), "<time>")
	})

	// Postgres unreachable on both planes: the caller cannot be read. Python answers the unhandled 500; query-api
	// answers the guard's 503 (declared, D6666).
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
	deadPublic, _ := Listeners("127.0.0.1:0", "", deadPlane, nil, []*net.IPNet(nil))
	if err := deadPublic.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = deadPublic.Shutdown(context.Background()) })
	deadCases := []edgeCase{{request: edgePost("Postgres unreachable", member, queryBody, nil),
		declared: "caller store unavailable: Python answers the unhandled 500; query-api answers the guard's 503 (D6666)",
		pyWant:   edgeAnswer{status: 500, body: `"Internal Server Error"`},
		goWant:   edgeAnswer{status: 503, body: `"Database temporarily unavailable"`}}}
	deadPython := golden.Python(t, venue, edgeRequests(deadCases))
	edgeCompare(t, "http://"+deadPublic.Address(), deadCases, deadPython, nil, golden)

	retiredClassCell(t, goBase, queryBody, edgeFixedToken(t, edgeOracleJWTKey, users[0], false))

	leaveRoot() // the golden's file path is relative to the package directory
	golden.Finish(t)
	venueoracle.WriteGoOnlyProof(t, "Go's /graphql edge (query-api's public listener) against the frozen answers of the Python edge")
}

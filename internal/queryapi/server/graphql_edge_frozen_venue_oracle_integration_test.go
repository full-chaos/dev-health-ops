//go:build integration

package server

import (
	"context"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	schemav1 "github.com/full-chaos/dev-health-ops/contracts/graphql/v1"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/digest"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// loopbackPort is the random loopback port of a run: the Python edge's typed errors name the query-api it forwarded to.
var loopbackPort = regexp.MustCompile(`127\.0\.0\.1:\d+`)

// edgeFixedToken signs the two time-bound access tokens of the edge oracle (one signed with another key and
// valid in time, one expired) with instants written in the test: a token valid until 2100 and one that
// expired in 2020, with a fixed issue time and id. The decision a plane takes on them (accept, refuse) does not
// depend on the day the test runs, and the recorded request is the same in every run. The golden stores no
// token: a bearer token is projected to its claims.
func edgeFixedToken(t *testing.T, key string, user edgeUser, expired bool) string {
	t.Helper()
	expires := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC)
	if expired {
		expires = time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	}
	claims := jwt.MapClaims{
		"sub": user.id.String(), "email": user.name + "@example.com", "org_id": user.tokenOrg, "role": user.tokenRole,
		"is_superuser": user.superuser, "type": "access", "iss": defaultEdgeJWTIssuer, "aud": defaultEdgeJWTAudience,
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
// run: the two time-bound tokens carry fixed instants (edgeFixedToken) and the venue's principals' tokens are
// projected to their claims.
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

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: golden.PythonRoot(t, root), JWTKey: edgeOracleJWTKey,
		Seed: edgeSeed(users, docs, schemaDigest),
	})
	settings, goBase, pythonEnv, leaveRoot := startEdgePlane(t, ctx, venue, root)
	suite := edgeOracleCases(t, docs, users, venue, func(key string, user edgeUser, expired bool) string {
		return edgeFixedToken(t, key, user, expired)
	}, edgeFixedToken(t, edgeOracleJWTKey, users[0], false))
	query, mutation, queryBody, member := suite.query, suite.mutation, suite.queryBody, suite.member

	run := func(cs []edgeCase, normalize func(venueoracle.Request, string) string) {
		python := golden.PythonWithEnv(t, venue, pythonEnv, edgeRequests(cs))
		edgeCompare(t, goBase, cs, python, normalize, golden)
	}
	clock := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
	run(suite.cases, func(_ venueoracle.Request, body string) string {
		if !strings.HasPrefix(body, `{"data":{`) {
			return body
		}
		return clock.ReplaceAllString(body, "<clock>")
	})

	// A registered operation whose routing row is off: both planes' rows are turned off, as an operator
	// would. The Python plane's database exists only while recording.
	for _, database := range []string{venue.SourceDB, venue.GoDB} {
		if database == venue.SourceDB && !golden.Recording() {
			continue
		}
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

	// A write last: createSavedReport mints an id and a timestamp per call, so those are blanked; nothing else is.
	uuidPattern := regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	timePattern := regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?`)
	run(suite.writes, func(_ venueoracle.Request, body string) string {
		return timePattern.ReplaceAllString(uuidPattern.ReplaceAllString(body, "<uuid>"), "<time>")
	})

	// Postgres unreachable on both planes: the caller cannot be read, and both answer the unhandled 500.
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
	deadRequests := []venueoracle.Request{edgePost("Postgres unreachable", member, queryBody, nil)}
	deadPython := golden.PythonWithEnv(t, venue, append(append([]string(nil), pythonEnv...),
		"POSTGRES_URI=postgresql+asyncpg://nobody:nothing@127.0.0.1:1/none"), deadRequests)
	t.Log("\n" + venueoracle.Diff(t, "http://"+deadPublic.Address(), deadRequests, deadPython, venueoracle.DiffOptions{Golden: golden}))

	leaveRoot() // the golden's file path is relative to the package directory
	golden.Finish(t)
	venueoracle.WriteGoOnlyProof(t, "Go's /graphql edge (query-api's public listener) against the frozen answers of the Python edge")
}

package ingressplanes

import (
	"strings"
	"testing"
)

// routingRow is one request and the plane that must answer it.
type routingRow struct {
	name  string
	path  string // the request target as the client sends it
	plane string
}

// fixedRoutingRows are written by hand: the plane each path reaches on prod's
// api hosts with the checked-in contract. They do not read the contract, so a
// contract change that moves one of these paths fails here until the row is
// changed on purpose.
var fixedRoutingRows = []routingRow{
	// /graphql is one exact path of query-api: anchored, and any case.
	{"graphql", "/graphql", PlaneQueryAPI},
	{"graphql, upper case", "/GRAPHQL", PlaneQueryAPI},
	{"graphql, mixed case", "/GraphQL", PlaneQueryAPI},
	{"graphql, trailing slash", "/graphql/", PlaneGoAPI},
	{"graphql, a longer word", "/graphqlish", PlaneGoAPI},
	{"graphql, a path below it", "/graphql/x", PlaneGoAPI},
	{"graphql, below another path", "/x/graphql", PlaneGoAPI},
	// A query-api path with no token.
	{"query path", "/api/v1/people", PlaneQueryAPI},
	{"query path, upper case", "/API/V1/PEOPLE", PlaneQueryAPI},
	{"query path, trailing slash", "/api/v1/people/", PlaneGoAPI},
	{"query path, a longer word", "/api/v1/peoples", PlaneGoAPI},
	{"query path, one segment more", "/api/v1/people/42", PlaneGoAPI},
	// A query-api path with a token: one segment, never empty, never two.
	{"query token path", "/api/v1/people/42/metric", PlaneQueryAPI},
	{"query token path, upper case", "/API/V1/People/42/METRIC", PlaneQueryAPI},
	{"query token path, a word with a dot", "/api/v1/people/a.b/metric", PlaneQueryAPI},
	{"query token path, two segments for the token", "/api/v1/people/4/2/metric", PlaneGoAPI},
	{"query token path, trailing slash", "/api/v1/people/42/metric/", PlaneGoAPI},
	{"query token path, a longer word", "/api/v1/people/42/metrics", PlaneGoAPI},
	{"query token path, a path below it", "/api/v1/people/42/metric/x", PlaneGoAPI},
	// Paths that are a start of one another are three rules, not one prefix.
	{"investment", "/api/v1/investment", PlaneQueryAPI},
	{"investment flow", "/api/v1/investment/flow", PlaneQueryAPI},
	{"investment flow repo-team", "/api/v1/investment/flow/repo-team", PlaneQueryAPI},
	{"investment, a child with no rule", "/api/v1/investment/other", PlaneGoAPI},
	{"investment flow, a child with no rule", "/api/v1/investment/flow/other", PlaneGoAPI},
	// The Go api: its listed paths, and every path with no rule (the default).
	{"go path", "/api/v1/auth/login", PlaneGoAPI},
	{"go token path", "/api/v1/admin/orgs/7/members/9", PlaneGoAPI},
	{"health", "/health", PlaneGoAPI},
	{"root", "/", PlaneGoAPI},
	{"unknown path", "/no/such/path", PlaneGoAPI},
	{"a path no plane serves", "/docs", PlaneGoAPI},
	{"an internal path", "/api/v1/internal/acr/health", PlaneGoAPI},
}

// samplePath is a concrete request path that the rule's path matches: the
// path without its "$", `\.` as a dot, each token as one segment.
func samplePath(r Rule) string {
	path := strings.TrimSuffix(r.Path, "$")
	path = strings.ReplaceAll(path, wildcard, "x0")
	return strings.ReplaceAll(path, `\.`, ".")
}

// routingRows is the fixed rows plus four rows for every rule of the contract
// that is not the default: the rule's own path, the same path in upper case
// (regex mode ignores case), the path with a trailing slash (the default
// plane: no rule ends in a slash and a token is never empty) and the path
// with one more character.
//
// "~" is in no rule path, so the longer path can match a rule only through a
// token in the last segment; a rule that ends in a literal segment therefore
// gives the default plane, and a rule that ends in a token still matches
// itself. No other rule can match it: that rule would also match the sample,
// and the contract refuses two rules that match one path.
func routingRows(t *testing.T, contract Contract) []routingRow {
	t.Helper()
	rows := append([]routingRow(nil), fixedRoutingRows...)
	derived := 0
	for _, r := range contract.Rules {
		if r.Path == defaultPath {
			continue
		}
		sample := samplePath(r)
		longer := contract.DefaultPlane()
		if strings.HasSuffix(r.Path, "/"+wildcard+"$") {
			longer = r.Plane
		}
		rows = append(rows,
			routingRow{"rule " + r.Path, sample, r.Plane},
			routingRow{"rule " + r.Path + ", upper case", strings.ToUpper(sample), r.Plane},
			routingRow{"rule " + r.Path + ", trailing slash", sample + "/", contract.DefaultPlane()},
			routingRow{"rule " + r.Path + ", one more character", sample + "~", longer},
		)
		derived++
	}
	if derived == 0 || derived != len(contract.Rules)-1 {
		t.Fatalf("rows were made for %d of %d rules: the contract was not covered", derived, len(contract.Rules)-1)
	}
	return rows
}

// TestRoutingRowsNameBothPlanes: a table that only held default-plane rows
// would pass on a router that sends everything to the Go api.
func TestRoutingRowsNameBothPlanes(t *testing.T) {
	planes := map[string]int{}
	for _, row := range fixedRoutingRows {
		planes[row.plane]++
	}
	if planes[PlaneGoAPI] < 10 || planes[PlaneQueryAPI] < 10 {
		t.Fatalf("the fixed rows must hold both planes many times, got %v", planes)
	}
}

// TestCheckedInRouterConfigRoutesEveryPathToItsPlane is the routing test that
// needs no nginx: every row is given to nginx's location rule over the
// locations of the CHECKED-IN file, read by the test's own parser.
//
// NOT proven here (the real-nginx test proves them): that nginx accepts the
// file, nginx's own regex engine, the normalisation nginx applies to a path
// before it chooses a location, and what the plane receives.
func TestCheckedInRouterConfigRoutesEveryPathToItsPlane(t *testing.T) {
	contract := checkedInContract(t)
	file := readRouterFile(t, string(checkedInRouterConfig(t)))
	rows := routingRows(t, contract)
	checked := 0
	for _, row := range rows {
		location, ok := chooseLocation(t, file.locations, row.path)
		if !ok {
			t.Errorf("%s: %s matches no location", row.name, row.path)
			continue
		}
		if got := planeOfUpstream[location.upstream]; got != row.plane {
			t.Errorf("%s: %s reaches %q (location %s %s -> %s), want %s", row.name, row.path, got, location.modifier, location.pattern, location.upstream, row.plane)
		}
		checked++
	}
	if checked < len(fixedRoutingRows)+4 {
		t.Fatalf("only %d rows were checked", checked)
	}
}

package pdp

import "testing"

// CHAOS-13162: the GraphQL registry carries Query.experiments (its build takes the ceiling) and not Query.home, which
// is a mixed root: an allowlist decision on it is refused with allowlist_route_unsupported and the root key as its
// class (D6743 option B).
func TestRootRegistryCarriesExperimentsAndRefusesHomeAsMixed(t *testing.T) {
	if !rootCutOver["Query.experiments"] || rootCutOver["Query.home"] {
		t.Fatalf("cut over: experiments %t, home %t; want true and false", rootCutOver["Query.experiments"], rootCutOver["Query.home"])
	}
	if reason, class := allowlistRefusal("Query.home"); reason != ReasonAllowlistRouteUnsupported || class != "Query.home" {
		t.Fatalf("home refusal = %s class %q, want %s class Query.home", reason, class, ReasonAllowlistRouteUnsupported)
	}
	// A root no ruling names keeps the not-carried-out refusal until its own change cuts it over.
	if reason, class := allowlistRefusal("Query.securityOverview"); reason != ReasonNotCarriedOut || class != "" {
		t.Fatalf("securityOverview refusal = %s class %q, want %s", reason, class, ReasonNotCarriedOut)
	}
}

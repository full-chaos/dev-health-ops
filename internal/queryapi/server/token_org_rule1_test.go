package server

import (
	"net/http"
	"testing"
)

// rule1QueryCases are the org claims a caller holds no membership row for: A,
// the caller left ecOrg; B-unknown, a UUID with no organizations row;
// B-notUUID, a claim that is not a UUID (the access-token.v1 verifier refuses
// it for the user access token before any org read).
var rule1QueryCases = []struct{ name, org string }{
	{"A removed member", ecOrg.String()},
	{"B-unknown org", "88888888-8888-4888-8888-888888888888"},
	{"B-notUUID claim", "not-a-uuid"},
}

// rule1QueryStore is newAccessStore with no membership row at all; member
// keeps ecUser's row in ecOrg (the control).
func rule1QueryStore(member bool) *accessStore {
	store := newAccessStore()
	if !member {
		store.members = nil
	}
	return store
}

// Product rule 1 (D6773), CHAOS-13227, query-api REST edge: the user access
// token of a caller without a membership row in its org claim gets the legacy
// edge answer, 401 Invalid or expired token. The legacy answer is executed:
// the HS256 edge token on the unrouted verifier (Verify, CheckTokenOrg).
func TestRule1RESTTokenOrgWithoutMembershipIsTheLegacyEdge401_CHAOS13227(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	if control := newRESTFixture(t, rule1QueryStore(true), testAccess(m.keys, nil, accessTestNow), true).do(http.MethodGet,
		m.mint(ecUser, nil, nil)); control.Status != http.StatusOK {
		t.Fatalf("control: member user access token %d %s, want 200", control.Status, control.Body)
	}
	for _, c := range rule1QueryCases {
		t.Run(c.name, func(t *testing.T) {
			legacy := newRESTFixture(t, rule1QueryStore(false), nil, true).do(http.MethodGet,
				ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: c.org, role: "member", tokenVersion: 5}))
			routed := newRESTFixture(t, rule1QueryStore(false), testAccess(m.keys, nil, accessTestNow), true).do(http.MethodGet,
				m.mint(ecUser, map[string]any{"org_id": c.org}, nil))
			t.Logf("legacy=%d %s routed=%d %s", legacy.Status, legacy.Body, routed.Status, routed.Body)
			if legacy.Status != http.StatusUnauthorized || legacy.Body != restRefusal {
				t.Fatalf("legacy %d %s, want 401 %s", legacy.Status, legacy.Body, restRefusal)
			}
			if routed != legacy {
				t.Fatalf("routed %d %s, legacy %d %s", routed.Status, routed.Body, legacy.Status, legacy.Body)
			}
		})
	}
}

// Product rule 1 (D6773), CHAOS-13227, query-api /graphql on the real
// listener handlers: the same callers get the legacy answer there too, 401,
// with nothing run. The legacy answer is executed: the HS256 edge token on
// the unrouted stack.
func TestRule1GraphQLTokenOrgWithoutMembershipIsTheLegacyEdge401_CHAOS13227(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	post := func(t *testing.T, routed, member bool, bearer string) (answer, int) {
		t.Helper()
		var setup *accessSetup
		if routed {
			setup = testAccess(m.keys, nil, accessTestNow)
		}
		stack := newAccessStack(t, rule1QueryStore(member), setup, true)
		got := stack.send("query", http.MethodPost, "/graphql", "", bearer)
		return got, len(*stack.seen)
	}
	if got, ran := post(t, true, true, m.mint(ecUser, nil, nil)); got.Status != http.StatusOK || ran != 1 {
		t.Fatalf("control: member user access token %d %q ran=%d, want 200 and the operation run", got.Status, got.Body, ran)
	}
	for _, c := range rule1QueryCases {
		t.Run(c.name, func(t *testing.T) {
			legacy, legacyRan := post(t, false, false,
				ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: c.org, role: "member", tokenVersion: 5}))
			got, ran := post(t, true, false, m.mint(ecUser, map[string]any{"org_id": c.org}, nil))
			t.Logf("legacy=%d %q routed=%d %q", legacy.Status, legacy.Body, got.Status, got.Body)
			if legacy.Status != http.StatusUnauthorized || legacyRan != 0 {
				t.Fatalf("legacy %d %q ran=%d, want 401 and nothing run", legacy.Status, legacy.Body, legacyRan)
			}
			if got != legacy || ran != 0 {
				t.Fatalf("routed %d %q ran=%d, legacy %d %q", got.Status, got.Body, ran, legacy.Status, legacy.Body)
			}
		})
	}
}

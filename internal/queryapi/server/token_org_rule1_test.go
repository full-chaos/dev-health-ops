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
// edge answer, 401 Invalid or expired token. The legacy answer is the
// executed HS256 run on main 715e3f93d (log 13154-legacy-rec-rule1q-715e3f93.log
// sha256 974cf8ca8ad3, legacy= lines); the HS256 class is retired here.
func TestRule1RESTTokenOrgWithoutMembershipIsTheLegacyEdge401_CHAOS13227(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	if control := newRESTFixture(t, rule1QueryStore(true), testAccess(m.keys, nil, accessTestNow)).do(http.MethodGet,
		m.mint(ecUser, nil, nil)); control.Status != http.StatusOK {
		t.Fatalf("control: member user access token %d %s, want 200", control.Status, control.Body)
	}
	for _, c := range rule1QueryCases {
		t.Run(c.name, func(t *testing.T) {
			routed := newRESTFixture(t, rule1QueryStore(false), testAccess(m.keys, nil, accessTestNow)).do(http.MethodGet,
				m.mint(ecUser, map[string]any{"org_id": c.org}, nil))
			if routed.Status != http.StatusUnauthorized || routed.Body != restRefusal {
				t.Fatalf("routed %d %s, legacy 401 %s", routed.Status, routed.Body, restRefusal)
			}
		})
	}
}

// Product rule 1 (D6773), CHAOS-13227, query-api /graphql on the real
// listener handlers: the same callers get the legacy answer there too, 401
// {"detail":"Authentication required"}, with nothing run (the executed HS256
// run on main 715e3f93d, same log).
func TestRule1GraphQLTokenOrgWithoutMembershipIsTheLegacyEdge401_CHAOS13227(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	post := func(t *testing.T, member bool, bearer string) (answer, int) {
		t.Helper()
		stack := newAccessStack(t, rule1QueryStore(member), testAccess(m.keys, nil, accessTestNow))
		got := stack.send("query", http.MethodPost, "/graphql", "", bearer)
		return got, len(*stack.seen)
	}
	if got, ran := post(t, true, m.mint(ecUser, nil, nil)); got.Status != http.StatusOK || ran != 1 {
		t.Fatalf("control: member user access token %d %q ran=%d, want 200 and the operation run", got.Status, got.Body, ran)
	}
	for _, c := range rule1QueryCases {
		t.Run(c.name, func(t *testing.T) {
			got, ran := post(t, false, m.mint(ecUser, map[string]any{"org_id": c.org}, nil))
			if want := (answer{http.StatusUnauthorized, `{"detail":"Authentication required"}`}); got != want || ran != 0 {
				t.Fatalf("routed %d %q ran=%d, legacy %d %q and nothing run", got.Status, got.Body, ran, want.Status, want.Body)
			}
		})
	}
}

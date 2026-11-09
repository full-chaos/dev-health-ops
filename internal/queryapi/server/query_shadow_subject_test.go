package server

import (
	"net/http"
	"os"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
)

// CHAOS-12659 G6: /query hands the operation layer the bearer it authenticated, as /graphql does, so the PDP can judge
// a user access token there; the header carrier has no bearer and hands none.
func TestQueryHandsTheOperationItsBearer(t *testing.T) {
	store := newAccessStore()
	m := newAccessMinter(t)
	setup := testAccess(m.keys, nil, accessTestNow)
	auth := ecEdgeAuth(t, newQueryAPIUserStoreFrom(store))
	if err := routeAuthenticator(auth, setup.opts); err != nil {
		t.Fatal(err)
	}
	verifier, priv := iaVerifier(t)
	type seen struct {
		subject graph.ShadowSubject
		has     bool
	}
	var got []seen
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, has := graph.ShadowSubjectFrom(r.Context())
		got = append(got, seen{subject, has})
		w.WriteHeader(http.StatusOK)
	}))
	// Served through a mux: a routed token is judged by the row of the pattern the mux recorded.
	routes := http.NewServeMux()
	routes.Handle("/query", newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, newQueryAPIUserStoreFrom(store), "", nil))
	routed := routes.ServeHTTP

	user := m.mint(ecUser, nil, nil)
	envelope := iaEnvelope(t, priv, principal.Claims{OrgID: "org-env", Role: "member"})
	cells := []struct {
		name    string
		handler http.HandlerFunc
		carry   func(*http.Request)
		want    graph.ShadowSubject
		wantOK  bool
	}{
		{"user access token", routed, irBearer(user), graph.ShadowSubject{Bearer: user, UserAccessToken: true, SkipReason: policy.ShadowReasonNone}, true},
		{"envelope", routed, irBearer(envelope), graph.ShadowSubject{Bearer: envelope, SkipReason: policy.ShadowReasonEnvelope}, true},
		{"headers", routed, iaHeaders("org-hdr", "member", false, false), graph.ShadowSubject{}, false},
	}
	for _, cell := range cells {
		got = nil
		if code := iaPost(t, cell.handler, cell.carry); code != http.StatusOK || len(got) != 1 {
			t.Errorf("%s: %d, ran %d times, want 200 once", cell.name, code, len(got))
			continue
		}
		if got[0].has != cell.wantOK || got[0].subject != cell.want {
			t.Errorf("%s: subject %+v (present %v), want %+v (present %v)", cell.name, got[0].subject, got[0].has, cell.want, cell.wantOK)
		}
	}
}

// CHAOS-13126: ops_access_token_hs256 is retired. An HS256 edge token on /query, served by the routed authenticator
// as production builds it, is the generic 401 and runs no operation; the REST edge is the same class's 401 in
// TestRule1RoutedEdgeTokenIsTheRetiredClass.
func TestQueryRoutedHS256EdgeTokenIsTheRetiredClass(t *testing.T) {
	store := newAccessStore()
	m := newAccessMinter(t)
	auth := ecEdgeAuth(t, newQueryAPIUserStoreFrom(store))
	if err := routeAuthenticator(auth, testAccess(m.keys, nil, accessTestNow).opts); err != nil {
		t.Fatal(err)
	}
	verifier, _ := iaVerifier(t)
	ran := 0
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"probe": true})
	mux.Register("probe", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		ran++
		w.WriteHeader(http.StatusOK)
	}))
	routes := http.NewServeMux()
	routes.Handle("/query", newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(iaDocument): "probe"}, verifier, auth, newQueryAPIUserStoreFrom(store), "", nil))
	hs256 := ecMintHS256Token(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), tokenVersion: 5})
	if code := iaPost(t, routes.ServeHTTP, irBearer(hs256)); code != http.StatusUnauthorized || ran != 0 {
		t.Fatalf("HS256 edge token on /query: %d, operation ran %d times; want 401 and none", code, ran)
	}
	if code := iaPost(t, routes.ServeHTTP, irBearer(m.mint(ecUser, nil, nil))); code != http.StatusOK || ran != 1 {
		t.Fatalf("user access token on /query: %d, operation ran %d times; want 200 once (the cell's control)", code, ran)
	}
}

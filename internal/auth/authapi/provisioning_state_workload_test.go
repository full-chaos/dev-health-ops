package authapi

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/org"
)

// The state route takes the go-api workload token: the workload principal and its jti are the caller.
func TestOrgStateAcceptsTheGoAPIWorkloadToken(t *testing.T) {
	f := newProvWorkloadFixture(t)
	f.reactivate.status, f.reactivate.revision = org.StatusSuspended, 5
	header, _ := f.w.mint("workload+jwt", nil)
	response := f.post(stateRoute, f.stateBody(), header)
	if response.Code != http.StatusOK {
		t.Fatalf("state with the workload token: %d %s", response.Code, response.Body)
	}
	want := `"revision":5`
	if got := response.Body.String(); !strings.Contains(got, want) || !strings.Contains(got, `"status":"suspended"`) || !strings.Contains(got, f.org.String()) {
		t.Fatalf("state answer %s", got)
	}
	if f.reactivate.gets != 1 || f.reactivate.gid != f.org {
		t.Fatalf("the route read the organization %d times", f.reactivate.gets)
	}
	if f.users.reads != 0 || f.reactivate.calls+f.reactivate.suspends != 0 {
		t.Fatal("the state route read an actor or wrote")
	}
}

// The retired session issuer credential is refused on the route with no read, and counted.
func TestOrgStateRefusesTheRetiredServiceCredential(t *testing.T) {
	f := newProvWorkloadFixture(t)
	expectRefusal(t, f.post(stateRoute, f.stateBody(), map[string]string{"Authorization": "Bearer " + provisioningToken}),
		http.StatusUnauthorized, reasonInvalidCredentials)
	if got, _ := f.refusals(opOrgState); got != 1 {
		t.Fatalf("retired refusals = %d, want 1", got)
	}
	if f.w.keys.reads != 0 || f.reactivate.gets != 0 {
		t.Fatal("a retired credential read the key set or the store")
	}
}

// Wrong audience, subject, typ, other classes and malformed values, and tokens the verifier refuses, are the one 401
// with no store read; a key-set outage is 503; with the path off a workload token is the same 401.
func TestOrgStateRefusesWrongWorkloadCredentials(t *testing.T) {
	for name, build := range issuePreKeyRefusals() {
		f := newProvWorkloadFixture(t)
		expectRefusal(t, f.post(stateRoute, f.stateBody(), build(f.w)), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.reactivate.gets != 0 || f.w.keys.reads != 0 {
			t.Fatalf("%s: read before the verdict", name)
		}
	}
	for name, build := range issueVerifierRefusals() {
		f := newProvWorkloadFixture(t)
		expectRefusal(t, f.post(stateRoute, f.stateBody(), build(f.w)), http.StatusUnauthorized, reasonInvalidCredentials)
		if f.reactivate.gets != 0 {
			t.Fatalf("%s: a refused token reached a store", name)
		}
	}
	f := newProvWorkloadFixture(t)
	f.w.keys.err = errors.New("custody down")
	header, _ := f.w.mint("workload+jwt", nil)
	expectRefusal(t, f.post(stateRoute, f.stateBody(), header), http.StatusServiceUnavailable, reasonDependency)
	if f.reactivate.gets != 0 {
		t.Fatal("a key-set outage reached the store")
	}
	w := newWorkloadIssueFixture(t)
	g := newProvFixtureOff(t, docWithRow(t, PatternOrgSuspend))
	header, _ = w.mint("workload+jwt", nil)
	expectRefusal(t, g.post(stateRoute, g.stateBody(), header), http.StatusUnauthorized, reasonInvalidCredentials)
	if g.reactivate.gets != 0 || w.keys.reads != 0 {
		t.Fatal("a workload token with the path off reached a store")
	}
}

// With a workload token the route answers an unknown organization 404 and a body that is not one organization_id 400.
func TestOrgStateWorkloadBodyAndNotFound(t *testing.T) {
	f := newProvWorkloadFixture(t)
	f.reactivate.getErr = org.ErrNotFound
	header, _ := f.w.mint("workload+jwt", nil)
	expectRefusal(t, f.post(stateRoute, f.stateBody(), header), http.StatusNotFound, reasonOrganizationNotFound)
	header, _ = f.w.mint("workload+jwt", nil)
	expectRefusal(t, f.post(stateRoute, `{"organization_id":"x"}`, header), http.StatusBadRequest, reasonMalformed)
}

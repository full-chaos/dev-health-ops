package apiservice

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/google/uuid"
)

// TestDHOAPICorpusEntriesAreMountedRoutes ties the REST prover's dho-api
// corpus to the real mux: an entry whose (method, path) this service does not
// mount would be sent to a route that answers 404 and read as a refusal on
// every run. The fully wired route set (Deps with a Guard) is used, so the
// guarded areas count as mounted.
func TestDHOAPICorpusEntriesAreMountedRoutes(t *testing.T) {
	verifier, err := edgetoken.New(strings.Repeat("fixture-key-", 3), "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	auth, err := policy.NewAuthenticator(verifier, scopeStore{admin: uuid.New(), target: uuid.New(), targetOrg: uuid.New()}, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	mounted := map[string]bool{}
	for _, route := range Routes(Deps{Auth: auth, Guard: policy.NewGuard(auth, quietLogger())}, quietLogger()) {
		mounted[route.Method+" "+route.Pattern] = true
	}

	checked := 0
	for _, operation := range goapiproof.KnownRESTOperations() {
		spec, err := goapiproof.SpecForREST(operation)
		if err != nil {
			t.Fatal(err)
		}
		if spec.EffectiveService() != goapiproof.RESTServiceDHOAPI {
			continue
		}
		checked++
		if key := spec.Method + " " + spec.Path; !mounted[key] {
			t.Errorf("dho-api corpus entry %s is not a route this service mounts (%s)", operation, key)
		}
	}
	if checked == 0 {
		t.Fatal("the corpus has no dho-api entry to check")
	}
}

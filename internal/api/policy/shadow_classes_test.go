package policy

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
)

// A principal with more readable repositories than an allowlist holds is denied by the PDP
// (repository_allowlist_too_large) where the legacy guard allows: a known, reviewed difference.
func TestShadowClassOfAnAllowlistOverTheBoundIsIntentional(t *testing.T) {
	rules, err := LoadShadowClasses(shadowClassTable)
	if err != nil {
		t.Fatal(err)
	}
	pdp := newTestPDP(t, denyWith("repository_allowlist_too_large"))
	d := pdp.client(t).Decide(context.Background(), pep.Request{PrincipalReference: shadowBearer, Action: "teams.read",
		ResourceType: "organization", ResourceKey: ownOrg, OrganizationID: ownOrg})
	if d.Allowed() || string(d.Reason()) != "repository_allowlist_too_large" {
		t.Fatalf("decision %v %s, want the wire deny", d.Allowed(), d.Reason())
	}
	s := &Shadow{classes: rules}
	for _, ns := range shadowNamespaces {
		if got := s.classify(true, d, ns); got != "intentional" {
			t.Errorf("namespace %s: class %q, want intentional", ns, got)
		}
	}
}

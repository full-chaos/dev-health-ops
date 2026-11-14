package policy

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
)

func allowAsPlatformAdmin(string) (bool, string) { return true, "allowed_by_platform_admin" }

// The reason auth-service sends for a platform admin's allow reaches go-api's enforcer as an allow: the real pep client
// reads allowed_by_platform_admin, and the cell answers as it does for allowed_by_role (200).
func TestAnEnforcedCellTakesThePlatformAdminAllowReasonFromTheWire(t *testing.T) {
	for _, cell := range enforceCells {
		t.Run(cell.namespace, func(t *testing.T) {
			recordEnforce(t)
			allow := newTestPDP(t, allowAsPlatformAdmin)
			got := enforceRequest(enforceHandler(t, personaStore(personaMember), userClaims(nil), newEnforcerFor(t, allow.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status != http.StatusOK {
				t.Fatalf("PDP allow by allowed_by_platform_admin: %d %q, want 200 (not a malformed decision)", got.status, got.body)
			}
		})
	}
}

// The same code on a DENY stays malformed: it is an allow reason only, so the cell is refused, never allowed.
func TestAPlatformAdminReasonOnADenyIsRefusedByTheEnforcer(t *testing.T) {
	for _, cell := range enforceCells {
		t.Run(cell.namespace, func(t *testing.T) {
			recordEnforce(t)
			bad := newTestPDP(t, func(string) (bool, string) { return false, "allowed_by_platform_admin" })
			got := enforceRequest(enforceHandler(t, personaStore(personaSuperuser), userClaims(nil), newEnforcerFor(t, bad.client(t), onFor(cell.namespace))), cell.method, cell.path)
			if got.status == http.StatusOK {
				t.Fatalf("a deny naming allowed_by_platform_admin was served: %d %q", got.status, got.body)
			}
		})
	}
}

// The shadow reads the code as an allow and classifies a legacy deny against it like any other allow: unresolved (no
// row names an allow), never a refusal of the table.
func TestShadowClassifiesAPlatformAdminAllowReasonLikeAnyAllow(t *testing.T) {
	rules, err := LoadShadowClasses(shadowClassTable)
	if err != nil {
		t.Fatal(err)
	}
	pdp := newTestPDP(t, allowAsPlatformAdmin)
	d := pdp.client(t).Decide(context.Background(), pep.Request{PrincipalReference: shadowBearer, Action: "teams.read",
		ResourceType: "organization", ResourceKey: ownOrg, OrganizationID: ownOrg})
	if !d.Allowed() || string(d.Reason()) != "allowed_by_platform_admin" {
		t.Fatalf("decision %v %s, want an allow by allowed_by_platform_admin", d.Allowed(), d.Reason())
	}
	s := &Shadow{classes: rules}
	for _, ns := range shadowNamespaces {
		if got := s.classify(false, d, ns); got != classUnresolved {
			t.Errorf("namespace %s: class %q, want %s", ns, got, classUnresolved)
		}
	}
}

// A class row may NOT name allowed_by_platform_admin while the Go client holds it ahead of the contract (pep.WireReasons
// lacks it): the table is refused. A "*" row is how a row would cover it today.
func TestAClassRowCannotNameTheAcceptAheadReasonButAStarRowCovers(t *testing.T) {
	named := shadowClassHeader + "\ndeny\tallow\tallowed_by_platform_admin\t*\tintentional\tfixture\n"
	if _, err := LoadShadowClasses([]byte(named)); err == nil || !strings.Contains(err.Error(), "not a PDP reason code") {
		t.Fatalf("a row naming allowed_by_platform_admin: err %v, want a refusal while the code is accepted ahead of the contract", err)
	}
	star := shadowClassHeader + "\ndeny\tallow\t*\t*\tintentional\tfixture\n"
	rules, err := LoadShadowClasses([]byte(star))
	if err != nil {
		t.Fatalf("a star row: %v", err)
	}
	pdp := newTestPDP(t, allowAsPlatformAdmin)
	d := pdp.client(t).Decide(context.Background(), pep.Request{PrincipalReference: shadowBearer, Action: "teams.read",
		ResourceType: "organization", ResourceKey: ownOrg, OrganizationID: ownOrg})
	if got := (&Shadow{classes: rules}).classify(false, d, "teams"); got != "intentional" {
		t.Fatalf("class %q, want intentional through the star row", got)
	}
}

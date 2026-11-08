package authorization

import (
	"reflect"
	"testing"
)

// acr.credential.issue is repository_scope filtered (catalog 18): device approval and consent take their
// repository scopes from this decision. One rule gives every caller its shape: the membership default and a live
// wildcard grant are org-wide (repository_scope organization), exact grants are the named allowlist, and the
// platform admin with no membership row is org-wide. Every organization role gets the org scope by default (rule 2).
func TestAcrCredentialIssueAllowCarriesTheScopeOfItsCaller(t *testing.T) {
	e := NewEvaluator()
	req := orgRequest("acr.credential.issue")
	orgWide := &Obligations{TenantScope: testOrg, RepositoryScope: RepositoryScopeOrganization}
	named := &Obligations{TenantScope: testOrg, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api", "acme/web"}}
	cells := []struct {
		name string
		snap Snapshot
		want *Obligations
	}{
		{"viewer, membership default", repoSnapshot("viewer"), orgWide},
		{"member, membership default", repoSnapshot("member"), orgWide},
		{"admin, membership default", repoSnapshot("admin"), orgWide},
		{"owner, membership default", repoSnapshot("owner"), orgWide},
		{"member, live wildcard grant", repoSnapshot("member", liveGrant("*")), orgWide},
		{"member, exact grants", repoSnapshot("member", liveGrant("acme/web"), liveGrant("acme/api")), named},
		{"platform admin, no membership row", grantFake{fakeSnapshot: platformAdminSnapshot("platform_admin", "", StatusActive), now: nowNano}, orgWide},
	}
	for _, c := range cells {
		got := e.Evaluate(req, c.snap)
		if !got.Allowed {
			t.Errorf("%s: %+v, want allow", c.name, got)
			continue
		}
		o := *got.Obligations
		o.Redaction = nil
		if !reflect.DeepEqual(&o, c.want) {
			t.Errorf("%s: obligations %+v, want %+v", c.name, got.Obligations, c.want)
		}
		if got.CacheClass != "" || got.ExpiresAtUnixNano != 0 {
			t.Errorf("%s: cache %q expires %d, want none of its own (catalog class none: never cached)", c.name, got.CacheClass, got.ExpiresAtUnixNano)
		}
	}
}

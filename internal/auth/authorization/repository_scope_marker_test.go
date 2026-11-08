package authorization

import (
	"reflect"
	"testing"
)

// A repository allow that is org-wide in fact carries repository_scope organization and no allowlist: the
// membership default (no grant row of the pair) and a live wildcard grant. An allow by exact grants keeps the
// allowlist of the asked key. The marker is the one source a consumer reads to know the allow covers every
// repository of the organization.
func TestRepositoryAllowCarriesTheOrganizationScopeWhenOrgWideInFact(t *testing.T) {
	e := NewEvaluator()
	for _, action := range []string{"acr.context.read", "acr.evidence.read"} {
		req := Request{Action: action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
		orgWide := &Obligations{TenantScope: testOrg, RepositoryScope: RepositoryScopeOrganization}
		exact := &Obligations{TenantScope: testOrg, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api"}}
		cells := []struct {
			name string
			snap grantFake
			want *Obligations
		}{
			{"membership default (no grant row)", repoSnapshot("viewer"), orgWide},
			{"live wildcard grant", repoSnapshot("viewer", liveGrant("*")), orgWide},
			{"live wildcard grant listed after an exact grant", repoSnapshot("viewer", liveGrant("acme/api"), liveGrant("*")), orgWide},
			{"exact grant only", repoSnapshot("viewer", liveGrant("acme/api")), exact},
		}
		for _, c := range cells {
			got := e.Evaluate(req, c.snap)
			if !got.Allowed {
				t.Errorf("%s %s: %+v, want allow", action, c.name, got)
				continue
			}
			o := *got.Obligations
			o.Redaction = nil
			if !reflect.DeepEqual(&o, c.want) {
				t.Errorf("%s %s: obligations %+v, want %+v", action, c.name, got.Obligations, c.want)
			}
		}
	}
}

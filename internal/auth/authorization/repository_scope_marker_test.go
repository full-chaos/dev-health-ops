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

// A delegated (impersonation) repository allow covers the asked repository only: the org-wide marker of the
// subject's own allow never reaches the delegated session (one key, no repository_scope).
func TestDelegatedRepositoryAllowCarriesOnlyTheAskedKey(t *testing.T) {
	e := NewEvaluator()
	for _, action := range []string{"acr.context.read", "acr.evidence.read"} {
		req := Request{Action: action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
		d := e.Evaluate(req, delegated("viewer", []string{action}, ""))
		if !d.Allowed || d.Obligations == nil || d.Obligations.RepositoryScope != "" || !d.Obligations.RepositoryRestricted ||
			!reflect.DeepEqual(d.Obligations.RepositoryAllowlist, []string{"acme/api"}) {
			t.Errorf("%s delegated viewer (membership default): %+v %+v, want the one key and no repository_scope", action, d, d.Obligations)
		}
	}
}

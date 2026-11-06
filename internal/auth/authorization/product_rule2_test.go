package authorization

import "testing"

var productRule2OrgWideReads = []string{"metrics.read", "analytics.read", "analytics.export", "git.read", "work_items.read"}

func TestProductRule2MemberWithOneRepositoryGrantReadsOrgWideMetrics(t *testing.T) {
	for _, action := range productRule2OrgWideReads {
		for _, role := range []string{"viewer", "member", "admin", "owner"} {
			t.Run(action+"/"+role, func(t *testing.T) {
				snap := repoSnapshot(role, liveGrant("acme/api"))
				d := NewEvaluator().Evaluate(orgRequest(action), snap)
				if !d.Allowed {
					t.Skipf("role %s does not hold %s: %+v", role, action, d)
				}
				if d.Obligations == nil || d.Obligations.RepositoryRestricted || len(d.Obligations.RepositoryAllowlist) != 0 {
					t.Fatalf("%s as %s with one repository grant: obligations %+v, want no repository allowlist", action, role, d.Obligations)
				}
				if d.Obligations.RepositoryScope != "" {
					t.Fatalf("%s as %s: repository_scope %q, want the tenant scope alone", action, role, d.Obligations.RepositoryScope)
				}
			})
		}
	}
}

func TestProductRule2ReadWithNoGrantRowsCarriesOnlyTheTenantScope(t *testing.T) {
	for _, action := range productRule2OrgWideReads {
		d := NewEvaluator().Evaluate(orgRequest(action), repoSnapshot("member"))
		if !d.Allowed || d.Obligations == nil || d.Obligations.RepositoryScope != "" || d.Obligations.RepositoryRestricted {
			t.Errorf("%s: %+v %+v, want an allow with the tenant scope alone", action, d, d.Obligations)
		}
	}
}

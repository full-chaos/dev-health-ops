package authorization

import "testing"

func TestDelegation_RepositoryAllowIsMarkedRestricted(t *testing.T) {
	e := plantedEvaluator()
	got := e.Evaluate(repoRequest("acme/api"), delegatedRepo([]string{repoAction}, liveGrant("acme/api")))
	if !got.Allowed || got.Obligations == nil {
		t.Fatalf("%+v", got)
	}
	if !got.Obligations.RepositoryRestricted {
		t.Fatalf("a repository allow names its one key and is restricted: %+v", got.Obligations)
	}
}

func TestDelegation_OrganizationAllowIsNotRepositoryRestricted(t *testing.T) {
	e := NewEvaluator()
	got := e.Evaluate(orgRequest("org.read"), delegated("viewer", allOrgActions(t), ""))
	if !got.Allowed || got.Obligations == nil {
		t.Fatalf("%+v", got)
	}
	if got.Obligations.RepositoryRestricted || len(got.Obligations.RepositoryAllowlist) != 0 {
		t.Fatalf("an organization allow has no repository member: %+v", got.Obligations)
	}
}

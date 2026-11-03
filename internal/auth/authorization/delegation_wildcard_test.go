package authorization

import "testing"

func TestDelegation_WildcardRepositoryKeyIsRefused(t *testing.T) {
	e := plantedEvaluator()
	for _, actorRole := range []string{"", ownerActorRol} {
		for _, grants := range [][]Grant{{liveGrant("*")}, {liveGrant("acme/api")}, nil} {
			s := delegatedRepo([]string{repoAction}, grants...)
			s.d.ActorPlatformRole = actorRole
			got := e.Evaluate(repoRequest("*"), s)
			if got.Allowed || string(got.Reason) != "impersonation_denied" || string(got.CacheClass) != "none" {
				t.Errorf("actor %q grants %v: %+v want deny impersonation_denied, cache none", actorRole, grants, got)
			}
			if got.Obligations != nil || got.Audit != nil {
				t.Errorf("actor %q: a refusal carries obligations or audit: %+v", actorRole, got)
			}
			if got.PolicyRevision == 0 {
				t.Errorf("actor %q: no policy revision on the refusal", actorRole)
			}
		}
	}
}

func TestDelegation_ConcreteRepositoryKeyIsUnchangedByTheWildcardRule(t *testing.T) {
	e := plantedEvaluator()
	for _, actorRole := range []string{"", ownerActorRol} {
		s := delegatedRepo([]string{repoAction}, liveGrant("*"))
		s.d.ActorPlatformRole = actorRole
		got := e.Evaluate(repoRequest("acme/api"), s)
		if !got.Allowed || got.Obligations == nil || len(got.Obligations.RepositoryAllowlist) != 1 || got.Obligations.RepositoryAllowlist[0] != "acme/api" {
			t.Errorf("actor %q: %+v", actorRole, got)
		}
	}
}

func TestDelegation_NonDelegatedWildcardRequestIsUnchanged(t *testing.T) {
	got := plantedEvaluator().Evaluate(repoRequest("*"), repoSnapshot("member", liveGrant("*")))
	if !got.Allowed || got.Obligations != nil || got.Audit != nil {
		t.Fatalf("%+v", got)
	}
}

func TestDelegation_WildcardRepositoryKeyOfAnotherOrganizationStaysInvalid(t *testing.T) {
	req := repoRequest("*")
	req.OrganizationID = "org-2"
	got := plantedEvaluator().Evaluate(req, delegatedRepo([]string{repoAction}, liveGrant("*")))
	if got.Allowed || string(got.Reason) != "delegation_invalid" {
		t.Fatalf("%+v", got)
	}
}

func TestDelegation_OrganizationRequestIsUnchangedByTheWildcardRule(t *testing.T) {
	got := NewEvaluator().Evaluate(orgRequest("org.read"), delegated("viewer", allOrgActions(t), ownerActorRol))
	if !got.Allowed || got.Obligations == nil || len(got.Obligations.RepositoryAllowlist) != 0 {
		t.Fatalf("%+v", got)
	}
}

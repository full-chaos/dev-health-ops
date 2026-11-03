package authorization

import "testing"

func platformWorkload(aud string, auds, acts []string) grantFake {
	s := grantFake{fakeSnapshot: platformSnapshot("platform_admin"), now: nowNano}
	s.workload = &WorkloadLimits{Audience: aud, Audiences: auds, AllowedActions: acts}
	return s
}

// The registry row is the workload's whole allowed set: it narrows platform
// actions too, not only organization and repository ones.
func TestWorkloadLimits_NarrowPlatformActions(t *testing.T) {
	e := plantedEvaluator()
	if d := e.Evaluate(platformRequest("admin.users"), platformWorkload("svc-a", []string{"svc-a"}, []string{"admin.users"})); !d.Allowed {
		t.Fatalf("listed platform action: %+v", d)
	}
	wantDeny(t, "platform action outside the registry row", e.Evaluate(platformRequest("admin.users"),
		platformWorkload("svc-a", []string{"svc-a"}, []string{"org.read"})), ReasonWorkloadActionDenied)
	wantDeny(t, "platform action, no allowed actions", e.Evaluate(platformRequest("admin.users"),
		platformWorkload("svc-a", []string{"svc-a"}, nil)), ReasonWorkloadActionDenied)
	wantDeny(t, "platform action, wrong audience", e.Evaluate(platformRequest("admin.users"),
		platformWorkload("svc-z", []string{"svc-a"}, []string{"admin.users"})), ReasonWorkloadAudienceDenied)
	wantDeny(t, "platform action, empty audience", e.Evaluate(platformRequest("admin.users"),
		platformWorkload("", []string{"svc-a"}, []string{"admin.users"})), ReasonWorkloadAudienceDenied)
}

func TestWorkloadLimits_NarrowOrganizationActions(t *testing.T) {
	e := plantedEvaluator()
	org := func(aud string, auds, acts []string) grantFake {
		s := repoSnapshot("owner")
		s.workload = &WorkloadLimits{Audience: aud, Audiences: auds, AllowedActions: acts}
		return s
	}
	if d := e.Evaluate(orgRequest("org.read"), org("svc-a", []string{"svc-a"}, []string{"org.read"})); !d.Allowed {
		t.Fatalf("listed organization action: %+v", d)
	}
	wantDeny(t, "organization action outside the registry row", e.Evaluate(orgRequest("org.read"),
		org("svc-a", []string{"svc-a"}, []string{"org.write"})), ReasonWorkloadActionDenied)
	wantDeny(t, "organization action, no allowed actions", e.Evaluate(orgRequest("org.read"),
		org("svc-a", []string{"svc-a"}, nil)), ReasonWorkloadActionDenied)
	wantDeny(t, "organization action, wrong audience", e.Evaluate(orgRequest("org.read"),
		org("svc-z", []string{"svc-a"}, []string{"org.read"})), ReasonWorkloadAudienceDenied)
}

// A grant gives nothing without an active membership in its organization.
func TestRepoGrant_LiveGrantWithoutMembershipDenies(t *testing.T) {
	e := plantedEvaluator()
	for _, key := range []string{"acme/api", "*"} {
		s := repoSnapshot("member", liveGrant(key))
		s.memberships = map[string]Membership{}
		wantDeny(t, "grant "+key+", no membership", e.Evaluate(repoRequest("acme/api"), s), ReasonNoMembership)
		wantDeny(t, "grant "+key+", org wide request, no membership", e.Evaluate(repoRequest("*"), s), ReasonNoMembership)
	}
}

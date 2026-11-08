package authorization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The two ACR repository reads that take the platform-admin flag. Legacy (query-api GraphQL catalog,
// internal/queryapi/graph/schema.resolvers.go) answers a superuser with no membership row in the organization
// of its session, so the decision point uses the organization-branch rule for them, unchanged.
var platformAdminRepositoryActions = []string{"acr.context.read", "acr.evidence.read"}

// catalogWithRepositoryFlags is the embedded catalog with platform_admin allowed on the two ACR repository reads.
func catalogWithRepositoryFlags(t *testing.T) *policycatalog.Catalog {
	t.Helper()
	var actions map[string]any
	if err := json.Unmarshal(vocabulary.ActionsV1, &actions); err != nil {
		t.Fatal(err)
	}
	flagged := map[string]bool{}
	for _, a := range platformAdminRepositoryActions {
		flagged[a] = true
	}
	for _, raw := range actions["actions"].([]any) {
		a := raw.(map[string]any)
		if flagged[a["action"].(string)] {
			a["platform_admin"] = "allowed"
		}
	}
	b, err := json.Marshal(actions)
	if err != nil {
		t.Fatal(err)
	}
	c, err := policycatalog.Load(b, vocabulary.RolesV1)
	if err != nil {
		t.Fatalf("a repository action with platform_admin allowed must load: %v", err)
	}
	return c
}

func platformAdminRepoSnapshot(platformRole string, org Status) grantFake {
	return grantFake{fakeSnapshot: platformAdminSnapshot(platformRole, "", org), now: nowNano}
}

func repositoryRequestFor(action string) Request {
	return Request{Action: action, ResourceKind: "repository", OrganizationID: testOrg, ResourceKey: "acme/api"}
}

// A holder of admin.orgs with no membership row reads the flagged repository actions org-wide in an active,
// suspended or unknown organization, exactly as the organization branch admits it.
func TestPlatformAdminReadsAFlaggedRepositoryActionOrgWide(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	for _, action := range platformAdminRepositoryActions {
		for _, org := range []Status{StatusActive, StatusSuspended, StatusUnknown} {
			got := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("platform_admin", org))
			want := &Obligations{TenantScope: testOrg, RepositoryScope: RepositoryScopeOrganization}
			if !got.Allowed || got.Reason != ReasonAllowedByPlatformAdmin || !reflect.DeepEqual(got.Obligations, want) {
				t.Errorf("%s org %v: %+v (obligations %+v), want allow %s with %+v", action, org, got, got.Obligations,
					ReasonAllowedByPlatformAdmin, want)
			}
			// acp A: the stricter of the organization-branch platform-admin allow and the action's own class. The
			// organization-branch allow leaves the class to the catalog and sets no expiry; for a resource_revision_bound
			// action that is never cached (authapi decisionBound is false, expires_at = the decision time), which is
			// stricter than the 30 s repository class. So a platform-role revoke ends the allow at the next decision.
			if got.Allowed && (got.CacheClass != "" || got.ExpiresAtUnixNano != 0) {
				t.Errorf("%s org %v: cache %q expires %d, want the organization-branch shape (no class, no expiry)", action, org,
					got.CacheClass, got.ExpiresAtUnixNano)
			}
		}
	}
}

// The rule is the organization branch's, nothing more: a removed organization, a platform role without
// admin.orgs, and an unflagged repository action keep the answers they have without the rule.
func TestPlatformAdminRepositoryRuleIsTheOrganizationBranchPredicate(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	for _, action := range platformAdminRepositoryActions {
		if got := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("platform_admin", StatusRemoved)); got.Allowed {
			t.Errorf("%s removed org: %+v, want deny", action, got)
		}
		if got := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("platform_owner", StatusActive)); got.Allowed ||
			got.Reason != ReasonNoMembership {
			t.Errorf("%s platform_owner (no admin.orgs): %+v, want deny no_membership", action, got)
		}
	}
}

// The org-branch predicate is the one rule: for every organization status the repository answer of a no-row
// admin.orgs holder on a flagged repository action equals the organization-branch answer on a flagged
// organization action (allowed or not).
func TestPlatformAdminRepositoryAnswerEqualsTheOrganizationBranchAnswer(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	for _, org := range []Status{StatusActive, StatusSuspended, StatusUnknown, StatusRemoved} {
		for _, role := range []string{"platform_admin", "platform_owner", ""} {
			orgAnswer := e.Evaluate(orgRequest("git.read"), platformAdminSnapshot(role, "", org)).Allowed
			for _, action := range platformAdminRepositoryActions {
				if got := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot(role, org)).Allowed; got != orgAnswer {
					t.Errorf("%s role %q org %v: repository allowed=%v, organization branch allowed=%v", action, role, org, got, orgAnswer)
				}
			}
		}
	}
}

// Impersonation (known flaw F1: no new widening): an admin.orgs holder who impersonates a user with no
// membership row gets no platform-admin allow on the flagged repository reads; the actor's platform role
// never widens the effective subject, and a delegated acr.credential.issue stays impersonation-denied.
func TestImpersonatingPlatformAdminGetsNoRepositoryAllow(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	all := append(append([]string{}, platformAdminRepositoryActions...), "acr.credential.issue")
	for _, action := range platformAdminRepositoryActions {
		s := delegatedSnapshot{
			grantFake: platformAdminRepoSnapshot("", StatusActive),
			d: Delegation{Chain: []Hop{{ActorPrincipalID: testActor, PermittedActions: all}}, ActorPlatformRole: "platform_admin",
				TargetOrganizationID: testOrg, Assurance: "aal1"},
			on: true,
		}
		if got := e.Evaluate(repositoryRequestFor(action), s); got.Allowed {
			t.Errorf("%s impersonating a no-row user as platform_admin: %+v, want deny", action, got)
		}
	}
	if got := e.Evaluate(orgRequest("acr.credential.issue"), delegated("owner", all, "platform_admin")); got.Allowed {
		t.Errorf("acr.credential.issue delegated: %+v, want deny (impersonation denied)", got)
	}
}

// The flag is opt-in per repository action: with only acr.context.read flagged, the same caller is still
// denied no_membership on acr.evidence.read.
func TestPlatformAdminRepositoryRuleNeedsTheFlagPerAction(t *testing.T) {
	var actions map[string]any
	if err := json.Unmarshal(vocabulary.ActionsV1, &actions); err != nil {
		t.Fatal(err)
	}
	for _, raw := range actions["actions"].([]any) {
		if a := raw.(map[string]any); a["action"] == "acr.context.read" {
			a["platform_admin"] = "allowed"
		}
	}
	b, err := json.Marshal(actions)
	if err != nil {
		t.Fatal(err)
	}
	c, err := policycatalog.Load(b, vocabulary.RolesV1)
	if err != nil {
		t.Fatal(err)
	}
	e := newEvaluator(c)
	if got := e.Evaluate(repositoryRequestFor("acr.context.read"), platformAdminRepoSnapshot("platform_admin", StatusActive)); !got.Allowed {
		t.Errorf("flagged acr.context.read: %+v, want allow", got)
	}
	if got := e.Evaluate(repositoryRequestFor("acr.evidence.read"), platformAdminRepoSnapshot("platform_admin", StatusActive)); got.Allowed ||
		got.Reason != ReasonNoMembership {
		t.Errorf("unflagged acr.evidence.read: %+v, want deny no_membership", got)
	}
}

// The repository allow has the cache fields of the organization-branch platform-admin allow of an organization
// action with the same catalog class (git.read, resource_revision_bound).
func TestPlatformAdminRepositoryAllowCachesLikeTheOrganizationBranchAllow(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	org := e.Evaluate(orgRequest("git.read"), platformAdminSnapshot("platform_admin", "", StatusActive))
	if !org.Allowed || org.Reason != ReasonAllowedByPlatformAdmin {
		t.Fatalf("organization branch git.read: %+v, want allowed_by_platform_admin", org)
	}
	for _, action := range platformAdminRepositoryActions {
		got := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("platform_admin", StatusActive))
		if got.CacheClass != org.CacheClass || got.ExpiresAtUnixNano != org.ExpiresAtUnixNano {
			t.Errorf("%s: cache %q expires %d, organization branch %q %d", action, got.CacheClass, got.ExpiresAtUnixNano,
				org.CacheClass, org.ExpiresAtUnixNano)
		}
	}
}

// A platform-role revoke ends the allow: the next decision of the same caller denies, and the allow it replaces
// carried no expiry, so no consumer may serve it from a cache.
func TestPlatformAdminRepositoryAllowEndsAtThePlatformRoleRevoke(t *testing.T) {
	e := newEvaluator(catalogWithRepositoryFlags(t))
	for _, action := range platformAdminRepositoryActions {
		before := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("platform_admin", StatusActive))
		if !before.Allowed || before.ExpiresAtUnixNano != 0 {
			t.Errorf("%s before the revoke: %+v, want an allow with no expiry", action, before)
		}
		after := e.Evaluate(repositoryRequestFor(action), platformAdminRepoSnapshot("", StatusActive))
		if after.Allowed || after.Reason != ReasonNoMembership || after.CacheClass != CacheNone {
			t.Errorf("%s after the revoke: %+v, want deny no_membership, not cacheable", action, after)
		}
	}
}

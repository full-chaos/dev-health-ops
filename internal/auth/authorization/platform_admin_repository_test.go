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
			if got.Allowed && (got.CacheClass != CacheResourceRevisionBound || got.ExpiresAtUnixNano == 0) {
				t.Errorf("%s org %v: cache %s expires %d, want the repository read class", action, org, got.CacheClass, got.ExpiresAtUnixNano)
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

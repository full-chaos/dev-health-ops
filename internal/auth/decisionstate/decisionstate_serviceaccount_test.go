package decisionstate

import (
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// pm Q2: a service-account principal with its row is an active principal; the row carries the account's state, active only
// when the row and the principal both say so. A disabled account is service_account_inactive, never principal_inactive.
func TestServiceAccountRuleIsActiveOnlyWhenRowAndPrincipalAre(t *testing.T) {
	org := uuid.New()
	for _, tc := range []struct {
		status  string
		enabled bool
		want    authorization.Status
	}{
		{"active", true, authorization.StatusActive},
		{"disabled", true, authorization.StatusSuspended},
		{"active", false, authorization.StatusSuspended},
		{"disabled", false, authorization.StatusSuspended},
		{"Active", true, authorization.StatusSuspended},
		{"", true, authorization.StatusSuspended},
	} {
		s := &snapshot{org: org.String()}
		s.applyServiceAccount(org, tc.status, tc.enabled)
		if s.PrincipalStatus() != authorization.StatusActive {
			t.Errorf("status %q enabled %v: principal %v, want active", tc.status, tc.enabled, s.PrincipalStatus())
		}
		sa, found := s.ServiceAccount(org.String())
		if !found || sa.OrgID != org.String() || sa.Status != tc.want {
			t.Errorf("status %q enabled %v: service account %+v %v, want %v in %s", tc.status, tc.enabled, sa, found, tc.want, org)
		}
		if s.PlatformRole() != "" {
			t.Errorf("status %q enabled %v: platform role %q, want none", tc.status, tc.enabled, s.PlatformRole())
		}
	}
}

// The account row answers for every organization asked about with its own organization, so a request of another
// organization is org_mismatch; a platform decision has no account row.
func TestServiceAccountSnapshotCrossOrgIsOrgMismatch(t *testing.T) {
	own, other := uuid.New(), uuid.New()
	e := authorization.NewEvaluator()
	req := func(org uuid.UUID) authorization.Request {
		return authorization.Request{Action: "acr.context.read", ResourceKind: "repository", OrganizationID: org.String(), ResourceKey: "acme/api"}
	}
	grant := func(org uuid.UUID) []authorization.Grant {
		return []authorization.Grant{{OrgID: org.String(), Role: "viewer", ResourceKind: "repository", ResourceKey: "acme/api"}}
	}
	at := &snapshot{org: other.String(), orgStatus: authorization.StatusActive, grants: grant(other), now: 1}
	at.applyServiceAccount(own, "active", true)
	if d := e.Evaluate(req(other), at); d.Allowed || d.Reason != authorization.ReasonOrgMismatch {
		t.Fatalf("another organization: %+v, want org_mismatch", d)
	}
	home := &snapshot{org: own.String(), orgStatus: authorization.StatusActive, grants: grant(own), now: 1}
	home.applyServiceAccount(own, "active", true)
	if d := e.Evaluate(req(own), home); !d.Allowed || d.Reason != authorization.ReasonAllowedByRepositoryGrant {
		t.Fatalf("own organization: %+v, want allowed_by_repository_grant", d)
	}
	if _, found := home.ServiceAccount(""); found {
		t.Fatal("a platform decision found a service-account row")
	}
}

// A user principal has no service-account row.
func TestAUserSnapshotHasNoServiceAccountRow(t *testing.T) {
	s := &snapshot{org: uuid.NewString(), principal: authorization.StatusActive}
	if _, found := s.ServiceAccount(s.org); found {
		t.Fatal("a user snapshot found a service-account row")
	}
}

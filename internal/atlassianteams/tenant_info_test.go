package atlassianteams

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func tenantContextsPage(cloudID, orgID string) map[string]any {
	return map[string]any{"data": map[string]any{"tenantContexts": []any{
		map[string]any{"orgId": orgID, "cloudId": cloudID},
	}}}
}

// TestResolveOrganizationIDReturnsTheFirstTenantContextsOrgID is the D2817/
// CHAOS-7020 happy path: the AGG tenantContexts query, called with the same
// basic-auth gateway client SearchTeams already proves works, returns the
// cloud id's organization id.
func TestResolveOrganizationIDReturnsTheFirstTenantContextsOrgID(t *testing.T) {
	g := newGateway(t, func(req request) (int, any) {
		if req.Operation != "TenantContexts" {
			t.Fatalf("unexpected operation %q", req.Operation)
		}
		cloudIDs, _ := req.Variables["cloudIds"].([]any)
		if len(cloudIDs) != 1 || cloudIDs[0] != "cloud-1" {
			t.Errorf("cloudIds = %v, want [cloud-1]", req.Variables["cloudIds"])
		}
		return http.StatusOK, tenantContextsPage("cloud-1", "org-999")
	})
	orgID, err := ResolveOrganizationID(context.Background(), g.client(), "cloud-1")
	if err != nil {
		t.Fatalf("ResolveOrganizationID: %v", err)
	}
	if orgID != "org-999" {
		t.Errorf("orgID = %q, want org-999", orgID)
	}
	if g.count("TenantContexts", "") != 1 {
		t.Error("expected exactly one TenantContexts request")
	}
}

// TestResolveOrganizationIDRefusesWithNoCloudID is the guard-failing proof for
// the configuration check: an empty cloud id must refuse before any request,
// not send a malformed query.
func TestResolveOrganizationIDRefusesWithNoCloudID(t *testing.T) {
	g := newGateway(t, func(request) (int, any) {
		t.Fatal("no request should have been sent with an empty cloud id")
		return http.StatusOK, nil
	})
	if _, err := ResolveOrganizationID(context.Background(), g.client(), "  "); err == nil {
		t.Fatal("expected a refusal with no cloud id")
	}
}

// TestResolveOrganizationIDRefusesWithNoExecutor is the guard-failing proof
// for the nil-executor check, matching ResolveCloudID's own nil-tenant guard.
func TestResolveOrganizationIDRefusesWithNoExecutor(t *testing.T) {
	if _, err := ResolveOrganizationID(context.Background(), nil, "cloud-1"); err == nil {
		t.Fatal("expected a refusal with no executor")
	}
}

// TestResolveOrganizationIDReturnsNotFoundWhenTenantContextsIsEmpty plants
// the "tenant has no organization context" case: a successful response with
// no entries must name ErrOrganizationNotFound, not a bare index-out-of-range
// or a silently empty organization id.
func TestResolveOrganizationIDReturnsNotFoundWhenTenantContextsIsEmpty(t *testing.T) {
	g := newGateway(t, func(request) (int, any) {
		return http.StatusOK, map[string]any{"data": map[string]any{"tenantContexts": []any{}}}
	})
	_, err := ResolveOrganizationID(context.Background(), g.client(), "cloud-1")
	if !errors.Is(err, ErrOrganizationNotFound) {
		t.Errorf("err = %v, want ErrOrganizationNotFound", err)
	}
}

// TestResolveOrganizationIDNamesAPermissionProblemOnForbidden plants the
// credential-lacks-access case as a transport-level 403: the resolver must
// tell it apart from a generic transport failure or an empty organization.
func TestResolveOrganizationIDNamesAPermissionProblemOnForbidden(t *testing.T) {
	g := newGateway(t, func(request) (int, any) {
		return http.StatusForbidden, map[string]any{"errors": []any{map[string]any{"message": "forbidden"}}}
	})
	_, err := ResolveOrganizationID(context.Background(), g.client(), "cloud-1")
	if !errors.Is(err, ErrOrganizationPermission) {
		t.Errorf("err = %v, want ErrOrganizationPermission: %v", ErrOrganizationPermission, err)
	}
}

// TestResolveOrganizationIDNamesAPermissionProblemOnScopeError plants the
// same case at the GraphQL-error level (a 200 carrying a required-scopes
// extension, the AGG's own shape for a missing-scope credential).
func TestResolveOrganizationIDNamesAPermissionProblemOnScopeError(t *testing.T) {
	g := newGateway(t, func(request) (int, any) {
		return http.StatusOK, map[string]any{"errors": []any{map[string]any{
			"message":    "missing scope",
			"extensions": map[string]any{"requiredScopes": []any{"read:jira-work"}},
		}}}
	})
	_, err := ResolveOrganizationID(context.Background(), g.client(), "cloud-1")
	if !errors.Is(err, ErrOrganizationPermission) {
		t.Errorf("err = %v, want ErrOrganizationPermission: %v", ErrOrganizationPermission, err)
	}
}

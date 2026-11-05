package authapi

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

// withGrants gives the fixture principal a member role and grant rows in its organization.
func (h *authorizeHandlerFixture) withGrants(role string, grants ...authorization.Grant) {
	snap := h.state.snapshots[snapKey{h.principal, h.org}]
	snap.membership.Role = role
	for i := range grants {
		grants[i].OrgID = h.org
		if grants[i].ResourceKind == "" {
			grants[i].ResourceKind = "repository"
		}
		if grants[i].Role == "" {
			grants[i].Role = "viewer"
		}
	}
	snap.grants = grants
}

func repoGrant(key string) authorization.Grant { return authorization.Grant{ResourceKey: key} }

func TestAuthorizeEmitsTheResolvedAllowlistOfARealDecision(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("member", repoGrant("acme/web"), repoGrant("acme/api"))
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	o := parseDecision(t, rec).Obligations
	if o == nil || o.TenantScope == nil || *o.TenantScope != h.org || o.RepositoryAllowlist == nil ||
		!slices.Equal(*o.RepositoryAllowlist, []string{"acme/api", "acme/web"}) {
		t.Fatalf("obligations %+v in %s", o, rec.Body)
	}
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
}

func TestAuthorizeStatesAnOrganizationWideReadByRepositoryScope(t *testing.T) {
	for name, grants := range map[string][]authorization.Grant{
		"no grant row":        nil,
		"a live wildcard row": {repoGrant("*"), repoGrant("acme/api")},
	} {
		h := newAuthorizeHandlerFixture(t)
		h.withGrants("member", grants...)
		rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
		o := parseDecision(t, rec).Obligations
		if o == nil || o.TenantScope == nil || *o.TenantScope != h.org || o.RepositoryAllowlist != nil ||
			o.RepositoryScope == nil || *o.RepositoryScope != "organization" {
			t.Errorf("%s: obligations %+v in %s, want repository_scope organization and no allowlist", name, o, rec.Body)
		}
		validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
	}
}

func TestAuthorizeSendsARestrictedListWithoutRepositoryScope(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("member", repoGrant("acme/api"))
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if strings.Contains(rec.Body.String(), `"repository_scope"`) || !strings.Contains(rec.Body.String(), `"repository_allowlist":["acme/api"]`) {
		t.Fatalf("a narrowed allow carries the allowlist alone: %s", rec.Body)
	}
}

func TestAuthorizeNeverEmitsAWildcardAllowlist(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("member", repoGrant("*"), repoGrant("acme/api"))
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if strings.Contains(rec.Body.String(), `"*"`) {
		t.Fatalf("a wildcard reached the wire: %s", rec.Body)
	}
	if o := parseDecision(t, rec).Obligations; o == nil || o.TenantScope == nil || o.RepositoryScope == nil {
		t.Fatalf("an organization-wide allow carries its tenant scope and repository_scope: %s", rec.Body)
	}
}

func TestAuthorizeSendsNoneAsAnEmptyList(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	revoked := repoGrant("acme/api")
	revoked.Revoked = true
	h.withGrants("member", revoked)
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if !strings.Contains(rec.Body.String(), `"repository_allowlist":[]`) {
		t.Fatalf("none must be written as []: %s", rec.Body)
	}
}

func TestAuthorizeRefusesAWildcardRepositoryKey(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("owner")
	d := decodeWire[wireDecision](t, h.one(h.question("acr.context.read", "repository", "*", h.org)))
	if d.Allowed || d.ReasonCode != "invalid_request" {
		t.Fatalf("decision %+v, want a deny with invalid_request", d)
	}
}

func TestAuthorizeDeniesAnAllowlistOverTheBoundWithItsOwnReason(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	var rows []authorization.Grant
	for i := 0; i <= authorization.MaxRepositoryAllowlist; i++ {
		rows = append(rows, repoGrant(fmt.Sprintf("o/r%03d", i)))
	}
	h.withGrants("member", rows...)
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	d := decodeWire[wireDecision](t, rec)
	if d.Allowed || d.ReasonCode != "repository_allowlist_too_large" || strings.Contains(rec.Body.String(), `"obligations"`) {
		t.Fatalf("decision %+v in %s", d, rec.Body)
	}
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
}

func TestTheWireBoundIsTheDecisionCoreBound(t *testing.T) {
	if maxRepositoryAllowlist != authorization.MaxRepositoryAllowlist {
		t.Fatalf("wire mapper bound %d, decision core bound %d", maxRepositoryAllowlist, authorization.MaxRepositoryAllowlist)
	}
}

func TestAuthorizeRefusesAWildcardInAnAllowlist(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{"*"}})
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), `"decision_id"`) {
		t.Fatalf("status %d: a wildcard allowlist must fail closed: %s", rec.Code, rec.Body)
	}
}

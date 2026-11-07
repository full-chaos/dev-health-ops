package authapi

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
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

func TestAuthorizeEmitsTheRequestedRepositoryOfARealRepositoryDecision(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("member", repoGrant("acme/web"), repoGrant("acme/api"))
	rec := h.one(h.question("acr.context.read", "repository", "acme/api", h.org))
	o := parseDecision(t, rec).Obligations
	if o == nil || o.TenantScope == nil || *o.TenantScope != h.org || o.RepositoryAllowlist == nil ||
		!slices.Equal(*o.RepositoryAllowlist, []string{"acme/api"}) {
		t.Fatalf("obligations %+v in %s", o, rec.Body)
	}
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
}

// Product rule 2: a repository grant never narrows the five metrics reads; their allow carries the tenant scope alone.
func TestAuthorizeStatesAnOrganizationWideReadByTheTenantScopeAlone(t *testing.T) {
	for name, grants := range map[string][]authorization.Grant{
		"no grant row":        nil,
		"a live wildcard row": {repoGrant("*"), repoGrant("acme/api")},
		"one live grant":      {repoGrant("acme/api")},
	} {
		for _, action := range []string{"analytics.read", "metrics.read", "git.read", "work_items.read"} {
			h := newAuthorizeHandlerFixture(t)
			h.withGrants("member", grants...)
			rec := h.one(h.question(action, "organization", "acme", h.org))
			o := parseDecision(t, rec).Obligations
			if o == nil || o.TenantScope == nil || *o.TenantScope != h.org || o.RepositoryAllowlist != nil || o.RepositoryScope != nil {
				t.Errorf("%s %s: obligations %+v in %s, want the tenant scope alone", name, action, o, rec.Body)
			}
			validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
		}
	}
}

func TestAuthorizeSendsARestrictedListWithoutRepositoryScope(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api"}})
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
	if o := parseDecision(t, rec).Obligations; o == nil || o.TenantScope == nil {
		t.Fatalf("an organization-wide allow carries its tenant scope: %s", rec.Body)
	}
}

func TestAuthorizeSendsNoneAsAnEmptyList(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{}})
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
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = authorization.Decision{Reason: authorization.ReasonRepositoryAllowlistTooLarge,
		PolicyRevision: policycatalog.Default().Version(), CacheClass: authorization.CacheNone}
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

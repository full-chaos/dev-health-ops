package authapi

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
)

func TestAuthorizeRefusesARepositoryScopeOutsideTheContract(t *testing.T) {
	for name, o := range map[string]*authorization.Obligations{
		"scope beside a list":        {TenantScope: "", RepositoryScope: authorization.RepositoryScopeOrganization, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api"}},
		"scope beside an empty list": {RepositoryScope: authorization.RepositoryScopeOrganization, RepositoryRestricted: true},
		"unknown scope value":        {RepositoryScope: "everything"},
	} {
		planted := map[string]authorization.Decision{}
		h := newAuthorizeHandlerFixture(t, withPlanted(planted))
		o.TenantScope = h.org
		planted["analytics.read"] = plantedAllow(o)
		rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), `"decision_id"`) {
			t.Errorf("%s: status %d, want a 503 and no decision: %s", name, rec.Code, rec.Body)
		}
	}
}

func TestAuthorizeBatchItemsKeepTheirOwnRepositoryMember(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	h.withGrants("member", repoGrant("acme/api"))
	rec := h.batch(h.question("analytics.read", "organization", "acme", h.org), h.question("teams.read", "organization", "acme", h.org),
		h.question("admin.users", "platform", "platform", ""))
	doc := decodeWire[struct {
		Decisions []struct {
			Decision wireDecision `json:"decision"`
		} `json:"decisions"`
	}](t, rec)
	if len(doc.Decisions) != 3 {
		t.Fatalf("%d decisions: %s", len(doc.Decisions), rec.Body)
	}
	want := []map[string]any{
		{"tenant_scope": h.org, "repository_allowlist": []any{"acme/api"}},
		{"tenant_scope": h.org},
		nil,
	}
	for i, item := range doc.Decisions {
		if !reflect.DeepEqual(item.Decision.Obligations, want[i]) {
			t.Errorf("item %d obligations %v, want %v", i, item.Decision.Obligations, want[i])
		}
	}
}

func TestACacheHitKeepsTheRepositoryScope(t *testing.T) {
	c := newCacheFixture(t)
	first, second := c.ask(), c.ask()
	want := map[string]any{"tenant_scope": c.org, "repository_scope": "organization"}
	if !reflect.DeepEqual(first.Obligations, want) || !reflect.DeepEqual(second.Obligations, want) {
		t.Fatalf("miss %v, hit %v, want %v", first.Obligations, second.Obligations, want)
	}
	if c.counter(MetricDecisionCacheLookups, "hit") != 1 {
		t.Fatal("the second call was not a cache hit")
	}
}

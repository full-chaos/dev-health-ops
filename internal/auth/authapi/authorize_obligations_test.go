package authapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/decisioncache"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// plantedCache answers a decision by action and never stores one. The handler
// serves a hit as it decided it, so a planted decision reaches the wire mapper
// with the obligations the test chose.
type plantedCache struct {
	byAction map[string]authorization.Decision
}

func (c plantedCache) Begin() decisioncache.Stamp { return decisioncache.Stamp{} }
func (c plantedCache) Get(k decisioncache.Key) (authorization.Decision, bool) {
	d, ok := c.byAction[k.Action]
	return d, ok
}
func (plantedCache) Put(decisioncache.Key, authorization.Decision, decisioncache.Read) decisioncache.PutResult {
	return decisioncache.Stored
}
func (plantedCache) Len() int { return 0 }

func withPlanted(m map[string]authorization.Decision) func(*AuthorizeDeps) {
	return func(d *AuthorizeDeps) {
		d.Cache = plantedCache{byAction: m}
		d.Ready = func() bool { return true }
	}
}

func plantedAllow(o *authorization.Obligations) authorization.Decision {
	return authorization.Decision{Allowed: true, Reason: authorization.ReasonAllowedByRole,
		PolicyRevision: policycatalog.Default().Version(), Obligations: o}
}

func obligationMembers(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var doc struct {
		Obligations map[string]json.RawMessage `json:"obligations"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("body %s: %v", raw, err)
	}
	return doc.Obligations
}

func parseDecision(t *testing.T, rec *httptest.ResponseRecorder) *contracts.AuthorizationDecision {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	d, err := contracts.ParseAuthorizationDecision(repoRoot(t), rec.Body.Bytes(), time.Unix(0, authorizeNow))
	if err != nil {
		t.Fatalf("the client refuses the decision: %v\nbody %s", err, rec.Body)
	}
	return d
}

func TestAuthorizeWireCarriesTheObligationsOfAnAllow(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{
		TenantScope: h.org, MinimumAssurance: "aal2", RepositoryRestricted: true,
		RepositoryAllowlist: []string{"acme/api", "acme/web"}, Redaction: []string{"free_text"},
	})
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	d := parseDecision(t, rec)
	validateSurface(t, "authorization_decision.v1", rec.Body.Bytes())
	o := d.Obligations
	if o == nil {
		t.Fatalf("the allow lost its obligations: %s", rec.Body)
	}
	if o.TenantScope == nil || *o.TenantScope != h.org {
		t.Errorf("tenant_scope %v, want %s", o.TenantScope, h.org)
	}
	if o.RepositoryAllowlist == nil || !slices.Equal(*o.RepositoryAllowlist, []string{"acme/api", "acme/web"}) {
		t.Errorf("repository_allowlist %v", o.RepositoryAllowlist)
	}
	if o.Redaction == nil || !slices.Equal(*o.Redaction, []string{"free_text"}) {
		t.Errorf("redaction %v", o.Redaction)
	}
	if o.MinimumAssurance == nil || *o.MinimumAssurance != "aal2" {
		t.Errorf("minimum_assurance %v", o.MinimumAssurance)
	}
	if o.StepUp != nil {
		t.Errorf("step_up %v, the decision point never sets it", *o.StepUp)
	}
}

// An empty allowlist means NONE and an absent member means unrestricted: a
// slice with omitempty drops the empty one and widens the decision.
func TestAuthorizeWireNeverOmitsAnEmptyAllowlist(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true})
	planted["metrics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{}})
	planted["work_items.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org})
	for _, action := range []string{"analytics.read", "metrics.read"} {
		rec := h.one(h.question(action, "organization", "acme", h.org))
		d := parseDecision(t, rec)
		if d.Obligations == nil || d.Obligations.RepositoryAllowlist == nil {
			t.Fatalf("%s: the empty allowlist was dropped from %s", action, rec.Body)
		}
		if len(*d.Obligations.RepositoryAllowlist) != 0 {
			t.Fatalf("%s: allowlist %v, want none", action, *d.Obligations.RepositoryAllowlist)
		}
		if !strings.Contains(rec.Body.String(), `"repository_allowlist":[]`) {
			t.Fatalf("%s: raw body lacks the empty array: %s", action, rec.Body)
		}
	}
	rec := h.one(h.question("work_items.read", "organization", "acme", h.org))
	if d := parseDecision(t, rec); d.Obligations == nil || d.Obligations.RepositoryAllowlist != nil {
		t.Fatalf("an unrestricted decision grew an allowlist: %s", rec.Body)
	}
}

func TestAuthorizeWireDenyCarriesNoObligations(t *testing.T) {
	h := newAuthorizeHandlerFixture(t)
	for _, q := range []map[string]any{
		h.question("analytics.nonsense", "organization", "acme", h.org),
		h.question("admin.users", "organization", "acme", h.org),
		h.question("settings.write", "organization", "acme", uuidOther()),
		h.question("analytics.read", "repository", "acme/api", h.org),
	} {
		rec := h.one(q)
		d := parseDecision(t, rec)
		if d.Allowed || d.Obligations != nil || strings.Contains(rec.Body.String(), `"obligations"`) {
			t.Fatalf("a deny carried obligations: %s", rec.Body)
		}
	}
}

func TestAuthorizeNeverServesObligationsOnADeny(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	d := plantedAllow(&authorization.Obligations{TenantScope: h.org})
	d.Allowed, d.Reason = false, authorization.ReasonRoleLacksAction
	planted["analytics.read"] = d
	rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), `"decision_id"`) {
		t.Fatalf("status %d, a deny with obligations must not be served: %s", rec.Code, rec.Body)
	}
}

func TestAuthorizeBatchItemsKeepTheirOwnObligations(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	planted["analytics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{"*"}})
	planted["metrics.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true})
	planted["work_items.read"] = plantedAllow(&authorization.Obligations{TenantScope: h.org, RepositoryRestricted: true, RepositoryAllowlist: []string{"acme/api"}})
	qs := []map[string]any{
		h.question("analytics.read", "organization", "acme", h.org),
		h.question("analytics.nonsense", "organization", "acme", h.org),
		h.question("metrics.read", "organization", "acme", h.org),
		h.question("work_items.read", "organization", "acme", h.org),
	}
	body := mustJSON(t, map[string]any{"schema_version": "authorization_batch_request.v1", "requests": qs})
	rec := h.post(PatternAuthorizeBatch, h.workloadToken(nil), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
	got, err := contracts.ParseAuthorizationBatchResponse(repoRoot(t), body, rec.Body.Bytes(), time.Unix(0, authorizeNow))
	if err != nil {
		t.Fatalf("the client refuses the batch: %v\n%s", err, rec.Body)
	}
	want := [][]string{{"*"}, nil, {}, {"acme/api"}}
	for i, w := range want {
		o := got[i].Obligations
		switch {
		case i == 1:
			if got[i].Allowed || o != nil {
				t.Errorf("item 1 is a deny and carries nothing: %+v", got[i])
			}
		case o == nil || o.RepositoryAllowlist == nil || !slices.Equal(*o.RepositoryAllowlist, w):
			t.Errorf("item %d allowlist %v, want %v", i, o, w)
		}
	}
}

func TestAuthorizeRefusesAnObligationThatIsNotPerContract(t *testing.T) {
	many := make([]string, 257)
	for i := range many {
		many[i] = "o/r" + strings.Repeat("a", 1) + string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
	}
	cases := map[string]func(org string) *authorization.Obligations{
		"an uppercase repository key": func(string) *authorization.Obligations {
			return &authorization.Obligations{RepositoryRestricted: true, RepositoryAllowlist: []string{"Acme/API"}}
		},
		"a repeated repository key": func(string) *authorization.Obligations {
			return &authorization.Obligations{RepositoryRestricted: true, RepositoryAllowlist: []string{"a/b", "a/b"}}
		},
		"more than 256 repository keys": func(string) *authorization.Obligations {
			return &authorization.Obligations{RepositoryRestricted: true, RepositoryAllowlist: many}
		},
		"an unregistered redaction class": func(string) *authorization.Obligations {
			return &authorization.Obligations{Redaction: []string{"secrets"}}
		},
		"a repeated redaction class": func(string) *authorization.Obligations {
			return &authorization.Obligations{Redaction: []string{"free_text", "free_text"}}
		},
		"an unknown assurance level": func(string) *authorization.Obligations {
			return &authorization.Obligations{MinimumAssurance: "aal9"}
		},
		"a tenant scope of another organization": func(string) *authorization.Obligations {
			return &authorization.Obligations{TenantScope: uuidOther()}
		},
	}
	for name, build := range cases {
		planted := map[string]authorization.Decision{}
		h := newAuthorizeHandlerFixture(t, withPlanted(planted))
		o := build(h.org)
		if o.TenantScope == "" {
			o.TenantScope = h.org
		}
		planted["analytics.read"] = plantedAllow(o)
		rec := h.one(h.question("analytics.read", "organization", "acme", h.org))
		if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), `"decision_id"`) {
			t.Errorf("%s: status %d, an invalid obligation must fail closed, not be served: %s", name, rec.Code, rec.Body)
		}
	}
}

// delegationFake is a delegated session over the handler fixture's state.
type delegationFake struct {
	*fakeSnapshot
	del authorization.Delegation
}

func (d delegationFake) Delegation() (authorization.Delegation, bool) { return d.del, true }

func TestAuthorizeWireCarriesTheObligationsOfADelegatedAllow(t *testing.T) {
	planted := map[string]authorization.Decision{}
	h := newAuthorizeHandlerFixture(t, withPlanted(planted))
	snap := delegationFake{
		fakeSnapshot: &fakeSnapshot{principal: authorization.StatusActive, org: h.org, orgStatus: authorization.StatusActive,
			membership: &authorization.Membership{OrgID: h.org, Role: "admin", Status: authorization.StatusActive}, now: authorizeNow},
		del: authorization.Delegation{
			Chain:                []authorization.Hop{{ActorPrincipalID: "actor-1", PermittedActions: []string{"analytics.read", "acr.context.read"}}},
			TargetOrganizationID: h.org, Assurance: "aal2",
		},
	}
	e := authorization.NewEvaluator()
	org := e.Evaluate(authorization.Request{Action: "analytics.read", ResourceKind: "organization", OrganizationID: h.org}, snap)
	repo := e.Evaluate(authorization.Request{Action: "acr.context.read", ResourceKind: "repository", OrganizationID: h.org, ResourceKey: "acme/api"}, snap)
	if !org.Allowed || !repo.Allowed || org.Obligations == nil || repo.Obligations == nil {
		t.Fatalf("the evaluator did not allow with obligations: %+v %+v", org, repo)
	}
	planted["analytics.read"], planted["acr.context.read"] = org, repo

	o := parseDecision(t, h.one(h.question("analytics.read", "organization", "acme", h.org))).Obligations
	if o == nil || o.TenantScope == nil || *o.TenantScope != h.org || o.MinimumAssurance == nil || *o.MinimumAssurance != "aal2" || o.RepositoryAllowlist != nil {
		t.Errorf("organization allow obligations %+v", o)
	}
	o = parseDecision(t, h.one(h.question("acr.context.read", "repository", "acme/api", h.org))).Obligations
	if o == nil || o.RepositoryAllowlist == nil || !slices.Equal(*o.RepositoryAllowlist, []string{"acme/api"}) {
		t.Errorf("repository allow obligations %+v", o)
	}
}

func uuidOther() string { return "00000000-0000-4000-8000-0000000000aa" }

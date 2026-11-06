package pdp

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/pep/pepfake"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// The pepgate seam (CHAOS-13081, PLAN-10864-v2 section 4 PR-1): an enforced allow is served only as a Scope built
// from the decision's tenant_scope and repository member; every obligation the gate does not carry out is a 503; the
// gate enforces a route only while the 5.17 cell of its action namespace is on.

const seamOrg = "0b6a1c9e-55d4-4a43-9e34-6f4c1d2b3a10"

func cells(list string) authzenforce.Switch { return authzenforce.Parse(authzenforce.QueryAPI, list) }

// countingDecider counts the decisions it was asked for.
type countingDecider struct {
	pep.Decider
	calls atomic.Int64
}

func (c *countingDecider) Decide(ctx context.Context, req pep.Request) pep.Decision {
	c.calls.Add(1)
	return c.Decider.Decide(ctx, req)
}

// denyDecider is a PDP that answers every request with a deny verdict (a wire deny, not a local no-decision).
func denyDecider(t *testing.T) *countingDecider {
	t.Helper()
	srv := pdptest.New(t)
	srv.Deny("role_lacks_action")
	return &countingDecider{Decider: pepClient(t, srv.URL)}
}

// allowRule is the PDP's allow of testProfile's action for seamOrg over every repository, edited by edit.
func allowRule(edit func(*pepfake.Rule)) pepfake.Rule {
	r := pepfake.Rule{PrincipalReference: testBearer, Action: testAction, ResourceType: "organization", ResourceKey: seamOrg,
		OrganizationID: seamOrg, TenantScope: seamOrg, RepositoryScope: "organization"}
	if edit != nil {
		edit(&r)
	}
	return r
}

// seamGate is an enforce gate with the analytics cell on over a fake that applies rules.
func seamGate(t *testing.T, rules ...pepfake.Rule) (*Gate, *countingDecider, *meterReader) {
	t.Helper()
	m, cfg := newMeter(t)
	decider := &countingDecider{Decider: pepfake.New(rules...)}
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, decider, cells("analytics")
	return New(cfg), decider, m
}

func wantLocal(t *testing.T, err error, reason pep.Reason) {
	t.Helper()
	var denied *DenyError
	if !errors.As(err, &denied) || !denied.Local || denied.Reason != reason {
		t.Fatalf("answered %#v, want a local DenyError %s", err, reason)
	}
}

func wantNoScope(t *testing.T, s pepgate.Scope) {
	t.Helper()
	if _, ok := s.OrgID(); ok || s.Kind() != pepgate.KindNone {
		t.Fatalf("a refusal carries scope kind %v", s.Kind())
	}
}

// P1, P2, P4-P8: an allow whose obligations the gate does not carry out is no verdict: 503, no scope.
func TestSeamAllowNotCarriedOutIs503(t *testing.T) {
	for name, tc := range map[string]struct {
		edit   func(*pepfake.Rule)
		reason pep.Reason
	}{
		"P1 tenant_scope absent":             {func(r *pepfake.Rule) { r.TenantScope = "" }, ReasonTenantMismatch},
		"P2 tenant_scope other org":          {func(r *pepfake.Rule) { r.TenantScope = "66666666-6666-6666-6666-666666666666" }, ReasonTenantMismatch},
		"P4 allowlist [] not cut over":       {func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{} }, ReasonNotCarriedOut},
		"P5 allowlist [acme/a] not cut over": {func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{"acme/a"} }, ReasonNotCarriedOut},
		"P6 scope and allowlist (malformed)": {func(r *pepfake.Rule) { r.RepositoryAllowlist = []string{"acme/a"} }, pep.ReasonMalformed},
		"P8 redaction":                       {func(r *pepfake.Rule) { r.Redaction = []pep.RedactionClass{pep.RedactFreeText} }, ReasonNotCarriedOut},
		"P8 minimum_assurance":               {func(r *pepfake.Rule) { r.MinimumAssurance = pep.AssuranceAAL2 }, ReasonNotCarriedOut},
		"P8 step_up":                         {func(r *pepfake.Rule) { r.StepUp = true }, ReasonNotCarriedOut},
	} {
		t.Run(name, func(t *testing.T) {
			g, decider, _ := seamGate(t, allowRule(tc.edit))
			scope, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
			wantLocal(t, err, tc.reason)
			wantNoScope(t, scope)
			if decider.calls.Load() != 1 {
				t.Fatalf("asked %d times, want 1", decider.calls.Load())
			}
		})
	}
}

// P7: a filtered action (an ACR read) with neither repository member is refused; a metrics read, org-wide for every
// member (product rule 2), is served as the tenant alone.
func TestSeamNeitherRepositoryMember(t *testing.T) {
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Switch = ModeEnforce, cells("analytics,acr")
	cfg.Decider = pepfake.New(allowRule(func(r *pepfake.Rule) { r.RepositoryScope = "" }),
		allowRule(func(r *pepfake.Rule) { r.Action, r.RepositoryScope = "acr.context.read", "" }))
	g := New(cfg)
	g.actions[acrTestProfile] = "acr.context.read"
	scope, err := g.Authorize(context.Background(), acrTestProfile, testBearer, seamOrg)
	wantLocal(t, err, ReasonNoRepositoryRule)
	wantNoScope(t, scope)
	scope, err = g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
	if err != nil || scope.Kind() != pepgate.KindOrganization {
		t.Fatalf("metrics read with the tenant alone: answered %v kind %v, want Organization", err, scope.Kind())
	}
}

// acp ruling (2): the request organization, the decision's organization and its tenant_scope compare as UUIDs
// (RFC 9562 text form, any case) by their canonical form; a value that does not parse is tenant_scope_mismatch. The
// scope carries the canonical (lowercase) form.
func TestSeamOrganizationsCompareAsUUIDs(t *testing.T) {
	upper := strings.ToUpper(seamOrg)
	for name, tc := range map[string]struct {
		request string
		edit    func(*pepfake.Rule)
		pass    bool
	}{
		"uppercase request org":  {upper, nil, true},
		"uppercase tenant_scope": {seamOrg, func(r *pepfake.Rule) { r.TenantScope = upper }, true},
		"different UUID tenant":  {seamOrg, func(r *pepfake.Rule) { r.TenantScope = "66666666-6666-6666-6666-666666666666" }, false},
		"non-UUID tenant":        {seamOrg, func(r *pepfake.Rule) { r.TenantScope = "acme" }, false},
		"non-UUID request org":   {"acme", func(r *pepfake.Rule) { r.ResourceKey, r.OrganizationID, r.TenantScope = "acme", "acme", "acme" }, false},
		"braced UUID request org": {"{" + seamOrg + "}", func(r *pepfake.Rule) {
			k := "{" + seamOrg + "}"
			r.ResourceKey, r.OrganizationID, r.TenantScope = k, k, k
		}, false},
		"unhyphenated UUID everywhere": {strings.ReplaceAll(seamOrg, "-", ""), func(r *pepfake.Rule) {
			k := strings.ReplaceAll(seamOrg, "-", "")
			r.ResourceKey, r.OrganizationID, r.TenantScope = k, k, k
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			g, decider, _ := seamGate(t, allowRule(tc.edit))
			scope, err := g.Authorize(context.Background(), testProfile, testBearer, tc.request)
			if decider.calls.Load() != 1 {
				t.Fatalf("asked %d times, want 1", decider.calls.Load())
			}
			if !tc.pass {
				wantLocal(t, err, ReasonTenantMismatch)
				wantNoScope(t, scope)
				return
			}
			if org, ok := scope.OrgID(); err != nil || !ok || org != seamOrg || scope.Kind() != pepgate.KindOrganization {
				t.Fatalf("answered %v, scope kind %v org %q, want Organization(%s)", err, scope.Kind(), org, seamOrg)
			}
		})
	}
}

// P3: repository_scope organization serves as Organization(tenant). P15: an allowlist with "*" is every repository.
func TestSeamOrganizationScope(t *testing.T) {
	for name, edit := range map[string]func(*pepfake.Rule){
		"P3 repository_scope organization": nil,
		"P15 allowlist [*]":                func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{"*"} },
		"P15 allowlist [acme/a, *]":        func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", []string{"acme/a", "*"} },
	} {
		t.Run(name, func(t *testing.T) {
			g, _, m := seamGate(t, allowRule(edit))
			scope, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
			if err != nil {
				t.Fatalf("allow answered %v", err)
			}
			if org, ok := scope.OrgID(); !ok || org != seamOrg || scope.Kind() != pepgate.KindOrganization || scope.Restricted() {
				t.Fatalf("scope kind %v org %q restricted %v, want Organization(%s)", scope.Kind(), org, scope.Restricted(), seamOrg)
			}
			if got := m.points(t, resultMetric); got["action=analytics.read,mode=enforce,result=allow"] != 1 {
				t.Fatalf("results %v", got)
			}
		})
	}
}

// A route on the cut-over registry carries the allowlist as the scope's ceiling; [] is a ceiling of nothing.
func TestSeamAllowlistOnACutOverRoute(t *testing.T) {
	for name, keys := range map[string][]string{"empty": {}, "one": {"acme/a"}} {
		t.Run(name, func(t *testing.T) {
			g, _, _ := seamGate(t, allowRule(func(r *pepfake.Rule) { r.RepositoryScope, r.RepositoryAllowlist = "", keys }))
			g.carriesAllowlist = map[string]bool{testProfile: true}
			scope, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
			if err != nil {
				t.Fatalf("allow answered %v", err)
			}
			if scope.Kind() != pepgate.KindAllowlist || !scope.Restricted() || !slices.Equal(scope.RepoKeys(), keys) {
				t.Fatalf("scope kind %v keys %v, want Allowlist %v", scope.Kind(), scope.RepoKeys(), keys)
			}
		})
	}
}

// PR-1 cuts no route over: an allowlist decision is refused on every REST row until the cutover lists it.
func TestSeamNoRouteCarriesAnAllowlistYet(t *testing.T) {
	g := New(Config{Mode: ModeOff})
	if len(g.carriesAllowlist) != 0 {
		t.Fatalf("cut-over registry %v, want empty in PR-1", g.carriesAllowlist)
	}
	for profile := range allowlistCutOver {
		if _, known := g.actions[profile]; !known {
			t.Errorf("cut-over registry names %q, which is no row", profile)
		}
	}
}

// P9: the cell on and a PDP deny is 403.
func TestSeamCellOnDenyIs403(t *testing.T) {
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, denyDecider(t), cells("analytics")
	scope, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, seamOrg)
	var denied *DenyError
	if !errors.As(err, &denied) || denied.Local {
		t.Fatalf("deny answered %#v, want a PDP DenyError", err)
	}
	wantNoScope(t, scope)
}

// P11 + P13: shadow mode with the cell of the route's namespace off never refuses and the scope is Legacy; Authorize
// itself asks nothing and counts nothing (the 5.12 shadow core, built by the gate, asks from the REST hooks).
func TestSeamCellOffIsShadow(t *testing.T) {
	for name, tc := range map[string]struct {
		mode, cells, profile, action string
	}{
		"P11 shadow, no cell":             {ModeShadow, "", testProfile, testAction},
		"P13 shadow, other namespace on":  {ModeShadow, "analytics", "GET /api/v1/flame", "metrics.read"},
		"shadow, other namespaces on":     {ModeShadow, "git,work_items", testProfile, testAction},
		"shadow, cell list of bad values": {ModeShadow, "Analytics,*,nope", testProfile, testAction},
	} {
		t.Run(name, func(t *testing.T) {
			m, cfg := newMeter(t)
			decider := denyDecider(t)
			cfg.Mode, cfg.Decider, cfg.Switch = tc.mode, decider, cells(tc.cells)
			g := New(cfg)
			scope, err := g.Authorize(context.Background(), tc.profile, testBearer, seamOrg)
			g.Wait()
			if err != nil {
				t.Fatalf("cell off refused: %v", err)
			}
			if org, ok := scope.OrgID(); !ok || org != seamOrg || scope.Kind() != pepgate.KindLegacy {
				t.Fatalf("scope kind %v org %q, want Legacy(%s)", scope.Kind(), org, seamOrg)
			}
			if decider.calls.Load() != 0 {
				t.Fatalf("Authorize asked %d times, want 0", decider.calls.Load())
			}
			if got := m.points(t, resultMetric); len(got) != 0 {
				t.Fatalf("results %v, want none", got)
			}
			if g.Shadow() == nil {
				t.Fatal("shadow-mode gate built no shadow")
			}
		})
	}
}

// acp ruling (1): a mode named enforce never acts as shadow. With the cell of the route's namespace off it refuses
// (503, authz_enforce_cell_off) and asks nothing.
func TestSeamEnforceModeWithTheCellOffIsRefused(t *testing.T) {
	for name, tc := range map[string]struct {
		cells, profile, action string
	}{
		"no cell":               {"", testProfile, testAction},
		"other namespace on":    {"analytics", "GET /api/v1/flame", "metrics.read"},
		"cell list of bad only": {"Analytics,*,nope", testProfile, testAction},
	} {
		t.Run(name, func(t *testing.T) {
			m, cfg := newMeter(t)
			decider := denyDecider(t)
			cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, decider, cells(tc.cells)
			g := New(cfg)
			scope, err := g.Authorize(context.Background(), tc.profile, testBearer, seamOrg)
			g.Wait()
			wantLocal(t, err, ReasonEnforceCellOff)
			wantNoScope(t, scope)
			if decider.calls.Load() != 0 {
				t.Fatalf("asked %d times, want 0", decider.calls.Load())
			}
			if got := m.points(t, resultMetric); got["action="+tc.action+",mode=enforce,result=not_configured"] != 1 || len(got) != 1 {
				t.Fatalf("results %v, want one enforce not_configured", got)
			}
		})
	}
}

// Q1: the cell, not QUERY_API_PDP_MODE, decides enforcement: shadow mode with the cell on enforces.
func TestSeamShadowModeWithTheCellOnEnforces(t *testing.T) {
	_, cfg := newMeter(t)
	cfg.Mode, cfg.Decider, cfg.Switch = ModeShadow, denyDecider(t), cells("analytics")
	scope, err := New(cfg).Authorize(context.Background(), testProfile, testBearer, seamOrg)
	var denied *DenyError
	if !errors.As(err, &denied) || denied.Local {
		t.Fatalf("shadow mode, cell on, deny answered %#v, want a PDP DenyError", err)
	}
	wantNoScope(t, scope)
}

// P12: the cell on with no decision point client (mode off, or a misconfigured gate) is 503
// authz_enforce_no_decision_point; the namespaces that are off keep today's path.
func TestSeamCellOnWithoutAClientIs503(t *testing.T) {
	for name, mode := range map[string]string{"off": ModeOff, "bad mode": "banana", "enforce, no client": ModeEnforce} {
		t.Run(name, func(t *testing.T) {
			m, cfg := newMeter(t)
			cfg.Mode, cfg.Switch = mode, cells("analytics")
			g := New(cfg)
			scope, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
			wantLocal(t, err, ReasonNoDecisionPoint)
			wantNoScope(t, scope)
			if got := m.points(t, configInvalidMetric)[""]; got != 1 {
				t.Fatalf("config_invalid = %d, want 1", got)
			}
			if mode != ModeOff {
				return
			}
			other, err := g.Authorize(context.Background(), "GET /api/v1/flame", testBearer, seamOrg)
			if err != nil || other.Kind() != pepgate.KindLegacy {
				t.Fatalf("a namespace that is off answered %v kind %v, want Legacy", err, other.Kind())
			}
			if got := m.points(t, resultMetric); got["action=analytics.read,mode=enforce,result=no_decision_point"] != 1 {
				t.Fatalf("results %v", got)
			}
		})
	}
}

// FromSettings reads the cell list through the declared lookup: QUERY_API_PDP_MODE unset with a cell on is the
// no-client 503, and one ERROR names the variable, never its value.
func TestSeamFromSettingsReadsTheCell(t *testing.T) {
	var logs bytes.Buffer
	env := map[string]string{authzenforce.QueryAPI.Env(): "analytics"}
	g := FromSettings((&recordingLookup{values: env}).lookup, slog.New(slog.NewTextHandler(&logs, nil)))
	_, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
	wantLocal(t, err, ReasonNoDecisionPoint)
	text := logs.String()
	if strings.Count(text, "level=ERROR") != 1 || !strings.Contains(text, "env="+authzenforce.QueryAPI.Env()) || strings.Contains(text, "analytics") {
		t.Fatalf("log %q, want one ERROR naming the variable, no value", text)
	}
}

// P10: QUERY_API_PDP_MODE and the cell list unset: Legacy scope, nothing asked, nothing counted.
func TestSeamAllUnsetIsLegacyAndAsksNothing(t *testing.T) {
	srv := pdptest.New(t)
	env := onEnv(t, "", srv.URL)
	g := FromSettings((&recordingLookup{values: env}).lookup, nil)
	scope, err := g.Authorize(context.Background(), testProfile, testBearer, seamOrg)
	g.Wait()
	if err != nil || scope.Kind() != pepgate.KindLegacy {
		t.Fatalf("unset answered %v kind %v, want Legacy", err, scope.Kind())
	}
	if org, ok := scope.OrgID(); !ok || org != seamOrg {
		t.Fatalf("legacy org %q", org)
	}
	if n := len(srv.Requests()); n != 0 || srv.Exchanges() != 0 {
		t.Fatalf("asked %d times, exchanged %d times", n, srv.Exchanges())
	}
}

// The gate exposes the 5.17 gauges of query-api: one series per namespace, 1 where the cell is on.
func TestSeamRegistersTheCellGauges(t *testing.T) {
	m, cfg := newMeter(t)
	cfg.Mode, cfg.Switch = ModeOff, cells("git")
	New(cfg)
	got := m.points(t, "dev_health_authz_enforcement_enabled")
	if got["namespace=git,service=query-api"] != 1 || got["namespace=analytics,service=query-api"] != 0 || len(got) != len(authzenforce.Namespaces()) {
		t.Fatalf("enforcement gauges %v", got)
	}
}

// An action the catalog marks repository_scope none (GET /buildinfo, org.read) carries no repository member: the
// tenant alone is the scope, served as Organization. An allowlist on it is still not carried out.
func TestSeamRepositoryScopeNoneAction(t *testing.T) {
	const profile = "GET /buildinfo"
	for name, tc := range map[string]struct {
		edit func(*pepfake.Rule)
		pass bool
	}{
		"tenant only": {func(r *pepfake.Rule) { r.Action, r.RepositoryScope = "org.read", "" }, true},
		"allowlist on it": {func(r *pepfake.Rule) {
			r.Action, r.RepositoryScope, r.RepositoryAllowlist = "org.read", "", []string{"acme/widgets"}
		}, false},
		"tenant other org":    {func(r *pepfake.Rule) { r.Action, r.RepositoryScope, r.TenantScope = "org.read", "", "other-org" }, false},
		"filtered, no member": {func(r *pepfake.Rule) { r.Action, r.RepositoryScope = "acr.context.read", "" }, false},
	} {
		t.Run(name, func(t *testing.T) {
			_, cfg := newMeter(t)
			cfg.Mode, cfg.Decider, cfg.Switch = ModeEnforce, pepfake.New(allowRule(tc.edit)), cells("analytics,org")
			p := profile
			if name == "filtered, no member" {
				p = acrTestProfile
			}
			scope, err := New(cfg).Authorize(context.Background(), p, testBearer, seamOrg)
			if !tc.pass {
				var denied *DenyError
				if !errors.As(err, &denied) || !denied.Local {
					t.Fatalf("answered %#v, want a local DenyError", err)
				}
				wantNoScope(t, scope)
				return
			}
			if err != nil || scope.Kind() != pepgate.KindOrganization {
				t.Fatalf("answered %v kind %v, want Organization", err, scope.Kind())
			}
		})
	}
}

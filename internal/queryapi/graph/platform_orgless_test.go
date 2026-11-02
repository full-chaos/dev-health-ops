package graph

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole/rule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
)

const (
	orglessDashboard = `query Q($input: ProductTelemetryDashboardInput!) { productTelemetryPlatformDashboard(input: $input) { __typename } }`
	orglessRefusal   = "Authorization required"
)

func orglessSet(actions ...string) *rule.Platform {
	set := rule.PlatformOf(actions...)
	return &set
}

// An org-less caller that has no superuser flag is admitted only for an
// operation whose every root field is in the org-less table and whose named
// action the caller holds.
func TestOrglessPlatformCallerIsAdmittedByTheOperationsNamedAction(t *testing.T) {
	for _, c := range []struct {
		name     string
		document string
		claims   authctx.Claims
		allowed  bool
	}{
		{"admin.telemetry holder, the dashboard", orglessDashboard, authctx.Claims{Platform: orglessSet(rule.ActionAdminTelemetry)}, true},
		{"the dashboard behind a fragment", `query Q($input: ProductTelemetryDashboardInput!) { ...F } fragment F on Query { productTelemetryPlatformDashboard(input: $input) { __typename } }`, authctx.Claims{Platform: orglessSet(rule.ActionAdminTelemetry)}, true},
		{"platform_admin set, the dashboard", orglessDashboard, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, true},
		{"admin.stats holder, the dashboard", orglessDashboard, authctx.Claims{Platform: orglessSet(rule.ActionAdminStats)}, false},
		{"admin.orgs holder, the dashboard", orglessDashboard, authctx.Claims{Platform: orglessSet(rule.ActionAdminOrgs)}, false},
		{"every action but admin.telemetry", orglessDashboard, authctx.Claims{Platform: orglessSet(otherActions(rule.ActionAdminTelemetry)...)}, false},
		{"an empty set", orglessDashboard, authctx.Claims{Platform: orglessSet()}, false},
		{"a holder while impersonating", orglessDashboard, authctx.Claims{Platform: orglessSet(rule.ActionAdminTelemetry), ImpersonationActive: true}, false},
		{"admin.telemetry holder, dataHealth", `query Q { dataHealth(team: "x") { __typename } }`, authctx.Claims{Platform: orglessSet(rule.ActionAdminTelemetry)}, false},
		{"admin.stats holder, dataHealth", `query Q { dataHealth(team: "x") { __typename } }`, authctx.Claims{Platform: orglessSet(rule.ActionAdminStats)}, false},
		{"the dashboard beside dataHealth", `query Q($input: ProductTelemetryDashboardInput!) { productTelemetryPlatformDashboard(input: $input) { __typename } dataHealth(team: "x") { __typename } }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
		{"the dashboard beside an introspection root", `query Q($input: ProductTelemetryDashboardInput!) { productTelemetryPlatformDashboard(input: $input) { __typename } __schema { types { name } } }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
		{"an introspection root alone", `query Q { __schema { types { name } } }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
		{"__typename alone", `query Q { __typename }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
		{"another field aliased as the dashboard", `query Q { productTelemetryPlatformDashboard: dataHealth(team: "x") { __typename } }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
		{"a mutation", `mutation M { productTelemetryPlatformDashboard }`, authctx.Claims{Platform: orglessSet(rule.PlatformActions...)}, false},
	} {
		ctx := authctx.WithClaims(context.Background(), c.claims)
		message, rebind := operationOrgDecision(ctx, guardOperation(t, c.document, map[string]any{}))
		if rebind != "" {
			t.Errorf("%s: rebound to %q", c.name, rebind)
		}
		switch {
		case c.allowed && message != "":
			t.Errorf("%s: refused with %q", c.name, message)
		case !c.allowed && message != orglessRefusal:
			t.Errorf("%s: message %q, want %q", c.name, message, orglessRefusal)
		}
	}
}

// Nothing else changes: the flag carrier, a carrier with no set, and a caller
// that has an org keep the decisions they had.
func TestOrglessRuleLeavesEveryOtherCallerAlone(t *testing.T) {
	dataHealth := `query Q { dataHealth(team: "x") { __typename } }`
	named := `query Q { dataHealth(team: "x") { __typename } coverage(orgId: "org-other") }`
	for _, c := range []struct {
		name     string
		document string
		claims   authctx.Claims
		want     string
	}{
		{"the flag, org-less, any operation", dataHealth, authctx.Claims{IsSuperuser: true}, ""},
		{"the flag beside a narrower set, org-less", dataHealth, authctx.Claims{IsSuperuser: true, Platform: orglessSet(rule.ActionAdminStats)}, ""},
		{"no flag and no set, org-less (other carriers)", dataHealth, authctx.Claims{}, ""},
		{"a set with an org, a credential-org field", dataHealth, authctx.Claims{OrgID: "org-own", Platform: orglessSet(rule.ActionAdminStats)}, ""},
		{"a set with an org naming another org without admin.orgs", named, authctx.Claims{OrgID: "org-own", Platform: orglessSet(rule.ActionAdminStats)}, "Access denied: cannot query org 'org-other'"},
	} {
		ctx := authctx.WithClaims(context.Background(), c.claims)
		if message, _ := operationOrgDecision(ctx, guardOperation(t, c.document, map[string]any{})); message != c.want {
			t.Errorf("%s: message %q, want %q", c.name, message, c.want)
		}
	}
}

func otherActions(skip string) []string {
	var rest []string
	for _, a := range rule.PlatformActions {
		if a != skip {
			rest = append(rest, a)
		}
	}
	return rest
}

// The org-less table is the contract's list of platform fields, each with the
// action its resolver checks.
func TestOrglessTableIsTheContractsPlatformFields(t *testing.T) {
	raw, err := os.ReadFile(fieldActionMapPath)
	if err != nil {
		t.Fatal(err)
	}
	var doc fieldActionDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var platform []string
	for _, row := range doc.Rows {
		if row.Type == "Query" && strings.HasPrefix(row.ResourceResolver, "none:") {
			platform = append(platform, row.Field)
		}
	}
	var table []string
	for field := range orglessRootActions {
		table = append(table, field)
	}
	sort.Strings(platform)
	sort.Strings(table)
	if strings.Join(platform, ",") != strings.Join(table, ",") {
		t.Fatalf("org-less table %v, contract platform fields %v", table, platform)
	}
	if orglessRootActions["productTelemetryPlatformDashboard"] != rule.ActionAdminTelemetry {
		t.Fatalf("the dashboard's action is %q", orglessRootActions["productTelemetryPlatformDashboard"])
	}
}

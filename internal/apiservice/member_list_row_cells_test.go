package apiservice

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const adminMemberListKey = "GET /api/v1/admin/orgs/{org_id}/members"

var (
	memberListViewer    = uuid.MustParse("00000000-0000-4000-8000-0000000000d5")
	memberListOwner     = uuid.MustParse("00000000-0000-4000-8000-0000000000d6")
	memberListViewerSID = uuid.MustParse("00000000-0000-4000-8000-0000000000d7")
	memberListOwnerSID  = uuid.MustParse("00000000-0000-4000-8000-0000000000d8")
)

// withMemberListCallers adds a viewer and an owner of the token org to the probe store, beside the org admin, the org
// member and the platform admins it has.
func withMemberListCallers(store *probeStore) {
	withPlatformAdmins(store)
	store.users[memberListViewer] = probeUserRow{state: policy.UserState{IsActive: true}, email: "viewer@example.test"}
	store.users[memberListOwner] = probeUserRow{state: policy.UserState{IsActive: true}, email: "owner@example.test"}
	store.members[[2]uuid.UUID{memberListViewer, probeOrg}] = "viewer"
	store.members[[2]uuid.UUID{memberListOwner, probeOrg}] = "owner"
	store.sessions[memberListViewerSID] = policy.SessionRow{PrincipalID: memberListViewer, LegacyTokenVersion: i64ptr(0)}
	store.sessions[memberListOwnerSID] = policy.SessionRow{PrincipalID: memberListOwner, LegacyTokenVersion: i64ptr(0)}
}

// memberListCallers are the five callers of the row, by the name the cells use.
func memberListCallers(minter *eddsaMinter) map[string]cellCaller {
	callers := cellCallers(minter)
	callers["viewer"] = cellCaller{name: "viewer", token: minter.mint(memberListViewer, map[string]any{"sid": sessionid.Wire(memberListViewerSID)}), snap: cellSnapshot{orgRole: "viewer"}}
	callers["owner"] = cellCaller{name: "owner", token: minter.mint(memberListOwner, map[string]any{"sid": sessionid.Wire(memberListOwnerSID)}), snap: cellSnapshot{orgRole: "owner"}}
	return callers
}

// The admin member list is an owner and admin read: legacy refuses a member and a viewer, so with the members cell on
// the real evaluator refuses them too, with the same body and before the handler; an owner, an admin and a platform
// admin with no membership row (D6773) reach the handler. On the row's old action a member and a viewer were allowed
// by role.
func TestAdminMemberListUnderEnforceAnswersAsLegacy(t *testing.T) {
	pdp := newCellPDP(t)
	handler, minter := adminConfigServerWith(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, "members", false)
	}, withMemberListCallers)
	callers := memberListCallers(minter)
	pdp.register(callers)
	for _, tc := range []struct {
		name    string
		refused bool
		reason  authorization.Reason
	}{
		{"org member", true, authorization.ReasonRoleLacksAction},
		{"viewer", true, authorization.ReasonRoleLacksAction},
		{"org admin", false, authorization.ReasonAllowedByRole},
		{"owner", false, authorization.ReasonAllowedByRole},
		{"platform admin + no membership", false, authorization.ReasonAllowedByPlatformAdmin},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pdp.reset("evaluate")
			got := membersDo(handler, adminMemberListKey, callers[tc.name].token)
			calls := pdp.calls()
			if len(calls) != 1 {
				t.Fatalf("%d decision point calls, want 1", len(calls))
			}
			if calls[0].Action != "members.manage" || calls[0].OrganizationID != probeOrg.String() {
				t.Errorf("asked %s on %s, want members.manage on %s", calls[0].Action, calls[0].OrganizationID, probeOrg)
			}
			if r := pdp.reasonsSeen(); len(r) != 1 || r[0] != tc.reason {
				t.Errorf("reasons %v, want %s", r, tc.reason)
			}
			if tc.refused && (got.status != http.StatusForbidden || got.body != adminRequiredBody) {
				t.Errorf("%d %s, want the legacy 403 %s before the handler", got.status, got.body, adminRequiredBody)
			}
			if !tc.refused && (isGuardRefusal(got) || got.status == http.StatusForbidden) {
				t.Errorf("%d %s, want the handler's answer", got.status, got.body)
			}
		})
	}
}

// With the members cell off the decision point is not asked and the legacy guard answers the same callers.
func TestAdminMemberListLegacyPathKeepsItsAnswers(t *testing.T) {
	pdp := newCellPDP(t)
	handler, minter := adminConfigServerWith(t, nil, withMemberListCallers)
	callers := memberListCallers(minter)
	for name, refused := range map[string]bool{"org member": true, "viewer": true, "org admin": false, "owner": false, "platform admin + no membership": false} {
		t.Run(name, func(t *testing.T) {
			pdp.reset("evaluate")
			got := membersDo(handler, adminMemberListKey, callers[name].token)
			if len(pdp.calls()) != 0 {
				t.Errorf("%d decision point calls, want 0", len(pdp.calls()))
			}
			if refused && (got.status != http.StatusForbidden || got.body != adminRequiredBody) {
				t.Errorf("%d %s, want the 403 %s", got.status, got.body, adminRequiredBody)
			}
			if !refused && (isGuardRefusal(got) || got.status == http.StatusForbidden) {
				t.Errorf("%d %s, want the handler's answer", got.status, got.body)
			}
		})
	}
}

// ensureOrgAdminAccessSites maps every function of the admin package that calls ensureOrgAdminAccess to the go-api rows
// it serves. A new call site fails the census until it is named here.
var ensureOrgAdminAccessSites = map[string][]string{
	"addMember":         {"POST /api/v1/admin/orgs/{org_id}/members"},
	"createOrgInvite":   {"POST /api/v1/admin/orgs/{org_id}/invites"},
	"createUserOrg":     {"POST /api/v1/admin/users"},
	"listMembers":       {adminMemberListKey},
	"listUsers":         {"GET /api/v1/admin/users"},
	"removeMember":      {"DELETE /api/v1/admin/orgs/{org_id}/members/{user_id}"},
	"transferOwnership": {"POST /api/v1/admin/orgs/{org_id}/transfer-ownership"},
	"updateMemberRole":  {"PATCH /api/v1/admin/orgs/{org_id}/members/{user_id}"},
}

func ensureOrgAdminAccessCallers(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir("admin")
	if err != nil {
		t.Fatal(err)
	}
	var callers []string
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join("admin", entry.Name()), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || fn.Name.Name == "ensureOrgAdminAccess" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if call, ok := n.(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ensureOrgAdminAccess" {
						callers = append(callers, fn.Name.Name)
					}
				}
				return true
			})
		}
	}
	slices.Sort(callers)
	return slices.Compact(callers)
}

// adminLevelProfileRows are the "METHOD /route" keys of the go-api profile rows whose primary validator is policy.Admin
// or policy.AdminOrg: the levels that refuse a member and a viewer of the token org.
func adminLevelProfileRows(t *testing.T) []string {
	t.Helper()
	var document struct {
		Rows []struct {
			Service          string `json:"service"`
			Method           string `json:"method"`
			Route            string `json:"route"`
			PrimaryValidator struct {
				Description string `json:"description"`
			} `json:"primary_validator"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(authv1.GoAPIEndpointProfiles, &document); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, row := range document.Rows {
		level, _, _ := strings.Cut(row.PrimaryValidator.Description, ":")
		if row.Service == goAPIAccessService && (level == "policy.Admin" || level == "policy.AdminOrg") {
			keys = append(keys, row.Method+" "+row.Route)
		}
	}
	return keys
}

// The census: no go-api row whose legacy answer refuses a member and a viewer has an action they hold, or the decision
// point allows what legacy refuses. It reads every row behind ensureOrgAdminAccess and every policy.Admin and
// policy.AdminOrg row of the profile; a row with no action is never asked.
func TestNoAdminRowAsksAnActionAMemberOrViewerHolds(t *testing.T) {
	rows, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ensureOrgAdminAccessCallers(t), slices.Sorted(maps.Keys(ensureOrgAdminAccessSites)); !slices.Equal(got, want) {
		t.Fatalf("functions that call ensureOrgAdminAccess %v, the census names %v", got, want)
	}
	catalog := policycatalog.Default()
	held := func(action string) []string {
		var roles []string
		for _, role := range []string{"member", "viewer"} {
			if catalog.Allows(role, action) {
				roles = append(roles, role)
			}
		}
		return roles
	}
	for site, keys := range ensureOrgAdminAccessSites {
		for _, key := range keys {
			row, enforced := rows[key]
			if !enforced {
				continue
			}
			if roles := held(row.Action); len(roles) != 0 {
				t.Errorf("%s (%s, ensureOrgAdminAccess) asks %s, which %v hold", key, site, row.Action, roles)
			}
		}
	}
	admins := adminLevelProfileRows(t)
	if len(admins) < 100 {
		t.Fatalf("%d policy.Admin and policy.AdminOrg rows, the census no longer reads the profile", len(admins))
	}
	for _, key := range admins {
		row, enforced := rows[key]
		if !enforced {
			continue
		}
		if roles := held(row.Action); len(roles) != 0 {
			t.Errorf("%s (policy.Admin level) asks %s, which %v hold", key, row.Action, roles)
		}
	}
}

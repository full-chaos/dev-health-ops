package apiservice

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/google/uuid"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
)

// A caller with a live go-api impersonation row on an enforced platform row: the decision point is asked the row's role
// question first, as the legacy platform wrap asks it before the session (policy.Enforcer, decideLiveRow). A caller
// that lost the platform role gets the cell-off answer; one that holds it is still refused by the session.

type liveRowRow struct {
	key, level, action, namespace string
}

// liveRowPlatformRows is every go-api profile row whose resource kind is platform and that the api listener mounts.
func liveRowPlatformRows(t *testing.T) []liveRowRow {
	t.Helper()
	mapping, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
	if err != nil {
		t.Fatal(err)
	}
	regs := scanRegistrations(t, "../api", ".")
	var out []liveRowRow
	for _, m := range mountedRoutes(t) {
		if m.Listener != "api" {
			continue
		}
		key := m.Method + " " + m.Pattern
		row, ok := mapping[key]
		if !ok || row.ResourceType != policycatalog.ScopePlatform {
			continue
		}
		level := ""
		if reg, ok := registrationFor(regs, m); ok {
			level = reg.Level
		}
		if level == "" {
			level = dispatchedLevels[m.key()]
		}
		out = append(out, liveRowRow{key: key, level: level, action: row.Action, namespace: row.Namespace})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	if len(out) < 30 {
		t.Fatalf("%d mounted platform rows, want the go-api profile's (30 or more)", len(out))
	}
	return out
}

func liveRowDo(h http.Handler, key, token string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	var body io.Reader
	if raw, ok := lifecycleBodies[key]; ok {
		body = strings.NewReader(raw)
	} else if raw, ok := adminUsersBodies[key]; ok {
		body = strings.NewReader(raw)
	} else if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
		body = strings.NewReader("{}")
	}
	request := httptest.NewRequest(method, cellPath(key), body)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

type liveRowClass struct {
	label, orgRole string
	orgClaim       bool
}

var liveRowClasses = []liveRowClass{
	{"orgless", "", false},
	{"org-owner", "owner", true},
	{"org-admin", "admin", true},
	{"org-member", "member", true},
	{"org-viewer", "viewer", true},
	{"org-claim-without-membership", "", true},
}

// liveRowServed is one class's answer ("<status> <body>") and decision point requests on every row.
type liveRowServed struct {
	answer map[string]string
	calls  map[string][]pep.Request
}

// liveRowServe serves every row to one class with the cell off (on=false) or on: the probe admin's impersonation row
// live (live), its platform role kept (superuser) or removed, the decision point in mode.
func liveRowServe(t *testing.T, rows []liveRowRow, class liveRowClass, on, live, superuser bool, mode string) liveRowServed {
	t.Helper()
	pdp := newCellPDP(t)
	var enforcer func(*testing.T) *policy.Enforcer
	if on {
		enforcer = func(t *testing.T) *policy.Enforcer {
			mapping, err := policy.ParseShadowRows(authv1.GoAPIEndpointProfiles, goAPIAccessService)
			if err != nil {
				t.Fatal(err)
			}
			enf, err := policy.NewEnforcer(policy.EnforceConfig{Rows: mapping, Switch: authzenforce.Parse(authzenforce.GoAPI, "admin"),
				Decider: pdp, Timeout: 2 * time.Second, Logger: quietLogger()})
			if err != nil {
				t.Fatal(err)
			}
			return enf
		}
	}
	scopeRows, err := goAPIScopeRows()
	if err != nil {
		t.Fatal(err)
	}
	handler, minter := adminConfigServerScoped(t, enforcer, func(s *probeStore) {
		withUsersCellCallers(s)
		if class.orgRole != "" {
			s.members[[2]uuid.UUID{probeAdminUser, probeOrg}] = class.orgRole
		}
		user := s.users[probeAdminUser]
		user.state.IsSuperuser = superuser
		s.users[probeAdminUser] = user
		if !live {
			delete(s.imperson, probeAdminUser)
		}
	}, lifecycleRoutes, func(scope *policy.Scope) { scope.DeclaredRows(scopeRows) })
	claims := map[string]any{"sid": sessionid.Wire(cellAdminSID)}
	if !class.orgClaim {
		claims["org_id"] = nil
	}
	token := minter.mint(probeAdminUser, claims)
	snap := cellSnapshot{orgRole: class.orgRole}
	if superuser {
		snap.platformRole = "platform_admin"
	}
	pdp.registerUsers([]usersCaller{{token: token, snap: snap, judged: true}})
	out := liveRowServed{answer: map[string]string{}, calls: map[string][]pep.Request{}}
	for _, row := range rows {
		pdp.reset(mode)
		got := liveRowDo(handler, row.key, token)
		out.answer[row.key] = strings.TrimSpace(strconv.Itoa(got.status) + " " + got.body)
		out.calls[row.key] = pdp.calls()
	}
	return out
}

func liveRowAdminOrWrite(row liveRowRow) bool {
	return row.level == "Admin" && !strings.HasPrefix(row.key, "GET ")
}

func liveRowAdminOrRead(row liveRowRow) bool {
	return row.level == "Admin" && strings.HasPrefix(row.key, "GET ")
}

func liveRowExemptRow(row liveRowRow) bool {
	return strings.Contains(row.key, "/admin/impersonate/")
}

// liveRowHandler: the answer is the handler's (the rig's pool dials a closed port, so a reached handler answers 500).
func liveRowHandler(answer string) bool {
	return strings.HasPrefix(answer, "500 ") || strings.HasPrefix(answer, "2")
}

// A caller whose platform role was removed while its legacy impersonation row is live gets the same answer with the
// admin cell on as with it off, on every platform row and every caller class (product rule 1). D6833: an admin-or
// write that the cell-off guard lets through to the handler keeps the session refusal with the cell on.
func TestADemotedCallerWithALiveRowGetsTheCellOffAnswerOnEveryPlatformRow(t *testing.T) {
	rows := liveRowPlatformRows(t)
	diff, same, d6833 := 0, 0, 0
	for _, class := range liveRowClasses {
		off := liveRowServe(t, rows, class, false, true, false, "evaluate")
		on := liveRowServe(t, rows, class, true, true, false, "evaluate")
		for _, row := range rows {
			if liveRowAdminOrWrite(row) && liveRowHandler(off.answer[row.key]) {
				d6833++
				if on.answer[row.key] != "403 "+impersonationBody {
					diff++
					t.Errorf("D6833 %s | %s | off %s | on %s, want the session refusal", class.label, row.key, off.answer[row.key], on.answer[row.key])
				}
				continue
			}
			if off.answer[row.key] != on.answer[row.key] {
				diff++
				t.Errorf("DIFF %s | %s | off %s | on %s", class.label, row.key, off.answer[row.key], on.answer[row.key])
				continue
			}
			same++
		}
	}
	if d6833 < 8 {
		t.Errorf("%d D6833 cells, want the 8 demoted org owner/admin /admin/users writes", d6833)
	}
	t.Logf("DEMOTED cells=%d diff=%d same=%d d6833=%d", diff+same+d6833, diff, same, d6833)
}

// A caller that keeps its platform role and has a live impersonation row is refused on every platform row that is
// not live-row exempt, with the cell on as with it off, and never reaches a handler; an admin-or write keeps the
// session refusal (D6833). The exempt rows and the admin-or reads keep the cell-off answer.
func TestALiveRowKeepsTheSessionBlockOnEveryPlatformRow(t *testing.T) {
	rows := liveRowPlatformRows(t)
	bad, total := 0, 0
	for _, class := range liveRowClasses {
		off := liveRowServe(t, rows, class, false, true, true, "evaluate")
		on := liveRowServe(t, rows, class, true, true, true, "evaluate")
		for _, row := range rows {
			total++
			o, n := off.answer[row.key], on.answer[row.key]
			switch {
			case liveRowExemptRow(row) || liveRowAdminOrRead(row):
				if o != n {
					bad++
					t.Errorf("KEPT DIFF %s | %s | off %s | on %s", class.label, row.key, o, n)
				}
			case liveRowAdminOrWrite(row) && liveRowHandler(o):
				if n != "403 "+impersonationBody {
					bad++
					t.Errorf("KEPT D6833 %s | %s | on %s", class.label, row.key, n)
				}
			default:
				if o != n || !strings.HasPrefix(n, "403 ") {
					bad++
					t.Errorf("KEPT REACHED %s | %s | off %s | on %s", class.label, row.key, o, n)
				}
			}
		}
	}
	t.Logf("KEPT cells=%d bad=%d", total, bad)
}

// liveRowWantCalls is the role question decideLiveRow asks of the caller's own bearer on row: the row's platform action,
// and on an admin-or row with a token organization members.manage in that organization too.
func liveRowWantCalls(row liveRowRow, orgClaim bool) []pep.Request {
	want := []pep.Request{{Action: row.action, ResourceType: policycatalog.ScopePlatform, ResourceKey: "platform"}}
	if row.level == "Admin" && orgClaim {
		org := strings.ToLower(probeOrg.String())
		want = append(want, pep.Request{Action: "members.manage", ResourceType: policycatalog.ScopeOrganization, ResourceKey: org, OrganizationID: org})
	}
	return want
}

// The decision point requests of one live-row request on a platform row, demoted caller and platform admin alike:
// exactly the row's role question (one platform request; a batch of two on an admin-or row with a token organization),
// asked once per request. Each is a high-risk action, so auth-service writes one auth.authorize.high_risk_decided event
// per request asked.
func TestALiveRowOnAPlatformRowAsksExactlyTheRoleQuestion(t *testing.T) {
	rows := liveRowPlatformRows(t)
	classes := []liveRowClass{liveRowClasses[0], liveRowClasses[1], liveRowClasses[2]}
	asked := 0
	for _, superuser := range []bool{false, true} {
		for _, class := range classes {
			on := liveRowServe(t, rows, class, true, true, superuser, "evaluate")
			for _, row := range rows {
				// The exempt rows are decided as any row; a demoted caller's admin-or read reads no row (the read
				// limit) and a platform admin's keeps the legacy answer: none is a live-row question.
				if liveRowExemptRow(row) || liveRowAdminOrRead(row) {
					continue
				}
				want := liveRowWantCalls(row, class.orgClaim)
				got := on.calls[row.key]
				for i := range got {
					got[i].PrincipalReference, got[i].RequestID = "", ""
				}
				if !slices.Equal(want, got) {
					t.Errorf("superuser=%v %s | %s: decision point requests %+v, want %+v", superuser, class.label, row.key, got, want)
				}
				for _, request := range got {
					action, err := policycatalog.Default().Action(request.Action)
					if err != nil || action.RiskClass != policycatalog.RiskHigh {
						t.Errorf("%s: %s is not a high-risk action (%v): the audit event count changes", row.key, request.Action, err)
					}
				}
				asked += len(got)
			}
		}
	}
	t.Logf("ASKED requests=%d", asked)
}

// A decision point that fails on the role question leaves a live-row caller refused with the session's 403: never a
// 503, never a handler. Logged for the compare with main's answers.
func TestALiveRowRoleQuestionFailureIsTheSessionRefusal(t *testing.T) {
	rows := liveRowPlatformRows(t)
	refused := 0
	for _, class := range liveRowClasses {
		for _, c := range []struct {
			label           string
			live, superuser bool
		}{{"demoted-live", true, false}, {"demoted-no-row", false, false}, {"admin-live", true, true}} {
			on := liveRowServe(t, rows, class, true, c.live, c.superuser, "fail")
			for _, row := range rows {
				answer := on.answer[row.key]
				t.Logf("FAIL %s | %s | %s | %s", c.label, class.label, row.key, answer)
				if !c.live || liveRowExemptRow(row) || liveRowAdminOrRead(row) {
					continue
				}
				if !strings.HasPrefix(answer, "403 ") {
					t.Errorf("%s | %s | %s: %s, want a 403 (the decision point failed on a live row)", c.label, class.label, row.key, answer)
				}
				if answer == "403 "+impersonationBody && len(on.calls[row.key]) > 0 {
					refused++
				}
			}
		}
	}
	if refused == 0 {
		t.Error("no live-row cell asked the failing decision point and got the session refusal")
	}
	t.Logf("FAILCLOSED session refusals after a failed role question=%d", refused)
}

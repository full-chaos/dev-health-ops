package apiservice

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// CHAOS-13603 (part of CHAOS-13592): PATCH /api/v1/orgs/me is judged as org.write and POST /api/v1/telemetry/report as
// admin.telemetry. A refused caller keeps the text the handler wrote before the decision point decided the row (product
// rule 1: the same answers as legacy). The cells run the real handlers with the cells off (legacy) and on (the real
// evaluator decides), status and body.

// denyTextNamespaces are the cells these rows turn on: org.write is in org, admin.telemetry in admin.
const denyTextNamespaces = "org,admin"

// legacyDenyBodies are the exact bodies a refused caller got before the rows had an action, one per row that sets a
// DenyText. The census below fails a DenyTextRows entry with no row here, and a row here with no DenyTextRows entry.
var legacyDenyBodies = map[string]string{
	"PATCH /api/v1/orgs/me":         `{"detail":"You must be an org admin or owner to update organization settings"}`,
	"POST /api/v1/telemetry/report": `{"detail":"Instance-wide telemetry statistics require a platform role"}`,
}

// denyTextActions is the action each row is asked as.
var denyTextActions = map[string]string{
	"PATCH /api/v1/orgs/me":         "org.write",
	"POST /api/v1/telemetry/report": "admin.telemetry",
}

func denyDo(handler http.Handler, key, token string, headers map[string]string) cellAnswer {
	method, _, _ := strings.Cut(key, " ")
	var body *strings.Reader
	if method == http.MethodPatch {
		body = strings.NewReader("{}")
	} else {
		body = strings.NewReader("")
	}
	request := httptest.NewRequest(method, cellPath(key), body)
	request.Header.Set("Authorization", "Bearer "+token)
	if method == http.MethodPatch {
		request.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return cellAnswer{status: recorder.Code, body: recorder.Body.String(), header: recorder.Header()}
}

// capturedLog is a logger whose lines the cells read: the report refusal counts and logs together, so the line count is
// the counter's increments.
type capturedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (c *capturedLog) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *capturedLog) refusals() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Count(c.buf.String(), "telemetry report refused")
}

func (c *capturedLog) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.Reset()
}

// denyClasses are the deny-side classes plus the org roles, over a minter.
func denyClasses(t *testing.T, minter *eddsaMinter) []deniedClass {
	t.Helper()
	classes := deniedClasses(t, minter)
	byName := map[string]usersCaller{}
	for _, c := range usersCallers(t, minter) {
		byName[c.name] = c
	}
	for _, name := range []string{"org owner", "org admin", "org member", "org viewer", "platform admin + org member"} {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("no caller class %q", name)
		}
		have := false
		for _, existing := range classes {
			have = have || existing.name == name
		}
		if !have {
			classes = append(classes, deniedClass{usersCaller: c})
		}
	}
	return classes
}

// denyTextServers serve the real routes with the cells off and on. refuseLegacy is the production posture of the
// impersonation aggregate (the org scope refuses a live impersonation row with 403 before the guard).
func denyTextServers(t *testing.T, pdp *cellPDP, log *capturedLog, refuseLegacy bool) (off, on http.Handler, offClasses, onClasses []deniedClass) {
	t.Helper()
	routes := func(_ *testing.T, deps Deps) []httpapi.Route {
		return Routes(deps, slog.New(slog.NewTextHandler(log, nil)))
	}
	scoped := func(scope *policy.Scope) { scope.RefuseLegacyImpersonation(refuseLegacy) }
	off, offMinter := adminConfigServerScoped(t, nil, withDeniedCellCallers, routes, scoped)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, denyTextNamespaces, false)
	}, withDeniedCellCallers, routes, scoped)
	return off, on, denyClasses(t, offMinter), denyClasses(t, onMinter)
}

func judgedOf(classes []deniedClass) []usersCaller {
	out := make([]usersCaller, 0, len(classes))
	for _, c := range classes {
		out = append(out, c.usersCaller)
	}
	return out
}

// dbBoundRows are the rows whose legacy handler reads the caller's membership row in Postgres before it refuses: with the
// closed pool of these cells the legacy answer is its database 500, so the legacy side of those cells runs on a real
// Postgres in TestLegacyNoRowAndPlatformAdminAnswersOrgsMePatchVenueOracle. Here the cells-on answer is held to the
// legacy body.
var dbBoundRows = map[string]bool{"PATCH /api/v1/orgs/me": true}

func classByName(t *testing.T, classes []deniedClass, name string) deniedClass {
	t.Helper()
	for _, c := range classes {
		if c.name == name {
			return c
		}
	}
	t.Fatalf("no caller class %q", name)
	return deniedClass{}
}

// C1 census and the legacy-body cell: every row that mounts a DenyText has an executed cell that asserts the legacy
// body for a refused caller (cells off and on for the row whose legacy handler needs no database, cells on for the
// database-bound row, whose legacy side is the venue oracle). A DenyText row with no cell fails.
func TestEveryDenyTextRowHasAnExecutedLegacyBodyCell(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log, true)
	got := policy.DenyTextRows()
	want := make([]string, 0, len(legacyDenyBodies))
	for key := range legacyDenyBodies {
		want = append(want, key)
	}
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("DenyText rows %v, cells for %v: a row that sets a DenyText needs a legacy-body cell", got, want)
	}
	refused := map[string]string{
		"PATCH /api/v1/orgs/me":         "org member",
		"POST /api/v1/telemetry/report": "org admin",
	}
	for key, body := range legacyDenyBodies {
		class := refused[key]
		offToken := classByName(t, offClasses, class).token
		onToken := classByName(t, onClasses, class).token
		pdp.registerUsers(judgedOf(onClasses))
		pdp.reset("evaluate")
		legacy := denyDo(off, key, offToken, nil)
		got := denyDo(on, key, onToken, nil)
		if !dbBoundRows[key] && (legacy.status != http.StatusForbidden || strings.TrimSpace(legacy.body) != body) {
			t.Errorf("%s / %s, cells off: %d %s, want 403 %s", key, class, legacy.status, strings.TrimSpace(legacy.body), body)
		}
		if got.status != http.StatusForbidden || strings.TrimSpace(got.body) != body {
			t.Errorf("%s / %s, cells on: %d %s, want 403 %s", key, class, got.status, strings.TrimSpace(got.body), body)
		}
		t.Logf("CELL\tdeny-text\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s", key, class, legacy.status, short(legacy.body), got.status, short(got.body))
	}
}

// Every caller class, both rows: the answer with the cells on equals the answer with the cells off, status and body, on
// the real handlers. The database-bound row's refused classes answer the legacy body with the cells on while the
// closed pool gives the legacy side its database 500; their legacy side is the venue oracle. Row 15 also runs without the
// impersonation aggregate (the handler's own refusal, the legacy text) as well as with it (the scope refuses first).
func TestDenyTextRowsAnswerEveryCallerClassAsLegacy(t *testing.T) {
	for _, posture := range []struct {
		name   string
		refuse bool
	}{{"impersonation aggregate on", true}, {"impersonation aggregate off", false}} {
		pdp := newCellPDP(t)
		log := &capturedLog{}
		off, on, offClasses, onClasses := denyTextServers(t, pdp, log, posture.refuse)
		for key, action := range denyTextActions {
			if key == "PATCH /api/v1/orgs/me" && !posture.refuse {
				continue // the ruling for the impersonated write is the production posture: the aggregate on
			}
			for i := range onClasses {
				name := onClasses[i].name
				pdp.registerUsers(judgedOf(onClasses))
				pdp.reset("evaluate")
				legacy := denyDo(off, key, offClasses[i].token, offClasses[i].headers)
				if calls := pdp.calls(); len(calls) != 0 {
					t.Fatalf("%s / %s: %d decision point calls with the cells off", key, name, len(calls))
				}
				got := denyDo(on, key, onClasses[i].token, onClasses[i].headers)
				calls := pdp.calls()
				result := "EQUAL"
				dbBound := dbBoundRows[key] && legacy.status == http.StatusInternalServerError &&
					got.status == http.StatusForbidden && strings.TrimSpace(got.body) == legacyDenyBodies[key]
				switch {
				case dbBound:
					result = "DB-BOUND (legacy side: venue oracle)"
				case got.status != legacy.status || got.body != legacy.body:
					result = "DIFF"
					t.Errorf("%s / %s / %s: new %d %s, legacy %d %s", posture.name, key, name, got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
				}
				t.Logf("CELL\t%s\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\tpdp_calls=%d(%s)\t%s", posture.name, key, name, legacy.status, short(legacy.body), got.status, short(got.body), len(calls), action, result)
				wantCalls, wantAction := 1, action
				switch {
				case name == "impersonating platform admin":
					wantCalls = 0
				case key == "PATCH /api/v1/orgs/me" && (name == "no platform role, no org claim" || name == "platform admin, no org claim"):
					// an org-less caller on an organization row is asked the legacy org-less action
					wantAction = policy.ActionAdminOrgs
				}
				if len(calls) != wantCalls || (len(calls) == 1 && calls[0].Action != wantAction) {
					t.Errorf("%s / %s / %s: decision point calls %+v, want %d asking %s", posture.name, key, name, calls, wantCalls, wantAction)
				}
				if name == "impersonating platform admin" {
					if got.status != http.StatusForbidden {
						t.Errorf("%s / %s / live impersonation row: %d %s, want 403", posture.name, key, got.status, strings.TrimSpace(got.body))
					}
					switch {
					case key == "POST /api/v1/telemetry/report" && !posture.refuse && strings.TrimSpace(got.body) != legacyDenyBodies[key]:
						t.Errorf("%s / %s / live impersonation row: body %s, want the legacy text", posture.name, key, strings.TrimSpace(got.body))
					case posture.refuse && strings.TrimSpace(got.body) != `{"detail":"Not allowed in an impersonation session"}`:
						t.Errorf("%s / %s / live impersonation row: body %s, want the impersonation text", posture.name, key, strings.TrimSpace(got.body))
					}
				}
			}
		}
	}
}

// C4, the rule-1 cells: a platform admin with no membership. Row 11: with the cells on, 403 with the legacy text
// (org.write has no platform-admin flag); the legacy side runs on a real Postgres in the venue oracle. Row 15: a holder keeps
// every answer with an X-Org-Id of an organization he is no member of: the cells on answer what the cells off answer.
func TestPlatformAdminWithNoMembershipKeepsTheLegacyAnswers(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log, true)
	const class = "platform admin, no membership"
	offC, onC := classByName(t, offClasses, class), classByName(t, onClasses, class)
	pdp.registerUsers(judgedOf(onClasses))

	pdp.reset("evaluate")
	got := denyDo(on, "PATCH /api/v1/orgs/me", onC.token, nil)
	want := legacyDenyBodies["PATCH /api/v1/orgs/me"]
	if got.status != http.StatusForbidden || strings.TrimSpace(got.body) != want {
		t.Errorf("row 11, cells on: %d %s, want 403 %s", got.status, strings.TrimSpace(got.body), want)
	}
	if calls := pdp.calls(); len(calls) != 1 || calls[0].Action != "org.write" || calls[0].OrganizationID != probeOrg.String() {
		t.Errorf("row 11: decision point calls %+v, want one org.write on the token organization", calls)
	}

	other := map[string]string{"X-Org-Id": probeOtherOrg.String()}
	pdp.reset("evaluate")
	legacy := denyDo(off, "POST /api/v1/telemetry/report", offC.token, other)
	got = denyDo(on, "POST /api/v1/telemetry/report", onC.token, other)
	if got.status != legacy.status || got.body != legacy.body {
		t.Errorf("row 15, X-Org-Id of another org: cells on %d %s, cells off %d %s", got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
	}
	t.Logf("CELL\trule-1\trow 15 holder, other org\tlegacy=%d:%s\tnew=%d:%s", legacy.status, short(legacy.body), got.status, short(got.body))
}

// The report refusal counts and logs once per refusal as the handler did: a holder none, a non-holder one, the same
// with the cells off and on.
func TestReportRefusalIsCountedOnceAndAHolderNever(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log, true)
	const key = "POST /api/v1/telemetry/report"
	for _, cell := range []struct {
		class string
		want  int
	}{{"org admin, X-Org-Id of another org", 1}, {"org member", 1}, {"platform admin, no membership", 0}, {"platform admin + org member", 0}} {
		pdp.registerUsers(judgedOf(onClasses))
		for mode, h := range map[string]struct {
			handler http.Handler
			token   string
		}{
			"cells off": {off, classByName(t, offClasses, cell.class).token},
			"cells on":  {on, classByName(t, onClasses, cell.class).token},
		} {
			pdp.reset("evaluate")
			log.reset()
			answer := denyDo(h.handler, key, h.token, nil)
			if got := log.refusals(); got != cell.want {
				t.Errorf("%s / %s: %d refusal log lines (%d %s), want %d", mode, cell.class, got, answer.status, strings.TrimSpace(answer.body), cell.want)
			}
		}
	}
}

// Each row's profile action is the action its cells ask, and no other.
func TestDenyTextRowsMatchTheProfile(t *testing.T) {
	got := map[string]string{}
	for _, row := range loadGoAPIRows(t) {
		route, _ := row["route"].(string)
		method, _ := row["method"].(string)
		key := method + " " + route
		if _, ok := denyTextActions[key]; ok && row["service"] == goAPIAccessService {
			got[key], _ = row["action"].(string)
		}
	}
	for key, want := range denyTextActions {
		if got[key] != want {
			t.Errorf("%s: profile action %q, cells ask %q", key, got[key], want)
		}
	}
}

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

func denyTextServers(t *testing.T, pdp *cellPDP, log *capturedLog) (off, on http.Handler, offClasses, onClasses []deniedClass) {
	t.Helper()
	routes := func(_ *testing.T, deps Deps) []httpapi.Route {
		return Routes(deps, slog.New(slog.NewTextHandler(log, nil)))
	}
	off, offMinter := adminConfigServerScoped(t, nil, withDeniedCellCallers, routes, nil)
	on, onMinter := adminConfigServerScoped(t, func(t *testing.T) *policy.Enforcer {
		return newCellEnforcer(t, pdp, denyTextNamespaces, false)
	}, withDeniedCellCallers, routes, nil)
	return off, on, deniedClasses(t, offMinter), deniedClasses(t, onMinter)
}

func judgedOf(classes []deniedClass) []usersCaller {
	out := make([]usersCaller, 0, len(classes))
	for _, c := range classes {
		out = append(out, c.usersCaller)
	}
	return out
}

// C1 census and the legacy-body cell: every row that mounts a DenyText has an executed cell that asserts the legacy
// body, the same with the cells off and on, for a refused caller. A DenyText row with no cell fails.
func TestEveryDenyTextRowHasAnExecutedLegacyBodyCell(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log)
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
		var offToken, onToken string
		for i := range onClasses {
			if onClasses[i].name == class {
				offToken, onToken = offClasses[i].token, onClasses[i].token
			}
		}
		if class == "" || offToken == "" {
			t.Fatalf("%s: no caller class %q among the denied classes", key, class)
		}
		pdp.registerUsers(judgedOf(onClasses))
		pdp.reset("evaluate")
		legacy := denyDo(off, key, offToken, nil)
		got := denyDo(on, key, onToken, nil)
		if legacy.status != http.StatusForbidden || strings.TrimSpace(legacy.body) != body {
			t.Errorf("%s / %s, cells off: %d %s, want 403 %s", key, class, legacy.status, strings.TrimSpace(legacy.body), body)
		}
		if got.status != http.StatusForbidden || strings.TrimSpace(got.body) != body {
			t.Errorf("%s / %s, cells on: %d %s, want 403 %s", key, class, got.status, strings.TrimSpace(got.body), body)
		}
		t.Logf("CELL\tdeny-text\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s", key, class, legacy.status, short(legacy.body), got.status, short(got.body))
	}
}

// Every caller class, both rows: the answer with the cells on equals the answer with the cells off, status and body, on
// the real handlers. The org.write row is asked once on the token organization; the admin.telemetry row is asked as a
// platform question and an org-admin caller is refused by the legacy platform wrap before any ask.
func TestDenyTextRowsAnswerEveryCallerClassAsLegacy(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log)
	for key, action := range denyTextActions {
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
			if name == noRowClass && key == "PATCH /api/v1/orgs/me" {
				// the no-row class is the named answer change of the M-1 ruling (b) on the org rows; this row keeps its text
				if got.status != legacy.status || got.body != legacy.body {
					result = "DIFF"
					t.Errorf("%s / %s: new %d %s, legacy %d %s", key, name, got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
				}
			} else if got.status != legacy.status || got.body != legacy.body {
				result = "DIFF"
				t.Errorf("%s / %s: new %d %s, legacy %d %s", key, name, got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
			}
			t.Logf("CELL\t%s\t%s\tlegacy=%d:%s\tnew=%d:%s\tpdp_calls=%d(%s)\t%s", key, name, legacy.status, short(legacy.body), got.status, short(got.body), len(calls), action, result)
		}
	}
}

// C4, the rule-1 cells: a platform admin with no membership. Row 11: 403 with the legacy text (org.write has no
// platform-admin flag), the same off and on. Row 15: a holder keeps every answer with an X-Org-Id of an organization he is
// no member of: the cells on answer what the cells off answer.
func TestPlatformAdminWithNoMembershipKeepsTheLegacyAnswers(t *testing.T) {
	pdp := newCellPDP(t)
	log := &capturedLog{}
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log)
	find := func(classes []deniedClass, name string) deniedClass {
		for _, c := range classes {
			if c.name == name {
				return c
			}
		}
		t.Fatalf("no class %q", name)
		return deniedClass{}
	}
	const class = "platform admin, no membership"
	offC, onC := find(offClasses, class), find(onClasses, class)
	pdp.registerUsers(judgedOf(onClasses))

	pdp.reset("evaluate")
	legacy := denyDo(off, "PATCH /api/v1/orgs/me", offC.token, nil)
	got := denyDo(on, "PATCH /api/v1/orgs/me", onC.token, nil)
	want := legacyDenyBodies["PATCH /api/v1/orgs/me"]
	if legacy.status != http.StatusForbidden || strings.TrimSpace(legacy.body) != want {
		t.Errorf("row 11, cells off: %d %s, want 403 %s", legacy.status, strings.TrimSpace(legacy.body), want)
	}
	if got.status != legacy.status || got.body != legacy.body {
		t.Errorf("row 11, cells on: %d %s, legacy %d %s", got.status, strings.TrimSpace(got.body), legacy.status, strings.TrimSpace(legacy.body))
	}

	other := map[string]string{"X-Org-Id": probeOtherOrg.String()}
	pdp.reset("evaluate")
	legacy = denyDo(off, "POST /api/v1/telemetry/report", offC.token, other)
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
	off, on, offClasses, onClasses := denyTextServers(t, pdp, log)
	byName := func(classes []deniedClass, name string) string {
		for _, c := range classes {
			if c.name == name {
				return c.token
			}
		}
		t.Fatalf("no class %q", name)
		return ""
	}
	const key = "POST /api/v1/telemetry/report"
	for _, cell := range []struct {
		class string
		want  int
	}{{"org admin, X-Org-Id of another org", 1}, {"platform admin, no membership", 0}} {
		pdp.registerUsers(judgedOf(onClasses))
		for mode, h := range map[string]struct {
			handler http.Handler
			token   string
		}{
			"cells off": {off, byName(offClasses, cell.class)},
			"cells on":  {on, byName(onClasses, cell.class)},
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

package policy

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	wantUseEvent = "api.delegated_use"
	invoicesRow  = "GET /api/v1/billing/invoices"
)

// useLines returns the parsed delegated-use lines of the log buffer.
func useLines(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, raw := range bytes.Split(logs.Bytes(), []byte("\n")) {
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var line map[string]any
		if err := json.Unmarshal(raw, &line); err != nil {
			t.Fatalf("log line is not JSON: %q", raw)
		}
		if line["event"] == wantUseEvent {
			out = append(out, line)
		}
	}
	return out
}

func jsonLogs() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return logging.NewJSON(buf, slog.LevelDebug), buf
}

func wantUseLine(t *testing.T, lines []map[string]any, sid string) {
	t.Helper()
	if len(lines) != 1 {
		t.Fatalf("delegated-use lines = %d, want exactly 1", len(lines))
	}
	want := map[string]string{"route": invoicesRow, "actor": actorID.String(), "target": userID.String(), "sid": sid}
	for key, value := range want {
		if lines[0][key] != value {
			t.Errorf("line %s = %v, want %s", key, lines[0][key], value)
		}
	}
	if lines[0]["level"] != "INFO" {
		t.Errorf("level = %v, want INFO", lines[0]["level"])
	}
}

type useCell struct {
	actor    bool
	noOrg    bool
	admitted func(string) bool
	enforce  *testPDP
	shadow   *testPDP
	// namespace is the enforced namespace of the Enforcer (default billing).
	namespace string
	// quiet discards the logger.
	quiet bool
	// row and path override the invoices route; an id in the path pins that no path value is logged.
	row, path string
}

// serveUse serves GET invoices to an access-token bearer as the cell says and returns the answer and the log.
func serveUse(t *testing.T, c useCell) (response, *bytes.Buffer) {
	t.Helper()
	store := personaStore(personaAdmin)
	if c.noOrg {
		store.users[userID] = UserState{IsActive: true, Platform: only(ActionAdminOrgs)}
	}
	claims := userClaims(func(a *AccessTokenClaims) {
		if c.actor {
			actor := "prn_" + wireID(actorID)
			a.Actor = &actor
		}
		if c.noOrg {
			a.OrgID = nil
		}
	})
	auth := routed(t, store, &fakeValidator{result: claims})
	if c.admitted != nil {
		auth.DelegatedRows(c.admitted)
	}
	logger, logs := jsonLogs()
	if c.quiet {
		logger = quiet()
	}
	guard := NewGuard(auth, logger)
	if c.enforce != nil {
		namespace := c.namespace
		if namespace == "" {
			namespace = "billing"
		}
		guard.SetEnforcer(parityEnforcer(t, c.enforce, goAPIRows(t), namespace))
	}
	var shadow *Shadow
	if c.shadow != nil {
		shadow, _ = newShadowFor(t, c.shadow.client(t), nil)
		guard.SetShadow(shadow)
	}
	row, path := invoicesRow, "/api/v1/billing/invoices"
	if c.row != "" {
		row, path = c.row, c.path
	}
	mux := http.NewServeMux()
	mux.Handle(row, guard.Wrap(Authenticated, echo))
	got := enforceRequest(mux, http.MethodGet, path)
	if shadow != nil {
		shadow.wg.Wait()
	}
	return got, logs
}

func admitAll(string) bool { return true }

// C1: a delegated bearer served on a row the PDP does not decide logs one line with the four ids.
func TestDelegatedBearerServedWithoutThePDPLogsOneLineWithFourIDs(t *testing.T) {
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	wantUseLine(t, useLines(t, logs), liveSID.String())
}

// C3: a plain bearer logs nothing.
func TestPlainBearerLogsNoDelegatedUseLine(t *testing.T) {
	got, logs := serveUse(t, useCell{admitted: admitAll})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	if lines := useLines(t, logs); len(lines) != 0 {
		t.Fatalf("plain bearer logged %d lines, want 0", len(lines))
	}
}

// C4: a row the PDP decides (allow) is recorded by the authorize audit row, so go-api logs no second line.
func TestDelegatedBearerOnAPDPDecidedRowLogsNoUseLine(t *testing.T) {
	recordEnforce(t)
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll, enforce: newTestPDP(t, allowAll)})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200 from the PDP allow", got.status, got.body)
	}
	if lines := useLines(t, logs); len(lines) != 0 {
		t.Fatalf("PDP-decided row logged %d lines, want 0", len(lines))
	}
	recordEnforce(t)
	got, logs = serveUse(t, useCell{actor: true, admitted: admitAll, enforce: newTestPDP(t, denyWith("role_lacks_action"))})
	if got.status != http.StatusForbidden {
		t.Fatalf("answer %d %s, want 403 from the PDP deny", got.status, got.body)
	}
	if lines := useLines(t, logs); len(lines) != 0 {
		t.Fatalf("PDP deny logged %d lines, want 0", len(lines))
	}
}

// C5: a cell is on but the PDP cannot judge the caller (a token with no organization): the legacy check answers and
// the request is served, so one line.
func TestDelegatedBearerTheEnforcerCannotJudgeLogsOneLine(t *testing.T) {
	recordEnforce(t)
	pdp := newTestPDP(t, denyWith("role_lacks_action"))
	got, logs := serveUse(t, useCell{actor: true, noOrg: true, admitted: admitAll, enforce: pdp})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want the legacy 200", got.status, got.body)
	}
	if len(pdp.seen) != 0 {
		t.Fatalf("the PDP was asked %d times, want 0: the cell must be a legacy verdict", len(pdp.seen))
	}
	lines := useLines(t, logs)
	if len(lines) != 1 {
		t.Fatalf("lines = %d, want 1", len(lines))
	}
}

// C6: a delegated bearer on a row that does not admit it is refused and logs no use line.
func TestRefusedDelegatedBearerLogsNoUseLine(t *testing.T) {
	got, logs := serveUse(t, useCell{actor: true, admitted: func(string) bool { return false }})
	if got.status != http.StatusForbidden {
		t.Fatalf("answer %d %s, want 403", got.status, got.body)
	}
	if lines := useLines(t, logs); len(lines) != 0 {
		t.Fatalf("refused request logged %d lines, want 0", len(lines))
	}
}

// C7: the line carries ids only.
func TestDelegatedUseLineCarriesIDsOnly(t *testing.T) {
	_, logs := serveUse(t, useCell{actor: true, admitted: admitAll})
	if len(useLines(t, logs)) != 1 {
		t.Fatal("want the line to inspect")
	}
	raw := logs.String()
	for _, banned := range []string{shadowBearer, "Bearer", "@", "user@example.com", ownOrg} {
		if strings.Contains(raw, banned) {
			t.Errorf("log carries %q: ids only", banned)
		}
	}
}

// C8: the fixture bearer's session id (the user id) is the sid; every guard form that admits the bearer logs once.
func TestDelegatedUseOnEveryAdmittingWrapLogsOneLine(t *testing.T) {
	for _, w := range delegatedWraps {
		store := newStore()
		if w.store != nil {
			w.store(store)
		}
		auth := authenticator(t, store)
		auth.DelegatedRows(onlyAllowedRow)
		logger, logs := jsonLogs()
		guard := NewGuard(auth, logger)
		got := atRow(allowedRow, w.wrap(guard, echo), bearer(t, both(w.claims, delegatedClaim)))
		lines := useLines(t, logs)
		if got.status != http.StatusOK {
			t.Errorf("%s: answer %d, want 200", w.name, got.status)
			continue
		}
		if len(lines) != 1 {
			t.Errorf("%s: lines = %d, want 1", w.name, len(lines))
			continue
		}
		if lines[0]["sid"] != userID.String() || lines[0]["actor"] != actorID.String() || lines[0]["target"] != userID.String() || lines[0]["route"] != allowedRow {
			t.Errorf("%s: line %v, want the fixture session id and the actor, target and route", w.name, lines[0])
		}
		// The same wrap on a row that denies the bearer logs none.
		logger, logs = jsonLogs()
		got = atRow(deniedRow, w.wrap(NewGuard(auth, logger), echo), bearer(t, both(w.claims, delegatedClaim)))
		if got.status != http.StatusForbidden || len(useLines(t, logs)) != 0 {
			t.Errorf("%s: denied row answered %d with %d lines, want 403 and 0", w.name, got.status, len(useLines(t, logs)))
		}
	}
}

// C11: a row the PDP only shadows is decided by the legacy check, so the request logs one line.
func TestDelegatedBearerOnAShadowOnlyRowLogsOneLine(t *testing.T) {
	recordShadow(t)
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll, shadow: newTestPDP(t, allowAll)})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	wantUseLine(t, useLines(t, logs), liveSID.String())
}

// C10: the line is written at two named sites of Guard.wrap only; the bearer reader outside the Guard that is not
// covered is named here.
func TestDelegatedUseIsLoggedOnlyInTheGuardWrap(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	sites := map[string]int{}
	callers := map[string]int{}
	readers := map[string]int{}
	notCovered := map[string]bool{}
	for _, dir := range []string{"internal/api", "internal/apiservice"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return err
			}
			for _, decl := range file.Decls {
				site := rel + ":" + declName(decl)
				ast.Inspect(decl, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						switch sel.Sel.Name {
						case "noteDelegatedUse":
							sites[site]++
						case "userRequest":
							callers[site]++
						case "currentUser", "optionalUser":
							if strings.HasPrefix(rel, "internal/api/policy/") {
								readers[site]++
							}
						case "RefuseDelegated":
							if !strings.HasPrefix(rel, "internal/api/policy/") {
								notCovered[site] = true
							}
						}
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(sites) != 1 || sites["internal/api/policy/delegated_use.go:Guard.userRequest"] != 1 {
		t.Errorf("noteDelegatedUse call sites %v, want exactly 1 call, in Guard.userRequest", sites)
	}
	if len(callers) != 1 || callers["internal/api/policy/guard.go:Guard.wrap"] != 2 {
		t.Errorf("Guard.userRequest call sites %v, want exactly 2 calls, both in Guard.wrap", callers)
	}
	// Every authentication of a bearer in the Guard is called from Guard.wrap only, so a row the Guard admits reaches the hook.
	if len(readers) != 1 || readers["internal/api/policy/guard.go:Guard.wrap"] != 2 {
		t.Errorf("Guard.currentUser/optionalUser callers %v, want exactly 2 calls, both in Guard.wrap", readers)
	}
	var names []string
	for site := range notCovered {
		names = append(names, site)
	}
	sort.Strings(names)
	want := []string{"internal/api/session/routes.go:handlers.optionalUser"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Errorf("bearer readers outside the Guard that refuse delegated bearers = %v, want %v (not covered by the use line)", names, want)
	}
}

// C-2: through the production logging handler (the redaction layer) the four values are present and not redacted;
// the key is "sid" because "session_id" is a protected key. The read-back needle is the event field of this line.
func TestDelegatedUseLineSurvivesTheProductionRedaction(t *testing.T) {
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	wantUseLine(t, useLines(t, logs), liveSID.String())
	if !strings.Contains(logs.String(), `"event":"api.delegated_use"`) {
		t.Errorf("the line has no read-back needle %q: %s", `"event":"api.delegated_use"`, logs.String())
	}
	for _, key := range []string{"route", "actor", "target", "sid"} {
		if strings.Contains(logs.String(), `"`+key+`":"[REDACTED]"`) {
			t.Errorf("key %q is redacted by the production handler", key)
		}
	}
}

// C-1: the helper only logs: the answer is byte-equal with a logger and with a discarding one.
func TestDelegatedUseLogChangesNoAnswer(t *testing.T) {
	cells := map[string]useCell{
		"no enforcer": {actor: true, admitted: admitAll},
		"legacy":      {actor: true, noOrg: true, admitted: admitAll, enforce: newTestPDP(t, denyWith("role_lacks_action"))},
		"refused":     {actor: true, admitted: func(string) bool { return false }},
	}
	for name, c := range cells {
		recordEnforce(t)
		with, _ := serveUse(t, c)
		c.quiet = true
		recordEnforce(t)
		without, _ := serveUse(t, c)
		if with.status != without.status || with.body != without.body || with.header.Get("Content-Type") != without.header.Get("Content-Type") {
			t.Errorf("%s: with a logger %d %q, without %d %q", name, with.status, with.body, without.status, without.body)
		}
	}
}

// C-7: the line names the route template, never a path parameter value.
func TestDelegatedUseLineHasNoPathParameterValue(t *testing.T) {
	const secret = "99999999-9999-4999-8999-999999999999"
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll, row: "GET /api/v1/billing/invoices/{id}", path: "/api/v1/billing/invoices/" + secret})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	lines := useLines(t, logs)
	if len(lines) != 1 || lines[0]["route"] != "GET /api/v1/billing/invoices/{id}" {
		t.Fatalf("lines %v, want one with the route template", lines)
	}
	if strings.Contains(logs.String(), secret) {
		t.Errorf("the log carries the path parameter value")
	}
}

// C-5: the admin cell is on, so the request carries platform decisions, and the route's own namespace is off: the
// route verdict is not enforceAllow, the request is served, and the line is written.
func TestDelegatedBearerWithPlatformDecisionsOnANonEnforcedRowLogsOneLine(t *testing.T) {
	recordEnforce(t)
	pdp := newTestPDP(t, allowAll)
	got, logs := serveUse(t, useCell{actor: true, admitted: admitAll, enforce: pdp, namespace: "admin"})
	if got.status != http.StatusOK {
		t.Fatalf("answer %d %s, want 200", got.status, got.body)
	}
	if len(pdp.seen) != 0 {
		t.Fatalf("the PDP was asked %d times, want 0 (the route is not enforced)", len(pdp.seen))
	}
	wantUseLine(t, useLines(t, logs), liveSID.String())
}

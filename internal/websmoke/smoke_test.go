package websmoke

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

const (
	fakeEmail    = "admin@example.test"
	fakePassword = "plainwordsecret"
	fakeOrg      = "org-one"
	publicHost   = "www.commanderkeen.dev"
)

// fake stands in for web and the api behind it. A zero row set, a missing plane or a refused login is a knob.
type fake struct {
	zero       map[string]bool // check name -> answer with no rows
	graphqlErr map[string]bool // operation -> answer with GraphQL errors
	loginFails bool
	logins     int
	noPlane    map[string]bool // path -> omit the plane header
	noPlaneOp  map[string]bool // graphql operation -> omit the plane header
	pageGone   bool
	knob       map[string]bool // named misbehaviours, see the handler
	vars       map[string]map[string]any
	drilldown  []int // range_days of each drilldown request
	hijackPath string
	gqlShape   map[string]any // operation -> data value override
}

func (f *fake) rows(name string, key string) any {
	if f.zero[name] {
		return map[string]any{key: []any{}}
	}
	return map[string]any{key: []any{map[string]any{"id": "r1"}}}
}

func (f *fake) handler(t *testing.T) http.Handler {
	writeJSON := func(w http.ResponseWriter, path string, v any) {
		if !f.noPlane[path] {
			w.Header().Set(planeHeader, "go")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(v)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		hasSession := strings.Contains(r.Header.Get("Cookie"), "authjs.session-token=")
		if f.hijackPath != "" && p == f.hijackPath {
			conn, _, _ := w.(http.Hijacker).Hijack()
			_ = conn.Close()
			return
		}
		if p != "/health" && !strings.HasPrefix(p, "/api/auth/") && !hasSession {
			switch {
			case f.knob["unauth_reaches_plane"]:
				writeJSON(w, p, map[string]any{"items": []any{}})
			case f.knob["unauth_redirect_with_plane"]:
				w.Header().Set(planeHeader, "go")
				http.Redirect(w, r, "https://"+publicHost+"/auth/signin", http.StatusSeeOther)
			default:
				http.Redirect(w, r, "https://"+publicHost+"/auth/signin", http.StatusSeeOther)
			}
			return
		}
		switch p {
		case "/api/v1/home", "/api/v1/investment", "/api/v1/opportunities":
			q := r.URL.Query()
			if q.Get("scope_id") != fakeOrg || q.Get("scope_type") != "org" || q.Get("thread") == "" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		switch p {
		case "/health":
			if r.Host != "traefik" {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			writeJSON(w, p, map[string]any{"status": "ok"})
		case "/api/auth/csrf":
			writeJSON(w, p, map[string]any{"csrfToken": "csrf-token"})
		case "/api/auth/callback/credentials":
			f.logins++
			_ = r.ParseForm()
			bad := f.loginFails || r.PostForm.Get("password") != fakePassword || r.PostForm.Get("email") != fakeEmail
			if bad && !f.knob["login_error_with_cookie"] && !f.knob["login_200_with_cookie"] {
				w.Header().Set("Location", "https://"+publicHost+"/auth/signin?error=CredentialsSignin")
				w.WriteHeader(http.StatusFound)
				return
			}
			callback := "https://" + publicHost + "/dashboard"
			if f.knob["callback_bind_origin"] {
				callback = "http://0.0.0.0:3000/dashboard"
			}
			http.SetCookie(w, &http.Cookie{Name: "authjs.session-token", Value: "sess", Secure: true})
			http.SetCookie(w, &http.Cookie{Name: "authjs.callback-url", Value: url.QueryEscape(callback)})
			switch {
			case f.knob["login_200_with_cookie"]:
				w.WriteHeader(http.StatusOK)
			case f.knob["login_error_with_cookie"]:
				w.Header().Set("Location", "https://"+publicHost+"/auth/signin?error=CredentialsSignin")
				w.WriteHeader(http.StatusFound)
			default:
				w.Header().Set("Location", "https://"+publicHost+"/dashboard")
				w.WriteHeader(http.StatusFound)
			}
		case "/api/auth/session":
			writeJSON(w, p, map[string]any{"user": map[string]any{"org_id": fakeOrg}})
		case "/api/auth/signout":
			http.SetCookie(w, &http.Cookie{Name: "authjs.session-token", Value: "", MaxAge: -1})
			w.Header().Set("Location", "https://"+publicHost+"/")
			if f.knob["signout_bind_origin"] {
				w.Header().Set("Location", "http://0.0.0.0:3000/")
			}
			if f.knob["signout_status_200"] {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.WriteHeader(http.StatusFound)
		case "/api/v1/home":
			if f.knob["understand_unknown_shape"] {
				writeJSON(w, p, map[string]any{"weird": 1})
				return
			}
			writeJSON(w, p, f.rows("understand", "tiles"))
		case "/api/v1/investment":
			switch {
			case f.knob["align_distribution_empties"]:
				writeJSON(w, p, map[string]any{"theme_distribution": map[string]any{"a": 0, "b": "", "c": []any{}, "d": nil}})
			case f.knob["align_distribution"]:
				writeJSON(w, p, map[string]any{"theme_distribution": map[string]any{"a": 0.5, "b": 0}})
			default:
				writeJSON(w, p, f.rows("align", "rows"))
			}
		case "/api/v1/opportunities":
			writeJSON(w, p, f.rows("execute", "opportunities"))
		case "/api/v1/filters/options":
			if f.zero["filters_options"] {
				writeJSON(w, p, map[string]any{"teams": []any{}})
			} else {
				writeJSON(w, p, map[string]any{"teams": []any{map[string]any{"id": "team-1"}}})
			}
		case "/api/v1/investment/explain":
			if f.zero["investment_explain"] {
				writeJSON(w, p, map[string]any{})
			} else {
				writeJSON(w, p, map[string]any{"summary": "text"})
			}
		case "/api/v1/work-units":
			writeJSON(w, p, f.rows("work_units", "items"))
		case "/api/v1/drilldown/prs":
			var body struct {
				Filters struct {
					Time struct {
						RangeDays int `json:"range_days"`
					} `json:"time"`
				} `json:"filters"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.drilldown = append(f.drilldown, body.Filters.Time.RangeDays)
			switch {
			case f.knob["drilldown_empty"], f.knob["drilldown_empty_14"] && body.Filters.Time.RangeDays == 14:
				writeJSON(w, p, map[string]any{"items": []any{}})
			default:
				writeJSON(w, p, map[string]any{"items": []any{map[string]any{"repo_id": "repo-1", "number": 7}}})
			}
		case "/api/v1/flame":
			if r.URL.Query().Get("entity_id") != "repo-1:7" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			writeJSON(w, p, f.rows("flame", "frames"))
		case "/graphql":
			f.graphql(w, r, writeJSON)
		case "/testops/risk":
			if f.pageGone {
				_, _ = w.Write([]byte("<html>Data service unavailable</html>"))
				return
			}
			_, _ = w.Write([]byte("<html>ok</html>"))
		default:
			t.Errorf("unexpected request %s", p)
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

func (f *fake) graphql(w http.ResponseWriter, r *http.Request, writeJSON func(http.ResponseWriter, string, any)) {
	var req struct {
		Query string `json:"query"`
	}
	mustBody, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(mustBody, &req)
	op := ""
	for _, name := range []string{"complexityTimeseries", "workGraphFlow", "testopsRisk", "home", "recommendations", "workItemTeamAttributions"} {
		if doc, _ := server.WebPathSmokeDocument(name); doc == req.Query {
			op = name
		}
	}
	if op == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	var gq struct {
		Variables map[string]any `json:"variables"`
	}
	_ = json.Unmarshal(mustBody, &gq)
	if f.vars == nil {
		f.vars = map[string]map[string]any{}
	}
	f.vars[op] = gq.Variables
	resp := map[string]any{}
	if f.graphqlErr[op] {
		resp["errors"] = []any{map[string]any{"message": "no"}}
	}
	nonEmpty := func(key string, zero bool) any {
		if zero {
			return map[string]any{key: []any{}}
		}
		if f.knob["empty_node_type"] {
			return map[string]any{key: []any{map[string]any{"nodeType": ""}}}
		}
		return map[string]any{key: []any{map[string]any{"nodeType": "PR"}}}
	}
	switch op {
	case "complexityTimeseries":
		resp["data"] = map[string]any{op: nonEmpty("points", f.zero["complexity"])}
	case "workGraphFlow":
		resp["data"] = map[string]any{op: nonEmpty("rows", f.zero["workGraphFlow"])}
	case "home":
		if f.knob["home_no_freshness"] {
			resp["data"] = map[string]any{op: map[string]any{}}
		} else {
			resp["data"] = map[string]any{op: map[string]any{"freshness": map[string]any{}}}
		}
	case "recommendations", "workItemTeamAttributions":
		if f.zero[op] {
			resp["data"] = map[string]any{op: []any{}}
		} else {
			resp["data"] = map[string]any{op: []any{map[string]any{"id": "x"}}}
		}
	case "testopsRisk":
		resp["data"] = map[string]any{op: map[string]any{}}
	}
	if v, ok := f.gqlShape[op]; ok {
		resp["data"] = map[string]any{op: v}
	}
	if f.noPlaneOp[op] {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	writeJSON(w, "/graphql", resp)
}

// harness builds the fixtures a run reads: web source, edge catalog, routing ops, password file.
type harness struct {
	t       *testing.T
	dir     string
	fake    *fake
	srv     *httptest.Server
	cfg     Config
	catalog []map[string]string
}

func newHarness(t *testing.T, routingOps string) *harness {
	t.Helper()
	dir := t.TempDir()
	h := &harness{t: t, dir: dir, fake: &fake{zero: map[string]bool{}, graphqlErr: map[string]bool{}, noPlane: map[string]bool{}, noPlaneOp: map[string]bool{}, knob: map[string]bool{}, gqlShape: map[string]any{}}}
	h.srv = httptest.NewServer(h.fake.handler(t))
	t.Cleanup(h.srv.Close)

	src := filepath.Join(dir, "web-src")
	for path, rel := range restSources {
		file := filepath.Join(src, rel)
		mustWrite(t, file, appendFile(file, "const p = \""+path+"\";\n"))
	}
	for _, g := range graphqlSources {
		file := filepath.Join(src, g.File)
		doc, ok := server.WebPathSmokeDocument(g.Op)
		if !ok {
			t.Fatalf("no registered document for %s", g.Op)
		}
		mustWrite(t, file, appendFile(file, "export const "+g.Const+" = `"+webText(doc)+"`;\n"))
	}
	for _, g := range graphqlSources {
		doc, _ := server.WebPathSmokeDocument(g.Op)
		h.catalog = append(h.catalog, map[string]string{"operation": g.Op, "digest": DocumentDigest(doc)})
	}
	h.writeCatalog()
	mustWrite(t, filepath.Join(dir, "known-missing.txt"), routingOps)
	mustWrite(t, filepath.Join(dir, "password"), fakePassword+"\n")

	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(h.srv.URL, "http://"))
	port, _ := strconv.Atoi(portText)
	h.cfg = Config{
		Target:              Target{Host: host, Port: port},
		PublicHost:          publicHost,
		Email:               fakeEmail,
		PasswordFile:        filepath.Join(dir, "password"),
		WebSrc:              src,
		Catalog:             filepath.Join(dir, "catalog.json"),
		RoutingOps:          filepath.Join(dir, "known-missing.txt"),
		ReceiptPath:         filepath.Join(dir, "receipts", "web-path-smoke-receipt.json"),
		Documents:           server.WebPathSmokeDocument,
		RegisteredDocuments: server.WebPathSmokeRegisteredDocuments,
		Now:                 func() time.Time { return time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC) },
	}
	return h
}

var typenameLine = regexp.MustCompile(`\n\s*__typename`)

// webText is the document as web's source holds it: the registered text without urql's __typename lines.
func webText(doc string) string { return typenameLine.ReplaceAllString(doc, "") }

func (h *harness) writeCatalog() {
	b, _ := json.Marshal(h.catalog)
	mustWrite(h.t, filepath.Join(h.dir, "catalog.json"), string(b))
}

func appendFile(path, add string) string {
	old, _ := os.ReadFile(path)
	return string(old) + add
}

func mustWrite(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

func (o Outcome) check(name string) map[string]any {
	for _, c := range o.Receipt.Checks {
		if c["check"] == name {
			return c
		}
	}
	return nil
}

func TestCleanRunPasses(t *testing.T) {
	h := newHarness(t, "")
	out := Run(h.cfg)
	if out.ExitCode() != ExitPass || len(out.Receipt.Failures) != 0 {
		t.Fatalf("exit=%d failures=%v", out.ExitCode(), out.Receipt.Failures)
	}
	if got := len(out.Receipt.Checks); got != 21 {
		t.Fatalf("checks=%d, want the 21 the Python smoke ran", got)
	}
	if out.Verdict() != "PASS" {
		t.Fatalf("verdict=%s", out.Verdict())
	}
}

// The gate: exactly these reads fail when they answer with no rows.
func TestGatedReadAtZeroRowsFails(t *testing.T) {
	gated := map[string]string{
		"understand": "rest:understand", "align": "rest:align", "execute": "rest:execute",
		"filters_options": "rest:filters_options", "investment_explain": "rest:investment_explain",
		"work_units": "rest:work_units", "flame": "rest:flame", "workGraphFlow": "graphql:workGraphFlow",
	}
	for knob, check := range gated {
		t.Run(knob, func(t *testing.T) {
			h := newHarness(t, "")
			h.fake.zero[knob] = true
			out := Run(h.cfg)
			if out.ExitCode() != ExitFail {
				t.Fatalf("exit=%d, want %d", out.ExitCode(), ExitFail)
			}
			c := out.check(check)
			if c == nil || c["ok"] != false || !strings.HasSuffix(fmt.Sprint(c["reason"]), "zero_rows") {
				t.Fatalf("check %s = %v", check, c)
			}
			if len(out.Receipt.Failures) != 1 {
				t.Fatalf("failures=%v, want only %s", out.Receipt.Failures, check)
			}
		})
	}
}

// "Reported, not gated" reads stay reported.
func TestNonGatedReadAtZeroRowsPasses(t *testing.T) {
	h := newHarness(t, "")
	for _, knob := range []string{"complexity", "recommendations", "workItemTeamAttributions"} {
		h.fake.zero[knob] = true
	}
	out := Run(h.cfg)
	if out.ExitCode() != ExitPass {
		t.Fatalf("exit=%d failures=%v", out.ExitCode(), out.Receipt.Failures)
	}
	if c := out.check("graphql:complexityTimeseries"); c["row_count"] != 0 {
		t.Fatalf("zero rows not reported: %v", c)
	}
}

func TestKnownMissingOperationExits3(t *testing.T) {
	h := newHarness(t, "testopsRisk KNOWN-MISSING reason-word\n")
	h.fake.graphqlErr["testopsRisk"] = true
	h.fake.pageGone = true
	var stdout, stderr bytes.Buffer
	err := report(h.cfg, &stdout, &stderr)
	var code *exitError
	if !asExit(err, &code) || code.code != ExitKnownGap {
		t.Fatalf("err=%v, want exit %d", err, ExitKnownGap)
	}
	if !strings.Contains(stdout.String(), "KNOWN-MISSING: graphql:testopsRisk (reason-word)") ||
		!strings.Contains(stdout.String(), "verdict=KNOWN-GAP") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestUnknownMissingOperationFails(t *testing.T) {
	h := newHarness(t, "")
	h.fake.graphqlErr["testopsRisk"] = true
	out := Run(h.cfg)
	if out.ExitCode() != ExitFail || len(out.Receipt.KnownMissing) != 0 {
		t.Fatalf("exit=%d known=%v", out.ExitCode(), out.Receipt.KnownMissing)
	}
	if c := out.check("graphql:testopsRisk"); !strings.HasPrefix(fmt.Sprint(c["reason"]), "testops_risk status=200") {
		t.Fatalf("check=%v", c)
	}
}

func TestKnownMissingNowServedFails(t *testing.T) {
	h := newHarness(t, "testopsRisk KNOWN-MISSING reason-word\n")
	out := Run(h.cfg)
	if out.ExitCode() != ExitFail {
		t.Fatalf("exit=%d, want fail so the marker is removed", out.ExitCode())
	}
	if c := out.check("graphql:testopsRisk"); !strings.Contains(fmt.Sprint(c["reason"]), "known_missing_now_served_by_go") {
		t.Fatalf("check=%v", c)
	}
}

func TestLoginFailureFailsAndStops(t *testing.T) {
	h := newHarness(t, "")
	h.fake.loginFails = true
	out := Run(h.cfg)
	if out.ExitCode() != ExitFail {
		t.Fatalf("exit=%d", out.ExitCode())
	}
	if c := out.check("login"); c == nil || !strings.HasPrefix(fmt.Sprint(c["reason"]), "login_failed status=302 session_cookie=false error=CredentialsSignin") {
		t.Fatalf("login check=%v", c)
	}
	if out.check("rest:understand") != nil {
		t.Fatal("a failed login must stop the run")
	}
}

func TestWrongPasswordFileFailsLogin(t *testing.T) {
	h := newHarness(t, "")
	mustWrite(t, h.cfg.PasswordFile, "otherword\n")
	if out := Run(h.cfg); out.ExitCode() != ExitFail || out.check("login")["ok"] != false {
		t.Fatalf("exit=%d", out.ExitCode())
	}
}

func TestDigestMismatchFailsBeforeLogin(t *testing.T) {
	h := newHarness(t, "")
	h.catalog[0]["digest"] = strings.Repeat("0", 64)
	h.writeCatalog()
	out := Run(h.cfg)
	if out.ExitCode() != ExitFail || h.fake.logins != 0 {
		t.Fatalf("exit=%d logins=%d", out.ExitCode(), h.fake.logins)
	}
	if c := out.check("web_source"); !strings.HasPrefix(fmt.Sprint(c["reason"]), "document_digest_mismatch=complexityTimeseries") {
		t.Fatalf("check=%v", c)
	}
}

func TestMissingWebSourcePathFails(t *testing.T) {
	h := newHarness(t, "")
	mustWrite(t, filepath.Join(h.cfg.WebSrc, "lib/api/system.ts"), "")
	out := Run(h.cfg)
	if c := out.check("web_source"); out.ExitCode() != ExitFail || !strings.Contains(fmt.Sprint(c["reason"]), "web_source_path_missing=/health") {
		t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
	}
}

func TestUnreadableRoutingOpsFailsLoud(t *testing.T) {
	h := newHarness(t, "")
	h.cfg.RoutingOps = filepath.Join(h.dir, "absent.txt")
	if out := Run(h.cfg); out.ExitCode() != ExitFail {
		t.Fatalf("exit=%d", out.ExitCode())
	}
}

func TestPlaneMissingFails(t *testing.T) {
	h := newHarness(t, "")
	h.fake.noPlane["/api/v1/work-units"] = true
	out := Run(h.cfg)
	if c := out.check("rest:work_units"); out.ExitCode() != ExitFail || c["reason"] != "work_units_plane_missing" {
		t.Fatalf("check=%v", c)
	}
}

// No credential value reaches the receipt, the printed lines, or the process's argv/environment.
func TestCredentialNeverLeaks(t *testing.T) {
	for _, loginFails := range []bool{false, true} {
		h := newHarness(t, "")
		h.fake.loginFails = loginFails
		var stdout, stderr bytes.Buffer
		_ = report(h.cfg, &stdout, &stderr)
		receipt, err := os.ReadFile(h.cfg.ReceiptPath)
		if err != nil {
			t.Fatal(err)
		}
		for name, text := range map[string]string{"receipt": string(receipt), "stdout": stdout.String(), "stderr": stderr.String()} {
			for _, secret := range []string{fakePassword, "csrf-token", "authjs.session-token=sess", fakeOrg} {
				if strings.Contains(text, secret) {
					t.Fatalf("loginFails=%t: %s holds %q", loginFails, name, secret)
				}
			}
		}
	}
	for _, env := range os.Environ() {
		if strings.Contains(env, fakePassword) {
			t.Fatal("the password reached the process environment")
		}
	}
}

func TestVerbTakesNoArgumentAndNoCredentialFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{
		Args:   []string{"--password", fakePassword},
		Lookup: func(string) (string, bool) { return "", false },
		Stdout: &stdout, Stderr: &stderr,
	}
	code := Command().Children[0].Run(nil, env)
	if code == ExitPass || strings.Contains(stdout.String()+stderr.String(), fakePassword) {
		t.Fatalf("code=%d out=%q err=%q", code, stdout.String(), stderr.String())
	}
}

func TestVerbRefusesTargetAndHost(t *testing.T) {
	for name, vars := range map[string]map[string]string{
		"scheme": {"DHO_SMOKE_BASE_URL": "https://traefik:3000"},
		"host":   {"DHO_SMOKE_BASE_URL": "http://example.test:3000"},
		"path":   {"DHO_SMOKE_BASE_URL": "http://traefik:3000/x"},
		"public": {"DHO_SMOKE_PUBLIC_HOST": "example.test"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			env := cli.Env{
				Lookup: func(k string) (string, bool) { v, ok := vars[k]; return v, ok },
				Stdout: &stdout, Stderr: &stderr,
			}
			if code := Command().Children[0].Run(nil, env); code != ExitRefused {
				t.Fatalf("code=%d stderr=%q", code, stderr.String())
			}
		})
	}
}

func TestVerbFailsWhenCredentialNamesUnset(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{Lookup: func(string) (string, bool) { return "", false }, Stdout: &stdout, Stderr: &stderr}
	if code := Command().Children[0].Run(nil, env); code != ExitFail || !strings.Contains(stderr.String(), "DHO_SMOKE_ADMIN_EMAIL") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

// Golden: the receipt schema the cut's readers parse. Regenerate only on purpose.
func TestReceiptGolden(t *testing.T) {
	h := newHarness(t, "testopsRisk KNOWN-MISSING reason-word\n")
	h.fake.graphqlErr["testopsRisk"] = true
	h.fake.pageGone = true
	var stdout, stderr bytes.Buffer
	_ = report(h.cfg, &stdout, &stderr)
	got, err := os.ReadFile(h.cfg.ReceiptPath)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/web-path-smoke-receipt.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("receipt differs from testdata/web-path-smoke-receipt.golden.json:\n%s", got)
	}
	var generic map[string]any
	if err := json.Unmarshal(got, &generic); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"ok", "failures", "known_missing", "generated_at", "checks"} {
		if _, ok := generic[k]; !ok {
			t.Fatalf("receipt lacks %q", k)
		}
	}
}

func TestEncodeFilterMatchesWeb(t *testing.T) {
	// web encodeFilter: sorted-key compact JSON, base64url, no padding.
	got := EncodeFilter(map[string]any{"b": 1, "a": []any{}})
	if got != "eyJhIjpbXSwiYiI6MX0" {
		t.Fatalf("got %s", got)
	}
}

func asExit(err error, target **exitError) bool {
	e, ok := err.(*exitError)
	if ok {
		*target = e
	}
	return ok
}

// ---- web-document drift (H1) -------------------------------------------------------------------

func setWebDoc(t *testing.T, h *harness, op, text string) {
	t.Helper()
	for _, g := range graphqlSources {
		if g.Op != op {
			continue
		}
		file := filepath.Join(h.cfg.WebSrc, g.File)
		src, _ := os.ReadFile(file)
		re := regexp.MustCompile("(?s)(export const " + g.Const + " = `)[^`]*(`;)")
		if !re.Match(src) {
			t.Fatalf("constant %s not found", g.Const)
		}
		mustWrite(t, file, string(re.ReplaceAll(src, []byte("${1}"+strings.ReplaceAll(text, "$", "$$")+"${2}"))))
		return
	}
	t.Fatalf("unknown op %s", op)
}

func TestWebDocumentDriftFails(t *testing.T) {
	doc, _ := server.WebPathSmokeDocument("workGraphFlow")
	for name, mutate := range map[string]func(string) string{
		"added field":   func(d string) string { return strings.Replace(d, "inflow", "inflow\n      netFlow", 1) },
		"removed field": func(d string) string { return strings.Replace(d, "outflow", "", 1) },
		"renamed var":   func(d string) string { return strings.ReplaceAll(d, "$orgId", "$org") },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, "")
			setWebDoc(t, h, "workGraphFlow", webText(mutate(doc)))
			out := Run(h.cfg)
			c := out.check("web_source")
			if out.ExitCode() != ExitFail || c["reason"] != "document_digest_mismatch=workGraphFlow" || h.fake.logins != 0 {
				t.Fatalf("exit=%d check=%v logins=%d", out.ExitCode(), c, h.fake.logins)
			}
		})
	}
}

func TestWebDocumentLayoutIsNotDrift(t *testing.T) {
	doc, _ := server.WebPathSmokeDocument("workGraphFlow")
	h := newHarness(t, "")
	spaced := "# a comment\n" + strings.NewReplacer("\n", "\n\n", "  ", "\t", "(", "( ", "$filters:", "$filters :").Replace(webText(doc))
	setWebDoc(t, h, "workGraphFlow", spaced)
	if out := Run(h.cfg); out.ExitCode() != ExitPass {
		t.Fatalf("exit=%d failures=%v", out.ExitCode(), out.Receipt.Failures)
	}
}

func TestWebDocumentConstantMissingFails(t *testing.T) {
	h := newHarness(t, "")
	for _, g := range graphqlSources {
		if g.Op == "home" {
			mustWrite(t, filepath.Join(h.cfg.WebSrc, g.File), strings.ReplaceAll(appendFile(filepath.Join(h.cfg.WebSrc, g.File), ""), "export const "+g.Const+" =", "const "+g.Const+" ="))
		}
	}
	out := Run(h.cfg)
	if c := out.check("web_source"); out.ExitCode() != ExitFail || c["reason"] != "web_source_document_missing=HOME_QUERY" {
		t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
	}
}

func TestWebDocumentWithInterpolationIsMissing(t *testing.T) {
	h := newHarness(t, "")
	setWebDoc(t, h, "home", "query { home { ${FRAGMENT} } }")
	if c := Run(h.cfg).check("web_source"); c["reason"] != "web_source_document_missing=HOME_QUERY" {
		t.Fatalf("check=%v", c)
	}
}

// F1: a legacy catalog row listed after the current one must not break the check.
func TestCatalogLegacyRowAfterCurrentPasses(t *testing.T) {
	h := newHarness(t, "")
	h.catalog = append(h.catalog, map[string]string{"operation": "workGraphFlow", "digest": strings.Repeat("a", 64), "legacy": "true"})
	h.writeCatalog()
	if out := Run(h.cfg); out.ExitCode() != ExitPass {
		t.Fatalf("exit=%d failures=%v", out.ExitCode(), out.Receipt.Failures)
	}
}

func TestGraphqlTokens(t *testing.T) {
	got := strings.Join(graphqlTokens("query Q($a: Int!, $b: [String]) { x(a: $a, s: \"q\\\"r\") { ...F __typename y } } # c\n"), " ")
	want := `query Q ( $ a : Int ! $ b : [ String ] ) { x ( a : $ a s : "q\"r" ) { ... F __typename y } }`
	if got != want {
		t.Fatalf("got %s", got)
	}
}

// ---- guards that must fail closed (H2) ---------------------------------------------------------

func failureOf(t *testing.T, h *harness, check string) string {
	t.Helper()
	out := Run(h.cfg)
	if out.ExitCode() != ExitFail {
		t.Fatalf("exit=%d, want fail", out.ExitCode())
	}
	c := out.check(check)
	if c == nil || c["ok"] != false {
		t.Fatalf("check %s = %v (failures %v)", check, c, out.Receipt.Failures)
	}
	return fmt.Sprint(c["reason"])
}

func TestGuardsFailClosed(t *testing.T) {
	cases := []struct {
		name, check, reason string
		arrange             func(*harness)
	}{
		{"workGraphFlow errors", "graphql:workGraphFlow", "work_graph_flow_graphql_errors", func(h *harness) { h.fake.graphqlErr["workGraphFlow"] = true }},
		{"complexity errors", "graphql:complexityTimeseries", "complexity_graphql_errors", func(h *harness) { h.fake.graphqlErr["complexityTimeseries"] = true }},
		{"home errors", "graphql:home", "home_graphql_errors", func(h *harness) { h.fake.graphqlErr["home"] = true }},
		{"recommendations errors", "graphql:recommendations", "recommendations_graphql_errors", func(h *harness) { h.fake.graphqlErr["recommendations"] = true }},
		{"attributions errors", "graphql:workItemTeamAttributions", "workItemTeamAttributions_graphql_errors", func(h *harness) { h.fake.graphqlErr["workItemTeamAttributions"] = true }},
		{"login 200 with a session cookie", "login", "login_failed status=200", func(h *harness) { h.fake.knob["login_200_with_cookie"] = true }},
		{"login error= with a session cookie", "login", "login_failed status=302 session_cookie=true error=CredentialsSignin", func(h *harness) { h.fake.knob["login_error_with_cookie"] = true; h.fake.loginFails = true }},
		{"unauthenticated request reaches a plane", "public_host_unauth_on_web", "public_host_unauth_not_on_web status=200 plane=go", func(h *harness) { h.fake.knob["unauth_reaches_plane"] = true }},
		{"unauthenticated redirect carries a plane", "public_host_unauth_on_web", "public_host_unauth_not_on_web status=303 plane=go", func(h *harness) { h.fake.knob["unauth_redirect_with_plane"] = true }},
		{"callback origin is the bind address", "callback_url_origin", "callback_url_origin=http://0.0.0.0:3000 expected=https://" + publicHost, func(h *harness) { h.fake.knob["callback_bind_origin"] = true }},
		{"logout to the bind address", "logout", "logout_redirect_origin=http://0.0.0.0:3000 status=302", func(h *harness) { h.fake.knob["signout_bind_origin"] = true }},
		{"logout status 200", "logout", "logout_redirect_origin=https://" + publicHost + " status=200", func(h *harness) { h.fake.knob["signout_status_200"] = true }},
		{"empty nodeType", "graphql:workGraphFlow", "work_graph_flow_empty_node_type=1", func(h *harness) { h.fake.knob["empty_node_type"] = true }},
		{"home without freshness", "graphql:home", "home_field_missing=freshness", func(h *harness) { h.fake.knob["home_no_freshness"] = true }},
		{"home as a list", "graphql:home", "home_data_shape", func(h *harness) { h.fake.gqlShape["home"] = []any{} }},
		{"recommendations as an object", "graphql:recommendations", "recommendations_data_shape", func(h *harness) { h.fake.gqlShape["recommendations"] = map[string]any{} }},
		{"attributions as an object", "graphql:workItemTeamAttributions", "workItemTeamAttributions_data_shape", func(h *harness) { h.fake.gqlShape["workItemTeamAttributions"] = map[string]any{} }},
		{"distribution of empty entries", "rest:align", "rest_align_zero_rows", func(h *harness) { h.fake.knob["align_distribution_empties"] = true }},
		{"unknown response shape", "rest:understand", "rest_understand_unknown_response_shape keys=weird", func(h *harness) { h.fake.knob["understand_unknown_shape"] = true }},
		{"no PR row in 14 or 90 days", "rest:drilldown_prs", "drilldown_prs_no_pr_row_for_flame", func(h *harness) { h.fake.knob["drilldown_empty"] = true }},
		{"testopsRisk without the go plane", "graphql:testopsRisk", "testops_risk status=200 plane=missing errors=false", func(h *harness) { h.fake.noPlaneOp["testopsRisk"] = true }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, "")
			c.arrange(h)
			if got := failureOf(t, h, c.check); !strings.HasPrefix(got, c.reason) {
				t.Fatalf("reason=%q, want prefix %q", got, c.reason)
			}
		})
	}
}

func TestDistributionCountsOnlyNonEmptyEntries(t *testing.T) {
	h := newHarness(t, "")
	h.fake.knob["align_distribution"] = true
	out := Run(h.cfg)
	if c := out.check("rest:align"); out.ExitCode() != ExitPass || c["row_count"] != 1 {
		t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
	}
}

func TestCountRowsShapes(t *testing.T) {
	if n, err := CountRows(map[string]any{"theme_distribution": map[string]any{"a": json.Number("0"), "b": "", "c": []any{}, "d": map[string]any{}, "e": nil, "f": false, "g": json.Number("2")}}); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err := CountRows(map[string]any{"b": 1, "a": 2}); err == nil || err.Error() != "unknown_response_shape keys=a,b" {
		t.Fatalf("err=%v", err)
	}
	if _, err := CountRows(json.Number("3")); err == nil || !strings.HasPrefix(err.Error(), "unknown_response_shape type=") {
		t.Fatalf("err=%v", err)
	}
	if n, err := CountRows(map[string]any{"data": map[string]any{"rows": []any{1, 2}}}); err != nil || n != 2 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestDrilldownFallsBackTo90Days(t *testing.T) {
	h := newHarness(t, "")
	h.fake.knob["drilldown_empty_14"] = true
	out := Run(h.cfg)
	if c := out.check("rest:drilldown_prs"); out.ExitCode() != ExitPass || c["range_days"] != 90 {
		t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
	}
	if fmt.Sprint(h.fake.drilldown) != "[14 90]" {
		t.Fatalf("drilldown requests %v", h.fake.drilldown)
	}
}

// The attribution probe names exactly one id that matches no row: an empty list would scan the whole org.
func TestAttributionProbeSendsOneUnmatchedID(t *testing.T) {
	h := newHarness(t, "")
	Run(h.cfg)
	ids, _ := h.fake.vars["workItemTeamAttributions"]["workItemIds"].([]any)
	if len(ids) != 1 || ids[0] != "smoke-probe-no-such-item" {
		t.Fatalf("workItemIds=%v", ids)
	}
}

func TestTransportErrorReportsTypeOnly(t *testing.T) {
	h := newHarness(t, "")
	h.fake.hijackPath = "/api/v1/home"
	out := Run(h.cfg)
	c := out.check("rest:understand")
	reason := fmt.Sprint(c["reason"])
	if out.ExitCode() != ExitFail || !strings.HasPrefix(reason, "unexpected_error=") || strings.ContainsAny(reason, "/? ") {
		t.Fatalf("check=%v", c)
	}
	var stdout, stderr bytes.Buffer
	_ = report(h.cfg, &stdout, &stderr)
	receipt, _ := os.ReadFile(h.cfg.ReceiptPath)
	for name, text := range map[string]string{"receipt": string(receipt), "stdout": stdout.String(), "stderr": stderr.String()} {
		if strings.Contains(text, fakeOrg) || strings.Contains(text, "scope_id") {
			t.Fatalf("%s holds the request URL", name)
		}
	}
}

func TestRefusedHostHeaderOpensNoConnection(t *testing.T) {
	h := newHarness(t, "")
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer srv.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	port, _ := strconv.Atoi(portText)
	h.cfg.Target = Target{Host: host, Port: port}
	c := newClient(h.cfg)
	for _, tc := range []struct{ host, path, want string }{
		{"evil.example.test", "/x", "host_header_not_allowed"},
		{"", "relative", "request_path_must_be_absolute"},
	} {
		if _, err := c.do("GET", tc.path, reqOpts{host: tc.host}); err == nil || err.Error() != tc.want {
			t.Fatalf("%+v: err=%v", tc, err)
		}
	}
	if hits != 0 {
		t.Fatalf("a refused request opened a connection (%d hits)", hits)
	}
}

// The client carries the login form and the session cookie, so a redirect to another origin must never be followed.
func TestTheSmokeClientNeverFollowsARedirect(t *testing.T) {
	other := 0
	second := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { other++ }))
	defer second.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, second.URL+"/x", http.StatusFound)
	}))
	defer first.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(first.URL, "http://"))
	port, _ := strconv.Atoi(portText)
	c := newClient(Config{Target: Target{Host: host, Port: port}, PublicHost: publicHost})
	if err := c.http.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatalf("CheckRedirect = %v, want ErrUseLastResponse", err)
	}
	r, err := c.do("GET", "/x", reqOpts{jar: &cookieJar{items: []cookie{{"authjs.session-token", "sess"}}}})
	if err != nil || r.status != http.StatusFound || other != 0 {
		t.Fatalf("status=%v err=%v other-origin requests=%d", r, err, other)
	}
}

// ---- __typename placement (urql formatDocument) ------------------------------------------------

func TestUrqlFormatRule(t *testing.T) {
	for name, tc := range map[string]struct{ web, wire string }{
		"nested sets, root excepted":                 {`query Q { a { b { c } } }`, `query Q { a { b { c __typename } __typename } }`},
		"existing last is kept":                      {`query Q { a { b __typename } }`, `query Q { a { b __typename } }`},
		"existing first is kept":                     {`query Q { a { __typename b } }`, `query Q { a { __typename b } }`},
		"alias does not count":                       {`query Q { a { kind: __typename } }`, `query Q { a { kind : __typename __typename } }`},
		"alias of a field":                           {`query Q { a { __typename: b } }`, `query Q { a { __typename : b __typename } }`},
		"root typename untouched":                    {`query Q { __typename a { b } }`, `query Q { __typename a { b __typename } }`},
		"anonymous root":                             {`{ a { b } }`, `{ a { b __typename } }`},
		"fragment definition":                        {`query Q { a { ...F } } fragment F on T { x }`, `query Q { a { ... F __typename } } fragment F on T { x __typename }`},
		"inline fragment":                            {`query Q { a { ... on T { x } } }`, `query Q { a { ... on T { x __typename } __typename } }`},
		"object value braces":                        {`query Q($v: In = {k: 1}) { a(f: {k: 1}) { b } }`, `query Q ( $ v : In = { k : 1 } ) { a ( f : { k : 1 } ) { b __typename } }`},
		"underscore directive":                       {`query Q { a @_opt(x: 1) { b } }`, `query Q { a { b __typename } }`},
		"fragment with an object-valued directive":   {`query Q { a { b } } fragment F on T @d(a: {b: 1}) { x }`, `query Q { a { b __typename } } fragment F on T @ d ( a : { b : 1 } ) { x __typename }`},
		"underscore directive on a variable is kept": {`query Q($v: Int @_x) { a { b } }`, `query Q ( $ v : Int @ _x ) { a { b __typename } }`},
		"plain directive kept":                       {`query Q { a @skip(if: true) { b } }`, `query Q { a @ skip ( if : true ) { b __typename } }`},
	} {
		t.Run(name, func(t *testing.T) {
			got, want := strings.Join(urqlFormat(graphqlTokens(tc.web)), " "), strings.Join(graphqlTokens(tc.wire), " ")
			if got != want {
				t.Fatalf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// Each of these breaks web's real request (the edge hashes web's urql output), so each must fail.
func TestTypenamePlacementDriftFails(t *testing.T) {
	doc, _ := server.WebPathSmokeDocument("workGraphFlow")
	web := webText(doc)
	cases := map[string]struct{ web, registered string }{
		"web explicit typename first in rows": {strings.Replace(web, "rows {", "rows {\n      __typename", 1), doc},
		"web typename at the operation root":  {strings.Replace(web, "{\n  workGraphFlow", "{\n  __typename\n  workGraphFlow", 1), doc},
		// CHAOS-4696 shape: the registered document lacks a typename urql adds.
		"registered lacks one typename":  {web, strings.Replace(doc, "      __typename\n    }\n    degradedReason", "    }\n    degradedReason", 1)},
		"registered has a root typename": {web, strings.Replace(doc, "{\n  workGraphFlow", "{\n  __typename\n  workGraphFlow", 1)},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if c.web == web && c.registered == doc {
				t.Fatal("the plant changed nothing")
			}
			if sameTokens(c.web, c.registered) {
				t.Fatal("drift not detected")
			}
		})
	}
	for name, c := range map[string]struct{ web, registered string }{
		"web explicit typename last": {strings.Replace(web, "outflow", "outflow\n      __typename", 1), doc},
		"real document":              {web, doc},
	} {
		if !sameTokens(c.web, c.registered) {
			t.Fatalf("%s: false drift", name)
		}
	}
}

func TestTypenameDriftFailsTheRun(t *testing.T) {
	h := newHarness(t, "")
	doc, _ := server.WebPathSmokeDocument("workGraphFlow")
	setWebDoc(t, h, "workGraphFlow", strings.Replace(webText(doc), "rows {", "rows {\n      __typename", 1))
	if c := Run(h.cfg).check("web_source"); c["reason"] != "document_digest_mismatch=workGraphFlow" {
		t.Fatalf("check=%v", c)
	}
}

func TestEverySmokeDocumentPassesAsWebWritesIt(t *testing.T) {
	for _, g := range graphqlSources {
		doc, _ := server.WebPathSmokeDocument(g.Op)
		if !sameTokens(webText(doc), doc) {
			t.Fatalf("%s: the registered document differs from urql's output for its own web text", g.Op)
		}
	}
}

// CHAOS-9146: in a two-step pin (this ops with the older web) web's document is a LEGACY registered
// text. It passes, and the receipt names the operation as legacy; the smoke still sends the current
// registered document. A legacy text whose digest the catalog does not hold, and a text no registered
// document carries, still fail.
func TestWebDocumentOfALegacyRegisteredTextPassesAndSaysLegacy(t *testing.T) {
	documents, ok := server.WebPathSmokeRegisteredDocuments("home")
	if !ok || len(documents) < 3 || documents[0].Legacy || !documents[1].Legacy {
		t.Fatalf("home registered documents = %d (ok %t): want the current text first, then legacy texts", len(documents), ok)
	}
	for index, legacy := range documents[1:] {
		t.Run(fmt.Sprintf("legacy text %d", index+1), func(t *testing.T) {
			h := newHarness(t, "")
			// The catalog of the deployed ops holds every registered digest of the operation.
			for _, document := range documents {
				h.catalog = append(h.catalog, map[string]string{"operation": "home", "digest": DocumentDigest(document.Text)})
			}
			h.writeCatalog()
			setWebDoc(t, h, "home", webText(legacy.Text))
			out := Run(h.cfg)
			c := out.check("web_source")
			if out.ExitCode() != ExitPass || c["legacy_documents"] != "home" || c["digests_match"] != true {
				t.Fatalf("exit=%d check=%v failures=%v", out.ExitCode(), c, out.Receipt.Failures)
			}
		})
	}
	t.Run("the current text says nothing of legacy", func(t *testing.T) {
		h := newHarness(t, "")
		out := Run(h.cfg)
		if c := out.check("web_source"); out.ExitCode() != ExitPass || c["legacy_documents"] != nil {
			t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
		}
	})
	t.Run("a legacy text the catalog does not hold is a mismatch", func(t *testing.T) {
		h := newHarness(t, "")
		setWebDoc(t, h, "home", webText(documents[1].Text))
		out := Run(h.cfg)
		if c := out.check("web_source"); out.ExitCode() != ExitFail || c["reason"] != "document_digest_mismatch=home" {
			t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
		}
	})
	t.Run("a text no registered document carries is a mismatch", func(t *testing.T) {
		h := newHarness(t, "")
		for _, document := range documents {
			h.catalog = append(h.catalog, map[string]string{"operation": "home", "digest": DocumentDigest(document.Text)})
		}
		h.writeCatalog()
		setWebDoc(t, h, "home", webText(strings.Replace(documents[0].Text, "freshness {", "freshness {\n      unknownField", 1)))
		out := Run(h.cfg)
		if c := out.check("web_source"); out.ExitCode() != ExitFail || c["reason"] != "document_digest_mismatch=home" {
			t.Fatalf("exit=%d check=%v", out.ExitCode(), c)
		}
	})
}

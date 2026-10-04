package websmoke

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
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
	zero        map[string]bool // check name -> answer with no rows
	graphqlErr  map[string]bool // operation -> answer with GraphQL errors
	loginFails  bool
	logins      int
	noPlane     map[string]bool // path -> omit the plane header
	pageGone    bool
	seenCookies []string
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
		if p != "/health" && !strings.HasPrefix(p, "/api/auth/") && !hasSession {
			http.Redirect(w, r, "https://"+publicHost+"/auth/signin", http.StatusSeeOther)
			return
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
			if f.loginFails || r.PostForm.Get("password") != fakePassword || r.PostForm.Get("email") != fakeEmail {
				w.Header().Set("Location", "https://"+publicHost+"/auth/signin?error=CredentialsSignin")
				w.WriteHeader(http.StatusFound)
				return
			}
			http.SetCookie(w, &http.Cookie{Name: "authjs.session-token", Value: "sess", Secure: true})
			http.SetCookie(w, &http.Cookie{Name: "authjs.callback-url", Value: url.QueryEscape("https://" + publicHost + "/dashboard")})
			w.Header().Set("Location", "https://"+publicHost+"/dashboard")
			w.WriteHeader(http.StatusFound)
		case "/api/auth/session":
			writeJSON(w, p, map[string]any{"user": map[string]any{"org_id": fakeOrg}})
		case "/api/auth/signout":
			http.SetCookie(w, &http.Cookie{Name: "authjs.session-token", Value: "", MaxAge: -1})
			w.Header().Set("Location", "https://"+publicHost+"/")
			w.WriteHeader(http.StatusFound)
		case "/api/v1/home":
			writeJSON(w, p, f.rows("understand", "tiles"))
		case "/api/v1/investment":
			writeJSON(w, p, f.rows("align", "rows"))
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
			writeJSON(w, p, map[string]any{"items": []any{map[string]any{"repo_id": "repo-1", "number": 7}}})
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
	_ = json.NewDecoder(r.Body).Decode(&req)
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
	resp := map[string]any{}
	if f.graphqlErr[op] {
		resp["errors"] = []any{map[string]any{"message": "no"}}
	}
	nonEmpty := func(key string, zero bool) any {
		if zero {
			return map[string]any{key: []any{}}
		}
		return map[string]any{key: []any{map[string]any{"nodeType": "PR"}}}
	}
	switch op {
	case "complexityTimeseries":
		resp["data"] = map[string]any{op: nonEmpty("points", f.zero["complexity"])}
	case "workGraphFlow":
		resp["data"] = map[string]any{op: nonEmpty("rows", f.zero["workGraphFlow"])}
	case "home":
		resp["data"] = map[string]any{op: map[string]any{"freshness": map[string]any{}}}
	case "recommendations", "workItemTeamAttributions":
		if f.zero[op] {
			resp["data"] = map[string]any{op: []any{}}
		} else {
			resp["data"] = map[string]any{op: []any{map[string]any{"id": "x"}}}
		}
	case "testopsRisk":
		resp["data"] = map[string]any{op: map[string]any{}}
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
	h := &harness{t: t, dir: dir, fake: &fake{zero: map[string]bool{}, graphqlErr: map[string]bool{}, noPlane: map[string]bool{}}}
	h.srv = httptest.NewServer(h.fake.handler(t))
	t.Cleanup(h.srv.Close)

	src := filepath.Join(dir, "web-src")
	for path, rel := range restSources {
		file := filepath.Join(src, rel)
		mustWrite(t, file, appendFile(file, "const p = \""+path+"\";\n"))
	}
	for _, g := range graphqlSources {
		file := filepath.Join(src, g.File)
		mustWrite(t, file, appendFile(file, "export const "+g.Const+" = `query {}`;\n"))
	}
	for _, g := range graphqlSources {
		doc, ok := server.WebPathSmokeDocument(g.Op)
		if !ok {
			t.Fatalf("no registered document for %s", g.Op)
		}
		h.catalog = append(h.catalog, map[string]string{"operation": g.Op, "digest": DocumentDigest(doc)})
	}
	h.writeCatalog()
	mustWrite(t, filepath.Join(dir, "routing-ops.txt"), routingOps)
	mustWrite(t, filepath.Join(dir, "password"), fakePassword+"\n")

	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(h.srv.URL, "http://"))
	port, _ := strconv.Atoi(portText)
	h.cfg = Config{
		Target:       Target{Host: host, Port: port},
		PublicHost:   publicHost,
		Email:        fakeEmail,
		PasswordFile: filepath.Join(dir, "password"),
		WebSrc:       src,
		Catalog:      filepath.Join(dir, "catalog.json"),
		RoutingOps:   filepath.Join(dir, "routing-ops.txt"),
		ReceiptPath:  filepath.Join(dir, "receipts", "web-path-smoke-receipt.json"),
		Documents:    server.WebPathSmokeDocument,
		Now:          func() time.Time { return time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC) },
	}
	return h
}

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

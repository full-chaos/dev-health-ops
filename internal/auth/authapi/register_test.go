package authapi

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity/local"
)

func registerEnv(t *testing.T, value string) {
	t.Helper()
	previous := registerGetenv
	registerGetenv = func(string) string { return value }
	t.Cleanup(func() { registerGetenv = previous })
}

const registerBody = `{"email":"newcomer@example.com","password":"Correct-horse-battery-9","full_name":"New Comer","org_name":"Ignored Org"}`

func TestRegisterRouteIsMountedAsGoAPIsPattern(t *testing.T) {
	registerEnv(t, "")
	c := newCredFixture(t)
	routes, err := CredentialRoutes(CredentialDeps{Flows: &local.Flows{}, Sessions: c.sessions, Users: c.users, Issuer: c.issuer, Limits: mustLimits(t, httpapi.NewMemoryCounters(nil))})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range routes {
		if route.Method == http.MethodPost && route.Pattern == "/api/v1/auth/register" {
			return
		}
	}
	t.Fatal("POST /api/v1/auth/register is not mounted")
}

func TestRegisterRouteAnswersAcceptedForNewAndTakenAddress(t *testing.T) {
	registerEnv(t, "")
	newcomer, taken := newCredFixture(t), newCredFixture(t)
	a := newcomer.call(http.MethodPost, PatternRegister, registerBody, nil)
	b := taken.call(http.MethodPost, PatternRegister, `{"email":"`+credKnownEmail+`","password":"Correct-horse-battery-9"}`, nil)
	const want = `{"message":"Check your email to continue"}`
	if a.Code != http.StatusAccepted || b.Code != http.StatusAccepted || a.Body.String() != want || b.Body.String() != want {
		t.Fatalf("new %d %s, taken %d %s", a.Code, a.Body, b.Code, b.Body)
	}
	if len(newcomer.fakes.provisioned) != 1 || len(taken.fakes.provisioned) != 0 {
		t.Fatalf("provision calls new %d taken %d, want 1 and 0", len(newcomer.fakes.provisioned), len(taken.fakes.provisioned))
	}
	if _, ok := newcomer.linkFor(local.PurposeEmailVerification); !ok {
		t.Fatal("a new sign-up got no verification link")
	}
	if len(taken.fakes.mails) != 1 || taken.fakes.mails[0].Purpose != local.PurposeAccountExists {
		t.Fatal("a taken address got no account-exists notice")
	}
	if len(a.Result().Cookies()) != 0 {
		t.Fatal("register set a cookie")
	}
}

func TestRegisterRouteRefusalsMatchTheCheckedInTable(t *testing.T) {
	registerEnv(t, "")
	cases := map[string]struct {
		body   string
		status int
		text   string
	}{
		"malformed_body":         {`{not json`, http.StatusUnprocessableEntity, ""},
		"invalid_email":          {`{"email":"not an address","password":"Correct-horse-battery-9"}`, http.StatusUnprocessableEntity, ""},
		"password_too_short":     {`{"email":"a@example.com","password":"short"}`, http.StatusUnprocessableEntity, ""},
		"password_policy":        {`{"email":"a@example.com","password":"aaaaaaaaaaaa"}`, http.StatusUnprocessableEntity, "Password validation failed"},
		"password_over_72_bytes": {`{"email":"a@example.com","password":"Aa1-` + strings.Repeat("é", 40) + `"}`, http.StatusUnprocessableEntity, "Password validation failed"},
	}
	for id, tc := range cases {
		c := newCredFixture(t)
		r := c.call(http.MethodPost, PatternRegister, tc.body, nil)
		if r.Code != tc.status || !strings.Contains(r.Body.String(), tc.text) || len(c.fakes.provisioned) != 0 || len(c.fakes.mails) != 0 {
			t.Fatalf("%s = %d %s, provisioned %d, mails %d", id, r.Code, r.Body, len(c.fakes.provisioned), len(c.fakes.mails))
		}
		if strings.Contains(r.Body.String(), "Correct-horse") || strings.Contains(r.Body.String(), "aaaaaaaaaaaa") {
			t.Fatalf("%s echoed the password", id)
		}
	}
	c := newCredFixture(t)
	for i := 1; i <= 4; i++ {
		want := http.StatusAccepted
		if i > 3 {
			want = http.StatusTooManyRequests
		}
		if r := c.call(http.MethodPost, PatternRegister, registerBody, nil); r.Code != want {
			t.Fatalf("rate_limited call %d = %d, want %d", i, r.Code, want)
		}
	}
}

func TestRegisterRouteLimitsThreePerHourPerEndClientAddress(t *testing.T) {
	registerEnv(t, "")
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	post := func(peer, forwarded string) int {
		return send(h, peer, forwarded, http.MethodPost, PatternRegister, registerBody, nil).Code
	}
	for i := 1; i <= 3; i++ {
		if code := post(proxyPeer, "198.51.100.1"); code != http.StatusAccepted {
			t.Fatalf("client one call %d = %d", i, code)
		}
	}
	if code := post(proxyPeer, "198.51.100.1"); code != http.StatusTooManyRequests {
		t.Fatalf("client one call 4 = %d, want 429", code)
	}
	if code := post(proxyPeer, "198.51.100.2"); code != http.StatusAccepted {
		t.Fatalf("client two shares client one's budget: %d", code)
	}
}

func TestRegisterRouteIgnoresAForwardedHeaderFromAnUntrustedPeer(t *testing.T) {
	registerEnv(t, "")
	c := newCredFixture(t)
	h := trustedProxy(t, c.routes)
	for i := 1; i <= 3; i++ {
		if code := send(h, clientPeer, "198.51.100."+string(rune('0'+i)), http.MethodPost, PatternRegister, registerBody, nil).Code; code != http.StatusAccepted {
			t.Fatalf("call %d = %d", i, code)
		}
	}
	if code := send(h, clientPeer, "198.51.100.9", http.MethodPost, PatternRegister, registerBody, nil).Code; code != http.StatusTooManyRequests {
		t.Fatalf("a spoofed header split the budget: %d", code)
	}
}

func TestRegisterRouteLogsNoSecretOrAddress(t *testing.T) {
	registerEnv(t, "")
	c := newCredFixture(t)
	c.fakes.issueErr = errors.New("db down for newcomer@example.com")
	c.call(http.MethodPost, PatternRegister, registerBody, nil)
	c.fakes.issueErr = nil
	c.call(http.MethodPost, PatternRegister, registerBody, nil)
	secret, _ := c.linkFor(local.PurposeEmailVerification)
	out := c.logs.String() + c.fixture.logs.String()
	for _, banned := range []string{"newcomer@example.com", "Correct-horse-battery-9", "New Comer", secret, "db down"} {
		if banned != "" && strings.Contains(out, banned) {
			t.Fatal("a log line carries a secret, a name or an address")
		}
	}
}

func TestRegisterParityTableNamesTheDivergentRows(t *testing.T) {
	registerEnv(t, "")
	rows := registerParityRows(t)
	for id, kind := range map[string]string{"public_audit_row_user_registered": "divergent", "password_over_72_bytes": "stricter"} {
		row, ok := rows[id]
		if !ok || row.kind != kind || row.reason == "" || row.reason == "-" {
			t.Fatalf("parity table lacks the named %s row %q with a reason", kind, id)
		}
	}
}

func TestRegisterParityTableNamesARealTestForEveryRow(t *testing.T) {
	registerEnv(t, "")
	declared := func(dir string) map[string]bool {
		pkgs, err := parser.ParseDir(token.NewFileSet(), dir, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		have := map[string]bool{}
		for _, pkg := range pkgs {
			for _, file := range pkg.Files {
				for _, decl := range file.Decls {
					if fn, ok := decl.(*ast.FuncDecl); ok {
						have[fn.Name.Name] = true
					}
				}
			}
		}
		return have
	}
	for id, row := range registerParityRows(t) {
		if row.test == "-" {
			continue
		}
		dir, name := ".", row.test
		if before, after, found := strings.Cut(row.test, ":"); found {
			dir, name = filepath.Join("..", before), after
		}
		if !declared(dir)[name] {
			t.Fatalf("row %q names test %q, which does not exist in %s", id, name, dir)
		}
	}
}

type parityRow struct{ kind, reason, test string }

func registerParityRows(t *testing.T) map[string]parityRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "ci", "auth_register_refusals.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]parityRow{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 6 {
			t.Fatalf("parity row %q has %d columns, want 6", line, len(f))
		}
		rows[f[0]] = parityRow{kind: f[3], reason: f[4], test: f[5]}
	}
	return rows
}

func TestRegisterRouteCreatesTheOrganizationByDefaultAndNamesItFromOrgName(t *testing.T) {
	c := newCredFixture(t)
	c.call(http.MethodPost, PatternRegister, registerBody, nil)
	c.call(http.MethodPost, PatternRegister, `{"email":"b@example.com","password":"Correct-horse-battery-9"}`, nil)
	c.call(http.MethodPost, PatternRegister, `{"email":"c@example.com","password":"Correct-horse-battery-9","org_name":""}`, nil)
	names := []string{}
	for _, req := range c.fakes.provisioned {
		if req.OrganizationName == nil {
			t.Fatal("a sign-up under the default setting created no organization")
		}
		names = append(names, *req.OrganizationName)
	}
	if len(names) != 3 || names[0] != "Ignored Org" || names[1] != "My Organization" || names[2] != "My Organization" {
		t.Fatalf("organization names = %v", names)
	}
}

func TestRegisterRouteOrganizationSettingParsesAsGoAPIDoes(t *testing.T) {
	cases := map[string]bool{
		"": true, "1": true, "true": true, "yes": true, "on": true, "junk": true, "offf": true,
		"0": false, "false": false, "FALSE": false, "No": false, "off": false, " OFF ": false, "\u2003off\u2003": false,
	}
	for value, wantOrg := range cases {
		registerEnv(t, value)
		c := newCredFixture(t)
		c.call(http.MethodPost, PatternRegister, registerBody, nil)
		if len(c.fakes.provisioned) != 1 {
			t.Fatalf("%q: provisioned %d", value, len(c.fakes.provisioned))
		}
		if got := c.fakes.provisioned[0].OrganizationName != nil; got != wantOrg {
			t.Fatalf("setting %q: organization = %v, want %v", value, got, wantOrg)
		}
	}
}

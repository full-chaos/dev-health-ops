package livee2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/full-chaos/dev-health-ops/internal/cigates/livee2e"
)

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type m = map[string]any

func healthy() m {
	return m{"status": "ok", "services": m{"clickhouse": "ok", "postgres": "ok"}}
}

func TestAssertHealth(t *testing.T) {
	if err := livee2e.AssertHealth(mustJSON(t, healthy())); err != nil {
		t.Fatal(err)
	}
	notConfigured := healthy()
	notConfigured["services"] = m{"clickhouse": "ok", "postgres": "not_configured"}
	if err := livee2e.AssertHealth(mustJSON(t, notConfigured)); err != nil {
		t.Fatalf("postgres not_configured is allowed: %v", err)
	}
	for name, p := range map[string]m{
		"status degraded":     {"status": "degraded", "services": m{"clickhouse": "ok", "postgres": "ok"}},
		"clickhouse down":     {"status": "ok", "services": m{"clickhouse": "down", "postgres": "ok"}},
		"postgres down":       {"status": "ok", "services": m{"clickhouse": "ok", "postgres": "down"}},
		"postgres missing":    {"status": "ok", "services": m{"clickhouse": "ok"}},
		"no services":         {"status": "ok"},
		"status missing":      {"services": m{"clickhouse": "ok", "postgres": "ok"}},
		"clickhouse missing":  {"status": "ok", "services": m{"postgres": "ok"}},
		"services not object": {"status": "ok", "services": "ok"},
	} {
		if err := livee2e.AssertHealth(mustJSON(t, p)); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
	if err := livee2e.AssertHealth([]byte("not json")); err == nil {
		t.Error("garbage must fail")
	}
	if err := livee2e.AssertHealth([]byte("null")); err == nil {
		t.Error("null must fail")
	}
}

func goodMeta() m {
	return m{"backend": "clickhouse", "limits": m{"max_days": 365, "max_repos": 1000}, "supported_endpoints": []any{"/api/v1/home", "/api/v1/meta"}}
}

func TestAssertMeta(t *testing.T) {
	if err := livee2e.AssertMeta(mustJSON(t, goodMeta())); err != nil {
		t.Fatal(err)
	}
	mutate := func(f func(p m)) []byte {
		p := goodMeta()
		f(p)
		return mustJSON(t, p)
	}
	for name, raw := range map[string][]byte{
		"backend":        mutate(func(p m) { p["backend"] = "postgres" }),
		"max_days":       mutate(func(p m) { p["limits"] = m{"max_days": 90, "max_repos": 1000} }),
		"max_repos":      mutate(func(p m) { p["limits"] = m{"max_days": 365, "max_repos": 10} }),
		"no limits":      mutate(func(p m) { delete(p, "limits") }),
		"no home route":  mutate(func(p m) { p["supported_endpoints"] = []any{"/api/v1/meta"} }),
		"no endpoints":   mutate(func(p m) { delete(p, "supported_endpoints") }),
		"endpoints type": mutate(func(p m) { p["supported_endpoints"] = "/api/v1/home" }),
	} {
		if err := livee2e.AssertMeta(raw); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
}

func goodHome() m {
	return m{
		"freshness": m{
			"last_ingested_at": "2026-10-20T00:00:00Z",
			"sources":          m{"ci": "ok", "github": "stale"},
			"coverage":         m{"repos_covered_pct": 100.0},
		},
		"deltas": []any{
			m{"metric": "cycle_time", "value": 1.0, "has_data": true, "has_prior_data": true, "rate_state": "ok"},
			m{"metric": "throughput", "value": 2.0, "has_data": true, "has_prior_data": false, "rate_state": "ok"},
		},
		"tiles":      m{"understand": m{}, "measure": m{}, "align": m{}, "execute": m{}},
		"constraint": m{"title": "A constraint", "claim": "c", "evidence": "e"},
	}
}

func TestAssertHome(t *testing.T) {
	if err := livee2e.AssertHome(mustJSON(t, goodHome()), "github"); err != nil {
		t.Fatal(err)
	}
	if err := livee2e.AssertHome(mustJSON(t, goodHome()), ""); err != nil {
		t.Fatalf("an empty FIXTURE_PROVIDER means github: %v", err)
	}
	synthetic := goodHome()
	synthetic["freshness"].(m)["sources"] = m{"ci": "ok"}
	if err := livee2e.AssertHome(mustJSON(t, synthetic), "synthetic"); err != nil {
		t.Fatalf("the synthetic provider expects only ci: %v", err)
	}
	if err := livee2e.AssertHome(mustJSON(t, goodHome()), "synthetic"); err == nil {
		t.Error("synthetic must reject an extra source")
	}
	noMove := goodHome()
	noMove["deltas"] = []any{m{"metric": "x", "value": 1, "has_data": true, "has_prior_data": false, "rate_state": "ok"}}
	noMove["constraint"] = m{}
	if err := livee2e.AssertHome(mustJSON(t, noMove), "github"); err != nil {
		t.Fatalf("no metric with data in both windows and an empty constraint card is valid: %v", err)
	}
	cases := map[string]func(p m){
		"no last_ingested_at": func(p m) { delete(p["freshness"].(m), "last_ingested_at") },
		"missing source":      func(p m) { p["freshness"].(m)["sources"] = m{"ci": "ok"} },
		"extra source":        func(p m) { p["freshness"].(m)["sources"] = m{"ci": "ok", "github": "ok", "gitlab": "ok"} },
		"bad state":           func(p m) { p["freshness"].(m)["sources"] = m{"ci": "ok", "github": "great"} },
		"zero coverage":       func(p m) { p["freshness"].(m)["coverage"] = m{"repos_covered_pct": 0} },
		"no coverage":         func(p m) { delete(p["freshness"].(m), "coverage") },
		"no deltas":           func(p m) { p["deltas"] = []any{} },
		"delta without value": func(p m) {
			p["deltas"] = []any{m{"metric": "x", "has_data": true, "has_prior_data": true, "rate_state": "ok"}}
		},
		"delta without flags": func(p m) { p["deltas"] = []any{m{"metric": "x", "value": 1}} },
		"missing tile":        func(p m) { delete(p["tiles"].(m), "execute") },
		"move but empty card": func(p m) { p["constraint"] = m{} },
		"move without evidence": func(p m) {
			p["constraint"] = m{"title": "t", "evidence": ""}
		},
	}
	for name, f := range cases {
		p := goodHome()
		f(p)
		if err := livee2e.AssertHome(mustJSON(t, p), "github"); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
	for name, constraint := range map[string]m{
		"claim without a move":    {"claim": "c"},
		"title without a move":    {"title": "t"},
		"evidence without a move": {"evidence": "e"},
	} {
		p := goodHome()
		p["deltas"] = []any{m{"metric": "x", "value": 1, "has_data": false, "has_prior_data": false, "rate_state": "ok"}}
		p["constraint"] = constraint
		if err := livee2e.AssertHome(mustJSON(t, p), "github"); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
}

func TestEdgeRequest(t *testing.T) {
	docs := mustJSON(t, []m{
		{"operation": "other", "document": "query { other }"},
		{"operation": "featureFlags", "document": "query FF($orgId: ID!) { featureFlags(orgId: $orgId) { id } }"},
	})
	got, err := livee2e.EdgeRequest(docs, "featureFlags", "org-1")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"query":"query FF($orgId: ID!) { featureFlags(orgId: $orgId) { id } }","variables":{"orgId":"org-1","provider":null,"project":null,"includeArchived":false,"limit":100}}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
	twice := mustJSON(t, []m{{"operation": "featureFlags", "document": "a"}, {"operation": "featureFlags", "document": "b"}})
	if _, err := livee2e.EdgeRequest(twice, "featureFlags", "o"); err == nil {
		t.Error("two documents for one operation must fail")
	}
	if _, err := livee2e.EdgeRequest(mustJSON(t, []m{}), "featureFlags", "o"); err == nil {
		t.Error("no document must fail")
	}
	other := mustJSON(t, []m{{"operation": "other", "document": "x"}})
	if _, err := livee2e.EdgeRequest(other, "other", "o"); err == nil {
		t.Error("an operation without request variables must fail")
	}
	if _, err := livee2e.EdgeRequest([]byte("{"), "featureFlags", "o"); err == nil {
		t.Error("garbage must fail")
	}
}

func TestJWTSecretIsTheHexSha256(t *testing.T) {
	if got, want := livee2e.JWTSecret("dev-key-not-for-prod"), "96dcb17571b53273398be201758373495565e7dbea74a9540b791ad6d0b42581"; got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestMintTokenIsAnHS256AdminToken(t *testing.T) {
	secret := livee2e.JWTSecret("k")
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	raw, err := livee2e.MintToken(secret, livee2e.DefaultOrgID, now, "jti-1")
	if err != nil {
		t.Fatal(err)
	}
	claims := jwt.MapClaims{}
	tok, err := jwt.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return []byte(secret), nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithTimeFunc(func() time.Time { return now.Add(time.Minute) }))
	if err != nil || !tok.Valid {
		t.Fatalf("token does not verify: %v", err)
	}
	for k, want := range map[string]any{
		"sub": livee2e.E2EUserID, "email": "e2e@test.local", "org_id": livee2e.DefaultOrgID, "role": "admin",
		"is_superuser": false, "type": "access", "jti": "jti-1",
	} {
		if claims[k] != want {
			t.Errorf("claim %s = %v, want %v", k, claims[k], want)
		}
	}
	if claims["exp"] != float64(now.Add(time.Hour).Unix()) || claims["iat"] != float64(now.Unix()) {
		t.Errorf("exp/iat = %v/%v", claims["exp"], claims["iat"])
	}
	if _, err := jwt.Parse(raw, func(*jwt.Token) (any, error) { return []byte("other"), nil }); err == nil {
		t.Error("a token signed with one key must not verify with another")
	}
}

type call struct {
	sql  string
	args []any
}

type fakeRow struct {
	vals []any
	err  error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = r.vals[i].(string)
		case *bool:
			*p = r.vals[i].(bool)
		case *int64:
			*p = r.vals[i].(int64)
		default:
			return errors.New("unsupported scan destination")
		}
	}
	return nil
}

type fakeDB struct {
	calls   []call
	execErr error
	row     fakeRow
}

func (d *fakeDB) Exec(_ context.Context, sql string, args ...any) error {
	d.calls = append(d.calls, call{sql, args})
	return d.execErr
}

func (d *fakeDB) QueryRow(_ context.Context, sql string, args ...any) livee2e.Row {
	d.calls = append(d.calls, call{sql, args})
	return d.row
}

var now = time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

func TestSeedE2EIdentityInsertsOrgUserAndAdminMembershipIdempotently(t *testing.T) {
	db := &fakeDB{}
	if err := livee2e.SeedE2EIdentity(context.Background(), db, livee2e.DefaultOrgID, now); err != nil {
		t.Fatal(err)
	}
	if len(db.calls) != 3 {
		t.Fatalf("%d statements, want 3", len(db.calls))
	}
	for i, table := range []string{"organizations", "users", "memberships"} {
		if !strings.Contains(db.calls[i].sql, "INSERT INTO "+table+" ") || !strings.Contains(db.calls[i].sql, "ON CONFLICT") {
			t.Errorf("statement %d is not an idempotent insert into %s: %s", i, table, db.calls[i].sql)
		}
	}
	if db.calls[0].args[0] != livee2e.DefaultOrgID || db.calls[0].args[4] != "enterprise" {
		t.Errorf("organization args %v", db.calls[0].args)
	}
	if db.calls[1].args[0] != livee2e.E2EUserID || db.calls[1].args[2] != "local" {
		t.Errorf("user args %v", db.calls[1].args)
	}
	if db.calls[2].args[1] != livee2e.E2EUserID || db.calls[2].args[2] != livee2e.DefaultOrgID || db.calls[2].args[3] != "admin" {
		t.Errorf("membership args %v", db.calls[2].args)
	}
	boom := &fakeDB{execErr: errors.New("boom")}
	if err := livee2e.SeedE2EIdentity(context.Background(), boom, livee2e.DefaultOrgID, now); err == nil || len(boom.calls) != 1 {
		t.Errorf("the first failure must stop the seed: %v after %d calls", err, len(boom.calls))
	}
}

const principalID = "00000000-0000-4000-8000-00000000e0e1"

func TestPrincipalAssertMigrated(t *testing.T) {
	good := &fakeDB{row: fakeRow{vals: []any{"service", true, true, false, int64(0), int64(0)}}}
	if err := livee2e.Principal(context.Background(), good, "assert-migrated", principalID, livee2e.DefaultOrgID, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(good.calls[0].sql, "FROM users WHERE id = $1::uuid") || good.calls[0].args[0] != principalID {
		t.Errorf("query %s args %v", good.calls[0].sql, good.calls[0].args)
	}
	for name, vals := range map[string][]any{
		"not a service":        {"local", true, true, false, int64(0), int64(0)},
		"has a password":       {"service", false, true, false, int64(0), int64(0)},
		"inactive":             {"service", true, false, false, int64(0), int64(0)},
		"superuser":            {"service", true, true, true, int64(0), int64(0)},
		"bumped token":         {"service", true, true, false, int64(1), int64(0)},
		"already a membership": {"service", true, true, false, int64(0), int64(1)},
	} {
		db := &fakeDB{row: fakeRow{vals: vals}}
		if err := livee2e.Principal(context.Background(), db, "assert-migrated", principalID, livee2e.DefaultOrgID, now); err == nil {
			t.Errorf("%s must fail", name)
		}
	}
	missing := &fakeDB{row: fakeRow{err: errors.New("no rows")}}
	if err := livee2e.Principal(context.Background(), missing, "assert-migrated", principalID, livee2e.DefaultOrgID, now); err == nil || !strings.Contains(err.Error(), "did not create the proof service principal row") {
		t.Errorf("a missing row must name the migration: %v", err)
	}
}

func TestPrincipalMutations(t *testing.T) {
	for action, wantSQL := range map[string]string{
		"grant-membership":   "INSERT INTO memberships",
		"bump-token-version": "UPDATE users SET token_version = token_version + 1",
		"deactivate":         "UPDATE users SET is_active = false",
	} {
		db := &fakeDB{}
		if err := livee2e.Principal(context.Background(), db, action, principalID, livee2e.DefaultOrgID, now); err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		if len(db.calls) != 1 || !strings.Contains(db.calls[0].sql, wantSQL) {
			t.Errorf("%s ran %v", action, db.calls)
		}
	}
	grant := &fakeDB{}
	_ = livee2e.Principal(context.Background(), grant, "grant-membership", principalID, livee2e.DefaultOrgID, now)
	if grant.calls[0].args[1] != principalID || grant.calls[0].args[2] != livee2e.DefaultOrgID || grant.calls[0].args[3] != "viewer" || !strings.Contains(grant.calls[0].sql, "ON CONFLICT DO NOTHING") {
		t.Errorf("grant args %v sql %s", grant.calls[0].args, grant.calls[0].sql)
	}
	if err := livee2e.Principal(context.Background(), &fakeDB{}, "drop-table", principalID, livee2e.DefaultOrgID, now); err == nil {
		t.Error("an unknown action must fail")
	}
	failing := &fakeDB{execErr: errors.New("boom")}
	if err := livee2e.Principal(context.Background(), failing, "deactivate", principalID, livee2e.DefaultOrgID, now); err == nil {
		t.Error("a failed statement must fail")
	}
}

// serveRedis answers each command with the next reply in order, then closes.
func serveRedis(t *testing.T, replies ...string) (url string, received func() []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skipf("cannot listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	got := make(chan []string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- nil
			return
		}
		defer conn.Close()
		var cmds []string
		buf := make([]byte, 4096)
		for _, reply := range replies {
			n, err := conn.Read(buf)
			if err != nil {
				break
			}
			cmds = append(cmds, string(buf[:n]))
			_, _ = conn.Write([]byte(reply + "\r\n"))
		}
		got <- cmds
	}()
	return "redis://" + ln.Addr().String() + "/1", func() []string { return <-got }
}

func TestValkeyPing(t *testing.T) {
	url, received := serveRedis(t, "+PONG")
	if err := livee2e.ValkeyPing(context.Background(), url); err != nil {
		t.Fatal(err)
	}
	if cmds := received(); len(cmds) != 1 || !strings.Contains(cmds[0], "PING") {
		t.Errorf("commands %q", cmds)
	}
	bad, _ := serveRedis(t, "-LOADING busy")
	if err := livee2e.ValkeyPing(context.Background(), bad); err == nil {
		t.Error("an error reply must fail")
	}
	authURL, authReceived := serveRedis(t, "+OK", "+PONG")
	authURL = strings.Replace(authURL, "redis://", "redis://:pw@", 1)
	if err := livee2e.ValkeyPing(context.Background(), authURL); err != nil {
		t.Fatal(err)
	}
	if cmds := authReceived(); len(cmds) != 2 || !strings.Contains(cmds[0], "AUTH") || !strings.Contains(cmds[0], "pw") {
		t.Errorf("commands %q", cmds)
	}
	refused := func() string {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := ln.Addr().String()
		_ = ln.Close()
		return "redis://" + addr
	}()
	if err := livee2e.ValkeyPing(context.Background(), refused); err == nil {
		t.Error("a closed port must fail")
	}
	if err := livee2e.ValkeyPing(context.Background(), "not a url"); err == nil {
		t.Error("a non-URL must fail")
	}
}

func run(t *testing.T, db *fakeDB, environ map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	connect := func(_ context.Context, uri string) (livee2e.DB, func(), error) {
		if db == nil {
			return nil, nil, errors.New("no database in this test")
		}
		db.calls = append(db.calls, call{sql: "CONNECT " + uri})
		return db, func() {}, nil
	}
	rc := livee2e.Run(args, func(k string) string { return environ[k] }, connect, &out, &errb)
	return rc, out.String(), errb.String()
}

func writeTemp(t *testing.T, name string, content []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestRunAssertVerbs(t *testing.T) {
	health := writeTemp(t, "health.json", mustJSON(t, healthy()))
	meta := writeTemp(t, "meta.json", mustJSON(t, goodMeta()))
	home := writeTemp(t, "home.json", mustJSON(t, goodHome()))
	bad := writeTemp(t, "bad.json", []byte(`{"status":"down"}`))
	for _, c := range []struct {
		args []string
		rc   int
	}{
		{[]string{"assert-health", health}, 0},
		{[]string{"assert-meta", meta}, 0},
		{[]string{"assert-home", home}, 0},
		{[]string{"assert-health", bad}, 1},
		{[]string{"assert-health", filepath.Join(t.TempDir(), "missing.json")}, 1},
		{[]string{"assert-health"}, 2},
	} {
		if rc, _, stderr := run(t, nil, nil, c.args...); rc != c.rc || (c.rc != 0 && stderr == "") {
			t.Errorf("%v: rc=%d stderr=%q", c.args, rc, stderr)
		}
	}
	if rc, _, stderr := run(t, nil, map[string]string{"FIXTURE_PROVIDER": "synthetic"}, "assert-home", home); rc != 1 {
		t.Errorf("FIXTURE_PROVIDER must reach the home check: rc=%d stderr=%q", rc, stderr)
	}
}

func TestRunEdgeRequestJWTSecretAndUsage(t *testing.T) {
	docs := writeTemp(t, "documents.json", mustJSON(t, []m{{"operation": "featureFlags", "document": "query X { featureFlags }"}}))
	rc, out, _ := run(t, nil, nil, "edge-request", docs, "featureFlags", "org-9")
	if rc != 0 || !strings.Contains(out, `"orgId":"org-9"`) {
		t.Fatalf("rc=%d out=%q", rc, out)
	}
	rc, out, _ = run(t, nil, map[string]string{"ENC_KEY_INPUT": "dev-key-not-for-prod"}, "jwt-secret")
	if rc != 0 || out != "96dcb17571b53273398be201758373495565e7dbea74a9540b791ad6d0b42581\n" {
		t.Fatalf("rc=%d out=%q", rc, out)
	}
	if rc, _, _ := run(t, nil, nil, "jwt-secret"); rc != 1 {
		t.Errorf("an empty ENC_KEY_INPUT must fail, rc=%d", rc)
	}
	for _, args := range [][]string{nil, {"bogus"}, {"edge-request", "a"}, {"principal", "assert-migrated"}} {
		if rc, _, stderr := run(t, nil, nil, args...); rc != 2 || !strings.Contains(stderr, "usage: cigate live-e2e") {
			t.Errorf("%v: rc=%d stderr=%q", args, rc, stderr)
		}
	}
	url, _ := serveRedis(t, "+PONG")
	if rc, _, stderr := run(t, nil, map[string]string{"REDIS_URL": url}, "valkey-ping"); rc != 0 {
		t.Errorf("valkey-ping rc=%d %q", rc, stderr)
	}
}

func TestRunAuthTokenAndPrincipalUseThePostgresURIWithoutTheAsyncDriverSuffix(t *testing.T) {
	db := &fakeDB{}
	environ := map[string]string{
		"POSTGRES_URI":            "postgresql+asyncpg://postgres:postgres@localhost:5432/test_db",
		"E2E_ORG_ID":              livee2e.DefaultOrgID,
		"SETTINGS_ENCRYPTION_KEY": "k",
	}
	rc, out, stderr := run(t, db, environ, "auth-token")
	if rc != 0 || stderr != "" {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
	if db.calls[0].sql != "CONNECT postgresql://postgres:postgres@localhost:5432/test_db" {
		t.Errorf("connected with %q", db.calls[0].sql)
	}
	token := strings.TrimSpace(out)
	if _, err := jwt.Parse(token, func(*jwt.Token) (any, error) { return []byte(livee2e.JWTSecret("k")), nil }, jwt.WithValidMethods([]string{"HS256"})); err != nil {
		t.Errorf("printed token does not verify with the secret of the encryption key: %v", err)
	}
	if len(db.calls) != 4 {
		t.Errorf("want connect + 3 inserts, got %d calls", len(db.calls))
	}
	pdb := &fakeDB{}
	if rc, _, stderr := run(t, pdb, environ, "principal", "deactivate", principalID); rc != 0 {
		t.Fatalf("rc=%d stderr=%q", rc, stderr)
	}
	if len(pdb.calls) != 2 || !strings.Contains(pdb.calls[1].sql, "is_active = false") {
		t.Errorf("calls %v", pdb.calls)
	}
	fallback := map[string]string{"DATABASE_URI": "postgresql://u:p@h/d"}
	fdb := &fakeDB{}
	if rc, _, _ := run(t, fdb, fallback, "principal", "deactivate", principalID); rc != 0 || fdb.calls[0].sql != "CONNECT postgresql://u:p@h/d" {
		t.Errorf("DATABASE_URI must be the fallback: %v", fdb.calls)
	}
	if rc, _, stderr := run(t, &fakeDB{}, nil, "auth-token"); rc != 1 || !strings.Contains(stderr, "POSTGRES_URI or DATABASE_URI is not set") {
		t.Errorf("rc=%d stderr=%q", rc, stderr)
	}
	if rc, _, _ := run(t, nil, environ, "auth-token"); rc != 1 {
		t.Errorf("a connect failure must fail, rc=%d", rc)
	}
	bdb := &fakeDB{execErr: errors.New("boom")}
	if rc, out, _ := run(t, bdb, environ, "auth-token"); rc != 1 || out != "" {
		t.Errorf("a failed seed must print no token: rc=%d out=%q", rc, out)
	}
}

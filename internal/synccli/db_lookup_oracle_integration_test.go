//go:build integration

package synccli

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "embed"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

//go:embed testdata/db_lookup_oracle.py
var dbLookupOracleProgram string

const upgradeProgram = `
import argparse, os, sys
from dev_health_ops.db import normalize_async_postgres_uri
from dev_health_ops.migrate import _run_upgrade
sys.exit(_run_upgrade(argparse.Namespace(db=normalize_async_postgres_uri(os.environ["DHO_ORACLE_DB_URL"]), revision="head")))
`

type dbOrg struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	CreatedAt string `json:"created_at"`
}

type dbCred struct {
	OrgID    string            `json:"org_id"`
	Provider string            `json:"provider,omitempty"`
	Name     string            `json:"name,omitempty"`
	Active   *bool             `json:"active,omitempty"`
	Payload  *string           `json:"payload"`
	Raw      *string           `json:"raw"`
	Legacy   bool              `json:"legacy,omitempty"`
	Env      map[string]string `json:"env"`
}

type dbScenario struct {
	Name   string
	Orgs   []dbOrg
	Creds  []dbCred
	Args   []string          // after "sync git --provider github --owner a --repo b"
	Env    map[string]string // the run environment (POSTGRES_URI is added)
	DBURL  func(base string) string
	NoPG   bool // do not put a database URL in the environment
	Expect string
}

const (
	orgOld = "11111111-1111-4111-8111-111111111111"
	orgNew = "22222222-2222-4222-8222-222222222222"
)

var encEnv = map[string]string{"SETTINGS_ENCRYPTION_KEY": "oracle-key-1", "SETTINGS_ENCRYPTION_SALT": "oracle-salt-1"}

func str(s string) *string { return &s }
func no() *bool            { b := false; return &b }

func merge(maps ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, m := range maps {
		for k, v := range m {
			out[k] = v
		}
	}
	return out
}

func twoOrgs() []dbOrg {
	// The NEWER organization is inserted first: only created_at may decide.
	return []dbOrg{
		{ID: orgNew, Slug: "new", CreatedAt: "2026-05-01T00:00:00+00:00"},
		{ID: orgOld, Slug: "old", CreatedAt: "2026-01-01T00:00:00+00:00"},
	}
}

func dbScenarios(keyFile string) []dbScenario {
	pat := `{"token": "ghp_db_token"}`
	app := `{"app_id": "42", "private_key": "-----BEGIN KEY-----\nabc", "installation_id": "7"}`
	cred := func(payload string, mutate ...func(*dbCred)) []dbCred {
		c := dbCred{OrgID: orgOld, Payload: str(payload), Env: encEnv}
		for _, m := range mutate {
			m(&c)
		}
		return []dbCred{c}
	}
	withOrg := []string{"--org", orgOld}
	plain := func(s string) func(string) string { return func(base string) string { return s } }
	out := []dbScenario{
		{Name: "pat", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: encEnv},
		{Name: "app", Orgs: twoOrgs(), Creds: cred(app), Args: withOrg, Env: encEnv},
		{Name: "pat with base_url", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "base_url": "https://ghe.example.com/api/v3"}`), Args: withOrg, Env: encEnv},
		{Name: "empty base_url", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "base_url": ""}`), Args: withOrg, Env: encEnv},
		{Name: "camelCase keys are not aliased", Orgs: twoOrgs(), Creds: cred(`{"appId": "1", "privateKey": "k", "installationId": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "unknown key", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "foo": "bar"}`), Args: withOrg, Env: encEnv},
		{Name: "null values are dropped", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "app_id": null, "base_url": null}`), Args: withOrg, Env: encEnv},
		{Name: "token and app", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "app_id": "1", "private_key": "k", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "non-string beside a token", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "installation_id": 5}`), Args: withOrg, Env: encEnv},
		{Name: "token and a partial app", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "app_id": "1"}`), Args: withOrg, Env: encEnv},
		{Name: "empty token and a full app", Orgs: twoOrgs(), Creds: cred(`{"token": "", "app_id": "1", "private_key": "k", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "token and an installation id", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "token and a private key", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "private_key": "k"}`), Args: withOrg, Env: encEnv},
		{Name: "app without an app id", Orgs: twoOrgs(), Creds: cred(`{"private_key": "k", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "app without a private key", Orgs: twoOrgs(), Creds: cred(`{"app_id": "1", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "partial app", Orgs: twoOrgs(), Creds: cred(`{"app_id": "1", "private_key": "k"}`), Args: withOrg, Env: encEnv},
		{Name: "empty token", Orgs: twoOrgs(), Creds: cred(`{"token": ""}`), Args: withOrg, Env: encEnv},
		{Name: "empty object", Orgs: twoOrgs(), Creds: cred(`{}`), Args: withOrg, Env: encEnv},
		{Name: "json null", Orgs: twoOrgs(), Creds: cred(`null`), Args: withOrg, Env: encEnv},
		{Name: "json list", Orgs: twoOrgs(), Creds: cred(`["a"]`), Args: withOrg, Env: encEnv},
		{Name: "json string", Orgs: twoOrgs(), Creds: cred(`"t"`), Args: withOrg, Env: encEnv},
		{Name: "not json", Orgs: twoOrgs(), Creds: cred(`not json`), Args: withOrg, Env: encEnv},
		{Name: "private key from a path", Orgs: twoOrgs(), Creds: cred(fmt.Sprintf(`{"app_id": "1", "private_key_path": %q, "installation_id": "2"}`, keyFile)), Args: withOrg, Env: encEnv},
		{Name: "private key null and a path", Orgs: twoOrgs(), Creds: cred(fmt.Sprintf(`{"app_id": "1", "private_key": null, "private_key_path": %q, "installation_id": "2"}`, keyFile)), Args: withOrg, Env: encEnv},
		{Name: "private key wins over a path", Orgs: twoOrgs(), Creds: cred(fmt.Sprintf(`{"app_id": "1", "private_key": "inline", "private_key_path": %q, "installation_id": "2"}`, keyFile)), Args: withOrg, Env: encEnv},
		{Name: "unreadable key path", Orgs: twoOrgs(), Creds: cred(`{"app_id": "1", "private_key_path": "/nonexistent/key.pem", "installation_id": "2"}`), Args: withOrg, Env: encEnv},
		{Name: "dataclass fields accepted", Orgs: twoOrgs(), Creds: cred(`{"token": "t", "provider": "github", "credential_name": "x", "extra": {"a": 1}}`), Args: withOrg, Env: encEnv},
		{Name: "only a non-default name", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.Name = "prod" }), Args: withOrg, Env: encEnv},
		{Name: "inactive default is still read", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.Active = no() }), Args: withOrg, Env: encEnv},
		{Name: "another provider only", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.Provider = "gitlab" }), Args: withOrg, Env: encEnv},
		{Name: "another organization only", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.OrgID = orgNew }), Args: withOrg, Env: encEnv},
		{Name: "no rows", Orgs: twoOrgs(), Args: withOrg, Env: encEnv},
		{Name: "null ciphertext", Orgs: twoOrgs(), Creds: []dbCred{{OrgID: orgOld}}, Args: withOrg, Env: encEnv},
		{Name: "empty ciphertext", Orgs: twoOrgs(), Creds: []dbCred{{OrgID: orgOld, Raw: str("")}}, Args: withOrg, Env: encEnv},
		{Name: "garbage ciphertext", Orgs: twoOrgs(), Creds: []dbCred{{OrgID: orgOld, Raw: str("v1:garbage")}}, Args: withOrg, Env: encEnv},
		{Name: "wrong key", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: merge(encEnv, map[string]string{"SETTINGS_ENCRYPTION_KEY": "another-key"})},
		{Name: "wrong salt", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: merge(encEnv, map[string]string{"SETTINGS_ENCRYPTION_SALT": "another-salt"})},
		{Name: "no key configured", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg},
		{Name: "default salt", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.Env = map[string]string{"SETTINGS_ENCRYPTION_KEY": "k-only"} }), Args: withOrg, Env: map[string]string{"SETTINGS_ENCRYPTION_KEY": "k-only"}},
		{Name: "legacy ciphertext", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.Legacy = true }), Args: withOrg, Env: encEnv},
		{Name: "first org: oldest wins", Orgs: twoOrgs(), Creds: cred(pat), Env: encEnv},
		{Name: "first org: its credential", Orgs: twoOrgs(), Creds: cred(pat, func(c *dbCred) { c.OrgID = orgNew }), Env: encEnv},
		{Name: "first org from ORG_ID", Orgs: twoOrgs(), Creds: cred(pat), Env: merge(encEnv, map[string]string{"ORG_ID": orgOld})},
		{Name: "ORG_ID empty", Orgs: twoOrgs(), Creds: cred(pat), Env: merge(encEnv, map[string]string{"ORG_ID": ""})},
		{Name: "--org empty", Orgs: twoOrgs(), Creds: cred(pat), Args: []string{"--org", ""}, Env: encEnv},
		{Name: "no organization at all", Creds: cred(pat), Env: encEnv},
		{Name: "--org names no organization", Orgs: twoOrgs(), Creds: cred(pat), Args: []string{"--org", "no-such-org"}, Env: encEnv},
		{Name: "no database configured", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: encEnv, NoPG: true},
		{Name: "--auth beats the database", Orgs: twoOrgs(), Creds: cred(pat), Args: append([]string{"--auth", "cli-token"}, withOrg...), Env: encEnv},
		{Name: "GITHUB_TOKEN beats the database", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: merge(encEnv, map[string]string{"GITHUB_TOKEN": "env-token"})},
		{Name: "--db flag", Orgs: twoOrgs(), Creds: cred(pat), Args: withOrg, Env: encEnv, NoPG: true},
	}
	// Database URL forms. The lookup reads the same database whatever the form
	// the operator typed, or fails the way Python fails.
	forms := []struct {
		name string
		make func(base string) string
	}{
		{"scheme postgresql", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "postgresql://", 1) }},
		{"scheme postgresql+psycopg2", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "postgresql+psycopg2://", 1) }},
		{"scheme postgres", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "postgres://", 1) }},
		{"scheme postgresql+psycopg", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "postgresql+psycopg://", 1) }},
		{"scheme postgresql+pg8000", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "postgresql+pg8000://", 1) }},
		{"scheme mysql", func(b string) string { return strings.Replace(b, "postgresql+asyncpg://", "mysql://", 1) }},
		{"not a url", plain("not a url")},
		{"query sslmode=disable", func(b string) string { return b + "?sslmode=disable" }},
		{"query sslmode=require", func(b string) string { return b + "?sslmode=require" }},
		{"query sslmode=prefer", func(b string) string { return b + "?sslmode=prefer" }},
		{"query ssl=disable", func(b string) string { return b + "?ssl=disable" }},
		{"query ssl=require", func(b string) string { return b + "?ssl=require" }},
		{"query ssl=prefer", func(b string) string { return b + "?ssl=prefer" }},
		{"query ssl=false", func(b string) string { return b + "?ssl=false" }},
		{"query ssl=bogus", func(b string) string { return b + "?ssl=bogus" }},
		{"query sslmode=bogus", func(b string) string { return b + "?sslmode=bogus" }},
		{"query connect_timeout=5", func(b string) string { return b + "?connect_timeout=5" }},
		{"query application_name=x", func(b string) string { return b + "?application_name=x" }},
		{"query timeout=5", func(b string) string { return b + "?timeout=5" }},
		{"query command_timeout=5", func(b string) string { return b + "?command_timeout=5" }},
		{"query statement_cache_size=0", func(b string) string { return b + "?statement_cache_size=0" }},
		{"query unknown_param=1", func(b string) string { return b + "?unknown_param=1" }},
		{"query both sslmode and ssl", func(b string) string { return b + "?sslmode=disable&ssl=disable" }},
		{"empty query", func(b string) string { return b + "?" }},
		{"fragment", func(b string) string { return b + "#frag" }},
		{"empty", plain("")},
		{"closed port", func(b string) string { return "postgresql+asyncpg://nobody:x@127.0.0.1:1/none" }},
		{"wrong password", func(b string) string {
			u, _ := url.Parse(b)
			u.User = url.UserPassword(u.User.Username(), "wrong")
			return u.String()
		}},
	}
	for _, form := range forms {
		for _, orgArgs := range [][]string{withOrg, nil} {
			label := "with --org"
			if orgArgs == nil {
				label = "first org"
			}
			out = append(out, dbScenario{Name: "url " + form.name + " " + label, Orgs: twoOrgs(), Creds: cred(pat), Args: orgArgs, Env: encEnv, DBURL: form.make})
		}
	}
	return out
}

// dbLookupPythonBuild is the build whose Python `dev-hops sync` lookups answered the scenarios: a build that
// still carried the Python CLI.
const dbLookupPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// dbLookupPythonSettings are the variables that shape the producer's answers, as constants: the producer's
// environment AND part of the golden's request key (a changed value fails the frozen replay). The database
// address (DHO_ORACLE_DB_URL) is a per-run value, appended by name.
var dbLookupPythonSettings = map[string]string{"PYTHONHASHSEED": "0", "OTEL_ENABLED": "false"}

func dbLookupPythonEnv(root string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "PYTHONPATH=" + filepath.Join(root, "src"), "PYTHONDONTWRITEBYTECODE=1"}
	for _, name := range []string{"OTEL_ENABLED", "PYTHONHASHSEED"} {
		env = append(env, name+"="+dbLookupPythonSettings[name])
	}
	return env
}

// dbKeyPlaceholder stands for the GitHub App key file some scenarios name (a per-run path).
const dbKeyPlaceholder = "/tmp/dho-db-lookup-app-key.pem"

// scenarioView is what a scenario contributes to the golden's key: its name, organizations, the credentials'
// shape and the arguments. The credential payloads and environment values are test constants shaped like
// credentials: they go into the key as a digest, never as text (CHAOS-6735).
type scenarioView struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
	NoPG bool     `json:"noPG"`
	Orgs []dbOrg  `json:"orgs"`
	Cred string   `json:"credentialsAndEnvSHA256"`
}

func dbScenarioViews(scenarios []dbScenario) []scenarioView {
	views := make([]scenarioView, len(scenarios))
	for index, scenario := range scenarios {
		raw, _ := json.Marshal(map[string]any{"creds": scenario.Creds, "env": scenario.Env})
		sum := sha256.Sum256(raw)
		views[index] = scenarioView{Name: scenario.Name, Args: scenario.Args, NoPG: scenario.NoPG, Orgs: scenario.Orgs, Cred: hex.EncodeToString(sum[:])}
	}
	return views
}

// TestDBLookupsMatchFrozenPython compares the two Postgres reads of `dho sync <target>` (the first
// organization and the stored GitHub credential) with what the REAL dev-hops producers answered over every
// scenario (database URL forms included), against a database each side seeded the same way. The answers were
// executed once on dbLookupPythonBuild and are frozen in testdata/golden/db_lookup.json (the recipe regenerates
// them by execution). The key names the scenarios by a digest of their credential payloads and environment
// values (test constants shaped like credentials); an answer holds a digest and a length in place of a token
// or a private key.
func TestDBLookupsMatchFrozenPython(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/db_lookup.json",
		PythonBuild: dbLookupPythonBuild,
		SHA256:      "14e80709091558deb9714f6bcda0b63a034a6ac988b88ec0141b29608d6237e6",
		Recipe: "git worktree add --detach $DIR " + dbLookupPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/synccli/ -test '^TestDBLookupsMatchFrozenPython$' -python-root $DIR",
	})
	root := golden.PythonRoot(t, repoRoot)

	scenarios := dbScenarios(dbKeyPlaceholder)
	keyBytes, err := json.Marshal(dbScenarioViews(scenarios))
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("db lookup scenarios", dbLookupOracleProgram, keyBytes, dbLookupPythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(root string, _ []venueoracle.Request) []venueoracle.Response {
		produced := produceDBLookupAnswers(ctx, t, root, scenarios)
		body, err := json.Marshal(produced)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: venueoracle.PackBody(body)}}
	})
	golden.Consumed(t, answers...)
	var frozen []map[string]any
	if err := json.Unmarshal([]byte(venueoracle.UnpackBody(t, answers[0].Body)), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(scenarios) {
		t.Fatalf("the golden answered %d scenarios, the test runs %d", len(frozen), len(scenarios))
	}

	pool, baseURL := startDBLookupPostgres(ctx, t)
	keyFile := filepath.Join(t.TempDir(), "app-key.pem")
	if err := os.WriteFile(keyFile, []byte("-----BEGIN KEY-----\nfrom-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	stages := map[string]int{}
	mismatches := 0
	for index, scenario := range scenarios {
		seedDBLookup(ctx, t, pool, scenario)
		dbURL := baseURL
		if scenario.DBURL != nil {
			dbURL = scenario.DBURL(baseURL)
		}
		env := merge(scenario.Env)
		args := append([]string{"git", "--provider", "github", "--owner", "a", "--repo", "b",
			"--analytics-db", "clickhouse://ch:ch@localhost:8123/default"}, scenario.Args...)
		for i, arg := range args {
			args[i] = strings.ReplaceAll(arg, dbKeyPlaceholder, keyFile)
		}
		for name, value := range env {
			env[name] = strings.ReplaceAll(value, dbKeyPlaceholder, keyFile)
		}
		switch {
		case scenario.NoPG:
		case scenario.Name == "--db flag":
			args = append(args, "--db", dbURL)
		default:
			env["POSTGRES_URI"] = dbURL
		}
		env["CLICKHOUSE_URI"] = "clickhouse://ch:ch@localhost:8123/default"
		want := frozen[index]
		got := goDBResult(ctx, args, env)
		if _, refusedAtParse := got["parse"]; refusedAtParse {
			delete(got, "parse")
			delete(want, "org") // Python resolved it; Go refused before any lookup
		}
		gotRedacted, _ := redactDBSecrets(mustGeneric(got)).(map[string]any)
		gotJSON, _ := json.Marshal(gotRedacted)
		wantJSON, _ := json.Marshal(want)
		stage, _ := want["stage"].(map[string]any)
		stages[fmt.Sprint(stage["v"])]++
		if canonical(gotJSON) != canonical(wantJSON) {
			mismatches++
			t.Errorf("%s: %s", scenario.Name, diffLeaves(gotRedacted, want))
		}
	}
	names := make([]string, 0, len(stages))
	for name := range stages {
		names = append(names, name)
	}
	sort.Strings(names)
	summary := make([]string, 0, len(names))
	for _, name := range names {
		summary = append(summary, fmt.Sprintf("%s=%d", name, stages[name]))
	}
	t.Logf("scenarios compared (%s), %d mismatches", strings.Join(summary, " "), mismatches)
	for _, stage := range []string{"ok", "exit", "error"} {
		if stages[stage] == 0 {
			t.Fatalf("no scenario ended in stage %q: the corpus does not reach it", stage)
		}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// redactDBSecrets replaces the value of every "token" and "private_key" field by a digest and a length.
func redactDBSecrets(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, child := range typed {
			if key == "token" || key == "private_key" {
				if leaf, ok := child.(map[string]any); ok && leaf["t"] == "str" {
					text, _ := leaf["v"].(string)
					sum := sha256.Sum256([]byte(text))
					out[key] = map[string]any{"t": "str", "v": fmt.Sprintf("<sha256:%s len:%d>", hex.EncodeToString(sum[:6]), len(text))}
					continue
				}
			}
			out[key] = redactDBSecrets(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, child := range typed {
			out[index] = redactDBSecrets(child)
		}
		return out
	}
	return value
}

// produceDBLookupAnswers runs the REAL Python producers over the scenarios against a database the Python side
// migrates and seeds itself (core.encryption's own ciphertext), and returns one redacted answer per scenario.
func produceDBLookupAnswers(ctx context.Context, t *testing.T, root string, scenarios []dbScenario) []any {
	t.Helper()
	python := pyoracle.Resolve(t, root)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	upgrade := exec.Command(python, "-c", upgradeProgram)
	// The database URL goes by environment, never argv (a process listing shows argv).
	upgrade.Env = append(dbLookupPythonEnv(root), "DHO_ORACLE_DB_URL="+instance.URI)
	if output, err := upgrade.CombinedOutput(); err != nil {
		t.Fatalf("python upgrade: %v", pyoracle.RunError(python, err, output))
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.RawQuery, parsed.Scheme = "", "postgresql+asyncpg"
	baseURL := parsed.String()

	keyFile := filepath.Join(t.TempDir(), "app-key.pem")
	if err := os.WriteFile(keyFile, []byte("-----BEGIN KEY-----\nfrom-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command(python, "-c", dbLookupOracleProgram)
	command.Env = append(dbLookupPythonEnv(root), "DHO_ORACLE_DB_URL="+baseURL)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdin.Close(); _ = command.Wait() })
	reader := bufio.NewReaderSize(stdout, 1<<20)
	ask := func(request map[string]any) map[string]any {
		raw, _ := json.Marshal(request)
		if _, err := io.WriteString(stdin, string(raw)+"\n"); err != nil {
			t.Fatalf("python stdin: %v", err)
		}
		line, err := reader.ReadBytes('\n')
		if err != nil {
			t.Fatalf("python answer: %v\n%s", err, stderr.String())
		}
		var answer map[string]any
		if err := json.Unmarshal(line, &answer); err != nil {
			t.Fatalf("python answer %q: %v", line, err)
		}
		return answer
	}
	produced := make([]any, 0, len(scenarios))
	for _, scenario := range scenarios {
		// The producer reads the key file by its real path: swap the placeholder in what it is sent.
		creds := scenario.Creds
		ask(map[string]any{"op": "seed", "orgs": scenario.Orgs, "creds": creds})
		dbURL := baseURL
		if scenario.DBURL != nil {
			dbURL = scenario.DBURL(baseURL)
		}
		env := merge(scenario.Env)
		args := append([]string{"git", "--provider", "github", "--owner", "a", "--repo", "b",
			"--analytics-db", "clickhouse://ch:ch@localhost:8123/default"}, scenario.Args...)
		for i, arg := range args {
			args[i] = strings.ReplaceAll(arg, dbKeyPlaceholder, keyFile)
		}
		for name, value := range env {
			env[name] = strings.ReplaceAll(value, dbKeyPlaceholder, keyFile)
		}
		switch {
		case scenario.NoPG:
		case scenario.Name == "--db flag":
			args = append(args, "--db", dbURL)
		default:
			env["POSTGRES_URI"] = dbURL
		}
		env["CLICKHOUSE_URI"] = "clickhouse://ch:ch@localhost:8123/default"
		answer := ask(map[string]any{"op": "resolve", "args": args, "env": env})
		raw, _ := json.Marshal(answer)
		produced = append(produced, redactDBSecrets(mustGenericAny(raw)))
	}
	return produced
}

func mustGenericAny(raw []byte) any {
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}

// goDBResult runs BuildPlan and the executor's lookups the way the verb does
// and renders the outcome in the shape the Python oracle prints.
func goDBResult(ctx context.Context, args []string, env map[string]string) map[string]any {
	lookup := func(name string) (string, bool) { v, ok := env[name]; return v, ok }
	plan, _, refusal := BuildPlan(args[0], args[1:], Inputs{Lookup: lookup, Now: time.Now})
	if refusal != nil {
		return map[string]any{"parse": true, "stage": tStr("exit"), "message": tStr(refusal.Message)}
	}
	plan, refusal = resolveDBLookups(ctx, defaultDBLookups(), plan, cli.Env{Lookup: lookup})
	result := map[string]any{"org": tOptStr(plan.Org)}
	switch {
	case refusal != nil && refusal.Stage == "error":
		result["stage"], result["type"] = tStr("error"), tStr(refusal.Type)
		return result
	case refusal != nil:
		result["stage"], result["message"] = tStr("exit"), tStr(refusal.Message)
		return result
	}
	creds := plan.GitHub
	opt := func(s string) typed {
		if s == "" {
			return tNull()
		}
		return tStr(s)
	}
	result["stage"], result["mode"], result["name"] = tStr("ok"), tStr(creds.Mode), tStr(creds.Name)
	result["token"], result["app_id"] = opt(creds.Token), opt(creds.AppID)
	result["private_key"], result["installation_id"] = opt(creds.PrivateKey), opt(creds.InstallationID)
	if creds.BaseURL != nil {
		result["base_url"] = opt(*creds.BaseURL)
	} else {
		result["base_url"] = tNull()
	}
	return result
}

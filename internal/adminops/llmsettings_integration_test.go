//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// `admin llm-settings get|set|delete` are compared with the real Python verbs on
// one scripted session over real PostgreSQL: every step's exit code and stdout,
// and the settings rows left after it (an encrypted value read back through the
// Go decryptor, which proves the two sides seal and open each other's values).

const (
	teamOrg      = "aaaaaaaa-0000-4000-8000-00000000000a"
	communityOrg = "bbbbbbbb-0000-4000-8000-00000000000b"
	llmKeySecret = "oracle-settings-key-not-real"
	llmSalt      = "oracle-salt"
)

type llmStep struct {
	args []string
	// sql runs before the step (seeding), through the test's own connection.
	sql string
	// noKey runs the step without SETTINGS_ENCRYPTION_KEY.
	noKey bool
}

func llm(verb string, org string, rest ...string) llmStep {
	args := append([]string{"llm-settings", verb, "--org", org}, rest...)
	return llmStep{args: args}
}

// The API keys are built at run time so no source or golden carries a token-shaped literal.
var (
	apiKeyLong  = "sk-" + "oracle-" + "0123456789abcdef"
	apiKeyShort = "abc" + "123"
	apiKeyUni   = "ключ-" + "0123456789"
)

var llmScript = []llmStep{
	{args: []string{"orgs", "list"}, sql: fmt.Sprintf(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at) VALUES
('%s', 'team-org', 'Team', 'team', 'manual', true, now(), now()), ('%s', 'free-org', 'Free', 'community', 'stripe', true, now(), now())`, teamOrg, communityOrg)},
	llm("get", communityOrg),
	llm("set", communityOrg, "--provider", "openai"),
	llm("delete", communityOrg),
	llm("get", "cccccccc-0000-4000-8000-00000000000c"),
	llm("get", "not-a-uuid"),
	llm("get", ""),
	llm("set", "", "--provider", "openai"),
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "  OpenAI "),
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "   "),
	llm("set", teamOrg, "--provider", "anthropic", "--model", "claude-x", "--api-key", apiKeyLong),
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "anthropic", "--api-key", apiKeyShort),
	llm("set", teamOrg, "--provider", "Ünï", "--model", "modèle-日本-😀", "--api-key", apiKeyUni),
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "openai", "--base-url", "http://169.254.169.254/latest"),
	llm("set", teamOrg, "--provider", "openai", "--base-url", "not a url"),
	llm("set", teamOrg, "--provider", "openai", "--base-url", "http://[::1"),
	llm("set", teamOrg, "--provider", "openai", "--base-url", "https://api.openai.com/v1"),
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "openai", "--model", "m", "--base-url", ""),
	{args: []string{"llm-settings", "set", "--org", teamOrg, "--provider", "openai", "--api-key", apiKeyLong}, noKey: true},
	llm("delete", teamOrg),
	llm("delete", teamOrg),
	llm("get", teamOrg),
	{args: []string{"orgs", "list"}, sql: `INSERT INTO feature_flags (id, key, name, description, category, min_tier, is_enabled, is_beta, is_deprecated, config_schema, created_at, updated_at)
VALUES (gen_random_uuid(), 'byo_llm', 'BYO LLM', 'd', 'llm', 'team', false, false, false, 'null', now(), now())`},
	llm("get", teamOrg),
	llm("set", teamOrg, "--provider", "openai"),
	llm("delete", teamOrg),
	{args: []string{"orgs", "list"}, sql: `UPDATE organizations SET tier = 'enterprise' WHERE id = '` + communityOrg + `'`},
	llm("get", communityOrg),
}

type llmResult struct {
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	State  string   `json:"state"`
}

// shownArgs is the step's command line with the API keys named, not written out.
func shownArgs(args []string) []string {
	names := map[string]string{apiKeyLong: "<api-key:long>", apiKeyShort: "<api-key:short>", apiKeyUni: "<api-key:unicode>"}
	out := make([]string, len(args))
	for index, arg := range args {
		if name, ok := names[arg]; ok {
			arg = name
		}
		out[index] = arg
	}
	return out
}

func (db *database) llmState(t *testing.T) string {
	t.Helper()
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmKeySecret), llmSalt)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.conn.Query(context.Background(), `SELECT org_id, category, key, value, is_encrypted, description FROM settings ORDER BY org_id, category, key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var org, category, key string
		var value, description *string
		var encrypted bool
		if err := rows.Scan(&org, &category, &key, &value, &encrypted, &description); err != nil {
			t.Fatal(err)
		}
		shown := "<null>"
		if value != nil {
			shown = *value
			if encrypted && shown != "" {
				plain, err := decryptor.Decrypt(secrets.NewValue(shown))
				if err != nil {
					t.Fatalf("%s/%s: the stored value does not decrypt: %v", org, key, err)
				}
				digest := sha256.Sum256(plain)
				shown = "enc:sha256:" + hex.EncodeToString(digest[:6])
			}
		}
		desc := "<null>"
		if description != nil {
			desc = *description
		}
		out = append(out, fmt.Sprintf("%s|%s|%s|%s|%v|%s", org, category, key, shown, encrypted, desc))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(out)
	return string(raw)
}

func llmEnv(noKey bool) map[string]string {
	env := map[string]string{"SETTINGS_ENCRYPTION_SALT": llmSalt}
	if !noKey {
		env["SETTINGS_ENCRYPTION_KEY"] = llmKeySecret
	}
	return env
}

func llmSession(t *testing.T, python bool) []llmResult {
	t.Helper()
	db := startDatabase(t)
	db.reset(t)
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE settings, feature_flags CASCADE"); err != nil {
		t.Fatal(err)
	}
	var out []llmResult
	for _, s := range llmScript {
		if s.sql != "" {
			if _, err := db.conn.Exec(context.Background(), s.sql); err != nil {
				t.Fatalf("seed: %v\n%s", err, s.sql)
			}
			if s.args[0] == "orgs" {
				// A seeding step prints nothing and is not a verb run.
				out = append(out, llmResult{Args: []string{"<seed>"}, State: db.llmState(t)})
				continue
			}
		}
		var code int
		var stdout string
		if python {
			var global, rest []string
			for index := 0; index < len(s.args); index++ {
				if s.args[index] == "--org" && index+1 < len(s.args) {
					global = []string{"--org", s.args[index+1]}
					index++
					continue
				}
				rest = append(rest, s.args[index])
			}
			code, stdout = pythonVerbFull(t, db, llmEnv(s.noKey), global, rest)
		} else {
			code, stdout = goVerbEnv(t, db, llmEnv(s.noKey), s.args)
		}
		out = append(out, llmResult{Args: shownArgs(s.args), Exit: code, Stdout: stdout, State: db.llmState(t)})
	}
	return out
}

func compareLLM(t *testing.T, got, want []llmResult, wantName string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d steps, %s has %d", len(got), wantName, len(want))
	}
	for index := range got {
		label := strings.Join(got[index].Args, " ")
		if got[index].Exit != want[index].Exit {
			t.Errorf("step %d (%s): exit %d, %s exit %d", index, label, got[index].Exit, wantName, want[index].Exit)
		}
		if got[index].Stdout != want[index].Stdout {
			t.Errorf("step %d (%s): stdout\n%s\n%s stdout\n%s", index, label, got[index].Stdout, wantName, want[index].Stdout)
		}
		if got[index].State != want[index].State {
			t.Errorf("step %d (%s): rows\n%s\n%s rows\n%s", index, label, got[index].State, wantName, want[index].State)
		}
	}
}

// TestLLMSettingsMatchTheFrozenPythonOutput runs the script and compares every step (exit, stdout, the
// settings rows left) with what the REAL `dev-hops admin llm-settings` verbs did. The answers were executed
// once on adminPythonBuild and are frozen in testdata/golden/llmsettings.json (the recipe regenerates them by
// execution); the script is part of the golden's key (the API keys by name and digest, never by value).
func TestLLMSettingsMatchTheFrozenPythonOutput(t *testing.T) {
	golden, root := adminGolden(t, "llmsettings", "22b5e4dda3bf3b7639b31eb6f0e606b305f7067647a948e128869eadcaae8742", "TestLLMSettingsMatchTheFrozenPythonOutput")
	digest := func(text string) string {
		sum := sha256.Sum256([]byte(text))
		return hex.EncodeToString(sum[:8])
	}
	script := make([]map[string]any, len(llmScript))
	for index, s := range llmScript {
		script[index] = map[string]any{"args": shownArgs(s.args), "sql": s.sql, "noKey": s.noKey}
	}
	input, err := json.Marshal(map[string]any{
		"script":    script,
		"apiKeys":   map[string]string{"long": digest(apiKeyLong), "short": digest(apiKeyShort), "unicode": digest(apiKeyUni)},
		"keySecret": digest(llmKeySecret), "salt": llmSalt,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw := adminProduce(t, golden, root, "llm-settings script", input, func() any { return llmSession(t, true) })
	var frozen []llmResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	compareLLM(t, llmSession(t, false), frozen, "frozen Python")
	// A script that stored nothing, or refused nothing, passes for any implementation.
	stored, refused := 0, 0
	for _, item := range frozen {
		if strings.Contains(item.State, "enc:") {
			stored++
		}
		if strings.HasPrefix(item.Stdout, "Error: ") {
			refused++
		}
	}
	if stored < 3 || refused < 8 {
		t.Fatalf("the golden has %d steps with a stored secret and %d refusals: it measures too little", stored, refused)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

// A refusal Python prints can carry text the operator typed: an unparsable base
// URL's diagnostic names its port. The API key given on the same command line
// (and the credentials of the base URL) must never reach the output, even when
// the operator typed the same text in both places (the review round's probe).
func TestLLMSettingsRefusalDoesNotPrintTheAPIKey(t *testing.T) {
	db := startDatabase(t)
	db.reset(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE settings, feature_flags CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ('%s', 'team-org', 'Team', 'team', 'manual', true, now(), now())`, teamOrg)); err != nil {
		t.Fatal(err)
	}
	canary := "sk-" + "canary-" + "0123456789abcdef"
	password := "pa55" + "w0rd" + "canary"
	for name, args := range map[string][]string{
		"the key typed as the port": {"llm-settings", "set", "--org", teamOrg, "--provider", "openai", "--api-key", canary, "--base-url", "https://host:" + canary + "/v1"},
		"a password in the URL":     {"llm-settings", "set", "--org", teamOrg, "--provider", "openai", "--api-key", "other-key-value", "--base-url", "https://user:" + password + "@host:" + password + "/v1"},
	} {
		code, stdout := goVerbEnv(t, db, llmEnv(false), args)
		if code != 1 || !strings.HasPrefix(stdout, "Error: ") {
			t.Fatalf("%s: exit %d, stdout %q: the refusal did not happen, the probe measures nothing", name, code, stdout)
		}
		for _, secret := range []string{canary, password} {
			if strings.Contains(stdout, secret) {
				t.Errorf("%s: stdout carries a credential: %q", name, stdout)
			}
		}
	}
}

// TestLLMSettingsDeleteClearsDerivedRows is CHAOS-6975 on the operator path:
// `admin llm-settings delete` must remove the readiness record and the per-role
// certification rows with the credentials, and leave every other row (the
// platform role key, llm_budget, other categories, other orgs) alone. Go-only:
// the Python verb left the derived rows behind, which is the defect.
func TestLLMSettingsDeleteClearsDerivedRows(t *testing.T) {
	db := startDatabase(t)
	db.reset(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE settings, feature_flags CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.conn.Exec(ctx, fmt.Sprintf(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ('%s', 'team-org', 'Team', 'team', 'manual', true, now(), now()), ('%s', 'other-org', 'Other', 'team', 'manual', true, now(), now())`, teamOrg, otherLLMOrg)); err != nil {
		t.Fatal(err)
	}
	type cell struct {
		category, key string
		gone          bool
	}
	cells := []cell{
		{"llm", "provider", true}, {"llm", "model", true}, {"llm", "api_key", true},
		{"llm", "base_url", true}, {"llm", "concurrency", true},
		{"llm", "ask_dev_agent_readiness", true},
		{"llm", "ask_dev_role_certification_profile:legacy_agent", true},
		{"llm", "ask_dev_role_certification_profile:intent_classification", true},
		{"llm", "platform_ask_dev_role_certification_profile:legacy_agent", false},
		{"llm", "ask_dev_role_certification_profile", false},
		{"llm", "unrelated_key", false},
		{"llm_budget", "limit_micro_usd", false},
		{"general", "site_name", false},
		{"ask_dev", "ask_dev_agent_readiness", false},
	}
	for _, org := range []string{teamOrg, otherLLMOrg} {
		for _, c := range cells {
			if _, err := db.conn.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, $3, 'v', false, NULL, now(), now())`, org, c.category, c.key); err != nil {
				t.Fatal(err)
			}
		}
	}
	exists := func(org, category, key string) bool {
		var n int
		if err := db.conn.QueryRow(ctx, `SELECT count(*) FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`, org, category, key).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	code, stdout := goVerbEnv(t, db, llmEnv(false), []string{"llm-settings", "delete", "--org", teamOrg})
	if code != 0 {
		t.Fatalf("delete: exit %d, stdout %q", code, stdout)
	}
	for _, c := range cells {
		if got := exists(teamOrg, c.category, c.key); got == c.gone {
			t.Errorf("%s/%s: present=%v after delete, want present=%v", c.category, c.key, got, !c.gone)
		}
		if !exists(otherLLMOrg, c.category, c.key) {
			t.Errorf("the other org lost %s/%s", c.category, c.key)
		}
	}
	// Orphan derived rows with no credential: still a refusal, and cleared.
	if code, _ := goVerbEnv(t, db, llmEnv(false), []string{"llm-settings", "delete", "--org", teamOrg}); code != 1 {
		t.Errorf("second delete: exit %d, want 1 (LLM settings not found)", code)
	}
	if _, err := db.conn.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES (gen_random_uuid(), $1, 'llm', 'ask_dev_agent_readiness', 'v', false, now(), now())`, teamOrg); err != nil {
		t.Fatal(err)
	}
	if code, _ := goVerbEnv(t, db, llmEnv(false), []string{"llm-settings", "delete", "--org", teamOrg}); code != 1 {
		t.Errorf("orphan delete: exit %d, want 1", code)
	}
	if exists(teamOrg, "llm", "ask_dev_agent_readiness") {
		t.Errorf("the orphan readiness row survived the delete")
	}
}

const otherLLMOrg = "00000000-0000-4000-8000-0000000000aa"

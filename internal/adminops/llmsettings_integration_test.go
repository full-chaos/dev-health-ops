//go:build integration

package adminops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
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

const llmGolden = "testdata/llmsettings_golden.json"

// llmGoldenSHA256 pins testdata/llmsettings_golden.json (R24): what the real
// `dev-hops admin llm-settings` verbs printed and left for every step. The
// producer is deleted with the Python CLI, so this is a rot guard: the file is
// only rewritten by TestLLMSettingsVenueOracleMatchesThePythonProducer with
// DHO_LLM_GOLDEN_UPDATE=1, then this digest is updated.
const llmGoldenSHA256 = "0a1da186f2dce31456ad1fd0e961a9fdd89a923e815405ffa41d82050bf0af92"

func TestLLMSettingsGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(llmGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != llmGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", llmGolden, got, llmGoldenSHA256)
	}
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

// TestLLMSettingsMatchTheFrozenPythonOutput runs the script and compares every
// step with what the real Python verbs did (frozen; no Python needed).
func TestLLMSettingsMatchTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(llmGolden)
	if err != nil {
		t.Fatal(err)
	}
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
}

// TestLLMSettingsVenueOracleMatchesThePythonProducer runs the script through the
// real Python verbs and through dho. With DHO_LLM_GOLDEN_UPDATE=1 it rewrites the
// frozen file.
func TestLLMSettingsVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	py := llmSession(t, true)
	got := llmSession(t, false)
	compareLLM(t, got, py, "python")
	if os.Getenv("DHO_LLM_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(llmGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

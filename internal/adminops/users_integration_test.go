//go:build integration

package adminops

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The users|orgs verbs are compared with the real `dev-hops admin users|orgs`
// verbs on one scripted session: every step's exit code and stdout, and the
// rows the session has left after each step, must agree. Random values (ids,
// the slug suffix of a collision, the password salt) are masked; a password is
// compared by which known password its hash verifies.

type step struct {
	args []string
	// seedTokenFor inserts a live refresh token for the user with this email
	// before the step runs (a password change must revoke it).
	seedTokenFor string
}

func verb(args ...string) step { return step{args: args} }

// A placeholder {{org:slug}} or {{user:email}} in an argument is replaced by the
// id the run holds for it.
var usersScript = []step{
	verb("users", "create", "--email", "alice@example.com", "--password", "password1", "--username", "alice", "--full-name", "Alice A", "--superuser"),
	verb("users", "create", "--email", "alice@example.com", "--password", "password1"),
	verb("users", "create", "--email", "other@example.com", "--password", "password1", "--username", "alice"),
	verb("users", "create", "--email", "short@example.com", "--password", "seven77"),
	verb("users", "create", "--email", "bob@example.com", "--password", "password2"),
	verb("users", "create", "--email", "carol@example.com", "--password", "pässwörd"),
	verb("users", "create", "--email", "long@example.com", "--password", strings.Repeat("a", 73)),
	verb("users", "create", "--email", "Upper@Example.COM", "--password", "password3", "--username", ""),
	verb("users", "create", "--email", "upper@example.com", "--password", "password3"),
	verb("users", "create", "--password", "password3"),
	// Python's str.lower() and str.strip() (not Go's): U+0130 lowers to two code
	// points, and \x1c..\x1f are whitespace to Python only.
	verb("users", "create", "--email", "\u0130-Review@Example.com", "--password", "password3"),
	verb("users", "create", "--email", "\u0130-Review@Example.com", "--password", "password3"),
	verb("users", "create", "--email", "i\u0307-review@example.com", "--password", "password3"),
	verb("users", "create", "--email", "\x1cpadded@example.com\x1f", "--password", "password3", "--username", "\x1e\u0130User\x1c"),
	verb("users", "create", "--email", "other-padded@example.com", "--password", "password3", "--username", "i\u0307user"),
	verb("users", "list"),
	verb("users", "list", "--limit", "2"),
	verb("orgs", "create", "--name", "Acme Inc", "--owner-email", "alice@example.com"),
	verb("orgs", "create", "--name", "\x1c"),
	verb("orgs", "create", "--name", "\x1f\x1eTrimmed \u2003Name\x1c", "--slug", "trimmed"),
	verb("orgs", "create", "--name", "\x1c\u0130stanbul\x1f"),
	verb("orgs", "create", "--name", "Acme Inc"),
	verb("orgs", "create", "--name", "Acme Inc", "--slug", "acme-inc"),
	verb("orgs", "create", "--name", "   "),
	verb("orgs", "create", "--name", strings.Repeat("n", 101)),
	verb("orgs", "create", "--name", strings.Repeat("n", 100), "--slug", "hundred"),
	verb("orgs", "create", "--name", "Ünïcode Wörks & Co!", "--tier", "pro", "--description", "d"),
	verb("orgs", "create", "--name", "Ghost Owner", "--owner-email", "missing@example.com"),
	verb("orgs", "create", "--name", "Owned By Bob", "--owner-email", "bob@example.com", "--tier", "enterprise"),
	verb("orgs", "list"),
	verb("orgs", "list", "--limit", "2"),
	verb("users", "update"),
	verb("users", "update", "--email", "alice@example.com", "--role", "admin"),
	verb("users", "update", "--full-name", "Nobody"),
	verb("users", "update", "--email", "nobody@example.com", "--full-name", "Nobody"),
	verb("users", "update", "--id", "{{user:alice@example.com}}", "--full-name", "Alice Again", "--new-username", "alice2"),
	verb("users", "update", "--username", "alice2", "--new-username", ""),
	verb("users", "update", "--username", "alice2", "--full-name", "x"),
	verb("users", "update", "--email", "alice@example.com", "--new-username", "alice"),
	verb("users", "update", "--email", "bob@example.com", "--new-email", "  Bobby@Example.com ", "--no-verified", "--no-active"),
	verb("users", "list"),
	verb("users", "list", "--include-inactive"),
	verb("users", "update", "--email", "bobby@example.com", "--active", "--verified", "--no-superuser"),
	verb("users", "update", "--email", "bobby@example.com", "--new-email", "\x1c\u0130-Bobby@Example.com\x1f", "--new-username", "\x1e\u0130Bob\x1c"),
	verb("users", "update", "--email", "i\u0307-bobby@example.com", "--new-email", "\u0130-Review@Example.com"),
	verb("users", "update", "--email", "i\u0307-bobby@example.com", "--new-email", "bobby@example.com", "--new-username", ""),
	verb("users", "update", "--email", "bobby@example.com", "--new-email", "alice@example.com", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "bobby@example.com", "--new-username", "alice", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "bobby@example.com", "--full-name", "Half", "--password", "seven77"),
	{args: []string{"users", "update", "--email", "bobby@example.com", "--password", "newpass99"}, seedTokenFor: "bobby@example.com"},
	verb("users", "update", "--email", "bobby@example.com", "--password", strings.Repeat("b", 73)),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--role", "viewer"),
	verb("users", "update", "--email", "carol@example.com", "--org", "{{org:acme-inc}}", "--role", "admin"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "no-such-org", "--full-name", "Rolled Back"),
	verb("users", "update", "--email", "carol@example.com", "--org", "00000000-0000-4000-8000-000000000000"),
	verb("users", "update", "--email", "carol@example.com", "--org", "{{org:owned-by-bob}}", "--role", "member"),
	verb("users", "update", "--email", "bobby@example.com", "--remove-from-org", "owned-by-bob"),
	verb("users", "update", "--email", "alice@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--remove-from-org", "no-such-org"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--remove-from-org", "acme-inc"),
	verb("users", "update", "--email", "carol@example.com", "--org", "acme-inc", "--role", "root"),
	verb("users", "update", "--id", "not-a-uuid", "--full-name", "x"),
	verb("orgs", "list", "--include-inactive"),
	verb("users", "list", "--limit", "0"),
}

// stepResult is what one step did and the state it left.
type stepResult struct {
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	State  string   `json:"state"`
}

type database struct {
	uri  string
	conn *pgx.Conn
	seq  int
}

func startDatabase(t *testing.T) *database {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgx.Connect(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_admin_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	uri := parsed.String()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	return &database{uri: uri, conn: conn}
}

func (db *database) reset(t *testing.T) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE users, organizations, memberships, refresh_tokens CASCADE"); err != nil {
		t.Fatal(err)
	}
}

var placeholder = regexp.MustCompile(`\{\{(org|user):([^}]+)\}\}`)

func (db *database) resolve(t *testing.T, args []string) []string {
	t.Helper()
	out := make([]string, len(args))
	for index, arg := range args {
		out[index] = placeholder.ReplaceAllStringFunc(arg, func(match string) string {
			parts := placeholder.FindStringSubmatch(match)
			query := "SELECT id::text FROM organizations WHERE slug = $1"
			if parts[1] == "user" {
				query = "SELECT id::text FROM users WHERE email = $1"
			}
			var id string
			if err := db.conn.QueryRow(context.Background(), query, parts[2]).Scan(&id); err != nil {
				t.Fatalf("resolve %s: %v", match, err)
			}
			return id
		})
	}
	return out
}

func (db *database) seedRefreshToken(t *testing.T, email string) {
	t.Helper()
	ctx := context.Background()
	var userID string
	if err := db.conn.QueryRow(ctx, "SELECT id::text FROM users WHERE email = $1", email).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	db.seq++
	if _, err := db.conn.Exec(ctx, `INSERT INTO refresh_tokens (id, user_id, family_id, token_hash, expires_at, created_at) VALUES ($1, $2, $3, $4, now() + interval '1 day', now())`,
		fmt.Sprintf("00000000-0000-4000-8000-%012d", db.seq), userID, fmt.Sprintf("00000001-0000-4000-8000-%012d", db.seq), fmt.Sprintf("hash-%d", db.seq)); err != nil {
		t.Fatalf("seed refresh token: %v", err)
	}
}

var knownPasswords = []string{"password1", "password2", "password3", "pässwörd", "newpass99"}

// verified remembers which known password a hash verifies: a hash is
// checked once, not once per step (bcrypt at cost 12 is slow).
var verified = map[string]string{}

func whichPassword(hash string) string {
	if matched, ok := verified[hash]; ok {
		return matched
	}
	matched := "none"
	for _, candidate := range knownPasswords {
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(candidate)) == nil {
			matched = candidate
			break
		}
	}
	verified[hash] = matched
	return matched
}

// state is the rows the session left, in a form two implementations agree on.
func (db *database) state(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	query := func(sql string) []map[string]any {
		rows, err := db.conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []map[string]any
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var row map[string]any
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	users := query(`SELECT to_jsonb(u) - 'id' - 'created_at' - 'updated_at' FROM users u ORDER BY email`)
	for _, row := range users {
		hash, _ := row["password_hash"].(string)
		matched := "none"
		if hash != "" {
			matched = whichPassword(hash)
			row["password_hash"] = fmt.Sprintf("bcrypt(%s) verifies:%s", hash[:7], matched)
		}
	}
	state := map[string]any{
		"users":         users,
		"organizations": query(`SELECT to_jsonb(o) - 'id' - 'created_at' - 'updated_at' FROM organizations o ORDER BY slug`),
		"memberships": query(`SELECT to_jsonb(m) - 'id' - 'org_id' - 'user_id' - 'created_at' - 'updated_at' - 'joined_at' || jsonb_build_object('org', o.slug, 'user', u.email)
FROM memberships m JOIN organizations o ON o.id = m.org_id JOIN users u ON u.id = m.user_id ORDER BY o.slug, u.email`),
		"refresh_tokens": query(`SELECT to_jsonb(r) - 'id' - 'user_id' - 'created_at' - 'updated_at' - 'expires_at' - 'revoked_at' || jsonb_build_object('user', u.email, 'revoked', r.revoked_at IS NOT NULL) FROM refresh_tokens r JOIN users u ON u.id = r.user_id ORDER BY u.email, r.token_hash`),
	}
	raw, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

var (
	uuidText = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	// A collision suffix is token_hex(4), after one of the slugs the script makes.
	suffixText = regexp.MustCompile(`(acme-inc|acme-inc-inc|hundred)-[0-9a-f]{8}\b`)
)

// masker numbers ids in order of first appearance, so two runs that create the
// same rows in the same order print the same text.
type masker struct{ seen map[string]int }

func (m *masker) mask(text string) string {
	text = uuidText.ReplaceAllStringFunc(text, func(id string) string {
		if m.seen == nil {
			m.seen = map[string]int{}
		}
		n, ok := m.seen[id]
		if !ok {
			n = len(m.seen) + 1
			m.seen[id] = n
		}
		return fmt.Sprintf("<id%02d>", n)
	})
	return suffixText.ReplaceAllString(text, "$1-<sfx>")
}

func goVerb(t *testing.T, db *database, args []string) (int, string) {
	return goVerbEnv(t, db, nil, args)
}

func goVerbEnv(t *testing.T, db *database, extra map[string]string, args []string) (int, string) {
	t.Helper()
	runs := map[string]func(context.Context, cli.Env) int{
		"users create": runUsersCreate, "users list": runUsersList, "users update": runUsersUpdate,
		"orgs create": runOrgsCreate, "orgs list": runOrgsList, "orgs delete": runOrgsDelete,
		"llm-settings get": runLLMGet, "llm-settings set": runLLMSet, "llm-settings delete": runLLMDelete,
		"licenses keygen": runLicensesKeygen, "licenses create": runLicensesCreate,
		"bundles create": runBundlesCreate, "bundles list": runBundlesList, "bundles assign-plan": runBundlesAssignPlan, "bundles assign-org": runBundlesAssignOrg,
		"billing seed": runBillingSeed, "billing list": runBillingList,
	}
	run, ok := runs[args[0]+" "+args[1]]
	if !ok {
		t.Fatalf("no verb %v", args)
	}
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	for key, value := range extra {
		env[key] = value
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Args: args[2:], Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String()
}

func pythonVerb(t *testing.T, db *database, args []string) (int, string) {
	return pythonVerbEnv(t, db, nil, args)
}

func pythonVerbEnv(t *testing.T, db *database, extra map[string]string, args []string) (int, string) {
	return pythonVerbFull(t, db, extra, nil, args)
}

// pythonVerbFull runs `dev-hops <global> admin <args>`: global holds the root
// flags Python takes before the subcommand (--org).
func pythonVerbFull(t *testing.T, db *database, extra map[string]string, global []string, args []string) (int, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append(append([]string{"-c", program}, global...), append([]string{"admin"}, args...)...)...)
	pyURI := strings.Replace(db.uri, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false")
	for key, value := range extra {
		command.Env = append(command.Env, key+"="+value)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	if code == 1 && strings.Contains(stderr.String(), "Traceback") {
		// A Python traceback is a crash, not a refusal: the step records it as a
		// crash so the difference is named rather than compared as text.
		return 70, "<python traceback>\n"
	}
	return code, stdout.String()
}

func (db *database) session(t *testing.T, run func(*testing.T, *database, []string) (int, string)) []stepResult {
	t.Helper()
	db.reset(t)
	db.seq = 0 // seeded token values are numbered from the session's start, so two sessions seed the same
	var m masker
	var out []stepResult
	for _, s := range usersScript {
		if s.seedTokenFor != "" {
			db.seedRefreshToken(t, s.seedTokenFor)
		}
		args := db.resolve(t, s.args)
		code, stdout := run(t, db, args)
		shown := make([]string, len(s.args))
		copy(shown, s.args)
		out = append(out, stepResult{Args: shown, Exit: code, Stdout: m.mask(stdout), State: m.mask(db.state(t))})
	}
	return out
}

const usersGolden = "testdata/users_golden.json"

// usersGoldenSHA256 pins testdata/users_golden.json (R24): what the real
// `dev-hops admin users|orgs` verbs printed and left for every step of the
// script. The producer is deleted with the Python CLI, so this is a rot guard:
// the file is only rewritten by TestUsersVenueOracleMatchesThePythonProducer
// with DHO_USERS_GOLDEN_UPDATE=1, then this digest is updated.
const usersGoldenSHA256 = "dd5788fcebf8d2edb4007392625b3a27ed7d54b1d220dad2810c561a0f98bfbb"

func TestUsersGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(usersGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != usersGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", usersGolden, got, usersGoldenSHA256)
	}
}

func compare(t *testing.T, got, want []stepResult, wantName string) {
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

// TestUsersMatchTheFrozenPythonOutput runs the script on a real PostgreSQL and
// compares every step with what the real Python verbs did (frozen; no Python
// needed).
func TestUsersMatchTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(usersGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []stepResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	db := startDatabase(t)
	got := db.session(t, goVerb)
	compare(t, got, frozen, "frozen Python")
	// A script whose every step is refused (or whose every step changes
	// nothing) passes for any implementation.
	refused, changed := 0, 0
	for _, item := range frozen {
		if item.Exit != 0 {
			refused++
		} else if strings.HasPrefix(item.Stdout, "Created") || strings.HasPrefix(item.Stdout, "Updated") {
			changed++
		}
	}
	if refused < 10 || changed < 10 {
		t.Fatalf("the golden has %d refusals and %d writes: it measures too little", refused, changed)
	}
}

// TestUsersVenueOracleMatchesThePythonProducer runs the script through the real
// Python verbs and through dho and compares every step. With
// DHO_USERS_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestUsersVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	db := startDatabase(t)
	py := db.session(t, pythonVerb)
	got := db.session(t, goVerb)
	compare(t, got, py, "python")
	if os.Getenv("DHO_USERS_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(usersGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

//go:build integration

package adminops

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The users|orgs verbs are compared with the real `dev-hops admin users|orgs`
// verbs on one scripted session: every step's exit code and stdout, and the
// rows the session has left after each step, must agree. Random values (ids,
// the slug suffix of a collision, the password salt) are masked; a password is
// compared by which known password its hash verifies.

// stepResult is what one step did and the state it left.
type stepResult struct {
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	State  string   `json:"state"`
	// Stderr is what a run said on stderr, normalized (service-credentials steps only).
	Stderr string `json:"stderr,omitempty"`
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

// verified remembers which known password a hash verifies: a hash is
// checked once, not once per step (bcrypt at cost 12 is slow).
var verified = map[string]string{}

// whichPassword names a hash by the legacy literal of the fixture it verifies,
// so the rows read the same as the frozen Python rows.
func whichPassword(hash string) string {
	if matched, ok := verified[hash]; ok {
		return matched
	}
	matched := "none"
	for _, pair := range legacyPasswords {
		if bcrypt.CompareHashAndPassword([]byte(hash), []byte(pair.fixture)) == nil {
			matched = pair.legacy
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
	users := query(`SELECT to_jsonb(u) - 'id' - 'created_at' - 'updated_at' - 'sso_provider_id' FROM users u ORDER BY email`)
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
	return goVerbRun(t, db, extra, args, "")
}

func goVerbStdin(t *testing.T, db *database, args []string, stdin string) (int, string) {
	t.Helper()
	return goVerbRun(t, db, nil, args, stdin)
}

func goVerbRun(t *testing.T, db *database, extra map[string]string, args []string, stdin string) (int, string) {
	t.Helper()
	runs := map[string]func(context.Context, cli.Env) int{
		"users create": runUsersCreate, "users list": runUsersList, "users update": runUsersUpdate,
		"orgs create": runOrgsCreate, "orgs list": runOrgsList, "orgs delete": runOrgsDelete,
		"llm-settings get": runLLMGet, "llm-settings set": runLLMSet, "llm-settings delete": runLLMDelete,
		"licenses keygen": runLicensesKeygen, "licenses create": runLicensesCreate,
		"bundles create": runBundlesCreate, "bundles list": runBundlesList, "bundles assign-plan": runBundlesAssignPlan, "bundles assign-org": runBundlesAssignOrg,
		"billing seed": runBillingSeed, "billing list": runBillingList,
		"billing pull-stripe": runBillingPullStripe, "billing sync-stripe": runBillingSyncStripe,
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
	code := run(context.Background(), cli.Env{Args: args[2:], Stdin: strings.NewReader(stdin), Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String()
}

// adminPythonBuild is the build whose Python `dev-hops admin` verbs answered the frozen sessions: a build
// that still carried the Python CLI.
const adminPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// adminGolden opens the golden of one admin session.
func adminGolden(t *testing.T, name, pin string) *venueoracle.Golden {
	t.Helper()
	return venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/" + name + ".json",
		PythonBuild: adminPythonBuild,
		SHA256:      pin,
	})
}

// adminProduce returns the frozen answers of one admin session as JSON.
func adminProduce(t *testing.T, golden *venueoracle.Golden, label string, input []byte) []byte {
	t.Helper()
	request := venueoracle.ProgramRequest(label, input)
	answers := golden.Produce(t, []venueoracle.Request{request})
	golden.Consumed(t, answers...)
	return []byte(answers[0].Body)
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

// compare checks every step against want. A step in retired is not compared on exit and stdout (Python's answer
// is retired by D6129); pin, when given, holds Go's own exit and stdout for it.
func compare(t *testing.T, got, want []stepResult, wantName string, retired map[int]bool, pin *venueoracle.GoPin) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%d steps, %s has %d", len(got), wantName, len(want))
	}
	for index := range got {
		label := strings.Join(got[index].Args, " ")
		if retired[index] {
			name := fmt.Sprintf("step %02d", index)
			pin.Check(t, name+" exit", fmt.Sprint(got[index].Exit))
			pin.Check(t, name+" stdout", got[index].Stdout)
		} else {
			if got[index].Exit != want[index].Exit {
				t.Errorf("step %d (%s): exit %d, %s exit %d", index, label, got[index].Exit, wantName, want[index].Exit)
			}
			if got[index].Stdout != want[index].Stdout {
				t.Errorf("step %d (%s): stdout\n%s\n%s stdout\n%s", index, label, got[index].Stdout, wantName, want[index].Stdout)
			}
		}
		if got[index].State != want[index].State {
			t.Errorf("step %d (%s): rows\n%s\n%s rows\n%s", index, label, got[index].State, wantName, want[index].State)
		}
	}
}

// TestUsersMatchTheFrozenPythonOutput runs the script on a real PostgreSQL and compares every step (exit,
// stdout, the rows left) with what the REAL `dev-hops admin users|orgs` verbs did. The answers were executed
// once on adminPythonBuild and are frozen in testdata/golden/users.json; the script is part of the golden's key.
func TestUsersMatchTheFrozenPythonOutput(t *testing.T) {
	golden := adminGolden(t, "users", "3e8bad8fd8e602826042b00f4e69eca20a7fce5cb57a86fd7871a1fc3663fbe5")
	raw := adminProduce(t, golden, "users script", usersGoldenInput(t))
	var frozen []stepResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	pin := venueoracle.OpenGoPin(t, venueoracle.GoPinSpec{Path: "testdata/golden/users.go-pin.json", SHA256: usersGoPinDigest, Ruling: usersPasswordRuling})
	db := startDatabase(t)
	got := db.session(t, goVerb)
	compare(t, got, frozen, "frozen Python", retiredUsersSteps, pin)
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
	golden.SkipDiff(t)
	pin.Finish(t)
	golden.Finish(t)
}

// usersPasswordRuling names the ruling behind testdata/golden/users.go-pin.json.
const usersPasswordRuling = "CHAOS-10943 D6129: operator users create/update apply the shared password policy (stricter than Python's 8 characters)"

// usersGoPinDigest is the digest of testdata/golden/users.go-pin.json.
const usersGoPinDigest = "RECORD-ME"

// A refused password prints the policy's violation and never the password, on
// the argv form and the stdin form.
func TestUsersPolicyRefusalNeverPrintsThePassword(t *testing.T) {
	db := startDatabase(t)
	db.reset(t)
	const eleven = "Aa1-xyzabcd"
	for name, args := range map[string][]string{
		"create":       {"users", "create", "--email", "eleven@example.com", "--password", eleven},
		"create stdin": {"users", "create", "--email", "eleven@example.com", "--password-stdin"},
	} {
		code, stdout := goVerbStdin(t, db, args, eleven+"\n")
		if code != 1 || !strings.Contains(stdout, "at least 12 characters") {
			t.Fatalf("%s: exit %d stdout %q: the refusal did not happen", name, code, stdout)
		}
		if strings.Contains(stdout, eleven) {
			t.Errorf("%s: stdout carries the password: %q", name, stdout)
		}
	}
}

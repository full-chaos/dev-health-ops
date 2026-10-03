//go:build integration

package maintenancecli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	orgA = "aaaaaaaa-0000-4000-8000-00000000000a"
	orgB = "bbbbbbbb-0000-4000-8000-00000000000b"
	// seedTime is where every seeded row's updated_at starts: a scrub or a
	// backfill that moves it is seen as "moved".
	seedTime = "2000-01-01 00:00:00+00"
)

// database is a fresh PostgreSQL at the head baseline.
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
	name := "dho_maint_" + hex.EncodeToString(suffix)
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
	// The seeds name only the columns a case is about; the foreign keys of the
	// rows around them are not the point.
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		t.Fatalf("relax foreign keys: %v", err)
	}
	return &database{uri: uri, conn: conn}
}

// dummy is the value the seed gives a NOT NULL column it was not told about.
func dummy(dataType string, id string) string {
	switch dataType {
	case "uuid":
		return "'" + id + "'::uuid"
	case "text", "character varying", "character":
		return "'x'"
	case "timestamp with time zone", "timestamp without time zone":
		return "'" + seedTime + "'"
	case "integer", "bigint", "smallint", "numeric", "double precision", "real":
		return "0"
	case "boolean":
		return "false"
	case "json", "jsonb":
		return "'{}'"
	case "ARRAY":
		return "'{}'"
	case "date":
		return "'2000-01-01'"
	}
	return "NULL"
}

// insert writes one row of a table: the given columns (SQL expressions), every
// other NOT NULL column without a default filled with a neutral value.
func (db *database) insert(t *testing.T, table string, values map[string]string) {
	t.Helper()
	ctx := context.Background()
	rows, err := db.conn.Query(ctx, `SELECT column_name, data_type, is_nullable, column_default IS NOT NULL, is_identity = 'YES' OR is_generated = 'ALWAYS'
FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position`, table)
	if err != nil {
		t.Fatal(err)
	}
	type column struct {
		name, dataType                  string
		nullable, hasDefault, generated bool
	}
	var columns []column
	for rows.Next() {
		var c column
		var nullable string
		if err := rows.Scan(&c.name, &c.dataType, &nullable, &c.hasDefault, &c.generated); err != nil {
			t.Fatal(err)
		}
		c.nullable = nullable == "YES"
		columns = append(columns, c)
	}
	rows.Close()
	var names, expressions []string
	for _, c := range columns {
		if c.generated {
			continue
		}
		if expression, ok := values[c.name]; ok {
			names = append(names, c.name)
			expressions = append(expressions, expression)
			continue
		}
		if !c.nullable && !c.hasDefault {
			db.seq++
			names = append(names, c.name)
			expressions = append(expressions, dummy(c.dataType, fmt.Sprintf("00000000-0000-4000-8000-%012d", db.seq)))
		}
	}
	for name := range values {
		found := false
		for _, c := range columns {
			found = found || c.name == name
		}
		if !found {
			t.Fatalf("%s has no column %s", table, name)
		}
	}
	statement := fmt.Sprintf("INSERT INTO public.%s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(expressions, ", "))
	if _, err := db.conn.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

func quote(text string) string { return "'" + strings.ReplaceAll(text, "'", "''") + "'" }

func uuidOf(kind, n int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012d", kind, n) }

// The credential shapes are built at run time so the source and the frozen
// golden carry no literal token (a secret scanner reads them as real ones).
var (
	seedBearer = "Bear" + "er abcdef0123456789abcdef"
	seedGitHub = "gh" + "p_0123456789abcdefghij0123"
	seedURL    = "https://user:" + "pa55" + "word@example.com/repo"
	seedKey    = "api" + "_key=" + "sk-" + "live-0123456789"
	seedBasic  = "Bas" + "ic dXNlcjpwYXNzd29yZA=="
	seedToken  = "tok" + "en=abc123"
)

// maskSeeds replaces each seeded credential in text with a placeholder: a
// dry run leaves them in place, and the frozen state must not carry them.
func maskSeeds(text string) string {
	for placeholder, secret := range map[string]string{
		"<seed:bearer>": seedBearer, "<seed:github>": seedGitHub, "<seed:url>": seedURL,
		"<seed:key>": seedKey, "<seed:basic>": seedBasic, "<seed:token>": seedToken,
	} {
		text = strings.ReplaceAll(text, secret, placeholder)
	}
	return text
}

// scrubTexts are the values the scrub is seeded with: a credential in each
// shape the sanitizer knows, clean text, an over-long clean text (the cap only),
// an over-long text with a credential, text with non-ASCII, and an empty string.
func scrubTexts() []string {
	long := strings.Repeat("clean text ", 400)
	return []string{
		"request failed: Authorization: " + seedBearer,
		"github said " + seedGitHub + " was rejected",
		"connect " + seedURL + " failed",
		seedKey + " was invalid",
		"clean error text",
		long,
		long + " " + seedToken,
		"\u00e9chec: " + seedBasic + " \u65e5\u672c\u8a9e",
		"",
	}
}

// seedScrub fills the eight tables: per organization, per text, one row per
// column; job_runs through scheduled_jobs; sync_configurations with JSON stats.
func (db *database) seedScrub(t *testing.T) {
	t.Helper()
	texts := scrubTexts()
	n := 0
	for orgIndex, org := range []string{orgA, orgB} {
		job := uuidOf(0x1b, orgIndex+1)
		db.insert(t, "scheduled_jobs", map[string]string{"id": quote(job) + "::uuid", "org_id": quote(org), "name": quote("job-" + org), "job_type": quote("sync")})
		for _, text := range texts {
			n++
			set := func(column string) string { return quote(text) }
			db.insert(t, "sync_run_units", map[string]string{"id": quote(uuidOf(1, n)) + "::uuid", "org_id": quote(org), "error": set("error"), "updated_at": quote(seedTime)})
			db.insert(t, "sync_runs", map[string]string{"id": quote(uuidOf(2, n)) + "::uuid", "org_id": quote(org), "error": set("error")})
			db.insert(t, "sync_run_reference_discoveries", map[string]string{"id": quote(uuidOf(3, n)) + "::uuid", "org_id": quote(org), "error": set("error"), "updated_at": quote(seedTime)})
			db.insert(t, "sync_dispatch_outbox", map[string]string{"id": quote(uuidOf(4, n)) + "::uuid", "org_id": quote(org), "last_error": set("last_error"), "updated_at": quote(seedTime)})
			db.insert(t, "job_runs", map[string]string{"id": quote(uuidOf(5, n)) + "::uuid", "job_id": quote(job) + "::uuid", "error": set("error"), "error_traceback": quote("Traceback: " + text)})
			db.insert(t, "backfill_jobs", map[string]string{"id": quote(uuidOf(6, n)) + "::uuid", "org_id": quote(org), "error_message": set("error_message"), "updated_at": quote(seedTime)})
			db.insert(t, "integration_credentials", map[string]string{"id": quote(uuidOf(7, n)) + "::uuid", "org_id": quote(org), "name": quote(fmt.Sprintf("credential-%d", n)), "last_test_error": set("last_test_error"), "updated_at": quote(seedTime)})
			// sync_configurations: the text column and a JSON stats document
			// (key order, floats, non-ASCII and nested values must survive).
			stats := fmt.Sprintf(`{"records": 12, "ratio": 1.0, "error": %s, "nested": {"a": [1, 2.50, null], "b": "é"}}`, jsonString(text))
			db.insert(t, "sync_configurations", map[string]string{"id": quote(uuidOf(8, n)) + "::uuid", "org_id": quote(org), "name": quote(fmt.Sprintf("config-%d", n)), "last_sync_error": set("last_sync_error"), "last_sync_stats": quote(stats), "updated_at": quote(seedTime)})
		}
		// JSON shapes the scrub must leave alone or skip.
		for index, document := range []string{`null`, `[1, 2]`, `"a string"`, `{"records": 1}`, `{"error": 7}`, `{"error": null}`, `{"error": "clean"}`} {
			n++
			db.insert(t, "sync_configurations", map[string]string{"id": quote(uuidOf(9, n+index)) + "::uuid", "org_id": quote(org), "name": quote(fmt.Sprintf("shape-%d", n+index)), "last_sync_stats": quote(document), "updated_at": quote(seedTime)})
		}
		// A row with every scrubbed column NULL is never selected.
		n++
		db.insert(t, "sync_runs", map[string]string{"id": quote(uuidOf(10, n)) + "::uuid", "org_id": quote(org)})
	}
}

func jsonString(text string) string {
	raw, _ := json.Marshal(text)
	return string(raw)
}

var scrubDumpTables = []struct {
	name    string
	columns []string // rendered as text; json columns via ::text
	moved   bool
}{
	{"sync_run_units", []string{"error"}, true},
	{"sync_runs", []string{"error"}, false},
	{"sync_run_reference_discoveries", []string{"error"}, true},
	{"sync_dispatch_outbox", []string{"last_error"}, true},
	{"job_runs", []string{"error", "error_traceback"}, false},
	{"backfill_jobs", []string{"error_message"}, true},
	{"sync_configurations", []string{"last_sync_error", "last_sync_stats::text"}, true},
	{"integration_credentials", []string{"last_test_error"}, true},
}

// scrubState is every scrubbed value and whether the row's updated_at moved,
// the check of what the scrub did to the database.
func (db *database) scrubState(t *testing.T) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, table := range scrubDumpTables {
		selects := []string{"id::text"}
		for _, column := range table.columns {
			selects = append(selects, "coalesce("+column+", '<NULL>')")
		}
		if table.moved {
			selects = append(selects, "(updated_at > '2001-01-01')::text")
		}
		rows, err := db.conn.Query(context.Background(), "SELECT "+strings.Join(selects, " || E'\\x1f' || ")+" FROM public."+table.name+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		for index := range lines {
			lines[index] = maskSeeds(lines[index])
		}
		out[table.name] = lines
	}
	return out
}

func (db *database) resetScrub(t *testing.T) {
	t.Helper()
	for _, table := range scrubDumpTables {
		if _, err := db.conn.Exec(context.Background(), "TRUNCATE public."+table.name+" CASCADE"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE public.scheduled_jobs CASCADE"); err != nil {
		t.Fatal(err)
	}
	db.seedScrub(t)
}

type scrubScenario struct {
	name string
	args []string
	env  map[string]string
}

var scrubScenarios = []scrubScenario{
	{name: "dry run", args: nil},
	{name: "dry run, one organization", args: []string{"--org", orgA}},
	{name: "dry run, small batches", args: []string{"--batch-size", "2"}},
	{name: "ORG_ID never scopes", args: nil, env: map[string]string{"ORG_ID": orgA}},
	{name: "an explicit empty org scopes nothing", args: []string{"--org", ""}},
	{name: "a batch size of zero is the default", args: []string{"--batch-size", "0"}},
	{name: "apply", args: []string{"--apply"}},
	{name: "apply, one organization", args: []string{"--apply", "--org", orgB}},
	{name: "apply, small batches", args: []string{"--apply", "--batch-size", "3"}},
}

type scrubResult struct {
	Name   string              `json:"name"`
	Exit   int                 `json:"exit"`
	Stdout string              `json:"stdout"`
	State  map[string][]string `json:"state"`
}

func (db *database) goScrub(t *testing.T, s scrubScenario) scrubResult {
	t.Helper()
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	for key, value := range s.env {
		env[key] = value
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runScrub(context.Background(), cli.Env{Args: s.args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	if code != 0 {
		t.Fatalf("%s: exit %d, stderr %s", s.name, code, stderr.String())
	}
	return scrubResult{Name: s.name, Exit: code, Stdout: stdout.String(), State: db.scrubState(t)}
}

// maintenancePythonBuild is the build whose Python `dev-hops maintenance` verbs answered the scenarios: a
// build that still carried the Python CLI.
const maintenancePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// maintenancePythonProgram is the entry point the producer runs: the real dev-hops CLI.
const maintenancePythonProgram = "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"

// maintenancePythonSettings are the variables that shape the producer's answers, as constants: the
// producer's environment AND part of each golden's request key (a changed value fails the frozen replay).
// ORG_ID, which the producer used to delete from the inherited environment by hand, is absent by
// construction. A scenario's own variables are in its input; the database address is a per-run value.
var maintenancePythonSettings = map[string]string{"PYTHONHASHSEED": "0", "OTEL_ENABLED": "false"}

func pythonProgram(t *testing.T, producer *venueoracle.Producer, db *database, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	pyURI := strings.Replace(db.uri, "postgres://", "postgresql://", 1)
	// A scenario's own variables are in its input (the golden's key): they are declared with the settings.
	declared := map[string]string{}
	for name, value := range maintenancePythonSettings {
		declared[name] = value
	}
	for key, value := range env {
		declared[key] = value
	}
	command, err := producer.Command(context.Background(), declared, []string{"POSTGRES_URI=" + pyURI, "DATABASE_URI=" + pyURI}, append([]string{"-c", maintenancePythonProgram, "maintenance"}, args...)...)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", pyoracle.RunError(command.Path, err, []byte(stderr.String())))
	}
	return code, stdout.String(), stderr.String()
}

// maintenanceGolden opens the golden of one maintenance session and the pinned Python root. pin is the
// literal "PIN:<name>" (the recorder looks for it in the test source until the golden exists).
func maintenanceGolden(t *testing.T, name, pin, test string) (*venueoracle.Golden, string) {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/" + name + ".json",
		PythonBuild: maintenancePythonBuild,
		SHA256:      pin,
		Recipe: "git worktree add --detach $DIR " + maintenancePythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/maintenancecli/ -test '^" + test + "$' -python-root $DIR",
	})
	return golden, golden.PythonRoot(t, repoRoot)
}

// maintenanceProduce runs one recorded session: produce is called only while recording, with the Python root
// of the pinned checkout, and returns the session's answers as JSON; the frozen run gets them from the golden.
func maintenanceProduce(t *testing.T, golden *venueoracle.Golden, root, label string, input []byte, produce func(producer *venueoracle.Producer) any) []byte {
	t.Helper()
	request := venueoracle.ProgramRequest(label, maintenancePythonProgram, input, maintenancePythonSettings)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		producer.RequireDeployed()
		body, err := json.Marshal(produce(producer))
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	golden.Consumed(t, answers...)
	return []byte(answers[0].Body)
}

func (db *database) pythonScrub(t *testing.T, producer *venueoracle.Producer, s scrubScenario) scrubResult {
	t.Helper()
	code, stdout, stderr := pythonProgram(t, producer, db, s.env, append([]string{"scrub-error-text"}, s.args...)...)
	if code != 0 {
		t.Fatalf("%s: python exit %d, stderr %s", s.name, code, stderr)
	}
	return scrubResult{Name: s.name, Exit: code, Stdout: stdout, State: db.scrubState(t)}
}

func canonicalState(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestScrubMatchesTheFrozenPythonOutput runs the verb on a real PostgreSQL and compares the report and every
// scrubbed value with what the REAL `dev-hops maintenance scrub-error-text` left. The answers were executed
// once on maintenancePythonBuild and are frozen in testdata/golden/scrub.json (the recipe regenerates them by
// execution); the scenarios (their arguments and variables) are part of the golden's key. The seeded error
// texts carry no literal token (a secret scanner would read it as a real one).
func TestScrubMatchesTheFrozenPythonOutput(t *testing.T) {
	golden, root := maintenanceGolden(t, "scrub", "1b14eab8972cfcb0a5077eef844f21ff94d49475de22ff9de671aa03824ec649", "TestScrubMatchesTheFrozenPythonOutput")
	keys := make([]map[string]any, len(scrubScenarios))
	for index, s := range scrubScenarios {
		keys[index] = map[string]any{"name": s.name, "args": s.args, "env": s.env}
	}
	input, err := json.Marshal(map[string]any{"scenarios": keys, "seededTexts": scrubTexts()})
	if err != nil {
		t.Fatal(err)
	}
	raw := maintenanceProduce(t, golden, root, "scrub-error-text scenarios", input, func(producer *venueoracle.Producer) any {
		db := startDatabase(t)
		var produced []scrubResult
		for _, s := range scrubScenarios {
			db.resetScrub(t)
			produced = append(produced, db.pythonScrub(t, producer, s))
		}
		return produced
	})
	var frozen []scrubResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(scrubScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), len(scrubScenarios))
	}
	db := startDatabase(t)
	for index, s := range scrubScenarios {
		db.resetScrub(t)
		got := db.goScrub(t, s)
		want := frozen[index]
		if got.Stdout != want.Stdout {
			t.Errorf("%s: report\n%s\nfrozen Python\n%s", s.name, got.Stdout, want.Stdout)
		}
		if a, b := canonicalState(t, got.State), canonicalState(t, want.State); a != b {
			t.Errorf("%s: the rows differ from the frozen Python rows\ngo:     %.600s\npython: %.600s", s.name, a, b)
		}
	}
	// A comparison of two reports that changed nothing passes for any implementation.
	changed := 0
	for _, item := range frozen {
		if strings.HasPrefix(item.Name, "apply") && !strings.Contains(item.Stdout, "TOTAL                                                     0          0") {
			changed++
		}
	}
	if changed == 0 {
		t.Fatal("no frozen apply scenario changed a row: the golden measures nothing")
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

var (
	tokenLine   = regexp.MustCompile(`Deleted (\d+) expired refresh tokens`)
	cleanupLine = regexp.MustCompile(`deleted=(\d+) total=(\d+)`)
	stampedLine = regexp.MustCompile(`stamped=(\d+)`)
)

func hoursAgo(n int) string { return fmt.Sprintf("now() - interval '%d hours'", n) }

// seedTokens writes refresh tokens around the 24-hour grace: two long expired,
// one inside the grace, two live.
func (db *database) seedTokens(t *testing.T) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE public.refresh_tokens CASCADE"); err != nil {
		t.Fatal(err)
	}
	for index, expires := range []string{hoursAgo(25), hoursAgo(300), hoursAgo(23), "now() + interval '1 hour'", "now() + interval '30 days'"} {
		db.insert(t, "refresh_tokens", map[string]string{"id": quote(uuidOf(0x2b, index+1)) + "::uuid", "token_hash": quote(fmt.Sprintf("hash-%d", index)), "expires_at": expires})
	}
}

func (db *database) tokenState(t *testing.T) string {
	t.Helper()
	rows, err := db.conn.Query(context.Background(), "SELECT id::text FROM public.refresh_tokens ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(ids, ",")
}

// seedConversations writes 0-day conversations in every state the backfill
// tells apart, enough of the stranded kind to need three batches.
func (db *database) seedConversations(t *testing.T) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE public.dev_conversations CASCADE"); err != nil {
		t.Fatal(err)
	}
	insert := func(n, retention int, expires, updated string) {
		db.insert(t, "dev_conversations", map[string]string{
			"id": quote(uuidOf(0x3b, n)) + "::uuid", "org_id": quote(orgA) + "::uuid", "user_id": quote(uuidOf(0x4b, 1)) + "::uuid",
			"retention_days": fmt.Sprint(retention), "expires_at": expires, "created_at": fmt.Sprintf("'2000-01-01 00:00:00+00'::timestamptz + interval '%d seconds'", n),
			"updated_at": updated,
		})
	}
	for n := 1; n <= 1201; n++ {
		insert(n, 0, "NULL", hoursAgo(2))
	}
	insert(2001, 0, "NULL", "now()")                               // idle for less than the grace
	insert(2002, 0, "NULL", hoursAgo(1)+" + interval '5 minutes'") // just inside the grace
	insert(2003, 0, "now() + interval '1 day'", hoursAgo(5))       // already stamped
	insert(2004, 30, "NULL", hoursAgo(500))                        // a 30-day conversation
}

// conversationState is, per conversation, whether it was stamped and whether
// its updated_at moved: what the backfill did.
func (db *database) conversationState(t *testing.T) string {
	t.Helper()
	rows, err := db.conn.Query(context.Background(), `SELECT id::text || ':' || (expires_at IS NOT NULL AND expires_at > now() - interval '10 minutes' AND expires_at <= now())::text || ':' || (updated_at > now() - interval '10 minutes')::text FROM public.dev_conversations ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

type housekeepingResult struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
	State string `json:"state"`
}

type housekeepingScenario struct {
	name    string
	verb    string
	seed    func(*database, *testing.T)
	state   func(*database, *testing.T) string
	pattern *regexp.Regexp
}

var housekeepingScenarios = []housekeepingScenario{
	{"cleanup-tokens", "cleanup-tokens", (*database).seedTokens, (*database).tokenState, tokenLine},
	{"cleanup-all", "cleanup-all", (*database).seedTokens, (*database).tokenState, cleanupLine},
	{"backfill-ask-dev-ephemeral-expiry", "backfill-ask-dev-ephemeral-expiry", (*database).seedConversations, (*database).conversationState, stampedLine},
}

func (db *database) goHousekeeping(t *testing.T, s housekeepingScenario) housekeepingResult {
	t.Helper()
	var run func(context.Context, cli.Env) int
	for _, child := range Command().Children {
		if child.Name == s.verb {
			run = child.Run
		}
	}
	lookup := func(key string) (string, bool) {
		if key == "MIGRATION_DATABASE_URI" {
			return db.uri, true
		}
		return "", false
	}
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr}); code != 0 {
		t.Fatalf("%s: exit %d, stderr %s", s.name, code, stderr.String())
	}
	var result map[string]int
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("%s: stdout %q: %v", s.name, stdout.String(), err)
	}
	count := result["deleted"]
	if s.verb == "backfill-ask-dev-ephemeral-expiry" {
		count = result["stamped"]
	}
	return housekeepingResult{Name: s.name, Count: count, State: s.state(db, t)}
}

func (db *database) pythonHousekeeping(t *testing.T, producer *venueoracle.Producer, s housekeepingScenario) housekeepingResult {
	t.Helper()
	code, _, stderr := pythonProgram(t, producer, db, nil, s.verb)
	if code != 0 {
		t.Fatalf("%s: python exit %d, stderr %s", s.name, code, stderr)
	}
	match := s.pattern.FindStringSubmatch(stderr)
	if match == nil {
		t.Fatalf("%s: python logged no count matching %s:\n%s", s.name, s.pattern, stderr)
	}
	var count int
	fmt.Sscan(match[1], &count)
	return housekeepingResult{Name: s.name, Count: count, State: s.state(db, t)}
}

// TestHousekeepingMatchesTheFrozenPythonOutput compares the count and the rows left by cleanup-tokens,
// cleanup-all and backfill-ask-dev-ephemeral-expiry with what the REAL `dev-hops maintenance` verbs did. The
// answers were executed once on maintenancePythonBuild and are frozen in testdata/golden/housekeeping.json
// (the recipe regenerates them by execution); the scenarios are part of the golden's key. The seeded rows are
// relative to the database's own clock (hoursAgo), so no answer depends on the day it runs.
func TestHousekeepingMatchesTheFrozenPythonOutput(t *testing.T) {
	golden, root := maintenanceGolden(t, "housekeeping", "be53a74fa67a52feb91ef0a924b13fb101a0a95a3785fe84360080e0079825d3", "TestHousekeepingMatchesTheFrozenPythonOutput")
	keys := make([]map[string]any, len(housekeepingScenarios))
	for index, s := range housekeepingScenarios {
		keys[index] = map[string]any{"name": s.name, "verb": s.verb, "pattern": s.pattern.String()}
	}
	input, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	raw := maintenanceProduce(t, golden, root, "housekeeping scenarios", input, func(producer *venueoracle.Producer) any {
		db := startDatabase(t)
		var produced []housekeepingResult
		for _, s := range housekeepingScenarios {
			s.seed(db, t)
			produced = append(produced, db.pythonHousekeeping(t, producer, s))
		}
		return produced
	})
	var frozen []housekeepingResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(housekeepingScenarios) {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), len(housekeepingScenarios))
	}
	db := startDatabase(t)
	for index, s := range housekeepingScenarios {
		s.seed(db, t)
		got := db.goHousekeeping(t, s)
		if got != frozen[index] {
			t.Errorf("%s: dho %+v, frozen Python %+v", s.name, got, frozen[index])
		}
		if frozen[index].Count == 0 {
			t.Errorf("%s: the frozen run changed nothing: the golden measures nothing", s.name)
		}
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

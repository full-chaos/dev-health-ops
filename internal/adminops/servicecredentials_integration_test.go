//go:build integration

package adminops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The service-credentials verbs are compared with the real `dev-hops service-credentials`
// verbs on one scripted session over a real PostgreSQL at the migration head: every step's
// exit code and stdout, and the rows the session has left after each step, must agree. The
// tokens are random, so they are masked, and the rows prove them instead: a row's token_hash
// must be the SHA-256 of a token some step printed, and its token_prefix the first 16
// characters of it. Timestamps taken from the clock are compared as their distance from the
// step's start, in one-minute buckets.

const (
	credUserID = "00000000-0000-4000-8000-0000000000aa"
	missingID  = "00000000-0000-4000-8000-0000000000ff"
)

type credStep struct {
	args []string
	sql  string // run on the session's database instead of a verb
}

func cverb(args ...string) credStep { return credStep{args: args} }
func csql(sql string) credStep      { return credStep{sql: sql} }

// {{cred:SERVICE:N}} is the id of the Nth credential (by creation) of SERVICE;
// {{cred:SERVICE:N:upper|nohyphen|braces|urn}} spells that id another way.
var credScript = []credStep{
	csql("INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at) VALUES ('" + credUserID + "', 'owner@example.com', true, false, false, 0, now(), now())"),

	// create
	cverb("create", "--scope", "entitlements:read"),
	cverb("create", "--scope", "entitlements:read", "--scope", "entitlements:read"),
	cverb("create", "--scope", "nope"),
	cverb("create"),
	cverb("create", "--service", "worker-operator", "--scope", "workers:read", "--scope", "workers:operate"),
	cverb("create", "--service", "worker-operator", "--scope", "workers:operate", "--scope", "workers:read", "--scope", "workers:read"),
	cverb("create", "--service", "worker-operator", "--scope", "entitlements:read"),
	cverb("create", "--service", "acr", "--scope", "workers:read"),
	cverb("create", "--service", "acr", "--scope", "entitlements:read", "--scope", "workers:read"),
	cverb("create", "--service", "bogus", "--scope", "x"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-01-01T00:00:00+00:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-06-01T12:30:00Z"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-06-01T12:30:00+05:30"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-01-01T00:00:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2001-01-01T00:00:00+00:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "tomorrow"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", ""),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-W10-1T00:00+00:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "20990101T000000+0000"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-01-01T00:00:00.123456789+00:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-01-01 00:00:00-12:00"),
	cverb("create", "--scope", "entitlements:read", "--expires-at", "2099-01-01T24:00:00+00:00"),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", credUserID),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", "{00000000-0000-4000-8000-0000000000AA}"),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", "00000000000040008000000000000aa"),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", "00000000000040008000-0000000000aa"),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", "urn:uuid:"+credUserID),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", missingID),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", "bad"),
	cverb("create", "--scope", "entitlements:read", "--created-by-user-id", ""),
	cverb("create", "--scope", "entitlements:read", "--nope"),
	cverb("create", "--scope", "entitlements:read", "extra"),
	cverb("create", "--scope=entitlements:read"),

	// list
	cverb("list"),
	cverb("list", "--service", "worker-operator"),
	cverb("list", "--service", "bogus"),
	cverb("list", "extra"),

	// rotate
	cverb("rotate", "{{cred:acr:0}}", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:0}}", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:1}}", "--overlap-seconds", "300", "--scope", "entitlements:read"),
	cverb("rotate", "--scope", "entitlements:read", "--overlap-seconds", "3600", "{{cred:acr:2}}"),
	cverb("rotate", "--scope", "entitlements:read", "{{cred:acr:3}}", "--overlap-seconds", "3601"),
	cverb("rotate", "{{cred:acr:3}}", "--scope", "entitlements:read", "--overlap-seconds", "-1"),
	cverb("rotate", "{{cred:acr:3}}", "--scope", "entitlements:read", "--overlap-seconds", "abc"),
	cverb("rotate", "{{cred:acr:3}}", "--scope", "entitlements:read", "--overlap-seconds", ""),
	cverb("rotate", "{{cred:acr:3}}", "--scope", "entitlements:read", "--overlap-seconds", " 300 "),
	cverb("rotate", "{{cred:acr:4}}", "--scope", "entitlements:read", "--overlap-seconds", "+300"),
	cverb("rotate", "{{cred:acr:5}}", "--scope", "entitlements:read", "--overlap-seconds", "3_00"),
	cverb("rotate", "{{cred:acr:6}}", "--scope", "entitlements:read", "--overlap-seconds", "99999999999999999999"),
	cverb("rotate", missingID, "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:worker-operator:0}}", "--scope", "workers:read"),
	// The scopes are valid for the service named, so the refusal is the credential's own service.
	cverb("rotate", "{{cred:worker-operator:0}}", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:0}}", "--service", "worker-operator", "--scope", "workers:read"),
	cverb("rotate", "{{cred:worker-operator:0}}", "--service", "worker-operator", "--scope", "workers:read", "--scope", "workers:operate", "--overlap-seconds", "60"),
	cverb("rotate", "not-a-uuid", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:7}}", "--scope", "nope"),
	cverb("rotate", "{{cred:acr:7}}"),
	cverb("rotate", "{{cred:acr:7}}", "--scope", "entitlements:read", "--expires-at", "2001-01-01T00:00:00+00:00"),
	cverb("rotate", "{{cred:acr:7}}", "--scope", "entitlements:read", "--created-by-user-id", "bad"),
	cverb("rotate", "{{cred:acr:7}}", "--scope", "entitlements:read", "--expires-at", "2099-03-04T05:06:07+00:00", "--created-by-user-id", credUserID),
	cverb("rotate", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:8}}", "{{cred:acr:9}}", "--scope", "entitlements:read"),
	cverb("rotate", "{{cred:acr:8:upper}}", "--scope", "entitlements:read", "--overlap-seconds", "300"),
	cverb("rotate", "{{cred:acr:9:nohyphen}}", "--scope", "entitlements:read", "--overlap-seconds", "300"),
	cverb("rotate", "{{cred:acr:10:braces}}", "--scope", "entitlements:read", "--overlap-seconds", "300"),
	cverb("rotate", "{{cred:acr:11:urn}}", "--scope", "entitlements:read", "--overlap-seconds", "300"),
	csql("UPDATE internal_service_credentials SET expires_at = now() - interval '1 hour' WHERE id = '{{cred:acr:12}}'"),
	cverb("rotate", "{{cred:acr:12}}", "--scope", "entitlements:read"),
	cverb("list"),

	// revoke
	cverb("revoke", "{{cred:acr:13}}"),
	cverb("revoke", "{{cred:acr:14}}"),
	cverb("revoke", "{{cred:acr:13}}"),
	cverb("rotate", "{{cred:acr:13}}", "--scope", "entitlements:read"),
	cverb("revoke", "{{cred:worker-operator:0}}"),
	cverb("revoke", "{{cred:acr:15:upper}}"),
	cverb("revoke", missingID),
	cverb("revoke", "not-a-uuid"),
	cverb("revoke"),
	cverb("revoke", "{{cred:acr:1}}", "{{cred:acr:2}}"),
	cverb("revoke", "{{cred:acr:16}}", "--nope"),
	cverb("list"),
	cverb("list", "--service", "worker-operator"),

	// "--" ends the options (r1 of this change found rotate re-parsing flags after it).
	cverb("create", "--scope", "entitlements:read"),
	cverb("rotate", "--scope", "entitlements:read", "--", "{{cred:acr:last}}", "--overlap-seconds", "60"),
	cverb("rotate", "--scope", "entitlements:read", "--", "{{cred:acr:last}}"),
	cverb("rotate", "--", "{{cred:acr:last}}", "--scope", "entitlements:read"),
	cverb("create", "--scope", "entitlements:read", "--"),
	cverb("create", "--scope", "--", "entitlements:read"),
	cverb("create", "--", "--scope", "entitlements:read"),
	cverb("revoke", "--", "{{cred:acr:last}}"),
	cverb("revoke", "{{cred:acr:last}}", "--"),
	cverb("revoke", "--", "{{cred:acr:last}}", "--"),
	cverb("list", "--"),
	cverb("list", "--", "--service", "worker-operator"),
	cverb("list"),
}

var credPlaceholder = regexp.MustCompile(`\{\{cred:([a-z-]+):(\d+|last)(?::(upper|nohyphen|braces|urn))?\}\}`)

func (db *database) credResolve(t *testing.T, text string) string {
	t.Helper()
	return credPlaceholder.ReplaceAllStringFunc(text, func(match string) string {
		parts := credPlaceholder.FindStringSubmatch(match)
		query, args := `SELECT id::text FROM internal_service_credentials WHERE service_name = $1 ORDER BY created_at OFFSET $2 LIMIT 1`, []any{parts[1], 0}
		if parts[2] == "last" {
			query, args = `SELECT id::text FROM internal_service_credentials WHERE service_name = $1 ORDER BY created_at DESC LIMIT 1`, []any{parts[1]}
		} else {
			var n int
			fmt.Sscan(parts[2], &n)
			args[1] = n
		}
		var id string
		if err := db.conn.QueryRow(context.Background(), query, args...).Scan(&id); err != nil {
			t.Fatalf("resolve %s: %v", match, err)
		}
		switch parts[3] {
		case "upper":
			return strings.ToUpper(id)
		case "nohyphen":
			return strings.ReplaceAll(id, "-", "")
		case "braces":
			return "{" + id + "}"
		case "urn":
			return "urn:uuid:" + id
		}
		return id
	})
}

func (db *database) credReset(t *testing.T) {
	t.Helper()
	if _, err := db.conn.Exec(context.Background(), "TRUNCATE users, internal_service_credentials, internal_service_credential_audits CASCADE"); err != nil {
		t.Fatal(err)
	}
}

var (
	credToken     = regexp.MustCompile(`svc_(?:acr|worker)_[A-Za-z0-9_-]{43}`)
	credPrefix    = regexp.MustCompile(`"token_prefix": "(svc_[A-Za-z0-9_-]{8,16})"`)
	credTimestamp = regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?\+00:00`)
)

// credentialRun is one session: the tokens it printed, keyed by their hash.
type credentialRun struct {
	tokens map[string]string // sha256 -> step label
	full   map[string]string // sha256 -> token
	m      masker
	// expiry remembers, per credential, when its clock-derived expires_at was first seen and
	// how far from that step's start it was: later steps show that, not a distance that grows
	// with the time the slower producer took.
	expiry map[string]expirySeen
}

type expirySeen struct {
	at     time.Time
	bucket int64
}

func maskStdout(run *credentialRun, stepIndex int, text string) string {
	text = credToken.ReplaceAllStringFunc(text, func(token string) string {
		sum := sha256.Sum256([]byte(token))
		key := hex.EncodeToString(sum[:])
		run.tokens[key] = fmt.Sprintf("token of step %d", stepIndex)
		run.full[key] = token
		if strings.HasPrefix(token, "svc_worker_") {
			return "<svc_worker_token>"
		}
		return "<svc_acr_token>"
	})
	// A listed token_prefix is the first 16 characters of a printed token: show which.
	text = credPrefix.ReplaceAllStringFunc(text, func(match string) string {
		prefix := credPrefix.FindStringSubmatch(match)[1]
		for hash, token := range run.full {
			if strings.HasPrefix(token, prefix) {
				return `"token_prefix": "<prefix of ` + run.tokens[hash] + `>"`
			}
		}
		return `"token_prefix": "<prefix of no printed token>"`
	})
	text = credTimestamp.ReplaceAllStringFunc(text, func(stamp string) string {
		if strings.HasPrefix(stamp, "2099-") || strings.HasPrefix(stamp, "2001-") {
			return stamp
		}
		return "<time>"
	})
	return run.m.mask(text)
}

func bucket(at, start time.Time) int64 {
	delta := at.Sub(start).Seconds()
	return int64(math.Round(delta/60) * 60)
}

// credState is every credential row as the session left it, tokens proven and clock times bucketed.
func (db *database) credState(t *testing.T, run *credentialRun, start time.Time) string {
	t.Helper()
	rows, err := db.conn.Query(context.Background(), `SELECT service_name, scopes::text, token_hash, token_prefix, created_by_user_id::text, expires_at, revoked_at, last_used_at, created_at IS NOT NULL
		FROM internal_service_credentials ORDER BY created_at`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type revokedRow struct {
		index int
		at    time.Time
	}
	var out []map[string]any
	var revoked []revokedRow
	for rows.Next() {
		var service, scopes, hash, prefix string
		var createdBy *string
		var expires, revokedAt, lastUsed *time.Time
		var created bool
		if err := rows.Scan(&service, &scopes, &hash, &prefix, &createdBy, &expires, &revokedAt, &lastUsed, &created); err != nil {
			t.Fatal(err)
		}
		row := map[string]any{"service_name": service, "scopes": scopes, "created_at_set": created}
		if label, ok := run.tokens[hash]; ok {
			row["token"] = label
			row["prefix_is_token_head"] = prefix == run.full[hash][:16]
		} else {
			row["token"] = "hash of no printed token"
		}
		if createdBy != nil {
			row["created_by_user_id"] = *createdBy
		}
		if expires != nil {
			if expires.Year() >= 2090 || expires.Year() <= 2010 {
				row["expires_at"] = expires.UTC().Format("2006-01-02T15:04:05.999999") + "+00:00"
			} else {
				seen, ok := run.expiry[hash]
				if !ok || !seen.at.Equal(*expires) {
					seen = expirySeen{at: *expires, bucket: bucket(*expires, start)}
					run.expiry[hash] = seen
				}
				row["expires_in_seconds_bucket"] = seen.bucket
			}
		}
		if revokedAt != nil {
			revoked = append(revoked, revokedRow{len(out), *revokedAt})
		}
		if lastUsed != nil {
			row["last_used_at"] = "set"
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	// The order the rows were revoked in shows an overwrite: revoking a revoked credential
	// moves it to the end.
	sort.Slice(revoked, func(i, j int) bool { return revoked[i].at.Before(revoked[j].at) })
	for rank, item := range revoked {
		out[item.index]["revoked_rank"] = rank + 1
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func credGo(t *testing.T, db *database, args []string) (int, string, string) {
	t.Helper()
	verbs := map[string]func(context.Context, cli.Env) int{"create": runCredentialCreate, "list": runCredentialList, "rotate": runCredentialRotate, "revoke": runCredentialRevoke}
	run := verbs[args[0]]
	env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Args: args[1:], Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stdout.String()+stderr.String(), "postgres://") {
		t.Fatal("the output carries the DSN")
	}
	return code, stdout.String(), credStderr(code, stderr.String(), false)
}

// credStderr reduces what a run said on stderr to what both producers can be compared on. Exit 0
// and the usage errors (2) say different things by construction (Python logs; argparse's usage
// text, Go's flag text) and compare as such. A refusal (exit 1) compares by message: a Python
// ValueError's message is the text dho puts in its JSON error's detail; any other failure (a
// database error) compares as one class.
func credStderr(code int, text string, python bool) string {
	switch code {
	case 0:
		return ""
	case 2:
		return "<usage error>"
	}
	if python {
		lines := strings.Split(strings.TrimSpace(text), "\n")
		last := lines[len(lines)-1]
		if message, ok := strings.CutPrefix(last, "ValueError: "); ok {
			return message
		}
		return "<failure>"
	}
	var payload struct {
		Error struct{ Code, Detail string } `json:"error"`
	}
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		if json.Unmarshal([]byte(line), &payload) == nil && payload.Error.Code != "" {
			if payload.Error.Code == "service_credential_refused" {
				return payload.Error.Detail
			}
			return "<failure>"
		}
	}
	return "<no error line>"
}

func credPython(t *testing.T, db *database, args []string) (int, string, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	command := exec.Command(python, append([]string{"-c", program, "service-credentials"}, args...)...)
	pyURI := strings.Replace(db.uri, "postgres://", "postgresql://", 1)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false", "DISABLE_DOTENV=1")
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
		// A ValueError (or a database error) is a traceback, exit 1, nothing on stdout.
		if stdout.Len() != 0 {
			t.Fatalf("python printed %q before its traceback", stdout.String())
		}
	}
	return code, stdout.String(), credStderr(code, stderr.String(), true)
}

func (db *database) credSession(t *testing.T, run func(*testing.T, *database, []string) (int, string, string)) []stepResult {
	t.Helper()
	db.credReset(t)
	session := &credentialRun{tokens: map[string]string{}, full: map[string]string{}, expiry: map[string]expirySeen{}}
	var out []stepResult
	for index, s := range credScript {
		start := time.Now()
		if s.sql != "" {
			if _, err := db.conn.Exec(context.Background(), db.credResolve(t, s.sql)); err != nil {
				t.Fatalf("step %d: %v", index, err)
			}
			out = append(out, stepResult{Args: []string{"sql", strings.Split(s.sql, " ")[0]}, State: session.m.mask(db.credState(t, session, start))})
			continue
		}
		args := make([]string, len(s.args))
		for i, arg := range s.args {
			args[i] = db.credResolve(t, arg)
		}
		code, stdout, stderr := run(t, db, args)
		out = append(out, stepResult{Args: s.args, Exit: code, Stdout: maskStdout(session, index, stdout), Stderr: session.m.mask(stderr), State: session.m.mask(db.credState(t, session, start))})
	}
	return out
}

// compareCredentials is compare plus the normalized stderr of every step.
func compareCredentials(t *testing.T, got, want []stepResult, wantName string) {
	t.Helper()
	compare(t, got, want, wantName)
	for index := range got {
		if index < len(want) && got[index].Stderr != want[index].Stderr {
			t.Errorf("step %d (%s): stderr %q, %s stderr %q", index, strings.Join(got[index].Args, " "), got[index].Stderr, wantName, want[index].Stderr)
		}
	}
}

const credGolden = "testdata/service_credentials_golden.json"

func TestServiceCredentialsGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(credGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != credGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", credGolden, got, credGoldenSHA256)
	}
}

func TestServiceCredentialsMatchTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(credGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []stepResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	db := startDatabase(t)
	got := db.credSession(t, credGo)
	compareCredentials(t, got, frozen, "frozen Python")
	refused, printed, usage := 0, 0, 0
	for _, item := range frozen {
		switch {
		case item.Exit == 1:
			refused++
		case item.Exit == 2:
			usage++
		case strings.Contains(item.Stdout, "token>"):
			printed++
		}
	}
	if refused < 20 || printed < 15 || usage < 8 {
		t.Fatalf("the golden has %d refusals, %d usage errors and %d printed tokens: it measures too little", refused, usage, printed)
	}
}

// TestServiceCredentialsVenueOracleMatchesThePythonProducer runs the script through the
// real Python verbs and through dho and compares every step. With
// DHO_SERVICE_CREDENTIALS_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestServiceCredentialsVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	db := startDatabase(t)
	py := db.credSession(t, credPython)
	got := db.credSession(t, credGo)
	compareCredentials(t, got, py, "python")
	if os.Getenv("DHO_SERVICE_CREDENTIALS_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(credGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

var _ = admin.ServiceACR

// credGoldenSHA256 pins the frozen golden; regenerate both with the live oracle.
const credGoldenSHA256 = "4c4a081b59503540316f12653ffe8829cc46867fb2e8900a996dc7893e056657"

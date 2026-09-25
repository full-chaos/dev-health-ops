//go:build integration

package adminops

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// `admin licenses keygen|create` and `admin bundles create|list|assign-plan|assign-org`
// are compared with the real Python verbs on one scripted session over real
// PostgreSQL: every step's exit code and stdout, and the rows left after it.
// Where Python prints an SQLAlchemy exception (a step marked dbError) the two
// sides are compared as "an Error: line, exit 1": Python's text carries the SQL
// statement and its parameters, which dho does not reproduce (named in the PR).

const (
	overrideOrg = "aaaaaaaa-0000-4000-8000-00000000000a"
)

// testSeed is the zero-seed test key of licensing.generator (never a real key).
var testSeed = base64.StdEncoding.EncodeToString(make([]byte, 32))

type bundleStep struct {
	args []string
	env  map[string]string
	// dbError marks a step whose Python failure is an SQLAlchemy exception.
	dbError bool
	// sql runs before the step (a seeding step: it prints nothing).
	sql  string
	seed bool
}

func vb(args ...string) bundleStep { return bundleStep{args: args} }

func dbErr(args ...string) bundleStep { return bundleStep{args: args, dbError: true} }

func withKey(key string, args ...string) bundleStep {
	return bundleStep{args: args, env: map[string]string{"LICENSE_PRIVATE_KEY": key}}
}

var bundleScript = []bundleStep{
	vb("licenses", "keygen"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-1", "--tier", "team"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-1", "--tier", "enterprise", "--duration-days", "30", "--org-name", "Acme é", "--contact-email", "billing@example.com"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "community", "--duration-days", "1_0"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "team", "--duration-days", " 7 "),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "team", "--duration-days", "0"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "team", "--duration-days", "-5"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "bogus"),
	withKey(testSeed, "licenses", "create", "--org-id", "org-2", "--tier", "team", "--duration-days", "many"),
	withKey(testSeed, "licenses", "create", "--tier", "team"),
	vb("licenses", "create", "--org-id", "org-1", "--tier", "team"),
	withKey("not base64 !!", "licenses", "create", "--org-id", "org-1", "--tier", "team"),
	withKey("AAAA", "licenses", "create", "--org-id", "org-1", "--tier", "team"),
	withKey("AAA", "licenses", "create", "--org-id", "org-1", "--tier", "team"),
	{args: []string{"bundles", "list"}, seed: true, sql: `INSERT INTO billing_plans (id, key, name, tier, is_active, display_order, created_at, updated_at) VALUES
(gen_random_uuid(), 'team-plan', 'Team', 'team', true, 1, now(), now()), (gen_random_uuid(), 'pro-plan', 'Pro', 'enterprise', true, 2, now(), now());
INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at) VALUES ('` + overrideOrg + `', 'ov-org', 'Override', 'community', 'stripe', true, now(), now())`},
	vb("bundles", "list"),
	vb("bundles", "create", "--key", "core", "--name", "Core", "--features", "git_sync, work_items_sync ,,basic_analytics", "--description", "The core"),
	dbErr("bundles", "create", "--key", "core", "--name", "Core again", "--features", "git_sync"),
	vb("bundles", "create", "--key", "nofeat", "--name", "No features", "--features", ""),
	vb("bundles", "create", "--key", "uni", "--name", "Ünï 日本", "--features", "sso_saml"),
	vb("bundles", "create", "--key", "bad", "--name", "Bad", "--features", "git_sync,nope"),
	vb("bundles", "create", "--key", "bad", "--name", "Bad", "--features", "it's"),
	vb("bundles", "create", "--key", "bad", "--name", "Bad"),
	vb("bundles", "list"),
	vb("bundles", "assign-plan", "--bundle-key", "core", "--plan-key", "team-plan"),
	dbErr("bundles", "assign-plan", "--bundle-key", "core", "--plan-key", "team-plan"),
	vb("bundles", "assign-plan", "--bundle-key", "core", "--plan-key", "pro-plan"),
	vb("bundles", "assign-plan", "--bundle-key", "missing", "--plan-key", "team-plan"),
	vb("bundles", "assign-plan", "--bundle-key", "core", "--plan-key", "missing"),
	vb("bundles", "list"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "sso_saml", "--reason", "support ticket", "--expires-days", "30"),
	dbErr("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "sso_saml"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "sso_oidc", "--expires-days", "0"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "git_sync", "--expires-days", "-3"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "webhooks", "--expires-days", "1_0"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "no_such_feature"),
	dbErr("bundles", "assign-org", "--org-id", "not-a-uuid", "--feature-key", "api_access"),
	dbErr("bundles", "assign-org", "--org-id", "bbbbbbbb-0000-4000-8000-00000000000b", "--feature-key", "api_access"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "api_access", "--expires-days", "1000000000"),
	vb("bundles", "assign-org", "--org-id", overrideOrg, "--feature-key", "api_access", "--expires-days", "999999999"),
	dbErr("bundles", "assign-org", "--org-id", "{AAAAAAAA-0000-4000-8000-00000000000A}", "--feature-key", "work_graph", "--reason", "é"),
	dbErr("bundles", "assign-org", "--org-id", "urn:uuid:aaaaaaaa-0000-4000-8000-00000000000a", "--feature-key", "work_graph"),
	vb("bundles", "assign-org", "--org-id", "AAAAAAAA00004000800000000000000A", "--feature-key", "work_graph", "--reason", "é"),
	vb("bundles", "assign-org", "--org-id", "aaaa-aaaa0000-4000-8000-00000000000a", "--feature-key", "quadrant_analysis"),
}

type bundleResult struct {
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Stdout string   `json:"stdout"`
	State  string   `json:"state"`
}

var (
	expiresLine = regexp.MustCompile(`(?m)^  Expires: \S+$`)
)

// normalizeLicense turns a printed license (or key pair) into a stable, checked
// form: the payload with its random or clock-derived fields masked, and whether
// the signature verifies under the key the step signed with.
func normalizeLicense(t *testing.T, stdout, seedB64 string) string {
	t.Helper()
	text := strings.TrimSuffix(stdout, "\n")
	if strings.HasPrefix(text, "PUBLIC_KEY=") {
		lines := strings.Split(text, "\n")
		if len(lines) != 2 || !strings.HasPrefix(lines[1], "LICENSE_PRIVATE_KEY=") {
			return "malformed key pair output"
		}
		public, err1 := base64.StdEncoding.DecodeString(strings.TrimPrefix(lines[0], "PUBLIC_KEY="))
		seed, err2 := base64.StdEncoding.DecodeString(strings.TrimPrefix(lines[1], "LICENSE_PRIVATE_KEY="))
		if err1 != nil || err2 != nil || len(seed) != ed25519.SeedSize {
			return "malformed key pair output"
		}
		derived := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		return fmt.Sprintf("key pair: public %d bytes, seed %d bytes, consistent=%v\n", len(public), len(seed), string(derived) == string(public))
	}
	parts := strings.Split(text, ".")
	if len(parts) != 2 || strings.ContainsAny(text, " \n") {
		return stdout
	}
	body, err1 := base64.StdEncoding.DecodeString(parts[0])
	signature, err2 := base64.StdEncoding.DecodeString(parts[1])
	seed, err3 := base64.StdEncoding.DecodeString(seedB64)
	if err1 != nil || err2 != nil || err3 != nil {
		return stdout
	}
	public := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	valid := ed25519.Verify(public, body, signature)
	var payload map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return stdout
	}
	iat, _ := payload["iat"].(json.Number).Int64()
	exp, _ := payload["exp"].(json.Number).Int64()
	payload["lifetime_days"] = (exp - iat) / 86400
	for _, key := range []string{"iat", "exp", "license_id"} {
		payload[key] = "<masked>"
	}
	masked, _ := json.Marshal(payload)
	// The byte layout of the payload (key order, separators) is part of what a
	// verifier reads: keep it, with the masked fields' values replaced.
	layout := regexp.MustCompile(`"iat":\d+,"exp":\d+`).ReplaceAllString(string(body), `"iat":<n>,"exp":<n>`)
	layout = regexp.MustCompile(`"license_id":"[^"]*"`).ReplaceAllString(layout, `"license_id":"<uuid>"`)
	return fmt.Sprintf("signature valid=%v\nlayout %s\nfields %s\n", valid, layout, masked)
}

func (db *database) bundleState(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	collect := func(sql string) []string {
		rows, err := db.conn.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var text string
			if err := rows.Scan(&text); err != nil {
				t.Fatal(err)
			}
			out = append(out, text)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	state := map[string][]string{
		"bundles":   collect(`SELECT concat_ws('|', key, name, coalesce(description, '<null>'), features::text) FROM feature_bundles ORDER BY key`),
		"plans":     collect(`SELECT concat_ws('|', b.key, p.key) FROM plan_feature_bundles l JOIN feature_bundles b ON b.id = l.bundle_id JOIN billing_plans p ON p.id = l.plan_id ORDER BY 1`),
		"overrides": collect(`SELECT concat_ws('|', o.org_id::text, f.key, o.is_enabled::text, coalesce(round(extract(epoch FROM (o.expires_at - now())) / 86400)::text, '<null>'), o.config::text, coalesce(o.reason, '<null>')) FROM org_feature_overrides o JOIN feature_flags f ON f.id = o.feature_id ORDER BY 1`),
	}
	raw, _ := json.Marshal(state)
	return string(raw)
}

func bundleSession(t *testing.T, python bool) []bundleResult {
	t.Helper()
	db := startDatabase(t)
	ctx := context.Background()
	if _, err := db.conn.Exec(ctx, "TRUNCATE feature_bundles, plan_feature_bundles, billing_plans, org_feature_overrides, feature_flags, organizations CASCADE"); err != nil {
		t.Fatal(err)
	}
	if _, err := admincli.Seed(ctx, db.conn, time.Now); err != nil {
		t.Fatalf("seed feature flags: %v", err)
	}
	var out []bundleResult
	for _, s := range bundleScript {
		if s.seed {
			if _, err := db.conn.Exec(ctx, s.sql); err != nil {
				t.Fatalf("seed: %v", err)
			}
			out = append(out, bundleResult{Args: []string{"<seed>"}, State: db.bundleState(t)})
			continue
		}
		var code int
		var stdout string
		if python {
			code, stdout = pythonVerbFull(t, db, s.env, nil, s.args)
		} else {
			code, stdout = goVerbEnv(t, db, s.env, s.args)
		}
		switch {
		case s.dbError && strings.HasPrefix(stdout, "Error: "):
			stdout = "Error: <database error>\n"
		case s.args[0] == "licenses":
			stdout = normalizeLicense(t, stdout, s.env["LICENSE_PRIVATE_KEY"])
		}
		stdout = expiresLine.ReplaceAllString(stdout, "  Expires: <time>")
		out = append(out, bundleResult{Args: s.args, Exit: code, Stdout: stdout, State: db.bundleState(t)})
	}
	return out
}

const bundlesGolden = "testdata/bundles_golden.json"

// bundlesGoldenSHA256 pins testdata/bundles_golden.json (R24): what the real
// `dev-hops admin licenses|bundles` verbs printed and left for every step. The
// producer is deleted with the Python CLI, so this is a rot guard: the file is
// only rewritten by TestBundlesVenueOracleMatchesThePythonProducer with
// DHO_BUNDLES_GOLDEN_UPDATE=1, then this digest is updated.
const bundlesGoldenSHA256 = "957eeb1ac01f93981647de1821236e7074089f3dcd1e1506d1b1748fa4ae9946"

func TestBundlesGoldenIsTheFileTheDigestPins(t *testing.T) {
	raw, err := os.ReadFile(bundlesGolden)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != bundlesGoldenSHA256 {
		t.Fatalf("%s digest = %s, want %s: the golden changed without its digest", bundlesGolden, got, bundlesGoldenSHA256)
	}
}

func compareBundles(t *testing.T, got, want []bundleResult, wantName string) {
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

// TestBundlesMatchTheFrozenPythonOutput runs the script and compares every step
// with what the real Python verbs did (frozen; no Python needed).
func TestBundlesMatchTheFrozenPythonOutput(t *testing.T) {
	raw, err := os.ReadFile(bundlesGolden)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []bundleResult
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	compareBundles(t, bundleSession(t, false), frozen, "frozen Python")
	created, refused := 0, 0
	for _, item := range frozen {
		if strings.HasPrefix(item.Stdout, "Created") || strings.HasPrefix(item.Stdout, "Assigned") || strings.HasPrefix(item.Stdout, "signature valid=true") {
			created++
		}
		if strings.HasPrefix(item.Stdout, "Error: ") {
			refused++
		}
	}
	if created < 8 || refused < 8 {
		t.Fatalf("the golden has %d successes and %d refusals: it measures too little", created, refused)
	}
}

// TestBundlesVenueOracleMatchesThePythonProducer runs the script through the real
// Python verbs and through dho. With DHO_BUNDLES_GOLDEN_UPDATE=1 it rewrites the
// frozen file.
func TestBundlesVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	py := bundleSession(t, true)
	got := bundleSession(t, false)
	compareBundles(t, got, py, "python")
	if os.Getenv("DHO_BUNDLES_GOLDEN_UPDATE") == "1" {
		raw, err := json.MarshalIndent(py, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(bundlesGolden, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

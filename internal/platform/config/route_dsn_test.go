package config_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-6902: the route-activate hook Job no longer builds its three DSNs in a
// Python init container. Each `dho workers routes apply` step is given the
// component form (DEV_HEALTH_PG_{DOMAIN,QUEUE,COORDINATOR}_{HOST,PORT,USER,PASSWORD}
// and DEV_HEALTH_PG_DB) and ResolveDSN assembles the DSN, so the Python encode
// script is deleted from the chart.
//
// The contract of that step is the CONNECTION IDENTITY the driver reads from the
// DSN (host, port, user, password, database), not the DSN's bytes: Python's
// urllib.parse.quote(safe="") and Go's net/url escape a userinfo differently
// (Go leaves sub-delimiters that Python encodes) and mean the same. This oracle
// ran the REAL init-container script (testdata/route_dsn_init.sh, the text the
// chart carried at RouteDSNScriptSource, sha256-pinned) under /bin/sh with python3
// on an adversarial grid, once, through the record verb, parses each URI it wrote with pgx, resolves the same
// inputs through ResolveDSN, and compares the two parsed identities.
//
// Named divergences, each asserted by the golden rather than hidden:
//   - a role that is empty next to a password: the script writes a URI with an
//     empty user, ResolveDSN refuses (a password without a user);
//   - the script writes a URI the driver refuses (an IPv6 zone id left unescaped)
//     where the component form escapes it and connects.

const (
	// routeDSNScriptSource is the commit whose chart carried the script.
	routeDSNScriptSource = "6d609ae3fddfd37566d31cb5e3bbe49ca46a64d6"
	routeDSNScript       = "testdata/route_dsn_init.sh"
	routeDSNScriptSHA256 = "bc2a579d87b6da09c7c76d70f7cec8b03671333686f3c85a4af0717425962e31"
	// routeDSNPythonBuild is the build whose interpreter ran the script when
	// the golden was recorded. The script uses the standard library only.
	routeDSNPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"
)

// routeDSNCase is one set of inputs, the chart's own environment for the step.
type routeDSNCase struct {
	Name string            `json:"name"`
	Env  map[string]string `json:"env"`
}

var routeDSNBase = map[string]string{
	"POSTGRES_HOST": "postgres.internal", "POSTGRES_PORT": "5432", "POSTGRES_DB": "devhealth",
	"RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain", "RIVER_QUEUE_DATABASE_ROLE": "devhealth_queue",
	"RIVER_COORDINATOR_DATABASE_ROLE": "devhealth_coordinator",
	"RIVER_DOMAIN_DATABASE_PASSWORD":  "d-pw", "RIVER_QUEUE_DATABASE_PASSWORD": "q-pw", "RIVER_COORDINATOR_DATABASE_PASSWORD": "c-pw",
}

// routeDSNValues is the RFC 3986 set the chart's own tests swept (tests/workers/
// test_helm_route_activate_hooks.py): every gen-delim and sub-delim alone, both
// combined, a percent, a space, non-ASCII, the empty string.
func routeDSNValues() [][2]string {
	values := [][2]string{
		{"simple", "plainpassword123"}, {"percent", "pw%with%percent"}, {"space", "pw with space"},
		{"unicode", "pw☃snowman"}, {"empty", ""}, {"gen-delims", ":/?#[]@"}, {"sub-delims", "!$&'()*+,;="},
		{"quote-and-backslash", `pw"with\back`}, {"percent-encoded-lookalike", "%41%2f%zz"},
		{"plus-and-tilde", "a+b~c"}, {"newline-free-tab", "a\tb"},
	}
	for _, c := range ":/?#[]@" {
		values = append(values, [2]string{fmt.Sprintf("gen-%q", c), fmt.Sprintf("pw%cwith%cchar", c, c)})
	}
	for _, c := range "!$&'()*+,;=" {
		values = append(values, [2]string{fmt.Sprintf("sub-%q", c), fmt.Sprintf("pw%cwith%cchar", c, c)})
	}
	return values
}

func routeDSNCases() []routeDSNCase {
	with := func(name string, over map[string]string) routeDSNCase {
		env := map[string]string{}
		for key, value := range routeDSNBase {
			env[key] = value
		}
		for key, value := range over {
			env[key] = value
		}
		return routeDSNCase{Name: name, Env: env}
	}
	cases := []routeDSNCase{with("base", nil)}
	for _, value := range routeDSNValues() {
		name, text := value[0], value[1]
		for _, role := range []string{"DOMAIN", "QUEUE", "COORDINATOR"} {
			cases = append(cases, with("password/"+strings.ToLower(role)+"/"+name, map[string]string{"RIVER_" + role + "_DATABASE_PASSWORD": text}))
			if text != "" { // an empty role next to a password is its own named case
				cases = append(cases, with("role/"+strings.ToLower(role)+"/"+name, map[string]string{"RIVER_" + role + "_DATABASE_ROLE": text}))
			}
		}
		if text != "" {
			cases = append(cases, with("database/"+name, map[string]string{"POSTGRES_DB": text}))
			cases = append(cases, with("host/"+name, map[string]string{"POSTGRES_HOST": text}))
		}
	}
	cases = append(cases,
		with("database/review#db", map[string]string{"POSTGRES_DB": "review#db"}),
		with("role/empty-with-password", map[string]string{"RIVER_QUEUE_DATABASE_ROLE": ""}),
		with("port/6432", map[string]string{"POSTGRES_PORT": "6432"}),
	)
	for _, host := range []string{
		"127.0.0.1", "::1", "::ffff:127.0.0.1", "2001:db8::1", "fe80::1%eth0", "POSTGRES.Internal", "pg_host.svc.cluster.local",
		"pg.internal.", "bücher.example", "a", "0.0.0.0", "::", "[::1]", "1.2.3", "postgres.internal:5433",
	} {
		cases = append(cases, with("host/"+host, map[string]string{"POSTGRES_HOST": host}))
	}
	return cases
}

// routeDSNIdentity is what the driver reads from a DSN.
type routeDSNIdentity struct {
	Host     string `json:"host,omitempty"`
	Port     uint16 `json:"port,omitempty"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
	Database string `json:"database,omitempty"`
	Refused  bool   `json:"refused,omitempty"`
}

func identityOf(t *testing.T, dsn string) routeDSNIdentity {
	t.Helper()
	// The driver reads defaults from the process environment and ~/.pgpass for
	// anything the DSN leaves out: pin them so the identity is the DSN's alone.
	for _, key := range []string{"PGHOST", "PGPORT", "PGUSER", "PGPASSWORD", "PGDATABASE", "PGSERVICE", "PGSSLMODE", "PGCONNECT_TIMEOUT"} {
		t.Setenv(key, "")
	}
	t.Setenv("PGPASSFILE", "/nonexistent")
	t.Setenv("PGSERVICEFILE", "/nonexistent")
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		return routeDSNIdentity{Refused: true}
	}
	return routeDSNIdentity{Host: parsed.Host, Port: parsed.Port, User: parsed.User, Password: parsed.Password, Database: parsed.Database}
}

// routeDSNResult is one plane's three identities (domain, queue, coordinator).
type routeDSNResult struct {
	Domain, Queue, Coordinator routeDSNIdentity
}

// goRouteDSN is the component form the chart now hands each routes apply step.
func goRouteDSN(t *testing.T, env map[string]string) routeDSNResult {
	t.Helper()
	component := map[string]string{"DEV_HEALTH_PG_DB": env["POSTGRES_DB"]}
	for _, role := range []string{"DOMAIN", "QUEUE", "COORDINATOR"} {
		component["DEV_HEALTH_PG_"+role+"_HOST"] = env["POSTGRES_HOST"]
		component["DEV_HEALTH_PG_"+role+"_PORT"] = env["POSTGRES_PORT"]
		component["DEV_HEALTH_PG_"+role+"_USER"] = env["RIVER_"+role+"_DATABASE_ROLE"]
		component["DEV_HEALTH_PG_"+role+"_PASSWORD"] = env["RIVER_"+role+"_DATABASE_PASSWORD"]
	}
	lookup := func(key string) (string, bool) { value, ok := component[key]; return value, ok }
	resolve := func(rawKey string, spec config.ComponentSpec) routeDSNIdentity {
		value, configured, err := config.ResolveDSN(lookup, rawKey, spec)
		if err != nil || !configured {
			return routeDSNIdentity{Refused: true}
		}
		return identityOf(t, value.Reveal())
	}
	return routeDSNResult{
		Domain:      resolve("POSTGRES_URI", config.DomainDatabaseSpec),
		Queue:       resolve("WORKER_DATABASE_URI", config.QueueDatabaseSpec),
		Coordinator: resolve("COORDINATOR_DATABASE_URI", config.CoordinatorDatabaseSpec),
	}
}

// routeDSNGoldenCase is what the real script produced for one case.
type routeDSNGoldenCase struct {
	Name   string         `json:"name"`
	Python routeDSNResult `json:"python"`
}

// routeDSNNamedDivergences are the cases where the component form and the script
// differ by ruling; every other case must give equal identities. There are two
// classes, and never the fail-open one (the script's DSN works where the component
// form refuses), which is why the first is listed by what the script's URI holds:
//
//   - "refused-by-script-uri": the script percent-encodes a host and the driver
//     refuses the encoded host (a sub-delimiter, `+`, `~`, or an IPv6 zone id it
//     leaves unescaped after the bracket), so the hook would have failed to
//     connect; the component form hands the driver the host as the driver reads it
//     and the driver accepts it. Every such case is a host no DNS name has.
//   - "empty-role-with-password": the script writes an empty user next to the
//     password; ResolveDSN refuses a password without a user.
func routeDSNNamedDivergence(name string) string {
	refusedByScriptURI := map[string]bool{"host/plus-and-tilde": true, "host/fe80::1%eth0": true}
	for _, c := range "!$&'()*+;=" { // the sub-delimiters as a single-character host ("," alone is refused by both)
		refusedByScriptURI[fmt.Sprintf("host/sub-%q", c)] = true
	}
	switch {
	case refusedByScriptURI[name]:
		return "refused-by-script-uri"
	case name == "role/empty-with-password":
		return "empty-role-with-password"
	}
	return ""
}

func compareRouteDSN(t *testing.T, name string, got, want routeDSNResult, wantName string) {
	t.Helper()
	pairs := map[string][2]routeDSNIdentity{
		"POSTGRES_URI": {got.Domain, want.Domain}, "WORKER_DATABASE_URI": {got.Queue, want.Queue}, "COORDINATOR_DATABASE_URI": {got.Coordinator, want.Coordinator},
	}
	differs, failOpen := false, false
	for label, pair := range pairs {
		if pair[0] == pair[1] {
			continue
		}
		differs = true
		// The component form refuses where the script's DSN was accepted: fail-open
		// for the script, a regression for the chart.
		if pair[0].Refused && !pair[1].Refused {
			failOpen = true
		}
		if routeDSNNamedDivergence(name) == "" {
			t.Errorf("%s: %s: component form %+v, %s %+v", name, label, pair[0], wantName, pair[1])
		}
	}
	switch class := routeDSNNamedDivergence(name); {
	case class == "":
	case !differs:
		t.Errorf("%s: named divergence (%s) no longer differs: remove it", name, class)
	case class == "refused-by-script-uri" && failOpen:
		t.Errorf("%s: the component form refuses where the script's URI was accepted", name)
	}
}

func digestFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func TestRouteDSNScriptIsPinned(t *testing.T) {
	if got := digestFile(t, routeDSNScript); got != routeDSNScriptSHA256 {
		t.Fatalf("%s digest = %s, want %s: the script is the text the chart carried at %s and must not change", routeDSNScript, got, routeDSNScriptSHA256, routeDSNScriptSource)
	}
}

// scriptRouteDSN runs the real init-container script for one case, in the
// producer's closed environment with its interpreter first on PATH and the
// case's variables after it, and parses the three URIs it wrote.
func scriptRouteDSN(t *testing.T, producer *venueoracle.Producer, script string, env map[string]string) routeDSNResult {
	t.Helper()
	pythonDir, err := producer.PythonDir()
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	entries := []string{"PATH=" + pythonDir + string(os.PathListSeparator) + "/usr/local/bin:/usr/bin:/bin"}
	for _, name := range names {
		entries = append(entries, name+"="+env[name])
	}
	command := exec.Command("/bin/sh", "-ec", strings.ReplaceAll(script, "/run/route-dsn", out))
	command.Env = producer.Env(nil, entries...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("the route-dsn script failed: %v\n%s", err, output)
	}
	read := func(name string) routeDSNIdentity {
		raw, err := os.ReadFile(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		return identityOf(t, string(raw))
	}
	return routeDSNResult{Domain: read("POSTGRES_URI"), Queue: read("WORKER_DATABASE_URI"), Coordinator: read("COORDINATOR_DATABASE_URI")}
}

// TestRouteDSNMatchesTheFrozenPythonOutput compares the component form with what
// the real script produced for every case of the grid. The script's answers
// were executed once by the record verb (the script under /bin/sh with the
// pinned build's python3, in the closed environment) and are frozen in
// testdata/golden/route_dsn.json; the script's text and the grid are the
// golden's key, so a changed script or grid must be recorded again. A frozen
// run needs no shell and no Python.
func TestRouteDSNMatchesTheFrozenPythonOutput(t *testing.T) {
	golden := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/route_dsn.json",
		PythonBuild: routeDSNPythonBuild,
		SHA256:      "91fa9f31450d55196bdcd0e915858c19116ea5a441e5e70b60c1ceb4004d10ad",
		Recipe: "git worktree add --detach $DIR " + routeDSNPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/platform/config/ -test '^TestRouteDSNMatchesTheFrozenPythonOutput$' -python-root $DIR",
	})
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root := golden.PythonRoot(t, repoRoot)
	script, err := os.ReadFile(routeDSNScript)
	if err != nil {
		t.Fatal(err)
	}
	cases := routeDSNCases()
	input, err := json.Marshal(cases)
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("route dsn grid", string(script), input, nil)
	answers := golden.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		recorded := make([]routeDSNGoldenCase, 0, len(cases))
		for _, c := range cases {
			recorded = append(recorded, routeDSNGoldenCase{Name: c.Name, Python: scriptRouteDSN(t, producer, string(script), c.Env)})
		}
		raw, err := json.MarshalIndent(recorded, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(raw) + "\n"}}
	})
	golden.Consumed(t, answers...)
	var frozen []routeDSNGoldenCase
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(cases) {
		t.Fatalf("the golden holds %d cases, the grid has %d: record it again", len(frozen), len(cases))
	}
	accepted, refused, named := 0, 0, 0
	for index, c := range cases {
		if frozen[index].Name != c.Name {
			t.Fatalf("case %d is %q in the golden, %q in the grid: record it again", index, frozen[index].Name, c.Name)
		}
		compareRouteDSN(t, c.Name, goRouteDSN(t, c.Env), frozen[index].Python, "frozen python")
		switch {
		case routeDSNNamedDivergence(c.Name) != "":
			named++
		case frozen[index].Python.Domain.Refused:
			refused++
		default:
			accepted++
		}
	}
	if accepted < 100 || named < 1 {
		t.Fatalf("the golden has %d accepted cases (%d refused by the driver, %d named): it measures too little", accepted, refused, named)
	}
	golden.SkipDiff(t)
	golden.Finish(t)
}

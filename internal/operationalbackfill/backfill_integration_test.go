//go:build integration

package operationalbackfill

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	orgMain  = "aaaaaaaa-0000-4000-8000-000000000001"
	orgOther = "bbbbbbbb-0000-4000-8000-000000000002"
	orgEmpty = "cccccccc-0000-4000-8000-000000000003"
	orgBad   = "dddddddd-0000-4000-8000-000000000004"
)

type clickHouse struct {
	instance *containers.Instance
	httpDSN  string
}

// startClickHouse migrates a container to the head under the given operational
// ordering contract: "" is the legacy table shape (contract 1), "2" the shape
// with the ordering columns (production's head).
func startClickHouse(t *testing.T, contract string) clickHouse {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	if contract == "2" {
		chschema.Apply(ctx, t, instance)
	} else {
		chschema.ApplyOrderingContract1(ctx, t, instance)
	}
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	return clickHouse{instance: instance, httpDSN: dsn}
}

func (ch clickHouse) do(t *testing.T, statement string) string {
	t.Helper()
	parsed, err := url.Parse(ch.httpDSN)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	request, err := http.NewRequest(http.MethodPost, "http://"+parsed.Host+"/?database="+strings.TrimPrefix(parsed.Path, "/")+
		"&output_format_json_quote_64bit_integers=1&output_format_json_quote_decimals=1", strings.NewReader(statement))
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth(parsed.User.Username(), password)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("clickhouse %d for %.120s: %s", response.StatusCode, statement, body)
	}
	return string(body)
}

// seedLegacy writes the legacy Atlassian Ops rows the scenarios migrate. The
// values are chosen to reach every branch of the mapping: each status, priority
// and severity spelling (with case, spaces and hyphens), values outside the
// vocabulary, NULLs, sub-second times, identifiers that need JSON escapes, a
// second version of a row (FINAL keeps the latest), another organization's rows.
func seedLegacy(t *testing.T, ch clickHouse) {
	t.Helper()
	seedLegacyWith(func(statement string) { ch.do(t, statement) })
}

// legacySeedDigest is a digest of every statement seedLegacy runs: the seeded legacy rows are the input of
// every scenario, so they are part of the golden's request key.
func legacySeedDigest() string {
	hash := sha256.New()
	seedLegacyWith(func(statement string) { fmt.Fprintf(hash, "%d:%s\n", len(statement), statement) })
	return hex.EncodeToString(hash.Sum(nil))
}

func seedLegacyWith(do func(statement string)) {
	ts := func(value string) string { return "toDateTime64('" + value + "', 6, 'UTC')" }
	incident := func(org, id, url, summary, description, status, severity, created, providerID, lastSynced string) {
		do(fmt.Sprintf("INSERT INTO atlassian_ops_incidents (org_id, id, url, summary, description, status, severity, created_at, provider_id, last_synced) VALUES ('%s', '%s', %s, '%s', %s, '%s', '%s', %s, %s, %s)",
			org, id, url, summary, description, status, severity, ts(created), providerID, ts(lastSynced)))
	}
	const null = "NULL"
	incident(orgMain, "inc-1", "'https://ops.example/inc-1'", "Database down", "'primary is unreachable'", "open", "SEV-1", "2026-03-01 10:00:00.123456", "'p-1'", "2026-03-01 10:05:00.654321")
	incident(orgMain, "inc-2", null, "Cache slow", null, " Resolved ", "critical", "2026-03-02 11:00:00.000001", null, "2026-03-02 12:00:00.999999")
	incident(orgMain, "inc-3", null, "Closed one", null, "CLOSED", "Sev2", "2026-03-03 00:00:00", null, "2026-03-03 01:00:00")
	incident(orgMain, "inc-4", null, "Weird", null, "on fire", "urgent", "2026-03-04 00:00:00", null, "2026-03-04 00:00:01")
	incident(orgMain, "inc-5", null, "Info sev", null, "OPENED", "info", "2026-03-05 00:00:00", null, "2026-03-05 00:00:01")
	incident(orgMain, "inc-6", null, "Dotted capital I", null, "Suppressed", "İnfo", "2026-03-06 00:00:00", null, "2026-03-06 00:00:01")
	incident(orgMain, "inc-7", null, "Hyphens", null, "acknowledged", "s-e-v-3", "2026-03-07 00:00:00", null, "2026-03-07 00:00:01")
	incident(orgMain, "inc-8", null, "Active low", null, "ACTIVE", "  LOW\\t", "2026-03-08 00:00:00", null, "2026-03-08 00:00:01")
	incident(orgMain, "inc-9", null, "Medium", null, "resolved", "Medium", "2026-03-09 00:00:00", null, "2026-03-09 00:00:01")
	incident(orgMain, "inc-10", null, "Sev4", null, "closed", "sev4", "2026-03-10 00:00:00", null, "2026-03-10 00:00:01")
	incident(orgMain, "inc-11", null, "High", null, "closed", "HIGH", "2026-03-11 00:00:00", null, "2026-03-11 00:00:01")
	// Identifiers that need JSON escapes in the identity seed.
	incident(orgMain, "q\"uote\\\\back\\x01ctl\\x7fdel-é-😀- ", null, "Escapes", null, "open", "low", "2026-03-12 00:00:00", null, "2026-03-12 00:00:01")
	// Two versions of one row: FINAL keeps the later last_synced.
	incident(orgMain, "inc-dup", null, "Old title", null, "open", "low", "2026-03-13 00:00:00", null, "2026-03-13 00:00:01")
	incident(orgMain, "inc-dup", null, "New title", null, "resolved", "low", "2026-03-13 00:00:00", null, "2026-03-13 09:00:00")
	incident(orgOther, "inc-other", null, "Not this org", null, "open", "low", "2026-03-14 00:00:00", null, "2026-03-14 00:00:01")

	alert := func(org, id, status, priority, created, acknowledged, snoozed, closed, lastSynced string) {
		do(fmt.Sprintf("INSERT INTO atlassian_ops_alerts (org_id, id, status, priority, created_at, acknowledged_at, snoozed_at, closed_at, last_synced) VALUES ('%s', '%s', '%s', '%s', %s, %s, %s, %s, %s)",
			org, id, status, priority, ts(created), acknowledged, snoozed, closed, ts(lastSynced)))
	}
	alert(orgMain, "al-1", "open", "P1", "2026-03-01 10:00:00.5", ts("2026-03-01 10:01:00.25"), null, null, "2026-03-01 10:02:00")
	alert(orgMain, "al-2", "closed", "p2", "2026-03-02 10:00:00", null, ts("2026-03-02 10:30:00"), ts("2026-03-02 11:00:00.000007"), "2026-03-02 11:00:01")
	alert(orgMain, "al-3", "acknowledged", " P3 ", "2026-03-03 10:00:00", null, null, null, "2026-03-03 10:00:01")
	alert(orgMain, "al-4", "snoozed", "P4", "2026-03-04 10:00:00", null, null, null, "2026-03-04 10:00:01")
	alert(orgMain, "al-5", "suppressed", "Critical", "2026-03-05 10:00:00", null, null, null, "2026-03-05 10:00:01")
	alert(orgMain, "al-6", "active", "HIGH", "2026-03-06 10:00:00", null, null, null, "2026-03-06 10:00:01")
	alert(orgMain, "al-7", "Resolved", "medium", "2026-03-07 10:00:00", null, null, null, "2026-03-07 10:00:01")
	alert(orgMain, "al-8", "?", "urgent", "2026-03-08 10:00:00", null, null, null, "2026-03-08 10:00:01")
	alert(orgOther, "al-other", "open", "P1", "2026-03-09 10:00:00", null, null, null, "2026-03-09 10:00:01")

	schedule := func(org, id, name, timezone, lastSynced string) {
		do(fmt.Sprintf("INSERT INTO atlassian_ops_schedules (org_id, id, name, timezone, last_synced) VALUES ('%s', '%s', '%s', %s, %s)",
			org, id, name, timezone, ts(lastSynced)))
	}
	schedule(orgMain, "sch-1", "Primary on-call", "'Europe/Berlin'", "2026-03-01 09:00:00.5")
	schedule(orgMain, "sch-2", "Secondary", null, "2026-03-02 09:00:00")
	schedule(orgOther, "sch-other", "Elsewhere", "'UTC'", "2026-03-03 09:00:00")

	// One organization whose legacy row cannot become a canonical identity.
	incident(orgBad, "", null, "No identifier", null, "open", "low", "2026-03-15 00:00:00", null, "2026-03-15 00:00:01")
}

var operationalTables = []string{"operational_incidents", "operational_alerts", "operational_on_call_schedules"}

func (ch clickHouse) truncate(t *testing.T) {
	t.Helper()
	for _, table := range operationalTables {
		ch.do(t, "TRUNCATE TABLE "+table)
	}
}

// stampKeys are the columns Python fills from the wall clock at construction
// (observed_at and last_synced) and the revision derived from both of them.
var stampKeys = []string{"observed_at", "last_synced", "ingest_revision"}

// dumpRow is one stored row with its stamps checked and masked: the stamps are
// the only values that differ between two runs, so each is verified by its own
// structure (the two timestamps are recent and the ingest revision is exactly
// last_synced then observed_at in microseconds) and then replaced.
type dumpRow map[string]any

func parseMicros(t *testing.T, text string) uint64 {
	t.Helper()
	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.000000", text, time.UTC)
	if err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return uint64(parsed.Sub(unixEpoch) / time.Microsecond)
}

func (ch clickHouse) dump(t *testing.T, window [2]time.Time) map[string][]dumpRow {
	t.Helper()
	out := map[string][]dumpRow{}
	for _, table := range operationalTables {
		body := ch.do(t, "SELECT * FROM "+table+" FINAL ORDER BY id FORMAT JSONEachRow")
		var rows []dumpRow
		for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
			if line == "" {
				continue
			}
			var row dumpRow
			decoder := json.NewDecoder(strings.NewReader(line))
			decoder.UseNumber()
			if err := decoder.Decode(&row); err != nil {
				t.Fatal(err)
			}
			observed, _ := row["observed_at"].(string)
			lastSynced, _ := row["last_synced"].(string)
			observedMicros, lastSyncedMicros := parseMicros(t, observed), parseMicros(t, lastSynced)
			for _, micros := range []uint64{observedMicros, lastSyncedMicros} {
				at := unixEpoch.Add(time.Duration(micros) * time.Microsecond)
				if at.Before(window[0].Add(-time.Second)) || at.After(window[1].Add(time.Second)) {
					t.Fatalf("%s stamp %s is not the wall clock of the run (%s .. %s)", table, at, window[0], window[1])
				}
			}
			if raw, ok := row["ingest_revision"]; ok {
				got, _ := new(big.Int).SetString(fmt.Sprint(raw), 10)
				want := new(big.Int).Lsh(new(big.Int).SetUint64(lastSyncedMicros), 64)
				want.Or(want, new(big.Int).SetUint64(observedMicros))
				if got == nil || got.Cmp(want) != 0 {
					t.Fatalf("%s row %v ingest_revision %v is not last_synced<<64 | observed_at (%v)", table, row["id"], raw, want)
				}
			}
			for _, key := range stampKeys {
				if _, ok := row[key]; ok {
					row[key] = "<stamp>"
				}
			}
			rows = append(rows, row)
		}
		out[table] = rows
	}
	return out
}

// scenario is one run of the verb. shape is the operational table shape the
// database is migrated to ("" legacy, "2" current); env is the run's contract
// variable ("" unset).
type scenario struct {
	name     string
	shape    string
	org      string
	instance string
	env      string
	envSet   bool
	// message is a text both implementations' stderr carry when the run fails;
	// "" for a run that must succeed.
	message string
	// exitOnly compares only the exit code (the two drivers word a server error
	// differently).
	exitOnly bool
	// goOnly marks a run whose Python side is not comparable: Python creates
	// and migrates the tables when it connects (ensure_tables), so against a
	// database at another shape than the contract it migrates it and succeeds,
	// where dho expects the schema at the head and refuses. Named deviation.
	goOnly bool
}

var scenarios = []scenario{
	{name: "legacy main", org: orgMain},
	{name: "legacy custom instance", org: orgMain, instance: "Atlassian-Ops-2"},
	{name: "legacy empty org", org: orgEmpty},
	{name: "legacy explicit 1", org: orgMain, env: "1", envSet: true},
	{name: "legacy explicit 2 is stale", org: orgMain, env: "2", envSet: true, goOnly: true, message: "operational ordering stale_state table=operational_services configured=2 stored=1"},
	{name: "legacy bad contract", org: orgMain, env: "3", envSet: true, message: "OPERATIONAL_ORDERING_CONTRACT must be exactly '1' or '2', got '3'"},
	{name: "legacy blank contract", org: orgMain, env: "", envSet: true, message: "OPERATIONAL_ORDERING_CONTRACT must be exactly '1' or '2', got ''"},
	{name: "legacy empty identifier", org: orgBad, message: "external_id must be non-empty, got ''"},
	{name: "current main", shape: "2", org: orgMain, env: "2", envSet: true},
	{name: "current custom instance", shape: "2", org: orgMain, instance: "Atlassian-Ops-2", env: "2", envSet: true},
	{name: "current empty org", shape: "2", org: orgEmpty, env: "2", envSet: true},
	{name: "current old writer rejected", shape: "2", org: orgMain, env: "1", envSet: true, message: "legacy operational writer rejected for operational_services service=dev-health-ops version=unknown"},
	{name: "current unset writes the wrong shape", shape: "2", org: orgMain, exitOnly: true, message: "-"},
}

func (s scenario) args() []string {
	args := []string{"--org", s.org}
	if s.instance != "" {
		args = append(args, "--atlassian-provider-instance-id", s.instance)
	}
	return args
}

func (s scenario) environ() []string {
	if !s.envSet {
		return nil
	}
	return []string{ContractEnv + "=" + s.env}
}

func goRun(t *testing.T, ch clickHouse, s scenario) (int, string, string, [2]time.Time) {
	t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": ch.instance.URI}
	if s.envSet {
		env[ContractEnv] = s.env
	}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	start := time.Now()
	code := runOperational(context.Background(), cli.Env{Args: s.args(), Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stderr.String(), "clickhouse://") || strings.Contains(stdout.String(), "clickhouse://") {
		t.Fatalf("output carries the DSN:\n%s", stderr.String())
	}
	return code, stdout.String(), stderr.String(), [2]time.Time{start, time.Now()}
}

// operationalPythonBuild is the build whose Python `dev-hops backfill operational` answered the scenarios: a
// build that still carried the Python CLI.
const operationalPythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"

// operationalPythonProgram is the entry point the producer runs: the real dev-hops CLI.
const operationalPythonProgram = "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"

// operationalPythonSettings are the variables of pyoracle.ClosedEnv that shape the answers, as constants:
// the producer's environment AND part of the golden's request key (a changed value fails the frozen replay).
// The scenario's own contract variable is in its input; the ClickHouse address is a per-run value.
var operationalPythonSettings = map[string]string{"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "TZ": "UTC", "PYTHONHASHSEED": "0", "OTEL_ENABLED": "false"}

func pythonRun(t *testing.T, producer *venueoracle.Producer, ch clickHouse, s scenario) (int, string, string, [2]time.Time) {
	t.Helper()
	// The launcher's closed environment (CHAOS-7471): the settings, the contract the scenario sets (none when it
	// is unset; it is in the scenario's input, so in the golden's key) and the ClickHouse address; nothing
	// inherited: SERVICE_NAME and SERVICE_VERSION stay unset, as the golden's writer-rejection text
	// ("service=dev-health-ops version=unknown") assumes.
	declared := map[string]string{}
	for name, value := range operationalPythonSettings {
		declared[name] = value
	}
	for _, entry := range s.environ() {
		name, value, _ := strings.Cut(entry, "=")
		declared[name] = value
	}
	command, err := producer.Command(context.Background(), declared, []string{"CLICKHOUSE_URI=" + ch.httpDSN}, append([]string{"-c", operationalPythonProgram, "backfill", "operational"}, s.args()...)...)
	if err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	start := time.Now()
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", err)
	}
	return code, stdout.String(), stderr.String(), [2]time.Time{start, time.Now()}
}

// result is what one implementation did for a scenario: the exit code, the
// summary line, and the rows it left, stamps masked.
type result struct {
	Exit   int                  `json:"exit"`
	Stdout string               `json:"stdout"`
	Rows   map[string][]dumpRow `json:"rows"`
}

func checkMessage(t *testing.T, who string, s scenario, code int, stderr string) {
	t.Helper()
	if s.message == "" {
		if code != 0 {
			t.Fatalf("%s: scenario %q exit %d, stderr:\n%s", who, s.name, code, stderr)
		}
		return
	}
	if code != 1 {
		t.Fatalf("%s: scenario %q exit %d, want 1; stderr:\n%s", who, s.name, code, stderr)
	}
	if !s.exitOnly && !strings.Contains(stderr, s.message) && !strings.Contains(strings.ReplaceAll(stderr, `\"`, `"`), s.message) {
		t.Fatalf("%s: scenario %q stderr does not carry %q:\n%s", who, s.name, s.message, stderr)
	}
}

func run(t *testing.T, producer *venueoracle.Producer, ch clickHouse, s scenario, python bool) result {
	t.Helper()
	ch.truncate(t)
	var code int
	var stdout, stderr string
	var window [2]time.Time
	who := "go"
	if python {
		who = "python"
		code, stdout, stderr, window = pythonRun(t, producer, ch, s)
	} else {
		code, stdout, stderr, window = goRun(t, ch, s)
	}
	checkMessage(t, who, s, code, stderr)
	return result{Exit: code, Stdout: stdout, Rows: ch.dump(t, window)}
}

type golden struct {
	Name   string `json:"name"`
	Result result `json:"result"`
}

func canonical(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	// Round-trip through a decoder that keeps numbers verbatim so the compared
	// text is the same for both sides.
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func comparable(group []scenario) []scenario {
	var out []scenario
	for _, s := range group {
		if !s.goOnly {
			out = append(out, s)
		}
	}
	return out
}

func shapes() []string { return []string{"", "2"} }

func forEachShape(t *testing.T, body func(t *testing.T, ch clickHouse, shape string, group []scenario)) {
	t.Helper()
	for _, shape := range shapes() {
		var group []scenario
		for _, s := range scenarios {
			if s.shape == shape {
				group = append(group, s)
			}
		}
		name := "legacy_shape"
		if shape == "2" {
			name = "current_shape"
		}
		t.Run(name, func(t *testing.T) {
			ch := startClickHouse(t, shape)
			seedLegacy(t, ch)
			body(t, ch, shape, group)
		})
	}
}

// TestBackfillOperationalWritesTheFrozenPythonRows runs the verb on a real ClickHouse migrated to each table
// shape and compares the rows it leaves and the line it prints with what the REAL `dev-hops backfill
// operational` left for the same legacy rows. The answers were executed once on operationalPythonBuild and
// are frozen in testdata/golden/backfill_operational.json (the recipe regenerates them by execution); the
// scenarios and the seeded legacy rows are part of the golden's key. The stamps Python takes from the wall
// clock are checked to lie inside the run's window and stored as a placeholder.
func TestBackfillOperationalWritesTheFrozenPythonRows(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	goldenFile := venueoracle.OpenGolden(t, venueoracle.GoldenSpec{
		Path:        "testdata/golden/backfill_operational.json",
		PythonBuild: operationalPythonBuild,
		SHA256:      "0c51bbaf645f96fc403cc9023fbad93313b23c905180095ba0e394d81aa6ac1a",
		Recipe: "git worktree add --detach $DIR " + operationalPythonBuild + " (with its .venv: uv sync --frozen --no-install-project); then from the repository root: " +
			"go run ./internal/testsupport/venueoracle/goldenrecord -pkg ./internal/operationalbackfill/ -test '^TestBackfillOperationalWritesTheFrozenPythonRows$' -python-root $DIR",
	})
	root := goldenFile.PythonRoot(t, repoRoot)

	type keyed struct {
		Name     string   `json:"name"`
		Shape    string   `json:"shape"`
		Args     []string `json:"args"`
		Environ  []string `json:"environ"`
		Message  string   `json:"message"`
		ExitOnly bool     `json:"exitOnly"`
	}
	var keys []keyed
	for _, shape := range shapes() {
		for _, s := range scenarios {
			if s.shape == shape && !s.goOnly {
				keys = append(keys, keyed{Name: s.name, Shape: s.shape, Args: s.args(), Environ: s.environ(), Message: s.message, ExitOnly: s.exitOnly})
			}
		}
	}
	input, err := json.Marshal(map[string]any{"scenarios": keys, "seededLegacyRows": legacySeedDigest()})
	if err != nil {
		t.Fatal(err)
	}
	request := venueoracle.ProgramRequest("backfill operational scenarios", operationalPythonProgram, input, operationalPythonSettings)
	answers := goldenFile.Produce(t, root, []venueoracle.Request{request}, func(producer *venueoracle.Producer, _ []venueoracle.Request) []venueoracle.Response {
		producer.RequireDeployed()
		var produced []golden
		forEachShape(t, func(t *testing.T, ch clickHouse, _ string, group []scenario) {
			for _, s := range comparable(group) {
				py := run(t, producer, ch, s, true)
				if s.message == "" {
					total := 0
					for _, rows := range py.Rows {
						total += len(rows)
					}
					if total == 0 && !strings.Contains(s.name, "empty") {
						t.Fatalf("%s: the producer wrote no rows: the comparison would measure nothing", s.name)
					}
				}
				produced = append(produced, golden{Name: s.name, Result: py})
			}
		})
		body, err := json.Marshal(produced)
		if err != nil {
			t.Fatal(err)
		}
		return []venueoracle.Response{{Status: 0, Body: string(body)}}
	})
	goldenFile.Consumed(t, answers...)
	var frozen []golden
	if err := json.Unmarshal([]byte(answers[0].Body), &frozen); err != nil {
		t.Fatal(err)
	}
	byName := map[string]golden{}
	for _, item := range frozen {
		byName[item.Name] = item
	}
	if want := len(comparable(scenarios)); len(byName) != want {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(byName), want)
	}
	forEachShape(t, func(t *testing.T, ch clickHouse, _ string, group []scenario) {
		for _, s := range comparable(group) {
			want, ok := byName[s.name]
			if !ok {
				t.Fatalf("no frozen scenario %q", s.name)
			}
			got := run(t, nil, ch, s, false)
			if got.Exit != want.Result.Exit {
				t.Fatalf("%s: exit %d, frozen Python %d", s.name, got.Exit, want.Result.Exit)
			}
			if got.Stdout != want.Result.Stdout {
				t.Fatalf("%s: stdout\n%s\nfrozen Python\n%s", s.name, got.Stdout, want.Result.Stdout)
			}
			if a, b := canonical(t, got.Rows), canonical(t, want.Result.Rows); a != b {
				t.Fatalf("%s: rows differ from the frozen Python rows\ngo:     %.600s\npython: %.600s", s.name, a, b)
			}
		}
	})
	// A comparison that froze nothing passes for any implementation: the frozen scenarios that must succeed
	// carry rows.
	for _, item := range frozen {
		if strings.HasSuffix(item.Name, "main") {
			if len(item.Result.Rows["operational_incidents"]) == 0 || len(item.Result.Rows["operational_alerts"]) == 0 || len(item.Result.Rows["operational_on_call_schedules"]) == 0 {
				t.Fatalf("%s froze no rows: %v", item.Name, item.Result.Stdout)
			}
		}
	}
	goldenFile.SkipDiff(t)
	goldenFile.Finish(t)
}

// TestBackfillOperationalRefusesATableShapeThatIsNotTheContract is the part of
// the guard Python cannot be compared on (see scenario.goOnly): the contract
// variable says 2 and the tables are at the legacy shape, so nothing is written.
func TestBackfillOperationalRefusesATableShapeThatIsNotTheContract(t *testing.T) {
	ch := startClickHouse(t, "")
	seedLegacy(t, ch)
	for _, s := range scenarios {
		if !s.goOnly {
			continue
		}
		result := run(t, nil, ch, s, false)
		for table, rows := range result.Rows {
			if len(rows) != 0 {
				t.Fatalf("%s: the refused run left %d rows in %s", s.name, len(rows), table)
			}
		}
	}
}

// TestVerifyReportsEveryMissingIncident proves the parity check can fail: the
// incidents of a batch that was never written are missing, and the error says
// how many, as the Python error does.
func TestVerifyReportsEveryMissingIncident(t *testing.T) {
	ch := startClickHouse(t, "")
	seedLegacy(t, ch)
	conn, err := chstorage.Open(context.Background(), chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	legacy, err := LoadLegacy(ctx, conn, orgMain)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := Map(orgMain, "atlassian-ops", legacy, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Incidents) != 13 {
		t.Fatalf("seed has %d incidents, the check below counts 13", len(batch.Incidents))
	}
	// Nothing written: every incident is missing.
	_, err = Verify(ctx, conn, orgMain, batch, ContractLegacy)
	want := "canonical operational backfill parity verification failed: missing incidents=13, missing service_repository_mappings=0; canonical backfill is incomplete"
	if err == nil || err.Error() != want {
		t.Fatalf("Verify with nothing written = %v, want %s", err, want)
	}
	// Alerts and schedules written, incidents not: still all missing.
	batchWithoutIncidents := Batch{Alerts: batch.Alerts, Schedules: batch.Schedules}
	if err := Write(ctx, conn, batchWithoutIncidents, ContractLegacy); err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(ctx, conn, orgMain, batch, ContractLegacy); err == nil {
		t.Fatal("Verify passed with the incidents unwritten")
	}
	// Written: the parity holds and counts every identity.
	if err := Write(ctx, conn, batch, ContractLegacy); err != nil {
		t.Fatal(err)
	}
	result, err := Verify(ctx, conn, orgMain, batch, ContractLegacy)
	if err != nil || !result.ParityVerified() || result.VerifiedIncidents != 13 {
		t.Fatalf("Verify after the write = %+v, %v", result, err)
	}
}

// TestBackfillOperationalPartialWriteIsNamedAndRerunnable is the named
// limitation of a writer with no cross-table transaction: when the last stage
// fails the error names it, the stages before it stay written, and running again
// writes the same identities (nothing doubles).
func TestBackfillOperationalPartialWriteIsNamedAndRerunnable(t *testing.T) {
	ch := startClickHouse(t, "")
	seedLegacy(t, ch)
	// A constraint no row satisfies makes the last stage fail when it writes, while
	// the table still passes the shape guard.
	ch.do(t, "ALTER TABLE operational_on_call_schedules ADD CONSTRAINT never CHECK name = 'no schedule has this name'")
	s := scenarios[0]
	code, _, stderr, _ := goRun(t, ch, s)
	if code != 1 || !strings.Contains(stderr, "write stage operational_on_call_schedules failed") ||
		!strings.Contains(stderr, "stages before it are written") {
		t.Fatalf("exit %d, stderr does not name the failed stage:\n%s", code, stderr)
	}
	count := func(table string) string {
		return strings.TrimSpace(ch.do(t, "SELECT count() FROM "+table+" FINAL WHERE org_id = '"+orgMain+"'"))
	}
	if count("operational_incidents") != "13" || count("operational_alerts") != "8" {
		t.Fatalf("the stages before the failure were not written: incidents %s alerts %s", count("operational_incidents"), count("operational_alerts"))
	}
	// Remove the constraint and run again: the same identities, no duplicates.
	ch.do(t, "ALTER TABLE operational_on_call_schedules DROP CONSTRAINT never")
	code, stdout, stderr, _ := goRun(t, ch, s)
	if code != 0 {
		t.Fatalf("re-run exit %d:\n%s", code, stderr)
	}
	if !strings.Contains(stdout, "incidents=13, alerts=8, schedules=2") || count("operational_incidents") != "13" || count("operational_alerts") != "8" || count("operational_on_call_schedules") != "2" {
		t.Fatalf("re-run left %s incidents, %s alerts, %s schedules\n%s", count("operational_incidents"), count("operational_alerts"), count("operational_on_call_schedules"), stdout)
	}
}

// TestBackfillOperationalRefusesAMalformedTableShapeWhateverTheContract: with the
// contract variable unset (legacy) a table that has only some of the ordering
// columns is neither shape. Python wrote into it (the rows got revision 0); dho
// refuses before anything is written.
func TestBackfillOperationalRefusesAMalformedTableShapeWhateverTheContract(t *testing.T) {
	ch := startClickHouse(t, "")
	seedLegacy(t, ch)
	ch.do(t, "ALTER TABLE operational_alerts ADD COLUMN source_revision UInt128 DEFAULT 0")
	s := scenarios[0]
	code, _, stderr, _ := goRun(t, ch, s)
	if code != 1 || !strings.Contains(stderr, "operational ordering stale_state table=operational_alerts") {
		t.Fatalf("exit %d, stderr:\n%s", code, stderr)
	}
	for _, table := range operationalTables {
		if count := strings.TrimSpace(ch.do(t, "SELECT count() FROM "+table)); count != "0" {
			t.Fatalf("the refused run left %s rows in %s", count, table)
		}
	}
}

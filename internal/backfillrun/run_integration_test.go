//go:build integration

package backfillrun

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	testOrg   = "c0ffee00-dead-4bee-8bad-f00dfeedface"
	otherOrg  = "c0ffee00-dead-4bee-8bad-f00dfeed0002"
	fixedNow  = "2026-03-10T12:00:00.123456+00:00"
	goldenDir = "testdata/backfill_run_golden.json"
	goldenEnv = "DHO_BACKFILL_RUN_GOLDEN_UPDATE"
)

func startDatabase(t *testing.T) (*containers.Instance, *pgx.Conn) {
	t.Helper()
	instance, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	conn, err := pgx.Connect(context.Background(), instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	return instance, conn
}

// freshDatabase creates a database at the PostgreSQL head baseline.
func freshDatabase(t *testing.T, instance *containers.Instance, admin *pgx.Conn) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	name := "dho_backfill_" + hex.EncodeToString(suffix)
	if _, err := admin.Exec(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + name
	uri := parsed.String()
	conn, err := pgx.Connect(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
		t.Fatalf("apply the baseline: %v", err)
	}
	return uri
}

// cfg is one sync configuration to seed.
type cfg struct {
	n              int // numbers the ids: the run's identity depends on the configuration id
	org            string
	name           string
	provider       string
	targets        []string
	options        map[string]any
	active         bool
	plannerManaged bool
	noIntegration  bool
	withSource     bool // pinned to one source (source_id set)
	sharedWith     int  // the integration of another cfg (a second parent)
	inactive       bool // the integration is not active
	noEnabled      bool // every source of the integration is disabled
	createdAt      string
}

func uuidN(kind, n int) string { return fmt.Sprintf("%08x-0000-4000-8000-%012d", kind, n) }

func (c cfg) id() string { return uuidN(0xc0, c.n) }

func seed(t *testing.T, uri string, configs []cfg) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	seenIntegration := map[int]bool{}
	for _, c := range configs {
		integration := c.n
		if c.sharedWith != 0 {
			integration = c.sharedWith
		}
		if !c.noIntegration && !seenIntegration[integration] {
			seenIntegration[integration] = true
			org := c.org
			if _, err := conn.Exec(ctx, `INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, '{}'::json, $5, now(), now())`, uuidN(0x1a, integration), org, c.provider, fmt.Sprintf("integration-%d", integration), !c.inactive); err != nil {
				t.Fatalf("seed integration: %v", err)
			}
			for index, enabled := range []bool{!c.noEnabled, !c.noEnabled, false} {
				if _, err := conn.Exec(ctx, `INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'repo', $5, $5, $5, '{}'::json, $6, now(), now())`,
					uuidN(0x5a, integration*10+index), org, uuidN(0x1a, integration), c.provider, fmt.Sprintf("org/repo-%d-%d", integration, index), enabled); err != nil {
					t.Fatalf("seed source: %v", err)
				}
			}
		}
		targets, _ := json.Marshal(c.targets)
		if c.targets == nil {
			targets = []byte("[]")
		}
		options, _ := json.Marshal(c.options)
		if c.options == nil {
			options = []byte("{}")
		}
		var integrationID, sourceID any
		if !c.noIntegration {
			integrationID = uuidN(0x1a, integration)
		}
		if c.withSource {
			sourceID = uuidN(0x5a, integration*10)
		}
		created := c.createdAt
		if created == "" {
			created = "2026-01-01T00:00:00Z"
		}
		if _, err := conn.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, source_id, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, $5::json, $6::json, $7, $8, $9::uuid, $10::uuid, $11::timestamptz, $11::timestamptz)`,
			c.id(), c.org, c.name, c.provider, string(targets), string(options), c.active, c.plannerManaged, integrationID, sourceID, created); err != nil {
			t.Fatalf("seed config %s: %v", c.name, err)
		}
	}
}

// scenario is one run of the verb over the seeded configurations.
type scenario struct {
	name string
	cfg  int      // the configuration the run names; 0 uses configID
	id   string   // an explicit --config-id (not seeded)
	args []string // flags after --config-id
	// message is text both implementations' stderr carry when the run fails;
	// "" is a run that must succeed.
	message string
	// goOnly marks a run Python does not refuse but this seam does (or the
	// reverse): only dho is run.
	goOnly  bool
	goExit  int
	preJobs bool // a scheduled_jobs marker already exists for the configuration
}

var configs = []cfg{
	{n: 1, org: testOrg, name: "gh parent cron", provider: "github", targets: []string{"git", "prs"}, options: map[string]any{"schedule_cron": "*/5 * * * *", "timezone": "Europe/Berlin"}, active: true, plannerManaged: true},
	{n: 2, org: testOrg, name: "gh parent paused", provider: "GitHub", targets: []string{"git", "cicd", "deployments"}, active: false, plannerManaged: true},
	{n: 3, org: testOrg, name: "gitlab git", provider: "gitlab", targets: []string{"git"}, options: map[string]any{"timezone": ""}, active: true, plannerManaged: true},
	{n: 4, org: testOrg, name: "jira", provider: "jira", targets: []string{"work-items", "operational"}, options: map[string]any{"schedule_cron": "0 3 * * *"}, active: true, plannerManaged: true},
	{n: 5, org: testOrg, name: "linear", provider: "linear", targets: []string{"work-items"}, active: true, plannerManaged: true},
	{n: 6, org: testOrg, name: "pagerduty", provider: "pagerduty", targets: []string{"operational"}, active: true, plannerManaged: true},
	{n: 7, org: testOrg, name: "launchdarkly", provider: "launchdarkly", targets: []string{"feature-flags"}, active: true, plannerManaged: true},
	{n: 8, org: testOrg, name: "no targets", provider: "github", targets: nil, active: true, plannerManaged: true},
	{n: 9, org: testOrg, name: "pinned to a source", provider: "github", targets: []string{"prs"}, active: true, withSource: true},
	{n: 10, org: testOrg, name: "unrouted", provider: "github", targets: []string{"git"}, active: true},
	{n: 11, org: testOrg, name: "bad target", provider: "github", targets: []string{"git", "bogus", "bogus", "Zed"}, active: true, plannerManaged: true},
	{n: 12, org: testOrg, name: "no integration", provider: "github", targets: []string{"git"}, active: true, plannerManaged: true, noIntegration: true},
	{n: 13, org: testOrg, name: "second parent", provider: "github", targets: []string{"git"}, active: true, plannerManaged: true, sharedWith: 1, createdAt: "2026-01-02T00:00:00Z"},
	{n: 14, org: testOrg, name: "it's quoted", provider: "github", targets: []string{"git", "nope"}, active: true, plannerManaged: true},
	{n: 16, org: testOrg, name: "jira incidents", provider: "jira", targets: []string{"incidents"}, active: true, plannerManaged: true},
	{n: 17, org: testOrg, name: "inactive integration", provider: "github", targets: []string{"git"}, active: true, plannerManaged: true, inactive: true},
	{n: 18, org: testOrg, name: "no enabled sources", provider: "github", targets: []string{"prs"}, active: true, plannerManaged: true, noEnabled: true},
	{n: 19, org: testOrg, name: "planner-managed and pinned", provider: "github", targets: []string{"prs"}, active: true, plannerManaged: true, withSource: true},
	{n: 15, org: testOrg, name: "gh all targets", provider: "github", targets: []string{"git", "prs", "blame", "cicd", "deployments", "security", "tests", "work-items"}, active: true, plannerManaged: true},
}

var scenarios = []scenario{
	{name: "github parent, a three-day window", cfg: 1, args: []string{"--since", "2026-03-01", "--before", "2026-03-04"}},
	{name: "github parent, backfill days", cfg: 1, args: []string{"--backfill", "7", "--before", "2026-03-10"}},
	{name: "github parent with a marker job already there", cfg: 1, args: []string{"--backfill", "1", "--before", "2026-03-10"}, preJobs: true},
	{name: "paused github parent, mixed-case provider", cfg: 2, args: []string{"--since", "2026-02-27", "--before", "2026-03-01"}},
	{name: "gitlab git adds blame", cfg: 3, args: []string{"--backfill", "2", "--before", "2026-03-10"}},
	{name: "jira", cfg: 4, args: []string{"--backfill", "1", "--before", "2026-03-11"}},
	{name: "linear", cfg: 5, args: []string{"--backfill", "30", "--before", "2026-03-10"}},
	{name: "pagerduty", cfg: 6, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "launchdarkly", cfg: 7, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "no targets selects every dataset", cfg: 8, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "child pinned to one source", cfg: 9, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "every github target", cfg: 15, args: []string{"--since", "2025-12-25", "--before", "2026-01-01"}},
	{name: "the org assertion holds", cfg: 1, args: []string{"--org", testOrg, "--backfill", "1", "--before", "2026-03-10"}},
	{name: "the org assertion fails", cfg: 1, args: []string{"--org", otherOrg}, message: "Org mismatch: --org " + otherOrg + " does not own sync config"},
	{name: "no such configuration", id: "00000000-0000-4000-8000-00000000dead", message: "Sync configuration not found: 00000000-0000-4000-8000-00000000dead"},
	{name: "not a uuid", id: "not-a-uuid", message: "badly formed hexadecimal UUID string"},
	{name: "unrecognized targets", cfg: 11, message: "has sync_targets ['Zed', 'bogus'] that provider 'github' does not recognize"},
	{name: "jira incidents are the operational target", cfg: 16, message: "has sync_targets ['incidents'] that provider 'jira' does not recognize"},
	{name: "a name that needs quotes in the message", cfg: 14, message: `("it's quoted") has sync_targets ['nope']`},
	{name: "no integration", cfg: 12, message: "has no integration_id; cannot plan a backfill"},
	{name: "not the canonical parent", cfg: 13, message: "is not the config the shared reference-discovery resolver"},
	{name: "an integration without enabled sources", cfg: 18, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "planner-managed configuration pinned to a source", cfg: 19, args: []string{"--backfill", "1", "--before", "2026-03-10"}},
	{name: "inactive integration", cfg: 17, goOnly: true, goExit: cli.ExitRefused, message: "is not active, and the scheduler materializes only an active integration"},
	{name: "unrouted configuration", cfg: 10, goOnly: true, goExit: cli.ExitRefused, message: "is neither planner-managed nor pinned to one source"},
}

func (s scenario) configID() string {
	if s.id != "" {
		return s.id
	}
	for _, c := range configs {
		if c.n == s.cfg {
			return c.id()
		}
	}
	return ""
}

func (s scenario) argv() []string {
	return append([]string{"--config-id", s.configID(), "--no-wait"}, s.args...)
}

func goRun(t *testing.T, uri string, s scenario) (int, string, string) {
	t.Helper()
	fixed, err := time.Parse(time.RFC3339Nano, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	saved := clock
	clock = func() time.Time { return fixed }
	defer func() { clock = saved }()
	env := map[string]string{"MIGRATION_DATABASE_URI": uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	var stdout, stderr bytes.Buffer
	code := runVerb(context.Background(), cli.Env{Args: s.argv(), Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stderr.String(), "postgres://") || strings.Contains(stdout.String(), "postgres://") {
		t.Fatalf("output carries the DSN:\n%s", stderr.String())
	}
	return code, stdout.String(), stderr.String()
}

// livePythonProgram runs the real `dev-hops backfill run`, with the planner
// replaced by a recorder (the verb plans in the CLI process; the request it
// builds is the thing recorded), then hands the recorded request to the real
// hand-off function that writes the scheduler's occurrence and trigger rows,
// at a fixed clock.
const livePythonProgram = `
import dataclasses, json, sys, uuid
from datetime import datetime
spec = json.load(sys.stdin)
import dev_health_ops.sync.execution_trigger as et
fixed = datetime.fromisoformat(spec["now"])
class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        return fixed
et.datetime = FixedDatetime
import dev_health_ops.sync.planner as planner
captured = {}
class Plan:
    dispatch_required = False
    sync_run_id = ""
    total_units = 0
    unit_ids = ()
    terminal_reason = ""
def fake_plan(session, request):
    captured["request"] = request
    return Plan()
planner.plan_sync_run = fake_plan
from dev_health_ops import cli
code = cli.main(["backfill", "run", *spec["args"]])
if code == 0 and "request" in captured:
    from dev_health_ops.db import get_postgres_session_sync
    from dev_health_ops.models.settings import SyncConfiguration
    with get_postgres_session_sync() as session:
        config = session.get(SyncConfiguration, uuid.UUID(spec["config_id"]))
        request = dataclasses.replace(captured["request"], triggered_by="backfill")
        if config.planner_managed and config.source_id is None:
            # The one named difference: Python planned source_ids=None as "every
            # enabled source"; the scheduler reads NULL on a planner-managed
            # configuration as "the sources tagged for it", so dho names them.
            from dev_health_ops.models.integrations import IntegrationSource
            enabled = (
                session.query(IntegrationSource)
                .filter(
                    IntegrationSource.org_id == config.org_id,
                    IntegrationSource.integration_id == config.integration_id,
                    IntegrationSource.is_enabled.is_(True),
                )
                .order_by(IntegrationSource.full_name, IntegrationSource.id)
                .all()
            )
            request = dataclasses.replace(request, source_ids=tuple(str(row.id) for row in enabled))
        et._create_go_manual_sync_execution_trigger(session, config, str(config.org_id), request)
        session.commit()
raise SystemExit(code)
`

func pythonRun(t *testing.T, uri string, s scenario) (int, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	input, err := json.Marshal(map[string]any{"args": append([]string{"--config-id", s.configID()}, s.args...), "config_id": s.configID(), "now": fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	pyURI := strings.Replace(uri, "postgres://", "postgresql://", 1)
	command := exec.Command(python, "-c", livePythonProgram)
	command.Stdin = bytes.NewReader(input)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "POSTGRES_URI="+pyURI, "DATABASE_URI="+pyURI, "OTEL_ENABLED=false")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	command.Stdout = &bytes.Buffer{}
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", pyoracle.RunError(python, err, []byte(stderr.String())))
	}
	return code, stderr.String()
}

var (
	uuidPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	timestampPattern = regexp.MustCompile(`^\d{4}-\d\d-\d\d[T ]\d\d:\d\d:\d\d`)
	// exactKeys are the columns whose value is the point of the comparison and
	// is fixed by the run: the identity and the instants.
	exactKeys = map[string]bool{"scheduled_for": true, "since": true, "before": true}
)

func mask(key string, value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, item := range typed {
			out[k] = mask(k, item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for index, item := range typed {
			out[index] = mask(key, item)
		}
		return out
	case string:
		if exactKeys[key] {
			parsed, err := time.Parse(time.RFC3339Nano, strings.Replace(typed, " ", "T", 1))
			if err == nil {
				return parsed.UTC().Format("2006-01-02T15:04:05.000000Z")
			}
			return typed
		}
		switch {
		case uuidPattern.MatchString(typed):
			return "<uuid>"
		case timestampPattern.MatchString(typed):
			return "<ts>"
		}
	}
	return value
}

// rows is the masked content of the three tables the verb writes, as sorted
// JSON lines.
func rows(t *testing.T, uri string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	out := map[string][]string{}
	for _, table := range []string{"scheduled_jobs", "scheduled_sync_occurrences", "sync_manual_triggers"} {
		result, err := conn.Query(ctx, fmt.Sprintf(`SELECT to_jsonb(t)::text FROM public.%s t`, table))
		if err != nil {
			t.Fatal(err)
		}
		texts, err := pgx.CollectRows(result, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		for _, text := range texts {
			var decoded any
			if err := json.Unmarshal([]byte(text), &decoded); err != nil {
				t.Fatal(err)
			}
			line, err := json.Marshal(mask("", decoded))
			if err != nil {
				t.Fatal(err)
			}
			out[table] = append(out[table], string(line))
		}
		sort.Strings(out[table])
	}
	return out
}

func reset(t *testing.T, uri string, s scenario) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, statement := range []string{"DELETE FROM sync_manual_triggers", "DELETE FROM scheduled_sync_occurrences", "DELETE FROM scheduled_jobs"} {
		if _, err := conn.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if s.preJobs {
		if _, err := conn.Exec(ctx, `INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status, created_at, updated_at)
VALUES (gen_random_uuid(), $1, $2, 'sync', 'github', '15 * * * *', 'UTC', '{}'::json, $3::uuid, 1, now(), now())`, testOrg, "sync-config-"+s.configID(), s.configID()); err != nil {
			t.Fatal(err)
		}
	}
}

type result struct {
	Name string              `json:"name"`
	Exit int                 `json:"exit"`
	Rows map[string][]string `json:"rows"`
}

func checkRefusal(t *testing.T, who string, s scenario, code int, stderr string) {
	t.Helper()
	if s.message == "" {
		if code != 0 {
			t.Fatalf("%s: %q exit %d, stderr:\n%s", who, s.name, code, stderr)
		}
		return
	}
	want := 1
	if who == "go" && s.goExit != 0 {
		want = s.goExit
	}
	if code != want {
		t.Fatalf("%s: %q exit %d, want %d; stderr:\n%s", who, s.name, code, want, stderr)
	}
	if !strings.Contains(stderr, s.message) && !strings.Contains(strings.ReplaceAll(stderr, `\"`, `"`), s.message) {
		t.Fatalf("%s: %q stderr does not carry %q:\n%s", who, s.name, s.message, stderr)
	}
}

func setup(t *testing.T) string {
	t.Helper()
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	seed(t, uri, configs)
	return uri
}

func loadGolden(t *testing.T) []result {
	t.Helper()
	raw, err := os.ReadFile(goldenDir)
	if err != nil {
		t.Fatal(err)
	}
	var out []result
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func comparable() []scenario {
	var out []scenario
	for _, s := range scenarios {
		if !s.goOnly {
			out = append(out, s)
		}
	}
	return out
}

// TestBackfillRunWritesTheFrozenPythonRows runs the verb on a real PostgreSQL at
// the head baseline and compares the rows it leaves with the rows the real
// Python hand-off wrote for the same configurations (testdata, R24). It runs
// without Python.
func TestBackfillRunWritesTheFrozenPythonRows(t *testing.T) {
	frozen := map[string]result{}
	for _, item := range loadGolden(t) {
		frozen[item.Name] = item
	}
	if want := len(comparable()); len(frozen) != want {
		t.Fatalf("golden has %d scenarios, the test defines %d", len(frozen), want)
	}
	uri := setup(t)
	for _, s := range comparable() {
		want, ok := frozen[s.name]
		if !ok {
			t.Fatalf("no frozen scenario %q", s.name)
		}
		reset(t, uri, s)
		code, stdout, stderr := goRun(t, uri, s)
		checkRefusal(t, "go", s, code, stderr)
		if s.message == "" && !strings.HasPrefix(stdout, "Backfill queued: occurrence_id=sha256:") {
			t.Fatalf("%s: stdout %q", s.name, stdout)
		}
		got := result{Name: s.name, Exit: code, Rows: rows(t, uri)}
		if got.Exit != want.Exit {
			t.Fatalf("%s: exit %d, frozen Python %d", s.name, got.Exit, want.Exit)
		}
		if a, b := canonicalRows(t, got.Rows), canonicalRows(t, want.Rows); a != b {
			t.Fatalf("%s: rows differ from the frozen Python rows\ngo:     %.700s\npython: %.700s", s.name, a, b)
		}
	}
}

func canonicalRows(t *testing.T, value map[string][]string) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// TestBackfillRunActuallyWrites guards the comparison against measuring
// nothing: every scenario that must succeed froze exactly one occurrence, one
// backfill trigger and one marker job.
func TestBackfillRunActuallyWrites(t *testing.T) {
	for _, item := range loadGolden(t) {
		ok := len(item.Rows["scheduled_sync_occurrences"]) == 1
		if item.Exit == 0 && (!ok || len(item.Rows["sync_manual_triggers"]) != 1 || len(item.Rows["scheduled_jobs"]) != 1) {
			t.Fatalf("%s froze no rows: %v", item.Name, item.Rows)
		}
		// A planner-managed parent names its enabled sources (never NULL): the
		// scheduler would otherwise read NULL as "the sources tagged for this
		// configuration" and could plan zero units.
		if item.Name == "github parent, a three-day window" {
			trigger := strings.Join(item.Rows["sync_manual_triggers"], "")
			if !strings.Contains(trigger, `"source_ids":["\u003cuuid\u003e","\u003cuuid\u003e"]`) {
				t.Fatalf("%s did not freeze the enabled source ids: %s", item.Name, trigger)
			}
		}
		if item.Name == "an integration without enabled sources" && !strings.Contains(strings.Join(item.Rows["sync_manual_triggers"], ""), `"source_ids":[]`) {
			t.Fatalf("%s did not freeze an empty source list: %v", item.Name, item.Rows["sync_manual_triggers"])
		}
		if item.Exit != 0 && len(item.Rows["scheduled_sync_occurrences"]) != 0 {
			t.Fatalf("%s: a refused run left rows: %v", item.Name, item.Rows)
		}
	}
}

// TestBackfillRunRefusesAnUnroutedConfiguration is the part Python cannot be
// compared on: Python plans any integration-linked configuration in its own
// process; the scheduler materializes only a planner-managed configuration or
// one pinned to a source, so this seam refuses the rest and writes nothing.
func TestBackfillRunRefusesAnUnroutedConfiguration(t *testing.T) {
	uri := setup(t)
	for _, s := range scenarios {
		if !s.goOnly {
			continue
		}
		reset(t, uri, s)
		code, _, stderr := goRun(t, uri, s)
		checkRefusal(t, "go", s, code, stderr)
		for table, lines := range rows(t, uri) {
			if len(lines) != 0 {
				t.Fatalf("%s: the refused run left %d rows in %s", s.name, len(lines), table)
			}
		}
	}
}

// TestBackfillRunVenueOracleMatchesThePythonProducer runs every comparable
// scenario through the real `dev-hops backfill run` and its hand-off writer and
// through dho, and compares the exit code, the refusal text and every column of
// the rows. With DHO_BACKFILL_RUN_GOLDEN_UPDATE=1 it rewrites the frozen file.
func TestBackfillRunVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	uri := setup(t)
	var frozen []result
	for _, s := range comparable() {
		reset(t, uri, s)
		pyCode, pyErr := pythonRun(t, uri, s)
		checkRefusal(t, "python", s, pyCode, pyErr)
		py := result{Name: s.name, Exit: pyCode, Rows: rows(t, uri)}
		reset(t, uri, s)
		goCode, _, goErr := goRun(t, uri, s)
		checkRefusal(t, "go", s, goCode, goErr)
		goResult := result{Name: s.name, Exit: goCode, Rows: rows(t, uri)}
		if py.Exit != goResult.Exit {
			t.Fatalf("%s: python exit %d, go exit %d", s.name, py.Exit, goResult.Exit)
		}
		if a, b := canonicalRows(t, py.Rows), canonicalRows(t, goResult.Rows); a != b {
			t.Fatalf("%s: rows differ\npython: %s\ngo:     %s", s.name, a, b)
		}
		frozen = append(frozen, py)
	}
	if os.Getenv(goldenEnv) == "1" {
		raw, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenDir, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

// plannedOccurrence starts the first scenario's run and returns its occurrence
// id, the pool, and a func that plays the scheduler: it links a planned run to
// the occurrence, or quarantines it.
func plannedOccurrence(t *testing.T) (pool *pgxpool.Pool, occurrenceID string, plan func(units int), quarantine func(code string)) {
	t.Helper()
	uri := setup(t)
	var err error
	pool, err = pgxpool.New(context.Background(), uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := scenarios[0]
	window, err := ResolveWindow(day("2026-03-01"), day("2026-03-04"), 1, *day("2026-03-10"))
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := Start(context.Background(), pool, s.configID(), Params{ConfigID: s.configID(), Window: window}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	plan = func(units int) {
		ctx := context.Background()
		var jobID string
		if err := pool.QueryRow(ctx, `SELECT scheduled_job_id::text FROM scheduled_sync_occurrences WHERE occurrence_id = $1`, trigger.OccurrenceID).Scan(&jobID); err != nil {
			t.Error(err)
			return
		}
		runID, jobRunID := uuidN(0x2a, 1), uuidN(0x3a, 1)
		if _, err := pool.Exec(ctx, `INSERT INTO job_runs (id, job_id, triggered_by, status, created_at) VALUES ($1::uuid, $2::uuid, 'backfill', 0, now())`, jobRunID, jobID); err != nil {
			t.Error(err)
			return
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_runs (id, org_id, integration_id, triggered_by, mode, status, total_units, completed_units, failed_units, created_at)
VALUES ($1::uuid, $2, $3::uuid, 'backfill', 'backfill', 'planned', $4, 0, 0, now())`, runID, testOrg, uuidN(0x1a, 1), units); err != nil {
			t.Error(err)
			return
		}
		if _, err := pool.Exec(ctx, `UPDATE scheduled_sync_occurrences SET job_run_id = $2::uuid, sync_run_id = $3::uuid, reconcile_status = 'completed' WHERE occurrence_id = $1`, trigger.OccurrenceID, jobRunID, runID); err != nil {
			t.Error(err)
		}
	}
	quarantine = func(code string) {
		if _, err := pool.Exec(context.Background(), `UPDATE scheduled_sync_occurrences SET reconcile_status = 'quarantined', reconcile_error_code = $2, reconcile_error_at = now() WHERE occurrence_id = $1`, trigger.OccurrenceID, code); err != nil {
			t.Error(err)
		}
	}
	return pool, trigger.OccurrenceID, plan, quarantine
}

// TestWaitFollowsTheSchedulerToTheOutcome is the wait's behaviour, not its
// return type: a planned run is reported with its unit count, a quarantine with
// its code, and an occurrence nobody picks up is pending (never an error).
func TestWaitFollowsTheSchedulerToTheOutcome(t *testing.T) {
	pool, occurrenceID, plan, quarantine := plannedOccurrence(t)
	ctx := context.Background()

	pending, err := Wait(ctx, pool, occurrenceID, 400*time.Millisecond, 50*time.Millisecond)
	if err != nil || pending.State != StatePending {
		t.Fatalf("nothing picked the occurrence up: %+v, %v", pending, err)
	}

	go func() { time.Sleep(300 * time.Millisecond); quarantine("ineligible") }()
	quarantined, err := Wait(ctx, pool, occurrenceID, 10*time.Second, 50*time.Millisecond)
	if err != nil || quarantined.State != StateQuarantined || quarantined.ErrorCode != "ineligible" {
		t.Fatalf("a quarantined occurrence: %+v, %v", quarantined, err)
	}

	if _, err := pool.Exec(ctx, `UPDATE scheduled_sync_occurrences SET reconcile_status = 'pending', reconcile_error_code = NULL, reconcile_error_at = NULL WHERE occurrence_id = $1`, occurrenceID); err != nil {
		t.Fatal(err)
	}
	go func() { time.Sleep(300 * time.Millisecond); plan(42) }()
	planned, err := Wait(ctx, pool, occurrenceID, 10*time.Second, 50*time.Millisecond)
	if err != nil || planned.State != StateMaterialized || planned.TotalUnits != 42 || planned.SyncRunID != uuidN(0x2a, 1) {
		t.Fatalf("a planned run: %+v, %v", planned, err)
	}
}

// TestVerbReportsWhatTheSchedulerDid runs the verb end to end: it prints the
// occurrence id at once with --no-wait, prints a pending line and exits 0 when
// the scheduler is slow, and fails with the quarantine code when the scheduler
// rejects the occurrence.
func TestVerbReportsWhatTheSchedulerDid(t *testing.T) {
	uri := setup(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	env := map[string]string{"MIGRATION_DATABASE_URI": uri}
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	run := func(args ...string) (int, string, string) {
		var stdout, stderr bytes.Buffer
		code := runVerb(ctx, cli.Env{Args: append([]string{"--config-id", configs[0].id(), "--before", "2026-03-04"}, args...), Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
		return code, stdout.String(), stderr.String()
	}
	code, stdout, stderr := run("--no-wait")
	if code != 0 || !strings.HasPrefix(stdout, "Backfill queued: occurrence_id=sha256:") {
		t.Fatalf("--no-wait: exit %d, stdout %q, stderr %s", code, stdout, stderr)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM sync_manual_triggers; DELETE FROM scheduled_sync_occurrences`); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	code, stdout, stderr = run("--wait-seconds", "1")
	if code != 0 || !strings.Contains(stdout, "Backfill pending: occurrence_id=sha256:") || time.Since(started) < time.Second {
		t.Fatalf("a slow scheduler: exit %d after %s, stdout %q, stderr %s", code, time.Since(started), stdout, stderr)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM sync_manual_triggers; DELETE FROM scheduled_sync_occurrences`); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(500 * time.Millisecond)
		_, _ = pool.Exec(ctx, `UPDATE scheduled_sync_occurrences SET reconcile_status = 'quarantined', reconcile_error_code = 'invalid_plan', reconcile_error_at = now()`)
	}()
	code, stdout, stderr = run("--wait-seconds", "10")
	if code != 1 || stdout != "" || !strings.Contains(stderr, "scheduled sync occurrence quarantined: invalid_plan") {
		t.Fatalf("a quarantined occurrence: exit %d, stdout %q, stderr %s", code, stdout, stderr)
	}
}

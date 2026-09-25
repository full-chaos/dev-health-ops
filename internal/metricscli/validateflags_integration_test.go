//go:build integration

package metricscli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	orgBad     = "aaaaaaaa-0000-4000-8000-000000000001"
	orgGood    = "bbbbbbbb-0000-4000-8000-000000000002"
	orgEmpty   = "cccccccc-0000-4000-8000-000000000003"
	orgDrifty  = "dddddddd-0000-4000-8000-000000000004"
	orgLowConf = "eeeeeeee-0000-4000-8000-000000000005"
	orgJoin    = "ffffffff-0000-4000-8000-000000000006"
	orgHalf    = "99999999-0000-4000-8000-000000000007"
	orgMid     = "88888888-0000-4000-8000-000000000008"
	orgFifty   = "77777777-0000-4000-8000-000000000009"
	orgSolo    = "66666666-0000-4000-8000-00000000000a"
)

type clickHouse struct {
	instance *containers.Instance
	httpDSN  string
}

func startClickHouse(t *testing.T) clickHouse {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	return clickHouse{instance: instance, httpDSN: dsn}
}

func (ch clickHouse) exec(t *testing.T, statement string) {
	t.Helper()
	parsed, err := url.Parse(ch.httpDSN)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	request, err := http.NewRequest(http.MethodPost, "http://"+parsed.Host+"/?database="+strings.TrimPrefix(parsed.Path, "/"), strings.NewReader(statement))
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
}

// seed writes five organizations whose reports cover every status of every
// check: orgBad (critical coverage, critical dedup, warn schema, drift spikes,
// critical join, warn confidence), orgGood (everything ok), orgEmpty (nothing:
// every check that can skip skips), orgDrifty (a smooth series and a spike series),
// orgLowConf (very low confidence only).
func seed(t *testing.T, ch clickHouse) {
	t.Helper()
	deployments := func(org string, releases int) {
		ch.exec(t, fmt.Sprintf(`INSERT INTO deployments (repo_id, deployment_id, release_ref, environment, started_at, deployed_at, org_id, last_synced)
			SELECT generateUUIDv4(), concat('d-%[1]s-', toString(number)), concat('r', toString(number)), 'prod', now64(3) - INTERVAL 2 DAY, now64(3) - INTERVAL 2 DAY, '%[1]s', now64(3) FROM numbers(1, %[2]d)`, org, releases))
	}
	buckets := func(org, release string, daysAgo, count int, keyPrefix string) {
		ch.exec(t, fmt.Sprintf(`INSERT INTO telemetry_signal_bucket (org_id, signal_type, signal_count, session_count, environment, repo_id, release_ref, bucket_start, bucket_end, ingested_at, schema_version, dedupe_key)
			SELECT '%[1]s', 'x', 1, 1, 'prod', 'repo', '%[2]s', toStartOfDay(now64(3)) - INTERVAL %[3]d DAY + INTERVAL 1 HOUR, toStartOfDay(now64(3)) - INTERVAL %[3]d DAY + INTERVAL 2 HOUR, now64(3), 'v1', concat('%[5]s', '%[2]s', '-', toString(%[3]d), '-', toString(number)) FROM numbers(%[4]d)`, org, release, daysAgo, count, keyPrefix))
	}
	covered := func(org string, releases int, keyPrefix string) {
		for release := 1; release <= releases; release++ {
			buckets(org, fmt.Sprintf("r%d", release), 2, 1, keyPrefix)
		}
	}
	flags := func(org string, complete, incomplete int) {
		ch.exec(t, fmt.Sprintf(`INSERT INTO feature_flag (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, last_synced)
			SELECT '%[1]s', 'launchdarkly', concat('f', toString(number)), 'p', 'repo', 'prod', 'release', now64(3), now64(3) FROM numbers(%[2]d)`, org, complete))
		if incomplete > 0 {
			ch.exec(t, fmt.Sprintf(`INSERT INTO feature_flag (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, last_synced)
				SELECT '%[1]s', 'launchdarkly', concat('g', toString(number)), 'p', 'repo', 'prod', '', now64(3), now64(3) FROM numbers(%[2]d)`, org, incomplete))
		}
	}
	events := func(org string, unique, duplicates int) {
		ch.exec(t, fmt.Sprintf(`INSERT INTO feature_flag_event (org_id, event_type, flag_key, environment, repo_id, actor_type, prev_state, next_state, event_ts, ingested_at, source_event_id, dedupe_key)
			SELECT '%[1]s', 'toggle', 'f0', 'prod', 'repo', 'user', 'off', 'on', now64(3), now64(3), '', concat('e', toString(number %% %[2]d)) FROM numbers(%[3]d)`, org, unique, unique+duplicates))
	}
	impacts := func(org string, scores []float64, refs []string) {
		for index, score := range scores {
			ch.exec(t, fmt.Sprintf(`INSERT INTO release_impact_daily (org_id, day, release_ref, environment, repo_id, release_impact_confidence_score, release_impact_coverage_ratio, rollback_or_disable_after_impact_spike, coverage_ratio, missing_required_fields_count, data_completeness, concurrent_deploy_count, computed_at)
				VALUES ('%s', today() - 1, '%s', 'prod', 'repo', %v, 0.5, 0, 0.5, 0, 1, 0, now64(3))`, org, refs[index], score))
		}
	}

	// orgBad
	deployments(orgBad, 4)
	buckets(orgBad, "r1", 6, 10, "k")
	buckets(orgBad, "r1", 5, 30, "k")
	buckets(orgBad, "r1", 4, 4, "k")
	buckets(orgBad, "r1", 3, 4, "k")
	flags(orgBad, 8, 2)
	events(orgBad, 8, 2)
	impacts(orgBad, []float64{0.1, 0.1, 0.15, 0.5, 0.9}, []string{"r1", "r2", "r9", "r10", "r11"})

	// orgGood: covered, complete, smooth, joined, confident.
	deployments(orgGood, 2)
	for _, day := range []int{6, 5, 4, 3} {
		buckets(orgGood, "r1", day, 10, "g")
	}
	buckets(orgGood, "r2", 2, 10, "g")
	flags(orgGood, 20, 0)
	events(orgGood, 10, 0)
	impacts(orgGood, []float64{0.7, 0.9, 0.85}, []string{"r1", "r2", "r1"})

	// orgDrifty: a shrinking series (ratio below one half) and coverage between the thresholds.
	deployments(orgDrifty, 10)
	for release := 1; release <= 6; release++ {
		buckets(orgDrifty, fmt.Sprintf("r%d", release), 7, 1, "d")
	}
	buckets(orgDrifty, "r1", 6, 20, "d")
	buckets(orgDrifty, "r1", 5, 9, "d")
	flags(orgDrifty, 19, 1)
	impacts(orgDrifty, []float64{0.3, 0.3}, []string{"r1", "r2"})

	// orgJoin: three of five impact rows join to a deployment (60%: the warn band).
	deployments(orgJoin, 5)
	covered(orgJoin, 5, "j")
	impacts(orgJoin, []float64{0.7, 0.8, 0.9, 0.6, 0.65}, []string{"r1", "r2", "r3", "r9", "r10"})

	// orgLowConf: three of four scores very low (one score of exactly 1.0 lands in the top bucket), all joined.
	deployments(orgLowConf, 4)
	covered(orgLowConf, 4, "l")
	impacts(orgLowConf, []float64{0.01, 0.02, 0.19, 1.0}, []string{"r1", "r2", "r3", "r4"})

	// orgHalf: coverage exactly 50% (warn, not critical); orgMid: 55% (warn band).
	deployments(orgHalf, 10)
	for release := 1; release <= 5; release++ {
		buckets(orgHalf, fmt.Sprintf("r%d", release), 2, 1, "h")
	}
	deployments(orgMid, 20)
	for release := 1; release <= 11; release++ {
		buckets(orgMid, fmt.Sprintf("r%d", release), 2, 1, "m")
	}

	// orgFifty: exactly half the confidence scores are very low (not more than half: ok).
	deployments(orgFifty, 4)
	covered(orgFifty, 4, "y")
	impacts(orgFifty, []float64{0.05, 0.1, 0.5, 0.9}, []string{"r1", "r2", "r3", "r4"})
}

// seedIsolation empties the tables and leaves rows of the empty organization and
// of one real organization only: the empty organization is not "another org".
func seedIsolation(t *testing.T, ch clickHouse) {
	t.Helper()
	for _, table := range []string{"deployments", "telemetry_signal_bucket", "feature_flag", "feature_flag_event", "release_impact_daily"} {
		ch.exec(t, "TRUNCATE TABLE "+table)
	}
	for _, org := range []string{"", orgSolo} {
		ch.exec(t, fmt.Sprintf(`INSERT INTO feature_flag (org_id, provider, flag_key, project_key, repo_id, environment, flag_type, created_at, last_synced)
			VALUES ('%s', 'launchdarkly', 'f1', 'p', 'repo', 'prod', 'release', now64(3), now64(3))`, org))
		ch.exec(t, fmt.Sprintf(`INSERT INTO release_impact_daily (org_id, day, release_ref, environment, repo_id, release_impact_confidence_score, release_impact_coverage_ratio, rollback_or_disable_after_impact_spike, coverage_ratio, missing_required_fields_count, data_completeness, concurrent_deploy_count, computed_at)
			VALUES ('%s', today() - 1, 'r1', 'prod', 'repo', 0.9, 0.5, 0, 0.5, 0, 1, 0, now64(3))`, org))
	}
}

func goRun(t *testing.T, ch clickHouse, org string, args ...string) (int, string) {
	code, stdout, _ := goRunErr(t, ch, org, args...)
	return code, stdout
}

func goRunErr(t *testing.T, ch clickHouse, org string, args ...string) (int, string, string) {
	t.Helper()
	env := map[string]string{"CLICKHOUSE_URI": ch.instance.URI}
	if org != "" {
		env["ORG_ID"] = org
	}
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := runValidateFlags(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	if strings.Contains(stderr.String(), "clickhouse://") {
		t.Fatalf("stderr carries the DSN:\n%s", stderr.String())
	}
	return code, stdout.String(), stderr.String()
}

func pythonRunErr(t *testing.T, ch clickHouse, org string, patched bool, args ...string) (int, string, string) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)
	// Patched=true replaces the one condition of the Python check that
	// ClickHouse 26.x refuses (last_synced != '' on a DateTime64: the whole
	// Python command fails with code 41); the four string conditions stay as
	// they are and the timestamp is never NULL, so the answer is the one the
	// check meant to give. patched=false is the producer as it ships.
	program := "import sys\nfrom dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	if patched {
		program = "import sys\nfrom dev_health_ops.metrics import ff_validation\n" +
			"ff_validation._REQUIRED_FLAG_FIELDS = ['provider', 'flag_key', 'environment', 'flag_type']\n" +
			"from dev_health_ops import cli\nraise SystemExit(cli.main(sys.argv[1:]))\n"
	}
	command := exec.Command(python, append([]string{"-c", program, "metrics", "validate-flags"}, args...)...)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "CLICKHOUSE_URI="+ch.httpDSN, "ORG_ID="+org, "OTEL_ENABLED=false")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	code := 0
	if exit, ok := err.(*exec.ExitError); ok {
		code = exit.ExitCode()
	} else if err != nil {
		t.Fatalf("run python: %v", err)
	}
	return code, stdout.String(), stderr.String()
}

var scenarios = []struct {
	org     string
	args    []string
	prepare func(*testing.T, clickHouse)
}{
	{orgBad, nil, nil}, {orgBad, []string{"--lookback", "7"}, nil}, {orgBad, []string{"--lookback", "3"}, nil}, {orgBad, []string{"--lookback", "0"}, nil}, {orgBad, []string{"--lookback", "365"}, nil},
	{orgGood, nil, nil}, {orgGood, []string{"--lookback", "3"}, nil},
	{orgEmpty, nil, nil}, {orgEmpty, []string{"--lookback", "1"}, nil},
	{orgDrifty, nil, nil}, {orgDrifty, []string{"--lookback", "14"}, nil},
	{orgLowConf, nil, nil}, {orgJoin, nil, nil}, {orgHalf, nil, nil}, {orgMid, nil, nil}, {orgFifty, nil, nil},
	{orgSolo, nil, seedIsolation}, {"", nil, nil},
}

const (
	goldenPath   = "testdata/validate_flags_golden.json"
	goldenUpdate = "DHO_VALIDATE_FLAGS_GOLDEN_UPDATE"
)

// goldenScenario is one frozen run of the Python producer (R24): the arguments,
// the exit code and the report text with dates masked (the drift check prints
// the days it saw, which move with the clock).
type goldenScenario struct {
	Org    string   `json:"org"`
	Args   []string `json:"args"`
	Exit   int      `json:"exit"`
	Output string   `json:"output"`
}

var datePattern = regexp.MustCompile(`\d{4}-\d\d-\d\d`)

func maskDates(text string) string { return datePattern.ReplaceAllString(text, "<date>") }

// The report against a real ClickHouse: every status of every check appears in
// some scenario, the exit code follows the report (1 only for a critical), and
// the output is the exact text.
func TestValidateFlagsAgainstClickHouse(t *testing.T) {
	ch := startClickHouse(t)
	seed(t, ch)
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []goldenScenario
	if err := json.Unmarshal(raw, &frozen); err != nil {
		t.Fatal(err)
	}
	if len(frozen) != len(scenarios) {
		t.Fatalf("the golden holds %d scenario(s), the test runs %d", len(frozen), len(scenarios))
	}
	seen := map[string]bool{}
	for index, scenario := range scenarios {
		if scenario.prepare != nil {
			scenario.prepare(t, ch)
		}
		code, stdout, stderr := goRunErr(t, ch, scenario.org, scenario.args...)
		if want := frozen[index]; want.Org != scenario.org || strings.Join(want.Args, " ") != strings.Join(scenario.args, " ") ||
			want.Exit != code || want.Output != maskDates(stdout) {
			t.Fatalf("org %q %v differs from the frozen Python run (exit go %d, python %d):\n--- python\n%s\n--- go\n%s", scenario.org, scenario.args, code, want.Exit, want.Output, maskDates(stdout))
		}
		want := 0
		if strings.Contains(stdout, "RESULT: CRITICAL") {
			want = 1
		}
		if code != want {
			t.Fatalf("org %s %v: exit %d, want %d:\n%s\nstderr:\n%s", scenario.org, scenario.args, code, want, stdout, stderr)
		}
		for _, line := range strings.Split(stdout, "\n") {
			if strings.HasPrefix(line, "  [") {
				fields := strings.SplitN(line, ": ", 2)
				seen[fields[0]] = true
			}
		}
	}
	// Non-vacuity: each check reported ok, warn or critical and skip somewhere.
	for _, want := range []string{"[✗] coverage", "[✓] coverage", "[—] coverage", "[⚠] schema_completeness", "[✓] schema_completeness", "[—] schema_completeness",
		"[✗] dedup_verification", "[✓] dedup_verification", "[⚠] drift_detection", "[✓] drift_detection", "[—] drift_detection",
		"[✗] join_integrity", "[⚠] join_integrity", "[✓] join_integrity", "[—] join_integrity", "[⚠] confidence_distribution", "[✓] confidence_distribution", "[—] confidence_distribution"} {
		if !seen["  "+want] {
			t.Errorf("no scenario produced %q; the comparison would not cover it", want)
		}
	}
	// An unreachable ClickHouse exits 1 with nothing on stdout.
	var stdout bytes.Buffer
	lookup := func(key string) (string, bool) {
		if key == "CLICKHOUSE_URI" {
			return "clickhouse://u:secretpw@127.0.0.1:1/default", true
		}
		return "", false
	}
	if code := runValidateFlags(context.Background(), cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &bytes.Buffer{}}); code != cli.ExitFailure || stdout.Len() != 0 {
		t.Fatalf("unreachable ClickHouse: exit %d, stdout %q", code, stdout.String())
	}
}

// The differential oracle, against the REAL producer while it exists: the same
// ClickHouse, the same organizations and lookbacks, `dev-hops metrics
// validate-flags` and `dho metrics validate-flags` print the same bytes and
// exit the same. Both only read, so one database serves both. It needs the full
// project Python environment; CI runs it in the venue-oracles job.
func TestValidateFlagsVenueOracleMatchesThePythonProducer(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	ch := startClickHouse(t)
	seed(t, ch)
	var frozen []goldenScenario
	for _, scenario := range scenarios {
		if scenario.prepare != nil {
			scenario.prepare(t, ch)
		}
		pyCode, pyOut, pyErr := pythonRunErr(t, ch, scenario.org, true, scenario.args...)
		frozen = append(frozen, goldenScenario{Org: scenario.org, Args: scenario.args, Exit: pyCode, Output: maskDates(pyOut)})
		goCode, goOut, goErr := goRunErr(t, ch, scenario.org, scenario.args...)
		if pyCode != goCode || pyOut != goOut {
			t.Fatalf("org %q %v differs (exit python %d, go %d):\n--- python\n%s\n%s\n--- go\n%s\n%s", scenario.org, scenario.args, pyCode, goCode, pyOut, pyErr, goOut, goErr)
		}
		if !strings.Contains(pyOut, "Feature Flag Pipeline Validation") {
			t.Fatalf("the producer printed no report for org %q %v: the comparison would measure nothing:\n%s\n%s", scenario.org, scenario.args, pyOut, pyErr)
		}
	}
	if os.Getenv(goldenUpdate) == "1" {
		raw, err := json.MarshalIndent(frozen, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(raw, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// The named difference is still real: the producer as it ships fails on this
	// ClickHouse before printing anything. When that stops being so, the
	// patched comparison above is unnecessary and this fails, so it is removed.
	code, stdout, stderr := pythonRunErr(t, ch, orgBad, false)
	if code == 0 || stdout != "" || !strings.Contains(stderr, "Cannot read DateTime") {
		t.Fatalf("the unpatched Python producer no longer fails on last_synced != '' (exit %d, stdout %q, stderr %q): drop the patch from the comparison", code, stdout, stderr)
	}
	venueoracle.WriteProof(t)
}

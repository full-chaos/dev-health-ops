//go:build integration

package rivermigrate_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/rivermigrate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestUpgradeRunsTheJobEndToEnd runs `dho migrate upgrade` through the
// command, with production's settings, against a fresh PostgreSQL database
// and a fresh ClickHouse database: every step applies its head and exits 0,
// in the Job's order; a second run changes nothing; a ClickHouse that cannot
// be reached stops the run at that step, after the PostgreSQL steps, with the
// step named.
func TestUpgradeRunsTheJobEndToEnd(t *testing.T) {
	ctx := context.Background()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	clickhouseInstance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = clickhouseInstance.Close(context.Background()) })

	pgURI := withDatabase(t, postgres.URI, scratchPostgres(t, postgres.URI))
	chURI := withDatabase(t, clickhouseInstance.URI, scratchClickHouse(t, clickhouseInstance.URI))
	settings := map[string]string{
		"MIGRATION_DATABASE_URI":                pgURI,
		"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER": "1",
		"CLICKHOUSE_URI":                        chURI,
		"OPERATIONAL_ORDERING_CONTRACT":         "2",
	}

	code, results, stderr := runUpgrade(t, settings, "--river=false")
	if code != cli.ExitOK || len(results) != 3 {
		t.Fatalf("first upgrade: exit %d, %d result(s), stderr:\n%s", code, len(results), stderr)
	}
	if results[0]["action"] != "baseline_applied" || results[2]["action"] != "baseline_applied" {
		t.Fatalf("first upgrade results = %v, want both schemas baseline_applied", results)
	}
	if _, ok := results[1]["created"]; !ok {
		t.Fatalf("the seed step printed %v, want its created list", results[1])
	}
	for _, step := range []string{"migrate postgres upgrade", "admin features seed", "migrate clickhouse upgrade"} {
		if !strings.Contains(stderr, `"msg":"migrate step done","step":"`+step+`"`) {
			t.Fatalf("no done line for %s:\n%s", step, stderr)
		}
	}
	if strings.Index(stderr, `"step":"migrate postgres upgrade"`) > strings.Index(stderr, `"step":"admin features seed"`) ||
		strings.Index(stderr, `"step":"admin features seed"`) > strings.Index(stderr, `"step":"migrate clickhouse upgrade"`) {
		t.Fatalf("the steps did not run in the Job's order:\n%s", stderr)
	}

	code, results, stderr = runUpgrade(t, settings, "--river=false")
	if code != cli.ExitOK || len(results) != 3 || results[0]["action"] != "up_to_date" || results[2]["action"] != "up_to_date" {
		t.Fatalf("second upgrade: exit %d, results %v, stderr:\n%s", code, results, stderr)
	}
	if created, _ := results[1]["created"].([]any); len(created) != 0 {
		t.Fatalf("the second seed created %v, want nothing", created)
	}

	// --river with no MIGRATION_DATABASE_URI (the database under
	// POSTGRES_URI, in the postgresql+asyncpg:// form the chart's bundled
	// PostgreSQL renders): the River step is skipped and logged, the rest run.
	fallback := map[string]string{
		"POSTGRES_URI":                          "postgresql+asyncpg://" + pgURI[strings.Index(pgURI, "://")+3:],
		"DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER": "1",
		"CLICKHOUSE_URI":                        chURI,
		"OPERATIONAL_ORDERING_CONTRACT":         "2",
	}
	code, results, stderr = runUpgrade(t, fallback, "--river")
	if code != cli.ExitOK || len(results) != 3 ||
		!strings.Contains(stderr, `"level":"WARN","msg":"migrate step skipped","step":"migrate river --apply-and-check","reason":"MIGRATION_DATABASE_URI is not configured"`) {
		t.Fatalf("--river without MIGRATION_DATABASE_URI: exit %d, results %v, stderr:\n%s", code, results, stderr)
	}

	unreachable := map[string]string{}
	for key, value := range settings {
		unreachable[key] = value
	}
	unreachable["CLICKHOUSE_URI"] = "clickhouse://default:unused@127.0.0.1:1/nothing"
	code, results, stderr = runUpgrade(t, unreachable, "--river=false")
	if code != cli.ExitFailure || len(results) != 2 ||
		!strings.Contains(stderr, `"code":"step_failed"`) || !strings.Contains(stderr, `"step":"migrate clickhouse upgrade"`) {
		t.Fatalf("an unreachable ClickHouse: exit %d, results %v, stderr:\n%s", code, results, stderr)
	}
	if strings.Contains(stderr, "unused") {
		t.Fatalf("the failure printed the ClickHouse password:\n%s", stderr)
	}
}

// runUpgrade runs `dho migrate upgrade` with exactly settings as its
// environment and returns its exit code, its stdout JSON lines, and stderr.
func runUpgrade(t *testing.T, settings map[string]string, args ...string) (int, []map[string]any, string) {
	t.Helper()
	var run func(context.Context, cli.Env) int
	for _, child := range rivermigrate.Command().Children {
		if child.Name == "upgrade" {
			run = child.Run
		}
	}
	if run == nil {
		t.Fatal("dho migrate has no upgrade verb")
	}
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) {
		value, ok := settings[key]
		return value, ok
	}
	code := run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		if line == "" {
			continue
		}
		var result map[string]any
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatalf("stdout line %q is not JSON: %v", line, err)
		}
		results = append(results, result)
	}
	return code, results, stderr.String()
}

func randomName(t *testing.T, prefix string) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	return prefix + hex.EncodeToString(suffix)
}

func scratchPostgres(t *testing.T, uri string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	name := randomName(t, "dho_upgrade_")
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

func scratchClickHouse(t *testing.T, uri string) string {
	t.Helper()
	options, err := clickhouse.ParseDSN(uri)
	if err != nil {
		t.Fatal(err)
	}
	options.Auth.Database = ""
	admin, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	name := randomName(t, "dho_upgrade_")
	if err := admin.Exec(context.Background(), "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" SYNC"); err != nil {
			t.Errorf("drop %s: %v", name, err)
		}
	})
	return name
}

func withDatabase(t *testing.T, uri, database string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Path = "/" + database
	return parsed.String()
}

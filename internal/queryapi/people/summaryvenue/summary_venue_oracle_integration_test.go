//go:build integration

package summaryvenue

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const (
	venueOrg      = "venue-org"
	venueIdentity = "alice@example.com"
	venueToday    = "2026-09-24"
)

// TestPeopleSummaryVenueOracle runs the real Python person summary service
// (services/people.py build_person_summary_response, then the response
// model's dump_json) and the Go BuildSummaryResponse over the same seeded
// ClickHouse rows, and requires identical response text. The seed reaches
// each naive timestamp on the wire: spark points (a Date column Python holds
// as a `date`, written as a midnight datetime) and last_ingested_at (a
// DateTime('UTC') column clickhouse-connect returns naive).
func TestPeopleSummaryVenueOracle(t *testing.T) {
	runPeopleSummaryVenue(t, "venue-people-summary.golden.json")
}

// TestPeopleSummaryServerZoneVenueOracle is the same comparison against a
// ClickHouse server whose zone is not UTC, the axis that decides how each
// driver decodes a Date and a DateTime.
func TestPeopleSummaryServerZoneVenueOracle(t *testing.T) {
	t.Setenv(containers.ClickHouseTimezoneEnv, "America/Los_Angeles")
	runPeopleSummaryVenue(t, "venue-people-summary-zone.golden.json")
}

func runPeopleSummaryVenue(t *testing.T, goldenName string) {
	golden, root := openVenueGolden(t, goldenName)
	ctx := context.Background()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		Golden: golden,
		JWTKey: "venue-oracle-test-secret-key-for-people-summary-32b!",
		Seed: func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
			return nil
		},
	})
	seedClickHouse(t, ctx, venue)

	personID := fmt.Sprintf("%x", md5.Sum([]byte(venueIdentity)))
	pythonBody := runPythonSummary(t, root, golden, venue, personID)

	zero := uint64(0)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{
		DSN: venue.AdminClickHouseURI(t, venue.GoClickHouseDB), MaxBytesToRead: &zero,
	})
	if err != nil {
		t.Fatalf("go clickhouse client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	reader, err := people.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	today, err := time.Parse("2006-01-02", venueToday)
	if err != nil {
		t.Fatal(err)
	}
	response, err := people.BuildSummaryResponse(ctx, reader, venueOrg, people.SummaryParams{
		PersonID: personID, RangeDays: 14, CompareDays: 14, Now: today,
	})
	if err != nil {
		t.Fatalf("BuildSummaryResponse: %v", err)
	}
	value, err := pyjson.FromGoModel(response)
	if err != nil {
		t.Fatal(err)
	}
	goBody, err := pyjson.MarshalModel(value)
	if err != nil {
		t.Fatal(err)
	}

	// The seed must reach every timestamp it was built for, or SAME could
	// be two empty responses agreeing.
	for _, want := range []string{
		`"last_ingested_at":"2026-09-24T12:00:00"`,
		`"ts":"2026-09-20T00:00:00"`, `"ts":"2026-09-23T00:00:00"`,
	} {
		if !strings.Contains(pythonBody, want) {
			t.Errorf("the Python body lacks %s:\n%s", want, pythonBody)
		}
	}
	// The raw text of every timestamp-bearing fragment is compared: each
	// deltas[].spark array and freshness.last_ingested_at. The rest of the
	// body differs between the planes only in the engine-chosen
	// row order of the unordered UNION ALL result sets in sections (the
	// corpus declares them order-insensitive), and is that oracle's to compare.
	if got, want := timestampFragments(string(goBody)), timestampFragments(pythonBody); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("timestamp text differs\n python: %s\n go:     %s", strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
	golden.SkipDiff(t)
	venueoracle.WriteGoOnlyProof(t, "Go's person summary timestamps against the frozen answer of Python's build_person_summary_response")
	golden.Finish(t)
}

var timestampFragment = regexp.MustCompile(`"last_ingested_at":[^,}]*|"spark":\[[^\]]*\]`)

func timestampFragments(body string) []string { return timestampFragment.FindAllString(body, -1) }

// seedClickHouse writes the same rows into BOTH planes' databases.
func seedClickHouse(t *testing.T, ctx context.Context, venue *venueoracle.Venue) {
	t.Helper()
	repo := venueoracle.StableUUID("people summary: repository")
	var statements []string
	for _, day := range []string{"2026-09-05", "2026-09-20", "2026-09-21", "2026-09-23"} {
		statements = append(statements, fmt.Sprintf(`INSERT INTO user_metrics_daily
(repo_id, day, identity_id, author_email, pr_first_review_p50_hours, computed_at, org_id)
VALUES ('%s', '%s', '%s', '%s', 6.5, '2026-09-24 12:00:00', '%s')`, repo, day, venueIdentity, venueIdentity, venueOrg))
	}
	statements = append(statements, fmt.Sprintf(`INSERT INTO repo_metrics_daily (repo_id, day, computed_at, org_id)
VALUES ('%s', '2026-09-23', '2026-09-24 12:00:00', '%s')`, repo, venueOrg))
	for _, database := range []string{venue.PythonClickHouseDB, venue.GoClickHouseDB} {
		conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(venue.AdminClickHouseURI(t, database)))
		if err != nil {
			t.Fatalf("seed clickhouse %s: %v", database, err)
		}
		for _, statement := range statements {
			if err := conn.Exec(ctx, statement); err != nil {
				_ = conn.Close()
				t.Fatalf("seed clickhouse %s: %v\n%s", database, err, statement)
			}
		}
		_ = conn.Close()
	}
}

// pythonSummaryEnv names the variable that hands the producer the ClickHouse
// address of the Python plane's database of this run. The address is new in
// every run, so it is not part of a request.
const pythonSummaryEnv = "ORACLE_CLICKHOUSE_URL"

// runPythonSummary runs the producer script, with the Python plane's ClickHouse
// database when the golden is recorded, and returns the body it printed.
func runPythonSummary(t *testing.T, root string, golden *venueoracle.Golden, venue *venueoracle.Venue, personID string) string {
	t.Helper()
	script, err := os.ReadFile(filepath.Join("testdata", "summary_producer.py"))
	if err != nil {
		t.Fatal(err)
	}
	program := programoracle.Script("people summary producer", "internal/queryapi/people/summaryvenue/testdata/summary_producer.py", string(script), nil)
	// The producer takes one JSON argument; the database address in it is the
	// run's, read from the environment, and every other field is the request.
	arguments := fmt.Sprintf(`{"person_id":%q,"org_id":%q,"range_days":14,"compare_days":14,"today":%q}`, personID, venueOrg, venueToday)
	program.Text = "import json, os, sys\nargs = json.loads(" + strconv.Quote(arguments) + ")\nargs[\"db_url\"] = os.environ[\"" + pythonSummaryEnv + "\"]\n" +
		"sys.argv = [sys.argv[0], json.dumps(args)]\n" + program.Text
	program.Env = map[string]string{"OTEL_SDK_DISABLED": "true", "ENVIRONMENT": "test", "IDENTITY_MAPPING_PATH": "/nonexistent/identity-mapping.yaml"}
	program.PerRun = func() map[string]string {
		return map[string]string{pythonSummaryEnv: venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB)}
	}
	answer := programoracle.Produce(t, golden, root, []programoracle.Program{program})[0]
	if answer.ExitCode != 0 {
		t.Fatalf("the python producer exited %d when it was recorded: %s", answer.ExitCode, answer.Stdout)
	}
	for _, line := range strings.Split(answer.Stdout, "\n") {
		if body, ok := strings.CutPrefix(line, "BODY "); ok {
			return body
		}
	}
	t.Fatalf("python producer printed no BODY line: %s", answer.Stdout)
	return ""
}

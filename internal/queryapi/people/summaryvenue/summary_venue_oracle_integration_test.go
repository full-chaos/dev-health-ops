//go:build integration

package summaryvenue

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
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
	runPeopleSummaryVenue(t)
}

// TestPeopleSummaryServerZoneVenueOracle is the same comparison against a
// ClickHouse server whose zone is not UTC, the axis that decides how each
// driver decodes a Date and a DateTime.
func TestPeopleSummaryServerZoneVenueOracle(t *testing.T) {
	t.Setenv(containers.ClickHouseTimezoneEnv, "America/Los_Angeles")
	runPeopleSummaryVenue(t)
}

func runPeopleSummaryVenue(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: "venue-oracle-test-secret-key-for-people-summary-32b!",
		Seed: func(*testing.T, context.Context, *pgxpool.Pool, *venueoracle.Venue) map[string]map[string]any {
			return nil
		},
	})
	seedClickHouse(t, ctx, venue)

	personID := fmt.Sprintf("%x", md5.Sum([]byte(venueIdentity)))
	pythonBody := runPythonSummary(t, root, venue, personID)

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
	// body (dict key order in freshness.sources and the collaboration
	// sections) differs between the planes for a separate reason, the
	// query-api dict-order defect, and is that oracle's to compare.
	if got, want := timestampFragments(string(goBody)), timestampFragments(pythonBody); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("timestamp text differs\n python: %s\n go:     %s", strings.Join(want, "\n"), strings.Join(got, "\n"))
	}
	venueoracle.WriteProof(t)
}

var timestampFragment = regexp.MustCompile(`"last_ingested_at":[^,}]*|"spark":\[[^\]]*\]`)

func timestampFragments(body string) []string { return timestampFragment.FindAllString(body, -1) }

// seedClickHouse writes the same rows into BOTH planes' databases.
func seedClickHouse(t *testing.T, ctx context.Context, venue *venueoracle.Venue) {
	t.Helper()
	repo := uuid.NewString()
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

// runPythonSummary runs the producer script with the venue's Python
// interpreter and the Python plane's ClickHouse database.
func runPythonSummary(t *testing.T, root string, venue *venueoracle.Venue, personID string) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	script := filepath.Join(filepath.Dir(file), "testdata", "summary_producer.py")
	args := fmt.Sprintf(`{"db_url":%q,"person_id":%q,"org_id":%q,"range_days":14,"compare_days":14,"today":%q}`,
		venue.AdminClickHouseHTTPURI(t, venue.PythonClickHouseDB), personID, venueOrg, venueToday)
	command := exec.Command("python3", script, args)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true",
		"ENVIRONMENT=test", "IDENTITY_MAPPING_PATH="+filepath.Join(t.TempDir(), "missing.yaml"))
	output, err := command.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("python producer: %v: %s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(output), "\n") {
		if body, ok := strings.CutPrefix(line, "BODY "); ok {
			return body
		}
	}
	t.Fatalf("python producer printed no BODY line: %s", output)
	return ""
}

// repoRoot walks up from this file to the directory holding src/dev_health_ops.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate this test's source file")
	}
	directory := filepath.Dir(file)
	for {
		if info, err := os.Stat(filepath.Join(directory, "src", "dev_health_ops")); err == nil && info.IsDir() {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatalf("no src/dev_health_ops above %s", file)
		}
		directory = parent
	}
}

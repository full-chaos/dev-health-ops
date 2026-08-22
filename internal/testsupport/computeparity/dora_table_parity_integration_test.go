//go:build integration

package computeparity_test

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/computeparity"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file is the end-to-end proof for CHAOS-3092 P0's whole-table boundary:
// two isolated scratch stores, BOTH built from the real checked-in migration
// chain, seeded from one producer-derived fixture, each run through a real
// producer, and then compared as whole tables.
//
// It follows internal/providersync/oracle_readback_integration_test.go for its
// ClickHouse side -- a real container, production row types, and the shared
// comparison vocabulary rather than a bespoke assertion list.
//
// The reference kind is metrics.dora: the smallest deterministic compute kind
// in the R1 pilot's scope. Its Go port does not exist yet, so both sides run
// the PYTHON producer. That is deliberate and is exactly the self-test the
// slice is accepted on -- the same implementation on both sides must report
// EQUAL, or the harness cannot be trusted to report a real port's differences.
// When the Go executor lands, the right side's command changes and nothing
// else does.

// doraMetricsDailyRow is the production shape of dora_metrics_daily.
//
// Both the SELECT list and the compared field set come from this struct, so a
// column added to the table is added to the comparison by declaring it here
// once -- there is no second list to keep in step.
//
// `day` is a ClickHouse Date and scans as time.Time. It is NOT given the
// date-tagged treatment oraclecompare offers, because that tag exists to stop
// a Python date and a Go string colliding at a Python<->Go crossing; here both
// sides are ClickHouse read through the identical driver path, so both encode
// identically either way.
type doraMetricsDailyRow struct {
	OrgID      string    `json:"org_id" ch:"org_id"`
	RepoID     string    `json:"repo_id" ch:"repo_id"`
	Day        time.Time `json:"day" ch:"day"`
	MetricName string    `json:"metric_name" ch:"metric_name"`
	Value      float64   `json:"value" ch:"value"`
	ComputedAt time.Time `json:"computed_at" ch:"computed_at"`
}

func doraTable() computeparity.Table {
	return computeparity.Table{
		Name:        "dora_metrics_daily",
		OrderBy:     "org_id, repo_id, day, metric_name",
		SemanticKey: []string{"org_id", "repo_id", "day", "metric_name"},
		Exclusions: map[string]string{
			"computed_at": "job_dora stamps datetime.now(UTC) once per job run; it " +
				"carries no product meaning and differs on every execution",
		},
		// dora_metrics_daily is a plain MergeTree and job_dora never deletes,
		// so a replay appends a second copy. That is the kind's REAL behaviour;
		// a port that quietly became idempotent would be a difference worth
		// failing on, not a tidier implementation to wave through.
		Repeat: computeparity.AppendDuplicates,
	}
}

const parityAsOf = "2026-08-22T00:00:00+00:00"

func TestDORATableParityAcrossTwoScratchStores(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })

	baseDSN, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatalf("clickhouse dsn: %v", err)
	}
	leftDSN := scratchDSN(t, baseDSN, "parity_left")
	rightDSN := scratchDSN(t, baseDSN, "parity_right")

	// Both stores are built by the same checked-in migration chain the CLI
	// runs. No DDL is authored here: a hand-typed schema is a second,
	// unversioned copy of one, and a comparison over it only ever confirms
	// what the test itself declared.
	fixtures(t, "provision", "--dsn", leftDSN, "--reset")
	fixtures(t, "provision", "--dsn", rightDSN, "--reset")

	// Seed ONE side from the production fixture generators through the
	// production writers, then clone the declared input tables to the other.
	// Cloning is what makes "both sides consumed identical input" a fact
	// rather than an assumption about generator reproducibility.
	fixtures(t, "seed", "--kind", "metrics.dora", "--dsn", leftDSN, "--as-of", parityAsOf)
	fixtures(t, "clone", "--kind", "metrics.dora", "--from-dsn", leftDSN, "--to-dsn", rightDSN)

	produce := func(dsn string) {
		fixtures(t, "produce", "--kind", "metrics.dora", "--dsn", dsn, "--as-of", parityAsOf)
	}
	produce(leftDSN)
	produce(rightDSN)

	table := doraTable()
	left := read(ctx, t, leftDSN, table, "python")
	right := read(ctx, t, rightDSN, table, "python_replica")

	if len(left.Rows) == 0 {
		t.Fatal("the reference producer wrote no rows -- a comparison over an " +
			"empty table proves nothing about the port")
	}

	t.Run("all four DORA metrics are actually produced", func(t *testing.T) {
		// A table that is missing a metric would still compare EQUAL while
		// silently narrowing what the run covers. time_to_restore_service is
		// the one that depends on the seeded incident mapping resolving
		// (CHAOS-4111), so it is the canary for a quietly narrower comparison.
		produced := map[string]bool{}
		for _, row := range left.Rows {
			produced[leaf(row["metric_name"])] = true
		}
		for _, metric := range []string{
			"deployment_frequency", "lead_time_for_changes",
			"change_failure_rate", "time_to_restore_service",
		} {
			if !produced[metric] {
				t.Errorf("metric %q is absent from the reference output", metric)
			}
		}
	})

	t.Run("same implementation both sides reports EQUAL", func(t *testing.T) {
		if messages := computeparity.Compare(t, table, left, right); len(messages) != 0 {
			t.Fatalf("self-test must be EQUAL, got:\n  %s", strings.Join(messages, "\n  "))
		}
	})

	t.Run("a replay honours the declared repeat policy on both sides", func(t *testing.T) {
		produce(leftDSN)
		produce(rightDSN)
		leftAfter := read(ctx, t, leftDSN, table, "python")
		rightAfter := read(ctx, t, rightDSN, table, "python_replica")

		for _, pair := range []struct{ before, after computeparity.Snapshot }{
			{left, leftAfter}, {right, rightAfter},
		} {
			if messages := computeparity.EvaluateRepeat(table, pair.before, pair.after); len(messages) != 0 {
				t.Errorf("replay: %s", strings.Join(messages, "; "))
			}
		}
		if len(leftAfter.Rows) != len(left.Rows)*2 {
			t.Errorf("append_duplicates should have doubled the rows: %d -> %d",
				len(left.Rows), len(leftAfter.Rows))
		}
		// Parity must survive the replay too: a port can match on run one and
		// diverge on run two.
		if messages := computeparity.Compare(t, table, leftAfter, rightAfter); len(messages) != 0 {
			t.Errorf("parity broke after a replay:\n  %s", strings.Join(messages, "\n  "))
		}
	})

	t.Run("negative controls against real rows", func(t *testing.T) {
		// Re-run each control from a KNOWN-EQUAL baseline. The perturbations
		// are injected into a copy of the right side's snapshot rather than
		// into the store, so one control cannot leak into the next -- a lossy
		// restore between live mutations is its own source of false findings.
		baseline := read(ctx, t, rightDSN, table, "python_replica")
		if messages := computeparity.Compare(t, table, read(ctx, t, leftDSN, table, "python"), baseline); len(messages) != 0 {
			t.Fatalf("the control baseline must be EQUAL first: %v", messages)
		}
		reference := read(ctx, t, leftDSN, table, "python")

		t.Run("mutated row", func(t *testing.T) {
			perturbed := clone(baseline)
			perturbed.Rows[1]["value"] = map[string]any{"t": "float", "v": "-1"}
			assertReports(t, table, reference, perturbed, `field "value"`)
		})
		t.Run("dropped row", func(t *testing.T) {
			// Remove EVERY row sharing one semantic key. By this point the
			// replay has already appended a second copy of each key, so
			// dropping a single row is a multiplicity difference (which the
			// comparator reports as exactly that) rather than an absent key.
			// The control has to inject the shape it claims to test.
			perturbed := clone(baseline)
			victim := computeparity.KeyOf(t, table, perturbed.Rows[len(perturbed.Rows)-1])
			kept := perturbed.Rows[:0]
			for _, row := range perturbed.Rows {
				if computeparity.KeyOf(t, table, row) != victim {
					kept = append(kept, row)
				}
			}
			perturbed.Rows = kept
			assertReports(t, table, reference, perturbed, "absent from")
		})
		t.Run("float nudged by one ULP", func(t *testing.T) {
			perturbed := clone(baseline)
			original := leaf(perturbed.Rows[0]["value"])
			perturbed.Rows[0]["value"] = map[string]any{
				"t": "float", "v": nudge(t, original),
			}
			assertReports(t, table, reference, perturbed, `field "value"`)
		})
	})
}

func assertReports(
	t *testing.T, table computeparity.Table, reference, perturbed computeparity.Snapshot,
	want string,
) {
	t.Helper()
	messages := computeparity.Compare(t, table, reference, perturbed)
	if len(messages) == 0 {
		t.Fatal("the injected difference was NOT reported -- a comparator that " +
			"has not been shown to fail has not been shown to work")
	}
	for _, message := range messages {
		if strings.Contains(message, want) {
			return
		}
	}
	t.Fatalf("no message mentioned %q:\n  %s", want, strings.Join(messages, "\n  "))
}

// leaf reads the value out of oraclecompare's {"t","v"} envelope. Formatting
// the envelope itself yields "map[t:str v:x]", which compares equal to nothing
// a caller means -- an assertion written that way silently never matches.
func leaf(value any) string {
	tagged, ok := value.(map[string]any)
	if !ok {
		return fmt.Sprintf("%v", value)
	}
	return fmt.Sprintf("%v", tagged["v"])
}

func clone(snapshot computeparity.Snapshot) computeparity.Snapshot {
	rows := make([]map[string]any, 0, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		copied := make(map[string]any, len(row))
		for key, value := range row {
			copied[key] = value
		}
		rows = append(rows, copied)
	}
	return computeparity.Snapshot{Table: snapshot.Table, Side: snapshot.Side, Rows: rows}
}

// nudge returns the next representable float64 above the given text.
func nudge(t *testing.T, text string) string {
	t.Helper()
	var value float64
	if _, err := fmt.Sscanf(text, "%g", &value); err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	next := math.Nextafter(value, math.Inf(1))
	if next == value {
		t.Fatalf("nudging %v did not change it", value)
	}
	return fmt.Sprintf("%v", next)
}

func read(
	ctx context.Context, t *testing.T, dsn string,
	table computeparity.Table, side string,
) computeparity.Snapshot {
	t.Helper()
	conn, err := clickhouse.Open(httpOptions(t, dsn))
	if err != nil {
		t.Fatalf("open clickhouse: %v", err)
	}
	defer conn.Close()

	query := computeparity.Query[doraMetricsDailyRow](table)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("query %s: %v", query, err)
	}
	defer rows.Close()

	var collected []doraMetricsDailyRow
	for rows.Next() {
		var row doraMetricsDailyRow
		if err := rows.ScanStruct(&row); err != nil {
			t.Fatalf("scan %s: %v", table.Name, err)
		}
		collected = append(collected, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s: %v", table.Name, err)
	}
	return computeparity.Encode(t, table, side, collected)
}

// fixtures runs the provisioning/seeding CLI. Comparison never goes through
// here -- this only fills the stores.
func fixtures(t *testing.T, args ...string) {
	t.Helper()
	root := repoRoot(t)
	python := filepath.Join(root, ".venv", "bin", "python")
	if _, err := os.Stat(python); err != nil {
		python = "python3"
	}
	command := exec.Command(python,
		append([]string{filepath.Join(root, "scripts", "worker", "compute_parity_fixtures.py")}, args...)...)
	command.Dir = root
	command.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "src")+string(os.PathListSeparator)+os.Getenv("PYTHONPATH"),
		"OTEL_ENABLED=false",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("compute_parity_fixtures.py %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func scratchDSN(t *testing.T, baseDSN, database string) string {
	t.Helper()
	cut := strings.LastIndex(baseDSN, "/")
	if cut < 0 {
		t.Fatalf("unexpected DSN shape: %s", baseDSN)
	}
	return baseDSN[:cut] + "/" + database
}

// httpOptions builds driver options for the container's HTTP port.
//
// The container harness maps only ClickHouse's HTTP port, so the driver must
// be told to speak HTTP. Pointing the NATIVE protocol at an HTTP port fails as
// "[handshake] unexpected packet [72] from server" -- 72 is 'H', the first
// byte of the HTTP response the native handshake could not understand.
func httpOptions(t *testing.T, dsn string) *clickhouse.Options {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn %s: %v", dsn, err)
	}
	password, _ := parsed.User.Password()
	return &clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source file")
	}
	directory := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("no go.mod above the test source file")
		}
		directory = parent
	}
}

package fixturescli

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/fixturesgen"
)

func runTelemetry(t *testing.T, env map[string]string, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr strings.Builder
	lookup := func(key string) (string, bool) { value, ok := env[key]; return value, ok }
	code := runProductTelemetry(t.Context(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

// The verb refuses a bad request before it opens any connection: nothing here
// has a ClickHouse to reach, so an exit other than the expected one would mean
// it tried.
func TestProductTelemetryRefusesBeforeTouchingClickHouse(t *testing.T) {
	configured := map[string]string{"CLICKHOUSE_URI": "clickhouse://default:x@127.0.0.1:1/default"}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		exit int
		want string
	}{
		"an unknown flag":         {configured, []string{"--nope"}, cli.ExitUsage, "flag provided but not defined"},
		"a positional":            {configured, []string{"extra"}, cli.ExitUsage, "positional arguments are not accepted"},
		"a seed that is no int":   {configured, []string{"--seed", "abc"}, cli.ExitUsage, "--seed must be an integer"},
		"a days that is no int":   {configured, []string{"--days", "x"}, cli.ExitUsage, "invalid value"},
		"no ClickHouse":           {nil, []string{"--org", "o"}, cli.ExitFailure, "CLICKHOUSE_URI is required"},
		"no orgs given and none":  {configured, []string{"--orgs", "0"}, cli.ExitFailure, "no orgs to seed"},
		"negative orgs, no orgs":  {configured, []string{"--orgs", "-3"}, cli.ExitFailure, "no orgs to seed"},
		"unreachable ClickHouse":  {configured, []string{"--org", "o", "--days", "1"}, cli.ExitFailure, "clickhouse_unavailable"},
		"a help request is clean": {configured, []string{"--help"}, cli.ExitOK, "Usage: dho fixtures product-telemetry"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runTelemetry(t, tc.env, tc.args...)
			if code != tc.exit || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exit %d (want %d), stderr %q, want it to contain %q", code, tc.exit, stderr, tc.want)
			}
			if stdout != "" {
				t.Fatalf("a refused request wrote to stdout: %q", stdout)
			}
		})
	}
}

// The verb is one of the fixtures group's children.
func TestProductTelemetryIsRegistered(t *testing.T) {
	for _, child := range Command().Children {
		if child.Name == "product-telemetry" && child.Run != nil {
			return
		}
	}
	t.Fatal("dho fixtures has no product-telemetry verb")
}

type recordingBatch struct {
	driver.Batch
	rows [][]any
	sent bool
}

func (b *recordingBatch) Append(values ...any) error { b.rows = append(b.rows, values); return nil }
func (b *recordingBatch) Send() error                { b.sent = true; return nil }

type recordingConn struct {
	statements []string
	batches    []*recordingBatch
}

func (c *recordingConn) PrepareBatch(_ context.Context, statement string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.statements = append(c.statements, statement)
	batch := &recordingBatch{}
	c.batches = append(c.batches, batch)
	return batch, nil
}

// One batch per organization, sent once, into the table and columns persist writes;
// the per-org volume steps down by index%3 (never below 1) and an org with nothing to
// write sends no batch.
func TestSeedProductTelemetryWritesOneBatchPerOrg(t *testing.T) {
	conn := &recordingConn{}
	seed := int64(5)
	end := mustTime("2026-09-26T07:00:00Z")
	counts, err := SeedProductTelemetry(t.Context(), conn, []string{"a", "b", "c", "d"}, 2, 5, &seed, end, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if len(counts) != 4 || len(conn.batches) != 4 {
		t.Fatalf("counts %v, %d batches", counts, len(conn.batches))
	}
	for i, org := range []string{"a", "b", "c", "d"} {
		// sessions per day step down by index%3: 5, 4, 3, then back to 5.
		want, err := fixturesgen.GenerateProductTelemetry(fixturesgen.ProductTelemetrySpec{OrgID: org, Days: 2, SessionsPerDay: 5 - i%3, Seed: &seed, EndTime: end})
		if err != nil || counts[i] != len(want) {
			t.Fatalf("org %s: wrote %d rows, a run of %d sessions/day generates %d (err %v)", org, counts[i], 5-i%3, len(want), err)
		}
	}
	for i, batch := range conn.batches {
		if !batch.sent || len(batch.rows) != counts[i] || counts[i] == 0 {
			t.Fatalf("batch %d: sent=%v rows=%d count=%d", i, batch.sent, len(batch.rows), counts[i])
		}
		if len(batch.rows[0]) != 11 {
			t.Fatalf("a row has %d values, the table has 11 columns", len(batch.rows[0]))
		}
		if conn.statements[i] != productTelemetryInsert {
			t.Fatalf("statement %q", conn.statements[i])
		}
	}
	empty := &recordingConn{}
	if counts, err := SeedProductTelemetry(t.Context(), empty, []string{"a"}, 0, 5, &seed, mustTime("2026-09-26T07:00:00Z"), discardLogger()); err != nil || counts[0] != 0 || len(empty.batches) != 0 {
		t.Fatalf("an empty generation wrote: counts %v err %v batches %d", counts, err, len(empty.batches))
	}
}

func mustTime(text string) time.Time {
	value, err := time.Parse(time.RFC3339, text)
	if err != nil {
		panic(err)
	}
	return value
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

//go:build integration

package fixturescli

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/fixturesgen"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
)

func queryRows(t *testing.T, dsn, query string) [][]string {
	t.Helper()
	body := clickHouseHTTP(t, dsn, query+" FORMAT JSONCompactEachRow")
	var rows [][]string
	for _, line := range strings.Split(strings.TrimSpace(body), "\n") {
		if line == "" {
			continue
		}
		var raw []any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		row := make([]string, len(raw))
		for i, value := range raw {
			row[i] = fmt.Sprint(value)
			if value == nil {
				row[i] = "\\N"
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// Through a real ClickHouse at the migration head: the seeding writes exactly the rows
// the generator makes (every column, timestamps at DateTime64(3)), one org's rows never
// land under another's hash, and days above the ceiling never write a row older than it.
func TestSeedProductTelemetryWritesTheGeneratedRows(t *testing.T) {
	ch := startClickHouse(t)
	conn := nativeConn(t, ch)
	seed := int64(11)
	end := mustTime("2026-09-26T07:31:22Z")
	orgs := []string{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "org-two"}
	counts, err := SeedProductTelemetry(t.Context(), conn, orgs, 400, 3, &seed, end, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	for i, org := range orgs {
		sessions := max(1, 3-(i%3))
		want, err := fixturesgen.GenerateProductTelemetry(fixturesgen.ProductTelemetrySpec{OrgID: org, Days: 400, SessionsPerDay: sessions, Seed: &seed, EndTime: end})
		if err != nil {
			t.Fatal(err)
		}
		if counts[i] != len(want) {
			t.Fatalf("org %s: wrote %d rows, generated %d", org, counts[i], len(want))
		}
		hash := fixturesgen.ProductTelemetryOrgHash(org)
		stored := queryRows(t, ch.httpDSN, "SELECT event_id, name, schema_version, session_id, anonymous_user_id, ifNull(route_pattern, '\\\\N'), payload_json, toString(toUnixTimestamp64Milli(occurred_at)), source FROM product_telemetry_events WHERE org_id_hash = '"+hash+"' ORDER BY event_id")
		if len(stored) != len(want) {
			t.Fatalf("org %s: %d rows stored for %d generated", org, len(stored), len(want))
		}
		byID := map[string]fixturesgen.ProductTelemetryRow{}
		for _, row := range want {
			byID[row.EventID] = row
		}
		for _, row := range stored {
			generated, ok := byID[row[0]]
			if !ok {
				t.Fatalf("org %s: stored event %s was never generated", org, row[0])
			}
			route := "\\N"
			if generated.RoutePattern != nil {
				route = *generated.RoutePattern
			}
			if row[1] != generated.Name || row[2] != generated.SchemaVersion || row[3] != generated.SessionID || row[4] != generated.AnonymousUserID ||
				row[5] != route || row[6] != generated.PayloadJSON || row[7] != strconv.FormatInt(generated.OccurredAt.UnixMilli(), 10) || row[8] != generated.Source {
				t.Fatalf("org %s: stored %v differs from generated %+v", org, row, generated)
			}
		}
	}
	oldest := queryRows(t, ch.httpDSN, "SELECT toString(toUnixTimestamp64Milli(min(occurred_at))) FROM product_telemetry_events")[0][0]
	floor := end.Add(-time.Duration(fixturesgen.ProductTelemetryCeilingDays) * 24 * time.Hour)
	floor = time.Date(floor.Year(), floor.Month(), floor.Day(), 9, 0, 0, 0, time.UTC)
	if ms, _ := strconv.ParseInt(oldest, 10, 64); ms < floor.UnixMilli() {
		t.Fatalf("the oldest stored row (%s ms) is before the ceiling's floor %s", oldest, floor)
	}
}

// The whole verb: flags, environment, ClickHouse, the summary on stdout.
func TestProductTelemetryVerbSeedsThroughRealClickHouse(t *testing.T) {
	ch := startClickHouse(t)
	env := map[string]string{"CLICKHOUSE_URI": ch.instance.URI}
	code, stdout, stderr := runTelemetry(t, env, "--org", "org-a", "--org", "org-b", "--days", "2", "--sessions-per-day", "4", "--seed", "3")
	if code != cli.ExitOK {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	var summary struct {
		Orgs  []string `json:"orgs"`
		Rows  []int    `json:"rows"`
		Total int      `json:"total"`
	}
	if err := json.Unmarshal([]byte(stdout), &summary); err != nil {
		t.Fatalf("summary %q: %v", stdout, err)
	}
	if strings.Join(summary.Orgs, ",") != "org-a,org-b" || len(summary.Rows) != 2 || summary.Total != summary.Rows[0]+summary.Rows[1] || summary.Total == 0 {
		t.Fatalf("summary %+v", summary)
	}
	stored := queryRows(t, ch.httpDSN, "SELECT count() FROM product_telemetry_events")[0][0]
	if stored != strconv.Itoa(summary.Total) {
		t.Fatalf("%s rows stored for a summary of %d", stored, summary.Total)
	}
	// Row counts and event ids come from draws, never from the clock: the same spec at any end time.
	seed := int64(3)
	want, err := fixturesgen.GenerateProductTelemetry(fixturesgen.ProductTelemetrySpec{OrgID: "org-a", Days: 2, SessionsPerDay: 4, Seed: &seed, EndTime: mustTime("2001-01-01T00:00:00Z")})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Rows[0] != len(want) {
		t.Fatalf("org-a: %d rows written, %d generated", summary.Rows[0], len(want))
	}
	ids := queryRows(t, ch.httpDSN, "SELECT event_id FROM product_telemetry_events WHERE org_id_hash = '"+fixturesgen.ProductTelemetryOrgHash("org-a")+"'")
	stored2 := map[string]bool{}
	for _, row := range ids {
		stored2[row[0]] = true
	}
	for _, row := range want {
		if !stored2[row.EventID] {
			t.Fatalf("generated event %s was not stored", row.EventID)
		}
	}
}

// nativeConn opens the native client the verb uses against the test ClickHouse.
func nativeConn(t *testing.T, ch clickHouse) telemetryConn {
	t.Helper()
	conn, err := chstorage.Open(t.Context(), chstorage.DefaultConfig(ch.instance.URI))
	if err != nil {
		t.Fatalf("open the native client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

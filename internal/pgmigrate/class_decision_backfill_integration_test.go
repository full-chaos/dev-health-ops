//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// CHAOS-8735 (D4819): revision 0146 creates go_api_class_decision and backfills, per MCP class operation, exactly the
// go_api_routing_state row the RUNNING (old) image serves: the row at that image's schema digest (the setting
// dho.class_decision_live_schema_digest) under the class document digest. Never a newer row at another digest.

const (
	backfillLiveDigest   = "sha256:fdff794c3fa3de956e07061645b7494cca33ed760f9405d405c912ae01d3e34b"
	backfillOldDigest    = "sha256:dd83956f18b5000000000000000000000000000000000000000000000000000a"
	backfillUnusedDigest = "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	// backfillClassRows is the number of mcp: rows backfillCaseRows holds (every document digest).
	backfillClassRows = 13
)

var (
	backfillT0 = time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	backfillT1 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	backfillT2 = time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	backfillT3 = time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
)

type routingRow struct {
	operation, schema, document, mode, build string
	at                                       time.Time
}

type classDecision struct {
	mode, build, schema string
	decidedAt           time.Time
}

// at0145 is a scratch database one revision below the head (0145), holding rows.
func at0145(t *testing.T, d downInstance, rows []routingRow) (string, *pgx.Conn) {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if got := chain[len(chain)-1].Revision; got != "0146" {
		t.Fatalf("the chain head is %s; this test seeds the database one revision below 0146", got)
	}
	uri := d.at(t, len(chain)-1)
	conn := connect(t, uri)
	ctx := context.Background()
	for _, row := range rows {
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, row.schema, row.document, row.operation, row.build); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, updated_at)
			VALUES ($1, $2, $3, $4, 'go', $5, 100, $6)`, row.schema, row.document, row.operation, row.build, row.mode, row.at); err != nil {
			t.Fatal(err)
		}
	}
	return uri, conn
}

// upgradeWithDigest runs dho's upgrade on conn with the live-digest setting set on its session (nil: not set).
func upgradeWithDigest(t *testing.T, conn *pgx.Conn, digest *string) error {
	t.Helper()
	ctx := context.Background()
	if digest != nil {
		if _, err := conn.Exec(ctx, "SELECT set_config('dho.class_decision_live_schema_digest', $1, false)", *digest); err != nil {
			t.Fatal(err)
		}
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	_, err = pgmigrate.Upgrade(ctx, conn, baseline, chain)
	return err
}

func readDecisions(t *testing.T, conn *pgx.Conn) map[string]classDecision {
	t.Helper()
	got := map[string]classDecision{}
	result, err := conn.Query(context.Background(), `SELECT operation, mode, current_candidate_build, schema_digest, decided_at FROM go_api_class_decision`)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	for result.Next() {
		var operation string
		var d classDecision
		if err := result.Scan(&operation, &d.mode, &d.build, &d.schema, &d.decidedAt); err != nil {
			t.Fatal(err)
		}
		got[operation] = d
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	return got
}

func assertDecisions(t *testing.T, got, want map[string]classDecision) {
	t.Helper()
	for operation, w := range want {
		g, ok := got[operation]
		if !ok {
			t.Errorf("%s: no decision, want %+v", operation, w)
			continue
		}
		if g.mode != w.mode || g.build != w.build || g.schema != w.schema || !g.decidedAt.Equal(w.decidedAt) {
			t.Errorf("%s: got %+v, want %+v", operation, g, w)
		}
	}
	for operation, g := range got {
		if _, ok := want[operation]; !ok {
			t.Errorf("%s: decision %+v, want none (dark)", operation, g)
		}
	}
}

// assertRolledBack: a refused walk leaves the database at 0145 with no decision table.
func assertRolledBack(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx := context.Background()
	var table *string
	if err := conn.QueryRow(ctx, `SELECT to_regclass('public.go_api_class_decision')::text`).Scan(&table); err != nil {
		t.Fatal(err)
	}
	if table != nil {
		t.Errorf("go_api_class_decision exists after a refused walk; the walk must roll back whole")
	}
	recorded, err := pgmigrate.Recorded(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if !hasRevision(recorded, "0145") || hasRevision(recorded, "0146") {
		t.Errorf("alembic_version = %v after a refused walk, want 0145 (not 0146)", recorded)
	}
}

func hasRevision(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// backfillCaseRows holds every case the backfill must decide, each named for the defect it pins.
func backfillCaseRows() []routingRow {
	class := mcpclass.DocumentDigest()
	return []routingRow{
		// The prod/bigboy mcp:securityAlerts shape: the live canary row is OLDER than a shadow row at a digest the
		// old image does not serve. The newest-row rule copied the shadow row and made a lit root dark.
		{"mcp:securityAlerts", backfillLiveDigest, class, "canary", "build-live", backfillT1},
		{"mcp:securityAlerts", backfillOldDigest, class, "shadow", "build-old", backfillT2},
		// A row at another digest that a repoint bumped after the live row: its build is not the one served.
		{"mcp:repointedElsewhere", backfillLiveDigest, class, "canary", "build-live", backfillT1},
		{"mcp:repointedElsewhere", backfillOldDigest, class, "canary", "build-repointed", backfillT3},
		// The live row itself repointed (newest, new build): the live row and its build are copied.
		{"mcp:repointedLive", backfillOldDigest, class, "canary", "build-old", backfillT1},
		{"mcp:repointedLive", backfillLiveDigest, class, "canary", "build-repointed", backfillT3},
		// No row at the live digest: the old image serves nothing for it, so no decision (dark).
		{"mcp:noLiveRow", backfillOldDigest, class, "canary", "build-old", backfillT2},
		// Equal timestamps: the live row wins whatever its mode; no tie-break takes part.
		{"mcp:tie", backfillLiveDigest, class, "canary", "build-live", backfillT1},
		{"mcp:tie", backfillOldDigest, class, "disabled", "build-old", backfillT1},
		// A newer row at the live schema digest under another document digest: the old class switch never reads it.
		{"mcp:otherDocument", backfillLiveDigest, class, "canary", "build-live", backfillT1},
		{"mcp:otherDocument", backfillLiveDigest, "another-document", "python", "build-other", backfillT2},
		// The live row's mode is copied as it is: a live shadow row stays shadow (dark on the serving switch).
		{"mcp:liveShadow", backfillLiveDigest, class, "shadow", "build-live", backfillT1},
		{"mcp:liveShadow", backfillOldDigest, class, "canary", "build-old", backfillT0},
		// A document operation is never a class decision.
		{"featureFlags", backfillLiveDigest, class, "canary", "build-live", backfillT3},
	}
}

func backfillWant() map[string]classDecision {
	return map[string]classDecision{
		"mcp:securityAlerts":     {"canary", "build-live", backfillLiveDigest, backfillT1},
		"mcp:repointedElsewhere": {"canary", "build-live", backfillLiveDigest, backfillT1},
		"mcp:repointedLive":      {"canary", "build-repointed", backfillLiveDigest, backfillT3},
		"mcp:tie":                {"canary", "build-live", backfillLiveDigest, backfillT1},
		"mcp:otherDocument":      {"canary", "build-live", backfillLiveDigest, backfillT1},
		"mcp:liveShadow":         {"shadow", "build-live", backfillLiveDigest, backfillT1},
	}
}

func TestClassDecisionBackfillCopiesTheRowTheOldImageServes(t *testing.T) {
	d := newDownInstance(t)
	rows := backfillCaseRows()
	_, conn := at0145(t, d, rows)
	live := backfillLiveDigest
	if err := upgradeWithDigest(t, conn, &live); err != nil {
		t.Fatalf("upgrade to the head: %v", err)
	}
	assertDecisions(t, readDecisions(t, conn), backfillWant())
	var sources int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM go_api_routing_state`).Scan(&sources); err != nil || sources != len(rows) {
		t.Errorf("source rows = %d (err %v), want all %d left in place", sources, err, len(rows))
	}
}

// The production path end to end: `dho migrate postgres upgrade` reads DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST from its
// environment and the walk copies the live rows (the prod mcp:securityAlerts shape among them).
func TestClassDecisionBackfillThroughTheUpgradeVerbReadsTheLiveDigestFromTheEnvironment(t *testing.T) {
	d := newDownInstance(t)
	uri, conn := at0145(t, d, backfillCaseRows())
	code, stderr := runUpgradeVerb(t, uri, map[string]string{"DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST": backfillLiveDigest})
	if code != cli.ExitOK {
		t.Fatalf("upgrade verb exit %d: %s", code, stderr)
	}
	assertDecisions(t, readDecisions(t, conn), backfillWant())
}

// The verb treats an EMPTY DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST exactly as an unset one (compose passes
// `${DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST:-}`, an empty string; the chart passes no env): with MCP class rows it
// refuses and the walk rolls back whole; with none it upgrades.
func TestClassDecisionBackfillThroughTheUpgradeVerbTreatsAnEmptyLiveDigestAsUnset(t *testing.T) {
	d := newDownInstance(t)
	for name, env := range map[string]map[string]string{
		"unset": nil,
		"empty": {"DHO_CLASS_DECISION_LIVE_SCHEMA_DIGEST": ""},
	} {
		t.Run(name+"/class rows refuse", func(t *testing.T) {
			uri, conn := at0145(t, d, backfillCaseRows())
			code, stderr := runUpgradeVerb(t, uri, env)
			if code == cli.ExitOK {
				t.Fatalf("upgrade verb succeeded with MCP class rows and a %s live digest; it must refuse", name)
			}
			// The error document is JSON (its '<' and '>' escaped): match the text before them.
			if !strings.Contains(stderr, `"code":"migration_failed"`) ||
				!strings.Contains(stderr, "13 MCP class rows exist and dho.class_decision_live_schema_digest is empty or not sha256:") {
				t.Errorf("refusal %q does not name the class row count and the missing digest", stderr)
			}
			assertRolledBack(t, conn)
		})
		t.Run(name+"/no class rows upgrade", func(t *testing.T) {
			uri, conn := at0145(t, d, nil)
			if code, stderr := runUpgradeVerb(t, uri, env); code != cli.ExitOK {
				t.Fatalf("upgrade verb exit %d with no class rows and a %s live digest: %s", code, name, stderr)
			}
			recorded, err := pgmigrate.Recorded(context.Background(), conn)
			if err != nil {
				t.Fatal(err)
			}
			if !hasRevision(recorded, "0146") {
				t.Errorf("alembic_version = %v, want 0146 recorded", recorded)
			}
			if got := readDecisions(t, conn); len(got) != 0 {
				t.Errorf("decisions %+v, want none", got)
			}
		})
	}
}

func runUpgradeVerb(t *testing.T, uri string, extra map[string]string) (int, string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "upgrade" {
			run = child.Run
		}
	}
	env := map[string]string{pgmigrate.CutoverEnv: "1"}
	for key, value := range extra {
		env[key] = value
	}
	lookup := func(key string) (string, bool) {
		value, ok := env[key]
		return value, ok
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), cli.Env{Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	return code, stderr.String()
}

// refusalPrintsNoDecisionValue: a refusal names digests and row counts only.
func refusalPrintsNoDecisionValue(t *testing.T, msg string) {
	t.Helper()
	for _, value := range []string{"build-", "canary", "shadow", "disabled", "python", "mcp:"} {
		if strings.Contains(msg, value) {
			t.Errorf("refusal %q prints a decision value (%q)", msg, value)
		}
	}
}

// With MCP class rows present, an unset, empty or malformed live digest refuses: the walk rolls back whole.
func TestClassDecisionBackfillRefusesAMissingOrMalformedLiveDigest(t *testing.T) {
	d := newDownInstance(t)
	empty, short, upper := "", "sha256:fdff794c3fa3", strings.ToUpper(backfillLiveDigest)
	bare := strings.TrimPrefix(backfillLiveDigest, "sha256:")
	trailing, leading := backfillLiveDigest+"0", " "+backfillLiveDigest
	for name, digest := range map[string]*string{
		"unset": nil, "empty": &empty, "short": &short, "uppercase": &upper, "no-prefix": &bare,
		"trailing-character": &trailing, "leading-character": &leading,
	} {
		t.Run(name, func(t *testing.T) {
			_, conn := at0145(t, d, backfillCaseRows())
			err := upgradeWithDigest(t, conn, digest)
			if err == nil {
				t.Fatalf("upgrade succeeded with MCP class rows and live digest %v; it must refuse", digest)
			}
			msg := err.Error()
			if !strings.Contains(msg, "13 MCP class rows exist and dho.class_decision_live_schema_digest is empty or not sha256:<64 hex>") {
				t.Errorf("refusal %q does not name the class row count and the bad setting", msg)
			}
			refusalPrintsNoDecisionValue(t, msg)
			assertRolledBack(t, conn)
		})
	}
}

// A well-formed digest that holds no class row under the class document digest is a typo: refuse. An mcp: row at that
// digest under ANOTHER document digest does not count (the old class switch never reads it), nor does a document
// operation's row under the class document digest.
func TestClassDecisionBackfillRefusesADigestThatHoldsNoClassRow(t *testing.T) {
	d := newDownInstance(t)
	rows := append(backfillCaseRows(),
		routingRow{"mcp:securityAlerts", backfillUnusedDigest, "another-document", "canary", "build-x", backfillT1},
		routingRow{"featureFlags", backfillUnusedDigest, mcpclass.DocumentDigest(), "canary", "build-x", backfillT1},
	)
	_, conn := at0145(t, d, rows)
	unused := backfillUnusedDigest
	err := upgradeWithDigest(t, conn, &unused)
	if err == nil {
		t.Fatalf("upgrade succeeded with a live digest that holds no class row; it must refuse")
	}
	msg := err.Error()
	if !strings.Contains(msg, "14 MCP class rows exist and none is at schema digest "+backfillUnusedDigest+" under the class document digest") {
		t.Errorf("refusal %q does not name the digest and the class row count", msg)
	}
	refusalPrintsNoDecisionValue(t, msg)
	assertRolledBack(t, conn)
}

// A database with no MCP class row needs no live digest (a fresh install, CI): document rows do not count.
func TestClassDecisionBackfillNeedsNoDigestWithoutClassRows(t *testing.T) {
	d := newDownInstance(t)
	for name, rows := range map[string][]routingRow{
		"empty":         nil,
		"document-only": {{"featureFlags", backfillLiveDigest, mcpclass.DocumentDigest(), "canary", "build-live", backfillT1}},
	} {
		t.Run(name, func(t *testing.T) {
			_, conn := at0145(t, d, rows)
			if err := upgradeWithDigest(t, conn, nil); err != nil {
				t.Fatalf("upgrade without a live digest and no class rows: %v", err)
			}
			if got := readDecisions(t, conn); len(got) != 0 {
				t.Errorf("decisions %+v, want none", got)
			}
		})
	}
}

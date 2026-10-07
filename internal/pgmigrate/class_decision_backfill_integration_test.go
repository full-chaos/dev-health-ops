//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
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

// positionOf0146 is the chain position of revision 0146 (the number of files before it). The walks stop at 0146:
// revision 0147 (CHAOS-8706) drops the source table of the backfill.
func positionOf0146(t *testing.T, chain []pgmigrate.ChainFile) int {
	t.Helper()
	for i, file := range chain {
		if file.Revision == "0146" {
			return i
		}
	}
	t.Fatal("the chain holds no revision 0146")
	return 0
}

// at0145 is a scratch database one revision below the head (0145), holding rows.
func at0145(t *testing.T, d downInstance, rows []routingRow) (string, *pgx.Conn) {
	t.Helper()
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	uri := d.at(t, positionOf0146(t, chain))
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

// upgradeWithDigest runs dho's upgrade on conn with the live digest as a walk setting (nil: not set), the way
// `dho migrate postgres upgrade` passes it.
func upgradeWithDigest(t *testing.T, conn *pgx.Conn, digest *string) error {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var settings pgmigrate.WalkSettings
	if digest != nil {
		settings = pgmigrate.WalkSettings{pgmigrate.ClassDecisionLiveDigestSetting: *digest}
	}
	_, err = pgmigrate.UpgradeLoggedWithSettings(context.Background(), conn, baseline, chain[:positionOf0146(t, chain)+1], slog.New(slog.DiscardHandler), settings)
	return err
}

// sessionSetting is the live-digest setting as the connection's session sees it after the walk: "" or NULL when the
// walk set it for its own transaction only.
func sessionSetting(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var value *string
	if err := conn.QueryRow(context.Background(), "SELECT current_setting($1, true)", pgmigrate.ClassDecisionLiveDigestSetting).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value == nil {
		return ""
	}
	return *value
}

// The walk sets the live digest for its own transaction only (set_config(..., true) inside the transaction that runs
// 0146): revision 0146 reads it, and neither the session nor a later transaction on the connection does. A session
// SET through a transaction-pooling pgbouncer could miss the walk's transaction or reach another client.
func TestClassDecisionLiveDigestIsSetForTheWalksTransactionOnly(t *testing.T) {
	d := newDownInstance(t)
	t.Run("applied", func(t *testing.T) {
		_, conn := at0145(t, d, backfillCaseRows())
		live := backfillLiveDigest
		if err := upgradeWithDigest(t, conn, &live); err != nil {
			t.Fatalf("upgrade to the head: %v", err)
		}
		assertDecisions(t, readDecisions(t, conn), backfillWant())
		if got := sessionSetting(t, conn); got != "" {
			t.Errorf("after the walk the session reads %s = %q; the walk must set it for its transaction only", pgmigrate.ClassDecisionLiveDigestSetting, got)
		}
	})
	t.Run("refused", func(t *testing.T) {
		_, conn := at0145(t, d, backfillCaseRows())
		unused := backfillUnusedDigest
		if err := upgradeWithDigest(t, conn, &unused); err == nil {
			t.Fatalf("upgrade succeeded with a live digest that holds no class row; it must refuse")
		}
		if got := sessionSetting(t, conn); got != "" {
			t.Errorf("after a refused walk the session reads %s = %q; the walk must set it for its transaction only", pgmigrate.ClassDecisionLiveDigestSetting, got)
		}
	})
	// A session-level value is not what the walk reads when the walk is given one: the walk's own value wins.
	t.Run("walk value wins over a session value", func(t *testing.T) {
		_, conn := at0145(t, d, backfillCaseRows())
		if _, err := conn.Exec(context.Background(), "SELECT set_config($1, $2, false)", pgmigrate.ClassDecisionLiveDigestSetting, backfillUnusedDigest); err != nil {
			t.Fatal(err)
		}
		live := backfillLiveDigest
		if err := upgradeWithDigest(t, conn, &live); err != nil {
			t.Fatalf("upgrade to the head: %v", err)
		}
		assertDecisions(t, readDecisions(t, conn), backfillWant())
	})
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
			// The verb walks to the head of the chain (never a literal: a later revision must not break this test).
			chain, err := pgmigrate.LoadChain()
			if err != nil {
				t.Fatal(err)
			}
			head := chain[len(chain)-1].Revision
			if !hasRevision(recorded, head) {
				t.Errorf("alembic_version = %v, want the head %s recorded", recorded, head)
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

// CHAOS-8755 (r1 of #3818, reproduced): with the environment naming no digest, a role or database default of the
// setting (ALTER ROLE/DATABASE ... SET) must not reach revision 0146. The walk sets the setting empty for its own
// transaction, so the unset environment refuses exactly as an empty one does, and the stale row is never copied.
func TestClassDecisionUnsetDigestDoesNotInheritARoleOrDatabaseDefault(t *testing.T) {
	ctx := context.Background()
	for _, scope := range []string{"role", "database"} {
		t.Run(scope, func(t *testing.T) {
			d := newDownInstance(t)
			// seeded is a database at 0145 holding the class rows, with the stale digest as the scope's default; a
			// connection opened after the default was set reads it (checked, so the test cannot pass unmeasured).
			seeded := func(t *testing.T) (string, *pgx.Conn) {
				t.Helper()
				uri, conn := at0145(t, d, backfillCaseRows())
				target := "ROLE CURRENT_USER"
				if scope == "database" {
					var name string
					if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&name); err != nil {
						t.Fatal(err)
					}
					target = "DATABASE " + pgx.Identifier{name}.Sanitize()
				}
				if _, err := conn.Exec(ctx, "ALTER "+target+" SET dho.class_decision_live_schema_digest TO '"+backfillOldDigest+"'"); err != nil {
					t.Fatalf("set the %s default: %v", scope, err)
				}
				fresh := connect(t, uri)
				var inherited string
				if err := fresh.QueryRow(ctx, "SELECT coalesce(current_setting('dho.class_decision_live_schema_digest', true), '')").Scan(&inherited); err != nil {
					t.Fatal(err)
				}
				if inherited != backfillOldDigest {
					t.Fatalf("the %s default is not in effect on a new connection (%q): nothing was measured", scope, inherited)
				}
				return uri, fresh
			}

			t.Run("upgrade verb, environment unset", func(t *testing.T) {
				uri, conn := seeded(t)
				code, stderr := runUpgradeVerb(t, uri, nil)
				if code == cli.ExitOK {
					t.Fatalf("the upgrade verb used the %s default %s and succeeded (securityAlerts = %+v); it must refuse",
						scope, backfillOldDigest, readDecisions(t, conn)["mcp:securityAlerts"])
				}
				if !strings.Contains(stderr, "13 MCP class rows exist and dho.class_decision_live_schema_digest is empty or not sha256:") {
					t.Errorf("refusal %q does not name the class row count and the missing digest", stderr)
				}
				assertRolledBack(t, conn)
			})
			t.Run("library walk, no settings", func(t *testing.T) {
				_, conn := seeded(t)
				if err := upgradeWithDigest(t, conn, nil); err == nil {
					t.Fatalf("the walk with no settings used the %s default and succeeded (securityAlerts = %+v); it must refuse",
						scope, readDecisions(t, conn)["mcp:securityAlerts"])
				}
				assertRolledBack(t, conn)
			})
		})
	}
}

func runPreflightVerb(t *testing.T, uri string, extra map[string]string) (int, pgmigrate.PreflightReport, string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == "preflight" {
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
	var report pgmigrate.PreflightReport
	if code != pgmigrate.ExitMeasurementFailed {
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf("preflight exit %d, stdout %q is not a report: %v", code, stdout.String(), err)
		}
	}
	return code, report, stderr.String()
}

// CHAOS-8755 (r1 of #3818, reproduced): with 0146 pending, the preflight predicts the 0146 guard. For every input the
// guard decides on, the preflight verb and then the upgrade verb run on the same database with the same environment:
// needs_manual/class_decision_digest exactly when the upgrade refuses, applies_cleanly exactly when it applies.
func TestPreflightPredictsTheClassDecisionGuard(t *testing.T) {
	ctx := context.Background()
	d := newDownInstance(t)
	digest := func(v string) map[string]string { return map[string]string{pgmigrate.ClassDecisionLiveDigestEnv: v} }
	cases := []struct {
		name       string
		rows       []routingRow
		env        map[string]string
		roleOldDef bool
		refuses    bool
	}{
		{"live digest", backfillCaseRows(), digest(backfillLiveDigest), false, false},
		{"unset", backfillCaseRows(), nil, false, true},
		{"empty", backfillCaseRows(), digest(""), false, true},
		{"malformed", backfillCaseRows(), digest("sha256:fdff794c3fa3"), false, true},
		{"trailing character", backfillCaseRows(), digest(backfillLiveDigest + "0"), false, true},
		{"digest with no class row", append(backfillCaseRows(),
			routingRow{"mcp:securityAlerts", backfillUnusedDigest, "another-document", "canary", "build-x", backfillT1},
			routingRow{"featureFlags", backfillUnusedDigest, mcpclass.DocumentDigest(), "canary", "build-x", backfillT1},
		), digest(backfillUnusedDigest), false, true},
		// A malformed digest that a class row does hold: the format check alone refuses it.
		{"malformed digest a class row holds", append(backfillCaseRows(),
			routingRow{"mcp:securityAlerts", "sha256:fdff794c3fa3", mcpclass.DocumentDigest(), "canary", "build-x", backfillT1},
		), digest("sha256:fdff794c3fa3"), false, true},
		{"unset with a stale role default", backfillCaseRows(), nil, true, true},
		{"no class rows, unset", nil, nil, false, false},
		{"document rows only, unset", []routingRow{{"featureFlags", backfillLiveDigest, mcpclass.DocumentDigest(), "canary", "build-live", backfillT1}}, nil, false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uri, conn := at0145(t, d, c.rows)
			if c.roleOldDef {
				if _, err := conn.Exec(ctx, "ALTER ROLE CURRENT_USER SET dho.class_decision_live_schema_digest TO '"+backfillOldDigest+"'"); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					_, _ = conn.Exec(context.Background(), "ALTER ROLE CURRENT_USER RESET dho.class_decision_live_schema_digest")
				})
			}
			code, report, stderr := runPreflightVerb(t, uri, c.env)
			upgradeCode, upgradeStderr := runUpgradeVerb(t, uri, c.env)
			upgradeRefused := upgradeCode != cli.ExitOK
			if upgradeRefused != c.refuses {
				t.Fatalf("upgrade refused = %v, want %v (exit %d: %s)", upgradeRefused, c.refuses, upgradeCode, upgradeStderr)
			}
			if c.refuses {
				if report.Verdict != pgmigrate.VerdictNeedsManual || report.Reason != pgmigrate.ReasonClassDecisionDigest || code != cli.ExitFailure {
					t.Errorf("preflight = %s/%s exit %d (%s), want needs_manual/%s exit 1: the upgrade refuses",
						report.Verdict, report.Reason, code, stderr, pgmigrate.ReasonClassDecisionDigest)
				}
				return
			}
			if report.Verdict != pgmigrate.VerdictAppliesCleanly || code != pgmigrate.ExitAppliesCleanly || !hasRevision(report.Pending, "0146") {
				t.Errorf("preflight = %s/%s exit %d pending %v (%s), want applies_cleanly exit 10 with 0146 pending: the upgrade applies",
					report.Verdict, report.Reason, code, report.Pending, stderr)
			}
		})
	}
}

// CHAOS-8755 G4 (preflight.go: the `containsRevision(report.Pending, classDecisionRevision)` clause): the 0146
// guard is predicted only while 0146 is pending. Once 0146 is applied and a later revision (0147) is the only
// pending one, the legacy class rows 0146 left in place and an unset live digest must NOT turn the verdict into
// needs_manual: the upgrade would not run the guard (0146 is recorded), so it applies. Without the clause the
// preflight evaluates the guard for a walk that never runs it.
func TestPreflightDoesNotPredictThe0146GuardOnceItIsApplied(t *testing.T) {
	ctx := context.Background()
	d := newDownInstance(t)
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	applied0146 := positionOf0146(t, chain) + 1
	if applied0146 >= len(chain) {
		t.Fatalf("the chain holds no revision after 0146 (position %d of %d): the case this test needs does not exist", applied0146, len(chain))
	}
	uri := d.at(t, applied0146)
	conn := connect(t, uri)
	for _, row := range backfillCaseRows() {
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_candidate_build (schema_digest, document_digest, selected_operation, candidate_build)
			VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`, row.schema, row.document, row.operation, row.build); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `INSERT INTO go_api_routing_state (schema_digest, document_digest, selected_operation, current_candidate_build, owner, mode, rollout_percentage, updated_at)
			VALUES ($1, $2, $3, $4, 'go', $5, 100, $6)`, row.schema, row.document, row.operation, row.build, row.mode, row.at); err != nil {
			t.Fatal(err)
		}
	}
	code, report, stderr := runPreflightVerb(t, uri, nil) // no live digest in the environment
	if report.Verdict != pgmigrate.VerdictAppliesCleanly || code != pgmigrate.ExitAppliesCleanly {
		t.Fatalf("preflight = %s/%s exit %d (%s), want applies_cleanly exit 10: 0146 is applied, so its guard does not run", report.Verdict, report.Reason, code, stderr)
	}
	if hasRevision(report.Pending, "0146") || !hasRevision(report.Pending, chain[applied0146].Revision) {
		t.Fatalf("pending = %v, want only the revisions after 0146 (%s)", report.Pending, chain[applied0146].Revision)
	}
	if upgradeCode, upgradeStderr := runUpgradeVerb(t, uri, nil); upgradeCode != cli.ExitOK {
		t.Fatalf("upgrade exit %d (%s): the preflight said it applies, and it must", upgradeCode, upgradeStderr)
	}
}

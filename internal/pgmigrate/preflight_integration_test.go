//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The preflight's oracle is the hook itself. For every state of a grid, the
// preflight's verdict is compared with what the REAL `upgrade` does to a copy of
// the same database (CREATE DATABASE ... TEMPLATE):
//
//   - at_head / applies_cleanly: the upgrade succeeds and leaves alembic_version at
//     the build's heads, and what it applied is exactly the preflight's pending list;
//   - needs_manual: the upgrade refuses, with the error kind the reason names, and
//     the database is byte-unchanged;
//   - the preflight itself changes nothing (byte-identical dump before and after).
//
// TestPreflightMatchesTheHookOverTheStateGrid builds the grid with dho itself (a
// truncated chain leaves a database at that revision; the baseline test proves dho's
// databases equal Python's). TestPreflightVenueOracleOverRealAlembicStates builds
// the states that need Python (below the baseline) and the ones the Python upgrade
// leaves, with the real Alembic.

// build is one build of dho: its chain and the Alembic walk it embeds. A rolled-back
// image is a build with a shorter chain and no walk entry for what it lacks.
type build struct {
	chain   []pgmigrate.ChainFile
	history []pgmigrate.HistoryEntry
}

func currentBuild(t *testing.T) (pgmigrate.Baseline, build) {
	t.Helper()
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	history, err := pgmigrate.LoadHistory()
	if err != nil {
		t.Fatal(err)
	}
	return baseline, build{chain: chain, history: history}
}

// olderBuild is the build without the last `dropped` chain revisions: the image a
// rollback restores.
func olderBuild(t *testing.T, current build, dropped int) build {
	t.Helper()
	keep := len(current.chain) - dropped
	if keep < 0 {
		t.Fatalf("cannot drop %d of %d chain revisions", dropped, len(current.chain))
	}
	gone := map[string]bool{}
	for _, file := range current.chain[keep:] {
		gone[file.Revision] = true
	}
	var history []pgmigrate.HistoryEntry
	for _, entry := range current.history {
		if !gone[entry.Revision] {
			history = append(history, entry)
		}
	}
	return build{chain: current.chain[:keep], history: history}
}

// stateDatabase makes a scratch database and lets set build its state over a
// connection that is closed before it returns, so the database can be a template.
func stateDatabase(t *testing.T, admin *pgx.Conn, instance *containers.Instance, set func(conn *pgx.Conn)) string {
	t.Helper()
	database := scratchDatabase(t, admin)
	conn, err := pgx.Connect(context.Background(), databaseURI(t, instance.URI, database))
	if err != nil {
		t.Fatal(err)
	}
	set(conn)
	if err := conn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	return database
}

func cloneDatabase(t *testing.T, admin *pgx.Conn, source string) string {
	t.Helper()
	clone := scratchDatabaseName(t)
	if _, err := admin.Exec(context.Background(), "CREATE DATABASE "+clone+" TEMPLATE "+source); err != nil {
		t.Fatalf("clone %s: %v", source, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+clone+" WITH (FORCE)"); err != nil {
			t.Errorf("drop %s: %v", clone, err)
		}
	})
	return clone
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(suffix)
}

// snapshot is the database's schema and data as pg_dump prints them: two equal
// snapshots are a database nothing wrote to.
func snapshot(t *testing.T, ctx context.Context, instance *containers.Instance, database string) string {
	t.Helper()
	return pgDump(t, ctx, instance, database, "--schema-only") + "\n--DATA--\n" +
		pgDump(t, ctx, instance, database, "--data-only", "--column-inserts")
}

// hookOutcome is what the real upgrade did to a copy.
type hookOutcome struct {
	err     error
	result  pgmigrate.Result
	heads   []string
	changed bool
}

func runHook(t *testing.T, ctx context.Context, admin *pgx.Conn, instance *containers.Instance, database string, settings pgmigrate.Settings, baseline pgmigrate.Baseline, b build) hookOutcome {
	t.Helper()
	copyName := cloneDatabase(t, admin, database)
	before := snapshot(t, ctx, instance, copyName)
	conn := connect(t, databaseURI(t, instance.URI, copyName))
	var outcome hookOutcome
	if err := pgmigrate.CheckSettings(settings, baseline); err != nil {
		outcome.err = err
	} else {
		outcome.result, outcome.err = pgmigrate.Upgrade(ctx, conn, baseline, b.chain)
	}
	rows, err := conn.Query(ctx, "SELECT version_num FROM alembic_version ORDER BY version_num")
	if err == nil {
		outcome.heads, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		outcome.heads = nil // no alembic_version (foreign database): nothing recorded
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatal(err)
	}
	outcome.changed = snapshot(t, ctx, instance, copyName) != before
	return outcome
}

func reasonForError(err error) []string {
	var below pgmigrate.BelowHeadError
	var foreign pgmigrate.ForeignDatabaseError
	var mismatch pgmigrate.SchemaMismatchError
	var settings pgmigrate.SettingsMismatchError
	switch {
	case errors.As(err, &settings):
		return []string{pgmigrate.ReasonSettingsMismatch}
	case errors.As(err, &mismatch):
		return []string{pgmigrate.ReasonSchemaMismatch}
	case errors.As(err, &foreign):
		return []string{pgmigrate.ReasonForeignDatabase}
	case errors.As(err, &below):
		return []string{pgmigrate.ReasonBelowBaseline, pgmigrate.ReasonCutoverMissing, pgmigrate.ReasonAheadOfBuild}
	}
	return nil
}

// checkAgainstHook is the oracle for one state. want is the verdict the state was
// built to have and wantReason the reason; both are ALSO checked against the hook, so
// a wrong expectation cannot hide a wrong preflight.
func checkAgainstHook(t *testing.T, ctx context.Context, admin *pgx.Conn, instance *containers.Instance,
	name, database string, settings pgmigrate.Settings, baseline pgmigrate.Baseline, b build, wantVerdict, wantReason string) pgmigrate.PreflightReport {
	t.Helper()
	before := snapshot(t, ctx, instance, database)
	conn, err := pgx.Connect(ctx, databaseURI(t, instance.URI, database))
	if err != nil {
		t.Fatal(err)
	}
	report, err := pgmigrate.Preflight(ctx, conn, settings, baseline, b.chain, b.history)
	if closeErr := conn.Close(ctx); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatalf("%s: preflight: %v", name, err)
	}
	if snapshot(t, ctx, instance, database) != before {
		t.Fatalf("%s: the preflight changed the database", name)
	}
	if report.Verdict != wantVerdict || report.Reason != wantReason {
		t.Fatalf("%s: preflight = %s/%s, want %s/%s (%+v)", name, report.Verdict, report.Reason, wantVerdict, wantReason, report)
	}
	if !reflect.DeepEqual(report.BuildHeads, pgmigrate.Heads(baseline, b.chain)) {
		t.Fatalf("%s: build_heads = %v, want %v", name, report.BuildHeads, pgmigrate.Heads(baseline, b.chain))
	}

	hook := runHook(t, ctx, admin, instance, database, settings, baseline, b)
	switch report.Verdict {
	case pgmigrate.VerdictAtHead, pgmigrate.VerdictAppliesCleanly:
		if hook.err != nil {
			t.Fatalf("%s: preflight said %s, the hook refused: %v", name, report.Verdict, hook.err)
		}
		if !reflect.DeepEqual(hook.heads, report.BuildHeads) {
			t.Fatalf("%s: the hook left alembic_version at %v, want the build heads %v", name, hook.heads, report.BuildHeads)
		}
		if !reflect.DeepEqual(nonEmpty(hook.result.Applied), report.Pending) {
			t.Fatalf("%s: the hook applied %v, the preflight said pending %v", name, hook.result.Applied, report.Pending)
		}
		if (report.Verdict == pgmigrate.VerdictAtHead) == hook.changed {
			t.Fatalf("%s: verdict %s but the hook changed the database = %v", name, report.Verdict, hook.changed)
		}
	case pgmigrate.VerdictNeedsManual:
		if hook.err == nil {
			t.Fatalf("%s: preflight said needs_manual (%s), the hook succeeded (%+v)", name, report.Reason, hook.result)
		}
		reasons := reasonForError(hook.err)
		found := false
		for _, reason := range reasons {
			found = found || reason == report.Reason
		}
		if !found {
			t.Fatalf("%s: reason %s, but the hook's refusal was %T (%v) which stands for %v", name, report.Reason, hook.err, hook.err, reasons)
		}
		if hook.changed {
			t.Fatalf("%s: the hook refused but changed the database", name)
		}
	}
	return report
}

func nonEmpty(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func productionEnv(t *testing.T) {
	t.Helper()
	t.Setenv(pgmigrate.CutoverEnv, "1")
	t.Setenv(pgmigrate.RiverSchemaEnv, productionSettings.RiverSchema)
}

func startInstance(t *testing.T) (*containers.Instance, *pgx.Conn) {
	t.Helper()
	instance, err := containers.StartPostgres(context.Background())
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(context.Background()); err != nil {
			t.Errorf("close postgres: %v", err)
		}
	})
	if instance.Container == nil {
		t.Fatal("the oracle dumps databases with the server's own pg_dump and needs a container instance, not a remote DSN")
	}
	return instance, connect(t, instance.URI)
}

func upgradeTo(t *testing.T, baseline pgmigrate.Baseline, chain []pgmigrate.ChainFile) func(*pgx.Conn) {
	return func(conn *pgx.Conn) {
		t.Helper()
		if _, err := pgmigrate.Upgrade(context.Background(), conn, baseline, chain); err != nil {
			t.Fatalf("build a state: %v", err)
		}
	}
}

func execSQL(t *testing.T, sql string) func(*pgx.Conn) {
	return func(conn *pgx.Conn) {
		t.Helper()
		if _, err := conn.Exec(context.Background(), sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

func sequence(steps ...func(*pgx.Conn)) func(*pgx.Conn) {
	return func(conn *pgx.Conn) {
		for _, step := range steps {
			step(conn)
		}
	}
}

// TestPreflightMatchesTheHookOverTheStateGrid is the mechanism oracle over states dho
// builds itself.
func TestPreflightMatchesTheHookOverTheStateGrid(t *testing.T) {
	ctx := context.Background()
	productionEnv(t)
	baseline, current := currentBuild(t)
	instance, admin := startInstance(t)
	if len(current.chain) < 2 {
		t.Fatalf("the grid needs a chain of at least two revisions, have %d", len(current.chain))
	}
	application := ""
	for _, head := range baseline.Heads {
		if head != "0066" {
			application = head
		}
	}
	empty := stateDatabase(t, admin, instance, func(*pgx.Conn) {})
	atBaseline := stateDatabase(t, admin, instance, upgradeTo(t, baseline, nil))
	atHead := stateDatabase(t, admin, instance, upgradeTo(t, baseline, current.chain))

	type state struct {
		name     string
		database string
		settings pgmigrate.Settings
		build    build
		verdict  string
		reason   string
	}
	states := []state{
		{"empty database", empty, productionSettings, current, pgmigrate.VerdictAppliesCleanly, pgmigrate.ReasonEmptyDatabase},
		{"at the baseline", atBaseline, productionSettings, current, pgmigrate.VerdictAppliesCleanly, pgmigrate.ReasonPendingRevs},
		{"at the head", atHead, productionSettings, current, pgmigrate.VerdictAtHead, pgmigrate.ReasonUpToDate},
		{"cutover row deleted", stateDatabase(t, admin, instance, sequence(upgradeTo(t, baseline, current.chain),
			execSQL(t, "DELETE FROM alembic_version WHERE version_num = '0066'"))), productionSettings, current,
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonCutoverMissing},
		{"below the baseline", stateDatabase(t, admin, instance, sequence(upgradeTo(t, baseline, nil),
			execSQL(t, "UPDATE alembic_version SET version_num = '0137' WHERE version_num = '"+application+"'"))), productionSettings, current,
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonBelowBaseline},
		{"a revision this build never heard of", stateDatabase(t, admin, instance, sequence(upgradeTo(t, baseline, nil),
			execSQL(t, "UPDATE alembic_version SET version_num = '9999' WHERE version_num = '"+application+"'"))), productionSettings, current,
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonAheadOfBuild},
		{"the image was rolled back one revision", atHead, productionSettings, olderBuild(t, current, 1),
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonAheadOfBuild},
		{"objects but no alembic_version", stateDatabase(t, admin, instance, execSQL(t, "CREATE TABLE public.not_ours (id int)")),
			productionSettings, current, pgmigrate.VerdictNeedsManual, pgmigrate.ReasonForeignDatabase},
		{"alembic_version at the baseline, a table gone", stateDatabase(t, admin, instance, sequence(upgradeTo(t, baseline, nil),
			execSQL(t, "DROP TABLE public."+baseline.Tables()[len(baseline.Tables())-1]+" CASCADE"))), productionSettings, current,
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonSchemaMismatch},
		{"the environment lacks the cutover", atHead, pgmigrate.Settings{Cutover: false, RiverSchema: "river"}, current,
			pgmigrate.VerdictNeedsManual, pgmigrate.ReasonSettingsMismatch},
	}
	// Every chain prefix: the database a Python upgrade would leave after each
	// revision, and the hook must reach the head from each.
	for index := 1; index < len(current.chain); index++ {
		prefix := current.chain[:index]
		states = append(states, state{
			fmt.Sprintf("at chain revision %s", prefix[len(prefix)-1].Revision),
			stateDatabase(t, admin, instance, upgradeTo(t, baseline, prefix)), productionSettings, current,
			pgmigrate.VerdictAppliesCleanly, pgmigrate.ReasonPendingRevs,
		})
	}
	for _, s := range states {
		t.Run(s.name, func(t *testing.T) {
			report := checkAgainstHook(t, ctx, admin, instance, s.name, s.database, s.settings, baseline, s.build, s.verdict, s.reason)
			if s.reason == pgmigrate.ReasonCutoverMissing && !reflect.DeepEqual(report.Missing, []string{"0066"}) {
				t.Fatalf("missing = %v, want [0066]", report.Missing)
			}
			if s.name == "at chain revision "+current.chain[0].Revision && len(report.Pending) != len(current.chain)-1 {
				t.Fatalf("pending = %v, want the %d revisions after %s", report.Pending, len(current.chain)-1, current.chain[0].Revision)
			}
		})
	}
}

// commandRun runs one verb of the group the way the binary does.
func commandRun(t *testing.T, uri string, lookup func(string) (string, bool), verb string, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == verb {
			run = child.Run
		}
	}
	if run == nil {
		t.Fatalf("no %s verb", verb)
	}
	var out, errOut bytes.Buffer
	code = run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &out, Stderr: &errOut})
	return code, out.String(), errOut.String()
}

func productionLookup(key string) (string, bool) {
	value, ok := map[string]string{pgmigrate.CutoverEnv: "1", pgmigrate.RiverSchemaEnv: "river"}[key]
	return value, ok
}

// TestPreflightCommand pins the verb's contract: exit codes, --strict, the JSON on
// stdout, nothing secret, and a measurement that did not happen is exit 3 and no
// verdict.
func TestPreflightCommand(t *testing.T) {
	ctx := context.Background()
	baseline, current := currentBuild(t)
	instance, admin := startInstance(t)
	atHead := stateDatabase(t, admin, instance, upgradeTo(t, baseline, current.chain))
	atBaseline := stateDatabase(t, admin, instance, upgradeTo(t, baseline, nil))
	foreign := stateDatabase(t, admin, instance, execSQL(t, "CREATE TABLE public.not_ours (id int)"))

	type report struct {
		Verdict        string   `json:"verdict"`
		Reason         string   `json:"reason"`
		Recorded       []string `json:"recorded"`
		BuildHeads     []string `json:"build_heads"`
		Pending        []string `json:"pending"`
		Missing        []string `json:"missing"`
		MigratorActive *bool    `json:"migrator_active"`
	}
	decode := func(t *testing.T, stdout string) report {
		t.Helper()
		var r report
		decoder := json.NewDecoder(strings.NewReader(stdout))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&r); err != nil {
			t.Fatalf("stdout %q is not the report: %v", stdout, err)
		}
		if r.MigratorActive == nil || r.Recorded == nil || r.BuildHeads == nil || r.Pending == nil || r.Missing == nil {
			t.Fatalf("a field is absent from %q", stdout)
		}
		return r
	}
	for _, tc := range []struct {
		name    string
		uri     string
		args    []string
		lookup  func(string) (string, bool)
		code    int
		verdict string
	}{
		{"at head", databaseURI(t, instance.URI, atHead), nil, productionLookup, cli.ExitOK, pgmigrate.VerdictAtHead},
		{"at head strict", databaseURI(t, instance.URI, atHead), []string{"--strict"}, productionLookup, cli.ExitOK, pgmigrate.VerdictAtHead},
		{"applies cleanly", databaseURI(t, instance.URI, atBaseline), nil, productionLookup, pgmigrate.ExitAppliesCleanly, pgmigrate.VerdictAppliesCleanly},
		{"applies cleanly strict", databaseURI(t, instance.URI, atBaseline), []string{"--strict"}, productionLookup, cli.ExitFailure, pgmigrate.VerdictAppliesCleanly},
		{"needs manual", databaseURI(t, instance.URI, foreign), nil, productionLookup, cli.ExitFailure, pgmigrate.VerdictNeedsManual},
		{"settings mismatch", databaseURI(t, instance.URI, atHead), nil, func(string) (string, bool) { return "", false }, cli.ExitFailure, pgmigrate.VerdictNeedsManual},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := commandRun(t, tc.uri, tc.lookup, "preflight", tc.args...)
			if code != tc.code {
				t.Fatalf("exit %d, want %d (stdout %q stderr %q)", code, tc.code, stdout, stderr)
			}
			r := decode(t, stdout)
			if r.Verdict != tc.verdict {
				t.Fatalf("verdict %s, want %s", r.Verdict, tc.verdict)
			}
			if *r.MigratorActive {
				t.Fatal("migrator_active with no migrator running")
			}
			if strings.Contains(stdout+stderr, "postgres://") || strings.Contains(stdout+stderr, "worker_test") {
				t.Fatalf("the output carries the connection: %q %q", stdout, stderr)
			}
		})
	}

	t.Run("a migrator holding the lock is reported and changes no verdict", func(t *testing.T) {
		holder := connect(t, databaseURI(t, instance.URI, atHead))
		if _, err := holder.Exec(ctx, "SELECT pg_advisory_lock($1)", pgmigrateLockKey); err != nil {
			t.Fatal(err)
		}
		code, stdout, _ := commandRun(t, databaseURI(t, instance.URI, atHead), productionLookup, "preflight")
		r := decode(t, stdout)
		if code != cli.ExitOK || r.Verdict != pgmigrate.VerdictAtHead || !*r.MigratorActive {
			t.Fatalf("with the lock held: exit %d, %+v, migrator_active %v", code, r, *r.MigratorActive)
		}
		if _, err := holder.Exec(ctx, "SELECT pg_advisory_unlock($1)", pgmigrateLockKey); err != nil {
			t.Fatal(err)
		}
		_, stdout, _ = commandRun(t, databaseURI(t, instance.URI, atHead), productionLookup, "preflight")
		if *decode(t, stdout).MigratorActive {
			t.Fatal("migrator_active after the lock was released")
		}
	})

	t.Run("no connection is exit 3 and no verdict", func(t *testing.T) {
		uri := databaseURI(t, instance.URI, atHead)
		parsed, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		parsed.Host = "127.0.0.1:1"
		code, stdout, stderr := commandRun(t, parsed.String(), productionLookup, "preflight")
		if code != pgmigrate.ExitMeasurementFailed || strings.TrimSpace(stdout) != "" {
			t.Fatalf("exit %d, stdout %q: an unreachable database must be exit 3 with no verdict", code, stdout)
		}
		if !strings.Contains(stderr, "postgres_unavailable") || strings.Contains(stderr, "postgres://") {
			t.Fatalf("stderr %q", stderr)
		}
	})

	t.Run("a database whose alembic_version cannot be read is exit 3", func(t *testing.T) {
		// alembic_version exists but is not the shape the migrator reads: its
		// version_num column is gone (renamed on a real, migrated database).
		broken := stateDatabase(t, admin, instance, sequence(upgradeTo(t, baseline, current.chain),
			execSQL(t, "ALTER TABLE public.alembic_version RENAME COLUMN version_num TO other")))
		code, stdout, stderr := commandRun(t, databaseURI(t, instance.URI, broken), productionLookup, "preflight")
		if code != pgmigrate.ExitMeasurementFailed || strings.TrimSpace(stdout) != "" || !strings.Contains(stderr, "preflight_failed") {
			t.Fatalf("exit %d, stdout %q, stderr %q: a database the preflight could not read must be exit 3 with no verdict", code, stdout, stderr)
		}
	})

	t.Run("positional arguments are a usage error", func(t *testing.T) {
		if code, _, _ := commandRun(t, databaseURI(t, instance.URI, atHead), productionLookup, "preflight", "extra"); code != cli.ExitUsage {
			t.Fatalf("exit %d, want %d", code, cli.ExitUsage)
		}
	})
}

// pgmigrateLockKey is pgmigrate's advisory lock key ("dho_pg"), which the hook takes.
const pgmigrateLockKey int64 = 0x64686f5f7067

// statementRecorder records every statement a connection sends.
type statementRecorder struct{ statements []string }

func (r *statementRecorder) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	r.statements = append(r.statements, data.SQL)
	return ctx
}

func (r *statementRecorder) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// TestPreflightIsReadOnly pins the contract by measurement: the connection the
// preflight uses sends no write and takes no advisory lock, and it runs inside one
// READ ONLY transaction; a role that can only read gets the same verdict.
func TestPreflightIsReadOnly(t *testing.T) {
	ctx := context.Background()
	baseline, current := currentBuild(t)
	instance, admin := startInstance(t)
	database := stateDatabase(t, admin, instance, upgradeTo(t, baseline, nil))

	config, err := pgx.ParseConfig(databaseURI(t, instance.URI, database))
	if err != nil {
		t.Fatal(err)
	}
	recorder := &statementRecorder{}
	config.Tracer = recorder
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	report, err := pgmigrate.Preflight(ctx, conn, productionSettings, baseline, current.chain, current.history)
	_ = conn.Close(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.Verdict != pgmigrate.VerdictAppliesCleanly {
		t.Fatalf("verdict %s", report.Verdict)
	}
	if len(recorder.statements) == 0 {
		t.Fatal("no statement was recorded: the tracer measured nothing")
	}
	for _, statement := range recorder.statements {
		upper := strings.ToUpper(statement)
		for _, forbidden := range []string{"INSERT ", "UPDATE ", "DELETE ", "CREATE ", "DROP ", "ALTER ", "TRUNCATE ", "GRANT ", "ADVISORY_LOCK", "ADVISORY_XACT_LOCK", "FOR UPDATE", "LOCK TABLE"} {
			if strings.Contains(upper, forbidden) {
				t.Fatalf("the preflight sent %q, which contains %q", statement, forbidden)
			}
		}
	}
	if !strings.Contains(strings.ToUpper(recorder.statements[0]), "READ ONLY") {
		t.Fatalf("the first statement is %q, want a READ ONLY transaction", recorder.statements[0])
	}

	// A role that can only read, and cannot write to the public schema.
	role := "dho_pf_reader_" + randomSuffix(t)
	t.Cleanup(func() { _, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+role) })
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN PASSWORD 'reader' NOSUPERUSER NOCREATEDB NOCREATEROLE",
		"GRANT CONNECT ON DATABASE " + database + " TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	dbAdmin := connect(t, databaseURI(t, instance.URI, database))
	for _, statement := range []string{
		"GRANT USAGE ON SCHEMA public TO " + role,
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO " + role,
	} {
		if _, err := dbAdmin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	readerURI, err := url.Parse(databaseURI(t, instance.URI, database))
	if err != nil {
		t.Fatal(err)
	}
	readerURI.User = url.UserPassword(role, "reader")
	code, stdout, stderr := commandRun(t, readerURI.String(), productionLookup, "preflight")
	if code != pgmigrate.ExitAppliesCleanly || !strings.Contains(stdout, `"verdict":"applies_cleanly"`) {
		t.Fatalf("a read-only role got exit %d, stdout %q, stderr %q", code, stdout, stderr)
	}
}

// TestPreflightVenueOracleOverRealAlembicStates is the live-Python oracle: the states
// are built by the REAL Python Alembic upgrade (below the baseline included), the
// verdict is compared with the real hook on a copy.
func TestPreflightVenueOracleOverRealAlembicStates(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the live Python producer runs only with DEV_HEALTH_LIVE_PYTHON_ORACLES=1 and the full project Python environment")
	}
	ctx := context.Background()
	productionEnv(t)
	t.Setenv("MIGRATION_DATABASE_URI", "")
	os.Unsetenv("MIGRATION_DATABASE_URI")
	baseline, current := currentBuild(t)
	instance, admin := startInstance(t)
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	python := pyoracle.Resolve(t, root)

	pythonState := func(revisions ...string) string {
		t.Helper()
		database := scratchDatabase(t, admin)
		pythonUpgrade(t, python, root, databaseURI(t, instance.URI, database), revisions...)
		return database
	}
	type state struct {
		name, database, verdict, reason string
	}
	states := []state{
		{"python: below the baseline (0137)", pythonState("0137", "0066"), pgmigrate.VerdictNeedsManual, pgmigrate.ReasonBelowBaseline},
		{"python: at the baseline (0138)", pythonState("0138", "0066"), pgmigrate.VerdictAppliesCleanly, pgmigrate.ReasonPendingRevs},
		{"python: the baseline without the cutover", pythonState("0138"), pgmigrate.VerdictNeedsManual, pgmigrate.ReasonCutoverMissing},
		{"python: at the head", pythonState("head"), pgmigrate.VerdictAtHead, pgmigrate.ReasonUpToDate},
	}
	for index := 0; index < len(current.chain)-1; index++ {
		revision := current.chain[index].Revision
		states = append(states, state{"python: at chain revision " + revision, pythonState(revision, "0066"), pgmigrate.VerdictAppliesCleanly, pgmigrate.ReasonPendingRevs})
	}
	for _, s := range states {
		t.Run(s.name, func(t *testing.T) {
			checkAgainstHook(t, ctx, admin, instance, s.name, s.database, productionSettings, baseline, current, s.verdict, s.reason)
		})
	}
	if !t.Failed() {
		venueoracle.WriteProof(t)
	}
}

//go:build integration

package pgmigrate_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// The applied-revision ledger (public.dho_chain_applied, CHAOS-11660). The fixture builds put a plain revision (0163)
// below the ledger revision (0164), so a database migrated by the build without the ledger has a chain revision the
// ledger must seed, and a plain revision above it (0165).

const ledgerFixtureProbeA = "-- Revision 0163 (down_revision 0162), written by hand.\nCREATE TABLE ledger_probe_a (id int PRIMARY KEY);\n"
const ledgerFixtureProbeB = "-- Revision 0165 (down_revision 0164), written by hand.\nCREATE TABLE ledger_probe_b (id int PRIMARY KEY);\n"

func ledgerRevisionSQL(t *testing.T, revision, previous string) string {
	t.Helper()
	data, err := os.ReadFile("sql/0163_add_dho_chain_applied.sql")
	if err != nil {
		t.Fatalf("the ledger revision file: %v", err)
	}
	header, rest, _ := strings.Cut(string(data), "\n")
	if header != "-- Revision 0163 (down_revision 0162), written by hand." {
		t.Fatalf("the ledger revision header is %q", header)
	}
	return "-- Revision " + revision + " (down_revision " + previous + "), written by hand.\n" + rest
}

// ledgerBuild is a chain of the fixture revisions through last (0163, 0164 or 0165).
func ledgerBuild(t *testing.T, last string) fstest.MapFS {
	t.Helper()
	fsys := fstest.MapFS{
		"sql/0163_probe_before_ledger.sql":      {Data: []byte(ledgerFixtureProbeA)},
		"sql/down/0163_probe_before_ledger.sql": {Data: []byte("-- Revision 0163 downgrade.\nDROP TABLE ledger_probe_a;\n")},
	}
	if last >= "0164" {
		fsys["sql/0164_add_dho_chain_applied.sql"] = &fstest.MapFile{Data: []byte(ledgerRevisionSQL(t, "0164", "0163"))}
		fsys["sql/down/0164_add_dho_chain_applied.sql"] = &fstest.MapFile{Data: []byte("-- Revision 0164 downgrade.\nDROP TABLE dho_chain_applied;\n")}
	}
	if last >= "0165" {
		fsys["sql/0165_probe_after_ledger.sql"] = &fstest.MapFile{Data: []byte(ledgerFixtureProbeB)}
		fsys["sql/down/0165_probe_after_ledger.sql"] = &fstest.MapFile{Data: []byte("-- Revision 0165 downgrade.\nDROP TABLE ledger_probe_b;\n")}
	}
	return fsys
}

func sha(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

// ledgerVerb runs `dho migrate postgres <name> [args]` against uri with the chain the test installed, and returns the
// exit code and the JSON document: the result on stdout, else the error line, the last line of stderr.
func ledgerVerb(t *testing.T, uri, name string, args ...string) (int, map[string]any) {
	t.Helper()
	resolve := pgmigrate.ResolveDSN(func(secrets.LookupEnv, io.Writer) (secrets.Value, string, bool) {
		return secrets.NewValue(uri), "test", true
	})
	var run func(context.Context, cli.Env) int
	for _, child := range pgmigrate.Command(resolve).Children {
		if child.Name == name {
			run = child.Run
		}
	}
	var stdout, stderr bytes.Buffer
	lookup := func(key string) (string, bool) {
		value, ok := map[string]string{pgmigrate.CutoverEnv: "1"}[key]
		return value, ok
	}
	code := run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &stdout, Stderr: &stderr})
	source := bytes.TrimSpace(stdout.Bytes())
	if len(source) == 0 {
		lines := bytes.Split(bytes.TrimRight(stderr.Bytes(), "\n"), []byte("\n"))
		source = lines[len(lines)-1]
	}
	var out map[string]any
	if err := json.Unmarshal(source, &out); err != nil {
		t.Fatalf("%s: exit %d, output %q is not JSON: %v (stderr %s)", name, code, source, err, stderr.String())
	}
	return code, out
}

// ledgerRows is the ledger as revision|source|sha (sha "-" for NULL), in revision order; nil when the table is absent.
func ledgerRows(t *testing.T, conn *pgx.Conn) []string {
	t.Helper()
	var present bool
	if err := conn.QueryRow(context.Background(), "SELECT to_regclass('public.dho_chain_applied') IS NOT NULL").Scan(&present); err != nil {
		t.Fatal(err)
	}
	if !present {
		return nil
	}
	rows, err := conn.Query(context.Background(), "SELECT revision || '|' || source || '|' || coalesce(sha256, '-') FROM public.dho_chain_applied ORDER BY revision")
	if err != nil {
		t.Fatal(err)
	}
	got, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func ledgerSnapshot(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var snapshot string
	if err := conn.QueryRow(context.Background(), "SELECT coalesce(string_agg(revision || '|' || source || '|' || coalesce(sha256, '-') || '|' || applied_at::text, ',' ORDER BY revision), '') FROM public.dho_chain_applied").Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// seededAt0164 is a database the fixture build without the ledger migrated to 0163, then the build with it to 0164.
func seededAt0164(t *testing.T, d downInstance) (string, *pgx.Conn) {
	t.Helper()
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0163"))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK {
		t.Fatalf("upgrade with the build without the ledger: exit %d %v", code, out)
	}
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0164"))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "chain_applied" {
		t.Fatalf("upgrade with the ledger build: exit %d %v, want chain_applied", code, out)
	}
	return uri, connect(t, uri)
}

func TestLedgerAFreshDatabaseRecordsEveryRevisionAsApplied(t *testing.T) {
	d := newDownInstance(t)
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0165"))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "baseline_applied" {
		t.Fatalf("upgrade an empty database: exit %d %v", code, out)
	}
	want := []string{
		"0163|apply|" + sha(ledgerFixtureProbeA),
		"0164|apply|" + sha(ledgerRevisionSQL(t, "0164", "0163")),
		"0165|apply|" + sha(ledgerFixtureProbeB),
	}
	if got := ledgerRows(t, connect(t, uri)); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ledger = %v, want %v", got, want)
	}
}

func TestLedgerTheRealChainRecordsItsRevisions(t *testing.T) {
	d := newDownInstance(t)
	uri := databaseURI(t, d.instance.URI, scratchDatabase(t, d.admin))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK {
		t.Fatalf("upgrade an empty database with the embedded chain: exit %d %v", code, out)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for _, file := range chain {
		want = append(want, file.Revision+"|apply|"+sha(file.SQL))
	}
	if got := ledgerRows(t, connect(t, uri)); len(want) == 0 || strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ledger = %v, want %v", got, want)
	}
}

func TestLedgerSeedsTheRevisionsRecordedBeforeIt(t *testing.T) {
	_, conn := seededAt0164(t, newDownInstance(t))
	want := []string{"0163|seed|-", "0164|apply|" + sha(ledgerRevisionSQL(t, "0164", "0163"))}
	if got := ledgerRows(t, conn); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("ledger = %v, want %v", got, want)
	}
}

// alembic_version holds two rows on every database this migrator builds: the application head and the River cutover
// head 0066, which the baseline records and the chain never moves (cutoverRevision). 0066 is no chain revision, so the
// ledger has no row for it and the walk does not ask for one.
func TestLedgerTwoRowAlembicVersion(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	if got := strings.Join(alembicVersions(t, uri), ","); got != "0066,0164" {
		t.Fatalf("alembic_version = %s, want 0066,0164", got)
	}
	for _, row := range ledgerRows(t, conn) {
		if strings.HasPrefix(row, "0066|") {
			t.Fatalf("the ledger has a row for the cutover head: %v", ledgerRows(t, conn))
		}
	}
	if code, out := ledgerVerb(t, uri, "status"); code != cli.ExitOK || out["state"] != "at_head" {
		t.Fatalf("status = %d %v, want at_head", code, out)
	}
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0165"))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "chain_applied" {
		t.Fatalf("upgrade to 0165 = %d %v, want chain_applied", code, out)
	}
	if got := strings.Join(alembicVersions(t, uri), ","); got != "0066,0165" {
		t.Fatalf("alembic_version = %s, want 0066,0165", got)
	}
	if got := ledgerRows(t, conn); len(got) != 3 || got[2] != "0165|apply|"+sha(ledgerFixtureProbeB) {
		t.Fatalf("ledger = %v, want 0165 recorded as applied", got)
	}
}

func TestLedgerARunAfterTheSeedWritesNothing(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	before := ledgerSnapshot(t, conn)
	if before == "" {
		t.Fatal("the ledger is empty after the seed")
	}
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "up_to_date" {
		t.Fatalf("re-run = %d %v, want up_to_date", code, out)
	}
	if after := ledgerSnapshot(t, conn); after != before {
		t.Fatalf("the re-run changed the ledger:\nbefore %s\nafter  %s", before, after)
	}
}

func TestLedgerRefusesARevisionBelowTheHeadWithNoRow(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	if _, err := conn.Exec(context.Background(), "DELETE FROM public.dho_chain_applied WHERE revision = '0163'"); err != nil {
		t.Fatal(err)
	}
	before := ledgerSnapshot(t, conn)
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0165"))
	code, out := ledgerVerb(t, uri, "upgrade")
	if code == cli.ExitOK || errorField(out, "code") != "chain_revision_unapplied" || !strings.Contains(errorField(out, "detail"), "0163") {
		t.Fatalf("upgrade = %d %v, want the chain_revision_unapplied refusal naming 0163", code, out)
	}
	if got := strings.Join(alembicVersions(t, uri), ","); got != "0066,0164" || ledgerSnapshot(t, conn) != before {
		t.Fatalf("the refusal changed the database: alembic_version %s", got)
	}
	if code, out := ledgerVerb(t, uri, "status"); code != cli.ExitOK || out["state"] != "chain_revision_unapplied" {
		t.Fatalf("status = %d %v, want chain_revision_unapplied", code, out)
	}
	if _, out := ledgerVerb(t, uri, "preflight"); out["verdict"] != "needs_manual" || out["reason"] != "chain_revision_unapplied" {
		t.Fatalf("preflight = %v, want needs_manual chain_revision_unapplied", out)
	}
}

func TestLedgerRefusesWhenTheTableIsGone(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	if _, err := conn.Exec(context.Background(), "DROP TABLE public.dho_chain_applied"); err != nil {
		t.Fatal(err)
	}
	if code, out := ledgerVerb(t, uri, "upgrade"); code == cli.ExitOK || errorField(out, "code") != "chain_revision_unapplied" {
		t.Fatalf("upgrade = %d %v, want chain_revision_unapplied", code, out)
	}
}

func TestLedgerRefusesAnAppliedRevisionWithAnotherSHA(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	if _, err := conn.Exec(context.Background(), "UPDATE public.dho_chain_applied SET sha256 = repeat('0', 64) WHERE revision = '0164'"); err != nil {
		t.Fatal(err)
	}
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0165"))
	code, out := ledgerVerb(t, uri, "upgrade")
	if code == cli.ExitOK || errorField(out, "code") != "chain_revision_sha_mismatch" || !strings.Contains(errorField(out, "detail"), "0164") {
		t.Fatalf("upgrade = %d %v, want the chain_revision_sha_mismatch refusal naming 0164", code, out)
	}
	if got := strings.Join(alembicVersions(t, uri), ","); got != "0066,0164" {
		t.Fatalf("the refusal moved alembic_version to %s", got)
	}
	if code, out := ledgerVerb(t, uri, "status"); code != cli.ExitOK || out["state"] != "chain_revision_sha_mismatch" {
		t.Fatalf("status = %d %v, want chain_revision_sha_mismatch", code, out)
	}
}

// A seed row has no sha, so the build's file for that revision is never compared with it.
func TestLedgerNeverComparesASeedRow(t *testing.T) {
	uri, _ := seededAt0164(t, newDownInstance(t))
	pgmigrate.UseChainFS(t, func() fstest.MapFS {
		fsys := ledgerBuild(t, "0165")
		fsys["sql/0163_probe_before_ledger.sql"] = &fstest.MapFile{Data: []byte(ledgerFixtureProbeA + "-- another build's text, same revision\n")}
		return fsys
	}())
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "chain_applied" {
		t.Fatalf("upgrade over a seed row = %d %v, want chain_applied", code, out)
	}
}

func TestLedgerDowngradeRemovesTheRevisionsRow(t *testing.T) {
	uri, conn := seededAt0164(t, newDownInstance(t))
	pgmigrate.UseChainFS(t, ledgerBuild(t, "0165"))
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK {
		t.Fatalf("upgrade to 0165 = %d %v", code, out)
	}
	if code, out := ledgerVerb(t, uri, "downgrade", "0164"); code != cli.ExitOK {
		t.Fatalf("downgrade to 0164 = %d %v", code, out)
	}
	if got := ledgerRows(t, conn); len(got) != 2 || strings.HasPrefix(got[len(got)-1], "0165|") {
		t.Fatalf("after the downgrade the ledger = %v, want no 0165 row", got)
	}
	if code, out := ledgerVerb(t, uri, "upgrade"); code != cli.ExitOK || out["action"] != "chain_applied" {
		t.Fatalf("up again = %d %v, want chain_applied", code, out)
	}
	if code, out := ledgerVerb(t, uri, "downgrade", "0163"); code != cli.ExitOK {
		t.Fatalf("downgrade to 0163 = %d %v", code, out)
	}
	if got := ledgerRows(t, conn); got != nil {
		t.Fatalf("below the ledger revision the table is still there: %v", got)
	}
}

// errorField is one field of the error document {"error": {"code", "detail"}}.
func errorField(out map[string]any, name string) string {
	inner, _ := out["error"].(map[string]any)
	text, _ := inner[name].(string)
	return text
}

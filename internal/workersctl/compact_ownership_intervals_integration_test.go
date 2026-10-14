//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestCompactOwnershipIntervalsVerbIsDryByDefault runs the verb against a real
// migrated ClickHouse: without --apply nothing is written and no audit row is
// asked for; --apply writes under one audit row; a second --apply finds
// nothing; --restore-run puts the rows back, also only with --apply. No output
// holds the organization id.
func TestCompactOwnershipIntervalsVerbIsDryByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	orgID := uuid.NewString()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for day := 0; day < 4; day++ {
		from, to := base.AddDate(0, 0, day), base.AddDate(0, 0, day+1)
		if err := conn.Exec(ctx, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'linear', 'linear:ENG', 'p1', NULL, 'native', 1, 100, 10, ?, ?, ?)`,
			orgID, from, to, to); err != nil {
			t.Fatal(err)
		}
	}
	rowCount := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM team_project_ownership FINAL WHERE org_id = ?`, orgID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	run := func(wantCode int, wantAudit int, args ...string) (map[string]providersync.OwnershipCompactionOutcome, string) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		runtime.lookup = func(key string) (string, bool) {
			if key == "CLICKHOUSE_URI" {
				return instance.URI, true
			}
			return "", false
		}
		runtime.stdin = strings.NewReader(orgID + "\n")
		var stdout, stderr bytes.Buffer
		full := append([]string{"providersync", "compact-ownership-intervals", "--org-stdin"}, args...)
		if code := dispatch(ctx, runtime, full, &stdout, &stderr); code != wantCode {
			t.Fatalf("%v: code=%d, want %d, stderr=%q", args, code, wantCode, stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), orgID) {
			t.Fatalf("%v: output holds the org id", args)
		}
		if len(auditor.events) != wantAudit {
			t.Fatalf("%v: %d audit rows, want %d", args, len(auditor.events), wantAudit)
		}
		answer := map[string]providersync.OwnershipCompactionOutcome{}
		if stdout.Len() > 0 {
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(stdout.Bytes(), &raw); err != nil {
				t.Fatalf("decode %q: %v", stdout.String(), err)
			}
			for key, value := range raw {
				if key == "error" {
					continue
				}
				var outcome providersync.OwnershipCompactionOutcome
				if err := json.Unmarshal(value, &outcome); err != nil {
					t.Fatalf("decode %s: %v", key, err)
				}
				answer[key] = outcome
			}
		}
		return answer, stderr.String()
	}

	if rowCount() != 4 {
		t.Fatalf("seeded rows %d", rowCount())
	}
	// No --apply: a dry run, no audit row, no write, no reason needed.
	answer, progress := run(0, 0, "--progress-every", "1")
	dry := answer["compact_ownership_intervals"]
	if dry.Apply || dry.Tables[0].Totals.RunsMerged != 1 || rowCount() != 4 {
		t.Fatalf("default run: %+v rows %d", dry, rowCount())
	}
	// CHAOS-9704: counts-only progress lines on stderr, one per phase at least.
	for _, phase := range []string{`"phase":"read"`, `"phase":"plan"`, `"phase":"runs"`, `"phase":"differential"`} {
		if !strings.Contains(progress, `{"progress":{"table":"team_project_ownership","org_ordinal":1,`+phase) {
			t.Fatalf("no %s progress line in %q", phase, progress)
		}
	}
	if _, quiet := run(0, 0, "--progress-every", "0"); strings.Contains(quiet, "progress") {
		t.Fatalf("--progress-every 0 printed progress: %q", quiet)
	}
	run(2, 0, "--timeout", "0")
	run(2, 0, "--timeout", "-1s")
	run(2, 0, "--progress-every", "-1")
	// A plan that runs out of its time exits non-zero with a typed code and writes nothing, also under --apply.
	if _, stderr := run(1, 1, append([]string{"--apply", "--workers-paused", "--quiet-seconds", "0", "--timeout", "1ns"}, auditFlags()...)...); !strings.Contains(stderr, `"code":"ownership_compaction_timeout"`) {
		t.Fatalf("timeout stderr %q", stderr)
	}
	if rowCount() != 4 {
		t.Fatal("a timed-out run wrote rows")
	}
	// --dry-run is accepted; --dry-run with --apply is refused; --apply needs its audit flags.
	run(0, 0, "--dry-run")
	run(2, 0, "--apply", "--dry-run")
	run(2, 0, "--apply")
	// Without the pause statement the audited write is refused after its audit row, and nothing is written.
	run(1, 1, append([]string{"--apply", "--quiet-seconds", "0"}, auditFlags()...)...)
	run(2, 0, "--batch-size", "0")
	run(2, 0, "--all-orgs")
	if rowCount() != 4 {
		t.Fatal("a refused request wrote rows")
	}

	applied, _ := run(0, 1, append([]string{"--apply", "--workers-paused", "--quiet-seconds", "0"}, auditFlags()...)...)
	result := applied["compact_ownership_intervals"]
	if !result.Apply || result.Tables[0].Totals.RunsMerged != 1 || result.Tables[0].Totals.ClosedAfter != 1 || rowCount() != 1 {
		t.Fatalf("apply: %+v rows %d", result, rowCount())
	}
	second, _ := run(0, 1, append([]string{"--apply", "--workers-paused", "--quiet-seconds", "0"}, auditFlags()...)...)
	if again := second["compact_ownership_intervals"].Tables[0].Totals; again.RunsMerged != 0 || again.BackupRows != 0 {
		t.Fatalf("second apply: %+v", again)
	}

	restored, _ := run(0, 0, "--restore-run", result.RunID)
	if got := restored["restore_ownership_compaction"].Tables[0].Totals.RowsMerged; got != 4 || rowCount() != 1 {
		t.Fatalf("restore dry run counts %d, rows %d", got, rowCount())
	}
	run(0, 1, append([]string{"--restore-run", result.RunID, "--apply", "--workers-paused", "--quiet-seconds", "0"}, auditFlags()...)...)
	if rowCount() != 4 {
		t.Fatalf("rows after the restore: %d", rowCount())
	}
}

// exportedTables are the two tables of the compaction, with the natural key column each is ordered by.
var exportedTables = [][2]string{{"team_project_ownership", "project_id"}, {"team_repo_ownership", "repo_full_name"}}

// writeOwnershipTSV writes every PHYSICAL row of both tables, all columns, to <dir>/<phase>-<table>.tsv (one header
// line, millisecond timestamps as stored, \\N for NULL: the format the providersync fixture export uses).
func writeOwnershipTSV(t *testing.T, ctx context.Context, conn driver.Conn, dir, phase string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, entry := range exportedTables {
		table := entry[0]
		rows, err := conn.Query(ctx, "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position", table)
		if err != nil {
			t.Fatal(err)
		}
		var names, cells []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			names = append(names, name)
			cells = append(cells, "coalesce(toString("+name+"), '\\\\N')")
		}
		rows.Close()
		lines, err := conn.Query(ctx, "SELECT concat("+strings.Join(cells, ", '\t', ")+") FROM "+table)
		if err != nil {
			t.Fatal(err)
		}
		var texts []string
		for lines.Next() {
			var line string
			if err := lines.Scan(&line); err != nil {
				t.Fatal(err)
			}
			texts = append(texts, line)
		}
		lines.Close()
		sort.Strings(texts)
		var out strings.Builder
		out.WriteString(strings.Join(names, "\t") + "\n")
		for _, line := range texts {
			out.WriteString(line + "\n")
		}
		if err := os.WriteFile(dir+"/"+phase+"-"+table+".tsv", []byte(out.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		counts[table] = len(texts)
	}
	return counts
}

// loadOwnershipTSV puts the rows of the files written by writeOwnershipTSV into the two tables, every column as given.
func loadOwnershipTSV(t *testing.T, ctx context.Context, conn driver.Conn, dir, phase string) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, entry := range exportedTables {
		table := entry[0]
		data, err := os.ReadFile(dir + "/" + phase + "-" + table + ".tsv")
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
		header := strings.Split(lines[0], "\t")
		types := map[string]string{}
		typeRows, err := conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ?", table)
		if err != nil {
			t.Fatal(err)
		}
		for typeRows.Next() {
			var name, kind string
			if err := typeRows.Scan(&name, &kind); err != nil {
				t.Fatal(err)
			}
			types[name] = kind
		}
		typeRows.Close()
		// One insert per row: ReplacingMergeTree merges the rows of ONE inserted block, which would drop the
		// stale versions that the exported file holds.
		for _, line := range lines[1:] {
			cells := strings.Split(line, "\t")
			args := make([]any, len(cells))
			for index, cell := range cells {
				args[index] = parseTSVCell(t, types[header[index]], cell)
			}
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(header, ", ")+")")
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Append(args...); err != nil {
				t.Fatalf("%s: %v", table, err)
			}
			if err := batch.Send(); err != nil {
				t.Fatal(err)
			}
		}
		counts[table] = len(lines) - 1
	}
	return counts
}

func parseTSVCell(t *testing.T, kind, cell string) any {
	t.Helper()
	nullable := strings.HasPrefix(kind, "Nullable(")
	kind = strings.TrimSuffix(strings.TrimPrefix(kind, "Nullable("), ")")
	if cell == `\N` {
		if !nullable {
			t.Fatalf("NULL in a non-nullable %s column", kind)
		}
		return nil
	}
	var value any
	var err error
	switch {
	case kind == "UUID":
		value, err = uuid.Parse(cell)
	case kind == "UInt8":
		var n uint64
		n, err = strconv.ParseUint(cell, 10, 8)
		value = uint8(n)
	case kind == "UInt16":
		var n uint64
		n, err = strconv.ParseUint(cell, 10, 16)
		value = uint16(n)
	case kind == "Int32":
		var n int64
		n, err = strconv.ParseInt(cell, 10, 32)
		value = int32(n)
	case kind == "Float64":
		value, err = strconv.ParseFloat(cell, 64)
	case strings.HasPrefix(kind, "DateTime64"):
		value, err = time.ParseInLocation("2006-01-02 15:04:05.000", cell, time.UTC)
	default:
		value = cell
	}
	if err != nil {
		t.Fatalf("cell %q as %s: %v", cell, kind, err)
	}
	return value
}

// TestCompactOwnershipIntervalsVerbOnTheExportedFixture runs the REAL verb (the dispatch path with its flags:
// --all-orgs --apply under an audit row) on the fixture that the providersync test exported, and writes the rows
// after. Set OWNERSHIP_COMPACTION_EXPORT_DIR; the directory must hold fixture-before.tsv.
func TestCompactOwnershipIntervalsVerbOnTheExportedFixture(t *testing.T) {
	dir := os.Getenv("OWNERSHIP_COMPACTION_EXPORT_DIR")
	if dir == "" {
		t.Skip("set OWNERSHIP_COMPACTION_EXPORT_DIR (with fixture-before.tsv) to run the verb on the exported fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	for _, entry := range exportedTables {
		if err := conn.Exec(ctx, "SYSTEM STOP MERGES "+entry[0]); err != nil {
			t.Fatalf("stop merges: %v", err)
		}
	}
	loaded := loadOwnershipTSV(t, ctx, conn, dir, "fixture-before")
	reloaded := writeOwnershipTSV(t, ctx, conn, dir, "fixture-before-reloaded")
	for _, entry := range exportedTables {
		original, _ := os.ReadFile(dir + "/fixture-before-" + entry[0] + ".tsv")
		again, _ := os.ReadFile(dir + "/fixture-before-reloaded-" + entry[0] + ".tsv")
		if !bytes.Equal(original, again) || loaded[entry[0]] != reloaded[entry[0]] {
			t.Fatalf("%s: the fixture the verb ran on is not the exported before file", entry[0])
		}
		if err := os.Remove(dir + "/fixture-before-reloaded-" + entry[0] + ".tsv"); err != nil {
			t.Fatal(err)
		}
	}
	// ALTER TABLE ... DELETE runs in the merge pool: with merges stopped it would wait for ever.
	for _, entry := range exportedTables {
		if err := conn.Exec(ctx, "SYSTEM START MERGES "+entry[0]); err != nil {
			t.Fatalf("start merges: %v", err)
		}
	}
	run := func(args ...string) (string, string) {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		runtime.lookup = func(key string) (string, bool) {
			if key == "CLICKHOUSE_URI" {
				return instance.URI, true
			}
			return "", false
		}
		var stdout, stderr bytes.Buffer
		if code := dispatch(ctx, runtime, append([]string{"providersync", "compact-ownership-intervals"}, args...), &stdout, &stderr); code != 0 {
			t.Fatalf("%v: code=%d stderr=%q stdout=%q", args, code, stderr.String(), stdout.String())
		}
		return stdout.String(), stderr.String()
	}
	dry, _ := run("--all-orgs")
	applied, _ := run(append([]string{"--all-orgs", "--apply", "--workers-paused", "--quiet-seconds", "0"}, auditFlags()...)...)
	second, _ := run(append([]string{"--all-orgs", "--apply", "--workers-paused", "--quiet-seconds", "0"}, auditFlags()...)...)
	after := writeOwnershipTSV(t, ctx, conn, dir, "fixture-after")
	for name, body := range map[string]string{"verb-dry-run.json": dry, "verb-apply.json": applied, "verb-second-apply.json": second} {
		if err := os.WriteFile(dir+"/"+name, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("%s", fmt.Sprintf("physical rows before %v, after %v", loaded, after))
}

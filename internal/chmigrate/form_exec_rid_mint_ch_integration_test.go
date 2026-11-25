//go:build integration

package chmigrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// DO NOT MERGE. This file executes the replica gate and the id read of a prod read FORM byte for byte, with every prod value
// and every comment text swapped to a synthetic one (the sha256 below is the sha256 of the synthetic copy). The id read mints
// each repository id in ClickHouse from its stored name (sha256 of the lowercased trimmed name, 32 hex shaped 8-4-4-4-12, the
// repositoryIdentity rule of the GitHub route) and prints counts only: rows, stored id = mint, mint has the cut, name = the
// expected name, per cut.
const formExecRidMintCHSHA256 = "ecb152872603328d0f027faa28071e0659e62df0f7feab4c0187bdbbfc591ebb"

const formExecRidMintCHSQL = ``

type formExecRidMintCHResult struct {
	columns []string
	rows    [][]string
}

func formExecRidMintCHQuery(ctx context.Context, conn driver.Conn, statement string) (formExecRidMintCHResult, error) {
	rows, err := conn.Query(ctx, statement)
	if err != nil {
		return formExecRidMintCHResult{}, err
	}
	defer func() { _ = rows.Close() }()
	result := formExecRidMintCHResult{columns: rows.Columns()}
	types := rows.ColumnTypes()
	for rows.Next() {
		dest := make([]any, len(types))
		for index, columnType := range types {
			dest[index] = reflect.New(columnType.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return formExecRidMintCHResult{}, fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(dest))
		for index := range dest {
			row[index] = fmt.Sprint(reflect.ValueOf(dest[index]).Elem().Interface())
		}
		result.rows = append(result.rows, row)
	}
	return result, rows.Err()
}

func formExecRidMintCHMint(name string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(name))))
	h := hex.EncodeToString(sum[:16])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// The fixture (org f0f0f0f0-...01):
//
//	cut B: fx/rewalk-mint-b stored at its mint (b28bc582-...).
//	cut A: stored as "FX/Rewalk-Mint-A " (mixed case, a trailing space) at the mint of fx/rewalk-mint-a (b07e2a0b-...): the
//	       read lowers and trims the stored name, so stored id = mint and name = the expected name.
//	not counted: a repo of another org (d0d0d0d0-...02) at cut B's mint, and a gitlab repo of the org with cut B's prefix.
//
// Mutants: each holds its own org prefix, and the org literal is swapped in a test copy of the read only.
//
//	e0e0e0e0: a repo named fx/rewalk-mint-b whose stored id has cut B's prefix but is NOT its mint (stored_eq_minted 0), and a
//	          repo with cut A's prefix whose stored name is another one (name_eq 0; then its id cannot be that name's mint
//	          with cut A's prefix either: stored_eq_minted 0, minted_has_cut 0).
//	e1e1e1e1: cut B's repo stored with a TAB after its name, at the id Go writes (TrimSpace drops the tab before the hash).
//	          trimBoth keeps the tab: name_eq 0, the in-SQL mint differs (stored_eq_minted 0, minted_has_cut 0) = a STOP,
//	          never a false PASS.
//	e2e2e2e2: a second repo of the same org with cut B's prefix: rows 2.
//	e3e3e3e3: the same repo (cut B's mint) in two orgs that share the org prefix: rows 2 (the mint is org-free).
func TestFormExecRidMintCH(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	exec := func(statement string) {
		t.Helper()
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	sum := sha256.Sum256([]byte(formExecRidMintCHSQL))
	if got := hex.EncodeToString(sum[:]); got != formExecRidMintCHSHA256 {
		t.Fatalf("sha256 of the embedded text = %s, want %s (the synthetic copy)", got, formExecRidMintCHSHA256)
	}
	statements := map[string]string{}
	var ids []string
	for _, piece := range strings.Split(formExecRidMintCHSQL, ";\n") {
		statement := strings.TrimSpace(piece)
		id := ""
		hasSQL := false
		for _, line := range strings.Split(statement, "\n") {
			if strings.HasPrefix(line, "--") {
				if strings.Contains(line, ";") {
					t.Errorf("a comment line holds a ';': %s", line)
				}
				if fields := strings.Fields(line); id == "" && len(fields) > 1 {
					id = fields[1]
				}
				continue
			}
			if strings.TrimSpace(line) != "" {
				hasSQL = true
			}
		}
		if hasSQL {
			ids = append(ids, id)
			statements[id] = statement
		}
	}
	if got := strings.Join(ids, ","); got != "stmtR,RID" {
		t.Fatalf("statements = %s, want stmtR,RID", got)
	}
	rid := statements["RID"]
	for _, literal := range []string{"> 1000000", "> 4294967296", "WITH (SELECT throwIf(", "'STOP-COST-GATE'", "FROM repos FINAL", "startsWith(org_id, 'f0f0f0f0')"} {
		if got := strings.Count(rid, literal); got != 1 {
			t.Fatalf("RID holds %q %d time(s), want 1", literal, got)
		}
	}
	if regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`).MatchString(rid) {
		t.Fatalf("RID embeds a full id")
	}

	const (
		org   = "f0f0f0f0-0000-4000-8000-000000000001"
		other = "d0d0d0d0-0000-4000-8000-000000000002"
		mut   = "e0e0e0e0-0000-4000-8000-000000000003"
		tab   = "e1e1e1e1-0000-4000-8000-000000000004"
		two   = "e2e2e2e2-0000-4000-8000-000000000005"
		twoA  = "e3e3e3e3-0000-4000-8000-000000000006"
		twoB  = "e3e3e3e3-0000-4000-8000-000000000007"
	)
	cutB := formExecRidMintCHMint("fx/rewalk-mint-b")
	cutA := formExecRidMintCHMint("fx/rewalk-mint-a")
	if !strings.HasPrefix(cutB, "b28bc582") || !strings.HasPrefix(cutA, "b07e2a0b") {
		t.Fatalf("fixture mints %s / %s, want the prefixes b28bc582 / b07e2a0b", cutB, cutA)
	}
	exec("SYSTEM STOP MERGES repos")
	repo := func(orgID, id, name, provider string) string {
		return fmt.Sprintf("('%s', '%s', '2025-04-22 00:00:00.000', '2026-02-20 00:00:00.000', '%s', '%s')", id, name, orgID, provider)
	}
	repos := `INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES `
	exec(repos + strings.Join([]string{
		repo(org, cutB, "fx/rewalk-mint-b", "github"),
		repo(org, cutA, "FX/Rewalk-Mint-A ", "github"),
		repo(other, cutB, "fx/rewalk-mint-b", "github"),
		repo(org, "b28bc582-0000-4000-8000-0000000000f1", "fx/rewalk-mint-b", "gitlab"),
		repo(mut, "b28bc582-0000-4000-8000-0000000000e1", "fx/rewalk-mint-b", "github"),
		repo(mut, "b07e2a0b-0000-4000-8000-0000000000e2", "fx/not-this-repo", "github"),
		repo(tab, formExecRidMintCHMint("fx/rewalk-mint-b\t"), `fx/rewalk-mint-b\t`, "github"),
		repo(two, cutB, "fx/rewalk-mint-b", "github"),
		repo(two, "b28bc582-0000-4000-8000-0000000000b2", "fx/rewalk-mint-b2", "github"),
		repo(twoA, cutB, "fx/rewalk-mint-b", "github"),
		repo(twoB, cutB, "fx/rewalk-mint-b", "github"),
	}, ", "))
	if formExecRidMintCHMint("fx/rewalk-mint-b\t") != cutB {
		t.Fatalf("the Go rule must drop the tab before the hash")
	}

	query := func(label, statement string) (formExecRidMintCHResult, string, error) {
		comment := fmt.Sprintf("formridmint-%s-%d", label, time.Now().UnixNano())
		qctx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"log_comment": comment}))
		result, err := formExecRidMintCHQuery(qctx, conn, statement)
		if err == nil {
			t.Logf("RID-MINT %s columns %v", label, result.columns)
			for index, row := range result.rows {
				t.Logf("RID-MINT %s row %d %v", label, index, row)
			}
		}
		return result, comment, err
	}
	want := func(label string, result formExecRidMintCHResult, rows ...string) {
		t.Helper()
		if got := strings.Join(result.columns, "|"); got != "cut|rows|stored_eq_minted|minted_has_cut|name_eq" {
			t.Errorf("%s: columns %s", label, got)
		}
		var got []string
		for _, row := range result.rows {
			got = append(got, strings.Join(row, "|"))
		}
		if strings.Join(got, ";") != strings.Join(rows, ";") {
			t.Errorf("%s: rows %v, want %v", label, got, rows)
		}
	}

	replicas, _, err := query("stmtR", statements["stmtR"])
	if err != nil || len(replicas.rows) != 1 || replicas.rows[0][0] != "1" {
		t.Fatalf("stmtR: %v %v, want replicas 1", replicas.rows, err)
	}
	pass, passComment, err := query("pass", rid)
	if err != nil {
		t.Fatalf("RID: %v", err)
	}
	want("pass", pass, "b07e2a0b|1|1|1|1", "b28bc582|1|1|1|1")

	mutant := func(label, prefix string, rows ...string) {
		t.Helper()
		result, _, err := query(label, strings.Replace(rid, "startsWith(org_id, 'f0f0f0f0')", "startsWith(org_id, '"+prefix+"')", 1))
		if err != nil {
			t.Fatalf("RID %s: %v", label, err)
		}
		want(label, result, rows...)
	}
	mutant("stored id not the mint, another name", "e0e0e0e0", "b07e2a0b|1|0|0|0", "b28bc582|1|0|1|1")
	mutant("tab in the stored name", "e1e1e1e1", "b28bc582|1|0|0|0")
	mutant("a second repo with the prefix", "e2e2e2e2", "b28bc582|2|1|1|1")
	mutant("the same repo in two orgs of the prefix", "e3e3e3e3", "b28bc582|2|2|2|2")

	// The gates fire: each cap literal lowered to 0 in a test copy only. WANT code 395 STOP-COST-GATE, query_log
	// ExceptionBeforeStart, no data table read.
	var fires []string
	for _, literal := range []string{"> 1000000", "> 4294967296"} {
		_, comment, err := query("gate-"+strings.TrimPrefix(literal, "> "), strings.Replace(rid, literal, "> 0", 1))
		if err == nil || !strings.Contains(err.Error(), "STOP-COST-GATE") || !strings.Contains(err.Error(), "395") {
			t.Errorf("GATE %s lowered to 0: err = %v, want code 395 STOP-COST-GATE", literal, err)
			continue
		}
		fires = append(fires, comment)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		t.Fatalf("the query log is not readable by this user: %v", err)
	}
	var parts, rowsInRepos uint64
	if err := conn.QueryRow(ctx, `SELECT count(), sum(rows) FROM system.parts WHERE active AND database = currentDatabase() AND table = 'repos'`).Scan(&parts, &rowsInRepos); err != nil {
		t.Fatal(err)
	}
	logOf := func(comment string) (string, uint64, string) {
		var typ, tables string
		var read uint64
		if err := conn.QueryRow(ctx, `SELECT toString(type), read_rows, arrayStringConcat(tables, ',') FROM system.query_log
			WHERE log_comment = ? AND type != 'QueryStart' ORDER BY event_time_microseconds DESC LIMIT 1`, comment).Scan(&typ, &read, &tables); err != nil {
			t.Fatalf("query log %s: %v", comment, err)
		}
		return typ, read, tables
	}
	for _, comment := range fires {
		typ, read, tables := logOf(comment)
		t.Logf("GATE %s type=%s read_rows=%d tables=%s", comment, typ, read, tables)
		if typ != "ExceptionBeforeStart" || read > 2*parts+1 || !strings.Contains(tables, "system.parts") || strings.Contains(tables, ".repos") {
			t.Errorf("GATE %s: type %s read_rows %d tables %s, want ExceptionBeforeStart, <= %d, system.parts only", comment, typ, read, tables, 2*parts+1)
		}
	}
	if len(fires) != 2 {
		t.Errorf("gates fired %d times, want 2", len(fires))
	}
	typ, read, _ := logOf(passComment)
	if bound := 2*rowsInRepos + 4*parts + 2; typ != "QueryFinish" || read == 0 || read > bound {
		t.Errorf("PASS RID: type %s read_rows %d, want QueryFinish and 1 .. %d", typ, read, bound)
	}
	t.Logf("PASS RID read_rows=%d parts=%d rows=%d", read, parts, rowsInRepos)
}

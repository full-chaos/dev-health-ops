//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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
		full := append([]string{"providersync", "compact-ownership-intervals", "--org-stdin", "--table", "team_project_ownership"}, args...)
		if code := dispatch(ctx, runtime, full, &stdout, &stderr); code != wantCode {
			t.Fatalf("%v: code=%d, want %d, stderr=%q", args, code, wantCode, stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), orgID) {
			t.Fatalf("%v: output holds the org id", args)
		}
		if len(auditor.events) != wantAudit {
			t.Fatalf("%v: %d audit rows, want %d", args, len(auditor.events), wantAudit)
		}
		var answer map[string]providersync.OwnershipCompactionOutcome
		if stdout.Len() > 0 {
			if err := json.Unmarshal(stdout.Bytes(), &answer); err != nil {
				t.Fatalf("decode %q: %v", stdout.String(), err)
			}
		}
		return answer, stderr.String()
	}

	if rowCount() != 4 {
		t.Fatalf("seeded rows %d", rowCount())
	}
	// No --apply: a dry run, no audit row, no write, no reason needed.
	answer, _ := run(0, 0)
	dry := answer["compact_ownership_intervals"]
	if dry.Apply || dry.Tables[0].Totals.RunsMerged != 1 || rowCount() != 4 {
		t.Fatalf("default run: %+v rows %d", dry, rowCount())
	}
	// --dry-run is accepted; --dry-run with --apply is refused; --apply needs its audit flags.
	run(0, 0, "--dry-run")
	run(2, 0, "--apply", "--dry-run")
	run(2, 0, "--apply")
	run(2, 0, "--batch-size", "0")
	run(2, 0, "--all-orgs")
	if rowCount() != 4 {
		t.Fatal("a refused request wrote rows")
	}

	applied, _ := run(0, 1, append([]string{"--apply"}, auditFlags()...)...)
	result := applied["compact_ownership_intervals"]
	if !result.Apply || result.Tables[0].Totals.RunsMerged != 1 || result.Tables[0].Totals.ClosedAfter != 1 || rowCount() != 1 {
		t.Fatalf("apply: %+v rows %d", result, rowCount())
	}
	second, _ := run(0, 1, append([]string{"--apply"}, auditFlags()...)...)
	if again := second["compact_ownership_intervals"].Tables[0].Totals; again.RunsMerged != 0 || again.BackupRows != 0 {
		t.Fatalf("second apply: %+v", again)
	}

	restored, _ := run(0, 0, "--restore-run", result.RunID)
	if got := restored["restore_ownership_compaction"].Tables[0].Totals.RowsMerged; got != 4 || rowCount() != 1 {
		t.Fatalf("restore dry run counts %d, rows %d", got, rowCount())
	}
	run(0, 1, append([]string{"--restore-run", result.RunID, "--apply"}, auditFlags()...)...)
	if rowCount() != 4 {
		t.Fatalf("rows after the restore: %d", rowCount())
	}
}
